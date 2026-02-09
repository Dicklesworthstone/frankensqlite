//! WAL-FEC sidecar format (`.wal-fec`) for self-healing WAL durability (§3.4.1).
//!
//! The sidecar is append-only. Each group is encoded as:
//! 1. length-prefixed [`WalFecGroupMeta`]
//! 2. `R` length-prefixed ECS [`SymbolRecord`] repair symbols (`esi = K..K+R-1`)
//!
//! Source symbols remain in `.wal` frames and are never duplicated in sidecar.

use std::fmt;
use std::fs::{self, File, OpenOptions};
use std::io::Write;
use std::mem::size_of;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, mpsc};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use fsqlite_error::{FrankenError, Result};
use fsqlite_types::{ObjectId, Oti, PageSize, SymbolRecord, SymbolRecordFlags};
use tracing::{debug, error, info, warn};
use xxhash_rust::xxh3::xxh3_64;

use crate::checksum::{WalSalts, Xxh3Checksum128, wal_fec_source_hash_xxh3_128};

/// Magic bytes for [`WalFecGroupMeta`].
pub const WAL_FEC_GROUP_META_MAGIC: [u8; 8] = *b"FSQLWFEC";
/// Current [`WalFecGroupMeta`] wire version.
pub const WAL_FEC_GROUP_META_VERSION: u32 = 1;

const LENGTH_PREFIX_BYTES: usize = 4;
const META_FIXED_PREFIX_BYTES: usize = 8 + 4 + (8 * 4) + 22 + 16;
const META_CHECKSUM_BYTES: usize = 8;

/// Unique commit-group identifier:
/// `group_id := (wal_salt1, wal_salt2, end_frame_no)`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct WalFecGroupId {
    pub wal_salt1: u32,
    pub wal_salt2: u32,
    pub end_frame_no: u32,
}

impl fmt::Display for WalFecGroupId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "({}, {}, {})",
            self.wal_salt1, self.wal_salt2, self.end_frame_no
        )
    }
}

/// Builder fields for [`WalFecGroupMeta`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WalFecGroupMetaInit {
    pub wal_salt1: u32,
    pub wal_salt2: u32,
    pub start_frame_no: u32,
    pub end_frame_no: u32,
    pub db_size_pages: u32,
    pub page_size: u32,
    pub k_source: u32,
    pub r_repair: u32,
    pub oti: Oti,
    pub object_id: ObjectId,
    pub page_numbers: Vec<u32>,
    pub source_page_xxh3_128: Vec<Xxh3Checksum128>,
}

/// Length-prefixed metadata record preceding repair symbols.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WalFecGroupMeta {
    pub magic: [u8; 8],
    pub version: u32,
    pub wal_salt1: u32,
    pub wal_salt2: u32,
    pub start_frame_no: u32,
    pub end_frame_no: u32,
    pub db_size_pages: u32,
    pub page_size: u32,
    pub k_source: u32,
    pub r_repair: u32,
    pub oti: Oti,
    pub object_id: ObjectId,
    pub page_numbers: Vec<u32>,
    pub source_page_xxh3_128: Vec<Xxh3Checksum128>,
    pub checksum: u64,
}

impl WalFecGroupMeta {
    /// Create and validate metadata, computing checksum automatically.
    pub fn from_init(init: WalFecGroupMetaInit) -> Result<Self> {
        let mut meta = Self {
            magic: WAL_FEC_GROUP_META_MAGIC,
            version: WAL_FEC_GROUP_META_VERSION,
            wal_salt1: init.wal_salt1,
            wal_salt2: init.wal_salt2,
            start_frame_no: init.start_frame_no,
            end_frame_no: init.end_frame_no,
            db_size_pages: init.db_size_pages,
            page_size: init.page_size,
            k_source: init.k_source,
            r_repair: init.r_repair,
            oti: init.oti,
            object_id: init.object_id,
            page_numbers: init.page_numbers,
            source_page_xxh3_128: init.source_page_xxh3_128,
            checksum: 0,
        };
        meta.validate_invariants()?;
        meta.checksum = meta.compute_checksum();
        Ok(meta)
    }

    /// Return `(wal_salt1, wal_salt2, end_frame_no)`.
    #[must_use]
    pub const fn group_id(&self) -> WalFecGroupId {
        WalFecGroupId {
            wal_salt1: self.wal_salt1,
            wal_salt2: self.wal_salt2,
            end_frame_no: self.end_frame_no,
        }
    }

    /// Verify metadata is bound to the WAL salts.
    pub fn verify_salt_binding(&self, salts: WalSalts) -> Result<()> {
        if self.wal_salt1 != salts.salt1 || self.wal_salt2 != salts.salt2 {
            return Err(FrankenError::WalCorrupt {
                detail: format!(
                    "wal-fec salt mismatch for group {}: sidecar=({}, {}), wal=({}, {})",
                    self.group_id(),
                    self.wal_salt1,
                    self.wal_salt2,
                    salts.salt1,
                    salts.salt2
                ),
            });
        }
        Ok(())
    }

    /// Serialize as on-disk record payload (without outer length prefix).
    #[must_use]
    pub fn to_record_bytes(&self) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(self.serialized_len_without_prefix());
        bytes.extend_from_slice(&self.magic);
        append_u32_le(&mut bytes, self.version);
        append_u32_le(&mut bytes, self.wal_salt1);
        append_u32_le(&mut bytes, self.wal_salt2);
        append_u32_le(&mut bytes, self.start_frame_no);
        append_u32_le(&mut bytes, self.end_frame_no);
        append_u32_le(&mut bytes, self.db_size_pages);
        append_u32_le(&mut bytes, self.page_size);
        append_u32_le(&mut bytes, self.k_source);
        append_u32_le(&mut bytes, self.r_repair);
        bytes.extend_from_slice(&self.oti.to_bytes());
        bytes.extend_from_slice(self.object_id.as_bytes());
        for &page_number in &self.page_numbers {
            append_u32_le(&mut bytes, page_number);
        }
        for &hash in &self.source_page_xxh3_128 {
            bytes.extend_from_slice(&hash.to_le_bytes());
        }
        append_u64_le(&mut bytes, self.checksum);
        bytes
    }

    /// Deserialize and validate metadata from an on-disk payload.
    pub fn from_record_bytes(bytes: &[u8]) -> Result<Self> {
        if bytes.len() < META_FIXED_PREFIX_BYTES + META_CHECKSUM_BYTES {
            return Err(FrankenError::WalCorrupt {
                detail: format!(
                    "wal-fec group meta too short: expected at least {}, got {}",
                    META_FIXED_PREFIX_BYTES + META_CHECKSUM_BYTES,
                    bytes.len()
                ),
            });
        }

        let mut cursor = 0usize;
        let magic = read_array::<8>(bytes, &mut cursor, "magic")?;
        if magic != WAL_FEC_GROUP_META_MAGIC {
            return Err(FrankenError::WalCorrupt {
                detail: format!("invalid wal-fec magic: {magic:02x?}"),
            });
        }

        let version = read_u32_le(bytes, &mut cursor, "version")?;
        if version != WAL_FEC_GROUP_META_VERSION {
            return Err(FrankenError::WalCorrupt {
                detail: format!(
                    "unsupported wal-fec version {version}, expected {WAL_FEC_GROUP_META_VERSION}"
                ),
            });
        }

        let wal_salt1 = read_u32_le(bytes, &mut cursor, "wal_salt1")?;
        let wal_salt2 = read_u32_le(bytes, &mut cursor, "wal_salt2")?;
        let start_frame_no = read_u32_le(bytes, &mut cursor, "start_frame_no")?;
        let end_frame_no = read_u32_le(bytes, &mut cursor, "end_frame_no")?;
        let db_size_pages = read_u32_le(bytes, &mut cursor, "db_size_pages")?;
        let page_size = read_u32_le(bytes, &mut cursor, "page_size")?;
        let k_source = read_u32_le(bytes, &mut cursor, "k_source")?;
        let r_repair = read_u32_le(bytes, &mut cursor, "r_repair")?;
        let oti_bytes = read_array::<22>(bytes, &mut cursor, "oti")?;
        let oti = Oti::from_bytes(&oti_bytes).ok_or_else(|| FrankenError::WalCorrupt {
            detail: "invalid wal-fec OTI encoding".to_owned(),
        })?;
        let object_id = ObjectId::from_bytes(read_array::<16>(bytes, &mut cursor, "object_id")?);

        let k_source_usize = usize::try_from(k_source).map_err(|_| FrankenError::WalCorrupt {
            detail: format!("k_source {k_source} does not fit in usize"),
        })?;
        let mut page_numbers = Vec::with_capacity(k_source_usize);
        for _ in 0..k_source_usize {
            page_numbers.push(read_u32_le(bytes, &mut cursor, "page_number")?);
        }
        let mut source_page_xxh3_128 = Vec::with_capacity(k_source_usize);
        for _ in 0..k_source_usize {
            let digest = read_array::<16>(bytes, &mut cursor, "source_page_hash")?;
            source_page_xxh3_128.push(Xxh3Checksum128 {
                low: u64::from_le_bytes(digest[..8].try_into().expect("8-byte low hash slice")),
                high: u64::from_le_bytes(digest[8..].try_into().expect("8-byte high hash slice")),
            });
        }
        let checksum = read_u64_le(bytes, &mut cursor, "checksum")?;
        if cursor != bytes.len() {
            return Err(FrankenError::WalCorrupt {
                detail: format!(
                    "wal-fec group meta trailing bytes: consumed {cursor}, total {}",
                    bytes.len()
                ),
            });
        }

        let meta = Self {
            magic,
            version,
            wal_salt1,
            wal_salt2,
            start_frame_no,
            end_frame_no,
            db_size_pages,
            page_size,
            k_source,
            r_repair,
            oti,
            object_id,
            page_numbers,
            source_page_xxh3_128,
            checksum,
        };
        meta.validate_invariants()?;
        let computed = meta.compute_checksum();
        if computed != meta.checksum {
            return Err(FrankenError::WalCorrupt {
                detail: format!(
                    "wal-fec group checksum mismatch: stored {:#018x}, computed {computed:#018x}",
                    meta.checksum
                ),
            });
        }
        Ok(meta)
    }

    fn serialized_len_without_prefix(&self) -> usize {
        META_FIXED_PREFIX_BYTES
            + self.page_numbers.len() * size_of::<u32>()
            + self.source_page_xxh3_128.len() * size_of::<[u8; 16]>()
            + META_CHECKSUM_BYTES
    }

    fn compute_checksum(&self) -> u64 {
        xxh3_64(&self.to_record_bytes_without_checksum())
    }

    fn to_record_bytes_without_checksum(&self) -> Vec<u8> {
        let mut bytes =
            Vec::with_capacity(self.serialized_len_without_prefix() - META_CHECKSUM_BYTES);
        bytes.extend_from_slice(&self.magic);
        append_u32_le(&mut bytes, self.version);
        append_u32_le(&mut bytes, self.wal_salt1);
        append_u32_le(&mut bytes, self.wal_salt2);
        append_u32_le(&mut bytes, self.start_frame_no);
        append_u32_le(&mut bytes, self.end_frame_no);
        append_u32_le(&mut bytes, self.db_size_pages);
        append_u32_le(&mut bytes, self.page_size);
        append_u32_le(&mut bytes, self.k_source);
        append_u32_le(&mut bytes, self.r_repair);
        bytes.extend_from_slice(&self.oti.to_bytes());
        bytes.extend_from_slice(self.object_id.as_bytes());
        for &page_number in &self.page_numbers {
            append_u32_le(&mut bytes, page_number);
        }
        for &hash in &self.source_page_xxh3_128 {
            bytes.extend_from_slice(&hash.to_le_bytes());
        }
        bytes
    }

    fn validate_invariants(&self) -> Result<()> {
        self.validate_meta_header()?;
        self.validate_frame_span()?;
        if self.r_repair == 0 {
            return Err(FrankenError::WalCorrupt {
                detail: "r_repair must be >= 1 for wal-fec groups".to_owned(),
            });
        }
        let k_source_usize =
            usize::try_from(self.k_source).map_err(|_| FrankenError::WalCorrupt {
                detail: format!("k_source {} does not fit in usize", self.k_source),
            })?;
        self.validate_array_lengths(k_source_usize)?;
        self.validate_page_size_and_oti()?;
        if self.db_size_pages == 0 {
            return Err(FrankenError::WalCorrupt {
                detail: "db_size_pages must be non-zero commit frame size".to_owned(),
            });
        }
        Ok(())
    }

    fn validate_meta_header(&self) -> Result<()> {
        if self.magic != WAL_FEC_GROUP_META_MAGIC {
            return Err(FrankenError::WalCorrupt {
                detail: "invalid wal-fec magic".to_owned(),
            });
        }
        if self.version != WAL_FEC_GROUP_META_VERSION {
            return Err(FrankenError::WalCorrupt {
                detail: format!(
                    "unsupported wal-fec meta version {} (expected {WAL_FEC_GROUP_META_VERSION})",
                    self.version
                ),
            });
        }
        Ok(())
    }

    fn validate_frame_span(&self) -> Result<()> {
        if self.start_frame_no == 0 {
            return Err(FrankenError::WalCorrupt {
                detail: "start_frame_no must be 1-based and nonzero".to_owned(),
            });
        }
        if self.end_frame_no < self.start_frame_no {
            return Err(FrankenError::WalCorrupt {
                detail: format!(
                    "end_frame_no {} must be >= start_frame_no {}",
                    self.end_frame_no, self.start_frame_no
                ),
            });
        }
        let expected_k = self
            .end_frame_no
            .checked_sub(self.start_frame_no)
            .and_then(|delta| delta.checked_add(1))
            .ok_or_else(|| FrankenError::WalCorrupt {
                detail: "frame-range overflow while validating k_source".to_owned(),
            })?;
        if self.k_source != expected_k {
            return Err(FrankenError::WalCorrupt {
                detail: format!(
                    "k_source {} must equal frame span {} ({}..={})",
                    self.k_source, expected_k, self.start_frame_no, self.end_frame_no
                ),
            });
        }
        Ok(())
    }

    fn validate_array_lengths(&self, k_source_usize: usize) -> Result<()> {
        if self.page_numbers.len() != k_source_usize {
            return Err(FrankenError::WalCorrupt {
                detail: format!(
                    "page_numbers length {} must equal k_source {}",
                    self.page_numbers.len(),
                    self.k_source
                ),
            });
        }
        if self.source_page_xxh3_128.len() != k_source_usize {
            return Err(FrankenError::WalCorrupt {
                detail: format!(
                    "source_page_xxh3_128 length {} must equal k_source {}",
                    self.source_page_xxh3_128.len(),
                    self.k_source
                ),
            });
        }
        Ok(())
    }

    fn validate_page_size_and_oti(&self) -> Result<()> {
        if PageSize::new(self.page_size).is_none() {
            return Err(FrankenError::WalCorrupt {
                detail: format!("invalid SQLite page_size {}", self.page_size),
            });
        }
        if self.oti.t != self.page_size {
            return Err(FrankenError::WalCorrupt {
                detail: format!(
                    "OTI.t {} must equal page_size {} for WAL source pages",
                    self.oti.t, self.page_size
                ),
            });
        }
        let expected_f = u64::from(self.k_source)
            .checked_mul(u64::from(self.page_size))
            .ok_or_else(|| FrankenError::WalCorrupt {
                detail: "overflow computing expected OTI.f".to_owned(),
            })?;
        if self.oti.f != expected_f {
            return Err(FrankenError::WalCorrupt {
                detail: format!(
                    "OTI.f {} must equal k_source*page_size ({expected_f})",
                    self.oti.f
                ),
            });
        }
        Ok(())
    }
}

/// One complete append-only sidecar group.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WalFecGroupRecord {
    pub meta: WalFecGroupMeta,
    pub repair_symbols: Vec<SymbolRecord>,
}

impl WalFecGroupRecord {
    pub fn new(meta: WalFecGroupMeta, repair_symbols: Vec<SymbolRecord>) -> Result<Self> {
        let group = Self {
            meta,
            repair_symbols,
        };
        group.validate_layout()?;
        Ok(group)
    }

    fn validate_layout(&self) -> Result<()> {
        let expected_repair =
            usize::try_from(self.meta.r_repair).map_err(|_| FrankenError::WalCorrupt {
                detail: format!("r_repair {} does not fit in usize", self.meta.r_repair),
            })?;
        if self.repair_symbols.len() != expected_repair {
            return Err(FrankenError::WalCorrupt {
                detail: format!(
                    "repair symbol count {} must equal r_repair {}",
                    self.repair_symbols.len(),
                    self.meta.r_repair
                ),
            });
        }
        for (index, symbol) in self.repair_symbols.iter().enumerate() {
            if symbol.object_id != self.meta.object_id {
                return Err(FrankenError::WalCorrupt {
                    detail: format!(
                        "repair symbol {index} object_id mismatch: {} != {}",
                        symbol.object_id, self.meta.object_id
                    ),
                });
            }
            if symbol.oti != self.meta.oti {
                return Err(FrankenError::WalCorrupt {
                    detail: format!("repair symbol {index} OTI mismatch"),
                });
            }
            let expected_esi = self
                .meta
                .k_source
                .checked_add(u32::try_from(index).map_err(|_| FrankenError::WalCorrupt {
                    detail: format!("repair symbol index {index} does not fit in u32"),
                })?)
                .ok_or_else(|| FrankenError::WalCorrupt {
                    detail: "repair ESI overflow".to_owned(),
                })?;
            if symbol.esi != expected_esi {
                return Err(FrankenError::WalCorrupt {
                    detail: format!(
                        "repair symbol {index} has ESI {}, expected {expected_esi}",
                        symbol.esi
                    ),
                });
            }
        }
        Ok(())
    }
}

/// Scan result for `.wal-fec` sidecar files.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct WalFecScanResult {
    pub groups: Vec<WalFecGroupRecord>,
    pub truncated_tail: bool,
}

const DEFAULT_REPAIR_PIPELINE_QUEUE_CAPACITY: usize = 64;
const REPAIR_PIPELINE_FLUSH_POLL_INTERVAL: Duration = Duration::from_millis(1);

/// Pipeline configuration for asynchronous WAL-FEC repair generation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WalFecRepairPipelineConfig {
    /// Maximum queued work items before backpressure.
    pub queue_capacity: usize,
    /// Optional deterministic delay per generated repair symbol (test hook).
    pub per_symbol_delay: Duration,
}

impl Default for WalFecRepairPipelineConfig {
    fn default() -> Self {
        Self {
            queue_capacity: DEFAULT_REPAIR_PIPELINE_QUEUE_CAPACITY,
            per_symbol_delay: Duration::ZERO,
        }
    }
}

/// A single asynchronous WAL-FEC repair-generation work item.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WalFecRepairWorkItem {
    pub sidecar_path: PathBuf,
    pub meta: WalFecGroupMeta,
    pub source_pages: Vec<Vec<u8>>,
}

impl WalFecRepairWorkItem {
    pub fn new(
        sidecar_path: impl Into<PathBuf>,
        meta: WalFecGroupMeta,
        source_pages: Vec<Vec<u8>>,
    ) -> Result<Self> {
        validate_source_pages(&meta, &source_pages)?;
        Ok(Self {
            sidecar_path: sidecar_path.into(),
            meta,
            source_pages,
        })
    }
}

/// Snapshot of asynchronous WAL-FEC pipeline counters.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct WalFecRepairPipelineStats {
    pub pending_jobs: usize,
    pub completed_jobs: usize,
    pub failed_jobs: usize,
    pub canceled_jobs: usize,
    pub max_pending_jobs: usize,
}

#[derive(Debug)]
enum WalFecPipelineMessage {
    Work(WalFecRepairWorkItem),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum WalFecWorkOutcome {
    Completed,
    Canceled,
}

/// Background worker that computes and appends WAL-FEC repair symbols.
pub struct WalFecRepairPipeline {
    sender: Option<mpsc::SyncSender<WalFecPipelineMessage>>,
    cancel_flag: Arc<AtomicBool>,
    pending_jobs: Arc<AtomicUsize>,
    completed_jobs: Arc<AtomicUsize>,
    failed_jobs: Arc<AtomicUsize>,
    canceled_jobs: Arc<AtomicUsize>,
    max_pending_jobs: Arc<AtomicUsize>,
    worker: Option<JoinHandle<()>>,
}

impl WalFecRepairPipeline {
    /// Start the pipeline worker.
    pub fn start(config: WalFecRepairPipelineConfig) -> Result<Self> {
        if config.queue_capacity == 0 {
            return Err(FrankenError::WalCorrupt {
                detail: "wal-fec repair pipeline queue_capacity must be >= 1".to_owned(),
            });
        }

        let (tx, rx) = mpsc::sync_channel(config.queue_capacity);
        let cancel_flag = Arc::new(AtomicBool::new(false));
        let pending_jobs = Arc::new(AtomicUsize::new(0));
        let completed_jobs = Arc::new(AtomicUsize::new(0));
        let failed_jobs = Arc::new(AtomicUsize::new(0));
        let canceled_jobs = Arc::new(AtomicUsize::new(0));
        let max_pending_jobs = Arc::new(AtomicUsize::new(0));

        let worker_cancel = Arc::clone(&cancel_flag);
        let worker_pending = Arc::clone(&pending_jobs);
        let worker_completed = Arc::clone(&completed_jobs);
        let worker_failed = Arc::clone(&failed_jobs);
        let worker_canceled = Arc::clone(&canceled_jobs);
        let worker_handle = thread::Builder::new()
            .name("wal-fec-repair-pipeline".to_owned())
            .spawn(move || {
                while let Ok(message) = rx.recv() {
                    match message {
                        WalFecPipelineMessage::Work(work_item) => {
                            let group_id = work_item.meta.group_id();
                            let outcome = process_repair_work_item(
                                &work_item,
                                worker_cancel.as_ref(),
                                config.per_symbol_delay,
                            );
                            worker_pending.fetch_sub(1, Ordering::SeqCst);
                            match outcome {
                                Ok(WalFecWorkOutcome::Completed) => {
                                    worker_completed.fetch_add(1, Ordering::SeqCst);
                                    info!(
                                        group_id = %group_id,
                                        "wal-fec repair work item completed"
                                    );
                                }
                                Ok(WalFecWorkOutcome::Canceled) => {
                                    worker_canceled.fetch_add(1, Ordering::SeqCst);
                                    warn!(
                                        group_id = %group_id,
                                        "wal-fec repair work item canceled before append"
                                    );
                                }
                                Err(err) => {
                                    worker_failed.fetch_add(1, Ordering::SeqCst);
                                    error!(
                                        group_id = %group_id,
                                        error = %err,
                                        "wal-fec repair work item failed"
                                    );
                                }
                            }
                        }
                    }
                }
            })
            .map_err(|err| FrankenError::WalCorrupt {
                detail: format!("failed to spawn wal-fec repair worker thread: {err}"),
            })?;

        Ok(Self {
            sender: Some(tx),
            cancel_flag,
            pending_jobs,
            completed_jobs,
            failed_jobs,
            canceled_jobs,
            max_pending_jobs,
            worker: Some(worker_handle),
        })
    }

    /// Queue a new repair-generation work item without blocking commit path.
    pub fn enqueue(&self, work_item: WalFecRepairWorkItem) -> Result<()> {
        if self.cancel_flag.load(Ordering::SeqCst) {
            return Err(FrankenError::WalCorrupt {
                detail: "wal-fec repair pipeline is canceled".to_owned(),
            });
        }
        let sender = self
            .sender
            .as_ref()
            .ok_or_else(|| FrankenError::WalCorrupt {
                detail: "wal-fec repair pipeline is shut down".to_owned(),
            })?;

        let pending_after = self.pending_jobs.fetch_add(1, Ordering::SeqCst) + 1;
        update_max_pending(&self.max_pending_jobs, pending_after);

        match sender.try_send(WalFecPipelineMessage::Work(work_item)) {
            Ok(()) => Ok(()),
            Err(mpsc::TrySendError::Full(_)) => {
                self.pending_jobs.fetch_sub(1, Ordering::SeqCst);
                Err(FrankenError::WalCorrupt {
                    detail: "wal-fec repair pipeline queue full".to_owned(),
                })
            }
            Err(mpsc::TrySendError::Disconnected(_)) => {
                self.pending_jobs.fetch_sub(1, Ordering::SeqCst);
                Err(FrankenError::WalCorrupt {
                    detail: "wal-fec repair pipeline worker is disconnected".to_owned(),
                })
            }
        }
    }

    /// Request cancellation for queued/in-flight work.
    pub fn cancel(&self) {
        self.cancel_flag.store(true, Ordering::SeqCst);
    }

    /// Wait until queue drains or timeout expires.
    #[must_use]
    pub fn flush(&self, timeout: Duration) -> bool {
        let Some(deadline) = Instant::now().checked_add(timeout) else {
            return false;
        };
        loop {
            if self.pending_jobs.load(Ordering::SeqCst) == 0 {
                return true;
            }
            if Instant::now() >= deadline {
                return false;
            }
            thread::sleep(REPAIR_PIPELINE_FLUSH_POLL_INTERVAL);
        }
    }

    /// Read current counters.
    #[must_use]
    pub fn stats(&self) -> WalFecRepairPipelineStats {
        WalFecRepairPipelineStats {
            pending_jobs: self.pending_jobs.load(Ordering::SeqCst),
            completed_jobs: self.completed_jobs.load(Ordering::SeqCst),
            failed_jobs: self.failed_jobs.load(Ordering::SeqCst),
            canceled_jobs: self.canceled_jobs.load(Ordering::SeqCst),
            max_pending_jobs: self.max_pending_jobs.load(Ordering::SeqCst),
        }
    }

    /// Stop the worker and join thread.
    pub fn shutdown(&mut self) -> Result<WalFecRepairPipelineStats> {
        self.cancel();
        self.sender.take();
        if let Some(worker) = self.worker.take() {
            worker.join().map_err(|_| FrankenError::WalCorrupt {
                detail: "wal-fec repair worker thread panicked".to_owned(),
            })?;
        }
        Ok(self.stats())
    }
}

impl Drop for WalFecRepairPipeline {
    fn drop(&mut self) {
        if self.worker.is_some() {
            let _ = self.shutdown();
        }
    }
}

/// Deterministically generate repair symbols from source pages.
pub fn generate_wal_fec_repair_symbols(
    meta: &WalFecGroupMeta,
    source_pages: &[Vec<u8>],
) -> Result<Vec<SymbolRecord>> {
    match generate_wal_fec_repair_symbols_inner(meta, source_pages, None, Duration::ZERO)? {
        Some(symbols) => Ok(symbols),
        None => Err(FrankenError::WalCorrupt {
            detail: "unexpected cancellation while generating wal-fec symbols".to_owned(),
        }),
    }
}

fn process_repair_work_item(
    work_item: &WalFecRepairWorkItem,
    cancel_flag: &AtomicBool,
    per_symbol_delay: Duration,
) -> Result<WalFecWorkOutcome> {
    if cancel_flag.load(Ordering::SeqCst) {
        return Ok(WalFecWorkOutcome::Canceled);
    }
    let Some(repair_symbols) = generate_wal_fec_repair_symbols_inner(
        &work_item.meta,
        &work_item.source_pages,
        Some(cancel_flag),
        per_symbol_delay,
    )?
    else {
        return Ok(WalFecWorkOutcome::Canceled);
    };
    if cancel_flag.load(Ordering::SeqCst) {
        return Ok(WalFecWorkOutcome::Canceled);
    }
    let group = WalFecGroupRecord::new(work_item.meta.clone(), repair_symbols)?;
    append_wal_fec_group(&work_item.sidecar_path, &group)?;
    Ok(WalFecWorkOutcome::Completed)
}

fn generate_wal_fec_repair_symbols_inner(
    meta: &WalFecGroupMeta,
    source_pages: &[Vec<u8>],
    cancel_flag: Option<&AtomicBool>,
    per_symbol_delay: Duration,
) -> Result<Option<Vec<SymbolRecord>>> {
    validate_source_pages(meta, source_pages)?;
    let symbol_len = usize::try_from(meta.oti.t).map_err(|_| FrankenError::WalCorrupt {
        detail: format!("OTI symbol size {} does not fit in usize", meta.oti.t),
    })?;
    let mut symbols = Vec::with_capacity(usize::try_from(meta.r_repair).map_err(|_| {
        FrankenError::WalCorrupt {
            detail: format!("r_repair {} does not fit in usize", meta.r_repair),
        }
    })?);

    for repair_index in 0..meta.r_repair {
        if let Some(flag) = cancel_flag {
            if flag.load(Ordering::SeqCst) {
                return Ok(None);
            }
        }
        let mut payload = vec![0_u8; symbol_len];
        let seed = derive_repair_seed(meta, repair_index);
        let seed_bytes = seed.to_le_bytes();
        let repair_index_usize =
            usize::try_from(repair_index).map_err(|_| FrankenError::WalCorrupt {
                detail: format!("repair_index {repair_index} does not fit in usize"),
            })?;

        for byte_index in 0..symbol_len {
            let mut mixed = seed_bytes[byte_index % seed_bytes.len()];
            for (page_index, page) in source_pages.iter().enumerate() {
                let read_index = (byte_index + page_index + repair_index_usize) % symbol_len;
                let source_byte = page[read_index];
                let tweak_raw = (page_index + byte_index) & 0xFF;
                let tweak = u8::try_from(tweak_raw).map_err(|_| FrankenError::WalCorrupt {
                    detail: format!("tweak value {tweak_raw} does not fit in u8"),
                })?;
                mixed ^= source_byte;
                mixed = mixed.rotate_left(u32::from(tweak & 0x07)) ^ tweak;
            }
            payload[byte_index] = mixed;
        }

        if per_symbol_delay > Duration::ZERO {
            thread::sleep(per_symbol_delay);
        }
        let esi =
            meta.k_source
                .checked_add(repair_index)
                .ok_or_else(|| FrankenError::WalCorrupt {
                    detail: "repair symbol ESI overflow".to_owned(),
                })?;
        symbols.push(SymbolRecord::new(
            meta.object_id,
            meta.oti,
            esi,
            payload,
            SymbolRecordFlags::empty(),
        ));
    }

    Ok(Some(symbols))
}

fn validate_source_pages(meta: &WalFecGroupMeta, source_pages: &[Vec<u8>]) -> Result<()> {
    let expected_pages = usize::try_from(meta.k_source).map_err(|_| FrankenError::WalCorrupt {
        detail: format!("k_source {} does not fit in usize", meta.k_source),
    })?;
    if source_pages.len() != expected_pages {
        return Err(FrankenError::WalCorrupt {
            detail: format!(
                "source page count {} must equal k_source {}",
                source_pages.len(),
                meta.k_source
            ),
        });
    }
    let expected_len = usize::try_from(meta.page_size).map_err(|_| FrankenError::WalCorrupt {
        detail: format!("page_size {} does not fit in usize", meta.page_size),
    })?;

    for (index, page) in source_pages.iter().enumerate() {
        if page.len() != expected_len {
            return Err(FrankenError::WalCorrupt {
                detail: format!(
                    "source page {index} has length {}, expected {expected_len}",
                    page.len()
                ),
            });
        }
        let actual_hash = wal_fec_source_hash_xxh3_128(page);
        let expected_hash = meta.source_page_xxh3_128[index];
        if actual_hash != expected_hash {
            return Err(FrankenError::WalCorrupt {
                detail: format!("source page hash mismatch at index {index}"),
            });
        }
    }
    Ok(())
}

fn derive_repair_seed(meta: &WalFecGroupMeta, repair_index: u32) -> u64 {
    let mut seed_material = Vec::with_capacity(16 + (7 * size_of::<u32>()));
    seed_material.extend_from_slice(meta.object_id.as_bytes());
    seed_material.extend_from_slice(&meta.wal_salt1.to_le_bytes());
    seed_material.extend_from_slice(&meta.wal_salt2.to_le_bytes());
    seed_material.extend_from_slice(&meta.start_frame_no.to_le_bytes());
    seed_material.extend_from_slice(&meta.end_frame_no.to_le_bytes());
    seed_material.extend_from_slice(&meta.k_source.to_le_bytes());
    seed_material.extend_from_slice(&meta.r_repair.to_le_bytes());
    seed_material.extend_from_slice(&repair_index.to_le_bytes());
    xxh3_64(&seed_material)
}

fn update_max_pending(max_pending: &AtomicUsize, candidate: usize) {
    let mut observed = max_pending.load(Ordering::SeqCst);
    while candidate > observed {
        match max_pending.compare_exchange(observed, candidate, Ordering::SeqCst, Ordering::SeqCst)
        {
            Ok(_) => break,
            Err(new_observed) => observed = new_observed,
        }
    }
}

/// Build source hashes for `K` WAL payload pages.
#[must_use]
pub fn build_source_page_hashes(page_payloads: &[Vec<u8>]) -> Vec<Xxh3Checksum128> {
    page_payloads
        .iter()
        .map(|page| wal_fec_source_hash_xxh3_128(page))
        .collect()
}

/// Resolve sidecar path from WAL path.
#[must_use]
pub fn wal_fec_path_for_wal(wal_path: &Path) -> PathBuf {
    let wal_name = wal_path.to_string_lossy();
    if wal_name.ends_with("-wal") || wal_name.ends_with(".wal") {
        PathBuf::from(format!("{wal_name}-fec"))
    } else {
        PathBuf::from(format!("{wal_name}.wal-fec"))
    }
}

/// Ensure WAL file and `.wal-fec` sidecar both exist.
pub fn ensure_wal_with_fec_sidecar(wal_path: &Path) -> Result<PathBuf> {
    OpenOptions::new()
        .create(true)
        .append(true)
        .open(wal_path)?;
    let sidecar_path = wal_fec_path_for_wal(wal_path);
    OpenOptions::new()
        .create(true)
        .append(true)
        .open(&sidecar_path)?;
    Ok(sidecar_path)
}

/// Append a complete group (meta + repair symbols) to a sidecar file.
pub fn append_wal_fec_group(sidecar_path: &Path, group: &WalFecGroupRecord) -> Result<()> {
    group.validate_layout()?;
    let group_id = group.meta.group_id();
    debug!(
        group_id = %group_id,
        k_source = group.meta.k_source,
        r_repair = group.meta.r_repair,
        "appending wal-fec group"
    );

    let mut file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(sidecar_path)?;
    let meta_bytes = group.meta.to_record_bytes();
    write_length_prefixed(&mut file, &meta_bytes, "group metadata")?;
    for symbol in &group.repair_symbols {
        write_length_prefixed(&mut file, &symbol.to_bytes(), "repair symbol")?;
    }
    file.sync_data()?;
    info!(
        group_id = %group_id,
        sidecar = %sidecar_path.display(),
        repair_symbols = group.repair_symbols.len(),
        "wal-fec group appended"
    );
    Ok(())
}

/// Scan a sidecar file and parse all fully-written groups.
///
/// On truncated tail (e.g. crash during append), returns `truncated_tail=true`
/// and only fully-validated preceding groups.
pub fn scan_wal_fec(sidecar_path: &Path) -> Result<WalFecScanResult> {
    if !sidecar_path.exists() {
        return Ok(WalFecScanResult::default());
    }
    let bytes = fs::read(sidecar_path)?;
    let mut cursor = 0usize;
    let mut groups = Vec::new();
    let mut truncated_tail = false;

    while cursor < bytes.len() {
        let Some(meta_bytes) = read_length_prefixed(&bytes, &mut cursor)? else {
            truncated_tail = true;
            warn!(
                sidecar = %sidecar_path.display(),
                cursor,
                "truncated wal-fec metadata tail detected"
            );
            break;
        };
        let meta = WalFecGroupMeta::from_record_bytes(meta_bytes)?;
        let mut repair_symbols =
            Vec::with_capacity(usize::try_from(meta.r_repair).map_err(|_| {
                FrankenError::WalCorrupt {
                    detail: format!("r_repair {} does not fit in usize", meta.r_repair),
                }
            })?);

        for _ in 0..meta.r_repair {
            let Some(symbol_bytes) = read_length_prefixed(&bytes, &mut cursor)? else {
                truncated_tail = true;
                warn!(
                    sidecar = %sidecar_path.display(),
                    group_id = %meta.group_id(),
                    cursor,
                    "truncated wal-fec repair-symbol tail detected"
                );
                break;
            };
            let symbol = SymbolRecord::from_bytes(symbol_bytes).map_err(|err| {
                error!(
                    sidecar = %sidecar_path.display(),
                    group_id = %meta.group_id(),
                    error = %err,
                    "invalid wal-fec repair symbol"
                );
                FrankenError::WalCorrupt {
                    detail: format!("invalid wal-fec repair symbol: {err}"),
                }
            })?;
            repair_symbols.push(symbol);
        }

        if truncated_tail {
            break;
        }
        groups.push(WalFecGroupRecord::new(meta, repair_symbols)?);
    }

    Ok(WalFecScanResult {
        groups,
        truncated_tail,
    })
}

/// Find one group by `(wal_salt1, wal_salt2, end_frame_no)`.
pub fn find_wal_fec_group(
    sidecar_path: &Path,
    group_id: WalFecGroupId,
) -> Result<Option<WalFecGroupRecord>> {
    let scan = scan_wal_fec(sidecar_path)?;
    Ok(scan
        .groups
        .into_iter()
        .find(|group| group.meta.group_id() == group_id))
}

fn write_length_prefixed(file: &mut File, payload: &[u8], what: &str) -> Result<()> {
    let len_u32 = u32::try_from(payload.len()).map_err(|_| FrankenError::WalCorrupt {
        detail: format!(
            "{what} too large for wal-fec length prefix: {}",
            payload.len()
        ),
    })?;
    file.write_all(&len_u32.to_le_bytes())?;
    file.write_all(payload)?;
    Ok(())
}

fn read_length_prefixed<'a>(bytes: &'a [u8], cursor: &mut usize) -> Result<Option<&'a [u8]>> {
    if *cursor >= bytes.len() {
        return Ok(None);
    }
    if bytes.len() - *cursor < LENGTH_PREFIX_BYTES {
        return Ok(None);
    }
    let mut len_raw = [0u8; LENGTH_PREFIX_BYTES];
    len_raw.copy_from_slice(&bytes[*cursor..*cursor + LENGTH_PREFIX_BYTES]);
    *cursor += LENGTH_PREFIX_BYTES;
    let payload_len =
        usize::try_from(u32::from_le_bytes(len_raw)).map_err(|_| FrankenError::WalCorrupt {
            detail: "wal-fec length prefix does not fit in usize".to_owned(),
        })?;
    let end = cursor
        .checked_add(payload_len)
        .ok_or_else(|| FrankenError::WalCorrupt {
            detail: "wal-fec length prefix overflow".to_owned(),
        })?;
    if end > bytes.len() {
        return Ok(None);
    }
    let payload = &bytes[*cursor..end];
    *cursor = end;
    Ok(Some(payload))
}

fn append_u32_le(buf: &mut Vec<u8>, value: u32) {
    buf.extend_from_slice(&value.to_le_bytes());
}

fn append_u64_le(buf: &mut Vec<u8>, value: u64) {
    buf.extend_from_slice(&value.to_le_bytes());
}

fn read_u32_le(bytes: &[u8], cursor: &mut usize, field: &str) -> Result<u32> {
    let raw = read_array::<4>(bytes, cursor, field)?;
    Ok(u32::from_le_bytes(raw))
}

fn read_u64_le(bytes: &[u8], cursor: &mut usize, field: &str) -> Result<u64> {
    let raw = read_array::<8>(bytes, cursor, field)?;
    Ok(u64::from_le_bytes(raw))
}

fn read_array<const N: usize>(bytes: &[u8], cursor: &mut usize, field: &str) -> Result<[u8; N]> {
    let end = cursor
        .checked_add(N)
        .ok_or_else(|| FrankenError::WalCorrupt {
            detail: format!("overflow reading wal-fec field {field}"),
        })?;
    if end > bytes.len() {
        return Err(FrankenError::WalCorrupt {
            detail: format!(
                "wal-fec field {field} out of bounds: need {N} bytes at offset {}, total {}",
                *cursor,
                bytes.len()
            ),
        });
    }
    let mut out = [0u8; N];
    out.copy_from_slice(&bytes[*cursor..end]);
    *cursor = end;
    Ok(out)
}
