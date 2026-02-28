//! `.db-fec` sidecar — erasure-coded page storage for on-the-fly repair (§3.4.6, bd-1hi.18).
//!
//! Provides `DbFecHeader`, `DbFecGroupMeta`, page group partitioning (G=64, R=4),
//! O(1) segment offset computation, stale-sidecar guard via `db_gen_digest`, and
//! the read-path repair algorithm.
//!
//! Note: the sidecar generation and group-read helpers are intentionally public so
//! the `fsqlite-e2e` recovery demos can validate end-to-end repair flows.

use std::fmt;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use fsqlite_error::{FrankenError, Result};
use fsqlite_vfs::host_fs;
use tracing::{Level, debug, error, info, span, warn};

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

const BEAD_ID: &str = "bd-1hi.18";

/// Magic bytes for `.db-fec` header.
pub const DB_FEC_MAGIC: [u8; 8] = *b"FSQLDFEC";

/// Magic bytes for group metadata.
pub const GROUP_META_MAGIC: [u8; 8] = *b"FSQLDGRP";

/// Current format version.
pub const DB_FEC_VERSION: u32 = 1;

/// Default pages per group (256 KiB blast radius at 4 KiB pages).
pub const DEFAULT_GROUP_SIZE: u32 = 64;

/// Default repair symbols per group (tolerates 4 corrupted pages per group).
pub const DEFAULT_R_REPAIR: u32 = 4;

/// Header page (page 1) gets special 400% redundancy: G=1, R=4.
pub const HEADER_PAGE_R_REPAIR: u32 = 4;

/// Domain separation string for `db_gen_digest`.
pub const DB_GEN_DIGEST_DOMAIN: &str = "fsqlite:compat:dbgen:v1";

/// Domain separation string for group `object_id`.
pub const GROUP_OBJECT_ID_DOMAIN: &str = "fsqlite:compat:db-fec-group:v1";

/// `DbFecHeader` serialized size: 8 (magic) + 4 (version) + 4 (page_size)
/// + 4 (default_group_size) + 4 (default_r_repair) + 4 (header_page_r_repair)
/// + 16 (db_gen_digest) + 8 (checksum) = 52 bytes.
pub const DB_FEC_HEADER_SIZE: usize = 52;

// ---------------------------------------------------------------------------
// Snapshot FEC metrics
// ---------------------------------------------------------------------------

/// Global snapshot FEC metrics singleton.
pub static GLOBAL_SNAPSHOT_FEC_METRICS: SnapshotFecMetrics = SnapshotFecMetrics::new();

/// Atomic counters for snapshot page FEC encoding.
pub struct SnapshotFecMetrics {
    /// Total pages encoded into FEC repair symbols.
    pub encoded_pages_total: AtomicU64,
    /// Total bytes of sidecar data generated.
    pub sidecar_bytes_total: AtomicU64,
    /// Total encoding operations.
    pub encode_ops: AtomicU64,
}

impl SnapshotFecMetrics {
    /// Create a zeroed metrics instance.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            encoded_pages_total: AtomicU64::new(0),
            sidecar_bytes_total: AtomicU64::new(0),
            encode_ops: AtomicU64::new(0),
        }
    }

    /// Record a snapshot FEC encoding operation.
    pub fn record_encode(&self, pages_encoded: u64, sidecar_bytes: u64) {
        self.encode_ops.fetch_add(1, Ordering::Relaxed);
        self.encoded_pages_total
            .fetch_add(pages_encoded, Ordering::Relaxed);
        self.sidecar_bytes_total
            .fetch_add(sidecar_bytes, Ordering::Relaxed);
    }

    /// Take a snapshot.
    #[must_use]
    pub fn snapshot(&self) -> SnapshotFecMetricsSnapshot {
        SnapshotFecMetricsSnapshot {
            encoded_pages_total: self.encoded_pages_total.load(Ordering::Relaxed),
            sidecar_bytes_total: self.sidecar_bytes_total.load(Ordering::Relaxed),
            encode_ops: self.encode_ops.load(Ordering::Relaxed),
        }
    }

    /// Reset all counters to zero.
    pub fn reset(&self) {
        self.encoded_pages_total.store(0, Ordering::Relaxed);
        self.sidecar_bytes_total.store(0, Ordering::Relaxed);
        self.encode_ops.store(0, Ordering::Relaxed);
    }
}

impl Default for SnapshotFecMetrics {
    fn default() -> Self {
        Self::new()
    }
}

/// Point-in-time snapshot of snapshot FEC metrics.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SnapshotFecMetricsSnapshot {
    pub encoded_pages_total: u64,
    pub sidecar_bytes_total: u64,
    pub encode_ops: u64,
}

impl fmt::Display for SnapshotFecMetricsSnapshot {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "snapshot_fec_pages_encoded={} sidecar_bytes={} encode_ops={}",
            self.encoded_pages_total, self.sidecar_bytes_total, self.encode_ops,
        )
    }
}

// ---------------------------------------------------------------------------
// PageGroup — partition of database pages into repair groups
// ---------------------------------------------------------------------------

/// A contiguous group of database pages sharing a single repair-symbol set.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PageGroup {
    /// 1-based first page number.
    pub start_pgno: u32,
    /// Number of source pages in this group (K).
    pub group_size: u32,
    /// Number of repair symbols (R).
    pub repair: u32,
}

/// Partition pages into groups per spec pseudocode (§3.4.6).
///
/// Page 1 gets its own group with `HEADER_PAGE_R_REPAIR` repair symbols.
/// Remaining pages are grouped in chunks of `DEFAULT_GROUP_SIZE`.
#[must_use]
pub fn partition_page_groups(db_size_pages: u32) -> Vec<PageGroup> {
    if db_size_pages == 0 {
        return Vec::new();
    }

    let mut groups = Vec::new();

    // Special group for database header page.
    groups.push(PageGroup {
        start_pgno: 1,
        group_size: 1,
        repair: HEADER_PAGE_R_REPAIR,
    });

    let mut pgno: u32 = 2;
    while pgno <= db_size_pages {
        let remaining = db_size_pages - pgno + 1;
        let group_size = remaining.min(DEFAULT_GROUP_SIZE);
        groups.push(PageGroup {
            start_pgno: pgno,
            group_size,
            repair: DEFAULT_R_REPAIR,
        });
        pgno += group_size;
    }

    debug!(
        bead_id = BEAD_ID,
        db_size_pages,
        group_count = groups.len(),
        "partitioned pages into .db-fec groups"
    );

    groups
}

// ---------------------------------------------------------------------------
// db_gen_digest — staleness guard
// ---------------------------------------------------------------------------

/// Compute `db_gen_digest` from `.db` header fields.
///
/// Uses offsets 24, 28, 36, 40 (big-endian u32):
/// `Trunc128(BLAKE3(domain || change_counter || page_count || freelist_count || schema_cookie))`.
#[must_use]
pub fn compute_db_gen_digest(
    change_counter: u32,
    page_count: u32,
    freelist_count: u32,
    schema_cookie: u32,
) -> [u8; 16] {
    let mut hasher = blake3::Hasher::new();
    hasher.update(DB_GEN_DIGEST_DOMAIN.as_bytes());
    hasher.update(&change_counter.to_be_bytes());
    hasher.update(&page_count.to_be_bytes());
    hasher.update(&freelist_count.to_be_bytes());
    hasher.update(&schema_cookie.to_be_bytes());
    let hash = hasher.finalize();
    let mut digest = [0u8; 16];
    digest.copy_from_slice(&hash.as_bytes()[..16]);
    digest
}

// ---------------------------------------------------------------------------
// DbFecHeader
// ---------------------------------------------------------------------------

/// Header of the `.db-fec` sidecar file (byte offset 0).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DbFecHeader {
    pub magic: [u8; 8],
    pub version: u32,
    pub page_size: u32,
    pub default_group_size: u32,
    pub default_r_repair: u32,
    pub header_page_r_repair: u32,
    pub db_gen_digest: [u8; 16],
    pub checksum: u64,
}

impl DbFecHeader {
    /// Create a new header for the given page size and db generation fields.
    #[must_use]
    pub fn new(
        page_size: u32,
        change_counter: u32,
        page_count: u32,
        freelist_count: u32,
        schema_cookie: u32,
    ) -> Self {
        let digest =
            compute_db_gen_digest(change_counter, page_count, freelist_count, schema_cookie);
        let mut hdr = Self {
            magic: DB_FEC_MAGIC,
            version: DB_FEC_VERSION,
            page_size,
            default_group_size: DEFAULT_GROUP_SIZE,
            default_r_repair: DEFAULT_R_REPAIR,
            header_page_r_repair: HEADER_PAGE_R_REPAIR,
            db_gen_digest: digest,
            checksum: 0,
        };
        hdr.checksum = hdr.compute_checksum();
        hdr
    }

    /// Serialize to bytes.
    #[must_use]
    pub fn to_bytes(&self) -> [u8; DB_FEC_HEADER_SIZE] {
        let mut buf = [0u8; DB_FEC_HEADER_SIZE];
        buf[0..8].copy_from_slice(&self.magic);
        buf[8..12].copy_from_slice(&self.version.to_le_bytes());
        buf[12..16].copy_from_slice(&self.page_size.to_le_bytes());
        buf[16..20].copy_from_slice(&self.default_group_size.to_le_bytes());
        buf[20..24].copy_from_slice(&self.default_r_repair.to_le_bytes());
        buf[24..28].copy_from_slice(&self.header_page_r_repair.to_le_bytes());
        buf[28..44].copy_from_slice(&self.db_gen_digest);
        buf[44..52].copy_from_slice(&self.checksum.to_le_bytes());
        buf
    }

    /// Deserialize from bytes.
    pub fn from_bytes(buf: &[u8; DB_FEC_HEADER_SIZE]) -> Result<Self> {
        let magic: [u8; 8] = buf[0..8].try_into().expect("slice len");
        if magic != DB_FEC_MAGIC {
            return Err(FrankenError::DatabaseCorrupt {
                detail: format!("bad .db-fec magic: {magic:?}"),
            });
        }
        let version = u32::from_le_bytes(buf[8..12].try_into().expect("slice len"));
        if version != DB_FEC_VERSION {
            return Err(FrankenError::DatabaseCorrupt {
                detail: format!("unsupported .db-fec version: {version}"),
            });
        }
        let page_size = u32::from_le_bytes(buf[12..16].try_into().expect("slice len"));
        let default_group_size = u32::from_le_bytes(buf[16..20].try_into().expect("slice len"));
        let default_r_repair = u32::from_le_bytes(buf[20..24].try_into().expect("slice len"));
        let header_page_r_repair = u32::from_le_bytes(buf[24..28].try_into().expect("slice len"));
        let mut db_gen_digest = [0u8; 16];
        db_gen_digest.copy_from_slice(&buf[28..44]);
        let checksum = u64::from_le_bytes(buf[44..52].try_into().expect("slice len"));

        let hdr = Self {
            magic,
            version,
            page_size,
            default_group_size,
            default_r_repair,
            header_page_r_repair,
            db_gen_digest,
            checksum,
        };

        let expected = hdr.compute_checksum();
        if hdr.checksum != expected {
            return Err(FrankenError::DatabaseCorrupt {
                detail: format!(
                    ".db-fec header checksum mismatch: stored={:#x}, computed={expected:#x}",
                    hdr.checksum
                ),
            });
        }

        info!(
            bead_id = BEAD_ID,
            page_size,
            G_pages_per_group = default_group_size,
            R_repair_pages = default_r_repair,
            header_group_policy = header_page_r_repair,
            format_version = version,
            ".db-fec config on open"
        );

        Ok(hdr)
    }

    /// Compute xxh3_64 checksum of all fields preceding the checksum field.
    #[must_use]
    fn compute_checksum(&self) -> u64 {
        let buf = self.to_bytes();
        // Checksum covers bytes 0..44 (everything except the checksum field itself).
        xxhash_rust::xxh3::xxh3_64(&buf[..44])
    }

    /// Verify that this header's `db_gen_digest` matches the current `.db` generation.
    #[must_use]
    pub fn is_current(
        &self,
        change_counter: u32,
        page_count: u32,
        freelist_count: u32,
        schema_cookie: u32,
    ) -> bool {
        let current =
            compute_db_gen_digest(change_counter, page_count, freelist_count, schema_cookie);
        self.db_gen_digest == current
    }
}

// ---------------------------------------------------------------------------
// DbFecGroupMeta
// ---------------------------------------------------------------------------

/// Per-group metadata stored in the `.db-fec` sidecar.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DbFecGroupMeta {
    pub magic: [u8; 8],
    pub version: u32,
    pub page_size: u32,
    pub start_pgno: u32,
    pub group_size: u32,
    pub r_repair: u32,
    /// Content-addressed: `Trunc128(BLAKE3(domain || canonical))`.
    pub object_id: [u8; 16],
    /// Per-source-page xxh3_128 hashes; length == group_size.
    pub source_page_xxh3_128: Vec<[u8; 16]>,
    /// Must match `DbFecHeader.db_gen_digest`.
    pub db_gen_digest: [u8; 16],
    pub checksum: u64,
}

impl DbFecGroupMeta {
    /// Create a new group meta. Computes `object_id` and `checksum` automatically.
    #[must_use]
    pub fn new(
        page_size: u32,
        start_pgno: u32,
        group_size: u32,
        r_repair: u32,
        source_page_xxh3_128: Vec<[u8; 16]>,
        db_gen_digest: [u8; 16],
    ) -> Self {
        assert!(
            source_page_xxh3_128.len() == group_size as usize,
            "source_page_xxh3_128.len() must equal group_size"
        );
        let mut meta = Self {
            magic: GROUP_META_MAGIC,
            version: DB_FEC_VERSION,
            page_size,
            start_pgno,
            group_size,
            r_repair,
            object_id: [0u8; 16],
            source_page_xxh3_128,
            db_gen_digest,
            checksum: 0,
        };
        meta.object_id = meta.compute_object_id();
        meta.checksum = meta.compute_checksum();
        meta
    }

    /// Fixed-size portion of the serialized meta (excluding variable-length hash array).
    /// 8 (magic) + 4 (version) + 4 (page_size) + 4 (start_pgno) + 4 (group_size)
    /// + 4 (r_repair) + 16 (object_id) + 16 (db_gen_digest) + 8 (checksum) = 68.
    const FIXED_SIZE: usize = 68;

    /// Total serialized size.
    #[must_use]
    pub fn serialized_size(&self) -> usize {
        Self::FIXED_SIZE + self.source_page_xxh3_128.len() * 16
    }

    /// Serialized size for a group with the given `group_size`.
    #[must_use]
    pub fn serialized_size_for(group_size: u32) -> usize {
        Self::FIXED_SIZE + group_size as usize * 16
    }

    /// Serialize to bytes.
    #[must_use]
    pub fn to_bytes(&self) -> Vec<u8> {
        let total = self.serialized_size();
        let mut buf = vec![0u8; total];
        buf[0..8].copy_from_slice(&self.magic);
        buf[8..12].copy_from_slice(&self.version.to_le_bytes());
        buf[12..16].copy_from_slice(&self.page_size.to_le_bytes());
        buf[16..20].copy_from_slice(&self.start_pgno.to_le_bytes());
        buf[20..24].copy_from_slice(&self.group_size.to_le_bytes());
        buf[24..28].copy_from_slice(&self.r_repair.to_le_bytes());
        buf[28..44].copy_from_slice(&self.object_id);
        let hash_start = 44;
        for (i, h) in self.source_page_xxh3_128.iter().enumerate() {
            let off = hash_start + i * 16;
            buf[off..off + 16].copy_from_slice(h);
        }
        let digest_off = hash_start + self.source_page_xxh3_128.len() * 16;
        buf[digest_off..digest_off + 16].copy_from_slice(&self.db_gen_digest);
        buf[digest_off + 16..digest_off + 24].copy_from_slice(&self.checksum.to_le_bytes());
        buf
    }

    /// Deserialize from bytes.
    pub fn from_bytes(buf: &[u8]) -> Result<Self> {
        if buf.len() < Self::FIXED_SIZE {
            return Err(FrankenError::DatabaseCorrupt {
                detail: format!("group meta too short: {} < {}", buf.len(), Self::FIXED_SIZE),
            });
        }
        let magic: [u8; 8] = buf[0..8].try_into().expect("slice len");
        if magic != GROUP_META_MAGIC {
            return Err(FrankenError::DatabaseCorrupt {
                detail: format!("bad group meta magic: {magic:?}"),
            });
        }
        let version = u32::from_le_bytes(buf[8..12].try_into().expect("slice len"));
        if version != DB_FEC_VERSION {
            return Err(FrankenError::DatabaseCorrupt {
                detail: format!("unsupported group meta version: {version}"),
            });
        }
        let page_size = u32::from_le_bytes(buf[12..16].try_into().expect("slice len"));
        let start_pgno = u32::from_le_bytes(buf[16..20].try_into().expect("slice len"));
        let group_size = u32::from_le_bytes(buf[20..24].try_into().expect("slice len"));
        let r_repair = u32::from_le_bytes(buf[24..28].try_into().expect("slice len"));
        let mut object_id = [0u8; 16];
        object_id.copy_from_slice(&buf[28..44]);

        let expected_total = Self::serialized_size_for(group_size);
        if buf.len() < expected_total {
            return Err(FrankenError::DatabaseCorrupt {
                detail: format!(
                    "group meta truncated: {} < {expected_total} for group_size={group_size}",
                    buf.len()
                ),
            });
        }

        let hash_start = 44;
        let mut source_page_xxh3_128 = Vec::with_capacity(group_size as usize);
        for i in 0..group_size as usize {
            let off = hash_start + i * 16;
            let mut h = [0u8; 16];
            h.copy_from_slice(&buf[off..off + 16]);
            source_page_xxh3_128.push(h);
        }

        let digest_off = hash_start + group_size as usize * 16;
        let mut db_gen_digest = [0u8; 16];
        db_gen_digest.copy_from_slice(&buf[digest_off..digest_off + 16]);
        let checksum = u64::from_le_bytes(
            buf[digest_off + 16..digest_off + 24]
                .try_into()
                .expect("slice len"),
        );

        let meta = Self {
            magic,
            version,
            page_size,
            start_pgno,
            group_size,
            r_repair,
            object_id,
            source_page_xxh3_128,
            db_gen_digest,
            checksum,
        };

        let expected_cksum = meta.compute_checksum();
        if meta.checksum != expected_cksum {
            return Err(FrankenError::DatabaseCorrupt {
                detail: format!(
                    "group meta checksum mismatch: stored={:#x}, computed={expected_cksum:#x}",
                    meta.checksum
                ),
            });
        }

        let expected_oid = meta.compute_object_id();
        if meta.object_id != expected_oid {
            return Err(FrankenError::DatabaseCorrupt {
                detail: "group meta object_id mismatch".into(),
            });
        }

        debug!(
            bead_id = BEAD_ID,
            group_idx = meta.start_pgno,
            pgno_start = meta.start_pgno,
            K = meta.group_size,
            R = meta.r_repair,
            "group meta validated"
        );

        Ok(meta)
    }

    /// Compute the content-addressed `object_id`.
    #[must_use]
    fn compute_object_id(&self) -> [u8; 16] {
        let mut hasher = blake3::Hasher::new();
        hasher.update(GROUP_OBJECT_ID_DOMAIN.as_bytes());
        // Canonical representation: all fields except object_id and checksum.
        hasher.update(&self.magic);
        hasher.update(&self.version.to_le_bytes());
        hasher.update(&self.page_size.to_le_bytes());
        hasher.update(&self.start_pgno.to_le_bytes());
        hasher.update(&self.group_size.to_le_bytes());
        hasher.update(&self.r_repair.to_le_bytes());
        for h in &self.source_page_xxh3_128 {
            hasher.update(h);
        }
        hasher.update(&self.db_gen_digest);
        let hash = hasher.finalize();
        let mut oid = [0u8; 16];
        oid.copy_from_slice(&hash.as_bytes()[..16]);
        oid
    }

    /// Compute xxh3_64 checksum of all fields except the checksum field itself.
    #[must_use]
    fn compute_checksum(&self) -> u64 {
        let bytes = self.to_bytes();
        // Everything except the last 8 bytes (checksum field).
        xxhash_rust::xxh3::xxh3_64(&bytes[..bytes.len() - 8])
    }
}

// ---------------------------------------------------------------------------
// Segment layout — O(1) random access
// ---------------------------------------------------------------------------

/// Compute the byte offset in the `.db-fec` file for the segment belonging to
/// the full-group at 0-based index `g` (groups starting at page 2).
///
/// Layout:
///   \[DbFecHeader\]\[Seg1 (page 1)\]\[SegG\_0\]\[SegG\_1\]...
///
/// `segment_1_len`: The total byte size of the page-1 segment (meta + R repair symbols).
/// `full_segment_len`: The total byte size of a full-group segment (meta + R repair symbols).
#[must_use]
pub fn segment_offset(g: u32, segment_1_len: usize, full_segment_len: usize) -> usize {
    DB_FEC_HEADER_SIZE + segment_1_len + g as usize * full_segment_len
}

/// Compute the total size of a group segment.
///
/// Each segment stores its `DbFecGroupMeta` plus R repair symbols of `page_size` bytes each.
#[must_use]
pub fn group_segment_size(group_size: u32, r_repair: u32, page_size: u32) -> usize {
    DbFecGroupMeta::serialized_size_for(group_size) + r_repair as usize * page_size as usize
}

/// Find which 0-based full-group index a page number belongs to.
/// Returns `None` for page 1 (header group) or invalid pgno.
#[must_use]
pub fn find_full_group_index(pgno: u32) -> Option<u32> {
    if pgno < 2 {
        return None;
    }
    Some((pgno - 2) / DEFAULT_GROUP_SIZE)
}

// ---------------------------------------------------------------------------
// Read path — on-the-fly repair
// ---------------------------------------------------------------------------

/// Result of an on-the-fly repair attempt.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RepairResult {
    /// Page was intact, no repair needed.
    Intact,
    /// Page was repaired from group erasure coding.
    Repaired { pgno: u32, symbols_used: u32 },
    /// Repair failed — insufficient symbols.
    Unrecoverable {
        pgno: u32,
        missing_pages: u32,
        r_budget: u32,
    },
}

/// Simulated page integrity check. In production this would check AEAD tag,
/// page checksum, or structural integrity. Here we check xxh3_128 against
/// the expected hash from group metadata.
#[must_use]
pub fn verify_page_xxh3_128(page_data: &[u8], expected_xxh3_128: &[u8; 16]) -> bool {
    let hash = xxhash_rust::xxh3::xxh3_128(page_data);
    hash.to_le_bytes() == *expected_xxh3_128
}

/// Compute xxh3_128 of page data, returned as a 16-byte LE array.
#[must_use]
pub fn page_xxh3_128(page_data: &[u8]) -> [u8; 16] {
    let hash = xxhash_rust::xxh3::xxh3_128(page_data);
    hash.to_le_bytes()
}

/// Attempt on-the-fly repair of a corrupted page using `.db-fec` group data.
///
/// `target_pgno` — the 1-based page to repair.
/// `group_meta` — metadata for the group containing the page.
/// `all_page_data` — function to read raw page data by pgno.
/// `repair_symbols` — the R repair symbol data blocks for this group.
///
/// Uses the RFC 6330 RaptorQ `InactivationDecoder` to reconstruct missing
/// source pages from any combination of intact sources and repair symbols,
/// provided at least K total symbols are available.
///
/// Returns the repaired page bytes or an error.
#[allow(clippy::too_many_lines)]
pub fn attempt_page_repair(
    target_pgno: u32,
    group_meta: &DbFecGroupMeta,
    all_page_data: &dyn Fn(u32) -> Vec<u8>,
    repair_symbols: &[(u32, Vec<u8>)],
) -> Result<(Vec<u8>, RepairResult)> {
    let local_idx = target_pgno - group_meta.start_pgno;
    let k = group_meta.group_size;

    debug!(
        bead_id = BEAD_ID,
        target_pgno,
        group_start = group_meta.start_pgno,
        K = k,
        R = group_meta.r_repair,
        "attempting on-the-fly page repair"
    );

    // Collect available source symbols (intact pages in the group, excluding target).
    let mut available: Vec<(u32, Vec<u8>)> = Vec::new();
    let mut corrupt_count: u32 = 0;

    for i in 0..k {
        let pgno = group_meta.start_pgno + i;
        if pgno == target_pgno {
            corrupt_count += 1;
            continue;
        }
        let data = all_page_data(pgno);
        if verify_page_xxh3_128(&data, &group_meta.source_page_xxh3_128[i as usize]) {
            available.push((i, data));
        } else {
            corrupt_count += 1;
        }
    }

    // Add repair symbols.
    for (esi, sym_data) in repair_symbols {
        available.push((*esi, sym_data.clone()));
    }

    debug!(
        bead_id = BEAD_ID,
        target_pgno,
        available_symbols = available.len(),
        corrupt_count,
        K = k,
        "collected symbols for repair"
    );

    #[allow(clippy::cast_possible_truncation)]
    let available_count = available.len() as u32;
    if available_count < k {
        error!(
            bead_id = BEAD_ID,
            target_pgno,
            missing_or_corrupt_pages = corrupt_count,
            R_budget = group_meta.r_repair,
            action = "fail",
            "unrecoverable group loss"
        );
        return Err(FrankenError::DatabaseCorrupt {
            detail: format!(
                "page {target_pgno}: insufficient symbols for repair ({} available, {k} needed, {corrupt_count} corrupt)",
                available.len()
            ),
        });
    }

    // Decode via RFC 6330 RaptorQ InactivationDecoder.
    let page_size = group_meta.page_size as usize;
    let k_usize = k as usize;
    let seed = derive_db_fec_repair_seed(group_meta);
    let decoder = asupersync::raptorq::decoder::InactivationDecoder::new(k_usize, page_size, seed);
    let params = decoder.params();
    let base_rows = params.s + params.h;
    let constraints = asupersync::raptorq::systematic::ConstraintMatrix::build(params, seed);

    let mut received = decoder.constraint_symbols();

    for (esi, data) in &available {
        if (*esi as usize) < k_usize {
            let (cols, coefs) = decoder.source_equation(*esi);
            received.push(asupersync::raptorq::decoder::ReceivedSymbol {
                esi: *esi,
                is_source: true,
                columns: cols,
                coefficients: coefs,
                data: data.clone(),
            });
        } else {
            let (cols, coefs) = decoder.repair_equation(*esi);
            received.push(asupersync::raptorq::decoder::ReceivedSymbol::repair(
                *esi,
                cols,
                coefs,
                data.clone(),
            ));
        }
    }

    // RFC 6330 decode requires K' source-domain rows; PI rows (K'−K) are
    // zero-padded source symbols that must be represented explicitly.
    for source_index in k_usize..params.k_prime {
        let row = base_rows + source_index;
        let mut columns = Vec::new();
        let mut coefficients = Vec::new();
        for col in 0..constraints.cols {
            let coeff = constraints.get(row, col);
            if !coeff.is_zero() {
                columns.push(col);
                coefficients.push(coeff);
            }
        }
        received.push(asupersync::raptorq::decoder::ReceivedSymbol {
            esi: u32::try_from(source_index).expect("source index fits u32"),
            is_source: true,
            columns,
            coefficients,
            data: vec![0_u8; page_size],
        });
    }

    let result = decoder
        .decode(&received)
        .map_err(|err| FrankenError::DatabaseCorrupt {
            detail: format!("page {target_pgno}: RaptorQ decode failed: {err:?}"),
        })?;

    if result.source.len() != k_usize {
        return Err(FrankenError::DatabaseCorrupt {
            detail: format!(
                "page {target_pgno}: RaptorQ decode returned {} source symbols, expected {k}",
                result.source.len()
            ),
        });
    }

    let recovered = result.source[local_idx as usize].clone();

    // Validate recovered page.
    if verify_page_xxh3_128(
        &recovered,
        &group_meta.source_page_xxh3_128[local_idx as usize],
    ) {
        info!(
            bead_id = BEAD_ID,
            target_pgno,
            group_start = group_meta.start_pgno,
            pages_repaired = 1,
            symbols_used = available.len(),
            "successful on-the-fly page repair"
        );
        Ok((
            recovered,
            RepairResult::Repaired {
                pgno: target_pgno,
                symbols_used: available_count,
            },
        ))
    } else {
        warn!(
            bead_id = BEAD_ID,
            target_pgno,
            missing_or_corrupt_pages = corrupt_count,
            R_budget = group_meta.r_repair,
            "near-capacity repair: recovered page xxh3 mismatch"
        );
        Err(FrankenError::DatabaseCorrupt {
            detail: format!("page {target_pgno}: recovered page failed xxh3_128 validation"),
        })
    }
}

// ---------------------------------------------------------------------------
// Sidecar generation utility (bd-2r4z)
// ---------------------------------------------------------------------------

/// Compute the `.db-fec` sidecar path from a database path.
#[must_use]
pub fn db_fec_path_for_db(db_path: &Path) -> PathBuf {
    let mut p = db_path.as_os_str().to_owned();
    p.push("-fec");
    PathBuf::from(p)
}

/// SQLite header field offsets (big-endian u32/u16).
const SQLITE_HEADER_MIN_BYTES: usize = 100;
const PAGE_SIZE_OFFSET: usize = 16;
const CHANGE_COUNTER_OFFSET: usize = 24;
const PAGE_COUNT_OFFSET: usize = 28;
const FREELIST_COUNT_OFFSET: usize = 36;
const SCHEMA_COOKIE_OFFSET: usize = 40;

/// Fields extracted from a SQLite database header for FEC generation.
#[derive(Debug, Clone, Copy)]
pub struct DbHeaderFields {
    pub page_size: u32,
    pub change_counter: u32,
    pub page_count: u32,
    pub freelist_count: u32,
    pub schema_cookie: u32,
}

/// Read the header fields from a SQLite database file.
pub fn read_db_header_fields(db_path: &Path) -> Result<DbHeaderFields> {
    let data = host_fs::read(db_path)?;
    parse_db_header_fields(&data)
}

/// Parse header fields from raw database bytes.
pub fn parse_db_header_fields(data: &[u8]) -> Result<DbHeaderFields> {
    if data.len() < SQLITE_HEADER_MIN_BYTES {
        return Err(FrankenError::DatabaseCorrupt {
            detail: format!(
                "database too short for header: {} < {SQLITE_HEADER_MIN_BYTES}",
                data.len()
            ),
        });
    }

    let page_size_raw = u16::from_be_bytes(
        data[PAGE_SIZE_OFFSET..PAGE_SIZE_OFFSET + 2]
            .try_into()
            .expect("fixed-length slice"),
    );
    // SQLite encoding: 1 means 65536.
    let page_size = if page_size_raw == 1 {
        65536
    } else {
        u32::from(page_size_raw)
    };

    let change_counter = u32::from_be_bytes(
        data[CHANGE_COUNTER_OFFSET..CHANGE_COUNTER_OFFSET + 4]
            .try_into()
            .expect("fixed-length slice"),
    );
    let page_count = u32::from_be_bytes(
        data[PAGE_COUNT_OFFSET..PAGE_COUNT_OFFSET + 4]
            .try_into()
            .expect("fixed-length slice"),
    );
    let freelist_count = u32::from_be_bytes(
        data[FREELIST_COUNT_OFFSET..FREELIST_COUNT_OFFSET + 4]
            .try_into()
            .expect("fixed-length slice"),
    );
    let schema_cookie = u32::from_be_bytes(
        data[SCHEMA_COOKIE_OFFSET..SCHEMA_COOKIE_OFFSET + 4]
            .try_into()
            .expect("fixed-length slice"),
    );

    Ok(DbHeaderFields {
        page_size,
        change_counter,
        page_count,
        freelist_count,
        schema_cookie,
    })
}

/// Derive a deterministic RaptorQ encoder seed from group metadata.
///
/// Uses xxh3_64 over the group's content-addressed fields to produce a
/// seed that is unique per group and deterministic across encode/decode.
fn derive_db_fec_repair_seed(meta: &DbFecGroupMeta) -> u64 {
    let mut seed_material = Vec::with_capacity(16 + 4 * 4 + 16);
    seed_material.extend_from_slice(&meta.object_id);
    seed_material.extend_from_slice(&meta.page_size.to_le_bytes());
    seed_material.extend_from_slice(&meta.start_pgno.to_le_bytes());
    seed_material.extend_from_slice(&meta.group_size.to_le_bytes());
    seed_material.extend_from_slice(&meta.r_repair.to_le_bytes());
    seed_material.extend_from_slice(&meta.db_gen_digest);
    xxhash_rust::xxh3::xxh3_64(&seed_material)
}

/// Compute RFC 6330 RaptorQ repair symbols for a group of source pages.
///
/// Uses `asupersync::raptorq::systematic::SystematicEncoder` to produce
/// `r_repair` repair symbols with ESIs `[K, K+R)`.
pub fn compute_raptorq_repair_symbols(
    meta: &DbFecGroupMeta,
    source_pages: &[&[u8]],
    page_size: usize,
) -> Result<Vec<Vec<u8>>> {
    if source_pages.len() != meta.group_size as usize {
        return Err(FrankenError::DatabaseCorrupt {
            detail: format!(
                "source_pages.len()={} != meta.group_size={}; encoder/decoder seed mismatch would corrupt data",
                source_pages.len(),
                meta.group_size,
            ),
        });
    }
    let seed = derive_db_fec_repair_seed(meta);
    let source_vecs: Vec<Vec<u8>> = source_pages.iter().map(|s| s.to_vec()).collect();
    let encoder =
        asupersync::raptorq::systematic::SystematicEncoder::new(&source_vecs, page_size, seed)
            .ok_or_else(|| FrankenError::DatabaseCorrupt {
                detail: "RaptorQ constraint matrix singular during encoding".to_owned(),
            })?;

    let k = u32::try_from(source_pages.len()).map_err(|_| FrankenError::DatabaseCorrupt {
        detail: "source page count does not fit in u32".to_owned(),
    })?;

    let mut symbols = Vec::with_capacity(meta.r_repair as usize);
    for r_idx in 0..meta.r_repair {
        let esi = k + r_idx;
        symbols.push(encoder.repair_symbol(esi));
    }
    Ok(symbols)
}

/// Read a single page from raw database bytes, zero-padding if file is short.
fn read_page_from_bytes(db_data: &[u8], pgno: u32, page_size: usize) -> Vec<u8> {
    let offset_u64 = (u64::from(pgno) - 1) * (page_size as u64);
    let offset = usize::try_from(offset_u64).unwrap_or(usize::MAX);
    if offset.saturating_add(page_size) <= db_data.len() {
        db_data[offset..offset + page_size].to_vec()
    } else {
        let mut page = vec![0u8; page_size];
        if offset < db_data.len() {
            let available = db_data.len() - offset;
            page[..available].copy_from_slice(&db_data[offset..offset + available]);
        }
        page
    }
}

/// Generate a complete `.db-fec` sidecar from raw database bytes.
///
/// Returns the sidecar file content as a byte vector. The layout is:
/// `[DbFecHeader][Seg_page1][Seg_group0][Seg_group1]...`
///
/// Each general segment is padded to `full_segment_len` for O(1) random access.
#[allow(clippy::too_many_lines)]
pub fn generate_db_fec_from_bytes(db_data: &[u8]) -> Result<Vec<u8>> {
    let fields = parse_db_header_fields(db_data)?;
    let ps = fields.page_size as usize;

    let header = DbFecHeader::new(
        fields.page_size,
        fields.change_counter,
        fields.page_count,
        fields.freelist_count,
        fields.schema_cookie,
    );
    let digest = header.db_gen_digest;
    let groups = partition_page_groups(fields.page_count);

    // Pre-compute segment sizes for O(1) layout.
    let seg1_len = group_segment_size(1, HEADER_PAGE_R_REPAIR, fields.page_size);
    let full_seg_len = group_segment_size(DEFAULT_GROUP_SIZE, DEFAULT_R_REPAIR, fields.page_size);

    // Total sidecar size: header + seg1 + (num_general_groups * full_seg_len).
    let num_general_groups = groups.len().saturating_sub(1);
    let total_size = DB_FEC_HEADER_SIZE + seg1_len + num_general_groups * full_seg_len;
    let mut sidecar = vec![0u8; total_size];

    // Write header.
    sidecar[..DB_FEC_HEADER_SIZE].copy_from_slice(&header.to_bytes());

    let mut cursor = DB_FEC_HEADER_SIZE;

    for (gi, group) in groups.iter().enumerate() {
        // Read source pages.
        let source_refs: Vec<Vec<u8>> = (0..group.group_size)
            .map(|i| read_page_from_bytes(db_data, group.start_pgno + i, ps))
            .collect();
        let source_slices: Vec<&[u8]> = source_refs.iter().map(Vec::as_slice).collect();

        // Compute per-page hashes.
        let hashes: Vec<[u8; 16]> = source_slices.iter().map(|p| page_xxh3_128(p)).collect();

        // Build group metadata.
        let meta = DbFecGroupMeta::new(
            fields.page_size,
            group.start_pgno,
            group.group_size,
            group.repair,
            hashes,
            digest,
        );

        // Compute repair symbols.
        let repair_symbols = compute_raptorq_repair_symbols(&meta, &source_slices, ps)?;

        // Write metadata.
        let meta_bytes = meta.to_bytes();
        sidecar[cursor..cursor + meta_bytes.len()].copy_from_slice(&meta_bytes);
        cursor += meta_bytes.len();

        // Write repair symbols.
        for sym in &repair_symbols {
            sidecar[cursor..cursor + ps].copy_from_slice(sym);
            cursor += ps;
        }

        // Pad general segments to full_seg_len for O(1) access.
        if gi > 0 {
            let actual_seg_size = meta_bytes.len() + group.repair as usize * ps;
            let padding = full_seg_len - actual_seg_size;
            cursor += padding; // Already zeroed by vec![0u8; total_size].
        }
    }

    let sidecar_len = sidecar.len() as u64;
    let page_count_u64 = u64::from(fields.page_count);

    // Structured tracing span for snapshot FEC encoding.
    let _span = span!(
        Level::INFO,
        "snapshot_raptorq",
        pages_encoded = page_count_u64,
        total_bytes = sidecar_len,
        groups = groups.len(),
    )
    .entered();

    GLOBAL_SNAPSHOT_FEC_METRICS.record_encode(page_count_u64, sidecar_len);

    info!(
        bead_id = "bd-2r4z",
        page_count = fields.page_count,
        page_size = fields.page_size,
        groups = groups.len(),
        sidecar_bytes = sidecar.len(),
        "generated .db-fec sidecar"
    );

    Ok(sidecar)
}

/// Generate a `.db-fec` sidecar for a database file path.
pub fn generate_db_fec_sidecar(db_path: &Path) -> Result<Vec<u8>> {
    let db_data = host_fs::read(db_path)?;
    generate_db_fec_from_bytes(&db_data)
}

/// Generate and write a `.db-fec` sidecar file, returning the sidecar path.
pub fn write_db_fec_sidecar(db_path: &Path) -> Result<PathBuf> {
    let sidecar_data = generate_db_fec_sidecar(db_path)?;
    let sidecar_path = db_fec_path_for_db(db_path);
    host_fs::write(&sidecar_path, &sidecar_data)?;

    info!(
        bead_id = "bd-2r4z",
        db_path = %db_path.display(),
        sidecar_path = %sidecar_path.display(),
        sidecar_bytes = sidecar_data.len(),
        "wrote .db-fec sidecar"
    );

    Ok(sidecar_path)
}

/// Read the [`DbFecHeader`] from a `.db-fec` sidecar file.
pub fn read_db_fec_header(sidecar_path: &Path) -> Result<DbFecHeader> {
    let data = host_fs::read(sidecar_path)?;
    if data.len() < DB_FEC_HEADER_SIZE {
        return Err(FrankenError::DatabaseCorrupt {
            detail: format!(
                "sidecar too short for header: {} < {DB_FEC_HEADER_SIZE}",
                data.len()
            ),
        });
    }
    let buf: [u8; DB_FEC_HEADER_SIZE] = data[..DB_FEC_HEADER_SIZE]
        .try_into()
        .expect("fixed-length slice");
    DbFecHeader::from_bytes(&buf)
}

/// Read group metadata and repair symbols for a target page from sidecar bytes.
///
/// Returns `(group_meta, repair_symbols)` where repair symbols are `(esi, data)` pairs
/// compatible with [`attempt_page_repair`].
#[allow(clippy::type_complexity)]
pub fn read_db_fec_group_for_page(
    sidecar_data: &[u8],
    header: &DbFecHeader,
    target_pgno: u32,
) -> Result<(DbFecGroupMeta, Vec<(u32, Vec<u8>)>)> {
    let ps = header.page_size as usize;

    // Determine which segment to read.
    let (seg_offset, group_size_hint) = if target_pgno == 1 {
        (DB_FEC_HEADER_SIZE, 1_u32)
    } else {
        let gi =
            find_full_group_index(target_pgno).ok_or_else(|| FrankenError::DatabaseCorrupt {
                detail: format!("invalid target page number: {target_pgno}"),
            })?;
        let seg1_len = group_segment_size(1, HEADER_PAGE_R_REPAIR, header.page_size);
        let full_seg_len =
            group_segment_size(DEFAULT_GROUP_SIZE, DEFAULT_R_REPAIR, header.page_size);
        let offset = segment_offset(gi, seg1_len, full_seg_len);
        (offset, DEFAULT_GROUP_SIZE)
    };

    if seg_offset >= sidecar_data.len() {
        return Err(FrankenError::DatabaseCorrupt {
            detail: format!(
                "sidecar too short for segment at offset {seg_offset}: len={}",
                sidecar_data.len()
            ),
        });
    }

    // Read group metadata (variable-length due to hash array).
    let meta_size = DbFecGroupMeta::serialized_size_for(group_size_hint);
    let meta_end = seg_offset + meta_size;
    if meta_end > sidecar_data.len() {
        return Err(FrankenError::DatabaseCorrupt {
            detail: format!(
                "sidecar truncated reading group meta at {seg_offset}: need {meta_size}, have {}",
                sidecar_data.len() - seg_offset
            ),
        });
    }
    let meta = DbFecGroupMeta::from_bytes(&sidecar_data[seg_offset..meta_end])?;

    let actual_r = meta.r_repair;

    // Read repair symbols.
    let mut symbols = Vec::with_capacity(actual_r as usize);
    let actual_meta_size = meta.serialized_size();
    let mut sym_cursor = seg_offset + actual_meta_size;
    for r_idx in 0..actual_r {
        if sym_cursor + ps > sidecar_data.len() {
            return Err(FrankenError::DatabaseCorrupt {
                detail: format!("sidecar truncated reading repair symbol {r_idx} at {sym_cursor}"),
            });
        }
        let esi = meta.group_size + r_idx;
        symbols.push((esi, sidecar_data[sym_cursor..sym_cursor + ps].to_vec()));
        sym_cursor += ps;
    }

    debug!(
        bead_id = "bd-2r4z",
        target_pgno,
        group_start = meta.start_pgno,
        K = meta.group_size,
        R = actual_r,
        "read .db-fec group for repair"
    );

    Ok((meta, symbols))
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    // -- DbFecHeader tests --

    #[test]
    fn test_db_fec_header_roundtrip() {
        let hdr = DbFecHeader::new(4096, 42, 100, 5, 99);
        let bytes = hdr.to_bytes();
        assert_eq!(bytes.len(), DB_FEC_HEADER_SIZE);
        let decoded = DbFecHeader::from_bytes(&bytes).expect("decode");
        assert_eq!(hdr, decoded);
    }

    #[test]
    fn test_db_gen_digest_computation() {
        // Known inputs.
        let d1 = compute_db_gen_digest(42, 100, 5, 99);
        let d2 = compute_db_gen_digest(42, 100, 5, 99);
        assert_eq!(d1, d2, "deterministic");

        // Changing any field changes the digest.
        let d3 = compute_db_gen_digest(43, 100, 5, 99);
        assert_ne!(d1, d3);
        let d4 = compute_db_gen_digest(42, 101, 5, 99);
        assert_ne!(d1, d4);
        let d5 = compute_db_gen_digest(42, 100, 6, 99);
        assert_ne!(d1, d5);
        let d6 = compute_db_gen_digest(42, 100, 5, 100);
        assert_ne!(d1, d6);
    }

    #[test]
    fn test_stale_sidecar_detection() {
        let hdr = DbFecHeader::new(4096, 42, 100, 5, 99);
        assert!(hdr.is_current(42, 100, 5, 99));
        // Mismatched db_gen_digest -> sidecar ignored.
        assert!(!hdr.is_current(43, 100, 5, 99));
        assert!(!hdr.is_current(42, 101, 5, 99));
    }

    #[test]
    fn test_db_fec_header_bad_checksum() {
        let hdr = DbFecHeader::new(4096, 42, 100, 5, 99);
        let mut bytes = hdr.to_bytes();
        // Corrupt checksum.
        bytes[44] ^= 0xFF;
        let result = DbFecHeader::from_bytes(&bytes);
        assert!(result.is_err());
    }

    #[test]
    fn test_db_fec_header_bad_magic() {
        let hdr = DbFecHeader::new(4096, 42, 100, 5, 99);
        let mut bytes = hdr.to_bytes();
        bytes[0] = b'X';
        let result = DbFecHeader::from_bytes(&bytes);
        assert!(result.is_err());
    }

    // -- Page group partitioning tests --

    #[test]
    fn test_page_group_partitioning_single_page() {
        let groups = partition_page_groups(1);
        assert_eq!(groups.len(), 1);
        assert_eq!(
            groups[0],
            PageGroup {
                start_pgno: 1,
                group_size: 1,
                repair: HEADER_PAGE_R_REPAIR
            }
        );
    }

    #[test]
    fn test_page_group_partitioning_64_pages() {
        let groups = partition_page_groups(64);
        assert_eq!(groups.len(), 2);
        // Page 1 special.
        assert_eq!(groups[0].start_pgno, 1);
        assert_eq!(groups[0].group_size, 1);
        assert_eq!(groups[0].repair, HEADER_PAGE_R_REPAIR);
        // Pages 2-64.
        assert_eq!(groups[1].start_pgno, 2);
        assert_eq!(groups[1].group_size, 63);
        assert_eq!(groups[1].repair, DEFAULT_R_REPAIR);
    }

    #[test]
    fn test_page_group_partitioning_65_pages() {
        let groups = partition_page_groups(65);
        assert_eq!(groups.len(), 2);
        assert_eq!(groups[1].start_pgno, 2);
        assert_eq!(groups[1].group_size, 64);
        assert_eq!(groups[1].repair, DEFAULT_R_REPAIR);
    }

    #[test]
    fn test_page_group_partitioning_128_pages() {
        let groups = partition_page_groups(128);
        assert_eq!(groups.len(), 3);
        assert_eq!(groups[0].start_pgno, 1);
        assert_eq!(groups[0].group_size, 1);
        assert_eq!(groups[1].start_pgno, 2);
        assert_eq!(groups[1].group_size, 64);
        assert_eq!(groups[2].start_pgno, 66);
        assert_eq!(groups[2].group_size, 63);
    }

    #[test]
    fn test_page_group_partitioning_1000_pages() {
        let groups = partition_page_groups(1000);
        // Page 1 + ceil((1000-1)/64) = 1 + 16 = 17 groups.
        assert_eq!(groups.len(), 17);
        assert_eq!(groups[0].group_size, 1);
        // Verify all pages covered.
        let total_pages: u32 = groups.iter().map(|g| g.group_size).sum();
        assert_eq!(total_pages, 1000);
    }

    #[test]
    fn test_page_group_partitioning_zero() {
        let groups = partition_page_groups(0);
        assert!(groups.is_empty());
    }

    #[test]
    fn test_header_page_400pct_redundancy() {
        let groups = partition_page_groups(100);
        // Page 1 group: G=1, R=4 -> 400% redundancy.
        assert_eq!(groups[0].group_size, 1);
        assert_eq!(groups[0].repair, 4);
    }

    // -- Segment offset tests --

    #[test]
    fn test_segment_offset_o1() {
        let page_size: u32 = 4096;
        let seg1_len = group_segment_size(1, HEADER_PAGE_R_REPAIR, page_size);
        let general_seg_len = group_segment_size(DEFAULT_GROUP_SIZE, DEFAULT_R_REPAIR, page_size);

        // Sequential layout check.
        for g in 0..10_u32 {
            let off = segment_offset(g, seg1_len, general_seg_len);
            let expected = DB_FEC_HEADER_SIZE + seg1_len + g as usize * general_seg_len;
            assert_eq!(off, expected, "segment offset mismatch for g={g}");
        }
    }

    // -- DbFecGroupMeta tests --

    #[test]
    fn test_group_meta_roundtrip() {
        let hashes: Vec<[u8; 16]> = (0..4)
            .map(|i| {
                let mut h = [0u8; 16];
                h[0] = i;
                h
            })
            .collect();
        let digest = compute_db_gen_digest(1, 100, 0, 42);
        let meta = DbFecGroupMeta::new(4096, 2, 4, 4, hashes, digest);
        let bytes = meta.to_bytes();
        let decoded = DbFecGroupMeta::from_bytes(&bytes).expect("decode");
        assert_eq!(meta, decoded);
    }

    #[test]
    fn test_group_meta_object_id() {
        let hashes: Vec<[u8; 16]> = (0..2)
            .map(|i| {
                let mut h = [0u8; 16];
                h[0] = i;
                h
            })
            .collect();
        let digest = compute_db_gen_digest(1, 100, 0, 42);
        let meta = DbFecGroupMeta::new(4096, 2, 2, 4, hashes, digest);

        // object_id must be deterministic and content-addressed.
        let oid = meta.object_id;
        assert_ne!(oid, [0u8; 16], "object_id should be non-zero");

        // Changing a hash changes the object_id.
        let mut hashes2: Vec<[u8; 16]> = (0..2)
            .map(|i| {
                let mut h = [0u8; 16];
                h[0] = i;
                h
            })
            .collect();
        hashes2[0][1] = 0xFF;
        let meta2 = DbFecGroupMeta::new(4096, 2, 2, 4, hashes2, digest);
        assert_ne!(meta.object_id, meta2.object_id);
    }

    #[test]
    fn test_group_meta_stale_guard() {
        let hashes = vec![[0u8; 16]; 1];
        let digest = compute_db_gen_digest(1, 100, 0, 42);
        let meta = DbFecGroupMeta::new(4096, 1, 1, 4, hashes, digest);

        let stale_digest = compute_db_gen_digest(2, 100, 0, 42);
        // Group meta with mismatched db_gen_digest should be ignored.
        assert_ne!(meta.db_gen_digest, stale_digest);
    }

    #[test]
    fn test_group_meta_bad_checksum() {
        let hashes = vec![[1u8; 16]; 2];
        let digest = compute_db_gen_digest(1, 100, 0, 42);
        let meta = DbFecGroupMeta::new(4096, 2, 2, 4, hashes, digest);
        let mut bytes = meta.to_bytes();
        // Corrupt last byte (checksum).
        let last = bytes.len() - 1;
        bytes[last] ^= 0xFF;
        let result = DbFecGroupMeta::from_bytes(&bytes);
        assert!(result.is_err());
    }

    // -- Read path repair tests --

    #[test]
    fn test_read_path_intact() {
        let page_size = 64_u32;
        let page_data: Vec<Vec<u8>> = (0..4_u8).map(|i| vec![i; page_size as usize]).collect();
        let hashes: Vec<[u8; 16]> = page_data.iter().map(|d| page_xxh3_128(d)).collect();
        let digest = compute_db_gen_digest(1, 5, 0, 1);
        let meta = DbFecGroupMeta::new(page_size, 2, 4, 4, hashes, digest);

        // All pages intact — verify_page_xxh3_128 succeeds.
        for (i, d) in page_data.iter().enumerate() {
            assert!(verify_page_xxh3_128(d, &meta.source_page_xxh3_128[i]));
        }
    }

    #[test]
    fn test_read_path_single_corruption() {
        let page_size = 64_u32;
        let page_data: Vec<Vec<u8>> = (0..4_u8).map(|i| vec![i + 1; page_size as usize]).collect();
        let hashes: Vec<[u8; 16]> = page_data.iter().map(|d| page_xxh3_128(d)).collect();
        let digest = compute_db_gen_digest(1, 5, 0, 1);
        let meta = DbFecGroupMeta::new(page_size, 2, 4, 4, hashes, digest);

        // Generate RaptorQ repair symbols.
        let source_slices: Vec<&[u8]> = page_data.iter().map(Vec::as_slice).collect();
        let repair_data = compute_raptorq_repair_symbols(&meta, &source_slices, page_size as usize)
            .expect("encode");

        // Corrupt page 3 (pgno=4, index=2 in group).
        let target_pgno = 4;
        let corrupted = vec![0xFF_u8; page_size as usize];

        let read_fn = |pgno: u32| -> Vec<u8> {
            if pgno == target_pgno {
                corrupted.clone()
            } else {
                page_data[(pgno - 2) as usize].clone()
            }
        };

        // Pair ESIs with repair data: ESI = K + r_idx.
        let repair_symbols: Vec<(u32, Vec<u8>)> = repair_data
            .into_iter()
            .enumerate()
            .map(|(i, d)| (4 + u32::try_from(i).expect("i fits u32"), d))
            .collect();
        let result = attempt_page_repair(target_pgno, &meta, &read_fn, &repair_symbols);
        let (recovered, status) = result.expect("repair should succeed");
        assert_eq!(
            recovered, page_data[2],
            "recovered page must match original"
        );
        assert!(matches!(status, RepairResult::Repaired { pgno: 4, .. }));
    }

    #[test]
    fn test_read_path_exceed_corruption() {
        let page_size = 64_u32;
        let page_data: Vec<Vec<u8>> = (0..4_u8).map(|i| vec![i + 1; page_size as usize]).collect();
        let hashes: Vec<[u8; 16]> = page_data.iter().map(|d| page_xxh3_128(d)).collect();
        let digest = compute_db_gen_digest(1, 5, 0, 1);
        let meta = DbFecGroupMeta::new(page_size, 2, 4, 4, hashes, digest);

        // All pages corrupted — no repair possible.
        let corrupted = vec![0xFF_u8; page_size as usize];
        let read_fn = |_pgno: u32| -> Vec<u8> { corrupted.clone() };
        let repair_symbols: Vec<(u32, Vec<u8>)> = Vec::new();

        let result = attempt_page_repair(3, &meta, &read_fn, &repair_symbols);
        assert!(result.is_err());
    }

    #[test]
    fn test_e2e_bitrot_recovery() {
        // Insert data, corrupt one page, read back with repair.
        let page_size = 128_u32;
        let num_pages = 4_u32;
        let pages: Vec<Vec<u8>> = (0..num_pages)
            .map(|i| {
                let mut data = vec![0u8; page_size as usize];
                // Write unique pattern.
                for (j, b) in data.iter_mut().enumerate() {
                    #[allow(clippy::cast_possible_truncation)]
                    {
                        *b = ((i as usize * 37 + j * 13) & 0xFF) as u8;
                    }
                }
                data
            })
            .collect();

        let hashes: Vec<[u8; 16]> = pages.iter().map(|d| page_xxh3_128(d)).collect();
        let digest = compute_db_gen_digest(1, num_pages + 1, 0, 1);
        let meta = DbFecGroupMeta::new(page_size, 2, num_pages, 4, hashes, digest);

        // Generate RaptorQ repair symbols.
        let source_slices: Vec<&[u8]> = pages.iter().map(Vec::as_slice).collect();
        let repair_data = compute_raptorq_repair_symbols(&meta, &source_slices, page_size as usize)
            .expect("encode");

        // Corrupt page 2 (index 0 in group, pgno=2).
        let target = 2_u32;
        let corrupted = vec![0xAA_u8; page_size as usize];

        let read_fn = |pgno: u32| -> Vec<u8> {
            if pgno == target {
                corrupted.clone()
            } else {
                pages[(pgno - 2) as usize].clone()
            }
        };

        let repair_symbols: Vec<(u32, Vec<u8>)> = repair_data
            .into_iter()
            .enumerate()
            .map(|(i, d)| (num_pages + u32::try_from(i).expect("i fits u32"), d))
            .collect();
        let (recovered, _) =
            attempt_page_repair(target, &meta, &read_fn, &repair_symbols).expect("repair");
        assert_eq!(recovered, pages[0]);
    }

    #[test]
    fn test_e2e_stale_sidecar_rejected() {
        let hdr1 = DbFecHeader::new(4096, 1, 100, 0, 1);
        let hdr2 = DbFecHeader::new(4096, 2, 100, 0, 1); // Different change_counter.
        assert_ne!(hdr1.db_gen_digest, hdr2.db_gen_digest);
        assert!(!hdr1.is_current(2, 100, 0, 1));
    }

    #[test]
    fn test_overflow_threshold_g64_r4() {
        // Overhead = R/G = 4/64 = 6.25%.
        let overhead = f64::from(DEFAULT_R_REPAIR) / f64::from(DEFAULT_GROUP_SIZE);
        assert!((overhead - 0.0625).abs() < f64::EPSILON);
    }

    #[test]
    fn test_last_group_partial() {
        // 100 pages: page 1 special, pages 2-65 (64 pages), pages 66-100 (35 pages).
        let groups = partition_page_groups(100);
        assert_eq!(groups.len(), 3);
        assert_eq!(groups[2].start_pgno, 66);
        assert_eq!(groups[2].group_size, 35);

        // Segment offset formula still applies (last group has smaller K but offset
        // is computed from the full-group formula for stable seekability).
        let page_size = 4096_u32;
        let seg1_len = group_segment_size(1, HEADER_PAGE_R_REPAIR, page_size);
        let general_seg_len = group_segment_size(DEFAULT_GROUP_SIZE, DEFAULT_R_REPAIR, page_size);
        let off = segment_offset(1, seg1_len, general_seg_len);
        assert_eq!(
            off,
            DB_FEC_HEADER_SIZE + seg1_len + general_seg_len,
            "second full-group offset"
        );
    }

    #[test]
    fn test_find_full_group_index() {
        assert_eq!(find_full_group_index(1), None); // Header page.
        assert_eq!(find_full_group_index(2), Some(0));
        assert_eq!(find_full_group_index(65), Some(0));
        assert_eq!(find_full_group_index(66), Some(1));
        assert_eq!(find_full_group_index(130), Some(2));
    }

    // -- Compliance gates --

    #[test]
    fn test_bd_1hi_18_unit_compliance_gate() {
        // Verify bead identifiers and mandatory test presence.
        assert_eq!(BEAD_ID, "bd-1hi.18");
        assert_eq!(DB_FEC_MAGIC, *b"FSQLDFEC");
        assert_eq!(GROUP_META_MAGIC, *b"FSQLDGRP");
        assert_eq!(DB_FEC_VERSION, 1);
        assert_eq!(DEFAULT_GROUP_SIZE, 64);
        assert_eq!(DEFAULT_R_REPAIR, 4);
        assert_eq!(HEADER_PAGE_R_REPAIR, 4);
    }

    #[test]
    fn prop_bd_1hi_18_structure_compliance() {
        // Property: partition_page_groups covers all pages exactly once.
        for n in [1_u32, 2, 63, 64, 65, 128, 129, 500, 1000] {
            let groups = partition_page_groups(n);
            let total: u32 = groups.iter().map(|g| g.group_size).sum();
            assert_eq!(total, n, "total pages mismatch for n={n}");

            // No overlaps.
            let mut covered = 0_u32;
            for g in &groups {
                assert!(g.start_pgno > covered, "overlap at pgno {}", g.start_pgno);
                covered = g.start_pgno + g.group_size - 1;
            }
            assert_eq!(covered, n);
        }
    }

    #[test]
    fn test_e2e_bd_1hi_18_compliance() {
        // End-to-end: create header, create groups, verify sidecar coherence.
        let page_size = 4096_u32;
        let db_pages = 200_u32;
        let hdr = DbFecHeader::new(page_size, 10, db_pages, 3, 42);

        // Verify round-trip.
        let hdr2 = DbFecHeader::from_bytes(&hdr.to_bytes()).expect("roundtrip");
        assert_eq!(hdr, hdr2);
        assert!(hdr.is_current(10, db_pages, 3, 42));

        // Verify groups.
        let groups = partition_page_groups(db_pages);
        assert!(!groups.is_empty());
        let total: u32 = groups.iter().map(|g| g.group_size).sum();
        assert_eq!(total, db_pages);

        // Page 1 special group.
        assert_eq!(groups[0].group_size, 1);
        assert_eq!(groups[0].repair, HEADER_PAGE_R_REPAIR);

        // Verify segment offset monotonicity.
        let seg1_len = group_segment_size(1, HEADER_PAGE_R_REPAIR, page_size);
        let general_seg_len = group_segment_size(DEFAULT_GROUP_SIZE, DEFAULT_R_REPAIR, page_size);
        let mut prev_off = 0;
        #[allow(clippy::cast_possible_truncation)]
        let group_count = groups.len().saturating_sub(1) as u32;
        for g in 0..group_count {
            let off = segment_offset(g, seg1_len, general_seg_len);
            assert!(
                off > prev_off || g == 0,
                "offsets must be monotonically increasing"
            );
            prev_off = off;
        }
    }

    // -- Property: db_gen_digest deterministic --

    #[test]
    fn prop_db_gen_digest_deterministic() {
        for i in 0..50_u32 {
            let d1 = compute_db_gen_digest(i, i * 10, i * 2, i * 3);
            let d2 = compute_db_gen_digest(i, i * 10, i * 2, i * 3);
            assert_eq!(d1, d2, "digest must be deterministic for i={i}");
        }
    }

    // -- Property: group_segment_size consistent --

    #[test]
    fn prop_group_segment_sizes_consistent() {
        for ps in [512_u32, 1024, 4096, 8192, 16384, 32768, 65536] {
            let seg1 = group_segment_size(1, HEADER_PAGE_R_REPAIR, ps);
            let general_seg = group_segment_size(DEFAULT_GROUP_SIZE, DEFAULT_R_REPAIR, ps);

            // seg1 should be smaller (fewer source pages = fewer hashes).
            assert!(seg1 < general_seg, "page-1 segment should be smaller");

            // Verify formula: meta_size + R * page_size.
            let expected_seg1 = DbFecGroupMeta::serialized_size_for(1)
                + HEADER_PAGE_R_REPAIR as usize * ps as usize;
            assert_eq!(seg1, expected_seg1);

            let expected_general_seg = DbFecGroupMeta::serialized_size_for(DEFAULT_GROUP_SIZE)
                + DEFAULT_R_REPAIR as usize * ps as usize;
            assert_eq!(general_seg, expected_general_seg);
        }
    }

    // -- Sidecar generation utility tests (bd-2r4z) --

    fn make_synthetic_db(page_size: u32, page_count: u32) -> Vec<u8> {
        let ps = page_size as usize;
        let mut db = vec![0u8; ps * page_count as usize];
        db[..16].copy_from_slice(b"SQLite format 3\0");
        #[allow(clippy::cast_possible_truncation)]
        let ps_enc: u16 = if page_size == 65536 {
            1
        } else {
            page_size as u16
        };
        db[PAGE_SIZE_OFFSET..PAGE_SIZE_OFFSET + 2].copy_from_slice(&ps_enc.to_be_bytes());
        db[CHANGE_COUNTER_OFFSET..CHANGE_COUNTER_OFFSET + 4].copy_from_slice(&1_u32.to_be_bytes());
        db[PAGE_COUNT_OFFSET..PAGE_COUNT_OFFSET + 4].copy_from_slice(&page_count.to_be_bytes());
        db[FREELIST_COUNT_OFFSET..FREELIST_COUNT_OFFSET + 4].copy_from_slice(&0_u32.to_be_bytes());
        db[SCHEMA_COOKIE_OFFSET..SCHEMA_COOKIE_OFFSET + 4].copy_from_slice(&42_u32.to_be_bytes());
        for pgno in 1..=page_count {
            let offset = (pgno as usize - 1) * ps;
            let start = if pgno == 1 { 100 } else { 0 };
            for j in start..ps {
                #[allow(clippy::cast_possible_truncation)]
                {
                    db[offset + j] = ((pgno as usize * 37 + j * 13) & 0xFF) as u8;
                }
            }
        }
        db
    }

    #[test]
    fn test_parse_db_header_fields() {
        let db = make_synthetic_db(4096, 10);
        let fields = parse_db_header_fields(&db).expect("parse");
        assert_eq!(fields.page_size, 4096);
        assert_eq!(fields.change_counter, 1);
        assert_eq!(fields.page_count, 10);
        assert_eq!(fields.freelist_count, 0);
        assert_eq!(fields.schema_cookie, 42);
    }

    #[test]
    fn test_parse_db_header_too_short() {
        assert!(parse_db_header_fields(&[0u8; 50]).is_err());
    }

    #[test]
    fn test_db_fec_path_for_db() {
        let p = db_fec_path_for_db(Path::new("/tmp/test.db"));
        assert_eq!(p, PathBuf::from("/tmp/test.db-fec"));
    }

    #[test]
    fn test_generate_db_fec_sidecar_header_valid() {
        let db = make_synthetic_db(512, 5);
        let sidecar = generate_db_fec_from_bytes(&db).expect("generate");
        assert!(sidecar.len() >= DB_FEC_HEADER_SIZE);
        let mut hdr_buf = [0u8; DB_FEC_HEADER_SIZE];
        hdr_buf.copy_from_slice(&sidecar[..DB_FEC_HEADER_SIZE]);
        let hdr = DbFecHeader::from_bytes(&hdr_buf).expect("header");
        assert_eq!(hdr.page_size, 512);
        assert!(hdr.is_current(1, 5, 0, 42));
    }

    #[test]
    fn test_generate_and_read_group_roundtrip() {
        let db = make_synthetic_db(512, 5);
        let sidecar = generate_db_fec_from_bytes(&db).expect("generate");
        let mut hdr_buf = [0u8; DB_FEC_HEADER_SIZE];
        hdr_buf.copy_from_slice(&sidecar[..DB_FEC_HEADER_SIZE]);
        let hdr = DbFecHeader::from_bytes(&hdr_buf).expect("header");
        let (meta1, syms1) = read_db_fec_group_for_page(&sidecar, &hdr, 1).expect("page 1 group");
        assert_eq!(meta1.start_pgno, 1);
        assert_eq!(meta1.group_size, 1);
        assert_eq!(meta1.r_repair, HEADER_PAGE_R_REPAIR);
        assert_eq!(syms1.len(), HEADER_PAGE_R_REPAIR as usize);
        let (meta2, syms2) = read_db_fec_group_for_page(&sidecar, &hdr, 2).expect("page 2 group");
        assert_eq!(meta2.start_pgno, 2);
        assert_eq!(meta2.group_size, 4);
        assert_eq!(syms2.len(), DEFAULT_R_REPAIR as usize);
        for i in 0..meta2.group_size {
            let page = read_page_from_bytes(&db, meta2.start_pgno + i, 512);
            assert!(verify_page_xxh3_128(
                &page,
                &meta2.source_page_xxh3_128[i as usize]
            ));
        }
    }

    #[test]
    fn test_sidecar_encode_corrupt_decode_cycle() {
        let ps = 512_usize;
        let mut db = make_synthetic_db(512, 5);
        let sidecar = generate_db_fec_from_bytes(&db).expect("generate");
        let mut hdr_buf = [0u8; DB_FEC_HEADER_SIZE];
        hdr_buf.copy_from_slice(&sidecar[..DB_FEC_HEADER_SIZE]);
        let hdr = DbFecHeader::from_bytes(&hdr_buf).expect("header");
        let target_pgno = 3_u32;
        let original_page = read_page_from_bytes(&db, target_pgno, ps);
        let corrupt_offset = (target_pgno as usize - 1) * ps;
        for b in &mut db[corrupt_offset..corrupt_offset + ps] {
            *b = 0xDE;
        }
        let (meta, repair_symbols) =
            read_db_fec_group_for_page(&sidecar, &hdr, target_pgno).expect("read group");
        let corrupted_data = read_page_from_bytes(&db, target_pgno, ps);
        let idx = (target_pgno - meta.start_pgno) as usize;
        assert!(!verify_page_xxh3_128(
            &corrupted_data,
            &meta.source_page_xxh3_128[idx]
        ));
        let read_fn = |pgno: u32| -> Vec<u8> { read_page_from_bytes(&db, pgno, ps) };
        let (recovered, result) =
            attempt_page_repair(target_pgno, &meta, &read_fn, &repair_symbols)
                .expect("repair should succeed");
        assert_eq!(recovered, original_page);
        assert!(matches!(result, RepairResult::Repaired { pgno: 3, .. }));
    }

    #[test]
    fn test_sidecar_header_page_repair() {
        let ps = 256_usize;
        let mut db = make_synthetic_db(256, 3);
        let sidecar = generate_db_fec_from_bytes(&db).expect("generate");
        let mut hdr_buf = [0u8; DB_FEC_HEADER_SIZE];
        hdr_buf.copy_from_slice(&sidecar[..DB_FEC_HEADER_SIZE]);
        let hdr = DbFecHeader::from_bytes(&hdr_buf).expect("header");
        let original_page1 = read_page_from_bytes(&db, 1, ps);
        for b in &mut db[..ps] {
            *b = 0xCC;
        }
        let (meta, repair_symbols) =
            read_db_fec_group_for_page(&sidecar, &hdr, 1).expect("read group");
        assert_eq!(meta.group_size, 1);
        assert_eq!(meta.r_repair, 4);
        let read_fn = |_pgno: u32| -> Vec<u8> { read_page_from_bytes(&db, 1, ps) };
        let (recovered, _) =
            attempt_page_repair(1, &meta, &read_fn, &repair_symbols).expect("repair page 1");
        assert_eq!(recovered, original_page1);
    }

    #[test]
    fn test_sidecar_stale_digest_detection() {
        let db = make_synthetic_db(512, 5);
        let sidecar = generate_db_fec_from_bytes(&db).expect("generate");
        let mut hdr_buf = [0u8; DB_FEC_HEADER_SIZE];
        hdr_buf.copy_from_slice(&sidecar[..DB_FEC_HEADER_SIZE]);
        let hdr = DbFecHeader::from_bytes(&hdr_buf).expect("header");
        assert!(hdr.is_current(1, 5, 0, 42));
        assert!(!hdr.is_current(2, 5, 0, 42));
        assert!(!hdr.is_current(1, 6, 0, 42));
    }

    #[test]
    fn test_sidecar_xxh3_validates_corruption() {
        let db = make_synthetic_db(512, 5);
        let sidecar = generate_db_fec_from_bytes(&db).expect("generate");
        let mut hdr_buf = [0u8; DB_FEC_HEADER_SIZE];
        hdr_buf.copy_from_slice(&sidecar[..DB_FEC_HEADER_SIZE]);
        let hdr = DbFecHeader::from_bytes(&hdr_buf).expect("header");
        let (meta, _) = read_db_fec_group_for_page(&sidecar, &hdr, 3).expect("read");
        let page = read_page_from_bytes(&db, 3, 512);
        let idx = (3 - meta.start_pgno) as usize;
        assert!(verify_page_xxh3_128(&page, &meta.source_page_xxh3_128[idx]));
        let corrupt = vec![0xFF_u8; 512];
        assert!(!verify_page_xxh3_128(
            &corrupt,
            &meta.source_page_xxh3_128[idx]
        ));
    }

    #[test]
    fn test_sidecar_large_db_128_pages() {
        let mut db = make_synthetic_db(512, 128);
        let sidecar = generate_db_fec_from_bytes(&db).expect("generate");
        let mut hdr_buf = [0u8; DB_FEC_HEADER_SIZE];
        hdr_buf.copy_from_slice(&sidecar[..DB_FEC_HEADER_SIZE]);
        let hdr = DbFecHeader::from_bytes(&hdr_buf).expect("header");
        let (m1, _) = read_db_fec_group_for_page(&sidecar, &hdr, 1).expect("page 1");
        assert_eq!(m1.group_size, 1);
        let (m2, _) = read_db_fec_group_for_page(&sidecar, &hdr, 30).expect("page 30");
        assert_eq!(m2.start_pgno, 2);
        assert_eq!(m2.group_size, 64);
        let (m3, _) = read_db_fec_group_for_page(&sidecar, &hdr, 100).expect("page 100");
        assert_eq!(m3.start_pgno, 66);
        assert_eq!(m3.group_size, 63);
        let original = read_page_from_bytes(&db, 100, 512);
        let off = (100 - 1) * 512;
        for b in &mut db[off..off + 512] {
            *b = 0xBB;
        }
        let (meta, syms) = read_db_fec_group_for_page(&sidecar, &hdr, 100).expect("read");
        let read_fn = |pgno: u32| -> Vec<u8> { read_page_from_bytes(&db, pgno, 512) };
        let (recovered, _) =
            attempt_page_repair(100, &meta, &read_fn, &syms).expect("repair page 100");
        assert_eq!(recovered, original);
    }

    #[test]
    fn test_sidecar_file_write_read_roundtrip() {
        let dir = tempfile::tempdir().expect("tempdir");
        let db_path = dir.path().join("test.db");
        let db = make_synthetic_db(512, 5);
        std::fs::write(&db_path, &db).expect("write db");
        let sidecar_path = write_db_fec_sidecar(&db_path).expect("write sidecar");
        assert_eq!(sidecar_path, db_fec_path_for_db(&db_path));
        assert!(sidecar_path.exists());
        let hdr = read_db_fec_header(&sidecar_path).expect("read header");
        assert_eq!(hdr.page_size, 512);
        assert!(hdr.is_current(1, 5, 0, 42));
    }

    // -- RaptorQ-specific tests (bd-n0g4q.2) --

    #[test]
    fn test_raptorq_encode_deterministic() {
        let page_size = 128_u32;
        let pages: Vec<Vec<u8>> = (0..4_u8).map(|i| vec![i + 1; page_size as usize]).collect();
        let hashes: Vec<[u8; 16]> = pages.iter().map(|d| page_xxh3_128(d)).collect();
        let digest = compute_db_gen_digest(1, 5, 0, 1);
        let meta = DbFecGroupMeta::new(page_size, 2, 4, 4, hashes, digest);
        let slices: Vec<&[u8]> = pages.iter().map(Vec::as_slice).collect();
        let r1 = compute_raptorq_repair_symbols(&meta, &slices, page_size as usize).expect("e1");
        let r2 = compute_raptorq_repair_symbols(&meta, &slices, page_size as usize).expect("e2");
        assert_eq!(r1, r2, "RaptorQ encoding must be deterministic");
    }

    #[test]
    fn test_raptorq_encode_produces_correct_count() {
        let page_size = 64_u32;
        let pages: Vec<Vec<u8>> = (0..8_u8).map(|i| vec![i; page_size as usize]).collect();
        let hashes: Vec<[u8; 16]> = pages.iter().map(|d| page_xxh3_128(d)).collect();
        let digest = compute_db_gen_digest(1, 9, 0, 1);
        let meta = DbFecGroupMeta::new(page_size, 2, 8, 4, hashes, digest);
        let slices: Vec<&[u8]> = pages.iter().map(Vec::as_slice).collect();
        let syms =
            compute_raptorq_repair_symbols(&meta, &slices, page_size as usize).expect("encode");
        assert_eq!(syms.len(), 4, "should produce R=4 repair symbols");
        for sym in &syms {
            assert_eq!(sym.len(), page_size as usize, "symbol size = page_size");
        }
    }

    #[test]
    fn test_raptorq_multi_corruption_recovery() {
        // Verify that RaptorQ can recover from multiple corrupted pages
        // (up to R) — something the old XOR parity could not do.
        let page_size = 128_u32;
        let k = 8_u32;
        let r = 4_u32;
        let pages: Vec<Vec<u8>> = (0..k)
            .map(|i| {
                let mut data = vec![0u8; page_size as usize];
                for (j, b) in data.iter_mut().enumerate() {
                    #[allow(clippy::cast_possible_truncation)]
                    {
                        *b = ((i as usize * 41 + j * 7) & 0xFF) as u8;
                    }
                }
                data
            })
            .collect();

        let hashes: Vec<[u8; 16]> = pages.iter().map(|d| page_xxh3_128(d)).collect();
        let digest = compute_db_gen_digest(1, k + 1, 0, 1);
        let meta = DbFecGroupMeta::new(page_size, 2, k, r, hashes, digest);

        let slices: Vec<&[u8]> = pages.iter().map(Vec::as_slice).collect();
        let repair_data =
            compute_raptorq_repair_symbols(&meta, &slices, page_size as usize).expect("encode");
        let repair_symbols: Vec<(u32, Vec<u8>)> = repair_data
            .into_iter()
            .enumerate()
            .map(|(i, d)| (k + u32::try_from(i).expect("i fits u32"), d))
            .collect();

        // Corrupt pages 2 and 3 (indices 0 and 1 in the group).
        let corrupt_pgnos = [2_u32, 3_u32];
        let corrupted = vec![0xDD_u8; page_size as usize];

        let read_fn = |pgno: u32| -> Vec<u8> {
            if corrupt_pgnos.contains(&pgno) {
                corrupted.clone()
            } else {
                pages[(pgno - 2) as usize].clone()
            }
        };

        // Repair page 2.
        let (recovered_p2, status) =
            attempt_page_repair(2, &meta, &read_fn, &repair_symbols).expect("repair page 2");
        assert_eq!(recovered_p2, pages[0]);
        assert!(matches!(status, RepairResult::Repaired { pgno: 2, .. }));

        // Repair page 3.
        let (recovered_p3, status) =
            attempt_page_repair(3, &meta, &read_fn, &repair_symbols).expect("repair page 3");
        assert_eq!(recovered_p3, pages[1]);
        assert!(matches!(status, RepairResult::Repaired { pgno: 3, .. }));
    }

    #[test]
    fn test_raptorq_seed_differs_per_group() {
        let digest = compute_db_gen_digest(1, 200, 0, 42);
        let meta_a = DbFecGroupMeta::new(4096, 1, 1, 4, vec![[0u8; 16]], digest);
        let meta_b = DbFecGroupMeta::new(4096, 2, 64, 4, vec![[0u8; 16]; 64], digest);
        let seed_a = derive_db_fec_repair_seed(&meta_a);
        let seed_b = derive_db_fec_repair_seed(&meta_b);
        assert_ne!(
            seed_a, seed_b,
            "different groups must produce different seeds"
        );
    }

    // -------------------------------------------------------------------
    // Snapshot FEC metrics tests
    // -------------------------------------------------------------------

    #[test]
    fn test_snapshot_fec_metrics_record_and_snapshot() {
        let m = SnapshotFecMetrics::new();
        m.record_encode(100, 4096);
        m.record_encode(64, 2048);
        let s = m.snapshot();
        assert_eq!(s.encoded_pages_total, 164);
        assert_eq!(s.sidecar_bytes_total, 6144);
        assert_eq!(s.encode_ops, 2);
    }

    #[test]
    fn test_snapshot_fec_metrics_reset() {
        let m = SnapshotFecMetrics::new();
        m.record_encode(10, 500);
        m.reset();
        let s = m.snapshot();
        assert_eq!(s.encoded_pages_total, 0);
        assert_eq!(s.sidecar_bytes_total, 0);
        assert_eq!(s.encode_ops, 0);
    }

    #[test]
    fn test_snapshot_fec_metrics_display() {
        let m = SnapshotFecMetrics::new();
        m.record_encode(42, 1024);
        let s = m.snapshot();
        let text = format!("{s}");
        assert!(text.contains("snapshot_fec_pages_encoded=42"));
        assert!(text.contains("sidecar_bytes=1024"));
        assert!(text.contains("encode_ops=1"));
    }

    #[test]
    fn test_snapshot_fec_metrics_global_delta() {
        // Delta-based test safe for parallel execution.
        let before = GLOBAL_SNAPSHOT_FEC_METRICS.snapshot();
        GLOBAL_SNAPSHOT_FEC_METRICS.record_encode(7, 256);
        let after = GLOBAL_SNAPSHOT_FEC_METRICS.snapshot();
        assert_eq!(after.encoded_pages_total - before.encoded_pages_total, 7);
        assert_eq!(after.sidecar_bytes_total - before.sidecar_bytes_total, 256);
        assert_eq!(after.encode_ops - before.encode_ops, 1);
    }
}
