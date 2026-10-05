//! Storage-backed two-barrier publication for native ECS commits.
//!
//! This is an I/O boundary, not an SQL executor or an SSI validator. Callers
//! supply validated commit markers and an object verifier that decodes and
//! checks their capsule/proof objects (including authentication when enabled).
//! The verifier is required on both publication and recovery; merely finding
//! an ObjectId in the symbol index is never sufficient evidence of durability.
//!
//! The two dedicated streams contain the existing `SymbolRecord` and
//! `CommitMarker` wire records, respectively, starting at offset zero. They
//! are NOT compatibility WAL files or the header-bearing core segment files.
//! The caller owns namespace admission, durable creation of the two names,
//! and the single append-owner lease. That lease must outlive outstanding VFS
//! writes, including writes whose awaiting future was dropped. This module
//! acquires no database-wide transaction lock and creates/deletes no files.

use std::collections::{BTreeMap, BTreeSet};

use fsqlite_error::{FrankenError, Result};
use fsqlite_types::cx::Cx;
use fsqlite_types::{COMMIT_MARKER_RECORD_V1_SIZE, CommitMarker, CommitSeq, ObjectId, SymbolRecord};
use fsqlite_vfs::{SyncKind, VfsFile, VfsWriteCompletion, VfsWriteCompletionState};

const SYMBOL_HEADER_BYTES: usize = 51;
const SYMBOL_TRAILER_BYTES: usize = 25;

/// Allocation and storage bounds. Limits are checked before reading or writing
/// a variable-length record. Large objects should be split by their producer.
#[derive(Debug, Clone, Copy)]
pub struct NativeDurabilityLimits {
    pub max_symbol_bytes: usize,
    pub max_batch_bytes: usize,
    pub max_stream_bytes: u64,
    pub max_records: usize,
}

impl Default for NativeDurabilityLimits {
    fn default() -> Self {
        Self {
            max_symbol_bytes: 65_536,
            max_batch_bytes: 16 * 1024 * 1024,
            max_stream_bytes: 512 * 1024 * 1024,
            max_records: 1_000_000,
        }
    }
}

impl NativeDurabilityLimits {
    fn validate(self) -> Result<()> {
        if self.max_symbol_bytes == 0
            || self.max_batch_bytes < SYMBOL_HEADER_BYTES + SYMBOL_TRAILER_BYTES
            || self.max_stream_bytes == 0
            || self.max_records == 0
        {
            return Err(FrankenError::OutOfRange {
                what: "native durability limits".to_owned(),
                value: format!("{self:?}"),
            });
        }
        Ok(())
    }
}

/// Receipt issued only after referent sync, marker append, and marker sync.
/// The storage/namespace guarantees of the supplied VFS still apply; MemoryVfs
/// is useful for tests but cannot make a power-loss durability guarantee.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NativeDurabilityReceipt {
    pub first_seq: CommitSeq,
    pub last_seq: CommitSeq,
    pub commits: usize,
    pub symbol_stream_bytes: u64,
    pub marker_stream_bytes: u64,
}

#[derive(Debug, Clone, Copy)]
struct SymbolLocation {
    offset: u64,
    len: usize,
}

/// An append-owner's pair of open native streams.
///
/// An I/O error or dropped write future poisons this instance BEFORE the side
/// effect. It cannot subsequently publish or append, even if the write later
/// succeeds. Settle the source-owned completion, retain the files, and recover
/// under the same append-owner authority. An error never means "rolled back".
#[derive(Debug)]
pub struct NativeDurabilityLog<S: VfsFile, M: VfsFile> {
    symbols: S,
    markers: M,
    limits: NativeDurabilityLimits,
    index: BTreeMap<ObjectId, Vec<SymbolLocation>>,
    record_count: usize,
    symbol_end: u64,
    marker_end: u64,
    published_tip: CommitSeq,
    previous_marker: Option<ObjectId>,
    last_time_ns: u64,
    poisoned: bool,
    last_write: Option<VfsWriteCompletion>,
    symbols_closed: bool,
    markers_closed: bool,
}

impl<S: VfsFile, M: VfsFile> NativeDurabilityLog<S, M> {
    /// Adopt two distinct, empty, already-admitted streams. This never
    /// truncates existing data. Parent-directory durability and append-owner
    /// authority are the caller's responsibility, not inferred from a path.
    pub fn create(
        cx: &Cx,
        symbols: S,
        markers: M,
        limits: NativeDurabilityLimits,
    ) -> Result<Self> {
        limits.validate()?;
        checkpoint(cx)?;
        ensure_distinct_files(&symbols, &markers)?;
        if symbols.file_size(cx)? != 0 || markers.file_size(cx)? != 0 {
            return Err(corrupt("create requires empty native streams"));
        }
        Ok(Self::empty(symbols, markers, limits))
    }

    fn empty(symbols: S, markers: M, limits: NativeDurabilityLimits) -> Self {
        Self {
            symbols,
            markers,
            limits,
            index: BTreeMap::new(),
            record_count: 0,
            symbol_end: 0,
            marker_end: 0,
            published_tip: CommitSeq::ZERO,
            previous_marker: None,
            last_time_ns: 0,
            poisoned: false,
            last_write: None,
            symbols_closed: false,
            markers_closed: false,
        }
    }

    #[must_use]
    pub const fn published_tip(&self) -> CommitSeq {
        self.published_tip
    }

    #[must_use]
    pub const fn needs_recovery(&self) -> bool {
        self.poisoned
    }

    /// Observe, rather than guess at, completion of an abandoned VFS write.
    #[must_use]
    pub fn outstanding_write(&self) -> Option<VfsWriteCompletion> {
        self.last_write.clone()
    }

    fn ready(&self) -> Result<()> {
        if self.poisoned || self.symbols_closed || self.markers_closed {
            return Err(FrankenError::BusyRecovery);
        }
        Ok(())
    }

    fn check_lengths(&mut self, cx: &Cx) -> Result<()> {
        if self.symbols.file_size(cx)? != self.symbol_end
            || self.markers.file_size(cx)? != self.marker_end
        {
            self.poisoned = true;
            return Err(FrankenError::BusyRecovery);
        }
        ensure_distinct_files(&self.symbols, &self.markers)
    }

    /// Stage already-encoded ECS symbols. No commit becomes visible here.
    /// Source and repair records use their existing canonical wire format.
    /// The producer's object decoder/authenticator is invoked by `publish`.
    pub async fn append_symbols(&mut self, cx: &Cx, records: &[SymbolRecord]) -> Result<()> {
        self.ready()?;
        checkpoint(cx)?;
        self.check_lengths(cx)?;
        if records.is_empty() {
            return Ok(());
        }
        let count = self
            .record_count
            .checked_add(records.len())
            .ok_or(FrankenError::TooBig)?;
        if count > self.limits.max_records {
            return Err(FrankenError::TooBig);
        }

        let mut bytes = Vec::new();
        let mut locations = Vec::new();
        locations
            .try_reserve_exact(records.len())
            .map_err(|_| FrankenError::OutOfMemory)?;
        for record in records {
            checkpoint(cx)?;
            let symbol_len = usize::try_from(record.oti.t).map_err(|_| FrankenError::TooBig)?;
            // Check before to_bytes/verify_integrity, which assume this invariant.
            if symbol_len == 0 || symbol_len != record.symbol_data.len() {
                return Err(corrupt("symbol payload does not match OTI.T"));
            }
            if symbol_len > self.limits.max_symbol_bytes {
                return Err(FrankenError::TooBig);
            }
            let len = symbol_len
                .checked_add(SYMBOL_HEADER_BYTES + SYMBOL_TRAILER_BYTES)
                .ok_or(FrankenError::TooBig)?;
            let end = bytes.len().checked_add(len).ok_or(FrankenError::TooBig)?;
            if end > self.limits.max_batch_bytes {
                return Err(FrankenError::TooBig);
            }
            let wire = record.to_bytes();
            SymbolRecord::from_bytes(&wire).map_err(|error| corrupt(&error.to_string()))?;
            let offset = self
                .symbol_end
                .checked_add(u64_len(bytes.len())?)
                .ok_or(FrankenError::TooBig)?;
            bytes
                .try_reserve(len)
                .map_err(|_| FrankenError::OutOfMemory)?;
            bytes.extend_from_slice(&wire);
            locations.push((record.object_id, SymbolLocation { offset, len }));
        }
        let end = self
            .symbol_end
            .checked_add(u64_len(bytes.len())?)
            .ok_or(FrankenError::TooBig)?;
        if end > self.limits.max_stream_bytes {
            return Err(FrankenError::TooBig);
        }

        // Arm before polling the side effect. A dropped future leaves the
        // owner poisoned and its completion token available for reconciliation.
        self.poisoned = true;
        let completion = VfsWriteCompletion::new();
        self.last_write = Some(completion.clone());
        self.symbols
            .write_tracked(cx, &bytes, self.symbol_end, completion)
            .await?;
        for (object_id, location) in locations {
            self.index.entry(object_id).or_default().push(location);
        }
        self.symbol_end = end;
        self.record_count = count;
        self.poisoned = false;
        self.last_write = None;
        Ok(())
    }

    /// Read actual storage bytes, rechecking envelopes and indexed identities.
    /// Returned symbols are NOT a claim of successful RaptorQ reconstruction.
    pub async fn read_object(&self, cx: &Cx, object_id: ObjectId) -> Result<Vec<SymbolRecord>> {
        self.ready()?;
        let locations = self
            .index
            .get(&object_id)
            .ok_or_else(|| corrupt("missing native object"))?;
        let total = locations.iter().try_fold(0_usize, |sum, location| {
            sum.checked_add(location.len).ok_or(FrankenError::TooBig)
        })?;
        if total > self.limits.max_batch_bytes {
            return Err(FrankenError::TooBig);
        }
        let mut records = Vec::new();
        records
            .try_reserve_exact(locations.len())
            .map_err(|_| FrankenError::OutOfMemory)?;
        for location in locations {
            checkpoint(cx)?;
            let mut bytes = zeroed(location.len())?;
            read_exact_at(&self.symbols, cx, &mut bytes, location.offset).await?;
            let record = SymbolRecord::from_bytes(&bytes)
                .map_err(|error| corrupt(&error.to_string()))?;
            if record.object_id != object_id {
                return Err(corrupt("native object locator identity mismatch"));
            }
            records.push(record);
        }
        Ok(records)
    }

    /// Publish a contiguous, already-validated commit batch through real VFS
    /// sync calls. `verify_object` must reconstruct each capsule/proof and
    /// verify its canonical identity, content, and required authentication.
    /// It must also validate any transitive dependencies required by the
    /// application's proof format. SQL/SSI validation belongs to the caller.
    ///
    /// All referents are read and verified BEFORE the first sync. FSYNC_1
    /// covers the symbol stream; only then are markers written and FSYNC_2
    /// issued. Publication state advances only after FSYNC_2 returns success.
    /// A sync/write error or cancellation is indeterminate, not a rollback.
    pub async fn publish<V, VF>(
        &mut self,
        cx: &Cx,
        markers: &[CommitMarker],
        mut verify_object: V,
    ) -> Result<Option<NativeDurabilityReceipt>>
    where
        V: FnMut(ObjectId, Vec<SymbolRecord>) -> VF,
        VF: std::future::Future<Output = Result<()>>,
    {
        self.ready()?;
        checkpoint(cx)?;
        self.check_lengths(cx)?;
        if markers.is_empty() {
            return Ok(None);
        }
        let len = markers
            .len()
            .checked_mul(COMMIT_MARKER_RECORD_V1_SIZE)
            .ok_or(FrankenError::TooBig)?;
        let end = self
            .marker_end
            .checked_add(u64_len(len)?)
            .ok_or(FrankenError::TooBig)?;
        if len > self.limits.max_batch_bytes
            || end > self.limits.max_stream_bytes
            || end / u64_len(COMMIT_MARKER_RECORD_V1_SIZE)? > u64_len(self.limits.max_records)?
        {
            return Err(FrankenError::TooBig);
        }
        let mut wire = Vec::new();
        wire.try_reserve_exact(len)
            .map_err(|_| FrankenError::OutOfMemory)?;
        let mut tip = self.published_tip;
        let mut previous = self.previous_marker;
        let mut time = self.last_time_ns;
        let mut verified = BTreeSet::new();
        for marker in markers {
            checkpoint(cx)?;
            validate_marker(marker, tip, previous, time)?;
            for object_id in [marker.capsule_object_id, marker.proof_object_id] {
                if verified.insert(object_id) {
                    let records = self.read_object(cx, object_id).await?;
                    verify_object(object_id, records).await?;
                }
            }
            let bytes = marker.to_record_bytes();
            wire.extend_from_slice(&bytes);
            tip = marker.commit_seq;
            previous = Some(ObjectId::derive_from_canonical_bytes(&bytes));
            time = marker.commit_time_unix_ns;
        }
        // Recheck the append window after arbitrary verifier code and reads.
        self.check_lengths(cx)?;
        let receipt = NativeDurabilityReceipt {
            first_seq: markers[0].commit_seq,
            last_seq: tip,
            commits: markers.len(),
            symbol_stream_bytes: self.symbol_end,
            marker_stream_bytes: end,
        };
        self.poisoned = true;
        self.symbols.durable_sync(cx, SyncKind::FullDurable)?; // FSYNC_1
        let completion = VfsWriteCompletion::new();
        self.last_write = Some(completion.clone());
        self.markers
            .write_tracked(cx, &wire, self.marker_end, completion)
            .await?;
        self.markers.durable_sync(cx, SyncKind::FullDurable)?; // FSYNC_2
        self.marker_end = end;
        self.published_tip = tip;
        self.previous_marker = previous;
        self.last_time_ns = time;
        self.last_write = None;
        self.poisoned = false;
        Ok(Some(receipt))
    }

    /// Close only after a source-owned write has settled. Unknown completion
    /// is refused rather than releasing ownership beneath an active writer.
    /// Failed closes retain their individual obligation for a subsequent call.
    pub fn close(&mut self, cx: &Cx) -> Result<()> {
        if self
            .last_write
            .as_ref()
            .is_some_and(|write| write.state() == VfsWriteCompletionState::Pending)
        {
            return Err(FrankenError::BusyRecovery);
        }
        self.poisoned = true;
        let symbols_result = if self.symbols_closed {
            Ok(())
        } else {
            self.symbols.close(cx)
        };
        if symbols_result.is_ok() {
            self.symbols_closed = true;
        }
        let markers_result = if self.markers_closed {
            Ok(())
        } else {
            self.markers.close(cx)
        };
        if markers_result.is_ok() {
            self.markers_closed = true;
        }
        symbols_result.and(markers_result)
    }
}

fn validate_marker(
    marker: &CommitMarker,
    tip: CommitSeq,
    previous: Option<ObjectId>,
    time: u64,
) -> Result<()> {
    if !marker.verify_integrity() {
        return Err(corrupt("native marker integrity mismatch"));
    }
    if tip.get().checked_add(1) != Some(marker.commit_seq.get()) {
        return Err(corrupt("non-contiguous native marker sequence"));
    }
    if marker.prev_marker != previous || marker.commit_time_unix_ns < time {
        return Err(corrupt("native marker chain or clock mismatch"));
    }
    Ok(())
}

fn ensure_distinct_files<S: VfsFile, M: VfsFile>(symbols: &S, markers: &M) -> Result<()> {
    let symbols_id = symbols
        .refresh_file_identity()?
        .ok_or(FrankenError::Unsupported)?;
    let markers_id = markers
        .refresh_file_identity()?
        .ok_or(FrankenError::Unsupported)?;
    if symbols_id == markers_id {
        return Err(corrupt("native symbol and marker streams alias the same file"));
    }
    Ok(())
}

fn checkpoint(cx: &Cx) -> Result<()> {
    cx.checkpoint().map_err(|_| FrankenError::Interrupt)
}

fn corrupt(detail: &str) -> FrankenError {
    FrankenError::WalCorrupt {
        detail: detail.to_owned(),
    }
}

fn u64_len(len: usize) -> Result<u64> {
    u64::try_from(len).map_err(|_| FrankenError::TooBig)
}

fn zeroed(len: usize) -> Result<Vec<u8>> {
    let mut bytes = Vec::new();
    bytes
        .try_reserve_exact(len)
        .map_err(|_| FrankenError::OutOfMemory)?;
    bytes.resize(len, 0);
    Ok(bytes)
}

async fn read_exact_at<F: VfsFile>(
    file: &F,
    cx: &Cx,
    bytes: &mut [u8],
    offset: u64,
) -> Result<()> {
    let mut read = 0;
    while read < bytes.len() {
        checkpoint(cx)?;
        let position = offset
            .checked_add(u64_len(read)?)
            .ok_or(FrankenError::TooBig)?;
        let count = file.read(cx, &mut bytes[read..], position).await?;
        if count == 0 || count > bytes.len() - read {
            return Err(corrupt("short native stream read"));
        }
        read += count;
    }
    Ok(())
}
