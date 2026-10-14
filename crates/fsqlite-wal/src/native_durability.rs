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

#[path = "native_durability_copy.rs"]
mod prefix_copy;
pub use prefix_copy::NativePrefixCopyReceipt;

mod recovery;
pub use recovery::{NativeRecovery, RecoveredNativeLog};

use std::collections::{BTreeMap, BTreeSet};

use fsqlite_error::{FrankenError, Result};
use fsqlite_types::cx::Cx;
use fsqlite_types::ecs::{SYMBOL_RECORD_MAGIC, SYMBOL_RECORD_VERSION, SymbolRecordError};
use fsqlite_types::{
    COMMIT_MARKER_RECORD_V1_SIZE, CommitMarker, CommitSeq, ObjectId, Oti, SymbolRecord,
};
use fsqlite_vfs::{SyncKind, VfsFile, VfsWriteCompletion, VfsWriteCompletionState};

const SYMBOL_HEADER_BYTES: usize = 51;
const SYMBOL_TRAILER_BYTES: usize = 25;

/// Allocation and storage bounds. Limits are checked before reading or writing
/// a variable-length record. Large objects should be split by their producer.
#[derive(Debug, Clone, Copy)]
pub struct NativeDurabilityLimits {
    /// Maximum payload bytes in one encoded symbol.
    pub max_symbol_bytes: usize,
    /// Maximum append/read batch bytes and advertised decoded object length.
    /// Also bounds all indexed wire records for one object across appends;
    /// adding redundancy must not make a committed referent unreadable.
    pub max_batch_bytes: usize,
    /// Maximum bytes in each of the two streams.
    pub max_stream_bytes: u64,
    /// Maximum complete records in each stream.
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
///
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

/// Verified recovery result.
///
/// Both streams have been re-synchronized before
/// this report is returned. Complete corrupt markers or undecodable referents
/// fail recovery; they are never reclassified as an uncommitted tail.
#[derive(Debug)]
pub struct NativeDurabilityRecovery {
    /// Contiguous verified markers from genesis, usable for capsule replay and
    /// restoration of the coordinator's conflict history.
    pub markers: Vec<CommitMarker>,
    /// Complete symbol records scanned, including checksum-damaged erasures.
    pub symbol_records: usize,
    /// Complete envelopes omitted because their integrity check failed.
    /// The required object verifier must reconstruct from surviving symbols.
    pub erased_symbols: usize,
    /// Retained incomplete bytes after the last complete symbol record.
    pub symbol_tail_bytes: u64,
    /// Retained incomplete bytes after the last complete marker record.
    pub marker_tail_bytes: u64,
}

impl NativeDurabilityRecovery {
    /// A retained torn tail is readable but must not be appended through.
    #[must_use]
    pub const fn append_blocked(&self) -> bool {
        self.symbol_tail_bytes != 0 || self.marker_tail_bytes != 0
    }
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
#[allow(clippy::struct_excessive_bools)]
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
    append_blocked: bool,
    last_write: Option<VfsWriteCompletion>,
    symbols_closed: bool,
    markers_closed: bool,
}

impl<S: VfsFile, M: VfsFile> NativeDurabilityLog<S, M> {
    /// Adopt two distinct, empty, already-admitted streams. This never
    /// truncates existing data. Parent-directory durability and append-owner
    /// authority are the caller's responsibility, not inferred from a path.
    ///
    /// # Errors
    /// Returns an error for invalid limits, unavailable/aliased identities,
    /// nonempty streams, cancellation, or VFS failure.
    pub fn create(cx: &Cx, symbols: S, markers: M, limits: NativeDurabilityLimits) -> Result<Self> {
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
            append_blocked: false,
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
        self.poisoned || self.append_blocked
    }

    /// Observe, rather than guess at, completion of an abandoned VFS write.
    #[must_use]
    pub fn outstanding_write(&self) -> Option<VfsWriteCompletion> {
        self.last_write.clone()
    }

    /// Transfer this exact log, including any source-owned write completion,
    /// into an explicitly driven rewrite-before-sync recovery owner.
    #[must_use]
    pub fn into_recovery(self) -> NativeRecovery<S, M> {
        NativeRecovery::from_log(self)
    }

    fn ready(&self) -> Result<()> {
        self.readable()?;
        if self.append_blocked {
            return Err(FrankenError::BusyRecovery);
        }
        Ok(())
    }

    fn readable(&self) -> Result<()> {
        if self.poisoned || self.symbols_closed || self.markers_closed {
            return Err(FrankenError::BusyRecovery);
        }
        Ok(())
    }

    /// Rebuild native append state from actual storage, starting at genesis.
    ///
    /// The caller must retain append-owner/namespace authority and settle ALL
    /// abandoned writes before supplying these handles. This method cannot
    /// discover a detached write that another owner has not reported.
    ///
    /// No bytes are truncated or replaced. Bounded complete envelopes with a
    /// bad checksum are erasures; malformed framing is fatal. Every committed
    /// capsule and proof must pass the mandatory async object verifier before
    /// referents and markers are re-synchronized in that order. Readable bytes
    /// after a failed sync are not themselves evidence of durability.
    /// A successful retry of fsync also need not write clean-but-lost cache
    /// pages. This non-rewriting entry requires that such writeback uncertainty
    /// has already been resolved (for example, a fresh system boot or a prior
    /// [`NativeRecovery::prepare`]). For live recovery after an I/O error use
    /// [`NativeRecovery`], which retains rewrite buffers and completion tokens
    /// outside its borrowed future. Reopening handles alone is not sufficient.
    ///
    /// An incomplete tail is reported and retained. The returned log permits
    /// object reads but refuses new appends/publication until the caller has
    /// separately reconciled that tail. No partial recovery escapes on error.
    ///
    /// # Errors
    /// Returns an error for malformed framing/markers, unrecoverable objects,
    /// exceeded limits, cancellation, changed stream lengths, or VFS failure.
    pub async fn recover<V, VF>(
        cx: &Cx,
        symbols: S,
        markers: M,
        limits: NativeDurabilityLimits,
        verify_object: V,
    ) -> Result<(Self, NativeDurabilityRecovery)>
    where
        V: FnMut(ObjectId, Vec<SymbolRecord>) -> VF,
        VF: std::future::Future<Output = Result<()>>,
    {
        let mut log = Self::empty(symbols, markers, limits);
        let report = log.scan_recovery(cx, verify_object).await?;
        log.symbols.durable_sync(cx, SyncKind::FullDurable)?;
        log.markers.durable_sync(cx, SyncKind::FullDurable)?;
        log.poisoned = false;
        Ok((log, report))
    }

    // Shared, non-mutating-on-disk admission for both recovery entry points.
    // A cancelled or failed scan leaves this owner poisoned, even if its
    // partially rebuilt index happens to contain every requested object.
    async fn scan_recovery<V, VF>(
        &mut self,
        cx: &Cx,
        mut verify_object: V,
    ) -> Result<NativeDurabilityRecovery>
    where
        V: FnMut(ObjectId, Vec<SymbolRecord>) -> VF,
        VF: std::future::Future<Output = Result<()>>,
    {
        self.poisoned = true;
        self.limits.validate()?;
        checkpoint(cx)?;
        ensure_distinct_files(&self.symbols, &self.markers)?;
        let limits = self.limits;
        let symbol_end = self.symbols.file_size(cx)?;
        let marker_end = self.markers.file_size(cx)?;
        let marker_width = u64_len(COMMIT_MARKER_RECORD_V1_SIZE)?;
        let marker_count =
            usize::try_from(marker_end / marker_width).map_err(|_| FrankenError::TooBig)?;
        if symbol_end > limits.max_stream_bytes
            || marker_end > limits.max_stream_bytes
            || marker_count > limits.max_records
        {
            return Err(FrankenError::TooBig);
        }
        self.index.clear();
        self.record_count = 0;
        self.symbol_end = symbol_end;
        self.marker_end = marker_end;
        let mut offset = 0_u64;
        let mut erased_symbols = 0_usize;
        while symbol_end - offset >= u64_len(SYMBOL_HEADER_BYTES)? {
            checkpoint(cx)?;
            if self.record_count == limits.max_records {
                return Err(FrankenError::TooBig);
            }
            let mut header = [0_u8; SYMBOL_HEADER_BYTES];
            read_exact_at(&self.symbols, cx, &mut header, offset).await?;
            let len = symbol_wire_len(&header, limits)?;
            let end = offset
                .checked_add(u64_len(len)?)
                .ok_or(FrankenError::TooBig)?;
            if end > symbol_end {
                break;
            }
            let mut bytes = zeroed(len)?;
            read_exact_at(&self.symbols, cx, &mut bytes, offset).await?;
            match SymbolRecord::from_bytes(&bytes) {
                Ok(record) => {
                    self.index
                        .entry(record.object_id)
                        .or_default()
                        .push(SymbolLocation { offset, len });
                }
                Err(SymbolRecordError::IntegrityFailure { .. }) => erased_symbols += 1,
                Err(error) => return Err(corrupt(&error.to_string())),
            }
            self.record_count += 1;
            offset = end;
        }
        let mut recovered = Vec::new();
        recovered
            .try_reserve_exact(marker_count)
            .map_err(|_| FrankenError::OutOfMemory)?;
        let mut tip = CommitSeq::ZERO;
        let mut previous = None;
        let mut time = 0;
        let mut verified = BTreeSet::new();
        for ordinal in 0..marker_count {
            checkpoint(cx)?;
            let mut bytes = [0_u8; COMMIT_MARKER_RECORD_V1_SIZE];
            let position = u64_len(ordinal)?
                .checked_mul(marker_width)
                .ok_or(FrankenError::TooBig)?;
            read_exact_at(&self.markers, cx, &mut bytes, position).await?;
            let marker = CommitMarker::from_record_bytes(&bytes)
                .ok_or_else(|| corrupt("malformed native marker record"))?;
            validate_marker(&marker, tip, previous, time)?;
            for object_id in [marker.capsule_object_id, marker.proof_object_id] {
                if verified.insert(object_id) {
                    let records = self.read_indexed_object(cx, object_id).await?;
                    verify_object(object_id, records).await?;
                }
            }
            tip = marker.commit_seq;
            previous = Some(ObjectId::derive_from_canonical_bytes(&bytes));
            time = marker.commit_time_unix_ns;
            recovered.push(marker);
        }
        let report = NativeDurabilityRecovery {
            markers: recovered,
            symbol_records: self.record_count,
            erased_symbols,
            symbol_tail_bytes: symbol_end - offset,
            marker_tail_bytes: marker_end % marker_width,
        };
        self.check_lengths(cx)?;
        checkpoint(cx)?;
        self.published_tip = tip;
        self.previous_marker = previous;
        self.last_time_ns = time;
        self.append_blocked = report.append_blocked();
        Ok(report)
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
    /// Each object's complete indexed wire size, including earlier appends,
    /// must fit the read budget. A rejected batch changes neither stream nor
    /// any locator, even when its earlier records would fit individually.
    ///
    /// # Errors
    /// Returns an error for blocked state, invalid records, exceeded limits,
    /// cancellation, changed stream lengths, or VFS failure.
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
        let mut object_bytes = BTreeMap::<ObjectId, usize>::new();
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
            if record.oti.f == 0 || record.oti.f > u64_len(self.limits.max_batch_bytes)? {
                return Err(FrankenError::TooBig);
            }
            let len = symbol_len
                .checked_add(SYMBOL_HEADER_BYTES + SYMBOL_TRAILER_BYTES)
                .ok_or(FrankenError::TooBig)?;
            let end = bytes.len().checked_add(len).ok_or(FrankenError::TooBig)?;
            if end > self.limits.max_batch_bytes {
                return Err(FrankenError::TooBig);
            }
            // The reader gathers ALL indexed records for an object, not just
            // this append's records. Checking only `end` can admit repeated
            // small writes that make an acknowledged object (and its recovery)
            // fail with TooBig. Project each touched object's complete wire
            // footprint once, then accumulate repeated IDs in this batch.
            // Keep this map local so a late rejection cannot admit a prefix.
            let total = match object_bytes.entry(record.object_id) {
                std::collections::btree_map::Entry::Occupied(entry) => entry.into_mut(),
                std::collections::btree_map::Entry::Vacant(entry) => {
                    let mut total = 0_usize;
                    if let Some(existing) = self.index.get(&record.object_id) {
                        for location in existing {
                            checkpoint(cx)?;
                            total = total
                                .checked_add(location.len)
                                .filter(|size| *size <= self.limits.max_batch_bytes)
                                .ok_or(FrankenError::TooBig)?;
                        }
                    }
                    entry.insert(total)
                }
            };
            *total = total
                .checked_add(len)
                .filter(|size| *size <= self.limits.max_batch_bytes)
                .ok_or(FrankenError::TooBig)?;
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
    /// Checksum-damaged records are omitted as erasures. Returned symbols are
    /// NOT a claim of successful RaptorQ reconstruction or authentication.
    ///
    /// # Errors
    /// Returns an error for unreadable state, missing/fully erased objects,
    /// malformed records, exceeded limits, cancellation, or VFS failure.
    pub async fn read_object(&self, cx: &Cx, object_id: ObjectId) -> Result<Vec<SymbolRecord>> {
        self.readable()?;
        self.read_indexed_object(cx, object_id).await
    }

    // Only the poisoned recovery owner may use the rebuilt index before the
    // durability barriers. Public reads still require `readable` above.
    async fn read_indexed_object(&self, cx: &Cx, object_id: ObjectId) -> Result<Vec<SymbolRecord>> {
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
            let mut bytes = zeroed(location.len)?;
            read_exact_at(&self.symbols, cx, &mut bytes, location.offset).await?;
            if symbol_wire_len(&bytes, self.limits)? != location.len {
                return Err(corrupt("native object locator length mismatch"));
            }
            let record = match SymbolRecord::from_bytes(&bytes) {
                Ok(record) => record,
                Err(SymbolRecordError::IntegrityFailure { .. }) => continue,
                Err(error) => return Err(corrupt(&error.to_string())),
            };
            if record.object_id != object_id {
                return Err(corrupt("native object locator identity mismatch"));
            }
            records.push(record);
        }
        if records.is_empty() {
            return Err(corrupt("no intact symbols for native object"));
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
    ///
    /// # Errors
    /// Returns an error for blocked state, invalid markers or referents,
    /// exceeded limits, cancellation, changed stream lengths, or VFS failure.
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
    ///
    /// # Errors
    /// Returns `BusyRecovery` while a tracked write is pending, otherwise any
    /// VFS close error. Both independent close obligations are attempted.
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

fn symbol_wire_len(header: &[u8], limits: NativeDurabilityLimits) -> Result<usize> {
    if header.len() < SYMBOL_HEADER_BYTES
        || header[..4] != SYMBOL_RECORD_MAGIC
        || header[4] != SYMBOL_RECORD_VERSION
    {
        return Err(corrupt("malformed native symbol header"));
    }
    let oti =
        Oti::from_bytes(&header[21..43]).ok_or_else(|| corrupt("malformed native symbol OTI"))?;
    let size = u32::from_le_bytes(
        header[47..51]
            .try_into()
            .map_err(|_| corrupt("short symbol size"))?,
    );
    if size == 0 || size != oti.t {
        return Err(corrupt("native symbol size does not match OTI.T"));
    }
    let size = usize::try_from(size).map_err(|_| FrankenError::TooBig)?;
    let len = size
        .checked_add(SYMBOL_HEADER_BYTES + SYMBOL_TRAILER_BYTES)
        .ok_or(FrankenError::TooBig)?;
    // Bound both the encoded allocation and the decoder's advertised output.
    if size > limits.max_symbol_bytes
        || len > limits.max_batch_bytes
        || oti.f == 0
        || oti.f > u64_len(limits.max_batch_bytes)?
    {
        return Err(FrankenError::TooBig);
    }
    Ok(len)
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
        return Err(corrupt(
            "native symbol and marker streams alias the same file",
        ));
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

async fn read_exact_at<F: VfsFile>(file: &F, cx: &Cx, bytes: &mut [u8], offset: u64) -> Result<()> {
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

#[cfg(all(test, not(target_arch = "wasm32")))]
mod object_admission_tests {
    use std::future::{Future, Ready, ready};
    use std::path::Path;

    use asupersync::runtime::RuntimeBuilder;
    use fsqlite_types::flags::VfsOpenFlags;
    use fsqlite_types::SymbolRecordFlags;
    use fsqlite_vfs::{MemoryVfs, Vfs};

    use super::*;

    type MemoryFile = <MemoryVfs as Vfs>::File;
    type Log = NativeDurabilityLog<MemoryFile, MemoryFile>;

    fn run(future: impl Future<Output = ()>) {
        RuntimeBuilder::current_thread()
            .blocking_threads(1, 2)
            .build()
            .unwrap()
            .block_on(future);
    }

    fn file(vfs: &MemoryVfs, cx: &Cx, name: &str) -> MemoryFile {
        vfs.open(
            cx,
            Some(Path::new(name)),
            VfsOpenFlags::READWRITE | VfsOpenFlags::CREATE | VfsOpenFlags::WAL,
        )
        .unwrap()
        .0
    }

    fn log(vfs: &MemoryVfs, cx: &Cx, limit: usize) -> Log {
        Log::create(
            cx,
            file(vfs, cx, "objects"),
            file(vfs, cx, "markers"),
            NativeDurabilityLimits {
                max_batch_bytes: limit,
                ..NativeDurabilityLimits::default()
            },
        )
        .unwrap()
    }

    fn symbol(seed: u8, len: usize) -> SymbolRecord {
        let bytes = vec![seed; len];
        SymbolRecord::new(
            ObjectId::derive_from_canonical_bytes(&bytes),
            Oti {
                f: u64::try_from(len).unwrap(),
                al: 1,
                t: u32::try_from(len).unwrap(),
                z: 1,
                n: 1,
            },
            0,
            bytes,
            SymbolRecordFlags::SYSTEMATIC_RUN_START,
        )
    }

    // These fixtures store redundant copies of a complete one-symbol object.
    // This verifies actual payload identity and envelope shape, not RaptorQ.
    // The separately gated test below uses the real authenticated codec.
    fn verify(id: ObjectId, records: Vec<SymbolRecord>) -> Ready<Result<()>> {
        let valid = !records.is_empty()
            && records.iter().all(|record| {
                record.object_id == id
                    && record.esi == 0
                    && record.oti.al == 1
                    && record.oti.z == 1
                    && record.oti.n == 1
                    && record.oti.f == u64::from(record.oti.t)
                    && usize::try_from(record.oti.t).ok() == Some(record.symbol_data.len())
                    && record.verify_integrity()
                    && ObjectId::derive_from_canonical_bytes(&record.symbol_data) == id
            });
        ready(if valid {
            Ok(())
        } else {
            Err(corrupt("invalid one-symbol admission fixture"))
        })
    }

    fn first_marker(a: &SymbolRecord, b: &SymbolRecord) -> CommitMarker {
        CommitMarker::new(CommitSeq::new(1), 100, a.object_id, b.object_id, None)
    }

    #[derive(Debug, PartialEq, Eq)]
    struct Snapshot {
        streams: (Vec<u8>, Vec<u8>),
        index: Vec<(ObjectId, u64, usize)>,
        records: usize,
        symbol_end: u64,
        marker_end: u64,
        tip: CommitSeq,
        previous: Option<ObjectId>,
        time: u64,
    }

    async fn stream_bytes(file: &MemoryFile, cx: &Cx) -> Vec<u8> {
        let mut bytes = vec![0; usize::try_from(file.file_size(cx).unwrap()).unwrap()];
        read_exact_at(file, cx, &mut bytes, 0).await.unwrap();
        bytes
    }

    async fn snapshot(log: &Log, cx: &Cx) -> Snapshot {
        Snapshot {
            streams: (
                stream_bytes(&log.symbols, cx).await,
                stream_bytes(&log.markers, cx).await,
            ),
            index: log
                .index
                .iter()
                .flat_map(|(id, locations)| {
                    locations
                        .iter()
                        .map(|location| (*id, location.offset, location.len))
                })
                .collect(),
            records: log.record_count,
            symbol_end: log.symbol_end,
            marker_end: log.marker_end,
            tip: log.published_tip,
            previous: log.previous_marker,
            time: log.last_time_ns,
        }
    }

    fn assert_ready(log: &Log) {
        assert!(
            !log.needs_recovery(),
            "preflight rejection is not uncertain I/O"
        );
        assert!(log.outstanding_write().is_none());
    }

    #[test]
    fn repeated_appends_cannot_invalidate_an_acknowledged_object() {
        run(async {
            let cx = Cx::new();
            let vfs = MemoryVfs::new();
            let a = symbol(1, 4);
            let b = symbol(2, 4);
            let limit = 3 * a.to_bytes().len();
            let mut owner = log(&vfs, &cx, limit);
            owner
                .append_symbols(&cx, &[a.clone(), a.clone(), b.clone()])
                .await
                .unwrap();
            let first = first_marker(&a, &b);
            owner
                .publish(&cx, std::slice::from_ref(&first), verify)
                .await
                .unwrap();
            owner
                .append_symbols(&cx, std::slice::from_ref(&a))
                .await
                .unwrap();
            assert_eq!(owner.read_object(&cx, a.object_id).await.unwrap().len(), 3);
            let before = snapshot(&owner, &cx).await;

            // The entire batch fits, and the new object's record fits. The
            // later record would nevertheless make the committed A unreadable.
            let fresh = symbol(9, 4);
            assert!(matches!(
                owner.append_symbols(&cx, &[fresh.clone(), a.clone()]).await,
                Err(FrankenError::TooBig)
            ));
            assert_eq!(snapshot(&owner, &cx).await, before);
            assert_ready(&owner);
            assert!(owner.read_object(&cx, fresh.object_id).await.is_err());
            assert_eq!(owner.read_object(&cx, a.object_id).await.unwrap().len(), 3);
            let second = CommitMarker::new(
                CommitSeq::new(2),
                101,
                a.object_id,
                b.object_id,
                Some(ObjectId::derive_from_canonical_bytes(
                    &first.to_record_bytes(),
                )),
            );
            owner.publish(&cx, &[second], verify).await.unwrap();
            owner.close(&cx).unwrap();
            let (mut reopened, report) = Log::recover(
                &cx,
                file(&vfs, &cx, "objects"),
                file(&vfs, &cx, "markers"),
                NativeDurabilityLimits {
                    max_batch_bytes: limit,
                    ..NativeDurabilityLimits::default()
                },
                verify,
            )
            .await
            .unwrap();
            assert_eq!(report.markers.len(), 2);
            assert_eq!(report.symbol_records, 4);
            assert_eq!(
                reopened.read_object(&cx, a.object_id).await.unwrap().len(),
                3
            );
            assert_ready(&reopened);
            reopened.close(&cx).unwrap();
        });
    }

    #[test]
    fn repeated_ids_in_one_batch_share_the_projected_object_total() {
        run(async {
            let cx = Cx::new();
            let vfs = MemoryVfs::new();
            let a = symbol(1, 4);
            let mut owner = log(&vfs, &cx, 3 * a.to_bytes().len());
            owner
                .append_symbols(&cx, std::slice::from_ref(&a))
                .await
                .unwrap();
            let before = snapshot(&owner, &cx).await;
            assert!(matches!(
                owner
                    .append_symbols(&cx, &[a.clone(), a.clone(), a.clone()])
                    .await,
                Err(FrankenError::TooBig)
            ));
            assert_eq!(snapshot(&owner, &cx).await, before);
            assert_ready(&owner);
            // Failed preflight must not spend any of the remaining capacity.
            owner
                .append_symbols(&cx, &[a.clone(), a.clone()])
                .await
                .unwrap();
            assert_eq!(owner.read_object(&cx, a.object_id).await.unwrap().len(), 3);
            owner.close(&cx).unwrap();
        });
    }

    #[test]
    fn independent_objects_have_independent_wire_budgets() {
        run(async {
            let cx = Cx::new();
            let vfs = MemoryVfs::new();
            let a = symbol(1, 4); // 80 wire bytes.
            let b = symbol(2, 44); // 120 wire bytes, not merely 44 payload bytes.
            let c = symbol(3, 64); // 140 wire bytes.
            let mut owner = log(&vfs, &cx, 240);
            for _ in 0..2 {
                owner
                    .append_symbols(&cx, &[a.clone(), b.clone()])
                    .await
                    .unwrap();
            }
            owner
                .append_symbols(&cx, std::slice::from_ref(&c))
                .await
                .unwrap();
            assert!(
                owner.symbol_end > 240,
                "the object budget is not a stream limit"
            );
            let before = snapshot(&owner, &cx).await;
            assert!(matches!(
                owner.append_symbols(&cx, &[b]).await,
                Err(FrankenError::TooBig)
            ));
            assert_eq!(snapshot(&owner, &cx).await, before);
            owner
                .append_symbols(&cx, std::slice::from_ref(&a))
                .await
                .unwrap();
            assert!(matches!(
                owner.append_symbols(&cx, std::slice::from_ref(&c)).await,
                Err(FrankenError::TooBig)
            ));
            assert_eq!(owner.read_object(&cx, a.object_id).await.unwrap().len(), 3);
            assert_eq!(owner.read_object(&cx, c.object_id).await.unwrap().len(), 1);
            assert_ready(&owner);
            owner.close(&cx).unwrap();
        });
    }

    #[test]
    fn recovered_index_counts_survivors_and_enforces_the_same_append_budget() {
        run(async {
            let cx = Cx::new();
            let vfs = MemoryVfs::new();
            let a = symbol(1, 4);
            let b = symbol(2, 4);
            let limit = 3 * a.to_bytes().len();
            let mut owner = log(&vfs, &cx, limit);
            for _ in 0..3 {
                owner
                    .append_symbols(&cx, std::slice::from_ref(&a))
                    .await
                    .unwrap();
            }
            owner
                .append_symbols(&cx, std::slice::from_ref(&b))
                .await
                .unwrap();
            owner
                .publish(&cx, &[first_marker(&a, &b)], verify)
                .await
                .unwrap();
            // A damaged indexed envelope still consumes the current read
            // budget. Only a real recovery scan may omit that erasure.
            owner
                .symbols
                .write(&cx, &[0xFF], u64::try_from(SYMBOL_HEADER_BYTES).unwrap())
                .await
                .unwrap();
            assert!(matches!(
                owner.append_symbols(&cx, std::slice::from_ref(&a)).await,
                Err(FrankenError::TooBig)
            ));
            owner.close(&cx).unwrap();
            let (mut reopened, report) = Log::recover(
                &cx,
                file(&vfs, &cx, "objects"),
                file(&vfs, &cx, "markers"),
                NativeDurabilityLimits {
                    max_batch_bytes: limit,
                    ..NativeDurabilityLimits::default()
                },
                verify,
            )
            .await
            .unwrap();
            assert_eq!(report.erased_symbols, 1);
            assert_eq!(
                reopened.read_object(&cx, a.object_id).await.unwrap().len(),
                2
            );
            reopened
                .append_symbols(&cx, std::slice::from_ref(&a))
                .await
                .unwrap();
            assert_eq!(
                reopened.read_object(&cx, a.object_id).await.unwrap().len(),
                3
            );
            let before = snapshot(&reopened, &cx).await;
            assert!(matches!(
                reopened.append_symbols(&cx, &[a]).await,
                Err(FrankenError::TooBig)
            ));
            assert_eq!(snapshot(&reopened, &cx).await, before);
            assert_ready(&reopened);
            reopened.close(&cx).unwrap();
        });
    }

    #[test]
    fn invalid_records_and_cancelled_admission_do_not_spend_object_capacity() {
        run(async {
            let cx = Cx::new();
            let vfs = MemoryVfs::new();
            let a = symbol(1, 4);
            let mut owner = log(&vfs, &cx, 3 * a.to_bytes().len());
            owner
                .append_symbols(&cx, std::slice::from_ref(&a))
                .await
                .unwrap();
            let before = snapshot(&owner, &cx).await;
            let mut malformed = symbol(2, 4);
            malformed.symbol_data.pop();
            assert!(matches!(
                owner.append_symbols(&cx, &[a.clone(), malformed]).await,
                Err(FrankenError::WalCorrupt { .. })
            ));
            assert_eq!(snapshot(&owner, &cx).await, before);
            let cancelled = Cx::new();
            cancelled.cancel();
            assert!(matches!(
                owner.append_symbols(&cancelled, std::slice::from_ref(&a)).await,
                Err(FrankenError::Interrupt)
            ));
            assert_eq!(snapshot(&owner, &cx).await, before);
            assert_ready(&owner);
            owner
                .append_symbols(&cx, &[a.clone(), a.clone()])
                .await
                .unwrap();
            assert_eq!(owner.read_object(&cx, a.object_id).await.unwrap().len(), 3);
            owner.close(&cx).unwrap();
        });
    }

    #[test]
    fn an_oversized_orphan_cannot_grow_but_does_not_block_unrelated_publication() {
        run(async {
            let cx = Cx::new();
            let vfs = MemoryVfs::new();
            let a = symbol(1, 4);
            let b = symbol(2, 4);
            let limits = NativeDurabilityLimits {
                max_batch_bytes: 3 * a.to_bytes().len(),
                ..NativeDurabilityLimits::default()
            };
            // Real file input that exceeds the object budget, not a fabricated
            // recovery report. With no marker it is an orphan, not a commit.
            let mut objects = file(&vfs, &cx, "objects");
            objects.write(&cx, &a.to_bytes().repeat(4), 0).await.unwrap();
            objects.close(&cx).unwrap();
            let (mut owner, report) = Log::recover(
                &cx,
                file(&vfs, &cx, "objects"),
                file(&vfs, &cx, "markers"),
                limits,
                verify,
            )
            .await
            .unwrap();
            assert!(report.markers.is_empty());
            assert!(matches!(
                owner.read_object(&cx, a.object_id).await,
                Err(FrankenError::TooBig)
            ));
            let before = snapshot(&owner, &cx).await;
            assert!(matches!(
                owner.append_symbols(&cx, &[a]).await,
                Err(FrankenError::TooBig)
            ));
            assert_eq!(snapshot(&owner, &cx).await, before);
            assert_ready(&owner);
            owner
                .append_symbols(&cx, std::slice::from_ref(&b))
                .await
                .unwrap();
            owner
                .publish(&cx, &[first_marker(&b, &b)], verify)
                .await
                .unwrap();
            owner.close(&cx).unwrap();
            let (mut reopened, report) = Log::recover(
                &cx,
                file(&vfs, &cx, "objects"),
                file(&vfs, &cx, "markers"),
                limits,
                verify,
            )
            .await
            .unwrap();
            assert_eq!(report.markers.len(), 1);
            assert_eq!(
                reopened.read_object(&cx, b.object_id).await.unwrap().len(),
                1
            );
            reopened.close(&cx).unwrap();
        });
    }

    #[cfg(feature = "native")]
    #[test]
    fn authenticated_raptorq_chunks_remain_readable_after_excess_redundancy_is_rejected() {
        use crate::native_commit::durable::NativeObjectCodec;
        use crate::native_commit::durable::codec::RaptorQNativeCodec;

        run(async {
            let cx = Cx::new();
            cx.set_native_cx(asupersync::Cx::current().unwrap());
            let vfs = MemoryVfs::new();
            let codec = RaptorQNativeCodec::new(Some([0x2B; 32]));
            let payload: Vec<_> = (0_u32..4097)
                .map(|n| u8::try_from(n % 251).unwrap())
                .collect();
            let records = codec.encode(&cx, &payload).unwrap();
            assert!(records.iter().any(|record| record.esi & (1 << 31) != 0));
            let limit = records.iter().map(|record| record.to_bytes().len()).sum();
            let id = RaptorQNativeCodec::object_id(&payload).unwrap();
            let mut owner = log(&vfs, &cx, limit);
            for chunk in records.chunks(3) {
                owner.append_symbols(&cx, chunk).await.unwrap();
            }
            let marker = CommitMarker::new(CommitSeq::new(1), 100, id, id, None);
            owner
                .publish(&cx, &[marker], |id, symbols| {
                    ready(codec.decode(&cx, id, &symbols).map(|_| ()))
                })
                .await
                .unwrap();
            let before = snapshot(&owner, &cx).await;
            assert!(matches!(
                owner.append_symbols(&cx, &records[..1]).await,
                Err(FrankenError::TooBig)
            ));
            assert_eq!(snapshot(&owner, &cx).await, before);
            assert_ready(&owner);
            let stored = owner.read_object(&cx, id).await.unwrap();
            assert_eq!(codec.decode(&cx, id, &stored).unwrap(), payload);
            assert!(stored.iter().all(|record| record.verify_auth(&[0x2B; 32])));
            owner.close(&cx).unwrap();
            let (mut reopened, report) = Log::recover(
                &cx,
                file(&vfs, &cx, "objects"),
                file(&vfs, &cx, "markers"),
                NativeDurabilityLimits {
                    max_batch_bytes: limit,
                    ..NativeDurabilityLimits::default()
                },
                |id, symbols| ready(codec.decode(&cx, id, &symbols).map(|_| ())),
            )
            .await
            .unwrap();
            assert_eq!(report.markers.len(), 1);
            assert_eq!(report.symbol_records, records.len());
            let stored = reopened.read_object(&cx, id).await.unwrap();
            assert_eq!(codec.decode(&cx, id, &stored).unwrap(), payload);
            reopened.close(&cx).unwrap();
        });
    }
}
