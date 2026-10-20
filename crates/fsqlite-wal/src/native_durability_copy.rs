//! Non-destructive migration of a recovered native prefix into fresh streams.

use super::{
    COMMIT_MARKER_RECORD_V1_SIZE, CommitMarker, CommitSeq, NativeDurabilityLog, ObjectId, Result,
    SymbolRecord, SyncKind, VfsFile, checkpoint, corrupt, ensure_distinct_files, read_exact_at,
    u64_len,
};
use fsqlite_error::FrankenError;
use fsqlite_types::cx::Cx;

use crate::native_commit::durable::NativeObjectCodec;

/// A complete copy of one accepted native commit prefix into different streams.
///
/// This is not a namespace-switch receipt or a cross-file atomic replacement.
/// The caller must durably admit the destination names and explicitly select
/// them only after success. Original streams, including torn tails, survive.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NativePrefixCopyReceipt {
    pub commits: usize,
    pub last_seq: CommitSeq,
    pub symbol_records: usize,
    pub symbol_stream_bytes: u64,
    pub marker_stream_bytes: u64,
}

/// Existing append/publish methods protect their own I/O, but an export can
/// also stop BETWEEN those operations. Keep the destination unavailable on
/// every error, unwind, or dropped future until ordinary recovery verifies it.
struct CopyGuard<'a, S: VfsFile, M: VfsFile> {
    destination: &'a mut NativeDurabilityLog<S, M>,
    complete: bool,
}

impl<S: VfsFile, M: VfsFile> Drop for CopyGuard<'_, S, M> {
    fn drop(&mut self) {
        if !self.complete {
            self.destination.poisoned = true;
        }
    }
}

impl<S: VfsFile, M: VfsFile> NativeDurabilityLog<S, M> {
    /// Copy this accepted commit prefix to a fresh, empty native log.
    ///
    /// A log returned by `recover` may be readable but append-blocked by torn
    /// tails. This operation copies its complete, intact indexed symbols and
    /// its complete marker chain, never those tails. It also preserves intact
    /// orphan objects: a caller's proof format may refer to them transitively.
    /// Checksum-damaged symbols remain erasures, not fabricated repair data.
    /// Each published referent is read from the DESTINATION and passed through
    /// the mandatory verifier before its ordinary two-barrier publication.
    /// The complete accepted marker chain is checked before any copying, then
    /// again after object staging and before publishing its first batch. A
    /// valid checksum on a substituted chain does not authorize its export.
    ///
    /// The destination is appendable on success; its original handles remain
    /// caller-owned on failure. A failed/dropped copy poisons that destination,
    /// including between successful batches. Observe `outstanding_write` and
    /// settle its source-owned completion before close/recovery. Earlier copied
    /// batches can recover as a prefix, but are NOT a successful copy receipt.
    /// The source bytes are never written, truncated, renamed, or deleted.
    ///
    /// Both namespaces and append-owner leases must stay exclusive throughout
    /// the operation and until all abandoned destination writes settle. File
    /// identities reject aliases; they do not acquire those leases. No public
    /// SQL storage selector or root/epoch manifest is changed here. The verifier
    /// must enforce the same identity, decoding, authentication and transitive
    /// evidence contract as `publish`/`recover`; a copy is not SQL/SSI validation.
    ///
    /// Memory is bounded by the source's per-object read limit, the destination's
    /// append limit, and at most 128 markers. Index storage retains the existing
    /// record-count bound. Symbols are not gathered into a whole-stream buffer.
    ///
    /// # Errors
    /// Refuses poisoned/closed sources, nonempty or unavailable destinations,
    /// any of the four handles aliasing, exceeded limits, changed lengths,
    /// malformed markers, invalid referents, cancellation, and storage errors.
    pub async fn copy_recovered_prefix_to<DS, DM, V, VF>(
        &mut self,
        cx: &Cx,
        destination: &mut NativeDurabilityLog<DS, DM>,
        verify_object: V,
    ) -> Result<NativePrefixCopyReceipt>
    where
        DS: VfsFile,
        DM: VfsFile,
        V: FnMut(ObjectId, Vec<SymbolRecord>) -> VF,
        VF: std::future::Future<Output = Result<()>>,
    {
        self.copy_prefix_to_with(cx, destination, |_, records| Ok(records), verify_object)
            .await
    }

    /// Reconstruct and re-encode every indexed object into fresh native streams.
    ///
    /// Unlike `copy_recovered_prefix_to`, this restores the source/repair symbol
    /// set emitted by the caller's codec instead of retaining only survivors.
    /// With `RaptorQNativeCodec` this runs real RaptorQ decoding and encoding;
    /// another codec supplies only its own guarantees. The codec must accept
    /// the original profile and authentication epoch. Re-encoding must preserve
    /// every `ObjectId`, and its decoded bytes must exactly equal the original
    /// reconstruction. No checksum-valid symbol alone establishes authenticity.
    ///
    /// All indexed objects, including orphans, must reconstruct successfully.
    /// Objects with no surviving indexed symbols cannot be invented; a missing
    /// committed referent still fails destination publication. Neither erasure
    /// repair nor a torn tail changes the complete marker chain. The source
    /// bytes remain untouched and any failed destination stays recovery-blocked.
    /// Namespace admission, root switching, external leases, and semantic replay
    /// remain the caller's responsibility, as for `copy_recovered_prefix_to`.
    ///
    /// Work is per object: bounded source symbols, decoded payload, regenerated
    /// symbols and a roundtrip payload, never all decoded objects at once. The
    /// codec must enforce its own allocation limits while decoding/encoding;
    /// destination append limits additionally bound the retained wire records.
    ///
    /// # Errors
    /// In addition to copy admission and I/O errors, refuses insufficient repair
    /// rank, failed authentication, changed identity/profile, excessive payloads,
    /// or an encoder whose output does not decode back to the exact source.
    pub async fn repair_recovered_prefix_to<DS, DM, C>(
        &mut self,
        cx: &Cx,
        destination: &mut NativeDurabilityLog<DS, DM>,
        codec: &C,
    ) -> Result<NativePrefixCopyReceipt>
    where
        DS: VfsFile,
        DM: VfsFile,
        C: NativeObjectCodec,
    {
        let payload_limit = self
            .limits
            .max_batch_bytes
            .min(destination.limits.max_batch_bytes);
        self.copy_prefix_to_with(
            cx,
            destination,
            |id, records| regenerate_object(cx, codec, id, records, payload_limit),
            |id, records| std::future::ready(codec.decode(cx, id, &records).map(|_| ())),
        )
        .await
    }

    // Both transfer modes share alias admission, publication, and the guard
    // retaining destination ownership across transforms, I/O and verification.
    async fn copy_prefix_to_with<DS, DM, T, V, VF>(
        &mut self,
        cx: &Cx,
        destination: &mut NativeDurabilityLog<DS, DM>,
        mut transform: T,
        mut verify_object: V,
    ) -> Result<NativePrefixCopyReceipt>
    where
        DS: VfsFile,
        DM: VfsFile,
        T: FnMut(ObjectId, Vec<SymbolRecord>) -> Result<Vec<SymbolRecord>>,
        V: FnMut(ObjectId, Vec<SymbolRecord>) -> VF,
        VF: std::future::Future<Output = Result<()>>,
    {
        self.readable()?; // Deliberately allow a verified, append-blocked source.
        destination.ready()?;
        checkpoint(cx)?;
        self.check_lengths(cx)?;
        destination.check_lengths(cx)?;
        if destination.symbol_end != 0
            || destination.marker_end != 0
            || destination.record_count != 0
            || !destination.index.is_empty()
            || destination.published_tip != CommitSeq::ZERO
        {
            return Err(corrupt("native prefix copy requires an empty destination"));
        }
        // check_lengths already checked each pair's internal distinctness.
        ensure_distinct_files(&self.symbols, &destination.symbols)?;
        ensure_distinct_files(&self.symbols, &destination.markers)?;
        ensure_distinct_files(&self.markers, &destination.symbols)?;
        ensure_distinct_files(&self.markers, &destination.markers)?;
        let marker_width = u64_len(COMMIT_MARKER_RECORD_V1_SIZE)?;
        let marker_count = self.marker_end / marker_width;
        if marker_count != self.published_tip.get() {
            return Err(corrupt(
                "native prefix copy has an unaccepted marker boundary",
            ));
        }
        // The entire marker prefix must fit, not just the first publication
        // batch. Exclude retained source tails; they are never exported.
        let marker_bytes = marker_count
            .checked_mul(marker_width)
            .ok_or(FrankenError::TooBig)?;
        if marker_bytes > destination.limits.max_stream_bytes {
            return Err(FrankenError::TooBig);
        }
        let marker_count = usize::try_from(marker_count).map_err(|_| FrankenError::TooBig)?;
        let capacity = marker_count
            .min(128)
            .min(destination.limits.max_batch_bytes / COMMIT_MARKER_RECORD_V1_SIZE);
        if (marker_count != 0 && capacity == 0)
            || self.record_count > destination.limits.max_records
            || marker_count > destination.limits.max_records
        {
            return Err(FrankenError::TooBig);
        }
        let mut batch = Vec::new();
        batch
            .try_reserve_exact(capacity)
            .map_err(|_| FrankenError::OutOfMemory)?;
        // Reuse the bounded, full-chain validator rather than discovering a
        // late malformed or replaced marker after publishing earlier batches.
        // With limit zero it retains no marker vector. A preflight refusal
        // leaves the empty destination healthy and reusable.
        self.replication_batch(cx, CommitSeq::ZERO, None, 0).await?;
        let mut copy = CopyGuard {
            destination,
            complete: false,
        };
        for &object_id in self.index.keys() {
            checkpoint(cx)?;
            let records = self.read_object(cx, object_id).await?;
            let records = transform(object_id, records)?;
            checkpoint(cx)?;
            copy.destination.append_symbols(cx, &records).await?;
        }
        // Object reads, codec calls and destination writes can yield. A source
        // change during staging must not become a published destination prefix.
        // Once staging starts, the guard still retains recovery obligations.
        self.replication_batch(cx, CommitSeq::ZERO, None, 0).await?;
        for ordinal in 0..marker_count {
            checkpoint(cx)?;
            let offset = u64_len(ordinal)?
                .checked_mul(marker_width)
                .ok_or(FrankenError::TooBig)?;
            let mut wire = [0; COMMIT_MARKER_RECORD_V1_SIZE];
            read_exact_at(&self.markers, cx, &mut wire, offset).await?;
            batch.push(
                CommitMarker::from_record_bytes(&wire)
                    .ok_or_else(|| corrupt("malformed marker during native prefix copy"))?,
            );
            if batch.len() == capacity || ordinal + 1 == marker_count {
                copy.destination
                    .publish(cx, &batch, &mut verify_object)
                    .await?;
                batch.clear();
            }
        }
        if marker_count == 0 {
            // publish([]) is intentionally a no-op. Still persist copied
            // orphan objects and the empty destination for this copy receipt.
            copy.destination.poisoned = true;
            copy.destination
                .symbols
                .durable_sync(cx, SyncKind::FullDurable)?;
            copy.destination
                .markers
                .durable_sync(cx, SyncKind::FullDurable)?;
            copy.destination.poisoned = false;
        }
        checkpoint(cx)?;
        self.check_lengths(cx)?;
        copy.destination.check_lengths(cx)?;
        if copy.destination.published_tip != self.published_tip
            || copy.destination.previous_marker != self.previous_marker
            || copy.destination.last_time_ns != self.last_time_ns
        {
            return Err(corrupt(
                "native prefix copy changed the accepted marker chain",
            ));
        }
        let receipt = NativePrefixCopyReceipt {
            commits: marker_count,
            last_seq: copy.destination.published_tip,
            symbol_records: copy.destination.record_count,
            symbol_stream_bytes: copy.destination.symbol_end,
            marker_stream_bytes: copy.destination.marker_end,
        };
        copy.complete = true;
        Ok(receipt)
    }
}

fn regenerate_object<C: NativeObjectCodec>(
    cx: &Cx,
    codec: &C,
    id: ObjectId,
    records: Vec<SymbolRecord>,
    payload_limit: usize,
) -> Result<Vec<SymbolRecord>> {
    if records
        .iter()
        .any(|record| usize::try_from(record.oti.f).map_or(true, |len| len > payload_limit))
    {
        return Err(FrankenError::TooBig);
    }
    let payload = codec.decode(cx, id, &records)?;
    drop(records);
    if payload.is_empty() || payload.len() > payload_limit {
        return Err(FrankenError::TooBig);
    }
    checkpoint(cx)?;
    let regenerated = codec.encode(cx, &payload)?;
    if regenerated.is_empty() || regenerated.iter().any(|record| record.object_id != id) {
        return Err(corrupt("native repair encoder changed the object identity"));
    }
    checkpoint(cx)?;
    if codec.decode(cx, id, &regenerated)? != payload {
        return Err(corrupt("native repair encoder changed the decoded object"));
    }
    Ok(regenerated)
}

impl<S: VfsFile, M: VfsFile> NativeDurabilityLog<S, M> {
    /// Advance a native replica by at most `max_commits` accepted commits.
    ///
    /// Unlike the fresh-copy APIs, the destination may already contain a
    /// prefix of this source. Both complete marker chains are read and checked
    /// against their accepted tips before any destination write. An ahead or
    /// forked destination is refused, never overwritten. An identical tip is a
    /// read-only no-op returning `None`, not a new durability acknowledgement.
    ///
    /// Missing indexed symbols are staged once. This includes orphan objects
    /// and objects for later commits: arbitrary proof formats can refer to them
    /// transitively. Existing envelopes are compared by object/OTI/ESI and
    /// reused, including a partly staged object from an earlier interrupted
    /// append. Conflicting envelopes are rejected, not overwritten. Publication
    /// re-reads its referents from the destination and calls `verify_object`.
    /// That verifier retains the full decoding, identity, authentication and
    /// transitive-dependency contract of `publish`. Merely copying envelopes
    /// is neither semantic/SSI validation nor proof that a replica may serve SQL.
    ///
    /// `max_commits` must be in 1..=128. It bounds one marker batch, not bytes
    /// transferred: each call scans both marker prefixes and source objects,
    /// compares existing destination symbols, and can stage all source objects.
    /// Existing per-object, stream and record limits bound allocation/storage;
    /// no whole-stream
    /// payload buffer is constructed. The selected suffix shares exactly the
    /// ordinary two publication barriers and preserves every marker byte.
    ///
    /// A failed or dropped transfer keeps the destination recovery-blocked,
    /// even between completed object writes. Retain its outstanding write token
    /// and use original-handle recovery after settlement before retrying. An
    /// uncertain marker write may recover a shorter committed prefix; a fresh
    /// call resumes from that verified tip and reuses surviving staged objects.
    /// Source tails are preserved and excluded; destination tails must first
    /// be reconciled and never authorize an append through incomplete bytes.
    ///
    /// Both external namespace/append leases must cover this entire operation
    /// and any abandoned destination writes. This acquires no transaction-wide
    /// lock, opens/closes no files, changes no root, and supplies no network or
    /// quorum protocol. Source bytes are never changed.
    ///
    /// # Errors
    /// Rejects invalid limits, aliases, ahead/divergent/changed histories,
    /// unavailable storage, invalid referents, cancellation and VFS errors.
    pub async fn replicate_committed_prefix_to<DS, DM, V, VF>(
        &mut self,
        cx: &Cx,
        destination: &mut NativeDurabilityLog<DS, DM>,
        max_commits: usize,
        mut verify_object: V,
    ) -> Result<Option<super::NativeDurabilityReceipt>>
    where
        DS: VfsFile,
        DM: VfsFile,
        V: FnMut(ObjectId, Vec<SymbolRecord>) -> VF,
        VF: std::future::Future<Output = Result<()>>,
    {
        self.readable()?;
        destination.ready()?;
        checkpoint(cx)?;
        if !(1..=128).contains(&max_commits) {
            return Err(FrankenError::OutOfRange {
                what: "native replication commit batch".to_owned(),
                value: max_commits.to_string(),
            });
        }
        self.check_lengths(cx)?;
        destination.check_lengths(cx)?;
        ensure_distinct_files(&self.symbols, &destination.symbols)?;
        ensure_distinct_files(&self.symbols, &destination.markers)?;
        ensure_distinct_files(&self.markers, &destination.symbols)?;
        ensure_distinct_files(&self.markers, &destination.markers)?;
        let base = destination.published_tip;
        let anchor = destination.previous_marker;
        destination.replication_batch(cx, base, anchor, 0).await?;
        let batch = self
            .replication_batch(cx, base, anchor, max_commits)
            .await?;
        if batch.is_empty() {
            return Ok(None);
        }
        let marker_bytes = batch
            .len()
            .checked_mul(COMMIT_MARKER_RECORD_V1_SIZE)
            .ok_or(FrankenError::TooBig)?;
        let marker_end = destination
            .marker_end
            .checked_add(u64_len(marker_bytes)?)
            .ok_or(FrankenError::TooBig)?;
        if marker_bytes > destination.limits.max_batch_bytes
            || marker_end > destination.limits.max_stream_bytes
            || marker_end / u64_len(COMMIT_MARKER_RECORD_V1_SIZE)?
                > u64_len(destination.limits.max_records)?
        {
            return Err(FrankenError::TooBig);
        }
        let mut copy = CopyGuard {
            destination,
            complete: false,
        };
        for &id in self.index.keys() {
            checkpoint(cx)?;
            let incoming = self.read_object(cx, id).await?;
            let existing = if copy.destination.index.contains_key(&id) {
                copy.destination.read_object(cx, id).await?
            } else {
                Vec::new()
            };
            let records = missing_replication_symbols(id, incoming, existing)?;
            if !records.is_empty() {
                // append_symbols bounds a batch, but an object's earlier staged
                // records also consume its eventual read/verification budget.
                let old_bytes =
                    copy.destination
                        .index
                        .get(&id)
                        .map_or(Ok(0_usize), |locations| {
                            locations.iter().try_fold(0_usize, |sum, location| {
                                sum.checked_add(location.len).ok_or(FrankenError::TooBig)
                            })
                        })?;
                let object_bytes = records.iter().try_fold(old_bytes, |sum, record| {
                    sum.checked_add(record.symbol_data.len())
                        .and_then(|n| {
                            n.checked_add(super::SYMBOL_HEADER_BYTES + super::SYMBOL_TRAILER_BYTES)
                        })
                        .ok_or(FrankenError::TooBig)
                })?;
                if object_bytes > copy.destination.limits.max_batch_bytes {
                    return Err(FrankenError::TooBig);
                }
                copy.destination.append_symbols(cx, &records).await?;
            }
        }
        // Reads and object staging can yield. Recheck both histories before
        // publishing, not just after irreversibly appending another marker.
        self.replication_batch(cx, base, anchor, 0).await?;
        copy.destination
            .replication_batch(cx, base, anchor, 0)
            .await?;
        checkpoint(cx)?;
        let receipt = copy
            .destination
            .publish(cx, &batch, &mut verify_object)
            .await?;
        // publish already checked this nonempty batch and completed FSYNC_2.
        // No late cancellation check may revoke its durability acknowledgement.
        copy.complete = true;
        Ok(receipt)
    }

    // Verify the entire accepted on-disk chain using one fixed record buffer.
    // Collect only the bounded suffix selected for this call. The final digest
    // binds even later, unselected markers to the source's accepted history.
    async fn replication_batch(
        &mut self,
        cx: &Cx,
        base: CommitSeq,
        anchor: Option<ObjectId>,
        limit: usize,
    ) -> Result<Vec<CommitMarker>> {
        self.check_lengths(cx)?;
        let width = u64_len(COMMIT_MARKER_RECORD_V1_SIZE)?;
        let count = self.marker_end / width;
        if count != self.published_tip.get() {
            self.poisoned = true;
            return Err(corrupt(
                "native replication has an unaccepted marker boundary",
            ));
        }
        if base > self.published_tip {
            return Err(corrupt(
                "native replica is ahead of the accepted source prefix",
            ));
        }
        if base == CommitSeq::ZERO && anchor.is_some() {
            return Err(corrupt("invalid native replication genesis anchor"));
        }
        let capacity = usize::try_from((count - base.get()).min(u64_len(limit)?))
            .map_err(|_| FrankenError::TooBig)?;
        let mut batch = Vec::new();
        batch
            .try_reserve_exact(capacity)
            .map_err(|_| FrankenError::OutOfMemory)?;
        let mut tip = CommitSeq::ZERO;
        let mut previous = None;
        let mut time = 0;
        for ordinal in 0..count {
            checkpoint(cx)?;
            let offset = ordinal.checked_mul(width).ok_or(FrankenError::TooBig)?;
            let mut wire = [0; COMMIT_MARKER_RECORD_V1_SIZE];
            read_exact_at(&self.markers, cx, &mut wire, offset).await?;
            let Some(marker) = CommitMarker::from_record_bytes(&wire) else {
                self.poisoned = true;
                return Err(corrupt("malformed native replication marker"));
            };
            if let Err(error) = super::validate_marker(&marker, tip, previous, time) {
                self.poisoned = true;
                return Err(error);
            }
            tip = marker.commit_seq;
            time = marker.commit_time_unix_ns;
            previous = Some(ObjectId::derive_from_canonical_bytes(&wire));
            if tip == base && previous != anchor {
                if base == self.published_tip && anchor == self.previous_marker {
                    self.poisoned = true;
                }
                return Err(corrupt("native replication marker histories diverge"));
            }
            if tip > base && batch.len() < capacity {
                batch.push(marker);
            }
        }
        if tip != self.published_tip
            || previous != self.previous_marker
            || time != self.last_time_ns
        {
            self.poisoned = true;
            return Err(corrupt(
                "native replication changed the accepted marker chain",
            ));
        }
        self.check_lengths(cx)?;
        Ok(batch)
    }
}

// Merge exact envelopes, not merely object-index presence: a torn multi-symbol
// append can leave a complete but undecodable subset indexed after recovery.
fn missing_replication_symbols(
    id: ObjectId,
    mut incoming: Vec<SymbolRecord>,
    mut existing: Vec<SymbolRecord>,
) -> Result<Vec<SymbolRecord>> {
    let oti = incoming
        .first()
        .ok_or_else(|| corrupt("empty native replication object"))?
        .oti;
    for records in [&mut incoming, &mut existing] {
        if records
            .iter()
            .any(|record| record.object_id != id || record.oti != oti)
        {
            return Err(corrupt(
                "native replication object identity or OTI mismatch",
            ));
        }
        records.sort_unstable_by_key(|record| record.esi);
        if records
            .windows(2)
            .any(|pair| pair[0].esi == pair[1].esi && pair[0] != pair[1])
        {
            return Err(corrupt("conflicting duplicate native replication symbol"));
        }
        records.dedup();
    }
    let mut missing = Vec::new();
    missing
        .try_reserve_exact(incoming.len())
        .map_err(|_| FrankenError::OutOfMemory)?;
    for record in incoming {
        match existing.binary_search_by_key(&record.esi, |symbol| symbol.esi) {
            Ok(index) if existing[index] != record => {
                return Err(corrupt(
                    "native replication symbol differs from staged envelope",
                ));
            }
            Ok(_) => {}
            Err(_) => missing.push(record),
        }
    }
    Ok(missing)
}

#[cfg(all(test, not(target_arch = "wasm32")))]
mod replication_tests {
    use std::future::{Future, Ready, pending, ready};
    use std::path::Path;
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::task::{Context, Waker};

    use asupersync::runtime::RuntimeBuilder;
    use fsqlite_types::flags::VfsOpenFlags;
    use fsqlite_types::{Oti, SymbolRecordFlags, reconstruct_systematic_happy_path};
    use fsqlite_vfs::{MemoryVfs, Vfs};

    use super::*;
    use crate::native_durability::NativeDurabilityLimits;

    type File = <MemoryVfs as Vfs>::File;
    type Log = NativeDurabilityLog<File, File>;

    fn run(future: impl Future<Output = ()>) {
        RuntimeBuilder::current_thread()
            .blocking_threads(1, 2)
            .build()
            .unwrap()
            .block_on(future);
    }

    fn file(vfs: &MemoryVfs, cx: &Cx, name: &str) -> File {
        vfs.open(
            cx,
            Some(Path::new(name)),
            VfsOpenFlags::READWRITE | VfsOpenFlags::CREATE | VfsOpenFlags::WAL,
        )
        .unwrap()
        .0
    }

    fn log(vfs: &MemoryVfs, cx: &Cx, name: &str) -> Log {
        NativeDurabilityLog::create(
            cx,
            file(vfs, cx, &format!("{name}-objects")),
            file(vfs, cx, &format!("{name}-markers")),
            NativeDurabilityLimits::default(),
        )
        .unwrap()
    }

    fn symbol(value: u64) -> SymbolRecord {
        let bytes = value.to_le_bytes().to_vec();
        SymbolRecord::new(
            ObjectId::derive_from_canonical_bytes(&bytes),
            Oti {
                f: 8,
                al: 1,
                t: 8,
                z: 1,
                n: 1,
            },
            0,
            bytes,
            SymbolRecordFlags::SYSTEMATIC_RUN_START,
        )
    }

    fn verify(id: ObjectId, records: Vec<SymbolRecord>) -> Ready<Result<()>> {
        ready(
            reconstruct_systematic_happy_path(&records)
                .map_err(|error| corrupt(&error.to_string()))
                .and_then(|bytes| {
                    if ObjectId::derive_from_canonical_bytes(&bytes) == id {
                        Ok(())
                    } else {
                        Err(corrupt("replication test object identity mismatch"))
                    }
                }),
        )
    }

    async fn append(log: &mut Log, cx: &Cx, time: u64) {
        let seq = log.published_tip.get() + 1;
        let records = [symbol(seq * 2), symbol(seq * 2 + 1)];
        let marker = CommitMarker::new(
            CommitSeq::new(seq),
            time,
            records[0].object_id,
            records[1].object_id,
            log.previous_marker,
        );
        log.append_symbols(cx, &records).await.unwrap();
        log.publish(cx, &[marker], verify).await.unwrap();
    }

    async fn bytes(file: &File, cx: &Cx) -> Vec<u8> {
        let mut bytes = vec![0; usize::try_from(file.file_size(cx).unwrap()).unwrap()];
        read_exact_at(file, cx, &mut bytes, 0).await.unwrap();
        bytes
    }

    async fn recover(log: Log, cx: &Cx) -> Log {
        let mut recovery = log.into_recovery();
        recovery.prepare(cx, verify).await.unwrap();
        recovery.finish().unwrap().into_log().0
    }

    #[test]
    fn replication_advances_existing_prefix_in_bounded_batches_without_duplicate_symbols() {
        run(async {
            let cx = Cx::new();
            let vfs = MemoryVfs::new();
            let mut source = log(&vfs, &cx, "source");
            let mut replica = log(&vfs, &cx, "replica");
            append(&mut source, &cx, 1).await;
            source
                .replicate_committed_prefix_to(&cx, &mut replica, 128, verify)
                .await
                .unwrap()
                .unwrap();
            for time in 2..=258 {
                append(&mut source, &cx, time).await;
            }
            // An intact orphan may be an application's transitive dependency.
            source.append_symbols(&cx, &[symbol(9999)]).await.unwrap();
            let original = (
                bytes(&source.symbols, &cx).await,
                bytes(&source.markers, &cx).await,
            );
            for (first, last, count) in [(2, 129, 128), (130, 257, 128), (258, 258, 1)] {
                let receipt = source
                    .replicate_committed_prefix_to(&cx, &mut replica, 128, verify)
                    .await
                    .unwrap()
                    .unwrap();
                assert_eq!(receipt.first_seq, CommitSeq::new(first));
                assert_eq!(receipt.last_seq, CommitSeq::new(last));
                assert_eq!(receipt.commits, count);
                assert_eq!(replica.record_count, source.record_count);
                assert_eq!(replica.symbol_end, source.symbol_end);
            }
            let copied = bytes(&replica.symbols, &cx).await;
            assert!(
                source
                    .replicate_committed_prefix_to(&cx, &mut replica, 1, verify)
                    .await
                    .unwrap()
                    .is_none()
            );
            assert_eq!(bytes(&replica.symbols, &cx).await, copied);
            assert_eq!(bytes(&replica.markers, &cx).await, original.1);
            assert_eq!(bytes(&source.symbols, &cx).await, original.0);
            assert_eq!(bytes(&source.markers, &cx).await, original.1);
            verify(
                symbol(9999).object_id,
                replica
                    .read_object(&cx, symbol(9999).object_id)
                    .await
                    .unwrap(),
            )
            .await
            .unwrap();
            source.close(&cx).unwrap();
            replica.close(&cx).unwrap();
        });
    }

    #[test]
    fn replication_rejects_ahead_forked_and_replaced_histories_before_staging() {
        run(async {
            for case in 0..3 {
                let cx = Cx::new();
                let vfs = MemoryVfs::new();
                let mut source = log(&vfs, &cx, "source");
                let mut replica = log(&vfs, &cx, "replica");
                append(&mut source, &cx, 1).await;
                append(&mut replica, &cx, if case == 1 { 2 } else { 1 }).await;
                if case == 0 {
                    append(&mut replica, &cx, 3).await;
                } else {
                    append(&mut source, &cx, 3).await;
                }
                if case == 2 {
                    // Same length and valid checksum, but not the accepted chain.
                    let replacement = CommitMarker::new(
                        CommitSeq::new(2),
                        4,
                        symbol(4).object_id,
                        symbol(5).object_id,
                        replica.previous_marker,
                    );
                    source
                        .markers
                        .write(
                            &cx,
                            &replacement.to_record_bytes(),
                            u64_len(COMMIT_MARKER_RECORD_V1_SIZE).unwrap(),
                        )
                        .await
                        .unwrap();
                }
                let before = (
                    bytes(&replica.symbols, &cx).await,
                    bytes(&replica.markers, &cx).await,
                );
                assert!(matches!(
                    source
                        .replicate_committed_prefix_to(&cx, &mut replica, 128, verify)
                        .await,
                    Err(FrankenError::WalCorrupt { .. })
                ));
                assert_eq!(bytes(&replica.symbols, &cx).await, before.0);
                assert_eq!(bytes(&replica.markers, &cx).await, before.1);
                assert!(!replica.needs_recovery());
                source.close(&cx).unwrap();
                replica.close(&cx).unwrap();
            }
        });
    }

    #[test]
    fn replication_preserves_source_tails_but_never_appends_through_destination_tails() {
        run(async {
            let cx = Cx::new();
            let vfs = MemoryVfs::new();
            let mut source = log(&vfs, &cx, "source");
            append(&mut source, &cx, 1).await;
            source
                .symbols
                .write(&cx, b"torn", source.symbol_end)
                .await
                .unwrap();
            source
                .markers
                .write(&cx, &[0xAA; 17], source.marker_end)
                .await
                .unwrap();
            source = recover(source, &cx).await;
            assert!(source.needs_recovery());
            let original = (
                bytes(&source.symbols, &cx).await,
                bytes(&source.markers, &cx).await,
            );
            let mut replica = log(&vfs, &cx, "replica");
            source
                .replicate_committed_prefix_to(&cx, &mut replica, 1, verify)
                .await
                .unwrap()
                .unwrap();
            assert!(!replica.needs_recovery());
            assert_eq!(
                replica.marker_end,
                u64_len(COMMIT_MARKER_RECORD_V1_SIZE).unwrap()
            );
            assert_eq!(replica.symbol_end + 4, source.symbol_end);
            assert_eq!(bytes(&source.symbols, &cx).await, original.0);
            assert_eq!(bytes(&source.markers, &cx).await, original.1);
            replica
                .markers
                .write(&cx, b"tail", replica.marker_end)
                .await
                .unwrap();
            replica = recover(replica, &cx).await;
            let before = bytes(&replica.markers, &cx).await;
            assert!(matches!(
                source
                    .replicate_committed_prefix_to(&cx, &mut replica, 1, verify)
                    .await,
                Err(FrankenError::BusyRecovery)
            ));
            assert_eq!(bytes(&replica.markers, &cx).await, before);
            source.close(&cx).unwrap();
            replica.close(&cx).unwrap();
        });
    }

    #[test]
    fn abandoned_replication_retains_staged_objects_and_resumes_only_after_recovery() {
        run(async {
            let cx = Cx::new();
            let vfs = MemoryVfs::new();
            let mut source = log(&vfs, &cx, "source");
            append(&mut source, &cx, 1).await;
            let mut replica = log(&vfs, &cx, "replica");
            let mut transfer = Box::pin(source.replicate_committed_prefix_to(
                &cx,
                &mut replica,
                1,
                |_, _| pending::<Result<()>>(),
            ));
            assert!(
                transfer
                    .as_mut()
                    .poll(&mut Context::from_waker(Waker::noop()))
                    .is_pending()
            );
            drop(transfer);
            assert!(replica.needs_recovery());
            assert_eq!(replica.published_tip(), CommitSeq::ZERO);
            assert_eq!(replica.markers.file_size(&cx).unwrap(), 0);
            let staged = bytes(&replica.symbols, &cx).await;
            assert!(!staged.is_empty());
            assert!(
                source
                    .replicate_committed_prefix_to(&cx, &mut replica, 1, verify)
                    .await
                    .is_err()
            );
            replica = recover(replica, &cx).await;
            let receipt = source
                .replicate_committed_prefix_to(&cx, &mut replica, 1, verify)
                .await
                .unwrap()
                .unwrap();
            assert_eq!(receipt.commits, 1);
            assert_eq!(bytes(&replica.symbols, &cx).await, staged);
            assert_eq!(
                bytes(&replica.markers, &cx).await,
                bytes(&source.markers, &cx).await
            );
            source.close(&cx).unwrap();
            replica.close(&cx).unwrap();
        });
    }

    #[test]
    fn replication_completes_partly_staged_objects_and_budgets_the_combined_symbols() {
        run(async {
            for budget in [128, 4096] {
                let cx = Cx::new();
                let vfs = MemoryVfs::new();
                let mut source = log(&vfs, &cx, "source");
                let mut replica = log(&vfs, &cx, "replica");
                let payload = [0x5A; 16];
                let id = ObjectId::derive_from_canonical_bytes(&payload);
                let oti = Oti {
                    f: 16,
                    al: 1,
                    t: 8,
                    z: 1,
                    n: 1,
                };
                let records = [
                    SymbolRecord::new(
                        id,
                        oti,
                        0,
                        payload[..8].to_vec(),
                        SymbolRecordFlags::SYSTEMATIC_RUN_START,
                    ),
                    SymbolRecord::new(
                        id,
                        oti,
                        1,
                        payload[8..].to_vec(),
                        SymbolRecordFlags::empty(),
                    ),
                ];
                source.append_symbols(&cx, &records).await.unwrap();
                source
                    .publish(
                        &cx,
                        &[CommitMarker::new(CommitSeq::new(1), 1, id, id, None)],
                        verify,
                    )
                    .await
                    .unwrap();
                replica.append_symbols(&cx, &records[..1]).await.unwrap();
                replica = recover(replica, &cx).await;
                let before = bytes(&replica.symbols, &cx).await;
                replica.limits.max_batch_bytes = budget;
                let result = source
                    .replicate_committed_prefix_to(&cx, &mut replica, 1, verify)
                    .await;
                if budget == 128 {
                    assert!(matches!(result, Err(FrankenError::TooBig)));
                    assert_eq!(bytes(&replica.symbols, &cx).await, before);
                    assert_eq!(replica.markers.file_size(&cx).unwrap(), 0);
                } else {
                    assert_eq!(result.unwrap().unwrap().last_seq, CommitSeq::new(1));
                    assert_eq!(
                        replica.record_count, 2,
                        "do not duplicate the retained first symbol"
                    );
                    let complete = replica.read_object(&cx, id).await.unwrap();
                    assert_eq!(
                        reconstruct_systematic_happy_path(&complete).unwrap(),
                        payload
                    );
                    assert!(bytes(&replica.symbols, &cx).await.starts_with(&before));
                }
                source.close(&cx).unwrap();
                replica.close(&cx).unwrap();
            }
        });
    }

    #[test]
    fn replication_rejects_conflicting_staged_envelopes_instead_of_overwriting_them() {
        run(async {
            let cx = Cx::new();
            let vfs = MemoryVfs::new();
            let mut source = log(&vfs, &cx, "source");
            let mut replica = log(&vfs, &cx, "replica");
            let original = symbol(12);
            let changed = SymbolRecord::new(
                original.object_id,
                original.oti,
                original.esi,
                vec![0xFF; 8],
                SymbolRecordFlags::SYSTEMATIC_RUN_START,
            );
            source
                .append_symbols(&cx, std::slice::from_ref(&original))
                .await
                .unwrap();
            source
                .publish(
                    &cx,
                    &[CommitMarker::new(
                        CommitSeq::new(1),
                        1,
                        original.object_id,
                        original.object_id,
                        None,
                    )],
                    verify,
                )
                .await
                .unwrap();
            replica.append_symbols(&cx, &[changed]).await.unwrap();
            let before = bytes(&replica.symbols, &cx).await;
            assert!(matches!(
                source
                    .replicate_committed_prefix_to(&cx, &mut replica, 1, verify)
                    .await,
                Err(FrankenError::WalCorrupt { .. })
            ));
            assert_eq!(bytes(&replica.symbols, &cx).await, before);
            assert_eq!(replica.markers.file_size(&cx).unwrap(), 0);
            assert!(replica.needs_recovery());
            source.close(&cx).unwrap();
            replica.close(&cx).unwrap();
        });
    }

    #[test]
    fn replication_limits_aliases_and_verifier_rejection_never_acknowledge() {
        run(async {
            let cx = Cx::new();
            let vfs = MemoryVfs::new();
            let mut source = log(&vfs, &cx, "source");
            append(&mut source, &cx, 1).await;
            let mut replica = log(&vfs, &cx, "replica");
            for limit in [0, 129, usize::MAX] {
                assert!(matches!(
                    source
                        .replicate_committed_prefix_to(&cx, &mut replica, limit, verify)
                        .await,
                    Err(FrankenError::OutOfRange { .. })
                ));
            }
            replica.limits.max_stream_bytes = 1;
            assert!(matches!(
                source
                    .replicate_committed_prefix_to(&cx, &mut replica, 1, verify)
                    .await,
                Err(FrankenError::TooBig)
            ));
            assert_eq!(replica.symbols.file_size(&cx).unwrap(), 0);
            replica.limits = NativeDurabilityLimits::default();
            // Independently opened descriptors can still alias physical files.
            let (mut alias, _) = NativeDurabilityLog::recover(
                &cx,
                file(&vfs, &cx, "source-objects"),
                file(&vfs, &cx, "source-markers"),
                NativeDurabilityLimits::default(),
                verify,
            )
            .await
            .unwrap();
            assert!(
                source
                    .replicate_committed_prefix_to(&cx, &mut alias, 1, verify)
                    .await
                    .is_err()
            );
            alias.close(&cx).unwrap();
            assert!(matches!(
                source
                    .replicate_committed_prefix_to(&cx, &mut replica, 1, |_, _| ready(Err(
                        FrankenError::Abort
                    )))
                    .await,
                Err(FrankenError::Abort)
            ));
            assert!(replica.needs_recovery());
            assert_eq!(replica.markers.file_size(&cx).unwrap(), 0);
            assert_eq!(replica.published_tip(), CommitSeq::ZERO);
            source.close(&cx).unwrap();
            replica.close(&cx).unwrap();
        });
    }

    // This one-symbol codec isolates copy admission. It is not a substitute
    // for the authenticated RaptorQ repair tests in native_prefix_copy.
    #[derive(Default)]
    struct CopyCodec {
        calls: AtomicUsize,
        shorten_markers: Mutex<Option<(File, u64)>>,
    }

    impl NativeObjectCodec for CopyCodec {
        fn encode(&self, cx: &Cx, payload: &[u8]) -> Result<Vec<SymbolRecord>> {
            self.calls.fetch_add(1, Ordering::Relaxed);
            let shorten = self.shorten_markers.lock().unwrap().take();
            if let Some((mut file, len)) = shorten {
                // Adversarial external change while the repair codec runs.
                // Only this sacrificial MemoryVfs fixture mutates the source.
                file.truncate(cx, len)?;
                file.close(cx)?;
            }
            let size = u32::try_from(payload.len()).map_err(|_| FrankenError::TooBig)?;
            Ok(vec![SymbolRecord::new(
                ObjectId::derive_from_canonical_bytes(payload),
                Oti {
                    f: u64::from(size),
                    al: 1,
                    t: size,
                    z: 1,
                    n: 1,
                },
                0,
                payload.to_vec(),
                SymbolRecordFlags::SYSTEMATIC_RUN_START,
            )])
        }

        fn decode(&self, _: &Cx, id: ObjectId, records: &[SymbolRecord]) -> Result<Vec<u8>> {
            self.calls.fetch_add(1, Ordering::Relaxed);
            let payload = reconstruct_systematic_happy_path(records)
                .map_err(|error| corrupt(&error.to_string()))?;
            if ObjectId::derive_from_canonical_bytes(&payload) != id {
                return Err(corrupt("copy admission fixture object identity mismatch"));
            }
            Ok(payload)
        }
    }

    async fn copy_mode(
        source: &mut Log,
        cx: &Cx,
        destination: &mut Log,
        codec: &CopyCodec,
        repair: bool,
    ) -> Result<NativePrefixCopyReceipt> {
        if repair {
            source
                .repair_recovered_prefix_to(cx, destination, codec)
                .await
        } else {
            source
                .copy_recovered_prefix_to(cx, destination, verify)
                .await
        }
    }

    async fn shared_object_chain(source: &mut Log, cx: &Cx, count: u64) {
        let record = symbol(777);
        let id = record.object_id;
        source.append_symbols(cx, &[record]).await.unwrap();
        for seq in 1..=count {
            let marker =
                CommitMarker::new(CommitSeq::new(seq), seq, id, id, source.previous_marker);
            source.publish(cx, &[marker], verify).await.unwrap();
        }
    }

    fn assert_copy_destination_unused(destination: &Log, cx: &Cx) {
        assert_eq!(destination.symbols.file_size(cx).unwrap(), 0);
        assert_eq!(destination.markers.file_size(cx).unwrap(), 0);
        assert_eq!(destination.record_count, 0);
        assert_eq!(destination.symbol_end, 0);
        assert_eq!(destination.marker_end, 0);
        assert!(destination.index.is_empty());
        assert_eq!(destination.published_tip(), CommitSeq::ZERO);
        assert!(destination.previous_marker.is_none());
        assert!(destination.outstanding_write().is_none());
        assert!(!destination.needs_recovery());
    }

    #[test]
    fn fresh_copy_rejects_corrupt_or_substituted_late_markers_before_any_staging() {
        run(async {
            for repair in [false, true] {
                for damage in 0..3 {
                    let cx = Cx::new();
                    let vfs = MemoryVfs::new();
                    let mut source = log(&vfs, &cx, "source");
                    shared_object_chain(&mut source, &cx, 129).await;
                    let original = bytes(&source.markers, &cx).await;
                    let offset = original.len() - COMMIT_MARKER_RECORD_V1_SIZE;
                    let last_bytes: &[u8; COMMIT_MARKER_RECORD_V1_SIZE] =
                        original[offset..].try_into().unwrap();
                    let last = CommitMarker::from_record_bytes(last_bytes).unwrap();
                    let mut changed = match damage {
                        0 => CommitMarker::new(
                            last.commit_seq,
                            999,
                            last.capsule_object_id,
                            last.proof_object_id,
                            last.prev_marker,
                        )
                        .to_record_bytes(),
                        1 => CommitMarker::new(
                            last.commit_seq,
                            last.commit_time_unix_ns,
                            last.capsule_object_id,
                            last.proof_object_id,
                            None,
                        )
                        .to_record_bytes(),
                        _ => last.to_record_bytes(),
                    };
                    if damage == 2 {
                        changed[0] ^= 0xFF;
                    }
                    source
                        .markers
                        .write(&cx, &changed, u64_len(offset).unwrap())
                        .await
                        .unwrap();
                    let before = (
                        bytes(&source.symbols, &cx).await,
                        bytes(&source.markers, &cx).await,
                    );
                    let mut destination = log(&vfs, &cx, "destination");
                    let codec = CopyCodec::default();
                    assert!(matches!(
                        copy_mode(&mut source, &cx, &mut destination, &codec, repair).await,
                        Err(FrankenError::WalCorrupt { .. })
                    ));
                    assert_copy_destination_unused(&destination, &cx);
                    assert_eq!(codec.calls.load(Ordering::Relaxed), 0);
                    assert!(source.needs_recovery());
                    assert_eq!(bytes(&source.symbols, &cx).await, before.0);
                    assert_eq!(bytes(&source.markers, &cx).await, before.1);
                    source.close(&cx).unwrap();
                    destination.close(&cx).unwrap();
                }
            }
        });
    }

    #[test]
    fn fresh_copy_rechecks_source_after_regeneration_before_publishing_any_marker() {
        run(async {
            let cx = Cx::new();
            let vfs = MemoryVfs::new();
            let mut source = log(&vfs, &cx, "source");
            shared_object_chain(&mut source, &cx, 129).await;
            let retained = 128 * u64_len(COMMIT_MARKER_RECORD_V1_SIZE).unwrap();
            let codec = CopyCodec {
                calls: AtomicUsize::new(0),
                shorten_markers: Mutex::new(Some((file(&vfs, &cx, "source-markers"), retained))),
            };
            let mut destination = log(&vfs, &cx, "destination");
            assert!(matches!(
                source
                    .repair_recovered_prefix_to(&cx, &mut destination, &codec)
                    .await,
                Err(FrankenError::BusyRecovery)
            ));
            assert!(codec.calls.load(Ordering::Relaxed) >= 3);
            assert_eq!(source.markers.file_size(&cx).unwrap(), retained);
            assert!(source.needs_recovery());
            assert!(destination.symbols.file_size(&cx).unwrap() > 0);
            assert_eq!(destination.markers.file_size(&cx).unwrap(), 0);
            assert_eq!(destination.published_tip(), CommitSeq::ZERO);
            assert!(destination.needs_recovery());
            assert!(destination.outstanding_write().is_none());
            // The staged object is an orphan, not a copied 128-commit prefix.
            destination = recover(destination, &cx).await;
            assert_eq!(destination.published_tip(), CommitSeq::ZERO);
            assert_eq!(destination.record_count, 1);
            destination.close(&cx).unwrap();
            source.close(&cx).unwrap();
        });
    }

    #[test]
    fn fresh_copy_preflights_total_marker_capacity_and_can_retry_the_same_destination() {
        run(async {
            for repair in [false, true] {
                let cx = Cx::new();
                let vfs = MemoryVfs::new();
                let mut source = log(&vfs, &cx, "source");
                shared_object_chain(&mut source, &cx, 129).await;
                let original = bytes(&source.markers, &cx).await;
                let total = u64_len(original.len()).unwrap();
                let mut destination = log(&vfs, &cx, "destination");
                destination.limits.max_stream_bytes = total - 1;
                let codec = CopyCodec::default();
                assert!(matches!(
                    copy_mode(&mut source, &cx, &mut destination, &codec, repair).await,
                    Err(FrankenError::TooBig)
                ));
                assert_copy_destination_unused(&destination, &cx);
                assert_eq!(codec.calls.load(Ordering::Relaxed), 0);
                assert!(!source.needs_recovery());
                destination.limits.max_stream_bytes = total;
                let receipt = copy_mode(&mut source, &cx, &mut destination, &codec, repair)
                    .await
                    .unwrap();
                assert_eq!(receipt.commits, 129);
                assert_eq!(receipt.symbol_records, 1);
                assert_eq!(receipt.marker_stream_bytes, total);
                assert_eq!(bytes(&destination.markers, &cx).await, original);
                assert!(!destination.needs_recovery());
                source.close(&cx).unwrap();
                destination.close(&cx).unwrap();
            }
        });
    }

    #[test]
    fn fresh_copy_chain_preflight_preserves_tails_and_full_bounded_history() {
        run(async {
            for repair in [false, true] {
                let cx = Cx::new();
                let vfs = MemoryVfs::new();
                let mut source = log(&vfs, &cx, "source");
                shared_object_chain(&mut source, &cx, 129).await;
                let marker_bytes = source.marker_end;
                source
                    .symbols
                    .write(&cx, b"torn", source.symbol_end)
                    .await
                    .unwrap();
                source
                    .markers
                    .write(&cx, &[0xAA; 17], source.marker_end)
                    .await
                    .unwrap();
                source = recover(source, &cx).await;
                let original = (
                    bytes(&source.symbols, &cx).await,
                    bytes(&source.markers, &cx).await,
                );
                let mut destination = log(&vfs, &cx, "destination");
                destination.limits.max_stream_bytes = marker_bytes;
                let receipt = copy_mode(
                    &mut source,
                    &cx,
                    &mut destination,
                    &CopyCodec::default(),
                    repair,
                )
                .await
                .unwrap();
                assert_eq!(receipt.commits, 129);
                assert_eq!(receipt.last_seq, CommitSeq::new(129));
                assert_eq!(receipt.marker_stream_bytes, marker_bytes);
                assert_eq!(receipt.symbol_stream_bytes + 4, source.symbol_end);
                assert_eq!(
                    bytes(&destination.markers, &cx).await.as_slice(),
                    &original.1[..usize::try_from(marker_bytes).unwrap()]
                );
                assert_eq!(bytes(&source.symbols, &cx).await, original.0);
                assert_eq!(bytes(&source.markers, &cx).await, original.1);
                assert!(source.needs_recovery());
                destination = recover(destination, &cx).await;
                assert!(!destination.needs_recovery());
                assert_eq!(destination.published_tip(), CommitSeq::new(129));
                source.close(&cx).unwrap();
                destination.close(&cx).unwrap();
            }
        });
    }
}
