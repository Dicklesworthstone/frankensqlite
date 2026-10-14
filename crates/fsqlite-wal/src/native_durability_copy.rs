//! Non-destructive migration of a recovered native prefix into fresh streams.

use super::{
    COMMIT_MARKER_RECORD_V1_SIZE, CommitMarker, CommitSeq, NativeDurabilityLog, ObjectId,
    Result, SymbolRecord, SyncKind, VfsFile, checkpoint, corrupt, ensure_distinct_files,
    read_exact_at, u64_len,
};
use fsqlite_error::FrankenError;
use fsqlite_types::cx::Cx;

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
        mut verify_object: V,
    ) -> Result<NativePrefixCopyReceipt>
    where
        DS: VfsFile,
        DM: VfsFile,
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
            return Err(corrupt("native prefix copy has an unaccepted marker boundary"));
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
        let mut copy = CopyGuard {
            destination,
            complete: false,
        };
        for &object_id in self.index.keys() {
            checkpoint(cx)?;
            let records = self.read_object(cx, object_id).await?;
            copy.destination.append_symbols(cx, &records).await?;
        }
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
            return Err(corrupt("native prefix copy changed the accepted marker chain"));
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
