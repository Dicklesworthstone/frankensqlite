//! Bounded ownership of decoded changesets until application handoff.
//!
//! The payload budget covers retained symbol bytes plus decoded page bytes.
//! Codec scratch, collection metadata, and explicitly collected audit proofs
//! are separate; this is not a bound on the process's total resident memory.

use fsqlite_error::{FrankenError, Result};
use fsqlite_types::cx::Cx;

use super::{DecodeResult, ReceiverState, ReplicationReceiver};
use crate::replication_sender::{
    ReplicationPacket, ReplicationWireVersion, derive_seed_from_changeset_id,
};

impl ReplicationReceiver {
    /// Number of decoded changesets still owned by this receiver.
    #[must_use]
    pub const fn pending_changesets(&self) -> usize {
        self.pending_results.len()
    }

    /// Retained symbol and decoded-page payload bytes, excluding codec scratch
    /// and audit proofs. Draining a result transfers its memory to the caller.
    #[must_use]
    pub const fn buffered_payload_bytes(&self) -> usize {
        self.buffered_symbol_bytes
            .saturating_add(self.pending_payload_bytes)
    }

    /// Apply the oldest decoded batch without surrendering retry ownership.
    ///
    /// The callback borrows the pages in place and may await the caller's
    /// database transaction. Only an `Ok(())` removes the batch, releases its
    /// budget, and returns it to the caller. An error, unwinding panic, or
    /// dropped future leaves the batch and all following batches queued,
    /// including their payload charges and decode proofs. An empty queue returns `Ok(None)`
    /// without invoking the callback. No page-sized retry clone is needed.
    ///
    /// This uses decode-ready order, not database commit order.
    /// The caller must validate database identity, generation, and ordering,
    /// apply each complete changeset atomically, and return success only after
    /// its required durability boundary. A callback abandoned during I/O must
    /// roll back or reconcile idempotently using the changeset ID before retry.
    /// Retaining a batch cannot undo external side effects, and a decoded
    /// result is not itself a durable commit certificate. Receiver restart or
    /// raw draining still requires caller-owned replay/deduplication state.
    ///
    /// There is deliberately no cancellation checkpoint between callback
    /// success and removal: an acknowledged apply must not become a retry just
    /// because cancellation arrived while that successful apply was settling.
    ///
    /// # Errors
    ///
    /// Propagates cancellation before application, the callback's error, or
    /// invalid payload accounting before the callback runs. No failed batch
    /// is consumed. The callback's future need not be `Send`, allowing a
    /// connection-local async transaction on the caller's executor.
    #[allow(clippy::future_not_send)]
    pub async fn apply_next_with<F>(
        &mut self,
        cx: &Cx,
        mut apply: F,
    ) -> Result<Option<DecodeResult>>
    where
        F: for<'a> AsyncFnMut(&'a Cx, &'a DecodeResult) -> Result<()>,
    {
        cx.checkpoint().map_err(|_| FrankenError::Abort)?;
        let Some(batch) = self.pending_results.front() else {
            return Ok(None);
        };
        let page_bytes = batch.pages.iter().try_fold(0_usize, |total, page| {
            total.checked_add(page.page_data.len()).ok_or_else(|| {
                FrankenError::Internal("replication payload accounting overflow".to_owned())
            })
        })?;
        let remaining_bytes = self
            .pending_payload_bytes
            .checked_sub(page_bytes)
            .ok_or_else(|| {
                FrankenError::Internal("replication pending payload accounting underflow".to_owned())
            })?;

        // `&mut self` remains exclusively borrowed across this await. Nothing
        // has been drained, so dropping this future cannot lose pending work.
        apply(cx, batch).await?;

        // No suspension or fallible operation after the apply acknowledgement.
        let completed = self.pending_results.pop_front();
        self.pending_payload_bytes = remaining_bytes;
        self.applied_count = self.applied_count.saturating_add(1);
        self.state = if !self.pending_results.is_empty() {
            ReceiverState::Applying
        } else if self.decoders.is_empty() {
            ReceiverState::Complete
        } else {
            ReceiverState::Collecting
        };
        Ok(completed)
    }

    pub(super) fn pending_retransmission(&self, packet: &ReplicationPacket) -> Result<bool> {
        if !self
            .pending_results
            .iter()
            .any(|result| result.changeset_id == packet.changeset_id)
        {
            return Ok(false);
        }
        // Authentication and structural admission run before this check.
        // A different valid packetization of an already-decoded object is
        // still a duplicate, but a noncanonical seed must not bypass refusal.
        if packet.wire_version == ReplicationWireVersion::FramedV2
            && packet.seed != derive_seed_from_changeset_id(&packet.changeset_id)
        {
            return Err(FrankenError::DatabaseCorrupt {
                detail: "seed mismatch for pending changeset".to_owned(),
            });
        }
        Ok(true)
    }

    pub(super) fn check_new_changeset_slot(&self) -> Result<()> {
        // A decoded batch still occupies its slot until ownership is handed
        // off. Completing K-symbol objects must not bypass the decoder cap.
        if self.decoders.len()
            >= self
                .config
                .max_inflight_decoders
                .saturating_sub(self.pending_results.len())
        {
            return Err(FrankenError::Busy);
        }
        Ok(())
    }

    pub(super) fn incoming_payload_fits(&self, bytes: usize) -> bool {
        self.buffered_symbol_bytes
            .checked_add(self.pending_payload_bytes)
            .and_then(|total| total.checked_add(bytes))
            .is_some_and(|total| total <= self.config.max_buffered_symbol_bytes)
    }

    pub(super) fn refresh_collection_state(&mut self) {
        self.state = if !self.pending_results.is_empty() {
            ReceiverState::Applying
        } else if self.decoders.is_empty() {
            ReceiverState::Listening
        } else {
            ReceiverState::Collecting
        };
    }

    pub(super) fn rollback_received_symbol(
        &mut self,
        changeset_id: crate::replication_sender::ChangesetId,
        esi: u32,
        created_decoder: bool,
    ) {
        if let Some(decoder) = self.decoders.get_mut(&changeset_id)
            && let Some(data) = decoder.symbols.remove(&esi)
        {
            decoder.received_isis.remove(&esi);
            self.buffered_symbol_bytes -= data.len();
            if let Some(count) = self.received_counts.get_mut(&changeset_id) {
                *count -= 1;
            }
        }
        if created_decoder {
            self.remove_decoder(changeset_id);
        }
        self.refresh_collection_state();
    }

    pub(super) fn enqueue_decoded(&mut self, result: DecodeResult) -> Result<()> {
        let page_bytes = result.pages.iter().try_fold(0_usize, |total, page| {
            total
                .checked_add(page.page_data.len())
                .ok_or(FrankenError::TooBig)
        })?;
        let pending_bytes = self
            .pending_payload_bytes
            .checked_add(page_bytes)
            .ok_or(FrankenError::TooBig)?;
        let released_bytes = self
            .decoders
            .get(&result.changeset_id)
            .ok_or_else(|| {
                FrankenError::Internal("decoded changeset has no admission owner".to_owned())
            })?
            .buffered_bytes();
        let remaining_symbols = self
            .buffered_symbol_bytes
            .checked_sub(released_bytes)
            .ok_or_else(|| {
                FrankenError::Internal("replication symbol accounting underflow".to_owned())
            })?;
        let total = remaining_symbols
            .checked_add(pending_bytes)
            .ok_or(FrankenError::TooBig)?;
        if total > self.config.max_buffered_symbol_bytes {
            return Err(FrankenError::TooBig);
        }
        self.pending_results
            .try_reserve(1)
            .map_err(|_| FrankenError::OutOfMemory)?;

        // All fallible admission is complete before replacing the symbol
        // owner. A rejected enqueue can roll back the triggering symbol and
        // retry it; it must not strand a fully collected, deduplicated object.
        if let Some(proof) = &result.decode_proof {
            self.record_decode_proof(proof.clone());
        }
        self.remove_decoder(result.changeset_id);
        self.pending_payload_bytes = pending_bytes;
        self.pending_results.push_back(result);
        self.state = ReceiverState::Applying;
        Ok(())
    }

    pub(super) fn take_pending_results(&mut self) -> Result<Vec<DecodeResult>> {
        if self.pending_results.is_empty() {
            return Err(FrankenError::Internal(format!(
                "receiver has no pending changesets, current state: {:?}",
                self.state
            )));
        }
        let mut results = Vec::new();
        results
            .try_reserve_exact(self.pending_results.len())
            .map_err(|_| FrankenError::OutOfMemory)?;
        results.extend(self.pending_results.drain(..));
        self.pending_payload_bytes = 0;
        self.applied_count = self
            .applied_count
            .saturating_add(u64::try_from(results.len()).unwrap_or(u64::MAX));
        self.state = if self.decoders.is_empty() {
            ReceiverState::Complete
        } else {
            ReceiverState::Collecting
        };
        Ok(results)
    }
}
