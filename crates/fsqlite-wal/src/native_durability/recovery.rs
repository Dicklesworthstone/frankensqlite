//! Caller-owned rewrite-before-sync recovery for native streams.
//!
//! A read/parse/checksum pass and a second fsync cannot repair a failed
//! writeback that left clean pages in the OS cache. This owner re-dirties the
//! exact retained bytes before either barrier. It is deliberately not hidden
//! inside the consuming `NativeDurabilityLog::recover` future: cancellation
//! must leave the write completion AND the original rewrite buffer accessible.

use std::fmt;

use fsqlite_error::{FrankenError, Result};
use fsqlite_types::cx::Cx;
use fsqlite_types::{ObjectId, SymbolRecord};
use fsqlite_vfs::{FileIdentity, SyncKind, VfsFile, VfsWriteCompletion, VfsWriteCompletionState};

use super::{
    NativeDurabilityLimits, NativeDurabilityLog, NativeDurabilityRecovery, checkpoint, corrupt,
    ensure_distinct_files, read_exact_at, u64_len, zeroed,
};

const REWRITE_CHUNK_BYTES: usize = 64 * 1024;

#[derive(Clone, Copy)]
enum Stream {
    Symbols,
    Markers,
}

#[derive(Clone, Copy, PartialEq, Eq)]
struct StreamBinding {
    symbols: FileIdentity,
    markers: FileIdentity,
    symbol_end: u64,
    marker_end: u64,
}

impl StreamBinding {
    fn capture<S: VfsFile, M: VfsFile>(log: &NativeDurabilityLog<S, M>, cx: &Cx) -> Result<Self> {
        let binding = Self {
            symbols: log.symbols.refresh_file_identity()?.ok_or(FrankenError::Unsupported)?,
            markers: log.markers.refresh_file_identity()?.ok_or(FrankenError::Unsupported)?,
            symbol_end: log.symbols.file_size(cx)?,
            marker_end: log.markers.file_size(cx)?,
        };
        if binding.symbols == binding.markers {
            return Err(corrupt("native recovery streams alias the same file"));
        }
        Ok(binding)
    }

    fn check<S: VfsFile, M: VfsFile>(&self, log: &NativeDurabilityLog<S, M>, cx: &Cx) -> Result<()> {
        if *self != Self::capture(log, cx)? {
            return Err(FrankenError::BusyRecovery);
        }
        Ok(())
    }
}

struct PendingRewrite {
    stream: Stream,
    offset: u64,
    bytes: Vec<u8>,
    binding: StreamBinding,
}

/// Explicit recovery owner for a failed or newly reopened native log.
///
/// Keep this object AND the external append/namespace lease alive until its
/// tracked writes settle. Dropping `prepare` only abandons the wait; it does
/// not drop this owner, lose the source token, or forget the bytes needed to
/// retry a partial overwrite. No task/runtime is created. New handles require
/// the caller to settle every old owner's outstanding write before admission.
///
/// Recovery copies bytes in place; it never deletes, truncates, changes a
/// logical record, invents a commit, or treats a torn tail as appendable.
/// A complete corrupt marker or an undecodable committed referent still fails
/// closed. This is writeback recovery, not reconstruction of lost source bytes.
#[must_use]
pub struct NativeRecovery<S: VfsFile, M: VfsFile> {
    log: NativeDurabilityLog<S, M>,
    pending: Option<PendingRewrite>,
    completed: Option<NativeDurabilityRecovery>,
}

impl<S: VfsFile, M: VfsFile> fmt::Debug for NativeRecovery<S, M> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("NativeRecovery")
            .field("completed", &self.completed.is_some())
            .field("retained_rewrite", &self.pending.is_some())
            .field("write_pending", &self.write_pending())
            .finish_non_exhaustive()
    }
}

impl<S: VfsFile, M: VfsFile> NativeRecovery<S, M> {
    /// Admit already-open streams without writing them or claiming recovery.
    /// The original namespace/append lease must cover the entire session.
    ///
    /// # Errors
    /// Refuses invalid limits, cancellation, aliased/unidentifiable streams.
    pub fn new(cx: &Cx, symbols: S, markers: M, limits: NativeDurabilityLimits) -> Result<Self> {
        limits.validate()?;
        checkpoint(cx)?;
        ensure_distinct_files(&symbols, &markers)?;
        Ok(Self::from_log(NativeDurabilityLog::empty(symbols, markers, limits)))
    }

    pub(super) fn from_log(mut log: NativeDurabilityLog<S, M>) -> Self {
        log.poisoned = true;
        Self { log, pending: None, completed: None }
    }

    /// The original source-owned completion, including an abandoned overwrite.
    #[must_use]
    pub fn outstanding_write(&self) -> Option<VfsWriteCompletion> {
        self.log.outstanding_write()
    }

    fn write_pending(&self) -> bool {
        self.log.last_write.as_ref().is_some_and(|write| {
            write.state() == VfsWriteCompletionState::Pending
        })
    }

    /// Validate the complete prefix, rewrite symbols, sync them, then rewrite
    /// and sync markers. All rewriting uses at most one 64-KiB owned chunk
    /// (also bounded by `max_batch_bytes`), independent of the stream length.
    /// The normal scanner/decoder bounds still apply to validation metadata.
    ///
    /// `verify_object` has the same mandatory decode/authentication/transitive
    /// dependency obligations as `NativeDurabilityLog::publish`. Validation
    /// precedes the first new rewrite. Retrying a previously attempted partial
    /// rewrite first restores its retained original bytes, then validates the
    /// complete prefix again. Subsequent retries re-dirty the entire streams:
    /// a failed sync is never retried by issuing just another sync.
    ///
    /// An error or dropped future retains ownership and blocks `finish`.
    /// Pending physical writes must settle before retry, scan, or close. A
    /// settled partial overwrite retains its original buffer until restored;
    /// close refuses to discard that recovery obligation. No cancellation
    /// check after the final successful barrier revokes a completed recovery.
    ///
    /// # Errors
    /// Propagates validation/I/O/resource errors or cancellation. Refuses
    /// outstanding writes, closed streams, or changed stream identities/sizes.
    pub async fn prepare<V, VF>(&mut self, cx: &Cx, verify_object: V) -> Result<()>
    where
        V: FnMut(ObjectId, Vec<SymbolRecord>) -> VF,
        VF: std::future::Future<Output = Result<()>>,
    {
        if self.write_pending() || self.log.symbols_closed || self.log.markers_closed {
            return Err(FrankenError::BusyRecovery);
        }
        checkpoint(cx)?;
        self.completed = None;
        self.log.poisoned = true;
        self.restore_pending(cx).await?;

        let binding = StreamBinding::capture(&self.log, cx)?;
        let report = self.log.scan_recovery(cx, verify_object).await?;
        binding.check(&self.log, cx)?;
        self.rewrite_stream(cx, Stream::Symbols, binding.symbol_end, binding).await?;
        checkpoint(cx)?;
        binding.check(&self.log, cx)?;
        self.log.symbols.durable_sync(cx, SyncKind::FullDurable)?;

        self.rewrite_stream(cx, Stream::Markers, binding.marker_end, binding).await?;
        checkpoint(cx)?;
        binding.check(&self.log, cx)?;
        self.log.markers.durable_sync(cx, SyncKind::FullDurable)?;
        // Do not turn an acknowledged barrier into a retry due to late cancel.
        self.log.last_write = None;
        self.log.poisoned = false;
        self.completed = Some(report);
        Ok(())
    }

    async fn rewrite_stream(
        &mut self,
        cx: &Cx,
        stream: Stream,
        end: u64,
        binding: StreamBinding,
    ) -> Result<()> {
        let chunk_size = self.log.limits.max_batch_bytes.min(REWRITE_CHUNK_BYTES);
        let mut offset = 0;
        while offset < end {
            checkpoint(cx)?;
            binding.check(&self.log, cx)?;
            let count = usize::try_from((end - offset).min(u64_len(chunk_size)?))
                .map_err(|_| FrankenError::TooBig)?;
            let mut bytes = zeroed(count)?;
            match stream {
                Stream::Symbols => read_exact_at(&self.log.symbols, cx, &mut bytes, offset).await?,
                Stream::Markers => read_exact_at(&self.log.markers, cx, &mut bytes, offset).await?,
            }
            // Retain BEFORE polling the source mutation. A failed/abandoned
            // write may have damaged the readable copy at this exact offset.
            self.pending = Some(PendingRewrite { stream, offset, bytes, binding });
            self.restore_pending(cx).await?;
            offset += u64_len(count)?; // count is bounded by end - offset.
        }
        Ok(())
    }

    async fn restore_pending(&mut self, cx: &Cx) -> Result<()> {
        if self.write_pending() {
            return Err(FrankenError::BusyRecovery);
        }
        let Some(pending) = &self.pending else { return Ok(()); };
        checkpoint(cx)?;
        pending.binding.check(&self.log, cx)?;
        let completion = VfsWriteCompletion::new();
        self.log.last_write = Some(completion.clone());
        match pending.stream {
            Stream::Symbols => {
                self.log.symbols.write_tracked(cx, &pending.bytes, pending.offset, completion).await?;
            }
            Stream::Markers => {
                self.log.markers.write_tracked(cx, &pending.bytes, pending.offset, completion).await?;
            }
        }
        self.pending = None;
        self.log.last_write = None;
        Ok(())
    }

    /// Transfer only a completely recovered log. On refusal the entire owner
    /// is returned, including any buffer/token needed to finish recovery.
    /// Torn tails are transferable but remain non-appendable in the log.
    ///
    /// # Errors
    /// Returns `self` until prepare succeeds, or after close has begun.
    #[allow(clippy::result_large_err)] // Returning the owner preserves physical write obligations.
    pub fn finish(mut self) -> std::result::Result<RecoveredNativeLog<S, M>, Self> {
        if self.log.poisoned || self.pending.is_some() || self.write_pending()
            || self.log.symbols_closed || self.log.markers_closed
        {
            return Err(self);
        }
        let Some(report) = self.completed.take() else { return Err(self); };
        Ok(RecoveredNativeLog { log: self.log, report })
    }

    /// Close only when no original rewrite bytes or physical write are owed.
    ///
    /// # Errors
    /// Returns BusyRecovery for a retained overwrite; preserves close failures.
    pub fn close(&mut self, cx: &Cx) -> Result<()> {
        if self.pending.is_some() {
            return Err(FrankenError::BusyRecovery);
        }
        self.completed = None;
        self.log.close(cx)
    }
}

/// Unforgeable handoff from a completed rewrite and both durability barriers.
/// Owning it does not replace the external namespace/append lease or SQL/SSI
/// validation, and its report does not acknowledge a new commit to old callers.
#[must_use]
pub struct RecoveredNativeLog<S: VfsFile, M: VfsFile> {
    log: NativeDurabilityLog<S, M>,
    report: NativeDurabilityRecovery,
}

impl<S: VfsFile, M: VfsFile> fmt::Debug for RecoveredNativeLog<S, M> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RecoveredNativeLog").field("report", &self.report).finish_non_exhaustive()
    }
}

impl<S: VfsFile, M: VfsFile> RecoveredNativeLog<S, M> {
    /// Verified committed prefix and preserved tail accounting.
    #[must_use]
    pub const fn report(&self) -> &NativeDurabilityRecovery { &self.report }

    /// Resume low-level log use without reopening either physical handle.
    /// Higher layers must independently reconstruct their semantic state.
    #[must_use]
    pub fn into_log(self) -> (NativeDurabilityLog<S, M>, NativeDurabilityRecovery) {
        (self.log, self.report)
    }
}

#[cfg(test)]
mod tests;
