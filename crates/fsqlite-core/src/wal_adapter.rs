//! Adapters bridging the WAL and pager crates at runtime.
//!
//! These adapters break the circular dependency between `fsqlite-pager` and
//! `fsqlite-wal`:
//!
//! - [`WalBackendAdapter`] wraps `WalFile` to satisfy the pager's
//!   [`WalBackend`] trait (pager → WAL direction).
//! - [`CheckpointTargetAdapterRef`] wraps `CheckpointPageWriter` to satisfy the
//!   WAL executor's [`CheckpointTarget`] trait (WAL → pager direction).

use std::collections::HashMap;

use fsqlite_error::{FrankenError, Result};
use fsqlite_pager::{CheckpointMode, CheckpointPageWriter, CheckpointResult, WalBackend};
use fsqlite_types::PageNumber;
use fsqlite_types::cx::Cx;
use fsqlite_types::flags::SyncFlags;
use fsqlite_vfs::VfsFile;
use fsqlite_wal::{
    CheckpointMode as WalCheckpointMode, CheckpointState, CheckpointTarget, WalFile,
    execute_checkpoint,
};
use fsqlite_wal::checksum::WalSalts;
use tracing::{debug, trace, warn};

use crate::wal_fec_adapter::{FecCommitHook, FecCommitResult};

// ---------------------------------------------------------------------------
// WalBackendAdapter: WalFile → WalBackend
// ---------------------------------------------------------------------------

/// Adapter wrapping [`WalFile`] to implement the pager's [`WalBackend`] trait.
///
/// The pager calls `dyn WalBackend` during WAL-mode commits and page reads.
/// This adapter delegates those calls to the concrete `WalFile<F>` from
/// `fsqlite-wal`.
pub struct WalBackendAdapter<F: VfsFile> {
    wal: WalFile<F>,
    /// Guard so commit-time append refresh runs only once per commit batch.
    refresh_before_append: bool,
    /// Optional FEC commit hook for encoding repair symbols on commit.
    fec_hook: Option<FecCommitHook>,
    /// Accumulated FEC commit results (for later sidecar persistence).
    fec_pending: Vec<FecCommitResult>,
    /// Cached mapping from page_number → frame_index for O(1) WAL page lookups.
    /// Maps each page to the index of the most recent committed frame containing it.
    page_index: HashMap<u32, usize>,
    /// The last committed frame index through which `page_index` has been built,
    /// or `None` if the index needs a full rebuild (e.g. after WAL reset).
    index_built_to: Option<usize>,
    /// WAL salts at the time `page_index` was last built.  If the current
    /// WAL header salts differ, the WAL was reset (new generation) and the
    /// index must be fully rebuilt — even if frame counts happen to match.
    index_salts: WalSalts,
}

impl<F: VfsFile> WalBackendAdapter<F> {
    /// Wrap an existing [`WalFile`] in the adapter (FEC disabled).
    #[must_use]
    pub fn new(wal: WalFile<F>) -> Self {
        let salts = wal.header().salts;
        Self {
            wal,
            refresh_before_append: true,
            fec_hook: None,
            fec_pending: Vec::new(),
            page_index: HashMap::new(),
            index_built_to: None,
            index_salts: salts,
        }
    }

    /// Wrap an existing [`WalFile`] with an FEC commit hook.
    #[must_use]
    pub fn with_fec_hook(wal: WalFile<F>, hook: FecCommitHook) -> Self {
        let salts = wal.header().salts;
        Self {
            wal,
            refresh_before_append: true,
            fec_hook: Some(hook),
            fec_pending: Vec::new(),
            page_index: HashMap::new(),
            index_built_to: None,
            index_salts: salts,
        }
    }

    /// Consume the adapter and return the inner [`WalFile`].
    #[must_use]
    pub fn into_inner(self) -> WalFile<F> {
        self.wal
    }

    /// Borrow the inner [`WalFile`].
    #[must_use]
    pub fn inner(&self) -> &WalFile<F> {
        &self.wal
    }

    /// Mutably borrow the inner [`WalFile`].
    ///
    /// Invalidates the page index so that the next `read_page` rebuilds
    /// from scratch, preventing stale lookups after structural WAL
    /// mutations (append, reset, truncate).
    ///
    /// # Warning
    ///
    /// FEC state (`fec_pending`, `fec_hook`) is **not** invalidated.
    /// Intended for test fixtures and one-off administrative operations,
    /// not production read/write paths.
    pub fn inner_mut(&mut self) -> &mut WalFile<F> {
        self.page_index.clear();
        self.index_built_to = None;
        &mut self.wal
    }

    /// Take any pending FEC commit results for sidecar persistence.
    ///
    /// Retains the Vec's capacity (capped at 256) so the next commit cycle
    /// avoids reallocation while shedding anomalous spikes from bulk
    /// transactions.
    #[must_use]
    pub fn take_fec_pending(&mut self) -> Vec<FecCommitResult> {
        if self.fec_pending.is_empty() {
            return Vec::new();
        }
        let cap = self.fec_pending.capacity().min(256);
        std::mem::replace(&mut self.fec_pending, Vec::with_capacity(cap))
    }

    /// Whether FEC encoding is active.
    #[must_use]
    pub fn fec_enabled(&self) -> bool {
        self.fec_hook
            .as_ref()
            .is_some_and(FecCommitHook::is_enabled)
    }

    /// Discard buffered FEC pages (e.g. on transaction rollback).
    pub fn fec_discard(&mut self) {
        if let Some(hook) = &mut self.fec_hook {
            hook.discard_buffered();
        }
    }

    /// Ensure `page_index` is up to date through `last_commit_frame`.
    ///
    /// Scans only the frames added since the last call, building the index
    /// incrementally.  Each page maps to the frame index of its most recent
    /// committed version (newest frame wins, matching SQLite WAL read protocol).
    ///
    /// The initial build reads one frame header per WAL frame (O(N) I/O).
    /// This is a one-time cost per WAL generation; subsequent lookups are O(1).
    fn ensure_index_current(&mut self, cx: &Cx, last_commit_frame: usize) -> Result<()> {
        // Detect WAL generation change via salt comparison.  If the WAL was
        // reset (checkpoint + truncate), the salts will differ even if frame
        // counts happen to match.
        let current_salts = self.wal.header().salts;
        let generation_changed = current_salts != self.index_salts;

        let start = if generation_changed {
            debug!(last_commit_frame, "WAL generation change detected (salts differ); full index rebuild");
            self.shrink_or_clear_index();
            0
        } else {
            match self.index_built_to {
                Some(prev) if prev == last_commit_frame => return Ok(()), // already current
                Some(prev) if prev < last_commit_frame => prev + 1,      // incremental extend
                Some(_) => {
                    // prev > last_commit_frame: WAL shrank externally.
                    debug!(last_commit_frame, "WAL shrank (prev > last_commit_frame); full index rebuild");
                    self.shrink_or_clear_index();
                    0
                }
                None => {
                    debug!(last_commit_frame, "Building WAL page index from scratch");
                    0
                }
            }
        };

        let result = self.build_index_range(cx, start, last_commit_frame);
        if result.is_err() {
            // On I/O error, discard the partially-built index to prevent
            // stale reads (a page might have a newer version in the
            // un-indexed tail).
            self.page_index.clear();
            self.index_built_to = None;
        } else if generation_changed {
            // Only record the new salts after a successful build.
            self.index_salts = current_salts;
        }
        result
    }

    /// Maximum entries to pre-allocate in `page_index.reserve()`.
    const MAX_RESERVE: usize = 65_536;

    /// Scan frame headers in `[start, end]` and insert into `page_index`.
    fn build_index_range(&mut self, cx: &Cx, start: usize, end: usize) -> Result<()> {
        debug_assert!(start <= end, "build_index_range: start ({start}) > end ({end})");
        let count = end.saturating_sub(start).saturating_add(1);
        if count > 1 {
            self.page_index.reserve(count.min(Self::MAX_RESERVE));
        }
        for i in start..=end {
            let header = self.wal.read_frame_header(cx, i)?;
            self.page_index.insert(header.page_number, i);
        }
        self.index_built_to = Some(end);
        Ok(())
    }

    /// Clear the page index, retaining capacity if within reasonable bounds.
    fn shrink_or_clear_index(&mut self) {
        if self.page_index.capacity() > Self::MAX_RESERVE {
            self.page_index = HashMap::new();
        } else {
            self.page_index.clear();
        }
    }

    /// Invalidate the page index, forcing a full rebuild on the next read.
    fn invalidate_page_index(&mut self) {
        self.page_index = HashMap::new();
        self.index_built_to = None;
    }
}

/// Convert pager checkpoint mode to WAL checkpoint mode.
fn to_wal_mode(mode: CheckpointMode) -> WalCheckpointMode {
    match mode {
        CheckpointMode::Passive => WalCheckpointMode::Passive,
        CheckpointMode::Full => WalCheckpointMode::Full,
        CheckpointMode::Restart => WalCheckpointMode::Restart,
        CheckpointMode::Truncate => WalCheckpointMode::Truncate,
    }
}

impl<F: VfsFile> WalBackend for WalBackendAdapter<F> {
    fn begin_transaction(&mut self, cx: &Cx) -> Result<()> {
        // Establish a transaction-bounded snapshot once, instead of doing an
        // expensive refresh for every page read.
        self.wal.refresh(cx)?;
        self.refresh_before_append = true;
        Ok(())
    }

    fn append_frame(
        &mut self,
        cx: &Cx,
        page_number: u32,
        page_data: &[u8],
        db_size_if_commit: u32,
    ) -> Result<()> {
        debug_assert!(page_number > 0, "page_number must be 1-based, got 0");
        if self.refresh_before_append {
            // Keep this handle synchronized with external WAL growth/reset
            // before choosing append offset and checksum seed.
            self.wal.refresh(cx)?;
        }
        self.wal
            .append_frame(cx, page_number, page_data, db_size_if_commit)?;
        self.refresh_before_append = false;

        // Feed the frame to the FEC hook.  On commit, it encodes repair
        // symbols and stores them for later sidecar persistence.
        if let Some(hook) = &mut self.fec_hook {
            match hook.on_frame(cx, page_number, page_data, db_size_if_commit) {
                Ok(Some(result)) => {
                    debug!(
                        pages = result.page_numbers.len(),
                        k_source = result.k_source,
                        symbols = result.symbols.len(),
                        "FEC commit group encoded"
                    );
                    self.fec_pending.push(result);
                }
                Ok(None) => {}
                Err(e) => {
                    // FEC encoding failure is non-fatal — log and continue.
                    // Discard the hook's buffered pages to prevent a partial
                    // FEC block from being emitted on the next commit frame.
                    hook.discard_buffered();
                    warn!(error = %e, "FEC encoding failed; commit proceeds without repair symbols");
                }
            }
        }

        Ok(())
    }

    fn read_page(&mut self, cx: &Cx, page_number: u32) -> Result<Option<Vec<u8>>> {
        debug_assert!(page_number > 0, "page_number must be 1-based, got 0");
        // Restrict visibility to committed frames only.
        let Some(last_commit_frame) = self.wal.last_commit_frame(cx)? else {
            return Ok(None);
        };

        // Build/extend the page index so lookups are O(1) instead of O(n).
        self.ensure_index_current(cx, last_commit_frame)?;

        if let Some(&frame_index) = self.page_index.get(&page_number) {
            let (header, data) = self.wal.read_frame(cx, frame_index)?;
            // Verify the physical frame header matches the requested page.
            // The old scan-based code implicitly validated this via its loop
            // condition; the HashMap lookup must check explicitly.  This is a
            // runtime check (not debug_assert) because a mismatch in release
            // builds would silently return wrong page data.
            if header.page_number != page_number {
                return Err(FrankenError::WalCorrupt {
                    detail: format!(
                        "page index inconsistency: frame {frame_index} contains page {}, expected {page_number}",
                        header.page_number
                    ),
                });
            }
            trace!(page_number, frame_index, "WAL adapter: page found in WAL");
            Ok(Some(data))
        } else {
            Ok(None)
        }
    }

    fn sync(&mut self, cx: &Cx) -> Result<()> {
        let result = self.wal.sync(cx, SyncFlags::NORMAL);
        self.refresh_before_append = true;
        result
    }

    fn frame_count(&self) -> usize {
        self.wal.frame_count()
    }

    fn checkpoint(
        &mut self,
        cx: &Cx,
        mode: CheckpointMode,
        writer: &mut dyn CheckpointPageWriter,
        backfilled_frames: u32,
        oldest_reader_frame: Option<u32>,
    ) -> Result<CheckpointResult> {
        // Refresh so planner state reflects the latest on-disk WAL shape.
        self.wal.refresh(cx)?;
        self.refresh_before_append = true;

        let frame_count = self.wal.frame_count();
        debug_assert!(
            frame_count <= u32::MAX as usize,
            "WAL frame count {frame_count} exceeds u32::MAX; checkpoint state will be wrong"
        );
        let total_frames = u32::try_from(frame_count).unwrap_or(u32::MAX);

        // Build checkpoint state for the planner.
        let state = CheckpointState {
            total_frames,
            backfilled_frames,
            oldest_reader_frame,
        };

        // Wrap the CheckpointPageWriter in a CheckpointTargetAdapter.
        let mut target = CheckpointTargetAdapterRef { writer };

        // Execute the checkpoint.
        let result = execute_checkpoint(cx, &mut self.wal, to_wal_mode(mode), state, &mut target)?;

        // Checkpoint-aware FEC lifecycle: once frames are backfilled to the
        // database file, their FEC symbols are no longer needed.
        // TODO: This clears ALL pending symbols regardless of how many frames
        // were actually backfilled.  For partial (passive) checkpoints where
        // readers block full backfill, symbols for non-backfilled frames are
        // lost.
        if result.frames_backfilled > 0 {
            let drained = self.fec_pending.len();
            self.fec_pending.clear();
            self.fec_pending.shrink_to(256);
            if drained > 0 {
                debug!(
                    drained_groups = drained,
                    frames_backfilled = result.frames_backfilled,
                    "FEC symbols reclaimed after checkpoint"
                );
            }
        }

        // If the WAL was fully reset, also discard any buffered FEC pages
        // and invalidate the page index.
        if result.wal_was_reset {
            self.fec_discard();
            if !self.fec_pending.is_empty() {
                self.fec_pending.clear();
                self.fec_pending.shrink_to(256);
            }
            self.invalidate_page_index();
            self.index_salts = self.wal.header().salts;
        }

        Ok(CheckpointResult {
            total_frames,
            frames_backfilled: result.frames_backfilled,
            completed: result.plan.completes_checkpoint(),
            wal_was_reset: result.wal_was_reset,
        })
    }
}

/// Adapter wrapping a `&mut dyn CheckpointPageWriter` to implement `CheckpointTarget`.
///
/// This is used internally by `WalBackendAdapter::checkpoint` to bridge the
/// pager's writer to the WAL executor's target trait.
struct CheckpointTargetAdapterRef<'a> {
    writer: &'a mut dyn CheckpointPageWriter,
}

impl CheckpointTarget for CheckpointTargetAdapterRef<'_> {
    #[inline]
    fn write_page(&mut self, cx: &Cx, page_no: PageNumber, data: &[u8]) -> Result<()> {
        self.writer.write_page(cx, page_no, data)
    }

    #[inline]
    fn truncate_db(&mut self, cx: &Cx, n_pages: u32) -> Result<()> {
        self.writer.truncate(cx, n_pages)
    }

    #[inline]
    fn sync_db(&mut self, cx: &Cx) -> Result<()> {
        self.writer.sync(cx)
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use fsqlite_pager::MockCheckpointPageWriter;
    use fsqlite_types::flags::VfsOpenFlags;
    use fsqlite_vfs::MemoryVfs;
    use fsqlite_vfs::traits::Vfs;
    use fsqlite_wal::checksum::WalSalts;

    use super::*;

    const PAGE_SIZE: u32 = 4096;

    fn test_cx() -> Cx {
        Cx::default()
    }

    fn test_salts() -> WalSalts {
        WalSalts {
            salt1: 0xDEAD_BEEF,
            salt2: 0xCAFE_BABE,
        }
    }

    fn sample_page(seed: u8) -> Vec<u8> {
        let page_size = usize::try_from(PAGE_SIZE).expect("page size fits usize");
        let mut page = vec![0u8; page_size];
        for (i, byte) in page.iter_mut().enumerate() {
            let reduced = u8::try_from(i % 251).expect("modulo fits u8");
            *byte = reduced ^ seed;
        }
        page
    }

    fn open_wal_file(vfs: &MemoryVfs, cx: &Cx) -> <MemoryVfs as Vfs>::File {
        let flags = VfsOpenFlags::READWRITE | VfsOpenFlags::CREATE | VfsOpenFlags::WAL;
        let (file, _) = vfs
            .open(cx, Some(std::path::Path::new("test.db-wal")), flags)
            .expect("open WAL file");
        file
    }

    fn make_adapter(vfs: &MemoryVfs, cx: &Cx) -> WalBackendAdapter<<MemoryVfs as Vfs>::File> {
        let file = open_wal_file(vfs, cx);
        let wal = WalFile::create(cx, file, PAGE_SIZE, 0, test_salts()).expect("create WAL");
        WalBackendAdapter::new(wal)
    }

    // -- WalBackendAdapter tests --

    #[test]
    fn test_adapter_append_and_frame_count() {
        let cx = test_cx();
        let vfs = MemoryVfs::new();
        let mut adapter = make_adapter(&vfs, &cx);

        assert_eq!(adapter.frame_count(), 0);

        let page = sample_page(0x42);
        adapter
            .append_frame(&cx, 1, &page, 0)
            .expect("append frame");
        assert_eq!(adapter.frame_count(), 1);

        adapter
            .append_frame(&cx, 2, &sample_page(0x43), 2)
            .expect("append commit frame");
        assert_eq!(adapter.frame_count(), 2);
    }

    #[test]
    fn test_adapter_read_page_found() {
        let cx = test_cx();
        let vfs = MemoryVfs::new();
        let mut adapter = make_adapter(&vfs, &cx);

        let page1 = sample_page(0x10);
        let page2 = sample_page(0x20);
        adapter.append_frame(&cx, 1, &page1, 0).expect("append");
        adapter
            .append_frame(&cx, 2, &page2, 2)
            .expect("append commit");

        let result = adapter.read_page(&cx, 1).expect("read page 1");
        assert_eq!(result, Some(page1));

        let result = adapter.read_page(&cx, 2).expect("read page 2");
        assert_eq!(result, Some(page2));
    }

    #[test]
    fn test_adapter_read_page_not_found() {
        let cx = test_cx();
        let vfs = MemoryVfs::new();
        let mut adapter = make_adapter(&vfs, &cx);

        adapter
            .append_frame(&cx, 1, &sample_page(0x10), 1)
            .expect("append");

        let result = adapter.read_page(&cx, 99).expect("read missing page");
        assert_eq!(result, None);
    }

    #[test]
    fn test_adapter_read_page_returns_latest_version() {
        let cx = test_cx();
        let vfs = MemoryVfs::new();
        let mut adapter = make_adapter(&vfs, &cx);

        let old_data = sample_page(0xAA);
        let new_data = sample_page(0xBB);

        // Write page 5 twice — the adapter should return the latest.
        adapter
            .append_frame(&cx, 5, &old_data, 0)
            .expect("append old");
        adapter
            .append_frame(&cx, 5, &new_data, 1)
            .expect("append new (commit)");

        let result = adapter.read_page(&cx, 5).expect("read page 5");
        assert_eq!(
            result,
            Some(new_data),
            "adapter should return the latest WAL version"
        );
    }

    #[test]
    fn test_adapter_refreshes_cross_handle_visibility_and_append_position() {
        let cx = test_cx();
        let vfs = MemoryVfs::new();

        let file1 = open_wal_file(&vfs, &cx);
        let wal1 = WalFile::create(&cx, file1, PAGE_SIZE, 0, test_salts()).expect("create WAL");
        let mut adapter1 = WalBackendAdapter::new(wal1);

        let file2 = open_wal_file(&vfs, &cx);
        let wal2 = WalFile::open(&cx, file2).expect("open WAL");
        let mut adapter2 = WalBackendAdapter::new(wal2);

        let page1 = sample_page(0x11);
        adapter1
            .append_frame(&cx, 1, &page1, 1)
            .expect("adapter1 append commit");
        adapter1.sync(&cx).expect("adapter1 sync");
        adapter2
            .begin_transaction(&cx)
            .expect("adapter2 begin transaction");
        assert_eq!(
            adapter2.read_page(&cx, 1).expect("adapter2 read page1"),
            Some(page1.clone()),
            "adapter2 should observe adapter1 commit at transaction begin"
        );

        let page2 = sample_page(0x22);
        adapter2
            .append_frame(&cx, 2, &page2, 2)
            .expect("adapter2 append commit");
        adapter2.sync(&cx).expect("adapter2 sync");
        adapter1
            .begin_transaction(&cx)
            .expect("adapter1 begin transaction");
        assert_eq!(
            adapter1.read_page(&cx, 2).expect("adapter1 read page2"),
            Some(page2.clone()),
            "adapter1 should observe adapter2 commit at transaction begin"
        );

        // Ensure the second writer appended to frame 1 (not frame 0 overwrite).
        assert_eq!(
            adapter1.frame_count(),
            2,
            "shared WAL should contain both commit frames"
        );
        assert_eq!(
            adapter2.frame_count(),
            2,
            "shared WAL should contain both commit frames"
        );
    }

    #[test]
    fn test_adapter_read_page_hides_uncommitted_frames() {
        let cx = test_cx();
        let vfs = MemoryVfs::new();
        let mut adapter = make_adapter(&vfs, &cx);

        let committed = sample_page(0x31);
        let uncommitted = sample_page(0x32);

        adapter
            .append_frame(&cx, 7, &committed, 7)
            .expect("append committed frame");
        adapter
            .append_frame(&cx, 7, &uncommitted, 0)
            .expect("append uncommitted frame");

        let result = adapter.read_page(&cx, 7).expect("read committed page");
        assert_eq!(
            result,
            Some(committed),
            "reader must ignore uncommitted tail frames"
        );
    }

    #[test]
    fn test_adapter_read_page_none_when_wal_has_no_commit_frame() {
        let cx = test_cx();
        let vfs = MemoryVfs::new();
        let mut adapter = make_adapter(&vfs, &cx);

        adapter
            .append_frame(&cx, 3, &sample_page(0x44), 0)
            .expect("append uncommitted frame");

        let result = adapter.read_page(&cx, 3).expect("read page");
        assert_eq!(result, None, "uncommitted WAL frames must stay invisible");
    }

    #[test]
    fn test_adapter_read_page_empty_wal() {
        let cx = test_cx();
        let vfs = MemoryVfs::new();
        let mut adapter = make_adapter(&vfs, &cx);

        let result = adapter.read_page(&cx, 1).expect("read from empty WAL");
        assert_eq!(result, None);
    }

    #[test]
    fn test_adapter_sync() {
        let cx = test_cx();
        let vfs = MemoryVfs::new();
        let mut adapter = make_adapter(&vfs, &cx);

        adapter
            .append_frame(&cx, 1, &sample_page(0), 1)
            .expect("append");
        adapter.sync(&cx).expect("sync should not fail");
    }

    #[test]
    fn test_adapter_into_inner_round_trip() {
        let cx = test_cx();
        let vfs = MemoryVfs::new();
        let mut adapter = make_adapter(&vfs, &cx);

        adapter
            .append_frame(&cx, 1, &sample_page(0), 1)
            .expect("append");

        assert_eq!(adapter.inner().frame_count(), 1);

        let wal = adapter.into_inner();
        assert_eq!(wal.frame_count(), 1);
    }

    #[test]
    fn test_adapter_as_dyn_wal_backend() {
        let cx = test_cx();
        let vfs = MemoryVfs::new();
        let mut adapter = make_adapter(&vfs, &cx);

        // Verify it can be used as a trait object.
        let backend: &mut dyn WalBackend = &mut adapter;
        backend
            .append_frame(&cx, 1, &sample_page(0x77), 1)
            .expect("append via dyn");
        assert_eq!(backend.frame_count(), 1);

        let page = backend.read_page(&cx, 1).expect("read via dyn");
        assert_eq!(page, Some(sample_page(0x77)));
    }

    #[test]
    fn test_adapter_index_rebuilds_after_wal_reset() {
        let cx = test_cx();
        let vfs = MemoryVfs::new();
        let mut adapter = make_adapter(&vfs, &cx);

        // Write and commit page 1.
        let original = sample_page(0xAA);
        adapter
            .append_frame(&cx, 1, &original, 1)
            .expect("append committed");

        // Force-read to build the index.
        let result = adapter.read_page(&cx, 1).expect("first read");
        assert_eq!(result, Some(original.clone()));

        // Simulate WAL reset: reset the WAL with new salts.
        let new_salts = WalSalts {
            salt1: 0x1111_1111,
            salt2: 0x2222_2222,
        };
        adapter
            .inner_mut()
            .reset(&cx, 1, new_salts)
            .expect("reset WAL");

        // Write a different page 1 in the new WAL generation.
        let updated = sample_page(0xBB);
        adapter
            .append_frame(&cx, 1, &updated, 1)
            .expect("append in new generation");

        // The adapter must detect the salt change and rebuild the index,
        // returning the new data — not the stale cached version.
        let result = adapter.read_page(&cx, 1).expect("read after reset");
        assert_eq!(
            result,
            Some(updated),
            "adapter must detect WAL generation change and return fresh data"
        );
    }

    // -- CheckpointTargetAdapterRef tests --

    #[test]
    fn test_checkpoint_adapter_write_page() {
        let cx = test_cx();
        let mut writer = MockCheckpointPageWriter;
        let mut adapter = CheckpointTargetAdapterRef {
            writer: &mut writer,
        };

        let page_no = PageNumber::new(1).expect("valid page number");
        adapter
            .write_page(&cx, page_no, &[0u8; 4096])
            .expect("write_page");
    }

    #[test]
    fn test_checkpoint_adapter_truncate_db() {
        let cx = test_cx();
        let mut writer = MockCheckpointPageWriter;
        let mut adapter = CheckpointTargetAdapterRef {
            writer: &mut writer,
        };

        adapter.truncate_db(&cx, 10).expect("truncate_db");
    }

    #[test]
    fn test_checkpoint_adapter_sync_db() {
        let cx = test_cx();
        let mut writer = MockCheckpointPageWriter;
        let mut adapter = CheckpointTargetAdapterRef {
            writer: &mut writer,
        };

        adapter.sync_db(&cx).expect("sync_db");
    }

    #[test]
    fn test_checkpoint_adapter_as_dyn_target() {
        let cx = test_cx();
        let mut writer = MockCheckpointPageWriter;
        let mut adapter = CheckpointTargetAdapterRef {
            writer: &mut writer,
        };

        // Verify it can be used as a trait object.
        let target: &mut dyn CheckpointTarget = &mut adapter;
        let page_no = PageNumber::new(3).expect("valid page number");
        target
            .write_page(&cx, page_no, &[0u8; 4096])
            .expect("write via dyn");
        target.truncate_db(&cx, 5).expect("truncate via dyn");
        target.sync_db(&cx).expect("sync via dyn");
    }
}
