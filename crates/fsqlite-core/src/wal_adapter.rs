//! Adapters bridging the WAL and pager crates at runtime.
//!
//! These adapters break the circular dependency between `fsqlite-pager` and
//! `fsqlite-wal`:
//!
//! - [`WalBackendAdapter`] wraps `WalFile` to satisfy the pager's
//!   [`WalBackend`] trait (pager -> WAL direction).
//! - [`CheckpointTargetAdapterRef`] wraps `CheckpointPageWriter` to satisfy the
//!   WAL executor's [`CheckpointTarget`] trait (WAL -> pager direction).

use std::collections::HashMap;

use fsqlite_error::{FrankenError, Result};
use fsqlite_pager::{CheckpointMode, CheckpointPageWriter, CheckpointResult, WalBackend};
use fsqlite_types::PageNumber;
use fsqlite_types::cx::Cx;
use fsqlite_types::flags::SyncFlags;
use fsqlite_vfs::VfsFile;
use fsqlite_wal::checksum::WalSalts;
use fsqlite_wal::{
    CheckpointMode as WalCheckpointMode, CheckpointState, CheckpointTarget, WalFile,
    execute_checkpoint,
};
use tracing::{debug, warn};

use crate::wal_fec_adapter::{FecCommitHook, FecCommitResult};

// ---------------------------------------------------------------------------
// WalBackendAdapter: WalFile -> WalBackend
// ---------------------------------------------------------------------------

/// Adapter wrapping [`WalFile`] to implement the pager's [`WalBackend`] trait.
///
/// The pager calls `dyn WalBackend` during WAL-mode commits and page reads.
/// This adapter delegates those calls to the concrete `WalFile<F>` from
/// `fsqlite-wal`.
/// Maximum number of entries in the page index before we stop growing it.
/// This caps memory usage at roughly 64K * (4 + 8) = ~768 KB.
const PAGE_INDEX_MAX_ENTRIES: usize = 65_536;

pub struct WalBackendAdapter<F: VfsFile> {
    wal: WalFile<F>,
    /// Guard so commit-time append refresh runs only once per commit batch.
    refresh_before_append: bool,
    /// Whether read visibility is pinned to a transaction-bounded snapshot.
    read_snapshot_pinned: bool,
    /// Highest committed frame visible to the current read snapshot.
    ///
    /// `Some(idx)` means committed frames `0..=idx` are visible.
    /// `None` means the snapshot saw no committed frame.
    read_snapshot_last_commit: Option<usize>,
    /// Optional FEC commit hook for encoding repair symbols on commit.
    fec_hook: Option<FecCommitHook>,
    /// Accumulated FEC commit results (for later sidecar persistence).
    fec_pending: Vec<FecCommitResult>,
    /// O(1) page lookup index: page_number -> most recent frame index.
    page_index: HashMap<u32, usize>,
    /// Last committed frame index the page index has been built up to (inclusive).
    /// `None` means the index has never been built.
    index_built_to: Option<usize>,
    /// WAL generation salts at the time the index was built, used to detect resets.
    index_salts: Option<WalSalts>,
    /// `true` when the page index hit the capacity cap and some pages were not
    /// indexed.  When this is set, a miss in the HashMap cannot be trusted ---
    /// the page may exist in the WAL but simply wasn't indexed.  In that case,
    /// `read_page` falls back to a backwards linear scan.
    index_is_partial: bool,
    /// Maximum number of unique pages the index will track.  Defaults to
    /// [`PAGE_INDEX_MAX_ENTRIES`].  Overridable in tests to exercise the
    /// partial-index fallback path without writing 64K+ frames.
    page_index_cap: usize,
}

impl<F: VfsFile> WalBackendAdapter<F> {
    /// Wrap an existing [`WalFile`] in the adapter (FEC disabled).
    #[must_use]
    pub fn new(wal: WalFile<F>) -> Self {
        Self {
            wal,
            refresh_before_append: true,
            read_snapshot_pinned: false,
            read_snapshot_last_commit: None,
            fec_hook: None,
            fec_pending: Vec::new(),
            page_index: HashMap::new(),
            index_built_to: None,
            index_salts: None,
            index_is_partial: false,
            page_index_cap: PAGE_INDEX_MAX_ENTRIES,
        }
    }

    /// Wrap an existing [`WalFile`] with an FEC commit hook.
    #[must_use]
    pub fn with_fec_hook(wal: WalFile<F>, hook: FecCommitHook) -> Self {
        Self {
            wal,
            refresh_before_append: true,
            read_snapshot_pinned: false,
            read_snapshot_last_commit: None,
            fec_hook: Some(hook),
            fec_pending: Vec::new(),
            page_index: HashMap::new(),
            index_built_to: None,
            index_salts: None,
            index_is_partial: false,
            page_index_cap: PAGE_INDEX_MAX_ENTRIES,
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
    /// Invalidates the page index since the caller may mutate WAL state.
    pub fn inner_mut(&mut self) -> &mut WalFile<F> {
        self.invalidate_page_index();
        &mut self.wal
    }

    /// Discard the page index, forcing a full rebuild on next `read_page`.
    fn invalidate_page_index(&mut self) {
        self.page_index.clear();
        self.index_built_to = None;
        self.index_salts = None;
        self.index_is_partial = false;
    }

    /// Ensure the page index covers all committed frames up to `last_commit_frame`.
    ///
    /// Performs incremental extension when possible, or a full rebuild when WAL
    /// salts change or the commit horizon shrinks (e.g., after checkpoint reset).
    fn ensure_page_index(&mut self, cx: &Cx, last_commit_frame: usize) -> Result<()> {
        let current_salts = self.wal.header().salts;

        // Detect WAL generation change (salts differ -> full rebuild).
        let needs_full_rebuild = match self.index_salts {
            Some(saved) => saved != current_salts,
            None => true,
        };

        if needs_full_rebuild {
            self.page_index.clear();
            self.index_built_to = None;
            self.index_is_partial = false;
            self.index_salts = Some(current_salts);
            self.build_index_range(cx, 0, last_commit_frame)?;
            self.index_built_to = Some(last_commit_frame);
            return Ok(());
        }

        match self.index_built_to {
            Some(built_to) if built_to == last_commit_frame => {
                // Already up to date.
                Ok(())
            }
            Some(built_to) if built_to < last_commit_frame => {
                // Incremental extend: scan only the new frames.
                self.build_index_range(cx, built_to + 1, last_commit_frame)?;
                self.index_built_to = Some(last_commit_frame);
                Ok(())
            }
            Some(_) => {
                // WAL shrank (e.g., after checkpoint reset) -> full rebuild.
                self.page_index.clear();
                self.index_is_partial = false;
                self.build_index_range(cx, 0, last_commit_frame)?;
                self.index_built_to = Some(last_commit_frame);
                Ok(())
            }
            None => {
                // First build.
                self.build_index_range(cx, 0, last_commit_frame)?;
                self.index_built_to = Some(last_commit_frame);
                Ok(())
            }
        }
    }

    /// Scan frame headers from `start..=end` (inclusive) and populate the page index.
    ///
    /// Since we scan forward, later frames naturally overwrite earlier entries
    /// for the same page number, ensuring "newest frame wins" semantics.
    fn build_index_range(&mut self, cx: &Cx, start: usize, end: usize) -> Result<()> {
        for frame_index in start..=end {
            let header = self.wal.read_frame_header(cx, frame_index)?;
            // Only insert if we haven't hit the capacity cap, or if this page
            // is already tracked (update is free).
            if self.page_index.len() < self.page_index_cap
                || self.page_index.contains_key(&header.page_number)
            {
                self.page_index.insert(header.page_number, frame_index);
            } else {
                // A page was dropped because the index is full -- mark it as
                // partial so that `read_page` knows a HashMap miss cannot be
                // trusted and must fall back to a linear scan.
                self.index_is_partial = true;
            }
        }
        Ok(())
    }

    /// Backwards linear scan of committed frames to find a page that was not
    /// captured by the capped page index.
    ///
    /// Scans from `last_commit_frame` down to frame 0 and returns the index
    /// of the first (i.e., most recent) frame containing `page_number`, or
    /// `None` if the page is not in the WAL at all.
    fn scan_backwards_for_page(
        &mut self,
        cx: &Cx,
        page_number: u32,
        last_commit_frame: usize,
    ) -> Result<Option<usize>> {
        for frame_index in (0..=last_commit_frame).rev() {
            let header = self.wal.read_frame_header(cx, frame_index)?;
            if header.page_number == page_number {
                return Ok(Some(frame_index));
            }
        }
        Ok(None)
    }

    /// Take any pending FEC commit results for sidecar persistence.
    pub fn take_fec_pending(&mut self) -> Vec<FecCommitResult> {
        std::mem::take(&mut self.fec_pending)
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

    /// Override the page index capacity (for testing only).
    #[cfg(test)]
    fn set_page_index_cap(&mut self, cap: usize) {
        self.page_index_cap = cap;
        // Invalidate so the next read rebuilds with the new cap.
        self.invalidate_page_index();
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
        self.read_snapshot_last_commit = self.wal.last_commit_frame(cx)?;
        self.read_snapshot_pinned = true;
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
                    // FEC encoding failure is non-fatal -- log and continue.
                    warn!(error = %e, "FEC encoding failed; commit proceeds without repair symbols");
                }
            }
        }

        Ok(())
    }

    fn read_page(&mut self, cx: &Cx, page_number: u32) -> Result<Option<Vec<u8>>> {
        // Restrict visibility to committed frames only.  If a transaction
        // snapshot is pinned, keep the commit horizon stable until the next
        // begin_transaction() call.
        let last_commit_frame = if self.read_snapshot_pinned {
            let Some(pinned) = self.read_snapshot_last_commit else {
                return Ok(None);
            };
            pinned
        } else {
            let Some(current) = self.wal.last_commit_frame(cx)? else {
                return Ok(None);
            };
            current
        };

        // Build or extend the O(1) page index covering all committed frames.
        self.ensure_page_index(cx, last_commit_frame)?;

        // O(1) lookup: page_number -> most recent frame index.
        let frame_index = match self.page_index.get(&page_number) {
            Some(&idx) => idx,
            None if !self.index_is_partial => {
                // The index covers every page in the WAL -- a miss here means
                // the page genuinely isn't in the WAL.
                return Ok(None);
            }
            None => {
                // The index is partial (capacity cap was hit).  A HashMap miss
                // might be a false negative -- fall back to a backwards linear
                // scan of committed frames to be safe.
                debug!(
                    page_number,
                    "WAL adapter: index miss with partial index, falling back to linear scan"
                );
                match self.scan_backwards_for_page(cx, page_number, last_commit_frame)? {
                    Some(idx) => idx,
                    None => return Ok(None),
                }
            }
        };

        // Read the frame data at the resolved position.
        let mut frame_buf = vec![0u8; self.wal.frame_size()];
        let header = self.wal.read_frame_into(cx, frame_index, &mut frame_buf)?;

        // Runtime integrity check: verify the frame actually contains our page.
        // This guards against index corruption or stale entries.
        if header.page_number != page_number {
            return Err(FrankenError::WalCorrupt {
                detail: format!(
                    "WAL page index integrity failure: expected page {page_number} \
                     at frame {frame_index}, found page {}",
                    header.page_number
                ),
            });
        }

        let data = frame_buf[fsqlite_wal::checksum::WAL_FRAME_HEADER_SIZE..].to_vec();
        debug!(
            page_number,
            frame_index, "WAL adapter: page found in WAL via index"
        );
        Ok(Some(data))
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
        let total_frames = u32::try_from(self.wal.frame_count()).unwrap_or(u32::MAX);

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
        // database file, their FEC symbols are no longer needed.  Clear
        // pending FEC results for the checkpointed range.
        if result.frames_backfilled > 0 {
            let drained = self.fec_pending.len();
            self.fec_pending.clear();
            if drained > 0 {
                debug!(
                    drained_groups = drained,
                    frames_backfilled = result.frames_backfilled,
                    "FEC symbols reclaimed after checkpoint"
                );
            }
        }

        // If the WAL was fully reset, also discard any buffered FEC pages
        // and invalidate the page index (salts changed).
        if result.wal_was_reset {
            self.fec_discard();
            self.invalidate_page_index();
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
    fn write_page(&mut self, cx: &Cx, page_no: PageNumber, data: &[u8]) -> Result<()> {
        self.writer.write_page(cx, page_no, data)
    }

    fn truncate_db(&mut self, cx: &Cx, n_pages: u32) -> Result<()> {
        self.writer.truncate(cx, n_pages)
    }

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

        // Write page 5 twice -- the adapter should return the latest.
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
    fn test_adapter_pins_read_snapshot_until_next_begin() {
        let cx = test_cx();
        let vfs = MemoryVfs::new();

        let file_writer = open_wal_file(&vfs, &cx);
        let wal_writer =
            WalFile::create(&cx, file_writer, PAGE_SIZE, 0, test_salts()).expect("create WAL");
        let mut writer = WalBackendAdapter::new(wal_writer);

        let file_reader = open_wal_file(&vfs, &cx);
        let wal_reader = WalFile::open(&cx, file_reader).expect("open WAL");
        let mut reader = WalBackendAdapter::new(wal_reader);

        let v1 = sample_page(0x41);
        writer.append_frame(&cx, 3, &v1, 3).expect("append v1");
        writer.sync(&cx).expect("sync v1");

        reader
            .begin_transaction(&cx)
            .expect("begin reader snapshot 1");
        assert_eq!(
            reader.read_page(&cx, 3).expect("reader sees v1"),
            Some(v1.clone())
        );

        let v2 = sample_page(0x42);
        writer.append_frame(&cx, 3, &v2, 3).expect("append v2");
        writer.sync(&cx).expect("sync v2");

        // Same transaction snapshot must stay stable (no mid-transaction drift).
        assert_eq!(
            reader
                .read_page(&cx, 3)
                .expect("reader remains on pinned snapshot"),
            Some(v1.clone())
        );

        // A new transaction snapshot should pick up the latest commit.
        reader
            .begin_transaction(&cx)
            .expect("begin reader snapshot 2");
        assert_eq!(reader.read_page(&cx, 3).expect("reader sees v2"), Some(v2));
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

    // -- Page index O(1) lookup tests --

    #[test]
    fn test_page_index_returns_correct_data() {
        // Write several pages, verify O(1) index returns the right data.
        let cx = test_cx();
        let vfs = MemoryVfs::new();
        let mut adapter = make_adapter(&vfs, &cx);

        let page1 = sample_page(0x01);
        let page2 = sample_page(0x02);
        let page3 = sample_page(0x03);

        adapter.append_frame(&cx, 1, &page1, 0).expect("append");
        adapter.append_frame(&cx, 2, &page2, 0).expect("append");
        adapter
            .append_frame(&cx, 3, &page3, 3)
            .expect("append commit");

        // All three pages should be readable via the index.
        assert_eq!(adapter.read_page(&cx, 1).expect("read"), Some(page1));
        assert_eq!(adapter.read_page(&cx, 2).expect("read"), Some(page2));
        assert_eq!(adapter.read_page(&cx, 3).expect("read"), Some(page3));

        // Non-existent page returns None.
        assert_eq!(adapter.read_page(&cx, 99).expect("read"), None);
    }

    #[test]
    fn test_page_index_returns_latest_version() {
        // Write the same page twice; the index should point to the newer frame.
        let cx = test_cx();
        let vfs = MemoryVfs::new();
        let mut adapter = make_adapter(&vfs, &cx);

        let old_data = sample_page(0xAA);
        let new_data = sample_page(0xBB);

        adapter
            .append_frame(&cx, 5, &old_data, 0)
            .expect("append old");
        adapter
            .append_frame(&cx, 5, &new_data, 1)
            .expect("append new (commit)");

        assert_eq!(
            adapter.read_page(&cx, 5).expect("read"),
            Some(new_data),
            "page index must return the latest frame for a page"
        );
    }

    #[test]
    fn test_page_index_invalidated_on_wal_reset() {
        // Simulate a WAL reset with new salts. The index must be rebuilt so
        // stale entries from the old generation are not returned.
        let cx = test_cx();
        let vfs = MemoryVfs::new();
        let mut adapter = make_adapter(&vfs, &cx);

        let old_data = sample_page(0x11);
        adapter
            .append_frame(&cx, 1, &old_data, 1)
            .expect("append commit");

        // Read page 1 to populate the index.
        assert_eq!(adapter.read_page(&cx, 1).expect("read old"), Some(old_data));

        // Reset WAL with new salts (simulates checkpoint reset).
        let new_salts = WalSalts {
            salt1: 0xAAAA_BBBB,
            salt2: 0xCCCC_DDDD,
        };
        adapter
            .inner_mut()
            .reset(&cx, 1, new_salts)
            .expect("WAL reset");

        // Write new data for the same page number in the new generation.
        let new_data = sample_page(0x22);
        adapter
            .append_frame(&cx, 1, &new_data, 1)
            .expect("append new generation commit");

        // The index must have been invalidated; we should get the new data.
        let result = adapter.read_page(&cx, 1).expect("read after reset");
        assert_eq!(
            result,
            Some(new_data),
            "after WAL reset, page index must return new-generation data, not stale cached data"
        );

        // A page that existed only in the old generation should be gone.
        let old_only = sample_page(0x33);
        // (We never wrote page 99 in the new generation.)
        assert_eq!(
            adapter.read_page(&cx, 99).expect("read non-existent"),
            None,
            "pages from old WAL generation must not appear after reset"
        );
        // Suppress unused variable warning.
        drop(old_only);
    }

    #[test]
    fn test_page_index_incremental_extend() {
        // Verify that the index extends incrementally when new frames are committed.
        let cx = test_cx();
        let vfs = MemoryVfs::new();
        let mut adapter = make_adapter(&vfs, &cx);

        let page1 = sample_page(0x10);
        adapter
            .append_frame(&cx, 1, &page1, 1)
            .expect("append commit 1");

        // First read builds the index.
        assert_eq!(
            adapter.read_page(&cx, 1).expect("read"),
            Some(page1.clone())
        );

        // Append more committed frames.
        let page2 = sample_page(0x20);
        let page1_v2 = sample_page(0x30);
        adapter
            .append_frame(&cx, 2, &page2, 0)
            .expect("append page 2");
        adapter
            .append_frame(&cx, 1, &page1_v2, 3)
            .expect("append page 1 v2 (commit)");

        // Reading should trigger incremental extend, not full rebuild.
        assert_eq!(
            adapter.read_page(&cx, 1).expect("read page 1 v2"),
            Some(page1_v2),
            "incremental index extend should pick up the updated page"
        );
        assert_eq!(adapter.read_page(&cx, 2).expect("read page 2"), Some(page2));
    }

    // -- Partial index fallback tests --

    #[test]
    fn test_partial_index_falls_back_to_linear_scan() {
        // Verify that when the page index cap is hit, pages that weren't
        // indexed are still found via the backwards linear scan fallback.
        let cx = test_cx();
        let vfs = MemoryVfs::new();
        let mut adapter = make_adapter(&vfs, &cx);

        // Set a very small cap so we can trigger the partial-index path
        // with just a handful of frames.
        adapter.set_page_index_cap(2);

        // Write 5 distinct pages.  With a cap of 2, only the first 2 unique
        // pages will be indexed; pages 3-5 will be dropped from the index.
        let p1 = sample_page(0x01);
        let p2 = sample_page(0x02);
        let p3 = sample_page(0x03);
        let p4 = sample_page(0x04);
        let p5 = sample_page(0x05);

        adapter.append_frame(&cx, 1, &p1, 0).expect("append p1");
        adapter.append_frame(&cx, 2, &p2, 0).expect("append p2");
        adapter.append_frame(&cx, 3, &p3, 0).expect("append p3");
        adapter.append_frame(&cx, 4, &p4, 0).expect("append p4");
        adapter
            .append_frame(&cx, 5, &p5, 5)
            .expect("append p5 (commit)");

        // Pages 1 and 2 should be in the index (fast path).
        assert_eq!(
            adapter.read_page(&cx, 1).expect("read p1"),
            Some(p1),
            "indexed page should be found via HashMap"
        );
        assert_eq!(
            adapter.read_page(&cx, 2).expect("read p2"),
            Some(p2),
            "indexed page should be found via HashMap"
        );

        // Pages 3-5 were NOT indexed, but must still be found via the
        // backwards linear scan fallback.
        assert_eq!(
            adapter.read_page(&cx, 3).expect("read p3"),
            Some(p3),
            "non-indexed page must be found via linear scan fallback"
        );
        assert_eq!(
            adapter.read_page(&cx, 4).expect("read p4"),
            Some(p4),
            "non-indexed page must be found via linear scan fallback"
        );
        assert_eq!(
            adapter.read_page(&cx, 5).expect("read p5"),
            Some(p5),
            "non-indexed page must be found via linear scan fallback"
        );

        // A page that was never written should still return None.
        assert_eq!(
            adapter.read_page(&cx, 99).expect("read non-existent"),
            None,
            "non-existent page must return None even with partial index"
        );

        // Verify the index was indeed marked partial.
        assert!(
            adapter.index_is_partial,
            "index_is_partial should be true when cap is exceeded"
        );
    }

    #[test]
    fn test_partial_index_returns_latest_version_via_fallback() {
        // When the same page appears multiple times and overflows the index,
        // the backwards scan must return the LATEST (highest frame index)
        // version, not the first one it encounters in a forward scan.
        let cx = test_cx();
        let vfs = MemoryVfs::new();
        let mut adapter = make_adapter(&vfs, &cx);

        // Cap at 1 so only page 1 fits in the index.
        adapter.set_page_index_cap(1);

        let old_p2 = sample_page(0xAA);
        let new_p2 = sample_page(0xBB);

        // Frame 0: page 1 (indexed)
        adapter
            .append_frame(&cx, 1, &sample_page(0x01), 0)
            .expect("append p1");
        // Frame 1: page 2 old version (NOT indexed -- cap exceeded)
        adapter
            .append_frame(&cx, 2, &old_p2, 0)
            .expect("append p2 old");
        // Frame 2: page 2 new version (NOT indexed -- cap exceeded, and
        // page 2 is not already in the index so it won't be updated)
        adapter
            .append_frame(&cx, 2, &new_p2, 3)
            .expect("append p2 new (commit)");

        // The backwards scan from frame 2 should find the newest version first.
        assert_eq!(
            adapter.read_page(&cx, 2).expect("read p2"),
            Some(new_p2),
            "backwards scan must return the most recent frame for the page"
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
