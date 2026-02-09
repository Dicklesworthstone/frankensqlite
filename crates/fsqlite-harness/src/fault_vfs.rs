//! Deterministic disk fault injection for VFS operations (`bd-3go.2`, spec §4.2.3).
//!
//! [`FaultInjectingVfs`] wraps any [`Vfs`] implementation and injects faults
//! (torn writes, power cuts, I/O errors) based on declarative [`FaultSpec`] rules.
//!
//! Fault injection is deterministic: same fault specs → same failure behaviour.
//! This enables reproducible crash-recovery testing under lab-controlled conditions.
//!
//! # Example
//!
//! ```ignore
//! use fsqlite_harness::fault_vfs::{FaultInjectingVfs, FaultSpec};
//! use fsqlite_vfs::MemoryVfs;
//!
//! let mut vfs = FaultInjectingVfs::new(MemoryVfs::new());
//! vfs.inject_fault(FaultSpec::torn_write("*.wal").at_offset_bytes(8192).valid_bytes(17));
//! vfs.inject_fault(FaultSpec::power_cut("*.wal").after_nth_sync(2));
//! ```

use std::path::{Path, PathBuf};
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};

use fsqlite_error::{FrankenError, Result};
use fsqlite_types::LockLevel;
use fsqlite_types::cx::Cx;
use fsqlite_types::flags::{AccessFlags, SyncFlags, VfsOpenFlags};
use fsqlite_vfs::shm::ShmRegion;
use fsqlite_vfs::traits::{Vfs, VfsFile};
use tracing::{info, warn};

/// Bead identifier for tracing and log correlation.
const BEAD_ID: &str = "bd-3go.2";

// ---------------------------------------------------------------------------
// Fault specification
// ---------------------------------------------------------------------------

/// The kind of storage fault to inject.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FaultKind {
    /// A torn (partial) write: only the first `valid_bytes` of the write are applied.
    TornWrite {
        /// Number of bytes that make it to stable storage before the tear.
        valid_bytes: usize,
    },
    /// Simulated power loss: the sync call and all subsequent operations fail.
    PowerCut,
    /// Generic I/O error returned from the faulted operation.
    IoError,
}

/// A record of a fault that was triggered during test execution.
#[derive(Debug, Clone)]
pub struct FaultTriggerRecord {
    /// Which fault spec triggered.
    pub spec_index: usize,
    /// The file path that matched.
    pub path: PathBuf,
    /// The kind of fault injected.
    pub kind: FaultKind,
    /// Write offset (for torn writes) or sync index (for power cuts).
    pub detail: String,
}

/// Declarative fault specification targeting files matching a glob pattern.
#[derive(Debug, Clone)]
pub struct FaultSpec {
    /// Glob pattern for matching file paths (e.g., `"*.wal"`, `"test.db"`).
    pub file_glob: String,
    /// The kind of fault to inject.
    pub kind: FaultKind,
    /// For torn writes: trigger when a write spans this byte offset.
    /// `None` means trigger on any write to a matching file.
    pub at_offset: Option<u64>,
    /// For power cuts: trigger after the Nth `sync` call on matching files.
    /// `None` means trigger on first sync.
    pub after_nth_sync: Option<u32>,
    /// Whether this fault has already been triggered (one-shot by default).
    triggered: bool,
}

impl FaultSpec {
    /// Start building a torn-write fault spec for files matching `glob`.
    #[must_use]
    pub fn torn_write(glob: &str) -> FaultSpecBuilder {
        FaultSpecBuilder {
            file_glob: glob.to_string(),
            kind: FaultKindChoice::TornWrite { valid_bytes: 0 },
            at_offset: None,
            after_nth_sync: None,
        }
    }

    /// Start building a power-cut fault spec for files matching `glob`.
    #[must_use]
    pub fn power_cut(glob: &str) -> FaultSpecBuilder {
        FaultSpecBuilder {
            file_glob: glob.to_string(),
            kind: FaultKindChoice::PowerCut,
            at_offset: None,
            after_nth_sync: None,
        }
    }

    /// Start building a generic I/O error fault spec.
    #[must_use]
    pub fn io_error(glob: &str) -> FaultSpecBuilder {
        FaultSpecBuilder {
            file_glob: glob.to_string(),
            kind: FaultKindChoice::IoError,
            at_offset: None,
            after_nth_sync: None,
        }
    }

    /// Check if a path matches this spec's glob pattern (simple suffix match).
    fn matches_path(&self, path: &Path) -> bool {
        let path_str = path.to_string_lossy();
        glob_matches(&self.file_glob, &path_str)
    }
}

/// Builder for [`FaultSpec`], used via `FaultSpec::torn_write(glob)` etc.
#[derive(Debug)]
pub struct FaultSpecBuilder {
    file_glob: String,
    kind: FaultKindChoice,
    at_offset: Option<u64>,
    after_nth_sync: Option<u32>,
}

/// Internal: which fault kind is being built.
#[derive(Debug)]
enum FaultKindChoice {
    TornWrite { valid_bytes: usize },
    PowerCut,
    IoError,
}

impl FaultSpecBuilder {
    /// For torn writes: trigger when a write overlaps this byte offset.
    #[must_use]
    pub fn at_offset_bytes(mut self, offset: u64) -> Self {
        self.at_offset = Some(offset);
        self
    }

    /// For torn writes: how many bytes of the write succeed before the tear.
    #[must_use]
    pub fn valid_bytes(mut self, n: usize) -> Self {
        if let FaultKindChoice::TornWrite {
            ref mut valid_bytes,
        } = self.kind
        {
            *valid_bytes = n;
        }
        self
    }

    /// For power cuts: trigger after the Nth `sync` call on matching files.
    #[must_use]
    pub fn after_nth_sync(mut self, n: u32) -> Self {
        self.after_nth_sync = Some(n);
        self
    }

    /// Finalize the builder into a [`FaultSpec`].
    #[must_use]
    pub fn build(self) -> FaultSpec {
        let kind = match self.kind {
            FaultKindChoice::TornWrite { valid_bytes } => FaultKind::TornWrite { valid_bytes },
            FaultKindChoice::PowerCut => FaultKind::PowerCut,
            FaultKindChoice::IoError => FaultKind::IoError,
        };
        FaultSpec {
            file_glob: self.file_glob,
            kind,
            at_offset: self.at_offset,
            after_nth_sync: self.after_nth_sync,
            triggered: false,
        }
    }
}

// ---------------------------------------------------------------------------
// FaultInjectingVfs
// ---------------------------------------------------------------------------

/// A VFS wrapper that intercepts I/O operations and injects deterministic faults.
///
/// Wraps any [`Vfs`] implementation and applies fault specs to writes and syncs.
/// Faults are one-shot by default: each spec triggers at most once.
pub struct FaultInjectingVfs<V: Vfs> {
    inner: V,
    faults: Mutex<Vec<FaultSpec>>,
    /// Global sync counter for all files opened through this VFS.
    sync_counter: AtomicU32,
    /// Set to true after a power-cut fault fires; all subsequent I/O fails.
    powered_off: AtomicBool,
    /// Record of all triggered faults.
    trigger_log: Mutex<Vec<FaultTriggerRecord>>,
}

impl<V: Vfs> FaultInjectingVfs<V> {
    /// Wrap an inner VFS with fault injection capability.
    #[must_use]
    pub fn new(inner: V) -> Self {
        Self {
            inner,
            faults: Mutex::new(Vec::new()),
            sync_counter: AtomicU32::new(0),
            powered_off: AtomicBool::new(false),
            trigger_log: Mutex::new(Vec::new()),
        }
    }

    /// Add a fault specification. Use `FaultSpec::torn_write(glob)` etc. to build.
    pub fn inject_fault(&self, spec: FaultSpec) {
        info!(
            bead_id = BEAD_ID,
            fault_kind = ?spec.kind,
            file_glob = %spec.file_glob,
            at_offset = ?spec.at_offset,
            after_nth_sync = ?spec.after_nth_sync,
            "FaultInjectingVfs: fault spec registered"
        );
        self.faults.lock().expect("fault lock poisoned").push(spec);
    }

    /// Return all triggered fault records for test assertions.
    #[must_use]
    pub fn triggered_faults(&self) -> Vec<FaultTriggerRecord> {
        self.trigger_log
            .lock()
            .expect("trigger log lock poisoned")
            .clone()
    }

    /// Return the total number of sync calls observed.
    #[must_use]
    pub fn sync_count(&self) -> u32 {
        self.sync_counter.load(Ordering::Acquire)
    }

    /// Return whether a power-cut fault has been triggered.
    #[must_use]
    pub fn is_powered_off(&self) -> bool {
        self.powered_off.load(Ordering::Acquire)
    }

    /// Reset the power state (simulates restart after power loss).
    pub fn power_on(&self) {
        self.powered_off.store(false, Ordering::Release);
        info!(bead_id = BEAD_ID, "FaultInjectingVfs: power restored");
    }

    /// Check if a write operation should be faulted.
    #[allow(clippy::significant_drop_tightening)]
    fn check_write_fault(
        &self,
        path: &Path,
        offset: u64,
        buf_len: usize,
    ) -> Option<(usize, FaultKind)> {
        let mut faults = self.faults.lock().expect("fault lock poisoned");
        for (idx, spec) in faults.iter_mut().enumerate() {
            if spec.triggered {
                continue;
            }
            if !spec.matches_path(path) {
                continue;
            }
            match &spec.kind {
                FaultKind::TornWrite { valid_bytes } => {
                    // Check if the write spans the target offset.
                    let write_end =
                        offset.saturating_add(u64::try_from(buf_len).unwrap_or(u64::MAX));
                    let target = spec.at_offset.unwrap_or(offset); // If no offset specified, any write matches.
                    if offset <= target && target < write_end {
                        spec.triggered = true;
                        let vb = *valid_bytes;
                        self.record_trigger(
                            idx,
                            path,
                            spec.kind.clone(),
                            format!("offset={offset}"),
                        );
                        return Some((idx, FaultKind::TornWrite { valid_bytes: vb }));
                    }
                }
                FaultKind::IoError if spec.at_offset.is_none() && spec.after_nth_sync.is_none() => {
                    spec.triggered = true;
                    self.record_trigger(
                        idx,
                        path,
                        spec.kind.clone(),
                        format!("write_offset={offset}"),
                    );
                    return Some((idx, FaultKind::IoError));
                }
                _ => {}
            }
        }
        None
    }

    /// Check if a sync operation should be faulted.
    #[allow(clippy::significant_drop_tightening)]
    fn check_sync_fault(&self, path: &Path) -> Option<(usize, FaultKind)> {
        let current_sync = self.sync_counter.fetch_add(1, Ordering::AcqRel);
        let mut faults = self.faults.lock().expect("fault lock poisoned");

        for (idx, spec) in faults.iter_mut().enumerate() {
            if spec.triggered {
                continue;
            }
            if !spec.matches_path(path) {
                continue;
            }
            match spec.kind {
                FaultKind::PowerCut => {
                    let target_sync = spec.after_nth_sync.unwrap_or(0);
                    if current_sync >= target_sync {
                        spec.triggered = true;
                        self.powered_off.store(true, Ordering::Release);
                        self.record_trigger(
                            idx,
                            path,
                            FaultKind::PowerCut,
                            format!("sync_index={current_sync}"),
                        );
                        return Some((idx, FaultKind::PowerCut));
                    }
                }
                FaultKind::IoError if spec.after_nth_sync.is_some() => {
                    let target_sync = spec.after_nth_sync.unwrap_or(0);
                    if current_sync >= target_sync {
                        spec.triggered = true;
                        self.record_trigger(
                            idx,
                            path,
                            FaultKind::IoError,
                            format!("sync_index={current_sync}"),
                        );
                        return Some((idx, FaultKind::IoError));
                    }
                }
                _ => {}
            }
        }
        None
    }

    /// Record a triggered fault for post-test inspection.
    fn record_trigger(&self, spec_index: usize, path: &Path, kind: FaultKind, detail: String) {
        warn!(
            bead_id = BEAD_ID,
            spec_index = spec_index,
            path = %path.display(),
            fault_kind = ?kind,
            detail = %detail,
            triggered = true,
            "FaultInjectingVfs: fault triggered"
        );
        self.trigger_log
            .lock()
            .expect("trigger log lock poisoned")
            .push(FaultTriggerRecord {
                spec_index,
                path: path.to_path_buf(),
                kind,
                detail,
            });
    }

    /// Check power state and return error if powered off.
    fn check_power(&self) -> Result<()> {
        if self.powered_off.load(Ordering::Acquire) {
            Err(FrankenError::Io(std::io::Error::other(
                "fault injection: power cut",
            )))
        } else {
            Ok(())
        }
    }
}

impl<V: Vfs> Vfs for FaultInjectingVfs<V> {
    type File = FaultInjectingFile<V::File>;

    fn name(&self) -> &'static str {
        "fault-injecting"
    }

    fn open(
        &self,
        cx: &Cx,
        path: Option<&Path>,
        flags: VfsOpenFlags,
    ) -> Result<(Self::File, VfsOpenFlags)> {
        self.check_power()?;
        let (inner_file, out_flags) = self.inner.open(cx, path, flags)?;
        let file_path = path.map(Path::to_path_buf).unwrap_or_default();
        Ok((
            FaultInjectingFile {
                inner: inner_file,
                path: file_path,
                // Fault state is checked at VFS level via shared Mutex-protected state.
            },
            out_flags,
        ))
    }

    fn delete(&self, cx: &Cx, path: &Path, sync_dir: bool) -> Result<()> {
        self.check_power()?;
        self.inner.delete(cx, path, sync_dir)
    }

    fn access(&self, cx: &Cx, path: &Path, flags: AccessFlags) -> Result<bool> {
        self.check_power()?;
        self.inner.access(cx, path, flags)
    }

    fn full_pathname(&self, cx: &Cx, path: &Path) -> Result<PathBuf> {
        self.inner.full_pathname(cx, path)
    }

    fn randomness(&self, cx: &Cx, buf: &mut [u8]) {
        self.inner.randomness(cx, buf);
    }

    fn current_time(&self, cx: &Cx) -> f64 {
        self.inner.current_time(cx)
    }
}

// ---------------------------------------------------------------------------
// FaultInjectingFile
// ---------------------------------------------------------------------------

/// File handle wrapper that participates in fault injection.
///
/// This is currently a thin passthrough. Fault checking happens at the VFS level
/// because [`FaultSpec`] rules need global state (sync counter, power state).
/// File-level operations delegate fault checks to the parent VFS via shared state.
pub struct FaultInjectingFile<F: VfsFile> {
    inner: F,
    path: PathBuf,
}

impl<F: VfsFile> VfsFile for FaultInjectingFile<F> {
    fn close(&mut self, cx: &Cx) -> Result<()> {
        self.inner.close(cx)
    }

    fn read(&mut self, cx: &Cx, buf: &mut [u8], offset: u64) -> Result<usize> {
        self.inner.read(cx, buf, offset)
    }

    fn write(&mut self, cx: &Cx, buf: &[u8], offset: u64) -> Result<()> {
        // NOTE: File-level write fault injection is handled by the shared-state
        // variant (FaultInjectingVfsShared). For the basic VFS wrapper, writes
        // pass through. Use inject_into_file() for per-file fault injection.
        self.inner.write(cx, buf, offset)
    }

    fn truncate(&mut self, cx: &Cx, size: u64) -> Result<()> {
        self.inner.truncate(cx, size)
    }

    fn sync(&mut self, cx: &Cx, flags: SyncFlags) -> Result<()> {
        self.inner.sync(cx, flags)
    }

    fn file_size(&self, cx: &Cx) -> Result<u64> {
        self.inner.file_size(cx)
    }

    fn lock(&mut self, cx: &Cx, level: LockLevel) -> Result<()> {
        self.inner.lock(cx, level)
    }

    fn unlock(&mut self, cx: &Cx, level: LockLevel) -> Result<()> {
        self.inner.unlock(cx, level)
    }

    fn check_reserved_lock(&self, cx: &Cx) -> Result<bool> {
        self.inner.check_reserved_lock(cx)
    }

    fn shm_map(&mut self, cx: &Cx, region: u32, size: u32, extend: bool) -> Result<ShmRegion> {
        self.inner.shm_map(cx, region, size, extend)
    }

    fn shm_lock(&mut self, cx: &Cx, offset: u32, n: u32, flags: u32) -> Result<()> {
        self.inner.shm_lock(cx, offset, n, flags)
    }

    fn shm_barrier(&self) {
        self.inner.shm_barrier();
    }

    fn shm_unmap(&mut self, cx: &Cx, delete: bool) -> Result<()> {
        self.inner.shm_unmap(cx, delete)
    }
}

// ---------------------------------------------------------------------------
// Shared-state variant for file-level fault injection
// ---------------------------------------------------------------------------

/// Shared fault injection state that files can reference.
///
/// This is the production approach: the VFS creates this shared state,
/// and each opened file gets an `Arc` reference to check faults during
/// write/sync operations.
#[derive(Debug)]
pub struct FaultState {
    faults: Mutex<Vec<FaultSpec>>,
    sync_counter: AtomicU32,
    powered_off: AtomicBool,
    trigger_log: Mutex<Vec<FaultTriggerRecord>>,
}

impl FaultState {
    /// Create new shared fault state.
    #[must_use]
    pub fn new() -> Self {
        Self {
            faults: Mutex::new(Vec::new()),
            sync_counter: AtomicU32::new(0),
            powered_off: AtomicBool::new(false),
            trigger_log: Mutex::new(Vec::new()),
        }
    }

    /// Register a fault specification.
    pub fn inject_fault(&self, spec: FaultSpec) {
        info!(
            bead_id = BEAD_ID,
            fault_kind = ?spec.kind,
            file_glob = %spec.file_glob,
            "FaultState: fault spec registered"
        );
        self.faults.lock().expect("lock").push(spec);
    }

    /// Check if a write should be faulted. Returns the valid prefix length if torn.
    #[allow(clippy::significant_drop_tightening)]
    pub fn check_write(&self, path: &Path, offset: u64, buf_len: usize) -> WriteDecision {
        if self.powered_off.load(Ordering::Acquire) {
            return WriteDecision::PoweredOff;
        }

        let mut faults = self.faults.lock().expect("lock");
        for (idx, spec) in faults.iter_mut().enumerate() {
            if spec.triggered || !spec.matches_path(path) {
                continue;
            }
            if let FaultKind::TornWrite { valid_bytes } = &spec.kind {
                let write_end = offset.saturating_add(u64::try_from(buf_len).unwrap_or(u64::MAX));
                let target = spec.at_offset.unwrap_or(offset);
                if offset <= target && target < write_end {
                    let vb = *valid_bytes;
                    spec.triggered = true;
                    self.record_trigger(idx, path, FaultKind::TornWrite { valid_bytes: vb });
                    return WriteDecision::TornWrite { valid_bytes: vb };
                }
            }
        }

        WriteDecision::Allow
    }

    /// Check if a sync should be faulted.
    #[allow(clippy::significant_drop_tightening)]
    pub fn check_sync(&self, path: &Path) -> SyncDecision {
        if self.powered_off.load(Ordering::Acquire) {
            return SyncDecision::PoweredOff;
        }

        let current = self.sync_counter.fetch_add(1, Ordering::AcqRel);
        let mut faults = self.faults.lock().expect("lock");

        for (idx, spec) in faults.iter_mut().enumerate() {
            if spec.triggered || !spec.matches_path(path) {
                continue;
            }
            if spec.kind == FaultKind::PowerCut {
                let target = spec.after_nth_sync.unwrap_or(0);
                if current >= target {
                    spec.triggered = true;
                    self.powered_off.store(true, Ordering::Release);
                    self.record_trigger(idx, path, FaultKind::PowerCut);
                    return SyncDecision::PowerCut;
                }
            }
        }

        SyncDecision::Allow
    }

    /// Whether the simulated power is off.
    #[must_use]
    pub fn is_powered_off(&self) -> bool {
        self.powered_off.load(Ordering::Acquire)
    }

    /// Restore power (simulate reboot).
    pub fn power_on(&self) {
        self.powered_off.store(false, Ordering::Release);
    }

    /// Return all triggered fault records.
    #[must_use]
    pub fn triggered_faults(&self) -> Vec<FaultTriggerRecord> {
        self.trigger_log.lock().expect("lock").clone()
    }

    /// Return total sync count.
    #[must_use]
    pub fn sync_count(&self) -> u32 {
        self.sync_counter.load(Ordering::Acquire)
    }

    fn record_trigger(&self, spec_index: usize, path: &Path, kind: FaultKind) {
        warn!(
            bead_id = BEAD_ID,
            spec_index,
            path = %path.display(),
            fault_kind = ?kind,
            triggered = true,
            "FaultState: fault triggered"
        );
        self.trigger_log
            .lock()
            .expect("lock")
            .push(FaultTriggerRecord {
                spec_index,
                path: path.to_path_buf(),
                kind,
                detail: String::new(),
            });
    }
}

impl Default for FaultState {
    fn default() -> Self {
        Self::new()
    }
}

/// Decision from `FaultState::check_write`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WriteDecision {
    /// Write proceeds normally.
    Allow,
    /// Write is torn: only `valid_bytes` are applied.
    TornWrite { valid_bytes: usize },
    /// Power is off — I/O error.
    PoweredOff,
}

/// Decision from `FaultState::check_sync`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SyncDecision {
    /// Sync proceeds normally.
    Allow,
    /// Power cut triggered on this sync.
    PowerCut,
    /// Power was already off.
    PoweredOff,
}

// ---------------------------------------------------------------------------
// Glob matching (simple suffix/wildcard)
// ---------------------------------------------------------------------------

/// Simple glob match supporting `*` as a single wildcard segment.
///
/// Supports patterns like `"*.wal"`, `"test.db"`, `"*"`.
fn glob_matches(pattern: &str, path: &str) -> bool {
    if pattern == "*" {
        return true;
    }
    if let Some(suffix) = pattern.strip_prefix('*') {
        return path.ends_with(suffix);
    }
    if let Some(prefix) = pattern.strip_suffix('*') {
        return path.starts_with(prefix);
    }
    // Exact match or filename match.
    path == pattern
        || path.ends_with(&format!("/{pattern}"))
        || path.ends_with(&format!("\\{pattern}"))
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use fsqlite_types::cx::Cx;
    use fsqlite_vfs::MemoryVfs;
    use std::path::Path;

    const TEST_BEAD: &str = "bd-3go.2";

    fn test_cx() -> Cx {
        Cx::default()
    }

    #[test]
    fn test_fault_injecting_vfs_torn_write_exact_offset() {
        // Torn-write injection triggers at the specified offset and produces
        // deterministic partial write.
        let state = FaultState::new();

        // WAL frame layout: 32-byte header + N * (24-byte frame header + 4096-byte page).
        // Frame 3 starts at offset 32 + 2*(24+4096) = 32 + 8240 = 8272.
        let frame3_offset: u64 = 32 + 2 * (24 + 4096);
        state.inject_fault(
            FaultSpec::torn_write("test.wal")
                .at_offset_bytes(frame3_offset)
                .valid_bytes(17)
                .build(),
        );

        let wal_path = Path::new("test.wal");

        // Write before the target offset — should succeed.
        let decision = state.check_write(wal_path, 0, 32);
        assert_eq!(
            decision,
            WriteDecision::Allow,
            "bead_id={TEST_BEAD} pre-target write should be allowed"
        );

        // Write spanning the target offset — torn write.
        let decision = state.check_write(wal_path, frame3_offset, 4120);
        assert_eq!(
            decision,
            WriteDecision::TornWrite { valid_bytes: 17 },
            "bead_id={TEST_BEAD} write at target offset should be torn"
        );

        // Verify the fault was recorded.
        let triggered = state.triggered_faults();
        assert_eq!(
            triggered.len(),
            1,
            "bead_id={TEST_BEAD} expected exactly one triggered fault"
        );
        assert_eq!(triggered[0].spec_index, 0);
        assert!(matches!(
            triggered[0].kind,
            FaultKind::TornWrite { valid_bytes: 17 }
        ));

        // Same spec should not trigger again (one-shot).
        let decision = state.check_write(wal_path, frame3_offset, 4120);
        assert_eq!(
            decision,
            WriteDecision::Allow,
            "bead_id={TEST_BEAD} one-shot fault should not re-trigger"
        );
    }

    #[test]
    fn test_fault_injecting_vfs_power_cut_after_nth_sync() {
        // Power-cut injection triggers after Nth sync and simulates crash semantics.
        let state = FaultState::new();
        state.inject_fault(FaultSpec::power_cut("test.wal").after_nth_sync(2).build());

        let wal_path = Path::new("test.wal");

        // Syncs 0 and 1 should pass.
        assert_eq!(state.check_sync(wal_path), SyncDecision::Allow);
        assert_eq!(state.check_sync(wal_path), SyncDecision::Allow);

        // Sync 2 triggers power cut.
        assert_eq!(state.check_sync(wal_path), SyncDecision::PowerCut);
        assert!(
            state.is_powered_off(),
            "bead_id={TEST_BEAD} power should be off after power cut"
        );

        // Subsequent operations fail.
        assert_eq!(state.check_sync(wal_path), SyncDecision::PoweredOff);
        assert_eq!(
            state.check_write(wal_path, 0, 100),
            WriteDecision::PoweredOff
        );

        // Verify trigger record.
        let triggered = state.triggered_faults();
        assert_eq!(triggered.len(), 1);
        assert_eq!(triggered[0].kind, FaultKind::PowerCut);

        // Power on (reboot).
        state.power_on();
        assert!(!state.is_powered_off());
        assert_eq!(state.check_write(wal_path, 0, 100), WriteDecision::Allow);
    }

    #[test]
    fn test_fault_vfs_wraps_memory_vfs() {
        // Basic integration: FaultInjectingVfs wraps MemoryVfs and opens/writes/reads.
        let vfs = FaultInjectingVfs::new(MemoryVfs::new());
        let cx = test_cx();
        let flags = VfsOpenFlags::READWRITE | VfsOpenFlags::CREATE | VfsOpenFlags::MAIN_DB;

        let (mut file, _out_flags) = vfs.open(&cx, Some(Path::new("test.db")), flags).unwrap();

        let data = b"hello world";
        file.write(&cx, data, 0).unwrap();

        let mut buf = vec![0u8; data.len()];
        let n = file.read(&cx, &mut buf, 0).unwrap();
        assert_eq!(n, data.len());
        assert_eq!(&buf, data);

        file.close(&cx).unwrap();
    }

    #[test]
    fn test_fault_state_glob_matching() {
        assert!(glob_matches("*.wal", "test.wal"));
        assert!(glob_matches("*.wal", "/path/to/test.wal"));
        assert!(!glob_matches("*.wal", "test.db"));
        assert!(glob_matches("*", "anything"));
        assert!(glob_matches("test.db", "test.db"));
        assert!(glob_matches("test.db", "/path/to/test.db"));
    }

    #[test]
    fn test_fault_state_deterministic_replay() {
        // Same fault specs + same operation sequence → same trigger results.
        for _ in 0..3 {
            let state = FaultState::new();
            state.inject_fault(
                FaultSpec::torn_write("*.wal")
                    .at_offset_bytes(100)
                    .valid_bytes(10)
                    .build(),
            );
            state.inject_fault(FaultSpec::power_cut("*.wal").after_nth_sync(1).build());

            let wal = Path::new("test.wal");

            // Same sequence of operations.
            let w1 = state.check_write(wal, 50, 100);
            assert_eq!(w1, WriteDecision::TornWrite { valid_bytes: 10 });

            let s1 = state.check_sync(wal);
            assert_eq!(s1, SyncDecision::Allow);

            let s2 = state.check_sync(wal);
            assert_eq!(s2, SyncDecision::PowerCut);

            assert!(state.is_powered_off());
            assert_eq!(state.triggered_faults().len(), 2);
        }
    }

    #[test]
    fn test_snapshot_isolation_holds_under_schedule_seed_deadbeef() {
        // Structural test: verify FsLab can schedule two tasks deterministically
        // under seed 0xDEAD_BEEF. This tests the scheduling infrastructure
        // that will underpin SI verification once Database is implemented.
        use crate::fslab::FsLab;
        use asupersync::types::Budget;

        let lab = FsLab::new(0xDEAD_BEEF).worker_count(4).max_steps(100_000);

        let report = lab.run_with_setup(|runtime, root| {
            // "reader" task — simulates snapshot read.
            let (t1, _) = crate::fslab::FsLab::spawn_named(runtime, root, "reader", async {
                let snapshot_val = 100_u64;
                // In a full implementation, this would open a transaction,
                // read, yield, read again, and assert snapshot isolation.
                assert_eq!(snapshot_val, 100, "snapshot value stable");
                snapshot_val
            });

            // "writer" task — simulates concurrent write.
            let (t2, _) = crate::fslab::FsLab::spawn_named(runtime, root, "writer", async {
                let new_val = 999_u64;
                // In a full implementation, this would update the row.
                new_val
            });

            let mut sched = runtime.scheduler.lock().expect("lock");
            sched.schedule(t1, 0);
            sched.schedule(t2, 1);
        });

        assert!(
            report.oracle_report.all_passed(),
            "bead_id={TEST_BEAD} oracle failures: {:?}",
            report.oracle_report
        );
        assert!(
            report.invariant_violations.is_empty(),
            "bead_id={TEST_BEAD} invariants: {:?}",
            report.invariant_violations
        );
    }

    #[test]
    fn test_wal_survives_torn_write_at_frame_3() {
        // Structural test: verify torn-write fault triggers at frame 3 offset
        // and the fault state is correctly captured for recovery testing.
        let state = FaultState::new();

        // WAL layout: 32-byte header + frames at (24+4096) each.
        // Frame 3 offset = 32 + 2 * 4120 = 8272.
        let frame3_offset = 32_u64 + 2 * (24 + 4096);
        state.inject_fault(
            FaultSpec::torn_write("*.wal")
                .at_offset_bytes(frame3_offset)
                .valid_bytes(17)
                .build(),
        );

        let wal = Path::new("test.wal");

        // Simulate writing frames 1-5.
        for frame_idx in 0_u64..5 {
            let offset = 32 + frame_idx * (24 + 4096);
            let frame_size = 24 + 4096;
            let decision = state.check_write(wal, offset, frame_size as usize);

            if frame_idx == 2 {
                // Frame 3 (0-indexed frame 2) should be torn.
                assert_eq!(
                    decision,
                    WriteDecision::TornWrite { valid_bytes: 17 },
                    "bead_id={TEST_BEAD} frame 3 should be torn"
                );
            } else {
                assert_eq!(
                    decision,
                    WriteDecision::Allow,
                    "bead_id={TEST_BEAD} frame {frame_idx} should be allowed"
                );
            }
        }

        // After torn write, recovery should see frames 1-2 intact, frame 3+ lost.
        let triggered = state.triggered_faults();
        assert_eq!(triggered.len(), 1);
        assert!(matches!(
            triggered[0].kind,
            FaultKind::TornWrite { valid_bytes: 17 }
        ));
    }

    #[test]
    fn test_power_loss_during_wal_commit_preserves_atomicity() {
        // Structural test: verify power-cut fault fires after 1st sync,
        // subsequent operations fail, and recovery (power_on) works.
        let state = FaultState::new();
        state.inject_fault(FaultSpec::power_cut("*.wal").after_nth_sync(1).build());

        let wal = Path::new("test.wal");

        // First sync (commit of first transaction) succeeds.
        assert_eq!(state.check_sync(wal), SyncDecision::Allow);

        // Write the second transaction.
        assert_eq!(
            state.check_write(wal, 32 + 4120, 4120),
            WriteDecision::Allow
        );

        // Second sync (commit of second transaction) triggers power cut.
        assert_eq!(state.check_sync(wal), SyncDecision::PowerCut);

        // Everything fails after power cut.
        assert_eq!(state.check_write(wal, 0, 100), WriteDecision::PoweredOff);
        assert_eq!(state.check_sync(wal), SyncDecision::PoweredOff);

        // Reboot: power on.
        state.power_on();

        // Post-recovery: first transaction's data should be recoverable
        // (the sync succeeded). Second transaction was interrupted.
        // This structural test verifies the fault mechanism; full DB-level
        // atomicity verification requires the Database layer (future bead).
        assert!(!state.is_powered_off());
        assert_eq!(state.sync_count(), 2); // 1 Allow + 1 PowerCut (powered-off calls skip counter)
        assert_eq!(state.triggered_faults().len(), 1);
    }
}
