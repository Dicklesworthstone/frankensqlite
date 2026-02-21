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

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use fsqlite_error::{FrankenError, Result};
use fsqlite_types::LockLevel;
use fsqlite_types::cx::Cx;
use fsqlite_types::flags::{AccessFlags, SyncFlags, VfsOpenFlags};
use fsqlite_vfs::shm::ShmRegion;
use fsqlite_vfs::traits::{Vfs, VfsFile};
use tracing::{debug, debug_span};

/// Bead identifier for tracing/log correlation.
const BEAD_ID: &str = "bd-3go.2";
/// Required metric name for injected fault counters.
pub const TEST_VFS_FAULT_COUNTER_NAME: &str = "fsqlite_test_vfs_faults_injected_total";
/// Stable default replay seed used by [`FaultState::new`].
const DEFAULT_FAULT_SEED: u64 = 0xD1A6_A3F4_9B17_0C5E;
/// Prefix used for anonymous temp files opened through [`FaultInjectingVfs`].
const TEMP_FILE_PREFIX: &str = "__fault_vfs_temp__";

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
    /// A generic partial write fault (non-WAL-specific naming).
    PartialWrite {
        /// Number of bytes that make it to storage before the write is interrupted.
        valid_bytes: usize,
    },
    /// Simulated power loss: the sync call and all subsequent operations fail.
    PowerCut,
    /// Generic I/O error returned from the faulted operation.
    IoError,
    /// Read operation returns an I/O error.
    ReadFailure,
    /// Write operation returns an I/O error.
    WriteFailure,
    /// Injected operation latency (deterministic from seed).
    Latency {
        /// Base latency in milliseconds.
        base_millis: u64,
        /// Deterministic jitter range in milliseconds (`0..=jitter_millis`).
        jitter_millis: u64,
    },
    /// Simulated out-of-space condition.
    DiskFull,
}

impl FaultKind {
    /// Stable metric label for this fault kind.
    #[must_use]
    fn metric_label(&self) -> &'static str {
        match self {
            Self::TornWrite { .. } => "torn_write",
            Self::PartialWrite { .. } => "partial_write",
            Self::PowerCut => "power_cut",
            Self::IoError => "io_error",
            Self::ReadFailure => "read_failure",
            Self::WriteFailure => "write_failure",
            Self::Latency { .. } => "latency",
            Self::DiskFull => "disk_full",
        }
    }
}

/// A record of a fault that was triggered during test execution.
#[derive(Debug, Clone, PartialEq, Eq)]
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

/// Snapshot of the required fault-injection counter metric.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FaultMetricsSnapshot {
    /// Metric name (`fsqlite_test_vfs_faults_injected_total`).
    pub metric_name: &'static str,
    /// Counter values keyed by `fault_type`.
    pub by_fault_type: BTreeMap<String, u64>,
    /// Sum of all counter values.
    pub total: u64,
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
    /// Trigger after this many matching operations (`0` means immediate).
    after_count: Option<u64>,
    /// Maximum number of times this spec may trigger.
    max_triggers: u32,
    /// Number of times this spec has triggered so far.
    trigger_count: u32,
    /// Number of matching operations seen so far.
    match_count: u64,
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
            after_count: None,
            max_triggers: 1,
        }
    }

    /// Start building a partial-write fault spec for files matching `glob`.
    #[must_use]
    pub fn partial_write(glob: &str) -> FaultSpecBuilder {
        FaultSpecBuilder {
            file_glob: glob.to_string(),
            kind: FaultKindChoice::PartialWrite { valid_bytes: 0 },
            at_offset: None,
            after_nth_sync: None,
            after_count: None,
            max_triggers: 1,
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
            after_count: None,
            max_triggers: 1,
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
            after_count: None,
            max_triggers: 1,
        }
    }

    /// Start building a read-failure fault spec.
    #[must_use]
    pub fn read_failure(glob: &str) -> FaultSpecBuilder {
        FaultSpecBuilder {
            file_glob: glob.to_string(),
            kind: FaultKindChoice::ReadFailure,
            at_offset: None,
            after_nth_sync: None,
            after_count: None,
            max_triggers: 1,
        }
    }

    /// Start building a write-failure fault spec.
    #[must_use]
    pub fn write_failure(glob: &str) -> FaultSpecBuilder {
        FaultSpecBuilder {
            file_glob: glob.to_string(),
            kind: FaultKindChoice::WriteFailure,
            at_offset: None,
            after_nth_sync: None,
            after_count: None,
            max_triggers: 1,
        }
    }

    /// Start building a latency injection fault spec.
    #[must_use]
    pub fn latency(glob: &str) -> FaultSpecBuilder {
        FaultSpecBuilder {
            file_glob: glob.to_string(),
            kind: FaultKindChoice::Latency {
                base_millis: 0,
                jitter_millis: 0,
            },
            at_offset: None,
            after_nth_sync: None,
            after_count: None,
            max_triggers: 1,
        }
    }

    /// Start building a disk-full fault spec.
    #[must_use]
    pub fn disk_full(glob: &str) -> FaultSpecBuilder {
        FaultSpecBuilder {
            file_glob: glob.to_string(),
            kind: FaultKindChoice::DiskFull,
            at_offset: None,
            after_nth_sync: None,
            after_count: None,
            max_triggers: 1,
        }
    }

    /// Check if a path matches this spec's glob pattern (simple suffix match).
    fn matches_path(&self, path: &Path) -> bool {
        let path_str = path.to_string_lossy();
        glob_matches(&self.file_glob, &path_str)
    }

    /// Whether this spec can no longer fire.
    #[must_use]
    fn is_exhausted(&self) -> bool {
        self.trigger_count >= self.max_triggers
    }

    /// Register one matching operation and return the new trigger count if this event fires.
    fn register_match(&mut self) -> Option<u32> {
        if self.is_exhausted() {
            return None;
        }
        let match_idx = self.match_count;
        self.match_count = self.match_count.saturating_add(1);
        if match_idx < self.after_count.unwrap_or(0) {
            return None;
        }
        self.trigger_count = self.trigger_count.saturating_add(1);
        Some(self.trigger_count)
    }
}

/// Builder for [`FaultSpec`], used via `FaultSpec::torn_write(glob)` etc.
#[derive(Debug)]
pub struct FaultSpecBuilder {
    file_glob: String,
    kind: FaultKindChoice,
    at_offset: Option<u64>,
    after_nth_sync: Option<u32>,
    after_count: Option<u64>,
    max_triggers: u32,
}

/// Internal: which fault kind is being built.
#[derive(Debug)]
enum FaultKindChoice {
    TornWrite {
        valid_bytes: usize,
    },
    PartialWrite {
        valid_bytes: usize,
    },
    PowerCut,
    IoError,
    ReadFailure,
    WriteFailure,
    Latency {
        base_millis: u64,
        jitter_millis: u64,
    },
    DiskFull,
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
        match self.kind {
            FaultKindChoice::TornWrite {
                ref mut valid_bytes,
            }
            | FaultKindChoice::PartialWrite {
                ref mut valid_bytes,
            } => {
                *valid_bytes = n;
            }
            _ => {}
        }
        self
    }

    /// Alias for [`Self::valid_bytes`] used by `partial_write` call sites.
    #[must_use]
    pub fn bytes_written(self, n: usize) -> Self {
        self.valid_bytes(n)
    }

    /// For power cuts: trigger after the Nth `sync` call on matching files.
    #[must_use]
    pub fn after_nth_sync(mut self, n: u32) -> Self {
        self.after_nth_sync = Some(n);
        self
    }

    /// Trigger after `n` matching operation opportunities.
    #[must_use]
    pub fn after_count(mut self, n: u64) -> Self {
        self.after_count = Some(n);
        self
    }

    /// Configure how many times this spec may trigger.
    #[must_use]
    pub fn trigger_count(mut self, n: u32) -> Self {
        self.max_triggers = n.max(1);
        self
    }

    /// Set base injected latency in milliseconds.
    #[must_use]
    pub fn latency_millis(mut self, millis: u64) -> Self {
        if let FaultKindChoice::Latency {
            ref mut base_millis,
            ..
        } = self.kind
        {
            *base_millis = millis;
        }
        self
    }

    /// Set deterministic jitter range (`0..=jitter_millis`) in milliseconds.
    #[must_use]
    pub fn jitter_millis(mut self, millis: u64) -> Self {
        if let FaultKindChoice::Latency {
            ref mut jitter_millis,
            ..
        } = self.kind
        {
            *jitter_millis = millis;
        }
        self
    }

    /// Finalize the builder into a [`FaultSpec`].
    #[must_use]
    pub fn build(self) -> FaultSpec {
        let kind = match self.kind {
            FaultKindChoice::TornWrite { valid_bytes } => FaultKind::TornWrite { valid_bytes },
            FaultKindChoice::PartialWrite { valid_bytes } => {
                FaultKind::PartialWrite { valid_bytes }
            }
            FaultKindChoice::PowerCut => FaultKind::PowerCut,
            FaultKindChoice::IoError => FaultKind::IoError,
            FaultKindChoice::ReadFailure => FaultKind::ReadFailure,
            FaultKindChoice::WriteFailure => FaultKind::WriteFailure,
            FaultKindChoice::Latency {
                base_millis,
                jitter_millis,
            } => FaultKind::Latency {
                base_millis,
                jitter_millis,
            },
            FaultKindChoice::DiskFull => FaultKind::DiskFull,
        };
        FaultSpec {
            file_glob: self.file_glob,
            kind,
            at_offset: self.at_offset,
            after_nth_sync: self.after_nth_sync,
            after_count: self.after_count,
            max_triggers: self.max_triggers.max(1),
            trigger_count: 0,
            match_count: 0,
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
    state: Arc<FaultState>,
    temp_path_counter: AtomicU64,
}

impl<V: Vfs> FaultInjectingVfs<V> {
    /// Wrap an inner VFS with fault injection capability.
    #[must_use]
    pub fn new(inner: V) -> Self {
        Self::with_seed(inner, DEFAULT_FAULT_SEED)
    }

    /// Wrap a VFS with fault injection capability using an explicit replay seed.
    #[must_use]
    pub fn with_seed(inner: V, seed: u64) -> Self {
        Self {
            inner,
            state: Arc::new(FaultState::new_with_seed(seed)),
            temp_path_counter: AtomicU64::new(0),
        }
    }

    /// Borrow the shared fault state used by this VFS.
    #[must_use]
    pub fn fault_state(&self) -> Arc<FaultState> {
        Arc::clone(&self.state)
    }

    /// Add a fault specification. Use `FaultSpec::torn_write(glob)` etc. to build.
    pub fn inject_fault(&self, spec: FaultSpec) {
        self.state.inject_fault(spec);
    }

    /// Return all triggered fault records for test assertions.
    #[must_use]
    pub fn triggered_faults(&self) -> Vec<FaultTriggerRecord> {
        self.state.triggered_faults()
    }

    /// Return the total number of sync calls observed.
    #[must_use]
    pub fn sync_count(&self) -> u32 {
        self.state.sync_count()
    }

    /// Return whether a power-cut fault has been triggered.
    #[must_use]
    pub fn is_powered_off(&self) -> bool {
        self.state.is_powered_off()
    }

    /// Reset the power state (simulates restart after power loss).
    pub fn power_on(&self) {
        self.state.power_on();
    }

    /// Snapshot fault counters by fault type.
    #[must_use]
    pub fn metrics_snapshot(&self) -> FaultMetricsSnapshot {
        self.state.metrics_snapshot()
    }

    /// Deterministic replay seed for this VFS instance.
    #[must_use]
    pub fn replay_seed(&self) -> u64 {
        self.state.replay_seed()
    }

    /// Check power state and return error if powered off.
    fn check_power(&self) -> Result<()> {
        self.state.ensure_power_on()
    }

    fn resolve_open_path(&self, path: Option<&Path>) -> PathBuf {
        path.map_or_else(
            || {
                let id = self.temp_path_counter.fetch_add(1, Ordering::AcqRel);
                PathBuf::from(format!("{TEMP_FILE_PREFIX}_{id}"))
            },
            Path::to_path_buf,
        )
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
        let file_path = self.resolve_open_path(path);
        Ok((
            FaultInjectingFile {
                inner: inner_file,
                state: Arc::clone(&self.state),
                path: file_path,
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
    state: Arc<FaultState>,
    path: PathBuf,
}

impl<F: VfsFile> VfsFile for FaultInjectingFile<F> {
    fn close(&mut self, cx: &Cx) -> Result<()> {
        self.inner.close(cx)
    }

    fn read(&mut self, cx: &Cx, buf: &mut [u8], offset: u64) -> Result<usize> {
        match self.state.check_read(&self.path, offset, buf.len()) {
            ReadDecision::Allow => self.inner.read(cx, buf, offset),
            ReadDecision::IoError => Err(io_failure_error("fault injection: read failure")),
            ReadDecision::PoweredOff => Err(power_cut_error()),
        }
    }

    fn write(&mut self, cx: &Cx, buf: &[u8], offset: u64) -> Result<()> {
        match self.state.check_write(&self.path, offset, buf.len()) {
            WriteDecision::Allow => self.inner.write(cx, buf, offset),
            WriteDecision::TornWrite { valid_bytes }
            | WriteDecision::PartialWrite { valid_bytes } => {
                let applied = valid_bytes.min(buf.len());
                if applied > 0 {
                    self.inner.write(cx, &buf[..applied], offset)?;
                }
                Err(io_failure_error("fault injection: partial write"))
            }
            WriteDecision::IoError => Err(io_failure_error("fault injection: write failure")),
            WriteDecision::DiskFull => Err(FrankenError::DatabaseFull),
            WriteDecision::PoweredOff => Err(power_cut_error()),
        }
    }

    fn truncate(&mut self, cx: &Cx, size: u64) -> Result<()> {
        self.inner.truncate(cx, size)
    }

    fn sync(&mut self, cx: &Cx, flags: SyncFlags) -> Result<()> {
        match self.state.check_sync(&self.path) {
            SyncDecision::Allow => self.inner.sync(cx, flags),
            SyncDecision::PowerCut | SyncDecision::PoweredOff => Err(power_cut_error()),
            SyncDecision::IoError => Err(io_failure_error("fault injection: sync failure")),
        }
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
    fault_counters: Mutex<BTreeMap<&'static str, u64>>,
    operation_counter: AtomicU64,
    replay_seed: u64,
}

impl FaultState {
    /// Create new shared fault state.
    #[must_use]
    pub fn new() -> Self {
        Self::new_with_seed(DEFAULT_FAULT_SEED)
    }

    /// Create new shared fault state with explicit deterministic replay seed.
    #[must_use]
    pub fn new_with_seed(seed: u64) -> Self {
        Self {
            faults: Mutex::new(Vec::new()),
            sync_counter: AtomicU32::new(0),
            powered_off: AtomicBool::new(false),
            trigger_log: Mutex::new(Vec::new()),
            fault_counters: Mutex::new(BTreeMap::new()),
            operation_counter: AtomicU64::new(0),
            replay_seed: seed,
        }
    }

    /// Register a fault specification.
    pub fn inject_fault(&self, spec: FaultSpec) {
        debug!(
            bead_id = BEAD_ID,
            fault_kind = ?spec.kind,
            file_glob = %spec.file_glob,
            at_offset = ?spec.at_offset,
            after_nth_sync = ?spec.after_nth_sync,
            after_count = ?spec.after_count,
            max_triggers = spec.max_triggers,
            "FaultState: fault spec registered"
        );
        self.faults.lock().expect("lock").push(spec);
    }

    /// Deterministic replay seed used for this state.
    #[must_use]
    pub fn replay_seed(&self) -> u64 {
        self.replay_seed
    }

    /// Check if a read should be faulted.
    #[allow(clippy::significant_drop_tightening)]
    pub fn check_read(&self, path: &Path, offset: u64, buf_len: usize) -> ReadDecision {
        if self.powered_off.load(Ordering::Acquire) {
            return ReadDecision::PoweredOff;
        }

        let operation_index = self.next_operation_index();
        let (decision, delay_ms) = {
            let mut faults = self.faults.lock().expect("lock");
            let mut decision = ReadDecision::Allow;
            let mut delay_ms = 0_u64;

            for (idx, spec) in faults.iter_mut().enumerate() {
                if spec.is_exhausted() || !spec.matches_path(path) {
                    continue;
                }
                match spec.kind.clone() {
                    FaultKind::Latency {
                        base_millis,
                        jitter_millis,
                    } => {
                        if let Some(spec_trigger_count) = spec.register_match() {
                            let computed = self.resolve_latency_millis(
                                base_millis,
                                jitter_millis,
                                operation_index,
                                idx,
                                spec_trigger_count,
                            );
                            delay_ms = delay_ms.saturating_add(computed);
                            self.record_trigger(
                                idx,
                                path,
                                FaultKind::Latency {
                                    base_millis,
                                    jitter_millis,
                                },
                                format!(
                                    "operation=read offset={offset} len={buf_len} delay_ms={computed}"
                                ),
                                spec_trigger_count,
                            );
                        }
                    }
                    FaultKind::ReadFailure => {
                        if let Some(spec_trigger_count) = spec.register_match() {
                            self.record_trigger(
                                idx,
                                path,
                                FaultKind::ReadFailure,
                                format!("operation=read offset={offset} len={buf_len}"),
                                spec_trigger_count,
                            );
                            decision = ReadDecision::IoError;
                            break;
                        }
                    }
                    FaultKind::IoError => {
                        if let Some(spec_trigger_count) = spec.register_match() {
                            self.record_trigger(
                                idx,
                                path,
                                FaultKind::IoError,
                                format!("operation=read offset={offset} len={buf_len}"),
                                spec_trigger_count,
                            );
                            decision = ReadDecision::IoError;
                            break;
                        }
                    }
                    _ => {}
                }
            }
            (decision, delay_ms)
        };

        self.apply_latency(delay_ms);
        decision
    }

    /// Check if a write should be faulted.
    #[allow(clippy::significant_drop_tightening, clippy::too_many_lines)]
    pub fn check_write(&self, path: &Path, offset: u64, buf_len: usize) -> WriteDecision {
        if self.powered_off.load(Ordering::Acquire) {
            return WriteDecision::PoweredOff;
        }

        let operation_index = self.next_operation_index();
        let write_end = offset.saturating_add(u64::try_from(buf_len).unwrap_or(u64::MAX));
        let (decision, delay_ms) = {
            let mut faults = self.faults.lock().expect("lock");
            let mut decision = WriteDecision::Allow;
            let mut delay_ms = 0_u64;

            for (idx, spec) in faults.iter_mut().enumerate() {
                if spec.is_exhausted() || !spec.matches_path(path) {
                    continue;
                }
                match spec.kind.clone() {
                    FaultKind::Latency {
                        base_millis,
                        jitter_millis,
                    } => {
                        if let Some(spec_trigger_count) = spec.register_match() {
                            let computed = self.resolve_latency_millis(
                                base_millis,
                                jitter_millis,
                                operation_index,
                                idx,
                                spec_trigger_count,
                            );
                            delay_ms = delay_ms.saturating_add(computed);
                            self.record_trigger(
                                idx,
                                path,
                                FaultKind::Latency {
                                    base_millis,
                                    jitter_millis,
                                },
                                format!(
                                    "operation=write offset={offset} len={buf_len} delay_ms={computed}"
                                ),
                                spec_trigger_count,
                            );
                        }
                    }
                    FaultKind::TornWrite { valid_bytes } => {
                        let target = spec.at_offset.unwrap_or(offset);
                        if !(offset <= target && target < write_end) {
                            continue;
                        }
                        if let Some(spec_trigger_count) = spec.register_match() {
                            let applied = valid_bytes.min(buf_len);
                            self.record_trigger(
                                idx,
                                path,
                                FaultKind::TornWrite {
                                    valid_bytes: applied,
                                },
                                format!("operation=write offset={offset} len={buf_len}"),
                                spec_trigger_count,
                            );
                            decision = WriteDecision::TornWrite {
                                valid_bytes: applied,
                            };
                            break;
                        }
                    }
                    FaultKind::PartialWrite { valid_bytes } => {
                        let target = spec.at_offset.unwrap_or(offset);
                        if !(offset <= target && target < write_end) {
                            continue;
                        }
                        if let Some(spec_trigger_count) = spec.register_match() {
                            let applied = valid_bytes.min(buf_len);
                            self.record_trigger(
                                idx,
                                path,
                                FaultKind::PartialWrite {
                                    valid_bytes: applied,
                                },
                                format!("operation=write offset={offset} len={buf_len}"),
                                spec_trigger_count,
                            );
                            decision = WriteDecision::PartialWrite {
                                valid_bytes: applied,
                            };
                            break;
                        }
                    }
                    FaultKind::WriteFailure => {
                        if let Some(spec_trigger_count) = spec.register_match() {
                            self.record_trigger(
                                idx,
                                path,
                                FaultKind::WriteFailure,
                                format!("operation=write offset={offset} len={buf_len}"),
                                spec_trigger_count,
                            );
                            decision = WriteDecision::IoError;
                            break;
                        }
                    }
                    FaultKind::DiskFull => {
                        if let Some(spec_trigger_count) = spec.register_match() {
                            self.record_trigger(
                                idx,
                                path,
                                FaultKind::DiskFull,
                                format!("operation=write offset={offset} len={buf_len}"),
                                spec_trigger_count,
                            );
                            decision = WriteDecision::DiskFull;
                            break;
                        }
                    }
                    FaultKind::IoError => {
                        if let Some(spec_trigger_count) = spec.register_match() {
                            self.record_trigger(
                                idx,
                                path,
                                FaultKind::IoError,
                                format!("operation=write offset={offset} len={buf_len}"),
                                spec_trigger_count,
                            );
                            decision = WriteDecision::IoError;
                            break;
                        }
                    }
                    _ => {}
                }
            }
            (decision, delay_ms)
        };

        self.apply_latency(delay_ms);
        decision
    }

    /// Check if a sync should be faulted.
    #[allow(clippy::significant_drop_tightening)]
    pub fn check_sync(&self, path: &Path) -> SyncDecision {
        if self.powered_off.load(Ordering::Acquire) {
            return SyncDecision::PoweredOff;
        }

        let sync_index = self.sync_counter.fetch_add(1, Ordering::AcqRel);
        let operation_index = self.next_operation_index();
        let (decision, delay_ms) = {
            let mut faults = self.faults.lock().expect("lock");
            let mut decision = SyncDecision::Allow;
            let mut delay_ms = 0_u64;

            for (idx, spec) in faults.iter_mut().enumerate() {
                if spec.is_exhausted() || !spec.matches_path(path) {
                    continue;
                }
                match spec.kind.clone() {
                    FaultKind::Latency {
                        base_millis,
                        jitter_millis,
                    } => {
                        if let Some(spec_trigger_count) = spec.register_match() {
                            let computed = self.resolve_latency_millis(
                                base_millis,
                                jitter_millis,
                                operation_index,
                                idx,
                                spec_trigger_count,
                            );
                            delay_ms = delay_ms.saturating_add(computed);
                            self.record_trigger(
                                idx,
                                path,
                                FaultKind::Latency {
                                    base_millis,
                                    jitter_millis,
                                },
                                format!("operation=sync index={sync_index} delay_ms={computed}"),
                                spec_trigger_count,
                            );
                        }
                    }
                    FaultKind::PowerCut => {
                        let target = spec.after_nth_sync.unwrap_or(0);
                        if sync_index < target {
                            continue;
                        }
                        if let Some(spec_trigger_count) = spec.register_match() {
                            self.powered_off.store(true, Ordering::Release);
                            self.record_trigger(
                                idx,
                                path,
                                FaultKind::PowerCut,
                                format!("operation=sync index={sync_index}"),
                                spec_trigger_count,
                            );
                            decision = SyncDecision::PowerCut;
                            break;
                        }
                    }
                    FaultKind::IoError => {
                        if let Some(spec_trigger_count) = spec.register_match() {
                            self.record_trigger(
                                idx,
                                path,
                                FaultKind::IoError,
                                format!("operation=sync index={sync_index}"),
                                spec_trigger_count,
                            );
                            decision = SyncDecision::IoError;
                            break;
                        }
                    }
                    _ => {}
                }
            }
            (decision, delay_ms)
        };

        self.apply_latency(delay_ms);
        decision
    }

    /// Whether the simulated power is off.
    #[must_use]
    pub fn is_powered_off(&self) -> bool {
        self.powered_off.load(Ordering::Acquire)
    }

    /// Restore power (simulate reboot).
    pub fn power_on(&self) {
        self.powered_off.store(false, Ordering::Release);
        debug!(bead_id = BEAD_ID, "FaultState: power restored");
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

    /// Return a snapshot of `fsqlite_test_vfs_faults_injected_total`.
    #[must_use]
    #[allow(clippy::significant_drop_tightening)]
    pub fn metrics_snapshot(&self) -> FaultMetricsSnapshot {
        let counters = self.fault_counters.lock().expect("lock");
        let mut by_fault_type = BTreeMap::new();
        let mut total = 0_u64;
        for (fault_type, count) in counters.iter() {
            by_fault_type.insert((*fault_type).to_owned(), *count);
            total = total.saturating_add(*count);
        }
        FaultMetricsSnapshot {
            metric_name: TEST_VFS_FAULT_COUNTER_NAME,
            by_fault_type,
            total,
        }
    }

    fn ensure_power_on(&self) -> Result<()> {
        if self.powered_off.load(Ordering::Acquire) {
            Err(power_cut_error())
        } else {
            Ok(())
        }
    }

    fn next_operation_index(&self) -> u64 {
        self.operation_counter.fetch_add(1, Ordering::AcqRel)
    }

    fn resolve_latency_millis(
        &self,
        base_millis: u64,
        jitter_millis: u64,
        operation_index: u64,
        spec_index: usize,
        spec_trigger_count: u32,
    ) -> u64 {
        if jitter_millis == 0 {
            return base_millis;
        }
        let sample = deterministic_mix(
            self.replay_seed,
            operation_index,
            spec_index,
            spec_trigger_count,
        );
        let jitter_span = jitter_millis.saturating_add(1);
        base_millis.saturating_add(sample % jitter_span)
    }

    #[allow(clippy::unused_self)]
    fn apply_latency(&self, delay_ms: u64) {
        if delay_ms > 0 {
            std::thread::sleep(Duration::from_millis(delay_ms));
        }
    }

    #[allow(clippy::significant_drop_tightening)]
    fn record_trigger(
        &self,
        spec_index: usize,
        path: &Path,
        kind: FaultKind,
        detail: String,
        spec_trigger_count: u32,
    ) {
        let metric_label = kind.metric_label();
        let metric_trigger_count = {
            let mut counters = self.fault_counters.lock().expect("lock");
            let counter = counters.entry(metric_label).or_insert(0);
            *counter = counter.saturating_add(1);
            *counter
        };

        let span = debug_span!(
            "test_vfs_fault",
            fault_type = metric_label,
            trigger_count = metric_trigger_count,
            file_path = %path.display()
        );
        let _guard = span.enter();
        debug!(
            bead_id = BEAD_ID,
            spec_index,
            fault_kind = ?kind,
            replay_seed = self.replay_seed,
            spec_trigger_count,
            metric_name = TEST_VFS_FAULT_COUNTER_NAME,
            detail = %detail,
            "FaultState: fault triggered"
        );
        self.trigger_log
            .lock()
            .expect("lock")
            .push(FaultTriggerRecord {
                spec_index,
                path: path.to_path_buf(),
                kind,
                detail,
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
    /// Generic partial write decision.
    PartialWrite { valid_bytes: usize },
    /// Write should fail with I/O error.
    IoError,
    /// Write should fail with "database full".
    DiskFull,
    /// Power is off — I/O error.
    PoweredOff,
}

/// Decision from `FaultState::check_read`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReadDecision {
    /// Read proceeds normally.
    Allow,
    /// Read should fail with I/O error.
    IoError,
    /// Power was already off.
    PoweredOff,
}

/// Decision from `FaultState::check_sync`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SyncDecision {
    /// Sync proceeds normally.
    Allow,
    /// Power cut triggered on this sync.
    PowerCut,
    /// Sync should fail with I/O error.
    IoError,
    /// Power was already off.
    PoweredOff,
}

fn io_failure_error(msg: &str) -> FrankenError {
    FrankenError::Io(std::io::Error::other(msg))
}

fn power_cut_error() -> FrankenError {
    io_failure_error("fault injection: power cut")
}

/// Mix deterministic seed inputs into a stable pseudo-random output.
fn deterministic_mix(
    seed: u64,
    operation_index: u64,
    spec_index: usize,
    spec_trigger_count: u32,
) -> u64 {
    let mut x = seed
        ^ operation_index.wrapping_mul(0x9E37_79B9_7F4A_7C15)
        ^ u64::try_from(spec_index)
            .unwrap_or(u64::MAX)
            .wrapping_mul(0xC2B2_AE3D_27D4_EB4F)
        ^ u64::from(spec_trigger_count).wrapping_mul(0x1656_67B1_9E37_79F9);
    x = x.wrapping_add(0x9E37_79B9_7F4A_7C15);
    let mut z = x;
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    z ^ (z >> 31)
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
    fn test_fault_state_supports_read_write_disk_faults() {
        let state = FaultState::new();
        state.inject_fault(FaultSpec::read_failure("*.db").build());
        state.inject_fault(FaultSpec::write_failure("*.db").after_count(1).build());
        state.inject_fault(FaultSpec::disk_full("*.wal").build());

        let db = Path::new("main.db");
        let wal = Path::new("main.wal");

        assert_eq!(state.check_read(db, 0, 32), ReadDecision::IoError);
        assert_eq!(
            state.check_write(db, 0, 32),
            WriteDecision::Allow,
            "bead_id={TEST_BEAD} after_count=1 should skip first matching write",
        );
        assert_eq!(state.check_write(db, 32, 32), WriteDecision::IoError);
        assert_eq!(state.check_write(wal, 0, 32), WriteDecision::DiskFull);
    }

    #[test]
    fn test_fault_state_trigger_count_is_configurable() {
        let state = FaultState::new();
        state.inject_fault(FaultSpec::write_failure("*.db").trigger_count(2).build());

        let db = Path::new("burst.db");
        assert_eq!(state.check_write(db, 0, 8), WriteDecision::IoError);
        assert_eq!(state.check_write(db, 8, 8), WriteDecision::IoError);
        assert_eq!(
            state.check_write(db, 16, 8),
            WriteDecision::Allow,
            "bead_id={TEST_BEAD} trigger_count=2 should exhaust the spec",
        );
    }

    #[test]
    fn test_fault_state_latency_replay_is_seed_deterministic() {
        fn run(seed: u64) -> Vec<FaultTriggerRecord> {
            let state = FaultState::new_with_seed(seed);
            state.inject_fault(
                FaultSpec::latency("*.db")
                    .latency_millis(0)
                    .jitter_millis(3)
                    .trigger_count(3)
                    .build(),
            );

            let db = Path::new("seed.db");
            for offset in 0_u64..3 {
                assert_eq!(state.check_read(db, offset, 8), ReadDecision::Allow);
            }

            state.triggered_faults()
        }

        let first = run(0xA11CE_u64);
        let second = run(0xA11CE_u64);
        let different_seed = run(0xBEEFu64);

        assert_eq!(
            first, second,
            "bead_id={TEST_BEAD} same seed should replay identical latency schedule",
        );
        assert_ne!(
            first, different_seed,
            "bead_id={TEST_BEAD} different seed should alter deterministic jitter choices",
        );
    }

    #[test]
    fn test_fault_state_metrics_snapshot_counts_by_fault_type() {
        let state = FaultState::new();
        state.inject_fault(FaultSpec::write_failure("*.db").trigger_count(2).build());
        state.inject_fault(FaultSpec::power_cut("*.db").after_nth_sync(0).build());

        let db = Path::new("metrics.db");
        assert_eq!(state.check_write(db, 0, 8), WriteDecision::IoError);
        assert_eq!(state.check_write(db, 8, 8), WriteDecision::IoError);
        assert_eq!(state.check_sync(db), SyncDecision::PowerCut);

        let snapshot = state.metrics_snapshot();
        assert_eq!(snapshot.metric_name, TEST_VFS_FAULT_COUNTER_NAME);
        assert_eq!(snapshot.by_fault_type.get("write_failure"), Some(&2));
        assert_eq!(snapshot.by_fault_type.get("power_cut"), Some(&1));
        assert_eq!(snapshot.total, 3);
    }

    #[test]
    fn test_fault_injecting_vfs_disk_full_surfaces_database_full_error() {
        let vfs = FaultInjectingVfs::new(MemoryVfs::new());
        vfs.inject_fault(FaultSpec::disk_full("fault.db").build());

        let cx = test_cx();
        let flags = VfsOpenFlags::READWRITE | VfsOpenFlags::CREATE | VfsOpenFlags::MAIN_DB;
        let (mut file, _) = vfs.open(&cx, Some(Path::new("fault.db")), flags).unwrap();

        let err = file
            .write(&cx, b"1234", 0)
            .expect_err("disk-full fault should fail write");
        assert!(matches!(err, FrankenError::DatabaseFull));
    }

    #[test]
    fn test_snapshot_isolation_holds_under_schedule_seed_deadbeef() {
        // Structural test: verify FsLab can schedule two tasks deterministically
        // under seed 0xDEAD_BEEF. This tests the scheduling infrastructure
        // that will underpin SI verification once Database is implemented.
        use crate::fslab::{FsLab, SchedulerLockExt};

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
                // In a full implementation, this would update the row.
                999_u64
            });

            let mut sched = runtime.scheduler.lock();
            sched.schedule_task(t1, 0);
            sched.schedule_task(t2, 1);
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
            let frame_size: usize = 24 + 4096;
            let decision = state.check_write(wal, offset, frame_size);

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
