//! Capability context (`Cx`) for FrankenSQLite.
//!
//! This is a **capability-passing style** context object that:
//! - threads cancellation checks (`checkpoint`) through long-running operations
//! - carries a [`Budget`] for deadline/priority propagation
//! - encodes available effects (spawn/time/random/io/remote) in the type system
//!   via [`cap::CapSet`], so widening is a **compile-time error**.
//!
//! # Compile-time capability narrowing
//!
//! Narrowing always succeeds:
//! ```
//! use fsqlite_types::cx::{cap, Cx};
//!
//! let cx = Cx::<cap::All>::new();
//! let _compute = cx.restrict::<cap::None>();
//! ```
//!
//! Widening is rejected at compile time:
//! ```compile_fail
//! use fsqlite_types::cx::{cap, Cx};
//!
//! let cx = Cx::<cap::None>::new();
//! let _nope = cx.restrict::<cap::All>();
//! ```

use std::marker::PhantomData;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Duration;

/// SQLite error code for `SQLITE_INTERRUPT`.
pub const SQLITE_INTERRUPT: i32 = 9;

/// Capability set definitions and subset reasoning.
pub mod cap {
    mod sealed {
        pub trait Sealed {}

        pub struct Bit<const V: bool>;

        pub trait Le {}
        impl Le for (Bit<false>, Bit<false>) {}
        impl Le for (Bit<false>, Bit<true>) {}
        impl Le for (Bit<true>, Bit<true>) {}
    }

    /// Type-level capability set: `[SPAWN, TIME, RANDOM, IO, REMOTE]`.
    #[derive(Debug, Clone, Copy, Default)]
    pub struct CapSet<
        const SPAWN: bool,
        const TIME: bool,
        const RANDOM: bool,
        const IO: bool,
        const REMOTE: bool,
    >;

    impl<
        const SPAWN: bool,
        const TIME: bool,
        const RANDOM: bool,
        const IO: bool,
        const REMOTE: bool,
    > sealed::Sealed for CapSet<SPAWN, TIME, RANDOM, IO, REMOTE>
    {
    }

    /// Full capability set.
    pub type All = CapSet<true, true, true, true, true>;
    /// No capabilities.
    pub type None = CapSet<false, false, false, false, false>;

    /// Type-level subset relation.
    ///
    /// Encodes pointwise ordering on capability bits: `false <= false`, `false <= true`,
    /// `true <= true`. The missing impl `(true <= false)` forbids widening.
    pub trait SubsetOf<Super>: sealed::Sealed {}

    impl<
        const S_SPAWN: bool,
        const S_TIME: bool,
        const S_RANDOM: bool,
        const S_IO: bool,
        const S_REMOTE: bool,
        const P_SPAWN: bool,
        const P_TIME: bool,
        const P_RANDOM: bool,
        const P_IO: bool,
        const P_REMOTE: bool,
    > SubsetOf<CapSet<P_SPAWN, P_TIME, P_RANDOM, P_IO, P_REMOTE>>
        for CapSet<S_SPAWN, S_TIME, S_RANDOM, S_IO, S_REMOTE>
    where
        (sealed::Bit<S_SPAWN>, sealed::Bit<P_SPAWN>): sealed::Le,
        (sealed::Bit<S_TIME>, sealed::Bit<P_TIME>): sealed::Le,
        (sealed::Bit<S_RANDOM>, sealed::Bit<P_RANDOM>): sealed::Le,
        (sealed::Bit<S_IO>, sealed::Bit<P_IO>): sealed::Le,
        (sealed::Bit<S_REMOTE>, sealed::Bit<P_REMOTE>): sealed::Le,
    {
    }

    pub trait HasSpawn: sealed::Sealed {}
    impl<const TIME: bool, const RANDOM: bool, const IO: bool, const REMOTE: bool> HasSpawn
        for CapSet<true, TIME, RANDOM, IO, REMOTE>
    {
    }

    pub trait HasTime: sealed::Sealed {}
    impl<const SPAWN: bool, const RANDOM: bool, const IO: bool, const REMOTE: bool> HasTime
        for CapSet<SPAWN, true, RANDOM, IO, REMOTE>
    {
    }

    pub trait HasRandom: sealed::Sealed {}
    impl<const SPAWN: bool, const TIME: bool, const IO: bool, const REMOTE: bool> HasRandom
        for CapSet<SPAWN, TIME, true, IO, REMOTE>
    {
    }

    pub trait HasIo: sealed::Sealed {}
    impl<const SPAWN: bool, const TIME: bool, const RANDOM: bool, const REMOTE: bool> HasIo
        for CapSet<SPAWN, TIME, RANDOM, true, REMOTE>
    {
    }

    pub trait HasRemote: sealed::Sealed {}
    impl<const SPAWN: bool, const TIME: bool, const RANDOM: bool, const IO: bool> HasRemote
        for CapSet<SPAWN, TIME, RANDOM, IO, true>
    {
    }
}

/// Connection-level capabilities: everything enabled.
pub type FullCaps = cap::All;
/// Storage-layer capabilities: time + I/O only.
pub type StorageCaps = cap::CapSet<false, true, false, true, false>;
/// Pure computation capabilities: no I/O, no time, no randomness.
pub type ComputeCaps = cap::None;

/// A budget for cancellation/deadline/priority propagation.
///
/// This is a product lattice with mixed meet/join semantics:
/// - resource constraints tighten by `min` (deadline/poll/cost)
/// - priority propagates by `max`
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Budget {
    pub deadline: Option<Duration>,
    pub poll_quota: u32,
    pub cost_quota: Option<u64>,
    pub priority: u8,
}

impl Budget {
    /// No constraints (identity for [`Self::meet`]).
    pub const INFINITE: Self = Self {
        deadline: None,
        poll_quota: u32::MAX,
        cost_quota: None,
        priority: 0,
    };

    /// Minimal budget for cleanup/finalizers.
    pub const MINIMAL: Self = Self {
        deadline: None,
        poll_quota: 100,
        cost_quota: None,
        priority: 0,
    };

    #[must_use]
    pub const fn with_deadline(self, deadline: Duration) -> Self {
        Self {
            deadline: Some(deadline),
            ..self
        }
    }

    #[must_use]
    pub const fn with_priority(self, priority: u8) -> Self {
        Self { priority, ..self }
    }

    #[must_use]
    pub const fn with_poll_quota(self, poll_quota: u32) -> Self {
        Self { poll_quota, ..self }
    }

    #[must_use]
    pub const fn with_cost_quota(self, cost_quota: u64) -> Self {
        Self {
            cost_quota: Some(cost_quota),
            ..self
        }
    }

    /// Meet (tighten) two budgets.
    #[must_use]
    pub fn meet(self, other: Self) -> Self {
        Self {
            deadline: match (self.deadline, other.deadline) {
                (Some(a), Some(b)) => Some(a.min(b)),
                (Some(a), None) => Some(a),
                (None, Some(b)) => Some(b),
                (None, None) => None,
            },
            poll_quota: self.poll_quota.min(other.poll_quota),
            cost_quota: match (self.cost_quota, other.cost_quota) {
                (Some(a), Some(b)) => Some(a.min(b)),
                (Some(a), None) => Some(a),
                (None, Some(b)) => Some(b),
                (None, None) => None,
            },
            priority: self.priority.max(other.priority),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ErrorKind {
    Cancelled,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Error {
    kind: ErrorKind,
}

impl Error {
    #[must_use]
    pub const fn cancelled() -> Self {
        Self {
            kind: ErrorKind::Cancelled,
        }
    }

    #[must_use]
    pub const fn kind(&self) -> ErrorKind {
        self.kind
    }

    #[must_use]
    pub const fn sqlite_error_code(&self) -> i32 {
        match self.kind {
            ErrorKind::Cancelled => SQLITE_INTERRUPT,
        }
    }
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Debug)]
struct CxInner {
    cancel_requested: AtomicBool,
    last_checkpoint_msg: Mutex<Option<String>>,
    // Deterministic clock: milliseconds since epoch for tests.
    unix_millis: AtomicU64,
}

impl CxInner {
    fn new() -> Self {
        Self {
            cancel_requested: AtomicBool::new(false),
            last_checkpoint_msg: Mutex::new(None),
            unix_millis: AtomicU64::new(0),
        }
    }
}

/// Capability context passed through all effectful operations.
#[derive(Debug)]
pub struct Cx<Caps = FullCaps> {
    inner: Arc<CxInner>,
    budget: Budget,
    // fn() -> Caps ensures Send+Sync regardless of Caps marker type.
    _caps: PhantomData<fn() -> Caps>,
}

impl<Caps> Clone for Cx<Caps> {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
            budget: self.budget,
            _caps: PhantomData,
        }
    }
}

impl Default for Cx<FullCaps> {
    fn default() -> Self {
        Self::new()
    }
}

impl Cx<FullCaps> {
    #[must_use]
    pub fn new() -> Self {
        Self::with_budget(Budget::INFINITE)
    }
}

impl<Caps> Cx<Caps> {
    #[must_use]
    pub fn with_budget(budget: Budget) -> Self {
        Self {
            inner: Arc::new(CxInner::new()),
            budget,
            _caps: PhantomData,
        }
    }

    #[must_use]
    pub fn budget(&self) -> Budget {
        self.budget
    }

    /// Returns a view of this context with a tighter effective budget.
    ///
    /// The effective budget is computed as `self.budget.meet(child)`, so the
    /// child cannot loosen its parent's constraints.
    #[must_use]
    pub fn scope_with_budget(&self, child: Budget) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
            budget: self.budget.meet(child),
            _caps: PhantomData,
        }
    }

    /// Returns a cleanup scope that uses [`Budget::MINIMAL`].
    #[must_use]
    pub fn cleanup_scope(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
            budget: Budget::MINIMAL,
            _caps: PhantomData,
        }
    }

    /// Re-type this context to a narrower capability set.
    ///
    /// This is zero-cost at runtime and shares cancellation state.
    #[must_use]
    pub fn restrict<NewCaps>(&self) -> Cx<NewCaps>
    where
        NewCaps: cap::SubsetOf<Caps>,
    {
        self.retype()
    }

    /// Internal re-typing helper without subset enforcement.
    #[must_use]
    pub fn retype<NewCaps>(&self) -> Cx<NewCaps> {
        Cx {
            inner: Arc::clone(&self.inner),
            budget: self.budget,
            _caps: PhantomData,
        }
    }

    #[must_use]
    pub fn is_cancel_requested(&self) -> bool {
        self.inner.cancel_requested.load(Ordering::Acquire)
    }

    pub fn cancel(&self) {
        self.inner.cancel_requested.store(true, Ordering::Release);
    }

    /// Check for cancellation at a yield point.
    pub fn checkpoint(&self) -> Result<()> {
        if self.is_cancel_requested() {
            Err(Error::cancelled())
        } else {
            Ok(())
        }
    }

    /// Check for cancellation and record a progress message.
    pub fn checkpoint_with(&self, msg: impl Into<String>) -> Result<()> {
        {
            let mut guard = self
                .inner
                .last_checkpoint_msg
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            *guard = Some(msg.into());
        }
        self.checkpoint()
    }

    #[must_use]
    pub fn last_checkpoint_message(&self) -> Option<String> {
        self.inner
            .last_checkpoint_msg
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }

    /// Set a deterministic unix time for tests.
    pub fn set_unix_millis_for_testing(&self, millis: u64)
    where
        Caps: cap::HasTime,
    {
        self.inner.unix_millis.store(millis, Ordering::Release);
    }

    /// Return current time as a Julian day (via deterministic unix millis).
    #[must_use]
    pub fn current_time_julian_day(&self) -> f64
    where
        Caps: cap::HasTime,
    {
        let millis = self.inner.unix_millis.load(Ordering::Acquire);
        #[allow(clippy::cast_precision_loss)]
        let secs = (millis as f64) / 1000.0;
        // Unix epoch in Julian days: 2440587.5
        2_440_587.5 + (secs / 86_400.0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::{Path, PathBuf};

    #[test]
    fn test_cx_checkpoint_observes_cancellation() {
        let cx = Cx::new();
        assert!(cx.checkpoint().is_ok());
        cx.cancel();
        let err = cx.checkpoint().unwrap_err();
        assert_eq!(err.kind(), ErrorKind::Cancelled);
        assert_eq!(err.sqlite_error_code(), SQLITE_INTERRUPT);
    }

    #[test]
    fn test_cx_capability_narrowing_compiles() {
        let cx = Cx::<FullCaps>::new();
        let _compute = cx.restrict::<ComputeCaps>();
        let _storage = cx.restrict::<StorageCaps>();
    }

    #[test]
    fn test_cx_budget_meet_tightens() {
        let parent = Budget::INFINITE.with_deadline(Duration::from_millis(100));
        let child = Budget::INFINITE.with_deadline(Duration::from_millis(200));
        let effective = parent.meet(child);
        assert_eq!(effective.deadline, Some(Duration::from_millis(100)));
    }

    #[test]
    fn test_cx_budget_priority_join() {
        let parent = Budget::INFINITE.with_priority(2);
        let child = Budget::INFINITE.with_priority(5);
        let effective = parent.meet(child);
        assert_eq!(effective.priority, 5);
    }

    #[test]
    fn test_cx_scope_with_budget_cannot_loosen() {
        let cx =
            Cx::<FullCaps>::with_budget(Budget::INFINITE.with_deadline(Duration::from_millis(50)));
        let child = Budget::INFINITE.with_deadline(Duration::from_millis(100));
        let scoped = cx.scope_with_budget(child);
        assert_eq!(scoped.budget().deadline, Some(Duration::from_millis(50)));
    }

    #[test]
    fn test_cx_checkpoint_with_message_records_message() {
        let cx = Cx::new();
        assert!(cx.checkpoint_with("vdbe pc=5").is_ok());
        assert_eq!(cx.last_checkpoint_message().as_deref(), Some("vdbe pc=5"));
    }

    #[test]
    fn test_cx_cleanup_uses_minimal_budget() {
        let cx = Cx::<FullCaps>::with_budget(Budget::INFINITE.with_poll_quota(10_000));
        let cleanup = cx.cleanup_scope();
        assert_eq!(cleanup.budget(), Budget::MINIMAL);
    }

    #[test]
    fn test_cx_restrict_storage_to_compute() {
        let cx = Cx::<FullCaps>::new();
        let storage = cx.restrict::<StorageCaps>();
        let _compute = storage.restrict::<ComputeCaps>();
    }

    #[test]
    fn test_cx_restrict_is_zero_cost() {
        // CapSet is a ZST; Cx carries only Arc + Budget + PhantomData.
        // Restrict changes only the phantom marker — same size, same pointer.
        assert_eq!(
            std::mem::size_of::<Cx<FullCaps>>(),
            std::mem::size_of::<Cx<ComputeCaps>>()
        );
    }

    #[test]
    fn test_budget_mixed_lattice() {
        let a = Budget {
            deadline: Some(Duration::from_millis(100)),
            poll_quota: 500,
            cost_quota: Some(1000),
            priority: 2,
        };
        let b = Budget {
            deadline: Some(Duration::from_millis(200)),
            poll_quota: 300,
            cost_quota: Some(2000),
            priority: 5,
        };
        let m = a.meet(b);
        // Resources tighten by min.
        assert_eq!(m.deadline, Some(Duration::from_millis(100)));
        assert_eq!(m.poll_quota, 300);
        assert_eq!(m.cost_quota, Some(1000));
        // Priority propagates by max (join).
        assert_eq!(m.priority, 5);
    }

    #[test]
    fn test_budget_meet_commutative() {
        let a = Budget {
            deadline: Some(Duration::from_millis(50)),
            poll_quota: 400,
            cost_quota: Some(800),
            priority: 3,
        };
        let b = Budget {
            deadline: Some(Duration::from_millis(150)),
            poll_quota: 200,
            cost_quota: None,
            priority: 7,
        };
        assert_eq!(a.meet(b), b.meet(a));
    }

    #[test]
    fn test_budget_meet_associative() {
        let a = Budget::INFINITE
            .with_deadline(Duration::from_millis(50))
            .with_poll_quota(100)
            .with_priority(1);
        let b = Budget::INFINITE
            .with_deadline(Duration::from_millis(150))
            .with_poll_quota(200)
            .with_priority(5);
        let c = Budget::INFINITE
            .with_deadline(Duration::from_millis(75))
            .with_poll_quota(50)
            .with_priority(3);
        assert_eq!(a.meet(b).meet(c), a.meet(b.meet(c)));
    }

    #[test]
    fn test_budget_minimal_is_stricter_than_normal() {
        let normal = Budget::INFINITE.with_poll_quota(10_000);
        let effective = normal.meet(Budget::MINIMAL);
        assert_eq!(effective.poll_quota, Budget::MINIMAL.poll_quota);
    }

    #[test]
    fn test_cx_cancel_shared_across_clones() {
        let cx1 = Cx::<FullCaps>::new();
        let cx2 = cx1.clone();
        assert!(!cx2.is_cancel_requested());
        cx1.cancel();
        assert!(cx2.is_cancel_requested());
        assert!(cx2.checkpoint().is_err());
    }

    #[test]
    fn test_cx_cancel_shared_across_restrict() {
        let cx = Cx::<FullCaps>::new();
        let compute = cx.restrict::<ComputeCaps>();
        cx.cancel();
        assert!(compute.checkpoint().is_err());
    }

    #[test]
    fn test_cx_current_time_julian_day() {
        let cx = Cx::<FullCaps>::new();
        // Unix epoch = Julian day 2440587.5
        cx.set_unix_millis_for_testing(0);
        let jd = cx.current_time_julian_day();
        assert!((jd - 2_440_587.5).abs() < 1e-10);

        // 1 day = 86_400_000 ms
        cx.set_unix_millis_for_testing(86_400_000);
        let jd = cx.current_time_julian_day();
        assert!((jd - 2_440_588.5).abs() < 1e-10);
    }

    #[test]
    fn test_capset_is_zero_sized() {
        assert_eq!(std::mem::size_of::<cap::All>(), 0);
        assert_eq!(std::mem::size_of::<cap::None>(), 0);
        assert_eq!(
            std::mem::size_of::<cap::CapSet<true, false, true, false, true>>(),
            0
        );
    }

    #[test]
    fn test_cx_checkpoint_not_cancelled() {
        let cx = Cx::new();
        assert!(cx.checkpoint().is_ok());
        assert!(cx.checkpoint_with("still going").is_ok());
    }

    #[test]
    fn test_cx_checkpoint_maps_to_sqlite_interrupt() {
        let cx = Cx::new();
        cx.cancel();
        let err = cx.checkpoint().unwrap_err();
        assert_eq!(err.sqlite_error_code(), SQLITE_INTERRUPT);
    }

    #[test]
    fn test_budget_infinite_is_identity_for_meet() {
        let budget = Budget {
            deadline: Some(Duration::from_millis(42)),
            poll_quota: 500,
            cost_quota: Some(1000),
            priority: 7,
        };
        assert_eq!(budget.meet(Budget::INFINITE), budget);
        assert_eq!(Budget::INFINITE.meet(budget), budget);
    }

    #[test]
    fn test_budget_none_constraints_propagate() {
        let a = Budget {
            deadline: None,
            poll_quota: u32::MAX,
            cost_quota: None,
            priority: 0,
        };
        let b = Budget {
            deadline: Some(Duration::from_millis(50)),
            poll_quota: 100,
            cost_quota: Some(500),
            priority: 3,
        };
        let m = a.meet(b);
        assert_eq!(m.deadline, Some(Duration::from_millis(50)));
        assert_eq!(m.poll_quota, 100);
        assert_eq!(m.cost_quota, Some(500));
        assert_eq!(m.priority, 3);
    }

    #[test]
    fn test_cx_scope_budget_chains() {
        let cx = Cx::<FullCaps>::with_budget(
            Budget::INFINITE
                .with_deadline(Duration::from_millis(100))
                .with_poll_quota(1000),
        );
        // First scope tightens deadline.
        let s1 = cx.scope_with_budget(Budget::INFINITE.with_deadline(Duration::from_millis(50)));
        assert_eq!(s1.budget().deadline, Some(Duration::from_millis(50)));
        assert_eq!(s1.budget().poll_quota, 1000);

        // Second scope tightens poll_quota further.
        let s2 = s1.scope_with_budget(Budget::INFINITE.with_poll_quota(200));
        assert_eq!(s2.budget().deadline, Some(Duration::from_millis(50)));
        assert_eq!(s2.budget().poll_quota, 200);
    }

    fn collect_rs_files(dir: &Path, out: &mut Vec<PathBuf>) -> std::io::Result<()> {
        for entry in std::fs::read_dir(dir)? {
            let entry = entry?;
            let path = entry.path();
            if path.is_dir() {
                collect_rs_files(&path, out)?;
            } else if path.extension().is_some_and(|ext| ext == "rs") {
                out.push(path);
            }
        }
        Ok(())
    }

    fn scan_file_outside_cfg_test_modules(src: &str, patterns: &[&str]) -> Vec<(usize, String)> {
        let mut hits = Vec::new();

        let mut brace_depth: i32 = 0;
        let mut pending_cfg_test = false;
        let mut skip_until_depth: Option<i32> = None;

        for (idx, line) in src.lines().enumerate() {
            let trimmed = line.trim_start();

            if skip_until_depth.is_none() {
                // Handle `#[cfg(test)] mod tests {` on a single line.
                if trimmed.starts_with("#[cfg(test)]") && trimmed.contains("mod ") {
                    pending_cfg_test = false;
                    skip_until_depth = Some(brace_depth);
                } else if trimmed.starts_with("#[cfg(test)]") {
                    pending_cfg_test = true;
                } else if pending_cfg_test {
                    // Allow additional attributes/blank lines before `mod ... {`.
                    if trimmed.is_empty() || trimmed.starts_with("//") || trimmed.starts_with("#[")
                    {
                        // keep pending
                    } else if trimmed.starts_with("mod ")
                        || trimmed.starts_with("pub mod ")
                        || trimmed.starts_with("pub(crate) mod ")
                    {
                        pending_cfg_test = false;
                        skip_until_depth = Some(brace_depth);
                    } else {
                        pending_cfg_test = false;
                    }
                } else {
                    for &pat in patterns {
                        if line.contains(pat) {
                            hits.push((idx + 1, pat.to_string()));
                        }
                    }
                }
            }

            // Update brace depth (coarse; sufficient for `#[cfg(test)] mod ... {}` blocks).
            let opens = i32::try_from(line.matches('{').count()).unwrap_or(i32::MAX);
            let closes = i32::try_from(line.matches('}').count()).unwrap_or(i32::MAX);
            brace_depth = brace_depth.saturating_add(opens).saturating_sub(closes);

            if let Some(until) = skip_until_depth {
                if brace_depth <= until {
                    skip_until_depth = None;
                }
            }
        }

        hits
    }

    #[test]
    fn test_ambient_authority_audit_gate() {
        // Scan `crates/*/src/**/*.rs` for ambient-authority usage, excluding
        // `#[cfg(test)] mod ...` blocks.
        let manifest_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        let repo_root = manifest_dir
            .parent()
            .and_then(Path::parent)
            .expect("fsqlite-types manifest dir must be crates/<name>");
        let crates_dir = repo_root.join("crates");

        // Always forbidden everywhere (outside cfg(test) modules).
        let always_forbidden = [
            "SystemTime::now(",
            "Instant::now(",
            "thread_rng(",
            "getrandom",
            "std::net::",
            "std::thread::spawn",
            "tokio::spawn",
        ];

        // Forbidden outside VFS boundary (outside cfg(test) modules).
        let non_vfs_forbidden = ["std::fs::"];

        let mut violations: Vec<String> = Vec::new();
        let mut crate_dirs: Vec<PathBuf> = Vec::new();
        for entry in std::fs::read_dir(&crates_dir).expect("read crates/ dir") {
            let entry = entry.expect("read crates/ entry");
            let path = entry.path();
            if path.is_dir() {
                crate_dirs.push(path);
            }
        }

        for crate_dir in crate_dirs {
            let crate_name = crate_dir
                .file_name()
                .and_then(|s| s.to_str())
                .unwrap_or("<unknown>");
            let src_dir = crate_dir.join("src");
            if !src_dir.is_dir() {
                continue;
            }

            let mut files = Vec::new();
            collect_rs_files(&src_dir, &mut files).expect("collect rs files");

            for file in files {
                let src = std::fs::read_to_string(&file).expect("read file");
                for (line, pat) in scan_file_outside_cfg_test_modules(&src, &always_forbidden) {
                    violations.push(format!(
                        "{crate_name}:{path}:{line} uses forbidden `{pat}`",
                        path = file.display()
                    ));
                }

                if crate_name != "fsqlite-vfs" {
                    for (line, pat) in scan_file_outside_cfg_test_modules(&src, &non_vfs_forbidden)
                    {
                        violations.push(format!(
                            "{crate_name}:{path}:{line} uses forbidden `{pat}` (non-vfs crate)",
                            path = file.display()
                        ));
                    }
                }
            }
        }

        assert!(
            violations.is_empty(),
            "ambient authority violations (outside cfg(test) modules):\n{}",
            violations.join("\n")
        );
    }
}
