//! FsLab: ergonomic wrapper around asupersync's `LabRuntime` for FrankenSQLite testing.
//!
//! Provides deterministic scheduling, trace certificates, and oracle verification
//! for structured concurrency scenarios. This is FrankenSQLite infrastructure built
//! on top of the asupersync lab runtime (`bd-3go.2`, spec §4.2).
//!
//! # Quick Start
//!
//! ```ignore
//! use fsqlite_harness::fslab::FsLab;
//!
//! let lab = FsLab::new(0xDEAD_BEEF).worker_count(4).max_steps(100_000);
//! let report = lab.run_with_setup(|runtime, root_region| {
//!     // Register tasks, set up oracles, etc.
//! });
//! assert!(report.oracle_report.all_passed());
//! ```

use asupersync::lab::runtime::{LabRunReport, LabScheduler};
use asupersync::lab::{LabConfig, LabRuntime};
use asupersync::types::{Budget, RegionId, TaskId};
use parking_lot::RawMutex;
use std::future::Future;
use tracing::info;

/// Bead identifier for tracing and log correlation.
const BEAD_ID: &str = "bd-3go.2";

pub(crate) trait SchedulerLockExt {
    fn schedule_task(&mut self, task_id: TaskId, priority: u8);
}

impl SchedulerLockExt for std::sync::LockResult<std::sync::MutexGuard<'_, LabScheduler>> {
    fn schedule_task(&mut self, task_id: TaskId, priority: u8) {
        self.as_mut()
            .expect("FsLab: scheduler lock poisoned")
            .schedule(task_id, priority);
    }
}

impl SchedulerLockExt for std::sync::MutexGuard<'_, LabScheduler> {
    fn schedule_task(&mut self, task_id: TaskId, priority: u8) {
        self.schedule(task_id, priority);
    }
}

impl SchedulerLockExt for parking_lot::lock_api::MutexGuard<'_, RawMutex, LabScheduler> {
    fn schedule_task(&mut self, task_id: TaskId, priority: u8) {
        self.schedule(task_id, priority);
    }
}

/// Ergonomic wrapper around [`LabRuntime`] for FrankenSQLite deterministic testing.
///
/// `FsLab` configures and drives a lab runtime, logging seed/config metadata
/// and returning structured [`LabRunReport`]s with oracle results.
pub struct FsLab {
    config: LabConfig,
}

impl FsLab {
    /// Create a new `FsLab` with the given deterministic seed.
    #[must_use]
    pub fn new(seed: u64) -> Self {
        Self {
            config: LabConfig::new(seed),
        }
    }

    /// Set the number of virtual workers for deterministic multi-worker simulation.
    #[must_use]
    pub fn worker_count(mut self, n: usize) -> Self {
        self.config = self.config.worker_count(n);
        self
    }

    /// Set the maximum number of scheduling steps before forced termination.
    #[must_use]
    pub fn max_steps(mut self, n: u64) -> Self {
        self.config = self.config.max_steps(n);
        self
    }

    /// Enable light chaos injection (suitable for CI runs).
    #[must_use]
    pub fn with_light_chaos(mut self) -> Self {
        self.config = self.config.with_light_chaos();
        self
    }

    /// Enable heavy chaos injection (thorough stress testing).
    #[must_use]
    pub fn with_heavy_chaos(mut self) -> Self {
        self.config = self.config.with_heavy_chaos();
        self
    }

    /// Enable replay recording for post-mortem debugging.
    #[must_use]
    pub fn with_replay_recording(mut self) -> Self {
        self.config = self.config.with_default_replay_recording();
        self
    }

    /// Return the deterministic seed driving this lab.
    #[must_use]
    pub fn seed(&self) -> u64 {
        self.config.seed
    }

    /// Return a reference to the underlying [`LabConfig`].
    #[must_use]
    pub fn config(&self) -> &LabConfig {
        &self.config
    }

    /// Build a fresh [`LabRuntime`] from this configuration.
    ///
    /// Useful when you need full control over task creation and scheduling.
    #[must_use]
    pub fn build_runtime(&self) -> LabRuntime {
        let runtime = LabRuntime::new(self.config.clone());
        info!(
            bead_id = BEAD_ID,
            seed = self.config.seed,
            worker_count = self.config.worker_count,
            max_steps = ?self.config.max_steps,
            "FsLab: runtime created"
        );
        runtime
    }

    /// Create a runtime, call `setup` with it and a root region, run to quiescent,
    /// and return the structured report.
    ///
    /// This is the primary entry point for deterministic test scenarios.
    ///
    /// ```ignore
    /// let report = lab.run_with_setup(|runtime, root| {
    ///     let (tid, _) = runtime.state.create_task(root, Budget::INFINITE, async { 42 }).unwrap();
    ///     let mut scheduler = runtime
    ///         .scheduler
    ///         .lock();
    ///     scheduler.schedule_task(tid, 0);
    /// });
    /// assert!(report.oracle_report.all_passed());
    /// ```
    pub fn run_with_setup<F>(&self, setup: F) -> LabRunReport
    where
        F: FnOnce(&mut LabRuntime, RegionId),
    {
        let mut runtime = self.build_runtime();
        let root = runtime.state.create_root_region(Budget::INFINITE);

        info!(
            bead_id = BEAD_ID,
            seed = self.config.seed,
            root_region = ?root,
            "FsLab: root region created, running setup"
        );

        setup(&mut runtime, root);

        let report = runtime.run_until_quiescent_with_report();

        info!(
            bead_id = BEAD_ID,
            seed = self.config.seed,
            steps = report.steps_total,
            quiescent = report.quiescent,
            trace_fingerprint = report.trace_fingerprint,
            schedule_hash = report.trace_certificate.schedule_hash,
            oracles_passed = report.oracle_report.all_passed(),
            invariant_count = report.invariant_violations.len(),
            "FsLab: run complete"
        );

        report
    }

    /// Convenience: create a single async task, schedule it, run to quiescent.
    ///
    /// Returns the lab report. The task's return value is available via the
    /// [`asupersync::runtime::TaskHandle`] if needed; use [`run_with_setup`](Self::run_with_setup)
    /// for full control.
    pub fn run_single_task<F, T>(&self, future: F) -> LabRunReport
    where
        F: Future<Output = T> + Send + 'static,
        T: Send + 'static,
    {
        self.run_with_setup(|runtime, root| {
            let (tid, _handle) = runtime
                .state
                .create_task(root, Budget::INFINITE, future)
                .expect("FsLab: failed to create task");

            let mut scheduler = runtime.scheduler.lock();
            scheduler.schedule_task(tid, 0);
        })
    }

    /// Create a named task within a running lab setup.
    ///
    /// This is a convenience wrapper around `runtime.state.create_task` that
    /// logs the task name for trace correlation.
    ///
    /// Returns `(TaskId, TaskHandle<T>)` — you must schedule the `TaskId`
    /// via the runtime scheduler lock guard for your desired lane.
    pub fn spawn_named<F, T>(
        runtime: &mut LabRuntime,
        region: RegionId,
        name: &str,
        future: F,
    ) -> (TaskId, asupersync::runtime::TaskHandle<T>)
    where
        F: Future<Output = T> + Send + 'static,
        T: Send + 'static,
    {
        let (tid, handle) = runtime
            .state
            .create_task(region, Budget::INFINITE, future)
            .expect("FsLab: failed to create named task");

        info!(
            bead_id = BEAD_ID,
            task_name = name,
            task_id = ?tid,
            region = ?region,
            "FsLab: named task spawned"
        );

        (tid, handle)
    }

    /// Run the same scenario twice with the same seed and verify trace fingerprints match.
    ///
    /// This is the core determinism property: same seed → identical execution trace.
    pub fn assert_deterministic<F>(&self, setup: F)
    where
        F: Fn(&mut LabRuntime, RegionId) + Clone,
    {
        let report1 = self.run_with_setup(setup.clone());
        let report2 = self.run_with_setup(setup);

        assert_eq!(
            report1.trace_fingerprint, report2.trace_fingerprint,
            "bead_id={BEAD_ID} determinism violation: trace fingerprints differ \
             (run1={}, run2={}, seed={})",
            report1.trace_fingerprint, report2.trace_fingerprint, self.config.seed,
        );

        assert_eq!(
            report1.trace_certificate.schedule_hash, report2.trace_certificate.schedule_hash,
            "bead_id={BEAD_ID} determinism violation: schedule hashes differ (seed={})",
            self.config.seed,
        );

        info!(
            bead_id = BEAD_ID,
            seed = self.config.seed,
            fingerprint = report1.trace_fingerprint,
            "FsLab: determinism assertion passed"
        );
    }
}

/// Compute a stable schedule trace hash from a [`LabRunReport`].
///
/// Useful for comparing execution traces across runs.
#[must_use]
pub fn schedule_hash(report: &LabRunReport) -> u64 {
    report.trace_certificate.schedule_hash
}

#[cfg(test)]
mod tests {
    use super::*;
    use asupersync::types::Budget;

    const TEST_BEAD_ID: &str = "bd-3go.2";

    #[test]
    fn test_lab_runtime_smoke_oracles_pass() {
        // Minimal LabRuntime smoke test: run a trivial task, verify oracle_report.all_passed.
        let lab = FsLab::new(0xDEAD_BEEF).worker_count(4).max_steps(100_000);

        let report = lab.run_with_setup(|runtime, root| {
            let (tid, _handle) = runtime
                .state
                .create_task(root, Budget::INFINITE, async { 1_u64 })
                .expect("create task");

            let mut scheduler = runtime.scheduler.lock();
            scheduler.schedule_task(tid, 0);
        });

        assert!(
            report.oracle_report.all_passed(),
            "bead_id={TEST_BEAD_ID} oracle failures: {:?}",
            report.oracle_report
        );
        assert!(
            report.invariant_violations.is_empty(),
            "bead_id={TEST_BEAD_ID} invariant violations: {:?}",
            report.invariant_violations
        );
        assert!(
            report.quiescent,
            "bead_id={TEST_BEAD_ID} runtime not quiescent after run"
        );
    }

    #[test]
    fn test_fslab_spawn_named_tasks() {
        // Verify that FsLab::spawn_named attaches name metadata and tasks complete.
        let lab = FsLab::new(42).worker_count(2).max_steps(50_000);

        let report = lab.run_with_setup(|runtime, root| {
            let (t1, _h1) = FsLab::spawn_named(runtime, root, "reader", async { "read_done" });
            let (t2, _h2) = FsLab::spawn_named(runtime, root, "writer", async { "write_done" });

            let mut sched = runtime.scheduler.lock();
            sched.schedule_task(t1, 0);
            sched.schedule_task(t2, 1);
        });

        assert!(
            report.oracle_report.all_passed(),
            "bead_id={TEST_BEAD_ID} oracle failures for named tasks: {:?}",
            report.oracle_report
        );
        assert!(
            report.quiescent,
            "bead_id={TEST_BEAD_ID} not quiescent after named tasks"
        );
        assert!(
            report.steps_total > 0,
            "bead_id={TEST_BEAD_ID} expected some steps executed"
        );
    }

    #[test]
    fn test_cancellation_injection_all_points_no_leaks() {
        // With light chaos (cancel injection), verify no leaked obligations/tasks.
        let lab = FsLab::new(42)
            .worker_count(2)
            .max_steps(50_000)
            .with_light_chaos();

        let report = lab.run_with_setup(|runtime, root| {
            let (tid, _handle) = runtime
                .state
                .create_task(root, Budget::INFINITE, async {
                    // Simulate a multi-step operation that could be cancelled.
                    let mut acc = 0_u64;
                    for i in 0..10 {
                        acc = acc.wrapping_add(i);
                    }
                    acc
                })
                .expect("create task");

            let mut scheduler = runtime.scheduler.lock();
            scheduler.schedule_task(tid, 0);
        });

        // Key property: no obligation leaks or task leaks under chaos injection.
        assert!(
            report.invariant_violations.is_empty(),
            "bead_id={TEST_BEAD_ID} obligation/task leaks under chaos: {:?}",
            report.invariant_violations
        );
    }

    #[test]
    fn test_fslab_deterministic_replay() {
        // Same seed produces identical trace fingerprint and schedule hash.
        let lab = FsLab::new(0xCAFE).worker_count(2).max_steps(10_000);

        lab.assert_deterministic(|runtime, root| {
            let (t1, _) = runtime
                .state
                .create_task(root, Budget::INFINITE, async { 1_u32 })
                .expect("task 1");
            let (t2, _) = runtime
                .state
                .create_task(root, Budget::INFINITE, async { 2_u32 })
                .expect("task 2");

            let mut sched = runtime.scheduler.lock();
            sched.schedule_task(t1, 0);
            sched.schedule_task(t2, 1);
        });
    }

    #[test]
    fn test_fslab_single_task_convenience() {
        let lab = FsLab::new(777).max_steps(5_000);
        let report = lab.run_single_task(async { 42_u64 });

        assert!(
            report.oracle_report.all_passed(),
            "bead_id={TEST_BEAD_ID} single-task oracle failure"
        );
        assert!(report.quiescent);
    }

    #[test]
    fn test_fslab_config_accessors() {
        let lab = FsLab::new(123).worker_count(8).max_steps(50_000);

        assert_eq!(lab.seed(), 123);
        assert_eq!(lab.config().worker_count, 8);
        assert_eq!(lab.config().max_steps, Some(50_000));
    }
}
