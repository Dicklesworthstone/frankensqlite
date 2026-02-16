//! Morsel-driven parallel dispatcher for vectorized pipelines (`bd-14vp7.6`).
//!
//! This module provides:
//! - page-range morsel partitioning,
//! - pipeline task definitions,
//! - crossbeam-deque work-stealing execution,
//! - pipeline barriers between pipeline waves.

use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering as AtomicOrdering};
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::Instant;

use crossbeam_deque::{Steal, Stealer, Worker};
use fsqlite_types::PageNumber;

use crate::vectorized_scan::PageMorsel;

/// Dispatcher errors.
#[derive(Debug)]
pub enum DispatchError {
    InvalidConfig(&'static str),
    InvalidTaskSet {
        expected_pipeline: PipelineId,
        found_pipeline: PipelineId,
        task_id: usize,
    },
    WorkerPanicked,
}

impl fmt::Display for DispatchError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidConfig(msg) => write!(f, "invalid dispatcher config: {msg}"),
            Self::InvalidTaskSet {
                expected_pipeline,
                found_pipeline,
                task_id,
            } => write!(
                f,
                "task {task_id} belongs to pipeline {:?}, expected {:?}",
                found_pipeline, expected_pipeline
            ),
            Self::WorkerPanicked => f.write_str("worker thread panicked during dispatch"),
        }
    }
}

impl std::error::Error for DispatchError {}

/// Result alias for dispatcher operations.
pub type DispatchResult<T> = std::result::Result<T, DispatchError>;

/// Fallback L2 cache size used for morsel auto-tuning when host details are unavailable.
pub const DEFAULT_L2_CACHE_BYTES: usize = 1_048_576;
/// Default database page size used for morsel auto-tuning.
pub const DEFAULT_PAGE_SIZE_BYTES: usize = 4_096;

// ── Morsel Dispatch Metrics (bd-1rw.2) ─────────────────────────────────────

/// Rows-per-second gauge for morsel execution throughput.
///
/// The current dispatcher tracks task-level throughput and uses that as a
/// stable proxy until row-level accounting is threaded through operator outputs.
static FSQLITE_MORSEL_THROUGHPUT_ROWS_PER_SEC: AtomicU64 = AtomicU64::new(0);
/// Active-workers gauge for the most recent pipeline dispatch.
static FSQLITE_MORSEL_WORKERS_ACTIVE: AtomicU64 = AtomicU64::new(0);

/// Snapshot of morsel dispatch gauges.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct MorselDispatchMetricsSnapshot {
    /// Gauge: `fsqlite_morsel_throughput_rows_per_sec`.
    pub fsqlite_morsel_throughput_rows_per_sec: u64,
    /// Gauge: `fsqlite_morsel_workers_active`.
    pub fsqlite_morsel_workers_active: u64,
}

/// Read a point-in-time snapshot of morsel dispatch gauges.
#[must_use]
pub fn morsel_dispatch_metrics_snapshot() -> MorselDispatchMetricsSnapshot {
    MorselDispatchMetricsSnapshot {
        fsqlite_morsel_throughput_rows_per_sec: FSQLITE_MORSEL_THROUGHPUT_ROWS_PER_SEC
            .load(AtomicOrdering::Relaxed),
        fsqlite_morsel_workers_active: FSQLITE_MORSEL_WORKERS_ACTIVE.load(AtomicOrdering::Relaxed),
    }
}

/// Reset morsel dispatch gauges (tests/diagnostics).
pub fn reset_morsel_dispatch_metrics() {
    FSQLITE_MORSEL_THROUGHPUT_ROWS_PER_SEC.store(0, AtomicOrdering::Relaxed);
    FSQLITE_MORSEL_WORKERS_ACTIVE.store(0, AtomicOrdering::Relaxed);
}

/// A contiguous scan morsel plus locality hint.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MorselDescriptor {
    pub morsel_id: usize,
    pub page_range: PageMorsel,
    pub preferred_numa_node: usize,
}

/// Partition a page interval into fixed-size morsels.
///
/// # Errors
///
/// Returns an error when `pages_per_morsel == 0`, `numa_nodes == 0`, or the
/// page bounds are invalid.
pub fn partition_page_morsels(
    start_page: PageNumber,
    end_page: PageNumber,
    pages_per_morsel: u32,
    numa_nodes: usize,
) -> DispatchResult<Vec<MorselDescriptor>> {
    if pages_per_morsel == 0 {
        return Err(DispatchError::InvalidConfig(
            "pages_per_morsel must be greater than zero",
        ));
    }
    if numa_nodes == 0 {
        return Err(DispatchError::InvalidConfig(
            "numa_nodes must be greater than zero",
        ));
    }

    let full = PageMorsel::new(start_page, end_page).map_err(|_| {
        DispatchError::InvalidConfig("start_page must be less than or equal to end_page")
    })?;

    let mut out = Vec::new();
    let mut current = full.start_page.get();
    let mut morsel_id = 0usize;
    while current <= full.end_page.get() {
        let span_end = current
            .saturating_add(pages_per_morsel.saturating_sub(1))
            .min(full.end_page.get());
        let range = PageMorsel::new(
            PageNumber::new(current).expect("current page should be non-zero"),
            PageNumber::new(span_end).expect("span end page should be non-zero"),
        )
        .map_err(|_| DispatchError::InvalidConfig("invalid morsel page range"))?;
        out.push(MorselDescriptor {
            morsel_id,
            page_range: range,
            preferred_numa_node: morsel_id % numa_nodes,
        });
        morsel_id = morsel_id.saturating_add(1);
        if span_end == u32::MAX {
            break;
        }
        current = span_end.saturating_add(1);
    }

    Ok(out)
}

/// Compute an L2-aware pages-per-morsel target.
///
/// Heuristic: reserve half of L2 for the working morsel and half for operator
/// state/auxiliary data. This keeps the active morsel cache-resident while
/// avoiding overfitting to any one operator shape.
///
/// # Errors
///
/// Returns an error when `l2_cache_bytes == 0` or `page_size_bytes == 0`.
pub fn auto_tuned_pages_per_morsel(
    l2_cache_bytes: usize,
    page_size_bytes: usize,
) -> DispatchResult<u32> {
    if l2_cache_bytes == 0 {
        return Err(DispatchError::InvalidConfig(
            "l2_cache_bytes must be greater than zero",
        ));
    }
    if page_size_bytes == 0 {
        return Err(DispatchError::InvalidConfig(
            "page_size_bytes must be greater than zero",
        ));
    }
    let target_bytes = l2_cache_bytes / 2;
    let pages = (target_bytes / page_size_bytes).max(1);
    let pages_u32 = u32::try_from(pages).unwrap_or(u32::MAX);
    Ok(pages_u32.max(1))
}

/// Partition a page interval into L2 auto-tuned morsels.
///
/// # Errors
///
/// Returns an error when auto-tuning inputs are invalid or page bounds are invalid.
pub fn partition_page_morsels_auto_tuned(
    start_page: PageNumber,
    end_page: PageNumber,
    l2_cache_bytes: usize,
    page_size_bytes: usize,
    numa_nodes: usize,
) -> DispatchResult<Vec<MorselDescriptor>> {
    let pages_per_morsel = auto_tuned_pages_per_morsel(l2_cache_bytes, page_size_bytes)?;
    partition_page_morsels(start_page, end_page, pages_per_morsel, numa_nodes)
}

/// Logical pipeline identifier.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct PipelineId(pub usize);

/// Pipeline kind metadata.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum PipelineKind {
    ScanFilterProject,
    HashJoinProbe,
    AggregateUpdate,
    PipelineBreaker,
}

/// A unit of work scheduled by the dispatcher.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PipelineTask {
    pub task_id: usize,
    pub pipeline: PipelineId,
    pub kind: PipelineKind,
    pub morsel: MorselDescriptor,
}

/// Build one pipeline task per morsel.
#[must_use]
pub fn build_pipeline_tasks(
    pipeline: PipelineId,
    kind: PipelineKind,
    morsels: &[MorselDescriptor],
) -> Vec<PipelineTask> {
    morsels
        .iter()
        .map(|morsel| PipelineTask {
            task_id: morsel.morsel_id,
            pipeline,
            kind,
            morsel: *morsel,
        })
        .collect()
}

/// Dispatcher configuration.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DispatcherConfig {
    pub worker_threads: usize,
    pub numa_nodes: usize,
}

impl Default for DispatcherConfig {
    fn default() -> Self {
        let workers = thread::available_parallelism()
            .map_or(2, std::num::NonZeroUsize::get)
            .saturating_sub(1)
            .max(1);
        Self {
            worker_threads: workers,
            numa_nodes: 1,
        }
    }
}

/// Completed task record with execution metadata.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompletedTask<R> {
    pub task_id: usize,
    pub worker_id: usize,
    pub result: R,
}

/// Per-pipeline execution report.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PipelineExecution<R> {
    pub pipeline: PipelineId,
    pub completed: Vec<CompletedTask<R>>,
    pub per_worker_task_counts: Vec<usize>,
}

/// Work-stealing dispatcher with pipeline barriers.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkStealingDispatcher {
    config: DispatcherConfig,
    worker_numa: Vec<usize>,
}

fn morsel_page_span(morsel: MorselDescriptor) -> u64 {
    let start = u64::from(morsel.page_range.start_page.get());
    let end = u64::from(morsel.page_range.end_page.get());
    end.saturating_sub(start).saturating_add(1)
}

impl WorkStealingDispatcher {
    /// Create a dispatcher from explicit config.
    ///
    /// # Errors
    ///
    /// Returns an error when the worker count or NUMA node count is zero.
    pub fn try_new(config: DispatcherConfig) -> DispatchResult<Self> {
        if config.worker_threads == 0 {
            return Err(DispatchError::InvalidConfig(
                "worker_threads must be greater than zero",
            ));
        }
        if config.numa_nodes == 0 {
            return Err(DispatchError::InvalidConfig(
                "numa_nodes must be greater than zero",
            ));
        }
        let worker_numa = (0..config.worker_threads)
            .map(|worker_id| worker_id % config.numa_nodes)
            .collect();
        Ok(Self {
            config,
            worker_numa,
        })
    }

    /// NUMA node assignment per worker index.
    #[must_use]
    pub fn worker_numa_nodes(&self) -> &[usize] {
        &self.worker_numa
    }

    /// Execute pipelines with an implicit barrier between each pipeline.
    ///
    /// All tasks in `pipelines[i]` are completed before any task in
    /// `pipelines[i + 1]` begins execution.
    ///
    /// # Errors
    ///
    /// Returns an error if task metadata is inconsistent or a worker panics.
    pub fn execute_with_barriers<R, F>(
        &self,
        pipelines: &[Vec<PipelineTask>],
        execute: F,
    ) -> DispatchResult<Vec<PipelineExecution<R>>>
    where
        R: Send + 'static,
        F: Fn(&PipelineTask, usize) -> R + Send + Sync + 'static,
    {
        let execute = Arc::new(execute);
        let mut reports = Vec::with_capacity(pipelines.len());

        for tasks in pipelines {
            if tasks.is_empty() {
                continue;
            }
            let report = self.execute_single_pipeline(tasks, &execute)?;
            reports.push(report);
        }

        Ok(reports)
    }

    fn execute_single_pipeline<R, F>(
        &self,
        tasks: &[PipelineTask],
        execute: &Arc<F>,
    ) -> DispatchResult<PipelineExecution<R>>
    where
        R: Send + 'static,
        F: Fn(&PipelineTask, usize) -> R + Send + Sync + 'static,
    {
        let expected_pipeline = tasks[0].pipeline;
        let pipeline_started = Instant::now();
        for task in tasks {
            if task.pipeline != expected_pipeline {
                return Err(DispatchError::InvalidTaskSet {
                    expected_pipeline,
                    found_pipeline: task.pipeline,
                    task_id: task.task_id,
                });
            }
        }

        let workers: Vec<Worker<PipelineTask>> = (0..self.config.worker_threads)
            .map(|_| Worker::new_fifo())
            .collect();
        let stealers: Vec<Stealer<PipelineTask>> = workers.iter().map(Worker::stealer).collect();

        let mut next_by_numa = vec![0usize; self.config.numa_nodes];
        for task in tasks.iter().cloned() {
            let target = self.select_worker(task.morsel.preferred_numa_node, &mut next_by_numa);
            tracing::debug!(
                pipeline_id = expected_pipeline.0,
                task_id = task.task_id,
                target_worker = target,
                preferred_numa_node = task.morsel.preferred_numa_node,
                morsel_start_page = task.morsel.page_range.start_page.get(),
                morsel_end_page = task.morsel.page_range.end_page.get(),
                morsel_size = morsel_page_span(task.morsel),
                "morsel.schedule"
            );
            workers[target].push(task);
        }

        let mut handles = Vec::with_capacity(self.config.worker_threads);
        let start_barrier = Arc::new(Barrier::new(self.config.worker_threads));
        for (worker_id, local_worker) in workers.into_iter().enumerate() {
            let execute = Arc::clone(execute);
            let stealers = stealers.clone();
            let start_barrier = Arc::clone(&start_barrier);
            handles.push(thread::spawn(move || {
                start_barrier.wait();
                let mut completed = Vec::new();
                let mut count = 0usize;
                let mut rows_processed = 0u64;
                while let Some(task) = pop_or_steal(&local_worker, worker_id, &stealers) {
                    let morsel_size = morsel_page_span(task.morsel);
                    let span = tracing::info_span!(
                        "morsel_exec",
                        morsel_size,
                        worker_id,
                        pipeline_id = task.pipeline.0,
                        task_id = task.task_id
                    );
                    let result = {
                        let _guard = span.enter();
                        tracing::debug!(
                            worker_id,
                            pipeline_id = task.pipeline.0,
                            task_id = task.task_id,
                            morsel_size,
                            "morsel.execute.start"
                        );
                        let result = execute(&task, worker_id);
                        tracing::debug!(
                            worker_id,
                            pipeline_id = task.pipeline.0,
                            task_id = task.task_id,
                            morsel_size,
                            "morsel.execute.complete"
                        );
                        result
                    };
                    completed.push(CompletedTask {
                        task_id: task.task_id,
                        worker_id,
                        result,
                    });
                    count = count.saturating_add(1);
                    rows_processed = rows_processed.saturating_add(morsel_size);
                }
                (completed, count, rows_processed)
            }));
        }

        let mut completed = Vec::with_capacity(tasks.len());
        let mut per_worker_task_counts = vec![0usize; self.config.worker_threads];
        let mut total_rows_processed = 0u64;
        for (worker_id, handle) in handles.into_iter().enumerate() {
            let (mut worker_completed, count, rows_processed) =
                handle.join().map_err(|_| DispatchError::WorkerPanicked)?;
            per_worker_task_counts[worker_id] = count;
            total_rows_processed = total_rows_processed.saturating_add(rows_processed);
            completed.append(&mut worker_completed);
        }

        completed.sort_by_key(|entry| entry.task_id);
        let active_workers = u64::try_from(
            per_worker_task_counts
                .iter()
                .filter(|&&count| count > 0)
                .count(),
        )
        .unwrap_or(u64::MAX);
        let elapsed = pipeline_started.elapsed();
        let elapsed_micros = elapsed.as_micros().max(1);
        let throughput_rows_per_sec_u128 =
            (u128::from(total_rows_processed) * 1_000_000) / elapsed_micros;
        let throughput_rows_per_sec =
            u64::try_from(throughput_rows_per_sec_u128).unwrap_or(u64::MAX);

        FSQLITE_MORSEL_WORKERS_ACTIVE.store(active_workers, AtomicOrdering::Relaxed);
        FSQLITE_MORSEL_THROUGHPUT_ROWS_PER_SEC
            .store(throughput_rows_per_sec, AtomicOrdering::Relaxed);

        tracing::info!(
            pipeline_id = expected_pipeline.0,
            completed_tasks = completed.len(),
            worker_threads = self.config.worker_threads,
            active_workers,
            rows_processed = total_rows_processed,
            fsqlite_morsel_throughput_rows_per_sec = throughput_rows_per_sec,
            elapsed_ms = elapsed.as_millis(),
            "morsel.pipeline.complete"
        );

        Ok(PipelineExecution {
            pipeline: expected_pipeline,
            completed,
            per_worker_task_counts,
        })
    }

    fn select_worker(&self, preferred_numa_node: usize, next_by_numa: &mut [usize]) -> usize {
        let candidates: Vec<usize> = self
            .worker_numa
            .iter()
            .enumerate()
            .filter_map(|(worker_id, &node)| (node == preferred_numa_node).then_some(worker_id))
            .collect();
        if candidates.is_empty() {
            return 0;
        }
        let slot = preferred_numa_node % next_by_numa.len();
        let selected = candidates[next_by_numa[slot] % candidates.len()];
        next_by_numa[slot] = next_by_numa[slot].saturating_add(1);
        selected
    }
}

fn pop_or_steal<T>(local: &Worker<T>, worker_id: usize, stealers: &[Stealer<T>]) -> Option<T> {
    if let Some(task) = local.pop() {
        return Some(task);
    }
    steal_from_peers(worker_id, stealers)
}

fn steal_from_peers<T>(worker_id: usize, stealers: &[Stealer<T>]) -> Option<T> {
    let peer_count = stealers.len();
    if peer_count <= 1 {
        return None;
    }

    for offset in 1..peer_count {
        let peer = (worker_id + offset) % peer_count;
        loop {
            match stealers[peer].steal() {
                Steal::Success(task) => return Some(task),
                Steal::Empty => break,
                Steal::Retry => (),
            }
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;
    use std::sync::{Arc, Mutex};

    use super::*;

    const BEAD_ID: &str = "bd-14vp7.6";
    const MORSEL_BEAD_ID: &str = "bd-1rw.2";

    #[test]
    fn partition_page_morsels_covers_range_without_gaps() {
        let start = PageNumber::new(10).expect("start page should be valid");
        let end = PageNumber::new(28).expect("end page should be valid");
        let morsels = partition_page_morsels(start, end, 4, 2).expect("partition should succeed");

        let mut covered = BTreeSet::new();
        for morsel in &morsels {
            for page in morsel.page_range.start_page.get()..=morsel.page_range.end_page.get() {
                covered.insert(page);
            }
        }

        let expected: BTreeSet<u32> = (start.get()..=end.get()).collect();
        assert_eq!(
            covered, expected,
            "bead_id={BEAD_ID} partition coverage mismatch"
        );
        assert!(
            morsels.iter().all(|m| m.preferred_numa_node < 2),
            "bead_id={BEAD_ID} invalid NUMA assignment"
        );
    }

    #[test]
    fn dispatcher_enforces_pipeline_barriers() {
        let morsels = partition_page_morsels(
            PageNumber::new(1).expect("page should be valid"),
            PageNumber::new(64).expect("page should be valid"),
            4,
            1,
        )
        .expect("partition should succeed");
        let pipeline0 =
            build_pipeline_tasks(PipelineId(0), PipelineKind::ScanFilterProject, &morsels);
        let pipeline1 =
            build_pipeline_tasks(PipelineId(1), PipelineKind::AggregateUpdate, &morsels);

        let dispatcher = WorkStealingDispatcher::try_new(DispatcherConfig {
            worker_threads: 4,
            numa_nodes: 1,
        })
        .expect("dispatcher should build");

        let events = Arc::new(Mutex::new(Vec::<usize>::new()));
        let events_for_exec = Arc::clone(&events);
        let reports = dispatcher
            .execute_with_barriers(&[pipeline0, pipeline1], move |task, _worker_id| {
                events_for_exec
                    .lock()
                    .expect("event lock should not be poisoned")
                    .push(task.pipeline.0);
                task.task_id
            })
            .expect("dispatch should succeed");

        assert_eq!(
            reports.len(),
            2,
            "bead_id={BEAD_ID} expected two pipeline reports"
        );
        let (first_pipeline1, last_pipeline0) = {
            let events = events.lock().expect("event lock should not be poisoned");
            let first_pipeline1 = events
                .iter()
                .position(|pipeline| *pipeline == 1)
                .expect("pipeline 1 events should exist");
            let last_pipeline0 = events
                .iter()
                .rposition(|pipeline| *pipeline == 0)
                .expect("pipeline 0 events should exist");
            drop(events);
            (first_pipeline1, last_pipeline0)
        };
        assert!(
            last_pipeline0 < first_pipeline1,
            "bead_id={BEAD_ID} pipeline barrier violated"
        );
    }

    #[test]
    fn dispatcher_completes_all_tasks_and_uses_multiple_workers() {
        let morsels = partition_page_morsels(
            PageNumber::new(1).expect("page should be valid"),
            PageNumber::new(320).expect("page should be valid"),
            2,
            2,
        )
        .expect("partition should succeed");
        let tasks = build_pipeline_tasks(PipelineId(0), PipelineKind::ScanFilterProject, &morsels);

        let dispatcher = WorkStealingDispatcher::try_new(DispatcherConfig {
            worker_threads: 4,
            numa_nodes: 2,
        })
        .expect("dispatcher should build");
        let reports = dispatcher
            .execute_with_barriers(&[tasks], |task, worker_id| (task.task_id, worker_id))
            .expect("dispatch should succeed");
        assert_eq!(
            reports.len(),
            1,
            "bead_id={BEAD_ID} expected one pipeline report"
        );
        let report = &reports[0];
        assert_eq!(
            report.completed.len(),
            morsels.len(),
            "bead_id={BEAD_ID} incomplete task execution"
        );

        let workers_used: BTreeSet<usize> = report
            .completed
            .iter()
            .map(|entry| entry.worker_id)
            .collect();
        assert!(
            workers_used.len() >= 2,
            "bead_id={BEAD_ID} expected work across multiple workers"
        );
    }

    #[test]
    fn auto_tuned_pages_per_morsel_uses_half_l2_budget() {
        let pages = auto_tuned_pages_per_morsel(1_048_576, 4_096)
            .expect("bead_id={MORSEL_BEAD_ID} auto tuning should succeed");
        assert_eq!(
            pages, 128,
            "bead_id={MORSEL_BEAD_ID} expected 1MiB L2 and 4KiB pages to yield 128 pages per morsel",
        );
    }

    #[test]
    fn auto_tuned_partition_covers_full_range() {
        let start = PageNumber::new(1).expect("page should be valid");
        let end = PageNumber::new(512).expect("page should be valid");
        let morsels = partition_page_morsels_auto_tuned(start, end, 1_048_576, 4_096, 2)
            .expect("bead_id={MORSEL_BEAD_ID} auto tuned partition should succeed");
        assert!(
            !morsels.is_empty(),
            "bead_id={MORSEL_BEAD_ID} expected non-empty morsel partition",
        );

        let first = morsels
            .first()
            .expect("bead_id={MORSEL_BEAD_ID} expected first morsel");
        let last = morsels
            .last()
            .expect("bead_id={MORSEL_BEAD_ID} expected last morsel");
        assert_eq!(
            first.page_range.start_page, start,
            "bead_id={MORSEL_BEAD_ID} first morsel should start at requested page",
        );
        assert_eq!(
            last.page_range.end_page, end,
            "bead_id={MORSEL_BEAD_ID} last morsel should end at requested page",
        );
    }

    #[test]
    fn dispatcher_updates_morsel_metrics_gauges() {
        reset_morsel_dispatch_metrics();
        let morsels = partition_page_morsels(
            PageNumber::new(1).expect("page should be valid"),
            PageNumber::new(128).expect("page should be valid"),
            2,
            2,
        )
        .expect("partition should succeed");
        let tasks = build_pipeline_tasks(PipelineId(0), PipelineKind::ScanFilterProject, &morsels);

        let dispatcher = WorkStealingDispatcher::try_new(DispatcherConfig {
            worker_threads: 4,
            numa_nodes: 2,
        })
        .expect("dispatcher should build");
        dispatcher
            .execute_with_barriers(&[tasks], |task, _worker_id| task.task_id)
            .expect("dispatch should succeed");

        let snapshot = morsel_dispatch_metrics_snapshot();
        assert!(
            snapshot.fsqlite_morsel_workers_active >= 1,
            "bead_id={MORSEL_BEAD_ID} expected active worker gauge to be positive",
        );
        assert!(
            snapshot.fsqlite_morsel_workers_active <= 4,
            "bead_id={MORSEL_BEAD_ID} active workers should not exceed configured worker count",
        );
        assert!(
            snapshot.fsqlite_morsel_throughput_rows_per_sec > 0,
            "bead_id={MORSEL_BEAD_ID} throughput gauge should be positive",
        );
    }
}
