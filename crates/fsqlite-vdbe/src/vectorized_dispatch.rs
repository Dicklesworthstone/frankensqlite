//! Morsel-driven parallel dispatcher for vectorized pipelines (`bd-14vp7.6`).
//!
//! This module provides:
//! - page-range morsel partitioning,
//! - pipeline task definitions,
//! - crossbeam-deque work-stealing execution,
//! - pipeline barriers between pipeline waves.

use std::collections::HashMap;
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

/// Exchange operator distribution mode.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ExchangeKind {
    /// Hash-partition data across worker partitions.
    HashPartition,
    /// Broadcast data to every worker partition.
    Broadcast,
}

/// One task identifier plus the exchange hash key used for partitioning.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ExchangeTaskRef {
    pub task_id: usize,
    pub hash_key: u64,
}

/// Default hot-partition spill threshold for hash exchange.
pub const DEFAULT_EXCHANGE_HOT_PARTITION_SPLIT_THRESHOLD: usize = 32;

/// Hash-partition task ids into worker partitions with skew spill-over.
///
/// # Errors
///
/// Returns an error when `partitions == 0` or `hot_partition_split_threshold == 0`.
pub fn hash_partition_exchange(
    task_refs: &[ExchangeTaskRef],
    partitions: usize,
    hot_partition_split_threshold: usize,
) -> DispatchResult<Vec<Vec<usize>>> {
    if partitions == 0 {
        return Err(DispatchError::InvalidConfig(
            "partitions must be greater than zero",
        ));
    }
    if hot_partition_split_threshold == 0 {
        return Err(DispatchError::InvalidConfig(
            "hot_partition_split_threshold must be greater than zero",
        ));
    }

    let partitions_u64 = u64::try_from(partitions)
        .map_err(|_| DispatchError::InvalidConfig("partitions does not fit in u64"))?;
    let mut partitioned = vec![Vec::new(); partitions];

    for task_ref in task_refs {
        let hashed_partition_u64 = task_ref.hash_key % partitions_u64;
        let mut target_partition = usize::try_from(hashed_partition_u64).map_err(|_| {
            DispatchError::InvalidConfig("hashed partition index does not fit in usize")
        })?;

        if partitioned[target_partition].len() >= hot_partition_split_threshold {
            target_partition = partitioned
                .iter()
                .enumerate()
                .min_by_key(|(_, bucket)| bucket.len())
                .map_or(target_partition, |(idx, _)| idx);
        }

        partitioned[target_partition].push(task_ref.task_id);
    }

    Ok(partitioned)
}

/// Broadcast task ids to every partition.
///
/// # Errors
///
/// Returns an error when `partitions == 0`.
pub fn broadcast_exchange(
    task_ids: &[usize],
    partitions: usize,
) -> DispatchResult<Vec<Vec<usize>>> {
    if partitions == 0 {
        return Err(DispatchError::InvalidConfig(
            "partitions must be greater than zero",
        ));
    }
    Ok((0..partitions).map(|_| task_ids.to_vec()).collect())
}

/// Build exchange assignments directly from pipeline tasks.
///
/// Hash partitioning uses `task_id` as a deterministic default key; callers that
/// need data-dependent partitioning can call `hash_partition_exchange` directly
/// with explicit [`ExchangeTaskRef`] keys.
///
/// # Errors
///
/// Returns the same validation errors as the selected exchange mode.
pub fn build_exchange_task_ids(
    tasks: &[PipelineTask],
    exchange_kind: ExchangeKind,
    partitions: usize,
    hot_partition_split_threshold: usize,
) -> DispatchResult<Vec<Vec<usize>>> {
    match exchange_kind {
        ExchangeKind::HashPartition => {
            let refs = tasks
                .iter()
                .map(|task| {
                    let hash_key = u64::try_from(task.task_id)
                        .map_err(|_| DispatchError::InvalidConfig("task_id does not fit in u64"))?;
                    Ok(ExchangeTaskRef {
                        task_id: task.task_id,
                        hash_key,
                    })
                })
                .collect::<DispatchResult<Vec<_>>>()?;
            hash_partition_exchange(&refs, partitions, hot_partition_split_threshold)
        }
        ExchangeKind::Broadcast => {
            let task_ids = tasks.iter().map(|task| task.task_id).collect::<Vec<_>>();
            broadcast_exchange(&task_ids, partitions)
        }
    }
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

/// Correlation fields attached to morsel-dispatch structured logs.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DispatchRunContext {
    pub run_id: String,
    pub trace_id: u64,
    pub scenario_id: String,
}

impl DispatchRunContext {
    /// Construct and validate a dispatch run context.
    ///
    /// # Errors
    ///
    /// Returns an error when `run_id` or `scenario_id` is empty.
    pub fn try_new(run_id: String, trace_id: u64, scenario_id: String) -> DispatchResult<Self> {
        let candidate = Self {
            run_id,
            trace_id,
            scenario_id,
        };
        candidate.validate()?;
        Ok(candidate)
    }

    fn validate(&self) -> DispatchResult<()> {
        if self.run_id.trim().is_empty() {
            return Err(DispatchError::InvalidConfig(
                "dispatch run_id must be non-empty",
            ));
        }
        if self.scenario_id.trim().is_empty() {
            return Err(DispatchError::InvalidConfig(
                "dispatch scenario_id must be non-empty",
            ));
        }
        Ok(())
    }
}

impl Default for DispatchRunContext {
    fn default() -> Self {
        Self {
            run_id: "dispatch-run-unspecified".to_owned(),
            trace_id: 0,
            scenario_id: "VDBE-UNSPECIFIED".to_owned(),
        }
    }
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
        let context = DispatchRunContext::default();
        self.execute_with_barriers_with_context(pipelines, &context, execute)
    }

    /// Execute pipelines with an implicit barrier between each pipeline and
    /// include explicit run correlation fields in structured logs.
    ///
    /// # Errors
    ///
    /// Returns an error if task metadata is inconsistent, worker execution
    /// panics, or the provided `context` is invalid.
    pub fn execute_with_barriers_with_context<R, F>(
        &self,
        pipelines: &[Vec<PipelineTask>],
        context: &DispatchRunContext,
        execute: F,
    ) -> DispatchResult<Vec<PipelineExecution<R>>>
    where
        R: Send + 'static,
        F: Fn(&PipelineTask, usize) -> R + Send + Sync + 'static,
    {
        context.validate()?;
        let execute = Arc::new(execute);
        let mut reports = Vec::with_capacity(pipelines.len());

        for tasks in pipelines {
            if tasks.is_empty() {
                continue;
            }
            let report = self.execute_single_pipeline(tasks, context, &execute)?;
            reports.push(report);
        }

        Ok(reports)
    }

    fn execute_single_pipeline<R, F>(
        &self,
        tasks: &[PipelineTask],
        context: &DispatchRunContext,
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

        let use_hash_exchange = tasks
            .iter()
            .all(|task| matches!(task.kind, PipelineKind::HashJoinProbe));
        let mut task_to_worker = HashMap::<usize, usize>::new();
        if use_hash_exchange {
            let refs = tasks
                .iter()
                .map(|task| {
                    let hash_key = u64::try_from(task.task_id)
                        .map_err(|_| DispatchError::InvalidConfig("task_id does not fit in u64"))?;
                    Ok(ExchangeTaskRef {
                        task_id: task.task_id,
                        hash_key,
                    })
                })
                .collect::<DispatchResult<Vec<_>>>()?;
            let assignments = hash_partition_exchange(
                &refs,
                self.config.worker_threads,
                DEFAULT_EXCHANGE_HOT_PARTITION_SPLIT_THRESHOLD,
            )?;
            for (worker_id, task_ids) in assignments.into_iter().enumerate() {
                for task_id in task_ids {
                    task_to_worker.insert(task_id, worker_id);
                }
            }
            if task_to_worker.len() != tasks.len() {
                return Err(DispatchError::InvalidConfig(
                    "hash exchange assignment did not cover every task",
                ));
            }
        }

        let mut next_by_numa = vec![0usize; self.config.numa_nodes];
        for task in tasks.iter().cloned() {
            let (target, schedule_strategy) = if use_hash_exchange {
                let target = task_to_worker.get(&task.task_id).copied().ok_or(
                    DispatchError::InvalidConfig("hash exchange assignment missing task"),
                )?;
                (target, "hash_exchange")
            } else {
                (
                    self.select_worker(task.morsel.preferred_numa_node, &mut next_by_numa),
                    "numa_round_robin",
                )
            };
            tracing::debug!(
                pipeline_id = expected_pipeline.0,
                task_id = task.task_id,
                run_id = %context.run_id,
                trace_id = context.trace_id,
                scenario_id = %context.scenario_id,
                target_worker = target,
                preferred_numa_node = task.morsel.preferred_numa_node,
                schedule_strategy,
                morsel_start_page = task.morsel.page_range.start_page.get(),
                morsel_end_page = task.morsel.page_range.end_page.get(),
                morsel_size = morsel_page_span(task.morsel),
                "morsel.schedule"
            );
            workers[target].push(task);
        }

        let mut handles = Vec::with_capacity(self.config.worker_threads);
        let start_barrier = Arc::new(Barrier::new(self.config.worker_threads));
        let run_id = context.run_id.clone();
        let scenario_id = context.scenario_id.clone();
        let trace_id = context.trace_id;
        for (worker_id, local_worker) in workers.into_iter().enumerate() {
            let execute = Arc::clone(execute);
            let stealers = stealers.clone();
            let start_barrier = Arc::clone(&start_barrier);
            let run_id = run_id.clone();
            let scenario_id = scenario_id.clone();
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
                        run_id = %run_id,
                        trace_id,
                        scenario_id = %scenario_id,
                        pipeline_id = task.pipeline.0,
                        task_id = task.task_id
                    );
                    let result = {
                        let _guard = span.enter();
                        tracing::debug!(
                            worker_id,
                            pipeline_id = task.pipeline.0,
                            task_id = task.task_id,
                            run_id = %run_id,
                            trace_id,
                            scenario_id = %scenario_id,
                            morsel_size,
                            "morsel.execute.start"
                        );
                        let result = execute(&task, worker_id);
                        tracing::debug!(
                            worker_id,
                            pipeline_id = task.pipeline.0,
                            task_id = task.task_id,
                            run_id = %run_id,
                            trace_id,
                            scenario_id = %scenario_id,
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
            run_id = %context.run_id,
            trace_id = context.trace_id,
            scenario_id = %context.scenario_id,
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
    use std::path::PathBuf;
    use std::sync::{Arc, Mutex};

    use super::*;

    const BEAD_ID: &str = "bd-14vp7.6";
    const MORSEL_BEAD_ID: &str = "bd-1rw.2";
    const MORSEL_SCENARIO_ID: &str = "VDBE-1";
    const MORSEL_E2E_SEED: u64 = 424_242;

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    struct E2eMeasurement {
        worker_threads: usize,
        elapsed_micros: u128,
        throughput_tasks_per_sec: u128,
        active_workers: usize,
        completed_tasks: usize,
        checksum: u64,
    }

    fn synthetic_e2e_task_cost(task_id: usize, worker_id: usize, seed: u64) -> u64 {
        let task_id_u64 =
            u64::try_from(task_id).expect("bead_id={MORSEL_BEAD_ID} task id should fit in u64");
        let worker_u64 =
            u64::try_from(worker_id).expect("bead_id={MORSEL_BEAD_ID} worker id should fit in u64");
        let mut state = task_id_u64
            .wrapping_mul(6_364_136_223_846_793_005_u64)
            .wrapping_add(seed ^ worker_u64.rotate_left(7));
        for round in 0_u64..512_u64 {
            state = state
                .wrapping_mul(2_862_933_555_777_941_757_u64)
                .wrapping_add(round ^ seed);
            state ^= state.rotate_left(11);
        }
        state
    }

    fn default_e2e_artifact_path() -> PathBuf {
        PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("target")
            .join("test-results")
            .join(MORSEL_BEAD_ID)
            .join("morsel_dispatch_e2e_artifact.json")
    }

    fn escape_json(input: &str) -> String {
        input
            .replace('\\', "\\\\")
            .replace('"', "\\\"")
            .replace('\n', "\\n")
    }

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

    #[test]
    fn hash_partition_exchange_spills_skewed_keys_across_partitions() {
        let refs = (0..64)
            .map(|task_id| ExchangeTaskRef {
                task_id,
                hash_key: 7,
            })
            .collect::<Vec<_>>();

        let partitioned = hash_partition_exchange(&refs, 4, 4)
            .expect("bead_id={MORSEL_BEAD_ID} hash exchange should succeed");
        let counts = partitioned
            .iter()
            .map(std::vec::Vec::len)
            .collect::<Vec<_>>();
        let max = *counts
            .iter()
            .max()
            .expect("bead_id={MORSEL_BEAD_ID} expected non-empty counts");
        let min = *counts
            .iter()
            .min()
            .expect("bead_id={MORSEL_BEAD_ID} expected non-empty counts");

        assert_eq!(
            counts.iter().sum::<usize>(),
            refs.len(),
            "bead_id={MORSEL_BEAD_ID} exchange should assign every task exactly once",
        );
        assert!(
            max.saturating_sub(min) <= 1,
            "bead_id={MORSEL_BEAD_ID} skew spill should keep partitions balanced",
        );
    }

    #[test]
    fn broadcast_exchange_replicates_all_tasks_to_each_partition() {
        let task_ids = vec![3, 5, 8, 13];
        let partitioned = broadcast_exchange(&task_ids, 3)
            .expect("bead_id={MORSEL_BEAD_ID} broadcast should work");
        assert_eq!(
            partitioned.len(),
            3,
            "bead_id={MORSEL_BEAD_ID} expected one task list per partition",
        );
        for partition in partitioned {
            assert_eq!(
                partition, task_ids,
                "bead_id={MORSEL_BEAD_ID} each broadcast partition should receive full task list",
            );
        }
    }

    #[test]
    fn build_exchange_task_ids_supports_hash_and_broadcast_modes() {
        let morsels = partition_page_morsels(
            PageNumber::new(1).expect("page should be valid"),
            PageNumber::new(16).expect("page should be valid"),
            2,
            1,
        )
        .expect("partition should succeed");
        let tasks = build_pipeline_tasks(PipelineId(0), PipelineKind::ScanFilterProject, &morsels);

        let hash_partitioned = build_exchange_task_ids(
            &tasks,
            ExchangeKind::HashPartition,
            2,
            DEFAULT_EXCHANGE_HOT_PARTITION_SPLIT_THRESHOLD,
        )
        .expect("bead_id={MORSEL_BEAD_ID} hash-mode exchange should succeed");
        assert_eq!(
            hash_partitioned
                .iter()
                .map(std::vec::Vec::len)
                .sum::<usize>(),
            tasks.len(),
            "bead_id={MORSEL_BEAD_ID} hash-mode exchange should keep a 1:1 assignment",
        );

        let broadcast_partitioned = build_exchange_task_ids(
            &tasks,
            ExchangeKind::Broadcast,
            3,
            DEFAULT_EXCHANGE_HOT_PARTITION_SPLIT_THRESHOLD,
        )
        .expect("bead_id={MORSEL_BEAD_ID} broadcast-mode exchange should succeed");
        assert!(
            broadcast_partitioned
                .iter()
                .all(|partition| partition.len() == tasks.len()),
            "bead_id={MORSEL_BEAD_ID} broadcast-mode exchange should replicate every task per partition",
        );
    }

    #[test]
    fn dispatcher_results_are_deterministic_across_worker_counts() {
        let morsels = partition_page_morsels(
            PageNumber::new(1).expect("page should be valid"),
            PageNumber::new(160).expect("page should be valid"),
            2,
            2,
        )
        .expect("partition should succeed");
        let tasks = build_pipeline_tasks(PipelineId(0), PipelineKind::ScanFilterProject, &morsels);

        let run = |worker_threads: usize| {
            let dispatcher = WorkStealingDispatcher::try_new(DispatcherConfig {
                worker_threads,
                numa_nodes: 2.min(worker_threads),
            })
            .expect("dispatcher should build");

            let reports = dispatcher
                .execute_with_barriers(std::slice::from_ref(&tasks), |task, _worker_id| {
                    let task_id_u64 = u64::try_from(task.task_id)
                        .expect("bead_id={MORSEL_BEAD_ID} task_id should fit in u64");
                    task_id_u64
                        .wrapping_mul(6_364_136_223_846_793_005_u64)
                        .rotate_left(11)
                })
                .expect("dispatch should succeed");
            let report = reports
                .first()
                .expect("bead_id={MORSEL_BEAD_ID} expected pipeline report");
            report
                .completed
                .iter()
                .map(|entry| (entry.task_id, entry.result))
                .collect::<Vec<_>>()
        };

        let single_worker = run(1);
        let two_workers = run(2);
        let four_workers = run(4);

        assert_eq!(
            single_worker, two_workers,
            "bead_id={MORSEL_BEAD_ID} results should be deterministic across worker counts (1 vs 2)",
        );
        assert_eq!(
            single_worker, four_workers,
            "bead_id={MORSEL_BEAD_ID} results should be deterministic across worker counts (1 vs 4)",
        );
    }

    #[test]
    fn hash_join_probe_scheduling_uses_hash_exchange_strategy() {
        let morsels = partition_page_morsels(
            PageNumber::new(1).expect("page should be valid"),
            PageNumber::new(128).expect("page should be valid"),
            2,
            1,
        )
        .expect("partition should succeed");
        let tasks = build_pipeline_tasks(PipelineId(0), PipelineKind::HashJoinProbe, &morsels);

        let dispatcher = WorkStealingDispatcher::try_new(DispatcherConfig {
            worker_threads: 4,
            numa_nodes: 2,
        })
        .expect("dispatcher should build");
        let reports = dispatcher
            .execute_with_barriers(&[tasks.clone()], |task, _worker_id| task.task_id)
            .expect("dispatch should succeed");
        let report = reports
            .first()
            .expect("bead_id={MORSEL_BEAD_ID} expected pipeline report");

        assert_eq!(
            report.completed.len(),
            tasks.len(),
            "bead_id={MORSEL_BEAD_ID} hash-join probe scheduling must execute all tasks",
        );
        let odd_worker_task_count: usize = report
            .per_worker_task_counts
            .iter()
            .enumerate()
            .filter(|(worker_id, _)| worker_id % 2 == 1)
            .map(|(_, &count)| count)
            .sum();
        assert!(
            odd_worker_task_count > 0,
            "bead_id={MORSEL_BEAD_ID} hash exchange scheduling should use workers outside preferred NUMA lane when balancing",
        );
    }

    #[test]
    fn dispatch_run_context_rejects_empty_identifiers() {
        let missing_run = DispatchRunContext::try_new(String::new(), 1, "VDBE-1".to_owned());
        assert!(
            matches!(
                missing_run,
                Err(DispatchError::InvalidConfig(
                    "dispatch run_id must be non-empty"
                ))
            ),
            "bead_id={MORSEL_BEAD_ID} empty run_id should be rejected",
        );

        let missing_scenario = DispatchRunContext::try_new("run-1".to_owned(), 1, String::new());
        assert!(
            matches!(
                missing_scenario,
                Err(DispatchError::InvalidConfig(
                    "dispatch scenario_id must be non-empty"
                ))
            ),
            "bead_id={MORSEL_BEAD_ID} empty scenario_id should be rejected",
        );
    }

    #[test]
    fn morsel_dispatch_e2e_replay_emits_artifact() {
        let run_id = std::env::var("RUN_ID")
            .unwrap_or_else(|_| format!("{MORSEL_BEAD_ID}-seed-{MORSEL_E2E_SEED}"));
        let trace_id = std::env::var("TRACE_ID")
            .ok()
            .and_then(|value| value.parse::<u64>().ok())
            .unwrap_or(MORSEL_E2E_SEED);
        let scenario_id =
            std::env::var("SCENARIO_ID").unwrap_or_else(|_| MORSEL_SCENARIO_ID.to_owned());
        let seed = std::env::var("SEED")
            .ok()
            .and_then(|value| value.parse::<u64>().ok())
            .unwrap_or(MORSEL_E2E_SEED);
        let context = DispatchRunContext::try_new(run_id, trace_id, scenario_id)
            .expect("bead_id={MORSEL_BEAD_ID} context should be valid");

        let artifact_path = std::env::var("FSQLITE_MORSEL_E2E_ARTIFACT")
            .map(PathBuf::from)
            .unwrap_or_else(|_| default_e2e_artifact_path());
        if let Some(parent) = artifact_path.parent() {
            std::fs::create_dir_all(parent)
                .expect("bead_id={MORSEL_BEAD_ID} artifact directory should be writable");
        }

        let morsels = partition_page_morsels_auto_tuned(
            PageNumber::new(1).expect("page should be valid"),
            PageNumber::new(8_192).expect("page should be valid"),
            DEFAULT_L2_CACHE_BYTES,
            DEFAULT_PAGE_SIZE_BYTES,
            2,
        )
        .expect("bead_id={MORSEL_BEAD_ID} auto-tuned partition should succeed");
        let tasks = build_pipeline_tasks(PipelineId(0), PipelineKind::ScanFilterProject, &morsels);
        let replay_command = format!(
            "RUN_ID='{}' TRACE_ID={} SCENARIO_ID='{}' SEED={} FSQLITE_MORSEL_E2E_ARTIFACT='{}' cargo test -p fsqlite-vdbe vectorized_dispatch::tests::morsel_dispatch_e2e_replay_emits_artifact -- --exact --nocapture",
            context.run_id,
            context.trace_id,
            context.scenario_id,
            seed,
            artifact_path.display()
        );

        let mut measurements = Vec::new();
        let mut canonical_results = None::<Vec<(usize, u64)>>;
        for worker_threads in [1_usize, 2, 4] {
            let dispatcher = WorkStealingDispatcher::try_new(DispatcherConfig {
                worker_threads,
                numa_nodes: 2.min(worker_threads),
            })
            .expect("bead_id={MORSEL_BEAD_ID} dispatcher should build");

            let start = Instant::now();
            let reports = dispatcher
                .execute_with_barriers_with_context(
                    std::slice::from_ref(&tasks),
                    &context,
                    move |task, worker_id| synthetic_e2e_task_cost(task.task_id, worker_id, seed),
                )
                .expect("bead_id={MORSEL_BEAD_ID} dispatch should succeed");
            let elapsed_micros = start.elapsed().as_micros().max(1);
            let report = reports
                .first()
                .expect("bead_id={MORSEL_BEAD_ID} expected one pipeline report");
            let ordered_results = report
                .completed
                .iter()
                .map(|entry| (entry.task_id, entry.result))
                .collect::<Vec<_>>();
            if let Some(expected) = &canonical_results {
                assert_eq!(
                    ordered_results.as_slice(),
                    expected.as_slice(),
                    "bead_id={MORSEL_BEAD_ID} e2e scenario should remain deterministic across worker counts",
                );
            } else {
                canonical_results = Some(ordered_results.clone());
            }

            let checksum = ordered_results
                .iter()
                .fold(0_u64, |acc, (_, result)| acc ^ *result);
            let completed_tasks_u128 = u128::try_from(report.completed.len())
                .expect("bead_id={MORSEL_BEAD_ID} completed task count should fit in u128");
            let throughput_tasks_per_sec = (completed_tasks_u128 * 1_000_000) / elapsed_micros;
            let active_workers = report
                .per_worker_task_counts
                .iter()
                .filter(|&&count| count > 0)
                .count();
            measurements.push(E2eMeasurement {
                worker_threads,
                elapsed_micros,
                throughput_tasks_per_sec,
                active_workers,
                completed_tasks: report.completed.len(),
                checksum,
            });
        }

        assert_eq!(
            measurements.len(),
            3,
            "bead_id={MORSEL_BEAD_ID} expected three worker-count measurements",
        );
        assert!(
            measurements[2].active_workers >= 2,
            "bead_id={MORSEL_BEAD_ID} 4-worker run should activate at least two workers",
        );
        assert!(
            measurements[2].throughput_tasks_per_sec
                >= measurements[0].throughput_tasks_per_sec / 2,
            "bead_id={MORSEL_BEAD_ID} 4-worker throughput should not collapse relative to single-worker baseline",
        );

        let measurement_lines = measurements
            .iter()
            .map(|measurement| {
                format!(
                    "    {{\"worker_threads\":{},\"elapsed_micros\":{},\"throughput_tasks_per_sec\":{},\"active_workers\":{},\"completed_tasks\":{},\"checksum\":\"0x{:016x}\"}}",
                    measurement.worker_threads,
                    measurement.elapsed_micros,
                    measurement.throughput_tasks_per_sec,
                    measurement.active_workers,
                    measurement.completed_tasks,
                    measurement.checksum
                )
            })
            .collect::<Vec<_>>()
            .join(",\n");

        let artifact_json = format!(
            "{{\n  \"bead_id\": \"{bead_id}\",\n  \"run_id\": \"{run_id}\",\n  \"trace_id\": {trace_id},\n  \"scenario_id\": \"{scenario_id}\",\n  \"seed\": {seed},\n  \"deterministic_checksum\": true,\n  \"replay_command\": \"{replay_command}\",\n  \"measurements\": [\n{measurements}\n  ]\n}}\n",
            bead_id = MORSEL_BEAD_ID,
            run_id = escape_json(&context.run_id),
            trace_id = context.trace_id,
            scenario_id = escape_json(&context.scenario_id),
            seed = seed,
            replay_command = escape_json(&replay_command),
            measurements = measurement_lines,
        );
        std::fs::write(&artifact_path, artifact_json)
            .expect("bead_id={MORSEL_BEAD_ID} expected artifact write to succeed");
        assert!(
            artifact_path.exists(),
            "bead_id={MORSEL_BEAD_ID} expected e2e artifact file to exist",
        );

        eprintln!(
            "INFO bead_id={MORSEL_BEAD_ID} run_id={} trace_id={} scenario_id={} seed={} phase=morsel_dispatch_e2e artifact_path={}",
            context.run_id,
            context.trace_id,
            context.scenario_id,
            seed,
            artifact_path.display()
        );
    }
}
