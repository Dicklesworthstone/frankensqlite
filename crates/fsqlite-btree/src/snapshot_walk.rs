//! Walking one b-tree of a committed snapshot on several threads
//! (bd-w4pr1.13.11).
//!
//! [`split_subtrees`] descends the interior pages of a b-tree until it has an
//! ordered list of disjoint subtrees that together hold every leaf;
//! [`ParallelWalk::run`] hands contiguous runs of those subtrees to threads,
//! each of which walks its subtrees depth-first through a
//! [`SnapshotPageReader`] into one page buffer of its own. Every run produces
//! its own state, and the states come back in key order, so a caller can fold
//! them exactly as a serial scan in key order would have.
//!
//! A walk uses about one thread per [`LEAF_PAGES_PER_THREAD`] leaf pages, up
//! to the workers asked for; a tree too small for two threads is left to the
//! serial path. The threads beyond each statement's own come from one budget
//! for the whole process ([`parallel_helper_budget`]), taken without waiting:
//! a walk gets what is left, and with nothing left the serial path runs.

use std::cell::Cell;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Mutex, OnceLock};

use fsqlite_error::{FrankenError, Result};
use fsqlite_pager::{SnapshotPageBuf, SnapshotPageReader};
use fsqlite_types::PageNumber;

use crate::cell::{self, BtreePageHeader};

/// Leaf pages that justify one more thread in a walk.
pub const LEAF_PAGES_PER_THREAD: u64 = 32;

/// Subtrees of one b-tree, in key order, that together hold every leaf once.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BtreeSplit {
    /// Roots of the subtrees, left to right.
    pub subtrees: Vec<PageNumber>,
    /// Cells of the interior pages above the subtrees. For an index b-tree
    /// each is an entry; a table b-tree's are only separators.
    pub cells_above: u64,
    /// Leaf pages of the tree: exact when the subtrees are leaves, otherwise
    /// the subtrees times the children of the first of them (a lower bound
    /// for trees more than one level deeper).
    pub leaf_pages: u64,
}

impl BtreeSplit {
    /// Threads a walk of this tree is worth, at most `workers`, at least 1.
    #[must_use]
    pub fn threads_for(&self, workers: usize) -> usize {
        let by_size =
            usize::try_from(self.leaf_pages / LEAF_PAGES_PER_THREAD).unwrap_or(usize::MAX);
        workers.min(by_size).max(1)
    }
}

/// A leaf page of the tree being walked.
#[derive(Debug, Clone, Copy)]
pub struct LeafPage<'a> {
    /// Its page number.
    pub page_no: PageNumber,
    /// Its bytes, up to the usable size.
    pub bytes: &'a [u8],
    /// Its parsed header.
    pub header: BtreePageHeader,
}

/// Split the b-tree rooted at `root` into at least `min_parts` subtrees when
/// it has that many pages on one level, descending one whole level at a time
/// and never past the leaves.
pub fn split_subtrees(
    reader: &SnapshotPageReader,
    root: PageNumber,
    usable_size: u32,
    min_parts: usize,
) -> Result<BtreeSplit> {
    let mut buf = reader.new_buffer();
    let mut frontier = vec![root];
    let mut cells_above = 0_u64;
    while frontier.len() < min_parts {
        let mut next = Vec::with_capacity(frontier.len().saturating_mul(64));
        for &page_no in &frontier {
            let page = cell::usable_prefix(reader.read_page(page_no, &mut buf)?, usable_size);
            let header = cell::parse_page_header(page, page_no)?;
            if header.page_type.is_leaf() {
                // Every leaf of a b-tree is on the same level.
                let leaf_pages = frontier.len() as u64;
                return Ok(BtreeSplit {
                    subtrees: frontier,
                    cells_above,
                    leaf_pages,
                });
            }
            cells_above += u64::from(header.cell_count);
            push_children(page, page_no, &header, &mut next)?;
        }
        frontier = next;
    }
    // The frontier may itself be the leaves: its first page says which.
    let first = frontier[0];
    let page = cell::usable_prefix(reader.read_page(first, &mut buf)?, usable_size);
    let header = cell::parse_page_header(page, first)?;
    let per_subtree = if header.page_type.is_leaf() {
        1
    } else {
        u64::from(header.cell_count) + 1
    };
    let leaf_pages = (frontier.len() as u64).saturating_mul(per_subtree);
    Ok(BtreeSplit {
        subtrees: frontier,
        cells_above,
        leaf_pages,
    })
}

/// Append the child pages of interior page `page_no`, left to right.
fn push_children(
    page: &[u8],
    page_no: PageNumber,
    header: &BtreePageHeader,
    out: &mut Vec<PageNumber>,
) -> Result<()> {
    let right = header
        .right_child
        .ok_or_else(|| FrankenError::DatabaseCorrupt {
            detail: format!("interior page {} has no right child", page_no.get()),
        })?;
    let pointers =
        cell::header_offset_for_page(page_no) + usize::from(header.page_type.header_size());
    for idx in 0..usize::from(header.cell_count) {
        let at = pointers + idx * 2;
        let ptr = page
            .get(at..at + 2)
            .map(|b| usize::from(u16::from_be_bytes([b[0], b[1]])))
            .ok_or_else(|| FrankenError::DatabaseCorrupt {
                detail: format!(
                    "cell pointer {idx} of page {} is past the page",
                    page_no.get()
                ),
            })?;
        out.push(cell::CellRef::read_interior_left_child(page, ptr)?);
    }
    out.push(right);
    Ok(())
}

/// Walk the subtree rooted at `root` depth-first, left to right, calling
/// `visit` on each leaf. Returns the number of interior cells passed, or
/// `None` when `visit` or `stop` ended the walk early.
fn walk_subtree(
    reader: &SnapshotPageReader,
    buf: &mut SnapshotPageBuf,
    stack: &mut Vec<PageNumber>,
    root: PageNumber,
    usable_size: u32,
    stop: &(dyn Fn() -> bool + Sync),
    visit: &mut dyn FnMut(LeafPage<'_>) -> Result<bool>,
) -> Result<Option<u64>> {
    stack.clear();
    stack.push(root);
    let mut interior_cells = 0_u64;
    let mut children = Vec::new();
    while let Some(page_no) = stack.pop() {
        if stop() {
            return Ok(None);
        }
        let page = cell::usable_prefix(reader.read_page(page_no, buf)?, usable_size);
        let header = cell::parse_page_header(page, page_no)?;
        if header.page_type.is_leaf() {
            if !visit(LeafPage {
                page_no,
                bytes: page,
                header,
            })? {
                return Ok(None);
            }
            continue;
        }
        interior_cells += u64::from(header.cell_count);
        children.clear();
        push_children(page, page_no, &header, &mut children)?;
        stack.extend(children.iter().rev());
    }
    Ok(Some(interior_cells))
}

/// How a [`ParallelWalk::run`] ended.
#[derive(Debug)]
pub enum WalkOutcome<S> {
    /// Every subtree was walked. One state per run of subtrees, in key order,
    /// each with the interior cells its subtrees held.
    Done(Vec<(S, u64)>),
    /// A visitor declined or `stop` asked to stop; the states are dropped.
    Stopped,
    /// The walk did not start: one thread is all it could have had. The
    /// caller runs its serial path.
    Serial(SerialReason),
}

/// Why a walk left its tree to the serial path.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SerialReason {
    /// Too few leaf pages for a second thread.
    SmallTree,
    /// The process's helper budget had no thread left.
    BudgetExhausted,
}

/// What the last [`ParallelWalk::run`] on a thread did.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WalkRecord {
    /// It walked on `workers` threads (the caller's included) granted from
    /// the budget, of which `threads` walked at least one run.
    Ran { workers: usize, threads: usize },
    /// It left the tree to the serial path.
    Serial(SerialReason),
}

/// Threads walking one split b-tree.
pub struct ParallelWalk<'a> {
    /// Pages of the snapshot.
    pub reader: &'a SnapshotPageReader,
    /// Usable bytes of each page.
    pub usable_size: u32,
    /// Threads to use at most, the calling thread included.
    pub workers: usize,
    /// Polled before every page; `true` stops every thread.
    pub stop: &'a (dyn Fn() -> bool + Sync),
}

thread_local! {
    static LAST_WALK: Cell<Option<WalkRecord>> = const { Cell::new(None) };
}

/// What the last [`ParallelWalk::run`] on this thread did, taken (so a
/// second call is `None`). `None` when no walk was attempted.
#[must_use]
pub fn take_last_parallel_walk() -> Option<WalkRecord> {
    LAST_WALK.with(Cell::take)
}

fn record_walk(record: WalkRecord) {
    LAST_WALK.with(|last| last.set(Some(record)));
}

/// Helper threads (beyond each walking statement's own) the process runs for
/// walks at once: the machine's available parallelism.
#[must_use]
pub fn parallel_helper_budget() -> usize {
    static BUDGET: OnceLock<usize> = OnceLock::new();
    *BUDGET.get_or_init(|| std::thread::available_parallelism().map_or(1, usize::from))
}

static HELPERS_IN_USE: AtomicUsize = AtomicUsize::new(0);
static HELPERS_HIGH_WATER: AtomicUsize = AtomicUsize::new(0);

/// The most helper threads granted at once since the process started.
#[must_use]
pub fn parallel_helpers_high_water() -> usize {
    HELPERS_HIGH_WATER.load(Ordering::Relaxed)
}

/// Helper threads taken from the budget; returned when dropped.
struct HelperGrant(usize);

impl HelperGrant {
    /// Up to `want` helpers, as many as the budget has left, without waiting.
    fn take(want: usize) -> Self {
        let budget = parallel_helper_budget();
        let mut in_use = HELPERS_IN_USE.load(Ordering::Relaxed);
        loop {
            let grant = want.min(budget.saturating_sub(in_use));
            if grant == 0 {
                return Self(0);
            }
            match HELPERS_IN_USE.compare_exchange_weak(
                in_use,
                in_use + grant,
                Ordering::AcqRel,
                Ordering::Relaxed,
            ) {
                Ok(_) => {
                    HELPERS_HIGH_WATER.fetch_max(in_use + grant, Ordering::Relaxed);
                    return Self(grant);
                }
                Err(now) => in_use = now,
            }
        }
    }
}

impl Drop for HelperGrant {
    fn drop(&mut self) {
        if self.0 > 0 {
            HELPERS_IN_USE.fetch_sub(self.0, Ordering::AcqRel);
        }
    }
}

impl ParallelWalk<'_> {
    /// Walk every subtree of `split` on the threads its size is worth (at
    /// most `workers`) that the process budget grants, or return
    /// [`WalkOutcome::Serial`] when that is one thread. Runs of consecutive
    /// subtrees are taken by threads in order of an atomic counter; each run
    /// starts a fresh state from `new_state`, and `visit` sees the run's
    /// leaves in key order with that state and the thread's own scratch from
    /// `new_scratch`. `visit` returns `Ok(false)` to decline the whole walk.
    pub fn run<W, S, NW, NS, V>(
        &self,
        split: &BtreeSplit,
        new_scratch: NW,
        new_state: NS,
        visit: V,
    ) -> Result<WalkOutcome<S>>
    where
        S: Send,
        NW: Fn() -> W + Sync,
        NS: Fn() -> S + Sync,
        V: Fn(&mut W, &mut S, LeafPage<'_>) -> Result<bool> + Sync,
    {
        let wanted = split.threads_for(self.workers.max(1));
        if wanted <= 1 {
            record_walk(WalkRecord::Serial(SerialReason::SmallTree));
            return Ok(WalkOutcome::Serial(SerialReason::SmallTree));
        }
        let helpers = HelperGrant::take(wanted - 1);
        if helpers.0 == 0 {
            record_walk(WalkRecord::Serial(SerialReason::BudgetExhausted));
            return Ok(WalkOutcome::Serial(SerialReason::BudgetExhausted));
        }
        let workers = helpers.0 + 1;
        let subtrees = &split.subtrees;
        let runs = subtrees.len().min(workers.saturating_mul(8)).max(1);
        let next_run = AtomicUsize::new(0);
        let halt = AtomicBool::new(false);
        let finished: Mutex<Vec<(usize, S, u64)>> = Mutex::new(Vec::with_capacity(runs));
        let first_error: Mutex<Option<FrankenError>> = Mutex::new(None);
        let threads_used = AtomicUsize::new(0);
        let stop = || halt.load(Ordering::Relaxed) || (self.stop)();

        let work = || {
            let mut scratch = new_scratch();
            let mut buf = self.reader.new_buffer();
            let mut stack = Vec::new();
            let mut used = false;
            loop {
                if stop() {
                    break;
                }
                let run = next_run.fetch_add(1, Ordering::Relaxed);
                if run >= runs {
                    break;
                }
                used = true;
                let begin = run * subtrees.len() / runs;
                let end = (run + 1) * subtrees.len() / runs;
                let mut state = new_state();
                let mut interior = 0_u64;
                let mut completed = true;
                for &root in &subtrees[begin..end] {
                    let walked = walk_subtree(
                        self.reader,
                        &mut buf,
                        &mut stack,
                        root,
                        self.usable_size,
                        &stop,
                        &mut |leaf| visit(&mut scratch, &mut state, leaf),
                    );
                    match walked {
                        Ok(Some(cells)) => interior += cells,
                        Ok(None) => {
                            completed = false;
                            break;
                        }
                        Err(error) => {
                            first_error
                                .lock()
                                .unwrap_or_else(std::sync::PoisonError::into_inner)
                                .get_or_insert(error);
                            completed = false;
                            break;
                        }
                    }
                }
                if !completed {
                    halt.store(true, Ordering::Relaxed);
                    break;
                }
                finished
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .push((run, state, interior));
            }
            if used {
                threads_used.fetch_add(1, Ordering::Relaxed);
            }
        };

        std::thread::scope(|scope| {
            for _ in 1..workers.min(runs) {
                // A thread the system refuses leaves its share to the others.
                if std::thread::Builder::new()
                    .name("fsqlite-scan".to_owned())
                    .spawn_scoped(scope, work)
                    .is_err()
                {
                    break;
                }
            }
            work();
        });
        drop(helpers);

        record_walk(WalkRecord::Ran {
            workers,
            threads: threads_used.load(Ordering::Relaxed),
        });
        if let Some(error) = first_error
            .into_inner()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
        {
            return Err(error);
        }
        if halt.load(Ordering::Relaxed) {
            return Ok(WalkOutcome::Stopped);
        }
        let mut finished = finished
            .into_inner()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if finished.len() != runs {
            return Ok(WalkOutcome::Stopped);
        }
        finished.sort_unstable_by_key(|(run, _, _)| *run);
        Ok(WalkOutcome::Done(
            finished
                .into_iter()
                .map(|(_, state, interior)| (state, interior))
                .collect(),
        ))
    }
}

/// Entries of the b-tree rooted at `root`, counted on up to `workers` threads.
///
/// The rows of a table b-tree, the keys of an index b-tree (its interior
/// cells included). `Ok(None)` when `stop` stopped the count or the walk left
/// the tree to the serial path ([`WalkOutcome::Serial`]).
pub fn count_entries_parallel(
    reader: &SnapshotPageReader,
    root: PageNumber,
    usable_size: u32,
    workers: usize,
    stop: &(dyn Fn() -> bool + Sync),
) -> Result<Option<i64>> {
    let split = split_subtrees(reader, root, usable_size, workers.saturating_mul(4))?;
    let is_index = {
        let mut buf = reader.new_buffer();
        let page = cell::usable_prefix(reader.read_page(root, &mut buf)?, usable_size);
        cell::parse_page_header(page, root)?.page_type.is_index()
    };
    let walk = ParallelWalk {
        reader,
        usable_size,
        workers,
        stop,
    };
    let outcome = walk.run(
        &split,
        || (),
        || 0_u64,
        |(), count, leaf| {
            *count += u64::from(leaf.header.cell_count);
            Ok(true)
        },
    )?;
    let WalkOutcome::Done(runs) = outcome else {
        return Ok(None);
    };
    let mut total = runs.iter().map(|(leaves, _)| *leaves).sum::<u64>();
    if is_index {
        total += split.cells_above + runs.iter().map(|(_, interior)| *interior).sum::<u64>();
    }
    Ok(Some(i64::try_from(total).unwrap_or(i64::MAX)))
}
