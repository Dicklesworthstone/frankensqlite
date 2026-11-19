//! bd-q4o7z: a plain `BEGIN` whose `COMMIT` loses a page conflict is replayed
//! within `busy_timeout` when everything it exposed comes out identical, and
//! is otherwise refused with `SQLITE_BUSY_SNAPSHOT` (owner decision on
//! bd-reality-check-1022-llk6g.5.4).
//!
//! Plain `BEGIN` is promoted to `BEGIN CONCURRENT`, so two transactions that
//! write the same leaf run optimistically and the second `COMMIT` is refused
//! first-committer-wins. Stock never returns that error: its write-first
//! transaction waits at its first write and then runs on a fresh snapshot.
//! This module gives the promoted transaction the same outcome without any
//! up-front waiting or writer lock. While the transaction is open, each
//! top-level call records its SQL, its bindings and what it returned. When the
//! `COMMIT` is refused (and .5.1 has ended the transaction), the calls are
//! re-issued through the same public methods on the newest snapshot. The
//! replay commits only if every value the caller already saw comes out
//! identical:
//!
//! - every returned row (RETURNING rows and `SELECT changes()`,
//!   `total_changes()`, `last_insert_rowid()` included), digested;
//! - every count `execute` returned, i.e. `changes()` of each DML;
//! - each call's success, or its error's extended code and message;
//! - every `Connection::last_insert_rowid()` read, at its position.
//!
//! A value that only flows into a write (`VALUES(last_insert_rowid())`) never
//! reached the caller, and the replay recomputes it on the newest snapshot, as
//! stock computes it after its wait.
//!
//! A transaction is never replayed (its record is poisoned) when it:
//! - runs on a connection with an application-defined function;
//! - writes through a virtual table;
//! - runs PRAGMA, ATTACH, DETACH, VACUUM, ANALYZE, REINDEX or schema DDL, or
//!   sees the schema change;
//! - runs any SQL operation other than a recorded call (streaming
//!   `query_with_params_for_each`, the bulk `*_skip_statement_savepoint_*`
//!   calls, blob or backup APIs): `background_status()` is the gate every
//!   SQL operation passes, and it poisons a record outside a recorded call;
//! - outgrows [`REPLAY_MAX_ENTRIES`] calls, or returns more than
//!   [`REPLAY_MAX_ROWS_PER_CALL`] rows from one call.
//!
//! Explicit `BEGIN CONCURRENT` (and DEFERRED / IMMEDIATE / EXCLUSIVE) keeps its
//! own contract and is never recorded.

#[allow(clippy::wildcard_imports)]
use super::*;

use xxhash_rust::xxh3::Xxh3;

/// A transaction with more top-level calls than this stops recording.
pub(super) const REPLAY_MAX_ENTRIES: usize = 1024;
/// A call returning more rows than this stops the transaction's recording.
pub(super) const REPLAY_MAX_ROWS_PER_CALL: usize = 256;
/// A transaction with more `last_insert_rowid()` reads than this stops
/// recording.
const REPLAY_MAX_ROWID_READS: usize = 4096;

static TXN_REPLAY_COMMITTED: AtomicU64 = AtomicU64::new(0);
static TXN_REPLAY_REFUSED: AtomicU64 = AtomicU64::new(0);
static TXN_REPLAY_ATTEMPTS: AtomicU64 = AtomicU64::new(0);

/// Process-wide outcomes of refused plain-BEGIN COMMITs that were replayed.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct TxnReplayCounts {
    /// A replay committed the transaction.
    pub committed: u64,
    /// The replay diverged or ran out of `busy_timeout`, so the COMMIT was
    /// refused after all.
    pub refused: u64,
    /// Internal replay attempts over all replays: one public COMMIT can make
    /// many (bd-7dxmt), and reports one outcome.
    pub attempts: u64,
}

/// bd-q4o7z: how many refused COMMITs this process replayed, by outcome.
#[must_use]
pub fn txn_replay_counts() -> TxnReplayCounts {
    TxnReplayCounts {
        committed: TXN_REPLAY_COMMITTED.load(AtomicOrdering::Relaxed),
        refused: TXN_REPLAY_REFUSED.load(AtomicOrdering::Relaxed),
        attempts: TXN_REPLAY_ATTEMPTS.load(AtomicOrdering::Relaxed),
    }
}

/// bd-7dxmt: what a COMMIT records about its outcome. A lone COMMIT that may
/// be replayed collects these instead of recording them, so the public COMMIT
/// reports exactly one outcome however many replay attempts it makes: the
/// first attempt's if the caller gets its refusal, the committing attempt's
/// if a replay commits.
pub(super) enum CommitEvidence {
    /// `conflicts_busy_snapshot_total` + 1.
    BusySnapshotConflict,
    /// One `PRAGMA fsqlite.commit_events` row.
    Execution(CommitExecutionRecord),
    /// One SSI decision card.
    SsiCard(SsiDecisionCardDraft),
    /// One SSI e-process observation (`true`: a pivot was found).
    SsiOutcome(bool),
    /// `commits_total` + 1 and one latency sample.
    CommitMetrics { seconds: f64 },
}

/// The forms of a call that holds nothing but the transaction's BEGIN.
const LONE_BEGIN_FORMS: &[&str] = &["BEGIN", "BEGIN TRANSACTION"];
/// The forms of a call that holds nothing but the transaction's COMMIT.
const LONE_COMMIT_FORMS: &[&str] = &["COMMIT", "COMMIT TRANSACTION", "END", "END TRANSACTION"];

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(super) enum TxnReplayMode {
    /// No transaction is being recorded.
    #[default]
    Idle,
    /// A plain-BEGIN transaction is open and every call so far is recorded.
    Recording,
    /// A plain-BEGIN transaction is open but can no longer be replayed.
    Poisoned,
}

/// Per-connection replay state.
#[derive(Default)]
pub(super) struct TxnReplayState {
    mode: Cell<TxnReplayMode>,
    log: RefCell<Option<TxnReplayLog>>,
    /// Depth of public SQL calls in progress; only the outermost one records.
    call_depth: Cell<u32>,
    /// A refused transaction is being replayed: nothing records or poisons.
    replaying: Cell<bool>,
    /// Bumped each time a refused COMMIT ended its transaction (.5.1).
    refused_commit_endings: Cell<u64>,
    /// Bumped by every successful BEGIN, so a call can tell it opened a new
    /// transaction.
    begin_generation: Cell<u64>,
    /// bd-7dxmt: a lone COMMIT that may be replayed is collecting its commit
    /// evidence into `evidence` instead of recording it. The outermost call's
    /// guard records the one outcome when the call ends, however it ends.
    collecting_evidence: Cell<bool>,
    evidence: RefCell<Vec<CommitEvidence>>,
    /// While a replay runs: the first attempt's evidence, which is the outcome
    /// unless a replay attempt commits or fails for good.
    evidence_original: RefCell<Option<Vec<CommitEvidence>>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ChangeCounters {
    last_changes: usize,
    total_changes: usize,
    last_insert_rowid: i64,
    trigger_step_changes: usize,
}

struct TxnReplayLog {
    begin_counters: ChangeCounters,
    schema_generation: u64,
    entries: Vec<ReplayEntry>,
    /// `(entry index, value)`: `last_insert_rowid()` was read through the API
    /// after `index` calls had completed.
    rowid_reads: Vec<(usize, i64)>,
}

/// The public method a recorded call went through.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum ReplayCallKind {
    Query,
    QueryWithParams,
    QueryRow,
    QueryRowWithParams,
    Execute,
    ExecuteWithParams,
    ExecuteBatch,
    ExecutePrepared,
    PreparedQuery,
    PreparedQueryWithParams,
    PreparedQueryRow,
    PreparedQueryRowWithParams,
    PreparedExecute,
    PreparedExecuteWithParams,
    BeginTransaction,
    CommitTransaction,
}

impl ReplayCallKind {
    /// Whether a lone COMMIT issued through this method may be replayed and
    /// then re-issued as the replay's own COMMIT. `query_row` is left out: a
    /// committed `query_row("COMMIT")` still errors (no row), which would make
    /// a committed replay indistinguishable from a failed one.
    const fn can_carry_replayed_commit(self) -> bool {
        !matches!(
            self,
            Self::QueryRow
                | Self::QueryRowWithParams
                | Self::PreparedQueryRow
                | Self::PreparedQueryRowWithParams
                | Self::BeginTransaction
        )
    }
}

struct ReplayCall {
    kind: ReplayCallKind,
    sql: Rc<str>,
    params: Vec<SqliteValue>,
}

struct ReplayEntry {
    call: ReplayCall,
    outcome: ReplayOutcome,
}

/// What a call exposed to its caller.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum ReplayOutcome {
    Rows { count: usize, digest: u128 },
    Row(u128),
    Count(usize),
    Unit,
    Error { extended_code: i32, message: String },
}

/// A replayed call's return value.
pub(super) enum ReplayValue {
    Rows(Vec<Row>),
    Row(Row),
    Count(usize),
    Unit,
}

/// The return types of the recorded public methods.
pub(super) trait ReplayOutput: Sized {
    /// What `result` exposed, or `None` when it is too large to record.
    fn outcome(result: &Result<Self>) -> Option<ReplayOutcome>;
    /// This call's value from a replayed call of the same method.
    fn from_value(value: ReplayValue) -> Option<Self>;
}

impl ReplayOutput for Vec<Row> {
    fn outcome(result: &Result<Self>) -> Option<ReplayOutcome> {
        match result {
            Ok(rows) => rows_outcome(rows),
            Err(error) => Some(error_outcome(error)),
        }
    }
    fn from_value(value: ReplayValue) -> Option<Self> {
        match value {
            ReplayValue::Rows(rows) => Some(rows),
            _ => None,
        }
    }
}

impl ReplayOutput for Row {
    fn outcome(result: &Result<Self>) -> Option<ReplayOutcome> {
        Some(match result {
            Ok(row) => ReplayOutcome::Row(digest_rows(std::slice::from_ref(row))),
            Err(error) => error_outcome(error),
        })
    }
    fn from_value(value: ReplayValue) -> Option<Self> {
        match value {
            ReplayValue::Row(row) => Some(row),
            _ => None,
        }
    }
}

impl ReplayOutput for usize {
    fn outcome(result: &Result<Self>) -> Option<ReplayOutcome> {
        Some(match result {
            Ok(count) => ReplayOutcome::Count(*count),
            Err(error) => error_outcome(error),
        })
    }
    fn from_value(value: ReplayValue) -> Option<Self> {
        match value {
            ReplayValue::Count(count) => Some(count),
            _ => None,
        }
    }
}

impl ReplayOutput for () {
    fn outcome(result: &Result<Self>) -> Option<ReplayOutcome> {
        Some(match result {
            Ok(()) => ReplayOutcome::Unit,
            Err(error) => error_outcome(error),
        })
    }
    fn from_value(value: ReplayValue) -> Option<Self> {
        match value {
            ReplayValue::Unit => Some(()),
            _ => None,
        }
    }
}

fn rows_outcome(rows: &[Row]) -> Option<ReplayOutcome> {
    (rows.len() <= REPLAY_MAX_ROWS_PER_CALL).then(|| ReplayOutcome::Rows {
        count: rows.len(),
        digest: digest_rows(rows),
    })
}

fn error_outcome(error: &FrankenError) -> ReplayOutcome {
    ReplayOutcome::Error {
        extended_code: error.extended_error_code(),
        message: error.to_string(),
    }
}

fn value_outcome(result: &Result<ReplayValue>) -> Option<ReplayOutcome> {
    match result {
        Ok(ReplayValue::Rows(rows)) => rows_outcome(rows),
        Ok(ReplayValue::Row(row)) => {
            Some(ReplayOutcome::Row(digest_rows(std::slice::from_ref(row))))
        }
        Ok(ReplayValue::Count(count)) => Some(ReplayOutcome::Count(*count)),
        Ok(ReplayValue::Unit) => Some(ReplayOutcome::Unit),
        Err(error) => Some(error_outcome(error)),
    }
}

/// An exact digest of rows: each value's storage class and its bytes (a REAL
/// by its bit pattern), so `1` and `1.0`, or `'1'` and `x'31'`, differ.
fn digest_rows(rows: &[Row]) -> u128 {
    let mut hasher = Xxh3::new();
    for row in rows {
        let values = row.values();
        hasher.update(&(values.len() as u64).to_le_bytes());
        for value in values {
            match value {
                SqliteValue::Null => hasher.update(&[0]),
                SqliteValue::Integer(integer) => {
                    hasher.update(&[1]);
                    hasher.update(&integer.to_le_bytes());
                }
                SqliteValue::Float(float) => {
                    hasher.update(&[2]);
                    hasher.update(&float.to_bits().to_le_bytes());
                }
                SqliteValue::Text(text) => {
                    let bytes = text.as_bytes_direct();
                    hasher.update(&[3]);
                    hasher.update(&(bytes.len() as u64).to_le_bytes());
                    hasher.update(bytes);
                }
                SqliteValue::Blob(blob) => {
                    hasher.update(&[4]);
                    hasher.update(&(blob.len() as u64).to_le_bytes());
                    hasher.update(blob);
                }
            }
        }
    }
    hasher.digest128()
}

/// Whether `sql` is, by its text, exactly one of `forms`: surrounding
/// whitespace and one trailing semicolon allowed, case ignored. Anything else
/// (comments, extra spaces, a second statement) does not count.
fn sql_is_lone(sql: &str, forms: &[&str]) -> bool {
    let body = sql.trim_matches(|c: char| c.is_ascii_whitespace());
    let body = body
        .strip_suffix(';')
        .unwrap_or(body)
        .trim_end_matches(|c: char| c.is_ascii_whitespace());
    forms.iter().any(|form| body.eq_ignore_ascii_case(form))
}

/// Whether a statement may run inside a replayable transaction. Everything
/// else (PRAGMA, ATTACH, DETACH, VACUUM, ANALYZE, REINDEX, DDL, and any kind
/// added later) poisons the record.
const fn statement_is_replayable(statement: &Statement) -> bool {
    matches!(
        statement,
        Statement::Select(_)
            | Statement::Insert(_)
            | Statement::Update(_)
            | Statement::Delete(_)
            | Statement::Begin(_)
            | Statement::Commit
            | Statement::Rollback(_)
            | Statement::Savepoint(_)
            | Statement::Release(_)
            | Statement::Explain { .. }
    )
}

/// Returns `call_depth` to zero when the outermost call ends, however it ends.
/// bd-7dxmt: when that call is a lone COMMIT still collecting its evidence
/// (it was not replayed, its replay stayed refused, or its future was
/// dropped), this records the call's one outcome.
pub(super) struct OutermostCallGuard<'a>(&'a Connection);

/// What the outermost public call saw before it ran (see
/// [`Connection::txn_replay_enter`]); holds the call's depth guard.
pub(super) struct ReplayCallScope<'a> {
    guard: Option<OutermostCallGuard<'a>>,
    kind: ReplayCallKind,
    recording: bool,
    lone_commit: bool,
    generation: u64,
    refusals: u64,
    vtab_writes: u64,
    started: Option<Instant>,
}

impl Drop for OutermostCallGuard<'_> {
    fn drop(&mut self) {
        self.0.txn_replay.call_depth.set(0);
        if self.0.txn_replay.collecting_evidence.get() {
            self.0.finish_commit_evidence();
        }
    }
}

/// Ends the replay, however it ends. A replay transaction still open at that
/// point (the COMMIT future was dropped mid-replay, or its rollback failed)
/// is handed to the abandoned-transaction cleanup, so no later statement
/// lands in it.
struct ReplayingGuard<'a>(&'a Connection);

impl Drop for ReplayingGuard<'_> {
    fn drop(&mut self) {
        self.0.txn_replay.replaying.set(false);
        if self.0.in_transaction.get() {
            self.0.mark_transaction_cleanup_required();
        }
    }
}

/// What [`Connection::txn_replay_leave`] hands back to the public method: the
/// call's result, or the replay of the transaction its refused COMMIT ended,
/// which the method awaits.
pub(super) enum ReplayLeave<'a, T> {
    Done(Result<T>),
    Replay(Pin<Box<dyn Future<Output = Result<T>> + 'a>>),
}

enum ReplayAttempt {
    Committed(ReplayValue),
    /// A transient failure: roll back and try again within the budget.
    Retry(Option<FrankenError>),
    /// Something the caller saw would differ: refuse.
    Diverged,
    /// The replay's COMMIT failed for good with this error.
    Failed(FrankenError),
}

impl Connection {
    fn change_counters(&self) -> ChangeCounters {
        ChangeCounters {
            last_changes: self.last_changes.get(),
            total_changes: self.total_changes.get(),
            last_insert_rowid: self.last_insert_rowid.get(),
            trigger_step_changes: self.trigger_step_changes.get(),
        }
    }

    fn restore_change_counters(&self, counters: ChangeCounters) {
        self.trigger_step_changes.set(counters.trigger_step_changes);
        self.restore_change_tracking_state(
            counters.last_changes,
            counters.total_changes,
            counters.last_insert_rowid,
        );
    }

    fn txn_replay_reset(&self) {
        self.txn_replay.mode.set(TxnReplayMode::Idle);
        self.txn_replay.log.borrow_mut().take();
    }

    /// Stop recording this transaction: it will be refused, not replayed.
    pub(super) fn txn_replay_poison(&self) {
        if self.txn_replay.mode.get() == TxnReplayMode::Recording {
            self.txn_replay.mode.set(TxnReplayMode::Poisoned);
            self.txn_replay.log.borrow_mut().take();
        }
    }

    /// `background_status()` runs at the start of every SQL operation. One
    /// that runs while a transaction records, outside any recorded call, is
    /// an operation the record cannot re-issue.
    pub(super) fn txn_replay_note_operation(&self) {
        if self.txn_replay.mode.get() == TxnReplayMode::Recording
            && self.txn_replay.call_depth.get() == 0
        {
            self.txn_replay_poison();
        }
    }

    /// Statement dispatch: a statement kind that is never replayed, or a
    /// write to a virtual table, poisons the record.
    pub(super) fn txn_replay_note_statement(&self, statement: &Statement) {
        if self.txn_replay.mode.get() != TxnReplayMode::Recording {
            return;
        }
        let virtual_target = match statement {
            Statement::Insert(insert) => Some(insert.table.name.as_str()),
            Statement::Update(update) => Some(update.table.name.name.as_str()),
            Statement::Delete(delete) => Some(delete.table.name.name.as_str()),
            _ => None,
        }
        .is_some_and(|name| self.table_name_is_virtual(name));
        if virtual_target || !statement_is_replayable(statement) {
            self.txn_replay_poison();
        }
    }

    /// The tail of a successful BEGIN. `promoted_plain` is a plain BEGIN that
    /// `concurrent_mode_default` promoted.
    pub(super) fn txn_replay_note_begin(&self, promoted_plain: bool) {
        if self.txn_replay.replaying.get() {
            return;
        }
        self.txn_replay
            .begin_generation
            .set(self.txn_replay.begin_generation.get().wrapping_add(1));
        let mode = if !promoted_plain {
            TxnReplayMode::Idle
        } else if self.func_registry.borrow().has_application_functions() {
            TxnReplayMode::Poisoned
        } else {
            *self.txn_replay.log.borrow_mut() = Some(TxnReplayLog {
                begin_counters: self.change_counters(),
                schema_generation: self.schema_generation.get(),
                entries: Vec::new(),
                rowid_reads: Vec::new(),
            });
            TxnReplayMode::Recording
        };
        if mode != TxnReplayMode::Recording {
            self.txn_replay.log.borrow_mut().take();
        }
        self.txn_replay.mode.set(mode);
    }

    /// `end_transaction_after_refused_commit` ended the transaction.
    pub(super) fn txn_replay_note_refused_commit_ended(&self) {
        self.txn_replay
            .refused_commit_endings
            .set(self.txn_replay.refused_commit_endings.get().wrapping_add(1));
    }

    /// bd-7dxmt: record one piece of commit evidence, or collect it while a
    /// lone COMMIT that may be replayed is running (see [`CommitEvidence`]).
    pub(super) fn commit_evidence(&self, item: CommitEvidence) {
        if self.txn_replay.collecting_evidence.get() {
            self.txn_replay.evidence.borrow_mut().push(item);
        } else {
            self.apply_commit_evidence(item);
        }
    }

    fn apply_commit_evidence(&self, item: CommitEvidence) {
        match item {
            CommitEvidence::BusySnapshotConflict => {
                if !fsqlite_observability::metrics::metrics_disabled() {
                    fsqlite_observability::metrics::global()
                        .conflicts_busy_snapshot_total
                        .inc();
                }
            }
            CommitEvidence::Execution(record) => {
                self.commit_execution_capture.borrow_mut().record(record);
            }
            CommitEvidence::SsiCard(card) => self.ssi_evidence_ledger.record_async(card),
            CommitEvidence::SsiOutcome(pivot_detected) => self.observe_ssi_outcome(pivot_detected),
            CommitEvidence::CommitMetrics { seconds } => {
                let metrics = fsqlite_observability::metrics::global();
                metrics.commits_total.inc();
                metrics.commit_duration_seconds.observe(seconds);
            }
        }
    }

    fn take_commit_evidence(&self) -> Vec<CommitEvidence> {
        std::mem::take(&mut *self.txn_replay.evidence.borrow_mut())
    }

    /// Put an emptied evidence buffer back, keeping its allocation.
    fn keep_commit_evidence_buffer(&self, mut buffer: Vec<CommitEvidence>) {
        buffer.clear();
        let mut slot = self.txn_replay.evidence.borrow_mut();
        if slot.is_empty() && slot.capacity() < buffer.capacity() {
            *slot = buffer;
        }
    }

    /// bd-7dxmt: a lone COMMIT that may be replayed starts collecting its
    /// evidence. Out of line, so the per-call enter stays small.
    #[inline(never)]
    fn txn_replay_begin_commit_evidence(&self) {
        self.txn_replay.collecting_evidence.set(true);
        self.txn_replay.evidence.borrow_mut().clear();
        drop(self.txn_replay.evidence_original.take());
    }

    /// End the collection and record `chosen`, the one outcome the public
    /// COMMIT reports (a replay attempt that committed or failed for good).
    fn settle_commit_evidence(&self, chosen: Vec<CommitEvidence>) {
        self.txn_replay.collecting_evidence.set(false);
        drop(self.txn_replay.evidence_original.take());
        self.txn_replay.evidence.borrow_mut().clear();
        for item in chosen {
            self.apply_commit_evidence(item);
        }
    }

    /// The outermost call ended while its lone COMMIT was still collecting:
    /// a COMMIT that was not replayed (what it collected is its outcome), a
    /// replay that stayed refused or diverged (the first attempt's refusal
    /// is), or a dropped future.
    #[inline(never)]
    fn finish_commit_evidence(&self) {
        self.txn_replay.collecting_evidence.set(false);
        let mut collected = self.take_commit_evidence();
        if let Some(original) = self.txn_replay.evidence_original.take() {
            collected.clear();
            for item in original {
                self.apply_commit_evidence(item);
            }
        } else {
            self.apply_commit_evidence_draining(&mut collected);
        }
        // The buffer keeps its capacity for the next COMMIT: the common
        // COMMIT that is never replayed allocates nothing.
        self.keep_commit_evidence_buffer(collected);
    }

    /// Apply every item of `buffer` in order and leave it empty, with its
    /// allocation, for the next COMMIT.
    fn apply_commit_evidence_draining(&self, buffer: &mut Vec<CommitEvidence>) {
        for item in buffer.drain(..) {
            self.apply_commit_evidence(item);
        }
    }

    /// bd-7dxmt / bd-29qkj: a replay attempt is internal to one public COMMIT,
    /// so none of its conflict events reach the shared observer: neither its
    /// commit preparation's nor its statements' page-lock contention. Set on
    /// the attempt's own transaction right after its BEGIN; the next BEGIN's
    /// handle reset clears it.
    fn txn_replay_quiet_attempt(&self) {
        let Some(session_id) = *self.concurrent_session_id.borrow() else {
            return;
        };
        let registry = lock_unpoisoned(&self.concurrent_registry);
        if let Some(mut handle) = registry.get_mut(session_id) {
            handle.set_observation_quiet(true);
        }
    }

    /// The caller read `last_insert_rowid()` through the API.
    pub(super) fn txn_replay_note_rowid_read(&self, value: i64) {
        if self.txn_replay.mode.get() != TxnReplayMode::Recording {
            return;
        }
        let full = {
            let mut log = self.txn_replay.log.borrow_mut();
            match log.as_mut() {
                Some(log) if log.rowid_reads.len() < REPLAY_MAX_ROWID_READS => {
                    let at = log.entries.len();
                    log.rowid_reads.push((at, value));
                    false
                }
                _ => true,
            }
        };
        if full {
            self.txn_replay_poison();
        }
    }

    /// Start a public call that runs no SQL of its own (`prepare`): it
    /// neither records nor poisons. The guard ends the call.
    pub(super) fn txn_replay_enter_neutral(&self) -> Option<OutermostCallGuard<'_>> {
        if self.txn_replay.call_depth.get() > 0 || self.txn_replay.replaying.get() {
            return None;
        }
        self.txn_replay.call_depth.set(1);
        Some(OutermostCallGuard(self))
    }

    /// Start one public SQL call. Only the outermost call records (not one
    /// nested in another, and none during a replay). The call's own future is
    /// awaited by its public method in place, between this and
    /// [`Self::txn_replay_leave`], so the record never moves it. Inlined like
    /// the leave: a prepared bulk insert enters once per row (bd-q4o7z probe
    /// 5), and the lone-COMMIT evidence setup stays out of line (bd-7dxmt).
    #[inline]
    pub(super) fn txn_replay_enter(&self, kind: ReplayCallKind, sql: &str) -> ReplayCallScope<'_> {
        let Some(guard) = self.txn_replay_enter_neutral() else {
            return ReplayCallScope {
                guard: None,
                kind,
                recording: false,
                lone_commit: false,
                generation: 0,
                refusals: 0,
                vtab_writes: 0,
                started: None,
            };
        };
        if self.txn_replay.mode.get() != TxnReplayMode::Idle && !self.in_transaction.get() {
            self.txn_replay_reset();
        }
        let recording = self.txn_replay.mode.get() == TxnReplayMode::Recording;
        let lone_commit = recording
            && kind.can_carry_replayed_commit()
            && (kind == ReplayCallKind::CommitTransaction || sql_is_lone(sql, LONE_COMMIT_FORMS));
        if lone_commit {
            // bd-7dxmt: the guard records the collected outcome when the
            // call ends.
            self.txn_replay_begin_commit_evidence();
        }
        ReplayCallScope {
            guard: Some(guard),
            kind,
            recording,
            lone_commit,
            generation: self.txn_replay.begin_generation.get(),
            refusals: self.txn_replay.refused_commit_endings.get(),
            vtab_writes: if recording {
                fsqlite_func::vtab::virtual_table_write_count()
            } else {
                0
            },
            started: lone_commit.then(Instant::now),
        }
    }

    /// Finish a public SQL call started with [`Self::txn_replay_enter`]:
    /// record it while a plain-BEGIN transaction is open, and hand back the
    /// replay of the transaction when this call is its COMMIT and the COMMIT
    /// was refused.
    ///
    /// It is synchronous, so a call that records nothing (any call outside
    /// a plain-BEGIN transaction, a nested call, a `BEGIN CONCURRENT`
    /// transaction's calls) adds no future of its own: awaiting an async
    /// leave after every call cost 6.5% of a one-writer bulk insert through
    /// a prepared statement (mt-mvcc-bench, bd-q4o7z probe 5). The call that
    /// records nothing returns from this inlined part; recording is out of
    /// line.
    #[inline]
    pub(super) fn txn_replay_leave<'a, T: ReplayOutput + 'a>(
        &'a self,
        scope: ReplayCallScope<'a>,
        sql: &str,
        params: &[SqliteValue],
        result: Result<T>,
    ) -> ReplayLeave<'a, T> {
        if scope.guard.is_none() {
            return ReplayLeave::Done(result);
        }
        if !scope.recording {
            if self.txn_replay.begin_generation.get() != scope.generation
                || self.txn_replay.mode.get() != TxnReplayMode::Idle
            {
                self.txn_replay_leave_unrecorded_call(scope.kind, scope.generation, sql);
            }
            return ReplayLeave::Done(result);
        }
        self.txn_replay_leave_recording_call(scope, sql, params, result)
    }

    /// [`Self::txn_replay_leave`] for a call that did not record, when a
    /// transaction began inside it or the record is not idle.
    #[inline(never)]
    fn txn_replay_leave_unrecorded_call(&self, kind: ReplayCallKind, generation: u64, sql: &str) {
        if self.txn_replay.begin_generation.get() != generation
            && kind != ReplayCallKind::BeginTransaction
            && !sql_is_lone(sql, LONE_BEGIN_FORMS)
        {
            // The transaction began inside a call that also ran other
            // statements, which the record does not hold.
            self.txn_replay_poison();
        }
        if self.txn_replay.mode.get() != TxnReplayMode::Idle && !self.in_transaction.get() {
            self.txn_replay_reset();
        }
    }

    /// [`Self::txn_replay_leave`] for a call made while the transaction
    /// records.
    #[inline(never)]
    fn txn_replay_leave_recording_call<'a, T: ReplayOutput + 'a>(
        &'a self,
        scope: ReplayCallScope<'a>,
        sql: &str,
        params: &[SqliteValue],
        result: Result<T>,
    ) -> ReplayLeave<'a, T> {
        let kind = scope.kind;
        let generation = scope.generation;
        let refusals = scope.refusals;
        let vtab_writes = scope.vtab_writes;

        if scope.lone_commit {
            let refused_and_ended = matches!(&result, Err(FrankenError::BusySnapshot { .. }))
                && self.txn_replay.refused_commit_endings.get() != refusals
                && !self.in_transaction.get()
                && self.txn_replay.begin_generation.get() == generation;
            if refused_and_ended
                && let Some(log) = self.txn_replay_take_replayable_log()
                && let Err(refusal) = result
            {
                let commit_call = ReplayCall {
                    kind,
                    sql: Rc::from(sql),
                    params: params.to_vec(),
                };
                let started = scope.started.unwrap_or_else(Instant::now);
                // The replay runs inside this outermost call: its depth guard
                // lives until the replay ends.
                let outermost = scope.guard;
                // bd-7dxmt: the first attempt's evidence waits for the
                // replay's outcome; the attempts keep collecting.
                if self.txn_replay.collecting_evidence.get() {
                    *self.txn_replay.evidence_original.borrow_mut() =
                        Some(self.take_commit_evidence());
                }
                return ReplayLeave::Replay(Box::pin(async move {
                    let _outermost = outermost;
                    self.replay_refused_transaction(log, commit_call, refusal, started)
                        .await
                        .and_then(|value| {
                            T::from_value(value).ok_or_else(|| {
                                FrankenError::internal(
                                    "a replayed COMMIT returned another kind of value",
                                )
                            })
                        })
                }));
            }
            if !self.in_transaction.get() {
                self.txn_replay_reset();
            }
            return ReplayLeave::Done(result);
        }

        if !self.in_transaction.get() {
            self.txn_replay_reset();
            return ReplayLeave::Done(result);
        }
        if self.txn_replay.begin_generation.get() != generation
            || fsqlite_func::vtab::virtual_table_write_count() != vtab_writes
        {
            // A new transaction began inside this call, or the call wrote
            // through a virtual table.
            self.txn_replay_poison();
            return ReplayLeave::Done(result);
        }
        if self.txn_replay.mode.get() != TxnReplayMode::Recording {
            return ReplayLeave::Done(result);
        }
        let recorded = match T::outcome(&result) {
            Some(outcome) => {
                let mut log = self.txn_replay.log.borrow_mut();
                match log.as_mut() {
                    Some(log)
                        if log.entries.len() < REPLAY_MAX_ENTRIES
                            && log.schema_generation == self.schema_generation.get() =>
                    {
                        log.entries.push(ReplayEntry {
                            call: ReplayCall {
                                kind,
                                sql: Rc::from(sql),
                                params: params.to_vec(),
                            },
                            outcome,
                        });
                        true
                    }
                    _ => false,
                }
            }
            None => false,
        };
        if !recorded {
            self.txn_replay_poison();
        }
        ReplayLeave::Done(result)
    }

    /// The record of the transaction a refused COMMIT just ended, if it may
    /// be replayed.
    fn txn_replay_take_replayable_log(&self) -> Option<TxnReplayLog> {
        if self.txn_replay.mode.get() != TxnReplayMode::Recording {
            return None;
        }
        self.txn_replay.mode.set(TxnReplayMode::Idle);
        let log = self.txn_replay.log.borrow_mut().take()?;
        if self.func_registry.borrow().has_application_functions() {
            return None;
        }
        Some(log)
    }

    /// Re-issue a recorded call through the public method it first went
    /// through.
    fn run_replay_call<'a>(
        &'a self,
        call: &'a ReplayCall,
    ) -> Pin<Box<dyn Future<Output = Result<ReplayValue>> + 'a>> {
        Box::pin(async move {
            let sql = call.sql.as_ref();
            let params = call.params.as_slice();
            match call.kind {
                ReplayCallKind::Query => self.query(sql).await.map(ReplayValue::Rows),
                ReplayCallKind::QueryWithParams => self
                    .query_with_params(sql, params)
                    .await
                    .map(ReplayValue::Rows),
                ReplayCallKind::QueryRow => self.query_row(sql).await.map(ReplayValue::Row),
                ReplayCallKind::QueryRowWithParams => self
                    .query_row_with_params(sql, params)
                    .await
                    .map(ReplayValue::Row),
                ReplayCallKind::Execute => self.execute(sql).await.map(ReplayValue::Count),
                ReplayCallKind::ExecuteWithParams => self
                    .execute_with_params(sql, params)
                    .await
                    .map(ReplayValue::Count),
                ReplayCallKind::ExecuteBatch => {
                    self.execute_batch(sql).await.map(|()| ReplayValue::Unit)
                }
                ReplayCallKind::ExecutePrepared => {
                    let stmt = self.prepare(sql).await?;
                    self.execute_prepared_with_params(&stmt, params)
                        .await
                        .map(ReplayValue::Count)
                }
                ReplayCallKind::PreparedQuery => self
                    .prepare(sql)
                    .await?
                    .query()
                    .await
                    .map(ReplayValue::Rows),
                ReplayCallKind::PreparedQueryWithParams => self
                    .prepare(sql)
                    .await?
                    .query_with_params(params)
                    .await
                    .map(ReplayValue::Rows),
                ReplayCallKind::PreparedQueryRow => self
                    .prepare(sql)
                    .await?
                    .query_row()
                    .await
                    .map(ReplayValue::Row),
                ReplayCallKind::PreparedQueryRowWithParams => self
                    .prepare(sql)
                    .await?
                    .query_row_with_params(params)
                    .await
                    .map(ReplayValue::Row),
                ReplayCallKind::PreparedExecute => self
                    .prepare(sql)
                    .await?
                    .execute()
                    .await
                    .map(ReplayValue::Count),
                ReplayCallKind::PreparedExecuteWithParams => self
                    .prepare(sql)
                    .await?
                    .execute_with_params(params)
                    .await
                    .map(ReplayValue::Count),
                ReplayCallKind::CommitTransaction => {
                    self.commit_transaction().await.map(|()| ReplayValue::Unit)
                }
                ReplayCallKind::BeginTransaction => Err(FrankenError::internal(
                    "a transaction's BEGIN is never a replayed call",
                )),
            }
        })
    }

    /// Whether every `last_insert_rowid()` read the caller made after `at`
    /// calls reads the same value now.
    fn replay_rowid_reads_match(&self, log: &TxnReplayLog, at: usize) -> bool {
        let now = self.last_insert_rowid.get();
        log.rowid_reads
            .iter()
            .filter(|(index, _)| *index == at)
            .all(|(_, value)| *value == now)
    }

    async fn replay_rollback(&self, cx: &Cx) {
        if self.in_transaction.get() {
            let full_rollback = fsqlite_ast::RollbackStatement { to_savepoint: None };
            if let Err(error) = Box::pin(self.execute_rollback_with_cx(cx, &full_rollback)).await {
                tracing::debug!(%error, "rolling back a replay attempt failed");
            }
        }
    }

    /// One replay attempt on the newest snapshot.
    async fn replay_attempt(
        &self,
        cx: &Cx,
        log: &TxnReplayLog,
        commit_call: &ReplayCall,
    ) -> ReplayAttempt {
        self.restore_change_counters(log.begin_counters);
        if let Err(error) =
            Box::pin(self.execute_begin(fsqlite_ast::BeginStatement { mode: None })).await
        {
            return if error.is_transient() {
                ReplayAttempt::Retry(None)
            } else {
                ReplayAttempt::Diverged
            };
        }
        if !self.is_concurrent_transaction() {
            // The connection stopped promoting plain BEGIN.
            self.replay_rollback(cx).await;
            return ReplayAttempt::Diverged;
        }
        self.txn_replay_quiet_attempt();
        for (index, entry) in log.entries.iter().enumerate() {
            if !self.replay_rowid_reads_match(log, index) {
                self.replay_rollback(cx).await;
                return ReplayAttempt::Diverged;
            }
            let result = self.run_replay_call(&entry.call).await;
            if value_outcome(&result).as_ref() != Some(&entry.outcome) {
                let transient = matches!(&result, Err(error) if error.is_transient());
                self.replay_rollback(cx).await;
                return if transient {
                    ReplayAttempt::Retry(None)
                } else {
                    ReplayAttempt::Diverged
                };
            }
            if !self.in_transaction.get() {
                return ReplayAttempt::Diverged;
            }
        }
        if !self.replay_rowid_reads_match(log, log.entries.len()) {
            self.replay_rollback(cx).await;
            return ReplayAttempt::Diverged;
        }
        match self.run_replay_call(commit_call).await {
            Ok(value) => ReplayAttempt::Committed(value),
            Err(error) if self.in_transaction.get() => {
                self.replay_rollback(cx).await;
                if error.is_transient() {
                    ReplayAttempt::Retry(None)
                } else {
                    ReplayAttempt::Diverged
                }
            }
            Err(error @ FrankenError::BusySnapshot { .. }) => ReplayAttempt::Retry(Some(error)),
            Err(error) => ReplayAttempt::Failed(error),
        }
    }

    /// Replay the transaction `log` recorded, whose COMMIT (`commit_call`)
    /// was refused with `refusal` and ended the transaction, within
    /// `busy_timeout` of `started`.
    ///
    /// bd-7dxmt: the attempts are internal. The first attempt's commit
    /// evidence waits in `evidence_original`; each attempt's own is collected
    /// and kept only when the caller gets that attempt's outcome (it
    /// committed, or failed for good). A replay refused after all reports the
    /// first attempt's refusal, once, when the outermost call's guard drops.
    async fn replay_refused_transaction(
        &self,
        log: TxnReplayLog,
        commit_call: ReplayCall,
        mut refusal: FrankenError,
        started: Instant,
    ) -> Result<ReplayValue> {
        let busy_timeout_ms =
            u64::try_from(self.pragma_state.borrow().busy_timeout_ms.max(0)).unwrap_or(0);
        let deadline = Duration::from_millis(busy_timeout_ms);
        if deadline.is_zero() {
            return Err(refusal);
        }
        let refusal_counters = self.change_counters();
        let cx = self.op_cx_after_background_status();
        self.txn_replay.replaying.set(true);
        let _replaying = ReplayingGuard(self);
        let mut handoff = BeginBusyRetryHandoff::default();
        let mut first_attempt = true;
        loop {
            if first_attempt {
                first_attempt = false;
            } else {
                let Some(wait) = handoff.next_wait(started, deadline) else {
                    break;
                };
                if let Err(error) = perform_begin_busy_retry_handoff(&cx, wait).await {
                    self.restore_change_counters(refusal_counters);
                    return Err(error);
                }
            }
            TXN_REPLAY_ATTEMPTS.fetch_add(1, AtomicOrdering::Relaxed);
            self.txn_replay.evidence.borrow_mut().clear();
            let attempt = self.replay_attempt(&cx, &log, &commit_call).await;
            let attempt_evidence = self.take_commit_evidence();
            match attempt {
                ReplayAttempt::Committed(value) => {
                    TXN_REPLAY_COMMITTED.fetch_add(1, AtomicOrdering::Relaxed);
                    self.settle_commit_evidence(attempt_evidence);
                    return Ok(value);
                }
                ReplayAttempt::Retry(error) => {
                    if let Some(error) = error {
                        refusal = error;
                    }
                }
                ReplayAttempt::Diverged => break,
                ReplayAttempt::Failed(error) => {
                    self.settle_commit_evidence(attempt_evidence);
                    return Err(error);
                }
            }
        }
        TXN_REPLAY_REFUSED.fetch_add(1, AtomicOrdering::Relaxed);
        self.restore_change_counters(refusal_counters);
        // The outermost call's guard records the first attempt's refusal.
        Err(refusal)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lone_statement_forms_admit_only_the_bare_keyword() {
        for sql in ["BEGIN", "begin;", "  BEGIN TRANSACTION ;\n", "Begin\t"] {
            assert!(sql_is_lone(sql, LONE_BEGIN_FORMS), "{sql:?}");
        }
        for sql in [
            "BEGIN; INSERT INTO t VALUES (1)",
            "BEGIN CONCURRENT",
            "BEGIN IMMEDIATE",
            "BEGIN  TRANSACTION",
            "/* c */ BEGIN",
            "BEGIN;;",
        ] {
            assert!(!sql_is_lone(sql, LONE_BEGIN_FORMS), "{sql:?}");
        }
        for sql in [
            "COMMIT",
            "end;",
            "COMMIT TRANSACTION",
            " End Transaction ; ",
        ] {
            assert!(sql_is_lone(sql, LONE_COMMIT_FORMS), "{sql:?}");
        }
        for sql in [
            "INSERT INTO t VALUES (1); COMMIT",
            "COMMIT; BEGIN",
            "ROLLBACK",
        ] {
            assert!(!sql_is_lone(sql, LONE_COMMIT_FORMS), "{sql:?}");
        }
    }

    #[test]
    fn row_digest_distinguishes_storage_classes_and_order() {
        let row = |values: Vec<SqliteValue>| Row { values };
        let one_int = digest_rows(&[row(vec![SqliteValue::Integer(1)])]);
        let one_real = digest_rows(&[row(vec![SqliteValue::Float(1.0)])]);
        let one_text = digest_rows(&[row(vec![SqliteValue::Text("1".into())])]);
        let one_blob = digest_rows(&[row(vec![SqliteValue::Blob(Arc::from(&b"1"[..]))])]);
        let null = digest_rows(&[row(vec![SqliteValue::Null])]);
        let all = [one_int, one_real, one_text, one_blob, null];
        for (i, a) in all.iter().enumerate() {
            for b in &all[i + 1..] {
                assert_ne!(a, b);
            }
        }
        let ab = digest_rows(&[
            row(vec![SqliteValue::Integer(1)]),
            row(vec![SqliteValue::Integer(2)]),
        ]);
        let ba = digest_rows(&[
            row(vec![SqliteValue::Integer(2)]),
            row(vec![SqliteValue::Integer(1)]),
        ]);
        let merged = digest_rows(&[row(vec![SqliteValue::Integer(1), SqliteValue::Integer(2)])]);
        assert_ne!(ab, ba, "row order is exposed");
        assert_ne!(ab, merged, "row boundaries are exposed");
        assert_eq!(
            ab,
            digest_rows(&[
                row(vec![SqliteValue::Integer(1)]),
                row(vec![SqliteValue::Integer(2)]),
            ])
        );
    }

    #[test]
    fn only_dml_reads_and_transaction_control_are_replayable() {
        let parse = |sql: &str| {
            let (mut statements, errors) = Parser::from_sql(sql).parse_all();
            assert!(errors.is_empty(), "{sql}: {errors:?}");
            statements.remove(0)
        };
        for sql in [
            "SELECT 1",
            "INSERT INTO t VALUES (1)",
            "UPDATE t SET a = 1",
            "DELETE FROM t",
            "SAVEPOINT s",
            "RELEASE s",
            "ROLLBACK TO s",
            "EXPLAIN SELECT 1",
        ] {
            assert!(statement_is_replayable(&parse(sql)), "{sql}");
        }
        for sql in [
            "PRAGMA user_version = 1",
            "ATTACH 'x.db' AS x",
            "DETACH x",
            "CREATE TABLE u(a)",
            "CREATE INDEX i ON t(a)",
            "DROP TABLE t",
            "ALTER TABLE t ADD COLUMN b",
            "VACUUM",
            "ANALYZE",
            "REINDEX",
        ] {
            assert!(!statement_is_replayable(&parse(sql)), "{sql}");
        }
    }
}
