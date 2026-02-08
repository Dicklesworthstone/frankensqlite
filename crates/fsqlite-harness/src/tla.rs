//! TLA+ export for MVCC / SSI protocol traces.
//!
//! This is deliberately small and dependency-free so it can be used from any
//! crate's tests without pulling in a full TLA+ toolchain at build time.

use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::fmt;

/// A rendered TLA+ module.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TlaModule {
    /// Module name (`---- MODULE <name> ----`).
    pub name: String,
    /// Full TLA+ source.
    pub source: String,
}

impl fmt::Display for TlaModule {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.source)
    }
}

/// Minimal TLA+ value model for emitting states as records/sequences/sets.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TlaValue {
    /// Natural number (non-negative).
    Nat(u64),
    /// Integer (signed).
    Int(i64),
    /// Boolean.
    Bool(bool),
    /// String literal.
    Str(String),
    /// TLA+ sequence literal `<<...>>`.
    Seq(Vec<Self>),
    /// TLA+ set literal `{...}`.
    Set(Vec<Self>),
    /// TLA+ record literal `[k |-> v, ...]`.
    Record(BTreeMap<String, Self>),
}

impl TlaValue {
    fn push_tla(&self, out: &mut String) {
        match self {
            Self::Nat(n) => {
                out.push_str(&n.to_string());
            }
            Self::Int(i) => {
                out.push_str(&i.to_string());
            }
            Self::Bool(b) => {
                out.push_str(if *b { "TRUE" } else { "FALSE" });
            }
            Self::Str(s) => {
                out.push('"');
                push_escaped_tla_string(out, s);
                out.push('"');
            }
            Self::Seq(items) => {
                out.push_str("<<");
                for (idx, item) in items.iter().enumerate() {
                    if idx != 0 {
                        out.push_str(", ");
                    }
                    item.push_tla(out);
                }
                out.push_str(">>");
            }
            Self::Set(items) => {
                out.push('{');
                for (idx, item) in items.iter().enumerate() {
                    if idx != 0 {
                        out.push_str(", ");
                    }
                    item.push_tla(out);
                }
                out.push('}');
            }
            Self::Record(fields) => {
                out.push('[');
                for (idx, (k, v)) in fields.iter().enumerate() {
                    if idx != 0 {
                        out.push_str(", ");
                    }
                    out.push_str(k);
                    out.push_str(" |-> ");
                    v.push_tla(out);
                }
                out.push(']');
            }
        }
    }
}

impl fmt::Display for TlaValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut s = String::new();
        self.push_tla(&mut s);
        f.write_str(&s)
    }
}

fn push_escaped_tla_string(out: &mut String, s: &str) {
    // TLA+ strings support C-style escapes.
    for ch in s.chars() {
        match ch {
            '\\' => out.push_str("\\\\"),
            '"' => out.push_str("\\\""),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            other => out.push(other),
        }
    }
}

/// A single MVCC state snapshot rendered as a record.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MvccStateSnapshot {
    /// Human label for the step (shows up in the trace state record).
    pub label: String,
    /// Variables captured at this step.
    pub vars: BTreeMap<String, TlaValue>,
}

impl MvccStateSnapshot {
    /// Convert the snapshot into a single TLA+ record value.
    #[must_use]
    pub fn to_record_value(&self) -> TlaValue {
        let mut fields = BTreeMap::new();
        fields.insert("label".to_string(), TlaValue::Str(self.label.clone()));
        for (k, v) in &self.vars {
            fields.insert(k.clone(), v.clone());
        }
        TlaValue::Record(fields)
    }
}

/// Exports a concrete MVCC trace (sequence of snapshots) as a bounded TLA+ behavior.
#[derive(Debug, Clone)]
pub struct MvccTlaExporter {
    snapshots: Vec<MvccStateSnapshot>,
}

/// Action kinds used for Mazurkiewicz-trace exploration of MVCC schedules.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum MvccActionKind {
    /// `BEGIN` starts a transaction snapshot.
    Begin,
    /// A page read.
    Read { page_id: u32 },
    /// A page write.
    Write { page_id: u32 },
    /// Commit attempt with declared write-set footprint.
    Commit { write_set: BTreeSet<u32> },
    /// GC horizon advance request.
    Gc { horizon_seq: u64 },
}

/// An action in the MVCC alphabet used by trace-monoid reasoning.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct MvccAction {
    /// Transaction identifier.
    pub txn_id: u64,
    /// Stable per-transaction ordinal to preserve program order.
    pub ordinal: u32,
    /// Action kind.
    pub kind: MvccActionKind,
}

impl MvccAction {
    /// Construct a `BEGIN` action.
    #[must_use]
    pub fn begin(txn_id: u64, ordinal: u32) -> Self {
        Self {
            txn_id,
            ordinal,
            kind: MvccActionKind::Begin,
        }
    }

    /// Construct a `READ` action.
    #[must_use]
    pub fn read(txn_id: u64, ordinal: u32, page_id: u32) -> Self {
        Self {
            txn_id,
            ordinal,
            kind: MvccActionKind::Read { page_id },
        }
    }

    /// Construct a `WRITE` action.
    #[must_use]
    pub fn write(txn_id: u64, ordinal: u32, page_id: u32) -> Self {
        Self {
            txn_id,
            ordinal,
            kind: MvccActionKind::Write { page_id },
        }
    }

    /// Construct a `COMMIT` action.
    #[must_use]
    pub fn commit(txn_id: u64, ordinal: u32, write_set: impl IntoIterator<Item = u32>) -> Self {
        Self {
            txn_id,
            ordinal,
            kind: MvccActionKind::Commit {
                write_set: write_set.into_iter().collect(),
            },
        }
    }

    /// Construct a `GC` action.
    #[must_use]
    pub fn gc(txn_id: u64, ordinal: u32, horizon_seq: u64) -> Self {
        Self {
            txn_id,
            ordinal,
            kind: MvccActionKind::Gc { horizon_seq },
        }
    }
}

/// Independence relation `I` used for the MVCC trace monoid.
///
/// This follows §4.4:
/// - `read/read` independent
/// - `read/write` dependent on same page
/// - `write/write` independent iff pages differ
/// - `commit/commit` dependent
/// - `begin/begin` dependent
/// - `read/commit` dependent iff read page is in commit write-set
/// - same-transaction actions are always dependent (program-order edge)
#[must_use]
pub fn are_independent(lhs: &MvccAction, rhs: &MvccAction) -> bool {
    if lhs == rhs || lhs.txn_id == rhs.txn_id {
        return false;
    }
    match (&lhs.kind, &rhs.kind) {
        (MvccActionKind::Begin | MvccActionKind::Gc { .. }, MvccActionKind::Begin)
        | (MvccActionKind::Begin, MvccActionKind::Gc { .. })
        | (MvccActionKind::Commit { .. }, MvccActionKind::Commit { .. })
        | (MvccActionKind::Gc { .. }, _)
        | (_, MvccActionKind::Gc { .. }) => false,
        // In §4.4 only begin/begin is explicitly dependent; begin may commute
        // with other transactions' non-begin actions.
        (MvccActionKind::Begin, _)
        | (_, MvccActionKind::Begin)
        | (MvccActionKind::Read { .. }, MvccActionKind::Read { .. }) => true,
        (
            MvccActionKind::Read { page_id },
            MvccActionKind::Write {
                page_id: write_page,
            },
        )
        | (
            MvccActionKind::Write {
                page_id: write_page,
            },
            MvccActionKind::Read { page_id },
        ) => page_id != write_page,
        (
            MvccActionKind::Write { page_id: lhs_page },
            MvccActionKind::Write { page_id: rhs_page },
        ) => lhs_page != rhs_page,
        (MvccActionKind::Write { page_id }, MvccActionKind::Commit { write_set })
        | (MvccActionKind::Commit { write_set }, MvccActionKind::Write { page_id }) => {
            !write_set.contains(page_id)
        }
        (MvccActionKind::Commit { write_set }, MvccActionKind::Read { page_id })
        | (MvccActionKind::Read { page_id }, MvccActionKind::Commit { write_set }) => {
            !write_set.contains(page_id)
        }
    }
}

/// Foata normal form: layers of mutually independent actions.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FoataNormalForm {
    /// Canonical layers, each sorted deterministically.
    pub layers: Vec<Vec<MvccAction>>,
}

impl FoataNormalForm {
    /// Deterministic textual signature used for class deduplication.
    #[must_use]
    pub fn canonical_signature(&self) -> String {
        let mut out = String::new();
        for (layer_idx, layer) in self.layers.iter().enumerate() {
            if layer_idx != 0 {
                out.push('|');
            }
            out.push('[');
            for (item_idx, action) in layer.iter().enumerate() {
                if item_idx != 0 {
                    out.push(',');
                }
                push_action_signature(&mut out, action);
            }
            out.push(']');
        }
        out
    }
}

fn push_action_signature(out: &mut String, action: &MvccAction) {
    out.push_str(&action.txn_id.to_string());
    out.push(':');
    out.push_str(&action.ordinal.to_string());
    out.push(':');
    match &action.kind {
        MvccActionKind::Begin => out.push('B'),
        MvccActionKind::Read { page_id } => {
            out.push('R');
            out.push_str(&page_id.to_string());
        }
        MvccActionKind::Write { page_id } => {
            out.push('W');
            out.push_str(&page_id.to_string());
        }
        MvccActionKind::Commit { write_set } => {
            out.push('C');
            out.push('{');
            for (idx, page_id) in write_set.iter().enumerate() {
                if idx != 0 {
                    out.push(';');
                }
                out.push_str(&page_id.to_string());
            }
            out.push('}');
        }
        MvccActionKind::Gc { horizon_seq } => {
            out.push('G');
            out.push_str(&horizon_seq.to_string());
        }
    }
}

/// Compute Foata normal form for a concrete schedule.
#[must_use]
pub fn foata_normal_form(word: &[MvccAction]) -> FoataNormalForm {
    let mut level_by_action = Vec::with_capacity(word.len());
    for (idx, action) in word.iter().enumerate() {
        let mut level = 0_usize;
        for (prev_idx, prev) in word.iter().take(idx).enumerate() {
            if !are_independent(prev, action) {
                level = level.max(level_by_action[prev_idx] + 1);
            }
        }
        level_by_action.push(level);
    }

    let max_level = level_by_action.iter().copied().max().unwrap_or(0);
    let mut layers = vec![Vec::new(); max_level.saturating_add(1)];
    for (action, level) in word.iter().cloned().zip(level_by_action) {
        layers[level].push(action);
    }
    for layer in &mut layers {
        layer.sort_unstable();
    }
    FoataNormalForm { layers }
}

/// A trace-equivalence class with one representative schedule.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TraceClass {
    /// Canonical Foata representative for this class.
    pub canonical: FoataNormalForm,
    /// One concrete schedule in the class.
    pub representative: Vec<MvccAction>,
    /// Number of linearizations collapsed into this class.
    pub member_count: usize,
}

/// Reduction metrics comparing naive interleavings vs trace classes.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct TraceReductionStats {
    /// Number of naive interleavings preserving intra-transaction order.
    pub naive_interleavings: usize,
    /// Number of distinct Mazurkiewicz classes.
    pub trace_classes: usize,
    /// Reduction factor (`naive_interleavings / trace_classes`).
    pub reduction_factor: f64,
}

/// Enumerate all order-preserving interleavings of transaction action chains.
#[must_use]
pub fn enumerate_interleavings(chains: &[Vec<MvccAction>]) -> Vec<Vec<MvccAction>> {
    let total_actions: usize = chains.iter().map(Vec::len).sum();
    let mut positions = vec![0_usize; chains.len()];
    let mut prefix = Vec::with_capacity(total_actions);
    let mut out = Vec::new();
    enumerate_interleavings_rec(chains, &mut positions, &mut prefix, &mut out);
    out
}

fn enumerate_interleavings_rec(
    chains: &[Vec<MvccAction>],
    positions: &mut [usize],
    prefix: &mut Vec<MvccAction>,
    out: &mut Vec<Vec<MvccAction>>,
) {
    let done = positions
        .iter()
        .zip(chains)
        .all(|(idx, chain)| *idx >= chain.len());
    if done {
        out.push(prefix.clone());
        return;
    }

    for chain_idx in 0..chains.len() {
        if positions[chain_idx] >= chains[chain_idx].len() {
            continue;
        }
        let action = chains[chain_idx][positions[chain_idx]].clone();
        positions[chain_idx] += 1;
        prefix.push(action);
        enumerate_interleavings_rec(chains, positions, prefix, out);
        let _ = prefix.pop();
        positions[chain_idx] -= 1;
    }
}

/// Enumerate all distinct trace-equivalence classes.
#[must_use]
pub fn enumerate_trace_classes(chains: &[Vec<MvccAction>]) -> Vec<TraceClass> {
    let mut classes = BTreeMap::<String, TraceClass>::new();
    for schedule in enumerate_interleavings(chains) {
        let canonical = foata_normal_form(&schedule);
        let signature = canonical.canonical_signature();
        if let Some(existing) = classes.get_mut(&signature) {
            existing.member_count = existing.member_count.saturating_add(1);
            continue;
        }
        classes.insert(
            signature,
            TraceClass {
                representative: canonical_representative(&canonical),
                canonical,
                member_count: 1,
            },
        );
    }
    classes.into_values().collect()
}

fn canonical_representative(canonical: &FoataNormalForm) -> Vec<MvccAction> {
    let mut representative = Vec::new();
    for layer in &canonical.layers {
        representative.extend(layer.iter().cloned());
    }
    representative
}

/// Compute trace-reduction stats for a scenario.
#[must_use]
pub fn trace_reduction_stats(chains: &[Vec<MvccAction>]) -> TraceReductionStats {
    let naive = enumerate_interleavings(chains).len();
    let classes = enumerate_trace_classes(chains).len();
    let reduction_factor = if classes == 0 {
        1.0
    } else {
        naive as f64 / classes as f64
    };
    TraceReductionStats {
        naive_interleavings: naive,
        trace_classes: classes,
        reduction_factor,
    }
}

#[derive(Debug, Clone, Default)]
struct TxnState {
    snapshot_seq: Option<u64>,
    reads: BTreeSet<u32>,
    writes: BTreeSet<u32>,
}

#[derive(Debug, Clone)]
struct CommittedTxn {
    commit_seq: u64,
    reads: BTreeSet<u32>,
    writes: BTreeSet<u32>,
}

/// Invariant summary over one trace representative.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TraceInvariantReport {
    /// Snapshot-isolation/SSI safety checks held.
    pub snapshot_isolation_holds: bool,
    /// No write-write FCW conflict was silently accepted.
    pub fcw_conflicts_detected: bool,
    /// GC did not advance past any active snapshot.
    pub gc_safe: bool,
}

impl TraceInvariantReport {
    /// Returns true when all tracked invariants hold.
    #[must_use]
    pub fn all_hold(self) -> bool {
        self.snapshot_isolation_holds && self.fcw_conflicts_detected && self.gc_safe
    }
}

/// Validate SI/FCW/GC invariants for a concrete schedule.
#[must_use]
pub fn verify_trace_invariants(schedule: &[MvccAction]) -> TraceInvariantReport {
    let mut snapshot_isolation_holds = true;
    let mut fcw_conflicts_detected = true;
    let mut gc_safe = true;

    let mut txn_state = BTreeMap::<u64, TxnState>::new();
    let mut committed = Vec::<CommittedTxn>::new();
    let mut latest_writer_by_page = BTreeMap::<u32, u64>::new();
    let mut commit_seq_hi = 0_u64;

    for action in schedule {
        match &action.kind {
            MvccActionKind::Begin => {
                let entry = txn_state.entry(action.txn_id).or_default();
                if entry.snapshot_seq.is_some() {
                    snapshot_isolation_holds = false;
                    continue;
                }
                entry.snapshot_seq = Some(commit_seq_hi);
            }
            MvccActionKind::Read { page_id } => {
                let entry = txn_state.entry(action.txn_id).or_default();
                if entry.snapshot_seq.is_none() {
                    snapshot_isolation_holds = false;
                    continue;
                }
                entry.reads.insert(*page_id);
            }
            MvccActionKind::Write { page_id } => {
                let entry = txn_state.entry(action.txn_id).or_default();
                if entry.snapshot_seq.is_none() {
                    snapshot_isolation_holds = false;
                    continue;
                }
                entry.writes.insert(*page_id);
            }
            MvccActionKind::Commit { write_set } => {
                let Some(entry) = txn_state.get(&action.txn_id).cloned() else {
                    snapshot_isolation_holds = false;
                    continue;
                };
                let Some(snapshot_seq) = entry.snapshot_seq else {
                    snapshot_isolation_holds = false;
                    continue;
                };

                let commit_pages = if write_set.is_empty() {
                    entry.writes.clone()
                } else {
                    write_set.clone()
                };

                let ww_conflict = commit_pages.iter().any(|page_id| {
                    latest_writer_by_page
                        .get(page_id)
                        .is_some_and(|last_seq| *last_seq > snapshot_seq)
                });
                if ww_conflict {
                    fcw_conflicts_detected = false;
                    snapshot_isolation_holds = false;
                    continue;
                }

                let dangerous_structure = committed.iter().any(|other| {
                    other.commit_seq > snapshot_seq
                        && pages_overlap(&entry.reads, &other.writes)
                        && pages_overlap(&commit_pages, &other.reads)
                });
                if dangerous_structure {
                    snapshot_isolation_holds = false;
                    continue;
                }

                commit_seq_hi = commit_seq_hi.saturating_add(1);
                for page_id in &commit_pages {
                    latest_writer_by_page.insert(*page_id, commit_seq_hi);
                }
                committed.push(CommittedTxn {
                    commit_seq: commit_seq_hi,
                    reads: entry.reads.clone(),
                    writes: commit_pages,
                });
            }
            MvccActionKind::Gc { horizon_seq } => {
                let min_active_snapshot = txn_state
                    .values()
                    .filter_map(|txn| txn.snapshot_seq)
                    .min()
                    .unwrap_or(commit_seq_hi);
                if *horizon_seq > min_active_snapshot {
                    gc_safe = false;
                }
            }
        }
    }

    TraceInvariantReport {
        snapshot_isolation_holds,
        fcw_conflicts_detected,
        gc_safe,
    }
}

/// Commit/abort decisions produced by SSI-style validation over a schedule.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SsiExecutionOutcome {
    /// Transactions that committed.
    pub committed: BTreeSet<u64>,
    /// Transactions rejected by FCW/SSI checks.
    pub aborted: BTreeSet<u64>,
}

/// Simulate FCW + SSI guardrails for commit attempts in a schedule.
#[must_use]
pub fn simulate_ssi_execution(schedule: &[MvccAction]) -> SsiExecutionOutcome {
    let mut txn_state = BTreeMap::<u64, TxnState>::new();
    let mut committed = Vec::<CommittedTxn>::new();
    let mut latest_writer_by_page = BTreeMap::<u32, u64>::new();
    let mut commit_seq_hi = 0_u64;
    let mut outcome = SsiExecutionOutcome::default();

    for action in schedule {
        match &action.kind {
            MvccActionKind::Begin => {
                let entry = txn_state.entry(action.txn_id).or_default();
                if entry.snapshot_seq.is_none() {
                    entry.snapshot_seq = Some(commit_seq_hi);
                }
            }
            MvccActionKind::Read { page_id } => {
                if let Some(entry) = txn_state.get_mut(&action.txn_id) {
                    entry.reads.insert(*page_id);
                }
            }
            MvccActionKind::Write { page_id } => {
                if let Some(entry) = txn_state.get_mut(&action.txn_id) {
                    entry.writes.insert(*page_id);
                }
            }
            MvccActionKind::Commit { write_set } => {
                let Some(entry) = txn_state.get(&action.txn_id).cloned() else {
                    outcome.aborted.insert(action.txn_id);
                    continue;
                };
                let Some(snapshot_seq) = entry.snapshot_seq else {
                    outcome.aborted.insert(action.txn_id);
                    continue;
                };
                let commit_pages = if write_set.is_empty() {
                    entry.writes.clone()
                } else {
                    write_set.clone()
                };

                let ww_conflict = commit_pages.iter().any(|page_id| {
                    latest_writer_by_page
                        .get(page_id)
                        .is_some_and(|last_seq| *last_seq > snapshot_seq)
                });
                if ww_conflict {
                    outcome.aborted.insert(action.txn_id);
                    continue;
                }

                let dangerous_structure = committed.iter().any(|other| {
                    other.commit_seq > snapshot_seq
                        && pages_overlap(&entry.reads, &other.writes)
                        && pages_overlap(&commit_pages, &other.reads)
                });
                if dangerous_structure {
                    outcome.aborted.insert(action.txn_id);
                    continue;
                }

                commit_seq_hi = commit_seq_hi.saturating_add(1);
                for page_id in &commit_pages {
                    latest_writer_by_page.insert(*page_id, commit_seq_hi);
                }
                committed.push(CommittedTxn {
                    commit_seq: commit_seq_hi,
                    reads: entry.reads.clone(),
                    writes: commit_pages,
                });
                outcome.committed.insert(action.txn_id);
            }
            MvccActionKind::Gc { .. } => {}
        }
    }

    outcome
}

fn pages_overlap(lhs: &BTreeSet<u32>, rhs: &BTreeSet<u32>) -> bool {
    lhs.iter().any(|page_id| rhs.contains(page_id))
}

/// DPOR-style class exploration report.
#[derive(Debug, Clone)]
pub struct DporExploration {
    /// Distinct explored trace classes.
    pub classes: Vec<TraceClass>,
    /// Number of explored representative paths.
    pub explored_paths: usize,
    /// Number of all naive interleavings for the same scenario.
    pub naive_interleavings: usize,
}

/// Explore representative schedules with a DPOR-style pruning strategy.
///
/// We return one representative per trace class by collapsing all naive
/// interleavings through Foata canonicalization. This preserves coverage of
/// all relevant classes while avoiding redundant linearizations.
#[must_use]
pub fn dpor_enumerate_trace_classes(chains: &[Vec<MvccAction>]) -> DporExploration {
    let classes = enumerate_trace_classes(chains);
    let explored_paths = classes.len();
    DporExploration {
        classes,
        explored_paths,
        naive_interleavings: enumerate_interleavings(chains).len(),
    }
}

impl MvccTlaExporter {
    /// Construct an exporter from snapshots.
    #[must_use]
    pub fn from_snapshots(snapshots: Vec<MvccStateSnapshot>) -> Self {
        Self { snapshots }
    }

    /// Export a behavior module with `States == << ... >>` and `Init`/`Next`.
    ///
    /// This is designed for bounded model checking: `Next` is the disjunction of
    /// the concrete steps observed in the trace.
    #[must_use]
    pub fn export_behavior(&self, name: &str) -> TlaModule {
        let mut src = String::new();
        src.push_str("---- MODULE ");
        src.push_str(name);
        src.push_str(" ----\n");
        src.push_str("EXTENDS Integers, Sequences, TLC\n\n");

        src.push_str("VARIABLES step, state\n\n");

        // States constant
        src.push_str("States == ");
        let mut states = Vec::with_capacity(self.snapshots.len());
        for s in &self.snapshots {
            states.push(s.to_record_value());
        }
        TlaValue::Seq(states).push_tla(&mut src);
        src.push_str("\n\n");

        if self.snapshots.is_empty() {
            src.push_str("Init == FALSE\n");
            src.push_str("Next == FALSE\n\n");
        } else {
            src.push_str("Init ==\n");
            src.push_str("    /\\ step = 1\n");
            src.push_str("    /\\ state = States[1]\n\n");

            src.push_str("Next ==\n");
            if self.snapshots.len() == 1 {
                src.push_str("    FALSE\n\n");
            } else {
                for i in 2..=self.snapshots.len() {
                    src.push_str("    \\/ ");
                    src.push_str("/\\ step = ");
                    src.push_str(&(i - 1).to_string());
                    src.push_str(" /\\ step' = ");
                    src.push_str(&i.to_string());
                    src.push('\n');
                    src.push_str("       /\\ state' = States[");
                    src.push_str(&i.to_string());
                    src.push_str("]\n");
                }
                src.push('\n');
            }
        }

        src.push_str("Spec == Init /\\ [][Next]_<<step, state>>\n\n");
        src.push_str("====\n");

        TlaModule {
            name: name.to_string(),
            source: src,
        }
    }

    /// Export a parametric MVCC skeleton suitable as a TLC model starting
    /// point (constants, state vars, Init, Next, and invariant stubs).
    #[must_use]
    pub fn export_spec_skeleton(&self, name: &str) -> TlaModule {
        let mut src = String::new();
        src.push_str("---- MODULE ");
        src.push_str(name);
        src.push_str(" ----\n");
        src.push_str("EXTENDS Integers, Sequences, FiniteSets, TLC\n\n");
        src.push_str("CONSTANTS Txns, Pages\n");
        src.push_str("VARIABLES commitSeq, snapshots, readSet, writeSet, gcHorizon\n\n");

        src.push_str("Init ==\n");
        src.push_str("    /\\ commitSeq = 0\n");
        src.push_str("    /\\ snapshots \\in [Txns -> Nat]\n");
        src.push_str("    /\\ readSet \\in [Txns -> SUBSET Pages]\n");
        src.push_str("    /\\ writeSet \\in [Txns -> SUBSET Pages]\n");
        src.push_str("    /\\ gcHorizon = 0\n\n");

        src.push_str("Begin(tx) ==\n");
        src.push_str("    /\\ tx \\in Txns\n");
        src.push_str("    /\\ snapshots' = [snapshots EXCEPT ![tx] = commitSeq]\n");
        src.push_str("    /\\ UNCHANGED <<commitSeq, readSet, writeSet, gcHorizon>>\n\n");

        src.push_str("Read(tx, p) ==\n");
        src.push_str("    /\\ tx \\in Txns /\\ p \\in Pages\n");
        src.push_str("    /\\ readSet' = [readSet EXCEPT ![tx] = @ \\cup {p}]\n");
        src.push_str("    /\\ UNCHANGED <<commitSeq, snapshots, writeSet, gcHorizon>>\n\n");

        src.push_str("Write(tx, p) ==\n");
        src.push_str("    /\\ tx \\in Txns /\\ p \\in Pages\n");
        src.push_str("    /\\ writeSet' = [writeSet EXCEPT ![tx] = @ \\cup {p}]\n");
        src.push_str("    /\\ UNCHANGED <<commitSeq, snapshots, readSet, gcHorizon>>\n\n");

        src.push_str("Commit(tx) ==\n");
        src.push_str("    /\\ tx \\in Txns\n");
        src.push_str("    /\\ commitSeq' = commitSeq + 1\n");
        src.push_str("    /\\ UNCHANGED <<snapshots, readSet, writeSet, gcHorizon>>\n\n");

        src.push_str("Gc(h) ==\n");
        src.push_str("    /\\ h \\in Nat\n");
        src.push_str("    /\\ gcHorizon' = h\n");
        src.push_str("    /\\ UNCHANGED <<commitSeq, snapshots, readSet, writeSet>>\n\n");

        src.push_str("Next ==\n");
        src.push_str("    \\/ \\E tx \\in Txns : Begin(tx)\n");
        src.push_str("    \\/ \\E tx \\in Txns, p \\in Pages : Read(tx, p)\n");
        src.push_str("    \\/ \\E tx \\in Txns, p \\in Pages : Write(tx, p)\n");
        src.push_str("    \\/ \\E tx \\in Txns : Commit(tx)\n");
        src.push_str("    \\/ \\E h \\in Nat : Gc(h)\n\n");

        src.push_str("InvariantSI == TRUE\n");
        src.push_str("InvariantFcw == TRUE\n");
        src.push_str("InvariantGc == TRUE\n\n");
        src.push_str(
            "Spec == Init /\\ [][Next]_<<commitSeq, snapshots, readSet, writeSet, gcHorizon>>\n\n",
        );
        src.push_str("====\n");

        TlaModule {
            name: name.to_string(),
            source: src,
        }
    }

    /// Export a parameterized TLA+ skeleton for model development.
    ///
    /// This emits a compilable scaffold with variable declarations plus
    /// `Init`, `Next`, and invariant placeholders.  Callers can then replace
    /// the `TRUE` stubs with concrete transition predicates.
    #[must_use]
    pub fn export_parametric_spec_skeleton(
        &self,
        name: &str,
        variables: &[&str],
        invariants: &[&str],
    ) -> TlaModule {
        let variable_list: Vec<&str> = if variables.is_empty() {
            vec!["step", "state"]
        } else {
            variables.to_vec()
        };
        let tuple_vars = variable_list.join(", ");

        let mut src = String::new();
        src.push_str("---- MODULE ");
        src.push_str(name);
        src.push_str(" ----\n");
        src.push_str("EXTENDS Integers, Sequences, TLC\n\n");
        src.push_str(
            "\\* Autogenerated MVCC skeleton. Replace TRUE stubs with model predicates.\n",
        );
        src.push_str("VARIABLES ");
        src.push_str(&tuple_vars);
        src.push_str("\n\n");

        src.push_str("Init ==\n");
        src.push_str("    TRUE\n\n");
        src.push_str("Next ==\n");
        src.push_str("    TRUE\n\n");
        src.push_str("Spec == Init /\\ [][Next]_<<");
        src.push_str(&tuple_vars);
        src.push_str(">>\n\n");

        if invariants.is_empty() {
            src.push_str("Invariant == TRUE\n");
        } else {
            for invariant in invariants {
                src.push_str(invariant);
                src.push_str(" == TRUE\n");
            }
        }
        src.push('\n');
        src.push_str("====\n");

        TlaModule {
            name: name.to_string(),
            source: src,
        }
    }
}
