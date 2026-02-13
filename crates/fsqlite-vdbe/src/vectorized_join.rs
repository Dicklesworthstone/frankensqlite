//! Trie-shaped relation storage primitives for Leapfrog-style join execution.
//!
//! This module provides the first `bd-2qr3a.1` prototype:
//! - a cache-friendly arena layout (`Vec<TrieNode>`) with range-based links,
//! - deterministic construction from lexicographically sorted join keys,
//! - a cursor API with `open`, `seek`, `next`, `at_end`, and `open_child`.
//!
//! ## Node Format
//! Each node stores:
//! - `key`: join-key value at this depth,
//! - `depth`: zero-based depth in the key tuple,
//! - `row_range`: contiguous slice of input rows sharing this prefix,
//! - `child_range`: contiguous node range for the next depth.
//!
//! Sibling nodes are contiguous in `TrieRelation::nodes`; no pointer chasing is
//! required for sibling scans or binary seek.
//!
//! ## Memory Layout
//! The layout is an arena of plain values:
//! - `TrieRelation::nodes: Vec<TrieNode>`
//! - `TrieRelation::rows: Vec<TrieRow>`
//! - parent/child and sibling relations are represented by `Range<usize>`
//!   indices into `nodes`.
//!
//! This keeps metadata compact, supports predictable cache behavior, and avoids
//! fragmentation from per-node allocations.
//!
//! ## Prefix Compression
//! The prototype stores full `SqliteValue` keys per node and does not yet apply
//! prefix compression. The range-based layout is compatible with a later
//! dictionary/prefix encoding pass without changing cursor semantics.
//!
//! ## MVCC Iterator Semantics
//! `TrieRelation` is immutable after construction. Cursors keep only indices
//! into the immutable arena and therefore remain stable for the lifetime of the
//! relation snapshot. Under MVCC, relations should be materialized from a single
//! snapshot (or transaction-local batch); cursor invalidation happens only when
//! that snapshot object is dropped.

use std::cmp::Ordering;
use std::fmt;
use std::ops::Range;
use std::sync::atomic::{AtomicU64, Ordering as AtomicOrdering};

use fsqlite_types::value::SqliteValue;

/// A single sorted input row for trie construction.
#[derive(Debug, Clone, PartialEq)]
pub struct TrieRow {
    /// Join-key tuple for this row.
    pub key: Vec<SqliteValue>,
    /// Stable payload reference (e.g. row ordinal in a materialized batch).
    pub payload_row_index: usize,
}

impl TrieRow {
    /// Build a trie row from key columns and payload row index.
    #[must_use]
    pub fn new(key: Vec<SqliteValue>, payload_row_index: usize) -> Self {
        Self {
            key,
            payload_row_index,
        }
    }
}

/// A trie node stored in the arena.
#[derive(Debug, Clone, PartialEq)]
pub struct TrieNode {
    /// Key value represented by this node.
    pub key: SqliteValue,
    /// Zero-based key depth.
    pub depth: u16,
    /// Input-row range sharing this prefix.
    pub row_range: Range<usize>,
    /// Child-node range (next key depth), if any.
    pub child_range: Option<Range<usize>>,
}

/// Errors that can occur while constructing a trie relation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TrieBuildError {
    EmptyKey {
        row_index: usize,
    },
    InconsistentArity {
        expected: usize,
        found: usize,
        row_index: usize,
    },
    UnsortedInput {
        previous_row_index: usize,
        row_index: usize,
    },
    NonComparableKey {
        row_index: usize,
        depth: usize,
    },
    ArityTooLarge {
        arity: usize,
    },
}

impl fmt::Display for TrieBuildError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::EmptyKey { row_index } => {
                write!(f, "row {row_index} has an empty join key")
            }
            Self::InconsistentArity {
                expected,
                found,
                row_index,
            } => write!(
                f,
                "row {row_index} has key arity {found}, expected {expected}",
            ),
            Self::UnsortedInput {
                previous_row_index,
                row_index,
            } => write!(
                f,
                "input rows are not sorted at indices {previous_row_index} and {row_index}",
            ),
            Self::NonComparableKey { row_index, depth } => {
                write!(f, "row {row_index} has non-comparable key at depth {depth}",)
            }
            Self::ArityTooLarge { arity } => write!(f, "key arity {arity} exceeds u16::MAX"),
        }
    }
}

impl std::error::Error for TrieBuildError {}

/// Errors that can occur during cursor seek.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TrieSeekError {
    NonComparableTarget { node_index: usize, depth: usize },
}

impl fmt::Display for TrieSeekError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NonComparableTarget { node_index, depth } => write!(
                f,
                "target key is not comparable with node {node_index} at depth {depth}",
            ),
        }
    }
}

impl std::error::Error for TrieSeekError {}

/// Point-in-time snapshot of Leapfrog join counters.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct LeapfrogMetricsSnapshot {
    /// Total output tuple multiplicity observed across all Leapfrog executions.
    pub fsqlite_leapfrog_tuples_total: u64,
    /// Total seek operations issued by Leapfrog across all executions.
    pub fsqlite_leapfrog_seeks_total: u64,
}

static FSQLITE_LEAPFROG_TUPLES_TOTAL: AtomicU64 = AtomicU64::new(0);
static FSQLITE_LEAPFROG_SEEKS_TOTAL: AtomicU64 = AtomicU64::new(0);

/// Snapshot Leapfrog counters.
#[must_use]
pub fn leapfrog_metrics_snapshot() -> LeapfrogMetricsSnapshot {
    LeapfrogMetricsSnapshot {
        fsqlite_leapfrog_tuples_total: FSQLITE_LEAPFROG_TUPLES_TOTAL.load(AtomicOrdering::Relaxed),
        fsqlite_leapfrog_seeks_total: FSQLITE_LEAPFROG_SEEKS_TOTAL.load(AtomicOrdering::Relaxed),
    }
}

/// Reset Leapfrog counters.
pub fn reset_leapfrog_metrics() {
    FSQLITE_LEAPFROG_TUPLES_TOTAL.store(0, AtomicOrdering::Relaxed);
    FSQLITE_LEAPFROG_SEEKS_TOTAL.store(0, AtomicOrdering::Relaxed);
}

/// Errors produced by Leapfrog Triejoin execution.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LeapfrogJoinError {
    /// Leapfrog requires at least two relations.
    NotEnoughRelations { found: usize },
    /// One relation has a different key arity than the first relation.
    ArityMismatch {
        expected: usize,
        found: usize,
        relation_index: usize,
    },
    /// Two keys cannot be compared under SQLite ordering rules.
    NonComparableKey { relation_index: usize, depth: usize },
    /// A cursor seek failed.
    SeekError {
        relation_index: usize,
        source: TrieSeekError,
    },
}

impl fmt::Display for LeapfrogJoinError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NotEnoughRelations { found } => {
                write!(f, "leapfrog requires at least 2 relations, found {found}",)
            }
            Self::ArityMismatch {
                expected,
                found,
                relation_index,
            } => write!(
                f,
                "relation {relation_index} has arity {found}, expected {expected}",
            ),
            Self::NonComparableKey {
                relation_index,
                depth,
            } => {
                write!(
                    f,
                    "relation {relation_index} has non-comparable key at depth {depth}",
                )
            }
            Self::SeekError {
                relation_index,
                source,
            } => {
                write!(f, "seek failed for relation {relation_index}: {source}")
            }
        }
    }
}

impl std::error::Error for LeapfrogJoinError {}

/// One aligned key match produced by Leapfrog join.
#[derive(Debug, Clone, PartialEq)]
pub struct LeapfrogMatch {
    /// Matched join key tuple.
    pub key: Vec<SqliteValue>,
    /// Leaf row ranges per input relation for this key.
    pub relation_row_ranges: Vec<Range<usize>>,
}

impl LeapfrogMatch {
    /// Multiplicity of joined output tuples for this key.
    #[must_use]
    pub fn tuple_multiplicity(&self) -> u64 {
        let mut product = 1_u64;
        for range in &self.relation_row_ranges {
            let span = range.end.saturating_sub(range.start);
            let span_u64 = u64::try_from(span).unwrap_or(u64::MAX);
            product = product.saturating_mul(span_u64);
        }
        product
    }
}

#[derive(Debug, Default)]
struct LeapfrogExecution {
    seeks: u64,
    tuples_produced: u64,
    matches: Vec<LeapfrogMatch>,
}

/// Executor for multi-way equi-joins over trie relations.
#[derive(Debug)]
pub struct LeapfrogJoinExecutor<'a> {
    relations: Vec<&'a TrieRelation>,
    arity: usize,
}

impl<'a> LeapfrogJoinExecutor<'a> {
    /// Create a validated executor over a set of trie relations.
    pub fn try_new(relations: Vec<&'a TrieRelation>) -> Result<Self, LeapfrogJoinError> {
        if relations.len() < 2 {
            return Err(LeapfrogJoinError::NotEnoughRelations {
                found: relations.len(),
            });
        }

        let arity = relations[0].arity();
        for (relation_index, relation) in relations.iter().enumerate().skip(1) {
            if relation.arity() != arity {
                return Err(LeapfrogJoinError::ArityMismatch {
                    expected: arity,
                    found: relation.arity(),
                    relation_index,
                });
            }
        }

        Ok(Self { relations, arity })
    }

    /// Execute Leapfrog over all input relations and collect key matches.
    pub fn execute(&self) -> Result<Vec<LeapfrogMatch>, LeapfrogJoinError> {
        if self.arity == 0 {
            return Ok(Vec::new());
        }
        if self
            .relations
            .iter()
            .any(|relation| relation.row_count() == 0)
        {
            return Ok(Vec::new());
        }

        let mut root_cursors = Vec::with_capacity(self.relations.len());
        for relation in &self.relations {
            let cursor = relation.open_root_cursor();
            if cursor.at_end() {
                return Ok(Vec::new());
            }
            root_cursors.push(cursor);
        }

        let span = tracing::span!(
            tracing::Level::INFO,
            "leapfrog_join",
            join_width = self.relations.len(),
            tuples_produced = tracing::field::Empty,
            seeks = tracing::field::Empty
        );
        let mut execution = LeapfrogExecution::default();
        {
            let _guard = span.enter();
            let mut prefix = Vec::with_capacity(self.arity);
            execute_depth(
                &mut root_cursors,
                0,
                self.arity,
                &mut prefix,
                &mut execution,
            )?;

            span.record("tuples_produced", execution.tuples_produced);
            span.record("seeks", execution.seeks);
            tracing::info!(
                join_width = self.relations.len(),
                tuples_produced = execution.tuples_produced,
                seeks = execution.seeks,
                matches = execution.matches.len(),
                "leapfrog.join.complete"
            );
        }

        FSQLITE_LEAPFROG_TUPLES_TOTAL.fetch_add(execution.tuples_produced, AtomicOrdering::Relaxed);
        FSQLITE_LEAPFROG_SEEKS_TOTAL.fetch_add(execution.seeks, AtomicOrdering::Relaxed);
        Ok(execution.matches)
    }
}

/// Execute Leapfrog Triejoin for 2..N relations.
pub fn leapfrog_join(relations: &[&TrieRelation]) -> Result<Vec<LeapfrogMatch>, LeapfrogJoinError> {
    let executor = LeapfrogJoinExecutor::try_new(relations.to_vec())?;
    executor.execute()
}

/// Immutable trie relation for Leapfrog-style join probes.
#[derive(Debug, Clone, PartialEq)]
pub struct TrieRelation {
    arity: usize,
    rows: Vec<TrieRow>,
    nodes: Vec<TrieNode>,
    root_range: Option<Range<usize>>,
}

impl TrieRelation {
    /// Build a trie from lexicographically sorted rows.
    ///
    /// Rows must be sorted by their full key tuple according to SQLite value
    /// ordering (`SqliteValue::partial_cmp`).
    pub fn from_sorted_rows(rows: Vec<TrieRow>) -> Result<Self, TrieBuildError> {
        if rows.is_empty() {
            return Ok(Self {
                arity: 0,
                rows,
                nodes: Vec::new(),
                root_range: None,
            });
        }

        let arity = rows[0].key.len();
        if arity == 0 {
            return Err(TrieBuildError::EmptyKey { row_index: 0 });
        }
        if arity > usize::from(u16::MAX) {
            return Err(TrieBuildError::ArityTooLarge { arity });
        }

        validate_sorted_rows(&rows, arity)?;
        let mut nodes = Vec::new();
        let root_range = build_level(&rows, 0, 0, arity, &mut nodes)?;

        Ok(Self {
            arity,
            rows,
            nodes,
            root_range,
        })
    }

    /// Key arity for this relation.
    #[must_use]
    pub const fn arity(&self) -> usize {
        self.arity
    }

    /// Total input rows represented by this trie.
    #[must_use]
    pub fn row_count(&self) -> usize {
        self.rows.len()
    }

    /// Total node count in the trie arena.
    #[must_use]
    pub fn node_count(&self) -> usize {
        self.nodes.len()
    }

    /// Root sibling range, if present.
    #[must_use]
    pub fn root_range(&self) -> Option<Range<usize>> {
        self.root_range.clone()
    }

    /// Open a cursor at the first root-level node.
    #[must_use]
    pub fn open_root_cursor(&self) -> TrieCursor<'_> {
        let sibling_range = self.root_range.clone().unwrap_or(0..0);
        TrieCursor::new(self, sibling_range)
    }

    fn node(&self, index: usize) -> Option<&TrieNode> {
        self.nodes.get(index)
    }
}

/// Cursor over contiguous siblings at a trie depth.
#[derive(Debug, Clone)]
pub struct TrieCursor<'a> {
    relation: &'a TrieRelation,
    sibling_range: Range<usize>,
    position: usize,
}

impl<'a> TrieCursor<'a> {
    fn new(relation: &'a TrieRelation, sibling_range: Range<usize>) -> Self {
        let position = sibling_range.start;
        Self {
            relation,
            sibling_range,
            position,
        }
    }

    /// Return true if the cursor has reached the end of the sibling range.
    #[must_use]
    pub fn at_end(&self) -> bool {
        self.position >= self.sibling_range.end
    }

    /// Current key at cursor position.
    #[must_use]
    pub fn current_key(&self) -> Option<&SqliteValue> {
        self.current_node().map(|node| &node.key)
    }

    /// Advance to the next sibling.
    pub fn next(&mut self) {
        if !self.at_end() {
            self.position = self.position.saturating_add(1);
        }
    }

    /// Seek to `target` or the next greater key using galloping + binary search.
    ///
    /// Returns `Ok(true)` when exact key equality is reached.
    /// Returns `Ok(false)` when positioned at the next greater key or end.
    pub fn seek(&mut self, target: &SqliteValue) -> Result<bool, TrieSeekError> {
        if self.at_end() {
            return Ok(false);
        }

        let current_cmp = self.compare_at(self.position, target)?;
        if current_cmp == Ordering::Equal {
            return Ok(true);
        }
        if current_cmp == Ordering::Greater {
            return Ok(false);
        }

        let mut low = self.position;
        let mut high = self.sibling_range.end;

        let mut step = 1usize;
        let mut probe = self.position.saturating_add(step);
        while probe < self.sibling_range.end {
            match self.compare_at(probe, target)? {
                Ordering::Less => {
                    low = probe.saturating_add(1);
                    step = step.saturating_mul(2);
                    probe = probe.saturating_add(step);
                }
                Ordering::Equal => {
                    self.position = probe;
                    return Ok(true);
                }
                Ordering::Greater => {
                    high = probe.saturating_add(1);
                    break;
                }
            }
        }

        self.position = lower_bound(self.relation, low, high, target)?;
        if self.at_end() {
            return Ok(false);
        }
        Ok(self.compare_at(self.position, target)? == Ordering::Equal)
    }

    /// Open a cursor for the child level of the current key, if present.
    #[must_use]
    pub fn open_child(&self) -> Option<Self> {
        let range = self.current_node()?.child_range.clone()?;
        Some(Self::new(self.relation, range))
    }

    fn current_node(&self) -> Option<&TrieNode> {
        if self.at_end() {
            return None;
        }
        self.relation.node(self.position)
    }

    fn compare_at(
        &self,
        node_index: usize,
        target: &SqliteValue,
    ) -> Result<Ordering, TrieSeekError> {
        let node = self
            .relation
            .node(node_index)
            .ok_or(TrieSeekError::NonComparableTarget {
                node_index,
                depth: 0,
            })?;
        node.key
            .partial_cmp(target)
            .ok_or_else(|| TrieSeekError::NonComparableTarget {
                node_index,
                depth: usize::from(node.depth),
            })
    }
}

fn validate_sorted_rows(rows: &[TrieRow], arity: usize) -> Result<(), TrieBuildError> {
    for (row_index, row) in rows.iter().enumerate() {
        let key_len = row.key.len();
        if key_len == 0 {
            return Err(TrieBuildError::EmptyKey { row_index });
        }
        if key_len != arity {
            return Err(TrieBuildError::InconsistentArity {
                expected: arity,
                found: key_len,
                row_index,
            });
        }
        if row_index > 0 {
            let previous = &rows[row_index - 1];
            let ordering = compare_key_slices(&previous.key, &row.key).ok_or(
                TrieBuildError::NonComparableKey {
                    row_index,
                    depth: 0,
                },
            )?;
            if ordering == Ordering::Greater {
                return Err(TrieBuildError::UnsortedInput {
                    previous_row_index: row_index - 1,
                    row_index,
                });
            }
        }
    }
    Ok(())
}

fn compare_key_slices(left: &[SqliteValue], right: &[SqliteValue]) -> Option<Ordering> {
    for (depth, (left_value, right_value)) in left.iter().zip(right).enumerate() {
        let ordering = left_value.partial_cmp(right_value)?;
        if ordering != Ordering::Equal {
            return Some(ordering);
        }
        if depth == left.len().saturating_sub(1) {
            return Some(Ordering::Equal);
        }
    }
    Some(Ordering::Equal)
}

fn build_level(
    rows: &[TrieRow],
    depth: usize,
    base_row_offset: usize,
    arity: usize,
    nodes: &mut Vec<TrieNode>,
) -> Result<Option<Range<usize>>, TrieBuildError> {
    if rows.is_empty() || depth >= arity {
        return Ok(None);
    }

    let mut groups = Vec::new();
    let mut group_start = 0usize;
    while group_start < rows.len() {
        let current_key = rows[group_start]
            .key
            .get(depth)
            .ok_or(TrieBuildError::InconsistentArity {
                expected: arity,
                found: rows[group_start].key.len(),
                row_index: base_row_offset + group_start,
            })?
            .clone();
        let mut group_end = group_start + 1;
        while group_end < rows.len() {
            let next_key =
                rows[group_end]
                    .key
                    .get(depth)
                    .ok_or(TrieBuildError::InconsistentArity {
                        expected: arity,
                        found: rows[group_end].key.len(),
                        row_index: base_row_offset + group_end,
                    })?;
            if *next_key != current_key {
                break;
            }
            group_end = group_end.saturating_add(1);
        }
        groups.push((current_key, group_start, group_end));
        group_start = group_end;
    }

    let range_start = nodes.len();
    let depth_u16 = u16::try_from(depth).map_err(|_| TrieBuildError::ArityTooLarge { arity })?;
    for (key, local_start, local_end) in &groups {
        nodes.push(TrieNode {
            key: key.clone(),
            depth: depth_u16,
            row_range: (base_row_offset + *local_start)..(base_row_offset + *local_end),
            child_range: None,
        });
    }
    let range_end = nodes.len();

    for (offset, (_, local_start, local_end)) in groups.iter().enumerate() {
        let child_rows = &rows[*local_start..*local_end];
        let child_base = base_row_offset + *local_start;
        nodes[range_start + offset].child_range =
            build_level(child_rows, depth + 1, child_base, arity, nodes)?;
    }

    Ok(Some(range_start..range_end))
}

fn lower_bound(
    relation: &TrieRelation,
    mut low: usize,
    mut high: usize,
    target: &SqliteValue,
) -> Result<usize, TrieSeekError> {
    while low < high {
        let mid = low + ((high - low) / 2);
        let node = relation
            .node(mid)
            .ok_or(TrieSeekError::NonComparableTarget {
                node_index: mid,
                depth: 0,
            })?;
        let ordering =
            node.key
                .partial_cmp(target)
                .ok_or_else(|| TrieSeekError::NonComparableTarget {
                    node_index: mid,
                    depth: usize::from(node.depth),
                })?;
        if ordering == Ordering::Less {
            low = mid.saturating_add(1);
        } else {
            high = mid;
        }
    }
    Ok(low)
}

fn execute_depth(
    cursors: &mut [TrieCursor<'_>],
    depth: usize,
    arity: usize,
    prefix: &mut Vec<SqliteValue>,
    execution: &mut LeapfrogExecution,
) -> Result<(), LeapfrogJoinError> {
    let mut pivot = 0_usize;
    while align_cursors(cursors, depth, pivot, execution)? {
        let Some(candidate_key) = cursors[0].current_key().cloned() else {
            return Ok(());
        };

        if matches!(candidate_key, SqliteValue::Null) {
            advance_aligned_nulls(cursors);
            continue;
        }

        tracing::debug!(
            depth,
            key = ?candidate_key,
            "leapfrog.level.aligned"
        );

        prefix.push(candidate_key.clone());
        if depth + 1 == arity {
            let relation_row_ranges = collect_current_row_ranges(cursors);
            let join_match = LeapfrogMatch {
                key: prefix.clone(),
                relation_row_ranges,
            };
            let multiplicity = join_match.tuple_multiplicity();
            execution.tuples_produced = execution.tuples_produced.saturating_add(multiplicity);
            execution.matches.push(join_match);
            tracing::debug!(
                depth,
                key = ?candidate_key,
                multiplicity,
                tuples_produced = execution.tuples_produced,
                "leapfrog.level.emit"
            );
        } else {
            let mut child_cursors = Vec::with_capacity(cursors.len());
            for cursor in &*cursors {
                let Some(child) = cursor.open_child() else {
                    child_cursors.clear();
                    break;
                };
                child_cursors.push(child);
            }

            if child_cursors.len() == cursors.len() {
                tracing::debug!(
                    depth,
                    key = ?candidate_key,
                    "leapfrog.level.descend"
                );
                execute_depth(&mut child_cursors, depth + 1, arity, prefix, execution)?;
            }
        }
        prefix.pop();

        cursors[pivot].next();
        pivot = (pivot + 1) % cursors.len();
    }
    Ok(())
}

fn align_cursors(
    cursors: &mut [TrieCursor<'_>],
    depth: usize,
    pivot: usize,
    execution: &mut LeapfrogExecution,
) -> Result<bool, LeapfrogJoinError> {
    if cursors.iter().any(TrieCursor::at_end) {
        return Ok(false);
    }

    let mut target = max_current_key(cursors, depth)?;
    loop {
        let mut all_equal = true;
        for offset in 0..cursors.len() {
            let relation_index = (pivot + offset) % cursors.len();
            if cursors[relation_index].at_end() {
                return Ok(false);
            }
            let Some(current_key) = cursors[relation_index].current_key() else {
                return Ok(false);
            };

            match compare_keys(current_key, &target, relation_index, depth)? {
                Ordering::Less => {
                    execution.seeks = execution.seeks.saturating_add(1);
                    cursors[relation_index].seek(&target).map_err(|source| {
                        LeapfrogJoinError::SeekError {
                            relation_index,
                            source,
                        }
                    })?;
                    if cursors[relation_index].at_end() {
                        return Ok(false);
                    }
                    if let Some(landed_key) = cursors[relation_index].current_key()
                        && landed_key != &target
                    {
                        target = landed_key.clone();
                        all_equal = false;
                        tracing::debug!(
                            depth,
                            relation_index,
                            key = ?target,
                            seeks = execution.seeks,
                            "leapfrog.level.advance.seek"
                        );
                    }
                }
                Ordering::Equal => {}
                Ordering::Greater => {
                    target = current_key.clone();
                    all_equal = false;
                    tracing::debug!(
                        depth,
                        relation_index,
                        key = ?target,
                        "leapfrog.level.advance.max"
                    );
                }
            }
        }

        if all_equal {
            return Ok(true);
        }
    }
}

fn max_current_key(
    cursors: &[TrieCursor<'_>],
    depth: usize,
) -> Result<SqliteValue, LeapfrogJoinError> {
    let mut max_key = cursors[0]
        .current_key()
        .cloned()
        .ok_or(LeapfrogJoinError::SeekError {
            relation_index: 0,
            source: TrieSeekError::NonComparableTarget {
                node_index: 0,
                depth,
            },
        })?;
    for (relation_index, cursor) in cursors.iter().enumerate().skip(1) {
        let Some(current_key) = cursor.current_key() else {
            continue;
        };
        if compare_keys(current_key, &max_key, relation_index, depth)? == Ordering::Greater {
            max_key = current_key.clone();
        }
    }
    Ok(max_key)
}

fn compare_keys(
    left: &SqliteValue,
    right: &SqliteValue,
    relation_index: usize,
    depth: usize,
) -> Result<Ordering, LeapfrogJoinError> {
    left.partial_cmp(right)
        .ok_or(LeapfrogJoinError::NonComparableKey {
            relation_index,
            depth,
        })
}

fn advance_aligned_nulls(cursors: &mut [TrieCursor<'_>]) {
    for cursor in cursors {
        while matches!(cursor.current_key(), Some(SqliteValue::Null)) {
            cursor.next();
            if cursor.at_end() {
                break;
            }
        }
    }
}

fn collect_current_row_ranges(cursors: &[TrieCursor<'_>]) -> Vec<Range<usize>> {
    let mut ranges = Vec::with_capacity(cursors.len());
    for cursor in cursors {
        let Some(node) = cursor.current_node() else {
            ranges.push(0..0);
            continue;
        };
        ranges.push(node.row_range.clone());
    }
    ranges
}

#[cfg(test)]
mod tests {
    use super::{
        LeapfrogJoinError, LeapfrogJoinExecutor, TrieBuildError, TrieRelation, TrieRow,
        leapfrog_join, leapfrog_metrics_snapshot, reset_leapfrog_metrics,
    };
    use fsqlite_types::value::SqliteValue;

    fn sample_rows() -> Vec<TrieRow> {
        vec![
            TrieRow::new(vec![SqliteValue::Integer(1), SqliteValue::Integer(1)], 0),
            TrieRow::new(vec![SqliteValue::Integer(1), SqliteValue::Integer(2)], 1),
            TrieRow::new(vec![SqliteValue::Integer(2), SqliteValue::Integer(1)], 2),
            TrieRow::new(vec![SqliteValue::Integer(2), SqliteValue::Integer(3)], 3),
        ]
    }

    #[test]
    fn builds_trie_from_sorted_rows() {
        let trie = TrieRelation::from_sorted_rows(sample_rows()).expect("build should succeed");
        assert_eq!(trie.arity(), 2);
        assert_eq!(trie.row_count(), 4);
        assert_eq!(trie.root_range(), Some(0..2));
        assert_eq!(trie.node_count(), 6);
    }

    #[test]
    fn rejects_unsorted_rows() {
        let unsorted = vec![
            TrieRow::new(vec![SqliteValue::Integer(2), SqliteValue::Integer(1)], 0),
            TrieRow::new(vec![SqliteValue::Integer(1), SqliteValue::Integer(1)], 1),
        ];
        let err = TrieRelation::from_sorted_rows(unsorted).expect_err("must reject unsorted rows");
        assert_eq!(
            err,
            TrieBuildError::UnsortedInput {
                previous_row_index: 0,
                row_index: 1
            }
        );
    }

    #[test]
    fn cursor_seek_and_child_navigation() {
        let trie = TrieRelation::from_sorted_rows(sample_rows()).expect("build should succeed");
        let mut root = trie.open_root_cursor();

        assert!(!root.at_end());
        assert_eq!(root.current_key(), Some(&SqliteValue::Integer(1)));

        let exact = root
            .seek(&SqliteValue::Integer(2))
            .expect("seek should succeed");
        assert!(exact);
        assert_eq!(root.current_key(), Some(&SqliteValue::Integer(2)));

        let mut child = root.open_child().expect("child cursor should exist");
        assert_eq!(child.current_key(), Some(&SqliteValue::Integer(1)));
        child.next();
        assert_eq!(child.current_key(), Some(&SqliteValue::Integer(3)));

        let exact_child = child
            .seek(&SqliteValue::Integer(2))
            .expect("seek should succeed");
        assert!(!exact_child);
        assert_eq!(child.current_key(), Some(&SqliteValue::Integer(3)));
    }

    #[test]
    fn leapfrog_join_two_way_duplicate_multiplicity() {
        reset_leapfrog_metrics();
        let left = TrieRelation::from_sorted_rows(vec![
            TrieRow::new(vec![SqliteValue::Integer(1)], 0),
            TrieRow::new(vec![SqliteValue::Integer(1)], 1),
            TrieRow::new(vec![SqliteValue::Integer(2)], 2),
        ])
        .expect("left build should succeed");
        let right = TrieRelation::from_sorted_rows(vec![
            TrieRow::new(vec![SqliteValue::Integer(1)], 0),
            TrieRow::new(vec![SqliteValue::Integer(1)], 1),
            TrieRow::new(vec![SqliteValue::Integer(1)], 2),
            TrieRow::new(vec![SqliteValue::Integer(3)], 3),
        ])
        .expect("right build should succeed");

        let matches = leapfrog_join(&[&left, &right]).expect("join should succeed");
        assert_eq!(matches.len(), 1);
        assert_eq!(matches[0].key, vec![SqliteValue::Integer(1)]);
        assert_eq!(matches[0].tuple_multiplicity(), 6);

        let metrics = leapfrog_metrics_snapshot();
        assert!(
            metrics.fsqlite_leapfrog_tuples_total >= 6,
            "tuple metric should include emitted multiplicity"
        );
        assert!(
            metrics.fsqlite_leapfrog_seeks_total >= 1,
            "seek metric should capture alignment seeks"
        );
    }

    #[test]
    fn leapfrog_join_three_way_composite_keys() {
        let rel_a = TrieRelation::from_sorted_rows(vec![
            TrieRow::new(vec![SqliteValue::Integer(1), SqliteValue::Integer(1)], 0),
            TrieRow::new(vec![SqliteValue::Integer(1), SqliteValue::Integer(2)], 1),
            TrieRow::new(vec![SqliteValue::Integer(2), SqliteValue::Integer(1)], 2),
        ])
        .expect("a build should succeed");
        let rel_b = TrieRelation::from_sorted_rows(vec![
            TrieRow::new(vec![SqliteValue::Integer(1), SqliteValue::Integer(2)], 0),
            TrieRow::new(vec![SqliteValue::Integer(2), SqliteValue::Integer(1)], 1),
            TrieRow::new(vec![SqliteValue::Integer(2), SqliteValue::Integer(2)], 2),
        ])
        .expect("b build should succeed");
        let rel_c = TrieRelation::from_sorted_rows(vec![
            TrieRow::new(vec![SqliteValue::Integer(0), SqliteValue::Integer(9)], 0),
            TrieRow::new(vec![SqliteValue::Integer(1), SqliteValue::Integer(2)], 1),
            TrieRow::new(vec![SqliteValue::Integer(2), SqliteValue::Integer(1)], 2),
        ])
        .expect("c build should succeed");

        let matches = leapfrog_join(&[&rel_a, &rel_b, &rel_c]).expect("join should succeed");
        assert_eq!(matches.len(), 2);
        assert_eq!(
            matches[0].key,
            vec![SqliteValue::Integer(1), SqliteValue::Integer(2)]
        );
        assert_eq!(
            matches[1].key,
            vec![SqliteValue::Integer(2), SqliteValue::Integer(1)]
        );
        assert_eq!(matches[0].tuple_multiplicity(), 1);
        assert_eq!(matches[1].tuple_multiplicity(), 1);
    }

    #[test]
    fn leapfrog_join_excludes_null_keys() {
        let rel_a = TrieRelation::from_sorted_rows(vec![
            TrieRow::new(vec![SqliteValue::Null], 0),
            TrieRow::new(vec![SqliteValue::Integer(1)], 1),
        ])
        .expect("a build should succeed");
        let rel_b = TrieRelation::from_sorted_rows(vec![
            TrieRow::new(vec![SqliteValue::Null], 0),
            TrieRow::new(vec![SqliteValue::Integer(1)], 1),
        ])
        .expect("b build should succeed");
        let rel_c = TrieRelation::from_sorted_rows(vec![
            TrieRow::new(vec![SqliteValue::Null], 0),
            TrieRow::new(vec![SqliteValue::Integer(1)], 1),
        ])
        .expect("c build should succeed");

        let matches = leapfrog_join(&[&rel_a, &rel_b, &rel_c]).expect("join should succeed");
        assert_eq!(matches.len(), 1);
        assert_eq!(matches[0].key, vec![SqliteValue::Integer(1)]);
    }

    #[test]
    fn leapfrog_join_rejects_mixed_arity() {
        let rel_a = TrieRelation::from_sorted_rows(vec![
            TrieRow::new(vec![SqliteValue::Integer(1)], 0),
            TrieRow::new(vec![SqliteValue::Integer(2)], 1),
        ])
        .expect("a build should succeed");
        let rel_b = TrieRelation::from_sorted_rows(vec![
            TrieRow::new(vec![SqliteValue::Integer(1), SqliteValue::Integer(2)], 0),
            TrieRow::new(vec![SqliteValue::Integer(3), SqliteValue::Integer(4)], 1),
        ])
        .expect("b build should succeed");

        let err = leapfrog_join(&[&rel_a, &rel_b]).expect_err("mixed arity should be rejected");
        assert_eq!(
            err,
            LeapfrogJoinError::ArityMismatch {
                expected: 1,
                found: 2,
                relation_index: 1
            }
        );
    }

    #[test]
    fn leapfrog_join_requires_two_relations() {
        let rel_a = TrieRelation::from_sorted_rows(vec![
            TrieRow::new(vec![SqliteValue::Integer(1)], 0),
            TrieRow::new(vec![SqliteValue::Integer(2)], 1),
        ])
        .expect("a build should succeed");

        let err = LeapfrogJoinExecutor::try_new(vec![&rel_a]).expect_err("must reject 1 relation");
        assert_eq!(err, LeapfrogJoinError::NotEnoughRelations { found: 1 });
    }
}
