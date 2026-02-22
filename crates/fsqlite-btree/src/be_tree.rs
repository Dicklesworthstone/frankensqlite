//! Bε-tree: Write-optimized tree with message buffers (§15.2).
//!
//! Interior nodes carry a *message buffer* that accumulates pending
//! insert / update / delete operations. When a buffer overflows
//! (exceeds `buffer_capacity`), its messages are flushed down to the
//! appropriate child. Cascading flushes may occur if children also
//! overflow.
//!
//! The epsilon parameter controls the read/write tradeoff:
//! - Higher epsilon → larger buffers → fewer flushes → better write throughput
//! - Lower epsilon  → smaller buffers → faster reads → more flushes
//!
//! # Design
//!
//! - `BeTree<K, V>` owns a root node and configuration.
//! - `BeNode<K, V>` is either an `Interior` (pivots + children + buffer)
//!   or a `Leaf` (sorted key-value pairs).
//! - `BeMessage<K, V>` represents a deferred operation: `Insert`, `Upsert`,
//!   or `Delete`.
//! - Point lookup drains applicable messages along the root-to-leaf path.
//! - Range scan collects and applies pending messages to produce a
//!   consistent snapshot.

use std::collections::BTreeMap;
use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering};

// ── Metrics ──────────────────────────────────────────────────────────────

static BETREE_BUFFER_FLUSHES_TOTAL: AtomicU64 = AtomicU64::new(0);
static BETREE_MESSAGES_BUFFERED_TOTAL: AtomicU64 = AtomicU64::new(0);
static BETREE_CASCADE_DEPTH_TOTAL: AtomicU64 = AtomicU64::new(0);
static BETREE_INSERTS_TOTAL: AtomicU64 = AtomicU64::new(0);

/// Snapshot of Bε-tree metrics.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct BeTreeMetricsSnapshot {
    /// Total buffer flush events.
    pub buffer_flushes_total: u64,
    /// Total messages buffered across all operations.
    pub messages_buffered_total: u64,
    /// Sum of cascade depths across all flush operations.
    pub cascade_depth_total: u64,
    /// Total insert/upsert/delete operations.
    pub inserts_total: u64,
}

impl fmt::Display for BeTreeMetricsSnapshot {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "betree_flushes={} betree_msgs_buffered={} betree_cascade_depth={} betree_inserts={}",
            self.buffer_flushes_total,
            self.messages_buffered_total,
            self.cascade_depth_total,
            self.inserts_total,
        )
    }
}

/// Return a snapshot of Bε-tree metrics.
#[must_use]
pub fn betree_metrics_snapshot() -> BeTreeMetricsSnapshot {
    BeTreeMetricsSnapshot {
        buffer_flushes_total: BETREE_BUFFER_FLUSHES_TOTAL.load(Ordering::Relaxed),
        messages_buffered_total: BETREE_MESSAGES_BUFFERED_TOTAL.load(Ordering::Relaxed),
        cascade_depth_total: BETREE_CASCADE_DEPTH_TOTAL.load(Ordering::Relaxed),
        inserts_total: BETREE_INSERTS_TOTAL.load(Ordering::Relaxed),
    }
}

/// Reset Bε-tree metrics.
pub fn reset_betree_metrics() {
    BETREE_BUFFER_FLUSHES_TOTAL.store(0, Ordering::Relaxed);
    BETREE_MESSAGES_BUFFERED_TOTAL.store(0, Ordering::Relaxed);
    BETREE_CASCADE_DEPTH_TOTAL.store(0, Ordering::Relaxed);
    BETREE_INSERTS_TOTAL.store(0, Ordering::Relaxed);
}

// ── Configuration ────────────────────────────────────────────────────────

/// Configuration for the Bε-tree.
#[derive(Debug, Clone, Copy)]
pub struct BeTreeConfig {
    /// Buffer capacity per interior node. When exceeded, messages are
    /// flushed to the appropriate child.
    pub buffer_capacity: usize,
    /// Maximum number of keys in a leaf node before it splits.
    pub leaf_capacity: usize,
    /// Maximum number of pivots in an interior node before it splits.
    /// (fanout = max_pivots + 1)
    pub max_pivots: usize,
}

impl Default for BeTreeConfig {
    fn default() -> Self {
        Self {
            buffer_capacity: 8,
            leaf_capacity: 16,
            max_pivots: 4,
        }
    }
}

// ── Message ──────────────────────────────────────────────────────────────

/// A buffered message representing a deferred operation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BeMessage<K: Ord + Clone, V: Clone> {
    /// Insert or overwrite a key-value pair.
    Insert { key: K, value: V },
    /// Delete a key.
    Delete { key: K },
}

impl<K: Ord + Clone, V: Clone> BeMessage<K, V> {
    /// Return the key this message targets.
    pub fn key(&self) -> &K {
        match self {
            Self::Insert { key, .. } | Self::Delete { key } => key,
        }
    }
}

// ── Node ─────────────────────────────────────────────────────────────────

/// A node in the Bε-tree.
#[derive(Debug, Clone)]
enum BeNode<K: Ord + Clone, V: Clone> {
    /// Leaf node storing sorted key-value pairs.
    Leaf { entries: Vec<(K, V)> },
    /// Interior node with pivots, children, and a message buffer.
    Interior {
        /// Pivot keys separating children.
        /// `children[i]` covers keys < `pivots[i]`.
        /// `children[pivots.len()]` covers keys >= last pivot.
        pivots: Vec<K>,
        /// Child nodes. len() == pivots.len() + 1.
        children: Vec<Self>,
        /// Pending messages waiting to be flushed down.
        buffer: Vec<BeMessage<K, V>>,
    },
}

impl<K: Ord + Clone, V: Clone> BeNode<K, V> {
    fn new_leaf() -> Self {
        Self::Leaf {
            entries: Vec::new(),
        }
    }
}

// ── BeTree ───────────────────────────────────────────────────────────────

/// A Bε-tree: write-optimized tree with message buffers at interior nodes.
///
/// Defers small writes into buffers, flushing them down in batches when
/// buffers overflow. This reduces write amplification compared to standard
/// B-trees for write-heavy workloads.
pub struct BeTree<K: Ord + Clone, V: Clone> {
    root: BeNode<K, V>,
    config: BeTreeConfig,
}

impl<K: Ord + Clone, V: Clone> BeTree<K, V> {
    /// Create a new empty Bε-tree with the given configuration.
    pub fn new(config: BeTreeConfig) -> Self {
        assert!(config.buffer_capacity >= 1, "buffer_capacity must be >= 1");
        assert!(config.leaf_capacity >= 2, "leaf_capacity must be >= 2");
        assert!(config.max_pivots >= 2, "max_pivots must be >= 2");
        Self {
            root: BeNode::new_leaf(),
            config,
        }
    }

    /// Return the number of live key-value pairs in the tree.
    #[must_use]
    pub fn len(&self) -> usize {
        let mut pending: BTreeMap<K, Option<V>> = BTreeMap::new();
        self.collect_all(&self.root, &mut pending);
        pending.into_values().filter(|v| v.is_some()).count()
    }

    /// Return whether the tree is empty.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Return the configuration.
    #[must_use]
    pub fn config(&self) -> &BeTreeConfig {
        &self.config
    }

    /// Return the depth of the tree (leaf = 1).
    #[must_use]
    pub fn depth(&self) -> usize {
        fn depth_of<K: Ord + Clone, V: Clone>(node: &BeNode<K, V>) -> usize {
            match node {
                BeNode::Leaf { .. } => 1,
                BeNode::Interior { children, .. } => 1 + depth_of(&children[0]),
            }
        }
        depth_of(&self.root)
    }

    /// Insert a key-value pair. If the key already exists, it is overwritten.
    pub fn insert(&mut self, key: K, value: V) {
        BETREE_INSERTS_TOTAL.fetch_add(1, Ordering::Relaxed);

        let msg = BeMessage::Insert { key, value };
        self.apply_message(msg);
    }

    /// Delete a key. No-op if the key doesn't exist.
    pub fn delete(&mut self, key: K) {
        BETREE_INSERTS_TOTAL.fetch_add(1, Ordering::Relaxed);

        let msg = BeMessage::Delete { key };
        self.apply_message(msg);
    }

    /// Look up a key, returning its value if present.
    pub fn get(&self, key: &K) -> Option<&V> {
        self.get_in_node(&self.root, key)
    }

    /// Collect all key-value pairs in [lo, hi] inclusive.
    pub fn range(&self, lo: &K, hi: &K) -> Vec<(K, V)> {
        let mut pending: BTreeMap<K, Option<V>> = BTreeMap::new();
        self.collect_range(&self.root, lo, hi, &mut pending);

        pending
            .into_iter()
            .filter_map(|(k, v_opt)| v_opt.map(|v| (k, v)))
            .collect()
    }

    /// Return all entries in sorted order (full scan).
    pub fn entries(&self) -> Vec<(K, V)> {
        let mut result = Vec::new();
        let mut pending: BTreeMap<K, Option<V>> = BTreeMap::new();
        self.collect_all(&self.root, &mut pending);

        for (k, v_opt) in pending {
            if let Some(v) = v_opt {
                result.push((k, v));
            }
        }
        result
    }

    /// Return the total number of pending messages in all interior buffers.
    #[must_use]
    pub fn total_buffered_messages(&self) -> usize {
        fn count_msgs<K: Ord + Clone, V: Clone>(node: &BeNode<K, V>) -> usize {
            match node {
                BeNode::Leaf { .. } => 0,
                BeNode::Interior {
                    buffer, children, ..
                } => {
                    let child_msgs: usize = children.iter().map(count_msgs).sum();
                    buffer.len() + child_msgs
                }
            }
        }
        count_msgs(&self.root)
    }

    // ── Internal ─────────────────────────────────────────────────────────

    /// Apply a message to the tree by buffering it at the root.
    fn apply_message(&mut self, msg: BeMessage<K, V>) {
        BETREE_MESSAGES_BUFFERED_TOTAL.fetch_add(1, Ordering::Relaxed);

        match &mut self.root {
            BeNode::Leaf { entries } => {
                // Apply directly to the leaf.
                apply_message_to_leaf(entries, msg);
            }
            BeNode::Interior { buffer, .. } => {
                buffer.push(msg);
            }
        }

        // Flush if root interior buffer overflows.
        self.flush_if_needed();

        // Handle root split.
        self.maybe_split_root();
    }

    /// If the root is an interior node with an overflowing buffer, flush it.
    fn flush_if_needed(&mut self) {
        if let BeNode::Interior { buffer, .. } = &self.root {
            if buffer.len() > self.config.buffer_capacity {
                let cap = self.config.buffer_capacity;
                let leaf_cap = self.config.leaf_capacity;
                let max_pivots = self.config.max_pivots;
                flush_node(&mut self.root, cap, leaf_cap, max_pivots, 0);
            }
        }
    }

    /// If the root leaf is over capacity, convert it to an interior node.
    fn maybe_split_root(&mut self) {
        match &self.root {
            BeNode::Leaf { entries } => {
                if entries.len() > self.config.leaf_capacity {
                    self.split_root_leaf();
                }
            }
            BeNode::Interior { pivots, .. } => {
                if pivots.len() > self.config.max_pivots {
                    self.split_root_interior();
                }
            }
        }
    }

    fn split_root_leaf(&mut self) {
        if let BeNode::Leaf { entries } = &mut self.root {
            let mid = entries.len() / 2;
            let right_entries = entries.split_off(mid);
            let pivot = right_entries[0].0.clone();
            let left = BeNode::Leaf {
                entries: std::mem::take(entries),
            };
            let right = BeNode::Leaf {
                entries: right_entries,
            };
            self.root = BeNode::Interior {
                pivots: vec![pivot],
                children: vec![left, right],
                buffer: Vec::new(),
            };
        }
    }

    fn split_root_interior(&mut self) {
        if let BeNode::Interior {
            pivots,
            children,
            buffer,
        } = &mut self.root
        {
            let mid = pivots.len() / 2;
            let promote_key = pivots[mid].clone();

            let right_pivots = pivots.split_off(mid + 1);
            pivots.pop(); // remove the promoted pivot

            let right_children = children.split_off(mid + 1);

            // Split buffer between left and right based on promoted key.
            let mut left_buf = Vec::new();
            let mut right_buf = Vec::new();
            for msg in buffer.drain(..) {
                if *msg.key() < promote_key {
                    left_buf.push(msg);
                } else {
                    right_buf.push(msg);
                }
            }

            let left = BeNode::Interior {
                pivots: std::mem::take(pivots),
                children: std::mem::take(children),
                buffer: left_buf,
            };
            let right = BeNode::Interior {
                pivots: right_pivots,
                children: right_children,
                buffer: right_buf,
            };
            self.root = BeNode::Interior {
                pivots: vec![promote_key],
                children: vec![left, right],
                buffer: Vec::new(),
            };
        }
    }

    /// Recursive lookup: check buffer messages, then descend.
    #[allow(clippy::self_only_used_in_recursion)]
    fn get_in_node<'a>(&'a self, node: &'a BeNode<K, V>, key: &K) -> Option<&'a V> {
        match node {
            BeNode::Leaf { entries } => entries
                .binary_search_by(|(k, _)| k.cmp(key))
                .ok()
                .map(|idx| &entries[idx].1),
            BeNode::Interior {
                pivots,
                children,
                buffer,
            } => {
                // Check buffer for the most recent message targeting this key
                // (last one wins).
                let mut latest_msg: Option<&BeMessage<K, V>> = None;
                for msg in buffer {
                    if msg.key() == key {
                        latest_msg = Some(msg);
                    }
                }
                if let Some(msg) = latest_msg {
                    return match msg {
                        BeMessage::Insert { value, .. } => Some(value),
                        BeMessage::Delete { .. } => None,
                    };
                }

                // Descend to the appropriate child.
                let child_idx = find_child_index(pivots, key);
                self.get_in_node(&children[child_idx], key)
            }
        }
    }

    /// Collect entries and pending messages in [lo, hi] for range queries.
    ///
    /// Messages are processed top-down: higher-level buffer messages take
    /// priority over lower-level buffer messages and leaf entries. Within
    /// the same buffer, later messages win (iterate in reverse + `or_insert`).
    #[allow(clippy::self_only_used_in_recursion)]
    fn collect_range(
        &self,
        node: &BeNode<K, V>,
        lo: &K,
        hi: &K,
        pending: &mut BTreeMap<K, Option<V>>,
    ) {
        match node {
            BeNode::Leaf { entries } => {
                for (k, v) in entries {
                    if k >= lo && k <= hi {
                        // Don't overwrite decisions from higher-level buffers.
                        pending.entry(k.clone()).or_insert(Some(v.clone()));
                    }
                }
            }
            BeNode::Interior {
                pivots,
                children,
                buffer,
            } => {
                // Apply buffer messages in range. Iterate in reverse so the
                // most recently buffered message for a key is encountered
                // first, then use or_insert to not overwrite higher-level
                // decisions from the caller.
                for msg in buffer.iter().rev() {
                    let mk = msg.key();
                    if mk >= lo && mk <= hi {
                        match msg {
                            BeMessage::Insert { key, value } => {
                                pending.entry(key.clone()).or_insert(Some(value.clone()));
                            }
                            BeMessage::Delete { key } => {
                                pending.entry(key.clone()).or_insert(None);
                            }
                        }
                    }
                }
                // Descend to children that overlap [lo, hi].
                for (i, child) in children.iter().enumerate() {
                    let child_lo_bound = if i > 0 { Some(&pivots[i - 1]) } else { None };
                    let child_hi_bound = if i < pivots.len() {
                        Some(&pivots[i])
                    } else {
                        None
                    };

                    // Skip children whose range doesn't overlap [lo, hi].
                    if let Some(child_lo) = child_lo_bound {
                        if child_lo > hi {
                            continue;
                        }
                    }
                    if let Some(child_hi) = child_hi_bound {
                        if child_hi <= lo {
                            continue;
                        }
                    }

                    self.collect_range(child, lo, hi, pending);
                }
            }
        }
    }

    /// Collect all entries and pending messages (for full scan / entries()).
    ///
    /// Same top-down priority as collect_range.
    #[allow(clippy::self_only_used_in_recursion)]
    fn collect_all(&self, node: &BeNode<K, V>, pending: &mut BTreeMap<K, Option<V>>) {
        match node {
            BeNode::Leaf { entries } => {
                for (k, v) in entries {
                    pending.entry(k.clone()).or_insert(Some(v.clone()));
                }
            }
            BeNode::Interior {
                children, buffer, ..
            } => {
                for msg in buffer.iter().rev() {
                    match msg {
                        BeMessage::Insert { key, value } => {
                            pending.entry(key.clone()).or_insert(Some(value.clone()));
                        }
                        BeMessage::Delete { key } => {
                            pending.entry(key.clone()).or_insert(None);
                        }
                    }
                }
                for child in children {
                    self.collect_all(child, pending);
                }
            }
        }
    }
}

#[allow(clippy::missing_fields_in_debug)]
impl<K: Ord + Clone + fmt::Debug, V: Clone + fmt::Debug> fmt::Debug for BeTree<K, V> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BeTree")
            .field("len", &self.len())
            .field("depth", &self.depth())
            .field("config", &self.config)
            .finish()
    }
}

// ── Helpers ──────────────────────────────────────────────────────────────

/// Find which child index a key belongs to given the pivots.
/// Returns `i` such that `pivots[i-1] <= key < pivots[i]`
/// (with virtual -inf at left and +inf at right).
fn find_child_index<K: Ord>(pivots: &[K], key: &K) -> usize {
    pivots.partition_point(|p| p <= key)
}

/// Apply a message directly to a leaf's sorted entries.
fn apply_message_to_leaf<K: Ord + Clone, V: Clone>(
    entries: &mut Vec<(K, V)>,
    msg: BeMessage<K, V>,
) {
    match msg {
        BeMessage::Insert { key, value } => match entries.binary_search_by(|(k, _)| k.cmp(&key)) {
            Ok(idx) => entries[idx].1 = value,
            Err(idx) => entries.insert(idx, (key, value)),
        },
        BeMessage::Delete { key } => {
            if let Ok(idx) = entries.binary_search_by(|(k, _)| k.cmp(&key)) {
                entries.remove(idx);
            }
        }
    }
}

/// Flush an interior node's buffer down to children. Recurse if children
/// overflow.
fn flush_node<K: Ord + Clone, V: Clone>(
    node: &mut BeNode<K, V>,
    buffer_cap: usize,
    leaf_cap: usize,
    max_pivots: usize,
    depth: usize,
) {
    let BeNode::Interior {
        pivots,
        children,
        buffer,
    } = node
    else {
        return;
    };

    if buffer.len() <= buffer_cap {
        return;
    }

    BETREE_BUFFER_FLUSHES_TOTAL.fetch_add(1, Ordering::Relaxed);
    BETREE_CASCADE_DEPTH_TOTAL.fetch_add(depth as u64, Ordering::Relaxed);

    // Drain all buffered messages and route them to the appropriate child.
    let messages: Vec<BeMessage<K, V>> = std::mem::take(buffer);

    for msg in messages {
        let child_idx = find_child_index(pivots, msg.key());
        match &mut children[child_idx] {
            BeNode::Leaf { entries } => {
                apply_message_to_leaf(entries, msg);
            }
            BeNode::Interior { buffer: cbuf, .. } => {
                cbuf.push(msg);
            }
        }
    }

    // Recursively flush any children that now overflow.
    for child in children.iter_mut() {
        if let BeNode::Interior { buffer: cbuf, .. } = child {
            if cbuf.len() > buffer_cap {
                flush_node(child, buffer_cap, leaf_cap, max_pivots, depth + 1);
            }
        }
    }

    // Split any leaf children that are over capacity.
    split_oversized_leaves(pivots, children, leaf_cap);

    // Split any interior children that are over capacity.
    split_oversized_interiors(pivots, children, max_pivots);
}

/// Split leaf children that exceed `leaf_cap`.
fn split_oversized_leaves<K: Ord + Clone, V: Clone>(
    pivots: &mut Vec<K>,
    children: &mut Vec<BeNode<K, V>>,
    leaf_cap: usize,
) {
    let mut i = 0;
    while i < children.len() {
        let split_data = if let BeNode::Leaf { entries } = &mut children[i] {
            if entries.len() > leaf_cap {
                let mid = entries.len() / 2;
                let right_entries = entries.split_off(mid);
                let new_pivot = right_entries[0].0.clone();
                let right_leaf = BeNode::Leaf {
                    entries: right_entries,
                };
                Some((new_pivot, right_leaf))
            } else {
                None
            }
        } else {
            None
        };

        if let Some((new_pivot, right_leaf)) = split_data {
            pivots.insert(i, new_pivot);
            children.insert(i + 1, right_leaf);
            // Don't increment i — re-check the left half.
            continue;
        }
        i += 1;
    }
}

/// Split interior children that exceed `max_pivots`.
fn split_oversized_interiors<K: Ord + Clone, V: Clone>(
    pivots: &mut Vec<K>,
    children: &mut Vec<BeNode<K, V>>,
    max_pivots: usize,
) {
    let mut i = 0;
    while i < children.len() {
        let split_data = if let BeNode::Interior {
            pivots: cpivots,
            children: cchildren,
            buffer: cbuffer,
        } = &mut children[i]
        {
            if cpivots.len() > max_pivots {
                let mid = cpivots.len() / 2;
                let promote = cpivots[mid].clone();

                let right_pivots = cpivots.split_off(mid + 1);
                cpivots.pop(); // remove promoted key

                let right_children = cchildren.split_off(mid + 1);

                // Split buffer.
                let mut left_buf = Vec::new();
                let mut right_buf = Vec::new();
                for msg in cbuffer.drain(..) {
                    if *msg.key() < promote {
                        left_buf.push(msg);
                    } else {
                        right_buf.push(msg);
                    }
                }
                *cbuffer = left_buf;

                let right_node = BeNode::Interior {
                    pivots: right_pivots,
                    children: right_children,
                    buffer: right_buf,
                };
                Some((promote, right_node))
            } else {
                None
            }
        } else {
            None
        };

        if let Some((promote, right_node)) = split_data {
            pivots.insert(i, promote);
            children.insert(i + 1, right_node);
            continue;
        }
        i += 1;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn basic_insert_get() {
        let mut tree = BeTree::new(BeTreeConfig::default());
        tree.insert(5, "five");
        tree.insert(3, "three");
        tree.insert(7, "seven");

        assert_eq!(tree.get(&5), Some(&"five"));
        assert_eq!(tree.get(&3), Some(&"three"));
        assert_eq!(tree.get(&7), Some(&"seven"));
        assert_eq!(tree.get(&1), None);
        assert_eq!(tree.len(), 3);
    }

    #[test]
    fn overwrite_existing_key() {
        let mut tree = BeTree::new(BeTreeConfig::default());
        tree.insert(1, "a");
        tree.insert(1, "b");
        assert_eq!(tree.get(&1), Some(&"b"));
        assert_eq!(tree.len(), 1);
    }

    #[test]
    fn delete_key() {
        let mut tree = BeTree::new(BeTreeConfig::default());
        tree.insert(1, "a");
        tree.insert(2, "b");
        tree.delete(1);
        assert_eq!(tree.get(&1), None);
        assert_eq!(tree.get(&2), Some(&"b"));
        assert_eq!(tree.len(), 1);
    }

    #[test]
    fn range_query() {
        let mut tree = BeTree::new(BeTreeConfig::default());
        for i in 0..20 {
            tree.insert(i, i * 10);
        }
        let result = tree.range(&5, &10);
        let keys: Vec<i32> = result.iter().map(|(k, _)| *k).collect();
        assert_eq!(keys, vec![5, 6, 7, 8, 9, 10]);
    }

    #[test]
    fn entries_sorted() {
        let mut tree = BeTree::new(BeTreeConfig::default());
        for i in (0..10).rev() {
            tree.insert(i, i);
        }
        let entries = tree.entries();
        let keys: Vec<i32> = entries.iter().map(|(k, _)| *k).collect();
        assert_eq!(keys, (0..10).collect::<Vec<_>>());
    }

    #[test]
    fn buffer_flush_on_overflow() {
        reset_betree_metrics();
        let config = BeTreeConfig {
            buffer_capacity: 2,
            leaf_capacity: 4,
            max_pivots: 2,
        };
        let mut tree = BeTree::new(config);
        // Insert enough to trigger flushes.
        for i in 0..20 {
            tree.insert(i, i);
        }
        let snap = betree_metrics_snapshot();
        assert!(snap.buffer_flushes_total > 0, "expected flush events");
        assert_eq!(tree.len(), 20);
        // Verify all values.
        for i in 0..20 {
            assert_eq!(tree.get(&i), Some(&i), "missing key {i}");
        }
    }

    #[test]
    fn depth_increases_with_data() {
        let config = BeTreeConfig {
            buffer_capacity: 2,
            leaf_capacity: 4,
            max_pivots: 2,
        };
        let mut tree = BeTree::new(config);
        assert_eq!(tree.depth(), 1);
        for i in 0..100 {
            tree.insert(i, i);
        }
        assert!(tree.depth() > 1, "expected tree to grow deeper");
    }
}
