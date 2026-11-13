//! Structured concurrency region tree (§4.11, bd-3go.9).
//!
//! Every background worker, coordinator, and long-lived service runs as a
//! region-owned task. The region tree enforces INV-REGION-QUIESCENCE: no
//! region closes until all children complete, all finalizers run, and all
//! obligations resolve.
//!
//! Close protocol: cancel → drain children → run finalizers → mark closed.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use fsqlite_error::{FrankenError, Result};
use fsqlite_types::Region;
use fsqlite_types::cx::{self, Cx};
use tracing::debug;

use crate::quiescence::{ChildRegionQuiescence, RegionQuiescenceSnapshot};

// ── Types ──────────────────────────────────────────────────────────────

/// Normative region kinds from §4.11.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum RegionKind {
    /// Top-level database root region.
    DbRoot,
    /// Write coordinator service region (native marker sequencer + compat WAL).
    WriteCoordinator,
    /// Symbol store service region (local symbol logs + tiered storage fetch).
    SymbolStore,
    /// Replication service region (stream symbols; anti-entropy; membership).
    Replication,
    /// Checkpoint/GC service region (checkpointer, compactor, GC horizon).
    CheckpointGc,
    /// Observability service region (deadline monitor, task inspector, metrics).
    Observability,
    /// Per-connection region (child of root).
    PerConnection,
    /// Per-transaction region (child of connection).
    PerTransaction,
}

/// State machine for region lifecycle.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RegionState {
    /// Region is open and accepting work.
    Open,
    /// Region is closing (cancellation requested, draining).
    Closing,
    /// Region is fully closed (quiescent, finalizers run).
    Closed,
}

/// A finalizer callback to run during region close.
type Finalizer = Box<dyn FnOnce() + Send>;

/// Shared atomic counter for tracking active tasks or obligations.
type SharedCounter = Arc<AtomicUsize>;

fn new_counter() -> SharedCounter {
    Arc::new(AtomicUsize::new(0))
}

// ── RegionNode ─────────────────────────────────────────────────────────

/// A node in the structured concurrency region tree.
struct RegionNode {
    kind: RegionKind,
    state: RegionState,
    cx: Cx<cx::FullCaps>,
    parent: Option<Region>,
    children: Vec<Region>,
    finalizers: Vec<Finalizer>,
    active_tasks: SharedCounter,
    active_obligations: SharedCounter,
}

// ── RAII handles ───────────────────────────────────────────────────────

/// RAII handle for a task registered in a region.
///
/// When dropped, the task count for the owning region is decremented.
/// This ensures tasks cannot leak without being accounted for.
pub struct TaskHandle {
    counter: SharedCounter,
    region: Region,
}

impl TaskHandle {
    /// The region this task belongs to.
    #[must_use]
    pub const fn region(&self) -> Region {
        self.region
    }
}

impl Drop for TaskHandle {
    fn drop(&mut self) {
        self.counter.fetch_sub(1, Ordering::AcqRel);
    }
}

/// RAII handle for an obligation registered in a region.
///
/// When dropped (resolved), the obligation count is decremented.
/// Obligations model the two-phase lifecycle: Reserved → Committed/Aborted.
pub struct ObligationHandle {
    counter: SharedCounter,
    region: Region,
}

impl ObligationHandle {
    /// The region this obligation belongs to.
    #[must_use]
    pub const fn region(&self) -> Region {
        self.region
    }

    /// Explicitly resolve the obligation (commit or abort).
    ///
    /// Equivalent to dropping the handle; provided for clarity at call sites.
    pub fn resolve(self) {
        // Ownership transfer triggers Drop, which decrements the counter.
    }
}

impl Drop for ObligationHandle {
    fn drop(&mut self) {
        self.counter.fetch_sub(1, Ordering::AcqRel);
    }
}

// ── RegionTree ─────────────────────────────────────────────────────────

/// Tree of regions enforcing structured concurrency (§4.11).
///
/// Every task/actor must be region-owned. The tree enforces
/// INV-REGION-QUIESCENCE: no region closes until all children
/// complete, all finalizers run, and all obligations resolve.
pub struct RegionTree {
    nodes: HashMap<Region, RegionNode>,
    next_id: u32,
    root: Option<Region>,
}

impl Default for RegionTree {
    fn default() -> Self {
        Self::new()
    }
}

impl RegionTree {
    /// Create an empty region tree.
    #[must_use]
    pub fn new() -> Self {
        Self {
            nodes: HashMap::new(),
            next_id: 0,
            root: None,
        }
    }

    /// Create the root region.
    ///
    /// Only one root region may exist. Returns an error if a root already exists.
    pub fn create_root(&mut self, kind: RegionKind, cx: Cx<cx::FullCaps>) -> Result<Region> {
        if self.root.is_some() {
            return Err(FrankenError::Internal(
                "root region already exists".to_owned(),
            ));
        }
        let id = self.alloc_id();
        self.nodes.insert(
            id,
            RegionNode {
                kind,
                state: RegionState::Open,
                cx,
                parent: None,
                children: Vec::new(),
                finalizers: Vec::new(),
                active_tasks: new_counter(),
                active_obligations: new_counter(),
            },
        );
        self.root = Some(id);
        debug!(region = id.get(), kind = ?kind, "region created (root)");
        Ok(id)
    }

    /// Create a child region under the given parent.
    pub fn create_child(
        &mut self,
        parent: Region,
        kind: RegionKind,
        cx: Cx<cx::FullCaps>,
    ) -> Result<Region> {
        let parent_state = self.nodes.get(&parent).map(|n| n.state).ok_or_else(|| {
            FrankenError::Internal(format!("parent region {} not found", parent.get()))
        })?;
        if parent_state != RegionState::Open {
            return Err(FrankenError::Busy);
        }
        let id = self.alloc_id();
        self.nodes.insert(
            id,
            RegionNode {
                kind,
                state: RegionState::Open,
                cx,
                parent: Some(parent),
                children: Vec::new(),
                finalizers: Vec::new(),
                active_tasks: new_counter(),
                active_obligations: new_counter(),
            },
        );
        if let Some(parent_node) = self.nodes.get_mut(&parent) {
            parent_node.children.push(id);
        }
        debug!(region = id.get(), parent = parent.get(), kind = ?kind, "region created (child)");
        Ok(id)
    }

    // ── Accessors ──────────────────────────────────────────────────────

    /// Root region, if created.
    #[must_use]
    pub fn root(&self) -> Option<Region> {
        self.root
    }

    /// Query the kind of a region.
    #[must_use]
    pub fn kind(&self, id: Region) -> Option<RegionKind> {
        self.nodes.get(&id).map(|n| n.kind)
    }

    /// Query the state of a region.
    #[must_use]
    pub fn state(&self, id: Region) -> Option<RegionState> {
        self.nodes.get(&id).map(|n| n.state)
    }

    /// Query the parent of a region.
    #[must_use]
    pub fn parent(&self, id: Region) -> Option<Option<Region>> {
        self.nodes.get(&id).map(|n| n.parent)
    }

    /// List children of a region.
    #[must_use]
    pub fn children(&self, id: Region) -> Option<&[Region]> {
        self.nodes.get(&id).map(|n| n.children.as_slice())
    }

    /// Get a clone of the region's `Cx` for cancellation inspection.
    #[must_use]
    pub fn cx(&self, id: Region) -> Option<Cx<cx::FullCaps>> {
        self.nodes.get(&id).map(|n| n.cx.clone())
    }

    /// Active task count for a region.
    #[must_use]
    pub fn active_tasks(&self, id: Region) -> usize {
        self.nodes
            .get(&id)
            .map_or(0, |n| n.active_tasks.load(Ordering::Acquire))
    }

    /// Active obligation count for a region.
    #[must_use]
    pub fn active_obligations(&self, id: Region) -> usize {
        self.nodes
            .get(&id)
            .map_or(0, |n| n.active_obligations.load(Ordering::Acquire))
    }

    // ── Task / obligation / finalizer registration ─────────────────────

    /// Register a task in a region, returning an RAII handle.
    ///
    /// The task count is incremented; when the handle is dropped, it decrements.
    /// Returns `Err(Busy)` if the region is not [`RegionState::Open`].
    pub fn register_task(&self, id: Region) -> Result<TaskHandle> {
        let node = self
            .nodes
            .get(&id)
            .ok_or_else(|| FrankenError::Internal(format!("region {} not found", id.get())))?;
        if node.state != RegionState::Open {
            return Err(FrankenError::Busy);
        }
        node.active_tasks.fetch_add(1, Ordering::AcqRel);
        debug!(region = id.get(), "task registered");
        Ok(TaskHandle {
            counter: Arc::clone(&node.active_tasks),
            region: id,
        })
    }

    /// Register an obligation in a region, returning an RAII handle.
    ///
    /// Obligations can be registered while the region is Open or Closing
    /// (to allow in-flight work to create follow-up obligations during drain).
    /// Returns `Err(Busy)` if the region is [`RegionState::Closed`].
    pub fn register_obligation(&self, id: Region) -> Result<ObligationHandle> {
        let node = self
            .nodes
            .get(&id)
            .ok_or_else(|| FrankenError::Internal(format!("region {} not found", id.get())))?;
        if node.state == RegionState::Closed {
            return Err(FrankenError::Busy);
        }
        node.active_obligations.fetch_add(1, Ordering::AcqRel);
        debug!(region = id.get(), "obligation registered");
        Ok(ObligationHandle {
            counter: Arc::clone(&node.active_obligations),
            region: id,
        })
    }

    /// Register a finalizer callback to run during region close.
    pub fn register_finalizer(
        &mut self,
        id: Region,
        finalizer: impl FnOnce() + Send + 'static,
    ) -> Result<()> {
        let node = self
            .nodes
            .get_mut(&id)
            .ok_or_else(|| FrankenError::Internal(format!("region {} not found", id.get())))?;
        if node.state != RegionState::Open {
            return Err(FrankenError::Busy);
        }
        node.finalizers.push(Box::new(finalizer));
        Ok(())
    }

    // ── Close protocol ─────────────────────────────────────────────────

    /// Begin closing a region: cancel its `Cx` and set state to `Closing`.
    ///
    /// Recursively begins close on all descendant regions (parent-first
    /// cancellation propagation per INV-CANCEL-PROPAGATES).
    ///
    /// Does NOT wait for quiescence. Use [`is_quiescent`](Self::is_quiescent)
    /// to poll, then [`complete_close`](Self::complete_close) to finalize.
    pub fn begin_close(&mut self, id: Region) -> Result<()> {
        let children = self
            .nodes
            .get(&id)
            .ok_or_else(|| FrankenError::Internal(format!("region {} not found", id.get())))?
            .children
            .clone();

        // Cancel this region's Cx first (parent-first propagation).
        let node = self
            .nodes
            .get_mut(&id)
            .expect("region confirmed present above");
        if node.state == RegionState::Closed {
            return Ok(());
        }
        node.cx.cancel();
        node.state = RegionState::Closing;
        debug!(region = id.get(), kind = ?node.kind, "region closing");

        // Then recursively close children.
        for child in children {
            if self.state(child) == Some(RegionState::Open) {
                self.begin_close(child)?;
            }
        }
        Ok(())
    }

    /// Check whether a region has reached quiescence.
    ///
    /// A region is quiescent when:
    /// - all child regions are [`RegionState::Closed`],
    /// - active task count is zero,
    /// - active obligation count is zero.
    #[must_use]
    pub fn is_quiescent(&self, id: Region) -> bool {
        self.quiescence_snapshot(id)
            .is_some_and(|snapshot| snapshot.is_quiescent())
    }

    /// Capture a deterministic quiescence snapshot for diagnostics and tests.
    #[must_use]
    pub fn quiescence_snapshot(&self, id: Region) -> Option<RegionQuiescenceSnapshot> {
        let node = self.nodes.get(&id)?;
        let non_closed_children = node
            .children
            .iter()
            .filter_map(|child| {
                let state = self.state(*child);
                if state == Some(RegionState::Closed) {
                    None
                } else {
                    Some(ChildRegionQuiescence {
                        region: *child,
                        state,
                    })
                }
            })
            .collect();

        Some(RegionQuiescenceSnapshot {
            region: id,
            state: node.state,
            active_tasks: node.active_tasks.load(Ordering::Acquire),
            active_obligations: node.active_obligations.load(Ordering::Acquire),
            non_closed_children,
        })
    }

    /// Complete region close: run finalizers and mark as [`RegionState::Closed`].
    ///
    /// Returns an error if the region is not quiescent.
    pub fn complete_close(&mut self, id: Region) -> Result<()> {
        let state = self
            .nodes
            .get(&id)
            .map(|n| n.state)
            .ok_or_else(|| FrankenError::Internal(format!("region {} not found", id.get())))?;
        if state == RegionState::Closed {
            return Ok(());
        }
        if state != RegionState::Closing {
            return Err(FrankenError::Internal(
                "region must be in Closing state before complete_close".to_owned(),
            ));
        }
        let snapshot = self
            .quiescence_snapshot(id)
            .ok_or_else(|| FrankenError::Internal(format!("region {} not found", id.get())))?;
        if !snapshot.is_quiescent() {
            return Err(FrankenError::Internal(format!(
                "region not quiescent; children_open={} active_tasks={} active_obligations={}",
                snapshot.non_closed_children.len(),
                snapshot.active_tasks,
                snapshot.active_obligations
            )));
        }
        let node = self
            .nodes
            .get_mut(&id)
            .ok_or_else(|| FrankenError::Internal(format!("region {} not found", id.get())))?;
        let finalizers = std::mem::take(&mut node.finalizers);
        for f in finalizers {
            f();
        }
        node.state = RegionState::Closed;
        debug!(region = id.get(), kind = ?node.kind, "region closed");
        Ok(())
    }

    /// One non-blocking step of the drain half of the close protocol: finalize,
    /// bottom-up, every region in `id`'s subtree that has become quiescent.
    /// Returns `true` once `id` itself is [`RegionState::Closed`].
    ///
    /// Call [`begin_close`](Self::begin_close) first, then drive this until it
    /// returns `true`, yielding between attempts so the tasks being waited for
    /// can run. The drain deliberately never blocks: a synchronous spin here,
    /// on the one thread able to poll the task it waited for, livelocked every
    /// close on a current-thread runtime (bd-viyz2, bd-1eqrr).
    pub fn try_finish_close(&mut self, id: Region) -> Result<bool> {
        let children = self
            .nodes
            .get(&id)
            .map(|n| n.children.clone())
            .unwrap_or_default();
        let mut children_closed = true;
        for child in children {
            children_closed &= self.try_finish_close(child)?;
        }
        if self.state(id) == Some(RegionState::Closed) {
            return Ok(true);
        }
        if !children_closed || self.active_tasks(id) > 0 || self.active_obligations(id) > 0 {
            return Ok(false);
        }
        self.complete_close(id)?;
        Ok(true)
    }

    fn alloc_id(&mut self) -> Region {
        let id = Region::new(self.next_id);
        self.next_id = self.next_id.checked_add(1).expect("region id overflow");
        id
    }
}

// ── Tests ──────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

    use super::*;

    const BEAD_ID: &str = "bd-3go.9";

    #[test]
    fn test_region_tree_structure() {
        let mut tree = RegionTree::new();
        let root = tree
            .create_root(RegionKind::DbRoot, Cx::new())
            .expect("root creation");
        let wc = tree
            .create_child(root, RegionKind::WriteCoordinator, Cx::new())
            .expect("wc");
        let ss = tree
            .create_child(root, RegionKind::SymbolStore, Cx::new())
            .expect("ss");
        let repl = tree
            .create_child(root, RegionKind::Replication, Cx::new())
            .expect("repl");
        let gc = tree
            .create_child(root, RegionKind::CheckpointGc, Cx::new())
            .expect("gc");
        let obs = tree
            .create_child(root, RegionKind::Observability, Cx::new())
            .expect("obs");

        // Verify root.
        assert_eq!(
            tree.root(),
            Some(root),
            "bead_id={BEAD_ID} case=root_exists"
        );
        assert_eq!(tree.kind(root), Some(RegionKind::DbRoot));

        // Verify children of root.
        let children = tree.children(root).expect("root has children");
        assert_eq!(
            children.len(),
            5,
            "bead_id={BEAD_ID} case=root_has_5_service_children"
        );
        assert_eq!(children, &[wc, ss, repl, gc, obs]);

        // Verify each child's kind and parent.
        assert_eq!(tree.kind(wc), Some(RegionKind::WriteCoordinator));
        assert_eq!(tree.kind(ss), Some(RegionKind::SymbolStore));
        assert_eq!(tree.kind(repl), Some(RegionKind::Replication));
        assert_eq!(tree.kind(gc), Some(RegionKind::CheckpointGc));
        assert_eq!(tree.kind(obs), Some(RegionKind::Observability));

        for &child in children {
            assert_eq!(
                tree.parent(child),
                Some(Some(root)),
                "bead_id={BEAD_ID} case=child_parent_is_root region={}",
                child.get()
            );
        }
    }

    #[test]
    fn test_region_quiescence_all_children_complete() {
        let mut tree = RegionTree::new();
        let root = tree
            .create_root(RegionKind::DbRoot, Cx::new())
            .expect("root");
        let region = tree
            .create_child(root, RegionKind::WriteCoordinator, Cx::new())
            .expect("wc");

        // Register 5 tasks.
        let tasks: Vec<TaskHandle> = (0..5)
            .map(|_| tree.register_task(region).expect("register task"))
            .collect();

        assert_eq!(
            tree.active_tasks(region),
            5,
            "bead_id={BEAD_ID} case=5_tasks_registered"
        );

        // Begin close.
        tree.begin_close(region).expect("begin close");
        assert_eq!(tree.state(region), Some(RegionState::Closing));
        assert!(
            !tree.is_quiescent(region),
            "bead_id={BEAD_ID} case=not_quiescent_with_active_tasks"
        );

        // Complete tasks one by one; quiescence only after all 5.
        for (i, task) in tasks.into_iter().enumerate() {
            drop(task);
            if i < 4 {
                assert!(
                    !tree.is_quiescent(region),
                    "bead_id={BEAD_ID} case=not_quiescent_after_{}_completions",
                    i + 1
                );
            }
        }

        assert!(
            tree.is_quiescent(region),
            "bead_id={BEAD_ID} case=quiescent_after_all_tasks_complete"
        );
        tree.complete_close(region).expect("complete close");
        assert_eq!(tree.state(region), Some(RegionState::Closed));
    }

    #[test]
    fn test_region_quiescence_finalizers_run() {
        let mut tree = RegionTree::new();
        let root = tree
            .create_root(RegionKind::DbRoot, Cx::new())
            .expect("root");
        let region = tree
            .create_child(root, RegionKind::WriteCoordinator, Cx::new())
            .expect("wc");

        // Register 3 tasks with corresponding finalizers.
        let flags: Vec<Arc<AtomicBool>> =
            (0..3).map(|_| Arc::new(AtomicBool::new(false))).collect();
        let tasks: Vec<TaskHandle> = (0..3)
            .map(|_| tree.register_task(region).expect("register task"))
            .collect();
        for flag in &flags {
            let f = Arc::clone(flag);
            tree.register_finalizer(region, move || {
                f.store(true, Ordering::Release);
            })
            .expect("register finalizer");
        }

        // Begin close and complete tasks.
        tree.begin_close(region).expect("begin close");
        drop(tasks);

        // Finalizers have NOT run yet.
        for (i, flag) in flags.iter().enumerate() {
            assert!(
                !flag.load(Ordering::Acquire),
                "bead_id={BEAD_ID} case=finalizer_{i}_not_run_before_complete_close"
            );
        }

        // complete_close runs finalizers.
        tree.complete_close(region).expect("complete close");
        for (i, flag) in flags.iter().enumerate() {
            assert!(
                flag.load(Ordering::Acquire),
                "bead_id={BEAD_ID} case=finalizer_{i}_ran_after_complete_close"
            );
        }
    }

    #[test]
    fn test_region_quiescence_obligations_resolved() {
        let mut tree = RegionTree::new();
        let root = tree
            .create_root(RegionKind::DbRoot, Cx::new())
            .expect("root");
        let region = tree
            .create_child(root, RegionKind::WriteCoordinator, Cx::new())
            .expect("wc");

        let obligations: Vec<ObligationHandle> = (0..3)
            .map(|_| {
                tree.register_obligation(region)
                    .expect("register obligation")
            })
            .collect();

        tree.begin_close(region).expect("begin close");
        assert!(
            !tree.is_quiescent(region),
            "bead_id={BEAD_ID} case=not_quiescent_with_pending_obligations"
        );

        // Resolve obligations one by one.
        for (i, obligation) in obligations.into_iter().enumerate() {
            obligation.resolve();
            if i < 2 {
                assert!(
                    !tree.is_quiescent(region),
                    "bead_id={BEAD_ID} case=not_quiescent_after_{}_resolutions",
                    i + 1
                );
            }
        }

        assert!(
            tree.is_quiescent(region),
            "bead_id={BEAD_ID} case=quiescent_after_all_obligations_resolved"
        );
        tree.complete_close(region).expect("complete close");
    }

    #[test]
    fn test_quiescence_snapshot_reports_precise_blockers() {
        let mut tree = RegionTree::new();
        let root = tree
            .create_root(RegionKind::DbRoot, Cx::new())
            .expect("root");
        let parent = tree
            .create_child(root, RegionKind::WriteCoordinator, Cx::new())
            .expect("parent");
        let child = tree
            .create_child(parent, RegionKind::PerConnection, Cx::new())
            .expect("child");
        let _task = tree.register_task(parent).expect("task");
        let _obligation = tree.register_obligation(parent).expect("obligation");

        tree.begin_close(parent).expect("begin close");
        let snapshot = tree
            .quiescence_snapshot(parent)
            .expect("snapshot for existing region");
        assert_eq!(snapshot.region, parent);
        assert_eq!(snapshot.state, RegionState::Closing);
        assert_eq!(snapshot.active_tasks, 1);
        assert_eq!(snapshot.active_obligations, 1);
        assert_eq!(snapshot.non_closed_children.len(), 1);
        assert_eq!(snapshot.non_closed_children[0].region, child);
        assert_eq!(
            snapshot.non_closed_children[0].state,
            Some(RegionState::Closing)
        );
        assert_eq!(snapshot.blocker_count(), 3);
        assert!(!snapshot.is_quiescent());
    }

    #[test]
    fn test_complete_close_error_includes_quiescence_counts() {
        let mut tree = RegionTree::new();
        let root = tree
            .create_root(RegionKind::DbRoot, Cx::new())
            .expect("root");
        let child = tree
            .create_child(root, RegionKind::WriteCoordinator, Cx::new())
            .expect("child");
        let _task = tree.register_task(child).expect("task");

        tree.begin_close(child).expect("begin close");
        let err = tree
            .complete_close(child)
            .expect_err("active task must block close");
        let FrankenError::Internal(message) = err else {
            panic!("expected internal error for non-quiescent close");
        };
        assert!(
            message.contains("children_open=0"),
            "bead_id={BEAD_ID} case=quiescence_error_children_count message={message}"
        );
        assert!(
            message.contains("active_tasks=1"),
            "bead_id={BEAD_ID} case=quiescence_error_task_count message={message}"
        );
        assert!(
            message.contains("active_obligations=0"),
            "bead_id={BEAD_ID} case=quiescence_error_obligation_count message={message}"
        );
    }

    #[test]
    fn test_no_detached_tasks() {
        let tree = RegionTree::new();
        // No regions exist — spawning a task without a valid region must fail.
        let result = tree.register_task(Region::new(999));
        assert!(
            result.is_err(),
            "bead_id={BEAD_ID} case=detached_task_rejected"
        );
    }

    #[test]
    fn test_complete_close_requires_closing_state() {
        let mut tree = RegionTree::new();
        let root = tree
            .create_root(RegionKind::DbRoot, Cx::new())
            .expect("root");
        let child = tree
            .create_child(root, RegionKind::WriteCoordinator, Cx::new())
            .expect("child");

        let err = tree
            .complete_close(child)
            .expect_err("must require begin_close");
        assert!(
            matches!(err, FrankenError::Internal(_)),
            "bead_id={BEAD_ID} case=complete_close_requires_closing_state got {err:?}"
        );
        assert_eq!(
            tree.state(child),
            Some(RegionState::Open),
            "bead_id={BEAD_ID} case=child_state_unchanged_when_close_rejected"
        );
    }

    #[test]
    fn test_database_close_awaits_quiescence() {
        let mut tree = RegionTree::new();
        let root = tree
            .create_root(RegionKind::DbRoot, Cx::new())
            .expect("root");

        // Create service regions with active workers.
        let wc = tree
            .create_child(root, RegionKind::WriteCoordinator, Cx::new())
            .expect("wc");
        let gc = tree
            .create_child(root, RegionKind::CheckpointGc, Cx::new())
            .expect("gc");

        let wc_task = tree.register_task(wc).expect("wc task");
        let gc_task = tree.register_task(gc).expect("gc task");

        let finalized = Arc::new(AtomicBool::new(false));
        {
            let flag = Arc::clone(&finalized);
            tree.register_finalizer(root, move || {
                flag.store(true, Ordering::Release);
            })
            .expect("root finalizer");
        }

        // Begin close of root (cascades to children).
        tree.begin_close(root).expect("begin close root");
        assert_eq!(tree.state(wc), Some(RegionState::Closing));
        assert_eq!(tree.state(gc), Some(RegionState::Closing));

        // Root is not quiescent yet (active child tasks).
        assert!(
            !tree.is_quiescent(root),
            "bead_id={BEAD_ID} case=root_not_quiescent_with_active_children"
        );

        // Complete child tasks.
        drop(wc_task);
        assert!(
            !tree.is_quiescent(root),
            "bead_id={BEAD_ID} case=root_not_quiescent_gc_still_active"
        );
        drop(gc_task);

        // Children are quiescent but not yet closed.
        assert!(tree.is_quiescent(wc));
        assert!(tree.is_quiescent(gc));
        tree.complete_close(wc).expect("close wc");
        tree.complete_close(gc).expect("close gc");

        // Now root is quiescent.
        assert!(
            tree.is_quiescent(root),
            "bead_id={BEAD_ID} case=root_quiescent_after_children_closed"
        );
        tree.complete_close(root).expect("close root");

        assert!(
            finalized.load(Ordering::Acquire),
            "bead_id={BEAD_ID} case=root_finalizer_ran"
        );
        assert_eq!(tree.state(root), Some(RegionState::Closed));
    }

    #[test]
    fn test_per_connection_region_child_of_root() {
        let mut tree = RegionTree::new();
        let root = tree
            .create_root(RegionKind::DbRoot, Cx::new())
            .expect("root");
        let conn = tree
            .create_child(root, RegionKind::PerConnection, Cx::new())
            .expect("conn");

        assert_eq!(
            tree.parent(conn),
            Some(Some(root)),
            "bead_id={BEAD_ID} case=connection_is_child_of_root"
        );
        assert_eq!(tree.kind(conn), Some(RegionKind::PerConnection));

        // Closing root cascades cancellation to connection region.
        let conn_cx = tree.cx(conn).expect("conn cx");
        tree.begin_close(root).expect("begin close root");
        assert!(
            conn_cx.is_cancel_requested(),
            "bead_id={BEAD_ID} case=root_close_cancels_connection"
        );
        assert_eq!(tree.state(conn), Some(RegionState::Closing));

        tree.complete_close(conn).expect("close conn");
        tree.complete_close(root).expect("close root");
    }

    #[test]
    #[allow(clippy::too_many_lines)]
    fn test_e2e_structured_concurrency_shutdown() {
        let mut tree = RegionTree::new();
        let root = tree
            .create_root(RegionKind::DbRoot, Cx::new())
            .expect("root");

        // Create normative service regions.
        let wc = tree
            .create_child(root, RegionKind::WriteCoordinator, Cx::new())
            .expect("wc");
        let ss = tree
            .create_child(root, RegionKind::SymbolStore, Cx::new())
            .expect("ss");
        let repl = tree
            .create_child(root, RegionKind::Replication, Cx::new())
            .expect("repl");
        let gc = tree
            .create_child(root, RegionKind::CheckpointGc, Cx::new())
            .expect("gc");
        let obs = tree
            .create_child(root, RegionKind::Observability, Cx::new())
            .expect("obs");

        // Create 3 connection regions with transaction children.
        let conns: Vec<Region> = (0..3)
            .map(|_| {
                tree.create_child(root, RegionKind::PerConnection, Cx::new())
                    .expect("conn")
            })
            .collect();

        let mut txn_tasks = Vec::new();
        for &conn in &conns {
            let txn = tree
                .create_child(conn, RegionKind::PerTransaction, Cx::new())
                .expect("txn");
            txn_tasks.push(tree.register_task(txn).expect("txn task"));
        }

        // Background workers in service regions.
        let service_tasks = vec![
            tree.register_task(wc).expect("wc task"),
            tree.register_task(ss).expect("ss task"),
            tree.register_task(repl).expect("repl task"),
            tree.register_task(gc).expect("gc task"),
            tree.register_task(obs).expect("obs task"),
        ];

        // Finalizers on root.
        let finalized_count = Arc::new(AtomicUsize::new(0));
        for _ in 0..3 {
            let counter = Arc::clone(&finalized_count);
            tree.register_finalizer(root, move || {
                counter.fetch_add(1, Ordering::AcqRel);
            })
            .expect("root finalizer");
        }

        // Begin close of root (should cascade to all descendants).
        tree.begin_close(root).expect("begin close root");

        // All regions should be Closing.
        assert_eq!(tree.state(root), Some(RegionState::Closing));
        for &conn in &conns {
            assert_eq!(tree.state(conn), Some(RegionState::Closing));
        }

        // Nothing is quiescent yet.
        assert!(
            !tree.is_quiescent(root),
            "bead_id={BEAD_ID} case=e2e_root_not_quiescent_initially"
        );

        // Complete all tasks.
        drop(txn_tasks);
        drop(service_tasks);

        // Close bottom-up: transactions → connections → services → root.
        for &conn in &conns {
            let txn_children = tree.children(conn).expect("conn children").to_vec();
            for txn in txn_children {
                tree.complete_close(txn).expect("close txn");
            }
        }
        for &conn in &conns {
            tree.complete_close(conn).expect("close conn");
        }
        for &svc in &[wc, ss, repl, gc, obs] {
            tree.complete_close(svc).expect("close svc");
        }

        // Finally close root.
        assert!(
            tree.is_quiescent(root),
            "bead_id={BEAD_ID} case=e2e_root_quiescent"
        );
        tree.complete_close(root).expect("close root");

        assert_eq!(
            finalized_count.load(Ordering::Acquire),
            3,
            "bead_id={BEAD_ID} case=e2e_all_finalizers_ran"
        );
        assert_eq!(
            tree.state(root),
            Some(RegionState::Closed),
            "bead_id={BEAD_ID} case=e2e_root_closed"
        );

        // Verify zero orphan tasks.
        assert_eq!(tree.active_tasks(root), 0);
        for &conn in &conns {
            assert_eq!(tree.active_tasks(conn), 0);
        }
    }

    #[test]
    fn test_try_finish_close_threaded() {
        use std::sync::Mutex;
        use std::thread;
        use std::time::Duration;

        let tree = Arc::new(Mutex::new(RegionTree::new()));
        let root = {
            let mut t = tree.lock().unwrap_or_else(|e| e.into_inner());
            t.create_root(RegionKind::DbRoot, Cx::new()).expect("root")
        };
        let wc = {
            let mut t = tree.lock().unwrap_or_else(|e| e.into_inner());
            t.create_child(root, RegionKind::WriteCoordinator, Cx::new())
                .expect("wc")
        };

        // Register tasks before spawning threads.
        let task1 = tree
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .register_task(wc)
            .expect("t1");
        let task2 = tree
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .register_task(wc)
            .expect("t2");

        let completed = Arc::new(AtomicBool::new(false));
        let flag = Arc::clone(&completed);

        // Spawn threads that hold tasks and complete after brief work.
        let t1 = thread::spawn(move || {
            thread::sleep(Duration::from_millis(20));
            drop(task1);
        });
        let t2 = thread::spawn(move || {
            thread::sleep(Duration::from_millis(30));
            drop(task2);
        });

        // Each drain step returns immediately; it reports "not yet" while a
        // task is still held and finalizes once both have dropped.
        tree.lock()
            .unwrap_or_else(|e| e.into_inner())
            .begin_close(root)
            .expect("begin_close");
        let mut pending_steps = 0_u32;
        while !tree
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .try_finish_close(root)
            .expect("try_finish_close")
        {
            pending_steps += 1;
            thread::yield_now();
        }
        assert!(
            pending_steps > 0,
            "bead_id={BEAD_ID} case=drain_step_does_not_block_on_held_tasks"
        );
        flag.store(true, Ordering::Release);

        t1.join().expect("t1 join");
        t2.join().expect("t2 join");

        assert!(
            completed.load(Ordering::Acquire),
            "bead_id={BEAD_ID} case=threaded_close_completed"
        );
        assert_eq!(
            tree.lock().unwrap_or_else(|e| e.into_inner()).state(root),
            Some(RegionState::Closed),
            "bead_id={BEAD_ID} case=threaded_root_closed"
        );
    }
}
