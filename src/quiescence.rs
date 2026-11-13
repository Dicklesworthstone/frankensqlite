//! Quiescence contract helpers for structured-concurrency shutdown.
//!
//! This module keeps the quiescence proof surface small and deterministic so
//! future asupersync lab-oracle tests can inspect why a region is still open
//! without reaching into [`crate::region::RegionTree`] internals.

use fsqlite_types::Region;

use crate::region::RegionState;

/// Snapshot of one child region that still blocks parent quiescence.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ChildRegionQuiescence {
    /// Child region identifier.
    pub region: Region,
    /// Child state at observation time.
    pub state: Option<RegionState>,
}

/// Deterministic quiescence view for a single region close attempt.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RegionQuiescenceSnapshot {
    /// Region being observed.
    pub region: Region,
    /// Region lifecycle state at observation time.
    pub state: RegionState,
    /// Number of active region-owned tasks still in flight.
    pub active_tasks: usize,
    /// Number of active obligations still unresolved.
    pub active_obligations: usize,
    /// Child regions that have not yet reached `Closed`.
    pub non_closed_children: Vec<ChildRegionQuiescence>,
}

impl RegionQuiescenceSnapshot {
    /// Whether the region satisfies the quiescence invariant.
    #[must_use]
    pub fn is_quiescent(&self) -> bool {
        self.active_tasks == 0
            && self.active_obligations == 0
            && self.non_closed_children.is_empty()
    }

    /// Count of distinct blocker classes still preventing close completion.
    #[must_use]
    pub fn blocker_count(&self) -> usize {
        usize::from(self.active_tasks > 0)
            + usize::from(self.active_obligations > 0)
            + usize::from(!self.non_closed_children.is_empty())
    }
}

#[cfg(test)]
mod tests {
    use super::{ChildRegionQuiescence, RegionQuiescenceSnapshot};
    use crate::region::RegionState;
    use fsqlite_types::Region;

    #[test]
    fn region_quiescence_snapshot_detects_blockers() {
        let snapshot = RegionQuiescenceSnapshot {
            region: Region::new(7),
            state: RegionState::Closing,
            active_tasks: 2,
            active_obligations: 1,
            non_closed_children: vec![ChildRegionQuiescence {
                region: Region::new(9),
                state: Some(RegionState::Closing),
            }],
        };

        assert!(!snapshot.is_quiescent());
        assert_eq!(snapshot.blocker_count(), 3);
    }

    #[test]
    fn region_quiescence_snapshot_accepts_closed_leaf() {
        let snapshot = RegionQuiescenceSnapshot {
            region: Region::new(3),
            state: RegionState::Closing,
            active_tasks: 0,
            active_obligations: 0,
            non_closed_children: Vec::new(),
        };

        assert!(snapshot.is_quiescent());
        assert_eq!(snapshot.blocker_count(), 0);
    }
}
