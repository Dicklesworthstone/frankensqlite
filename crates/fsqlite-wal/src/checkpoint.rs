//! WAL checkpoint planning primitives for PASSIVE/FULL/RESTART/TRUNCATE modes.
//!
//! This module models the mode semantics as deterministic pure functions so
//! higher layers can execute checkpoint I/O while preserving mode behavior.

use serde::Serialize;

/// Checkpoint modes matching SQLite WAL checkpoint variants.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub enum CheckpointMode {
    /// Opportunistically backfill frames that do not require waiting.
    Passive,
    /// Attempt to backfill all frames, blocking completion if readers pin the tail.
    Full,
    /// Full checkpoint plus WAL reset when no readers remain.
    Restart,
    /// Restart checkpoint plus WAL truncation when no readers remain.
    Truncate,
}

/// Snapshot of WAL checkpoint state used to compute a mode plan.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CheckpointState {
    /// Highest valid WAL frame index (`mxFrame` equivalent).
    pub total_frames: u32,
    /// Already backfilled frame count (`nBackfill` equivalent).
    pub backfilled_frames: u32,
    /// Oldest active reader end mark frame, if any reader is active.
    ///
    /// `None` means no active readers currently pinning the WAL tail.
    pub oldest_reader_frame: Option<u32>,
}

impl CheckpointState {
    /// Normalize counters to a consistent state before planning.
    #[must_use]
    pub fn normalized(self) -> Self {
        let total_frames = self.total_frames;
        let backfilled_frames = self.backfilled_frames.min(total_frames);
        let oldest_reader_frame = self
            .oldest_reader_frame
            .map(|frame| frame.min(total_frames));
        Self {
            total_frames,
            backfilled_frames,
            oldest_reader_frame,
        }
    }

    /// Number of frames still pending backfill.
    #[must_use]
    pub fn remaining_frames(self) -> u32 {
        self.total_frames.saturating_sub(self.backfilled_frames)
    }
}

/// Planned checkpoint actions for a single checkpoint decision.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CheckpointPlan {
    /// Checkpoint mode used for this plan.
    pub mode: CheckpointMode,
    /// Number of additional frames to backfill immediately.
    pub frames_to_backfill: u32,
    /// Whether frame backfill completes at plan end.
    pub progress: CheckpointProgress,
    /// Whether active readers prevent mode completion behavior right now.
    pub blocked_by_readers: bool,
    /// Post-backfill action requested by the mode.
    pub post_action: CheckpointPostAction,
}

impl CheckpointPlan {
    /// Whether this plan fully completes frame backfill.
    #[must_use]
    pub const fn completes_checkpoint(self) -> bool {
        matches!(self.progress, CheckpointProgress::Complete)
    }

    /// Whether this plan requests a WAL reset.
    #[must_use]
    pub const fn should_reset_wal(self) -> bool {
        matches!(self.post_action, CheckpointPostAction::ResetWal)
    }

    /// Whether this plan requests WAL truncation.
    #[must_use]
    pub const fn should_truncate_wal(self) -> bool {
        matches!(self.post_action, CheckpointPostAction::TruncateWal)
    }
}

/// Backfill completion state for a checkpoint plan.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CheckpointProgress {
    Partial,
    Complete,
}

/// Post-backfill WAL action requested by a checkpoint mode.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CheckpointPostAction {
    None,
    ResetWal,
    TruncateWal,
}

/// Compute a deterministic checkpoint plan from mode and current state.
#[must_use]
pub fn plan_checkpoint(mode: CheckpointMode, state: CheckpointState) -> CheckpointPlan {
    let state = state.normalized();
    let remaining_frames = state.remaining_frames();
    let has_active_reader = state.oldest_reader_frame.is_some();
    let reader_limit = state.oldest_reader_frame.unwrap_or(state.total_frames);
    let reader_eligible = reader_limit.saturating_sub(state.backfilled_frames);

    match mode {
        CheckpointMode::Passive => {
            let frames_to_backfill = reader_eligible.min(remaining_frames);
            CheckpointPlan {
                mode,
                frames_to_backfill,
                progress: completion_for(frames_to_backfill, remaining_frames),
                blocked_by_readers: false,
                post_action: CheckpointPostAction::None,
            }
        }
        CheckpointMode::Full => {
            let frames_to_backfill = reader_eligible.min(remaining_frames);
            let progress = completion_for(frames_to_backfill, remaining_frames);
            CheckpointPlan {
                mode,
                frames_to_backfill,
                progress,
                blocked_by_readers: matches!(progress, CheckpointProgress::Partial),
                post_action: CheckpointPostAction::None,
            }
        }
        CheckpointMode::Restart => {
            let frames_to_backfill = reader_eligible.min(remaining_frames);
            let progress = completion_for(frames_to_backfill, remaining_frames);
            let post_action = if matches!(progress, CheckpointProgress::Complete)
                && !has_active_reader
                && state.total_frames > 0
            {
                CheckpointPostAction::ResetWal
            } else {
                CheckpointPostAction::None
            };
            CheckpointPlan {
                mode,
                frames_to_backfill,
                progress,
                blocked_by_readers: has_active_reader,
                post_action,
            }
        }
        CheckpointMode::Truncate => {
            let frames_to_backfill = reader_eligible.min(remaining_frames);
            let progress = completion_for(frames_to_backfill, remaining_frames);
            let post_action = if matches!(progress, CheckpointProgress::Complete)
                && !has_active_reader
                && state.total_frames > 0
            {
                CheckpointPostAction::TruncateWal
            } else {
                CheckpointPostAction::None
            };
            CheckpointPlan {
                mode,
                frames_to_backfill,
                progress,
                blocked_by_readers: has_active_reader,
                post_action,
            }
        }
    }
}

#[must_use]
const fn completion_for(frames_to_backfill: u32, remaining_frames: u32) -> CheckpointProgress {
    if frames_to_backfill == remaining_frames {
        CheckpointProgress::Complete
    } else {
        CheckpointProgress::Partial
    }
}

#[cfg(test)]
mod tests {
    use super::{CheckpointMode, CheckpointState, plan_checkpoint};

    #[test]
    fn test_passive_respects_reader_limit() {
        let plan = plan_checkpoint(
            CheckpointMode::Passive,
            CheckpointState {
                total_frames: 100,
                backfilled_frames: 40,
                oldest_reader_frame: Some(65),
            },
        );

        assert_eq!(plan.frames_to_backfill, 25);
        assert!(!plan.completes_checkpoint());
        assert!(!plan.blocked_by_readers);
        assert!(!plan.should_reset_wal());
        assert!(!plan.should_truncate_wal());
    }

    #[test]
    fn test_full_marks_blocked_when_reader_pins_tail() {
        let plan = plan_checkpoint(
            CheckpointMode::Full,
            CheckpointState {
                total_frames: 200,
                backfilled_frames: 120,
                oldest_reader_frame: Some(150),
            },
        );

        assert_eq!(plan.frames_to_backfill, 30);
        assert!(!plan.completes_checkpoint());
        assert!(plan.blocked_by_readers);
        assert!(!plan.should_reset_wal());
        assert!(!plan.should_truncate_wal());
    }

    #[test]
    fn test_full_completes_without_readers() {
        let plan = plan_checkpoint(
            CheckpointMode::Full,
            CheckpointState {
                total_frames: 75,
                backfilled_frames: 60,
                oldest_reader_frame: None,
            },
        );

        assert_eq!(plan.frames_to_backfill, 15);
        assert!(plan.completes_checkpoint());
        assert!(!plan.blocked_by_readers);
    }

    #[test]
    fn test_restart_requires_reader_drain_before_reset() {
        let plan = plan_checkpoint(
            CheckpointMode::Restart,
            CheckpointState {
                total_frames: 90,
                backfilled_frames: 90,
                oldest_reader_frame: Some(90),
            },
        );

        assert_eq!(plan.frames_to_backfill, 0);
        assert!(plan.completes_checkpoint());
        assert!(plan.blocked_by_readers);
        assert!(!plan.should_reset_wal());
    }

    #[test]
    fn test_restart_resets_when_complete_and_reader_free() {
        let plan = plan_checkpoint(
            CheckpointMode::Restart,
            CheckpointState {
                total_frames: 64,
                backfilled_frames: 48,
                oldest_reader_frame: None,
            },
        );

        assert_eq!(plan.frames_to_backfill, 16);
        assert!(plan.completes_checkpoint());
        assert!(!plan.blocked_by_readers);
        assert!(plan.should_reset_wal());
    }

    #[test]
    fn test_truncate_requires_reader_drain_before_truncate() {
        let plan = plan_checkpoint(
            CheckpointMode::Truncate,
            CheckpointState {
                total_frames: 40,
                backfilled_frames: 40,
                oldest_reader_frame: Some(40),
            },
        );

        assert_eq!(plan.frames_to_backfill, 0);
        assert!(plan.completes_checkpoint());
        assert!(plan.blocked_by_readers);
        assert!(!plan.should_truncate_wal());
    }

    #[test]
    fn test_truncate_requests_truncate_when_complete_and_reader_free() {
        let plan = plan_checkpoint(
            CheckpointMode::Truncate,
            CheckpointState {
                total_frames: 10,
                backfilled_frames: 4,
                oldest_reader_frame: None,
            },
        );

        assert_eq!(plan.frames_to_backfill, 6);
        assert!(plan.completes_checkpoint());
        assert!(!plan.blocked_by_readers);
        assert!(plan.should_truncate_wal());
        assert!(!plan.should_reset_wal());
    }

    #[test]
    fn test_normalization_clamps_invalid_counters() {
        let plan = plan_checkpoint(
            CheckpointMode::Passive,
            CheckpointState {
                total_frames: 5,
                backfilled_frames: 99,
                oldest_reader_frame: Some(77),
            },
        );

        assert_eq!(plan.frames_to_backfill, 0);
        assert!(plan.completes_checkpoint());
    }

    #[test]
    fn test_empty_wal_all_modes_are_noop() {
        let empty = CheckpointState {
            total_frames: 0,
            backfilled_frames: 0,
            oldest_reader_frame: None,
        };
        for mode in [
            CheckpointMode::Passive,
            CheckpointMode::Full,
            CheckpointMode::Restart,
            CheckpointMode::Truncate,
        ] {
            let plan = plan_checkpoint(mode, empty);
            assert_eq!(plan.frames_to_backfill, 0, "{mode:?} on empty WAL");
            assert!(plan.completes_checkpoint(), "{mode:?} on empty WAL");
            assert!(!plan.blocked_by_readers, "{mode:?} on empty WAL");
            assert!(
                !plan.should_reset_wal() && !plan.should_truncate_wal(),
                "{mode:?} on empty WAL should not request post-actions"
            );
        }
    }

    #[test]
    fn test_passive_no_readers_backfills_all() {
        let plan = plan_checkpoint(
            CheckpointMode::Passive,
            CheckpointState {
                total_frames: 50,
                backfilled_frames: 20,
                oldest_reader_frame: None,
            },
        );
        assert_eq!(plan.frames_to_backfill, 30);
        assert!(plan.completes_checkpoint());
        assert!(!plan.blocked_by_readers);
    }

    #[test]
    fn test_already_fully_backfilled_is_complete() {
        let plan = plan_checkpoint(
            CheckpointMode::Full,
            CheckpointState {
                total_frames: 100,
                backfilled_frames: 100,
                oldest_reader_frame: Some(80),
            },
        );
        assert_eq!(plan.frames_to_backfill, 0);
        assert!(plan.completes_checkpoint());
        assert!(!plan.blocked_by_readers);
    }

    #[test]
    fn test_reader_at_exact_backfill_boundary_yields_zero_work() {
        let plan = plan_checkpoint(
            CheckpointMode::Passive,
            CheckpointState {
                total_frames: 100,
                backfilled_frames: 60,
                oldest_reader_frame: Some(60),
            },
        );
        assert_eq!(plan.frames_to_backfill, 0);
        assert!(!plan.completes_checkpoint());
    }

    #[test]
    fn test_restart_on_fully_backfilled_with_reader_blocks_reset() {
        let plan = plan_checkpoint(
            CheckpointMode::Restart,
            CheckpointState {
                total_frames: 50,
                backfilled_frames: 50,
                oldest_reader_frame: Some(50),
            },
        );
        assert_eq!(plan.frames_to_backfill, 0);
        assert!(plan.completes_checkpoint());
        assert!(plan.blocked_by_readers);
        assert!(!plan.should_reset_wal());
    }

    #[test]
    fn test_truncate_on_fully_backfilled_no_readers_truncates() {
        let plan = plan_checkpoint(
            CheckpointMode::Truncate,
            CheckpointState {
                total_frames: 50,
                backfilled_frames: 50,
                oldest_reader_frame: None,
            },
        );
        assert_eq!(plan.frames_to_backfill, 0);
        assert!(plan.completes_checkpoint());
        assert!(!plan.blocked_by_readers);
        assert!(plan.should_truncate_wal());
        assert!(!plan.should_reset_wal());
    }

    #[test]
    fn test_remaining_frames_saturates_at_zero() {
        let state = CheckpointState {
            total_frames: 10,
            backfilled_frames: 10,
            oldest_reader_frame: None,
        };
        assert_eq!(state.remaining_frames(), 0);
        let over = CheckpointState {
            total_frames: 5,
            backfilled_frames: 99,
            oldest_reader_frame: None,
        };
        assert_eq!(over.remaining_frames(), 0);
    }

    #[test]
    fn test_normalized_clamps_reader_to_total() {
        let state = CheckpointState {
            total_frames: 20,
            backfilled_frames: 30,
            oldest_reader_frame: Some(50),
        };
        let n = state.normalized();
        assert_eq!(n.backfilled_frames, 20);
        assert_eq!(n.oldest_reader_frame, Some(20));
    }

    #[test]
    fn test_full_reader_at_backfill_boundary_is_blocked() {
        let plan = plan_checkpoint(
            CheckpointMode::Full,
            CheckpointState {
                total_frames: 100,
                backfilled_frames: 60,
                oldest_reader_frame: Some(60),
            },
        );
        assert_eq!(plan.frames_to_backfill, 0);
        assert!(!plan.completes_checkpoint());
        assert!(plan.blocked_by_readers);
    }

    #[test]
    fn test_passive_never_reports_blocked() {
        for reader in [Some(10), Some(50), None] {
            let plan = plan_checkpoint(
                CheckpointMode::Passive,
                CheckpointState {
                    total_frames: 50,
                    backfilled_frames: 0,
                    oldest_reader_frame: reader,
                },
            );
            assert!(
                !plan.blocked_by_readers,
                "Passive must never report blocked (reader={reader:?})"
            );
        }
    }

    #[test]
    fn test_restart_no_post_action_on_empty_wal() {
        let plan = plan_checkpoint(
            CheckpointMode::Restart,
            CheckpointState {
                total_frames: 0,
                backfilled_frames: 0,
                oldest_reader_frame: None,
            },
        );
        assert!(plan.completes_checkpoint());
        assert!(!plan.should_reset_wal());
    }

    #[test]
    fn test_normalized_is_idempotent() {
        let state = CheckpointState {
            total_frames: 10,
            backfilled_frames: 50,
            oldest_reader_frame: Some(99),
        };
        let n1 = state.normalized();
        let n2 = n1.normalized();
        assert_eq!(n1, n2);
    }

    #[test]
    fn test_normalized_none_reader_passes_through() {
        let state = CheckpointState {
            total_frames: 30,
            backfilled_frames: 10,
            oldest_reader_frame: None,
        };
        let n = state.normalized();
        assert_eq!(n.total_frames, 30);
        assert_eq!(n.backfilled_frames, 10);
        assert!(n.oldest_reader_frame.is_none());
    }

    #[test]
    fn test_reset_and_truncate_are_mutually_exclusive() {
        for mode in [
            CheckpointMode::Passive,
            CheckpointMode::Full,
            CheckpointMode::Restart,
            CheckpointMode::Truncate,
        ] {
            for reader in [Some(50), None] {
                let plan = plan_checkpoint(
                    mode,
                    CheckpointState {
                        total_frames: 50,
                        backfilled_frames: 0,
                        oldest_reader_frame: reader,
                    },
                );
                assert!(
                    !(plan.should_reset_wal() && plan.should_truncate_wal()),
                    "{mode:?} reader={reader:?}: reset and truncate must be mutually exclusive"
                );
            }
        }
    }

    #[test]
    fn test_checkpoint_mode_copy_and_eq() {
        let a = CheckpointMode::Restart;
        let b = a;
        assert_eq!(a, b);
        assert_ne!(CheckpointMode::Passive, CheckpointMode::Full);
        assert_ne!(CheckpointMode::Restart, CheckpointMode::Truncate);
    }

    #[test]
    fn test_checkpoint_mode_debug_and_serialize() {
        let dbg = format!("{:?}", CheckpointMode::Truncate);
        assert!(dbg.contains("Truncate"));
        let json = serde_json::to_string(&CheckpointMode::Passive).unwrap();
        assert_eq!(json, "\"Passive\"");
        let json_full = serde_json::to_string(&CheckpointMode::Full).unwrap();
        assert_eq!(json_full, "\"Full\"");
    }

    #[test]
    fn test_checkpoint_state_clone_copy_debug() {
        let state = CheckpointState {
            total_frames: 100,
            backfilled_frames: 50,
            oldest_reader_frame: Some(75),
        };
        let copied = state;
        let cloned = state;
        assert_eq!(copied, cloned);
        let dbg = format!("{state:?}");
        assert!(dbg.contains("CheckpointState"));
        assert!(dbg.contains("total_frames"));
        assert!(dbg.contains("100"));
    }

    #[test]
    fn test_checkpoint_plan_clone_debug() {
        use super::{CheckpointPostAction, CheckpointProgress};
        let plan = plan_checkpoint(
            CheckpointMode::Restart,
            CheckpointState {
                total_frames: 20,
                backfilled_frames: 20,
                oldest_reader_frame: None,
            },
        );
        let cloned = plan;
        assert_eq!(plan, cloned);
        let dbg = format!("{plan:?}");
        assert!(dbg.contains("CheckpointPlan"));
        assert!(dbg.contains("Restart"));
        assert_eq!(plan.progress, CheckpointProgress::Complete);
        assert_eq!(plan.post_action, CheckpointPostAction::ResetWal);
    }

    #[test]
    fn test_progress_and_post_action_variants_eq_debug() {
        use super::{CheckpointPostAction, CheckpointProgress};
        assert_ne!(CheckpointProgress::Partial, CheckpointProgress::Complete);
        assert_eq!(CheckpointProgress::Partial, CheckpointProgress::Partial);
        assert_ne!(CheckpointPostAction::None, CheckpointPostAction::ResetWal);
        assert_ne!(
            CheckpointPostAction::ResetWal,
            CheckpointPostAction::TruncateWal
        );
        let dbg_prog = format!("{:?}", CheckpointProgress::Complete);
        assert!(dbg_prog.contains("Complete"));
        let dbg_act = format!("{:?}", CheckpointPostAction::TruncateWal);
        assert!(dbg_act.contains("TruncateWal"));
    }
}
