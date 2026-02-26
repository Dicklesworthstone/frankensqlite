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
}
