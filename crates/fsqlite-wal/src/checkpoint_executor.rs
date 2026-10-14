//! WAL checkpoint admission and execution.
//!
//! Physical source admission is separate from the executor's backfill/reset
//! state machine so even a zero-backfill plan is checked before any target
//! callback. The caller still owns generation/append coordination throughout
//! this operation; these read-only checks do not replace that coordination.

#[path = "checkpoint_executor_impl.rs"]
mod implementation;
#[path = "checkpoint_admission.rs"]
mod admission;

pub use implementation::{
    CheckpointExecutionResult, CheckpointTarget, CheckpointTargetFuture,
};

use fsqlite_error::Result;
use fsqlite_types::cx::Cx;
use fsqlite_vfs::VfsFile;

use crate::checkpoint::{CheckpointMode, CheckpointPostAction, CheckpointState, plan_checkpoint};
use crate::wal::WalFile;

/// Validate the accepted WAL snapshot, then backfill and optionally reset it.
///
/// Unlike a cached frame-count comparison, admission detects a newer committed
/// suffix written through another handle and a replaced/truncated generation.
/// Rejection changes neither the WAL handle nor the target. An already empty,
/// headerless WAL is a no-op, not permission to write a synthetic generation.
///
/// # Errors
/// Returns source corruption, stale-plan, cancellation, or I/O errors before
/// mutation when admission fails. Execution errors retain the existing
/// backfill/reset ownership and recovery semantics.
pub async fn execute_checkpoint<F: VfsFile>(
    cx: &Cx,
    wal: &mut WalFile<F>,
    mode: CheckpointMode,
    state: CheckpointState,
    target: &mut impl CheckpointTarget,
) -> Result<CheckpointExecutionResult> {
    if admission::validate_source(cx, wal, state).await? {
        // There is no physical generation to publish or reset. In particular,
        // TRUNCATE on an already zero-byte WAL must not create a new header.
        let mut plan = plan_checkpoint(mode, state);
        plan.post_action = CheckpointPostAction::None;
        return Ok(CheckpointExecutionResult {
            plan,
            frames_backfilled: 0,
            db_size_pages: None,
            wal_was_reset: false,
        });
    }
    implementation::execute_checkpoint(cx, wal, mode, state, target).await
}
