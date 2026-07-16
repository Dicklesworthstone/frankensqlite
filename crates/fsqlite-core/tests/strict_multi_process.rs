//! frankensqlite#81 — strict multi-process refusal mode.
//!
//! These tests cover the infrastructure for the strict-multi-process
//! opt-in: the `ConnectionEnv` flag, the `Connection::open_strict_multi_process`
//! convenience constructor, and the `MultiProcessContractViolation` error
//! variant. Transaction lock admission is an enforced refusal site; open-time
//! WAL-checkpoint and freelist-integrity refusals remain separate work.

use fsqlite_core::connection::{Connection, ConnectionEnv};
use fsqlite_error::{ErrorCode, FrankenError};

#[test]
fn connection_env_strict_multi_process_defaults_off() {
    let env = ConnectionEnv::default();
    assert!(
        !env.strict_multi_process(),
        "default ConnectionEnv should leave strict_multi_process disabled to preserve existing best-effort behavior"
    );
}

#[test]
fn connection_env_strict_multi_process_round_trips() {
    let mut env = ConnectionEnv::default();
    env.set_strict_multi_process(true);
    assert!(
        env.strict_multi_process(),
        "after enable, flag should read true"
    );
    env.set_strict_multi_process(false);
    assert!(
        !env.strict_multi_process(),
        "after disable, flag should read false"
    );
}

#[test]
fn open_strict_multi_process_constructor_opens_a_usable_connection() {
    let conn = Connection::open_strict_multi_process(":memory:")
        .expect("strict multi-process constructor should open an in-memory database");
    conn.execute("CREATE TABLE strict_smoke (id INTEGER PRIMARY KEY)")
        .expect("strict multi-process connection should execute ordinary DDL");
}

#[test]
fn multi_process_contract_violation_carries_detail() {
    let err = FrankenError::MultiProcessContractViolation {
        detail: "freelist trunk page 42 exceeds db_size 10".to_string(),
    };
    let msg = format!("{err}");
    assert!(
        msg.contains("multi-process contract violation"),
        "error display should mention the contract violation kind: {msg}"
    );
    assert!(
        msg.contains("freelist trunk page 42"),
        "error display should propagate the detail: {msg}"
    );
    assert_eq!(
        err.error_code(),
        ErrorCode::Busy,
        "strict multi-process refusal should preserve SQLite BUSY compatibility"
    );
}

#[test]
fn strict_multi_process_refuses_expired_transaction_lock_admission() {
    let dir = tempfile::tempdir().expect("create temp directory");
    let path = dir.path().join("strict-lock-admission.db");
    let path = path.to_string_lossy().into_owned();

    let holder = Connection::open(path.clone()).expect("open lock holder");
    holder
        .execute("CREATE TABLE guarded(value INTEGER NOT NULL);")
        .expect("initialize test database");
    let waiter = Connection::open_strict_multi_process(path).expect("open strict waiter");

    holder
        .execute("BEGIN EXCLUSIVE;")
        .expect("holder acquires exclusive transaction lock");
    waiter
        .execute("PRAGMA busy_timeout = 0;")
        .expect("disable retries for deterministic refusal");
    let error = waiter
        .execute("BEGIN IMMEDIATE;")
        .expect_err("strict waiter must classify exhausted lock admission");

    assert!(
        matches!(error, FrankenError::MultiProcessContractViolation { .. }),
        "strict mode must surface a typed contract violation, got {error:?}"
    );
    holder.execute("ROLLBACK;").expect("release holder lock");
}
