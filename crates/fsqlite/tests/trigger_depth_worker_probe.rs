#![cfg(all(feature = "async-api", not(target_arch = "wasm32")))]
// The raw Connection is intentionally !Send, and keeping its composed future
// on the requested 1 MiB thread is the contract this gate measures.
#![allow(clippy::future_not_send, clippy::large_futures)]

//! Physical-stack release gate for the shared trigger/FK and expression-depth
//! limits. Each release scenario runs in its own process because a native stack
//! overflow aborts the process rather than unwinding through the test harness.
//!
//! The raw-engine scenarios additionally run on an explicitly requested 1 MiB
//! stack. The actor scenario exercises the real dedicated worker owned by
//! `AsyncConnection`.
//!
//! This file also retains the bd-wymdl defect-4a diagnostic for manually
//! probing worker trigger depth.
//!
//! A stack overflow aborts the process, so the sweep drives this test
//! out-of-process, one depth per run:
//!
//! ```text
//! FSQLITE_PROBE_DEPTH=200 cargo test -p fsqlite --features async-api \
//!     --test trigger_depth_worker_probe -- --ignored --nocapture
//! ```

use asupersync::runtime::RuntimeBuilder;
use fsqlite::{AsyncConnection, Connection, FrankenError, Row, SqliteValue};
use fsqlite_core::connection::{
    hot_path_profile_snapshot, reset_hot_path_profile, set_hot_path_profile_enabled,
};
use fsqlite_types::cx::Cx;
use fsqlite_types::limits::{MAX_EXPR_DEPTH, MAX_TRIGGER_DEPTH};
use std::fmt::Write as _;
use std::io::Read as _;
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

const STACK_GATE_SCENARIO_ENV: &str = "FSQLITE_STACK_GATE_SCENARIO";
const STACK_GATE_CHILD_ENV: &str = "FSQLITE_STACK_GATE_CHILD";
const RAW_STACK_BYTES: usize = 1024 * 1024;
const SCENARIO_DEADLINE: Duration = Duration::from_secs(180);
const GATE_DEADLINE: Duration = Duration::from_secs(600);
const CHILD_REAP_DEADLINE: Duration = Duration::from_secs(5);
const STACK_GATE_SCENARIOS: [&str; 7] = [
    "raw_fk",
    "raw_trigger",
    "raw_trigger_fk",
    "raw_fk_trigger_fk",
    "raw_expr_vdbe",
    "raw_expr_subquery",
    "actor",
];

fn trigger_depth_limit() -> usize {
    usize::try_from(MAX_TRIGGER_DEPTH).expect("MAX_TRIGGER_DEPTH must fit usize")
}

fn expression_depth_limit() -> usize {
    usize::try_from(MAX_EXPR_DEPTH).expect("MAX_EXPR_DEPTH must fit usize")
}

fn run_stack_gate_child(scenario: &str, deadline: Duration) {
    let executable = std::env::current_exe().expect("resolve current test executable");
    let mut child = Command::new(executable)
        .args([
            "--exact",
            "physical_stack_release_gate",
            "--nocapture",
            "--test-threads=1",
        ])
        .env(STACK_GATE_CHILD_ENV, "1")
        .env(STACK_GATE_SCENARIO_ENV, scenario)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap_or_else(|error| panic!("spawn stack-gate scenario {scenario}: {error}"));

    let mut stdout = child.stdout.take().expect("child stdout pipe");
    let mut stderr = child.stderr.take().expect("child stderr pipe");
    let stdout_reader = std::thread::spawn(move || {
        let mut bytes = Vec::new();
        stdout
            .read_to_end(&mut bytes)
            .expect("read stack-gate child stdout");
        bytes
    });
    let stderr_reader = std::thread::spawn(move || {
        let mut bytes = Vec::new();
        stderr
            .read_to_end(&mut bytes)
            .expect("read stack-gate child stderr");
        bytes
    });

    let started = Instant::now();
    let (status, timed_out) = 'wait_for_child: loop {
        if let Some(status) = child
            .try_wait()
            .unwrap_or_else(|error| panic!("poll stack-gate scenario {scenario}: {error}"))
        {
            break (status, false);
        }
        if started.elapsed() >= deadline {
            child
                .kill()
                .unwrap_or_else(|error| panic!("terminate timed-out scenario {scenario}: {error}"));
            let reap_started = Instant::now();
            loop {
                if let Some(status) = child
                    .try_wait()
                    .unwrap_or_else(|error| panic!("reap timed-out scenario {scenario}: {error}"))
                {
                    break 'wait_for_child (status, true);
                }
                assert!(
                    reap_started.elapsed() < CHILD_REAP_DEADLINE,
                    "terminated stack-gate scenario {scenario} was not reaped within {}s",
                    CHILD_REAP_DEADLINE.as_secs()
                );
                std::thread::sleep(Duration::from_millis(10));
            }
        }
        std::thread::sleep(Duration::from_millis(10));
    };

    let stdout = stdout_reader
        .join()
        .expect("stack-gate stdout reader must not panic");
    let stderr = stderr_reader
        .join()
        .expect("stack-gate stderr reader must not panic");
    let stdout = String::from_utf8_lossy(&stdout);
    let stderr = String::from_utf8_lossy(&stderr);

    assert!(
        !timed_out,
        "stack-gate scenario {scenario} exceeded {}s\nstdout:\n{stdout}\nstderr:\n{stderr}",
        deadline.as_secs()
    );
    assert!(
        status.success(),
        "stack-gate scenario {scenario} failed with {status}\nstdout:\n{stdout}\nstderr:\n{stderr}"
    );
    let marker = format!("STACK_GATE_OK scenario={scenario}");
    assert!(
        stdout.lines().any(|line| line.contains(&marker)),
        "stack-gate scenario {scenario} exited successfully without `{marker}`\n\
         stdout:\n{stdout}\nstderr:\n{stderr}"
    );
}

#[test]
fn physical_stack_release_gate() {
    if std::env::var(STACK_GATE_CHILD_ENV).as_deref() == Ok("1")
        && let Ok(scenario) = std::env::var(STACK_GATE_SCENARIO_ENV)
    {
        run_stack_gate_scenario(&scenario);
        println!("STACK_GATE_OK scenario={scenario}");
        return;
    }

    let gate_started = Instant::now();
    for scenario in STACK_GATE_SCENARIOS {
        let remaining = GATE_DEADLINE.checked_sub(gate_started.elapsed()).unwrap_or_else(|| {
            panic!(
                "physical stack release gate exceeded its aggregate {}s deadline before scenario {scenario}",
                GATE_DEADLINE.as_secs()
            )
        });
        run_stack_gate_child(scenario, remaining.min(SCENARIO_DEADLINE));
    }
}

fn run_stack_gate_scenario(scenario: &str) {
    match scenario {
        "raw_fk" => run_on_raw_stack(raw_fk_worker),
        "raw_trigger" => run_on_raw_stack(raw_trigger_worker),
        "raw_trigger_fk" => run_on_raw_stack(raw_trigger_fk_worker),
        "raw_fk_trigger_fk" => run_on_raw_stack(raw_fk_trigger_fk_worker),
        "raw_expr_vdbe" => run_on_raw_stack(raw_expr_vdbe_worker),
        "raw_expr_subquery" => run_on_raw_stack(raw_expr_subquery_worker),
        "actor" => run_actor_scenario(),
        other => panic!("unknown stack-gate scenario `{other}`"),
    }
}

fn run_on_raw_stack(task: fn()) {
    let worker = std::thread::Builder::new()
        .stack_size(RAW_STACK_BYTES)
        .spawn(task)
        .expect("spawn requested 1 MiB raw-engine stack");
    if let Err(payload) = worker.join() {
        std::panic::resume_unwind(payload);
    }
}

fn raw_runtime() -> asupersync::runtime::Runtime {
    RuntimeBuilder::current_thread()
        .blocking_threads(1, 1)
        .build()
        .expect("raw stack-gate runtime should build")
}

fn raw_fk_worker() {
    raw_runtime().block_on(run_raw_fk());
}

fn raw_trigger_worker() {
    raw_runtime().block_on(run_raw_trigger());
}

fn raw_trigger_fk_worker() {
    raw_runtime().block_on(run_raw_trigger_fk());
}

fn raw_fk_trigger_fk_worker() {
    raw_runtime().block_on(run_raw_fk_trigger_fk());
}

fn raw_expr_vdbe_worker() {
    raw_runtime().block_on(run_raw_expr_vdbe());
}

fn raw_expr_subquery_worker() {
    raw_runtime().block_on(run_raw_expr_subquery());
}

fn chain_insert_sql(table: &str, max_id: usize) -> String {
    let mut sql = format!("INSERT INTO {table}(id, parent_id) VALUES (0, NULL)");
    for id in 1..=max_id {
        write!(&mut sql, ", ({id}, {})", id - 1).expect("write chain fixture SQL");
    }
    sql.push(';');
    sql
}

fn parameter_sum_sql(height: usize) -> String {
    assert!(height > 0, "parameter expression height must be non-zero");
    let mut sql = String::from("SELECT ");
    for index in 1..=height {
        if index > 1 {
            sql.push_str(" + ");
        }
        write!(&mut sql, "?{index}").expect("write numbered parameter expression");
    }
    sql.push(';');
    sql
}

fn fallback_expression_sql(height: usize) -> String {
    assert!(height > 0, "fallback expression height must be non-zero");
    let expression = format!(
        "{}1{}",
        "(SELECT ".repeat(height - 1),
        ")".repeat(height - 1)
    );
    format!(
        "SELECT {expression} AS value \
         FROM (SELECT 1 AS marker UNION ALL SELECT 2 AS marker) AS derived \
         WHERE marker = 1;"
    )
}

fn integer_at(row: &Row, column: usize, context: &str) -> i64 {
    match row.values().get(column) {
        Some(SqliteValue::Integer(value)) => *value,
        other => panic!("{context}: expected integer at column {column}, got {other:?}"),
    }
}

fn only_integer(rows: &[Row], column: usize, context: &str) -> i64 {
    assert_eq!(rows.len(), 1, "{context}: expected exactly one row");
    integer_at(&rows[0], column, context)
}

fn change_state(rows: &[Row], context: &str) -> (i64, i64) {
    assert_eq!(rows.len(), 1, "{context}: expected one change-state row");
    (
        integer_at(&rows[0], 0, context),
        integer_at(&rows[0], 1, context),
    )
}

fn txn_rollback_stats(rows: &[Row], context: &str) -> (i64, i64) {
    let metric = |wanted: &str| {
        rows.iter()
            .find_map(|row| match row.values() {
                [SqliteValue::Text(name), SqliteValue::Integer(value)]
                    if name.as_ref() == wanted =>
                {
                    Some(*value)
                }
                _ => None,
            })
            .unwrap_or_else(|| panic!("{context}: missing transaction metric `{wanted}`"))
    };
    (
        metric("rollback_count_active"),
        metric("rollback_count_total"),
    )
}

fn assert_exact_chain(rows: &[Row], max_id: usize, context: &str) {
    assert_eq!(
        rows.len(),
        max_id + 1,
        "{context}: unexpected chain cardinality"
    );
    for (id, row) in rows.iter().enumerate() {
        let expected_id = i64::try_from(id).expect("chain id must fit i64");
        assert_eq!(
            row.values().first(),
            Some(&SqliteValue::Integer(expected_id)),
            "{context}: wrong id at chain position {id}"
        );
        if id == 0 {
            assert_eq!(
                row.values().get(1),
                Some(&SqliteValue::Null),
                "{context}: root parent must be NULL"
            );
        } else {
            assert_eq!(
                row.values().get(1),
                Some(&SqliteValue::Integer(expected_id - 1)),
                "{context}: wrong parent at chain position {id}"
            );
        }
    }
}

fn assert_rows(rows: &[Row], expected: &[Vec<SqliteValue>], context: &str) {
    let actual = rows
        .iter()
        .map(|row| row.values().to_vec())
        .collect::<Vec<_>>();
    assert_eq!(actual.as_slice(), expected, "{context}: exact rows differ");
}

async fn raw_change_state(conn: &Connection, context: &str) -> (i64, i64) {
    let rows = conn
        .query("SELECT changes(), total_changes();")
        .await
        .unwrap_or_else(|error| panic!("{context}: query change state: {error}"));
    change_state(&rows, context)
}

async fn raw_txn_rollback_stats(conn: &Connection, context: &str) -> (i64, i64) {
    let rows = conn
        .query("PRAGMA fsqlite.txn_stats;")
        .await
        .unwrap_or_else(|error| panic!("{context}: query transaction stats: {error}"));
    txn_rollback_stats(&rows, context)
}

async fn assert_raw_failure_envelope(
    conn: &Connection,
    before_changes: (i64, i64),
    before_rollbacks: (i64, i64),
    context: &str,
) {
    assert!(
        conn.in_transaction(),
        "{context}: failed statement closed caller transaction"
    );
    let after_changes = raw_change_state(conn, context).await;
    assert_eq!(
        after_changes.0, 0,
        "{context}: failed statement must publish changes() = 0"
    );
    assert_eq!(
        after_changes.1, before_changes.1,
        "{context}: rolled-back work changed total_changes()"
    );
    let after_rollbacks = raw_txn_rollback_stats(conn, context).await;
    assert_eq!(
        after_rollbacks.0,
        before_rollbacks.0 + 1,
        "{context}: statement rollback counter did not advance exactly once"
    );
    assert_eq!(
        after_rollbacks.1,
        before_rollbacks.1 + 1,
        "{context}: total rollback counter did not advance exactly once"
    );
}

async fn assert_raw_markers(conn: &Connection, context: &str) {
    let rows = conn
        .query("SELECT marker FROM gate_marker ORDER BY rowid;")
        .await
        .unwrap_or_else(|error| panic!("{context}: query reuse markers: {error}"));
    assert_rows(
        &rows,
        &[
            vec![SqliteValue::Text("before-failure".into())],
            vec![SqliteValue::Text("after-failure".into())],
        ],
        context,
    );
}

async fn run_raw_fk() {
    let depth = trigger_depth_limit();
    let conn = Connection::open(":memory:")
        .await
        .expect("raw FK connection should open");
    conn.execute_batch(
        "PRAGMA foreign_keys = ON;
         CREATE TABLE fk_ok (
             id INTEGER PRIMARY KEY,
             parent_id INTEGER REFERENCES fk_ok(id) ON DELETE CASCADE
         );
         CREATE TABLE fk_bad (
             id INTEGER PRIMARY KEY,
             parent_id INTEGER REFERENCES fk_bad(id) ON DELETE CASCADE
         );
         CREATE TABLE gate_marker (marker TEXT NOT NULL);",
    )
    .await
    .expect("create raw FK depth fixture");
    conn.execute(&chain_insert_sql("fk_ok", depth + 1))
        .await
        .expect("seed exact-depth FK chain");
    conn.execute(&chain_insert_sql("fk_bad", depth + 1))
        .await
        .expect("seed over-depth FK chain");

    conn.execute("BEGIN;").await.expect("begin raw FK gate");
    assert!(
        conn.in_transaction(),
        "raw FK BEGIN state was not published"
    );
    let before_success = raw_change_state(&conn, "raw FK before success").await;
    assert_eq!(
        conn.execute("DELETE FROM fk_ok WHERE id = 1;")
            .await
            .expect("D nested FK programs must succeed"),
        1
    );
    let after_success = raw_change_state(&conn, "raw FK exact success").await;
    assert_eq!(after_success.0, 1, "raw FK top-level changes() mismatch");
    assert_eq!(
        after_success.1 - before_success.1,
        i64::try_from(depth + 1).expect("FK success delta fits i64"),
        "raw FK total_changes() must include every cascaded row"
    );
    let rows = conn
        .query("SELECT id, parent_id FROM fk_ok ORDER BY id;")
        .await
        .expect("query exact-depth FK survivors");
    assert_exact_chain(&rows, 0, "raw FK exact-depth survivors");

    conn.execute("SAVEPOINT caller;")
        .await
        .expect("create raw FK caller savepoint");
    conn.execute("INSERT INTO gate_marker VALUES ('before-failure');")
        .await
        .expect("seed raw FK reuse marker");
    let before_failure = raw_change_state(&conn, "raw FK before failure").await;
    let before_rollbacks = raw_txn_rollback_stats(&conn, "raw FK before failure").await;
    let error = conn
        .execute("DELETE FROM fk_bad WHERE id = 0;")
        .await
        .expect_err("D+1 nested FK programs must be rejected");
    assert!(
        matches!(&error, FrankenError::TriggerRecursionDepthExceeded),
        "raw FK returned wrong over-depth error: {error:?}"
    );
    assert_raw_failure_envelope(&conn, before_failure, before_rollbacks, "raw FK over-depth").await;
    let rows = conn
        .query("SELECT id, parent_id FROM fk_bad ORDER BY id;")
        .await
        .expect("query rolled-back FK chain");
    assert_exact_chain(&rows, depth + 1, "raw FK rejected-statement rows");
    conn.execute("RELEASE SAVEPOINT caller;")
        .await
        .expect("failed raw FK statement must preserve caller savepoint");
    conn.execute("INSERT INTO gate_marker VALUES ('after-failure');")
        .await
        .expect("raw FK connection must be reusable after rejection");
    assert_raw_markers(&conn, "raw FK markers").await;
    conn.execute("COMMIT;").await.expect("commit raw FK gate");
    assert!(!conn.in_transaction(), "raw FK COMMIT state stayed active");
    conn.close().await.expect("close raw FK connection");
}

fn pure_trigger_schema_sql(depth: usize) -> String {
    let rejected_depth = depth
        .checked_add(1)
        .expect("trigger depth fixture must fit usize");
    format!(
        "PRAGMA recursive_triggers = ON;
         CREATE TABLE pure_ok (n INTEGER NOT NULL);
         CREATE TABLE pure_bad (n INTEGER NOT NULL);
         CREATE TABLE pure_audit (lane TEXT NOT NULL, n INTEGER NOT NULL);
         CREATE TABLE gate_marker (marker TEXT NOT NULL);
         INSERT INTO pure_ok VALUES (0);
         INSERT INTO pure_bad VALUES (0);
         CREATE TRIGGER pure_ok_au AFTER UPDATE ON pure_ok
         WHEN NEW.n < {depth}
         BEGIN
             INSERT INTO pure_audit VALUES ('ok', NEW.n);
             UPDATE pure_ok SET n = NEW.n + 1;
         END;
         CREATE TRIGGER pure_bad_au AFTER UPDATE ON pure_bad
         WHEN NEW.n < {rejected_depth}
         BEGIN
             INSERT INTO pure_audit VALUES ('bad', NEW.n);
             UPDATE pure_bad SET n = NEW.n + 1;
         END;"
    )
}

async fn run_raw_trigger() {
    let depth = trigger_depth_limit();
    let conn = Connection::open(":memory:")
        .await
        .expect("raw pure-trigger connection should open");
    conn.execute_batch(&pure_trigger_schema_sql(depth))
        .await
        .expect("create raw pure-trigger fixture");

    conn.execute("BEGIN;")
        .await
        .expect("begin raw pure-trigger gate");
    let before_success = raw_change_state(&conn, "raw pure trigger before success").await;
    assert_eq!(
        conn.execute("UPDATE pure_ok SET n = 1;")
            .await
            .expect("D nested trigger programs must succeed"),
        1
    );
    let after_success = raw_change_state(&conn, "raw pure trigger exact success").await;
    assert_eq!(
        after_success.1 - before_success.1,
        i64::try_from(depth.saturating_mul(2).saturating_sub(1))
            .expect("pure-trigger success delta fits i64"),
        "raw pure-trigger total_changes() must include nested updates and audit rows"
    );
    let rows = conn
        .query("SELECT n FROM pure_ok;")
        .await
        .expect("query pure-trigger final value");
    assert_eq!(
        only_integer(&rows, 0, "raw pure-trigger final value"),
        i64::try_from(depth).expect("trigger depth fits i64")
    );
    let rows = conn
        .query("SELECT COUNT(*) FROM pure_audit WHERE lane = 'ok';")
        .await
        .expect("query exact pure-trigger audit count");
    assert_eq!(
        only_integer(&rows, 0, "raw pure-trigger audit count"),
        i64::try_from(depth.saturating_sub(1)).expect("trigger audit count fits i64")
    );

    conn.execute("SAVEPOINT caller;")
        .await
        .expect("create raw pure-trigger caller savepoint");
    conn.execute("INSERT INTO gate_marker VALUES ('before-failure');")
        .await
        .expect("seed raw pure-trigger reuse marker");
    let before_failure = raw_change_state(&conn, "raw pure trigger before failure").await;
    let before_rollbacks = raw_txn_rollback_stats(&conn, "raw pure trigger before failure").await;
    let error = conn
        .execute("UPDATE pure_bad SET n = 1;")
        .await
        .expect_err("D+1 nested trigger programs must be rejected");
    assert!(
        matches!(&error, FrankenError::TriggerRecursionDepthExceeded),
        "raw pure trigger returned wrong over-depth error: {error:?}"
    );
    assert_raw_failure_envelope(
        &conn,
        before_failure,
        before_rollbacks,
        "raw pure trigger over-depth",
    )
    .await;
    let rows = conn
        .query("SELECT n FROM pure_bad;")
        .await
        .expect("query rolled-back pure-trigger row");
    assert_eq!(only_integer(&rows, 0, "raw pure-trigger rejected row"), 0);
    let rows = conn
        .query("SELECT COUNT(*) FROM pure_audit WHERE lane = 'bad';")
        .await
        .expect("query rolled-back pure-trigger audit count");
    assert_eq!(
        only_integer(&rows, 0, "raw pure-trigger rejected audit count"),
        0
    );
    conn.execute("RELEASE SAVEPOINT caller;")
        .await
        .expect("failed pure-trigger statement must preserve caller savepoint");
    conn.execute("INSERT INTO gate_marker VALUES ('after-failure');")
        .await
        .expect("raw pure-trigger connection must be reusable after rejection");
    assert_raw_markers(&conn, "raw pure-trigger markers").await;
    conn.execute("COMMIT;")
        .await
        .expect("commit raw pure-trigger gate");
    assert!(
        !conn.in_transaction(),
        "raw pure-trigger COMMIT state stayed active"
    );
    conn.close()
        .await
        .expect("close raw pure-trigger connection");
}

async fn run_raw_trigger_fk() {
    let depth = trigger_depth_limit();
    let conn = Connection::open(":memory:")
        .await
        .expect("raw trigger-FK connection should open");
    conn.execute_batch(
        "PRAGMA foreign_keys = ON;
         PRAGMA recursive_triggers = ON;
         CREATE TABLE tf_chain_ok (
             id INTEGER PRIMARY KEY,
             parent_id INTEGER REFERENCES tf_chain_ok(id) ON DELETE CASCADE
         );
         CREATE TABLE tf_chain_bad (
             id INTEGER PRIMARY KEY,
             parent_id INTEGER REFERENCES tf_chain_bad(id) ON DELETE CASCADE
         );
         CREATE TABLE tf_driver_ok (id INTEGER PRIMARY KEY, start_id INTEGER NOT NULL);
         CREATE TABLE tf_driver_bad (id INTEGER PRIMARY KEY, start_id INTEGER NOT NULL);
         CREATE TABLE tf_audit (lane TEXT NOT NULL, start_id INTEGER NOT NULL);
         CREATE TABLE gate_marker (marker TEXT NOT NULL);
         CREATE TRIGGER tf_driver_ok_ad AFTER DELETE ON tf_driver_ok BEGIN
             INSERT INTO tf_audit VALUES ('ok', OLD.start_id);
             DELETE FROM tf_chain_ok WHERE id = OLD.start_id;
         END;
         CREATE TRIGGER tf_driver_bad_ad AFTER DELETE ON tf_driver_bad BEGIN
             INSERT INTO tf_audit VALUES ('bad', OLD.start_id);
             DELETE FROM tf_chain_bad WHERE id = OLD.start_id;
         END;",
    )
    .await
    .expect("create raw trigger-FK fixture");
    conn.execute(&chain_insert_sql("tf_chain_ok", depth))
        .await
        .expect("seed exact trigger-FK chain");
    conn.execute(&chain_insert_sql("tf_chain_bad", depth))
        .await
        .expect("seed over-depth trigger-FK chain");
    conn.execute("INSERT INTO tf_driver_ok VALUES (1, 1);")
        .await
        .expect("seed exact trigger driver");
    conn.execute("INSERT INTO tf_driver_bad VALUES (1, 0);")
        .await
        .expect("seed over-depth trigger driver");

    conn.execute("BEGIN;")
        .await
        .expect("begin raw trigger-FK gate");
    assert!(
        conn.in_transaction(),
        "raw trigger-FK BEGIN state was not published"
    );
    let before_success = raw_change_state(&conn, "raw trigger-FK before success").await;
    assert_eq!(
        conn.execute("DELETE FROM tf_driver_ok WHERE id = 1;")
            .await
            .expect("one trigger plus D-1 FK programs must succeed"),
        1
    );
    let after_success = raw_change_state(&conn, "raw trigger-FK exact success").await;
    assert_eq!(
        after_success.0, 1,
        "raw trigger-FK top-level changes() mismatch"
    );
    assert_eq!(
        after_success.1 - before_success.1,
        i64::try_from(depth + 2).expect("trigger-FK success delta fits i64"),
        "raw trigger-FK total_changes() must include trigger and cascade writes"
    );
    let rows = conn
        .query("SELECT id, parent_id FROM tf_chain_ok ORDER BY id;")
        .await
        .expect("query exact trigger-FK survivors");
    assert_exact_chain(&rows, 0, "raw trigger-FK exact-depth survivors");

    conn.execute("SAVEPOINT caller;")
        .await
        .expect("create raw trigger-FK caller savepoint");
    conn.execute("INSERT INTO gate_marker VALUES ('before-failure');")
        .await
        .expect("seed raw trigger-FK reuse marker");
    let before_failure = raw_change_state(&conn, "raw trigger-FK before failure").await;
    let before_rollbacks = raw_txn_rollback_stats(&conn, "raw trigger-FK before failure").await;
    let error = conn
        .execute("DELETE FROM tf_driver_bad WHERE id = 1;")
        .await
        .expect_err("one trigger plus D FK programs must be rejected");
    assert!(
        matches!(&error, FrankenError::TriggerRecursionDepthExceeded),
        "raw trigger-FK returned wrong over-depth error: {error:?}"
    );
    assert_raw_failure_envelope(
        &conn,
        before_failure,
        before_rollbacks,
        "raw trigger-FK over-depth",
    )
    .await;
    let rows = conn
        .query("SELECT id, parent_id FROM tf_chain_bad ORDER BY id;")
        .await
        .expect("query rolled-back trigger-FK chain");
    assert_exact_chain(&rows, depth, "raw trigger-FK rejected-statement chain");
    let rows = conn
        .query("SELECT id, start_id FROM tf_driver_bad ORDER BY id;")
        .await
        .expect("query rolled-back trigger driver");
    assert_rows(
        &rows,
        &[vec![SqliteValue::Integer(1), SqliteValue::Integer(0)]],
        "raw trigger-FK rejected driver",
    );
    let rows = conn
        .query("SELECT lane, start_id FROM tf_audit ORDER BY rowid;")
        .await
        .expect("query trigger-FK audit");
    assert_rows(
        &rows,
        &[vec![
            SqliteValue::Text("ok".into()),
            SqliteValue::Integer(1),
        ]],
        "raw trigger-FK audit rollback",
    );
    conn.execute("RELEASE SAVEPOINT caller;")
        .await
        .expect("failed raw trigger-FK statement must preserve caller savepoint");
    conn.execute("INSERT INTO gate_marker VALUES ('after-failure');")
        .await
        .expect("raw trigger-FK connection must be reusable after rejection");
    assert_raw_markers(&conn, "raw trigger-FK markers").await;
    conn.execute("COMMIT;")
        .await
        .expect("commit raw trigger-FK gate");
    assert!(
        !conn.in_transaction(),
        "raw trigger-FK COMMIT state stayed active"
    );
    conn.close().await.expect("close raw trigger-FK connection");
}

const MIXED_SCHEMA: &str = "PRAGMA foreign_keys = ON;
     PRAGMA recursive_triggers = ON;
     CREATE TABLE mixed_root_ok (id INTEGER PRIMARY KEY);
     CREATE TABLE mixed_root_bad (id INTEGER PRIMARY KEY);
     CREATE TABLE mixed_bridge_ok (
         id INTEGER PRIMARY KEY,
         root_id INTEGER NOT NULL REFERENCES mixed_root_ok(id) ON DELETE CASCADE,
         tail_start INTEGER NOT NULL
     );
     CREATE TABLE mixed_bridge_bad (
         id INTEGER PRIMARY KEY,
         root_id INTEGER NOT NULL REFERENCES mixed_root_bad(id) ON DELETE CASCADE,
         tail_start INTEGER NOT NULL
     );
     CREATE TABLE mixed_tail_ok (
         id INTEGER PRIMARY KEY,
         parent_id INTEGER REFERENCES mixed_tail_ok(id) ON DELETE CASCADE
     );
     CREATE TABLE mixed_tail_bad (
         id INTEGER PRIMARY KEY,
         parent_id INTEGER REFERENCES mixed_tail_bad(id) ON DELETE CASCADE
     );
     CREATE TABLE mixed_audit (
         lane TEXT NOT NULL,
         bridge_id INTEGER NOT NULL,
         tail_start INTEGER NOT NULL
     );
     CREATE TABLE gate_marker (marker TEXT NOT NULL);
     CREATE TRIGGER mixed_bridge_ok_ad AFTER DELETE ON mixed_bridge_ok BEGIN
         INSERT INTO mixed_audit VALUES ('ok', OLD.id, OLD.tail_start);
         DELETE FROM mixed_tail_ok WHERE id = OLD.tail_start;
     END;
     CREATE TRIGGER mixed_bridge_bad_ad AFTER DELETE ON mixed_bridge_bad BEGIN
         INSERT INTO mixed_audit VALUES ('bad', OLD.id, OLD.tail_start);
         DELETE FROM mixed_tail_bad WHERE id = OLD.tail_start;
     END;";

async fn seed_raw_mixed(conn: &Connection, depth: usize) {
    conn.execute_batch(MIXED_SCHEMA)
        .await
        .expect("create raw mixed-depth fixture");
    conn.execute(&chain_insert_sql("mixed_tail_ok", depth - 1))
        .await
        .expect("seed exact mixed tail");
    conn.execute(&chain_insert_sql("mixed_tail_bad", depth - 1))
        .await
        .expect("seed over-depth mixed tail");
    conn.execute("INSERT INTO mixed_root_ok VALUES (1);")
        .await
        .expect("seed exact mixed root");
    conn.execute("INSERT INTO mixed_root_bad VALUES (1);")
        .await
        .expect("seed over-depth mixed root");
    conn.execute("INSERT INTO mixed_bridge_ok VALUES (10, 1, 1);")
        .await
        .expect("seed exact mixed bridge");
    conn.execute("INSERT INTO mixed_bridge_bad VALUES (20, 1, 0);")
        .await
        .expect("seed over-depth mixed bridge");
}

async fn run_raw_fk_trigger_fk() {
    let depth = trigger_depth_limit();
    assert!(
        depth >= 2,
        "mixed fixture requires trigger depth at least two"
    );
    let conn = Connection::open(":memory:")
        .await
        .expect("raw mixed connection should open");
    seed_raw_mixed(&conn, depth).await;

    conn.execute("BEGIN;").await.expect("begin raw mixed gate");
    assert!(
        conn.in_transaction(),
        "raw mixed BEGIN state was not published"
    );
    let before_success = raw_change_state(&conn, "raw mixed before success").await;
    assert_eq!(
        conn.execute("DELETE FROM mixed_root_ok WHERE id = 1;")
            .await
            .expect("one outer FK, one trigger, and D-2 tail FKs must succeed"),
        1
    );
    let after_success = raw_change_state(&conn, "raw mixed exact success").await;
    assert_eq!(after_success.0, 1, "raw mixed top-level changes() mismatch");
    assert_eq!(
        after_success.1 - before_success.1,
        i64::try_from(depth + 2).expect("mixed success delta fits i64"),
        "raw mixed total_changes() must include root, bridge, trigger, and tail writes"
    );
    let rows = conn
        .query("SELECT id, parent_id FROM mixed_tail_ok ORDER BY id;")
        .await
        .expect("query exact mixed survivors");
    assert_exact_chain(&rows, 0, "raw mixed exact-depth survivors");

    conn.execute("SAVEPOINT caller;")
        .await
        .expect("create raw mixed caller savepoint");
    conn.execute("INSERT INTO gate_marker VALUES ('before-failure');")
        .await
        .expect("seed raw mixed reuse marker");
    let before_failure = raw_change_state(&conn, "raw mixed before failure").await;
    let before_rollbacks = raw_txn_rollback_stats(&conn, "raw mixed before failure").await;
    let error = conn
        .execute("DELETE FROM mixed_root_bad WHERE id = 1;")
        .await
        .expect_err("one outer FK, one trigger, and D-1 tail FKs must be rejected");
    assert!(
        matches!(&error, FrankenError::TriggerRecursionDepthExceeded),
        "raw mixed returned wrong over-depth error: {error:?}"
    );
    assert_raw_failure_envelope(
        &conn,
        before_failure,
        before_rollbacks,
        "raw mixed over-depth",
    )
    .await;
    let rows = conn
        .query("SELECT id, parent_id FROM mixed_tail_bad ORDER BY id;")
        .await
        .expect("query rolled-back mixed tail");
    assert_exact_chain(&rows, depth - 1, "raw mixed rejected tail");
    let rows = conn
        .query("SELECT id FROM mixed_root_bad ORDER BY id;")
        .await
        .expect("query rolled-back mixed root");
    assert_rows(
        &rows,
        &[vec![SqliteValue::Integer(1)]],
        "raw mixed rejected root",
    );
    let rows = conn
        .query("SELECT id, root_id, tail_start FROM mixed_bridge_bad ORDER BY id;")
        .await
        .expect("query rolled-back mixed bridge");
    assert_rows(
        &rows,
        &[vec![
            SqliteValue::Integer(20),
            SqliteValue::Integer(1),
            SqliteValue::Integer(0),
        ]],
        "raw mixed rejected bridge",
    );
    let rows = conn
        .query("SELECT lane, bridge_id, tail_start FROM mixed_audit ORDER BY rowid;")
        .await
        .expect("query mixed audit");
    assert_rows(
        &rows,
        &[vec![
            SqliteValue::Text("ok".into()),
            SqliteValue::Integer(10),
            SqliteValue::Integer(1),
        ]],
        "raw mixed audit rollback",
    );
    conn.execute("RELEASE SAVEPOINT caller;")
        .await
        .expect("failed raw mixed statement must preserve caller savepoint");
    conn.execute("INSERT INTO gate_marker VALUES ('after-failure');")
        .await
        .expect("raw mixed connection must be reusable after rejection");
    assert_raw_markers(&conn, "raw mixed markers").await;
    conn.execute("COMMIT;")
        .await
        .expect("commit raw mixed gate");
    assert!(
        !conn.in_transaction(),
        "raw mixed COMMIT state stayed active"
    );
    conn.close().await.expect("close raw mixed connection");
}

struct VdbeProfileGuard;

impl VdbeProfileGuard {
    fn enable() -> Self {
        set_hot_path_profile_enabled(true);
        reset_hot_path_profile();
        Self
    }
}

impl Drop for VdbeProfileGuard {
    fn drop(&mut self) {
        set_hot_path_profile_enabled(false);
    }
}

fn assert_sum_result(rows: &[Row], expected: usize, context: &str) {
    assert_eq!(
        only_integer(rows, 0, context),
        i64::try_from(expected).expect("expression result fits i64"),
        "{context}: wrong parameter-sum result"
    );
}

async fn run_raw_expr_vdbe() {
    let depth = expression_depth_limit();
    let conn = Connection::open(":memory:")
        .await
        .expect("raw VDBE-expression connection should open");
    let exact_sql = parameter_sum_sql(depth);
    let over_sql = parameter_sum_sql(depth + 1);
    let exact_params = vec![SqliteValue::Integer(1); depth];
    let over_params = vec![SqliteValue::Integer(1); depth + 1];

    let profile = VdbeProfileGuard::enable();
    let rows = conn
        .query_with_params(&exact_sql, &exact_params)
        .await
        .expect("expression height E must execute through VDBE");
    assert_sum_result(&rows, depth, "raw VDBE expression exact depth");
    let evidence = hot_path_profile_snapshot().vdbe;
    assert!(
        evidence.opcodes_executed_total > 0,
        "raw VDBE expression recorded no executed opcodes"
    );
    assert!(
        evidence.statements_total > 0 && !evidence.opcode_execution_totals.is_empty(),
        "raw VDBE expression profiler lacks per-opcode statement evidence: {evidence:?}"
    );

    let error = conn
        .query_with_params(&over_sql, &over_params)
        .await
        .expect_err("expression height E+1 must fail closed");
    assert!(
        matches!(
            &error,
            FrankenError::ExpressionTooDeep { max } if *max == depth
        ),
        "raw VDBE expression returned wrong depth error: {error:?}"
    );
    assert!(
        !conn.in_transaction(),
        "raw VDBE expression error leaked transaction state"
    );
    let rows = conn
        .query_with_params(&exact_sql, &exact_params)
        .await
        .expect("raw VDBE expression marker must be reusable after rejection");
    assert_sum_result(&rows, depth, "raw VDBE expression reuse");
    drop(profile);
    conn.close()
        .await
        .expect("close raw VDBE-expression connection");
}

fn assert_fallback_result(rows: &[Row], context: &str) {
    assert_eq!(
        only_integer(rows, 0, context),
        1,
        "{context}: wrong nested scalar-subquery result"
    );
}

async fn run_raw_expr_subquery() {
    let depth = expression_depth_limit();
    let conn = Connection::open(":memory:")
        .await
        .expect("raw fallback-expression connection should open");
    let shallow_sql = fallback_expression_sql(2);
    let exact_sql = fallback_expression_sql(depth);
    let over_sql = fallback_expression_sql(depth + 1);

    conn.execute("PRAGMA fsqlite.parity_cert_strict = ON;")
        .await
        .expect("enable raw fallback strict parity");
    let strict_error = conn
        .query(&shallow_sql)
        .await
        .expect_err("strict parity must reject the derived-source fallback");
    let strict_error = strict_error.to_string();
    assert!(
        strict_error.contains("decision_reason=join_or_subquery_fallback"),
        "strict fallback rejection omitted its decision reason: {strict_error}"
    );
    assert!(
        !conn.in_transaction(),
        "raw strict fallback rejection leaked transaction state"
    );
    conn.execute("PRAGMA fsqlite.parity_cert_strict = OFF;")
        .await
        .expect("disable raw fallback strict parity");

    let rows = conn
        .query(&exact_sql)
        .await
        .expect("fallback expression height E must succeed");
    assert_fallback_result(&rows, "raw fallback expression exact depth");
    let error = conn
        .query(&over_sql)
        .await
        .expect_err("fallback expression height E+1 must fail closed");
    assert!(
        matches!(
            &error,
            FrankenError::ExpressionTooDeep { max } if *max == depth
        ),
        "raw fallback expression returned wrong depth error: {error:?}"
    );
    assert!(
        !conn.in_transaction(),
        "raw fallback expression error leaked transaction state"
    );
    let rows = conn
        .query(&exact_sql)
        .await
        .expect("raw fallback expression marker must be reusable after rejection");
    assert_fallback_result(&rows, "raw fallback expression reuse");
    conn.close()
        .await
        .expect("close raw fallback-expression connection");
}

fn actor_change_state(conn: &AsyncConnection, context: &str) -> (i64, i64) {
    let rows = conn
        .query_sync("SELECT changes(), total_changes();")
        .unwrap_or_else(|error| panic!("{context}: query actor change state: {error}"));
    change_state(&rows, context)
}

fn actor_txn_rollback_stats(conn: &AsyncConnection, context: &str) -> (i64, i64) {
    let rows = conn
        .query_sync("PRAGMA fsqlite.txn_stats;")
        .unwrap_or_else(|error| panic!("{context}: query actor transaction stats: {error}"));
    txn_rollback_stats(&rows, context)
}

fn assert_actor_markers(conn: &AsyncConnection, context: &str) {
    let rows = conn
        .query_sync("SELECT marker FROM gate_marker ORDER BY rowid;")
        .unwrap_or_else(|error| panic!("{context}: query actor reuse markers: {error}"));
    assert_rows(
        &rows,
        &[
            vec![SqliteValue::Text("before-failure".into())],
            vec![SqliteValue::Text("after-failure".into())],
        ],
        context,
    );
}

fn seed_actor_mixed(conn: &AsyncConnection, depth: usize) {
    conn.execute_batch_sync(MIXED_SCHEMA)
        .expect("create actor mixed-depth fixture");
    conn.execute_sync(&chain_insert_sql("mixed_tail_ok", depth - 1))
        .expect("seed actor exact mixed tail");
    conn.execute_sync(&chain_insert_sql("mixed_tail_bad", depth - 1))
        .expect("seed actor over-depth mixed tail");
    conn.execute_sync("INSERT INTO mixed_root_ok VALUES (1);")
        .expect("seed actor exact mixed root");
    conn.execute_sync("INSERT INTO mixed_root_bad VALUES (1);")
        .expect("seed actor over-depth mixed root");
    conn.execute_sync("INSERT INTO mixed_bridge_ok VALUES (10, 1, 1);")
        .expect("seed actor exact mixed bridge");
    conn.execute_sync("INSERT INTO mixed_bridge_bad VALUES (20, 1, 0);")
        .expect("seed actor over-depth mixed bridge");
}

fn run_actor_mixed(conn: &AsyncConnection) {
    let depth = trigger_depth_limit();
    assert!(
        depth >= 2,
        "actor mixed fixture requires depth at least two"
    );
    seed_actor_mixed(conn, depth);
    assert!(
        !conn.in_transaction(),
        "actor setup unexpectedly published a transaction"
    );

    conn.begin_transaction_sync()
        .expect("begin actor mixed gate");
    assert!(
        conn.in_transaction(),
        "actor BEGIN state was not immediately published"
    );
    let before_success = actor_change_state(conn, "actor mixed before success");
    assert_eq!(
        conn.execute_sync("DELETE FROM mixed_root_ok WHERE id = 1;")
            .expect("actor exact aggregate depth must succeed"),
        1
    );
    assert!(
        conn.in_transaction(),
        "actor successful statement lost published transaction state"
    );
    let after_success = actor_change_state(conn, "actor mixed exact success");
    assert_eq!(after_success.0, 1, "actor mixed top-level changes mismatch");
    assert_eq!(
        after_success.1 - before_success.1,
        i64::try_from(depth + 2).expect("actor mixed delta fits i64"),
        "actor mixed total_changes mismatch"
    );

    conn.execute_sync("SAVEPOINT caller;")
        .expect("create actor caller savepoint");
    conn.execute_sync("INSERT INTO gate_marker VALUES ('before-failure');")
        .expect("seed actor reuse marker");
    let before_failure = actor_change_state(conn, "actor mixed before failure");
    let before_rollbacks = actor_txn_rollback_stats(conn, "actor mixed before failure");
    let error = conn
        .execute_sync("DELETE FROM mixed_root_bad WHERE id = 1;")
        .expect_err("actor aggregate depth D+1 must be rejected");
    assert!(
        matches!(&error, FrankenError::TriggerRecursionDepthExceeded),
        "actor mixed returned wrong over-depth error: {error:?}"
    );
    assert!(
        conn.in_transaction(),
        "actor failure state was not immediately published as in-transaction"
    );
    let after_failure = actor_change_state(conn, "actor mixed over-depth");
    assert_eq!(
        after_failure.0, 0,
        "actor failed statement changes mismatch"
    );
    assert_eq!(
        after_failure.1, before_failure.1,
        "actor rolled-back work changed total_changes"
    );
    let after_rollbacks = actor_txn_rollback_stats(conn, "actor mixed over-depth");
    assert_eq!(
        after_rollbacks,
        (before_rollbacks.0 + 1, before_rollbacks.1 + 1),
        "actor statement rollback counters must advance exactly once"
    );
    let rows = conn
        .query_sync("SELECT id, parent_id FROM mixed_tail_bad ORDER BY id;")
        .expect("query actor rolled-back tail");
    assert_exact_chain(&rows, depth - 1, "actor mixed rejected tail");
    let rows = conn
        .query_sync("SELECT id FROM mixed_root_bad ORDER BY id;")
        .expect("query actor rolled-back root");
    assert_rows(
        &rows,
        &[vec![SqliteValue::Integer(1)]],
        "actor mixed rejected root",
    );
    let rows = conn
        .query_sync("SELECT id, root_id, tail_start FROM mixed_bridge_bad ORDER BY id;")
        .expect("query actor rolled-back bridge");
    assert_rows(
        &rows,
        &[vec![
            SqliteValue::Integer(20),
            SqliteValue::Integer(1),
            SqliteValue::Integer(0),
        ]],
        "actor mixed rejected bridge",
    );
    let rows = conn
        .query_sync("SELECT lane, bridge_id, tail_start FROM mixed_audit ORDER BY rowid;")
        .expect("query actor mixed audit");
    assert_rows(
        &rows,
        &[vec![
            SqliteValue::Text("ok".into()),
            SqliteValue::Integer(10),
            SqliteValue::Integer(1),
        ]],
        "actor mixed audit rollback",
    );
    conn.execute_sync("RELEASE SAVEPOINT caller;")
        .expect("actor failure must preserve caller savepoint");
    conn.execute_sync("INSERT INTO gate_marker VALUES ('after-failure');")
        .expect("actor connection must be reusable after depth rejection");
    assert_actor_markers(conn, "actor mixed markers");
    conn.commit_transaction_sync()
        .expect("commit actor mixed gate");
    assert!(
        !conn.in_transaction(),
        "actor COMMIT state was not immediately published"
    );
}

fn run_actor_expr_vdbe(conn: &AsyncConnection) {
    let depth = expression_depth_limit();
    let exact_sql = parameter_sum_sql(depth);
    let over_sql = parameter_sum_sql(depth + 1);
    let exact_params = vec![SqliteValue::Integer(1); depth];
    let over_params = vec![SqliteValue::Integer(1); depth + 1];
    let profile = VdbeProfileGuard::enable();

    let rows = conn
        .query_with_params_sync(&exact_sql, &exact_params)
        .expect("actor expression height E must execute through VDBE");
    assert_sum_result(&rows, depth, "actor VDBE expression exact depth");
    assert!(
        !conn.in_transaction(),
        "actor VDBE expression success published a transaction"
    );
    let evidence = hot_path_profile_snapshot().vdbe;
    assert!(
        evidence.opcodes_executed_total > 0
            && evidence.statements_total > 0
            && !evidence.opcode_execution_totals.is_empty(),
        "actor VDBE expression profiler lacks opcode evidence: {evidence:?}"
    );
    let error = conn
        .query_with_params_sync(&over_sql, &over_params)
        .expect_err("actor expression height E+1 must fail closed");
    assert!(
        matches!(
            &error,
            FrankenError::ExpressionTooDeep { max } if *max == depth
        ),
        "actor VDBE expression returned wrong depth error: {error:?}"
    );
    assert!(
        !conn.in_transaction(),
        "actor VDBE expression error state was not immediately idle"
    );
    let rows = conn
        .query_with_params_sync(&exact_sql, &exact_params)
        .expect("actor VDBE marker must be reusable after rejection");
    assert_sum_result(&rows, depth, "actor VDBE expression reuse");
    assert!(
        !conn.in_transaction(),
        "actor VDBE reuse state was not immediately idle"
    );
    drop(profile);
}

fn run_actor_expr_subquery(conn: &AsyncConnection) {
    let depth = expression_depth_limit();
    let shallow_sql = fallback_expression_sql(2);
    let exact_sql = fallback_expression_sql(depth);
    let over_sql = fallback_expression_sql(depth + 1);

    conn.execute_sync("PRAGMA fsqlite.parity_cert_strict = ON;")
        .expect("enable actor fallback strict parity");
    let strict_error = conn
        .query_sync(&shallow_sql)
        .expect_err("actor strict parity must reject derived-source fallback")
        .to_string();
    assert!(
        strict_error.contains("decision_reason=join_or_subquery_fallback"),
        "actor strict fallback rejection omitted decision reason: {strict_error}"
    );
    assert!(
        !conn.in_transaction(),
        "actor strict fallback state was not immediately idle"
    );
    conn.execute_sync("PRAGMA fsqlite.parity_cert_strict = OFF;")
        .expect("disable actor fallback strict parity");

    let rows = conn
        .query_sync(&exact_sql)
        .expect("actor fallback expression height E must succeed");
    assert_fallback_result(&rows, "actor fallback expression exact depth");
    assert!(
        !conn.in_transaction(),
        "actor fallback success state was not immediately idle"
    );
    let error = conn
        .query_sync(&over_sql)
        .expect_err("actor fallback expression height E+1 must fail closed");
    assert!(
        matches!(
            &error,
            FrankenError::ExpressionTooDeep { max } if *max == depth
        ),
        "actor fallback expression returned wrong depth error: {error:?}"
    );
    assert!(
        !conn.in_transaction(),
        "actor fallback error state was not immediately idle"
    );
    let rows = conn
        .query_sync(&exact_sql)
        .expect("actor fallback marker must be reusable after rejection");
    assert_fallback_result(&rows, "actor fallback expression reuse");
    assert!(
        !conn.in_transaction(),
        "actor fallback reuse state was not immediately idle"
    );
}

fn run_actor_scenario() {
    let mut conn = AsyncConnection::open_sync(":memory:")
        .expect("actor stack-gate connection should open on its real worker");
    assert!(
        !conn.in_transaction(),
        "new actor connection unexpectedly published a transaction"
    );
    run_actor_mixed(&conn);
    run_actor_expr_vdbe(&conn);
    run_actor_expr_subquery(&conn);
    conn.close_sync()
        .expect("actor stack-gate worker should close and join");
}

#[test]
#[ignore = "diagnostic measurement, not a regression assertion"]
fn diag_worker_trigger_depth_survival() {
    let depth: usize = std::env::var("FSQLITE_PROBE_DEPTH")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(8);

    let runtime = RuntimeBuilder::current_thread()
        .blocking_threads(1, 1)
        .build()
        .expect("test runtime should build");

    runtime.block_on(async {
        let cx = Cx::new();
        let connection = AsyncConnection::open(&cx, ":memory:".to_owned())
            .await
            .expect("in-memory async connection should open");

        for statement in [
            "PRAGMA recursive_triggers = ON;".to_owned(),
            "CREATE TABLE a (n INTEGER);".to_owned(),
            "CREATE TABLE b (n INTEGER);".to_owned(),
            "INSERT INTO a VALUES (0);".to_owned(),
            "INSERT INTO b VALUES (0);".to_owned(),
            format!(
                "CREATE TRIGGER trg_a AFTER UPDATE ON a WHEN NEW.n < {depth} \
                 BEGIN UPDATE b SET n = NEW.n + 1; END;"
            ),
            format!(
                "CREATE TRIGGER trg_b AFTER UPDATE ON b WHEN NEW.n < {depth} \
                 BEGIN UPDATE a SET n = NEW.n + 1; END;"
            ),
        ] {
            connection
                .execute(&cx, &statement)
                .await
                .unwrap_or_else(|error| panic!("setup statement failed: {statement}: {error}"));
        }

        let result = connection.execute(&cx, "UPDATE a SET n = 1;").await;
        match result {
            Ok(_) => println!("PROBE_SURVIVED worker depth={depth}"),
            Err(error) => println!("PROBE_ERROR worker depth={depth} error={error}"),
        }
    });
}
