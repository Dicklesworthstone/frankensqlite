use fsqlite::{Connection, SqliteValue};
use fsqlite_harness::e2e_traceability::{ScriptKind, build_canonical_inventory};
use serde_json::{Value, json};

const BEAD_ID: &str = "bd-1sf8n";
const SCENARIO_FAMILY: &str = "MVCC-7";
const REPLAY_COMMAND: &str = "cargo test -p fsqlite-harness --test bd_1sf8n_phase9_time_travel_gate -- --nocapture --test-threads=1";
const SCENARIO_IDS: [&str; 3] = [
    "MVCC-7-COMMITSEQ-HISTORY",
    "MVCC-7-TIMESTAMP-LATEST",
    "MVCC-7-TIMESTAMP-MISS",
];
const SHELL_RUNNER_PATH: &str = "scripts/verify_bd_1sf8n_phase9_time_travel.sh";
const HARNESS_TEST_PATH: &str = "crates/fsqlite-harness/tests/bd_1sf8n_phase9_time_travel_gate.rs";

async fn open_connection() -> Connection {
    let conn = Connection::open(":memory:")
        .await
        .expect("in-memory connection should open");
    assert!(
        conn.is_concurrent_mode_default(),
        "bead_id={BEAD_ID} case=concurrent_mode_default_guard"
    );
    conn
}

async fn seed_time_travel_rows(conn: &Connection) {
    conn.execute("CREATE TABLE tt_events (id INTEGER PRIMARY KEY, name TEXT);")
        .await
        .expect("create tt_events table");

    for (id, name) in [(1_i64, "boot"), (2_i64, "steady"), (3_i64, "settled")] {
        conn.execute("BEGIN;").await.expect("begin transaction");
        conn.execute(&format!("INSERT INTO tt_events VALUES ({id}, '{name}');"))
            .await
            .expect("insert tt_events row");
        conn.execute("COMMIT;").await.expect("commit transaction");
    }
}

async fn query_names(conn: &Connection, sql: &str) -> Vec<String> {
    conn.query(sql)
        .await
        .expect("query should succeed")
        .into_iter()
        .map(|row| match row.get(0) {
            Some(SqliteValue::Text(value)) => value.to_string(),
            other => panic!("expected text result column, got {other:?}"),
        })
        .collect()
}

fn scenario_outcome(scenario_id: &str, assertions: Value) -> Value {
    json!({
        "bead_id": BEAD_ID,
        "scenario_family": SCENARIO_FAMILY,
        "scenario_id": scenario_id,
        "replay_command": REPLAY_COMMAND,
        "assertions": assertions,
    })
}

fn emit_scenario_outcome(scenario_id: &str, assertions: Value) {
    println!(
        "SCENARIO_OUTCOME:{}",
        scenario_outcome(scenario_id, assertions)
    );
}

#[test]
fn scenario_catalog_matches_phase9_traceability_contract() {
    let expected_prefix = format!("{SCENARIO_FAMILY}-");
    let expected_ids = SCENARIO_IDS
        .into_iter()
        .collect::<std::collections::BTreeSet<_>>();

    assert_eq!(
        expected_ids.len(),
        SCENARIO_IDS.len(),
        "bead_id={BEAD_ID} case=scenario_ids_unique"
    );

    assert_eq!(
        expected_ids,
        std::collections::BTreeSet::from([
            "MVCC-7-COMMITSEQ-HISTORY",
            "MVCC-7-TIMESTAMP-LATEST",
            "MVCC-7-TIMESTAMP-MISS",
        ]),
        "bead_id={BEAD_ID} case=scenario_ids_expected"
    );

    for scenario_id in SCENARIO_IDS {
        assert!(
            scenario_id.starts_with(&expected_prefix),
            "bead_id={BEAD_ID} case=scenario_family_prefix scenario_id={scenario_id}"
        );
    }

    assert!(
        REPLAY_COMMAND.contains("--test bd_1sf8n_phase9_time_travel_gate"),
        "bead_id={BEAD_ID} case=replay_command_targets_gate"
    );
    assert!(
        REPLAY_COMMAND.contains("--nocapture --test-threads=1"),
        "bead_id={BEAD_ID} case=replay_command_keeps_scenario_output"
    );

    let inventory = build_canonical_inventory();
    let harness_entry = inventory
        .scripts
        .iter()
        .find(|entry| entry.path == HARNESS_TEST_PATH)
        .expect("time-travel harness entry should exist in canonical inventory");
    assert_eq!(
        harness_entry.kind,
        ScriptKind::RustHarnessTest,
        "bead_id={BEAD_ID} case=harness_entry_kind"
    );
    assert_eq!(
        harness_entry.bead_id.as_deref(),
        Some(BEAD_ID),
        "bead_id={BEAD_ID} case=harness_entry_bead_id"
    );
    assert_eq!(
        harness_entry.invocation.command, REPLAY_COMMAND,
        "bead_id={BEAD_ID} case=harness_entry_replay_command"
    );
    assert_eq!(
        harness_entry.scenario_ids,
        vec![SCENARIO_FAMILY.to_owned()],
        "bead_id={BEAD_ID} case=harness_entry_scenario_family"
    );

    let shell_entry = inventory
        .scripts
        .iter()
        .find(|entry| entry.path == SHELL_RUNNER_PATH)
        .expect("time-travel shell runner should exist in canonical inventory");
    assert_eq!(
        shell_entry.kind,
        ScriptKind::ShellUtility,
        "bead_id={BEAD_ID} case=shell_entry_kind"
    );
    assert_eq!(
        shell_entry.bead_id.as_deref(),
        Some(BEAD_ID),
        "bead_id={BEAD_ID} case=shell_entry_bead_id"
    );
    assert_eq!(
        shell_entry.scenario_ids,
        vec![SCENARIO_FAMILY.to_owned()],
        "bead_id={BEAD_ID} case=shell_entry_scenario_family"
    );
}

#[test]
fn shell_runner_matches_time_travel_traceability_contract() {
    let shell_runner = include_str!("../../../scripts/verify_bd_1sf8n_phase9_time_travel.sh");

    assert!(
        shell_runner.contains("SCENARIO_ID=\"MVCC-7\""),
        "bead_id={BEAD_ID} case=shell_runner_scenario_family"
    );
    assert!(
        shell_runner.contains(&format!("REPLAY_COMMAND=\"{REPLAY_COMMAND}\"")),
        "bead_id={BEAD_ID} case=shell_runner_replay_command"
    );
    assert!(
        shell_runner.contains("expected at least 3 scenario outcomes"),
        "bead_id={BEAD_ID} case=shell_runner_expected_scenario_count"
    );
}

#[test]
fn scenario_outcome_metadata_contract() {
    for scenario_id in SCENARIO_IDS {
        let outcome = scenario_outcome(scenario_id, json!({"probe": true}));

        assert_eq!(outcome["bead_id"], BEAD_ID);
        assert_eq!(outcome["scenario_family"], SCENARIO_FAMILY);
        assert_eq!(outcome["scenario_id"], scenario_id);
        assert_eq!(outcome["replay_command"], REPLAY_COMMAND);
        assert!(
            outcome["assertions"].is_object(),
            "bead_id={BEAD_ID} case=assertions_payload_object scenario_id={scenario_id}"
        );
    }
}

#[test]
fn commitseq_historical_read_returns_point_in_time_state() {
    asupersync::test_utils::run_test(|| async {
        let conn = open_connection().await;
        seed_time_travel_rows(&conn).await;

        let historical_names = query_names(
            &conn,
            "SELECT name FROM tt_events FOR SYSTEM_TIME AS OF COMMITSEQ 3 ORDER BY id;",
        )
        .await;
        assert_eq!(
            historical_names,
            vec!["boot".to_owned(), "steady".to_owned()],
            "bead_id={BEAD_ID} case=commitseq_historical_rows"
        );

        let live_names = query_names(&conn, "SELECT name FROM tt_events ORDER BY id;").await;
        assert_eq!(
            live_names,
            vec!["boot".to_owned(), "steady".to_owned(), "settled".to_owned()],
            "bead_id={BEAD_ID} case=commitseq_live_state_preserved"
        );

        emit_scenario_outcome(
            SCENARIO_IDS[0],
            json!({
                "historical_commit_seq": 3,
                "historical_names": historical_names,
                "live_names": live_names,
            }),
        );
    });
}

#[test]
fn future_timestamp_resolves_latest_retained_snapshot() {
    asupersync::test_utils::run_test(|| async {
        let conn = open_connection().await;
        seed_time_travel_rows(&conn).await;

        let historical_names = query_names(
            &conn,
            "SELECT name FROM tt_events FOR SYSTEM_TIME AS OF '9999-12-31 23:59:59' ORDER BY id;",
        )
        .await;
        let live_names = query_names(&conn, "SELECT name FROM tt_events ORDER BY id;").await;
        assert_eq!(
            historical_names, live_names,
            "bead_id={BEAD_ID} case=future_timestamp_latest_snapshot"
        );

        emit_scenario_outcome(
            SCENARIO_IDS[1],
            json!({
                "target_timestamp": "9999-12-31 23:59:59",
                "resolved_names": historical_names,
            }),
        );
    });
}

#[test]
fn early_timestamp_errors_explicitly_without_corrupting_live_state() {
    asupersync::test_utils::run_test(|| async {
        let conn = open_connection().await;
        seed_time_travel_rows(&conn).await;

        let error = conn
            .query(
                "SELECT name FROM tt_events FOR SYSTEM_TIME AS OF '1970-01-01 00:00:00' ORDER BY id;",
            )
            .await
            .expect_err("early timestamp should miss the retained snapshot ring");
        let error_text = error.to_string();
        assert!(
            error_text.contains("time-travel") || error_text.contains("no snapshot available"),
            "bead_id={BEAD_ID} case=missing_snapshot_error_text error={error_text}"
        );

        let live_names = query_names(&conn, "SELECT name FROM tt_events ORDER BY id;").await;
        assert_eq!(
            live_names,
            vec!["boot".to_owned(), "steady".to_owned(), "settled".to_owned()],
            "bead_id={BEAD_ID} case=early_timestamp_live_state_preserved"
        );

        emit_scenario_outcome(
            SCENARIO_IDS[2],
            json!({
                "target_timestamp": "1970-01-01 00:00:00",
                "error_text": error_text,
                "live_names": live_names,
            }),
        );
    });
}
