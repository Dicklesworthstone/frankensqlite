//! GH-434: semantic guardrails and reproductions for composite-key DML seeks.
//!
//! The parity matrix is enabled. The two performance regressions are deliberately
//! ignored while GH-434 remains unfixed: they document a known failure, not a
//! passing optimization. Run them explicitly with `--ignored --nocapture`.
#![recursion_limit = "512"]

use std::time::{Duration, Instant};

use fsqlite_core::connection::Connection;
use fsqlite_types::SqliteValue;
use rusqlite::types::Value;
use serde::Deserialize;

#[derive(Deserialize)]
struct Case {
    name: String,
    setup: Vec<String>,
    mutations: Vec<Mutation>,
}

#[derive(Deserialize)]
struct Mutation {
    sql: String,
    params: Vec<serde_json::Value>,
    changes: usize,
}

fn oracle_param(value: &serde_json::Value) -> Value {
    match value {
        serde_json::Value::Null => Value::Null,
        serde_json::Value::String(text) => Value::Text(text.clone()),
        serde_json::Value::Number(number) => number.as_i64().map_or_else(
            || Value::Real(number.as_f64().expect("finite numeric fixture")),
            Value::Integer,
        ),
        other => panic!("unsupported fixture parameter: {other}"),
    }
}

fn frank_value(value: &Value) -> SqliteValue {
    match value {
        Value::Null => SqliteValue::Null,
        Value::Integer(value) => SqliteValue::Integer(*value),
        Value::Real(value) => SqliteValue::Float(*value),
        Value::Text(value) => SqliteValue::from(value.as_str()),
        Value::Blob(value) => SqliteValue::Blob(value.clone().into()),
    }
}

async fn compare_query(
    frank: &Connection,
    stock: &rusqlite::Connection,
    sql: &str,
    params: &[Value],
    context: &str,
) {
    let bound: Vec<_> = params.iter().map(frank_value).collect();
    let mut actual: Vec<Vec<SqliteValue>> = frank
        .query_with_params(sql, &bound)
        .await
        .unwrap_or_else(|error| panic!("{context}: {sql}: {error}"))
        .iter()
        .map(|row| row.values().to_vec())
        .collect();
    let mut statement = stock.prepare(sql).expect("oracle prepare");
    let width = statement.column_count();
    let mut expected: Vec<Vec<SqliteValue>> = statement
        .query_map(rusqlite::params_from_iter(params.iter()), |row| {
            (0..width)
                .map(|column| row.get::<_, Value>(column).map(|value| frank_value(&value)))
                .collect::<rusqlite::Result<Vec<_>>>()
        })
        .expect("oracle query")
        .collect::<rusqlite::Result<_>>()
        .expect("oracle rows");
    // RETURNING order is unspecified. Keep duplicate rows, but compare as a
    // multiset so an index-ordered candidate scan may differ from rowid order.
    actual.sort_by_cached_key(|row| format!("{row:?}"));
    expected.sort_by_cached_key(|row| format!("{row:?}"));
    assert_eq!(actual, expected, "{context}: {sql}");
}

const SNAPSHOT: &str = "SELECT rowid,a,b,c,v,gate FROM kv ORDER BY rowid";

#[test]
fn gh434_keyed_dml_matches_sqlite() {
    asupersync::test_utils::run_test(|| async {
        let cases: Vec<Case> =
            serde_json::from_str(include_str!("gh434_keyed_dml_cases.json")).expect("fixtures");
        for case in &cases {
            for persistent in [false, true] {
                let dir = tempfile::tempdir().expect("temporary database directory");
                let path = if persistent {
                    dir.path()
                        .join("keyed.sqlite")
                        .to_string_lossy()
                        .into_owned()
                } else {
                    ":memory:".to_owned()
                };
                let frank = Connection::open(&path).await.expect("open FrankenSQLite");
                let stock = rusqlite::Connection::open_in_memory().expect("open oracle");
                let context = format!("{}; persistent={persistent}", case.name);
                for sql in &case.setup {
                    frank.execute(sql).await.expect("FrankenSQLite fixture");
                    stock.execute_batch(sql).expect("oracle fixture");
                }
                for mutation in &case.mutations {
                    let params: Vec<_> = mutation.params.iter().map(oracle_param).collect();
                    compare_query(&frank, &stock, &mutation.sql, &params, &context).await;
                    let expected_changes: i64 = stock
                        .query_row("SELECT changes()", [], |row| row.get(0))
                        .expect("oracle changes");
                    assert_eq!(
                        expected_changes,
                        i64::try_from(mutation.changes).expect("small fixture count"),
                        "{context}: fixture changed unexpectedly"
                    );
                    compare_query(&frank, &stock, "SELECT changes()", &[], &context).await;
                    compare_query(&frank, &stock, SNAPSHOT, &[], &context).await;
                    compare_query(&frank, &stock, "PRAGMA integrity_check", &[], &context).await;
                }
                frank.close().await.expect("close FrankenSQLite");
                if persistent {
                    let reopened = Connection::open(&path).await.expect("reopen FrankenSQLite");
                    compare_query(&reopened, &stock, SNAPSHOT, &[], &context).await;
                    compare_query(&reopened, &stock, "PRAGMA integrity_check", &[], &context).await;
                    reopened.close().await.expect("close reopened database");
                }
            }
        }
    });
}

async fn populate(connection: &Connection, rows: i64) {
    connection
        .execute("CREATE TABLE kv(a INTEGER,b INTEGER,c INTEGER,v INTEGER,PRIMARY KEY(a,b,c))")
        .await
        .expect("create scaling table");
    connection.execute("BEGIN").await.expect("begin population");
    {
        let insert = connection
            .prepare("INSERT INTO kv(a,b,c,v) VALUES(1,1,?,0)")
            .await
            .expect("prepare population");
        for key in 0..rows {
            insert
                .execute_with_params(&[SqliteValue::Integer(key)])
                .await
                .expect("populate");
        }
    }
    connection.execute("COMMIT").await.expect("commit population");
}

fn integer(value: &SqliteValue) -> i64 {
    match value {
        SqliteValue::Integer(value) => *value,
        other => panic!("expected EXPLAIN integer, got {other:?}"),
    }
}

fn text(value: &SqliteValue) -> &str {
    match value {
        SqliteValue::Text(value) => value.as_str(),
        other => panic!("expected EXPLAIN text, got {other:?}"),
    }
}

#[test]
#[ignore = "GH-434: known DML full-scan failure; enable after implementing composite seeks"]
fn gh434_keyed_dml_must_not_walk_the_table() {
    asupersync::test_utils::run_test(|| async {
        let dir = tempfile::tempdir().expect("temporary database directory");
        let path = dir.path()
            .join("plan.sqlite")
            .to_string_lossy()
            .into_owned();
        let connection = Connection::open(&path).await.expect("open database");
        populate(&connection, 256).await;
        let roots = connection
            .query("SELECT rootpage FROM sqlite_master WHERE type='table' AND name='kv'")
            .await
            .expect("table root");
        let root = integer(&roots[0].values()[0]);
        let params = [
            SqliteValue::Integer(1),
            SqliteValue::Integer(1),
            SqliteValue::Integer(255),
        ];
        for sql in [
            "SELECT v FROM kv WHERE a=? AND b=? AND c=?",
            "UPDATE kv SET v=v+1 WHERE a=? AND b=? AND c=?",
            "DELETE FROM kv WHERE a=? AND b=? AND c=?",
        ] {
            let rows = connection
                .query_with_params(&format!("EXPLAIN {sql}"), &params)
                .await
                .expect("EXPLAIN keyed statement");
            let table_cursors: Vec<_> = rows
                .iter()
                .map(|row| row.values())
                .filter(|row| {
                    matches!(text(&row[1]), "OpenRead" | "OpenWrite") && integer(&row[3]) == root
                })
                .map(|row| integer(&row[2]))
                .collect();
            assert!(!table_cursors.is_empty(), "missing table cursor: {sql}");
            for row in &rows {
                let op = row.values();
                assert!(
                    !table_cursors.contains(&integer(&op[2]))
                        || !matches!(text(&op[1]), "Rewind" | "Last" | "Next" | "Prev"),
                    "GH-434: table walk in {sql}: {op:?}"
                );
            }
            assert!(
                rows.iter().any(|row| {
                    matches!(text(&row.values()[1]), "SeekGE" | "SeekLE" | "NoConflict")
                }),
                "missing keyed seek: {sql}"
            );
        }
        connection.close().await.expect("close plan database");
    });
}

async fn measure_pair(
    connection: &Connection,
    rows: i64,
    delete_miss: bool,
) -> (Duration, Duration) {
    let (keyed_sql, rowid_sql, key, rowid, changed) = if delete_miss {
        (
            "DELETE FROM kv WHERE a=? AND b=? AND c=?",
            "DELETE FROM kv WHERE rowid=?",
            rows + 1,
            rows + 2,
            0,
        )
    } else {
        (
            "UPDATE kv SET v=v+1 WHERE a=? AND b=? AND c=?",
            "UPDATE kv SET v=v+1 WHERE rowid=?",
            rows - 1,
            rows,
            1,
        )
    };
    let keyed = connection
        .prepare(keyed_sql)
        .await
        .expect("prepare keyed DML");
    let direct = connection
        .prepare(rowid_sql)
        .await
        .expect("prepare rowid DML");
    let key_params = [
        SqliteValue::Integer(1),
        SqliteValue::Integer(1),
        SqliteValue::Integer(key),
    ];
    let rowid_params = [SqliteValue::Integer(rowid)];
    for _ in 0..8 {
        assert_eq!(
            keyed.execute_with_params(&key_params).await.expect("warm keyed"),
            changed
        );
        assert_eq!(
            direct
                .execute_with_params(&rowid_params)
                .await
                .expect("warm rowid"),
            changed
        );
    }
    let mut keyed_times = Vec::new();
    let mut rowid_times = Vec::new();
    for sample in 0..9 {
        let mut workloads = [
            (&keyed, key_params.as_slice(), &mut keyed_times),
            (&direct, rowid_params.as_slice(), &mut rowid_times),
        ];
        // Alternate order to avoid consistently favoring either cache state.
        if sample % 2 == 1 {
            workloads.reverse();
        }
        for (statement, params, samples) in workloads {
            let start = Instant::now();
            for _ in 0..16 {
                assert_eq!(
                    statement.execute_with_params(params).await.expect("timed DML"),
                    changed
                );
            }
            samples.push(start.elapsed());
        }
    }
    keyed_times.sort_unstable();
    rowid_times.sort_unstable();
    (keyed_times[4], rowid_times[4])
}

#[test]
#[ignore = "GH-434: known linear scaling; run in release mode with --ignored --nocapture"]
fn gh434_keyed_dml_scaling() {
    asupersync::test_utils::run_test(|| async {
        let mut measurements = Vec::new();
        for rows in [1_600_i64, 19_200] {
            let dir = tempfile::tempdir().expect("temporary database directory");
            let path = dir.path()
                .join("scaling.sqlite")
                .to_string_lossy()
                .into_owned();
            let connection = Connection::open(&path).await.expect("open scaling database");
            populate(&connection, rows).await;
            // Exclude population, preparation, warm-up and commit/fsync costs.
            // All rows share the first TWO key terms: a first-column-only seek
            // still has linear work and must not satisfy this regression.
            connection.execute("BEGIN").await.expect("begin measurements");
            let update = measure_pair(&connection, rows, false).await;
            let delete = measure_pair(&connection, rows, true).await;
            connection
                .execute("ROLLBACK")
                .await
                .expect("rollback measurements");
            eprintln!(
                "rows={rows}, 16-execution medians: update={update:?}, delete_miss={delete:?}"
            );
            measurements.push((update, delete));
            connection.close().await.expect("close scaling database");
        }
        let (small_update, small_delete) = measurements[0];
        let (large_update, large_delete) = measurements[1];
        for (name, small, large) in [
            ("UPDATE hit", small_update, large_update),
            ("DELETE miss", small_delete, large_delete),
        ] {
            assert!(
                large.0 <= small.0.saturating_mul(4),
                "GH-434: {name} grew more than 4x for 12x rows; keyed/rowid small={small:?}, large={large:?}"
            );
        }
    });
}
