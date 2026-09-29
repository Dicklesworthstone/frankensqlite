//! GH-434: keyed candidate collection must preserve transaction semantics.
//!
//! A UNIQUE failure on the second matching row exercises real partial work:
//! ABORT must undo the statement, whereas OR ROLLBACK must also undo an earlier
//! outer-transaction write. Retrying must not retain a stale candidate rowset.
//! The subsequent key-moving UPDATE and DELETE are rolled back to a savepoint
//! before a final successful update is committed and, for files, reopened.

use fsqlite_core::connection::Connection;
use fsqlite_error::FrankenError;
use fsqlite_types::SqliteValue;
use rusqlite::types::Value;

const SNAPSHOT: &str = "SELECT rowid,a,b,c,v,gate FROM kv ORDER BY rowid";
const KEYED_READ: &str =
    "SELECT rowid,a,b,c,v,gate FROM kv INDEXED BY keyed WHERE a=?1 AND b=?2 ORDER BY rowid";
const MOVE_KEYS: &str = "UPDATE kv SET b=b+10,c=c+10 \
    WHERE b=?2 AND gate=?3 AND a=?1 RETURNING rowid,a,b,c,v,gate";
const DELETE_MOVED: &str = "DELETE FROM kv WHERE b=?2 AND gate=?3 AND a=?1 \
    RETURNING rowid,a,b,c,v,gate";
const RETRY_UPDATE: &str = "UPDATE kv SET v=v+100 WHERE b=?2 AND gate=?3 AND a=?1 \
    RETURNING rowid,a,b,c,v,gate";

fn value(value: Value) -> SqliteValue {
    match value {
        Value::Null => SqliteValue::Null,
        Value::Integer(value) => SqliteValue::Integer(value),
        Value::Real(value) => SqliteValue::Float(value),
        Value::Text(value) => SqliteValue::from(value.as_str()),
        Value::Blob(value) => SqliteValue::Blob(value.into()),
    }
}

async fn execute_both(frank: &Connection, stock: &rusqlite::Connection, sql: &str) {
    frank
        .execute(sql)
        .await
        .unwrap_or_else(|error| panic!("FrankenSQLite: {sql}: {error}"));
    stock
        .execute_batch(sql)
        .unwrap_or_else(|error| panic!("SQLite: {sql}: {error}"));
}

async fn compare_query(
    frank: &Connection,
    stock: &rusqlite::Connection,
    sql: &str,
    params: &[i64],
    context: &str,
) -> usize {
    let bound: Vec<_> = params.iter().copied().map(SqliteValue::Integer).collect();
    let mut actual: Vec<Vec<SqliteValue>> = frank
        .query_with_params(sql, &bound)
        .await
        .unwrap_or_else(|error| panic!("{context}: FrankenSQLite: {sql}: {error}"))
        .iter()
        .map(|row| row.values().to_vec())
        .collect();
    let mut statement = stock
        .prepare(sql)
        .unwrap_or_else(|error| panic!("{context}: SQLite prepare: {sql}: {error}"));
    let width = statement.column_count();
    let mut expected: Vec<Vec<SqliteValue>> = statement
        .query_map(rusqlite::params_from_iter(params.iter()), |row| {
            (0..width)
                .map(|column| row.get::<_, Value>(column).map(value))
                .collect::<rusqlite::Result<Vec<_>>>()
        })
        .expect("oracle query")
        .collect::<rusqlite::Result<_>>()
        .expect("oracle rows");
    // UPDATE/DELETE RETURNING order is unspecified; do not discard duplicates.
    actual.sort_by_cached_key(|row| format!("{row:?}"));
    expected.sort_by_cached_key(|row| format!("{row:?}"));
    assert_eq!(actual, expected, "{context}: {sql}");
    actual.len()
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

async fn assert_keyed_plan(connection: &Connection, sql: &str, context: &str) {
    let table = connection
        .query("SELECT rootpage FROM sqlite_master WHERE type='table' AND name='kv'")
        .await
        .expect("table root");
    let index = connection
        .query("SELECT rootpage FROM sqlite_master WHERE type='index' AND name='keyed'")
        .await
        .expect("keyed index root");
    let table_root = integer(&table[0].values()[0]);
    let index_root = integer(&index[0].values()[0]);
    let rows = connection
        .query_with_params(
            &format!("EXPLAIN {sql}"),
            &[
                SqliteValue::Integer(1),
                SqliteValue::Integer(2),
                SqliteValue::Integer(1),
            ],
        )
        .await
        .expect("EXPLAIN keyed mutation");
    let cursors = |root| {
        rows.iter()
            .map(|row| row.values())
            .filter(|row| {
                matches!(text(&row[1]), "OpenRead" | "OpenWrite") && integer(&row[3]) == root
            })
            .map(|row| integer(&row[2]))
            .collect::<Vec<_>>()
    };
    let table_cursors = cursors(table_root);
    let index_cursors = cursors(index_root);
    assert!(!table_cursors.is_empty(), "{context}: missing table cursor");
    assert!(!index_cursors.is_empty(), "{context}: missing keyed cursor");
    for row in &rows {
        let op = row.values();
        assert!(
            !table_cursors.contains(&integer(&op[2]))
                || !matches!(text(&op[1]), "Rewind" | "Last" | "Next" | "Prev"),
            "{context}: table walk in {sql}: {op:?}"
        );
    }
    assert!(
        rows.iter().any(|row| {
            let op = row.values();
            index_cursors.contains(&integer(&op[2])) && text(&op[1]) == "SeekGE"
        }),
        "{context}: the candidate seek must use keyed, not just a UNIQUE check: {sql}"
    );
}

#[test]
fn gh434_keyed_dml_abort_rollback_and_savepoint_match_sqlite() {
    asupersync::test_utils::run_test(|| async {
        for index_terms in ["a,b", "a,a", "a,a,b"] {
            for rollback_outer in [false, true] {
                for persistent in [false, true] {
                    let directory = tempfile::tempdir().expect("temporary database directory");
                    let path = if persistent {
                        directory
                            .path()
                            .join("keyed.sqlite")
                            .to_string_lossy()
                            .into_owned()
                    } else {
                        ":memory:".to_owned()
                    };
                    let frank = Connection::open(&path).await.expect("open FrankenSQLite");
                    let stock = rusqlite::Connection::open_in_memory().expect("open SQLite");
                    let context = format!(
                        "index=({index_terms}); rollback_outer={rollback_outer}; persistent={persistent}"
                    );
                    execute_both(
                        &frank,
                        &stock,
                        "CREATE TABLE kv(a INTEGER,b INTEGER,c INTEGER,v INTEGER UNIQUE,gate INTEGER)",
                    )
                    .await;
                    execute_both(
                        &frank,
                        &stock,
                        &format!("CREATE INDEX keyed ON kv({index_terms})"),
                    )
                    .await;
                    execute_both(
                        &frank,
                        &stock,
                        "INSERT INTO kv(rowid,a,b,c,v,gate) VALUES \
                         (1,1,2,0,10,1),(2,1,2,1,20,1),(3,1,2,2,30,0),\
                         (4,1,2,3,40,NULL),(5,1,3,0,50,1),(6,9,9,0,60,1)",
                    )
                    .await;

                    let failing_sql = if rollback_outer {
                        "UPDATE OR ROLLBACK kv SET v=900 WHERE b=?2 AND gate=?3 AND a=?1"
                    } else {
                        "UPDATE kv SET v=900 WHERE b=?2 AND gate=?3 AND a=?1"
                    };
                    // A scan or the unrelated UNIQUE index's NoConflict opcode
                    // must not satisfy this regression's indexed-execution gate.
                    if persistent {
                        assert_keyed_plan(&frank, failing_sql, &context).await;
                    }
                    execute_both(&frank, &stock, "BEGIN").await;
                    execute_both(&frank, &stock, "UPDATE kv SET v=61 WHERE rowid=6").await;
                    let error = frank
                        .execute_with_params(
                            failing_sql,
                            &[
                                SqliteValue::Integer(1),
                                SqliteValue::Integer(2),
                                SqliteValue::Integer(1),
                            ],
                        )
                        .await
                        .expect_err("two distinct rows cannot both acquire UNIQUE value 900");
                    assert!(
                        matches!(error, FrankenError::UniqueViolation { .. }),
                        "{context}: expected UNIQUE failure, not an unsupported path: {error}"
                    );
                    let error = stock
                        .execute(failing_sql, [1_i64, 2, 1])
                        .expect_err("oracle UNIQUE failure");
                    assert_eq!(
                        error.sqlite_error_code(),
                        Some(rusqlite::ErrorCode::ConstraintViolation)
                    );
                    assert_eq!(stock.is_autocommit(), rollback_outer, "{context}");
                    compare_query(&frank, &stock, SNAPSHOT, &[], &context).await;
                    compare_query(&frank, &stock, KEYED_READ, &[1, 2], &context).await;

                    // BEGIN detects the native transaction boundary without
                    // assuming a private connection-state representation.
                    let begin = frank.execute("BEGIN").await;
                    let oracle_begin = stock.execute_batch("BEGIN");
                    assert_eq!(oracle_begin.is_ok(), rollback_outer, "{context}");
                    if rollback_outer {
                        begin.expect("OR ROLLBACK must have released the outer transaction");
                    } else {
                        assert!(
                            matches!(begin, Err(FrankenError::NestedTransaction)),
                            "{context}: ABORT must preserve the outer transaction: {begin:?}"
                        );
                    }
                    execute_both(&frank, &stock, "SAVEPOINT keyed_retry").await;
                    assert_eq!(
                        compare_query(&frank, &stock, MOVE_KEYS, &[1, 2, 1], &context).await,
                        2
                    );
                    compare_query(&frank, &stock, "SELECT changes()", &[], &context).await;
                    compare_query(&frank, &stock, SNAPSHOT, &[], &context).await;
                    assert_eq!(
                        compare_query(&frank, &stock, DELETE_MOVED, &[1, 12, 1], &context).await,
                        2
                    );
                    compare_query(&frank, &stock, "SELECT changes()", &[], &context).await;
                    compare_query(&frank, &stock, SNAPSHOT, &[], &context).await;
                    execute_both(&frank, &stock, "ROLLBACK TO keyed_retry").await;
                    compare_query(&frank, &stock, SNAPSHOT, &[], &context).await;
                    compare_query(&frank, &stock, KEYED_READ, &[1, 2], &context).await;
                    execute_both(&frank, &stock, "RELEASE keyed_retry").await;
                    assert_eq!(
                        compare_query(&frank, &stock, RETRY_UPDATE, &[1, 2, 1], &context).await,
                        2
                    );
                    execute_both(&frank, &stock, "COMMIT").await;
                    compare_query(&frank, &stock, SNAPSHOT, &[], &context).await;
                    compare_query(&frank, &stock, "PRAGMA integrity_check", &[], &context).await;
                    frank.close().await.expect("close FrankenSQLite");
                    if persistent {
                        let reopened = Connection::open(&path).await.expect("reopen FrankenSQLite");
                        compare_query(&reopened, &stock, SNAPSHOT, &[], &context).await;
                        compare_query(&reopened, &stock, KEYED_READ, &[1, 2], &context).await;
                        compare_query(
                            &reopened,
                            &stock,
                            "PRAGMA integrity_check",
                            &[],
                            &context,
                        )
                        .await;
                        reopened.close().await.expect("close reopened database");
                    }
                }
            }
        }
    });
}
