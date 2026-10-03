//! Column-list CTAS and trigger frames must preserve SQLite storage classes.

use fsqlite_core::connection::Connection;
use fsqlite_types::SqliteValue;

async fn compare(setup: &[&str], query: &str) {
    let frank = Connection::open(":memory:").await.unwrap();
    let stock = rusqlite::Connection::open_in_memory().unwrap();
    for sql in setup {
        if let Some(sql) = sql.strip_prefix("-- expect-error: ") {
            assert!(frank.execute(sql).await.is_err(), "FrankenSQLite: {sql}");
            assert!(stock.execute_batch(sql).is_err(), "SQLite: {sql}");
        } else {
            frank.execute(sql).await.unwrap();
            stock.execute_batch(sql).unwrap();
        }
    }
    let actual = frank
        .query(query)
        .await
        .unwrap()
        .iter()
        .map(|row| row.values().to_vec())
        .collect::<Vec<_>>();
    let mut statement = stock.prepare(query).unwrap();
    let width = statement.column_count();
    let expected = statement
        .query_map([], |row| {
            (0..width)
                .map(|index| {
                    Ok(match row.get::<_, rusqlite::types::Value>(index)? {
                        rusqlite::types::Value::Null => SqliteValue::Null,
                        rusqlite::types::Value::Integer(value) => SqliteValue::Integer(value),
                        rusqlite::types::Value::Real(value) => SqliteValue::Float(value),
                        rusqlite::types::Value::Text(value) => SqliteValue::Text(value.into()),
                        rusqlite::types::Value::Blob(value) => SqliteValue::Blob(value.into()),
                    })
                })
                .collect::<rusqlite::Result<Vec<_>>>()
        })
        .unwrap()
        .collect::<rusqlite::Result<Vec<_>>>()
        .unwrap();
    let classes = |rows: &[Vec<SqliteValue>]| {
        rows.iter()
            .map(|row| row.iter().map(SqliteValue::storage_class).collect::<Vec<_>>())
            .collect::<Vec<_>>()
    };
    assert_eq!(
        classes(&actual),
        classes(&expected),
        "storage classes: {query}"
    );
    assert_eq!(actual, expected, "values differ from stock SQLite: {query}");
}

#[test]
fn temp_ctas_preserves_inferred_affinity_for_later_inserts() {
    asupersync::test_utils::run_test(|| async {
        compare(
            &[
                "CREATE TABLE src(a INTEGER, r REAL, s TEXT, n NUMERIC, b BLOB, u)",
                "INSERT INTO src VALUES(1, 1.5, 'start', 5, X'01', 6)",
                "CREATE TEMP TABLE copy AS SELECT * FROM src",
                "INSERT INTO temp.copy VALUES('02', '2.0', 3, '4.0', '5', '6')",
            ],
            "SELECT a, typeof(a), r, typeof(r), s, typeof(s), n, typeof(n), b, typeof(b), u, typeof(u) FROM temp.copy ORDER BY rowid",
        )
        .await;
    });
}

#[test]
fn main_ctas_under_temp_shadow_preserves_inferred_affinity() {
    asupersync::test_utils::run_test(|| async {
        compare(
            &[
                "CREATE TABLE src(a INTEGER, s TEXT)",
                "INSERT INTO src VALUES(1, 'start')",
                "CREATE TEMP TABLE copy(a, s)",
                "INSERT INTO temp.copy VALUES(99, 'temp')",
                "CREATE TABLE main.copy AS SELECT * FROM src",
                "INSERT INTO main.copy VALUES('02', 3)",
            ],
            "SELECT a, typeof(a), s, typeof(s) FROM main.copy ORDER BY rowid",
        )
        .await;
    });
}

#[test]
fn strict_any_before_trigger_preserves_numeric_text() {
    asupersync::test_utils::run_test(|| async {
        compare(
            &[
                "CREATE TABLE q(a ANY) STRICT",
                "CREATE TABLE log(v, ty)",
                "CREATE TRIGGER qb BEFORE INSERT ON q BEGIN INSERT INTO log VALUES(NEW.a, typeof(NEW.a)); END",
                "INSERT INTO q VALUES('001')",
            ],
            "SELECT v, ty FROM log ORDER BY rowid",
        )
        .await;
    });
}

#[test]
fn delete_replay_preserves_fail_and_abort_statement_boundaries() {
    asupersync::test_utils::run_test(|| async {
        for action in ["FAIL", "ABORT"] {
            for timing in ["BEFORE", "AFTER"] {
                let trigger = format!(
                    "CREATE TRIGGER td {timing} DELETE ON t BEGIN \
                     INSERT INTO log VALUES(OLD.k); \
                     SELECT CASE WHEN OLD.k = 2 THEN RAISE({action}, 'stop') END; END"
                );
                let setup = [
                    "CREATE TABLE t(k INTEGER PRIMARY KEY)",
                    "CREATE TABLE log(k)",
                    "INSERT INTO t VALUES(1), (2), (3)",
                    trigger.as_str(),
                    "BEGIN",
                    "INSERT INTO t VALUES(4)",
                    "-- expect-error: DELETE FROM t WHERE k <= 3",
                ];
                compare(
                    &setup,
                    "SELECT 'target', k FROM t UNION ALL SELECT 'log', k FROM log ORDER BY 1, 2",
                )
                .await;

            }
        }
    });
}
