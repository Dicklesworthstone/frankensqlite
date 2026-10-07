//! GH#435: a multi-row `INSERT OR REPLACE ... VALUES (...), (...)` reports the
//! number of rows it wrote as its change count, as stock SQLite does, whatever
//! the table's shape and whether foreign keys are enforced.
#![recursion_limit = "512"]

use fsqlite_core::connection::Connection;
use fsqlite_types::value::SqliteValue;

const SCHEMA: &str = "
    CREATE TABLE runs (run_id TEXT PRIMARY KEY NOT NULL);
    CREATE TABLE interactions (
      run_id TEXT NOT NULL, tick INTEGER NOT NULL CHECK (tick >= 0),
      seq INTEGER NOT NULL CHECK (seq >= 0), actor_agent_uid INTEGER, target_agent_uid INTEGER,
      kind TEXT NOT NULL CHECK (kind <> ''), value REAL, payload_json TEXT NOT NULL,
      island_id INTEGER NOT NULL,
      PRIMARY KEY (run_id, tick, seq), FOREIGN KEY (run_id) REFERENCES runs (run_id));
    CREATE INDEX i_actor ON interactions (run_id, actor_agent_uid, tick, seq);
    CREATE INDEX i_target ON interactions (run_id, target_agent_uid, tick, seq);
    INSERT INTO runs VALUES ('r');
";

fn insert_sql(rows: usize) -> String {
    let mut sql = String::from(
        "INSERT OR REPLACE INTO interactions (run_id, tick, seq, actor_agent_uid, \
         target_agent_uid, kind, value, payload_json, island_id) VALUES ",
    );
    for row in 0..rows {
        if row > 0 {
            sql.push_str(", ");
        }
        let base = row * 9;
        let params: Vec<String> = (1..=9).map(|i| format!("?{}", base + i)).collect();
        sql.push_str(&format!("({})", params.join(", ")));
    }
    sql
}

fn params(rows: usize, tick: i64) -> Vec<SqliteValue> {
    (0..rows)
        .flat_map(|i| {
            let i = i64::try_from(i).expect("row index");
            [
                SqliteValue::Text("r".into()),
                SqliteValue::Integer(tick),
                SqliteValue::Integer(i),
                SqliteValue::Integer(i % 60),
                SqliteValue::Integer((i * 7) % 60),
                SqliteValue::Text("contact".into()),
                SqliteValue::Float(0.5),
                SqliteValue::Text("{\"d\":1}".into()),
                SqliteValue::Integer(0),
            ]
        })
        .collect()
}

fn integer(value: &SqliteValue) -> usize {
    match value {
        SqliteValue::Integer(n) => usize::try_from(*n).expect("non-negative"),
        other => panic!("expected an integer, got {other:?}"),
    }
}

#[test]
fn multirow_insert_or_replace_counts_every_row() {
    asupersync::test_utils::run_test(|| async {
        for foreign_keys in ["OFF", "ON"] {
            for rows in [1_usize, 63, 64, 65, 113, 129] {
                let conn = Connection::open(":memory:").await.expect("open");
                conn.execute(&format!("PRAGMA foreign_keys = {foreign_keys}"))
                    .await
                    .expect("foreign_keys");
                conn.execute_batch(SCHEMA).await.expect("schema");
                let sql = insert_sql(rows);
                let fresh = conn
                    .execute_with_params(&sql, &params(rows, 0))
                    .await
                    .expect("insert");
                // Replacing every row writes every row again.
                let replaced = conn
                    .execute_with_params(&sql, &params(rows, 0))
                    .await
                    .expect("replace");
                let sql_changes = conn.query("SELECT changes()").await.expect("changes()");
                let sql_changes = integer(&sql_changes[0].values()[0]);
                let stored = conn
                    .query("SELECT count(*) FROM interactions")
                    .await
                    .expect("count");
                let stored = integer(&stored[0].values()[0]);
                assert_eq!(
                    (fresh, replaced, sql_changes, stored),
                    (rows, rows, rows, rows),
                    "foreign_keys={foreign_keys} rows={rows}"
                );
                conn.close().await.expect("close");
            }
        }
    });
}

// GH#495 reported the same 100 -> 36 symptom on v0.4.6, using a file-backed
// 13-column table, explicit transactions, and 1,300 numbered bindings. Keep
// this shape alongside GH#435: neither the returned count nor count(*) alone
// proves that REPLACE preserved the rows and SQLite's change accounting.
const EVENT_SCHEMA: &str = "
    PRAGMA foreign_keys = OFF;
    CREATE TABLE runs (run_id TEXT PRIMARY KEY);
    CREATE TABLE ev (
      run_id TEXT, tick INT, seq INT, agent_uid INT, scope TEXT,
      event_type TEXT, payload TEXT, px REAL, py REAL, cu INT,
      cx REAL, cy REAL, island INT,
      PRIMARY KEY (run_id, island, tick, seq),
      FOREIGN KEY (run_id) REFERENCES runs(run_id));
    CREATE INDEX ev_agent ON ev (run_id, agent_uid, tick, seq);
    INSERT INTO runs VALUES ('r');
";

fn event_sql(rows: usize, replace: bool, returning: bool) -> String {
    let action = if replace { "OR REPLACE " } else { "" };
    let values = (0..rows)
        .map(|row| {
            let bindings = (1..=13)
                .map(|column| format!("?{}", row * 13 + column))
                .collect::<Vec<_>>()
                .join(",");
            format!("({bindings})")
        })
        .collect::<Vec<_>>()
        .join(",");
    let suffix = if returning { " RETURNING seq, payload" } else { "" };
    format!("INSERT {action}INTO ev (run_id,tick,seq,agent_uid,scope,event_type,payload,px,py,cu,cx,cy,island) VALUES {values}{suffix}")
}

fn event_params(rows: usize, tick: i64, offset: usize, version: usize) -> Vec<SqliteValue> {
    (offset..offset + rows)
        .flat_map(|seq| {
            let seq = i64::try_from(seq).expect("seq fits");
            [
                SqliteValue::Text("r".into()),
                SqliteValue::Integer(tick),
                SqliteValue::Integer(seq),
                SqliteValue::Integer(seq % 60),
                SqliteValue::Text("world".into()),
                SqliteValue::Text("move".into()),
                SqliteValue::Text(format!("version={version};seq={seq}").into()),
                SqliteValue::Float(0.5),
                SqliteValue::Float(1.5),
                SqliteValue::Integer(7),
                SqliteValue::Float(2.5),
                SqliteValue::Float(3.5),
                SqliteValue::Integer(0),
            ]
        })
        .collect()
}

fn stock_value(value: &SqliteValue) -> rusqlite::types::Value {
    match value {
        SqliteValue::Null => rusqlite::types::Value::Null,
        SqliteValue::Integer(value) => rusqlite::types::Value::Integer(*value),
        SqliteValue::Float(value) => rusqlite::types::Value::Real(*value),
        SqliteValue::Text(value) => rusqlite::types::Value::Text(value.to_string()),
        SqliteValue::Blob(value) => rusqlite::types::Value::Blob(value.to_vec()),
    }
}

fn stock_rows(
    stock: &rusqlite::Connection,
    sql: &str,
    params: &[SqliteValue],
) -> Vec<Vec<SqliteValue>> {
    let mut statement = stock.prepare(sql).expect("stock prepare");
    let columns = statement.column_count();
    statement
        .query_map(rusqlite::params_from_iter(params.iter().map(stock_value)), |row| {
            (0..columns)
                .map(|column| {
                    row.get::<_, rusqlite::types::Value>(column).map(|value| match value {
                        rusqlite::types::Value::Null => SqliteValue::Null,
                        rusqlite::types::Value::Integer(value) => SqliteValue::Integer(value),
                        rusqlite::types::Value::Real(value) => SqliteValue::Float(value),
                        rusqlite::types::Value::Text(value) => SqliteValue::Text(value.into()),
                        rusqlite::types::Value::Blob(value) => SqliteValue::Blob(value.into()),
                    })
                })
                .collect()
        })
        .expect("stock query")
        .collect::<rusqlite::Result<Vec<_>>>()
        .expect("stock rows")
}

async fn assert_event_state(conn: &Connection, stock: &rusqlite::Connection, label: &str) {
    for sql in [
        "SELECT changes(), total_changes(), (SELECT count(*) FROM ev)",
        "SELECT * FROM ev ORDER BY run_id,island,tick,seq",
        "SELECT run_id,agent_uid,tick,seq FROM ev INDEXED BY ev_agent ORDER BY run_id,agent_uid,tick,seq",
    ] {
        let actual = conn.query(sql).await.expect("FrankenSQLite state");
        let actual = actual.iter().map(|row| row.values().to_vec()).collect::<Vec<_>>();
        assert_eq!(actual, stock_rows(stock, sql, &[]), "{label}: {sql}");
    }
}

async fn event_batch(
    conn: &Connection,
    stock: &rusqlite::Connection,
    sql: &str,
    params: &[SqliteValue],
    returning: bool,
    label: &str,
) {
    conn.execute("BEGIN").await.expect("begin");
    stock.execute_batch("BEGIN").expect("stock begin");
    if returning {
        let actual = conn.query_with_params(sql, params).await.expect("returning");
        let actual = actual.iter().map(|row| row.values().to_vec()).collect::<Vec<_>>();
        assert_eq!(actual, stock_rows(stock, sql, params), "{label}: RETURNING");
        assert_eq!(actual.len(), params.len() / 13, "{label}: returned rows");
    } else {
        let actual = conn.execute_with_params(sql, params).await.expect("insert");
        let expected = stock
            .execute(sql, rusqlite::params_from_iter(params.iter().map(stock_value)))
            .expect("stock insert");
        assert_eq!(actual, expected, "{label}: affected rows");
        assert_eq!(actual, params.len() / 13, "{label}: input rows");
    }
    assert_event_state(conn, stock, label).await;
    conn.execute("COMMIT").await.expect("commit");
    stock.execute_batch("COMMIT").expect("stock commit");
    assert_event_state(conn, stock, label).await;
}

#[test]
fn gh495_event_counts_match_sqlite_at_morsel_boundaries() {
    asupersync::test_utils::run_test(|| async {
        for file_backed in [false, true] {
            for rows in [1_usize, 63, 64, 65, 100, 127, 128, 129] {
                for returning in [false, true] {
                    let directory = tempfile::tempdir().expect("directory");
                    let path = directory.path().join("events.db");
                    let database = if file_backed { path.to_str().expect("path") } else { ":memory:" };
                    let conn = Connection::open(database).await.expect("open");
                    let stock = if file_backed {
                        rusqlite::Connection::open(directory.path().join("stock.db")).expect("stock file")
                    } else {
                        rusqlite::Connection::open_in_memory().expect("stock memory")
                    };
                    conn.execute_batch(EVENT_SCHEMA).await.expect("schema");
                    stock.execute_batch(EVENT_SCHEMA).expect("stock schema");
                    // Fresh plain INSERT, fresh REPLACE, full replacement with
                    // changed payloads, then a mixture of replaced and new keys.
                    for (phase, replace, tick, offset) in [
                        (0, false, 1, 0),
                        (1, true, 0, 0),
                        (2, true, 0, 0),
                        (3, true, 0, rows / 2),
                    ] {
                        let label = format!("file={file_backed} rows={rows} returning={returning} phase={phase}");
                        event_batch(&conn, &stock, &event_sql(rows, replace, returning),
                            &event_params(rows, tick, offset, phase), returning, &label).await;
                    }
                    conn.close().await.expect("close");
                    if file_backed {
                        let reopened = Connection::open(database).await.expect("reopen");
                        let sql = "SELECT * FROM ev ORDER BY run_id,island,tick,seq";
                        let actual = reopened.query(sql).await.expect("persisted rows");
                        let actual = actual.iter().map(|row| row.values().to_vec()).collect::<Vec<_>>();
                        assert_eq!(actual, stock_rows(&stock, sql, &[]), "persisted rows");
                        reopened.close().await.expect("close reopened");
                    }
                }
            }
        }
    });
}

#[test]
fn gh495_thirty_file_backed_batches_report_one_hundred_changes_each() {
    asupersync::test_utils::run_test(|| async {
        let directory = tempfile::tempdir().expect("directory");
        let path = directory.path().join("events.db");
        let conn = Connection::open(path.to_str().expect("path")).await.expect("open");
        let stock = rusqlite::Connection::open(directory.path().join("stock.db")).expect("stock");
        conn.execute_batch(EVENT_SCHEMA).await.expect("schema");
        stock.execute_batch(EVENT_SCHEMA).expect("stock schema");
        let sql = event_sql(100, true, false);
        for batch in 0..30 {
            event_batch(&conn, &stock, &sql, &event_params(100, batch, 0, 0), false,
                &format!("batch={batch}")).await;
        }
        let rows = conn.query("SELECT count(*) FROM ev").await.expect("count");
        assert_eq!(integer(&rows[0].values()[0]), 3000);
        conn.close().await.expect("close");
    });
}
