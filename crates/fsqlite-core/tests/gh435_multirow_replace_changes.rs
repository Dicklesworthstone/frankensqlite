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
