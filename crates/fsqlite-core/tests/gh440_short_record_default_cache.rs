#![recursion_limit = "512"]

//! GH#440: `PRAGMA foreign_key_check` ran 4-20x slower than stock on a small
//! schema and ~565x on a large one. Every table-backed statement execution
//! rebuilt the map of short-record column defaults (the values a row written
//! before `ALTER TABLE ADD COLUMN` reads for the new column) for EVERY table
//! in the schema, re-parsing each non-literal DEFAULT through the SQL parser.
//!
//! The map is a pure function of the schema, so it is now built once per
//! schema generation, with stock's `sqlite3ValueFromExpr` rule: literals,
//! signed literals and CAST of literals produce a value; functions, CURRENT_*
//! and other expressions read as NULL.
//!
//! rusqlite (bundled stock SQLite) is the oracle. The pass-count keeper lives
//! in this binary because the hot-path profile counters are process-global.

use fsqlite_core::connection::{
    Connection, hot_path_profile_snapshot, reset_hot_path_profile, set_hot_path_profile_enabled,
};
use fsqlite_types::value::SqliteValue;

/// The profile counters are process-global: serialize this binary's tests so
/// the pass-count keeper sees only its own work.
static SERIAL: std::sync::Mutex<()> = std::sync::Mutex::new(());

fn serial() -> std::sync::MutexGuard<'static, ()> {
    SERIAL
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

fn tag_f(v: &SqliteValue) -> String {
    match v {
        SqliteValue::Null => "NULL".to_owned(),
        SqliteValue::Integer(n) => format!("int:{n}"),
        SqliteValue::Float(f) => format!("real:{f}"),
        SqliteValue::Text(s) => format!("text:{s}"),
        SqliteValue::Blob(b) => format!(
            "blob:{}",
            b.iter().map(|x| format!("{x:02X}")).collect::<String>()
        ),
    }
}

fn tag_r(v: &rusqlite::types::Value) -> String {
    match v {
        rusqlite::types::Value::Null => "NULL".to_owned(),
        rusqlite::types::Value::Integer(n) => format!("int:{n}"),
        rusqlite::types::Value::Real(f) => format!("real:{f}"),
        rusqlite::types::Value::Text(s) => format!("text:{s}"),
        rusqlite::types::Value::Blob(b) => format!(
            "blob:{}",
            b.iter().map(|x| format!("{x:02X}")).collect::<String>()
        ),
    }
}

async fn fsqlite_rows(conn: &Connection, sql: &str) -> Vec<Vec<String>> {
    conn.query(sql)
        .await
        .unwrap_or_else(|e| panic!("fsqlite `{sql}`: {e:?}"))
        .iter()
        .map(|r| r.values().iter().map(tag_f).collect())
        .collect()
}

fn stock_rows(conn: &rusqlite::Connection, sql: &str) -> Vec<Vec<String>> {
    let mut st = conn.prepare(sql).unwrap();
    let n = st.column_count();
    st.query_map([], |row| {
        Ok((0..n)
            .map(|i| tag_r(&row.get_unwrap::<_, rusqlite::types::Value>(i)))
            .collect())
    })
    .unwrap()
    .collect::<Result<Vec<_>, _>>()
    .unwrap()
}

async fn assert_agree(f: &Connection, r: &rusqlite::Connection, sql: &str) {
    assert_eq!(
        fsqlite_rows(f, sql).await,
        stock_rows(r, sql),
        "GH#440 short-record default mismatch on `{sql}`"
    );
}

/// Rows written before the schema grew columns read each column's DEFAULT
/// exactly as stock does, through every read shape.
#[test]
fn gh440_short_record_defaults_match_stock_value_from_expr() {
    let _serial = serial();
    asupersync::test_utils::run_test(|| async {
        let dir = tempfile::tempdir().unwrap();
        let stock_path = dir.path().join("stock.db");
        let frank_path = dir.path().join("frank.db");
        {
            let r = rusqlite::Connection::open(&stock_path).unwrap();
            r.set_db_config(rusqlite::config::DbConfig::SQLITE_DBCONFIG_DEFENSIVE, false)
                .unwrap();
            // Two-column records, then a schema that declares more columns:
            // the only way to get short records for non-literal defaults,
            // since ALTER refuses them on a non-empty table.
            r.execute_batch(
                "CREATE TABLE t(a, z);
                 INSERT INTO t VALUES (1, 'r1'), (2, 'r2'), (3, 'r3');
                 PRAGMA writable_schema = ON;
                 UPDATE sqlite_master SET sql = 'CREATE TABLE t(a, z,
                     b DEFAULT (lower(''X'')), c DEFAULT 5,
                     d DEFAULT CURRENT_TIMESTAMP, e DEFAULT -2.5,
                     f DEFAULT (''q''), g DEFAULT x''ab'', h DEFAULT TRUE,
                     i DEFAULT (1+1), j INTEGER DEFAULT ''42'',
                     k DEFAULT (CAST(''7'' AS INTEGER)), l DEFAULT NULL,
                     m TEXT DEFAULT 100, n DEFAULT (-7))' WHERE name = 't';
                 PRAGMA writable_schema = OFF;",
            )
            .unwrap();
        }
        std::fs::copy(&stock_path, &frank_path).unwrap();

        let r = rusqlite::Connection::open(&stock_path).unwrap();
        let f = Connection::open(frank_path.to_string_lossy().as_ref())
            .await
            .unwrap();
        for sql in [
            "SELECT * FROM t ORDER BY a",
            "SELECT a, b, d, i FROM t WHERE b IS NULL AND d IS NULL ORDER BY a",
            "SELECT typeof(j), typeof(m), typeof(h), typeof(e) FROM t ORDER BY a",
            "SELECT count(*), sum(c), sum(j), sum(n) FROM t",
            "SELECT a FROM t WHERE c = 5 AND m = '100' ORDER BY a",
        ] {
            assert_agree(&f, &r, sql).await;
        }
    });
}

/// The cached default map follows schema changes on the same connection:
/// ALTER TABLE ADD COLUMN, DROP + re-CREATE under the same name, and a
/// statement prepared before the change.
#[test]
fn gh440_cached_defaults_follow_schema_changes() {
    let _serial = serial();
    asupersync::test_utils::run_test(|| async {
        let dir = tempfile::tempdir().unwrap();
        let f = Connection::open(dir.path().join("frank.db").to_string_lossy().as_ref())
            .await
            .unwrap();
        let r = rusqlite::Connection::open(dir.path().join("stock.db")).unwrap();
        let run = async |sql: &str| {
            f.execute(sql)
                .await
                .unwrap_or_else(|e| panic!("{sql}: {e:?}"));
            r.execute_batch(sql).unwrap();
        };

        run("CREATE TABLE t(a)").await;
        run("INSERT INTO t VALUES (1), (2), (3)").await;
        let prepared = f.prepare("SELECT * FROM t ORDER BY a").await.unwrap();
        run("ALTER TABLE t ADD COLUMN b DEFAULT 7").await;
        assert_agree(&f, &r, "SELECT * FROM t ORDER BY a").await;
        assert_eq!(
            prepared
                .query()
                .await
                .unwrap()
                .iter()
                .map(|row| row.values().iter().map(tag_f).collect::<Vec<_>>())
                .collect::<Vec<_>>(),
            stock_rows(&r, "SELECT * FROM t ORDER BY a"),
            "a statement prepared before ALTER must read the new default"
        );
        run("ALTER TABLE t ADD COLUMN c TEXT DEFAULT 100").await;
        assert_agree(&f, &r, "SELECT a, b, c, typeof(c) FROM t ORDER BY a").await;
        run("INSERT INTO t(a) VALUES (4)").await;
        assert_agree(&f, &r, "SELECT * FROM t ORDER BY a").await;

        run("DROP TABLE t").await;
        run("CREATE TABLE t(a)").await;
        run("INSERT INTO t VALUES (10), (20)").await;
        run("ALTER TABLE t ADD COLUMN b INTEGER DEFAULT '-3'").await;
        assert_agree(&f, &r, "SELECT a, b, typeof(b) FROM t ORDER BY a").await;
        assert_agree(&f, &r, "SELECT sum(b) FROM t WHERE b < 0").await;
    });
}

/// The complexity property behind GH#440: building short-record defaults is
/// per schema generation, not per statement execution. Before the fix every
/// execution against a schema with any DEFAULT paid one pass over every
/// table's defaults (so passes >= executions); now the count is bounded by
/// the number of tables with defaults.
#[test]
fn gh440_default_map_is_not_rebuilt_per_execution() {
    let _serial = serial();
    asupersync::test_utils::run_test(|| async {
        const TABLES: usize = 6;
        const EXECUTIONS: usize = 300;
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("fk.db").to_string_lossy().to_string();
        {
            let f = Connection::open(&path).await.unwrap();
            for i in 0..TABLES {
                f.execute(&format!(
                    "CREATE TABLE parent_{i}(id INTEGER PRIMARY KEY, code TEXT,
                         created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')))"
                ))
                .await
                .unwrap();
                f.execute(&format!(
                    "CREATE TABLE child_{i}(id INTEGER PRIMARY KEY,
                         parent_id INTEGER NOT NULL REFERENCES parent_{i}(id),
                         tag TEXT NOT NULL DEFAULT (lower('X')), n INTEGER DEFAULT 0)"
                ))
                .await
                .unwrap();
                f.execute(&format!(
                    "INSERT INTO parent_{i}(id, code) VALUES (1, 'k1')"
                ))
                .await
                .unwrap();
                for r in 0..20 {
                    f.execute(&format!(
                        "INSERT INTO child_{i}(id, parent_id) VALUES ({r}, 1)"
                    ))
                    .await
                    .unwrap();
                }
            }
        }

        let f = Connection::open(&path).await.unwrap();
        f.execute("PRAGMA foreign_keys = ON").await.unwrap();
        set_hot_path_profile_enabled(true);
        reset_hot_path_profile();
        let violations = f.query("PRAGMA foreign_key_check").await.unwrap();
        for r in 0..EXECUTIONS {
            let rows = f
                .query(&format!(
                    "SELECT tag, n FROM child_{} WHERE id = {}",
                    r % TABLES,
                    r % 20
                ))
                .await
                .unwrap();
            assert_eq!(rows.len(), 1);
        }
        let passes = hot_path_profile_snapshot().column_default_evaluation_passes;
        set_hot_path_profile_enabled(false);
        eprintln!("GH#440: {passes} column-default passes for {EXECUTIONS} executions");

        assert!(violations.is_empty(), "no FK violations expected");
        let tables_with_defaults = (2 * TABLES) as u64;
        assert!(
            passes <= tables_with_defaults,
            "GH#440: {passes} column-default passes for {EXECUTIONS} executions over \
             {tables_with_defaults} tables with defaults; the default map must be built \
             once per schema generation, not per execution"
        );
    });
}
