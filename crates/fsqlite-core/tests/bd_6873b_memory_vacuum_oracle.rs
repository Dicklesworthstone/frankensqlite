#![recursion_limit = "512"]

//! bd-6873b — Stock SQLite 3.54.1 differential oracle for in-place `VACUUM`
//! on `:memory:` databases.
//!
//! Previously, `execute_vacuum_main` treated `self.path == ":memory:"` without
//! `INTO` as a silent `Ok(())` no-op, leaving `PRAGMA page_count` and
//! `PRAGMA freelist_count` completely uncompacted (e.g. 53/51 where stock
//! compacts to 2/0). These tests compare `:memory:` `VACUUM` directly against
//! live stock `rusqlite` across:
//! 1. Full DELETE + VACUUM compaction (`page_count == 2`, `freelist_count == 0`).
//! 2. Partial DELETE with secondary/expression/partial indexes, WITHOUT ROWID
//!    tables, overflow payloads, views, triggers, and AUTOINCREMENT sequence
//!    preservation + post-VACUUM DML/SELECT integrity.
//! 3. Drop-all-tables + VACUUM down to 1 page (`page_count == 1`, `freelist == 0`).
//! 4. Pending `PRAGMA auto_vacuum = FULL / INCREMENTAL` transitions via `VACUUM`
//!    on `:memory:` followed by live `auto_vacuum = FULL` shrinkage on DELETE.
//! 5. Negative controls: `VACUUM` inside an active transaction still fails with
//!    `VacuumWithinTransaction`, and `VACUUM INTO '<file>'` from `:memory:`
//!    still writes a stock-valid standalone file.

use fsqlite_core::connection::Connection;
use fsqlite_error::FrankenError;
use fsqlite_types::value::SqliteValue;

async fn franken_i64(conn: &Connection, sql: &str) -> i64 {
    let rows = conn.query(sql).await.unwrap();
    match &rows[0].values()[0] {
        SqliteValue::Integer(n) => *n,
        other => panic!("expected integer from `{sql}`, got {other:?}"),
    }
}

fn rusqlite_i64(conn: &rusqlite::Connection, sql: &str) -> i64 {
    conn.query_row(sql, [], |row| row.get::<_, i64>(0)).unwrap()
}

async fn franken_text(conn: &Connection, sql: &str) -> String {
    let rows = conn.query(sql).await.unwrap();
    match &rows[0].values()[0] {
        SqliteValue::Text(s) => s.to_string(),
        other => panic!("expected text from `{sql}`, got {other:?}"),
    }
}

async fn franken_dump(conn: &Connection, sql: &str) -> Vec<Vec<SqliteValue>> {
    conn.query(sql)
        .await
        .unwrap()
        .iter()
        .map(|row| row.values().to_vec())
        .collect()
}

fn rusqlite_dump(conn: &rusqlite::Connection, sql: &str, ncols: usize) -> Vec<Vec<SqliteValue>> {
    let mut stmt = conn.prepare(sql).unwrap();
    stmt.query_map([], |row| {
        let mut out = Vec::with_capacity(ncols);
        for i in 0..ncols {
            let val = match row.get_ref(i).unwrap() {
                rusqlite::types::ValueRef::Null => SqliteValue::Null,
                rusqlite::types::ValueRef::Integer(n) => SqliteValue::Integer(n),
                rusqlite::types::ValueRef::Real(x) => SqliteValue::Float(x),
                rusqlite::types::ValueRef::Text(t) => {
                    SqliteValue::Text(String::from_utf8_lossy(t).into_owned().into())
                }
                rusqlite::types::ValueRef::Blob(b) => SqliteValue::Blob(b.into()),
            };
            out.push(val);
        }
        Ok(out)
    })
    .unwrap()
    .map(Result::unwrap)
    .collect()
}

#[test]
fn bd_6873b_memory_vacuum_compacts_freelist_to_match_stock() {
    asupersync::test_utils::run_test(|| async {
        let f = Connection::open(":memory:").await.unwrap();
        let r = rusqlite::Connection::open_in_memory().unwrap();

        // Brand-new empty :memory: DB must accept VACUUM cleanly too.
        f.execute("VACUUM;").await.unwrap();
        r.execute("VACUUM;", []).unwrap();
        assert_eq!(franken_text(&f, "PRAGMA integrity_check;").await, "ok");

        let setup = [
            "CREATE TABLE t (id INTEGER PRIMARY KEY, pad TEXT);",
            "BEGIN;",
            "WITH RECURSIVE cnt(x) AS (
                SELECT 1 UNION ALL SELECT x + 1 FROM cnt WHERE x < 400
            )
            INSERT INTO t(id, pad)
            SELECT x, printf('%0500d', x) FROM cnt;",
            "COMMIT;",
            "DELETE FROM t;",
        ];
        for sql in setup {
            f.execute(sql).await.unwrap();
            r.execute(sql, []).unwrap();
        }

        let f_pages_before = franken_i64(&f, "PRAGMA page_count;").await;
        let f_free_before = franken_i64(&f, "PRAGMA freelist_count;").await;
        let r_pages_before = rusqlite_i64(&r, "PRAGMA page_count;");
        let r_free_before = rusqlite_i64(&r, "PRAGMA freelist_count;");
        assert!(
            f_pages_before >= 30 && f_free_before >= 25,
            "precondition: :memory: DB must have bloated pages and freelist before VACUUM (got pages={f_pages_before}, free={f_free_before}; stock pages={r_pages_before}, free={r_free_before})"
        );

        f.execute("VACUUM;").await.unwrap();
        r.execute("VACUUM;", []).unwrap();

        let f_pages_after = franken_i64(&f, "PRAGMA page_count;").await;
        let f_free_after = franken_i64(&f, "PRAGMA freelist_count;").await;
        let r_pages_after = rusqlite_i64(&r, "PRAGMA page_count;");
        let r_free_after = rusqlite_i64(&r, "PRAGMA freelist_count;");

        assert_eq!(
            (f_pages_after, f_free_after),
            (r_pages_after, r_free_after),
            ":memory: VACUUM must compact page_count/freelist_count identically to stock"
        );
        assert_eq!(f_pages_after, 2);
        assert_eq!(f_free_after, 0);
        assert_eq!(franken_text(&f, "PRAGMA integrity_check;").await, "ok");

        // Post-VACUUM writes and reads must work cleanly on the compacted image.
        f.execute("INSERT INTO t VALUES (1, 'after-vacuum');")
            .await
            .unwrap();
        r.execute("INSERT INTO t VALUES (1, 'after-vacuum');", [])
            .unwrap();
        assert_eq!(
            franken_dump(&f, "SELECT id, pad FROM t ORDER BY id;").await,
            rusqlite_dump(&r, "SELECT id, pad FROM t ORDER BY id;", 2)
        );
        assert_eq!(franken_text(&f, "PRAGMA integrity_check;").await, "ok");

        // Dropping the last table and running VACUUM compacts down to page 1.
        f.execute("DROP TABLE t;").await.unwrap();
        r.execute("DROP TABLE t;", []).unwrap();
        f.execute("VACUUM;").await.unwrap();
        r.execute("VACUUM;", []).unwrap();
        assert_eq!(
            (
                franken_i64(&f, "PRAGMA page_count;").await,
                franken_i64(&f, "PRAGMA freelist_count;").await
            ),
            (
                rusqlite_i64(&r, "PRAGMA page_count;"),
                rusqlite_i64(&r, "PRAGMA freelist_count;")
            )
        );
        assert_eq!(franken_i64(&f, "PRAGMA page_count;").await, 1);
        assert_eq!(franken_text(&f, "PRAGMA integrity_check;").await, "ok");

        f.close().await.unwrap();
    });
}

#[test]
fn bd_6873b_memory_vacuum_preserves_rich_schema_indexes_triggers_and_overflows() {
    asupersync::test_utils::run_test(|| async {
        let f = Connection::open(":memory:").await.unwrap();
        let r = rusqlite::Connection::open_in_memory().unwrap();

        let setup = [
            "CREATE TABLE items (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                code TEXT NOT NULL,
                active INTEGER NOT NULL,
                body TEXT NOT NULL
            );",
            "CREATE INDEX idx_items_code ON items(code);",
            "CREATE UNIQUE INDEX idx_items_lower_code ON items(lower(code)) WHERE active = 1;",
            "CREATE TABLE kv (
                k TEXT PRIMARY KEY,
                v TEXT NOT NULL
            ) WITHOUT ROWID;",
            "CREATE TABLE audit (msg TEXT);",
            "CREATE VIEW active_items AS SELECT id, code, length(body) AS body_len FROM items WHERE active = 1;",
            "CREATE TRIGGER items_ai AFTER INSERT ON items BEGIN
                INSERT INTO audit VALUES ('ins:' || NEW.id || ':' || NEW.code);
            END;",
            "WITH RECURSIVE cnt(x) AS (
                SELECT 1 UNION ALL SELECT x + 1 FROM cnt WHERE x < 120
            )
            INSERT INTO items(code, active, body)
            SELECT 'Code_' || x, CASE WHEN x % 2 = 0 THEN 1 ELSE 0 END,
                   CASE WHEN x % 15 = 0 THEN replace(hex(zeroblob(3000)), '00', 'ab')
                        ELSE printf('%0400d', x) END
            FROM cnt;",
            "WITH RECURSIVE cnt(x) AS (
                SELECT 1 UNION ALL SELECT x + 1 FROM cnt WHERE x < 120
            )
            INSERT INTO kv(k, v) SELECT 'k_' || printf('%04d', x), printf('%0400d', x) FROM cnt;",
            "DELETE FROM items WHERE id > 20;",
            "DELETE FROM kv WHERE k > 'k_0015';",
            "DELETE FROM audit;",
        ];
        for sql in setup {
            f.execute(sql).await.unwrap();
            r.execute(sql, []).unwrap();
        }

        let pages_before = franken_i64(&f, "PRAGMA page_count;").await;
        let free_before = franken_i64(&f, "PRAGMA freelist_count;").await;
        assert!(
            free_before >= 20,
            "expected substantial freelist before VACUUM, got pages={pages_before}, free={free_before}"
        );

        f.execute("VACUUM;").await.unwrap();
        r.execute("VACUUM;", []).unwrap();

        let pages_after = franken_i64(&f, "PRAGMA page_count;").await;
        let free_after = franken_i64(&f, "PRAGMA freelist_count;").await;
        assert_eq!(free_after, 0, "freelist must be 0 after :memory: VACUUM");
        assert!(
            pages_after < pages_before / 2,
            ":memory: VACUUM must reclaim freed pages (before={pages_before}, after={pages_after})"
        );
        assert_eq!(franken_text(&f, "PRAGMA integrity_check;").await, "ok");

        // Verify all tables, indexes, views, sqlite_sequence, and triggers match stock.
        for (sql, ncols) in [
            ("SELECT id, code, active, body FROM items ORDER BY id;", 4),
            ("SELECT id, code, body_len FROM active_items ORDER BY id;", 3),
            ("SELECT k, v FROM kv ORDER BY k;", 2),
            ("SELECT name, seq FROM sqlite_sequence ORDER BY name;", 2),
        ] {
            assert_eq!(
                franken_dump(&f, sql).await,
                rusqlite_dump(&r, sql, ncols),
                "post-VACUUM mismatch for `{sql}`"
            );
        }

        // Verify AUTOINCREMENT high-water and AFTER INSERT trigger still fire after VACUUM.
        let post_insert = "INSERT INTO items(code, active, body) VALUES ('PostVac', 1, 'ok');";
        f.execute(post_insert).await.unwrap();
        r.execute(post_insert, []).unwrap();
        assert_eq!(
            franken_dump(&f, "SELECT msg FROM audit ORDER BY rowid;").await,
            rusqlite_dump(&r, "SELECT msg FROM audit ORDER BY rowid;", 1),
            "trigger + AUTOINCREMENT high-water must survive :memory: VACUUM"
        );
        assert_eq!(franken_text(&f, "PRAGMA integrity_check;").await, "ok");

        f.close().await.unwrap();
    });
}

#[test]
fn bd_6873b_memory_vacuum_auto_vacuum_mode_transition_and_negative_controls() {
    asupersync::test_utils::run_test(|| async {
        let f = Connection::open(":memory:").await.unwrap();
        let r = rusqlite::Connection::open_in_memory().unwrap();

        for sql in [
            "CREATE TABLE t (id INTEGER PRIMARY KEY, pad TEXT);",
            "WITH RECURSIVE cnt(x) AS (
                SELECT 1 UNION ALL SELECT x + 1 FROM cnt WHERE x < 200
            )
            INSERT INTO t(id, pad) SELECT x, printf('%0400d', x) FROM cnt;",
            "PRAGMA auto_vacuum = FULL;",
            "VACUUM;",
        ] {
            f.execute(sql).await.unwrap();
            r.execute(sql, []).unwrap();
        }

        assert_eq!(
            franken_i64(&f, "PRAGMA auto_vacuum;").await,
            rusqlite_i64(&r, "PRAGMA auto_vacuum;"),
            "VACUUM on :memory: must apply pending PRAGMA auto_vacuum = FULL"
        );
        assert_eq!(franken_i64(&f, "PRAGMA freelist_count;").await, 0);
        assert_eq!(franken_text(&f, "PRAGMA integrity_check;").await, "ok");

        // Deleting rows under auto_vacuum = FULL on :memory: must now auto-shrink!
        let peak_pages = franken_i64(&f, "PRAGMA page_count;").await;
        f.execute("DELETE FROM t WHERE id > 5;").await.unwrap();
        r.execute("DELETE FROM t WHERE id > 5;", []).unwrap();
        let shrunk_pages = franken_i64(&f, "PRAGMA page_count;").await;
        assert!(
            shrunk_pages < peak_pages / 2,
            "auto_vacuum=FULL enabled via :memory: VACUUM must shrink on DELETE (peak={peak_pages}, shrunk={shrunk_pages})"
        );
        assert_eq!(franken_i64(&f, "PRAGMA freelist_count;").await, 0);
        assert_eq!(franken_text(&f, "PRAGMA integrity_check;").await, "ok");

        // Negative control 1: VACUUM inside an explicit transaction MUST fail.
        f.execute("BEGIN;").await.unwrap();
        let in_txn_err = f.execute("VACUUM;").await.unwrap_err();
        assert!(
            matches!(in_txn_err, FrankenError::VacuumWithinTransaction),
            "VACUUM inside transaction must fail with VacuumWithinTransaction, got {in_txn_err:?}"
        );
        f.execute("ROLLBACK;").await.unwrap();

        // Negative control 2: VACUUM INTO '<file>' from :memory: writes a valid stock file.
        let dir = tempfile::tempdir().unwrap();
        let out_path = dir.path().join("from_memory.db");
        let into_sql = format!("VACUUM INTO '{}';", out_path.display());
        f.execute(&into_sql).await.unwrap();
        let stock_check = rusqlite::Connection::open(&out_path).unwrap();
        let ic: String = stock_check
            .query_row("PRAGMA integrity_check;", [], |row| row.get(0))
            .unwrap();
        assert_eq!(ic, "ok");
        assert_eq!(
            rusqlite_dump(&stock_check, "SELECT id, pad FROM t ORDER BY id;", 2),
            franken_dump(&f, "SELECT id, pad FROM t ORDER BY id;").await
        );

        f.close().await.unwrap();
    });
}
