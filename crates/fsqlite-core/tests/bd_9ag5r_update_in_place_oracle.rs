#![recursion_limit = "512"]

//! bd-9ag5r (GH#439): an UPDATE that cannot change the rowid rewrites each row
//! in place — overwriting a same-size cell, or replacing it at its position —
//! instead of a delete, a re-seek and an insert per row. Pinned against stock
//! SQLite (rusqlite) on the outcome (ok / error) and the final table contents
//! of every statement:
//!
//! - same-size, growing, shrinking and storage-class-changing rewrites,
//!   NULL in and out, and overflow rows in every direction;
//! - INTEGER PRIMARY KEY tables updating other columns (in place) and the key
//!   itself (delete + insert), hidden-rowid rewrites, WITHOUT ROWID tables;
//! - UNIQUE conflicts under ABORT, OR IGNORE, OR REPLACE and OR FAIL, indexed
//!   WHERE scans over the updated column, triggers, RETURNING, foreign keys,
//!   changes()/total_changes();
//! - ROLLBACK and ROLLBACK TO undo in-place rewrites, and a file-backed
//!   database passes stock `integrity_check` after reopening.
//!
//! The EXPLAIN test pins which UPDATE shapes the codegen marks for the in-place
//! rewrite (`Delete` p5 = OPFLAG_ISUPDATE | OPFLAG_UPDATE_KEEPS_ROWID).

use fsqlite_core::connection::Connection;
use fsqlite_types::SqliteValue;
use rusqlite::types::Value;

fn value(value: Value) -> SqliteValue {
    match value {
        Value::Null => SqliteValue::Null,
        Value::Integer(value) => SqliteValue::Integer(value),
        Value::Real(value) => SqliteValue::Float(value),
        Value::Text(value) => SqliteValue::from(value.as_str()),
        Value::Blob(value) => SqliteValue::Blob(value.into()),
    }
}

async fn frank_rows(conn: &Connection, sql: &str) -> Vec<Vec<SqliteValue>> {
    conn.query(sql)
        .await
        .unwrap_or_else(|e| panic!("FrankenSQLite: `{sql}`: {e:?}"))
        .iter()
        .map(|row| row.values().to_vec())
        .collect()
}

fn stock_rows(conn: &rusqlite::Connection, sql: &str) -> Vec<Vec<SqliteValue>> {
    let mut stmt = conn
        .prepare(sql)
        .unwrap_or_else(|e| panic!("SQLite: `{sql}`: {e}"));
    let width = stmt.column_count();
    stmt.query_map([], |row| {
        (0..width)
            .map(|i| row.get::<_, Value>(i).map(value))
            .collect::<rusqlite::Result<Vec<_>>>()
    })
    .unwrap()
    .collect::<rusqlite::Result<Vec<_>>>()
    .unwrap()
}

/// Run `sql` on both engines; both must succeed or both must fail. Returns
/// whether it succeeded.
async fn run_both(frank: &Connection, stock: &rusqlite::Connection, sql: &str) -> bool {
    let frank_result = frank.execute_batch(sql).await;
    let stock_result = stock.execute_batch(sql);
    assert_eq!(
        frank_result.is_ok(),
        stock_result.is_ok(),
        "`{sql}`: FrankenSQLite {frank_result:?} vs SQLite {stock_result:?}"
    );
    stock_result.is_ok()
}

async fn compare(frank: &Connection, stock: &rusqlite::Connection, sql: &str) {
    assert_eq!(
        frank_rows(frank, sql).await,
        stock_rows(stock, sql),
        "`{sql}` differs from SQLite"
    );
}

/// Run each statement on both engines, then compare `check` after each one.
async fn script(frank: &Connection, stock: &rusqlite::Connection, check: &str, stmts: &[&str]) {
    const TOTAL: &str = "SELECT total_changes()";
    for sql in stmts {
        let frank_before = frank_rows(frank, TOTAL).await;
        let stock_before = stock_rows(stock, TOTAL);
        let ok = run_both(frank, stock, sql).await;
        assert_eq!(
            frank_rows(frank, check).await,
            stock_rows(stock, check),
            "after `{sql}`: `{check}` differs from SQLite"
        );
        // Counters are compared as a per-statement delta, and only for plain
        // statements that succeed. Two pre-existing differences, the same on
        // the delete + insert path, are out of scope here: after an aborted
        // UPDATE stock's total_changes() keeps the rows changed before the
        // conflict and fsqlite's does not, and UPDATE OR IGNORE adds its
        // skipped rows to fsqlite's total_changes().
        if ok && !sql.starts_with("UPDATE OR ") {
            let delta = |before: &[Vec<SqliteValue>], after: &[Vec<SqliteValue>]| {
                match (&before[0][0], &after[0][0]) {
                    (SqliteValue::Integer(b), SqliteValue::Integer(a)) => a - b,
                    other => panic!("total_changes(): {other:?}"),
                }
            };
            assert_eq!(
                delta(&frank_before, &frank_rows(frank, TOTAL).await),
                delta(&stock_before, &stock_rows(stock, TOTAL)),
                "after `{sql}`: total_changes() delta differs from SQLite"
            );
            assert_eq!(
                frank_rows(frank, "SELECT changes()").await,
                stock_rows(stock, "SELECT changes()"),
                "after `{sql}`: changes() differs from SQLite"
            );
        }
    }
}

const SEED_T: &str = "CREATE TABLE t(a, b TEXT, c BLOB);
     WITH RECURSIVE n(i) AS (SELECT 1 UNION ALL SELECT i + 1 FROM n WHERE i < 300)
     INSERT INTO t SELECT i, 'row' || i, zeroblob(i % 7) FROM n;";

#[test]
fn rowid_preserving_updates_match_stock() {
    asupersync::test_utils::run_test(|| async {
        let frank = Connection::open(":memory:").await.unwrap();
        let stock = rusqlite::Connection::open_in_memory().unwrap();
        run_both(&frank, &stock, SEED_T).await;
        let check = "SELECT rowid, a, typeof(a), b, typeof(b), hex(c) FROM t ORDER BY rowid";
        script(
            &frank,
            &stock,
            check,
            &[
                // Same size: small integers stay one-byte serial types.
                "UPDATE t SET a = a + 0",
                "UPDATE t SET a = a * 2 WHERE a < 60",
                // Growing and shrinking cells, storage-class changes.
                "UPDATE t SET a = a * 1000003",
                "UPDATE t SET b = b || b || b WHERE rowid % 3 = 0",
                "UPDATE t SET a = 'text' || a WHERE rowid % 5 = 0",
                "UPDATE t SET a = 1.5 * rowid WHERE rowid % 7 = 0",
                "UPDATE t SET b = substr(b, 1, 2)",
                "UPDATE t SET a = NULL WHERE rowid % 11 = 0",
                "UPDATE t SET a = rowid WHERE a IS NULL",
                "UPDATE t SET c = NULL WHERE rowid > 250",
                "UPDATE t SET c = x'00ff' WHERE c IS NULL",
                // Same text length, different bytes.
                "UPDATE t SET b = upper(b)",
                // No-op and empty WHERE.
                "UPDATE t SET a = a WHERE rowid = 1",
                "UPDATE t SET a = 0 WHERE rowid > 100000",
            ],
        )
        .await;
    });
}

#[test]
fn overflow_rows_rewrite_in_every_direction() {
    asupersync::test_utils::run_test(|| async {
        let frank = Connection::open(":memory:").await.unwrap();
        let stock = rusqlite::Connection::open_in_memory().unwrap();
        run_both(
            &frank,
            &stock,
            "CREATE TABLE big(k INTEGER PRIMARY KEY, v BLOB, w);
             WITH RECURSIVE n(i) AS (SELECT 1 UNION ALL SELECT i + 1 FROM n WHERE i < 40)
             INSERT INTO big SELECT i, x'', i FROM n;",
        )
        .await;
        let check = "SELECT k, length(v), hex(substr(v, 1, 8)), hex(substr(v, -8)), w FROM big ORDER BY k";
        script(
            &frank,
            &stock,
            check,
            &[
                // Small -> overflow (deterministic bytes so both engines agree).
                "UPDATE big SET v = zeroblob(9000) WHERE k % 2 = 0",
                "UPDATE big SET v = cast(replace(hex(zeroblob(3000)), '0', 'x') AS BLOB) WHERE k % 2 = 1",
                // Overflow -> same size, different bytes.
                "UPDATE big SET v = cast(replace(hex(zeroblob(4500)), '0', 'y') AS BLOB) WHERE k % 2 = 0",
                // Overflow row, update another column only.
                "UPDATE big SET w = w * 3",
                // Overflow -> larger overflow, and overflow -> small.
                "UPDATE big SET v = v || v WHERE k % 4 = 0",
                "UPDATE big SET v = x'abcd' WHERE k % 3 = 0",
            ],
        )
        .await;
    });
}

#[test]
fn ipk_hidden_rowid_and_without_rowid_updates_match_stock() {
    asupersync::test_utils::run_test(|| async {
        let frank = Connection::open(":memory:").await.unwrap();
        let stock = rusqlite::Connection::open_in_memory().unwrap();
        run_both(
            &frank,
            &stock,
            "CREATE TABLE p(id INTEGER PRIMARY KEY, v, w TEXT);
             WITH RECURSIVE n(i) AS (SELECT 1 UNION ALL SELECT i + 1 FROM n WHERE i < 50)
             INSERT INTO p SELECT i * 2, i, 'w' || i FROM n;
             CREATE TABLE h(v);
             INSERT INTO h VALUES (1), (2), (3), (4);
             CREATE TABLE wr(k TEXT PRIMARY KEY, v) WITHOUT ROWID;
             INSERT INTO wr VALUES ('a', 1), ('b', 2), ('c', 3);",
        )
        .await;
        script(
            &frank,
            &stock,
            "SELECT rowid, id, v, w FROM p ORDER BY id",
            &[
                // Non-key columns: rewritten in place.
                "UPDATE p SET v = v * 2, w = w || '!'",
                // The key itself: moves rows, conflicts on collision.
                "UPDATE p SET id = id + 1",
                "UPDATE p SET id = id + 1000 WHERE id > 50",
                "UPDATE p SET id = 3 WHERE id = 4",
                "UPDATE p SET rowid = rowid + 1 WHERE id > 1000",
                // A key assignment that keeps every row where it is.
                "UPDATE p SET id = id, v = v + 1",
            ],
        )
        .await;
        script(
            &frank,
            &stock,
            "SELECT rowid, v FROM h ORDER BY rowid",
            &[
                "UPDATE h SET v = v * 10",
                "UPDATE h SET rowid = rowid + 10 WHERE v > 20",
                "UPDATE h SET rowid = 1 WHERE rowid = 13",
            ],
        )
        .await;
        script(
            &frank,
            &stock,
            "SELECT k, v FROM wr ORDER BY k",
            &[
                "UPDATE wr SET v = v * 7",
                "UPDATE wr SET k = k || k WHERE v > 10",
            ],
        )
        .await;
    });
}

#[test]
fn unique_conflicts_triggers_returning_and_fks_match_stock() {
    asupersync::test_utils::run_test(|| async {
        let frank = Connection::open(":memory:").await.unwrap();
        let stock = rusqlite::Connection::open_in_memory().unwrap();
        run_both(
            &frank,
            &stock,
            "PRAGMA foreign_keys = ON;
             CREATE TABLE u(a, b UNIQUE, c);
             CREATE INDEX u_c ON u(c);
             WITH RECURSIVE n(i) AS (SELECT 1 UNION ALL SELECT i + 1 FROM n WHERE i < 20)
             INSERT INTO u SELECT i, i * 10, i % 4 FROM n;
             CREATE TABLE log(ev, old_b, new_b);
             CREATE TRIGGER u_au AFTER UPDATE ON u BEGIN
               INSERT INTO log VALUES ('au', old.b, new.b);
             END;
             CREATE TRIGGER u_bu BEFORE UPDATE OF c ON u WHEN new.c > 100 BEGIN
               INSERT INTO log VALUES ('bu', old.c, new.c);
             END;
             CREATE TABLE par(id INTEGER PRIMARY KEY, name);
             INSERT INTO par VALUES (1, 'one'), (2, 'two');
             CREATE TABLE kid(id INTEGER PRIMARY KEY, pid REFERENCES par(id), note);
             INSERT INTO kid VALUES (1, 1, 'x'), (2, 2, 'y'), (3, 1, 'z');",
        )
        .await;
        let check = "SELECT rowid, a, b, c FROM u ORDER BY rowid";
        script(
            &frank,
            &stock,
            check,
            &[
                // UNIQUE column rewritten without collisions (old keys freed first
                // only row by row, so a shift up must not collide mid-statement).
                "UPDATE u SET b = b + 1",
                // Indexed scan over the column being updated (Halloween shape).
                // These run before the OR IGNORE below: a skipped OR IGNORE row
                // currently leaves a duplicate entry in u_c (pre-existing, also
                // on the delete + insert path), which a later scan of u_c visits
                // twice.
                "UPDATE u SET c = c + 10 WHERE c >= 2",
                "UPDATE u SET c = c * 50 WHERE c > 1",
                "UPDATE u SET a = a * 2 WHERE b > 100",
                // Collision under ABORT leaves the table untouched.
                "UPDATE u SET b = 11",
            ],
        )
        .await;
        // The order a multi-row UPDATE visits rows (and so fires triggers) is
        // unspecified, and it differs from stock for index-driven scans.
        compare(&frank, &stock, "SELECT ev, old_b, new_b FROM log ORDER BY 1, 2, 3").await;
        // The trigger log is not compared after these: UPDATE OR IGNORE fires
        // AFTER UPDATE triggers for the rows it skips (pre-existing, also on the
        // delete + insert path).
        script(
            &frank,
            &stock,
            check,
            &[
                "UPDATE OR IGNORE u SET b = 21 WHERE a > 1",
                "UPDATE OR REPLACE u SET b = 31 WHERE a = 5",
                "UPDATE OR FAIL u SET b = 51 WHERE a > 3",
            ],
        )
        .await;
        compare(
            &frank,
            &stock,
            "UPDATE u SET a = a + 1 WHERE a < 8 RETURNING rowid, a, b",
        )
        .await;
        compare(&frank, &stock, check).await;
        script(
            &frank,
            &stock,
            "SELECT id, pid, note FROM kid ORDER BY id",
            &[
                "UPDATE kid SET note = note || note",
                "UPDATE par SET name = upper(name)",
                "UPDATE kid SET pid = 2 WHERE id = 3",
                "UPDATE kid SET pid = 99 WHERE id = 1",
            ],
        )
        .await;
        compare(&frank, &stock, "SELECT id, name FROM par ORDER BY id").await;
    });
}

#[test]
fn rollback_undoes_in_place_rewrites() {
    asupersync::test_utils::run_test(|| async {
        let frank = Connection::open(":memory:").await.unwrap();
        let stock = rusqlite::Connection::open_in_memory().unwrap();
        run_both(&frank, &stock, SEED_T).await;
        let check = "SELECT rowid, a, b, hex(c) FROM t ORDER BY rowid";
        script(
            &frank,
            &stock,
            check,
            &[
                "BEGIN",
                "UPDATE t SET a = a + 1",
                "SAVEPOINT s1",
                "UPDATE t SET a = a * 2, b = upper(b)",
                "ROLLBACK TO s1",
                "UPDATE t SET b = b || '#'",
                "ROLLBACK",
                "BEGIN",
                "UPDATE t SET a = -a",
                "COMMIT",
            ],
        )
        .await;
    });
}

#[test]
fn file_backed_in_place_updates_survive_reopen_and_pass_stock_integrity_check() {
    asupersync::test_utils::run_test(|| async {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("inplace.db");
        let path_str = path.to_str().unwrap().to_owned();
        let frank = Connection::open(&path_str).await.unwrap();
        let stock = rusqlite::Connection::open_in_memory().unwrap();
        let stmts = [
            SEED_T,
            "CREATE INDEX t_a ON t(a)",
            "UPDATE t SET a = a * 2",
            "UPDATE t SET b = b || b WHERE rowid % 2 = 0",
            "UPDATE t SET c = zeroblob(5000) WHERE rowid % 9 = 0",
            "UPDATE t SET a = a + 1",
        ];
        for sql in stmts {
            run_both(&frank, &stock, sql).await;
        }
        drop(frank);
        let check = "SELECT rowid, a, b, length(c) FROM t ORDER BY rowid";
        let reopened = Connection::open(&path_str).await.unwrap();
        compare(&reopened, &stock, check).await;
        compare(&reopened, &stock, "SELECT a FROM t INDEXED BY t_a WHERE a > 100 ORDER BY a").await;
        drop(reopened);
        let oracle = rusqlite::Connection::open(&path).unwrap();
        let integrity: String = oracle
            .query_row("PRAGMA integrity_check", [], |row| row.get(0))
            .unwrap();
        assert_eq!(integrity, "ok");
        assert_eq!(stock_rows(&oracle, check), stock_rows(&stock, check));
    });
}

#[test]
fn explain_marks_only_rowid_preserving_updates_for_in_place_rewrite() {
    asupersync::test_utils::run_test(|| async {
        const KEEPS_ROWID: i64 = 0x10 | 0x80;
        const MOVES_ROWID: i64 = 0x10;
        let frank = Connection::open(":memory:").await.unwrap();
        frank
            .execute_batch("CREATE TABLE p(id INTEGER PRIMARY KEY, v, w); CREATE TABLE h(v)")
            .await
            .unwrap();
        for (sql, expected) in [
            ("UPDATE p SET v = v + 1", KEEPS_ROWID),
            ("UPDATE p SET v = 1, w = 2 WHERE id = 7", KEEPS_ROWID),
            ("UPDATE p SET id = id + 1", MOVES_ROWID),
            ("UPDATE p SET v = 1, id = 5 WHERE id = 7", MOVES_ROWID),
            ("UPDATE p SET rowid = 5 WHERE id = 7", MOVES_ROWID),
            ("UPDATE h SET v = v * 2", KEEPS_ROWID),
            ("UPDATE h SET rowid = rowid + 1", MOVES_ROWID),
            ("UPDATE h SET _rowid_ = 9 WHERE v = 1", MOVES_ROWID),
        ] {
            let rows = frank_rows(&frank, &format!("EXPLAIN {sql}")).await;
            let deletes: Vec<i64> = rows
                .iter()
                .filter(|row| row.get(1) == Some(&SqliteValue::from("Delete")))
                .map(|row| match row.get(6) {
                    Some(SqliteValue::Integer(p5)) => *p5,
                    other => panic!("`{sql}`: Delete p5 {other:?}"),
                })
                .collect();
            assert_eq!(deletes, vec![expected], "`{sql}`: Delete p5 values");
        }
    });
}
