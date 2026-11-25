#![recursion_limit = "512"]

//! Trigger / CREATE TABLE AS parity gaps, pinned against stock SQLite
//! (rusqlite), in memory and file-backed:
//!
//! - bd-y26jy: `CREATE TEMP TABLE x AS SELECT ...` (and `temp.x`) creates the
//!   table in the temp schema, so `CREATE INDEX temp.i ON x(...)` works and
//!   `temp.sqlite_master` lists it. fsqlite used to create a main table.
//! - bd-oh34i: trigger NEW values carry the column's affinity, in BEFORE and
//!   AFTER INSERT/UPDATE triggers and their WHEN clauses. fsqlite used to show
//!   `INSERT ... VALUES('1')` into an INTEGER column as the text '1', and a
//!   `WHEN NEW.k = 1` trigger never fired.
//! - bd-5pt42: a multi-row DELETE runs each row's BEFORE trigger, delete and
//!   AFTER trigger before the next row's, so trigger bodies observe the rows
//!   already deleted. fsqlite used to fire every BEFORE trigger, then delete
//!   every row, then fire every AFTER trigger.

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

async fn run_both(frank: &Connection, stock: &rusqlite::Connection, sql: &str) {
    frank
        .execute(sql)
        .await
        .unwrap_or_else(|e| panic!("FrankenSQLite: `{sql}`: {e:?}"));
    stock
        .execute_batch(sql)
        .unwrap_or_else(|e| panic!("SQLite: `{sql}`: {e}"));
}

async fn compare(frank: &Connection, stock: &rusqlite::Connection, sql: &str) {
    assert_eq!(
        frank_rows(frank, sql).await,
        stock_rows(stock, sql),
        "`{sql}` differs from SQLite"
    );
}

/// Runs `body` once against in-memory databases and once against fresh files.
fn for_each_backing<F, Fut>(body: F)
where
    F: Fn(Connection, rusqlite::Connection) -> Fut,
    Fut: std::future::Future<Output = ()>,
{
    asupersync::test_utils::run_test(|| async {
        let frank = Connection::open(":memory:").await.unwrap();
        let stock = rusqlite::Connection::open_in_memory().unwrap();
        body(frank, stock).await;

        let dir = tempfile::tempdir().unwrap();
        let frank_path = dir.path().join("frank.db");
        let stock_path = dir.path().join("stock.db");
        let frank = Connection::open(frank_path.to_str().unwrap()).await.unwrap();
        let stock = rusqlite::Connection::open(&stock_path).unwrap();
        body(frank, stock).await;
    });
}

#[test]
fn create_temp_table_as_select_lives_in_temp_schema() {
    for_each_backing(|frank, stock| async move {
        for sql in [
            "CREATE TABLE src(a INTEGER, b TEXT)",
            "INSERT INTO src VALUES (1, 'x'), (2, 'y'), (3, 'z')",
            "CREATE TEMP TABLE x AS SELECT * FROM src",
            "CREATE INDEX temp.ix ON x(a)",
            "CREATE TABLE temp.y AS SELECT a * 10 AS m FROM src WHERE a > 1",
            "CREATE TEMP TABLE IF NOT EXISTS x AS SELECT 99",
            // A main table may share the name of a temp table.
            "CREATE TABLE x AS SELECT b FROM src WHERE a = 1",
        ] {
            run_both(&frank, &stock, sql).await;
        }
        for sql in [
            "SELECT type, name, tbl_name FROM temp.sqlite_master ORDER BY name",
            "SELECT type, name, tbl_name FROM main.sqlite_master ORDER BY name",
            "SELECT * FROM x ORDER BY 1",
            "SELECT * FROM temp.x ORDER BY a",
            "SELECT * FROM main.x",
            "SELECT * FROM y ORDER BY m",
            "SELECT typeof(a), typeof(b) FROM temp.x ORDER BY a",
        ] {
            compare(&frank, &stock, sql).await;
        }
        let duplicate = "CREATE TEMP TABLE x AS SELECT 1";
        assert!(frank.execute(duplicate).await.is_err());
        assert!(stock.execute_batch(duplicate).is_err());
    });
}

/// A CTAS whose result names repeat gets stock's `name:N` column names, in
/// main and temp. fsqlite used to refuse the temp form ("duplicate column
/// name") and write the main form with a repeated column name, which stock
/// then refused to open ("malformed database schema").
#[test]
fn create_table_as_select_renames_repeated_result_names() {
    for_each_backing(|frank, stock| async move {
        for sql in [
            "CREATE TABLE src(a INTEGER, b TEXT)",
            "INSERT INTO src VALUES (1, 'x'), (2, 'y')",
            "CREATE TABLE m AS SELECT a, a, b AS A, b AS a, b AS \"a:1\" FROM src",
            "CREATE TEMP TABLE t AS SELECT a, a, b AS A FROM src",
        ] {
            run_both(&frank, &stock, sql).await;
        }
        for sql in [
            "SELECT name FROM pragma_table_info('m') ORDER BY cid",
            "SELECT name FROM pragma_table_info('t', 'temp') ORDER BY cid",
            "SELECT * FROM m ORDER BY 1",
            "SELECT * FROM t ORDER BY 1",
        ] {
            compare(&frank, &stock, sql).await;
        }
    });

    // The file fsqlite wrote must open under stock SQLite.
    asupersync::test_utils::run_test(|| async {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("ctas_dupes.db");
        {
            let frank = Connection::open(path.to_str().unwrap()).await.unwrap();
            for sql in [
                "CREATE TABLE src(a INTEGER, b TEXT)",
                "INSERT INTO src VALUES (1, 'x'), (2, 'y')",
                "CREATE TABLE m AS SELECT a, a, b AS a FROM src",
            ] {
                frank.execute(sql).await.unwrap();
            }
            frank.close().await.unwrap();
        }
        let stock = rusqlite::Connection::open(&path).unwrap();
        assert_eq!(
            stock_rows(&stock, "PRAGMA integrity_check"),
            vec![vec![SqliteValue::from("ok")]]
        );
        assert_eq!(
            stock_rows(&stock, "SELECT name FROM pragma_table_info('m') ORDER BY cid"),
            vec![
                vec![SqliteValue::from("a")],
                vec![SqliteValue::from("a:1")],
                vec![SqliteValue::from("a:2")],
            ]
        );
    });
}

#[test]
fn trigger_new_values_carry_column_affinity() {
    for_each_backing(|frank, stock| async move {
        for sql in [
            "CREATE TABLE t(k INTEGER, r REAL, s TEXT, n NUMERIC, b BLOB, u)",
            "CREATE TABLE log(w, v, ty)",
            "CREATE TRIGGER bi BEFORE INSERT ON t BEGIN \
               INSERT INTO log VALUES('bi-k', NEW.k, typeof(NEW.k)); \
               INSERT INTO log VALUES('bi-r', NEW.r, typeof(NEW.r)); \
               INSERT INTO log VALUES('bi-s', NEW.s, typeof(NEW.s)); \
               INSERT INTO log VALUES('bi-n', NEW.n, typeof(NEW.n)); \
               INSERT INTO log VALUES('bi-b', NEW.b, typeof(NEW.b)); \
               INSERT INTO log VALUES('bi-u', NEW.u, typeof(NEW.u)); END",
            "CREATE TRIGGER ai AFTER INSERT ON t BEGIN \
               INSERT INTO log VALUES('ai-k', NEW.k, typeof(NEW.k)); \
               INSERT INTO log VALUES('ai-r', NEW.r, typeof(NEW.r)); \
               INSERT INTO log VALUES('ai-s', NEW.s, typeof(NEW.s)); END",
            "CREATE TRIGGER bw BEFORE INSERT ON t WHEN NEW.k = 1 BEGIN \
               INSERT INTO log VALUES('bw', NEW.k, typeof(NEW.k)); END",
            "CREATE TRIGGER aw AFTER INSERT ON t WHEN NEW.s = '3' BEGIN \
               INSERT INTO log VALUES('aw', NEW.s, typeof(NEW.s)); END",
            "INSERT INTO t VALUES('1', '2', 3, '4.0', 'x', '5')",
            "INSERT INTO t VALUES('abc', 'x1', 2.5, '7e0', 8, 9)",
            "CREATE TRIGGER bu BEFORE UPDATE ON t BEGIN \
               INSERT INTO log VALUES('bu-new', NEW.k, typeof(NEW.k)); \
               INSERT INTO log VALUES('bu-old', OLD.k, typeof(OLD.k)); END",
            "CREATE TRIGGER au AFTER UPDATE ON t WHEN NEW.k = 7 BEGIN \
               INSERT INTO log VALUES('au-new', NEW.k, typeof(NEW.k)); \
               INSERT INTO log VALUES('au-s', NEW.s, typeof(NEW.s)); END",
            "UPDATE t SET k = '7', s = 8 WHERE k = 1",
            "CREATE TABLE p(id INTEGER PRIMARY KEY, v INTEGER)",
            "CREATE TRIGGER pbi BEFORE INSERT ON p BEGIN \
               INSERT INTO log VALUES('pbi-v', NEW.v, typeof(NEW.v)); END",
            "CREATE TRIGGER pai AFTER INSERT ON p BEGIN \
               INSERT INTO log VALUES('pai-id', NEW.id, typeof(NEW.id)); \
               INSERT INTO log VALUES('pai-v', NEW.v, typeof(NEW.v)); END",
            "INSERT INTO p VALUES('10', '11')",
            "INSERT INTO p(v) VALUES('12')",
        ] {
            run_both(&frank, &stock, sql).await;
        }
        compare(&frank, &stock, "SELECT w, v, ty FROM log ORDER BY rowid").await;
        compare(&frank, &stock, "SELECT k, typeof(k), s, typeof(s) FROM t ORDER BY rowid").await;
    });
}

#[test]
fn multi_row_delete_interleaves_before_and_after_triggers_per_row() {
    for_each_backing(|frank, stock| async move {
        for sql in [
            "CREATE TABLE log(w, k, c, m)",
            // INTEGER PRIMARY KEY table, BEFORE and AFTER triggers.
            "CREATE TABLE t(k INTEGER PRIMARY KEY, v)",
            "INSERT INTO t VALUES (1, 'a'), (2, 'b'), (3, 'c'), (4, 'd')",
            "CREATE TRIGGER bd BEFORE DELETE ON t BEGIN \
               INSERT INTO log VALUES('bd', OLD.k, (SELECT count(*) FROM t), \
                                      (SELECT max(k) FROM t)); END",
            "CREATE TRIGGER ad AFTER DELETE ON t BEGIN \
               INSERT INTO log VALUES('ad', OLD.k, (SELECT count(*) FROM t), \
                                      (SELECT max(k) FROM t)); END",
            "DELETE FROM t WHERE k >= 2",
            // Rowid table with a BEFORE trigger only, deleting every row.
            "CREATE TABLE u(k, v)",
            "INSERT INTO u VALUES (1, 'a'), (2, 'b'), (3, 'c')",
            "CREATE TRIGGER ubd BEFORE DELETE ON u BEGIN \
               INSERT INTO log VALUES('ubd', OLD.k, (SELECT count(*) FROM u), \
                                      (SELECT group_concat(k) FROM u)); END",
            "DELETE FROM u",
            // WITHOUT ROWID table.
            "CREATE TABLE w(k TEXT PRIMARY KEY, v) WITHOUT ROWID",
            "INSERT INTO w VALUES ('a', 1), ('b', 2), ('c', 3)",
            "CREATE TRIGGER wbd BEFORE DELETE ON w BEGIN \
               INSERT INTO log VALUES('wbd', OLD.k, (SELECT count(*) FROM w), \
                                      (SELECT group_concat(k) FROM w)); END",
            "DELETE FROM w WHERE v > 1",
            // A BEFORE trigger deleting a later target row: that row is skipped
            // (no second delete, no triggers), and changes() counts only the
            // outer statement's own deletes.
            "CREATE TABLE s(k INTEGER PRIMARY KEY)",
            "INSERT INTO s VALUES (1), (2), (3), (4)",
            "CREATE TRIGGER sbd BEFORE DELETE ON s BEGIN \
               INSERT INTO log VALUES('sbd', OLD.k, (SELECT count(*) FROM s), NULL); \
               DELETE FROM s WHERE k = OLD.k + 1; END",
            "CREATE TRIGGER sad AFTER DELETE ON s BEGIN \
               INSERT INTO log VALUES('sad', OLD.k, (SELECT count(*) FROM s), NULL); END",
            "DELETE FROM s WHERE k IN (1, 2, 3)",
        ] {
            run_both(&frank, &stock, sql).await;
        }
        compare(&frank, &stock, "SELECT changes()").await;
        compare(&frank, &stock, "SELECT w, k, c, m FROM log ORDER BY rowid").await;
        for table in ["t", "u", "w", "s"] {
            compare(&frank, &stock, &format!("SELECT * FROM {table} ORDER BY k")).await;
        }
        // RETURNING across the row-by-row replay, and RAISE(IGNORE) skipping one row.
        for sql in [
            "CREATE TABLE r(k INTEGER PRIMARY KEY, v)",
            "INSERT INTO r VALUES (1, 'a'), (2, 'b'), (3, 'c'), (4, 'd')",
            "CREATE TRIGGER rbd BEFORE DELETE ON r WHEN OLD.k = 2 BEGIN \
               SELECT RAISE(IGNORE); END",
            "CREATE TRIGGER rad AFTER DELETE ON r BEGIN \
               INSERT INTO log VALUES('rad', OLD.k, (SELECT count(*) FROM r), NULL); END",
        ] {
            run_both(&frank, &stock, sql).await;
        }
        compare(&frank, &stock, "DELETE FROM r WHERE k > 0 RETURNING k, v").await;
        compare(&frank, &stock, "SELECT changes()").await;
        compare(&frank, &stock, "SELECT * FROM r ORDER BY k").await;
        compare(&frank, &stock, "SELECT w, k, c, m FROM log WHERE w = 'rad' ORDER BY rowid").await;
    });
}

/// Every row of a multi-row DELETE or UPDATE fires its triggers in a frame
/// that sees the `changes()` from before the statement, as stock restores it
/// around each trigger program; an earlier row's count (or the trigger body's
/// own inserts) never leaks into a later row's triggers. The row-by-row
/// replay published each row's count, so the second row's AFTER trigger saw 1
/// where stock sees the previous statement's 3.
#[test]
fn replayed_row_triggers_see_the_statement_entry_changes() {
    for_each_backing(|frank, stock| async move {
        for sql in [
            "CREATE TABLE log(w, k, c, tc)",
            "CREATE TABLE e(k INTEGER PRIMARY KEY, v)",
            "INSERT INTO e VALUES (1, 'a'), (2, 'b'), (3, 'c')",
            "CREATE TRIGGER ead AFTER DELETE ON e BEGIN \
               INSERT INTO log VALUES('ead', OLD.k, changes(), NULL); \
               INSERT INTO log VALUES('ead2', OLD.k, changes(), NULL); END",
            "DELETE FROM e WHERE k <= 3",
            "INSERT INTO e VALUES (1, 'a'), (2, 'b'), (3, 'c'), (4, 'd')",
            "CREATE TRIGGER ebd BEFORE DELETE ON e BEGIN \
               INSERT INTO log VALUES('ebd', OLD.k, changes(), NULL); END",
            "DELETE FROM e WHERE k >= 2",
            "CREATE TABLE p(k INTEGER PRIMARY KEY, v)",
            "INSERT INTO p VALUES (1, 'a'), (2, 'b'), (3, 'c'), (4, 'd'), (5, 'e')",
            "CREATE TRIGGER pau AFTER UPDATE ON p BEGIN \
               INSERT INTO log VALUES('pau', NEW.k, changes(), NULL); END",
            "UPDATE p SET v = v || '!' WHERE k IN (2, 3, 4)",
        ] {
            run_both(&frank, &stock, sql).await;
        }
        compare(&frank, &stock, "SELECT changes()").await;
        compare(&frank, &stock, "SELECT w, k, c FROM log ORDER BY rowid").await;
        compare(&frank, &stock, "SELECT * FROM e ORDER BY k").await;
        compare(&frank, &stock, "SELECT * FROM p ORDER BY k").await;
    });
}

/// `CREATE TABLE ... AS SELECT` writes stock's createTableStmt text: names
/// quoted only when needed, an unaliased expression named by its source text,
/// each column's affinity as its declared type, one line while short. The
/// declared types are what a reopened schema (fsqlite's or stock's) reads the
/// affinities from; the old text (`CREATE TABLE "x" ("a", "b")`) lost them, so
/// after a reopen '5' stayed TEXT in an INTEGER-affinity column.
#[test]
fn create_table_as_select_writes_stock_schema_text_and_keeps_affinity() {
    asupersync::test_utils::run_test(|| async {
        let setup = [
            "CREATE TABLE src(a INTEGER, b TEXT, c REAL, d NUMERIC, e, \"my col\" INT, [select] TEXT)",
            "INSERT INTO src VALUES (1, 'x', 1.5, 2, 3, 4, 5)",
            "CREATE TABLE x AS SELECT * FROM src",
            "CREATE TABLE y AS SELECT a+1, b || '', CAST(e AS INTEGER), a AS \"Mixed Case\" FROM src",
            "CREATE TABLE z AS SELECT a, b FROM src",
            "CREATE TEMP TABLE tz AS SELECT a, b, c, d FROM src",
        ];
        // (sqlite_temp_master re-renders every TEMP table's text, CTAS or not,
        // so the temp table's declared types are checked through table_info.)
        let checks = [
            "SELECT name, sql FROM sqlite_master WHERE name IN ('x', 'y', 'z') ORDER BY name",
            "SELECT name, type FROM pragma_table_info('x') ORDER BY cid",
            "SELECT name, type FROM pragma_table_info('y') ORDER BY cid",
            "SELECT name, type FROM pragma_table_info('tz') ORDER BY cid",
        ];
        let dir = tempfile::tempdir().unwrap();
        let frank_path = dir.path().join("frank_ctas.db");
        let stock_path = dir.path().join("stock_ctas.db");
        {
            let frank = Connection::open(frank_path.to_str().unwrap()).await.unwrap();
            let stock = rusqlite::Connection::open(&stock_path).unwrap();
            for sql in setup {
                run_both(&frank, &stock, sql).await;
            }
            for sql in checks {
                compare(&frank, &stock, sql).await;
            }
            for sql in [
                "CREATE TEMP TABLE main.bad AS SELECT 1",
                "CREATE TEMP TABLE main.bad(a)",
            ] {
                let frank_error = frank.execute(sql).await.expect_err(sql);
                let stock_error = stock.execute_batch(sql).expect_err(sql);
                assert!(
                    stock_error.to_string().contains("temporary table name must be unqualified")
                        && frank_error
                            .to_string()
                            .contains("temporary table name must be unqualified"),
                    "`{sql}`: {frank_error:?} vs {stock_error}"
                );
            }
        }
        // Reopened (by each engine, and the fsqlite file by stock), the
        // columns keep the SELECT's affinities.
        let insert = "INSERT INTO x(a, b, c, d) VALUES ('5', 6, '7', '8.0')";
        let read = "SELECT typeof(a), typeof(b), typeof(c), typeof(d) FROM x ORDER BY rowid";
        let frank = Connection::open(frank_path.to_str().unwrap()).await.unwrap();
        let stock = rusqlite::Connection::open(&stock_path).unwrap();
        run_both(&frank, &stock, insert).await;
        compare(&frank, &stock, read).await;
        drop(frank);
        let stock_reading_frank = rusqlite::Connection::open(&frank_path).unwrap();
        stock_reading_frank.execute_batch(insert).unwrap();
        // The fsqlite file now holds the copied row plus the row each engine
        // inserted; both inserted rows must have the stock-inserted row's types.
        let stock_types = stock_rows(&stock, read);
        let mut expected = stock_types.clone();
        expected.push(stock_types[1].clone());
        assert_eq!(stock_rows(&stock_reading_frank, read), expected);
        assert_eq!(
            stock_rows(&stock_reading_frank, "PRAGMA integrity_check"),
            vec![vec![SqliteValue::from("ok")]]
        );
    });
}

fn snapshot_main_db_and_sidecars(db_path: &std::path::Path) -> Vec<(String, Option<Vec<u8>>)> {
    let base = db_path.as_os_str();
    ["", "-wal", "-wal-cert", "-wal-cert-head", "-shm"]
        .into_iter()
        .map(|suffix| {
            let mut full = base.to_os_string();
            full.push(suffix);
            let path = std::path::PathBuf::from(full);
            let bytes = match std::fs::read(&path) {
                Ok(data) => Some(data),
                Err(err) if err.kind() == std::io::ErrorKind::NotFound => None,
                Err(err) => panic!("failed reading {}: {err}", path.display()),
            };
            (suffix.to_owned(), bytes)
        })
        .collect()
}

/// bd-s7bjg: `CREATE TEMP TABLE ... AS SELECT` (and other connection-local TEMP
/// DDL) on a file-backed WAL database must NOT write to the main database file
/// or any of its sidecars (`-wal`, `-wal-cert`, `-wal-cert-head`, `-shm`).
/// Every main sidecar must remain byte-identical, while the negative control
/// (`CREATE TABLE main_copy AS SELECT ...`) DOES mutate main's WAL/DB.
#[test]
fn bd_s7bjg_temp_ctas_leaves_main_sidecars_byte_identical_and_matches_stock() {
    asupersync::test_utils::run_test(|| async {
        let dir = tempfile::tempdir().unwrap();
        let frank_path = dir.path().join("frank_s7bjg.db");
        let stock_path = dir.path().join("stock_s7bjg.db");

        // Seed a file-backed WAL database and close so we reopen cleanly with
        // an untouched main file + sidecars (`memdb_rows_loaded == false`).
        {
            let frank = Connection::open(frank_path.to_str().unwrap()).await.unwrap();
            let stock = rusqlite::Connection::open(&stock_path).unwrap();
            for sql in [
                "PRAGMA journal_mode = WAL",
                "CREATE TABLE src(id INTEGER PRIMARY KEY, a INTEGER, r REAL, s TEXT, b BLOB, n NUMERIC)",
                "BEGIN",
            ] {
                run_both(&frank, &stock, sql).await;
            }
            for i in 1..=500_i64 {
                let sql = if i % 29 == 0 {
                    format!("INSERT INTO src VALUES ({i}, NULL, NULL, NULL, NULL, NULL)")
                } else {
                    format!(
                        "INSERT INTO src VALUES ({i}, {}, {}.5, 'row-{i}', X'{:04X}', '{}')",
                        i % 17,
                        i % 13,
                        i & 0xFFFF,
                        i % 11
                    )
                };
                run_both(&frank, &stock, &sql).await;
            }
            run_both(&frank, &stock, "COMMIT").await;
            frank.close().await.unwrap();
        }

        let frank = Connection::open(frank_path.to_str().unwrap()).await.unwrap();
        let stock = rusqlite::Connection::open(&stock_path).unwrap();
        let before = snapshot_main_db_and_sidecars(&frank_path);

        // Verify change-tracking counters stay untouched across TEMP CTAS.
        let before_counters = frank_rows(
            &frank,
            "SELECT changes(), total_changes(), last_insert_rowid()",
        )
        .await;
        assert_eq!(
            before_counters,
            stock_rows(
                &stock,
                "SELECT changes(), total_changes(), last_insert_rowid()"
            )
        );

        // Exercise TEMP CTAS (unqualified + temp.-qualified + empty + grouped +
        // multi-temp roots), TEMP index, TEMP view, TEMP trigger, TEMP DML,
        // transaction & savepoint rollback of TEMP CTAS, and DROP TEMP TABLE.
        for sql in [
            "CREATE TEMP TABLE t1 AS SELECT id, a, r, s, b, n FROM src WHERE id <= 300",
            "CREATE TABLE temp.t2 AS SELECT a, count(*) AS cnt, sum(id) AS tot FROM src GROUP BY a",
            "CREATE TEMP TABLE t_empty AS SELECT id, s FROM src WHERE id < 0",
            "CREATE TEMP TABLE t3(x INTEGER, y TEXT)",
            "INSERT INTO t3 VALUES (10, 'hi'), (20, 'lo')",
            "CREATE INDEX temp.t1_a ON t1(a)",
            "CREATE TEMP VIEW tv AS SELECT id, a, s FROM t1 WHERE id <= 15",
            "CREATE TEMP TRIGGER t3_tr AFTER INSERT ON t3 BEGIN SELECT 1; END",
            "BEGIN",
            "CREATE TEMP TABLE t_roll AS SELECT id, a FROM src WHERE id <= 50",
            "ROLLBACK",
            "BEGIN",
            "SAVEPOINT sp1",
            "CREATE TEMP TABLE t_sp AS SELECT id, a FROM src WHERE id <= 25",
            "ROLLBACK TO sp1",
            "RELEASE sp1",
            "COMMIT",
            "DROP TABLE t3",
        ] {
            run_both(&frank, &stock, sql).await;
        }

        let after_temp = snapshot_main_db_and_sidecars(&frank_path);
        for ((suffix, before_bytes), (_, after_bytes)) in before.iter().zip(after_temp.iter()) {
            assert_eq!(
                after_bytes,
                before_bytes,
                "bd-s7bjg: TEMP DDL/CTAS must NOT touch main file/sidecar `{suffix}` (before={:?} bytes, after={:?} bytes)",
                before_bytes.as_ref().map(Vec::len),
                after_bytes.as_ref().map(Vec::len),
            );
        }

        // Compare temp table contents, affinities, catalog entries, and counters
        // directly against live bundled SQLite.
        for sql in [
            "SELECT id, a, typeof(a), r, typeof(r), s, typeof(s), b, typeof(b), n, typeof(n) FROM t1 ORDER BY id",
            "SELECT a, cnt, tot FROM temp.t2 ORDER BY a",
            "SELECT * FROM t_empty",
            "SELECT * FROM tv ORDER BY id",
            "SELECT type, name, tbl_name FROM temp.sqlite_master ORDER BY type, name",
            "SELECT type, name, tbl_name FROM main.sqlite_master ORDER BY type, name",
            "SELECT changes(), total_changes(), last_insert_rowid()",
        ] {
            compare(&frank, &stock, sql).await;
        }

        // Rolled-back TEMP CTAS tables must not exist on either engine.
        for missing in ["SELECT * FROM t_roll", "SELECT * FROM t_sp", "SELECT * FROM t3"] {
            assert!(frank.query(missing).await.is_err(), "FrankenSQLite: {missing}");
            assert!(stock.prepare(missing).is_err(), "SQLite: {missing}");
        }

        // PRAGMA query_only = ON must still reject CREATE TEMP TABLE like stock.
        run_both(&frank, &stock, "PRAGMA query_only = ON").await;
        assert!(frank.execute("CREATE TEMP TABLE blocked AS SELECT 1").await.is_err());
        assert!(stock.execute_batch("CREATE TEMP TABLE blocked AS SELECT 1").is_err());
        run_both(&frank, &stock, "PRAGMA query_only = OFF").await;

        // Negative control: CREATE TABLE ... AS SELECT on main MUST write main's WAL/DB.
        run_both(
            &frank,
            &stock,
            "CREATE TABLE main_copy AS SELECT id, a, s FROM src WHERE id <= 50",
        )
        .await;
        compare(&frank, &stock, "SELECT id, a, s FROM main_copy ORDER BY id").await;
        let after_main = snapshot_main_db_and_sidecars(&frank_path);
        assert_ne!(
            after_main, before,
            "negative control: CREATE TABLE main_copy AS SELECT must mutate main DB/WAL"
        );
        frank.close().await.unwrap();
    });
}

/// bd-s7bjg live 300k-row `--profile release-perf` A/B probe vs bundled SQLite.
/// Run via: `cargo test --profile release-perf -p fsqlite-core --test bd_trigger_temp_ctas_oracle --locked -- bd_s7bjg_temp_ctas_300k_ab_live --ignored --nocapture`
#[test]
#[ignore = "bd-s7bjg live 300k-row release-perf A/B probe vs bundled rusqlite"]
#[allow(clippy::cast_precision_loss)]
fn bd_s7bjg_temp_ctas_300k_ab_live() {
    use std::time::Instant;

    fn median(values: &mut [f64]) -> f64 {
        values.sort_by(f64::total_cmp);
        values[values.len() / 2]
    }

    asupersync::test_utils::run_test(|| async {
        let dir = tempfile::tempdir().unwrap();
        let frank_path = dir.path().join("frank_300k.db");
        let stock_path = dir.path().join("stock_300k.db");

        // Build identical 300,000-row WAL file databases.
        {
            let frank = Connection::open(frank_path.to_str().unwrap()).await.unwrap();
            let stock = rusqlite::Connection::open(&stock_path).unwrap();
            for sql in [
                "PRAGMA journal_mode = WAL",
                "CREATE TABLE src(id INTEGER PRIMARY KEY, a INTEGER, v INTEGER, s TEXT)",
            ] {
                run_both(&frank, &stock, sql).await;
            }
            frank.execute("BEGIN").await.unwrap();
            let f_ins = frank
                .prepare("INSERT INTO src(id, a, v, s) VALUES (?1, ?2, ?3, ?4)")
                .await
                .unwrap();
            let s_tx = stock.unchecked_transaction().unwrap();
            {
                let mut s_ins = s_tx
                    .prepare("INSERT INTO src(id, a, v, s) VALUES (?1, ?2, ?3, ?4)")
                    .unwrap();
                for id in 1..=300_000_i64 {
                    let a = id % 1000;
                    let v = (id * 7) % 97;
                    let s = format!("val-{a}");
                    f_ins
                        .execute_with_params(&[
                            SqliteValue::Integer(id),
                            SqliteValue::Integer(a),
                            SqliteValue::Integer(v),
                            SqliteValue::Text(s.clone().into()),
                        ])
                        .await
                        .unwrap();
                    s_ins.execute(rusqlite::params![id, a, v, s]).unwrap();
                }
            }
            drop(f_ins);
            frank.execute("COMMIT").await.unwrap();
            s_tx.commit().unwrap();
            frank.close().await.unwrap();
        }

        let frank = Connection::open(frank_path.to_str().unwrap()).await.unwrap();
        let stock = rusqlite::Connection::open(&stock_path).unwrap();
        let before = snapshot_main_db_and_sidecars(&frank_path);

        let ctas_sql = "CREATE TEMP TABLE x AS SELECT id, a, v, s FROM src";
        let drop_sql = "DROP TABLE x";
        let check_sql = "SELECT count(*), sum(id), sum(a), sum(v) FROM x";

        for (label, pragma) in [
            ("default temp_store (300k file DB)", None),
            ("MEMORY temp_store (300k file DB)", Some("PRAGMA temp_store = MEMORY")),
        ] {
            if let Some(p) = pragma {
                run_both(&frank, &stock, p).await;
            }
            let rounds = 5usize;
            let mut f_ms = Vec::with_capacity(rounds);
            let mut s_ms = Vec::with_capacity(rounds);
            for round in 0..rounds {
                if round % 2 == 0 {
                    let t0 = Instant::now();
                    frank.execute(ctas_sql).await.unwrap();
                    f_ms.push(t0.elapsed().as_secs_f64() * 1e3);

                    let t1 = Instant::now();
                    stock.execute_batch(ctas_sql).unwrap();
                    s_ms.push(t1.elapsed().as_secs_f64() * 1e3);
                } else {
                    let t1 = Instant::now();
                    stock.execute_batch(ctas_sql).unwrap();
                    s_ms.push(t1.elapsed().as_secs_f64() * 1e3);

                    let t0 = Instant::now();
                    frank.execute(ctas_sql).await.unwrap();
                    f_ms.push(t0.elapsed().as_secs_f64() * 1e3);
                }
                if round == 0 {
                    compare(&frank, &stock, check_sql).await;
                }
                run_both(&frank, &stock, drop_sql).await;
            }
            let after = snapshot_main_db_and_sidecars(&frank_path);
            assert_eq!(after, before, "bd-s7bjg: main sidecars changed during {label}");
            let fm = median(&mut f_ms);
            let sm = median(&mut s_ms);
            eprintln!(
                "bd-s7bjg A/B [{label}] sqlite={} | frank={fm:.2}ms stock={sm:.2}ms ratio={:.2}x",
                rusqlite::version(),
                fm / sm,
            );
        }
        frank.close().await.unwrap();
    });
}
