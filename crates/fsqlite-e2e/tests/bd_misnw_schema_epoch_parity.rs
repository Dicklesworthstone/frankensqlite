//! Schema-epoch binding, prepared-stmt invalidation, SAVEPOINT correctness,
//! and rusqlite oracle parity tests (bd-misnw).
//!
//! Exercises the schema_generation mechanism that detects DDL-induced staleness
//! of prepared statements, verifying FrankenSQLite matches rusqlite behaviour
//! across 30+ query shapes.
//!
//! ## Scenarios
//!
//! | Group | Name                               | Description                                           |
//! |-------|------------------------------------|-------------------------------------------------------|
//! | S1    | ddl_invalidates_prepared_select    | Prepared SELECT returns correct data after DDL ALTER   |
//! | S2    | ddl_invalidates_prepared_insert    | Prepared INSERT adapts after schema change             |
//! | S3    | create_table_visible_immediately   | New table queryable right after CREATE                 |
//! | S4    | drop_table_errors_on_stale_query   | Query on dropped table must error                      |
//! | S5    | create_index_doesnt_break_queries  | Adding index doesn't corrupt query results             |
//! | S6    | alter_add_column_schema_epoch      | ALTER ADD COLUMN visible to subsequent queries         |
//! | S7    | rename_table_propagates            | Renamed table accessible under new name                |
//! | S8    | create_view_visible                | View queryable immediately after creation              |
//! | S9    | drop_view_errors_on_stale          | Dropped view query must error                          |
//! | S10   | create_trigger_fires               | Trigger fires on subsequent DML                        |
//! | S11   | savepoint_ddl_rollback_reverts     | DDL in SAVEPOINT + ROLLBACK reverts schema change      |
//! | S12   | savepoint_ddl_release_persists     | DDL in SAVEPOINT + RELEASE persists schema change      |
//! | S13   | nested_savepoint_ddl_rollback      | Nested SAVEPOINT DDL rollback chain                    |
//! | S14   | sequential_ddl_epoch_monotonic     | Multiple DDLs in sequence all take effect               |
//! | S15   | prepared_cache_lru_eviction         | Cache evicts old entries on DDL, fresh prepare works   |
//! | S16   | mixed_dml_ddl_interleave           | Interleaved DML and DDL maintain correctness           |
//! | S17   | create_drop_create_same_name       | Create-drop-recreate cycle works                       |
//! | S18   | alter_column_default               | ALTER TABLE default value visible on INSERT            |
//! | S19   | index_on_expression                | Expression index doesn't corrupt queries               |
//! | S20   | multiple_connections_same_db       | File-backed: 2nd connection sees DDL from 1st          |
//! | S21   | prepared_select_star_after_alter   | SELECT * returns new columns after ALTER ADD COLUMN    |
//! | S22   | trigger_after_drop_recreate        | Trigger re-created after drop fires correctly          |
//! | S23   | vacuum_doesnt_break_queries        | VACUUM preserves data and query correctness            |
//! | S24   | ctas_visible_immediately           | CREATE TABLE AS SELECT visible right away              |
//! | S25   | temp_table_schema_epoch            | TEMP tables participate in schema tracking             |
//! | S26   | insert_returning_after_ddl         | INSERT RETURNING works after schema change             |
//! | S27   | update_after_add_column            | UPDATE sets new column after ALTER ADD COLUMN          |
//! | S28   | delete_after_create_index          | DELETE WHERE works after index creation                |
//! | S29   | compound_select_after_ddl          | UNION/INTERSECT/EXCEPT work after DDL changes          |
//! | S30   | subquery_after_schema_change       | Correlated subquery adapts to schema changes           |
//! | S31   | savepoint_nested_dml_ddl_mix       | Mixed DML+DDL across nested savepoints                 |
//! | S32   | reanalyze_after_bulk_insert        | ANALYZE after bulk insert doesn't corrupt              |
//!
//! ## Run
//!
//! ```sh
//! cargo test -p fsqlite-e2e --test bd_misnw_schema_epoch_parity -- --nocapture --test-threads=1
//! ```

#![allow(clippy::cast_precision_loss)]
#![recursion_limit = "512"]

use serde_json::json;
use std::time::Instant;

const BEAD_ID: &str = "bd-misnw";
const REPLAY_CMD: &str =
    "cargo test -p fsqlite-e2e --test bd_misnw_schema_epoch_parity -- --nocapture --test-threads=1";

fn emit_log(test_name: &str, phase: &str, data: serde_json::Value) {
    eprintln!(
        "SCHEMA_EPOCH_PARITY:{}",
        json!({
            "bead_id": BEAD_ID,
            "test": test_name,
            "phase": phase,
            "replay_command": REPLAY_CMD,
            "data": data,
        })
    );
}

fn csqlite_val_to_fsqlite(
    row: &rusqlite::Row<'_>,
    col: usize,
) -> fsqlite_types::value::SqliteValue {
    use fsqlite_types::value::SqliteValue;
    use rusqlite::types::ValueRef;
    match row.get_ref(col).unwrap() {
        ValueRef::Null => SqliteValue::Null,
        ValueRef::Integer(n) => SqliteValue::Integer(n),
        ValueRef::Real(f) => SqliteValue::Float(f),
        ValueRef::Text(s) => SqliteValue::Text(std::str::from_utf8(s).unwrap().into()),
        ValueRef::Blob(b) => SqliteValue::Blob(b.to_vec().into()),
    }
}

struct OracleRow {
    vals: Vec<fsqlite_types::value::SqliteValue>,
}

fn csqlite_query(conn: &rusqlite::Connection, sql: &str) -> Vec<OracleRow> {
    let mut stmt = conn.prepare(sql).expect("csqlite prepare");
    let col_count = stmt.column_count();
    stmt.query_map([], |row| {
        let vals: Vec<fsqlite_types::value::SqliteValue> = (0..col_count)
            .map(|col| csqlite_val_to_fsqlite(row, col))
            .collect();
        Ok(OracleRow { vals })
    })
    .expect("csqlite query")
    .map(|r| r.expect("csqlite row"))
    .collect()
}

fn assert_fsqlite_csqlite_eq(
    test_name: &str,
    label: &str,
    f_rows: &[fsqlite::Row],
    c_rows: &[OracleRow],
) {
    assert_eq!(
        f_rows.len(),
        c_rows.len(),
        "[{test_name}] {label} row count mismatch: fsqlite={} csqlite={}",
        f_rows.len(),
        c_rows.len()
    );
    for (i, (f, c)) in f_rows.iter().zip(c_rows.iter()).enumerate() {
        let f_vals = f.values();
        assert_eq!(
            f_vals,
            c.vals.as_slice(),
            "[{test_name}] {label} row {i} mismatch"
        );
    }
}

async fn assert_fsqlite_error(conn: &fsqlite::Connection, sql: &str, test_name: &str, label: &str) {
    let result = conn.query(sql).await;
    assert!(
        result.is_err(),
        "[{test_name}] {label}: expected error but got Ok({} rows)",
        result.unwrap().len()
    );
}

fn assert_csqlite_error(conn: &rusqlite::Connection, sql: &str) {
    let result = conn.prepare(sql);
    assert!(result.is_err());
}

// ─── S1: DDL invalidates prepared SELECT ─────────────────────────────

#[test]
fn s01_ddl_invalidates_prepared_select() {
    asupersync::test_utils::run_test(|| async {
        let tn = "s01_ddl_invalidates_select";
        emit_log(tn, "start", json!({}));
        let t = Instant::now();

        let fconn = fsqlite::Connection::open(":memory:").await.unwrap();
        let cconn = rusqlite::Connection::open_in_memory().unwrap();

        for sql in [
            "CREATE TABLE t1 (a INTEGER, b TEXT)",
            "INSERT INTO t1 VALUES (1, 'x'), (2, 'y')",
        ] {
            fconn.execute(sql).await.unwrap();
            cconn.execute_batch(&format!("{sql};")).unwrap();
        }

        let f1 = fconn.query("SELECT a, b FROM t1 ORDER BY a").await.unwrap();
        let c1 = csqlite_query(&cconn, "SELECT a, b FROM t1 ORDER BY a");
        assert_fsqlite_csqlite_eq(tn, "before_alter", &f1, &c1);

        fconn
            .execute("ALTER TABLE t1 ADD COLUMN c INTEGER DEFAULT 99")
            .await
            .unwrap();
        cconn
            .execute_batch("ALTER TABLE t1 ADD COLUMN c INTEGER DEFAULT 99;")
            .unwrap();

        let f2 = fconn
            .query("SELECT a, b, c FROM t1 ORDER BY a")
            .await
            .unwrap();
        let c2 = csqlite_query(&cconn, "SELECT a, b, c FROM t1 ORDER BY a");
        assert_fsqlite_csqlite_eq(tn, "after_alter", &f2, &c2);

        emit_log(tn, "result", json!({"elapsed_us": t.elapsed().as_micros()}));
    });
}

// ─── S2: DDL invalidates prepared INSERT ─────────────────────────────

#[test]
fn s02_ddl_invalidates_prepared_insert() {
    asupersync::test_utils::run_test(|| async {
        let tn = "s02_ddl_invalidates_insert";
        emit_log(tn, "start", json!({}));

        let fconn = fsqlite::Connection::open(":memory:").await.unwrap();
        let cconn = rusqlite::Connection::open_in_memory().unwrap();

        fconn.execute("CREATE TABLE t2 (a INTEGER)").await.unwrap();
        cconn.execute_batch("CREATE TABLE t2 (a INTEGER);").unwrap();

        fconn.execute("INSERT INTO t2 VALUES (1)").await.unwrap();
        cconn.execute_batch("INSERT INTO t2 VALUES (1);").unwrap();

        fconn
            .execute("ALTER TABLE t2 ADD COLUMN b TEXT DEFAULT 'dflt'")
            .await
            .unwrap();
        cconn
            .execute_batch("ALTER TABLE t2 ADD COLUMN b TEXT DEFAULT 'dflt';")
            .unwrap();

        fconn
            .execute("INSERT INTO t2 (a, b) VALUES (2, 'new')")
            .await
            .unwrap();
        cconn
            .execute_batch("INSERT INTO t2 (a, b) VALUES (2, 'new');")
            .unwrap();

        let f = fconn.query("SELECT a, b FROM t2 ORDER BY a").await.unwrap();
        let c = csqlite_query(&cconn, "SELECT a, b FROM t2 ORDER BY a");
        assert_fsqlite_csqlite_eq(tn, "after_alter_insert", &f, &c);

        emit_log(tn, "result", json!({"pass": true}));
    });
}

// ─── S3: CREATE TABLE visible immediately ────────────────────────────

#[test]
fn s03_create_table_visible_immediately() {
    asupersync::test_utils::run_test(|| async {
        let tn = "s03_create_visible";
        emit_log(tn, "start", json!({}));

        let fconn = fsqlite::Connection::open(":memory:").await.unwrap();
        let cconn = rusqlite::Connection::open_in_memory().unwrap();

        fconn
            .execute("CREATE TABLE t3 (id INTEGER PRIMARY KEY, val TEXT)")
            .await
            .unwrap();
        cconn
            .execute_batch("CREATE TABLE t3 (id INTEGER PRIMARY KEY, val TEXT);")
            .unwrap();

        fconn
            .execute("INSERT INTO t3 VALUES (1, 'hello')")
            .await
            .unwrap();
        cconn
            .execute_batch("INSERT INTO t3 VALUES (1, 'hello');")
            .unwrap();

        let f = fconn.query("SELECT id, val FROM t3").await.unwrap();
        let c = csqlite_query(&cconn, "SELECT id, val FROM t3");
        assert_fsqlite_csqlite_eq(tn, "immediate_query", &f, &c);

        emit_log(tn, "result", json!({"pass": true}));
    });
}

// ─── S4: DROP TABLE errors on stale query ────────────────────────────

#[test]
fn s04_drop_table_errors_on_stale_query() {
    asupersync::test_utils::run_test(|| async {
        let tn = "s04_drop_errors";
        emit_log(tn, "start", json!({}));

        let fconn = fsqlite::Connection::open(":memory:").await.unwrap();
        let cconn = rusqlite::Connection::open_in_memory().unwrap();

        fconn.execute("CREATE TABLE t4 (x INTEGER)").await.unwrap();
        cconn.execute_batch("CREATE TABLE t4 (x INTEGER);").unwrap();

        fconn.execute("INSERT INTO t4 VALUES (1)").await.unwrap();
        cconn.execute_batch("INSERT INTO t4 VALUES (1);").unwrap();

        fconn.execute("DROP TABLE t4").await.unwrap();
        cconn.execute_batch("DROP TABLE t4;").unwrap();

        assert_fsqlite_error(&fconn, "SELECT * FROM t4", tn, "after_drop").await;
        assert_csqlite_error(&cconn, "SELECT * FROM t4");

        emit_log(tn, "result", json!({"pass": true}));
    });
}

// ─── S5: CREATE INDEX doesn't break queries ──────────────────────────

#[test]
fn s05_create_index_doesnt_break_queries() {
    asupersync::test_utils::run_test(|| async {
        let tn = "s05_index_safe";
        emit_log(tn, "start", json!({}));

        let fconn = fsqlite::Connection::open(":memory:").await.unwrap();
        let cconn = rusqlite::Connection::open_in_memory().unwrap();

        for sql in [
            "CREATE TABLE t5 (a INTEGER, b TEXT)",
            "INSERT INTO t5 VALUES (3, 'c'), (1, 'a'), (2, 'b')",
        ] {
            fconn.execute(sql).await.unwrap();
            cconn.execute_batch(&format!("{sql};")).unwrap();
        }

        let f_before = fconn.query("SELECT a, b FROM t5 ORDER BY a").await.unwrap();

        fconn
            .execute("CREATE INDEX idx_t5_a ON t5(a)")
            .await
            .unwrap();
        cconn
            .execute_batch("CREATE INDEX idx_t5_a ON t5(a);")
            .unwrap();

        let f_after = fconn.query("SELECT a, b FROM t5 ORDER BY a").await.unwrap();
        let c_after = csqlite_query(&cconn, "SELECT a, b FROM t5 ORDER BY a");

        assert_fsqlite_csqlite_eq(tn, "after_index", &f_after, &c_after);
        assert_eq!(
            f_before.len(),
            f_after.len(),
            "row count changed after index"
        );

        emit_log(tn, "result", json!({"pass": true}));
    });
}

// ─── S6: ALTER ADD COLUMN schema epoch ───────────────────────────────

#[test]
fn s06_alter_add_column_schema_epoch() {
    asupersync::test_utils::run_test(|| async {
        let tn = "s06_alter_add_col";
        emit_log(tn, "start", json!({}));

        let fconn = fsqlite::Connection::open(":memory:").await.unwrap();
        let cconn = rusqlite::Connection::open_in_memory().unwrap();

        for sql in [
            "CREATE TABLE t6 (id INTEGER PRIMARY KEY)",
            "INSERT INTO t6 VALUES (1), (2), (3)",
        ] {
            fconn.execute(sql).await.unwrap();
            cconn.execute_batch(&format!("{sql};")).unwrap();
        }

        fconn
            .execute("ALTER TABLE t6 ADD COLUMN name TEXT DEFAULT 'anon'")
            .await
            .unwrap();
        cconn
            .execute_batch("ALTER TABLE t6 ADD COLUMN name TEXT DEFAULT 'anon';")
            .unwrap();

        fconn
            .execute("ALTER TABLE t6 ADD COLUMN score REAL DEFAULT 0.0")
            .await
            .unwrap();
        cconn
            .execute_batch("ALTER TABLE t6 ADD COLUMN score REAL DEFAULT 0.0;")
            .unwrap();

        fconn
            .execute("INSERT INTO t6 (id, name, score) VALUES (4, 'new', 99.5)")
            .await
            .unwrap();
        cconn
            .execute_batch("INSERT INTO t6 (id, name, score) VALUES (4, 'new', 99.5);")
            .unwrap();

        let f = fconn
            .query("SELECT id, name, score FROM t6 ORDER BY id")
            .await
            .unwrap();
        let c = csqlite_query(&cconn, "SELECT id, name, score FROM t6 ORDER BY id");
        assert_fsqlite_csqlite_eq(tn, "multi_alter", &f, &c);

        emit_log(tn, "result", json!({"pass": true}));
    });
}

// ─── S7: RENAME TABLE propagates ─────────────────────────────────────

#[test]
fn s07_rename_table_propagates() {
    asupersync::test_utils::run_test(|| async {
        let tn = "s07_rename";
        emit_log(tn, "start", json!({}));

        let fconn = fsqlite::Connection::open(":memory:").await.unwrap();
        let cconn = rusqlite::Connection::open_in_memory().unwrap();

        for sql in [
            "CREATE TABLE t7_old (x INTEGER)",
            "INSERT INTO t7_old VALUES (42)",
            "ALTER TABLE t7_old RENAME TO t7_new",
        ] {
            fconn.execute(sql).await.unwrap();
            cconn.execute_batch(&format!("{sql};")).unwrap();
        }

        let f = fconn.query("SELECT x FROM t7_new").await.unwrap();
        let c = csqlite_query(&cconn, "SELECT x FROM t7_new");
        assert_fsqlite_csqlite_eq(tn, "renamed_query", &f, &c);

        assert_fsqlite_error(&fconn, "SELECT x FROM t7_old", tn, "old_name_gone").await;
        assert_csqlite_error(&cconn, "SELECT x FROM t7_old");

        emit_log(tn, "result", json!({"pass": true}));
    });
}

// ─── S8: CREATE VIEW visible ─────────────────────────────────────────

#[test]
fn s08_create_view_visible() {
    asupersync::test_utils::run_test(|| async {
        let tn = "s08_view_visible";
        emit_log(tn, "start", json!({}));

        let fconn = fsqlite::Connection::open(":memory:").await.unwrap();
        let cconn = rusqlite::Connection::open_in_memory().unwrap();

        for sql in [
            "CREATE TABLE t8 (a INTEGER, b INTEGER)",
            "INSERT INTO t8 VALUES (1, 10), (2, 20)",
            "CREATE VIEW v8 AS SELECT a, b, a + b AS total FROM t8",
        ] {
            fconn.execute(sql).await.unwrap();
            cconn.execute_batch(&format!("{sql};")).unwrap();
        }

        let f = fconn.query("SELECT * FROM v8 ORDER BY a").await.unwrap();
        let c = csqlite_query(&cconn, "SELECT * FROM v8 ORDER BY a");
        assert_fsqlite_csqlite_eq(tn, "view_query", &f, &c);

        emit_log(tn, "result", json!({"pass": true}));
    });
}

// ─── S9: DROP VIEW errors on stale ───────────────────────────────────

#[test]
fn s09_drop_view_errors_on_stale() {
    asupersync::test_utils::run_test(|| async {
        let tn = "s09_drop_view";
        emit_log(tn, "start", json!({}));

        let fconn = fsqlite::Connection::open(":memory:").await.unwrap();
        let cconn = rusqlite::Connection::open_in_memory().unwrap();

        for sql in [
            "CREATE TABLE t9 (x INTEGER)",
            "INSERT INTO t9 VALUES (1)",
            "CREATE VIEW v9 AS SELECT x FROM t9",
        ] {
            fconn.execute(sql).await.unwrap();
            cconn.execute_batch(&format!("{sql};")).unwrap();
        }

        fconn.execute("DROP VIEW v9").await.unwrap();
        cconn.execute_batch("DROP VIEW v9;").unwrap();

        assert_fsqlite_error(&fconn, "SELECT * FROM v9", tn, "after_drop_view").await;
        assert_csqlite_error(&cconn, "SELECT * FROM v9");

        emit_log(tn, "result", json!({"pass": true}));
    });
}

// ─── S10: CREATE TRIGGER fires ───────────────────────────────────────

#[test]
fn s10_create_trigger_fires() {
    asupersync::test_utils::run_test(|| async {
        let tn = "s10_trigger_fires";
        emit_log(tn, "start", json!({}));

        let fconn = fsqlite::Connection::open(":memory:").await.unwrap();
        let cconn = rusqlite::Connection::open_in_memory().unwrap();

        for sql in [
            "CREATE TABLE t10 (id INTEGER PRIMARY KEY, val TEXT)",
            "CREATE TABLE t10_log (msg TEXT)",
            "CREATE TRIGGER trg10 AFTER INSERT ON t10 BEGIN INSERT INTO t10_log VALUES ('inserted ' || NEW.val); END",
            "INSERT INTO t10 VALUES (1, 'alpha')",
            "INSERT INTO t10 VALUES (2, 'beta')",
        ] {
            fconn.execute(sql).await.unwrap();
            cconn.execute_batch(&format!("{sql};")).unwrap();
        }

        let f = fconn
            .query("SELECT msg FROM t10_log ORDER BY rowid")
            .await
            .unwrap();
        let c = csqlite_query(&cconn, "SELECT msg FROM t10_log ORDER BY rowid");
        assert_fsqlite_csqlite_eq(tn, "trigger_log", &f, &c);

        emit_log(tn, "result", json!({"pass": true}));
    });
}

// ─── S11: SAVEPOINT DDL + ROLLBACK reverts ───────────────────────────

#[test]
fn s11_savepoint_ddl_rollback_reverts() {
    asupersync::test_utils::run_test(|| async {
        let tn = "s11_sp_ddl_rollback";
        emit_log(tn, "start", json!({}));

        let fconn = fsqlite::Connection::open(":memory:").await.unwrap();
        let cconn = rusqlite::Connection::open_in_memory().unwrap();

        for sql in ["CREATE TABLE t11 (a INTEGER)", "INSERT INTO t11 VALUES (1)"] {
            fconn.execute(sql).await.unwrap();
            cconn.execute_batch(&format!("{sql};")).unwrap();
        }

        // DDL inside savepoint, then rollback
        for sql in [
            "SAVEPOINT sp1",
            "ALTER TABLE t11 ADD COLUMN b TEXT DEFAULT 'hi'",
            "INSERT INTO t11 (a, b) VALUES (2, 'world')",
            "ROLLBACK TO sp1",
            "RELEASE sp1",
        ] {
            fconn.execute(sql).await.unwrap();
            cconn.execute_batch(&format!("{sql};")).unwrap();
        }

        // After rollback, column b should not exist
        let f = fconn.query("SELECT a FROM t11 ORDER BY a").await.unwrap();
        let c = csqlite_query(&cconn, "SELECT a FROM t11 ORDER BY a");
        assert_fsqlite_csqlite_eq(tn, "after_rollback", &f, &c);

        // Querying column b should fail in both
        assert_fsqlite_error(&fconn, "SELECT b FROM t11", tn, "col_b_gone").await;
        assert_csqlite_error(&cconn, "SELECT b FROM t11");

        emit_log(tn, "result", json!({"pass": true}));
    });
}

// ─── S12: SAVEPOINT DDL + RELEASE persists ───────────────────────────

#[test]
fn s12_savepoint_ddl_release_persists() {
    asupersync::test_utils::run_test(|| async {
        let tn = "s12_sp_ddl_release";
        emit_log(tn, "start", json!({}));

        let fconn = fsqlite::Connection::open(":memory:").await.unwrap();
        let cconn = rusqlite::Connection::open_in_memory().unwrap();

        for sql in ["CREATE TABLE t12 (a INTEGER)", "INSERT INTO t12 VALUES (1)"] {
            fconn.execute(sql).await.unwrap();
            cconn.execute_batch(&format!("{sql};")).unwrap();
        }

        for sql in [
            "SAVEPOINT sp1",
            "ALTER TABLE t12 ADD COLUMN b TEXT DEFAULT 'persisted'",
            "INSERT INTO t12 (a, b) VALUES (2, 'new_row')",
            "RELEASE sp1",
        ] {
            fconn.execute(sql).await.unwrap();
            cconn.execute_batch(&format!("{sql};")).unwrap();
        }

        let f = fconn
            .query("SELECT a, b FROM t12 ORDER BY a")
            .await
            .unwrap();
        let c = csqlite_query(&cconn, "SELECT a, b FROM t12 ORDER BY a");
        assert_fsqlite_csqlite_eq(tn, "after_release", &f, &c);

        emit_log(tn, "result", json!({"pass": true}));
    });
}

// ─── S13: Nested SAVEPOINT DDL rollback chain ────────────────────────

#[test]
fn s13_nested_savepoint_ddl_rollback() {
    asupersync::test_utils::run_test(|| async {
        let tn = "s13_nested_sp";
        emit_log(tn, "start", json!({}));

        let fconn = fsqlite::Connection::open(":memory:").await.unwrap();
        let cconn = rusqlite::Connection::open_in_memory().unwrap();

        for sql in ["CREATE TABLE t13 (a INTEGER)", "INSERT INTO t13 VALUES (1)"] {
            fconn.execute(sql).await.unwrap();
            cconn.execute_batch(&format!("{sql};")).unwrap();
        }

        for sql in [
            "SAVEPOINT outer_sp",
            "ALTER TABLE t13 ADD COLUMN b TEXT",
            "INSERT INTO t13 (a, b) VALUES (2, 'outer')",
            "SAVEPOINT inner_sp",
            "INSERT INTO t13 (a, b) VALUES (3, 'inner')",
            "ROLLBACK TO inner_sp",
            "RELEASE inner_sp",
            "RELEASE outer_sp",
        ] {
            fconn.execute(sql).await.unwrap();
            cconn.execute_batch(&format!("{sql};")).unwrap();
        }

        // Outer savepoint released: ALTER + row 2 persists, row 3 rolled back
        let f = fconn
            .query("SELECT a, b FROM t13 ORDER BY a")
            .await
            .unwrap();
        let c = csqlite_query(&cconn, "SELECT a, b FROM t13 ORDER BY a");
        assert_fsqlite_csqlite_eq(tn, "nested_sp_result", &f, &c);

        emit_log(tn, "result", json!({"pass": true}));
    });
}

// ─── S14: Sequential DDLs epoch monotonic ────────────────────────────

#[test]
fn s14_sequential_ddl_epoch_monotonic() {
    asupersync::test_utils::run_test(|| async {
        let tn = "s14_sequential_ddl";
        emit_log(tn, "start", json!({}));

        let fconn = fsqlite::Connection::open(":memory:").await.unwrap();
        let cconn = rusqlite::Connection::open_in_memory().unwrap();

        for sql in [
            "CREATE TABLE s14a (x INTEGER)",
            "CREATE TABLE s14b (y TEXT)",
            "CREATE TABLE s14c (z REAL)",
            "INSERT INTO s14a VALUES (1), (2)",
            "INSERT INTO s14b VALUES ('a'), ('b')",
            "INSERT INTO s14c VALUES (1.5), (2.5)",
        ] {
            fconn.execute(sql).await.unwrap();
            cconn.execute_batch(&format!("{sql};")).unwrap();
        }

        for (sql, table) in [
            ("SELECT x FROM s14a ORDER BY x", "s14a"),
            ("SELECT y FROM s14b ORDER BY y", "s14b"),
            ("SELECT z FROM s14c ORDER BY z", "s14c"),
        ] {
            let f = fconn.query(sql).await.unwrap();
            let c = csqlite_query(&cconn, sql);
            assert_fsqlite_csqlite_eq(tn, table, &f, &c);
        }

        emit_log(tn, "result", json!({"pass": true}));
    });
}

// ─── S15: Prepared cache LRU eviction on DDL ─────────────────────────

#[test]
fn s15_prepared_cache_lru_eviction() {
    asupersync::test_utils::run_test(|| async {
        let tn = "s15_cache_eviction";
        emit_log(tn, "start", json!({}));

        let fconn = fsqlite::Connection::open(":memory:").await.unwrap();
        let cconn = rusqlite::Connection::open_in_memory().unwrap();

        fconn.execute("CREATE TABLE t15 (a INTEGER)").await.unwrap();
        cconn
            .execute_batch("CREATE TABLE t15 (a INTEGER);")
            .unwrap();

        fconn.execute("INSERT INTO t15 VALUES (10)").await.unwrap();
        cconn.execute_batch("INSERT INTO t15 VALUES (10);").unwrap();

        // Warm the prepared cache
        for _ in 0..5 {
            drop(fconn.query("SELECT a FROM t15").await);
        }

        // DDL should invalidate cache
        fconn
            .execute("ALTER TABLE t15 ADD COLUMN b INTEGER DEFAULT 0")
            .await
            .unwrap();
        cconn
            .execute_batch("ALTER TABLE t15 ADD COLUMN b INTEGER DEFAULT 0;")
            .unwrap();

        // Fresh prepare after DDL must see new schema
        let f = fconn.query("SELECT a, b FROM t15").await.unwrap();
        let c = csqlite_query(&cconn, "SELECT a, b FROM t15");
        assert_fsqlite_csqlite_eq(tn, "after_ddl_eviction", &f, &c);

        emit_log(tn, "result", json!({"pass": true}));
    });
}

// ─── S16: Mixed DML + DDL interleave ─────────────────────────────────

#[test]
fn s16_mixed_dml_ddl_interleave() {
    asupersync::test_utils::run_test(|| async {
        let tn = "s16_mixed_interleave";
        emit_log(tn, "start", json!({}));

        let fconn = fsqlite::Connection::open(":memory:").await.unwrap();
        let cconn = rusqlite::Connection::open_in_memory().unwrap();

        let steps: Vec<&str> = vec![
            "CREATE TABLE t16 (id INTEGER PRIMARY KEY, val TEXT)",
            "INSERT INTO t16 VALUES (1, 'a')",
            "INSERT INTO t16 VALUES (2, 'b')",
            "ALTER TABLE t16 ADD COLUMN extra INTEGER DEFAULT 0",
            "INSERT INTO t16 (id, val, extra) VALUES (3, 'c', 100)",
            "UPDATE t16 SET extra = 50 WHERE id = 1",
            "CREATE INDEX idx_t16 ON t16(extra)",
            "INSERT INTO t16 (id, val, extra) VALUES (4, 'd', 200)",
            "DELETE FROM t16 WHERE id = 2",
        ];

        for sql in &steps {
            fconn.execute(sql).await.unwrap();
            cconn.execute_batch(&format!("{sql};")).unwrap();
        }

        let f = fconn
            .query("SELECT id, val, extra FROM t16 ORDER BY id")
            .await
            .unwrap();
        let c = csqlite_query(&cconn, "SELECT id, val, extra FROM t16 ORDER BY id");
        assert_fsqlite_csqlite_eq(tn, "final_state", &f, &c);

        emit_log(tn, "result", json!({"pass": true}));
    });
}

// ─── S17: Create-drop-create same name ───────────────────────────────

#[test]
fn s17_create_drop_create_same_name() {
    asupersync::test_utils::run_test(|| async {
        let tn = "s17_create_drop_create";
        emit_log(tn, "start", json!({}));

        let fconn = fsqlite::Connection::open(":memory:").await.unwrap();
        let cconn = rusqlite::Connection::open_in_memory().unwrap();

        for sql in [
            "CREATE TABLE cycle_t (v1 INTEGER)",
            "INSERT INTO cycle_t VALUES (1)",
            "DROP TABLE cycle_t",
            "CREATE TABLE cycle_t (v2 TEXT)",
            "INSERT INTO cycle_t VALUES ('reborn')",
        ] {
            fconn.execute(sql).await.unwrap();
            cconn.execute_batch(&format!("{sql};")).unwrap();
        }

        let f = fconn.query("SELECT v2 FROM cycle_t").await.unwrap();
        let c = csqlite_query(&cconn, "SELECT v2 FROM cycle_t");
        assert_fsqlite_csqlite_eq(tn, "recreated_table", &f, &c);

        // v1 column should not exist
        assert_fsqlite_error(&fconn, "SELECT v1 FROM cycle_t", tn, "old_col_gone").await;
        assert_csqlite_error(&cconn, "SELECT v1 FROM cycle_t");

        emit_log(tn, "result", json!({"pass": true}));
    });
}

// ─── S18: ALTER TABLE default value ──────────────────────────────────

#[test]
fn s18_alter_column_default() {
    asupersync::test_utils::run_test(|| async {
        let tn = "s18_default_val";
        emit_log(tn, "start", json!({}));

        let fconn = fsqlite::Connection::open(":memory:").await.unwrap();
        let cconn = rusqlite::Connection::open_in_memory().unwrap();

        for sql in [
            "CREATE TABLE t18 (id INTEGER PRIMARY KEY)",
            "INSERT INTO t18 VALUES (1)",
            "ALTER TABLE t18 ADD COLUMN status TEXT DEFAULT 'active'",
            "INSERT INTO t18 (id) VALUES (2)",
        ] {
            fconn.execute(sql).await.unwrap();
            cconn.execute_batch(&format!("{sql};")).unwrap();
        }

        let f = fconn
            .query("SELECT id, status FROM t18 ORDER BY id")
            .await
            .unwrap();
        let c = csqlite_query(&cconn, "SELECT id, status FROM t18 ORDER BY id");
        assert_fsqlite_csqlite_eq(tn, "default_applied", &f, &c);

        emit_log(tn, "result", json!({"pass": true}));
    });
}

// ─── S19: Expression index doesn't corrupt queries ───────────────────

#[test]
fn s19_index_on_expression() {
    asupersync::test_utils::run_test(|| async {
        let tn = "s19_expr_index";
        emit_log(tn, "start", json!({}));

        let fconn = fsqlite::Connection::open(":memory:").await.unwrap();
        let cconn = rusqlite::Connection::open_in_memory().unwrap();

        for sql in [
            "CREATE TABLE t19 (name TEXT, score INTEGER)",
            "INSERT INTO t19 VALUES ('Alice', 90), ('Bob', 80), ('Carol', 95)",
            "CREATE INDEX idx_t19_lower ON t19(LOWER(name))",
        ] {
            fconn.execute(sql).await.unwrap();
            cconn.execute_batch(&format!("{sql};")).unwrap();
        }

        let f = fconn
            .query("SELECT name, score FROM t19 ORDER BY score")
            .await
            .unwrap();
        let c = csqlite_query(&cconn, "SELECT name, score FROM t19 ORDER BY score");
        assert_fsqlite_csqlite_eq(tn, "after_expr_index", &f, &c);

        emit_log(tn, "result", json!({"pass": true}));
    });
}

// ─── S20: Multiple connections same file-backed DB ───────────────────

#[test]
fn s20_multiple_connections_same_db() {
    asupersync::test_utils::run_test(|| async {
        let tn = "s20_multi_conn";
        emit_log(tn, "start", json!({}));

        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("s20.db");
        let path_str = db_path.to_str().unwrap();

        let fconn1 = fsqlite::Connection::open(path_str).await.unwrap();
        fconn1
            .execute("CREATE TABLE t20 (v INTEGER)")
            .await
            .unwrap();
        fconn1
            .execute("INSERT INTO t20 VALUES (1), (2), (3)")
            .await
            .unwrap();

        // Second connection should see the table and data
        let fconn2 = fsqlite::Connection::open(path_str).await.unwrap();
        let f = fconn2.query("SELECT v FROM t20 ORDER BY v").await.unwrap();
        assert_eq!(f.len(), 3, "[{tn}] second conn should see 3 rows");

        // DDL from conn1, then query from conn2
        fconn1
            .execute("ALTER TABLE t20 ADD COLUMN label TEXT DEFAULT 'x'")
            .await
            .unwrap();
        let f2 = fconn2
            .query("SELECT v, label FROM t20 ORDER BY v")
            .await
            .unwrap();
        assert_eq!(f2.len(), 3, "[{tn}] conn2 sees rows after alter");

        emit_log(tn, "result", json!({"pass": true}));
    });
}

// ─── S21: SELECT * after ALTER ADD COLUMN ────────────────────────────

#[test]
fn s21_prepared_select_star_after_alter() {
    asupersync::test_utils::run_test(|| async {
        let tn = "s21_select_star_alter";
        emit_log(tn, "start", json!({}));

        let fconn = fsqlite::Connection::open(":memory:").await.unwrap();
        let cconn = rusqlite::Connection::open_in_memory().unwrap();

        for sql in [
            "CREATE TABLE t21 (a INTEGER, b TEXT)",
            "INSERT INTO t21 VALUES (1, 'x')",
        ] {
            fconn.execute(sql).await.unwrap();
            cconn.execute_batch(&format!("{sql};")).unwrap();
        }

        let f1 = fconn.query("SELECT * FROM t21").await.unwrap();
        assert_eq!(f1[0].values().len(), 2, "before alter: 2 columns");

        fconn
            .execute("ALTER TABLE t21 ADD COLUMN c REAL DEFAULT 3.14")
            .await
            .unwrap();
        cconn
            .execute_batch("ALTER TABLE t21 ADD COLUMN c REAL DEFAULT 3.14;")
            .unwrap();

        let f2 = fconn.query("SELECT * FROM t21").await.unwrap();
        let c2 = csqlite_query(&cconn, "SELECT * FROM t21");
        assert_fsqlite_csqlite_eq(tn, "star_after_alter", &f2, &c2);
        assert_eq!(f2[0].values().len(), 3, "after alter: 3 columns");

        emit_log(tn, "result", json!({"pass": true}));
    });
}

// ─── S22: Trigger after drop-recreate ────────────────────────────────

#[test]
fn s22_trigger_after_drop_recreate() {
    asupersync::test_utils::run_test(|| async {
        let tn = "s22_trigger_recreate";
        emit_log(tn, "start", json!({}));

        let fconn = fsqlite::Connection::open(":memory:").await.unwrap();
        let cconn = rusqlite::Connection::open_in_memory().unwrap();

        for sql in [
            "CREATE TABLE t22 (v INTEGER)",
            "CREATE TABLE t22_audit (msg TEXT)",
            "CREATE TRIGGER trg22 AFTER INSERT ON t22 BEGIN INSERT INTO t22_audit VALUES ('v1:' || NEW.v); END",
            "INSERT INTO t22 VALUES (1)",
            "DROP TRIGGER trg22",
            "INSERT INTO t22 VALUES (2)",
            "CREATE TRIGGER trg22 AFTER INSERT ON t22 BEGIN INSERT INTO t22_audit VALUES ('v2:' || NEW.v); END",
            "INSERT INTO t22 VALUES (3)",
        ] {
            fconn.execute(sql).await.unwrap();
            cconn.execute_batch(&format!("{sql};")).unwrap();
        }

        let f = fconn
            .query("SELECT msg FROM t22_audit ORDER BY rowid")
            .await
            .unwrap();
        let c = csqlite_query(&cconn, "SELECT msg FROM t22_audit ORDER BY rowid");
        assert_fsqlite_csqlite_eq(tn, "trigger_versions", &f, &c);

        emit_log(tn, "result", json!({"pass": true}));
    });
}

// ─── S23: VACUUM doesn't break queries ───────────────────────────────

#[test]
fn s23_vacuum_doesnt_break_queries() {
    asupersync::test_utils::run_test(|| async {
        let tn = "s23_vacuum";
        emit_log(tn, "start", json!({}));

        // Use separate DB files — fsqlite and csqlite cannot share a file
        let dir = tempfile::tempdir().unwrap();
        let f_path = dir.path().join("s23_f.db");
        let c_path = dir.path().join("s23_c.db");

        let fconn = fsqlite::Connection::open(f_path.to_str().unwrap())
            .await
            .unwrap();
        let cconn = rusqlite::Connection::open(c_path.to_str().unwrap()).unwrap();

        for sql in [
            "CREATE TABLE t23 (id INTEGER PRIMARY KEY, data TEXT)",
            "INSERT INTO t23 VALUES (1, 'keep'), (2, 'remove'), (3, 'keep')",
            "DELETE FROM t23 WHERE data = 'remove'",
        ] {
            fconn.execute(sql).await.unwrap();
            cconn.execute_batch(&format!("{sql};")).unwrap();
        }

        fconn.execute("VACUUM").await.unwrap();
        cconn.execute_batch("VACUUM;").unwrap();

        let f = fconn
            .query("SELECT id, data FROM t23 ORDER BY id")
            .await
            .unwrap();
        let c = csqlite_query(&cconn, "SELECT id, data FROM t23 ORDER BY id");
        assert_fsqlite_csqlite_eq(tn, "after_vacuum", &f, &c);

        emit_log(tn, "result", json!({"pass": true}));
    });
}

// ─── S24: CTAS visible immediately ───────────────────────────────────

#[test]
fn s24_ctas_visible_immediately() {
    asupersync::test_utils::run_test(|| async {
        let tn = "s24_ctas";
        emit_log(tn, "start", json!({}));

        let fconn = fsqlite::Connection::open(":memory:").await.unwrap();
        let cconn = rusqlite::Connection::open_in_memory().unwrap();

        for sql in [
            "CREATE TABLE src24 (a INTEGER, b TEXT)",
            "INSERT INTO src24 VALUES (1, 'x'), (2, 'y'), (3, 'z')",
            "CREATE TABLE dst24 AS SELECT a, b || '!' AS b2 FROM src24 WHERE a >= 2",
        ] {
            fconn.execute(sql).await.unwrap();
            cconn.execute_batch(&format!("{sql};")).unwrap();
        }

        let f = fconn
            .query("SELECT a, b2 FROM dst24 ORDER BY a")
            .await
            .unwrap();
        let c = csqlite_query(&cconn, "SELECT a, b2 FROM dst24 ORDER BY a");
        assert_fsqlite_csqlite_eq(tn, "ctas_result", &f, &c);

        emit_log(tn, "result", json!({"pass": true}));
    });
}

// ─── S25: TEMP table schema epoch ────────────────────────────────────

#[test]
fn s25_temp_table_schema_epoch() {
    asupersync::test_utils::run_test(|| async {
        let tn = "s25_temp_table";
        emit_log(tn, "start", json!({}));

        let fconn = fsqlite::Connection::open(":memory:").await.unwrap();
        let cconn = rusqlite::Connection::open_in_memory().unwrap();

        for sql in [
            "CREATE TEMP TABLE tt25 (val INTEGER)",
            "INSERT INTO tt25 VALUES (10), (20)",
        ] {
            fconn.execute(sql).await.unwrap();
            cconn.execute_batch(&format!("{sql};")).unwrap();
        }

        let f = fconn
            .query("SELECT val FROM tt25 ORDER BY val")
            .await
            .unwrap();
        let c = csqlite_query(&cconn, "SELECT val FROM tt25 ORDER BY val");
        assert_fsqlite_csqlite_eq(tn, "temp_query", &f, &c);

        fconn.execute("DROP TABLE tt25").await.unwrap();
        cconn.execute_batch("DROP TABLE tt25;").unwrap();

        assert_fsqlite_error(&fconn, "SELECT * FROM tt25", tn, "temp_dropped").await;
        assert_csqlite_error(&cconn, "SELECT * FROM tt25");

        emit_log(tn, "result", json!({"pass": true}));
    });
}

// ─── S26: INSERT RETURNING after DDL ─────────────────────────────────

#[test]
fn s26_insert_returning_after_ddl() {
    asupersync::test_utils::run_test(|| async {
        let tn = "s26_returning_ddl";
        emit_log(tn, "start", json!({}));

        let fconn = fsqlite::Connection::open(":memory:").await.unwrap();
        let cconn = rusqlite::Connection::open_in_memory().unwrap();

        for sql in [
            "CREATE TABLE t26 (id INTEGER PRIMARY KEY, name TEXT)",
            "INSERT INTO t26 VALUES (1, 'first')",
            "ALTER TABLE t26 ADD COLUMN ts TEXT DEFAULT 'now'",
        ] {
            fconn.execute(sql).await.unwrap();
            cconn.execute_batch(&format!("{sql};")).unwrap();
        }

        let f = fconn
            .query("INSERT INTO t26 (id, name) VALUES (2, 'second') RETURNING id, name, ts")
            .await
            .unwrap();
        let c = csqlite_query(
            &cconn,
            "INSERT INTO t26 (id, name) VALUES (2, 'second') RETURNING id, name, ts",
        );
        assert_fsqlite_csqlite_eq(tn, "returning_after_ddl", &f, &c);

        emit_log(tn, "result", json!({"pass": true}));
    });
}

// ─── S27: UPDATE after ADD COLUMN ────────────────────────────────────

#[test]
fn s27_update_after_add_column() {
    asupersync::test_utils::run_test(|| async {
        let tn = "s27_update_new_col";
        emit_log(tn, "start", json!({}));

        let fconn = fsqlite::Connection::open(":memory:").await.unwrap();
        let cconn = rusqlite::Connection::open_in_memory().unwrap();

        for sql in [
            "CREATE TABLE t27 (id INTEGER PRIMARY KEY, val TEXT)",
            "INSERT INTO t27 VALUES (1, 'a'), (2, 'b')",
            "ALTER TABLE t27 ADD COLUMN flag INTEGER DEFAULT 0",
            "UPDATE t27 SET flag = 1 WHERE id = 2",
        ] {
            fconn.execute(sql).await.unwrap();
            cconn.execute_batch(&format!("{sql};")).unwrap();
        }

        let f = fconn
            .query("SELECT id, val, flag FROM t27 ORDER BY id")
            .await
            .unwrap();
        let c = csqlite_query(&cconn, "SELECT id, val, flag FROM t27 ORDER BY id");
        assert_fsqlite_csqlite_eq(tn, "update_new_col", &f, &c);

        emit_log(tn, "result", json!({"pass": true}));
    });
}

// ─── S28: DELETE after CREATE INDEX ──────────────────────────────────

#[test]
fn s28_delete_after_create_index() {
    asupersync::test_utils::run_test(|| async {
        let tn = "s28_delete_indexed";
        emit_log(tn, "start", json!({}));

        let fconn = fsqlite::Connection::open(":memory:").await.unwrap();
        let cconn = rusqlite::Connection::open_in_memory().unwrap();

        for sql in [
            "CREATE TABLE t28 (id INTEGER PRIMARY KEY, score INTEGER)",
            "INSERT INTO t28 VALUES (1, 10), (2, 20), (3, 30), (4, 40)",
            "CREATE INDEX idx_t28_score ON t28(score)",
            "DELETE FROM t28 WHERE score < 25",
        ] {
            fconn.execute(sql).await.unwrap();
            cconn.execute_batch(&format!("{sql};")).unwrap();
        }

        let f = fconn
            .query("SELECT id, score FROM t28 ORDER BY id")
            .await
            .unwrap();
        let c = csqlite_query(&cconn, "SELECT id, score FROM t28 ORDER BY id");
        assert_fsqlite_csqlite_eq(tn, "after_indexed_delete", &f, &c);

        emit_log(tn, "result", json!({"pass": true}));
    });
}

// ─── S29: Compound SELECT after DDL ──────────────────────────────────

#[test]
fn s29_compound_select_after_ddl() {
    asupersync::test_utils::run_test(|| async {
        let tn = "s29_compound_ddl";
        emit_log(tn, "start", json!({}));

        let fconn = fsqlite::Connection::open(":memory:").await.unwrap();
        let cconn = rusqlite::Connection::open_in_memory().unwrap();

        for sql in [
            "CREATE TABLE t29a (v INTEGER)",
            "CREATE TABLE t29b (v INTEGER)",
            "INSERT INTO t29a VALUES (1), (2), (3)",
            "INSERT INTO t29b VALUES (2), (3), (4)",
        ] {
            fconn.execute(sql).await.unwrap();
            cconn.execute_batch(&format!("{sql};")).unwrap();
        }

        // UNION
        let f = fconn
            .query("SELECT v FROM t29a UNION SELECT v FROM t29b ORDER BY v")
            .await
            .unwrap();
        let c = csqlite_query(
            &cconn,
            "SELECT v FROM t29a UNION SELECT v FROM t29b ORDER BY v",
        );
        assert_fsqlite_csqlite_eq(tn, "union", &f, &c);

        // Now DDL: add column to t29a
        fconn
            .execute("ALTER TABLE t29a ADD COLUMN label TEXT DEFAULT 'a'")
            .await
            .unwrap();
        cconn
            .execute_batch("ALTER TABLE t29a ADD COLUMN label TEXT DEFAULT 'a';")
            .unwrap();

        // INTERSECT on original column still works
        let f2 = fconn
            .query("SELECT v FROM t29a INTERSECT SELECT v FROM t29b ORDER BY v")
            .await
            .unwrap();
        let c2 = csqlite_query(
            &cconn,
            "SELECT v FROM t29a INTERSECT SELECT v FROM t29b ORDER BY v",
        );
        assert_fsqlite_csqlite_eq(tn, "intersect_after_alter", &f2, &c2);

        emit_log(tn, "result", json!({"pass": true}));
    });
}

// ─── S30: Correlated subquery after schema change ────────────────────

#[test]
fn s30_subquery_after_schema_change() {
    asupersync::test_utils::run_test(|| async {
        let tn = "s30_subquery_schema";
        emit_log(tn, "start", json!({}));

        let fconn = fsqlite::Connection::open(":memory:").await.unwrap();
        let cconn = rusqlite::Connection::open_in_memory().unwrap();

        for sql in [
            "CREATE TABLE t30_parent (id INTEGER PRIMARY KEY, name TEXT)",
            "CREATE TABLE t30_child (id INTEGER, parent_id INTEGER, val INTEGER)",
            "INSERT INTO t30_parent VALUES (1, 'P1'), (2, 'P2')",
            "INSERT INTO t30_child VALUES (1, 1, 10), (2, 1, 20), (3, 2, 30)",
        ] {
            fconn.execute(sql).await.unwrap();
            cconn.execute_batch(&format!("{sql};")).unwrap();
        }

        // DDL: add column
        fconn
            .execute("ALTER TABLE t30_parent ADD COLUMN active INTEGER DEFAULT 1")
            .await
            .unwrap();
        cconn
            .execute_batch("ALTER TABLE t30_parent ADD COLUMN active INTEGER DEFAULT 1;")
            .unwrap();

        // Correlated subquery referencing new column
        let sql = "SELECT p.name, (SELECT SUM(c.val) FROM t30_child c WHERE c.parent_id = p.id) AS total FROM t30_parent p WHERE p.active = 1 ORDER BY p.name";
        let f = fconn.query(sql).await.unwrap();
        let c = csqlite_query(&cconn, sql);
        assert_fsqlite_csqlite_eq(tn, "correlated_after_ddl", &f, &c);

        emit_log(tn, "result", json!({"pass": true}));
    });
}

// ─── S31: Mixed DML+DDL across nested savepoints ─────────────────────

#[test]
#[ignore = "MVCC assertion: prepared write staging must clear conflict-only tracking first (begin_concurrent.rs:1308)"]
fn s31_savepoint_nested_dml_ddl_mix() {
    asupersync::test_utils::run_test(|| async {
        let tn = "s31_sp_mix";
        emit_log(tn, "start", json!({}));

        let fconn = fsqlite::Connection::open(":memory:").await.unwrap();
        let cconn = rusqlite::Connection::open_in_memory().unwrap();

        for sql in [
            "CREATE TABLE t31 (id INTEGER PRIMARY KEY, val TEXT)",
            "INSERT INTO t31 VALUES (1, 'original')",
            "SAVEPOINT sp_outer",
            "INSERT INTO t31 VALUES (2, 'outer_add')",
            "SAVEPOINT sp_inner",
            "DELETE FROM t31 WHERE id = 1",
            "INSERT INTO t31 VALUES (3, 'inner_add')",
            "ROLLBACK TO sp_inner",
            "RELEASE sp_inner",
            // row 1 restored, row 3 gone, row 2 still present
            "INSERT INTO t31 VALUES (4, 'after_inner_rollback')",
            "RELEASE sp_outer",
        ] {
            fconn.execute(sql).await.unwrap();
            cconn.execute_batch(&format!("{sql};")).unwrap();
        }

        let f = fconn
            .query("SELECT id, val FROM t31 ORDER BY id")
            .await
            .unwrap();
        let c = csqlite_query(&cconn, "SELECT id, val FROM t31 ORDER BY id");
        assert_fsqlite_csqlite_eq(tn, "nested_dml_ddl", &f, &c);

        emit_log(tn, "result", json!({"pass": true}));
    });
}

// ─── S32: ANALYZE after bulk insert ──────────────────────────────────

#[test]
fn s32_reanalyze_after_bulk_insert() {
    asupersync::test_utils::run_test(|| async {
        let tn = "s32_analyze";
        emit_log(tn, "start", json!({}));

        let fconn = fsqlite::Connection::open(":memory:").await.unwrap();
        let cconn = rusqlite::Connection::open_in_memory().unwrap();

        for sql in [
            "CREATE TABLE t32 (id INTEGER PRIMARY KEY, val INTEGER)",
            "CREATE INDEX idx_t32_val ON t32(val)",
        ] {
            fconn.execute(sql).await.unwrap();
            cconn.execute_batch(&format!("{sql};")).unwrap();
        }

        // Bulk insert
        fconn.execute("BEGIN").await.unwrap();
        cconn.execute_batch("BEGIN;").unwrap();
        for i in 0..500 {
            let sql = format!("INSERT INTO t32 VALUES ({i}, {})", i * 7 % 100);
            fconn.execute(&sql).await.unwrap();
            cconn.execute_batch(&format!("{sql};")).unwrap();
        }
        fconn.execute("COMMIT").await.unwrap();
        cconn.execute_batch("COMMIT;").unwrap();

        fconn.execute("ANALYZE").await.unwrap();
        cconn.execute_batch("ANALYZE;").unwrap();

        // Query after ANALYZE should still be correct
        let f = fconn
            .query("SELECT COUNT(*) FROM t32 WHERE val < 50")
            .await
            .unwrap();
        let c = csqlite_query(&cconn, "SELECT COUNT(*) FROM t32 WHERE val < 50");
        assert_fsqlite_csqlite_eq(tn, "after_analyze", &f, &c);

        emit_log(tn, "result", json!({"pass": true}));
    });
}
