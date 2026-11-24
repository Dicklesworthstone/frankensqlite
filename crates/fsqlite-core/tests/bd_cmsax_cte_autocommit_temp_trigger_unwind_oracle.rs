//! bd-cmsax: statement atomicity for TEMP trigger writes under failing
//! CTE-fed and plain DML, compared live against bundled SQLite 3.54.1
//! (`rusqlite`).
//!
//! Covers:
//! - Exact coordinator repro (`S9_nopre.sql`): failing recursive-CTE autocommit
//!   `INSERT` with main (`l3`) and TEMP (`temp_log`) `AFTER INSERT` triggers,
//!   followed by a failing plain `INSERT ... SELECT`,
//! - Full matrix across `:memory:` and file-backed × direct and prepared:
//!   - CTE-fed (recursive and non-recursive) and plain (`INSERT ... SELECT`
//!     and multi-row `INSERT ... VALUES`),
//!   - autocommit and explicit transaction (`COMMIT` and `ROLLBACK`),
//!   - failure on the first, middle, and last row,
//!   - `OR ABORT` (unwinds main and TEMP writes, preserving pre-existing TEMP
//!     rows) and `OR FAIL` (keeps earlier rows' main and TEMP writes identically
//!     to stock, and still unwinds them on outer `ROLLBACK`),
//! - Sibling paths: single-row `BEFORE INSERT` TEMP trigger + constraint
//!   failure, multi-row `UPDATE` / `WITH ... UPDATE` with TEMP trigger, trigger
//!   `RAISE(ABORT)` vs `RAISE(FAIL)`, and post-failure CTE cleanup + recovery.

use fsqlite_core::connection::Connection;
use fsqlite_types::value::SqliteValue;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum StorageMode {
    Memory,
    File,
}

impl StorageMode {
    const ALL: [Self; 2] = [Self::Memory, Self::File];

    const fn label(self) -> &'static str {
        match self {
            Self::Memory => "memory",
            Self::File => "file",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SourceForm {
    CteRecursive,
    CteNonRecursive,
    PlainSelect,
    PlainValues,
}

impl SourceForm {
    const ALL: [Self; 4] = [
        Self::CteRecursive,
        Self::CteNonRecursive,
        Self::PlainSelect,
        Self::PlainValues,
    ];

    const fn label(self) -> &'static str {
        match self {
            Self::CteRecursive => "cte_recursive",
            Self::CteNonRecursive => "cte_nonrecursive",
            Self::PlainSelect => "plain_select",
            Self::PlainValues => "plain_values",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum FailureRow {
    First,
    Middle,
    Last,
}

impl FailureRow {
    const ALL: [Self; 3] = [Self::First, Self::Middle, Self::Last];

    const fn label(self) -> &'static str {
        match self {
            Self::First => "first_row",
            Self::Middle => "middle_row",
            Self::Last => "last_row",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ConflictMode {
    Abort,
    Fail,
}

impl ConflictMode {
    const ALL: [Self; 2] = [Self::Abort, Self::Fail];

    const fn label(self) -> &'static str {
        match self {
            Self::Abort => "OR ABORT",
            Self::Fail => "OR FAIL",
        }
    }

    const fn sql_verb(self) -> &'static str {
        match self {
            Self::Abort => "INSERT",
            Self::Fail => "INSERT OR FAIL",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TxnMode {
    Autocommit,
    InTxnCommit,
    InTxnRollback,
}

impl TxnMode {
    const ALL: [Self; 3] = [Self::Autocommit, Self::InTxnCommit, Self::InTxnRollback];

    const fn label(self) -> &'static str {
        match self {
            Self::Autocommit => "autocommit",
            Self::InTxnCommit => "in_txn_commit",
            Self::InTxnRollback => "in_txn_rollback",
        }
    }
}

struct DualConnections {
    _dir: Option<tempfile::TempDir>,
    stock: rusqlite::Connection,
    fsqlite: Connection,
}

impl DualConnections {
    async fn open(storage: StorageMode) -> Self {
        match storage {
            StorageMode::Memory => Self {
                _dir: None,
                stock: rusqlite::Connection::open_in_memory().expect("open stock :memory:"),
                fsqlite: Connection::open(":memory:").await.expect("open fsqlite :memory:"),
            },
            StorageMode::File => {
                let dir = tempfile::tempdir().expect("create tempdir");
                let stock_path = dir.path().join("stock.db");
                let fsqlite_path = dir.path().join("fsqlite.db");
                let stock = rusqlite::Connection::open(&stock_path).expect("open stock file");
                let fsqlite = Connection::open(fsqlite_path.to_str().expect("utf-8 path"))
                    .await
                    .expect("open fsqlite file");
                Self {
                    _dir: Some(dir),
                    stock,
                    fsqlite,
                }
            }
        }
    }

    async fn exec_ok(&self, sql: &str) {
        self.stock
            .execute_batch(sql)
            .unwrap_or_else(|e| panic!("stock setup failed for `{sql}`: {e}"));
        self.fsqlite
            .execute(sql)
            .await
            .unwrap_or_else(|e| panic!("fsqlite setup failed for `{sql}`: {e}"));
    }

    async fn exec_both_err(&self, sql: &str, prepared: bool, ctx: &str) {
        let stock_res = if prepared {
            self.stock
                .prepare(sql)
                .and_then(|mut stmt| stmt.execute([]))
                .map(|_| ())
                .map_err(|e| e.to_string())
        } else {
            self.stock.execute(sql, []).map(|_| ()).map_err(|e| e.to_string())
        };
        let fsqlite_res = if prepared {
            match self.fsqlite.prepare(sql).await {
                Ok(stmt) => stmt.execute().await.map(|_| ()).map_err(|e| e.to_string()),
                Err(e) => Err(e.to_string()),
            }
        } else {
            self.fsqlite
                .execute(sql)
                .await
                .map(|_| ())
                .map_err(|e| e.to_string())
        };
        assert!(
            stock_res.is_err(),
            "[{ctx}] expected stock to fail for `{sql}`, got Ok"
        );
        assert!(
            fsqlite_res.is_err(),
            "[{ctx}] expected fsqlite to fail for `{sql}`, got Ok (stock err: {stock_res:?})"
        );
    }

    async fn assert_query_matches(&self, sql: &str, ctx: &str) -> Vec<Vec<String>> {
        let stock = stock_rows(&self.stock, sql)
            .unwrap_or_else(|e| panic!("[{ctx}] stock query failed for `{sql}`: {e}"));
        let fsqlite = fsqlite_rows(&self.fsqlite, sql)
            .await
            .unwrap_or_else(|e| panic!("[{ctx}] fsqlite query failed for `{sql}`: {e}"));
        assert_eq!(
            fsqlite, stock,
            "[{ctx}] mismatch on `{sql}`:\n  fsqlite: {fsqlite:?}\n  stock:   {stock:?}"
        );
        stock
    }

    async fn assert_state_matches(&self, ctx: &str) {
        self.assert_query_matches("SELECT id, n FROM t3 ORDER BY id", ctx)
            .await;
        self.assert_query_matches("SELECT id, c FROM l3 ORDER BY rowid", ctx)
            .await;
        self.assert_query_matches("SELECT v FROM temp_log ORDER BY rowid", ctx)
            .await;
        self.assert_query_matches("SELECT count(*), sum(v) FROM temp_log", ctx)
            .await;
    }

    async fn close(self) {
        self.fsqlite.close().await.expect("close fsqlite");
        drop(self.stock);
    }
}

fn render_fsqlite(value: &SqliteValue) -> String {
    match value {
        SqliteValue::Null => "NULL".into(),
        SqliteValue::Integer(n) => n.to_string(),
        SqliteValue::Float(f) => format!("{f:?}"),
        SqliteValue::Text(s) => format!("'{s}'"),
        SqliteValue::Blob(b) => format!("x{b:02x?}"),
    }
}

fn render_stock(value: &rusqlite::types::Value) -> String {
    match value {
        rusqlite::types::Value::Null => "NULL".into(),
        rusqlite::types::Value::Integer(n) => n.to_string(),
        rusqlite::types::Value::Real(f) => format!("{f:?}"),
        rusqlite::types::Value::Text(s) => format!("'{s}'"),
        rusqlite::types::Value::Blob(b) => format!("x{b:02x?}"),
    }
}

fn stock_rows(conn: &rusqlite::Connection, sql: &str) -> Result<Vec<Vec<String>>, String> {
    let mut statement = conn.prepare(sql).map_err(|e| e.to_string())?;
    let columns = statement.column_count();
    statement
        .query_map([], |row| {
            Ok((0..columns)
                .map(|i| render_stock(&row.get_unwrap::<_, rusqlite::types::Value>(i)))
                .collect())
        })
        .map_err(|e| e.to_string())?
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| e.to_string())
}

async fn fsqlite_rows(conn: &Connection, sql: &str) -> Result<Vec<Vec<String>>, String> {
    let rows = conn.query(sql).await.map_err(|e| e.to_string())?;
    Ok(rows
        .iter()
        .map(|row| row.values().iter().map(render_fsqlite).collect())
        .collect())
}

fn build_failing_insert_sql(
    form: SourceForm,
    conflict: ConflictMode,
    failure_row: FailureRow,
) -> String {
    let verb = conflict.sql_verb();
    // `t3` has `CHECK (n >= 0)` so first-row failure trips on row 1 without
    // requiring an earlier autocommit write to `t3` (keeping in-txn scenarios
    // orthogonal to unlanded bd-732ft). Middle and last rows trip UNIQUE on id=21.
    match (form, failure_row) {
        (SourceForm::CteRecursive, FailureRow::First) => format!(
            "WITH RECURSIVE s(i) AS (SELECT 21 UNION ALL SELECT i + 1 FROM s WHERE i < 25) \
             {verb} INTO t3 SELECT i, CASE WHEN i = 21 THEN -1 ELSE i END FROM s"
        ),
        (SourceForm::CteRecursive, FailureRow::Middle) => format!(
            "WITH RECURSIVE s(i) AS (SELECT 21 UNION ALL SELECT i + 1 FROM s WHERE i < 25) \
             {verb} INTO t3 SELECT CASE WHEN i = 23 THEN 21 ELSE i END, i FROM s"
        ),
        (SourceForm::CteRecursive, FailureRow::Last) => format!(
            "WITH RECURSIVE s(i) AS (SELECT 21 UNION ALL SELECT i + 1 FROM s WHERE i < 25) \
             {verb} INTO t3 SELECT CASE WHEN i = 25 THEN 21 ELSE i END, i FROM s"
        ),
        (SourceForm::CteNonRecursive, FailureRow::First) => format!(
            "WITH s(id, n) AS (VALUES (21, -1), (22, 22), (23, 23), (24, 24), (25, 25)) \
             {verb} INTO t3 SELECT id, n FROM s"
        ),
        (SourceForm::CteNonRecursive, FailureRow::Middle) => format!(
            "WITH s(id, n) AS (VALUES (21, 21), (22, 22), (21, 23), (24, 24), (25, 25)) \
             {verb} INTO t3 SELECT id, n FROM s"
        ),
        (SourceForm::CteNonRecursive, FailureRow::Last) => format!(
            "WITH s(id, n) AS (VALUES (21, 21), (22, 22), (23, 23), (24, 24), (21, 25)) \
             {verb} INTO t3 SELECT id, n FROM s"
        ),
        (SourceForm::PlainSelect, FailureRow::First) => format!(
            "{verb} INTO t3 SELECT id, n FROM (\
             SELECT 21 AS id, -1 AS n UNION ALL SELECT 22, 22 UNION ALL SELECT 23, 23 \
             UNION ALL SELECT 24, 24 UNION ALL SELECT 25, 25)"
        ),
        (SourceForm::PlainSelect, FailureRow::Middle) => format!(
            "{verb} INTO t3 SELECT id, n FROM (\
             SELECT 21 AS id, 21 AS n UNION ALL SELECT 22, 22 UNION ALL SELECT 21, 23 \
             UNION ALL SELECT 24, 24 UNION ALL SELECT 25, 25)"
        ),
        (SourceForm::PlainSelect, FailureRow::Last) => format!(
            "{verb} INTO t3 SELECT id, n FROM (\
             SELECT 21 AS id, 21 AS n UNION ALL SELECT 22, 22 UNION ALL SELECT 23, 23 \
             UNION ALL SELECT 24, 24 UNION ALL SELECT 21, 25)"
        ),
        (SourceForm::PlainValues, FailureRow::First) => format!(
            "{verb} INTO t3 VALUES (21, -1), (22, 22), (23, 23), (24, 24), (25, 25)"
        ),
        (SourceForm::PlainValues, FailureRow::Middle) => format!(
            "{verb} INTO t3 VALUES (21, 21), (22, 22), (21, 23), (24, 24), (25, 25)"
        ),
        (SourceForm::PlainValues, FailureRow::Last) => format!(
            "{verb} INTO t3 VALUES (21, 21), (22, 22), (23, 23), (24, 24), (21, 25)"
        ),
    }
}

#[test]
fn bd_cmsax_coordinator_repro_s9_nopre() {
    asupersync::test_utils::run_test(|| async {
        for storage in StorageMode::ALL {
            for prepared in [false, true] {
                let ctx = format!("s9_nopre storage={} prepared={prepared}", storage.label());
                let dual = DualConnections::open(storage).await;
                dual.exec_ok("CREATE TEMP TABLE temp_log(v)").await;
                dual.exec_ok("CREATE TABLE t3(id INTEGER PRIMARY KEY, n INTEGER)")
                    .await;
                dual.exec_ok("CREATE TABLE l3(id, c)").await;
                dual.exec_ok(
                    "CREATE TRIGGER t3i AFTER INSERT ON t3 \
                     BEGIN INSERT INTO l3 VALUES (NEW.id, 0); END",
                )
                .await;
                dual.exec_ok(
                    "CREATE TEMP TRIGGER t3_temp AFTER INSERT ON t3 \
                     BEGIN INSERT INTO temp_log VALUES (-NEW.id); END",
                )
                .await;

                // Failing CTE-fed autocommit INSERT (fails with UNIQUE on last row).
                dual.exec_both_err(
                    "WITH RECURSIVE s(i) AS (SELECT 21 UNION ALL SELECT i + 1 FROM s WHERE i < 25) \
                     INSERT INTO t3 SELECT CASE WHEN i = 25 THEN 21 ELSE i END, i FROM s",
                    prepared,
                    &format!("{ctx} cte_insert"),
                )
                .await;

                let counts = dual
                    .assert_query_matches(
                        "SELECT count(*), sum(v) FROM temp_log",
                        &format!("{ctx} after_cte"),
                    )
                    .await;
                assert_eq!(
                    counts,
                    vec![vec!["0".to_owned(), "NULL".to_owned()]],
                    "[{ctx}] stock and fsqlite must unwind temp_log to 0 rows"
                );
                dual.assert_state_matches(&format!("{ctx} after_cte")).await;

                // Ensure temporary CTE table `s` did not leak after failure.
                assert!(stock_rows(&dual.stock, "SELECT * FROM s").is_err());
                assert!(fsqlite_rows(&dual.fsqlite, "SELECT * FROM s").await.is_err());

                // Failing plain INSERT ... SELECT.
                dual.exec_both_err(
                    "INSERT INTO t3 SELECT CASE WHEN value = 5 THEN 1 ELSE value END, value \
                     FROM (SELECT 1 AS value UNION ALL SELECT 2 UNION ALL SELECT 5)",
                    prepared,
                    &format!("{ctx} plain_insert"),
                )
                .await;
                dual.assert_state_matches(&format!("{ctx} after_plain"))
                    .await;

                // Subsequent valid CTE-fed INSERT must still work cleanly.
                dual.exec_ok(
                    "WITH RECURSIVE s(i) AS (SELECT 1 UNION ALL SELECT i + 1 FROM s WHERE i < 3) \
                     INSERT INTO t3 SELECT i, i * 10 FROM s",
                )
                .await;
                dual.assert_state_matches(&format!("{ctx} after_recovery"))
                    .await;

                dual.close().await;
            }
        }
    });
}

#[test]
fn bd_cmsax_cte_and_plain_atomicity_matrix() {
    asupersync::test_utils::run_test(|| async {
        for storage in StorageMode::ALL {
            for prepared in [false, true] {
                for txn_mode in TxnMode::ALL {
                    for form in SourceForm::ALL {
                        for failure_row in FailureRow::ALL {
                            for conflict in ConflictMode::ALL {
                                let ctx = format!(
                                    "storage={} prepared={prepared} txn={} form={} row={} conflict={}",
                                    storage.label(),
                                    txn_mode.label(),
                                    form.label(),
                                    failure_row.label(),
                                    conflict.label()
                                );
                                let dual = DualConnections::open(storage).await;
                                dual.exec_ok("CREATE TEMP TABLE temp_log(v)").await;
                                dual.exec_ok("INSERT INTO temp_log VALUES (-999)").await;
                                dual.exec_ok(
                                    "CREATE TABLE t3(id INTEGER PRIMARY KEY, n INTEGER CHECK (n >= 0))",
                                )
                                .await;
                                dual.exec_ok("CREATE TABLE l3(id, c)").await;
                                dual.exec_ok(
                                    "CREATE TRIGGER t3i AFTER INSERT ON t3 \
                                     BEGIN INSERT INTO l3 VALUES (NEW.id, 0); END",
                                )
                                .await;
                                dual.exec_ok(
                                    "CREATE TEMP TRIGGER t3_temp AFTER INSERT ON t3 \
                                     BEGIN INSERT INTO temp_log VALUES (-NEW.id); END",
                                )
                                .await;

                                if txn_mode != TxnMode::Autocommit {
                                    dual.exec_ok("BEGIN").await;
                                    dual.exec_ok("INSERT INTO temp_log VALUES (-888)").await;
                                }

                                let sql = build_failing_insert_sql(form, conflict, failure_row);
                                dual.exec_both_err(&sql, prepared, &ctx).await;

                                // Check exact state immediately after the failing statement.
                                dual.assert_state_matches(&format!("{ctx} post_error"))
                                    .await;

                                // Non-vacuous sanity check: under OR FAIL on middle/last row,
                                // earlier rows MUST actually be retained in both stock & fsqlite.
                                if conflict == ConflictMode::Fail
                                    && failure_row != FailureRow::First
                                {
                                    let expected_kept = match failure_row {
                                        FailureRow::Middle => 2,
                                        FailureRow::Last => 4,
                                        FailureRow::First => 0,
                                    };
                                    let pre_rows =
                                        if txn_mode == TxnMode::Autocommit { 1 } else { 2 };
                                    let rows = dual
                                        .assert_query_matches(
                                            "SELECT count(*) FROM temp_log",
                                            &format!("{ctx} non_vacuous_fail_count"),
                                        )
                                        .await;
                                    assert_eq!(
                                        rows,
                                        vec![vec![(pre_rows + expected_kept).to_string()]],
                                        "[{ctx}] OR FAIL must keep earlier temp_log rows"
                                    );
                                }

                                match txn_mode {
                                    TxnMode::Autocommit => {}
                                    TxnMode::InTxnCommit => {
                                        dual.exec_ok("COMMIT").await;
                                        dual.assert_state_matches(&format!("{ctx} post_commit"))
                                            .await;
                                    }
                                    TxnMode::InTxnRollback => {
                                        dual.exec_ok("ROLLBACK").await;
                                        dual.assert_state_matches(&format!("{ctx} post_rollback"))
                                            .await;
                                    }
                                }

                                dual.close().await;
                            }
                        }
                    }
                }
            }
        }
    });
}

#[test]
fn bd_cmsax_sibling_dml_before_trigger_and_raise_matrix() {
    asupersync::test_utils::run_test(|| async {
        for storage in StorageMode::ALL {
            for prepared in [false, true] {
                let ctx = format!("siblings storage={} prepared={prepared}", storage.label());

                // 1. Single-row BEFORE INSERT TEMP trigger + UNIQUE failure (OR ABORT vs OR FAIL).
                for conflict in ConflictMode::ALL {
                    let c = format!("{ctx} before_insert {}", conflict.label());
                    let dual = DualConnections::open(storage).await;
                    dual.exec_ok("CREATE TEMP TABLE temp_log(v)").await;
                    dual.exec_ok("INSERT INTO temp_log VALUES (-999)").await;
                    dual.exec_ok("CREATE TABLE t3(id INTEGER PRIMARY KEY, n INTEGER)")
                        .await;
                    dual.exec_ok("CREATE TABLE l3(id, c)").await;
                    dual.exec_ok("INSERT INTO t3 VALUES (1, 10)").await;
                    dual.exec_ok(
                        "CREATE TEMP TRIGGER t3_before BEFORE INSERT ON t3 \
                         BEGIN INSERT INTO temp_log VALUES (-NEW.id); END",
                    )
                    .await;
                    let sql = format!("{} INTO t3 VALUES (1, 20)", conflict.sql_verb());
                    dual.exec_both_err(&sql, prepared, &c).await;
                    dual.assert_state_matches(&c).await;
                    dual.close().await;
                }

                // 2. Multi-row UPDATE (plain and CTE-fed) with TEMP trigger failing on later row.
                for (label, sql) in [
                    (
                        "plain_update_abort",
                        "UPDATE t3 SET id = CASE WHEN id = 3 THEN 11 ELSE id + 10 END",
                    ),
                    (
                        "plain_update_fail",
                        "UPDATE OR FAIL t3 SET id = CASE WHEN id = 3 THEN 11 ELSE id + 10 END",
                    ),
                    (
                        "cte_update_abort",
                        "WITH target(k) AS (SELECT id FROM t3) \
                         UPDATE t3 SET id = CASE WHEN id = 3 THEN 11 ELSE id + 10 END \
                         WHERE id IN (SELECT k FROM target)",
                    ),
                    (
                        "cte_update_fail",
                        "WITH target(k) AS (SELECT id FROM t3) \
                         UPDATE OR FAIL t3 SET id = CASE WHEN id = 3 THEN 11 ELSE id + 10 END \
                         WHERE id IN (SELECT k FROM target)",
                    ),
                ] {
                    let c = format!("{ctx} {label}");
                    let dual = DualConnections::open(storage).await;
                    dual.exec_ok("CREATE TEMP TABLE temp_log(v)").await;
                    dual.exec_ok("INSERT INTO temp_log VALUES (-999)").await;
                    dual.exec_ok("CREATE TABLE t3(id INTEGER PRIMARY KEY, n INTEGER)")
                        .await;
                    dual.exec_ok("CREATE TABLE l3(id, c)").await;
                    dual.exec_ok("INSERT INTO t3 VALUES (1, 1), (2, 2), (3, 3), (4, 4)")
                        .await;
                    dual.exec_ok(
                        "CREATE TRIGGER t3u AFTER UPDATE ON t3 \
                         BEGIN INSERT INTO l3 VALUES (NEW.id, OLD.id); END",
                    )
                    .await;
                    dual.exec_ok(
                        "CREATE TEMP TRIGGER t3u_temp AFTER UPDATE ON t3 \
                         BEGIN INSERT INTO temp_log VALUES (-NEW.id); END",
                    )
                    .await;
                    dual.exec_both_err(sql, prepared, &c).await;
                    dual.assert_state_matches(&c).await;
                    dual.close().await;
                }

                // 3. CTE-fed INSERT where trigger raises RAISE(ABORT) vs RAISE(FAIL) on row 3.
                for (label, raise_action) in [
                    ("cte_raise_abort", "ABORT"),
                    ("cte_raise_fail", "FAIL"),
                ] {
                    let c = format!("{ctx} {label}");
                    let dual = DualConnections::open(storage).await;
                    dual.exec_ok("CREATE TEMP TABLE temp_log(v)").await;
                    dual.exec_ok("INSERT INTO temp_log VALUES (-999)").await;
                    dual.exec_ok("CREATE TABLE t3(id INTEGER PRIMARY KEY, n INTEGER)")
                        .await;
                    dual.exec_ok("CREATE TABLE l3(id, c)").await;
                    dual.exec_ok(
                        "CREATE TRIGGER t3i AFTER INSERT ON t3 \
                         BEGIN INSERT INTO l3 VALUES (NEW.id, 0); END",
                    )
                    .await;
                    dual.exec_ok(
                        "CREATE TEMP TRIGGER t3_temp AFTER INSERT ON t3 \
                         BEGIN INSERT INTO temp_log VALUES (-NEW.id); END",
                    )
                    .await;
                    dual.exec_ok(&format!(
                        "CREATE TRIGGER t3_stop BEFORE INSERT ON t3 WHEN NEW.id = 23 \
                         BEGIN SELECT RAISE({raise_action}, 'stop_at_23'); END"
                    ))
                    .await;
                    dual.exec_both_err(
                        "WITH RECURSIVE s(i) AS (SELECT 21 UNION ALL SELECT i + 1 FROM s WHERE i < 25) \
                         INSERT INTO t3 SELECT i, i FROM s",
                        prepared,
                        &c,
                    )
                    .await;
                    dual.assert_state_matches(&c).await;
                    dual.close().await;
                }
            }
        }
    });
}
