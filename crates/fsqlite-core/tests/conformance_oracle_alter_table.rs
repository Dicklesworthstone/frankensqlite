//! Conformance oracle tests — ALTER TABLE variants (cc_3)
//!
//! Schema mutation is one of the trickiest paths in any SQLite implementation:
//! ADD COLUMN must back-fill existing rows with the column default (and apply
//! the new column's affinity to that default), RENAME COLUMN/TABLE must rewrite
//! references, and DROP COLUMN (3.35+) must repack the row image. rusqlite is
//! used as the oracle for both query results and `PRAGMA table_info`.

use fsqlite_core::connection::Connection;
use fsqlite_types::value::SqliteValue;

async fn oracle_compare(
    fconn: &Connection,
    rconn: &rusqlite::Connection,
    queries: &[&str],
) -> Vec<String> {
    let mut mismatches = Vec::new();
    for query in queries {
        let frank_result = fconn.query(query).await;
        let csql_result: std::result::Result<Vec<Vec<String>>, String> = (|| {
            let mut stmt = rconn.prepare(query).map_err(|e| format!("prepare: {e}"))?;
            let col_count = stmt.column_count();
            let rows: Vec<Vec<String>> = stmt
                .query_map([], |row| {
                    let mut vals = Vec::new();
                    for i in 0..col_count {
                        let v: rusqlite::types::Value = row.get_unwrap(i);
                        let s = match v {
                            rusqlite::types::Value::Null => "NULL".to_owned(),
                            rusqlite::types::Value::Integer(n) => n.to_string(),
                            rusqlite::types::Value::Real(f) => format!("{f}"),
                            rusqlite::types::Value::Text(s) => format!("'{s}'"),
                            rusqlite::types::Value::Blob(b) => format!(
                                "X'{}'",
                                b.iter().map(|x| format!("{x:02X}")).collect::<String>()
                            ),
                        };
                        vals.push(s);
                    }
                    Ok(vals)
                })
                .map_err(|e| format!("query: {e}"))?
                .collect::<std::result::Result<Vec<_>, _>>()
                .map_err(|e| format!("row: {e}"))?;
            Ok(rows)
        })();
        match (frank_result, csql_result) {
            (Ok(rows), Ok(csql_rows)) => {
                let frank_strs: Vec<Vec<String>> = rows
                    .iter()
                    .map(|row| {
                        row.values()
                            .iter()
                            .map(|v| match v {
                                SqliteValue::Null => "NULL".to_owned(),
                                SqliteValue::Integer(n) => n.to_string(),
                                SqliteValue::Float(f) => format!("{f}"),
                                SqliteValue::Text(s) => format!("'{s}'"),
                                SqliteValue::Blob(b) => format!(
                                    "X'{}'",
                                    b.iter().map(|x| format!("{x:02X}")).collect::<String>()
                                ),
                            })
                            .collect()
                    })
                    .collect();
                if frank_strs != csql_rows {
                    mismatches.push(format!(
                        "MISMATCH: {query}\n  frank: {frank_strs:?}\n  csql:  {csql_rows:?}"
                    ));
                }
            }
            (Ok(rows), Err(csql_err)) => {
                mismatches.push(format!(
                    "DIVERGE: {query}\n  frank: OK ({} rows)\n  csql:  ERROR({csql_err})",
                    rows.len()
                ));
            }
            (Err(e), Ok(csql_rows)) => {
                mismatches.push(format!(
                    "PAIR_FRANK_ERROR: {query}\n  frank: ERROR({e})\n  csql:  {csql_rows:?}"
                ));
            }
            (Err(_), Err(_)) => {}
        }
    }
    mismatches
}

fn assert_no_mismatches(mismatches: &[String], label: &str) {
    if !mismatches.is_empty() {
        for m in mismatches {
            eprintln!("{m}\n");
        }
        panic!("{} {label} mismatch(es)", mismatches.len());
    }
}

/// Apply identical statements to both engines, asserting agreement on whether
/// each statement succeeds. A statement that succeeds on one engine but fails
/// on the other is itself a divergence worth surfacing.
async fn apply_checked(
    fconn: &Connection,
    rconn: &rusqlite::Connection,
    stmts: &[&str],
) -> Vec<String> {
    let mut diverged = Vec::new();
    for s in stmts {
        let f = fconn.execute(s).await;
        let r = rconn.execute_batch(s);
        match (f, r) {
            (Ok(_), Ok(())) | (Err(_), Err(_)) => {}
            (Ok(_), Err(e)) => {
                diverged.push(format!(
                    "STMT_DIVERGE: {s}\n  frank: OK\n  csql:  ERROR({e})"
                ));
            }
            (Err(e), Ok(())) => {
                diverged.push(format!(
                    "STMT_DIVERGE: {s}\n  frank: ERROR({e})\n  csql:  OK"
                ));
            }
        }
    }
    diverged
}

/// DDL/DML that must succeed on both engines.
async fn apply(fconn: &Connection, rconn: &rusqlite::Connection, stmts: &[&str]) {
    for s in stmts {
        fconn.execute(s).await.unwrap();
        rconn.execute_batch(s).unwrap();
    }
}

#[test]
fn alter_add_column_backfills_constant_default() {
    asupersync::test_utils::run_test(|| async {
        let fconn = Connection::open(":memory:").await.unwrap();
        let rconn = rusqlite::Connection::open_in_memory().unwrap();
        apply(
            &fconn,
            &rconn,
            &[
                "CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)",
                "INSERT INTO t VALUES (1, 'a'), (2, 'b'), (3, 'c')",
                "ALTER TABLE t ADD COLUMN score INTEGER DEFAULT 0",
                "ALTER TABLE t ADD COLUMN label TEXT DEFAULT 'none'",
                "ALTER TABLE t ADD COLUMN ratio REAL DEFAULT 1.5",
            ],
        )
        .await;
        let m = oracle_compare(
            &fconn,
            &rconn,
            &[
                "SELECT id, name, score, label, ratio FROM t ORDER BY id",
                "SELECT typeof(score), typeof(label), typeof(ratio) FROM t LIMIT 1",
                "PRAGMA table_info(t)",
            ],
        )
        .await;
        assert_no_mismatches(&m, "alter_add_column_backfills_constant_default");
    });
}

#[test]
fn alter_add_column_null_default_when_unspecified() {
    asupersync::test_utils::run_test(|| async {
        let fconn = Connection::open(":memory:").await.unwrap();
        let rconn = rusqlite::Connection::open_in_memory().unwrap();
        apply(
            &fconn,
            &rconn,
            &[
                "CREATE TABLE t (id INTEGER PRIMARY KEY)",
                "INSERT INTO t VALUES (1), (2)",
                "ALTER TABLE t ADD COLUMN extra TEXT",
                "INSERT INTO t (id, extra) VALUES (3, 'x')",
            ],
        )
        .await;
        let m = oracle_compare(
            &fconn,
            &rconn,
            &[
                "SELECT id, extra, typeof(extra) FROM t ORDER BY id",
                "SELECT count(*) FROM t WHERE extra IS NULL",
            ],
        )
        .await;
        assert_no_mismatches(&m, "alter_add_column_null_default_when_unspecified");
    });
}

#[test]
fn alter_add_column_default_affinity_coercion() {
    asupersync::test_utils::run_test(|| async {
        let fconn = Connection::open(":memory:").await.unwrap();
        let rconn = rusqlite::Connection::open_in_memory().unwrap();
        apply(
            &fconn,
            &rconn,
            &[
                "CREATE TABLE t (id INTEGER PRIMARY KEY)",
                "INSERT INTO t VALUES (1)",
                // Default '42' should be stored under INTEGER affinity as int 42.
                "ALTER TABLE t ADD COLUMN n INTEGER DEFAULT '42'",
                // Default 100 should be stored under TEXT affinity as text '100'.
                "ALTER TABLE t ADD COLUMN s TEXT DEFAULT 100",
            ],
        )
        .await;
        let m = oracle_compare(
            &fconn,
            &rconn,
            &["SELECT id, typeof(n), n, typeof(s), s FROM t ORDER BY id"],
        )
        .await;
        assert_no_mismatches(&m, "alter_add_column_default_affinity_coercion");
    });
}

#[test]
fn alter_rename_table() {
    asupersync::test_utils::run_test(|| async {
        let fconn = Connection::open(":memory:").await.unwrap();
        let rconn = rusqlite::Connection::open_in_memory().unwrap();
        apply(
            &fconn,
            &rconn,
            &[
                "CREATE TABLE old_t (id INTEGER PRIMARY KEY, v TEXT)",
                "INSERT INTO old_t VALUES (1, 'one'), (2, 'two')",
                "ALTER TABLE old_t RENAME TO new_t",
                "INSERT INTO new_t VALUES (3, 'three')",
            ],
        )
        .await;
        let m = oracle_compare(
            &fconn,
            &rconn,
            &[
                "SELECT id, v FROM new_t ORDER BY id",
                "SELECT name FROM sqlite_master WHERE type='table' AND name='new_t'",
                "SELECT count(*) FROM sqlite_master WHERE type='table' AND name='old_t'",
            ],
        )
        .await;
        assert_no_mismatches(&m, "alter_rename_table");
    });
}

#[test]
fn alter_rename_column() {
    asupersync::test_utils::run_test(|| async {
        let fconn = Connection::open(":memory:").await.unwrap();
        let rconn = rusqlite::Connection::open_in_memory().unwrap();
        apply(
            &fconn,
            &rconn,
            &[
                "CREATE TABLE t (id INTEGER PRIMARY KEY, old_name TEXT, qty INTEGER)",
                "INSERT INTO t VALUES (1, 'a', 10), (2, 'b', 20)",
                "ALTER TABLE t RENAME COLUMN old_name TO new_name",
            ],
        )
        .await;
        let m = oracle_compare(
            &fconn,
            &rconn,
            &[
                "SELECT id, new_name, qty FROM t ORDER BY id",
                "SELECT new_name FROM t WHERE qty > 15",
                "PRAGMA table_info(t)",
            ],
        )
        .await;
        assert_no_mismatches(&m, "alter_rename_column");
    });
}

#[test]
fn alter_drop_column() {
    asupersync::test_utils::run_test(|| async {
        let fconn = Connection::open(":memory:").await.unwrap();
        let rconn = rusqlite::Connection::open_in_memory().unwrap();
        // DROP COLUMN is SQLite 3.35+; surface a divergence if frank lacks it.
        let setup = [
            "CREATE TABLE t (id INTEGER PRIMARY KEY, a TEXT, b INTEGER, c REAL)",
            "INSERT INTO t VALUES (1, 'x', 5, 1.1), (2, 'y', 6, 2.2)",
            "ALTER TABLE t DROP COLUMN b",
        ];
        let diverged = apply_checked(&fconn, &rconn, &setup).await;
        assert_no_mismatches(&diverged, "alter_drop_column(setup)");
        let m = oracle_compare(
            &fconn,
            &rconn,
            &["SELECT id, a, c FROM t ORDER BY id", "PRAGMA table_info(t)"],
        )
        .await;
        assert_no_mismatches(&m, "alter_drop_column");
    });
}

#[test]
fn alter_add_column_not_null_default() {
    asupersync::test_utils::run_test(|| async {
        let fconn = Connection::open(":memory:").await.unwrap();
        let rconn = rusqlite::Connection::open_in_memory().unwrap();
        apply(
            &fconn,
            &rconn,
            &[
                "CREATE TABLE t (id INTEGER PRIMARY KEY)",
                "INSERT INTO t VALUES (1), (2)",
                "ALTER TABLE t ADD COLUMN flag INTEGER NOT NULL DEFAULT 7",
            ],
        )
        .await;
        let m = oracle_compare(
            &fconn,
            &rconn,
            &[
                "SELECT id, flag FROM t ORDER BY id",
                "SELECT count(*) FROM t WHERE flag = 7",
            ],
        )
        .await;
        assert_no_mismatches(&m, "alter_add_column_not_null_default");
    });
}

#[test]
fn alter_sequence_then_index_and_query() {
    asupersync::test_utils::run_test(|| async {
        let fconn = Connection::open(":memory:").await.unwrap();
        let rconn = rusqlite::Connection::open_in_memory().unwrap();
        apply(
            &fconn,
            &rconn,
            &[
                "CREATE TABLE t (id INTEGER PRIMARY KEY, a TEXT)",
                "INSERT INTO t VALUES (1, 'alpha'), (2, 'beta')",
                "ALTER TABLE t ADD COLUMN grp INTEGER DEFAULT 1",
                "ALTER TABLE t RENAME COLUMN a TO label",
                "CREATE INDEX idx_grp ON t(grp)",
                "INSERT INTO t (id, label, grp) VALUES (3, 'gamma', 2)",
                "UPDATE t SET grp = 2 WHERE id = 1",
            ],
        )
        .await;
        let m = oracle_compare(
            &fconn,
            &rconn,
            &[
                "SELECT id, label, grp FROM t ORDER BY id",
                "SELECT label FROM t WHERE grp = 2 ORDER BY id",
                "SELECT grp, count(*) FROM t GROUP BY grp ORDER BY grp",
            ],
        )
        .await;
        assert_no_mismatches(&m, "alter_sequence_then_index_and_query");
    });
}

/// bd-wneoh: ADD COLUMN must preserve an explicit UNIQUE index as an explicit
/// index. Folding it into the rebuilt table creates an extra implicit
/// autoindex; if that autoindex is not backfilled, stock SQLite diagnoses the
/// otherwise-successful migration as a malformed database image.
#[test]
fn alter_add_column_preserves_separate_unique_index_for_stock_sqlite() {
    asupersync::test_utils::run_test(|| async {
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("bd_wneoh.db");
        let fconn = Connection::open(path.to_str().unwrap()).await.unwrap();

        for sql in [
            "CREATE TABLE t (id TEXT PRIMARY KEY, mic TEXT NOT NULL, name TEXT NOT NULL)",
            "INSERT INTO t(id, mic, name) VALUES \
                ('XNAS','XNAS','Nasdaq'), \
                ('XNYS','XNYS','NYSE'), \
                ('XTKS','XTKS','Tokyo'), \
                ('XLON','XLON','London'), \
                ('XETR','XETR','Xetra')",
            "CREATE UNIQUE INDEX idx_t_mic ON t(mic)",
            "ALTER TABLE t ADD COLUMN scope TEXT NOT NULL DEFAULT 'x' CHECK(scope = 'x')",
        ] {
            fconn.execute(sql).await.unwrap();
        }
        fconn.close().await.expect("close FrankenSQLite writer");

        let stock = rusqlite::Connection::open(&path).expect("stock SQLite reopen");
        let integrity: String = stock
            .query_row("PRAGMA integrity_check", [], |row| row.get(0))
            .expect("stock SQLite integrity_check");
        assert_eq!(
            integrity, "ok",
            "bd-wneoh: ALTER ADD COLUMN produced an image rejected by stock SQLite"
        );

        let indexes = {
            let mut statement = stock
                .prepare(
                    "SELECT name, \"unique\", origin \
                     FROM pragma_index_list('t') \
                     ORDER BY name",
                )
                .expect("prepare stock index inventory");
            statement
                .query_map([], |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, i64>(1)?,
                        row.get::<_, String>(2)?,
                    ))
                })
                .expect("query stock index inventory")
                .collect::<rusqlite::Result<Vec<_>>>()
                .expect("collect stock index inventory")
        };
        assert_eq!(
            indexes,
            vec![
                ("idx_t_mic".to_owned(), 1, "c".to_owned()),
                ("sqlite_autoindex_t_1".to_owned(), 1, "pk".to_owned()),
            ],
            "bd-wneoh: separate UNIQUE index was folded into a table constraint"
        );

        let index_sql: String = stock
            .query_row(
                "SELECT sql FROM sqlite_schema WHERE type = 'index' AND name = 'idx_t_mic'",
                [],
                |row| row.get(0),
            )
            .expect("read explicit index DDL");
        assert_eq!(
            index_sql, "CREATE UNIQUE INDEX idx_t_mic ON t(mic)",
            "bd-wneoh: explicit index DDL changed across ALTER ADD COLUMN"
        );

        let backfilled: i64 = stock
            .query_row("SELECT count(*) FROM t WHERE scope = 'x'", [], |row| {
                row.get(0)
            })
            .expect("read backfilled column through stock SQLite");
        assert_eq!(backfilled, 5, "all pre-ALTER rows must receive the default");

        let indexed_mics = {
            let mut statement = stock
                .prepare("SELECT mic FROM t INDEXED BY idx_t_mic ORDER BY mic")
                .expect("prepare forced named-index read");
            statement
                .query_map([], |row| row.get::<_, String>(0))
                .expect("query through preserved named index")
                .collect::<rusqlite::Result<Vec<_>>>()
                .expect("collect named-index rows")
        };
        assert_eq!(
            indexed_mics,
            vec![
                "XETR".to_owned(),
                "XLON".to_owned(),
                "XNAS".to_owned(),
                "XNYS".to_owned(),
                "XTKS".to_owned(),
            ],
            "preserved named index must contain every pre-ALTER row"
        );

        let duplicate_error = stock
            .execute(
                "INSERT INTO t(id, mic, name, scope) VALUES ('DUP', 'XNAS', 'duplicate', 'x')",
                [],
            )
            .expect_err("preserved named index must still enforce uniqueness");
        assert!(
            duplicate_error
                .to_string()
                .contains("UNIQUE constraint failed: t.mic"),
            "expected UNIQUE constraint failure from idx_t_mic, got {duplicate_error}"
        );
    });
}

/// GH #252: ADD COLUMN with a subquery-containing CHECK must be rejected the
/// way CREATE TABLE already rejects it. Accepting it persists a schema stock
/// SQLite refuses to parse, making the whole file unreadable ("malformed
/// database schema"). Both engines must refuse, and the schema must be
/// untouched afterward.
#[test]
fn alter_add_column_check_with_subquery_rejected() {
    asupersync::test_utils::run_test(|| async {
        let fconn = Connection::open(":memory:").await.unwrap();
        let rconn = rusqlite::Connection::open_in_memory().unwrap();
        apply(&fconn, &rconn, &["CREATE TABLE t (a INTEGER)"]).await;

        let diverged = apply_checked(
            &fconn,
            &rconn,
            &[
                "ALTER TABLE t ADD COLUMN b CHECK((SELECT 1))",
                "ALTER TABLE t ADD COLUMN c INTEGER CHECK(c > (SELECT count(*) FROM t))",
                "ALTER TABLE t ADD COLUMN d CHECK(d IN (SELECT a FROM t))",
                "ALTER TABLE t ADD COLUMN e CHECK(EXISTS (SELECT 1))",
            ],
        )
        .await;
        assert_no_mismatches(&diverged, "alter_add_column_check_with_subquery_rejected");

        // The rejected ALTERs must leave no trace: no subquery text may reach
        // the persisted schema (that is exactly what poisons the file for stock
        // SQLite), the table stays usable, and a plain CHECK column is still
        // addable. The exact DDL text is NOT compared: fsqlite currently
        // re-prints ADD COLUMN schema text (column CHECK becomes a table-level
        // CHECK) while C SQLite splices the original text — a pre-existing
        // cosmetic divergence separate from this guard.
        apply(
            &fconn,
            &rconn,
            &[
                "INSERT INTO t VALUES (1)",
                "ALTER TABLE t ADD COLUMN ok INTEGER CHECK(ok IS NULL OR ok > 0)",
                "INSERT INTO t (a, ok) VALUES (2, 5)",
            ],
        )
        .await;
        let schema_rows = fconn
            .query("SELECT sql FROM sqlite_master WHERE name = 't'")
            .await
            .unwrap();
        let schema_text = format!("{:?}", schema_rows.first().map(|r| r.values().to_vec()));
        assert!(
            !schema_text.to_ascii_uppercase().contains("SELECT"),
            "rejected subquery CHECKs must leave no trace in the schema: {schema_text}"
        );
        let m = oracle_compare(&fconn, &rconn, &["SELECT a, ok FROM t ORDER BY a"]).await;
        assert_no_mismatches(&m, "alter_add_column_check_with_subquery_schema_clean");
    });
}

/// GH #231: ADD COLUMN back-fills existing rows, so on a *non-empty* table the
/// default must be a literal constant (a bare literal or a signed number). C
/// SQLite rejects an expression / function / CURRENT_* default with "Cannot add
/// a column with non-constant default" once the table has rows, while still
/// accepting a plain literal. Frank must match statement-for-statement: refuse
/// the non-constant forms, admit the literal forms, and back-fill the admitted
/// ones. Before the fix Frank accepted the non-constant forms and froze one
/// evaluated value into every existing row — the exact divergence this guards.
#[test]
fn alter_add_column_nonconstant_default_rejected_on_nonempty_table() {
    asupersync::test_utils::run_test(|| async {
        let fconn = Connection::open(":memory:").await.unwrap();
        let rconn = rusqlite::Connection::open_in_memory().unwrap();
        apply(&fconn, &rconn, &["CREATE TABLE t (a INTEGER)", "INSERT INTO t VALUES (1)"]).await;

        let diverged = apply_checked(
            &fconn,
            &rconn,
            &[
                // Refused on a non-empty table (non-constant back-fill value).
                "ALTER TABLE t ADD COLUMN r1 DEFAULT (random())",
                "ALTER TABLE t ADD COLUMN r2 DEFAULT (1 + 2)",
                "ALTER TABLE t ADD COLUMN r3 DEFAULT (abs(-5))",
                "ALTER TABLE t ADD COLUMN r4 DEFAULT CURRENT_TIMESTAMP",
                // Admitted: bare literal and signed-number literal defaults.
                "ALTER TABLE t ADD COLUMN k1 INTEGER DEFAULT 7",
                "ALTER TABLE t ADD COLUMN k2 INTEGER DEFAULT (-5)",
                "ALTER TABLE t ADD COLUMN k3 TEXT DEFAULT 'x'",
            ],
        )
        .await;
        assert_no_mismatches(
            &diverged,
            "alter_add_column_nonconstant_default_rejected_on_nonempty_table",
        );

        // The admitted literal defaults back-fill the existing row; the refused
        // columns never came into being (PRAGMA table_info agreement proves no
        // partial admission of r1..r4).
        let m = oracle_compare(
            &fconn,
            &rconn,
            &["SELECT a, k1, k2, k3 FROM t ORDER BY a", "PRAGMA table_info(t)"],
        )
        .await;
        assert_no_mismatches(
            &m,
            "alter_add_column_nonconstant_default_rejected_on_nonempty_table_backfill",
        );
    });
}

/// GH #231 control: the non-constant-default restriction is *row-gated*. On an
/// empty table there are no rows to freeze a single evaluated value into, so C
/// SQLite accepts an expression / function / CURRENT_* default — and Frank must
/// match, i.e. the fix must not over-reject rowless tables. A row inserted
/// afterward exercises the admitted deterministic defaults (the time-dependent
/// CURRENT_TIMESTAMP column is admitted but its value is not compared).
#[test]
fn alter_add_column_nonconstant_default_allowed_on_empty_table() {
    asupersync::test_utils::run_test(|| async {
        let fconn = Connection::open(":memory:").await.unwrap();
        let rconn = rusqlite::Connection::open_in_memory().unwrap();
        apply(&fconn, &rconn, &["CREATE TABLE t (a INTEGER)"]).await;

        let diverged = apply_checked(
            &fconn,
            &rconn,
            &[
                "ALTER TABLE t ADD COLUMN e1 INTEGER DEFAULT (1 + 2)",
                "ALTER TABLE t ADD COLUMN e2 INTEGER DEFAULT (abs(-5))",
                "ALTER TABLE t ADD COLUMN e3 DEFAULT (random())",
                "ALTER TABLE t ADD COLUMN e4 DEFAULT CURRENT_TIMESTAMP",
            ],
        )
        .await;
        assert_no_mismatches(
            &diverged,
            "alter_add_column_nonconstant_default_allowed_on_empty_table",
        );

        // Insert after the fact: the deterministic expression defaults evaluate
        // per-row exactly as on stock SQLite (e1 = 3, e2 = 5).
        apply(&fconn, &rconn, &["INSERT INTO t (a) VALUES (10)"]).await;
        let m = oracle_compare(&fconn, &rconn, &["SELECT a, e1, e2 FROM t ORDER BY a"]).await;
        assert_no_mismatches(
            &m,
            "alter_add_column_nonconstant_default_allowed_on_empty_table_values",
        );
    });
}
