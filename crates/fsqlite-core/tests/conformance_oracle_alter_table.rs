//! Conformance oracle tests — ALTER TABLE variants (cc_3)
//!
//! Schema mutation is one of the trickiest paths in any SQLite implementation:
//! ADD COLUMN must back-fill existing rows with the column default (and apply
//! the new column's affinity to that default), RENAME COLUMN/TABLE must rewrite
//! references, and DROP COLUMN (3.35+) must repack the row image. rusqlite is
//! used as the oracle for both query results and `PRAGMA table_info`.

use fsqlite_core::connection::Connection;
use fsqlite_types::value::SqliteValue;

fn oracle_compare(
    fconn: &Connection,
    rconn: &rusqlite::Connection,
    queries: &[&str],
) -> Vec<String> {
    let mut mismatches = Vec::new();
    for query in queries {
        let frank_result = fconn.query(query);
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
fn apply_checked(fconn: &Connection, rconn: &rusqlite::Connection, stmts: &[&str]) -> Vec<String> {
    let mut diverged = Vec::new();
    for s in stmts {
        let f = fconn.execute(s);
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
fn apply(fconn: &Connection, rconn: &rusqlite::Connection, stmts: &[&str]) {
    for s in stmts {
        fconn.execute(s).unwrap();
        rconn.execute_batch(s).unwrap();
    }
}

#[test]
fn alter_add_column_backfills_constant_default() {
    let fconn = Connection::open(":memory:").unwrap();
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
    );
    let m = oracle_compare(
        &fconn,
        &rconn,
        &[
            "SELECT id, name, score, label, ratio FROM t ORDER BY id",
            "SELECT typeof(score), typeof(label), typeof(ratio) FROM t LIMIT 1",
            "PRAGMA table_info(t)",
        ],
    );
    assert_no_mismatches(&m, "alter_add_column_backfills_constant_default");
}

#[test]
fn alter_add_column_null_default_when_unspecified() {
    let fconn = Connection::open(":memory:").unwrap();
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
    );
    let m = oracle_compare(
        &fconn,
        &rconn,
        &[
            "SELECT id, extra, typeof(extra) FROM t ORDER BY id",
            "SELECT count(*) FROM t WHERE extra IS NULL",
        ],
    );
    assert_no_mismatches(&m, "alter_add_column_null_default_when_unspecified");
}

#[test]
fn alter_add_column_default_affinity_coercion() {
    let fconn = Connection::open(":memory:").unwrap();
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
    );
    let m = oracle_compare(
        &fconn,
        &rconn,
        &["SELECT id, typeof(n), n, typeof(s), s FROM t ORDER BY id"],
    );
    assert_no_mismatches(&m, "alter_add_column_default_affinity_coercion");
}

#[test]
fn alter_rename_table() {
    let fconn = Connection::open(":memory:").unwrap();
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
    );
    let m = oracle_compare(
        &fconn,
        &rconn,
        &[
            "SELECT id, v FROM new_t ORDER BY id",
            "SELECT name FROM sqlite_master WHERE type='table' AND name='new_t'",
            "SELECT count(*) FROM sqlite_master WHERE type='table' AND name='old_t'",
        ],
    );
    assert_no_mismatches(&m, "alter_rename_table");
}

#[test]
fn alter_rename_column() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open_in_memory().unwrap();
    apply(
        &fconn,
        &rconn,
        &[
            "CREATE TABLE t (id INTEGER PRIMARY KEY, old_name TEXT, qty INTEGER)",
            "INSERT INTO t VALUES (1, 'a', 10), (2, 'b', 20)",
            "ALTER TABLE t RENAME COLUMN old_name TO new_name",
        ],
    );
    let m = oracle_compare(
        &fconn,
        &rconn,
        &[
            "SELECT id, new_name, qty FROM t ORDER BY id",
            "SELECT new_name FROM t WHERE qty > 15",
            "PRAGMA table_info(t)",
        ],
    );
    assert_no_mismatches(&m, "alter_rename_column");
}

#[test]
fn alter_drop_column() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open_in_memory().unwrap();
    // DROP COLUMN is SQLite 3.35+; surface a divergence if frank lacks it.
    let setup = [
        "CREATE TABLE t (id INTEGER PRIMARY KEY, a TEXT, b INTEGER, c REAL)",
        "INSERT INTO t VALUES (1, 'x', 5, 1.1), (2, 'y', 6, 2.2)",
        "ALTER TABLE t DROP COLUMN b",
    ];
    let diverged = apply_checked(&fconn, &rconn, &setup);
    assert_no_mismatches(&diverged, "alter_drop_column(setup)");
    let m = oracle_compare(
        &fconn,
        &rconn,
        &["SELECT id, a, c FROM t ORDER BY id", "PRAGMA table_info(t)"],
    );
    assert_no_mismatches(&m, "alter_drop_column");
}

#[test]
fn alter_add_column_not_null_default() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open_in_memory().unwrap();
    apply(
        &fconn,
        &rconn,
        &[
            "CREATE TABLE t (id INTEGER PRIMARY KEY)",
            "INSERT INTO t VALUES (1), (2)",
            "ALTER TABLE t ADD COLUMN flag INTEGER NOT NULL DEFAULT 7",
        ],
    );
    let m = oracle_compare(
        &fconn,
        &rconn,
        &[
            "SELECT id, flag FROM t ORDER BY id",
            "SELECT count(*) FROM t WHERE flag = 7",
        ],
    );
    assert_no_mismatches(&m, "alter_add_column_not_null_default");
}

#[test]
fn alter_sequence_then_index_and_query() {
    let fconn = Connection::open(":memory:").unwrap();
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
    );
    let m = oracle_compare(
        &fconn,
        &rconn,
        &[
            "SELECT id, label, grp FROM t ORDER BY id",
            "SELECT label FROM t WHERE grp = 2 ORDER BY id",
            "SELECT grp, count(*) FROM t GROUP BY grp ORDER BY grp",
        ],
    );
    assert_no_mismatches(&m, "alter_sequence_then_index_and_query");
}

/// GH #252: ADD COLUMN with a subquery-containing CHECK must be rejected the
/// way CREATE TABLE already rejects it. Accepting it persists a schema stock
/// SQLite refuses to parse, making the whole file unreadable ("malformed
/// database schema"). Both engines must refuse, and the schema must be
/// untouched afterward.
#[test]
fn alter_add_column_check_with_subquery_rejected() {
    let fconn = Connection::open(":memory:").unwrap();
    let rconn = rusqlite::Connection::open_in_memory().unwrap();
    apply(&fconn, &rconn, &["CREATE TABLE t (a INTEGER)"]);

    let diverged = apply_checked(
        &fconn,
        &rconn,
        &[
            "ALTER TABLE t ADD COLUMN b CHECK((SELECT 1))",
            "ALTER TABLE t ADD COLUMN c INTEGER CHECK(c > (SELECT count(*) FROM t))",
            "ALTER TABLE t ADD COLUMN d CHECK(d IN (SELECT a FROM t))",
            "ALTER TABLE t ADD COLUMN e CHECK(EXISTS (SELECT 1))",
        ],
    );
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
    );
    let schema_rows = fconn
        .query("SELECT sql FROM sqlite_master WHERE name = 't'")
        .unwrap();
    let schema_text = format!("{:?}", schema_rows.first().map(|r| r.values().to_vec()));
    assert!(
        !schema_text.to_ascii_uppercase().contains("SELECT"),
        "rejected subquery CHECKs must leave no trace in the schema: {schema_text}"
    );
    let m = oracle_compare(&fconn, &rconn, &["SELECT a, ok FROM t ORDER BY a"]);
    assert_no_mismatches(&m, "alter_add_column_check_with_subquery_schema_clean");
}
