//! bd-1zzvh — Oracle-parity e2e: pragma table-valued functions vs rusqlite.
//!
//! pragma_introspection_oracle exercises the `PRAGMA table_info(t)` *statement*
//! form. SQLite also exposes most introspection pragmas as table-valued
//! functions — `SELECT ... FROM pragma_table_info('t')` — which, unlike the bare
//! statement, can be filtered with WHERE, projected to specific columns, counted,
//! and joined. This file pins that TVF surface for `pragma_table_info` and
//! `pragma_foreign_key_list` against rusqlite. (The `pragma_index_list` TVF is
//! deliberately omitted: its numbering already diverges under bd-uylfy.) Reserved
//! words used as column names (`notnull`, `from`, `to`, `table`) are double-quoted.

use fsqlite::Connection;
use fsqlite_types::SqliteValue;

fn render_frank(v: &SqliteValue) -> String {
    match v {
        SqliteValue::Null => "NULL".to_owned(),
        SqliteValue::Integer(n) => n.to_string(),
        SqliteValue::Float(f) => format!("{f}"),
        SqliteValue::Text(s) => format!("'{s}'"),
        SqliteValue::Blob(b) => format!(
            "X'{}'",
            b.iter().map(|x| format!("{x:02X}")).collect::<String>()
        ),
    }
}

async fn frank_rows(conn: &Connection, sql: &str) -> Result<Vec<Vec<String>>, String> {
    let rows = conn.query(sql).await.map_err(|e| e.to_string())?;
    Ok(rows
        .iter()
        .map(|row| row.values().iter().map(render_frank).collect())
        .collect())
}

fn sqlite_rows(conn: &rusqlite::Connection, sql: &str) -> Result<Vec<Vec<String>>, String> {
    let mut stmt = conn.prepare(sql).map_err(|e| e.to_string())?;
    let n = stmt.column_count();
    stmt.query_map([], |row| {
        let mut out = Vec::with_capacity(n);
        for i in 0..n {
            let v: rusqlite::types::Value = row.get_unwrap(i);
            out.push(match v {
                rusqlite::types::Value::Null => "NULL".to_owned(),
                rusqlite::types::Value::Integer(x) => x.to_string(),
                rusqlite::types::Value::Real(f) => format!("{f}"),
                rusqlite::types::Value::Text(s) => format!("'{s}'"),
                rusqlite::types::Value::Blob(b) => format!(
                    "X'{}'",
                    b.iter().map(|x| format!("{x:02X}")).collect::<String>()
                ),
            });
        }
        Ok(out)
    })
    .map_err(|e| e.to_string())?
    .collect::<Result<Vec<_>, _>>()
    .map_err(|e| e.to_string())
}

async fn setup(stmts: &[&str]) -> (Connection, rusqlite::Connection) {
    let f = Connection::open(":memory:").await.expect("open frank");
    let r = rusqlite::Connection::open_in_memory().expect("open rusqlite");
    for s in stmts {
        let fe = f.execute(s).await;
        let re = r.execute_batch(s);
        match (&fe, &re) {
            (Ok(_), Ok(())) | (Err(_), Err(_)) => {}
            (Ok(_), Err(e)) => panic!("setup `{s}`\n  frank: OK\n  csql:  ERROR({e})"),
            (Err(e), Ok(())) => panic!("setup `{s}`\n  frank: ERROR({e})\n  csql:  OK"),
        }
    }
    (f, r)
}

async fn check(f: &Connection, r: &rusqlite::Connection, queries: &[&str], label: &str) {
    let mut mismatches = Vec::new();
    for q in queries {
        match (frank_rows(f, q).await, sqlite_rows(r, q)) {
            (Ok(a), Ok(b)) if a == b => {}
            (Ok(a), Ok(b)) => {
                mismatches.push(format!("MISMATCH: {q}\n  frank: {a:?}\n  csql:  {b:?}"))
            }
            (Err(e), Ok(b)) => mismatches.push(format!(
                "FRANK_ERR: {q}\n  frank: ERROR({e})\n  csql:  {b:?}"
            )),
            (Ok(a), Err(e)) => {
                mismatches.push(format!("CSQL_ERR: {q}\n  frank: {a:?}\n  csql: ERROR({e})"))
            }
            (Err(_), Err(_)) => {}
        }
    }
    assert!(
        mismatches.is_empty(),
        "{label}: {} mismatch(es)\n{}",
        mismatches.len(),
        mismatches.join("\n")
    );
}

const TBL: &[&str] =
    &["CREATE TABLE t (id INTEGER PRIMARY KEY, a INTEGER NOT NULL, b TEXT DEFAULT 'x', c REAL)"];

#[test]
fn pragma_table_info_tvf_projection() {
    asupersync::test_utils::run_test(|| async {
        let (f, r) = setup(TBL).await;
        check(
            &f,
            &r,
            &[
                // Whole-row projection of the metadata, in column (cid) order.
                "SELECT cid, name, type, \"notnull\", pk FROM pragma_table_info('t') ORDER BY cid",
                // Just the names, ordered.
                "SELECT name FROM pragma_table_info('t') ORDER BY cid",
            ],
            "pragma_table_info_tvf_projection",
        )
        .await;
    });
}

#[test]
fn pragma_table_info_tvf_filter_and_count() {
    asupersync::test_utils::run_test(|| async {
        let (f, r) = setup(TBL).await;
        check(
            &f,
            &r,
            &[
                "SELECT count(*) FROM pragma_table_info('t')", // 4
                "SELECT name FROM pragma_table_info('t') WHERE pk = 1", // id
                "SELECT name FROM pragma_table_info('t') WHERE \"notnull\" = 1 ORDER BY cid", // a
                "SELECT count(*) FROM pragma_table_info('t') WHERE type = 'INTEGER'", // 2
                // dflt_value carries the literal text of the default expression.
                "SELECT name, dflt_value FROM pragma_table_info('t') WHERE dflt_value IS NOT NULL", // b, 'x'
            ],
            "pragma_table_info_tvf_filter_and_count",
        )
        .await;
    });
}

#[test]
fn pragma_table_info_tvf_self_subquery() {
    asupersync::test_utils::run_test(|| async {
        let (f, r) = setup(TBL).await;
        check(
            &f,
            &r,
            &[
                // The cid of the primary-key column, fed back through a subquery.
                "SELECT name FROM pragma_table_info('t') \
             WHERE cid = (SELECT cid FROM pragma_table_info('t') WHERE pk = 1)",
                // Max cid == column_count - 1.
                "SELECT max(cid) FROM pragma_table_info('t')", // 3
            ],
            "pragma_table_info_tvf_self_subquery",
        )
        .await;
    });
}

#[test]
fn pragma_foreign_key_list_tvf() {
    asupersync::test_utils::run_test(|| async {
        let (f, r) = setup(&[
            "CREATE TABLE parent (id INTEGER PRIMARY KEY, code TEXT UNIQUE)",
            "CREATE TABLE child (\
         id INTEGER PRIMARY KEY, \
         pid INTEGER REFERENCES parent(id) ON DELETE CASCADE ON UPDATE SET NULL)",
        ])
        .await;
        check(
            &f,
            &r,
            &[
                // The referenced table, local/foreign columns, and the actions.
                "SELECT \"table\", \"from\", \"to\", on_update, on_delete \
             FROM pragma_foreign_key_list('child')",
                "SELECT count(*) FROM pragma_foreign_key_list('child')", // 1
                "SELECT count(*) FROM pragma_foreign_key_list('parent')", // 0
                "SELECT \"from\" FROM pragma_foreign_key_list('child') WHERE \"table\" = 'parent'", // pid
            ],
            "pragma_foreign_key_list_tvf",
        )
        .await;
    });
}
