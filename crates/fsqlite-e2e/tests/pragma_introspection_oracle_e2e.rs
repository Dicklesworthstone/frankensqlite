//! bd-00aan — Oracle-parity e2e: schema-introspection PRAGMAs vs rusqlite.
//!
//! `PRAGMA table_info` / `table_xinfo` / `foreign_key_list` / `index_list` /
//! `index_info` / `index_xinfo` expose schema metadata with well-defined column
//! layouts. These are exactly the surfaces ORMs and tooling read, so divergence
//! in column counts, notnull/pk flags, default-value rendering, FK action
//! strings, or index origin/uniqueness is user-visible. All schemas are fixed.

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
        f.execute(s)
            .await
            .unwrap_or_else(|e| panic!("frank `{s}`: {e}"));
        r.execute_batch(s)
            .unwrap_or_else(|e| panic!("rusqlite `{s}`: {e}"));
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

#[test]
fn pragma_table_info_columns() {
    asupersync::test_utils::run_test(|| async {
        let (f, r) = setup(&["CREATE TABLE t (\
           id INTEGER PRIMARY KEY, \
           name TEXT NOT NULL, \
           qty INTEGER DEFAULT 0, \
           price REAL DEFAULT 1.5, \
           note TEXT, \
           tag TEXT NOT NULL DEFAULT 'x')"])
        .await;
        check(
            &f,
            &r,
            &["PRAGMA table_info(t)"],
            "pragma_table_info_columns",
        )
        .await;
    });
}

#[test]
fn pragma_table_info_composite_pk() {
    asupersync::test_utils::run_test(|| async {
        let (f, r) = setup(&["CREATE TABLE t (\
           a INTEGER, b TEXT, c INTEGER, \
           PRIMARY KEY (b, a))"])
        .await;
        // The `pk` column reflects the position within the composite primary key.
        check(
            &f,
            &r,
            &["PRAGMA table_info(t)"],
            "pragma_table_info_composite_pk",
        )
        .await;
    });
}

#[test]
fn pragma_table_xinfo_hidden_column() {
    asupersync::test_utils::run_test(|| async {
        let (f, r) =
            setup(&["CREATE TABLE t (a INTEGER, b INTEGER, c INTEGER AS (a + b) STORED)"]).await;
        // table_xinfo adds the trailing `hidden` column (generated => 2/3).
        check(
            &f,
            &r,
            &["PRAGMA table_xinfo(t)"],
            "pragma_table_xinfo_hidden_column",
        )
        .await;
    });
}

#[test]
fn pragma_foreign_key_list() {
    asupersync::test_utils::run_test(|| async {
        let (f, r) = setup(&[
            "CREATE TABLE parent (id INTEGER PRIMARY KEY, code TEXT UNIQUE)",
            "CREATE TABLE child (\
           id INTEGER PRIMARY KEY, \
           pid INTEGER REFERENCES parent(id) ON DELETE CASCADE ON UPDATE SET NULL, \
           pcode TEXT REFERENCES parent(code) ON DELETE RESTRICT)",
        ])
        .await;
        check(
            &f,
            &r,
            &["PRAGMA foreign_key_list(child)"],
            "pragma_foreign_key_list",
        )
        .await;
    });
}

#[test]
fn pragma_index_info() {
    asupersync::test_utils::run_test(|| async {
        let (f, r) = setup(&[
            "CREATE TABLE t (id INTEGER PRIMARY KEY, a INTEGER, b TEXT, c INTEGER)",
            "CREATE UNIQUE INDEX idx_a ON t(a)",
            "CREATE INDEX idx_bc ON t(b, c DESC)",
        ])
        .await;
        check(
            &f,
            &r,
            &["PRAGMA index_info(idx_a)", "PRAGMA index_info(idx_bc)"],
            "pragma_index_info",
        )
        .await;
    });
}

#[test]
fn pragma_index_list() {
    asupersync::test_utils::run_test(|| async {
        let (f, r) = setup(&[
            "CREATE TABLE t (id INTEGER PRIMARY KEY, a INTEGER, b TEXT, c INTEGER)",
            "CREATE UNIQUE INDEX idx_a ON t(a)",
            "CREATE INDEX idx_bc ON t(b, c DESC)",
        ])
        .await;
        check(&f, &r, &["PRAGMA index_list(t)"], "pragma_index_list").await;
    });
}

#[test]
fn pragma_index_xinfo_with_direction() {
    asupersync::test_utils::run_test(|| async {
        let (f, r) = setup(&[
            "CREATE TABLE t (id INTEGER PRIMARY KEY, a INTEGER, b TEXT)",
            "CREATE INDEX idx_ab ON t(a DESC, b COLLATE NOCASE)",
        ])
        .await;
        // index_xinfo exposes seqno, cid, name, desc, coll, key for index + covered cols.
        check(
            &f,
            &r,
            &["PRAGMA index_xinfo(idx_ab)"],
            "pragma_index_xinfo_with_direction",
        )
        .await;
    });
}

#[test]
fn pragma_index_list_unique_origin() {
    asupersync::test_utils::run_test(|| async {
        // A UNIQUE constraint creates an auto-index with origin 'u'; an explicit
        // CREATE INDEX has origin 'c'; the PK auto-index has origin 'pk'.
        let (f, r) = setup(&[
            "CREATE TABLE t (id INTEGER PRIMARY KEY, email TEXT UNIQUE, name TEXT)",
            "CREATE INDEX idx_name ON t(name)",
        ])
        .await;
        check(
            &f,
            &r,
            &["PRAGMA index_list(t)"],
            "pragma_index_list_unique_origin",
        )
        .await;
    });
}
