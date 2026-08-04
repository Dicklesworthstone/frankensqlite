//! bd-3hmwg — Oracle-parity e2e: column DEFAULT values vs rusqlite (real SQLite).
//!
//! Covers literal defaults (int/real/text/NULL/negative), parenthesised
//! expression defaults `DEFAULT (expr)`, the affinity coercion of a default to
//! its column type, `INSERT DEFAULT VALUES`, defaults filling omitted columns
//! (single + multi-row), the rejection of the bare `DEFAULT` keyword inside a
//! VALUES tuple (a syntax error in SQLite — only `INSERT ... DEFAULT VALUES` is
//! valid), and CURRENT_TIMESTAMP / CURRENT_DATE defaults (checked by storage
//! class + length, which are deterministic regardless of the wall clock). DML
//! is autocommit.
#![recursion_limit = "512"]

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
fn default_literal_values() {
    asupersync::test_utils::run_test(|| async {
        let (f, r) = setup(&[
            "CREATE TABLE t (id INTEGER PRIMARY KEY, \
               i INTEGER DEFAULT 7, \
               r REAL DEFAULT 1.5, \
               s TEXT DEFAULT 'hi', \
               n INTEGER DEFAULT NULL, \
               neg INTEGER DEFAULT -3)",
            "INSERT INTO t DEFAULT VALUES",
            "INSERT INTO t(id, i) VALUES (2, 99)", // others use defaults
        ])
        .await;
        check(
            &f,
            &r,
            &[
                "SELECT id, i, r, s, n, neg FROM t ORDER BY id",
                "SELECT typeof(i), typeof(r), typeof(s), typeof(n) FROM t WHERE id = 1",
            ],
            "default_literal_values",
        )
        .await;
    });
}

#[test]
fn default_expression_values() {
    asupersync::test_utils::run_test(|| async {
        let (f, r) = setup(&[
            "CREATE TABLE t (id INTEGER PRIMARY KEY, \
               a INTEGER DEFAULT (2 + 3 * 4), \
               b INTEGER DEFAULT (abs(-5)), \
               c TEXT DEFAULT ('x' || 'y'))",
            "INSERT INTO t(id) VALUES (1)",
        ])
        .await;
        check(
            &f,
            &r,
            &["SELECT id, a, b, c FROM t ORDER BY id"],
            "default_expression_values",
        )
        .await;
    });
}

#[test]
fn default_affinity_coercion() {
    // The default literal is coerced to the column's declared affinity, like a
    // normal inserted value (this is the CREATE TABLE path, cf. bd-v7y8q for the
    // ALTER ADD COLUMN path).
    asupersync::test_utils::run_test(|| async {
        let (f, r) = setup(&[
            "CREATE TABLE t (id INTEGER PRIMARY KEY, \
               n INTEGER DEFAULT '42', \
               s TEXT DEFAULT 100, \
               rr REAL DEFAULT 5)",
            "INSERT INTO t(id) VALUES (1)",
        ])
        .await;
        check(
            &f,
            &r,
            &["SELECT id, typeof(n), n, typeof(s), s, typeof(rr), rr FROM t ORDER BY id"],
            "default_affinity_coercion",
        )
        .await;
    });
}

#[test]
fn default_omitted_columns_multi_row() {
    asupersync::test_utils::run_test(|| async {
        let (f, r) = setup(&[
            "CREATE TABLE t (id INTEGER PRIMARY KEY, a INTEGER DEFAULT 0, b TEXT DEFAULT 'z')",
            "INSERT INTO t(id) VALUES (1),(2),(3)", // a,b default for all
            "INSERT INTO t(id, a) VALUES (4, 40),(5, 50)", // b defaults
        ])
        .await;
        check(
            &f,
            &r,
            &["SELECT id, a, b FROM t ORDER BY id"],
            "default_omitted_columns_multi_row",
        )
        .await;
    });
}

#[test]
fn default_explicit_keyword() {
    // SQLite does NOT accept the bare `DEFAULT` keyword as a value inside a
    // `VALUES (...)` tuple — only the whole-row `INSERT ... DEFAULT VALUES`
    // form (exercised by `default_values_insert` above). `VALUES (1, DEFAULT,
    // 200)` is a syntax error in real SQLite, so for parity FrankenSQLite must
    // reject it too. (Verified vs sqlite3 CLI 3.46.1 and the bundled rusqlite;
    // bd-yw5kx was filed on a mistaken premise that SQLite accepts it.)
    asupersync::test_utils::run_test(|| async {
        let f = Connection::open(":memory:").await.expect("open frank");
        let r = rusqlite::Connection::open_in_memory().expect("open rusqlite");
        let ddl =
            "CREATE TABLE t (id INTEGER PRIMARY KEY, a INTEGER DEFAULT 11, b INTEGER DEFAULT 22)";
        f.execute(ddl).await.expect("frank create");
        r.execute_batch(ddl).expect("rusqlite create");

        let bad = "INSERT INTO t(id, a, b) VALUES (1, DEFAULT, 200)";
        assert!(
            f.execute(bad).await.is_err(),
            "frank must reject the DEFAULT keyword inside a VALUES tuple (parity with SQLite)"
        );
        assert!(
            r.execute_batch(bad).is_err(),
            "oracle (rusqlite/SQLite) rejects the DEFAULT keyword inside a VALUES tuple"
        );
    });
}

#[test]
fn default_current_timestamp_shape() {
    // CURRENT_* default values depend on the clock, so compare their storage
    // class and length (deterministic): TIMESTAMP -> 'YYYY-MM-DD HH:MM:SS' (19),
    // DATE -> 'YYYY-MM-DD' (10), TIME -> 'HH:MM:SS' (8).
    asupersync::test_utils::run_test(|| async {
        let (f, r) = setup(&[
            "CREATE TABLE t (id INTEGER PRIMARY KEY, \
               ts TEXT DEFAULT CURRENT_TIMESTAMP, \
               d  TEXT DEFAULT CURRENT_DATE, \
               tm TEXT DEFAULT CURRENT_TIME)",
            "INSERT INTO t(id) VALUES (1)",
        ])
        .await;
        check(
            &f,
            &r,
            &[
                "SELECT typeof(ts), length(ts), typeof(d), length(d), typeof(tm), length(tm) FROM t",
                // The date portion of ts must equal CURRENT_DATE's value (same day).
                "SELECT date(ts) = d FROM t",
            ],
            "default_current_timestamp_shape",
        )
        .await;
    });
}
