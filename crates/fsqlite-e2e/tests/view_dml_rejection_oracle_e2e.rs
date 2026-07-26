//! bd-p6b01 — Oracle-parity e2e: DML on a non-updatable view vs rusqlite.
//!
//! view_semantics_oracle covers SELECT *from* views. A plain view has no backing
//! storage, so without an INSTEAD OF trigger SQLite rejects INSERT / UPDATE /
//! DELETE against it ("cannot modify <v> because it is a view"). This pins that
//! frank rejects the same DML (rather than silently no-op'ing or, worse, mutating
//! the base table), while SELECT from the view still returns the right rows and
//! DML against the base table works. Statement success/failure is compared for
//! DML; rows are compared for the SELECT.

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

const SETUP: &[&str] = &[
    "CREATE TABLE t (a INTEGER, b INTEGER)",
    "INSERT INTO t VALUES (1,10),(2,20),(3,30)",
    "CREATE VIEW v AS SELECT a, b FROM t",
];

async fn engines() -> (Connection, rusqlite::Connection) {
    let f = Connection::open(":memory:").await.expect("open frank");
    let r = rusqlite::Connection::open_in_memory().expect("open rusqlite");
    for s in SETUP {
        f.execute(s).await.unwrap();
        r.execute_batch(s).unwrap();
    }
    (f, r)
}

/// Run a statement on both engines (fresh setup each time) and return a mismatch
/// if they disagree on success/failure.
async fn stmt_agreement(stmt: &str) -> Option<String> {
    let (f, r) = engines().await;
    let fe = f.execute(stmt).await;
    let re = r.execute_batch(stmt);
    match (&fe, &re) {
        (Ok(_), Ok(())) | (Err(_), Err(_)) => None,
        (Ok(_), Err(e)) => Some(format!("FRANK_OK / CSQL_ERR: `{stmt}`\n  csql: ERROR({e})")),
        (Err(e), Ok(())) => Some(format!(
            "FRANK_ERR / CSQL_OK: `{stmt}`\n  frank: ERROR({e})"
        )),
    }
}

#[test]
fn dml_on_view_rejected() {
    asupersync::test_utils::run_test(|| async {
        let mut mismatches: Vec<String> = Vec::new();
        for s in [
            "INSERT INTO v VALUES (4, 40)",
            "UPDATE v SET b = 0",
            "DELETE FROM v",
            "DELETE FROM v WHERE a = 1",
        ]
        .iter()
        {
            if let Some(m) = stmt_agreement(s).await {
                mismatches.push(m);
            }
        }
        assert!(
            mismatches.is_empty(),
            "dml_on_view_rejected: {} mismatch(es)\n{}",
            mismatches.len(),
            mismatches.join("\n")
        );
    });
}

#[test]
fn select_from_view_and_base_dml_ok() {
    asupersync::test_utils::run_test(|| async {
        let (f, r) = engines().await;
        // SELECT from the view returns the right rows...
        let mut mismatches = Vec::new();
        let q = "SELECT a, b FROM v ORDER BY a";
        match (frank_rows(&f, q).await, sqlite_rows(&r, q)) {
            (Ok(a), Ok(b)) if a == b => {}
            (fa, rb) => mismatches.push(format!(
                "SELECT mismatch: {q}\n  frank: {fa:?}\n  csql: {rb:?}"
            )),
        }
        // ...and DML against the BASE table is fine on both, reflected by the view.
        assert!(
            stmt_agreement("INSERT INTO t VALUES (4, 40)")
                .await
                .is_none()
        );
        assert!(stmt_agreement("UPDATE t SET b = b + 1").await.is_none());
        assert!(
            mismatches.is_empty(),
            "select_from_view_and_base_dml_ok:\n{}",
            mismatches.join("\n")
        );
    });
}
