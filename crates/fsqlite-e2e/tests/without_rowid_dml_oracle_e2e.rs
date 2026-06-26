//! bd-pt5co — Oracle-parity e2e: WITHOUT ROWID DML & indexing vs rusqlite.
//!
//! rowid_oracle covers WITHOUT ROWID storage/ordering and the no-rowid-column
//! rule; this exercises mutation on that distinct storage model (the PK *is* the
//! key, there is no rowid B-tree): UPDATE/DELETE of non-key columns, UPDATE of
//! the PRIMARY KEY itself (re-keying + re-ordering), a secondary index lookup,
//! duplicate-PK conflict (error and INSERT OR REPLACE), and an INTEGER-PK
//! WITHOUT ROWID table's PK ordering. Each scenario asserts per-statement
//! agreement with rusqlite, then compares the resulting rows.

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

fn frank_rows(conn: &Connection, sql: &str) -> Result<Vec<Vec<String>>, String> {
    let rows = conn.query(sql).map_err(|e| e.to_string())?;
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

fn scenario(stmts: &[&str], queries: &[&str], label: &str) {
    let f = Connection::open(":memory:").expect("open frank");
    let r = rusqlite::Connection::open_in_memory().expect("open rusqlite");
    for s in stmts {
        let fe = f.execute(s);
        let re = r.execute_batch(s);
        match (&fe, &re) {
            (Ok(_), Ok(())) | (Err(_), Err(_)) => {}
            (Ok(_), Err(e)) => panic!("{label}: `{s}`\n  frank: OK\n  csql:  ERROR({e})"),
            (Err(e), Ok(())) => panic!("{label}: `{s}`\n  frank: ERROR({e})\n  csql:  OK"),
        }
    }
    let mut mismatches = Vec::new();
    for q in queries {
        match (frank_rows(&f, q), sqlite_rows(&r, q)) {
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
fn without_rowid_update_and_delete() {
    scenario(
        &[
            "CREATE TABLE wr (k TEXT PRIMARY KEY, v INTEGER) WITHOUT ROWID",
            "INSERT INTO wr VALUES ('banana',1),('apple',2),('cherry',3),('date',4)",
            "UPDATE wr SET v = v * 10 WHERE k = 'apple'", // apple -> 20
            "DELETE FROM wr WHERE k = 'cherry'",
        ],
        &["SELECT k, v FROM wr ORDER BY k"], // (apple,20),(banana,1),(date,4)
        "without_rowid_update_and_delete",
    );
}

#[test]
fn without_rowid_update_primary_key() {
    scenario(
        &[
            "CREATE TABLE wr (k TEXT PRIMARY KEY, v INTEGER) WITHOUT ROWID",
            "INSERT INTO wr VALUES ('apple',1),('banana',2)",
            "UPDATE wr SET k = 'zebra' WHERE k = 'apple'", // re-key + re-order
        ],
        &["SELECT k, v FROM wr ORDER BY k"], // (banana,2),(zebra,1)
        "without_rowid_update_primary_key",
    );
}

#[test]
fn without_rowid_secondary_index() {
    scenario(
        &[
            "CREATE TABLE wr (k TEXT PRIMARY KEY, v INTEGER) WITHOUT ROWID",
            "CREATE INDEX idx_v ON wr(v)",
            "INSERT INTO wr VALUES ('a',30),('b',10),('c',20),('d',10)",
        ],
        &[
            "SELECT k FROM wr WHERE v = 10 ORDER BY k",    // b,d
            "SELECT k FROM wr WHERE v > 15 ORDER BY v, k", // c(20),a(30)
            "SELECT k, v FROM wr ORDER BY v, k",
        ],
        "without_rowid_secondary_index",
    );
}

#[test]
fn without_rowid_pk_conflict_and_replace() {
    scenario(
        &[
            "CREATE TABLE wr (k TEXT PRIMARY KEY, v INTEGER) WITHOUT ROWID",
            "INSERT INTO wr VALUES ('apple',1),('banana',2)",
            "INSERT INTO wr VALUES ('apple',99)", // duplicate PK -> error both
            "INSERT OR REPLACE INTO wr VALUES ('apple',99)", // replaces apple
        ],
        &["SELECT k, v FROM wr ORDER BY k"], // (apple,99),(banana,2)
        "without_rowid_pk_conflict_and_replace",
    );
}

#[test]
fn without_rowid_integer_pk_ordering() {
    scenario(
        &[
            "CREATE TABLE wr (id INTEGER PRIMARY KEY, v TEXT) WITHOUT ROWID",
            "INSERT INTO wr VALUES (3,'c'),(1,'a'),(2,'b'),(10,'j')",
            "UPDATE wr SET v = 'B' WHERE id = 2",
            "DELETE FROM wr WHERE id = 10",
        ],
        &["SELECT id, v FROM wr ORDER BY id"], // (1,a),(2,B),(3,c)
        "without_rowid_integer_pk_ordering",
    );
}
