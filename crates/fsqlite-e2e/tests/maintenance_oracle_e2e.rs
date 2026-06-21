//! bd-1kstg — Oracle-parity e2e: ANALYZE / REINDEX correctness vs rusqlite.
//!
//! ANALYZE and REINDEX are maintenance commands that must not change query
//! RESULTS — only internal structures (the sqlite_stat1 stats / the rebuilt
//! index). These verify that: queries return the same correct rows after
//! ANALYZE; ANALYZE creates the sqlite_stat1 table; REINDEX rebuilds an index
//! (lookups stay correct and UNIQUE enforcement still holds); and a REINDEX
//! after INSERT/UPDATE/DELETE churn leaves index-driven queries correct. (Stat
//! VALUES are engine-specific and not compared — only results and existence.)

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

const SEED: [&str; 3] = [
    "CREATE TABLE t (id INTEGER PRIMARY KEY, a INTEGER, b TEXT)",
    "CREATE INDEX idx_a ON t(a)",
    "INSERT INTO t VALUES (1,10,'x'),(2,20,'y'),(3,20,'z'),(4,30,'w'),(5,10,'v')",
];

#[test]
fn analyze_keeps_queries_correct() {
    scenario(
        &{
            let mut v = SEED.to_vec();
            v.push("ANALYZE");
            v
        },
        &[
            "SELECT id FROM t WHERE a = 20 ORDER BY id",       // 2,3
            "SELECT id FROM t WHERE a > 15 ORDER BY id",       // 2,3,4
            "SELECT count(*), sum(a) FROM t",                  // 5, 90
            "SELECT a, count(*) FROM t GROUP BY a ORDER BY a", // (10,2),(20,2),(30,1)
        ],
        "analyze_keeps_queries_correct",
    );
}

#[test]
fn analyze_creates_stat1() {
    scenario(
        &{
            let mut v = SEED.to_vec();
            v.push("ANALYZE");
            v
        },
        &[
            // ANALYZE materializes the sqlite_stat1 table.
            "SELECT count(*) FROM sqlite_master WHERE type='table' AND name='sqlite_stat1'", // 1
        ],
        "analyze_creates_stat1",
    );
}

#[test]
fn reindex_rebuilds_unique_index() {
    scenario(
        &[
            "CREATE TABLE t (id INTEGER PRIMARY KEY, u INTEGER UNIQUE, label TEXT)",
            "INSERT INTO t VALUES (1,10,'a'),(2,20,'b'),(3,30,'c')",
            "REINDEX",
            "INSERT INTO t VALUES (4,20,'d')", // still rejected after REINDEX -> error both
        ],
        &[
            "SELECT id, u FROM t WHERE u = 20 ORDER BY id", // 2
            "SELECT count(*) FROM t",                       // 3
        ],
        "reindex_rebuilds_unique_index",
    );
}

#[test]
fn reindex_after_dml_churn() {
    scenario(
        &[
            "CREATE TABLE t (id INTEGER PRIMARY KEY, a INTEGER)",
            "CREATE INDEX idx_a ON t(a)",
            "INSERT INTO t VALUES (1,10),(2,20),(3,30),(4,40),(5,50)",
            "DELETE FROM t WHERE a = 30",
            "UPDATE t SET a = 99 WHERE id = 1",
            "INSERT INTO t VALUES (6,15)",
            "REINDEX idx_a",
        ],
        &[
            "SELECT id, a FROM t ORDER BY id", // (1,99),(2,20),(4,40),(5,50),(6,15)
            "SELECT id FROM t WHERE a >= 40 ORDER BY id", // 1(99),4(40),5(50)
            "SELECT id FROM t WHERE a = 99",   // 1
        ],
        "reindex_after_dml_churn",
    );
}

#[test]
fn reindex_specific_table() {
    scenario(
        &[
            "CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT COLLATE NOCASE)",
            "CREATE INDEX idx_name ON t(name)",
            "INSERT INTO t VALUES (1,'Alpha'),(2,'beta'),(3,'GAMMA')",
            "REINDEX t", // rebuild all indexes on t
        ],
        &[
            "SELECT id FROM t WHERE name = 'alpha'", // NOCASE -> 1
            "SELECT id, name FROM t ORDER BY name",  // NOCASE order: Alpha,beta,GAMMA
        ],
        "reindex_specific_table",
    );
}

/// bd-n3ukk: REINDEX <collation-name> (rebuild all indexes using that collation)
/// is not supported — frank only resolves table/index targets.
#[test]
fn reindex_collation_name() {
    scenario(
        &[
            "CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT COLLATE NOCASE)",
            "CREATE INDEX idx_name ON t(name)",
            "INSERT INTO t VALUES (1,'Alpha'),(2,'beta'),(3,'GAMMA')",
            "REINDEX NOCASE", // rebuild all NOCASE-collation indexes
        ],
        &["SELECT id FROM t WHERE name = 'alpha'"],
        "reindex_collation_name",
    );
}
