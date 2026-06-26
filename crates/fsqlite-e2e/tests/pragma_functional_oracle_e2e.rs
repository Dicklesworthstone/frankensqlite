//! bd-kf9wf — Oracle-parity e2e: functional/settable PRAGMAs vs rusqlite.
//!
//! pragma_introspection_oracle covers the schema-introspection pragmas
//! (table_info / index_* / foreign_key_list); this covers the settable and
//! diagnostic ones: the `user_version` and `application_id` header integers
//! (default 0, set then read back), the `foreign_keys` enable setting, and
//! `foreign_key_check` (empty when clean, and the violation rows it reports for
//! an orphaned child). Each scenario asserts per-statement agreement with
//! rusqlite, then compares the pragma query results. The exact-row shape of
//! foreign_key_check is isolated in case the column layout differs.

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
fn pragma_user_version_default_zero() {
    scenario(
        &[],
        &["PRAGMA user_version"], // 0 on a fresh database
        "pragma_user_version_default_zero",
    );
}

#[test]
fn pragma_user_version_roundtrip() {
    scenario(
        &["PRAGMA user_version = 42"],
        &["PRAGMA user_version"], // 42
        "pragma_user_version_roundtrip",
    );
}

#[test]
fn pragma_application_id_roundtrip() {
    scenario(
        &["PRAGMA application_id = 12345"],
        &["PRAGMA application_id"], // 12345
        "pragma_application_id_roundtrip",
    );
}

#[test]
fn pragma_foreign_keys_setting() {
    scenario(
        &["PRAGMA foreign_keys = ON"],
        &["PRAGMA foreign_keys"], // 1
        "pragma_foreign_keys_setting",
    );
}

#[test]
fn pragma_foreign_key_check_clean() {
    scenario(
        &[
            "PRAGMA foreign_keys = ON",
            "CREATE TABLE parent (id INTEGER PRIMARY KEY)",
            "CREATE TABLE child (id INTEGER PRIMARY KEY, pid INTEGER REFERENCES parent(id))",
            "INSERT INTO parent VALUES (1),(2)",
            "INSERT INTO child VALUES (10,1),(11,2)",
        ],
        &["PRAGMA foreign_key_check"], // no violations -> empty
        "pragma_foreign_key_check_clean",
    );
}

/// bd-avlou: PRAGMA foreign_key_check is unimplemented — frank returns an empty
/// result even when an orphaned child row exists, so it silently reports a clean
/// database. (The clean-database case passes only because it is also empty.)
#[test]
fn pragma_foreign_key_check_reports_violation() {
    // With FK enforcement OFF we can insert an orphan, then foreign_key_check
    // reports it. The reported columns: (table, rowid, referred_table, fkid).
    scenario(
        &[
            "PRAGMA foreign_keys = OFF",
            "CREATE TABLE parent (id INTEGER PRIMARY KEY)",
            "CREATE TABLE child (id INTEGER PRIMARY KEY, pid INTEGER REFERENCES parent(id))",
            "INSERT INTO parent VALUES (1)",
            "INSERT INTO child VALUES (10,1),(11,99)", // 99 is an orphan
        ],
        &["PRAGMA foreign_key_check"],
        "pragma_foreign_key_check_reports_violation",
    );
}

/// br-xugii: `PRAGMA foreign_key_check` must scale. The previous implementation
/// issued one parent-existence query per child row (O(child × parent_lookup),
/// degrading to O(child × parent) and pegging a core for minutes on tens of
/// thousands of rows). The anti-join rewrite runs one set-difference query per
/// FK. This builds a non-trivial table and asserts the orphan report still
/// matches rusqlite at scale (and completes effectively instantly).
#[test]
fn pragma_foreign_key_check_scale() {
    const N: i64 = 1500;
    let f = Connection::open(":memory:").expect("open frank");
    let r = rusqlite::Connection::open_in_memory().expect("open rusqlite");

    for stmt in [
        "PRAGMA foreign_keys = OFF",
        "CREATE TABLE parent (id INTEGER PRIMARY KEY)",
        "CREATE TABLE child (id INTEGER PRIMARY KEY, pid INTEGER REFERENCES parent(id))",
    ] {
        f.execute(stmt).expect("frank ddl");
        r.execute_batch(stmt).expect("sqlite ddl");
    }

    // Insert N parent rows and N child rows in chunks. Every 200th child is an
    // orphan (pid points past the parent range), so there are exactly N/200.
    let mut parent_chunk = String::new();
    let mut child_chunk = String::new();
    for id in 1..=N {
        if !parent_chunk.is_empty() {
            parent_chunk.push(',');
            child_chunk.push(',');
        }
        parent_chunk.push_str(&format!("({id})"));
        let pid = if id % 200 == 0 { id + 1_000_000 } else { id };
        child_chunk.push_str(&format!("({id},{pid})"));
        if id % 300 == 0 || id == N {
            let psql = format!("INSERT INTO parent (id) VALUES {parent_chunk}");
            let csql = format!("INSERT INTO child (id, pid) VALUES {child_chunk}");
            f.execute(&psql).expect("frank parent insert");
            r.execute_batch(&psql).expect("sqlite parent insert");
            f.execute(&csql).expect("frank child insert");
            r.execute_batch(&csql).expect("sqlite child insert");
            parent_chunk.clear();
            child_chunk.clear();
        }
    }

    let fc = frank_rows(&f, "PRAGMA foreign_key_check").expect("frank fkc");
    let rc = sqlite_rows(&r, "PRAGMA foreign_key_check").expect("sqlite fkc");
    assert_eq!(
        fc, rc,
        "foreign_key_check orphan report must match rusqlite at scale"
    );
    assert_eq!(
        i64::try_from(fc.len()).unwrap(),
        N / 200,
        "expected exactly N/200 planted orphans"
    );
}
