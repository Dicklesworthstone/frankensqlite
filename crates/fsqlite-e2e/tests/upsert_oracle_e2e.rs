//! bd-95bef — Oracle-parity e2e: UPSERT (ON CONFLICT DO ...) vs rusqlite.
//!
//! The UPSERT clause (`INSERT ... ON CONFLICT(target) DO UPDATE/NOTHING`,
//! SQLite 3.24+) is distinct from the `INSERT OR REPLACE/IGNORE` prefix and has
//! several subtle pieces: the `excluded.*` pseudo-table referring to the row
//! that would have been inserted, SET expressions that combine the existing
//! column with `excluded` (the counter idiom `n = n + excluded.n`), a
//! conditional `DO UPDATE ... WHERE` that can turn the update into a no-op, a
//! conflict target on a non-PK UNIQUE column, the bare targetless
//! `ON CONFLICT DO NOTHING`, and multi-row VALUES where some rows insert and
//! others upsert. Each scenario asserts per-statement agreement with rusqlite,
//! then compares the resulting table state.

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

async fn scenario(stmts: &[&str], queries: &[&str], label: &str) {
    let f = Connection::open(":memory:").await.expect("open frank");
    let r = rusqlite::Connection::open_in_memory().expect("open rusqlite");
    for s in stmts {
        let fe = f.execute(s).await;
        let re = r.execute_batch(s);
        match (&fe, &re) {
            (Ok(_), Ok(())) | (Err(_), Err(_)) => {}
            (Ok(_), Err(e)) => panic!("{label}: `{s}`\n  frank: OK\n  csql:  ERROR({e})"),
            (Err(e), Ok(())) => panic!("{label}: `{s}`\n  frank: ERROR({e})\n  csql:  OK"),
        }
    }
    let mut mismatches = Vec::new();
    for q in queries {
        match (frank_rows(&f, q).await, sqlite_rows(&r, q)) {
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
fn upsert_do_nothing() {
    asupersync::test_utils::run_test(|| async {
        scenario(
            &[
                "CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER)",
                "INSERT INTO t VALUES (1,10),(2,20)",
                "INSERT INTO t (id,v) VALUES (1,999) ON CONFLICT(id) DO NOTHING", // skipped
                "INSERT INTO t (id,v) VALUES (3,30) ON CONFLICT(id) DO NOTHING",  // inserted
            ],
            &["SELECT id, v FROM t ORDER BY id"], // (1,10),(2,20),(3,30)
            "upsert_do_nothing",
        )
        .await;
    });
}

#[test]
fn upsert_do_update_excluded() {
    asupersync::test_utils::run_test(|| async {
        scenario(
            &[
                "CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER)",
                "INSERT INTO t VALUES (1,10)",
                "INSERT INTO t (id,v) VALUES (1,99) ON CONFLICT(id) DO UPDATE SET v = excluded.v", // 1->99
                "INSERT INTO t (id,v) VALUES (2,5) ON CONFLICT(id) DO UPDATE SET v = excluded.v", // insert
            ],
            &["SELECT id, v FROM t ORDER BY id"], // (1,99),(2,5)
            "upsert_do_update_excluded",
        )
        .await;
    });
}

#[test]
fn upsert_combine_existing_and_excluded_counter() {
    asupersync::test_utils::run_test(|| async {
        scenario(
            &[
                "CREATE TABLE counts (k TEXT PRIMARY KEY, n INTEGER)",
                "INSERT INTO counts VALUES ('a',1)",
                // Classic counter idiom: n = existing n + excluded n.
                "INSERT INTO counts (k,n) VALUES ('a',10) ON CONFLICT(k) DO UPDATE SET n = n + excluded.n",
                "INSERT INTO counts (k,n) VALUES ('b',5) ON CONFLICT(k) DO UPDATE SET n = n + excluded.n",
                "INSERT INTO counts (k,n) VALUES ('a',100) ON CONFLICT(k) DO UPDATE SET n = n + excluded.n",
            ],
            &["SELECT k, n FROM counts ORDER BY k"], // (a,111),(b,5)
            "upsert_combine_existing_and_excluded_counter",
        )
        .await;
    });
}

#[test]
fn upsert_do_update_conditional_where() {
    asupersync::test_utils::run_test(|| async {
        scenario(
            &[
                "CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER)",
                "INSERT INTO t VALUES (1,10),(2,20)",
                // Only update when the new value beats the existing one.
                "INSERT INTO t (id,v) VALUES (1,5) ON CONFLICT(id) DO UPDATE SET v = excluded.v WHERE excluded.v > t.v",
                "INSERT INTO t (id,v) VALUES (2,99) ON CONFLICT(id) DO UPDATE SET v = excluded.v WHERE excluded.v > t.v",
            ],
            &["SELECT id, v FROM t ORDER BY id"], // (1,10) unchanged, (2,99) updated
            "upsert_do_update_conditional_where",
        )
        .await;
    });
}

#[test]
fn upsert_on_non_pk_unique_target() {
    asupersync::test_utils::run_test(|| async {
        scenario(
            &[
                "CREATE TABLE t (id INTEGER PRIMARY KEY, email TEXT UNIQUE, hits INTEGER)",
                "INSERT INTO t VALUES (1,'a@x',1)",
                // Conflict on the UNIQUE email column -> bump the existing row's hits.
                "INSERT INTO t (id,email,hits) VALUES (2,'a@x',1) ON CONFLICT(email) DO UPDATE SET hits = hits + 1",
            ],
            &["SELECT id, email, hits FROM t ORDER BY id"], // single row (1,'a@x',2)
            "upsert_on_non_pk_unique_target",
        )
        .await;
    });
}

#[test]
fn upsert_bare_on_conflict_do_nothing() {
    asupersync::test_utils::run_test(|| async {
        scenario(
            &[
                "CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER)",
                "INSERT INTO t VALUES (1,10)",
                // Targetless DO NOTHING swallows any conflict.
                "INSERT INTO t (id,v) VALUES (1,99) ON CONFLICT DO NOTHING",
                "INSERT INTO t (id,v) VALUES (2,20) ON CONFLICT DO NOTHING",
            ],
            &["SELECT id, v FROM t ORDER BY id"], // (1,10),(2,20)
            "upsert_bare_on_conflict_do_nothing",
        )
        .await;
    });
}

#[test]
fn upsert_multi_row_mixed_insert_and_update() {
    asupersync::test_utils::run_test(|| async {
        scenario(
            &[
                "CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER)",
                "INSERT INTO t VALUES (1,10)",
                // Row 1 conflicts (updated), rows 2/3 are fresh inserts.
                "INSERT INTO t (id,v) VALUES (1,1),(2,2),(3,3) ON CONFLICT(id) DO UPDATE SET v = excluded.v",
            ],
            &["SELECT id, v FROM t ORDER BY id"], // (1,1),(2,2),(3,3)
            "upsert_multi_row_mixed_insert_and_update",
        )
        .await;
    });
}
