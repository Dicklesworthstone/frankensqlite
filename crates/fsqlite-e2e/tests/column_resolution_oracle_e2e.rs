//! bd-xmtrp — Oracle-parity e2e: column-resolution & ambiguity error parity.
//!
//! Like the misuse checks, these confirm frank's name-resolution errors match
//! SQLite's: an unqualified column that exists in two joined tables is
//! "ambiguous column name" (but a qualified reference, or a column unique to one
//! side, resolves fine); a missing column/table errors; and an output-column
//! ALIAS is visible in ORDER BY but NOT in WHERE (where SQLite reports "no such
//! column"). The shared comparison treats (Err,Err) as agreement and flags a
//! divergence (one engine errors, the other succeeds).
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

async fn check(f: &Connection, r: &rusqlite::Connection, queries: &[&str], label: &str) {
    let mut mismatches = Vec::new();
    for q in queries {
        match (frank_rows(f, q).await, sqlite_rows(r, q)) {
            (Ok(a), Ok(b)) if a == b => {}
            (Ok(a), Ok(b)) => {
                mismatches.push(format!("MISMATCH: {q}\n  frank: {a:?}\n  csql:  {b:?}"))
            }
            (Err(e), Ok(b)) => mismatches.push(format!(
                "FRANK_ERR (frank rejected, csql accepted): {q}\n  frank: ERROR({e})\n  csql:  {b:?}"
            )),
            (Ok(a), Err(e)) => mismatches.push(format!(
                "CSQL_ERR (frank accepted, csql rejected): {q}\n  frank: {a:?}\n  csql: ERROR({e})"
            )),
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

async fn ab() -> (Connection, rusqlite::Connection) {
    let f = Connection::open(":memory:").await.unwrap();
    let r = rusqlite::Connection::open_in_memory().unwrap();
    for s in [
        "CREATE TABLE a (id INTEGER, x INTEGER)",
        "CREATE TABLE b (id INTEGER, y INTEGER)",
        "INSERT INTO a VALUES (1,10),(2,20)",
        "INSERT INTO b VALUES (1,100),(2,200),(3,300)",
    ] {
        f.execute(s).await.unwrap();
        r.execute_batch(s).unwrap();
    }
    (f, r)
}

#[test]
fn ambiguous_column_in_join_is_rejected() {
    asupersync::test_utils::run_test(|| async {
        let (f, r) = ab().await;
        check(
            &f,
            &r,
            &[
                // `id` exists in both a and b -> ambiguous -> error on both.
                "SELECT id FROM a JOIN b ON a.id = b.id",
            ],
            "ambiguous_column_in_join_is_rejected",
        )
        .await;
    });
}

#[test]
fn qualified_or_unique_column_resolves() {
    asupersync::test_utils::run_test(|| async {
        let (f, r) = ab().await;
        check(
            &f,
            &r,
            &[
                // Qualified references disambiguate.
                "SELECT a.id, b.id, y FROM a JOIN b ON a.id = b.id ORDER BY a.id",
                // x is unique to a, y unique to b -> unambiguous.
                "SELECT x, y FROM a JOIN b ON a.id = b.id ORDER BY x",
            ],
            "qualified_or_unique_column_resolves",
        )
        .await;
    });
}

#[test]
fn nonexistent_column_or_table_is_rejected() {
    asupersync::test_utils::run_test(|| async {
        let (f, r) = ab().await;
        check(
            &f,
            &r,
            &[
                "SELECT no_such FROM a",       // no such column
                "SELECT a.nope FROM a",        // no such column on a
                "SELECT * FROM no_such_table", // no such table
            ],
            "nonexistent_column_or_table_is_rejected",
        )
        .await;
    });
}

#[test]
fn output_alias_resolves_in_order_by_and_group_by() {
    asupersync::test_utils::run_test(|| async {
        let (f, r) = ab().await;
        check(
            &f,
            &r,
            &[
                // Alias `w` is visible in ORDER BY...
                "SELECT x AS w FROM a ORDER BY w",
                // ...and in GROUP BY.
                "SELECT x AS w, count(*) FROM a GROUP BY w ORDER BY w",
            ],
            "output_alias_resolves_in_order_by_and_group_by",
        )
        .await;
    });
}

/// bd-7mymt: three-part `schema.table.column` references (e.g. `main.a.id`)
/// must parse and resolve exactly like SQLite. Before the fix the parser
/// rejected the second dot ("expected RightParen, got Dot") and skipped the
/// whole statement. For the `main` database the schema qualifier resolves to
/// the same table as the two-part form, in the SELECT list, FROM clause,
/// JOIN/ON, WHERE, and ORDER BY.
#[test]
fn three_part_schema_qualified_column_resolves() {
    asupersync::test_utils::run_test(|| async {
        let (f, r) = ab().await;
        check(
            &f,
            &r,
            &[
                // Three-part column in SELECT list + FROM + ORDER BY.
                "SELECT main.a.id, main.a.x FROM main.a ORDER BY main.a.id",
                // Three-part column against an unqualified FROM table.
                "SELECT main.a.id, main.a.x FROM a ORDER BY main.a.id",
                // Three-part columns across a JOIN, in ON and WHERE.
                "SELECT main.a.id, main.b.y FROM main.a JOIN main.b \
                 ON main.a.id = main.b.id WHERE main.a.x > 5 ORDER BY main.a.id",
                // Round-trips as a plain projection.
                "SELECT main.a.x FROM main.a WHERE main.a.id = 1",
            ],
            "three_part_schema_qualified_column_resolves",
        )
        .await;
    });
}

/// bd-ujuzr: SQLite resolves a result-column alias in WHERE (leniency); frank
/// rejects it with "no such column". The ORDER BY/GROUP BY alias cases above
/// work, so frank is stricter than SQLite only in the WHERE clause.
#[test]
fn output_alias_in_where() {
    asupersync::test_utils::run_test(|| async {
        let (f, r) = ab().await;
        check(
            &f,
            &r,
            &["SELECT x AS w FROM a WHERE w > 5"], // SQLite -> [10,20]
            "output_alias_in_where",
        )
        .await;
    });
}
