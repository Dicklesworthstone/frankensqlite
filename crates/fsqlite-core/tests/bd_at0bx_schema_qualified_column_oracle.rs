#![recursion_limit = "512"]

//! bd-at0bx: a schema-qualified column reference (`schema.table.column`)
//! binds only to a FROM item of that table (or alias) in that schema, as
//! SQLite's `lookupName` does with its `zDb` argument. Same-name tables in
//! `main`, `temp` and an attached database each keep their own rows, an
//! unqualified or table-qualified reference that matches more than one of them
//! is "ambiguous column name", and a qualifier naming a schema the FROM item
//! is not in is "no such column: schema.table.column". Covered: the three
//! reported shapes, aliases, attached databases, correlated subqueries,
//! UPDATE/DELETE with a schema-qualified target, and a `main.`/`temp.`
//! qualifier on a table that exists only in the other schema. Compared with
//! stock SQLite (rusqlite, bundled): result rows, and the error message of
//! every failing statement.

use fsqlite_core::connection::Connection;
use fsqlite_types::value::SqliteValue;

fn tag_f(v: &SqliteValue) -> String {
    match v {
        SqliteValue::Null => "NULL".to_owned(),
        SqliteValue::Integer(n) => n.to_string(),
        SqliteValue::Float(f) => format!("{f:?}"),
        SqliteValue::Text(s) => format!("'{s}'"),
        SqliteValue::Blob(b) => format!("X'{}'", b.len()),
    }
}

fn tag_r(v: &rusqlite::types::Value) -> String {
    match v {
        rusqlite::types::Value::Null => "NULL".to_owned(),
        rusqlite::types::Value::Integer(n) => n.to_string(),
        rusqlite::types::Value::Real(f) => format!("{f:?}"),
        rusqlite::types::Value::Text(s) => format!("'{s}'"),
        rusqlite::types::Value::Blob(b) => format!("X'{}'", b.len()),
    }
}

async fn frank(f: &Connection, sql: &str) -> Vec<Vec<String>> {
    match f.query(sql).await {
        Ok(rows) => rows
            .iter()
            .map(|r| r.values().iter().map(tag_f).collect())
            .collect(),
        Err(e) => vec![vec![format!("<ERR {e}>")]],
    }
}

fn stock(r: &rusqlite::Connection, sql: &str) -> Vec<Vec<String>> {
    let mut st = match r.prepare(sql) {
        Ok(st) => st,
        Err(
            rusqlite::Error::SqliteFailure(_, Some(m)) | rusqlite::Error::SqlInputError { msg: m, .. },
        ) => return vec![vec![format!("<ERR {m}>")]],
        Err(e) => return vec![vec![format!("<ERR {e}>")]],
    };
    let n = st.column_count();
    match st
        .query_map([], |row| {
            Ok((0..n)
                .map(|i| tag_r(&row.get_unwrap::<_, rusqlite::types::Value>(i)))
                .collect::<Vec<_>>())
        })
        .and_then(Iterator::collect)
    {
        Ok(rows) => rows,
        Err(rusqlite::Error::SqliteFailure(_, Some(m))) => vec![vec![format!("<ERR {m}>")]],
        Err(e) => vec![vec![format!("<ERR {e}>")]],
    }
}

/// `main.users` holds 'm', `temp.users` holds 't', `aux.users` holds 'a';
/// `t` exists only in MAIN and `tonly` only in TEMP.
const SETUP: &[&str] = &[
    "CREATE TABLE main.users(name)",
    "INSERT INTO main.users VALUES ('m')",
    "CREATE TEMP TABLE users(name)",
    "INSERT INTO temp.users VALUES ('t')",
    "CREATE TABLE t(x)",
    "INSERT INTO t VALUES (1), (2)",
    "CREATE TEMP TABLE tonly(x)",
    "INSERT INTO tonly VALUES (7)",
    "ATTACH ':memory:' AS aux",
    "CREATE TABLE aux.users(name)",
    "INSERT INTO aux.users VALUES ('a')",
];

/// The three shapes reported in bd-at0bx.
const REPORTED: &[&str] = &[
    "SELECT main.users.name, temp.users.name FROM main.users JOIN temp.users",
    "SELECT users.name FROM main.users JOIN temp.users",
    "SELECT temp.t.x FROM t",
];

/// main/temp same-name tables, a qualifier on a table only the other schema
/// has, unknown schemas, and the clauses a column reference can sit in.
const MAIN_TEMP: &[&str] = &[
    "SELECT temp.users.name, main.users.name FROM temp.users JOIN main.users",
    "SELECT MAIN.USERS.NAME, Temp.Users.Name FROM main.users JOIN temp.users",
    "SELECT name FROM main.users JOIN temp.users",
    "SELECT * FROM main.users JOIN temp.users",
    "SELECT main.users.name FROM main.users JOIN temp.users WHERE temp.users.name = 't'",
    "SELECT temp.users.name FROM main.users JOIN temp.users WHERE main.users.name = 'zz'",
    "SELECT temp.users.name FROM main.users, temp.users ORDER BY main.users.name",
    "SELECT main.users.rowid, temp.users.rowid FROM main.users JOIN temp.users",
    "SELECT main.users.name FROM main.users JOIN temp.users ON main.users.name <> temp.users.name",
    "SELECT count(*) FROM main.users JOIN temp.users USING (name)",
    "SELECT main.users.name FROM users",
    "SELECT temp.users.name FROM users",
    "SELECT users.name FROM users",
    "SELECT main.users.name FROM main.users",
    "SELECT temp.users.name FROM main.users",
    "SELECT main.users.name FROM temp.users",
    "SELECT nosuch.users.name FROM users",
    "SELECT main.t.x FROM t ORDER BY 1",
    "SELECT temp.t.x FROM main.t",
    "SELECT main.t.x FROM t WHERE temp.t.x = 1",
    "SELECT x FROM t ORDER BY temp.t.x",
    "SELECT main.tonly.x FROM tonly",
    "SELECT temp.tonly.x FROM tonly",
    "SELECT x FROM temp.t",
    "SELECT x FROM main.tonly",
];

/// Aliases: the qualifier must name the alias, and the alias's FROM item must
/// be in the named schema.
const ALIASES: &[&str] = &[
    "SELECT m.name, tt.name FROM main.users AS m JOIN temp.users AS tt",
    "SELECT main.m.name FROM main.users AS m",
    "SELECT temp.m.name FROM main.users AS m",
    "SELECT main.users.name FROM main.users AS m",
    "SELECT main.x.name, temp.y.name FROM main.users AS x JOIN temp.users AS y",
    "SELECT temp.x.name FROM main.users AS x JOIN temp.users AS y",
    "SELECT main.t.x FROM t AS q",
    "SELECT main.q.x FROM t AS q ORDER BY main.q.x DESC",
];

/// Correlated subqueries: a qualified reference that the inner FROM cannot
/// supply (wrong schema) binds to the outer query.
const CORRELATED: &[&str] = &[
    "SELECT (SELECT main.users.name FROM temp.users) FROM main.users",
    "SELECT (SELECT users.name FROM temp.users) FROM main.users",
    "SELECT (SELECT temp.users.name FROM main.users) FROM temp.users",
    "SELECT (SELECT temp.users.name FROM temp.users WHERE main.users.name = 'm') FROM main.users",
    "SELECT main.users.name FROM main.users WHERE EXISTS (SELECT 1 FROM temp.users WHERE temp.users.name = 't' AND main.users.name = 'm')",
    "SELECT main.users.name FROM main.users WHERE main.users.name IN (SELECT main.users.name FROM temp.users)",
    "SELECT (SELECT count(*) FROM temp.users WHERE temp.users.name = main.users.name) FROM main.users",
];

/// An attached database with a table of the same name.
const ATTACHED: &[&str] = &[
    "SELECT main.users.name, aux.users.name FROM main.users JOIN aux.users",
    "SELECT aux.users.name, temp.users.name FROM aux.users JOIN temp.users",
    "SELECT users.name FROM main.users JOIN aux.users",
    "SELECT aux.users.name FROM main.users",
    "SELECT main.users.name FROM aux.users",
    "SELECT aux.users.name FROM aux.users",
    "SELECT aux.u.name FROM aux.users AS u",
    "SELECT main.u.name FROM aux.users AS u",
    "SELECT (SELECT aux.users.name FROM main.users) FROM aux.users",
];

/// DML with a schema-qualified target, each followed by `STATE`.
const DML: &[&str] = &[
    "UPDATE main.users SET name = main.users.name || '1'",
    "UPDATE temp.users SET name = temp.users.name || '2'",
    "UPDATE temp.users SET name = main.users.name",
    "UPDATE main.users SET name = 'z' WHERE temp.users.name = 't2'",
    "UPDATE users SET name = temp.users.name || '3'",
    "UPDATE users SET name = main.users.name",
    "DELETE FROM temp.users WHERE main.users.name = 'm1'",
    "DELETE FROM main.users WHERE main.users.name = 'zz'",
    "UPDATE main.t SET x = main.t.x + 10 WHERE main.t.x = 1",
    "UPDATE t SET x = temp.t.x",
    "DELETE FROM main.t WHERE temp.t.x = 2",
    "DELETE FROM t WHERE main.t.x = 2",
    "UPDATE tonly SET x = main.tonly.x",
    "UPDATE temp.tonly SET x = temp.tonly.x + 1",
    "UPDATE main.users SET name = (SELECT temp.users.name FROM temp.users) || main.users.name",
    "UPDATE aux.users SET name = aux.users.name || '!'",
    "UPDATE aux.users SET name = main.users.name",
    "DELETE FROM aux.users WHERE main.users.name = 'x'",
    "DELETE FROM temp.users WHERE temp.users.name = 'nope'",
];

const STATE: &[&str] = &[
    "SELECT name FROM main.users",
    "SELECT name FROM temp.users",
    "SELECT name FROM aux.users",
    "SELECT x FROM main.t ORDER BY x",
    "SELECT x FROM temp.tonly",
];

async fn run_statement(
    f: &Connection,
    r: &rusqlite::Connection,
    sql: &str,
    failures: &mut Vec<String>,
) {
    let fo = match f.execute(sql).await {
        Ok(_) => "ok".to_owned(),
        Err(e) => format!("error: {e}"),
    };
    let so = match r.execute_batch(sql) {
        Ok(()) => "ok".to_owned(),
        Err(rusqlite::Error::SqliteFailure(_, Some(m))) => format!("error: {m}"),
        Err(e) => format!("error: {e}"),
    };
    if fo != so {
        failures.push(format!("`{sql}`: frank {fo:?} vs stock {so:?}"));
    }
}

async fn compare_queries(
    f: &Connection,
    r: &rusqlite::Connection,
    queries: &[&str],
    failures: &mut Vec<String>,
) {
    for sql in queries {
        let (fv, sv) = (frank(f, sql).await, stock(r, sql));
        if fv != sv {
            failures.push(format!("`{sql}`:\n  frank {fv:?}\n  stock {sv:?}"));
        }
    }
}

/// Opens both engines (FrankenSQLite on a file or `:memory:`, stock in
/// memory), runs `SETUP` on both, and checks every setup statement agrees.
async fn open_pair(
    file_backed: bool,
    dir: &tempfile::TempDir,
    failures: &mut Vec<String>,
) -> (Connection, rusqlite::Connection) {
    let path = if file_backed {
        dir.path()
            .join("at0bx.db")
            .to_str()
            .expect("utf-8 path")
            .to_owned()
    } else {
        ":memory:".to_owned()
    };
    let f = Connection::open(&path).await.expect("open");
    let r = rusqlite::Connection::open_in_memory().expect("stock open");
    for sql in SETUP {
        run_statement(&f, &r, sql, failures).await;
    }
    (f, r)
}

async fn run_queries(file_backed: bool, queries: &[&str]) -> Vec<String> {
    let dir = tempfile::tempdir().expect("temp dir");
    let mut failures = Vec::new();
    let (f, r) = open_pair(file_backed, &dir, &mut failures).await;
    compare_queries(&f, &r, queries, &mut failures).await;
    f.close().await.expect("close");
    failures
}

async fn run_dml(file_backed: bool) -> Vec<String> {
    let dir = tempfile::tempdir().expect("temp dir");
    let mut failures = Vec::new();
    let (f, r) = open_pair(file_backed, &dir, &mut failures).await;
    for sql in DML {
        run_statement(&f, &r, sql, &mut failures).await;
        let before = failures.len();
        compare_queries(&f, &r, STATE, &mut failures).await;
        for failure in &mut failures[before..] {
            *failure = format!("after `{sql}`, {failure}");
        }
    }
    f.close().await.expect("close");
    failures
}

fn assert_no_failures(label: &str, failures: &[String]) {
    assert!(
        failures.is_empty(),
        "{label}: {} differences from SQLite:\n{}",
        failures.len(),
        failures.join("\n")
    );
}

fn check_queries(label: &str, queries: &'static [&'static str]) {
    asupersync::test_utils::run_test(|| async move {
        let mut failures = Vec::new();
        for file_backed in [false, true] {
            for failure in run_queries(file_backed, queries).await {
                failures.push(format!("[file_backed={file_backed}] {failure}"));
            }
        }
        assert_no_failures(label, &failures);
    });
}

#[test]
fn reported_main_temp_qualified_columns_match_stock() {
    check_queries("reported", REPORTED);
}

#[test]
fn main_temp_qualified_columns_match_stock() {
    check_queries("main/temp", MAIN_TEMP);
}

#[test]
fn aliased_qualified_columns_match_stock() {
    check_queries("aliases", ALIASES);
}

#[test]
fn correlated_qualified_columns_match_stock() {
    check_queries("correlated", CORRELATED);
}

#[test]
fn attached_qualified_columns_match_stock() {
    check_queries("attached", ATTACHED);
}

#[test]
fn qualified_dml_targets_match_stock() {
    asupersync::test_utils::run_test(|| async {
        let mut failures = Vec::new();
        for file_backed in [false, true] {
            for failure in run_dml(file_backed).await {
                failures.push(format!("[file_backed={file_backed}] {failure}"));
            }
        }
        assert_no_failures("dml", &failures);
    });
}
