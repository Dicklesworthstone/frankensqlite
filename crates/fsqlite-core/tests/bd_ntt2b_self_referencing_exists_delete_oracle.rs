#![recursion_limit = "512"]

//! bd-ntt2b: a DELETE whose WHERE holds a correlated EXISTS reading the target
//! table (the dedupe idiom) must delete exactly the rows stock deletes. The
//! DELETE lane for that shape froze the matching rows with `SELECT *` and took
//! each row's first column as its rowid, so it deleted the row the statement
//! meant to keep (an integer first column), or nothing (a text first column, a
//! WITHOUT ROWID table). Compared with stock SQLite (rusqlite, bundled): the
//! changed-row count or RETURNING rows, then the table's rows, on `:memory:`
//! and on a file, through `Connection::execute` and a prepared statement.
//! WITHOUT ROWID cases run on `:memory:` as well since bd-pxn52.

use fsqlite_core::connection::Connection;
use fsqlite_types::value::SqliteValue;

struct Case {
    setup: &'static [&'static str],
    statement: &'static str,
    state: &'static str,
}

const ROWID_INT_FIRST: &[&str] = &[
    "CREATE TABLE t(k, v)",
    "INSERT INTO t VALUES (1, 'a'), (1, 'b'), (5, 'c'), (2, 'd'), (5, 'e')",
];
const ROWID_TEXT_FIRST: &[&str] = &[
    "CREATE TABLE t(v, k)",
    "INSERT INTO t VALUES ('a', 1), ('b', 1), ('c', 5), ('d', 2), ('e', 5)",
];
const IPK: &[&str] = &[
    "CREATE TABLE t(id INTEGER PRIMARY KEY, k, v)",
    "INSERT INTO t VALUES (10, 1, 'a'), (20, 1, 'b'), (30, 5, 'c'), (40, 2, 'd'), (50, 5, 'e')",
];
const WITHOUT_ROWID: &[&str] = &[
    "CREATE TABLE t(k PRIMARY KEY, v) WITHOUT ROWID",
    "INSERT INTO t VALUES (1, 'a'), (2, 'a'), (3, 'b'), (4, 'c'), (5, 'c')",
];
const WITHOUT_ROWID_KEY_LAST: &[&str] = &[
    "CREATE TABLE t(v, k PRIMARY KEY) WITHOUT ROWID",
    "INSERT INTO t VALUES ('a', 10), ('a', 20), ('b', 30), ('c', 40), ('c', 50)",
];
const WITHOUT_ROWID_COMPOSITE: &[&str] = &[
    "CREATE TABLE t(a, b, v, PRIMARY KEY (a, b)) WITHOUT ROWID",
    "INSERT INTO t VALUES (1, 'x', 'p'), (1, 'y', 'p'), (2, 'x', 'q'), (2, 'y', 'r'), (3, 'z', 'r')",
];
const MANY: &[&str] = &[
    "CREATE TABLE t(k, v)",
    "WITH RECURSIVE n(i) AS (SELECT 1 UNION ALL SELECT i + 1 FROM n WHERE i < 300) INSERT INTO t SELECT i % 37, 'v' || i FROM n",
];

const CASES: &[Case] = &[
    Case {
        setup: ROWID_INT_FIRST,
        statement: "DELETE FROM t WHERE EXISTS (SELECT 1 FROM t AS t2 WHERE t2.k = t.k AND t2.rowid < t.rowid)",
        state: "SELECT rowid, k, v FROM t ORDER BY rowid",
    },
    Case {
        setup: ROWID_INT_FIRST,
        statement: "DELETE FROM t AS x WHERE EXISTS (SELECT 1 FROM t WHERE t.k = x.k AND t.rowid > x.rowid)",
        state: "SELECT rowid, k, v FROM t ORDER BY rowid",
    },
    Case {
        setup: ROWID_INT_FIRST,
        statement: "DELETE FROM t WHERE NOT EXISTS (SELECT 1 FROM t AS t2 WHERE t2.k = t.k AND t2.rowid <> t.rowid)",
        state: "SELECT rowid, k, v FROM t ORDER BY rowid",
    },
    Case {
        setup: ROWID_INT_FIRST,
        statement: "DELETE FROM t WHERE EXISTS (SELECT 1 FROM t AS t2 WHERE t2.k = t.k AND t2.rowid < t.rowid) RETURNING k, v",
        state: "SELECT rowid, k, v FROM t ORDER BY rowid",
    },
    Case {
        setup: ROWID_TEXT_FIRST,
        statement: "DELETE FROM t WHERE EXISTS (SELECT 1 FROM t AS t2 WHERE t2.k = t.k AND t2.rowid < t.rowid)",
        state: "SELECT rowid, v, k FROM t ORDER BY rowid",
    },
    Case {
        setup: IPK,
        statement: "DELETE FROM t WHERE EXISTS (SELECT 1 FROM t AS t2 WHERE t2.k = t.k AND t2.id < t.id)",
        state: "SELECT id, k, v FROM t ORDER BY id",
    },
    Case {
        setup: WITHOUT_ROWID,
        statement: "DELETE FROM t WHERE EXISTS (SELECT 1 FROM t AS t2 WHERE t2.v = t.v AND t2.k < t.k)",
        state: "SELECT k, v FROM t ORDER BY k",
    },
    Case {
        setup: WITHOUT_ROWID_KEY_LAST,
        statement: "DELETE FROM t WHERE EXISTS (SELECT 1 FROM t AS t2 WHERE t2.v = t.v AND t2.k < t.k)",
        state: "SELECT v, k FROM t ORDER BY k",
    },
    Case {
        setup: WITHOUT_ROWID_COMPOSITE,
        statement: "DELETE FROM t WHERE EXISTS (SELECT 1 FROM t AS t2 WHERE t2.v = t.v AND (t2.a < t.a OR (t2.a = t.a AND t2.b < t.b)))",
        state: "SELECT a, b, v FROM t ORDER BY a, b",
    },
    Case {
        setup: MANY,
        statement: "DELETE FROM t WHERE EXISTS (SELECT 1 FROM t AS t2 WHERE t2.k = t.k AND t2.rowid < t.rowid)",
        state: "SELECT rowid, k, v FROM t ORDER BY rowid",
    },
];

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

fn stock_message(error: rusqlite::Error) -> String {
    match error {
        rusqlite::Error::SqliteFailure(_, Some(m)) | rusqlite::Error::SqlInputError { msg: m, .. } => m,
        other => other.to_string(),
    }
}

fn stock_rows(r: &rusqlite::Connection, sql: &str) -> Result<Vec<Vec<String>>, String> {
    let mut statement = r.prepare(sql).map_err(stock_message)?;
    let n = statement.column_count();
    statement
        .query_map([], |row| {
            Ok((0..n)
                .map(|i| tag_r(&row.get_unwrap::<_, rusqlite::types::Value>(i)))
                .collect::<Vec<_>>())
        })
        .and_then(Iterator::collect)
        .map_err(stock_message)
}

fn frank_rows(rows: &[fsqlite_core::connection::Row]) -> Vec<Vec<String>> {
    rows.iter()
        .map(|row| row.values().iter().map(tag_f).collect())
        .collect()
}

fn stock_outcome(r: &rusqlite::Connection, sql: &str) -> String {
    if sql.contains("RETURNING") {
        return match stock_rows(r, sql) {
            Ok(rows) => format!("rows {rows:?}"),
            Err(m) => format!("error: {m}"),
        };
    }
    match r.prepare(sql).and_then(|mut statement| statement.execute([])) {
        Ok(changed) => format!("changed {changed}"),
        Err(e) => format!("error: {}", stock_message(e)),
    }
}

async fn frank_outcome(f: &Connection, sql: &str, prepared: bool) -> String {
    if sql.contains("RETURNING") {
        return match f.query(sql).await {
            Ok(rows) => format!("rows {:?}", frank_rows(&rows)),
            Err(e) => format!("error: {e}"),
        };
    }
    let changed = if prepared {
        match f.prepare(sql).await {
            Ok(statement) => statement.execute().await,
            Err(e) => Err(e),
        }
    } else {
        f.execute(sql).await
    };
    match changed {
        Ok(changed) => format!("changed {changed}"),
        Err(e) => format!("error: {e}"),
    }
}

async fn check_case(case: &Case, file_backed: bool, prepared: bool) -> Vec<String> {
    let dir = tempfile::tempdir().expect("temp dir");
    let path = if file_backed {
        dir.path().join("ntt2b.db").to_str().expect("utf-8 path").to_owned()
    } else {
        ":memory:".to_owned()
    };
    let f = Connection::open(&path).await.expect("open");
    let r = rusqlite::Connection::open_in_memory().expect("stock open");
    for setup in case.setup {
        f.execute(setup).await.expect("frank setup");
        r.execute(setup, []).expect("stock setup");
    }
    let mut failures = Vec::new();
    let label = format!("[file_backed={file_backed} prepared={prepared}] `{}`", case.statement);
    let fo = frank_outcome(&f, case.statement, prepared).await;
    let so = stock_outcome(&r, case.statement);
    if fo != so {
        failures.push(format!("{label}: frank {fo:?} vs stock {so:?}"));
    }
    let fv = match f.query(case.state).await {
        Ok(rows) => Ok(frank_rows(&rows)),
        Err(e) => Err(e.to_string()),
    };
    let sv = stock_rows(&r, case.state);
    if fv != sv {
        failures.push(format!("{label}, then `{}`: frank {fv:?} vs stock {sv:?}", case.state));
    }
    f.close().await.expect("close");
    failures
}

#[test]
fn self_referencing_exists_delete_matches_stock() {
    asupersync::test_utils::run_test(|| async {
        let mut failures = Vec::new();
        for case in CASES {
            // RETURNING rows of a prepared DML statement are not readable
            // through `PreparedStatement::query`; that case runs directly.
            let modes: &[bool] = if case.statement.contains("RETURNING") {
                &[false]
            } else {
                &[false, true]
            };
            for file_backed in [false, true] {
                for &prepared in modes {
                    failures.extend(check_case(case, file_backed, prepared).await);
                }
            }
        }
        assert!(
            failures.is_empty(),
            "{} differences from SQLite:\n{}",
            failures.len(),
            failures.join("\n")
        );
    });
}
