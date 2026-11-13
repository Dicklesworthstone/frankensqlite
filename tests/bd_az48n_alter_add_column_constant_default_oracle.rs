#![recursion_limit = "512"]

//! bd-az48n: ALTER TABLE ADD COLUMN on a non-empty table accepts every default
//! that stock SQLite can fold to a value (`sqlite3ValueFromExpr`): unary plus is
//! dropped, unary minus negates (numerifying strings, `-'5x'` is -5), nested
//! signs and CAST fold, and TRUE/FALSE, NULL and blob literals are values.
//! Functions, arithmetic, COLLATE and CURRENT_* stay "non-constant".
//!
//! Each case runs on FrankenSQLite (file-backed and `:memory:`) and on stock
//! SQLite (rusqlite), and compares the ALTER outcome and the value and type
//! every pre-existing row reads back, then a row inserted afterwards.

use fsqlite_core::connection::Connection;
use fsqlite_types::value::SqliteValue;

fn tag_f(v: &SqliteValue) -> String {
    match v {
        SqliteValue::Null => "NULL".to_owned(),
        SqliteValue::Integer(n) => n.to_string(),
        SqliteValue::Float(f) => format!("{f:?}"),
        SqliteValue::Text(s) => format!("'{s}'"),
        SqliteValue::Blob(b) => format!(
            "X'{}'",
            b.iter().map(|x| format!("{x:02X}")).collect::<String>()
        ),
    }
}

fn tag_r(v: &rusqlite::types::Value) -> String {
    match v {
        rusqlite::types::Value::Null => "NULL".to_owned(),
        rusqlite::types::Value::Integer(n) => n.to_string(),
        rusqlite::types::Value::Real(f) => format!("{f:?}"),
        rusqlite::types::Value::Text(s) => format!("'{s}'"),
        rusqlite::types::Value::Blob(b) => format!(
            "X'{}'",
            b.iter().map(|x| format!("{x:02X}")).collect::<String>()
        ),
    }
}

async fn frank_rows(f: &Connection, sql: &str) -> Vec<Vec<String>> {
    match f.query(sql).await {
        Ok(rows) => rows
            .iter()
            .map(|r| r.values().iter().map(tag_f).collect())
            .collect(),
        Err(e) => vec![vec![format!("<ERR {e}>")]],
    }
}

fn stock_rows(r: &rusqlite::Connection, sql: &str) -> Vec<Vec<String>> {
    let mut st = match r.prepare(sql) {
        Ok(st) => st,
        Err(e) => return vec![vec![format!("<ERR {e}>")]],
    };
    let n = st.column_count();
    st.query_map([], |row| {
        Ok((0..n)
            .map(|i| tag_r(&row.get_unwrap::<_, rusqlite::types::Value>(i)))
            .collect())
    })
    .unwrap()
    .map(Result::unwrap)
    .collect()
}

fn stock_outcome(result: rusqlite::Result<()>) -> String {
    match result {
        Ok(()) => "ok".to_owned(),
        Err(rusqlite::Error::SqliteFailure(_, Some(message))) => format!("error: {message}"),
        Err(other) => format!("error: {other}"),
    }
}

const COLUMN_TYPES: [&str; 5] = ["", "INTEGER", "TEXT", "REAL", "BLOB"];

/// Defaults stock accepts on a non-empty table, then ones it refuses.
const DEFAULTS: [&str; 33] = [
    "+'5'",
    "-'5x'",
    "(-(-1.50))",
    "(CAST('5' AS INTEGER))",
    "(CAST(1.5 AS INTEGER))",
    "(CAST(-'2' AS REAL))",
    "- 'abc'",
    "(+(-3))",
    "-9223372036854775808",
    "(- -0x10)",
    "-0x10",
    "TRUE",
    "FALSE",
    "(-TRUE)",
    "x'0102'",
    "(-x'31')",
    "NULL",
    "(NULL)",
    "-NULL",
    "(CAST(NULL AS TEXT))",
    "(CAST(x'41' AS TEXT))",
    "+x'41'",
    "(-'')",
    "(-'1e3')",
    "(CAST('12abc' AS NUMERIC))",
    "(-(-(-2)))",
    "abc",
    "('a' COLLATE nocase)",
    "(1+1)",
    "(abs(-1))",
    "CURRENT_TIME",
    "(-CURRENT_DATE)",
    "(-(1))",
];

async fn run_case(file_backed: bool, col_type: &str, default: &str, failures: &mut Vec<String>) {
    let label = format!(
        "[{} | {col_type:?}] DEFAULT {default}",
        if file_backed { "file" } else { "memory" }
    );
    let dir = tempfile::tempdir().expect("temp dir");
    let path = dir.path().join("frank.db");
    let frank = if file_backed {
        Connection::open(path.to_str().expect("utf-8 path"))
            .await
            .expect("open")
    } else {
        Connection::open(":memory:").await.expect("open")
    };
    let stock = rusqlite::Connection::open_in_memory().expect("stock open");
    let setup = "CREATE TABLE t(a); INSERT INTO t VALUES (1), (2);";
    frank.execute_batch(setup).await.expect("frank setup");
    stock.execute_batch(setup).expect("stock setup");

    let alter = format!("ALTER TABLE t ADD COLUMN c {col_type} DEFAULT {default}");
    let frank_outcome = match frank.execute(&alter).await {
        Ok(_) => "ok".to_owned(),
        Err(e) => format!("error: {e}"),
    };
    let stock_outcome = stock_outcome(stock.execute_batch(&alter));
    if frank_outcome != stock_outcome {
        failures.push(format!(
            "{label}: ALTER outcome {frank_outcome:?} vs SQLite {stock_outcome:?}"
        ));
        return;
    }
    if stock_outcome != "ok" {
        return;
    }
    for step in ["", "INSERT INTO t(a) VALUES (3)"] {
        if !step.is_empty() {
            frank.execute(step).await.expect("frank insert");
            stock.execute_batch(step).expect("stock insert");
        }
        let query = "SELECT a, c, typeof(c) FROM t ORDER BY a";
        let (f, s) = (frank_rows(&frank, query).await, stock_rows(&stock, query));
        if f != s {
            failures.push(format!(
                "{label}: `{query}`{}:\n  frank {f:?}\n  stock {s:?}",
                if step.is_empty() { "" } else { " after INSERT" }
            ));
        }
    }
    frank.close().await.expect("close");
}

#[test]
fn add_column_on_non_empty_table_accepts_every_default_stock_folds_to_a_value() {
    asupersync::test_utils::run_test(|| async {
        let mut failures = Vec::new();
        let mut cases = 0_usize;
        for file_backed in [true, false] {
            for col_type in COLUMN_TYPES {
                for default in DEFAULTS {
                    run_case(file_backed, col_type, default, &mut failures).await;
                    cases += 1;
                }
            }
        }
        assert!(
            failures.is_empty(),
            "{} of {cases} ADD COLUMN cases differ from SQLite:\n{}",
            failures.len(),
            failures.join("\n")
        );
    });
}
