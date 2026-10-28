#![recursion_limit = "512"]

//! bd-8djon: STRICT violations read as stock's do. Stock names the type the
//! column declared, so a column declared `INT` reads "INT" rather than
//! "INTEGER". It checks an INTEGER PRIMARY KEY (a rowid alias) only as a rowid,
//! so a value that is not an integer is the bare "datatype mismatch", also when
//! it arrives as a bound parameter or through INSERT ... SELECT. Every
//! statement runs on fsqlite and on rusqlite's bundled SQLite; the error texts,
//! change counts and rows must match.

use fsqlite_core::connection::Connection;
use fsqlite_types::value::SqliteValue;

#[derive(Clone, Copy, Debug)]
enum P {
    I(i64),
    R(f64),
    T(&'static str),
    B(&'static [u8]),
    Null,
}

impl P {
    fn fsqlite(self) -> SqliteValue {
        match self {
            Self::I(v) => SqliteValue::Integer(v),
            Self::R(v) => SqliteValue::Float(v),
            Self::T(v) => SqliteValue::Text(v.into()),
            Self::B(v) => SqliteValue::Blob(v.into()),
            Self::Null => SqliteValue::Null,
        }
    }

    fn stock(self) -> rusqlite::types::Value {
        match self {
            Self::I(v) => rusqlite::types::Value::Integer(v),
            Self::R(v) => rusqlite::types::Value::Real(v),
            Self::T(v) => rusqlite::types::Value::Text(v.to_owned()),
            Self::B(v) => rusqlite::types::Value::Blob(v.to_vec()),
            Self::Null => rusqlite::types::Value::Null,
        }
    }
}

const SCHEMA: &[&str] = &[
    "CREATE TABLE s2(a INT, b TEXT) STRICT",
    "CREATE TABLE s(a INTEGER PRIMARY KEY, b TEXT) STRICT",
    "CREATE TABLE s3(a int, b integer, c Int, d REAL, e TEXT, f BLOB, g ANY) STRICT",
];

use P::{B, I, Null, R, T};

const STATEMENTS: &[(&str, &[P])] = &[
    // A column declared INT.
    ("INSERT INTO s2 VALUES ('abc', 'v')", &[]),
    ("INSERT INTO s2 SELECT 'abc', 'v'", &[]),
    ("INSERT INTO s2 VALUES (?1, ?2)", &[T("abc"), T("v")]),
    ("INSERT INTO s2 SELECT * FROM (VALUES (1, 'a'), ('x', 'b'))", &[]),
    ("INSERT INTO s2 VALUES (1, 'v')", &[]),
    ("UPDATE s2 SET a = 'abc'", &[]),
    ("UPDATE s2 SET a = ?1", &[B(b"\x00")]),
    // A rowid alias: only the rowid check applies.
    ("INSERT INTO s VALUES ('abc', 'v')", &[]),
    ("INSERT INTO s SELECT 'abc', 'v'", &[]),
    ("INSERT INTO s VALUES (?1, ?2)", &[T("abc"), T("v")]),
    ("INSERT INTO s VALUES (1.5, 'v')", &[]),
    ("INSERT INTO s SELECT 1.5, 'v'", &[]),
    ("INSERT INTO s VALUES (?1, ?2)", &[R(2.5), T("v")]),
    ("INSERT INTO s VALUES (?1, ?2)", &[B(b"\x01"), T("v")]),
    ("INSERT INTO s SELECT x'02', 'v'", &[]),
    ("INSERT INTO s VALUES ('7', 'v')", &[]),
    ("INSERT INTO s SELECT '8', 'v'", &[]),
    ("INSERT INTO s VALUES (9.0, 'v')", &[]),
    ("INSERT INTO s VALUES (?1, ?2)", &[T("10"), T("v")]),
    ("INSERT INTO s VALUES (?1, ?2)", &[R(11.0), T("v")]),
    ("INSERT INTO s SELECT 12.0, 'v'", &[]),
    ("INSERT INTO s VALUES (?1, ?2)", &[Null, T("v")]),
    ("UPDATE s SET a = 'abc' WHERE a = 7", &[]),
    ("UPDATE s SET a = ?1 WHERE a = 7", &[R(2.5)]),
    ("UPDATE s SET a = '70' WHERE a = 7", &[]),
    // Each STRICT type, declared in different cases.
    ("INSERT INTO s3 VALUES ('x', 1, 1, 1.0, 't', x'00', 0)", &[]),
    ("INSERT INTO s3 VALUES (1, 'x', 1, 1.0, 't', x'00', 0)", &[]),
    ("INSERT INTO s3 VALUES (1, 1, 'x', 1.0, 't', x'00', 0)", &[]),
    ("INSERT INTO s3 VALUES (1, 1, x'00', 1.0, 't', x'00', 0)", &[]),
    ("INSERT INTO s3 VALUES (1, 1, 1, 'x', 't', x'00', 0)", &[]),
    ("INSERT INTO s3 VALUES (1, 1, 1, 1.0, 5, x'00', 0)", &[]),
    ("INSERT INTO s3 VALUES (1, 1, 1, 1.0, 't', 5, 0)", &[]),
    ("INSERT INTO s3 SELECT ?1, 1, 1, 1.0, 't', x'00', 0", &[T("y")]),
    (
        "INSERT INTO s3 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
        &[I(1), I(1), T("z"), R(1.0), T("t"), B(b"\x00"), T("any")],
    ),
    (
        "INSERT INTO s3 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
        &[I(2), T("2"), T("3"), I(4), T("t"), B(b"\x00"), R(0.5)],
    ),
    ("UPDATE s3 SET c = 'q'", &[]),
    ("UPDATE s3 SET b = ?1", &[R(1.5)]),
    ("SELECT a, typeof(a), b FROM s2 ORDER BY rowid", &[]),
    ("SELECT a, typeof(a), b FROM s ORDER BY a", &[]),
    ("SELECT *, typeof(b), typeof(d), typeof(g) FROM s3 ORDER BY rowid", &[]),
];

fn tag_f(v: &SqliteValue) -> String {
    match v {
        SqliteValue::Null => "NULL".to_owned(),
        SqliteValue::Integer(n) => format!("int:{n}"),
        SqliteValue::Float(f) => format!("real:{f}"),
        SqliteValue::Text(s) => format!("text:{s}"),
        SqliteValue::Blob(b) => format!("blob:{b:?}"),
    }
}

fn tag_r(v: &rusqlite::types::Value) -> String {
    match v {
        rusqlite::types::Value::Null => "NULL".to_owned(),
        rusqlite::types::Value::Integer(n) => format!("int:{n}"),
        rusqlite::types::Value::Real(f) => format!("real:{f}"),
        rusqlite::types::Value::Text(s) => format!("text:{s}"),
        rusqlite::types::Value::Blob(b) => format!("blob:{b:?}"),
    }
}

/// Stock's error message alone, without the SQL text rusqlite appends to an
/// error found while preparing.
fn stock_err(e: rusqlite::Error) -> String {
    match e {
        rusqlite::Error::SqlInputError { msg, .. }
        | rusqlite::Error::SqliteFailure(_, Some(msg)) => msg,
        other => other.to_string(),
    }
}

async fn run_fsqlite(conn: &Connection, sql: &str, params: &[P]) -> String {
    let params: Vec<SqliteValue> = params.iter().map(|p| p.fsqlite()).collect();
    if sql.starts_with("SELECT") {
        match conn.query_with_params(sql, &params).await {
            Ok(rows) => rows
                .iter()
                .map(|r| r.values().iter().map(tag_f).collect::<Vec<_>>().join("|"))
                .collect::<Vec<_>>()
                .join("\n"),
            Err(e) => format!("error: {e}"),
        }
    } else {
        match conn.execute_with_params(sql, &params).await {
            Ok(_) => format!("ok changes={}", changes_f(conn).await),
            Err(e) => format!("error: {e}"),
        }
    }
}

async fn changes_f(conn: &Connection) -> String {
    match conn.query("SELECT changes()").await {
        Ok(rows) => rows
            .first()
            .and_then(|r| r.values().first().map(tag_f))
            .unwrap_or_default(),
        Err(e) => format!("error: {e}"),
    }
}

fn run_stock(conn: &rusqlite::Connection, sql: &str, params: &[P]) -> String {
    let params: Vec<rusqlite::types::Value> = params.iter().map(|p| p.stock()).collect();
    let result = (|| -> Result<String, rusqlite::Error> {
        let mut st = conn.prepare(sql)?;
        if sql.starts_with("SELECT") {
            let n = st.column_count();
            let rows = st
                .query_map(rusqlite::params_from_iter(params.iter()), |row| {
                    Ok((0..n)
                        .map(|i| tag_r(&row.get_unwrap::<_, rusqlite::types::Value>(i)))
                        .collect::<Vec<_>>()
                        .join("|"))
                })?
                .collect::<Result<Vec<_>, _>>()?;
            Ok(rows.join("\n"))
        } else {
            st.execute(rusqlite::params_from_iter(params.iter()))?;
            let changes: i64 = conn.query_row("SELECT changes()", [], |r| r.get(0))?;
            Ok(format!("ok changes=int:{changes}"))
        }
    })();
    result.unwrap_or_else(|e| format!("error: {}", stock_err(e)))
}

#[test]
fn strict_violation_text_matches_stock() {
    asupersync::test_utils::run_test(|| async {
        let fsqlite = Connection::open(":memory:").await.expect("open fsqlite");
        let stock = rusqlite::Connection::open_in_memory().expect("open stock");
        for sql in SCHEMA {
            fsqlite.execute(sql).await.expect("fsqlite schema");
            stock.execute(sql, []).expect("stock schema");
        }

        let mut mismatches = Vec::new();
        for (sql, params) in STATEMENTS {
            let got = run_fsqlite(&fsqlite, sql, params).await;
            let want = run_stock(&stock, sql, params);
            if got != want {
                mismatches.push(format!(
                    "{sql} {params:?}\n  fsqlite: {got}\n  stock:   {want}"
                ));
            }
        }
        assert!(
            mismatches.is_empty(),
            "{} of {} statements differ from stock:\n{}",
            mismatches.len(),
            STATEMENTS.len(),
            mismatches.join("\n")
        );
    });
}
