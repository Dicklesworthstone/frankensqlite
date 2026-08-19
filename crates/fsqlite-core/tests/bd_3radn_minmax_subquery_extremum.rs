//! bd-3radn H2: SQLite sources every bare output from the `min()`/`max()`
//! extremum row when that is the query's only aggregate — INCLUDING a scalar
//! subquery in the output that correlates to the outer row (e.g. `(SELECT name)`
//! resolves to the extremum row's `name`).
//!
//! Commit b7c53f8d2 (bd-0174u) rewrote the bare-column tracking to descend into
//! output expressions but regressed a subquery output to "opaque" (never set
//! `has_bare`), so `SELECT max(price), (SELECT name) FROM t` returned the wrong
//! row. Pinned differentially against rusqlite (which ships stock min/max).

use fsqlite_core::connection::Connection;
use fsqlite_types::value::SqliteValue;

fn tag_f(v: &SqliteValue) -> String {
    match v {
        SqliteValue::Null => "NULL".to_owned(),
        SqliteValue::Integer(n) => n.to_string(),
        SqliteValue::Float(f) => format!("{f}"),
        SqliteValue::Text(s) => format!("'{s}'"),
        SqliteValue::Blob(b) => format!("blob{}", b.len()),
    }
}

fn tag_r(v: &rusqlite::types::Value) -> String {
    match v {
        rusqlite::types::Value::Null => "NULL".to_owned(),
        rusqlite::types::Value::Integer(n) => n.to_string(),
        rusqlite::types::Value::Real(f) => format!("{f}"),
        rusqlite::types::Value::Text(s) => format!("'{s}'"),
        rusqlite::types::Value::Blob(b) => format!("blob{}", b.len()),
    }
}

async fn assert_agree(fconn: &Connection, rconn: &rusqlite::Connection, sql: &str) {
    let frows = fconn
        .query(sql)
        .await
        .unwrap_or_else(|e| panic!("franken `{sql}`: {e:?}"));
    let ff: Vec<Vec<String>> = frows
        .iter()
        .map(|row| row.values().iter().map(tag_f).collect())
        .collect();

    let mut stmt = rconn.prepare(sql).expect("rusqlite prepare");
    let ncol = stmt.column_count();
    let rr: Vec<Vec<String>> = stmt
        .query_map([], |row| {
            Ok((0..ncol)
                .map(|i| tag_r(&row.get::<_, rusqlite::types::Value>(i).unwrap()))
                .collect())
        })
        .expect("rusqlite query")
        .map(|r| r.unwrap())
        .collect();

    assert_eq!(ff, rr, "bd-3radn mismatch on `{sql}`");
}

#[test]
fn h2_scalar_subquery_output_sourced_from_extremum_row() {
    asupersync::test_utils::run_test(|| async {
        let f = Connection::open(":memory:").await.unwrap();
        let r = rusqlite::Connection::open_in_memory().unwrap();
        for ddl in [
            "CREATE TABLE t(name TEXT, price INTEGER)",
            "INSERT INTO t VALUES('a',10),('b',30),('c',20)",
        ] {
            f.execute(ddl).await.unwrap();
            r.execute_batch(ddl).unwrap();
        }

        // Bare-column control: `name` must come from the max-price row (b).
        assert_agree(&f, &r, "SELECT max(price), name FROM t").await;
        // H2: a correlated scalar subquery output must ALSO be sourced from the
        // extremum row — `(SELECT name)` resolves to the max-price row's name.
        assert_agree(&f, &r, "SELECT max(price), (SELECT name) FROM t").await;
        // min() variant — sources from the min-price row (a).
        assert_agree(&f, &r, "SELECT min(price), (SELECT name) FROM t").await;
        // A subquery nested inside a larger output expression still counts.
        assert_agree(
            &f,
            &r,
            "SELECT max(price), (SELECT name) || '!' FROM t",
        )
        .await;
    });
}
