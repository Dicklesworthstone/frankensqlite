#![recursion_limit = "512"]

//! Confirmation keepers for three GH-triage beads found already-fixed at HEAD
//! but lacking exact coverage:
//!   * GH #234 (bd-gh-having-agg-filter): aggregate FILTER honored in a grouped
//!     HAVING term.
//!   * GH #235 (bd-gh-having-agg-filter-match): aggregate FILTER honored in a
//!     HAVING term with no GROUP BY.
//!   * GH #269 (bd-gh-groupby-fn-resolution): GROUP BY of an unknown function
//!     (e.g. ROLLUP/CUBE, unsupported by SQLite) raises "no such function".
//! rusqlite is the oracle; these pin the fixed behavior so it cannot regress.

use fsqlite_core::connection::Connection;
use fsqlite_types::value::SqliteValue;

fn tag_f(v: &SqliteValue) -> String {
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
fn tag_r(v: &rusqlite::types::Value) -> String {
    match v {
        rusqlite::types::Value::Null => "NULL".to_owned(),
        rusqlite::types::Value::Integer(n) => n.to_string(),
        rusqlite::types::Value::Real(f) => format!("{f}"),
        rusqlite::types::Value::Text(s) => format!("'{s}'"),
        rusqlite::types::Value::Blob(b) => format!(
            "X'{}'",
            b.iter().map(|x| format!("{x:02X}")).collect::<String>()
        ),
    }
}

async fn assert_agree(fconn: &Connection, rconn: &rusqlite::Connection, sql: &str) {
    let f: Result<Vec<Vec<String>>, ()> = match fconn.query(sql).await {
        Ok(rows) => {
            let mut v: Vec<Vec<String>> = rows
                .iter()
                .map(|r| r.values().iter().map(tag_f).collect())
                .collect();
            v.sort();
            Ok(v)
        }
        Err(_) => Err(()),
    };
    let r: Result<Vec<Vec<String>>, ()> = (|| {
        let mut st = rconn.prepare(sql).map_err(|_| ())?;
        let n = st.column_count();
        let mut rows: Vec<Vec<String>> = st
            .query_map([], |row| {
                Ok((0..n)
                    .map(|i| tag_r(&row.get_unwrap::<_, rusqlite::types::Value>(i)))
                    .collect())
            })
            .map_err(|_| ())?
            .collect::<Result<Vec<_>, _>>()
            .map_err(|_| ())?;
        rows.sort();
        Ok(rows)
    })();
    match (&f, &r) {
        (Ok(fr), Ok(rr)) => assert_eq!(fr, rr, "row mismatch on `{sql}`"),
        (Err(()), Err(())) => {}
        _ => panic!("error-vs-rows divergence on `{sql}`\n  frank: {f:?}\n  csql:  {r:?}"),
    }
}

async fn seed(fconn: &Connection, rconn: &rusqlite::Connection) {
    for s in [
        "CREATE TABLE t (g INTEGER, x INTEGER)",
        "INSERT INTO t VALUES (1, 5), (1, -2), (2, 10), (2, -3), (2, 4)",
    ] {
        fconn.execute(s).await.unwrap();
        rconn.execute_batch(s).unwrap();
    }
}

#[test]
fn having_aggregate_filter_grouped_gh234() {
    asupersync::test_utils::run_test(|| async {
        let f = Connection::open(":memory:").await.unwrap();
        let r = rusqlite::Connection::open_in_memory().unwrap();
        seed(&f, &r).await;
        // FILTER restricts the aggregate inside the HAVING term (per group).
        assert_agree(
            &f,
            &r,
            "SELECT g FROM t GROUP BY g HAVING sum(x) FILTER (WHERE x > 0) > 6 ORDER BY g",
        )
        .await;
        assert_agree(&f, &r, "SELECT g, count(*) FILTER (WHERE x > 0) AS pos FROM t GROUP BY g HAVING count(*) FILTER (WHERE x > 0) = 1 ORDER BY g").await;
    });
}

#[test]
fn having_aggregate_filter_no_group_by_gh235() {
    asupersync::test_utils::run_test(|| async {
        let f = Connection::open(":memory:").await.unwrap();
        let r = rusqlite::Connection::open_in_memory().unwrap();
        seed(&f, &r).await;
        // Implicit single group: FILTER applies inside the HAVING aggregate.
        assert_agree(
            &f,
            &r,
            "SELECT sum(x) FROM t HAVING count(*) FILTER (WHERE x > 0) = 3",
        )
        .await;
        assert_agree(
            &f,
            &r,
            "SELECT sum(x) FROM t HAVING sum(x) FILTER (WHERE x > 0) > 100",
        )
        .await;
    });
}

#[test]
fn group_by_unknown_function_errors_gh269() {
    asupersync::test_utils::run_test(|| async {
        let f = Connection::open(":memory:").await.unwrap();
        let r = rusqlite::Connection::open_in_memory().unwrap();
        seed(&f, &r).await;
        // ROLLUP/CUBE are not SQLite functions: a GROUP BY that calls one must
        // raise "no such function", not silently accept it.
        assert_agree(&f, &r, "SELECT g, count(*) FROM t GROUP BY rollup(g)").await;
        assert_agree(&f, &r, "SELECT g FROM t GROUP BY totally_unknown_fn(g)").await;
    });
}
