#![recursion_limit = "512"]

//! GH #152 (bd-gh-recursive-cte-limit): a `LIMIT` inside a recursive CTE caps
//! the number of rows the CTE produces (stock sqlite3 stops the recursion once
//! LIMIT is reached). fsqlite previously ignored it and ran to the 1000-row
//! recursion bound. rusqlite is the oracle.

use fsqlite_core::connection::Connection;
use fsqlite_types::value::SqliteValue;

fn tag_f(v: &SqliteValue) -> String {
    match v {
        SqliteValue::Null => "NULL".to_owned(),
        SqliteValue::Integer(n) => n.to_string(),
        SqliteValue::Float(f) => format!("{f}"),
        SqliteValue::Text(s) => format!("'{s}'"),
        SqliteValue::Blob(b) => format!("X'{}'", b.iter().map(|x| format!("{x:02X}")).collect::<String>()),
    }
}
fn tag_r(v: &rusqlite::types::Value) -> String {
    match v {
        rusqlite::types::Value::Null => "NULL".to_owned(),
        rusqlite::types::Value::Integer(n) => n.to_string(),
        rusqlite::types::Value::Real(f) => format!("{f}"),
        rusqlite::types::Value::Text(s) => format!("'{s}'"),
        rusqlite::types::Value::Blob(b) => format!("X'{}'", b.iter().map(|x| format!("{x:02X}")).collect::<String>()),
    }
}

async fn assert_agree(fconn: &Connection, rconn: &rusqlite::Connection, sql: &str) {
    let mut fr: Vec<Vec<String>> = fconn.query(sql).await.unwrap_or_else(|e| panic!("{sql}: {e:?}")).iter().map(|r| r.values().iter().map(tag_f).collect()).collect();
    fr.sort();
    let mut st = rconn.prepare(sql).unwrap();
    let n = st.column_count();
    let mut rr: Vec<Vec<String>> = st.query_map([], |row| Ok((0..n).map(|i| tag_r(&row.get_unwrap::<_, rusqlite::types::Value>(i))).collect())).unwrap().collect::<Result<Vec<_>, _>>().unwrap();
    rr.sort();
    assert_eq!(fr, rr, "recursive-CTE LIMIT mismatch on `{sql}`");
}

#[test]
fn recursive_cte_limit_caps_rows_gh152() {
    asupersync::test_utils::run_test(|| async {
        let f = Connection::open(":memory:").await.unwrap();
        let r = rusqlite::Connection::open_in_memory().unwrap();
        // Unbounded recursion capped by LIMIT.
        assert_agree(&f, &r, "WITH RECURSIVE c(x) AS (SELECT 1 UNION ALL SELECT x+1 FROM c LIMIT 5) SELECT group_concat(x) FROM c").await;
        assert_agree(&f, &r, "WITH RECURSIVE c(x) AS (SELECT 1 UNION ALL SELECT x+1 FROM c LIMIT 3) SELECT x FROM c ORDER BY x").await;
        // count(*)/SUM over a LIMIT'd recursive CTE must route off the closed-form
        // fast path and reflect the cap.
        assert_agree(&f, &r, "WITH RECURSIVE c(x) AS (SELECT 1 UNION ALL SELECT x+1 FROM c LIMIT 5) SELECT count(*) FROM c").await;
        assert_agree(&f, &r, "WITH RECURSIVE c(x) AS (SELECT 1 UNION ALL SELECT x+1 FROM c LIMIT 5) SELECT sum(x) FROM c").await;
        // LIMIT larger than a naturally-bounded recursion is a no-op.
        assert_agree(&f, &r, "WITH RECURSIVE c(x) AS (SELECT 1 UNION ALL SELECT x+1 FROM c WHERE x < 4 LIMIT 100) SELECT group_concat(x) FROM c").await;
    });
}

#[test]
fn recursive_cte_without_limit_unaffected_gh152() {
    asupersync::test_utils::run_test(|| async {
        let f = Connection::open(":memory:").await.unwrap();
        let r = rusqlite::Connection::open_in_memory().unwrap();
        // Control: a WHERE-bounded recursive CTE with no LIMIT is unchanged.
        assert_agree(&f, &r, "WITH RECURSIVE c(x) AS (SELECT 1 UNION ALL SELECT x+1 FROM c WHERE x < 6) SELECT count(*), sum(x) FROM c").await;
        assert_agree(&f, &r, "WITH RECURSIVE c(x) AS (SELECT 1 UNION ALL SELECT x+1 FROM c WHERE x < 6) SELECT group_concat(x) FROM c").await;
    });
}
