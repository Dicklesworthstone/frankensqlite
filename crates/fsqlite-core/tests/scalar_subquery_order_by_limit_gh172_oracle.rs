#![recursion_limit = "512"]

//! GH #172 (bd-gh-scalar-subquery-order-by-limit): a correlated scalar subquery
//! with `ORDER BY ... [LIMIT/OFFSET]` must pick the ordered row, not the first
//! row in scan order. fsqlite previously grabbed the first matching row and
//! ignored ORDER BY / LIMIT / OFFSET entirely. rusqlite is the oracle.

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

/// Agree on rows in the emitted order (NO sort — outer query has its own ORDER BY).
async fn assert_agree_ordered(fconn: &Connection, rconn: &rusqlite::Connection, sql: &str) {
    let fr: Vec<Vec<String>> = fconn
        .query(sql)
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e:?}"))
        .iter()
        .map(|r| r.values().iter().map(tag_f).collect())
        .collect();
    let mut st = rconn.prepare(sql).unwrap();
    let n = st.column_count();
    let rr: Vec<Vec<String>> = st
        .query_map([], |row| {
            Ok((0..n)
                .map(|i| tag_r(&row.get_unwrap::<_, rusqlite::types::Value>(i)))
                .collect())
        })
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(fr, rr, "scalar-subquery ORDER BY/LIMIT mismatch on `{sql}`");
}

async fn seed(fconn: &Connection, rconn: &rusqlite::Connection) {
    for s in [
        "CREATE TABLE outer_t (id INTEGER PRIMARY KEY)",
        "CREATE TABLE inner_t (oid INTEGER, v INTEGER)",
        "INSERT INTO outer_t VALUES (1),(2)",
        "INSERT INTO inner_t VALUES (1,30),(1,10),(1,20),(2,5),(2,50),(2,25)",
    ] {
        fconn.execute(s).await.unwrap();
        rconn.execute_batch(s).unwrap();
    }
}

#[test]
fn scalar_subquery_order_by_asc_limit1_gh172() {
    asupersync::test_utils::run_test(|| async {
        let f = Connection::open(":memory:").await.unwrap();
        let r = rusqlite::Connection::open_in_memory().unwrap();
        seed(&f, &r).await;
        // ASC LIMIT 1 -> per outer row the minimum v: 1|10, 2|5.
        assert_agree_ordered(&f, &r,
            "SELECT id,(SELECT v FROM inner_t WHERE inner_t.oid=outer_t.id ORDER BY v ASC LIMIT 1) FROM outer_t ORDER BY id").await;
    });
}

#[test]
fn scalar_subquery_order_by_desc_limit1_gh172() {
    asupersync::test_utils::run_test(|| async {
        let f = Connection::open(":memory:").await.unwrap();
        let r = rusqlite::Connection::open_in_memory().unwrap();
        seed(&f, &r).await;
        // DESC LIMIT 1 -> per outer row the maximum v: 1|30, 2|50.
        assert_agree_ordered(&f, &r,
            "SELECT id,(SELECT v FROM inner_t WHERE inner_t.oid=outer_t.id ORDER BY v DESC LIMIT 1) FROM outer_t ORDER BY id").await;
    });
}

#[test]
fn scalar_subquery_order_by_limit1_offset1_gh172() {
    asupersync::test_utils::run_test(|| async {
        let f = Connection::open(":memory:").await.unwrap();
        let r = rusqlite::Connection::open_in_memory().unwrap();
        seed(&f, &r).await;
        // LIMIT 1 OFFSET 1 -> per outer row the 2nd smallest v: 1|20, 2|25.
        assert_agree_ordered(&f, &r,
            "SELECT id,(SELECT v FROM inner_t WHERE inner_t.oid=outer_t.id ORDER BY v ASC LIMIT 1 OFFSET 1) FROM outer_t ORDER BY id").await;
    });
}

#[test]
fn scalar_subquery_order_by_no_limit_gh172() {
    asupersync::test_utils::run_test(|| async {
        let f = Connection::open(":memory:").await.unwrap();
        let r = rusqlite::Connection::open_in_memory().unwrap();
        seed(&f, &r).await;
        // ORDER BY without LIMIT: scalar subquery still uses only the first
        // (smallest) ordered row: 1|10, 2|5.
        assert_agree_ordered(&f, &r,
            "SELECT id,(SELECT v FROM inner_t WHERE inner_t.oid=outer_t.id ORDER BY v ASC) FROM outer_t ORDER BY id").await;
    });
}

#[test]
fn scalar_subquery_order_by_expr_and_offset_past_end_gh172() {
    asupersync::test_utils::run_test(|| async {
        let f = Connection::open(":memory:").await.unwrap();
        let r = rusqlite::Connection::open_in_memory().unwrap();
        seed(&f, &r).await;
        // ORDER BY an expression (v*-1 == DESC by v): 1|30, 2|50.
        assert_agree_ordered(&f, &r,
            "SELECT id,(SELECT v FROM inner_t WHERE inner_t.oid=outer_t.id ORDER BY v*-1 LIMIT 1) FROM outer_t ORDER BY id").await;
        // OFFSET past the group size -> NULL (no such row).
        assert_agree_ordered(&f, &r,
            "SELECT id,(SELECT v FROM inner_t WHERE inner_t.oid=outer_t.id ORDER BY v LIMIT 1 OFFSET 10) FROM outer_t ORDER BY id").await;
    });
}

#[test]
fn scalar_subquery_no_order_unaffected_gh172() {
    asupersync::test_utils::run_test(|| async {
        let f = Connection::open(":memory:").await.unwrap();
        let r = rusqlite::Connection::open_in_memory().unwrap();
        seed(&f, &r).await;
        // Control: uncorrelated ORDER BY DESC LIMIT 1 (different path) stays 50,
        // and an aggregate scalar subquery stays correct.
        assert_agree_ordered(
            &f,
            &r,
            "SELECT (SELECT v FROM inner_t ORDER BY v DESC LIMIT 1)",
        )
        .await;
        assert_agree_ordered(&f, &r,
            "SELECT id,(SELECT max(v) FROM inner_t WHERE inner_t.oid=outer_t.id) FROM outer_t ORDER BY id").await;
    });
}

#[test]
fn scalar_subquery_wrapped_order_parameters_remain_constants() {
    asupersync::test_utils::run_test(|| async {
        let f = Connection::open(":memory:").await.unwrap();
        let r = rusqlite::Connection::open_in_memory().unwrap();
        seed(&f, &r).await;

        for term in [
            "?1",
            "+?1",
            "-?1",
            "?1 COLLATE BINARY",
            "(+?1 COLLATE NOCASE) COLLATE RTRIM",
            "-(-(?1 COLLATE BINARY))",
            "(?1 AND outer_t.id)",
        ] {
            // Both fixture ids are nonzero, so each term supplies a constant
            // key. The following id DESC selects id 2; converting the bound
            // parameter to ordinal 1 changes the order or causes an error.
            let sql = format!(
                "SELECT outer_t.id,\
                 (SELECT count(*) FROM inner_t WHERE inner_t.oid=outer_t.id) \
                 FROM outer_t ORDER BY {term},outer_t.id DESC LIMIT ?2 OFFSET ?3"
            );
            let prepared = f.prepare(&sql).await.unwrap();
            let mut stock = r.prepare(&sql).unwrap();
            let columns = stock.column_count();

            // Rebind one prepared statement across ordinary, zero, negative
            // and out-of-range ordinal-looking values, then repeat the first.
            for key in [1, 0, -1, 7, i64::MIN, i64::MAX, 1] {
                let params = [key, 1, 0];
                let expected: Vec<Vec<String>> = stock
                    .query_map(rusqlite::params_from_iter(params.iter()), |row| {
                        Ok((0..columns)
                            .map(|column| {
                                tag_r(&row.get_unwrap::<_, rusqlite::types::Value>(column))
                            })
                            .collect())
                    })
                    .unwrap()
                    .collect::<Result<Vec<_>, _>>()
                    .unwrap();
                let values = params.map(SqliteValue::Integer);
                for actual in [
                    f.query_with_params(&sql, &values).await.unwrap(),
                    prepared.query_with_params(&values).await.unwrap(),
                ] {
                    let actual: Vec<Vec<String>> = actual
                        .iter()
                        .map(|row| row.values().iter().map(tag_f).collect())
                        .collect();
                    assert_eq!(actual, expected, "{sql}; parameters={params:?}");
                }
            }
        }

        // Genuine literal ordinals keep their original meaning.
        for term in ["1", "+1", "1 COLLATE BINARY", "-(-1) COLLATE BINARY"] {
            let sql = format!(
                "SELECT outer_t.id,\
                 (SELECT count(*) FROM inner_t WHERE inner_t.oid=outer_t.id) \
                 FROM outer_t ORDER BY {term} LIMIT 1"
            );
            assert_agree_ordered(&f, &r, &sql).await;
        }

        // Binding must not turn invalid literal ordinals into accepted
        // constants, even when LIMIT 0 prevents row evaluation.
        for term in ["0", "-1", "3", "+0 COLLATE BINARY", "-4 COLLATE NOCASE"] {
            let sql = format!(
                "SELECT outer_t.id,\
                 (SELECT count(*) FROM inner_t WHERE inner_t.oid=outer_t.id) \
                 FROM outer_t ORDER BY {term} LIMIT 0"
            );
            assert!(r.prepare(&sql).is_err(), "stock must reject {sql}");
            assert!(
                f.query(&sql).await.is_err(),
                "invalid ordinal accepted: {sql}"
            );
        }
        f.close().await.unwrap();
    });
}
