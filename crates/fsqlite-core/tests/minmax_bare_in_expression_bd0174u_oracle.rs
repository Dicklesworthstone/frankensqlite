#![recursion_limit = "512"]

//! bd-0174u: SQLite's "bare column tracks the single min()/max() row" rule must
//! apply even when the aggregate and the bare column(s) are NESTED inside the
//! SAME output expression (for example `max(price) || ':' || name`), not only
//! when they are separate top-level result columns. FrankenSQLite previously
//! bailed on any result column that mixed the aggregate with a bare column,
//! yielding NULL (ungrouped) or the group's first row (grouped) instead of the
//! extremum row. rusqlite (bundled sqlite3) is the oracle.
//!
//! Oracle cross-check (sqlite3 3.46.1):
//!   SELECT max(price)||':'||name FROM t;        -> 50:kiwi
//!   SELECT min(price)||':'||name FROM t;        -> 10:pear
//!   SELECT 'winner='||name||'@'||max(price);    -> winner=kiwi@50
//!   SELECT max(price), name FROM t;             -> 50|kiwi   (separate control)
//!   SELECT max(price)||':'||name, name FROM t;  -> 50:kiwi|kiwi
//!   SELECT grp||max(n)||id FROM g GROUP BY grp; -> a92 / b33
//!   SELECT grp||min(n)||id FROM g GROUP BY grp; -> a51 / b33
//!   SELECT grp, max(n), id FROM g GROUP BY grp; -> a|9|2 / b|3|3
//!   SELECT max(price)||count(*) FROM t;         -> 503     (>1 agg, no bare col)

use fsqlite_core::connection::Connection;
use fsqlite_types::value::SqliteValue;

fn tag_f(v: &SqliteValue) -> String {
    match v {
        SqliteValue::Null => "NULL".to_owned(),
        SqliteValue::Integer(n) => n.to_string(),
        SqliteValue::Float(f) => format!("{f}"),
        SqliteValue::Text(s) => format!("'{s}'"),
        SqliteValue::Blob(b) => {
            format!("X'{}'", b.iter().map(|x| format!("{x:02X}")).collect::<String>())
        }
    }
}
fn tag_r(v: &rusqlite::types::Value) -> String {
    match v {
        rusqlite::types::Value::Null => "NULL".to_owned(),
        rusqlite::types::Value::Integer(n) => n.to_string(),
        rusqlite::types::Value::Real(f) => format!("{f}"),
        rusqlite::types::Value::Text(s) => format!("'{s}'"),
        rusqlite::types::Value::Blob(b) => {
            format!("X'{}'", b.iter().map(|x| format!("{x:02X}")).collect::<String>())
        }
    }
}

/// Compare every result row of `sql` between FrankenSQLite and rusqlite.
async fn assert_agree(fconn: &Connection, rconn: &rusqlite::Connection, sql: &str) {
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
    assert_eq!(fr, rr, "min/max bare-in-expression mismatch on `{sql}`");
}

async fn seed(fconn: &Connection, rconn: &rusqlite::Connection, ddl: &[&str]) {
    for s in ddl {
        fconn.execute(s).await.unwrap();
        rconn.execute_batch(s).unwrap();
    }
}

/// Ungrouped whole-table: the single min/max plus a bare column nested inside
/// one output expression sources the bare column from the extremum row.
#[test]
fn minmax_bare_nested_ungrouped_bd0174u() {
    asupersync::test_utils::run_test(|| async {
        let f = Connection::open(":memory:").await.unwrap();
        let r = rusqlite::Connection::open_in_memory().unwrap();
        seed(
            &f,
            &r,
            &[
                "CREATE TABLE t(name TEXT, price INTEGER)",
                "INSERT INTO t VALUES ('apple',30),('pear',10),('kiwi',50)",
            ],
        )
        .await;
        // The three-repro core: aggregate and bare column nested together.
        assert_agree(&f, &r, "SELECT max(price) || ':' || name FROM t").await;
        assert_agree(&f, &r, "SELECT min(price) || ':' || name FROM t").await;
        // Bare column appears BEFORE the aggregate in the expression.
        assert_agree(&f, &r, "SELECT 'winner=' || name || '@' || max(price) FROM t").await;
        // Casty / different scalar shapes around the aggregate.
        assert_agree(&f, &r, "SELECT 'p' || min(price) || name FROM t").await;
    });
}

/// The separate-column form (aggregate and bare column as distinct result
/// columns) must keep working exactly as before — it is a subset of the rule.
#[test]
fn minmax_bare_separate_control_bd0174u() {
    asupersync::test_utils::run_test(|| async {
        let f = Connection::open(":memory:").await.unwrap();
        let r = rusqlite::Connection::open_in_memory().unwrap();
        seed(
            &f,
            &r,
            &[
                "CREATE TABLE t(name TEXT, price INTEGER)",
                "INSERT INTO t VALUES ('apple',30),('pear',10),('kiwi',50)",
            ],
        )
        .await;
        assert_agree(&f, &r, "SELECT max(price), name FROM t").await;
        assert_agree(&f, &r, "SELECT min(price), name FROM t").await;
        assert_agree(&f, &r, "SELECT max(price), price, name FROM t").await;
    });
}

/// A bare column appears BOTH as its own separate column AND nested inside the
/// mixed aggregate expression of the same query.
#[test]
fn minmax_bare_separate_and_nested_bd0174u() {
    asupersync::test_utils::run_test(|| async {
        let f = Connection::open(":memory:").await.unwrap();
        let r = rusqlite::Connection::open_in_memory().unwrap();
        seed(
            &f,
            &r,
            &[
                "CREATE TABLE t(name TEXT, price INTEGER)",
                "INSERT INTO t VALUES ('apple',30),('pear',10),('kiwi',50)",
            ],
        )
        .await;
        assert_agree(&f, &r, "SELECT max(price) || ':' || name, name FROM t").await;
        assert_agree(&f, &r, "SELECT name, min(price) || ':' || name FROM t").await;
    });
}

/// GROUP BY: each group's bare columns nested in a mixed expression come from
/// that group's extremum row, not its first scanned row.
#[test]
fn minmax_bare_nested_grouped_bd0174u() {
    asupersync::test_utils::run_test(|| async {
        let f = Connection::open(":memory:").await.unwrap();
        let r = rusqlite::Connection::open_in_memory().unwrap();
        seed(
            &f,
            &r,
            &[
                "CREATE TABLE g(grp TEXT, n INTEGER, id INTEGER)",
                "INSERT INTO g VALUES('a',5,1),('a',9,2),('b',3,3)",
            ],
        )
        .await;
        assert_agree(&f, &r, "SELECT grp || max(n) || id FROM g GROUP BY grp").await;
        assert_agree(&f, &r, "SELECT grp || min(n) || id FROM g GROUP BY grp").await;
        // Separate-column grouped control.
        assert_agree(&f, &r, "SELECT grp, max(n), id FROM g GROUP BY grp").await;
        // Grouped, nested, with a bare column that is also the GROUP BY key.
        assert_agree(&f, &r, "SELECT grp || '=' || max(n) FROM g GROUP BY grp").await;
    });
}

/// WHERE filtering: the extremum row is chosen among the filtered rows.
#[test]
fn minmax_bare_nested_with_where_bd0174u() {
    asupersync::test_utils::run_test(|| async {
        let f = Connection::open(":memory:").await.unwrap();
        let r = rusqlite::Connection::open_in_memory().unwrap();
        seed(
            &f,
            &r,
            &[
                "CREATE TABLE t(name TEXT, price INTEGER)",
                "INSERT INTO t VALUES ('apple',30),('pear',10),('kiwi',50)",
            ],
        )
        .await;
        assert_agree(&f, &r, "SELECT max(price) || ':' || name FROM t WHERE price < 40").await;
        // Empty scan: implicit aggregation yields one all-NULL row.
        assert_agree(&f, &r, "SELECT max(price) || ':' || name FROM t WHERE price > 1000").await;
    });
}

/// All-NULL aggregate argument: max()/min() is NULL, so the whole concatenation
/// is NULL regardless of which row supplies the bare column.
#[test]
fn minmax_bare_nested_all_null_arg_bd0174u() {
    asupersync::test_utils::run_test(|| async {
        let f = Connection::open(":memory:").await.unwrap();
        let r = rusqlite::Connection::open_in_memory().unwrap();
        seed(
            &f,
            &r,
            &[
                "CREATE TABLE z(a, b)",
                "INSERT INTO z VALUES(NULL,'x'),(NULL,'y')",
            ],
        )
        .await;
        assert_agree(&f, &r, "SELECT max(a) || ':' || b FROM z").await;
        assert_agree(&f, &r, "SELECT min(a) || ':' || b FROM z").await;
    });
}

/// More than one aggregate in a nested expression (here with NO bare column):
/// the single-min/max rule does not apply, and the plain concatenation of the
/// two aggregate values still matches sqlite. Guards against a false positive
/// in the new nested detection.
#[test]
fn minmax_multi_aggregate_no_bare_bd0174u() {
    asupersync::test_utils::run_test(|| async {
        let f = Connection::open(":memory:").await.unwrap();
        let r = rusqlite::Connection::open_in_memory().unwrap();
        seed(
            &f,
            &r,
            &[
                "CREATE TABLE t(name TEXT, price INTEGER)",
                "INSERT INTO t VALUES ('apple',30),('pear',10),('kiwi',50)",
            ],
        )
        .await;
        // Two aggregates, no bare column: max(price)=50, count(*)=3 -> "503".
        assert_agree(&f, &r, "SELECT max(price) || count(*) FROM t").await;
        // Two aggregates including a min/max, still no bare column.
        assert_agree(&f, &r, "SELECT max(price) || '/' || count(*) FROM t").await;
    });
}
