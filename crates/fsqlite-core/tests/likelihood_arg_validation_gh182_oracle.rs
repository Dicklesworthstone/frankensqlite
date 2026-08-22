//! GH #182 (bd-gh-likelihood-arg-validation-bynz9): `likelihood(X, Y)` must
//! reject a second argument that is not a floating-point literal in the closed
//! range `[0.0, 1.0]`.
//!
//! SQLite validates this at prepare time in `exprProbability()` (expr.c): the
//! argument is accepted only when `p->op == TK_FLOAT` and its value is in
//! `[0.0, 1.0]`. So an integer literal (`1`, `0`), an out-of-range float
//! (`1.0000001`), a unary-minus literal (`-0.1`), a constant expression
//! (`0.4 + 0.1`), a function call (`abs(0.5)`), a column reference, and `NULL`
//! are all rejected — even though some of them are constants and some are in
//! range. Before the fix FrankenSQLite ignored the second argument entirely and
//! returned the first argument for every one of these.
//!
//! rusqlite (stock SQLite) is the oracle. Valid calls must agree on the value;
//! invalid calls must be rejected by both engines.

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

async fn frank_rows(conn: &Connection, sql: &str) -> Vec<Vec<String>> {
    conn.query(sql)
        .await
        .unwrap_or_else(|e| panic!("frank error on `{sql}`: {e:?}"))
        .iter()
        .map(|r| r.values().iter().map(tag_f).collect())
        .collect()
}

fn sqlite_rows(conn: &rusqlite::Connection, sql: &str) -> Vec<Vec<String>> {
    let mut stmt = conn.prepare(sql).unwrap();
    let n = stmt.column_count();
    stmt.query_map([], |row| {
        Ok((0..n)
            .map(|i| tag_r(&row.get_unwrap::<_, rusqlite::types::Value>(i)))
            .collect())
    })
    .unwrap()
    .collect::<Result<Vec<_>, _>>()
    .unwrap()
}

/// A valid call: frank and rusqlite must return the same value.
async fn assert_agree(f: &Connection, r: &rusqlite::Connection, sql: &str) {
    let fr = frank_rows(f, sql).await;
    let rr = sqlite_rows(r, sql);
    assert_eq!(fr, rr, "oracle value mismatch on `{sql}`");
}

/// An illegal probability argument: BOTH engines must reject the statement.
/// SQLite fails at prepare, so `prepare()` returning `Err` is the oracle signal.
async fn assert_both_reject(f: &Connection, r: &rusqlite::Connection, sql: &str) {
    let sqlite_rejected = r.prepare(sql).is_err();
    assert!(
        sqlite_rejected,
        "oracle drift: stock SQLite accepted `{sql}` (test premise wrong)"
    );
    let frank_rejected = f.query(sql).await.is_err();
    assert!(
        frank_rejected,
        "frank accepted illegal likelihood arg on `{sql}` (must be rejected)"
    );
}

fn setup_r() -> rusqlite::Connection {
    let r = rusqlite::Connection::open_in_memory().unwrap();
    r.execute_batch("CREATE TABLE t(p REAL); INSERT INTO t VALUES (0.5);")
        .unwrap();
    r
}

async fn setup_f() -> Connection {
    let f = Connection::open(":memory:").await.unwrap();
    f.execute("CREATE TABLE t(p REAL)").await.unwrap();
    f.execute("INSERT INTO t VALUES (0.5)").await.unwrap();
    f
}

/// The exact GH#182 reproductions plus the full illegal-argument matrix.
#[test]
fn likelihood_rejects_illegal_probability_gh182() {
    asupersync::test_utils::run_test(|| async {
        let f = setup_f().await;
        let r = setup_r();
        for sql in [
            "SELECT likelihood(1, 2)",         // integer, out of range
            "SELECT likelihood(1, 1)",         // integer in range (still rejected)
            "SELECT likelihood(1, 0)",         // integer in range (still rejected)
            "SELECT likelihood(1, 1.0000001)", // float out of range
            "SELECT likelihood(1, -0.1)",      // unary-minus literal
            "SELECT likelihood(1, 0.4 + 0.1)", // constant expression, not a literal
            "SELECT likelihood(1, abs(0.5))",  // function call, not a literal
            "SELECT likelihood(1, NULL)",      // NULL
            "SELECT likelihood(1, p) FROM t",  // non-constant column reference
        ] {
            assert_both_reject(&f, &r, sql).await;
        }
    });
}

/// A legal probability argument (a REAL literal in `[0.0, 1.0]`) is accepted and
/// returns the first argument unchanged, matching the oracle.
#[test]
fn likelihood_accepts_real_literal_in_range_gh182() {
    asupersync::test_utils::run_test(|| async {
        let f = setup_f().await;
        let r = setup_r();
        for sql in [
            "SELECT likelihood(1, 0.5)",
            "SELECT likelihood(42, 0.5)",
            "SELECT likelihood(1, 0.0)",
            "SELECT likelihood(1, 1.0)",
            "SELECT likelihood('x', 0.125)",
        ] {
            assert_agree(&f, &r, sql).await;
        }
    });
}

/// `likely`/`unlikely` take a single argument (no probability) and are never
/// affected by the validation; they pass their argument through unchanged.
#[test]
fn likely_unlikely_unaffected_gh182() {
    asupersync::test_utils::run_test(|| async {
        let f = setup_f().await;
        let r = setup_r();
        for sql in [
            "SELECT likely(7)",
            "SELECT unlikely(7)",
            "SELECT likely('a')",
            "SELECT unlikely(0.5)",
        ] {
            assert_agree(&f, &r, sql).await;
        }
    });
}
