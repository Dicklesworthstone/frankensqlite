//! bd-pgqsk M6: truth-context `AND` folds. In a FILTER position (WHERE / HAVING
//! / JOIN-ON) a `<never-true-const> AND E` conjunction is never TRUE, so the row
//! is filtered and E — even an erroring uncorrelated subquery — is never
//! evaluated, matching stock. Frank previously folded only the integer literal
//! `0`, so `0.0`/`NULL`/`FALSE`/`+0`/falsy-text still eagerly materialized E and
//! surfaced its error; aggregate/GROUP BY shapes were excluded entirely.
//!
//! rusqlite (stock SQLite 3.46.x) is the oracle. `E` is
//! `(SELECT 1 FROM json_each('bare'))`, which errors when evaluated.

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
            .map(|i| match row.get_unwrap::<_, rusqlite::types::Value>(i) {
                rusqlite::types::Value::Null => "NULL".to_owned(),
                rusqlite::types::Value::Integer(x) => x.to_string(),
                rusqlite::types::Value::Real(x) => format!("{x}"),
                rusqlite::types::Value::Text(t) => format!("'{t}'"),
                rusqlite::types::Value::Blob(b) => {
                    format!("X'{}'", b.iter().map(|x| format!("{x:02X}")).collect::<String>())
                }
            })
            .collect())
    })
    .unwrap()
    .collect::<Result<Vec<_>, _>>()
    .unwrap()
}

async fn assert_agree(f: &Connection, r: &rusqlite::Connection, sql: &str) {
    let fr = frank_rows(f, sql).await;
    let rr = sqlite_rows(r, sql);
    assert_eq!(fr, rr, "oracle mismatch on `{sql}`");
}

async fn setup_f() -> Connection {
    let f = Connection::open(":memory:").await.unwrap();
    f.execute("CREATE TABLE t(a INTEGER)").await.unwrap();
    f.execute("INSERT INTO t VALUES (1),(2),(3)").await.unwrap();
    f
}

fn setup_r() -> rusqlite::Connection {
    let r = rusqlite::Connection::open_in_memory().unwrap();
    r.execute_batch("CREATE TABLE t(a INTEGER); INSERT INTO t VALUES (1),(2),(3);")
        .unwrap();
    r
}

#[test]
fn bd_pgqsk_m6_where_never_true_and_short_circuits() {
    asupersync::test_utils::run_test(|| async {
        let f = setup_f().await;
        let r = setup_r();
        const E: &str = "(SELECT 1 FROM json_each('bare'))";
        for lhs in ["0", "0.0", "NULL", "FALSE", "+0", "'0'", "'x'", "''"] {
            // Plain scan.
            assert_agree(&f, &r, &format!("SELECT a FROM t WHERE {lhs} AND {E} ORDER BY a")).await;
            // Aggregate (was excluded from the short-circuit routing).
            assert_agree(&f, &r, &format!("SELECT count(*) FROM t WHERE {lhs} AND {E}")).await;
            // Nested under OR — the dead AND branch is still folded.
            assert_agree(
                &f,
                &r,
                &format!("SELECT count(*) FROM t WHERE ({lhs} AND {E}) OR a = 2"),
            )
            .await;
        }
        // A truthy constant still evaluates E (both engines error) — no over-fold.
        assert!(
            f.query(&format!("SELECT count(*) FROM t WHERE 1 AND {E}"))
                .await
                .is_err(),
            "a truthy LHS must NOT short-circuit E"
        );
    });
}

#[test]
fn bd_pgqsk_m6_having_never_true_and_short_circuits() {
    asupersync::test_utils::run_test(|| async {
        let f = setup_f().await;
        let r = setup_r();
        const E: &str = "(SELECT 1 FROM json_each('bare'))";
        for lhs in ["0", "0.0", "NULL", "FALSE"] {
            assert_agree(&f, &r, &format!("SELECT count(*) FROM t HAVING {lhs} AND {E}")).await;
            assert_agree(
                &f,
                &r,
                &format!("SELECT a, count(*) FROM t GROUP BY a HAVING {lhs} AND {E} ORDER BY a"),
            )
            .await;
        }
    });
}

#[test]
fn bd_pgqsk_m6_join_on_never_true_and_short_circuits() {
    asupersync::test_utils::run_test(|| async {
        let f = setup_f().await;
        f.execute("CREATE TABLE u(b INTEGER)").await.unwrap();
        f.execute("INSERT INTO u VALUES (1),(2)").await.unwrap();
        let r = setup_r();
        r.execute_batch("CREATE TABLE u(b INTEGER); INSERT INTO u VALUES (1),(2);")
            .unwrap();
        const E: &str = "(SELECT 1 FROM json_each('bare'))";
        assert_agree(
            &f,
            &r,
            &format!("SELECT count(*) FROM t JOIN u ON (NULL AND {E}) OR t.a = u.b"),
        )
        .await;
    });
}
