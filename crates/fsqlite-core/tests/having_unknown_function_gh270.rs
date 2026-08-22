#![recursion_limit = "512"]

//! GH #270 (bd-gh-group-by-having-eval): a HAVING clause that calls a function
//! which does not exist must raise "no such function", not silently evaluate to
//! NULL (which drops the group). The HAVING interpreter previously bypassed the
//! prepare-time resolution check. rusqlite is the oracle for both the row set
//! and whether the statement errors. The battery of built-in-function HAVING
//! clauses guards against the fix over-rejecting real built-ins (the registry is
//! narrower than the built-in scalar evaluator).

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

/// Run `sql` on both engines; assert they agree on error-vs-rows and, when both
/// succeed, on the (sorted) row set.
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
        "CREATE TABLE t (g INTEGER, v INTEGER)",
        "INSERT INTO t VALUES (1,10),(1,20),(2,5),(2,50),(3,-7)",
    ] {
        fconn.execute(s).await.unwrap();
        rconn.execute_batch(s).unwrap();
    }
}

#[test]
fn having_unknown_function_errors() {
    asupersync::test_utils::run_test(|| async {
        let f = Connection::open(":memory:").await.unwrap();
        let r = rusqlite::Connection::open_in_memory().unwrap();
        seed(&f, &r).await;
        // The bug: an unknown function was silently NULL, dropping the group.
        assert_agree(&f, &r, "SELECT g FROM t GROUP BY g HAVING nosuchfn(g) > 0").await;
        assert_agree(
            &f,
            &r,
            "SELECT g FROM t GROUP BY g HAVING totally_made_up(sum(v)) IS NOT NULL",
        )
        .await;
        // Wrong-arity is also a resolution error, not NULL.
        assert_agree(&f, &r, "SELECT g FROM t GROUP BY g HAVING abs() > 0").await;
    });
}

#[test]
fn having_builtin_functions_are_not_over_rejected() {
    asupersync::test_utils::run_test(|| async {
        let f = Connection::open(":memory:").await.unwrap();
        let r = rusqlite::Connection::open_in_memory().unwrap();
        seed(&f, &r).await;
        // A battery of real built-ins in HAVING must keep working (parity with
        // sqlite3) — the fix must not reject built-ins the registry omits.
        for sql in [
            "SELECT g FROM t GROUP BY g HAVING abs(g) > 0",
            "SELECT g FROM t GROUP BY g HAVING length(hex(g)) > 0",
            "SELECT g FROM t GROUP BY g HAVING coalesce(max(v), 0) >= 0",
            "SELECT g FROM t GROUP BY g HAVING typeof(g) = 'integer'",
            "SELECT g FROM t GROUP BY g HAVING upper('x') = 'X'",
            "SELECT g FROM t GROUP BY g HAVING lower('Y') = 'y'",
            "SELECT g FROM t GROUP BY g HAVING round(sum(v) * 1.0, 1) IS NOT NULL",
            "SELECT g FROM t GROUP BY g HAVING ifnull(min(v), 0) <= max(v)",
            "SELECT g FROM t GROUP BY g HAVING nullif(g, 0) IS NOT NULL",
            "SELECT g FROM t GROUP BY g HAVING substr(CAST(g AS TEXT), 1, 1) = CAST(g AS TEXT)",
            "SELECT g FROM t GROUP BY g HAVING trim('  x ') = 'x'",
            "SELECT g FROM t GROUP BY g HAVING replace('aa', 'a', 'b') = 'bb'",
            "SELECT g FROM t GROUP BY g HAVING instr('abc', 'b') = 2",
            "SELECT g FROM t GROUP BY g HAVING quote(g) IS NOT NULL",
            "SELECT g FROM t GROUP BY g HAVING max(abs(v)) > 0",
        ] {
            assert_agree(&f, &r, sql).await;
        }
    });
}
