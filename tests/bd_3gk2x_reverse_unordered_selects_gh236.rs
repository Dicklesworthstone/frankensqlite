#![recursion_limit = "512"]

//! GH #236 (bd-3gk2x): PRAGMA reverse_unordered_selects must (a) read back its
//! enabled state as 0/1 and (b) actually reverse the row order of a SELECT whose
//! order is not fixed by an ORDER BY, while leaving ORDER BY queries untouched.
//!
//! rusqlite (real SQLite 3.46.1) is the oracle. Order matters here, so the
//! comparisons are VERBATIM (never sorted). The final toggle-off also proves the
//! compiled-program cache is partitioned by the pragma (the same SQL must return
//! a different order once the pragma flips).

use fsqlite_core::connection::Connection;
use fsqlite_types::value::SqliteValue;

fn tag_f(v: &SqliteValue) -> String {
    match v {
        SqliteValue::Null => "NULL".to_owned(),
        SqliteValue::Integer(n) => n.to_string(),
        SqliteValue::Float(f) => format!("{f}"),
        SqliteValue::Text(s) => format!("'{s}'"),
        SqliteValue::Blob(b) => {
            format!(
                "X'{}'",
                b.iter().map(|x| format!("{x:02X}")).collect::<String>()
            )
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
            format!(
                "X'{}'",
                b.iter().map(|x| format!("{x:02X}")).collect::<String>()
            )
        }
    }
}

async fn fq(fconn: &Connection, sql: &str) -> Vec<Vec<String>> {
    fconn
        .query(sql)
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e:?}"))
        .iter()
        .map(|r| r.values().iter().map(tag_f).collect())
        .collect()
}
fn rq(rconn: &rusqlite::Connection, sql: &str) -> Vec<Vec<String>> {
    let mut st = rconn.prepare(sql).unwrap();
    let n = st.column_count();
    st.query_map([], |row| {
        Ok((0..n)
            .map(|i| tag_r(&row.get_unwrap::<_, rusqlite::types::Value>(i)))
            .collect())
    })
    .unwrap()
    .collect::<Result<Vec<_>, _>>()
    .unwrap()
}

/// Compare row sets VERBATIM (order-sensitive) — do NOT sort.
async fn agree_ordered(fconn: &Connection, rconn: &rusqlite::Connection, sql: &str) {
    let fr = fq(fconn, sql).await;
    let rr = rq(rconn, sql);
    assert_eq!(fr, rr, "ordered row-set mismatch on `{sql}`");
}

async fn exec_both(fconn: &Connection, rconn: &rusqlite::Connection, sql: &str) {
    fconn.execute(sql).await.unwrap();
    rconn.execute_batch(sql).unwrap();
}

#[test]
fn reverse_unordered_selects_reverses_scan_and_reads_back_gh236() {
    asupersync::test_utils::run_test(|| async {
        let f = Connection::open(":memory:").await.unwrap();
        let r = rusqlite::Connection::open_in_memory().unwrap();

        for s in [
            "CREATE TABLE t (a INTEGER PRIMARY KEY, b TEXT)",
            "INSERT INTO t VALUES (1, 'x'), (2, 'y'), (3, 'z')",
        ] {
            exec_both(&f, &r, s).await;
        }

        // Default: disabled, readback 0, unordered scan is forward on both.
        agree_ordered(&f, &r, "PRAGMA reverse_unordered_selects").await;
        agree_ordered(&f, &r, "SELECT a FROM t").await;

        // Enable on both engines.
        exec_both(&f, &r, "PRAGMA reverse_unordered_selects=ON").await;
        agree_ordered(&f, &r, "PRAGMA reverse_unordered_selects").await;

        // The unordered scan is now reversed (both engines agree, verbatim).
        agree_ordered(&f, &r, "SELECT a FROM t").await;
        agree_ordered(&f, &r, "SELECT a, b FROM t").await;

        // ORDER BY still fixes the order — NOT reversed by the pragma.
        agree_ordered(&f, &r, "SELECT a FROM t ORDER BY a").await;
        agree_ordered(&f, &r, "SELECT a FROM t ORDER BY a DESC").await;

        // Toggle back off: the SAME unordered SELECT returns forward order again,
        // proving the compiled-program cache is keyed on the pragma.
        exec_both(&f, &r, "PRAGMA reverse_unordered_selects=OFF").await;
        agree_ordered(&f, &r, "PRAGMA reverse_unordered_selects").await;
        agree_ordered(&f, &r, "SELECT a FROM t").await;
    });
}
