#![recursion_limit = "512"]

//! bd-hx3zu (GH#227 sibling): a VIRTUAL generated column used as a GROUP BY key
//! or as an aggregate argument read the raw `Opcode::Column` record slot — which
//! for a VIRTUAL column is the unmaterialized NULL placeholder — so grouping and
//! aggregation ran over NULLs instead of the generated value. The GROUP BY /
//! aggregate sorter-fill (fsqlite-vdbe codegen.rs) now routes a VIRTUAL column
//! through `emit_table_column_read`, computing the generating expression on
//! read, exactly as single-table projection and the GH#227 `emit_join_expr` fix.
//!
//! Pins Franken differentially against rusqlite for: GROUP BY on a VIRTUAL key,
//! aggregates over a VIRTUAL argument (with and without GROUP BY), DISTINCT over
//! a VIRTUAL column, a STORED-generated control, and a base-table read.

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

async fn assert_agree(fconn: &Connection, rconn: &rusqlite::Connection, sql: &str) -> usize {
    let fr: Vec<Vec<String>> = fconn
        .query(sql)
        .await
        .unwrap_or_else(|e| panic!("franken `{sql}`: {e:?}"))
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
    assert_eq!(fr, rr, "bd-hx3zu mismatch on `{sql}`");
    fr.len()
}

async fn seed(fconn: &Connection, rconn: &rusqlite::Connection) {
    let script = [
        // k is a VIRTUAL generated GROUP BY key; n is a VIRTUAL aggregate arg;
        // s is a STORED control (already materialized in the record).
        "CREATE TABLE t (\
            a TEXT, \
            k TEXT AS (json_extract(a, '$.k')) VIRTUAL, \
            n INTEGER AS (json_extract(a, '$.n')) VIRTUAL, \
            s TEXT AS (json_extract(a, '$.k') || '!') STORED\
        )",
        "INSERT INTO t(a) VALUES ('{\"k\":\"apple\",\"n\":3}')",
        "INSERT INTO t(a) VALUES ('{\"k\":\"banana\",\"n\":5}')",
        "INSERT INTO t(a) VALUES ('{\"k\":\"apple\",\"n\":7}')",
        "INSERT INTO t(a) VALUES ('{\"k\":\"cherry\",\"n\":2}')",
        "INSERT INTO t(a) VALUES ('{\"k\":\"banana\",\"n\":11}')",
        "INSERT INTO t(a) VALUES ('{\"k\":\"apple\",\"n\":1}')",
    ];
    for sql in script {
        fconn
            .execute(sql)
            .await
            .unwrap_or_else(|e| panic!("franken exec `{sql}`: {e:?}"));
        rconn
            .execute_batch(sql)
            .unwrap_or_else(|e| panic!("rusqlite exec `{sql}`: {e:?}"));
    }
}

#[test]
fn hx3zu_virtual_generated_column_groupby_and_aggregate() {
    asupersync::test_utils::run_test(|| async {
        let dir = tempfile::tempdir().expect("temp dir");
        let db = dir.path().join("hx3zu.db").to_string_lossy().into_owned();
        let fconn = Connection::open(&db).await.expect("open franken file db");
        let rconn = rusqlite::Connection::open(dir.path().join("hx3zu_oracle.db"))
            .expect("open rusqlite oracle file db");

        seed(&fconn, &rconn).await;

        // Base-table read of the VIRTUAL columns (regression guard — already worked).
        assert_agree(&fconn, &rconn, "SELECT a, k, n, s FROM t ORDER BY rowid").await;

        // (1) GROUP BY on a VIRTUAL key — the primary bug: grouped on NULL before.
        let g = assert_agree(
            &fconn,
            &rconn,
            "SELECT k, count(*) FROM t GROUP BY k ORDER BY k",
        )
        .await;
        assert_eq!(g, 3, "expected 3 groups (apple/banana/cherry), got {g}");

        // (2) GROUP BY on a VIRTUAL key + aggregates over a VIRTUAL arg.
        assert_agree(
            &fconn,
            &rconn,
            "SELECT k, sum(n), avg(n), min(n), max(n), count(n) \
             FROM t GROUP BY k ORDER BY k",
        )
        .await;

        // (3) Aggregates over a VIRTUAL arg with NO GROUP BY.
        assert_agree(&fconn, &rconn, "SELECT sum(n), count(n), max(n), min(n) FROM t").await;

        // (4) DISTINCT over a VIRTUAL column (as key and as aggregate arg).
        assert_agree(&fconn, &rconn, "SELECT count(DISTINCT k) FROM t").await;
        assert_agree(&fconn, &rconn, "SELECT DISTINCT k FROM t ORDER BY k").await;

        // (5) STORED control: GROUP BY on a STORED generated column still agrees.
        assert_agree(
            &fconn,
            &rconn,
            "SELECT s, count(*) FROM t GROUP BY s ORDER BY s",
        )
        .await;

        // (6) HAVING over an aggregate of a VIRTUAL arg.
        assert_agree(
            &fconn,
            &rconn,
            "SELECT k, sum(n) FROM t GROUP BY k HAVING sum(n) > 3 ORDER BY k",
        )
        .await;
    });
}
