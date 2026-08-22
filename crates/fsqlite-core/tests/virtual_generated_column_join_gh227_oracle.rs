#![recursion_limit = "512"]

//! GH #227 (bd-gh-virtual-generated-columns): in a FILE-BACKED database, a join
//! whose key is a VIRTUAL generated column must compute that column on read.
//! The file-backed join-scan fast path reconstructed only the physically stored
//! columns, leaving the VIRTUAL column uncomputed so the join matched nothing.
//! rusqlite is the oracle. Both engines use their own temp file (they cannot
//! share one), same DDL/DML, compared query results.

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
    assert_eq!(fr, rr, "VGC join mismatch on `{sql}`");
}

// GH #227 fixed: the plain SELECT join path is VDBE-codegen'd, and its
// join-column reader (`emit_join_expr`) emitted a raw `Opcode::Column` for the
// VIRTUAL generated column — the NULL placeholder slot — so the join key
// matched nothing. Fixed by routing VIRTUAL generated columns through the
// canonical `emit_table_column_read` (which computes the generating expression
// on read). The file-backed inflate/memdb streaming scan paths were fixed in
// parallel for the INSERT..SELECT join shapes. (STORED generated columns are
// physically present and already joined correctly — see the control test.)
#[test]
fn virtual_generated_column_join_file_backed_gh227() {
    asupersync::test_utils::run_test(|| async {
        let dir = tempfile::tempdir().unwrap();
        let f_path = dir.path().join("frank.db").to_string_lossy().to_string();
        let r_path = dir.path().join("csql.db").to_string_lossy().to_string();
        let f = Connection::open(&f_path).await.unwrap();
        let r = rusqlite::Connection::open(&r_path).unwrap();
        for s in [
            "CREATE TABLE t (a INTEGER, b INTEGER AS (a * 2) VIRTUAL)",
            "CREATE TABLE u (x INTEGER)",
            "INSERT INTO t(a) VALUES (1),(2),(3)",
            "INSERT INTO u VALUES (2),(4),(6)",
        ] {
            f.execute(s).await.unwrap();
            r.execute_batch(s).unwrap();
        }
        // Join key is the VIRTUAL generated column t.b.
        assert_agree(
            &f,
            &r,
            "SELECT t.a, t.b, u.x FROM t JOIN u ON t.b = u.x ORDER BY t.a",
        )
        .await;
        // Direct read of the VIRTUAL column.
        assert_agree(&f, &r, "SELECT a, b FROM t ORDER BY a").await;
        // Reverse join direction.
        assert_agree(&f, &r, "SELECT u.x FROM u JOIN t ON u.x = t.b ORDER BY u.x").await;
    });
}

#[test]
fn stored_generated_column_join_file_backed_gh227() {
    asupersync::test_utils::run_test(|| async {
        let dir = tempfile::tempdir().unwrap();
        let f_path = dir.path().join("frank.db").to_string_lossy().to_string();
        let r_path = dir.path().join("csql.db").to_string_lossy().to_string();
        let f = Connection::open(&f_path).await.unwrap();
        let r = rusqlite::Connection::open(&r_path).unwrap();
        // Control: a STORED generated column (physically present) must also join.
        for s in [
            "CREATE TABLE t (a INTEGER, b INTEGER AS (a + 100) STORED)",
            "CREATE TABLE u (x INTEGER)",
            "INSERT INTO t(a) VALUES (1),(2)",
            "INSERT INTO u VALUES (101),(102)",
        ] {
            f.execute(s).await.unwrap();
            r.execute_batch(s).unwrap();
        }
        assert_agree(
            &f,
            &r,
            "SELECT t.a, t.b, u.x FROM t JOIN u ON t.b = u.x ORDER BY t.a",
        )
        .await;
    });
}
