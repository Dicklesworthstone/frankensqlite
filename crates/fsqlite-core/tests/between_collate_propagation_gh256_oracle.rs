#![recursion_limit = "512"]

//! GH #256 (bd-gh-between-collate-propagation): BETWEEN desugars to
//! `x >= low AND x <= high`, and each leg's collation follows the standard
//! binary-comparison rule — an explicit COLLATE on x OR on a bound propagates.
//! This keeper pins the bead's six enumerated cases (fromless + table path)
//! differentially against rusqlite to establish exactly what still diverges at
//! HEAD (the codegen emit_between_expr has evolved since the Jul-14 anchor).

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
    assert_eq!(fr, rr, "BETWEEN collate mismatch on `{sql}`");
}

#[test]
fn between_collate_fromless_gh256() {
    asupersync::test_utils::run_test(|| async {
        let f = Connection::open(":memory:").await.unwrap();
        let r = rusqlite::Connection::open_in_memory().unwrap();
        // (1) explicit COLLATE on both bounds.
        assert_agree(
            &f,
            &r,
            "SELECT 'B' BETWEEN 'a' COLLATE NOCASE AND 'c' COLLATE NOCASE",
        )
        .await;
        // (2) explicit COLLATE on the left operand.
        assert_agree(&f, &r, "SELECT 'B' COLLATE NOCASE BETWEEN 'a' AND 'c'").await;
        // (3) explicit COLLATE on only the low bound.
        assert_agree(
            &f,
            &r,
            "SELECT ('B') BETWEEN ('a' COLLATE NOCASE) AND ('c')",
        )
        .await;
        // NOT BETWEEN inverse + a genuinely-out-of-range case.
        assert_agree(
            &f,
            &r,
            "SELECT 'B' NOT BETWEEN 'a' COLLATE NOCASE AND 'c' COLLATE NOCASE",
        )
        .await;
        assert_agree(
            &f,
            &r,
            "SELECT 'Z' BETWEEN 'a' COLLATE NOCASE AND 'c' COLLATE NOCASE",
        )
        .await;
    });
}

#[test]
fn between_collate_table_path_gh256() {
    asupersync::test_utils::run_test(|| async {
        let f = Connection::open(":memory:").await.unwrap();
        let r = rusqlite::Connection::open_in_memory().unwrap();
        for s in [
            "CREATE TABLE t (x TEXT COLLATE NOCASE)",
            "INSERT INTO t VALUES ('B')",
            "CREATE TABLE t2 (x TEXT)",
            "INSERT INTO t2 VALUES ('B')",
        ] {
            f.execute(s).await.unwrap();
            r.execute_batch(s).unwrap();
        }
        // (4) implicit column collation (NOCASE) drives BETWEEN.
        assert_agree(&f, &r, "SELECT x FROM t WHERE x BETWEEN 'a' AND 'c'").await;
        // (5) bound-side explicit COLLATE on a plain-column table path.
        assert_agree(
            &f,
            &r,
            "SELECT x FROM t2 WHERE x BETWEEN 'a' COLLATE NOCASE AND 'c'",
        )
        .await;
        // (6) left-operand explicit COLLATE on the table path.
        assert_agree(
            &f,
            &r,
            "SELECT x FROM t2 WHERE x COLLATE NOCASE BETWEEN 'a' AND 'c'",
        )
        .await;
        // Control: plain BINARY BETWEEN (no collate) excludes 'B' from a..c.
        assert_agree(&f, &r, "SELECT x FROM t2 WHERE x BETWEEN 'a' AND 'c'").await;
    });
}
