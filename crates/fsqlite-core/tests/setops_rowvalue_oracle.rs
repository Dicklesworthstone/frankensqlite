//! Differential oracle: compound set operations + row-value (tuple)
//! comparisons vs rusqlite (bundled SQLite 3.53). A probe sweep found this
//! surface stock-correct across 17 cases; this keeper locks it in.
//!
//! Notable semantics asserted: set-op dedup treats NULLs as EQUAL (unlike `=`),
//! UNION/INTERSECT/EXCEPT deduplicate while UNION ALL keeps duplicates, compound
//! operators evaluate left-to-right, ORDER BY over a compound binds to output
//! columns, and row-value comparisons are lexicographic with SQL three-valued
//! logic (a NULL component yields NULL → row excluded from a WHERE filter).

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

async fn fq(f: &Connection, sql: &str) -> Vec<Vec<String>> {
    match f.query_with_params(sql, &[]).await {
        Ok(rows) => rows
            .iter()
            .map(|r| r.values().iter().map(tag_f).collect())
            .collect(),
        Err(e) => vec![vec![format!("<ERR {e:?}>")]],
    }
}
fn rq(r: &rusqlite::Connection, sql: &str) -> Vec<Vec<String>> {
    let mut st = match r.prepare(sql) {
        Ok(st) => st,
        Err(e) => return vec![vec![format!("<ERR {e}>")]],
    };
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

async fn agree(setup: &[&str], sql: &str, msg: &str) {
    let f = Connection::open(":memory:").await.unwrap();
    let r = rusqlite::Connection::open_in_memory().unwrap();
    for s in setup {
        let _ = f.execute(s).await;
        r.execute_batch(s).unwrap();
    }
    let fr = fq(&f, sql).await;
    let rr = rq(&r, sql);
    assert_eq!(
        fr, rr,
        "{msg}\n  sql   ={sql}\n  frank ={fr:?}\n  sqlite={rr:?}"
    );
}

const AB: &[&str] = &[
    "CREATE TABLE a(x INT)",
    "CREATE TABLE b(x INT)",
    "INSERT INTO a VALUES (1),(2),(3)",
    "INSERT INTO b VALUES (2),(3),(4)",
];

// ───────────────────────── compound set operations ────────────────────────

#[test]
fn union_deduplicates() {
    asupersync::test_utils::run_test(|| async {
        agree(
            AB,
            "SELECT x FROM a UNION SELECT x FROM b ORDER BY x",
            "UNION dedups",
        )
        .await;
    });
}

#[test]
fn union_all_keeps_duplicates() {
    asupersync::test_utils::run_test(|| async {
        agree(
            &[
                "CREATE TABLE a(x INT)",
                "CREATE TABLE b(x INT)",
                "INSERT INTO a VALUES (1),(2)",
                "INSERT INTO b VALUES (2),(3)",
            ],
            "SELECT x FROM a UNION ALL SELECT x FROM b ORDER BY x",
            "UNION ALL keeps duplicates",
        )
        .await;
    });
}

#[test]
fn intersect_and_except() {
    asupersync::test_utils::run_test(|| async {
        agree(
            AB,
            "SELECT x FROM a INTERSECT SELECT x FROM b ORDER BY x",
            "INTERSECT",
        )
        .await;
        agree(
            AB,
            "SELECT x FROM a EXCEPT SELECT x FROM b ORDER BY x",
            "EXCEPT",
        )
        .await;
    });
}

#[test]
fn union_treats_nulls_as_equal() {
    asupersync::test_utils::run_test(|| async {
        agree(
            &[],
            "SELECT NULL UNION SELECT NULL UNION SELECT 1 ORDER BY 1",
            "set-op dedup treats NULLs as equal",
        )
        .await;
    });
}

#[test]
fn intersect_and_except_with_nulls() {
    asupersync::test_utils::run_test(|| async {
        agree(
            &[
                "CREATE TABLE a(x INT)",
                "CREATE TABLE b(x INT)",
                "INSERT INTO a VALUES (NULL),(1)",
                "INSERT INTO b VALUES (NULL),(2)",
            ],
            "SELECT x FROM a INTERSECT SELECT x FROM b",
            "INTERSECT matches NULL to NULL",
        )
        .await;
        agree(
            &[
                "CREATE TABLE a(x INT)",
                "CREATE TABLE b(x INT)",
                "INSERT INTO a VALUES (NULL),(1)",
                "INSERT INTO b VALUES (1)",
            ],
            "SELECT x FROM a EXCEPT SELECT x FROM b",
            "EXCEPT keeps the unmatched NULL",
        )
        .await;
    });
}

#[test]
fn union_mixed_affinity_distinct() {
    asupersync::test_utils::run_test(|| async {
        agree(
            &[],
            "SELECT 1 UNION SELECT '1' ORDER BY 1",
            "1 and '1' are distinct under set-op dedup (no affinity coercion)",
        )
        .await;
    });
}

#[test]
fn compound_chain_left_to_right() {
    asupersync::test_utils::run_test(|| async {
        agree(
            &[
                "CREATE TABLE a(x INT)",
                "CREATE TABLE b(x INT)",
                "CREATE TABLE c(x INT)",
                "INSERT INTO a VALUES (1),(2),(3)",
                "INSERT INTO b VALUES (2),(3)",
                "INSERT INTO c VALUES (3)",
            ],
            "SELECT x FROM a EXCEPT SELECT x FROM b UNION SELECT x FROM c ORDER BY x",
            "compound operators evaluate left-to-right",
        )
        .await;
    });
}

#[test]
fn compound_order_by_output_name() {
    asupersync::test_utils::run_test(|| async {
        agree(
            &[
                "CREATE TABLE a(x INT)",
                "CREATE TABLE b(x INT)",
                "INSERT INTO a VALUES (3),(1)",
                "INSERT INTO b VALUES (2)",
            ],
            "SELECT x AS v FROM a UNION SELECT x FROM b ORDER BY v DESC",
            "ORDER BY over a compound binds to the output column name",
        )
        .await;
    });
}

#[test]
fn union_multicolumn_dedup() {
    asupersync::test_utils::run_test(|| async {
        agree(
            &[
                "CREATE TABLE a(x INT, y INT)",
                "CREATE TABLE b(x INT, y INT)",
                "INSERT INTO a VALUES (1,1),(1,2)",
                "INSERT INTO b VALUES (1,2),(2,1)",
            ],
            "SELECT x,y FROM a UNION SELECT x,y FROM b ORDER BY x,y",
            "multi-column UNION dedups on the whole row",
        )
        .await;
    });
}

// ─────────────────────── row-value (tuple) comparisons ─────────────────────

#[test]
fn rowvalue_less_than_lexicographic() {
    asupersync::test_utils::run_test(|| async {
        agree(
            &[
                "CREATE TABLE t(a INT,b INT)",
                "INSERT INTO t VALUES (1,5),(1,2),(2,1),(0,9)",
            ],
            "SELECT a,b FROM t WHERE (a,b) < (1,5) ORDER BY a,b",
            "(a,b) < (c,d) is lexicographic",
        )
        .await;
    });
}

#[test]
fn rowvalue_equality() {
    asupersync::test_utils::run_test(|| async {
        agree(
            &[
                "CREATE TABLE t(a INT,b INT)",
                "INSERT INTO t VALUES (1,2),(1,3),(2,2)",
            ],
            "SELECT a,b FROM t WHERE (a,b) = (1,2)",
            "(a,b) = (c,d) matches all components",
        )
        .await;
    });
}

#[test]
fn rowvalue_in_list_and_subquery() {
    asupersync::test_utils::run_test(|| async {
        agree(
            &[
                "CREATE TABLE t(a INT,b INT)",
                "INSERT INTO t VALUES (1,2),(3,4),(5,6)",
            ],
            "SELECT a,b FROM t WHERE (a,b) IN ((1,2),(5,6)) ORDER BY a",
            "row-value IN a value list",
        )
        .await;
        agree(
            &[
                "CREATE TABLE t(a INT,b INT)",
                "CREATE TABLE u(a INT,b INT)",
                "INSERT INTO t VALUES (1,2),(3,4),(5,6)",
                "INSERT INTO u VALUES (3,4),(5,6)",
            ],
            "SELECT a,b FROM t WHERE (a,b) IN (SELECT a,b FROM u) ORDER BY a",
            "row-value IN a subquery",
        )
        .await;
    });
}

#[test]
fn rowvalue_null_component_excludes_row() {
    asupersync::test_utils::run_test(|| async {
        agree(
            &[
                "CREATE TABLE t(a INT,b INT)",
                "INSERT INTO t VALUES (1,NULL),(1,2),(0,5)",
            ],
            "SELECT a,b FROM t WHERE (a,b) < (1,3) ORDER BY a,b",
            "(1,NULL) < (1,3) is NULL → row excluded",
        )
        .await;
    });
}

#[test]
fn rowvalue_greater_equal() {
    asupersync::test_utils::run_test(|| async {
        agree(
            &[
                "CREATE TABLE t(a INT,b INT)",
                "INSERT INTO t VALUES (1,5),(1,2),(2,1),(2,9)",
            ],
            "SELECT a,b FROM t WHERE (a,b) >= (2,1) ORDER BY a,b",
            "(a,b) >= (c,d) lexicographic",
        )
        .await;
    });
}
