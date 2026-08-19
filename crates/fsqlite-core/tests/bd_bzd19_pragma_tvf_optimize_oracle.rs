#![recursion_limit = "512"]

//! bd-bzd19 tail — differential/behavioral conformance for two review findings:
//!
//! * **L7**: `PRAGMA optimize` (and its masked forms) on a *read-only*
//!   connection is a silent no-op in stock SQLite — it can neither create nor
//!   update `sqlite_stat1`. FrankenSQLite previously took a write transaction to
//!   count rows and errored `ReadOnly`/`Busy`.
//! * **L11**: the bare pragma table-valued-function rewrite
//!   (`SELECT ... FROM pragma_database_list`) previously only visited the first
//!   SELECT core's FROM clause, so a compound (UNION/…) arm or a FROM-subquery
//!   raised "no such table". It also must respect a real relation of the same
//!   name (TEMP table / CTE) that shadows the eponymous pragma.
//!
//! Cross-checked against stock SQLite (rusqlite, bundled 3.4x) and sqlite3
//! 3.46.1.

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
    match f.query(sql).await {
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
async fn agree(setup: &[&str], query: &str, msg: &str) {
    let f = Connection::open(":memory:").await.unwrap();
    let r = rusqlite::Connection::open_in_memory().unwrap();
    for s in setup {
        let _ = f.execute(s).await;
        let _ = r.execute_batch(s);
    }
    let fr = fq(&f, query).await;
    let rr = rq(&r, query);
    assert_eq!(fr, rr, "{msg}\n  frank={fr:?}\n  stock={rr:?}");
}

// ---------------------------------------------------------------------------
// L11: bare pragma-TVF must resolve in compound arms and FROM-subqueries, and
// must yield to a same-named TEMP table / CTE that shadows the pragma.
// ---------------------------------------------------------------------------

#[test]
fn bare_pragma_tvf_in_compound_union_arm() {
    asupersync::test_utils::run_test(|| async {
        agree(
            &[],
            "SELECT 1 UNION SELECT seq FROM pragma_database_list ORDER BY 1",
            "L11: bare pragma_database_list in a UNION arm",
        )
        .await;
    });
}

#[test]
fn bare_pragma_tvf_in_from_subquery() {
    asupersync::test_utils::run_test(|| async {
        agree(
            &[],
            "SELECT x FROM (SELECT seq AS x FROM pragma_database_list)",
            "L11: bare pragma_database_list inside a FROM-subquery",
        )
        .await;
    });
}

#[test]
fn bare_pragma_tvf_in_compound_arm_from_subquery() {
    asupersync::test_utils::run_test(|| async {
        agree(
            &[],
            "SELECT 9 UNION SELECT x FROM (SELECT seq AS x FROM pragma_database_list) ORDER BY 1",
            "L11: bare pragma_database_list in a subquery inside a compound arm",
        )
        .await;
    });
}

#[test]
fn bare_pragma_tvf_call_form_still_works() {
    asupersync::test_utils::run_test(|| async {
        agree(
            &[],
            "SELECT 1 UNION SELECT seq FROM pragma_database_list() ORDER BY 1",
            "L11 control: the parenthesized call form works in a compound arm",
        )
        .await;
    });
}

#[test]
fn temp_table_shadows_bare_pragma_tvf() {
    asupersync::test_utils::run_test(|| async {
        agree(
            &[
                "CREATE TEMP TABLE pragma_database_list(x)",
                "INSERT INTO pragma_database_list VALUES (42)",
            ],
            "SELECT x FROM pragma_database_list",
            "L11: a TEMP table named pragma_database_list shadows the pragma-TVF",
        )
        .await;
    });
}

#[test]
fn cte_shadows_bare_pragma_tvf() {
    asupersync::test_utils::run_test(|| async {
        agree(
            &[],
            "WITH pragma_database_list(x) AS (SELECT 77) SELECT x FROM pragma_database_list",
            "L11: a CTE named pragma_database_list shadows the pragma-TVF",
        )
        .await;
    });
}

#[test]
fn nested_cte_shadows_bare_pragma_tvf_in_subquery() {
    asupersync::test_utils::run_test(|| async {
        agree(
            &[],
            "SELECT y FROM (WITH pragma_database_list(y) AS (SELECT 5) \
                            SELECT y FROM pragma_database_list)",
            "L11: a CTE local to a FROM-subquery shadows the pragma-TVF there",
        )
        .await;
    });
}

// ---------------------------------------------------------------------------
// L7: PRAGMA optimize on a read-only connection is a silent no-op.
// ---------------------------------------------------------------------------

#[test]
fn pragma_optimize_on_read_only_connection_is_noop() {
    asupersync::test_utils::run_test(|| async {
        let dir = tempfile::tempdir().expect("temp dir");
        let src = dir.path().join("t.db").to_string_lossy().into_owned();

        // Populate a read-write database, then close it.
        {
            let rw = Connection::open(&src).await.expect("create source");
            rw.execute("CREATE TABLE t(a); CREATE TABLE u(b);")
                .await
                .expect("create tables");
            rw.execute("INSERT INTO t VALUES (1),(2),(3); INSERT INTO u VALUES (9);")
                .await
                .expect("insert");
            rw.close().await.expect("close source");
        }

        // Reopen read-only (open_schema_only opens a read-only pager). Every
        // PRAGMA optimize form must return Ok with no rows — never ReadOnly/Busy.
        let ro = Connection::open_schema_only(&src)
            .await
            .expect("open read-only source");
        for sql in [
            "PRAGMA optimize",
            "PRAGMA optimize(0x02)",
            "PRAGMA optimize(0x10)",
            "PRAGMA optimize(0x12)",
        ] {
            let rows = ro.query(sql).await.unwrap_or_else(|e| {
                panic!("`{sql}` must be a no-op on a read-only conn, got {e:?}")
            });
            assert!(
                rows.is_empty(),
                "`{sql}` returns no rows outside debug mode, got {rows:?}"
            );
        }
        ro.close().await.expect("close read-only");

        // Cross-engine: stock SQLite also treats these as no-ops on a read-only
        // connection (exit 0, no error).
        let stock =
            rusqlite::Connection::open_with_flags(&src, rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY)
                .expect("stock read-only open");
        for sql in ["PRAGMA optimize", "PRAGMA optimize(0x02)"] {
            stock
                .execute_batch(sql)
                .unwrap_or_else(|e| panic!("stock `{sql}` must be a read-only no-op, got {e}"));
        }
    });
}
