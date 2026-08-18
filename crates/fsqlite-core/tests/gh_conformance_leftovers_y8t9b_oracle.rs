#![recursion_limit = "512"]

//! bd-y8t9b July conformance tail — differential vs rusqlite (bundled SQLite).
//! Each case checks the externally-observable contract (rows / storage class /
//! success-vs-error), not an internal error string.
//!  #164 STRICT INTEGER accepts an exact-integer REAL (1.0 -> integer 1)
//!  #165 UPDATE of a STORED generated column must error
//!  #166 UPDATE of a VIRTUAL generated column must error
//!  #169 CHECK runs AFTER column affinity (TEXT affinity: 1 -> '1' passes typeof='text')
//!  #172 correlated scalar subquery honors ORDER BY when picking LIMIT 1

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
async fn fq(f: &Connection, sql: &str) -> Result<Vec<Vec<String>>, String> {
    match f.query(sql).await {
        Ok(rows) => Ok(rows
            .iter()
            .map(|r| r.values().iter().map(tag_f).collect())
            .collect()),
        Err(e) => Err(format!("{e:?}")),
    }
}
fn rq(r: &rusqlite::Connection, sql: &str) -> Result<Vec<Vec<String>>, String> {
    let mut st = r.prepare(sql).map_err(|e| e.to_string())?;
    let n = st.column_count();
    st.query_map([], |row| {
        Ok((0..n)
            .map(|i| tag_r(&row.get_unwrap::<_, rusqlite::types::Value>(i)))
            .collect())
    })
    .map_err(|e| e.to_string())?
    .collect::<Result<Vec<_>, _>>()
    .map_err(|e| e.to_string())
}
async fn fx(f: &Connection, sql: &str) -> Result<(), String> {
    f.execute(sql)
        .await
        .map(|_| ())
        .map_err(|e| format!("{e:?}"))
}

/// Run `setup` on both, then compare `query` results (rows, or an `<ERR>` marker).
async fn agree(setup: &[&str], query: &str, msg: &str) {
    let f = Connection::open(":memory:").await.unwrap();
    let r = rusqlite::Connection::open_in_memory().unwrap();
    for s in setup {
        let _ = fx(&f, s).await;
        let _ = r.execute_batch(s);
    }
    let fr = fq(&f, query)
        .await
        .unwrap_or_else(|e| vec![vec![format!("<ERR {e}>")]]);
    let rr = rq(&r, query).unwrap_or_else(|e| vec![vec![format!("<ERR {e}>")]]);
    assert_eq!(fr, rr, "{msg}\n  frank={fr:?}\n  stock={rr:?}");
}

/// After `setup`, run `stmt` on both and assert they AGREE on success-vs-error.
/// Then compare the `check` query rows so a silent no-op divergence is caught.
async fn agree_stmt(setup: &[&str], stmt: &str, check: &str, msg: &str) {
    let f = Connection::open(":memory:").await.unwrap();
    let r = rusqlite::Connection::open_in_memory().unwrap();
    for s in setup {
        let _ = fx(&f, s).await;
        let _ = r.execute_batch(s);
    }
    let f_ok = fx(&f, stmt).await.is_ok();
    let r_ok = r.execute_batch(stmt).is_ok();
    assert_eq!(
        f_ok, r_ok,
        "{msg}: success-vs-error diverges (frank_ok={f_ok}, stock_ok={r_ok})"
    );
    let fr = fq(&f, check)
        .await
        .unwrap_or_else(|e| vec![vec![format!("<ERR {e}>")]]);
    let rr = rq(&r, check).unwrap_or_else(|e| vec![vec![format!("<ERR {e}>")]]);
    assert_eq!(
        fr, rr,
        "{msg}: post-state diverges\n  frank={fr:?}\n  stock={rr:?}"
    );
}

#[test]
fn gh164_strict_int_accepts_exact_real() {
    asupersync::test_utils::run_test(|| async {
        // Exact-integer REAL is coerced and stored as integer.
        agree(
            &["CREATE TABLE t(a INT) STRICT", "INSERT INTO t VALUES(1.0)"],
            "SELECT typeof(a), a FROM t",
            "GH#164: STRICT INT must accept exact-integer REAL 1.0 as integer 1",
        )
        .await;
    });
}

#[test]
fn gh164_strict_int_rejects_nonexact_real() {
    asupersync::test_utils::run_test(|| async {
        // Non-exact REAL still violates the STRICT INT contract on both engines.
        agree_stmt(
            &["CREATE TABLE t(a INT) STRICT"],
            "INSERT INTO t VALUES(1.5)",
            "SELECT count(*) FROM t",
            "GH#164: STRICT INT must reject non-exact REAL 1.5",
        )
        .await;
    });
}

#[test]
fn gh165_update_stored_generated_column_errors() {
    asupersync::test_utils::run_test(|| async {
        agree_stmt(
            &[
                "CREATE TABLE t(a, s GENERATED ALWAYS AS (a*2) STORED)",
                "INSERT INTO t(a) VALUES(1)",
            ],
            "UPDATE t SET s=99",
            "SELECT a, s FROM t",
            "GH#165: UPDATE of a STORED generated column must error",
        )
        .await;
    });
}

#[test]
fn gh166_update_virtual_generated_column_errors() {
    asupersync::test_utils::run_test(|| async {
        agree_stmt(
            &[
                "CREATE TABLE t(a, v GENERATED ALWAYS AS (a*2) VIRTUAL)",
                "INSERT INTO t(a) VALUES(1)",
            ],
            "UPDATE t SET v=99",
            "SELECT a, v FROM t",
            "GH#166: UPDATE of a VIRTUAL generated column must error",
        )
        .await;
    });
}

#[test]
fn gh169_check_runs_after_affinity_insert() {
    asupersync::test_utils::run_test(|| async {
        // TEXT affinity converts 1 -> '1' BEFORE the CHECK, so typeof(a)='text' passes.
        agree(
            &[
                "CREATE TABLE t(a TEXT CHECK(typeof(a)='text'))",
                "INSERT INTO t VALUES(1)",
            ],
            "SELECT typeof(a), a FROM t",
            "GH#169: CHECK must run after TEXT affinity on INSERT (1 -> '1')",
        )
        .await;
    });
}

#[test]
fn gh169_check_runs_after_affinity_update() {
    asupersync::test_utils::run_test(|| async {
        agree_stmt(
            &[
                "CREATE TABLE t(a TEXT CHECK(typeof(a)='text'))",
                "INSERT INTO t VALUES('x')",
            ],
            "UPDATE t SET a=1",
            "SELECT typeof(a), a FROM t",
            "GH#169: CHECK must run after TEXT affinity on UPDATE (1 -> '1')",
        )
        .await;
    });
}

// --- DIAGNOSTIC PROBES (not a GH issue): typeof() must never return NULL.
// Discovered while checking #164's reject path: `SELECT typeof(a) FROM <empty>`
// in a bare-aggregate context returned NULL in frank vs 'null' in stock.
#[test]
fn probe_typeof_null_literal() {
    asupersync::test_utils::run_test(|| async {
        agree(
            &[],
            "SELECT typeof(NULL)",
            "probe: typeof(NULL) must be 'null'",
        )
        .await;
    });
}
// bd-9zlcs FIXED: a scan-referencing bare expression over an EMPTY aggregate
// group is now evaluated against NULL columns at finalize (codegen_select_aggregate),
// so typeof(a) = typeof(NULL) = 'null' instead of a bare NULL.
#[test]
fn probe_typeof_empty_bare_aggregate() {
    asupersync::test_utils::run_test(|| async {
        agree(
            &["CREATE TABLE t(a INT)"],
            "SELECT count(*), typeof(a) FROM t",
            "bd-9zlcs: typeof(a) over empty bare-aggregate must be 'null'",
        )
        .await;
    });
}

#[test]
fn probe_scalar_wrappers_empty_bare_aggregate() {
    asupersync::test_utils::run_test(|| async {
        // Several NULL-transforming scalar wrappers over an empty group.
        agree(
            &["CREATE TABLE t(a INT, b TEXT)"],
            "SELECT count(*), typeof(a), coalesce(a, -1), ifnull(b, 'x'), \
                    a IS NULL, length(b), quote(a) FROM t",
            "bd-9zlcs: scalar wrappers over empty bare-aggregate match stock",
        )
        .await;
    });
}

#[test]
fn probe_typeof_where_filters_all_rows() {
    asupersync::test_utils::run_test(|| async {
        // Non-empty table but WHERE excludes every row -> same empty-group path.
        agree(
            &[
                "CREATE TABLE t(a INT)",
                "INSERT INTO t VALUES(1),(2),(3)",
            ],
            "SELECT count(*), typeof(a), coalesce(a,-1) FROM t WHERE a > 100",
            "bd-9zlcs: typeof/coalesce over WHERE-emptied group match stock",
        )
        .await;
    });
}

#[test]
fn probe_typeof_nonempty_bare_aggregate_unchanged() {
    asupersync::test_utils::run_test(|| async {
        // Regression guard: a non-empty group keeps the captured scanned value.
        agree(
            &[
                "CREATE TABLE t(a INT)",
                "INSERT INTO t VALUES(7),(8)",
            ],
            "SELECT count(*), typeof(a), coalesce(a,-1) FROM t",
            "bd-9zlcs: non-empty bare-aggregate value unchanged",
        )
        .await;
    });
}
#[test]
fn probe_typeof_empty_plain_select() {
    asupersync::test_utils::run_test(|| async {
        agree(
            &["CREATE TABLE t(a INT)"],
            "SELECT typeof(a) FROM t",
            "probe: typeof(a) over empty plain SELECT yields zero rows on both",
        )
        .await;
    });
}
#[test]
fn probe_typeof_empty_max_aggregate() {
    asupersync::test_utils::run_test(|| async {
        agree(
            &["CREATE TABLE t(a INT)"],
            "SELECT typeof(max(a)) FROM t",
            "probe: typeof(max(a)) over empty must be 'null'",
        )
        .await;
    });
}

#[test]
fn gh172_correlated_subquery_honors_order_by_limit1() {
    asupersync::test_utils::run_test(|| async {
        agree(
            &[
                "CREATE TABLE o(id INT)",
                "CREATE TABLE i(oid INT, v INT)",
                "INSERT INTO o VALUES(1),(2)",
                // Non-sorted insertion order so ORDER BY vs insertion-order diverge.
                "INSERT INTO i VALUES(1,30),(1,10),(1,20),(2,5),(2,50)",
            ],
            "SELECT id, \
                (SELECT v FROM i WHERE i.oid=o.id ORDER BY v ASC LIMIT 1) AS asc_min, \
                (SELECT v FROM i WHERE i.oid=o.id ORDER BY v DESC LIMIT 1) AS desc_max \
             FROM o ORDER BY id",
            "GH#172: correlated scalar subquery must honor ORDER BY when choosing LIMIT 1",
        )
        .await;
    });
}
