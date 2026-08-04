//! bd-q18el — Oracle-parity e2e: aggregate-function semantics vs rusqlite.
//!
//! Aggregates carry a lot of subtle SQLite-specific rules that are easy to get
//! subtly wrong: the storage class of `sum`/`avg`/`total` results, the
//! empty-set vs all-NULL distinctions (`sum`→NULL but `total`→0.0; `count`→0),
//! NULL-skipping in `count(col)` and `group_concat`, `count(DISTINCT)` /
//! `sum(DISTINCT)` dedup, `min`/`max` over mixed storage classes (the
//! NULL<number<text<blob ordering), and the integer-overflow behaviour of
//! `sum` vs `total`. Each scenario asserts per-statement agreement with
//! rusqlite, then compares query results.
#![recursion_limit = "512"]

use fsqlite::Connection;
use fsqlite_types::SqliteValue;

fn render_frank(v: &SqliteValue) -> String {
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

async fn frank_rows(conn: &Connection, sql: &str) -> Result<Vec<Vec<String>>, String> {
    let rows = conn.query(sql).await.map_err(|e| e.to_string())?;
    Ok(rows
        .iter()
        .map(|row| row.values().iter().map(render_frank).collect())
        .collect())
}

fn sqlite_rows(conn: &rusqlite::Connection, sql: &str) -> Result<Vec<Vec<String>>, String> {
    let mut stmt = conn.prepare(sql).map_err(|e| e.to_string())?;
    let n = stmt.column_count();
    stmt.query_map([], |row| {
        let mut out = Vec::with_capacity(n);
        for i in 0..n {
            let v: rusqlite::types::Value = row.get_unwrap(i);
            out.push(match v {
                rusqlite::types::Value::Null => "NULL".to_owned(),
                rusqlite::types::Value::Integer(x) => x.to_string(),
                rusqlite::types::Value::Real(f) => format!("{f}"),
                rusqlite::types::Value::Text(s) => format!("'{s}'"),
                rusqlite::types::Value::Blob(b) => format!(
                    "X'{}'",
                    b.iter().map(|x| format!("{x:02X}")).collect::<String>()
                ),
            });
        }
        Ok(out)
    })
    .map_err(|e| e.to_string())?
    .collect::<Result<Vec<_>, _>>()
    .map_err(|e| e.to_string())
}

async fn scenario(stmts: &[&str], queries: &[&str], label: &str) {
    let f = Connection::open(":memory:").await.expect("open frank");
    let r = rusqlite::Connection::open_in_memory().expect("open rusqlite");
    for s in stmts {
        let fe = f.execute(s).await;
        let re = r.execute_batch(s);
        match (&fe, &re) {
            (Ok(_), Ok(())) | (Err(_), Err(_)) => {}
            (Ok(_), Err(e)) => panic!("{label}: `{s}`\n  frank: OK\n  csql:  ERROR({e})"),
            (Err(e), Ok(())) => panic!("{label}: `{s}`\n  frank: ERROR({e})\n  csql:  OK"),
        }
    }
    let mut mismatches = Vec::new();
    for q in queries {
        match (frank_rows(&f, q).await, sqlite_rows(&r, q)) {
            (Ok(a), Ok(b)) if a == b => {}
            (Ok(a), Ok(b)) => {
                mismatches.push(format!("MISMATCH: {q}\n  frank: {a:?}\n  csql:  {b:?}"))
            }
            (Err(e), Ok(b)) => mismatches.push(format!(
                "FRANK_ERR: {q}\n  frank: ERROR({e})\n  csql:  {b:?}"
            )),
            (Ok(a), Err(e)) => {
                mismatches.push(format!("CSQL_ERR: {q}\n  frank: {a:?}\n  csql: ERROR({e})"))
            }
            (Err(_), Err(_)) => {}
        }
    }
    assert!(
        mismatches.is_empty(),
        "{label}: {} mismatch(es)\n{}",
        mismatches.len(),
        mismatches.join("\n")
    );
}

#[test]
fn agg_sum_avg_total_type_rules() {
    asupersync::test_utils::run_test(|| async {
        scenario(
            &[
                "CREATE TABLE t (id INTEGER PRIMARY KEY, i INTEGER, r REAL)",
                "INSERT INTO t VALUES (1,10,1.5),(2,20,2.5),(3,30,3.0)",
            ],
            &[
                // sum over integers stays integer; over reals becomes real.
                "SELECT sum(i), typeof(sum(i)) FROM t",
                "SELECT sum(r), typeof(sum(r)) FROM t",
                // avg is always real even over integers.
                "SELECT avg(i), typeof(avg(i)) FROM t",
                // total is always real.
                "SELECT total(i), typeof(total(i)) FROM t",
                // mixed: integer + real promotes to real.
                "SELECT sum(i) + sum(r) FROM t",
            ],
            "agg_sum_avg_total_type_rules",
        )
        .await;
    });
}

#[test]
fn agg_empty_set() {
    asupersync::test_utils::run_test(|| async {
        scenario(
            &["CREATE TABLE e (x INTEGER)"],
            &[
                // Empty input: count->0, sum->NULL, total->0.0, avg/min/max->NULL.
                "SELECT count(*), count(x), sum(x), total(x), avg(x), min(x), max(x) FROM e",
                "SELECT typeof(sum(x)), typeof(total(x)), typeof(count(x)), \
                 typeof(avg(x)), typeof(min(x)), typeof(max(x)) FROM e",
            ],
            "agg_empty_set",
        )
        .await;
    });
}

#[test]
fn agg_all_null_inputs() {
    asupersync::test_utils::run_test(|| async {
        scenario(
            &[
                "CREATE TABLE n (x INTEGER)",
                "INSERT INTO n VALUES (NULL),(NULL),(NULL)",
            ],
            &[
                // All-NULL behaves like empty for value aggregates, but count(*) sees rows.
                "SELECT count(*), count(x), sum(x), total(x), avg(x), min(x), max(x) FROM n",
                "SELECT typeof(sum(x)), typeof(total(x)) FROM n",
            ],
            "agg_all_null_inputs",
        )
        .await;
    });
}

#[test]
fn agg_count_variants() {
    asupersync::test_utils::run_test(|| async {
        scenario(
            &[
                "CREATE TABLE c (id INTEGER PRIMARY KEY, v INTEGER, g TEXT)",
                "INSERT INTO c VALUES (1,10,'a'),(2,10,'a'),(3,20,'b'),(4,NULL,'b'),(5,20,NULL)",
            ],
            &[
                "SELECT count(*) FROM c",          // 5
                "SELECT count(v) FROM c",          // 4 (NULL skipped)
                "SELECT count(DISTINCT v) FROM c", // 2 (10,20; NULL excluded)
                "SELECT count(DISTINCT g) FROM c", // 2 (a,b; NULL excluded)
                // GROUP BY a column with a NULL group; NULL sorts first.
                "SELECT g, count(*), count(v) FROM c GROUP BY g ORDER BY g",
            ],
            "agg_count_variants",
        )
        .await;
    });
}

#[test]
fn agg_distinct_aggregates() {
    asupersync::test_utils::run_test(|| async {
        scenario(
            &[
                "CREATE TABLE d (v INTEGER)",
                "INSERT INTO d VALUES (1),(1),(2),(2),(3),(NULL)",
            ],
            &[
                // DISTINCT dedups before aggregating; NULL excluded throughout.
                "SELECT sum(DISTINCT v), avg(DISTINCT v), count(DISTINCT v) FROM d",
                "SELECT sum(v), avg(v), count(v) FROM d",
            ],
            "agg_distinct_aggregates",
        )
        .await;
    });
}

#[test]
fn agg_min_max_mixed_storage_classes() {
    asupersync::test_utils::run_test(|| async {
        scenario(
            &[
                // No declared type -> values keep their literal storage class.
                "CREATE TABLE m (x)",
                "INSERT INTO m VALUES (NULL),(5),(2.5),('apple'),('banana'),(100)",
            ],
            &[
                // SQLite ordering: NULL < numbers < text < blob. min/max ignore NULL.
                // min over numbers -> 2.5 (real); max over text -> 'banana'.
                "SELECT min(x), max(x), typeof(min(x)), typeof(max(x)) FROM m",
            ],
            "agg_min_max_mixed_storage_classes",
        )
        .await;
    });
}

#[test]
fn agg_min_max_numeric_only() {
    asupersync::test_utils::run_test(|| async {
        scenario(
            &[
                "CREATE TABLE p (x INTEGER)",
                "INSERT INTO p VALUES (NULL),(7),(3),(NULL),(11),(3)",
            ],
            &[
                "SELECT min(x), max(x) FROM p",                 // 3, 11
                "SELECT typeof(min(x)), typeof(max(x)) FROM p", // integer, integer
            ],
            "agg_min_max_numeric_only",
        )
        .await;
    });
}

#[test]
fn agg_group_concat_separator_and_null_skip() {
    asupersync::test_utils::run_test(|| async {
        scenario(
            &[
                "CREATE TABLE gc (id INTEGER PRIMARY KEY, grp TEXT, val TEXT)",
                "INSERT INTO gc VALUES (1,'a','x'),(2,'a','y'),(3,'a',NULL),(4,'b','z')",
            ],
            &[
                // Default separator is ','; NULL values are skipped.
                "SELECT group_concat(val) FROM gc WHERE grp='a'",
                // Custom separator.
                "SELECT group_concat(val, '|') FROM gc WHERE grp='a'",
                // Grouped: one concatenation per group.
                "SELECT grp, group_concat(val) FROM gc GROUP BY grp ORDER BY grp",
            ],
            "agg_group_concat_separator_and_null_skip",
        )
        .await;
    });
}

#[test]
fn agg_sum_integer_overflow() {
    asupersync::test_utils::run_test(|| async {
        // SQLite's sum() throws "integer overflow" when all inputs are integers and
        // the running total exceeds i64; total() never does. If frank silently
        // promotes to REAL instead of erroring, the (Err,Ok)/(Ok,Err) arms catch it.
        scenario(
            &[
                "CREATE TABLE so (x INTEGER)",
                "INSERT INTO so VALUES (9223372036854775807),(9223372036854775807)",
            ],
            &["SELECT sum(x) FROM so"],
            "agg_sum_integer_overflow",
        )
        .await;
    });
}

#[test]
fn agg_total_overflow_stays_real() {
    asupersync::test_utils::run_test(|| async {
        scenario(
            &[
                "CREATE TABLE big (x INTEGER)",
                "INSERT INTO big VALUES (9223372036854775807),(9223372036854775807)",
            ],
            &[
                // total() never overflows -> approximate real result on both engines.
                "SELECT total(x), typeof(total(x)) FROM big",
            ],
            "agg_total_overflow_stays_real",
        )
        .await;
    });
}
