//! Conformance oracle tests — type affinity & coercion corners (cc_3)
//!
//! SQLite's column affinity, comparison affinity, CAST semantics, and arithmetic
//! type rules are foundational and famously corner-heavy. These are the exact
//! places a clean-room reimplementation tends to diverge, and rusqlite (real
//! SQLite) is a perfect oracle. Each query selects `typeof(...)` alongside the
//! value so storage-class divergences are caught, not just value differences.
//!
//! References: <https://www.sqlite.org/datatype3.html> sections 3 (affinity),
//! 4 (comparison), 7 (affinity of expressions), and the CAST docs.

use fsqlite_core::connection::Connection;
use fsqlite_types::value::SqliteValue;

/// Run each query against FrankenSQLite and rusqlite, collecting human-readable
/// mismatch descriptions. An empty return means full parity.
async fn oracle_compare(
    fconn: &Connection,
    rconn: &rusqlite::Connection,
    queries: &[&str],
) -> Vec<String> {
    let mut mismatches = Vec::new();
    for query in queries {
        let frank_result = fconn.query(query).await;
        let csql_result: std::result::Result<Vec<Vec<String>>, String> = (|| {
            let mut stmt = rconn.prepare(query).map_err(|e| format!("prepare: {e}"))?;
            let col_count = stmt.column_count();
            let rows: Vec<Vec<String>> = stmt
                .query_map([], |row| {
                    let mut vals = Vec::new();
                    for i in 0..col_count {
                        let v: rusqlite::types::Value = row.get_unwrap(i);
                        let s = match v {
                            rusqlite::types::Value::Null => "NULL".to_owned(),
                            rusqlite::types::Value::Integer(n) => n.to_string(),
                            rusqlite::types::Value::Real(f) => format!("{f}"),
                            rusqlite::types::Value::Text(s) => format!("'{s}'"),
                            rusqlite::types::Value::Blob(b) => format!(
                                "X'{}'",
                                b.iter().map(|x| format!("{x:02X}")).collect::<String>()
                            ),
                        };
                        vals.push(s);
                    }
                    Ok(vals)
                })
                .map_err(|e| format!("query: {e}"))?
                .collect::<std::result::Result<Vec<_>, _>>()
                .map_err(|e| format!("row: {e}"))?;
            Ok(rows)
        })();
        match (frank_result, csql_result) {
            (Ok(rows), Ok(csql_rows)) => {
                let frank_strs: Vec<Vec<String>> = rows
                    .iter()
                    .map(|row| {
                        row.values()
                            .iter()
                            .map(|v| match v {
                                SqliteValue::Null => "NULL".to_owned(),
                                SqliteValue::Integer(n) => n.to_string(),
                                SqliteValue::Float(f) => format!("{f}"),
                                SqliteValue::Text(s) => format!("'{s}'"),
                                SqliteValue::Blob(b) => format!(
                                    "X'{}'",
                                    b.iter().map(|x| format!("{x:02X}")).collect::<String>()
                                ),
                            })
                            .collect()
                    })
                    .collect();
                if frank_strs != csql_rows {
                    mismatches.push(format!(
                        "MISMATCH: {query}\n  frank: {frank_strs:?}\n  csql:  {csql_rows:?}"
                    ));
                }
            }
            (Ok(rows), Err(csql_err)) => {
                mismatches.push(format!(
                    "DIVERGE: {query}\n  frank: OK ({} rows)\n  csql:  ERROR({csql_err})",
                    rows.len()
                ));
            }
            (Err(e), Ok(csql_rows)) => {
                mismatches.push(format!(
                    "PAIR_FRANK_ERROR: {query}\n  frank: ERROR({e})\n  csql:  {csql_rows:?}"
                ));
            }
            (Err(_), Err(_)) => {
                // Both reject: acceptable parity for these affinity probes.
            }
        }
    }
    mismatches
}

fn assert_no_mismatches(mismatches: &[String], label: &str) {
    if !mismatches.is_empty() {
        for m in mismatches {
            eprintln!("{m}\n");
        }
        panic!("{} {label} mismatch(es)", mismatches.len());
    }
}

/// Apply identical DDL/DML to both engines.
async fn apply(fconn: &Connection, rconn: &rusqlite::Connection, stmts: &[&str]) {
    for s in stmts {
        fconn.execute(s).await.unwrap();
        rconn.execute_batch(s).unwrap();
    }
}

// ---------------------------------------------------------------------------
// Column affinity on INSERT
// ---------------------------------------------------------------------------

#[test]
fn affinity_insert_coercion_per_column() {
    asupersync::test_utils::run_test(|| async {
        let fconn = Connection::open(":memory:").await.unwrap();
        let rconn = rusqlite::Connection::open_in_memory().unwrap();
        apply(
            &fconn,
            &rconn,
            &[
                "CREATE TABLE t (i INTEGER, t TEXT, r REAL, n NUMERIC, b BLOB)",
                // '123' -> int 123, 456 -> text '456', '78.5' -> real, '90' -> int 90,
                // 'hi' -> stays text (BLOB affinity performs no coercion).
                "INSERT INTO t VALUES ('123', 456, '78.5', '90', 'hi')",
            ],
        )
        .await;
        let m = oracle_compare(
            &fconn,
            &rconn,
            &[
                "SELECT typeof(i), i FROM t",
                "SELECT typeof(t), t FROM t",
                "SELECT typeof(r), r FROM t",
                "SELECT typeof(n), n FROM t",
                "SELECT typeof(b), b FROM t",
            ],
        )
        .await;
        assert_no_mismatches(&m, "affinity_insert_coercion_per_column");
    });
}

#[test]
fn affinity_integer_lossless_vs_lossy_real() {
    asupersync::test_utils::run_test(|| async {
        let fconn = Connection::open(":memory:").await.unwrap();
        let rconn = rusqlite::Connection::open_in_memory().unwrap();
        apply(
            &fconn,
            &rconn,
            &[
                "CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER)",
                // 1.0 -> int 1 (lossless), 1.5 -> stays real 1.5 (lossy),
                // '1e3' -> int 1000, '2.0' -> int 2, '9223372036854775807' -> int max.
                "INSERT INTO t(v) VALUES (1.0), (1.5), ('1e3'), ('2.0'), (9223372036854775807)",
            ],
        )
        .await;
        let m = oracle_compare(
            &fconn,
            &rconn,
            &["SELECT id, typeof(v), v FROM t ORDER BY id"],
        )
        .await;
        assert_no_mismatches(&m, "affinity_integer_lossless_vs_lossy_real");
    });
}

#[test]
fn affinity_numeric_column_reduction() {
    asupersync::test_utils::run_test(|| async {
        let fconn = Connection::open(":memory:").await.unwrap();
        let rconn = rusqlite::Connection::open_in_memory().unwrap();
        apply(
            &fconn,
            &rconn,
            &[
                "CREATE TABLE t (id INTEGER PRIMARY KEY, n NUMERIC)",
                // '3.0e2' -> int 300, 'abc' -> text, '123' -> int, 4.0 -> int 4,
                // 5.5 -> real, '007' -> int 7, '' -> text ''.
                "INSERT INTO t(n) VALUES ('3.0e2'), ('abc'), ('123'), (4.0), (5.5), ('007'), ('')",
            ],
        )
        .await;
        let m = oracle_compare(
            &fconn,
            &rconn,
            &["SELECT id, typeof(n), n FROM t ORDER BY id"],
        )
        .await;
        assert_no_mismatches(&m, "affinity_numeric_column_reduction");
    });
}

// ---------------------------------------------------------------------------
// Comparison affinity (RHS literal acquires column affinity)
// ---------------------------------------------------------------------------

#[test]
fn affinity_comparison_applies_column_affinity() {
    asupersync::test_utils::run_test(|| async {
        let fconn = Connection::open(":memory:").await.unwrap();
        let rconn = rusqlite::Connection::open_in_memory().unwrap();
        apply(
            &fconn,
            &rconn,
            &[
                "CREATE TABLE t (i INTEGER, x TEXT)",
                "INSERT INTO t VALUES (2, '2'), (5, '5'), (10, '10'), (100, '100')",
            ],
        )
        .await;
        let m = oracle_compare(
            &fconn,
            &rconn,
            &[
                // i = '5' : RHS gets INTEGER affinity -> numeric match.
                "SELECT i FROM t WHERE i = '5'",
                // x = 5 : RHS gets TEXT affinity -> '5' matches.
                "SELECT x FROM t WHERE x = 5",
                // numeric ordering on INTEGER column.
                "SELECT i FROM t WHERE i > '3' ORDER BY i",
                // lexicographic ordering on TEXT column ('10' < '100' < '2' < '5').
                "SELECT x FROM t WHERE x < '7' ORDER BY x",
                "SELECT x FROM t ORDER BY x",
                "SELECT i FROM t ORDER BY i",
            ],
        )
        .await;
        assert_no_mismatches(&m, "affinity_comparison_applies_column_affinity");
    });
}

/// BETWEEN must apply the comparison affinity of the tested expression to its
/// bounds, exactly as `>=`/`<=` do. Today an INTEGER column compared against
/// text bounds matches nothing because the bounds are not coerced.
#[test]
fn affinity_between_applies_column_affinity() {
    asupersync::test_utils::run_test(|| async {
        let fconn = Connection::open(":memory:").await.unwrap();
        let rconn = rusqlite::Connection::open_in_memory().unwrap();
        apply(
            &fconn,
            &rconn,
            &[
                "CREATE TABLE t (i INTEGER, x TEXT)",
                "INSERT INTO t VALUES (2, '2'), (5, '5'), (10, '10'), (100, '100')",
            ],
        )
        .await;
        let m = oracle_compare(
            &fconn,
            &rconn,
            &[
                // INTEGER column, text bounds -> bounds coerce to INTEGER (matches 5, 10).
                "SELECT i FROM t WHERE i BETWEEN '3' AND '50' ORDER BY i",
                // TEXT column, integer bounds -> bounds coerce to TEXT.
                "SELECT x FROM t WHERE x BETWEEN 2 AND 7 ORDER BY x",
            ],
        )
        .await;
        assert_no_mismatches(&m, "affinity_between_applies_column_affinity");
    });
}

#[test]
fn affinity_no_affinity_column_compares_as_stored() {
    asupersync::test_utils::run_test(|| async {
        let fconn = Connection::open(":memory:").await.unwrap();
        let rconn = rusqlite::Connection::open_in_memory().unwrap();
        apply(
            &fconn,
            &rconn,
            &[
                // No declared type => BLOB (none) affinity: values stored as given.
                "CREATE TABLE t (v)",
                "INSERT INTO t VALUES (1), (2.5), ('1'), ('2.5'), (NULL)",
            ],
        )
        .await;
        let m = oracle_compare(
            &fconn,
            &rconn,
            &[
                // No affinity: integer 1 does NOT equal text '1'.
                "SELECT typeof(v), v FROM t WHERE v = 1",
                "SELECT typeof(v), v FROM t WHERE v = '1'",
                "SELECT typeof(v), v FROM t ORDER BY v",
            ],
        )
        .await;
        assert_no_mismatches(&m, "affinity_no_affinity_column_compares_as_stored");
    });
}

// ---------------------------------------------------------------------------
// Type sort order: NULL < numbers < text < blob
// ---------------------------------------------------------------------------

#[test]
fn affinity_storage_class_sort_order() {
    asupersync::test_utils::run_test(|| async {
        let fconn = Connection::open(":memory:").await.unwrap();
        let rconn = rusqlite::Connection::open_in_memory().unwrap();
        apply(
            &fconn,
            &rconn,
            &[
                "CREATE TABLE t (id INTEGER PRIMARY KEY, v)",
                "INSERT INTO t(v) VALUES (NULL), (3), (2.5), ('apple'), ('banana'), (X'00'), (1), (X'FF')",
            ],
        )
        .await;
        let m = oracle_compare(
            &fconn,
            &rconn,
            &[
                "SELECT typeof(v), v FROM t ORDER BY v, id",
                "SELECT typeof(v), v FROM t ORDER BY v DESC, id",
                "SELECT count(*) FROM t WHERE v < 'a'",
                "SELECT max(v), min(v) FROM t",
            ],
        )
        .await;
        assert_no_mismatches(&m, "affinity_storage_class_sort_order");
    });
}

// ---------------------------------------------------------------------------
// CAST semantics
// ---------------------------------------------------------------------------

#[test]
fn affinity_cast_to_integer() {
    asupersync::test_utils::run_test(|| async {
        let fconn = Connection::open(":memory:").await.unwrap();
        let rconn = rusqlite::Connection::open_in_memory().unwrap();
        let m = oracle_compare(
            &fconn,
            &rconn,
            &[
                "SELECT CAST('123abc' AS INTEGER)",
                "SELECT CAST('abc' AS INTEGER)",
                "SELECT CAST('  12  ' AS INTEGER)",
                "SELECT CAST('-45xyz' AS INTEGER)",
                "SELECT CAST('+7' AS INTEGER)",
                "SELECT CAST(1.9 AS INTEGER)",
                "SELECT CAST(-1.9 AS INTEGER)",
                "SELECT CAST('3.99' AS INTEGER)",
                "SELECT CAST('1e3' AS INTEGER)",
                "SELECT CAST('0x1F' AS INTEGER)",
                "SELECT CAST(NULL AS INTEGER)",
                "SELECT CAST('' AS INTEGER)",
                "SELECT typeof(CAST(5 AS INTEGER)), CAST(5 AS INTEGER)",
            ],
        )
        .await;
        assert_no_mismatches(&m, "affinity_cast_to_integer");
    });
}

#[test]
fn affinity_cast_to_real_and_text() {
    asupersync::test_utils::run_test(|| async {
        let fconn = Connection::open(":memory:").await.unwrap();
        let rconn = rusqlite::Connection::open_in_memory().unwrap();
        let m = oracle_compare(
            &fconn,
            &rconn,
            &[
                "SELECT CAST('3.14' AS REAL)",
                "SELECT CAST('1e3' AS REAL)",
                "SELECT CAST('2.5abc' AS REAL)",
                "SELECT CAST('abc' AS REAL)",
                "SELECT typeof(CAST(5 AS REAL)), CAST(5 AS REAL)",
                "SELECT CAST(100 AS TEXT)",
                "SELECT CAST(3.5 AS TEXT)",
                "SELECT typeof(CAST(42 AS TEXT)), CAST(42 AS TEXT)",
                "SELECT CAST(CAST('9.99' AS REAL) AS TEXT)",
                "SELECT typeof(CAST(7 AS NUMERIC)), CAST(7 AS NUMERIC)",
                "SELECT typeof(CAST('7.0' AS NUMERIC)), CAST('7.0' AS NUMERIC)",
                "SELECT typeof(CAST(7.5 AS NUMERIC)), CAST(7.5 AS NUMERIC)",
            ],
        )
        .await;
        assert_no_mismatches(&m, "affinity_cast_to_real_and_text");
    });
}

#[test]
fn affinity_cast_blob_roundtrips() {
    asupersync::test_utils::run_test(|| async {
        let fconn = Connection::open(":memory:").await.unwrap();
        let rconn = rusqlite::Connection::open_in_memory().unwrap();
        let m = oracle_compare(
            &fconn,
            &rconn,
            &[
                "SELECT typeof(CAST('hi' AS BLOB)), CAST('hi' AS BLOB)",
                "SELECT CAST(X'31' AS INTEGER)",
                "SELECT CAST(X'3132' AS INTEGER)",
                "SELECT typeof(CAST(123 AS BLOB)), CAST(123 AS BLOB)",
                "SELECT CAST(X'312E35' AS REAL)",
            ],
        )
        .await;
        assert_no_mismatches(&m, "affinity_cast_blob_roundtrips");
    });
}

// ---------------------------------------------------------------------------
// Arithmetic / operator type results
// ---------------------------------------------------------------------------

#[test]
fn affinity_arithmetic_result_types() {
    asupersync::test_utils::run_test(|| async {
        let fconn = Connection::open(":memory:").await.unwrap();
        let rconn = rusqlite::Connection::open_in_memory().unwrap();
        let m = oracle_compare(
            &fconn,
            &rconn,
            &[
                "SELECT typeof(1 + 1), 1 + 1",
                "SELECT typeof(1 + 1.0), 1 + 1.0",
                "SELECT typeof(3 / 2), 3 / 2",
                "SELECT typeof(3.0 / 2), 3.0 / 2",
                "SELECT typeof(7 % 3), 7 % 3",
                "SELECT typeof('5' + 3), '5' + 3",
                "SELECT typeof('5.5' + 0), '5.5' + 0",
                "SELECT typeof('abc' + 0), 'abc' + 0",
                "SELECT typeof(5 / 0), 5 / 0",
                "SELECT typeof(5 % 0), 5 % 0",
                "SELECT typeof(2 * 3.0), 2 * 3.0",
                "SELECT typeof(-'4'), -'4'",
                "SELECT typeof('10' - '3'), '10' - '3'",
            ],
        )
        .await;
        assert_no_mismatches(&m, "affinity_arithmetic_result_types");
    });
}

#[test]
fn affinity_concat_and_comparison_operators() {
    asupersync::test_utils::run_test(|| async {
        let fconn = Connection::open(":memory:").await.unwrap();
        let rconn = rusqlite::Connection::open_in_memory().unwrap();
        let m = oracle_compare(
            &fconn,
            &rconn,
            &[
                "SELECT typeof(1 || 2), 1 || 2",
                "SELECT typeof(1.5 || 'x'), 1.5 || 'x'",
                "SELECT 5 = 5.0",
                "SELECT '5' = 5",
                "SELECT '5' < 5",
                "SELECT 1 < 'a'",
                "SELECT 'a' < X'00'",
                "SELECT NULL = NULL",
                "SELECT NULL IS NULL",
                "SELECT 2 IN (1, '2', 3)",
                "SELECT '2' IN (1, 2, 3)",
            ],
        )
        .await;
        assert_no_mismatches(&m, "affinity_concat_and_comparison_operators");
    });
}

// ---------------------------------------------------------------------------
// Affinity propagation through UNION / aggregates
// ---------------------------------------------------------------------------

#[test]
fn affinity_union_and_aggregate_types() {
    asupersync::test_utils::run_test(|| async {
        let fconn = Connection::open(":memory:").await.unwrap();
        let rconn = rusqlite::Connection::open_in_memory().unwrap();
        apply(
            &fconn,
            &rconn,
            &[
                "CREATE TABLE a (v INTEGER)",
                "CREATE TABLE b (v TEXT)",
                "INSERT INTO a VALUES (1), (2), (3)",
                "INSERT INTO b VALUES ('10'), ('20')",
            ],
        )
        .await;
        let m = oracle_compare(
            &fconn,
            &rconn,
            &[
                "SELECT typeof(v), v FROM a UNION ALL SELECT typeof(v), v FROM b",
                "SELECT typeof(sum(v)), sum(v) FROM a",
                "SELECT typeof(avg(v)), avg(v) FROM a",
                "SELECT typeof(total(v)), total(v) FROM a",
                "SELECT typeof(count(v)), count(v) FROM a",
                "SELECT typeof(max(v)) FROM b",
            ],
        )
        .await;
        assert_no_mismatches(&m, "affinity_union_and_aggregate_types");
    });
}
