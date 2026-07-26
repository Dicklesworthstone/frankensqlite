//! Conformance oracle tests — Session 83 (cc4 multi-table JOIN permutations + aggregate edge cases)
//!
//! Probes for PLANNER-3 join-order reshaping correctness (commit eecba647):
//! five+ table JOINs, mixed LEFT/INNER, self-joins with aggregates,
//! complex GROUP BY + HAVING, window functions over JOINs, and DML with
//! subquery sources referencing multiple tables.

use fsqlite_core::connection::Connection;
use fsqlite_types::value::SqliteValue;

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
            (Ok(_), Err(csql_err)) => {
                mismatches.push(format!(
                    "DIVERGE: {query}\n  frank: OK\n  csql:  ERROR({csql_err})"
                ));
            }
            (Err(e), Ok(csql_rows)) => {
                mismatches.push(format!(
                    "PAIR_FRANK_ERROR[{query}]\n  frank: ERROR({e})\n  csql:  {csql_rows:?}"
                ));
            }
            (Err(frank_err), Err(csql_err)) => {
                mismatches.push(format!(
                    "BOTH_ERROR: {query}\n  frank: ERROR({frank_err})\n  csql:  ERROR({csql_err})"
                ));
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

async fn setup_five_table_schema(fconn: &Connection, rconn: &rusqlite::Connection) {
    let ddl = [
        "CREATE TABLE regions (id INTEGER PRIMARY KEY, name TEXT)",
        "CREATE TABLE stores (id INTEGER PRIMARY KEY, region_id INTEGER, name TEXT)",
        "CREATE TABLE products (id INTEGER PRIMARY KEY, name TEXT, base_price REAL)",
        "CREATE TABLE sales (id INTEGER PRIMARY KEY, store_id INTEGER, product_id INTEGER, qty INTEGER, sale_date TEXT)",
        "CREATE TABLE returns (id INTEGER PRIMARY KEY, sale_id INTEGER, qty INTEGER, reason TEXT)",
        "INSERT INTO regions VALUES (1, 'North'), (2, 'South'), (3, 'East')",
        "INSERT INTO stores VALUES (10, 1, 'Store-A'), (20, 1, 'Store-B'), (30, 2, 'Store-C'), (40, 3, 'Store-D')",
        "INSERT INTO products VALUES (100, 'Widget', 9.99), (200, 'Gadget', 24.99), (300, 'Bolt', 1.50)",
        "INSERT INTO sales VALUES (1, 10, 100, 5, '2025-01-15'), (2, 10, 200, 2, '2025-01-16'), (3, 20, 100, 10, '2025-02-01'), (4, 30, 300, 100, '2025-02-10'), (5, 40, 200, 3, '2025-03-01'), (6, 30, 100, 7, '2025-03-05'), (7, 10, 300, 50, '2025-03-10')",
        "INSERT INTO returns VALUES (1, 1, 1, 'defective'), (2, 4, 5, 'wrong_item'), (3, 6, 2, 'defective')",
    ];
    for s in &ddl {
        fconn.execute(s).await.unwrap();
        rconn.execute_batch(s).unwrap();
    }
}

#[test]
fn test_conformance_five_table_join_s83a() {
    asupersync::test_utils::run_test(|| async {
        let fconn = Connection::open(":memory:").await.unwrap();
        let rconn = rusqlite::Connection::open_in_memory().unwrap();
        setup_five_table_schema(&fconn, &rconn).await;
        let m = oracle_compare(
        &fconn,
        &rconn,
        &[
            "SELECT r.name AS region, st.name AS store, p.name AS product, s.qty, s.sale_date FROM regions r JOIN stores st ON st.region_id = r.id JOIN sales s ON s.store_id = st.id JOIN products p ON p.id = s.product_id ORDER BY r.name, st.name, s.sale_date",
        ],
    )
    .await;
        assert_no_mismatches(&m, "five_table_join_s83a");
    });
}

#[test]
fn test_conformance_five_table_left_join_returns_s83b() {
    asupersync::test_utils::run_test(|| async {
        let fconn = Connection::open(":memory:").await.unwrap();
        let rconn = rusqlite::Connection::open_in_memory().unwrap();
        setup_five_table_schema(&fconn, &rconn).await;
        let m = oracle_compare(
        &fconn,
        &rconn,
        &[
            "SELECT s.id AS sale_id, p.name, s.qty AS sold, COALESCE(ret.qty, 0) AS returned FROM sales s JOIN products p ON p.id = s.product_id LEFT JOIN returns ret ON ret.sale_id = s.id ORDER BY s.id",
        ],
    )
    .await;
        assert_no_mismatches(&m, "five_table_left_join_returns_s83b");
    });
}

#[test]
fn test_conformance_five_table_aggregate_by_region_s83c() {
    asupersync::test_utils::run_test(|| async {
        let fconn = Connection::open(":memory:").await.unwrap();
        let rconn = rusqlite::Connection::open_in_memory().unwrap();
        setup_five_table_schema(&fconn, &rconn).await;
        let m = oracle_compare(
        &fconn,
        &rconn,
        &[
            "SELECT r.name, SUM(s.qty) AS total_sold, COUNT(DISTINCT p.id) AS distinct_products FROM regions r JOIN stores st ON st.region_id = r.id JOIN sales s ON s.store_id = st.id JOIN products p ON p.id = s.product_id GROUP BY r.name ORDER BY total_sold DESC",
        ],
    )
    .await;
        assert_no_mismatches(&m, "five_table_aggregate_by_region_s83c");
    });
}

#[test]
fn test_conformance_five_table_having_filter_s83d() {
    asupersync::test_utils::run_test(|| async {
        let fconn = Connection::open(":memory:").await.unwrap();
        let rconn = rusqlite::Connection::open_in_memory().unwrap();
        setup_five_table_schema(&fconn, &rconn).await;
        let m = oracle_compare(
        &fconn,
        &rconn,
        &[
            "SELECT st.name, SUM(s.qty * p.base_price) AS revenue FROM stores st JOIN sales s ON s.store_id = st.id JOIN products p ON p.id = s.product_id GROUP BY st.name HAVING revenue > 50.0 ORDER BY revenue DESC",
        ],
    )
    .await;
        assert_no_mismatches(&m, "five_table_having_filter_s83d");
    });
}

#[test]
fn test_conformance_five_table_subquery_in_where_s83e() {
    asupersync::test_utils::run_test(|| async {
        let fconn = Connection::open(":memory:").await.unwrap();
        let rconn = rusqlite::Connection::open_in_memory().unwrap();
        setup_five_table_schema(&fconn, &rconn).await;
        let m = oracle_compare(
        &fconn,
        &rconn,
        &[
            "SELECT p.name, SUM(s.qty) AS total FROM products p JOIN sales s ON s.product_id = p.id WHERE s.store_id IN (SELECT id FROM stores WHERE region_id = 1) GROUP BY p.name ORDER BY total DESC",
        ],
    )
    .await;
        assert_no_mismatches(&m, "five_table_subquery_in_where_s83e");
    });
}

#[test]
fn test_conformance_net_sales_after_returns_s83f() {
    asupersync::test_utils::run_test(|| async {
        let fconn = Connection::open(":memory:").await.unwrap();
        let rconn = rusqlite::Connection::open_in_memory().unwrap();
        setup_five_table_schema(&fconn, &rconn).await;
        let m = oracle_compare(
        &fconn,
        &rconn,
        &[
            "SELECT p.name, SUM(s.qty) - COALESCE(SUM(ret.qty), 0) AS net_qty FROM products p JOIN sales s ON s.product_id = p.id LEFT JOIN returns ret ON ret.sale_id = s.id GROUP BY p.name ORDER BY net_qty DESC",
        ],
    )
    .await;
        assert_no_mismatches(&m, "net_sales_after_returns_s83f");
    });
}

#[test]
fn test_conformance_window_over_join_s83g() {
    asupersync::test_utils::run_test(|| async {
        let fconn = Connection::open(":memory:").await.unwrap();
        let rconn = rusqlite::Connection::open_in_memory().unwrap();
        setup_five_table_schema(&fconn, &rconn).await;
        let m = oracle_compare(
        &fconn,
        &rconn,
        &[
            "SELECT s.id, p.name, s.qty, SUM(s.qty) OVER (PARTITION BY p.id ORDER BY s.sale_date) AS running_total FROM sales s JOIN products p ON p.id = s.product_id ORDER BY p.name, s.sale_date",
        ],
    )
    .await;
        assert_no_mismatches(&m, "window_over_join_s83g");
    });
}

#[test]
fn test_conformance_cross_join_filtered_s83h() {
    asupersync::test_utils::run_test(|| async {
        let fconn = Connection::open(":memory:").await.unwrap();
        let rconn = rusqlite::Connection::open_in_memory().unwrap();
        setup_five_table_schema(&fconn, &rconn).await;
        let m = oracle_compare(
        &fconn,
        &rconn,
        &[
            "SELECT r.name, p.name, p.base_price FROM regions r CROSS JOIN products p WHERE p.base_price > 5.0 ORDER BY r.name, p.name",
        ],
    )
    .await;
        assert_no_mismatches(&m, "cross_join_filtered_s83h");
    });
}

#[test]
fn test_conformance_self_join_store_comparison_s83i() {
    asupersync::test_utils::run_test(|| async {
        let fconn = Connection::open(":memory:").await.unwrap();
        let rconn = rusqlite::Connection::open_in_memory().unwrap();
        setup_five_table_schema(&fconn, &rconn).await;
        let m = oracle_compare(
        &fconn,
        &rconn,
        &[
            "SELECT a.name AS store_a, b.name AS store_b FROM stores a JOIN stores b ON a.region_id = b.region_id AND a.id < b.id ORDER BY a.name, b.name",
        ],
    )
    .await;
        assert_no_mismatches(&m, "self_join_store_comparison_s83i");
    });
}

#[test]
fn test_conformance_correlated_exists_multi_table_s83j() {
    asupersync::test_utils::run_test(|| async {
        let fconn = Connection::open(":memory:").await.unwrap();
        let rconn = rusqlite::Connection::open_in_memory().unwrap();
        setup_five_table_schema(&fconn, &rconn).await;
        let m = oracle_compare(
        &fconn,
        &rconn,
        &[
            "SELECT r.name FROM regions r WHERE EXISTS (SELECT 1 FROM stores st JOIN sales s ON s.store_id = st.id WHERE st.region_id = r.id AND s.qty > 5) ORDER BY r.name",
            "SELECT r.name FROM regions r WHERE EXISTS (SELECT 1 FROM stores st JOIN sales s ON s.store_id = st.id AND st.region_id = r.id WHERE s.qty > 5) ORDER BY r.name",
            "SELECT st.name, s.qty FROM stores st JOIN sales s ON s.store_id = (SELECT st.id) WHERE s.qty > 5 ORDER BY st.name, s.qty",
            "SELECT st.name FROM stores st WHERE (SELECT COUNT(*) FILTER (WHERE s.store_id = st.id) FROM sales s) > 0 ORDER BY st.name",
        ],
    )
    .await;
        assert_no_mismatches(&m, "correlated_exists_multi_table_s83j");
    });
}

#[test]
fn test_conformance_cte_multi_join_s83k() {
    asupersync::test_utils::run_test(|| async {
        let fconn = Connection::open(":memory:").await.unwrap();
        let rconn = rusqlite::Connection::open_in_memory().unwrap();
        setup_five_table_schema(&fconn, &rconn).await;
        let m = oracle_compare(
        &fconn,
        &rconn,
        &[
            "WITH store_totals AS (SELECT st.id, st.name, SUM(s.qty) AS total FROM stores st JOIN sales s ON s.store_id = st.id GROUP BY st.id, st.name) SELECT r.name AS region, t.name AS store, t.total FROM regions r JOIN store_totals t ON t.id IN (SELECT id FROM stores WHERE region_id = r.id) ORDER BY r.name, t.total DESC",
        ],
    )
    .await;
        assert_no_mismatches(&m, "cte_multi_join_s83k");
    });
}

#[test]
fn test_conformance_union_all_join_s83l() {
    asupersync::test_utils::run_test(|| async {
        let fconn = Connection::open(":memory:").await.unwrap();
        let rconn = rusqlite::Connection::open_in_memory().unwrap();
        setup_five_table_schema(&fconn, &rconn).await;
        let m = oracle_compare(
        &fconn,
        &rconn,
        &[
            "SELECT 'sale' AS type, s.id AS eid, p.name, s.qty FROM sales s JOIN products p ON p.id = s.product_id WHERE s.qty >= 10 UNION ALL SELECT 'return' AS type, ret.id AS eid, p.name, ret.qty FROM returns ret JOIN sales s ON s.id = ret.sale_id JOIN products p ON p.id = s.product_id ORDER BY 1, 2",
        ],
    )
    .await;
        assert_no_mismatches(&m, "union_all_join_s83l");
    });
}

#[test]
fn test_conformance_derived_table_join_s83m() {
    asupersync::test_utils::run_test(|| async {
        let fconn = Connection::open(":memory:").await.unwrap();
        let rconn = rusqlite::Connection::open_in_memory().unwrap();
        setup_five_table_schema(&fconn, &rconn).await;
        let m = oracle_compare(
        &fconn,
        &rconn,
        &[
            "SELECT d.region_name, d.store_count, SUM(s.qty) AS total_qty FROM (SELECT r.id AS region_id, r.name AS region_name, COUNT(*) AS store_count FROM regions r JOIN stores st ON st.region_id = r.id GROUP BY r.id, r.name) d JOIN stores st2 ON st2.region_id = d.region_id JOIN sales s ON s.store_id = st2.id GROUP BY d.region_name, d.store_count ORDER BY total_qty DESC",
        ],
    )
    .await;
        assert_no_mismatches(&m, "derived_table_join_s83m");
    });
}

#[test]
fn test_conformance_case_in_join_s83n() {
    asupersync::test_utils::run_test(|| async {
        let fconn = Connection::open(":memory:").await.unwrap();
        let rconn = rusqlite::Connection::open_in_memory().unwrap();
        setup_five_table_schema(&fconn, &rconn).await;
        let m = oracle_compare(
        &fconn,
        &rconn,
        &[
            "SELECT p.name, CASE WHEN SUM(s.qty) > 20 THEN 'high' WHEN SUM(s.qty) > 5 THEN 'medium' ELSE 'low' END AS demand, SUM(s.qty) AS total FROM products p JOIN sales s ON s.product_id = p.id GROUP BY p.name ORDER BY total DESC",
        ],
    )
    .await;
        assert_no_mismatches(&m, "case_in_join_s83n");
    });
}

#[test]
fn test_conformance_multi_agg_group_by_expr_s83o() {
    asupersync::test_utils::run_test(|| async {
        let fconn = Connection::open(":memory:").await.unwrap();
        let rconn = rusqlite::Connection::open_in_memory().unwrap();
        setup_five_table_schema(&fconn, &rconn).await;
        let m = oracle_compare(
        &fconn,
        &rconn,
        &[
            "SELECT substr(s.sale_date, 1, 7) AS month, COUNT(*) AS num_sales, SUM(s.qty) AS total_qty, AVG(s.qty) AS avg_qty, MIN(s.qty) AS min_qty, MAX(s.qty) AS max_qty FROM sales s GROUP BY substr(s.sale_date, 1, 7) ORDER BY month",
        ],
    )
    .await;
        assert_no_mismatches(&m, "multi_agg_group_by_expr_s83o");
    });
}

#[test]
fn test_conformance_left_join_null_agg_s83p() {
    asupersync::test_utils::run_test(|| async {
        let fconn = Connection::open(":memory:").await.unwrap();
        let rconn = rusqlite::Connection::open_in_memory().unwrap();
        setup_five_table_schema(&fconn, &rconn).await;
        let m = oracle_compare(
        &fconn,
        &rconn,
        &[
            "SELECT st.name, COALESCE(SUM(s.qty), 0) AS total FROM stores st LEFT JOIN sales s ON s.store_id = st.id GROUP BY st.name ORDER BY st.name",
            "SELECT p.name, COUNT(ret.id) AS return_count FROM products p LEFT JOIN sales s ON s.product_id = p.id LEFT JOIN returns ret ON ret.sale_id = s.id GROUP BY p.name ORDER BY p.name",
        ],
    )
    .await;
        assert_no_mismatches(&m, "left_join_null_agg_s83p");
    });
}

#[test]
fn test_conformance_delete_via_subquery_join_s83q() {
    asupersync::test_utils::run_test(|| async {
        let fconn = Connection::open(":memory:").await.unwrap();
        let rconn = rusqlite::Connection::open_in_memory().unwrap();
        setup_five_table_schema(&fconn, &rconn).await;
        let del = "DELETE FROM sales WHERE store_id IN (SELECT st.id FROM stores st JOIN regions r ON r.id = st.region_id WHERE r.name = 'East')";
        fconn.execute(del).await.unwrap();
        rconn.execute_batch(del).unwrap();
        let m = oracle_compare(
        &fconn,
        &rconn,
        &[
            "SELECT COUNT(*) FROM sales",
            "SELECT s.id, st.name FROM sales s JOIN stores st ON st.id = s.store_id ORDER BY s.id",
        ],
    )
    .await;
        assert_no_mismatches(&m, "delete_via_subquery_join_s83q");
    });
}

#[test]
fn test_conformance_update_via_subquery_join_s83r() {
    asupersync::test_utils::run_test(|| async {
        let fconn = Connection::open(":memory:").await.unwrap();
        let rconn = rusqlite::Connection::open_in_memory().unwrap();
        setup_five_table_schema(&fconn, &rconn).await;
        let upd = "UPDATE products SET base_price = base_price * 0.9 WHERE id IN (SELECT DISTINCT s.product_id FROM sales s JOIN stores st ON s.store_id = st.id WHERE st.region_id = 2)";
        fconn.execute(upd).await.unwrap();
        rconn.execute_batch(upd).unwrap();
        let m = oracle_compare(
            &fconn,
            &rconn,
            &["SELECT id, name, base_price FROM products ORDER BY id"],
        )
        .await;
        assert_no_mismatches(&m, "update_via_subquery_join_s83r");
    });
}

#[test]
fn test_conformance_insert_from_join_agg_s83s() {
    asupersync::test_utils::run_test(|| async {
        let fconn = Connection::open(":memory:").await.unwrap();
        let rconn = rusqlite::Connection::open_in_memory().unwrap();
        setup_five_table_schema(&fconn, &rconn).await;
        let setup2 = "CREATE TABLE region_summary (region TEXT PRIMARY KEY, total_revenue REAL)";
        fconn.execute(setup2).await.unwrap();
        rconn.execute_batch(setup2).unwrap();
        let ins = "INSERT INTO region_summary SELECT r.name, SUM(s.qty * p.base_price) FROM regions r JOIN stores st ON st.region_id = r.id JOIN sales s ON s.store_id = st.id JOIN products p ON p.id = s.product_id GROUP BY r.name";
        fconn.execute(ins).await.unwrap();
        rconn.execute_batch(ins).unwrap();
        let m = oracle_compare(
            &fconn,
            &rconn,
            &["SELECT region, total_revenue FROM region_summary ORDER BY region"],
        )
        .await;
        assert_no_mismatches(&m, "insert_from_join_agg_s83s");
    });
}

#[test]
fn test_conformance_window_rank_over_join_s83t() {
    asupersync::test_utils::run_test(|| async {
        let fconn = Connection::open(":memory:").await.unwrap();
        let rconn = rusqlite::Connection::open_in_memory().unwrap();
        setup_five_table_schema(&fconn, &rconn).await;
        let m = oracle_compare(
        &fconn,
        &rconn,
        &[
            "SELECT st.name, p.name AS product, s.qty, RANK() OVER (PARTITION BY st.id ORDER BY s.qty DESC) AS qty_rank FROM stores st JOIN sales s ON s.store_id = st.id JOIN products p ON p.id = s.product_id ORDER BY st.name, qty_rank",
        ],
    )
    .await;
        assert_no_mismatches(&m, "window_rank_over_join_s83t");
    });
}
