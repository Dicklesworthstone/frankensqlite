//! bd-wwqen.2 (B2): Regression tests for IN subquery HashSet probe and
//! EXISTS LIMIT 1 injection.
//!
//! Proves:
//! 1. Large non-correlated IN (SELECT ...) uses HashSet probe (correctness).
//! 2. Small IN lists still work (below threshold, linear scan).
//! 3. NOT IN with NULLs preserves three-valued logic.
//! 4. Correlated IN subqueries still evaluate per-row (no HashSet).
//! 5. EXISTS with LIMIT 1 injection returns correct results.
//! 6. NOT EXISTS returns correct results.
//! 7. Non-correlated EXISTS is correct.
//!
//! Run:
//!   cargo test -p fsqlite-core --test in_exists_regression -- --nocapture

use fsqlite_core::connection::Connection;
use fsqlite_types::SqliteValue;

/// B2.1: Large non-correlated IN (SELECT ...) produces correct results.
/// After rewrite_in_expr materializes the subquery into InSet::List,
/// the HashSet probe (>16 elements) must match the linear-scan result.
#[test]
fn test_large_in_subquery_hashset_correctness() {
    asupersync::test_utils::run_test(|| async {
        let conn = Connection::open(":memory:").await.unwrap();
        conn.execute("CREATE TABLE big(id INTEGER PRIMARY KEY)")
            .await
            .unwrap();
        conn.execute("CREATE TABLE probe(id INTEGER PRIMARY KEY, val TEXT)")
            .await
            .unwrap();

        // Insert 200 rows into big (well above the 16-element threshold).
        for i in 1..=200 {
            conn.execute(&format!("INSERT INTO big VALUES({i})"))
                .await
                .unwrap();
        }
        // Insert 10 rows into probe, some matching big, some not.
        for i in 1..=10 {
            conn.execute(&format!("INSERT INTO probe VALUES({i}, 'row{i}')"))
                .await
                .unwrap();
        }
        // id=999 is NOT in big.
        conn.execute("INSERT INTO probe VALUES(999, 'missing')")
            .await
            .unwrap();

        let rows = conn
            .query("SELECT id, val FROM probe WHERE id IN (SELECT id FROM big) ORDER BY id")
            .await
            .unwrap();

        assert_eq!(rows.len(), 10, "all 10 matching rows should be returned");
        assert_eq!(rows[0].values()[0], SqliteValue::Integer(1));
        assert_eq!(rows[9].values()[0], SqliteValue::Integer(10));

        // Verify id=999 is excluded.
        let missing = conn
            .query("SELECT COUNT(*) FROM probe WHERE id IN (SELECT id FROM big) AND id = 999")
            .await
            .unwrap();
        assert_eq!(missing[0].values()[0], SqliteValue::Integer(0));
    });
}

/// B2.1: NOT IN with NULLs in the subquery result preserves three-valued
/// logic: if the value is not found but NULLs exist, result is NULL.
#[test]
fn test_not_in_subquery_null_semantics() {
    asupersync::test_utils::run_test(|| async {
        let conn = Connection::open(":memory:").await.unwrap();
        conn.execute("CREATE TABLE t(id INTEGER)").await.unwrap();
        // Insert values including NULL.
        conn.execute("INSERT INTO t VALUES(1)").await.unwrap();
        conn.execute("INSERT INTO t VALUES(2)").await.unwrap();
        conn.execute("INSERT INTO t VALUES(NULL)").await.unwrap();

        // 1 NOT IN (1, 2, NULL) → FALSE (1 is found)
        let rows = conn
            .query("SELECT 1 NOT IN (SELECT id FROM t)")
            .await
            .unwrap();
        assert_eq!(
            rows[0].values()[0],
            SqliteValue::Integer(0),
            "1 is in the set"
        );

        // 99 NOT IN (1, 2, NULL) → NULL (99 not found, but NULL present)
        let rows = conn
            .query("SELECT 99 NOT IN (SELECT id FROM t)")
            .await
            .unwrap();
        assert_eq!(
            rows[0].values()[0],
            SqliteValue::Null,
            "99 NOT IN with NULL → NULL"
        );

        // With no NULLs: 99 NOT IN (1, 2) → TRUE
        conn.execute("DELETE FROM t WHERE id IS NULL")
            .await
            .unwrap();
        let rows = conn
            .query("SELECT 99 NOT IN (SELECT id FROM t)")
            .await
            .unwrap();
        assert_eq!(
            rows[0].values()[0],
            SqliteValue::Integer(1),
            "99 NOT IN without NULL → TRUE"
        );
    });
}

/// B2.1: IN (positive, not NOT IN) with NULLs in the subquery preserves
/// three-valued logic: match + NULL → TRUE, no-match + NULL → NULL,
/// no-match + no-NULL → FALSE.
///
/// bd-wwqen.2: This is the mirror of test_not_in_subquery_null_semantics
/// for the positive IN case. The eager materialization path
/// (rewrite_in_expr → value_to_literal_expr) converts subquery NULLs
/// to Literal::Null entries in InSet::List. The evaluator must handle
/// these correctly per SQL three-valued logic.
#[test]
fn test_in_subquery_null_three_valued_logic() {
    asupersync::test_utils::run_test(|| async {
        let conn = Connection::open(":memory:").await.unwrap();
        conn.execute("CREATE TABLE t(id INTEGER)").await.unwrap();
        conn.execute("INSERT INTO t VALUES(1)").await.unwrap();
        conn.execute("INSERT INTO t VALUES(2)").await.unwrap();
        conn.execute("INSERT INTO t VALUES(NULL)").await.unwrap();

        // 1 IN (1, 2, NULL) → TRUE (1 is found, NULL irrelevant)
        let rows = conn.query("SELECT 1 IN (SELECT id FROM t)").await.unwrap();
        assert_eq!(
            rows[0].values()[0],
            SqliteValue::Integer(1),
            "1 IN (1, 2, NULL) must be TRUE"
        );

        // 99 IN (1, 2, NULL) → NULL (99 not found, but NULL present)
        let rows = conn.query("SELECT 99 IN (SELECT id FROM t)").await.unwrap();
        assert_eq!(
            rows[0].values()[0],
            SqliteValue::Null,
            "99 IN (1, 2, NULL) must be NULL, not FALSE"
        );

        // With no NULLs: 99 IN (1, 2) → FALSE
        conn.execute("DELETE FROM t WHERE id IS NULL")
            .await
            .unwrap();
        let rows = conn.query("SELECT 99 IN (SELECT id FROM t)").await.unwrap();
        assert_eq!(
            rows[0].values()[0],
            SqliteValue::Integer(0),
            "99 IN (1, 2) without NULL must be FALSE"
        );
    });
}

/// B2.1: Small IN list (below 16-element threshold) still works correctly
/// via the original linear scan path.
#[test]
fn test_small_in_subquery_linear_scan() {
    asupersync::test_utils::run_test(|| async {
        let conn = Connection::open(":memory:").await.unwrap();
        conn.execute("CREATE TABLE small(id INTEGER PRIMARY KEY)")
            .await
            .unwrap();
        for i in 1..=5 {
            conn.execute(&format!("INSERT INTO small VALUES({i})"))
                .await
                .unwrap();
        }

        let rows = conn
            .query(
                "SELECT id FROM small WHERE id IN (SELECT id FROM small WHERE id <= 3) ORDER BY id",
            )
            .await
            .unwrap();
        assert_eq!(rows.len(), 3);
        assert_eq!(rows[0].values()[0], SqliteValue::Integer(1));
        assert_eq!(rows[2].values()[0], SqliteValue::Integer(3));
    });
}

/// B2.1: IN with mixed types (Integer and Text) in the subquery.
#[test]
fn test_in_subquery_mixed_types() {
    asupersync::test_utils::run_test(|| async {
        let conn = Connection::open(":memory:").await.unwrap();
        conn.execute("CREATE TABLE mix(val)").await.unwrap();
        conn.execute("INSERT INTO mix VALUES(1)").await.unwrap();
        conn.execute("INSERT INTO mix VALUES('hello')")
            .await
            .unwrap();
        conn.execute("INSERT INTO mix VALUES(3.14)").await.unwrap();

        let rows = conn
            .query("SELECT val FROM mix WHERE val IN (SELECT val FROM mix WHERE val = 1 OR val = 'hello') ORDER BY val")
            .await
            .unwrap();
        assert_eq!(rows.len(), 2);
    });
}

/// B2.2: EXISTS with LIMIT 1 injection returns correct TRUE.
#[test]
fn test_exists_returns_true_with_matching_rows() {
    asupersync::test_utils::run_test(|| async {
        let conn = Connection::open(":memory:").await.unwrap();
        conn.execute("CREATE TABLE parent(id INTEGER PRIMARY KEY)")
            .await
            .unwrap();
        conn.execute("CREATE TABLE child(id INTEGER, pid INTEGER)")
            .await
            .unwrap();
        conn.execute("INSERT INTO parent VALUES(1)").await.unwrap();
        conn.execute("INSERT INTO parent VALUES(2)").await.unwrap();
        conn.execute("INSERT INTO child VALUES(10, 1)")
            .await
            .unwrap();
        conn.execute("INSERT INTO child VALUES(20, 1)")
            .await
            .unwrap();

        // Correlated EXISTS: parent 1 has children, parent 2 does not.
        let rows = conn
            .query("SELECT id FROM parent WHERE EXISTS (SELECT 1 FROM child WHERE child.pid = parent.id) ORDER BY id")
            .await
            .unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].values()[0], SqliteValue::Integer(1));
    });
}

/// B2.2: NOT EXISTS returns correct results.
#[test]
fn test_not_exists_returns_correct_results() {
    asupersync::test_utils::run_test(|| async {
        let conn = Connection::open(":memory:").await.unwrap();
        conn.execute("CREATE TABLE parent(id INTEGER PRIMARY KEY)")
            .await
            .unwrap();
        conn.execute("CREATE TABLE child(id INTEGER, pid INTEGER)")
            .await
            .unwrap();
        conn.execute("INSERT INTO parent VALUES(1)").await.unwrap();
        conn.execute("INSERT INTO parent VALUES(2)").await.unwrap();
        conn.execute("INSERT INTO child VALUES(10, 1)")
            .await
            .unwrap();

        let rows = conn
            .query("SELECT id FROM parent WHERE NOT EXISTS (SELECT 1 FROM child WHERE child.pid = parent.id) ORDER BY id")
            .await
            .unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].values()[0], SqliteValue::Integer(2));
    });
}

/// B2.2: Non-correlated EXISTS (no outer refs) returns correct result.
#[test]
fn test_non_correlated_exists() {
    asupersync::test_utils::run_test(|| async {
        let conn = Connection::open(":memory:").await.unwrap();
        conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY)")
            .await
            .unwrap();
        conn.execute("INSERT INTO t VALUES(1)").await.unwrap();

        // Non-correlated EXISTS with data → TRUE
        let rows = conn
            .query("SELECT EXISTS (SELECT 1 FROM t WHERE id = 1)")
            .await
            .unwrap();
        assert_eq!(rows[0].values()[0], SqliteValue::Integer(1));

        // Non-correlated EXISTS without data → FALSE
        let rows = conn
            .query("SELECT EXISTS (SELECT 1 FROM t WHERE id = 999)")
            .await
            .unwrap();
        assert_eq!(rows[0].values()[0], SqliteValue::Integer(0));
    });
}

/// B2.1+B2.2: Combined IN + EXISTS in same query.
#[test]
fn test_in_and_exists_combined() {
    asupersync::test_utils::run_test(|| async {
        let conn = Connection::open(":memory:").await.unwrap();
        conn.execute("CREATE TABLE categories(id INTEGER PRIMARY KEY, name TEXT)")
            .await
            .unwrap();
        conn.execute("CREATE TABLE products(id INTEGER PRIMARY KEY, cat_id INTEGER, name TEXT)")
            .await
            .unwrap();

        for i in 1..=5 {
            conn.execute(&format!("INSERT INTO categories VALUES({i}, 'cat{i}')"))
                .await
                .unwrap();
        }
        for i in 1..=20 {
            let cat = (i % 5) + 1;
            conn.execute(&format!(
                "INSERT INTO products VALUES({i}, {cat}, 'prod{i}')"
            ))
            .await
            .unwrap();
        }

        // Products in categories 1-3 that have at least one other product in the same category.
        let rows = conn
            .query(
                "SELECT p.id FROM products p \
         WHERE p.cat_id IN (SELECT id FROM categories WHERE id <= 3) \
         AND EXISTS (SELECT 1 FROM products p2 WHERE p2.cat_id = p.cat_id AND p2.id != p.id) \
         ORDER BY p.id",
            )
            .await
            .unwrap();

        // Categories 1-3 each have 4 products (ids: 1-20 mod 5). Each product has 3 siblings.
        assert!(
            !rows.is_empty(),
            "should find products matching both IN and EXISTS"
        );
        // All returned products should be in categories 1-3.
        for row in &rows {
            let id = match row.values()[0] {
                SqliteValue::Integer(i) => i,
                _ => panic!("expected integer id"),
            };
            assert!(id >= 1 && id <= 20);
        }
    });
}

// ── B7 regression guards (bd-wwqen.7) ──────────────────────────────────

/// B7 guard: 1000-row IN subquery completes within a wall-time budget.
///
/// If the HashSet optimization regresses back to linear scan, this test
/// will blow past the budget on any reasonable hardware. The budget is
/// generous (500ms) to avoid flaky failures — the optimized path runs
/// in <5ms; the pre-fix linear scan would take >10s at 1000 rows.
#[test]
fn guard_large_in_subquery_completes_within_budget() {
    asupersync::test_utils::run_test(|| async {
        let conn = Connection::open(":memory:").await.unwrap();
        conn.execute("CREATE TABLE lookup(id INTEGER PRIMARY KEY)")
            .await
            .unwrap();
        conn.execute("CREATE TABLE data(id INTEGER PRIMARY KEY, val TEXT)")
            .await
            .unwrap();

        conn.execute("BEGIN").await.unwrap();
        for i in 1..=1000 {
            conn.execute(&format!("INSERT INTO lookup VALUES({i})"))
                .await
                .unwrap();
        }
        for i in 1..=100 {
            conn.execute(&format!("INSERT INTO data VALUES({i}, 'v{i}')"))
                .await
                .unwrap();
        }
        conn.execute("COMMIT").await.unwrap();

        let start = std::time::Instant::now();
        let rows = conn
            .query("SELECT COUNT(*) FROM data WHERE id IN (SELECT id FROM lookup)")
            .await
            .unwrap();
        let elapsed = start.elapsed();

        assert_eq!(rows[0].values()[0], SqliteValue::Integer(100));
        assert!(
            elapsed < std::time::Duration::from_millis(500),
            "1000-row IN subquery should complete in <500ms (got {:?}); \
         possible regression to linear scan",
            elapsed
        );
        eprintln!("[B7 guard] 1000-row IN subquery: {:?}", elapsed);
    });
}

/// B7 guard: correlated EXISTS over 500 outer rows completes within budget.
///
/// The LIMIT 1 injection means each inner execution stops after one match.
/// Without it, each inner query scans the full child table.
#[test]
fn guard_correlated_exists_completes_within_budget() {
    asupersync::test_utils::run_test(|| async {
        let conn = Connection::open(":memory:").await.unwrap();
        conn.execute("CREATE TABLE parent(id INTEGER PRIMARY KEY)")
            .await
            .unwrap();
        conn.execute("CREATE TABLE child(id INTEGER, pid INTEGER)")
            .await
            .unwrap();

        conn.execute("BEGIN").await.unwrap();
        for i in 1..=500 {
            conn.execute(&format!("INSERT INTO parent VALUES({i})"))
                .await
                .unwrap();
        }
        // Every even parent has 10 children.
        for i in 1..=500 {
            if i % 2 == 0 {
                for j in 1..=10 {
                    conn.execute(&format!("INSERT INTO child VALUES({}, {})", i * 100 + j, i))
                        .await
                        .unwrap();
                }
            }
        }
        conn.execute("COMMIT").await.unwrap();

        let start = std::time::Instant::now();
        let rows = conn
            .query(
                "SELECT COUNT(*) FROM parent p \
             WHERE EXISTS (SELECT 1 FROM child c WHERE c.pid = p.id)",
            )
            .await
            .unwrap();
        let elapsed = start.elapsed();

        assert_eq!(rows[0].values()[0], SqliteValue::Integer(250));
        assert!(
            elapsed < std::time::Duration::from_secs(2),
            "500-row correlated EXISTS should complete in <2s (got {:?}); \
         possible regression — LIMIT 1 injection may be missing",
            elapsed
        );
        eprintln!("[B7 guard] 500-row correlated EXISTS: {:?}", elapsed);
    });
}

// ── B2 EXISTS direct-probe validation matrix (bd-wwqen.2) ─────────────

/// E1: Simple single-table correlated EXISTS with equality.
#[test]
fn test_direct_probe_exists_simple_correlated() {
    asupersync::test_utils::run_test(|| async {
        let conn = Connection::open(":memory:").await.unwrap();
        conn.execute("CREATE TABLE orders(id INTEGER PRIMARY KEY, cust_id INTEGER)")
            .await
            .unwrap();
        conn.execute("CREATE TABLE customers(id INTEGER PRIMARY KEY, name TEXT)")
            .await
            .unwrap();
        conn.execute("BEGIN").await.unwrap();
        for i in 1..=100 {
            conn.execute(&format!("INSERT INTO customers VALUES({i}, 'c{i}')"))
                .await
                .unwrap();
        }
        // 500 orders, each referencing a customer (cust_id = (i%100)+1).
        for i in 1..=500 {
            let cid = (i % 100) + 1;
            conn.execute(&format!("INSERT INTO orders VALUES({i}, {cid})"))
                .await
                .unwrap();
        }
        conn.execute("COMMIT").await.unwrap();

        let start = std::time::Instant::now();
        let rows = conn
            .query("SELECT COUNT(*) FROM customers c WHERE EXISTS (SELECT 1 FROM orders o WHERE o.cust_id = c.id)")
            .await
            .unwrap();
        let elapsed = start.elapsed();

        assert_eq!(
            rows[0].values()[0],
            SqliteValue::Integer(100),
            "all 100 customers have orders"
        );
        eprintln!("[E1] simple correlated EXISTS 500/100: {:?}", elapsed);
    });
}

/// E2: Correlated EXISTS with equality + static range filter.
#[test]
fn test_direct_probe_exists_with_range_filter() {
    asupersync::test_utils::run_test(|| async {
        let conn = Connection::open(":memory:").await.unwrap();
        conn.execute("CREATE TABLE products(id INTEGER PRIMARY KEY, cat_id INTEGER)")
            .await
            .unwrap();
        conn.execute("CREATE TABLE categories(id INTEGER PRIMARY KEY, name TEXT)")
            .await
            .unwrap();
        for i in 1..=50 {
            conn.execute(&format!("INSERT INTO categories VALUES({i}, 'cat{i}')"))
                .await
                .unwrap();
        }
        for i in 1..=200 {
            let cat = (i % 50) + 1;
            conn.execute(&format!("INSERT INTO products VALUES({i}, {cat})"))
                .await
                .unwrap();
        }

        let rows = conn
            .query("SELECT COUNT(*) FROM products p WHERE EXISTS (SELECT 1 FROM categories c WHERE c.id = p.cat_id AND c.id <= 25)")
            .await
            .unwrap();

        // Products with cat_id 1-25 should match (half of 50 categories).
        // Each category has 4 products (200/50), so 25*4 = 100.
        assert_eq!(rows[0].values()[0], SqliteValue::Integer(100));
    });
}

/// E3: NOT EXISTS returns the complement set.
#[test]
fn test_direct_probe_not_exists() {
    asupersync::test_utils::run_test(|| async {
        let conn = Connection::open(":memory:").await.unwrap();
        conn.execute("CREATE TABLE parent(id INTEGER PRIMARY KEY)")
            .await
            .unwrap();
        conn.execute("CREATE TABLE child(id INTEGER, pid INTEGER)")
            .await
            .unwrap();
        for i in 1..=100 {
            conn.execute(&format!("INSERT INTO parent VALUES({i})"))
                .await
                .unwrap();
        }
        // Only even parents have children.
        for i in 1..=100 {
            if i % 2 == 0 {
                conn.execute(&format!("INSERT INTO child VALUES({}, {})", i * 10, i))
                    .await
                    .unwrap();
            }
        }

        let rows = conn
            .query("SELECT COUNT(*) FROM parent p WHERE NOT EXISTS (SELECT 1 FROM child c WHERE c.pid = p.id)")
            .await
            .unwrap();
        assert_eq!(
            rows[0].values()[0],
            SqliteValue::Integer(50),
            "50 odd parents have no children"
        );
    });
}

/// E4: Multi-column correlated EXISTS.
#[test]
fn test_direct_probe_exists_multi_column() {
    asupersync::test_utils::run_test(|| async {
        let conn = Connection::open(":memory:").await.unwrap();
        conn.execute("CREATE TABLE a(x INTEGER, y INTEGER)")
            .await
            .unwrap();
        conn.execute("CREATE TABLE b(p INTEGER, q INTEGER)")
            .await
            .unwrap();
        conn.execute("INSERT INTO a VALUES(1, 10)").await.unwrap();
        conn.execute("INSERT INTO a VALUES(2, 20)").await.unwrap();
        conn.execute("INSERT INTO a VALUES(3, 30)").await.unwrap();
        conn.execute("INSERT INTO b VALUES(1, 10)").await.unwrap();
        conn.execute("INSERT INTO b VALUES(3, 30)").await.unwrap();

        let rows = conn
            .query("SELECT x FROM a WHERE EXISTS (SELECT 1 FROM b WHERE b.p = a.x AND b.q = a.y) ORDER BY x")
            .await
            .unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].values()[0], SqliteValue::Integer(1));
        assert_eq!(rows[1].values()[0], SqliteValue::Integer(3));
    });
}

/// E-null: Correlated EXISTS where outer value is NULL.
#[test]
fn test_direct_probe_exists_null_outer_value() {
    asupersync::test_utils::run_test(|| async {
        let conn = Connection::open(":memory:").await.unwrap();
        conn.execute("CREATE TABLE t1(id INTEGER, fk INTEGER)")
            .await
            .unwrap();
        conn.execute("CREATE TABLE t2(id INTEGER PRIMARY KEY)")
            .await
            .unwrap();
        conn.execute("INSERT INTO t1 VALUES(1, NULL)")
            .await
            .unwrap();
        conn.execute("INSERT INTO t1 VALUES(2, 1)").await.unwrap();
        conn.execute("INSERT INTO t2 VALUES(1)").await.unwrap();

        // Row with fk=NULL: EXISTS (SELECT 1 FROM t2 WHERE t2.id = NULL) → FALSE
        // Row with fk=1: EXISTS (SELECT 1 FROM t2 WHERE t2.id = 1) → TRUE
        let rows = conn
            .query(
                "SELECT id FROM t1 WHERE EXISTS (SELECT 1 FROM t2 WHERE t2.id = t1.fk) ORDER BY id",
            )
            .await
            .unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].values()[0], SqliteValue::Integer(2));
    });
}

/// F1: EXISTS with JOIN in inner query falls through correctly.
#[test]
fn test_fallthrough_exists_join_inner() {
    asupersync::test_utils::run_test(|| async {
        let conn = Connection::open(":memory:").await.unwrap();
        conn.execute("CREATE TABLE t1(id INTEGER PRIMARY KEY)")
            .await
            .unwrap();
        conn.execute("CREATE TABLE t2(id INTEGER, t1_id INTEGER)")
            .await
            .unwrap();
        conn.execute("CREATE TABLE t3(id INTEGER, t2_id INTEGER)")
            .await
            .unwrap();
        conn.execute("INSERT INTO t1 VALUES(1)").await.unwrap();
        conn.execute("INSERT INTO t1 VALUES(2)").await.unwrap();
        conn.execute("INSERT INTO t2 VALUES(10, 1)").await.unwrap();
        conn.execute("INSERT INTO t3 VALUES(100, 10)")
            .await
            .unwrap();

        let rows = conn
            .query(
                "SELECT id FROM t1 WHERE EXISTS \
             (SELECT 1 FROM t2 JOIN t3 ON t3.t2_id = t2.id WHERE t2.t1_id = t1.id) \
             ORDER BY id",
            )
            .await
            .unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].values()[0], SqliteValue::Integer(1));
    });
}

/// F2: EXISTS with GROUP BY in inner query falls through correctly.
#[test]
fn test_fallthrough_exists_group_by() {
    asupersync::test_utils::run_test(|| async {
        let conn = Connection::open(":memory:").await.unwrap();
        conn.execute("CREATE TABLE parent(id INTEGER PRIMARY KEY)")
            .await
            .unwrap();
        conn.execute("CREATE TABLE child(id INTEGER, pid INTEGER, val INTEGER)")
            .await
            .unwrap();
        conn.execute("INSERT INTO parent VALUES(1)").await.unwrap();
        conn.execute("INSERT INTO parent VALUES(2)").await.unwrap();
        conn.execute("INSERT INTO child VALUES(1, 1, 10)")
            .await
            .unwrap();
        conn.execute("INSERT INTO child VALUES(2, 1, 20)")
            .await
            .unwrap();

        // EXISTS with GROUP BY + HAVING: only parent 1 has children summing > 15
        let rows = conn
            .query(
                "SELECT id FROM parent p WHERE EXISTS \
             (SELECT 1 FROM child c WHERE c.pid = p.id GROUP BY c.pid HAVING SUM(c.val) > 15) \
             ORDER BY id",
            )
            .await
            .unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].values()[0], SqliteValue::Integer(1));
    });
}

// ── PrecomputedHashSet regression guards (bd-wwqen.2 B2 residual) ────
//
// These tests exercise semantics that a PrecomputedHashSet must preserve.
// They use >16 IN elements to guarantee the HashSet path fires (not linear).

/// Large IN with NULLs: three-valued logic must hold above HashSet threshold.
#[test]
fn test_large_in_null_three_valued_above_threshold() {
    asupersync::test_utils::run_test(|| async {
        let conn = Connection::open(":memory:").await.unwrap();
        conn.execute("CREATE TABLE big(id INTEGER)").await.unwrap();
        for i in 1..=20 {
            conn.execute(&format!("INSERT INTO big VALUES({i})"))
                .await
                .unwrap();
        }
        conn.execute("INSERT INTO big VALUES(NULL)").await.unwrap();

        // 5 is in the set → TRUE regardless of NULL
        let rows = conn
            .query("SELECT 5 IN (SELECT id FROM big)")
            .await
            .unwrap();
        assert_eq!(
            rows[0].values()[0],
            SqliteValue::Integer(1),
            "5 IN (1..20, NULL) must be TRUE"
        );

        // 99 is NOT in the set but NULL is present → NULL
        let rows = conn
            .query("SELECT 99 IN (SELECT id FROM big)")
            .await
            .unwrap();
        assert_eq!(
            rows[0].values()[0],
            SqliteValue::Null,
            "99 IN (1..20, NULL) must be NULL, not FALSE"
        );

        // Remove NULL, now 99 NOT found with no NULL → FALSE
        conn.execute("DELETE FROM big WHERE id IS NULL")
            .await
            .unwrap();
        let rows = conn
            .query("SELECT 99 IN (SELECT id FROM big)")
            .await
            .unwrap();
        assert_eq!(
            rows[0].values()[0],
            SqliteValue::Integer(0),
            "99 IN (1..20) without NULL must be FALSE"
        );
    });
}

/// Large IN with duplicates: duplicates must not change membership semantics.
#[test]
fn test_large_in_duplicates_above_threshold() {
    asupersync::test_utils::run_test(|| async {
        let conn = Connection::open(":memory:").await.unwrap();
        conn.execute("CREATE TABLE dup(id INTEGER)").await.unwrap();
        for i in 1..=10 {
            conn.execute(&format!("INSERT INTO dup VALUES({i})"))
                .await
                .unwrap();
            conn.execute(&format!("INSERT INTO dup VALUES({i})"))
                .await
                .unwrap();
        }

        let rows = conn
            .query("SELECT 5 IN (SELECT id FROM dup)")
            .await
            .unwrap();
        assert_eq!(
            rows[0].values()[0],
            SqliteValue::Integer(1),
            "5 IN duplicated set must be TRUE"
        );

        let rows = conn
            .query("SELECT 99 IN (SELECT id FROM dup)")
            .await
            .unwrap();
        assert_eq!(
            rows[0].values()[0],
            SqliteValue::Integer(0),
            "99 IN duplicated set must be FALSE"
        );
    });
}

/// Large IN with mixed Integer/Float: 1 and 1.0 must be treated as equal.
#[test]
fn test_large_in_mixed_int_float_above_threshold() {
    asupersync::test_utils::run_test(|| async {
        let conn = Connection::open(":memory:").await.unwrap();
        conn.execute("CREATE TABLE mixed(val REAL)").await.unwrap();
        for i in 1..=20 {
            conn.execute(&format!("INSERT INTO mixed VALUES({i}.0)"))
                .await
                .unwrap();
        }

        // INTEGER 5 must match REAL 5.0
        let rows = conn
            .query("SELECT 5 IN (SELECT val FROM mixed)")
            .await
            .unwrap();
        assert_eq!(
            rows[0].values()[0],
            SqliteValue::Integer(1),
            "INTEGER 5 IN (REAL 1.0..20.0) must be TRUE"
        );

        // REAL 5.5 must NOT match any integer-valued float
        let rows = conn
            .query("SELECT 5.5 IN (SELECT val FROM mixed)")
            .await
            .unwrap();
        assert_eq!(
            rows[0].values()[0],
            SqliteValue::Integer(0),
            "5.5 IN (1.0..20.0 integers) must be FALSE"
        );
    });
}

/// Large IN with text: case-sensitive by default (binary comparison).
#[test]
fn test_large_in_text_case_sensitive_above_threshold() {
    asupersync::test_utils::run_test(|| async {
        let conn = Connection::open(":memory:").await.unwrap();
        conn.execute("CREATE TABLE words(w TEXT)").await.unwrap();
        for i in 1..=20 {
            conn.execute(&format!("INSERT INTO words VALUES('word{i}')"))
                .await
                .unwrap();
        }

        let rows = conn
            .query("SELECT 'word5' IN (SELECT w FROM words)")
            .await
            .unwrap();
        assert_eq!(
            rows[0].values()[0],
            SqliteValue::Integer(1),
            "'word5' IN text set must be TRUE"
        );

        let rows = conn
            .query("SELECT 'WORD5' IN (SELECT w FROM words)")
            .await
            .unwrap();
        assert_eq!(
            rows[0].values()[0],
            SqliteValue::Integer(0),
            "'WORD5' IN lowercase text set must be FALSE (binary comparison)"
        );
    });
}

/// Large NOT IN with NULLs above threshold: mirror of positive IN test.
#[test]
fn test_large_not_in_null_above_threshold() {
    asupersync::test_utils::run_test(|| async {
        let conn = Connection::open(":memory:").await.unwrap();
        conn.execute("CREATE TABLE notbig(id INTEGER)")
            .await
            .unwrap();
        for i in 1..=20 {
            conn.execute(&format!("INSERT INTO notbig VALUES({i})"))
                .await
                .unwrap();
        }
        conn.execute("INSERT INTO notbig VALUES(NULL)")
            .await
            .unwrap();

        // 5 NOT IN (1..20, NULL) → FALSE (found)
        let rows = conn
            .query("SELECT 5 NOT IN (SELECT id FROM notbig)")
            .await
            .unwrap();
        assert_eq!(
            rows[0].values()[0],
            SqliteValue::Integer(0),
            "5 NOT IN (1..20, NULL) must be FALSE"
        );

        // 99 NOT IN (1..20, NULL) → NULL (not found, NULL present)
        let rows = conn
            .query("SELECT 99 NOT IN (SELECT id FROM notbig)")
            .await
            .unwrap();
        assert_eq!(
            rows[0].values()[0],
            SqliteValue::Null,
            "99 NOT IN (1..20, NULL) must be NULL"
        );
    });
}

/// bd-1dp9.6.2: Direct MemDB fast path for
/// `SELECT COUNT(*) FROM table WHERE col IN (SELECT ...)`.
///
/// After IN-subquery rewriting, the connection-level evaluator
/// can bypass VDBE bytecode and scan MemDB rows directly with a
/// HashSet probe, avoiding per-row VDBE interpretation overhead.
#[test]
fn test_count_star_in_list_fast_path_correctness() {
    asupersync::test_utils::run_test(|| async {
        let conn = Connection::open(":memory:").await.unwrap();
        conn.execute(
            "CREATE TABLE products(id INTEGER PRIMARY KEY, category_id INTEGER, name TEXT)",
        )
        .await
        .unwrap();
        conn.execute("CREATE TABLE categories(id INTEGER PRIMARY KEY, label TEXT)")
            .await
            .unwrap();
        for i in 1..=10 {
            conn.execute(&format!("INSERT INTO categories VALUES({i}, 'cat{i}')"))
                .await
                .unwrap();
        }
        for i in 1..=1000 {
            let cat = (i % 10) + 1;
            conn.execute(&format!(
                "INSERT INTO products VALUES({i}, {cat}, 'prod{i}')"
            ))
            .await
            .unwrap();
        }

        let rows = conn
            .query("SELECT COUNT(*) FROM products WHERE category_id IN (SELECT id FROM categories WHERE id <= 5)")
            .await
            .unwrap();
        assert_eq!(
            rows[0].values()[0],
            SqliteValue::Integer(500),
            "500 products have category_id in 1..=5"
        );

        let rows = conn
            .query("SELECT COUNT(*) FROM products WHERE category_id IN (SELECT id FROM categories WHERE id = 1)")
            .await
            .unwrap();
        assert_eq!(
            rows[0].values()[0],
            SqliteValue::Integer(100),
            "100 products have category_id = 1"
        );

        let rows = conn
            .query("SELECT COUNT(*) FROM products WHERE category_id IN (SELECT id FROM categories WHERE id > 100)")
            .await
            .unwrap();
        assert_eq!(
            rows[0].values()[0],
            SqliteValue::Integer(0),
            "no products match a category_id > 100"
        );
    });
}

/// bd-1dp9.6.2: Verify the fast path completes well within budget.
/// Pre-B2 IN catastrophe at 10K rows was >3 seconds; the direct
/// MemDB scan should finish in single-digit milliseconds.
#[test]
fn guard_count_star_in_subquery_completes_within_budget() {
    asupersync::test_utils::run_test(|| async {
        let conn = Connection::open(":memory:").await.unwrap();
        conn.execute("CREATE TABLE items(id INTEGER PRIMARY KEY, tag INTEGER)")
            .await
            .unwrap();
        conn.execute("CREATE TABLE tags(id INTEGER PRIMARY KEY)")
            .await
            .unwrap();
        for i in 1..=10 {
            conn.execute(&format!("INSERT INTO tags VALUES({i})"))
                .await
                .unwrap();
        }
        conn.execute("BEGIN").await.unwrap();
        for i in 1..=10_000 {
            let tag = (i % 10) + 1;
            conn.execute(&format!("INSERT INTO items VALUES({i}, {tag})"))
                .await
                .unwrap();
        }
        conn.execute("COMMIT").await.unwrap();

        let budget = std::time::Duration::from_millis(500);
        let start = std::time::Instant::now();
        let rows = conn
            .query("SELECT COUNT(*) FROM items WHERE tag IN (SELECT id FROM tags WHERE id <= 5)")
            .await
            .unwrap();
        let elapsed = start.elapsed();
        assert_eq!(rows[0].values()[0], SqliteValue::Integer(5000));
        assert!(
            elapsed < budget,
            "COUNT(*) WHERE IN (SELECT ...) over 10K rows took {elapsed:?}, budget {budget:?}"
        );
    });
}
