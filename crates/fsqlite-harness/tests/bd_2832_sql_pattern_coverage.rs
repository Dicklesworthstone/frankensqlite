//! bd-2832: Public API integration tests for common SQL patterns.
//!
//! Each test exercises a specific SQL feature through `Connection::query`/`execute`.
//! Features that return `NotImplemented` are documented as skipped.
//!
//! Now that ScanCtx is threaded through emit_expr (commit c664162) and
//! aggregate support is landed, most features work correctly.

use fsqlite::Connection;
use fsqlite_types::value::SqliteValue;

/// Helper: open an in-memory connection and seed it with a standard test table.
async fn seeded_conn() -> Connection {
    let conn = Connection::open(":memory:").await.unwrap();
    conn.execute("CREATE TABLE t (id INTEGER, name TEXT, score REAL, flag INTEGER);")
        .await
        .unwrap();
    conn.execute("INSERT INTO t VALUES (1, 'alice', 95.5, 1);")
        .await
        .unwrap();
    conn.execute("INSERT INTO t VALUES (2, 'bob', 87.3, 0);")
        .await
        .unwrap();
    conn.execute("INSERT INTO t VALUES (3, 'carol', 92.1, 1);")
        .await
        .unwrap();
    conn.execute("INSERT INTO t VALUES (4, 'dave', 78.9, 0);")
        .await
        .unwrap();
    conn.execute("INSERT INTO t VALUES (5, 'eve', 99.0, 1);")
        .await
        .unwrap();
    conn
}

fn int(row: &fsqlite::Row, col: usize) -> i64 {
    match row.get(col) {
        Some(SqliteValue::Integer(n)) => *n,
        other => unreachable!("expected Integer at col {col}, got {other:?}"),
    }
}

fn text(row: &fsqlite::Row, col: usize) -> String {
    match row.get(col) {
        Some(SqliteValue::Text(s)) => s.to_string(),
        other => unreachable!("expected Text at col {col}, got {other:?}"),
    }
}

fn float(row: &fsqlite::Row, col: usize) -> f64 {
    match row.get(col) {
        Some(SqliteValue::Float(f)) => *f,
        other => unreachable!("expected Float at col {col}, got {other:?}"),
    }
}

fn is_null(row: &fsqlite::Row, col: usize) -> bool {
    matches!(row.get(col), Some(SqliteValue::Null))
}

// ── Compound WHERE predicates ───────────────────────────────────────────────

#[test]
fn test_where_and() {
    asupersync::test_utils::run_test(|| async {
        let conn = seeded_conn().await;
        let rows = conn
            .query("SELECT id FROM t WHERE flag = 1 AND score > 93.0;")
            .await
            .unwrap();
        let ids: Vec<i64> = rows.iter().map(|r| int(r, 0)).collect();
        assert!(ids.contains(&1)); // alice: 95.5, flag=1
        assert!(ids.contains(&5)); // eve: 99.0, flag=1
        assert!(!ids.contains(&3)); // carol: 92.1, flag=1 but score < 93
    });
}

#[test]
fn test_where_or() {
    asupersync::test_utils::run_test(|| async {
        let conn = seeded_conn().await;
        let rows = conn
            .query("SELECT id FROM t WHERE id = 1 OR id = 5;")
            .await
            .unwrap();
        assert_eq!(rows.len(), 2);
        let ids: Vec<i64> = rows.iter().map(|r| int(r, 0)).collect();
        assert!(ids.contains(&1));
        assert!(ids.contains(&5));
    });
}

#[test]
fn test_where_not() {
    asupersync::test_utils::run_test(|| async {
        let conn = seeded_conn().await;
        let rows = conn
            .query("SELECT id FROM t WHERE NOT flag = 1;")
            .await
            .unwrap();
        let ids: Vec<i64> = rows.iter().map(|r| int(r, 0)).collect();
        assert_eq!(ids.len(), 2);
        assert!(ids.contains(&2)); // bob
        assert!(ids.contains(&4)); // dave
    });
}

// ── Comparison operators ────────────────────────────────────────────────────

#[test]
fn test_comparison_greater_than() {
    asupersync::test_utils::run_test(|| async {
        let conn = seeded_conn().await;
        let rows = conn
            .query("SELECT id FROM t WHERE score > 92.0;")
            .await
            .unwrap();
        let ids: Vec<i64> = rows.iter().map(|r| int(r, 0)).collect();
        assert!(ids.contains(&1)); // 95.5
        assert!(ids.contains(&3)); // 92.1
        assert!(ids.contains(&5)); // 99.0
    });
}

#[test]
fn test_comparison_less_than_equal() {
    asupersync::test_utils::run_test(|| async {
        let conn = seeded_conn().await;
        let rows = conn
            .query("SELECT id FROM t WHERE score <= 87.3;")
            .await
            .unwrap();
        let ids: Vec<i64> = rows.iter().map(|r| int(r, 0)).collect();
        assert!(ids.contains(&2)); // 87.3
        assert!(ids.contains(&4)); // 78.9
    });
}

#[test]
fn test_comparison_not_equal() {
    asupersync::test_utils::run_test(|| async {
        let conn = seeded_conn().await;
        let rows = conn.query("SELECT id FROM t WHERE id != 3;").await.unwrap();
        assert_eq!(rows.len(), 4);
    });
}

// ── NULL handling ───────────────────────────────────────────────────────────

#[test]
fn test_null_is_null() {
    asupersync::test_utils::run_test(|| async {
        let conn = Connection::open(":memory:").await.unwrap();
        conn.execute("CREATE TABLE n (a INTEGER, b TEXT);")
            .await
            .unwrap();
        conn.execute("INSERT INTO n VALUES (1, 'hello');")
            .await
            .unwrap();
        conn.execute("INSERT INTO n VALUES (2, NULL);")
            .await
            .unwrap();
        conn.execute("INSERT INTO n VALUES (NULL, 'world');")
            .await
            .unwrap();

        let rows = conn
            .query("SELECT a FROM n WHERE b IS NULL;")
            .await
            .unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(int(&rows[0], 0), 2);
    });
}

#[test]
fn test_null_is_not_null() {
    asupersync::test_utils::run_test(|| async {
        let conn = Connection::open(":memory:").await.unwrap();
        conn.execute("CREATE TABLE n (a INTEGER, b TEXT);")
            .await
            .unwrap();
        conn.execute("INSERT INTO n VALUES (1, 'hello');")
            .await
            .unwrap();
        conn.execute("INSERT INTO n VALUES (2, NULL);")
            .await
            .unwrap();

        let rows = conn
            .query("SELECT a FROM n WHERE b IS NOT NULL;")
            .await
            .unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(int(&rows[0], 0), 1);
    });
}

#[test]
fn test_null_coalesce() {
    asupersync::test_utils::run_test(|| async {
        let conn = Connection::open(":memory:").await.unwrap();
        conn.execute("CREATE TABLE n (a INTEGER, b INTEGER);")
            .await
            .unwrap();
        conn.execute("INSERT INTO n VALUES (1, NULL);")
            .await
            .unwrap();
        conn.execute("INSERT INTO n VALUES (2, 42);").await.unwrap();

        let rows = conn.query("SELECT COALESCE(b, -1) FROM n;").await.unwrap();
        assert_eq!(rows.len(), 2);
        // Row with NULL b should get -1, row with 42 should keep 42.
        let vals: Vec<i64> = rows.iter().map(|r| int(r, 0)).collect();
        assert!(vals.contains(&-1));
        assert!(vals.contains(&42));
    });
}

#[test]
fn test_null_nullif() {
    asupersync::test_utils::run_test(|| async {
        let conn = Connection::open(":memory:").await.unwrap();
        let rows = conn.query("SELECT NULLIF(1, 1);").await.unwrap();
        assert_eq!(rows.len(), 1);
        assert!(is_null(&rows[0], 0));

        let rows = conn.query("SELECT NULLIF(1, 2);").await.unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(int(&rows[0], 0), 1);
    });
}

// ── Expression columns ──────────────────────────────────────────────────────

#[test]
fn test_expression_arithmetic() {
    asupersync::test_utils::run_test(|| async {
        let conn = seeded_conn().await;
        let rows = conn
            .query("SELECT id, score + 10.0 FROM t WHERE id = 1;")
            .await
            .unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(int(&rows[0], 0), 1);
        let boosted = float(&rows[0], 1);
        assert!((boosted - 105.5).abs() < 0.001);
    });
}

#[test]
fn test_expression_string_concat() {
    asupersync::test_utils::run_test(|| async {
        let conn = Connection::open(":memory:").await.unwrap();
        let rows = conn
            .query("SELECT 'hello' || ' ' || 'world';")
            .await
            .unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(text(&rows[0], 0), "hello world");
    });
}

// ── CASE WHEN ───────────────────────────────────────────────────────────────

#[test]
fn test_case_when_simple() {
    asupersync::test_utils::run_test(|| async {
        let conn = seeded_conn().await;
        let rows = conn
            .query("SELECT id, CASE WHEN flag = 1 THEN 'yes' ELSE 'no' END FROM t WHERE id <= 2;")
            .await
            .unwrap();
        assert_eq!(rows.len(), 2);
        // id=1, flag=1 → 'yes'; id=2, flag=0 → 'no'
        for row in &rows {
            let id = int(row, 0);
            let val = text(row, 1);
            match id {
                1 => assert_eq!(val, "yes"),
                2 => assert_eq!(val, "no"),
                _ => unreachable!("unexpected id {id}"),
            }
        }
    });
}

// ── Built-in functions ──────────────────────────────────────────────────────

#[test]
fn test_builtin_abs() {
    asupersync::test_utils::run_test(|| async {
        let conn = Connection::open(":memory:").await.unwrap();
        let rows = conn.query("SELECT ABS(-42);").await.unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(int(&rows[0], 0), 42);
    });
}

#[test]
fn test_builtin_length() {
    asupersync::test_utils::run_test(|| async {
        let conn = Connection::open(":memory:").await.unwrap();
        let rows = conn.query("SELECT LENGTH('hello');").await.unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(int(&rows[0], 0), 5);
    });
}

#[test]
fn test_builtin_upper_lower() {
    asupersync::test_utils::run_test(|| async {
        let conn = Connection::open(":memory:").await.unwrap();
        let rows = conn
            .query("SELECT UPPER('hello'), LOWER('WORLD');")
            .await
            .unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(text(&rows[0], 0), "HELLO");
        assert_eq!(text(&rows[0], 1), "world");
    });
}

#[test]
fn test_builtin_typeof() {
    asupersync::test_utils::run_test(|| async {
        let conn = Connection::open(":memory:").await.unwrap();
        let rows = conn
            .query("SELECT TYPEOF(42), TYPEOF(3.14), TYPEOF('hi'), TYPEOF(NULL);")
            .await
            .unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(text(&rows[0], 0), "integer");
        assert_eq!(text(&rows[0], 1), "real");
        assert_eq!(text(&rows[0], 2), "text");
        assert_eq!(text(&rows[0], 3), "null");
    });
}

// ── Aggregate functions ─────────────────────────────────────────────────────

#[test]
fn test_aggregate_count() {
    asupersync::test_utils::run_test(|| async {
        let conn = seeded_conn().await;
        let rows = conn.query("SELECT COUNT(*) FROM t;").await.unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(int(&rows[0], 0), 5);
    });
}

#[test]
fn test_aggregate_sum() {
    asupersync::test_utils::run_test(|| async {
        let conn = Connection::open(":memory:").await.unwrap();
        conn.execute("CREATE TABLE nums (v INTEGER);")
            .await
            .unwrap();
        conn.execute("INSERT INTO nums VALUES (10);").await.unwrap();
        conn.execute("INSERT INTO nums VALUES (20);").await.unwrap();
        conn.execute("INSERT INTO nums VALUES (30);").await.unwrap();

        let rows = conn.query("SELECT SUM(v) FROM nums;").await.unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(int(&rows[0], 0), 60);
    });
}

#[test]
fn test_aggregate_min_max() {
    asupersync::test_utils::run_test(|| async {
        let conn = seeded_conn().await;
        let rows = conn
            .query("SELECT MIN(score), MAX(score) FROM t;")
            .await
            .unwrap();
        assert_eq!(rows.len(), 1);
        let min_score = float(&rows[0], 0);
        let max_score = float(&rows[0], 1);
        assert!((min_score - 78.9).abs() < 0.001);
        assert!((max_score - 99.0).abs() < 0.001);
    });
}

#[test]
fn test_aggregate_avg() {
    asupersync::test_utils::run_test(|| async {
        let conn = Connection::open(":memory:").await.unwrap();
        conn.execute("CREATE TABLE nums (v INTEGER);")
            .await
            .unwrap();
        conn.execute("INSERT INTO nums VALUES (10);").await.unwrap();
        conn.execute("INSERT INTO nums VALUES (20);").await.unwrap();
        conn.execute("INSERT INTO nums VALUES (30);").await.unwrap();

        let rows = conn.query("SELECT AVG(v) FROM nums;").await.unwrap();
        assert_eq!(rows.len(), 1);
        let avg = float(&rows[0], 0);
        assert!((avg - 20.0).abs() < 0.001);
    });
}

#[test]
fn test_aggregate_count_with_group_by() {
    asupersync::test_utils::run_test(|| async {
        let conn = seeded_conn().await;
        let rows = conn
            .query("SELECT flag, COUNT(*) FROM t GROUP BY flag;")
            .await
            .unwrap();
        assert_eq!(rows.len(), 2);
        for row in &rows {
            let flag = int(row, 0);
            let count = int(row, 1);
            match flag {
                0 => assert_eq!(count, 2),
                1 => assert_eq!(count, 3),
                _ => unreachable!("unexpected flag {flag}"),
            }
        }
    });
}

// ── DISTINCT ────────────────────────────────────────────────────────────────

#[test]
fn test_distinct() {
    asupersync::test_utils::run_test(|| async {
        let conn = seeded_conn().await;
        let rows = conn.query("SELECT DISTINCT flag FROM t;").await.unwrap();
        assert_eq!(rows.len(), 2);
        let vals: Vec<i64> = rows.iter().map(|r| int(r, 0)).collect();
        assert!(vals.contains(&0));
        assert!(vals.contains(&1));
    });
}

// ── LIKE pattern matching ───────────────────────────────────────────────────

#[test]
fn test_like_percent() {
    asupersync::test_utils::run_test(|| async {
        let conn = seeded_conn().await;
        let rows = conn
            .query("SELECT name FROM t WHERE name LIKE 'a%';")
            .await
            .unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(text(&rows[0], 0), "alice");
    });
}

#[test]
fn test_like_underscore() {
    asupersync::test_utils::run_test(|| async {
        let conn = seeded_conn().await;
        let rows = conn
            .query("SELECT name FROM t WHERE name LIKE '_ve';")
            .await
            .unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(text(&rows[0], 0), "eve");
    });
}

// ── BETWEEN / IN ────────────────────────────────────────────────────────────

#[test]
fn test_between() {
    asupersync::test_utils::run_test(|| async {
        let conn = seeded_conn().await;
        let rows = conn
            .query("SELECT id FROM t WHERE score BETWEEN 85.0 AND 95.0;")
            .await
            .unwrap();
        let ids: Vec<i64> = rows.iter().map(|r| int(r, 0)).collect();
        assert!(ids.contains(&2)); // 87.3
        assert!(ids.contains(&3)); // 92.1
        // 95.5 is > 95.0 so alice excluded; 78.9 is < 85.0 so dave excluded
        assert!(!ids.contains(&4));
    });
}

#[test]
fn test_in_list() {
    asupersync::test_utils::run_test(|| async {
        let conn = seeded_conn().await;
        let rows = conn
            .query("SELECT name FROM t WHERE id IN (1, 3, 5);")
            .await
            .unwrap();
        assert_eq!(rows.len(), 3);
        let names: Vec<String> = rows.iter().map(|r| text(r, 0)).collect();
        assert!(names.contains(&"alice".to_owned()));
        assert!(names.contains(&"carol".to_owned()));
        assert!(names.contains(&"eve".to_owned()));
    });
}

// ── Multi-row INSERT ────────────────────────────────────────────────────────

#[test]
fn test_multi_row_insert() {
    asupersync::test_utils::run_test(|| async {
        let conn = Connection::open(":memory:").await.unwrap();
        conn.execute("CREATE TABLE m (x INTEGER);").await.unwrap();
        // SQLite supports INSERT INTO ... VALUES (...), (...), (...)
        conn.execute("INSERT INTO m VALUES (1), (2), (3);")
            .await
            .unwrap();
        // Verify all three rows were inserted.
        let rows = conn.query("SELECT x FROM m ORDER BY x;").await.unwrap();
        assert_eq!(rows.len(), 3);
        assert_eq!(int(&rows[0], 0), 1);
        assert_eq!(int(&rows[1], 0), 2);
        assert_eq!(int(&rows[2], 0), 3);
    });
}

// ── PreparedStatement (table-backed SELECT) ───────────────────────────────

#[test]
fn test_prepared_select_from_table_uses_connection_state() {
    asupersync::test_utils::run_test(|| async {
        let conn = Connection::open(":memory:").await.unwrap();
        conn.execute("CREATE TABLE p (v INTEGER);").await.unwrap();
        conn.execute("INSERT INTO p VALUES (1), (2);")
            .await
            .unwrap();

        let stmt = conn.prepare("SELECT v FROM p ORDER BY v;").await.unwrap();
        let rows = stmt.query().await.unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(int(&rows[0], 0), 1);
        assert_eq!(int(&rows[1], 0), 2);
    });
}

// ── CAST expressions ────────────────────────────────────────────────────────

#[test]
fn test_cast_integer_to_text() {
    asupersync::test_utils::run_test(|| async {
        let conn = Connection::open(":memory:").await.unwrap();
        let rows = conn.query("SELECT CAST(42 AS TEXT);").await.unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(text(&rows[0], 0), "42");
    });
}

#[test]
fn test_cast_text_to_integer() {
    asupersync::test_utils::run_test(|| async {
        let conn = Connection::open(":memory:").await.unwrap();
        let rows = conn.query("SELECT CAST('123' AS INTEGER);").await.unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(int(&rows[0], 0), 123);
    });
}

// ── ORDER BY ────────────────────────────────────────────────────────────────

#[test]
fn test_order_by_asc() {
    asupersync::test_utils::run_test(|| async {
        let conn = seeded_conn().await;
        let rows = conn
            .query("SELECT id FROM t ORDER BY score;")
            .await
            .unwrap();
        assert_eq!(rows.len(), 5);
        // Ascending by score: dave(78.9), bob(87.3), carol(92.1), alice(95.5), eve(99.0)
        assert_eq!(int(&rows[0], 0), 4);
        assert_eq!(int(&rows[4], 0), 5);
    });
}

#[test]
fn test_order_by_desc() {
    asupersync::test_utils::run_test(|| async {
        let conn = seeded_conn().await;
        let rows = conn
            .query("SELECT id FROM t ORDER BY score DESC;")
            .await
            .unwrap();
        assert_eq!(rows.len(), 5);
        // Descending by score: eve(99.0), alice(95.5), carol(92.1), bob(87.3), dave(78.9)
        assert_eq!(int(&rows[0], 0), 5);
        assert_eq!(int(&rows[4], 0), 4);
    });
}

// ── LIMIT / OFFSET ──────────────────────────────────────────────────────────

#[test]
fn test_limit() {
    asupersync::test_utils::run_test(|| async {
        let conn = seeded_conn().await;
        let rows = conn
            .query("SELECT id FROM t ORDER BY id LIMIT 3;")
            .await
            .unwrap();
        assert_eq!(rows.len(), 3);
        assert_eq!(int(&rows[0], 0), 1);
        assert_eq!(int(&rows[2], 0), 3);
    });
}

#[test]
fn test_limit_offset() {
    asupersync::test_utils::run_test(|| async {
        let conn = seeded_conn().await;
        let rows = conn
            .query("SELECT id FROM t ORDER BY id LIMIT 2 OFFSET 2;")
            .await
            .unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(int(&rows[0], 0), 3);
        assert_eq!(int(&rows[1], 0), 4);
    });
}

// ── Parameterized queries ───────────────────────────────────────────────────

#[test]
fn test_parameterized_query() {
    asupersync::test_utils::run_test(|| async {
        let conn = seeded_conn().await;
        let rows = conn
            .query_with_params(
                "SELECT name FROM t WHERE id = ?;",
                &[SqliteValue::Integer(3)],
            )
            .await
            .unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(text(&rows[0], 0), "carol");
    });
}

// ── E2E combined ────────────────────────────────────────────────────────────

#[test]
fn test_e2e_bd_2832_sql_pattern_coverage() {
    asupersync::test_utils::run_test(|| async {
        let conn = seeded_conn().await;

        // Complex query combining multiple features.
        let rows = conn
            .query(
                "SELECT name, CASE WHEN score >= 90.0 THEN 'A' ELSE 'B' END \
                 FROM t \
                 WHERE flag = 1 AND score > 80.0 \
                 ORDER BY score DESC;",
            )
            .await
            .unwrap();

        // flag=1 AND score>80: alice(95.5), carol(92.1), eve(99.0) — 3 rows
        assert_eq!(rows.len(), 3);
        // Ordered DESC: eve, alice, carol
        assert_eq!(text(&rows[0], 0), "eve");
        assert_eq!(text(&rows[0], 1), "A");
        assert_eq!(text(&rows[1], 0), "alice");
        assert_eq!(text(&rows[1], 1), "A");
        assert_eq!(text(&rows[2], 0), "carol");
        assert_eq!(text(&rows[2], 1), "A");
    });
}
