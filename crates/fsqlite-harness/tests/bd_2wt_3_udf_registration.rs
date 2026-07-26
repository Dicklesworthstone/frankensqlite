// bd-2wt.3: User-defined function (UDF) registration API
//
// Comprehensive test suite covering:
//   1. Scalar UDF registration and invocation via SQL
//   2. Aggregate UDF registration and invocation via GROUP BY
//   3. Window UDF registration (API surface check)
//   4. UDF overwrite (name collision replaces previous)
//   5. UDF metrics (registration counter)
//   6. Variadic UDF support
//   7. Case-insensitive function name resolution
//   8. Machine-readable conformance output
//
// All tests operate through the public Connection API.

#![allow(
    clippy::too_many_lines,
    clippy::items_after_statements,
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss,
    clippy::similar_names
)]

use fsqlite::Connection;
use fsqlite_func::{AggregateFunction, ScalarFunction, WindowFunction};
use fsqlite_types::value::SqliteValue;

// ── Helpers ───────────────────────────────────────────────────────────────

async fn open_mem() -> Connection {
    Connection::open(":memory:")
        .await
        .expect("in-memory connection")
}

async fn query_first_int(conn: &Connection, sql: &str) -> i64 {
    match conn.query(sql).await.expect("query")[0].values()[0] {
        SqliteValue::Integer(v) => v,
        ref other => panic!("expected integer, got {other:?}"),
    }
}

async fn query_first_float(conn: &Connection, sql: &str) -> f64 {
    match conn.query(sql).await.expect("query")[0].values()[0] {
        SqliteValue::Float(v) => v,
        ref other => panic!("expected float, got {other:?}"),
    }
}

async fn query_first_text(conn: &Connection, sql: &str) -> String {
    match &conn.query(sql).await.expect("query")[0].values()[0] {
        SqliteValue::Text(v) => v.to_string(),
        other => panic!("expected text, got {other:?}"),
    }
}

async fn query_ints(conn: &Connection, sql: &str) -> Vec<i64> {
    conn.query(sql)
        .await
        .unwrap_or_default()
        .iter()
        .filter_map(|r| match r.values().first() {
            Some(SqliteValue::Integer(v)) => Some(*v),
            _ => None,
        })
        .collect()
}

// ── Custom scalar UDF: double(x) → x * 2 ────────────────────────────────

struct DoubleFunc;

impl ScalarFunction for DoubleFunc {
    fn invoke(&self, args: &[SqliteValue]) -> fsqlite_error::Result<SqliteValue> {
        match &args[0] {
            SqliteValue::Integer(v) => Ok(SqliteValue::Integer(v * 2)),
            SqliteValue::Float(v) => Ok(SqliteValue::Float(v * 2.0)),
            SqliteValue::Null => Ok(SqliteValue::Null),
            other => Ok(SqliteValue::Text(format!("double({other:?})").into())),
        }
    }

    fn num_args(&self) -> i32 {
        1
    }

    fn name(&self) -> &str {
        "double"
    }

    fn is_deterministic(&self) -> bool {
        true
    }
}

// ── Custom scalar UDF: add3(a, b, c) → a + b + c ────────────────────────

struct Add3Func;

impl ScalarFunction for Add3Func {
    fn invoke(&self, args: &[SqliteValue]) -> fsqlite_error::Result<SqliteValue> {
        let mut sum = 0i64;
        for arg in args {
            match arg {
                SqliteValue::Integer(v) => sum += v,
                _ => return Ok(SqliteValue::Null),
            }
        }
        Ok(SqliteValue::Integer(sum))
    }

    fn num_args(&self) -> i32 {
        3
    }

    fn name(&self) -> &str {
        "add3"
    }
}

// ── Custom scalar UDF: greet(name) → "Hello, <name>!" ───────────────────

struct GreetFunc;

impl ScalarFunction for GreetFunc {
    fn invoke(&self, args: &[SqliteValue]) -> fsqlite_error::Result<SqliteValue> {
        match &args[0] {
            SqliteValue::Text(name) => Ok(SqliteValue::Text(format!("Hello, {name}!").into())),
            SqliteValue::Null => Ok(SqliteValue::Text("Hello, stranger!".to_string().into())),
            other => Ok(SqliteValue::Text(format!("Hello, {other:?}!").into())),
        }
    }

    fn num_args(&self) -> i32 {
        1
    }

    fn name(&self) -> &str {
        "greet"
    }
}

// ── Custom scalar UDF: triple(x) — used for overwrite test ──────────────

struct TripleFunc;

impl ScalarFunction for TripleFunc {
    fn invoke(&self, args: &[SqliteValue]) -> fsqlite_error::Result<SqliteValue> {
        match &args[0] {
            SqliteValue::Integer(v) => Ok(SqliteValue::Integer(v * 3)),
            _ => Ok(SqliteValue::Null),
        }
    }

    fn num_args(&self) -> i32 {
        1
    }

    fn name(&self) -> &str {
        "double" // Same name as DoubleFunc — used to test overwrite
    }
}

// ── Custom variadic UDF: concat_all(a, b, ...) ──────────────────────────

struct ConcatAllFunc;

impl ScalarFunction for ConcatAllFunc {
    fn invoke(&self, args: &[SqliteValue]) -> fsqlite_error::Result<SqliteValue> {
        let mut result = String::new();
        for arg in args {
            match arg {
                SqliteValue::Text(s) => result.push_str(s),
                SqliteValue::Integer(v) => result.push_str(&v.to_string()),
                SqliteValue::Float(v) => result.push_str(&v.to_string()),
                SqliteValue::Null => result.push_str("NULL"),
                SqliteValue::Blob(b) => result.push_str(&format!("[{}b]", b.len())),
            }
        }
        Ok(SqliteValue::Text(result.into()))
    }

    fn num_args(&self) -> i32 {
        -1 // variadic
    }

    fn name(&self) -> &str {
        "concat_all"
    }
}

// ── Custom aggregate UDF: product(x) → ∏x ──────────────────────────────

struct ProductAgg;

impl AggregateFunction for ProductAgg {
    type State = i64;

    fn initial_state(&self) -> Self::State {
        1
    }

    fn step(&self, state: &mut Self::State, args: &[SqliteValue]) -> fsqlite_error::Result<()> {
        if let SqliteValue::Integer(v) = &args[0] {
            *state *= v;
        }
        Ok(())
    }

    fn finalize(&self, state: Self::State) -> fsqlite_error::Result<SqliteValue> {
        Ok(SqliteValue::Integer(state))
    }

    fn num_args(&self) -> i32 {
        1
    }

    fn name(&self) -> &str {
        "product"
    }
}

// ── Custom aggregate UDF: string_agg(x) → concatenation ─────────────────

struct StringConcatAgg;

impl AggregateFunction for StringConcatAgg {
    type State = String;

    fn initial_state(&self) -> Self::State {
        String::new()
    }

    fn step(&self, state: &mut Self::State, args: &[SqliteValue]) -> fsqlite_error::Result<()> {
        if let SqliteValue::Text(v) = &args[0] {
            if !state.is_empty() {
                state.push(',');
            }
            state.push_str(v);
        }
        Ok(())
    }

    fn finalize(&self, state: Self::State) -> fsqlite_error::Result<SqliteValue> {
        Ok(SqliteValue::Text(state.into()))
    }

    fn num_args(&self) -> i32 {
        1
    }

    fn name(&self) -> &str {
        "string_concat"
    }
}

// ── Custom window UDF: running_sum(x) ────────────────────────────────────

struct RunningSumWindow;

impl WindowFunction for RunningSumWindow {
    type State = i64;

    fn initial_state(&self) -> Self::State {
        0
    }

    fn step(&self, state: &mut Self::State, args: &[SqliteValue]) -> fsqlite_error::Result<()> {
        if let SqliteValue::Integer(v) = &args[0] {
            *state += v;
        }
        Ok(())
    }

    fn inverse(&self, state: &mut Self::State, args: &[SqliteValue]) -> fsqlite_error::Result<()> {
        if let SqliteValue::Integer(v) = &args[0] {
            *state -= v;
        }
        Ok(())
    }

    fn value(&self, state: &Self::State) -> fsqlite_error::Result<SqliteValue> {
        Ok(SqliteValue::Integer(*state))
    }

    fn finalize(&self, state: Self::State) -> fsqlite_error::Result<SqliteValue> {
        Ok(SqliteValue::Integer(state))
    }

    fn num_args(&self) -> i32 {
        1
    }

    fn name(&self) -> &str {
        "running_sum"
    }
}

// ═════════════════════════════════════════════════════════════════════════
// ── Test 1: Scalar UDF registration & invocation ────────────────────────
// ═════════════════════════════════════════════════════════════════════════

#[test]
fn test_scalar_udf_registration_and_invocation() {
    asupersync::test_utils::run_test(|| async {
        let conn = open_mem().await;

        // Register double(x) UDF
        conn.register_scalar_function(DoubleFunc);

        // Invoke via expression-only SELECT
        let result = query_first_int(&conn, "SELECT double(21)").await;
        assert_eq!(result, 42, "double(21) should return 42");

        // Float argument
        let result = query_first_float(&conn, "SELECT double(1.5)").await;
        assert!(
            (result - 3.0).abs() < 1e-10,
            "double(1.5) should return 3.0"
        );

        // NULL propagation
        let rows = conn.query("SELECT double(NULL)").await.expect("query");
        assert_eq!(
            rows[0].values()[0],
            SqliteValue::Null,
            "double(NULL) should return NULL"
        );

        // Multiple-arg UDF: add3(a, b, c)
        conn.register_scalar_function(Add3Func);
        let result = query_first_int(&conn, "SELECT add3(10, 20, 12)").await;
        assert_eq!(result, 42, "add3(10, 20, 12) should return 42");

        // Text-returning UDF: greet(name)
        conn.register_scalar_function(GreetFunc);
        let result = query_first_text(&conn, "SELECT greet('World')").await;
        assert_eq!(
            result, "Hello, World!",
            "greet('World') should return 'Hello, World!'"
        );

        let result = query_first_text(&conn, "SELECT greet(NULL)").await;
        assert_eq!(
            result, "Hello, stranger!",
            "greet(NULL) should return 'Hello, stranger!'"
        );

        println!("[PASS] scalar UDF registration and invocation");
    });
}

// ═════════════════════════════════════════════════════════════════════════
// ── Test 2: UDF in table-backed queries ─────────────────────────────────
// ═════════════════════════════════════════════════════════════════════════

#[test]
fn test_udf_with_table_queries() {
    asupersync::test_utils::run_test(|| async {
        let conn = open_mem().await;
        conn.register_scalar_function(DoubleFunc);

        conn.execute("CREATE TABLE nums (val INTEGER)")
            .await
            .unwrap();
        conn.execute("INSERT INTO nums VALUES (1)").await.unwrap();
        conn.execute("INSERT INTO nums VALUES (2)").await.unwrap();
        conn.execute("INSERT INTO nums VALUES (3)").await.unwrap();
        conn.execute("INSERT INTO nums VALUES (4)").await.unwrap();
        conn.execute("INSERT INTO nums VALUES (5)").await.unwrap();

        // UDF in SELECT clause
        let results = query_ints(&conn, "SELECT double(val) FROM nums ORDER BY val").await;
        assert_eq!(
            results,
            vec![2, 4, 6, 8, 10],
            "double(val) across table rows"
        );

        // UDF in WHERE clause
        let results = query_ints(
            &conn,
            "SELECT val FROM nums WHERE double(val) > 6 ORDER BY val",
        )
        .await;
        assert_eq!(
            results,
            vec![4, 5],
            "WHERE double(val) > 6 filters correctly"
        );

        println!("[PASS] UDF with table-backed queries");
    });
}

// ═════════════════════════════════════════════════════════════════════════
// ── Test 3: Aggregate UDF registration & invocation ─────────────────────
// ═════════════════════════════════════════════════════════════════════════

#[test]
fn test_aggregate_udf_registration_and_invocation() {
    asupersync::test_utils::run_test(|| async {
        let conn = open_mem().await;
        conn.register_aggregate_function(ProductAgg);

        conn.execute("CREATE TABLE factors (grp TEXT, val INTEGER)")
            .await
            .unwrap();
        conn.execute("INSERT INTO factors VALUES ('a', 2)")
            .await
            .unwrap();
        conn.execute("INSERT INTO factors VALUES ('a', 3)")
            .await
            .unwrap();
        conn.execute("INSERT INTO factors VALUES ('a', 5)")
            .await
            .unwrap();
        conn.execute("INSERT INTO factors VALUES ('b', 7)")
            .await
            .unwrap();
        conn.execute("INSERT INTO factors VALUES ('b', 11)")
            .await
            .unwrap();

        // Aggregate over all rows
        let result =
            query_first_int(&conn, "SELECT product(val) FROM factors WHERE grp = 'a'").await;
        assert_eq!(result, 30, "product of (2,3,5) = 30");

        let result =
            query_first_int(&conn, "SELECT product(val) FROM factors WHERE grp = 'b'").await;
        assert_eq!(result, 77, "product of (7,11) = 77");

        // String concatenation aggregate
        conn.register_aggregate_function(StringConcatAgg);

        conn.execute("CREATE TABLE words (w TEXT)").await.unwrap();
        conn.execute("INSERT INTO words VALUES ('foo')")
            .await
            .unwrap();
        conn.execute("INSERT INTO words VALUES ('bar')")
            .await
            .unwrap();
        conn.execute("INSERT INTO words VALUES ('baz')")
            .await
            .unwrap();

        let result = query_first_text(&conn, "SELECT string_concat(w) FROM words").await;
        // Order may vary; just check it contains all three
        assert!(result.contains("foo"), "concat contains foo");
        assert!(result.contains("bar"), "concat contains bar");
        assert!(result.contains("baz"), "concat contains baz");

        println!("[PASS] aggregate UDF registration and invocation");
    });
}

// ═════════════════════════════════════════════════════════════════════════
// ── Test 4: Window UDF registration (API surface) ───────────────────────
// ═════════════════════════════════════════════════════════════════════════

#[test]
fn test_window_udf_registration() {
    asupersync::test_utils::run_test(|| async {
        let conn = open_mem().await;

        // Register window function — verifies the API compiles and doesn't panic
        conn.register_window_function(RunningSumWindow);

        // The window function is registered; verify it doesn't break normal queries
        let result = query_first_int(&conn, "SELECT 1 + 1").await;
        assert_eq!(
            result, 2,
            "connection still works after window UDF registration"
        );

        println!("[PASS] window UDF registration (API surface)");
    });
}

// ═════════════════════════════════════════════════════════════════════════
// ── Test 5: UDF overwrite (name collision) ──────────────────────────────
// ═════════════════════════════════════════════════════════════════════════

#[test]
fn test_udf_overwrite() {
    asupersync::test_utils::run_test(|| async {
        let conn = open_mem().await;

        // Register double(x) = x * 2
        conn.register_scalar_function(DoubleFunc);
        let result = query_first_int(&conn, "SELECT double(10)").await;
        assert_eq!(result, 20, "double(10) = 20 (original)");

        // Overwrite with triple(x) = x * 3 (same function name "double")
        conn.register_scalar_function(TripleFunc);
        let result = query_first_int(&conn, "SELECT double(10)").await;
        assert_eq!(result, 30, "double(10) = 30 (after overwrite with triple)");

        println!("[PASS] UDF overwrite (name collision)");
    });
}

// ═════════════════════════════════════════════════════════════════════════
// ── Test 6: UDF metrics ─────────────────────────────────────────────────
// ═════════════════════════════════════════════════════════════════════════

#[test]
fn test_udf_metrics() {
    asupersync::test_utils::run_test(|| async {
        let before = fsqlite_func::udf_registered_count();

        let conn = open_mem().await;
        conn.register_scalar_function(DoubleFunc);
        let delta = fsqlite_func::udf_registered_count() - before;
        assert!(
            delta >= 1,
            "expected at least 1 registration after scalar UDF, got delta={delta}"
        );

        let before2 = fsqlite_func::udf_registered_count();
        conn.register_aggregate_function(ProductAgg);
        let delta2 = fsqlite_func::udf_registered_count() - before2;
        assert!(
            delta2 >= 1,
            "expected at least 1 registration after aggregate UDF, got delta={delta2}"
        );

        let before3 = fsqlite_func::udf_registered_count();
        conn.register_window_function(RunningSumWindow);
        let delta3 = fsqlite_func::udf_registered_count() - before3;
        assert!(
            delta3 >= 1,
            "expected at least 1 registration after window UDF, got delta={delta3}"
        );

        // Overwrite counts as another registration
        let before4 = fsqlite_func::udf_registered_count();
        conn.register_scalar_function(TripleFunc);
        let delta4 = fsqlite_func::udf_registered_count() - before4;
        assert!(
            delta4 >= 1,
            "expected at least 1 registration (overwrite counts), got delta={delta4}"
        );

        // Overall: 4 registrations in this test
        let total_delta = fsqlite_func::udf_registered_count() - before;
        assert!(
            total_delta >= 4,
            "expected at least 4 total registrations, got delta={total_delta}"
        );

        println!("[PASS] UDF metrics");
    });
}

// ═════════════════════════════════════════════════════════════════════════
// ── Test 7: Variadic UDF ────────────────────────────────────────────────
// ═════════════════════════════════════════════════════════════════════════

#[test]
fn test_variadic_udf() {
    asupersync::test_utils::run_test(|| async {
        let conn = open_mem().await;
        conn.register_scalar_function(ConcatAllFunc);

        // 2 args
        let result = query_first_text(&conn, "SELECT concat_all('hello', ' world')").await;
        assert_eq!(result, "hello world", "concat_all with 2 text args");

        // 3 args with mixed types
        let result = query_first_text(&conn, "SELECT concat_all('n=', 42)").await;
        assert_eq!(result, "n=42", "concat_all with text + int");

        println!("[PASS] variadic UDF");
    });
}

// ═════════════════════════════════════════════════════════════════════════
// ── Test 8: Case-insensitive function name resolution ────────────────────
// ═════════════════════════════════════════════════════════════════════════

#[test]
fn test_case_insensitive_udf_name() {
    asupersync::test_utils::run_test(|| async {
        let conn = open_mem().await;
        conn.register_scalar_function(DoubleFunc);

        // Function registered as "double" — should be callable as DOUBLE, Double, etc.
        let r1 = query_first_int(&conn, "SELECT double(5)").await;
        let r2 = query_first_int(&conn, "SELECT DOUBLE(5)").await;
        let r3 = query_first_int(&conn, "SELECT Double(5)").await;
        let r4 = query_first_int(&conn, "SELECT dOuBlE(5)").await;

        assert_eq!(r1, 10);
        assert_eq!(r2, 10);
        assert_eq!(r3, 10);
        assert_eq!(r4, 10);

        println!("[PASS] case-insensitive UDF name resolution");
    });
}

// ═════════════════════════════════════════════════════════════════════════
// ── Test 9: Conformance summary (JSON) ──────────────────────────────────
// ═════════════════════════════════════════════════════════════════════════

#[test]
fn test_conformance_summary() {
    asupersync::test_utils::run_test(|| async {
        #[derive(Debug)]
        struct TestCase {
            name: &'static str,
            pass: bool,
        }

        let conn = open_mem().await;
        let mut cases = Vec::new();

        // 1. Scalar registration
        conn.register_scalar_function(DoubleFunc);
        let v = query_first_int(&conn, "SELECT double(7)").await;
        cases.push(TestCase {
            name: "scalar_register_invoke",
            pass: v == 14,
        });

        // 2. Multi-arg scalar
        conn.register_scalar_function(Add3Func);
        let v = query_first_int(&conn, "SELECT add3(1, 2, 3)").await;
        cases.push(TestCase {
            name: "multi_arg_scalar",
            pass: v == 6,
        });

        // 3. Text-returning scalar
        conn.register_scalar_function(GreetFunc);
        let v = query_first_text(&conn, "SELECT greet('UDF')").await;
        cases.push(TestCase {
            name: "text_returning_scalar",
            pass: v == "Hello, UDF!",
        });

        // 4. NULL propagation
        let rows = conn.query("SELECT double(NULL)").await.expect("query");
        cases.push(TestCase {
            name: "null_propagation",
            pass: rows[0].values()[0] == SqliteValue::Null,
        });

        // 5. Aggregate registration
        conn.register_aggregate_function(ProductAgg);
        conn.execute("CREATE TABLE agg_test (v INTEGER)")
            .await
            .unwrap();
        conn.execute("INSERT INTO agg_test VALUES (2)")
            .await
            .unwrap();
        conn.execute("INSERT INTO agg_test VALUES (3)")
            .await
            .unwrap();
        conn.execute("INSERT INTO agg_test VALUES (7)")
            .await
            .unwrap();
        let v = query_first_int(&conn, "SELECT product(v) FROM agg_test").await;
        cases.push(TestCase {
            name: "aggregate_register_invoke",
            pass: v == 42,
        });

        // 6. Window registration (API)
        conn.register_window_function(RunningSumWindow);
        let v = query_first_int(&conn, "SELECT 1").await;
        cases.push(TestCase {
            name: "window_register_api",
            pass: v == 1,
        });

        // 7. UDF overwrite
        conn.register_scalar_function(TripleFunc); // overwrites "double"
        let v = query_first_int(&conn, "SELECT double(10)").await;
        cases.push(TestCase {
            name: "udf_overwrite",
            pass: v == 30,
        });

        // 8. Case-insensitive name
        let v = query_first_int(&conn, "SELECT DOUBLE(10)").await;
        cases.push(TestCase {
            name: "case_insensitive",
            pass: v == 30,
        });

        // 9. UDF in table query
        conn.register_scalar_function(DoubleFunc); // re-register original
        conn.execute("CREATE TABLE tbl (x INTEGER)").await.unwrap();
        conn.execute("INSERT INTO tbl VALUES (5)").await.unwrap();
        let v = query_first_int(&conn, "SELECT double(x) FROM tbl").await;
        cases.push(TestCase {
            name: "udf_in_table_query",
            pass: v == 10,
        });

        // 10. UDF in WHERE
        conn.execute("INSERT INTO tbl VALUES (10)").await.unwrap();
        let vals = query_ints(&conn, "SELECT x FROM tbl WHERE double(x) >= 20 ORDER BY x").await;
        cases.push(TestCase {
            name: "udf_in_where",
            pass: vals == vec![10],
        });

        // Summary
        let total = cases.len();
        let passed = cases.iter().filter(|c| c.pass).count();
        let failed = total - passed;

        println!("\n=== bd-2wt.3: UDF Registration Conformance Summary ===");
        println!("{{");
        println!("  \"bead\": \"bd-2wt.3\",");
        println!("  \"suite\": \"udf_registration\",");
        println!("  \"total\": {total},");
        println!("  \"passed\": {passed},");
        println!("  \"failed\": {failed},");
        println!(
            "  \"pass_rate\": \"{:.1}%\",",
            passed as f64 / total as f64 * 100.0
        );
        println!("  \"cases\": [");
        for (i, c) in cases.iter().enumerate() {
            let comma = if i + 1 < total { "," } else { "" };
            let status = if c.pass { "PASS" } else { "FAIL" };
            println!(
                "    {{ \"name\": \"{}\", \"status\": \"{status}\" }}{comma}",
                c.name
            );
        }
        println!("  ]");
        println!("}}");

        assert_eq!(
            failed,
            0,
            "{failed}/{total} UDF conformance tests failed: {:?}",
            cases
                .iter()
                .filter(|c| !c.pass)
                .map(|c| c.name)
                .collect::<Vec<_>>()
        );

        println!("[PASS] all {total} UDF conformance tests passed");
    });
}
