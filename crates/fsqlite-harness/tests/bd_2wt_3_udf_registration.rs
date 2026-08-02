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
//   9. Cross-kind arity precedence and replacement parity with SQLite
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
use fsqlite_func::collation::CollationFunction;
use fsqlite_func::{AggregateFunction, ScalarFunction, WindowFunction};
use fsqlite_types::value::SqliteValue;
use std::sync::{Arc, Mutex};

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

async fn query_integer_column(conn: &Connection, sql: &str) -> Vec<i64> {
    conn.query(sql)
        .await
        .expect("integer-column query")
        .iter()
        .map(|row| match row.values().first() {
            Some(SqliteValue::Integer(value)) => *value,
            other => panic!("expected an integer first column, got {other:?}"),
        })
        .collect()
}

async fn seed_precedence_values(conn: &Connection) {
    conn.execute("CREATE TABLE udf_precedence_values (v INTEGER NOT NULL)")
        .await
        .expect("create precedence table");
    conn.execute("INSERT INTO udf_precedence_values VALUES (1), (2), (3)")
        .await
        .expect("seed precedence table");
}

async fn seed_ordered_values(conn: &Connection) {
    conn.execute(
        "CREATE TABLE udf_ordered_values (\
             grp INTEGER NOT NULL, v INTEGER NOT NULL, \
             marker INTEGER NOT NULL, ord INTEGER NOT NULL\
         )",
    )
    .await
    .expect("create ordered-aggregate table");
    conn.execute(
        "INSERT INTO udf_ordered_values VALUES \
             (1, 3, 9, 30), (1, 1, 7, 10), (1, 2, 8, 20)",
    )
    .await
    .expect("seed ordered-aggregate table");
    conn.execute("CREATE TABLE udf_ordered_anchor (id INTEGER NOT NULL)")
        .await
        .expect("create ordered-aggregate join anchor");
    conn.execute("INSERT INTO udf_ordered_anchor VALUES (1)")
        .await
        .expect("seed ordered-aggregate join anchor");
}

fn sqlite_builtin_aggregate_values() -> (i64, i64) {
    let conn = rusqlite::Connection::open_in_memory().expect("open SQLite oracle");
    conn.execute_batch(
        "CREATE TABLE udf_precedence_values (v INTEGER NOT NULL);\
         INSERT INTO udf_precedence_values VALUES (1), (2), (3);",
    )
    .expect("seed SQLite oracle");
    let sum = conn
        .query_row("SELECT sum(v) FROM udf_precedence_values", [], |row| {
            row.get::<_, i64>(0)
        })
        .expect("query SQLite sum oracle");
    let max = conn
        .query_row("SELECT max(v) FROM udf_precedence_values", [], |row| {
            row.get::<_, i64>(0)
        })
        .expect("query SQLite max oracle");
    (sum, max)
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

// ── Tagged UDFs for cross-kind resolution tests ─────────────────────────

struct TaggedScalar {
    name: &'static str,
    num_args: i32,
    tag: i64,
}

impl ScalarFunction for TaggedScalar {
    fn invoke(&self, args: &[SqliteValue]) -> fsqlite_error::Result<SqliteValue> {
        let argument_sum = args.iter().map(SqliteValue::to_integer).sum::<i64>();
        Ok(SqliteValue::Integer(self.tag + argument_sum))
    }

    fn num_args(&self) -> i32 {
        self.num_args
    }

    fn name(&self) -> &str {
        self.name
    }
}

struct PositionalTaggedScalar {
    name: &'static str,
    tag: i64,
}

impl ScalarFunction for PositionalTaggedScalar {
    fn invoke(&self, args: &[SqliteValue]) -> fsqlite_error::Result<SqliteValue> {
        let first = args.first().map_or(0, SqliteValue::to_integer);
        let second = args.get(1).map_or(0, SqliteValue::to_integer);
        Ok(SqliteValue::Integer(self.tag + first * 100 + second))
    }

    fn num_args(&self) -> i32 {
        2
    }

    fn name(&self) -> &str {
        self.name
    }
}

struct BoundedTaggedScalar {
    name: &'static str,
    min_args: i32,
    max_args: i32,
    tag: i64,
}

impl ScalarFunction for BoundedTaggedScalar {
    fn invoke(&self, args: &[SqliteValue]) -> fsqlite_error::Result<SqliteValue> {
        let argument_sum = args.iter().map(SqliteValue::to_integer).sum::<i64>();
        Ok(SqliteValue::Integer(self.tag + argument_sum))
    }

    fn num_args(&self) -> i32 {
        -1
    }

    fn min_args(&self) -> i32 {
        self.min_args
    }

    fn max_args(&self) -> Option<i32> {
        Some(self.max_args)
    }

    fn name(&self) -> &str {
        self.name
    }
}

struct TaggedAggregate {
    name: &'static str,
    num_args: i32,
    tag: i64,
}

impl AggregateFunction for TaggedAggregate {
    type State = i64;

    fn initial_state(&self) -> Self::State {
        0
    }

    fn step(&self, state: &mut Self::State, _args: &[SqliteValue]) -> fsqlite_error::Result<()> {
        *state += 1;
        Ok(())
    }

    fn finalize(&self, state: Self::State) -> fsqlite_error::Result<SqliteValue> {
        Ok(SqliteValue::Integer(self.tag + state))
    }

    fn num_args(&self) -> i32 {
        self.num_args
    }

    fn name(&self) -> &str {
        self.name
    }
}

struct TaggedWindow {
    name: &'static str,
    num_args: i32,
    tag: i64,
}

impl WindowFunction for TaggedWindow {
    type State = i64;

    fn initial_state(&self) -> Self::State {
        0
    }

    fn step(&self, state: &mut Self::State, args: &[SqliteValue]) -> fsqlite_error::Result<()> {
        *state += args.iter().map(SqliteValue::to_integer).sum::<i64>();
        Ok(())
    }

    fn inverse(&self, state: &mut Self::State, args: &[SqliteValue]) -> fsqlite_error::Result<()> {
        *state -= args.iter().map(SqliteValue::to_integer).sum::<i64>();
        Ok(())
    }

    fn value(&self, state: &Self::State) -> fsqlite_error::Result<SqliteValue> {
        Ok(SqliteValue::Integer(self.tag + state))
    }

    fn finalize(&self, state: Self::State) -> fsqlite_error::Result<SqliteValue> {
        Ok(SqliteValue::Integer(self.tag + state))
    }

    fn num_args(&self) -> i32 {
        self.num_args
    }

    fn name(&self) -> &str {
        self.name
    }
}

struct OrderedArgsFunction {
    name: &'static str,
}

fn ordered_argument_pair(args: &[SqliteValue]) -> fsqlite_error::Result<(i64, i64)> {
    if args.len() != 2 {
        return Err(fsqlite_error::FrankenError::function_error(format!(
            "expected two ordered aggregate arguments, got {}",
            args.len()
        )));
    }
    Ok((args[0].to_integer(), args[1].to_integer()))
}

fn fold_ordered_argument_pairs(pairs: &[(i64, i64)]) -> i64 {
    pairs.iter().fold(0, |state, (first, second)| {
        state * 100 + first * 10 + second
    })
}

impl AggregateFunction for OrderedArgsFunction {
    type State = Vec<(i64, i64)>;

    fn initial_state(&self) -> Self::State {
        Vec::new()
    }

    fn step(&self, state: &mut Self::State, args: &[SqliteValue]) -> fsqlite_error::Result<()> {
        state.push(ordered_argument_pair(args)?);
        Ok(())
    }

    fn finalize(&self, state: Self::State) -> fsqlite_error::Result<SqliteValue> {
        Ok(SqliteValue::Integer(fold_ordered_argument_pairs(&state)))
    }

    fn num_args(&self) -> i32 {
        2
    }

    fn name(&self) -> &str {
        self.name
    }
}

impl WindowFunction for OrderedArgsFunction {
    type State = Vec<(i64, i64)>;

    fn initial_state(&self) -> Self::State {
        Vec::new()
    }

    fn step(&self, state: &mut Self::State, args: &[SqliteValue]) -> fsqlite_error::Result<()> {
        state.push(ordered_argument_pair(args)?);
        Ok(())
    }

    fn inverse(&self, state: &mut Self::State, args: &[SqliteValue]) -> fsqlite_error::Result<()> {
        let pair = ordered_argument_pair(args)?;
        let position = state
            .iter()
            .position(|candidate| *candidate == pair)
            .ok_or_else(|| {
                fsqlite_error::FrankenError::function_error(
                    "ordered window inverse could not find its outgoing arguments",
                )
            })?;
        state.remove(position);
        Ok(())
    }

    fn value(&self, state: &Self::State) -> fsqlite_error::Result<SqliteValue> {
        Ok(SqliteValue::Integer(fold_ordered_argument_pairs(state)))
    }

    fn finalize(&self, state: Self::State) -> fsqlite_error::Result<SqliteValue> {
        Ok(SqliteValue::Integer(fold_ordered_argument_pairs(&state)))
    }

    fn num_args(&self) -> i32 {
        2
    }

    fn name(&self) -> &str {
        self.name
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
struct WindowLifecycleCounts {
    initial: usize,
    step: usize,
    inverse: usize,
    value: usize,
    finalize: usize,
}

struct LifecycleWindow {
    name: &'static str,
    counts: Arc<Mutex<WindowLifecycleCounts>>,
    fail_step: bool,
    fail_finalize: bool,
}

impl WindowFunction for LifecycleWindow {
    type State = i64;

    fn initial_state(&self) -> Self::State {
        self.counts.lock().unwrap().initial += 1;
        0
    }

    fn step(&self, state: &mut Self::State, args: &[SqliteValue]) -> fsqlite_error::Result<()> {
        self.counts.lock().unwrap().step += 1;
        if self.fail_step {
            return Err(fsqlite_error::FrankenError::function_error(
                "lifecycle step sentinel",
            ));
        }
        *state += args.first().map_or(0, SqliteValue::to_integer);
        Ok(())
    }

    fn inverse(&self, state: &mut Self::State, args: &[SqliteValue]) -> fsqlite_error::Result<()> {
        self.counts.lock().unwrap().inverse += 1;
        *state -= args.first().map_or(0, SqliteValue::to_integer);
        Ok(())
    }

    fn value(&self, state: &Self::State) -> fsqlite_error::Result<SqliteValue> {
        self.counts.lock().unwrap().value += 1;
        Ok(SqliteValue::Integer(*state))
    }

    fn finalize(&self, state: Self::State) -> fsqlite_error::Result<SqliteValue> {
        self.counts.lock().unwrap().finalize += 1;
        if self.fail_finalize {
            return Err(fsqlite_error::FrankenError::function_error(
                "lifecycle finalize sentinel",
            ));
        }
        Ok(SqliteValue::Integer(state))
    }

    fn num_args(&self) -> i32 {
        1
    }

    fn name(&self) -> &str {
        self.name
    }
}

struct DistinctValueFinalizeWindow {
    name: &'static str,
    counts: Arc<Mutex<WindowLifecycleCounts>>,
}

impl WindowFunction for DistinctValueFinalizeWindow {
    type State = i64;

    fn initial_state(&self) -> Self::State {
        self.counts.lock().unwrap().initial += 1;
        0
    }

    fn step(&self, state: &mut Self::State, args: &[SqliteValue]) -> fsqlite_error::Result<()> {
        self.counts.lock().unwrap().step += 1;
        *state += args.first().map_or(0, SqliteValue::to_integer);
        Ok(())
    }

    fn inverse(&self, state: &mut Self::State, args: &[SqliteValue]) -> fsqlite_error::Result<()> {
        self.counts.lock().unwrap().inverse += 1;
        *state -= args.first().map_or(0, SqliteValue::to_integer);
        Ok(())
    }

    fn value(&self, state: &Self::State) -> fsqlite_error::Result<SqliteValue> {
        self.counts.lock().unwrap().value += 1;
        Ok(SqliteValue::Integer(10_000 + state))
    }

    fn finalize(&self, state: Self::State) -> fsqlite_error::Result<SqliteValue> {
        self.counts.lock().unwrap().finalize += 1;
        Ok(SqliteValue::Integer(20_000 + state))
    }

    fn num_args(&self) -> i32 {
        1
    }

    fn name(&self) -> &str {
        self.name
    }
}

struct EventLogWindow {
    name: &'static str,
    events: Arc<Mutex<Vec<String>>>,
}

impl WindowFunction for EventLogWindow {
    type State = i64;

    fn initial_state(&self) -> Self::State {
        self.events.lock().unwrap().push("initial".to_owned());
        0
    }

    fn step(&self, state: &mut Self::State, args: &[SqliteValue]) -> fsqlite_error::Result<()> {
        let value = args.first().map_or(0, SqliteValue::to_integer);
        self.events.lock().unwrap().push(format!("step({value})"));
        *state += value;
        Ok(())
    }

    fn inverse(&self, state: &mut Self::State, args: &[SqliteValue]) -> fsqlite_error::Result<()> {
        let value = args.first().map_or(0, SqliteValue::to_integer);
        self.events
            .lock()
            .unwrap()
            .push(format!("inverse({value})"));
        *state -= value;
        Ok(())
    }

    fn value(&self, state: &Self::State) -> fsqlite_error::Result<SqliteValue> {
        self.events.lock().unwrap().push(format!("value({state})"));
        Ok(SqliteValue::Integer(*state))
    }

    fn finalize(&self, state: Self::State) -> fsqlite_error::Result<SqliteValue> {
        self.events
            .lock()
            .unwrap()
            .push(format!("finalize({state})"));
        Ok(SqliteValue::Integer(state))
    }

    fn num_args(&self) -> i32 {
        1
    }

    fn name(&self) -> &str {
        self.name
    }
}

fn snapshot_lifecycle_counts(counts: &Arc<Mutex<WindowLifecycleCounts>>) -> WindowLifecycleCounts {
    counts.lock().unwrap().clone()
}

struct RecordingWindowExpression {
    name: &'static str,
    role: &'static str,
    calls: Arc<Mutex<Vec<(&'static str, i64)>>>,
    is_filter: bool,
}

impl ScalarFunction for RecordingWindowExpression {
    fn invoke(&self, args: &[SqliteValue]) -> fsqlite_error::Result<SqliteValue> {
        let value = args.first().map_or(0, SqliteValue::to_integer);
        self.calls.lock().unwrap().push((self.role, value));
        Ok(SqliteValue::Integer(if self.is_filter {
            i64::from(value == 2)
        } else {
            value
        }))
    }

    fn num_args(&self) -> i32 {
        1
    }

    fn name(&self) -> &str {
        self.name
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
        conn.register_deterministic_scalar_function(DoubleFunc);

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
        conn.register_deterministic_scalar_function(Add3Func);
        let result = query_first_int(&conn, "SELECT add3(10, 20, 12)").await;
        assert_eq!(result, 42, "add3(10, 20, 12) should return 42");

        // Text-returning UDF: greet(name)
        conn.register_deterministic_scalar_function(GreetFunc);
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
        conn.register_deterministic_scalar_function(DoubleFunc);

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
        conn.register_deterministic_scalar_function(DoubleFunc);
        let result = query_first_int(&conn, "SELECT double(10)").await;
        assert_eq!(result, 20, "double(10) = 20 (original)");

        // Overwrite with triple(x) = x * 3 (same function name "double")
        conn.register_deterministic_scalar_function(TripleFunc);
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
        conn.register_deterministic_scalar_function(DoubleFunc);
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
        conn.register_deterministic_scalar_function(TripleFunc);
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
        conn.register_deterministic_scalar_function(ConcatAllFunc);

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
        conn.register_deterministic_scalar_function(DoubleFunc);

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
        conn.register_deterministic_scalar_function(DoubleFunc);
        let v = query_first_int(&conn, "SELECT double(7)").await;
        cases.push(TestCase {
            name: "scalar_register_invoke",
            pass: v == 14,
        });

        // 2. Multi-arg scalar
        conn.register_deterministic_scalar_function(Add3Func);
        let v = query_first_int(&conn, "SELECT add3(1, 2, 3)").await;
        cases.push(TestCase {
            name: "multi_arg_scalar",
            pass: v == 6,
        });

        // 3. Text-returning scalar
        conn.register_deterministic_scalar_function(GreetFunc);
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
        conn.register_deterministic_scalar_function(TripleFunc); // overwrites "double"
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
        conn.register_deterministic_scalar_function(DoubleFunc); // re-register original
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

// ═════════════════════════════════════════════════════════════════════════
// ── SQLite parity: cross-kind registration and arity precedence ─────────
// ═════════════════════════════════════════════════════════════════════════

#[test]
fn test_cross_kind_exact_application_precedes_variadic_application() {
    asupersync::test_utils::run_test(|| async {
        let exact_scalar = open_mem().await;
        seed_precedence_values(&exact_scalar).await;
        exact_scalar.register_aggregate_function(TaggedAggregate {
            name: "app_precedence",
            num_args: -1,
            tag: 20_000,
        });
        exact_scalar.register_deterministic_scalar_function(TaggedScalar {
            name: "app_precedence",
            num_args: 1,
            tag: 10_000,
        });

        assert_eq!(
            query_integer_column(
                &exact_scalar,
                "SELECT app_precedence(v) FROM udf_precedence_values ORDER BY v",
            )
            .await,
            vec![10_001, 10_002, 10_003],
            "an exact application scalar must outrank an application variadic aggregate",
        );
        assert_eq!(
            query_integer_column(
                &exact_scalar,
                "SELECT app_precedence(v, 10) FROM udf_precedence_values",
            )
            .await,
            vec![20_003],
            "the application variadic aggregate remains selected outside the exact arity",
        );

        let exact_aggregate = open_mem().await;
        seed_precedence_values(&exact_aggregate).await;
        exact_aggregate.register_deterministic_scalar_function(TaggedScalar {
            name: "app_precedence",
            num_args: -1,
            tag: 30_000,
        });
        exact_aggregate.register_aggregate_function(TaggedAggregate {
            name: "app_precedence",
            num_args: 1,
            tag: 40_000,
        });

        assert_eq!(
            query_integer_column(
                &exact_aggregate,
                "SELECT app_precedence(v) FROM udf_precedence_values",
            )
            .await,
            vec![40_003],
            "an exact application aggregate must outrank an application variadic scalar",
        );
        assert_eq!(
            query_integer_column(
                &exact_aggregate,
                "SELECT app_precedence(v, 10) FROM udf_precedence_values ORDER BY v",
            )
            .await,
            vec![30_011, 30_012, 30_013],
            "the application variadic scalar remains selected outside the exact arity",
        );
    });
}

#[test]
fn test_application_variadic_precedes_builtin_exact_sum_and_max() {
    asupersync::test_utils::run_test(|| async {
        assert_eq!(
            sqlite_builtin_aggregate_values(),
            (6, 3),
            "the rusqlite oracle confirms the unshadowed exact built-ins",
        );

        let sum_conn = open_mem().await;
        seed_precedence_values(&sum_conn).await;
        sum_conn.register_deterministic_scalar_function(TaggedScalar {
            name: "sum",
            num_args: -1,
            tag: 50_000,
        });
        assert_eq!(
            query_integer_column(
                &sum_conn,
                "SELECT sum(v) FROM udf_precedence_values ORDER BY v",
            )
            .await,
            vec![50_001, 50_002, 50_003],
            "an application variadic scalar must shadow SQLite's exact aggregate sum/1",
        );

        let max_conn = open_mem().await;
        seed_precedence_values(&max_conn).await;
        max_conn.register_aggregate_function(TaggedAggregate {
            name: "max",
            num_args: -1,
            tag: 60_000,
        });
        assert_eq!(
            query_integer_column(&max_conn, "SELECT max(v) FROM udf_precedence_values").await,
            vec![60_003],
            "an application variadic aggregate must shadow SQLite's exact aggregate max/1",
        );
    });
}

#[test]
fn test_bounded_variadic_application_selection_and_builtin_fallback() {
    asupersync::test_utils::run_test(|| async {
        let conn = open_mem().await;
        seed_precedence_values(&conn).await;
        conn.register_deterministic_scalar_function(BoundedTaggedScalar {
            name: "sum",
            min_args: 2,
            max_args: 2,
            tag: 65_000,
        });

        assert_eq!(
            query_first_int(&conn, "SELECT sum(20, 22)").await,
            65_042,
            "an in-range call must select the bounded variadic application scalar",
        );
        assert_eq!(
            query_first_int(&conn, "SELECT sum(v) FROM udf_precedence_values").await,
            6,
            "an out-of-range call must fall back to the compatible exact built-in aggregate",
        );
    });
}

#[test]
fn test_last_cross_kind_registration_wins_and_window_keeps_aggregate_form() {
    asupersync::test_utils::run_test(|| async {
        let conn = open_mem().await;
        seed_precedence_values(&conn).await;

        conn.register_deterministic_scalar_function(TaggedScalar {
            name: "cross_kind",
            num_args: 1,
            tag: 70_000,
        });
        assert_eq!(
            query_integer_column(
                &conn,
                "SELECT cross_kind(v) FROM udf_precedence_values ORDER BY v",
            )
            .await,
            vec![70_001, 70_002, 70_003],
        );

        conn.register_aggregate_function(TaggedAggregate {
            name: "cross_kind",
            num_args: 1,
            tag: 80_000,
        });
        assert_eq!(
            query_integer_column(&conn, "SELECT cross_kind(v) FROM udf_precedence_values",).await,
            vec![80_003],
            "a later aggregate registration must replace the scalar at the same key",
        );

        conn.register_window_function(TaggedWindow {
            name: "cross_kind",
            num_args: 1,
            tag: 90_000,
        });
        assert_eq!(
            query_integer_column(&conn, "SELECT cross_kind(v) FROM udf_precedence_values",).await,
            vec![90_006],
            "a window registration must retain SQLite's aggregate-call form",
        );
        assert_eq!(
            query_integer_column(
                &conn,
                "SELECT cross_kind(v) OVER (\
                     ORDER BY v ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW\
                 ) FROM udf_precedence_values ORDER BY v",
            )
            .await,
            vec![90_001, 90_003, 90_006],
            "the same window registration must support OVER execution",
        );

        conn.register_deterministic_scalar_function(TaggedScalar {
            name: "cross_kind",
            num_args: 1,
            tag: 100_000,
        });
        assert_eq!(
            query_integer_column(
                &conn,
                "SELECT cross_kind(v) FROM udf_precedence_values ORDER BY v",
            )
            .await,
            vec![100_001, 100_002, 100_003],
            "a later scalar registration must replace the window at the same key",
        );

        let over_error = conn
            .query(
                "SELECT cross_kind(v) OVER (ORDER BY v) \
                 FROM udf_precedence_values ORDER BY v",
            )
            .await
            .expect_err("a scalar replacement must reject the stale OVER call form");
        assert!(
            over_error
                .to_string()
                .to_ascii_lowercase()
                .contains("window"),
            "unexpected scalar-with-OVER error: {over_error}",
        );
    });
}

#[test]
fn test_recursive_cte_sum_shortcut_respects_every_application_function_kind() {
    asupersync::test_utils::run_test(|| async {
        const RECURSIVE_SUM_SQL: &str = "WITH RECURSIVE cnt(v) AS (\
             SELECT 1 UNION ALL SELECT v + 1 FROM cnt WHERE v < 5\
         ) SELECT sum(v) FROM cnt";

        let scalar = open_mem().await;
        scalar.register_deterministic_scalar_function(TaggedScalar {
            name: "sum",
            num_args: 1,
            tag: 110_000,
        });
        assert_eq!(
            query_integer_column(&scalar, RECURSIVE_SUM_SQL).await,
            vec![110_001, 110_002, 110_003, 110_004, 110_005],
            "the closed-form recursive SUM shortcut must not bypass a scalar sum/1",
        );

        let aggregate = open_mem().await;
        aggregate.register_aggregate_function(TaggedAggregate {
            name: "sum",
            num_args: 1,
            tag: 120_000,
        });
        assert_eq!(
            query_integer_column(&aggregate, RECURSIVE_SUM_SQL).await,
            vec![120_005],
            "the closed-form recursive SUM shortcut must not bypass an aggregate sum/1",
        );

        let window = open_mem().await;
        window.register_window_function(TaggedWindow {
            name: "sum",
            num_args: 1,
            tag: 130_000,
        });
        assert_eq!(
            query_integer_column(&window, RECURSIVE_SUM_SQL).await,
            vec![130_015],
            "a window sum/1 must retain its custom aggregate-call behavior for a recursive CTE",
        );
    });
}

#[test]
fn test_table_backed_builtin_named_windows_use_custom_lifecycle() {
    asupersync::test_utils::run_test(|| async {
        let conn = open_mem().await;
        seed_precedence_values(&conn).await;
        conn.register_window_function(TaggedWindow {
            name: "rank",
            num_args: 0,
            tag: 140_000,
        });
        conn.register_window_function(TaggedWindow {
            name: "nth_value",
            num_args: 2,
            tag: 150_000,
        });

        assert_eq!(
            query_integer_column(
                &conn,
                "SELECT rank() OVER (\
                     ORDER BY v ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW\
                 ) FROM udf_precedence_values ORDER BY v",
            )
            .await,
            vec![140_000, 140_000, 140_000],
            "a custom rank/0 must not run the built-in peer-rank shortcut",
        );
        conn.execute("CREATE TABLE udf_window_groups (g INTEGER NOT NULL, v INTEGER NOT NULL)")
            .await
            .expect("create grouped-window table");
        conn.execute("INSERT INTO udf_window_groups VALUES (1, 10), (1, 20), (2, 30), (2, 40)")
            .await
            .expect("seed grouped-window table");
        assert_eq!(
            query_integer_column(
                &conn,
                "SELECT rank() OVER (ORDER BY g), g \
                 FROM udf_window_groups GROUP BY g ORDER BY g",
            )
            .await,
            vec![140_000, 140_000],
            "the GROUP BY plus window path must use the custom rank/0 lifecycle",
        );
        assert_eq!(
            query_integer_column(
                &conn,
                "SELECT nth_value(v, 2) OVER (\
                     ORDER BY v ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING\
                 ) FROM udf_precedence_values ORDER BY v",
            )
            .await,
            vec![150_012, 150_012, 150_012],
            "a custom nth_value/2 must not run the built-in positional shortcut",
        );
    });
}

#[test]
fn test_application_window_sliding_lifecycle_is_one_state_per_partition() {
    asupersync::test_utils::run_test(|| async {
        let conn = open_mem().await;
        conn.execute("CREATE TABLE udf_window_lifecycle (g INTEGER NOT NULL, v INTEGER NOT NULL)")
            .await
            .expect("create lifecycle table");
        conn.execute(
            "INSERT INTO udf_window_lifecycle VALUES \
             (1, 1), (1, 2), (1, 3), (2, 10), (2, 20), (2, 30)",
        )
        .await
        .expect("seed lifecycle table");
        let counts = Arc::new(Mutex::new(WindowLifecycleCounts::default()));
        conn.register_window_function(LifecycleWindow {
            name: "lifecycle_sum",
            counts: Arc::clone(&counts),
            fail_step: false,
            fail_finalize: false,
        });

        const SQL: &str = "SELECT lifecycle_sum(v) OVER (\
             PARTITION BY g ORDER BY v \
             ROWS BETWEEN 1 PRECEDING AND CURRENT ROW\
         ) FROM udf_window_lifecycle ORDER BY g, v";
        assert_eq!(
            query_integer_column(&conn, SQL).await,
            vec![1, 3, 5, 10, 30, 50],
        );
        assert_eq!(
            snapshot_lifecycle_counts(&counts),
            WindowLifecycleCounts {
                initial: 2,
                step: 6,
                inverse: 2,
                value: 6,
                finalize: 2,
            },
            "each partition must have one state and use inverse for its outgoing row",
        );

        let prepared = conn.prepare(SQL).await.expect("prepare lifecycle window");
        let prepared_values = prepared
            .query()
            .await
            .expect("execute prepared lifecycle window")
            .iter()
            .map(|row| row.values()[0].to_integer())
            .collect::<Vec<_>>();
        assert_eq!(prepared_values, vec![1, 3, 5, 10, 30, 50]);
        assert_eq!(
            snapshot_lifecycle_counts(&counts),
            WindowLifecycleCounts {
                initial: 4,
                step: 12,
                inverse: 4,
                value: 12,
                finalize: 4,
            },
            "prepared execution must repeat the same per-partition lifecycle exactly once",
        );
    });
}

#[test]
fn test_application_window_preceding_end_before_partition_is_empty() {
    asupersync::test_utils::run_test(|| async {
        let rows = open_mem().await;
        seed_precedence_values(&rows).await;
        rows.register_window_function(RunningSumWindow);
        assert_eq!(
            query_integer_column(
                &rows,
                "SELECT running_sum(v) OVER (\
                     ORDER BY v ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING\
                 ) FROM udf_precedence_values ORDER BY v",
            )
            .await,
            vec![0, 1, 3],
            "an ending PRECEDING bound before the partition must produce an empty ROWS frame",
        );

        let groups = open_mem().await;
        groups
            .execute("CREATE TABLE udf_preceding_groups (v INTEGER NOT NULL)")
            .await
            .expect("create preceding-groups table");
        groups
            .execute("INSERT INTO udf_preceding_groups VALUES (1), (1), (2)")
            .await
            .expect("seed preceding-groups table");
        groups.register_window_function(RunningSumWindow);
        assert_eq!(
            query_integer_column(
                &groups,
                "SELECT running_sum(v) OVER (\
                     ORDER BY v GROUPS BETWEEN 2 PRECEDING AND 1 PRECEDING\
                 ) FROM udf_preceding_groups ORDER BY v",
            )
            .await,
            vec![0, 0, 2],
            "an ending PRECEDING bound before the partition must produce an empty GROUPS frame",
        );

        let following = open_mem().await;
        seed_precedence_values(&following).await;
        let following_counts = Arc::new(Mutex::new(WindowLifecycleCounts::default()));
        following.register_window_function(LifecycleWindow {
            name: "following_lifecycle",
            counts: Arc::clone(&following_counts),
            fail_step: false,
            fail_finalize: false,
        });
        assert_eq!(
            query_integer_column(
                &following,
                "SELECT following_lifecycle(v) OVER (\
                     ORDER BY v ROWS BETWEEN 2 FOLLOWING AND 2 FOLLOWING\
                 ) FROM udf_precedence_values ORDER BY v",
            )
            .await,
            vec![3, 0, 0],
        );
        assert_eq!(
            snapshot_lifecycle_counts(&following_counts),
            WindowLifecycleCounts {
                initial: 1,
                step: 3,
                inverse: 3,
                value: 3,
                finalize: 1,
            },
            "FOLLOWING frames must preserve SQLite's observable warm-up step/inverse callbacks",
        );

        let following_groups = open_mem().await;
        following_groups
            .execute("CREATE TABLE udf_following_groups (v INTEGER NOT NULL)")
            .await
            .expect("create following-groups table");
        following_groups
            .execute("INSERT INTO udf_following_groups VALUES (1), (1), (2), (3)")
            .await
            .expect("seed following-groups table");
        let group_counts = Arc::new(Mutex::new(WindowLifecycleCounts::default()));
        following_groups.register_window_function(LifecycleWindow {
            name: "following_group_lifecycle",
            counts: Arc::clone(&group_counts),
            fail_step: false,
            fail_finalize: false,
        });
        assert_eq!(
            query_integer_column(
                &following_groups,
                "SELECT following_group_lifecycle(v) OVER (\
                     ORDER BY v GROUPS BETWEEN 1 FOLLOWING AND 1 FOLLOWING\
                 ) FROM udf_following_groups ORDER BY v",
            )
            .await,
            vec![2, 2, 3, 0],
        );
        assert_eq!(
            snapshot_lifecycle_counts(&group_counts),
            WindowLifecycleCounts {
                initial: 1,
                step: 4,
                inverse: 4,
                value: 3,
                finalize: 1,
            },
            "GROUPS FOLLOWING warm-up advances through whole peer groups",
        );

        let following_range = open_mem().await;
        seed_precedence_values(&following_range).await;
        let range_counts = Arc::new(Mutex::new(WindowLifecycleCounts::default()));
        following_range.register_window_function(LifecycleWindow {
            name: "following_range_lifecycle",
            counts: Arc::clone(&range_counts),
            fail_step: false,
            fail_finalize: false,
        });
        assert_eq!(
            query_integer_column(
                &following_range,
                "SELECT following_range_lifecycle(v) OVER (\
                     ORDER BY v RANGE BETWEEN 1 FOLLOWING AND 2 FOLLOWING\
                 ) FROM udf_precedence_values ORDER BY v",
            )
            .await,
            vec![5, 3, 0],
        );
        assert_eq!(
            snapshot_lifecycle_counts(&range_counts),
            WindowLifecycleCounts {
                initial: 1,
                step: 3,
                inverse: 3,
                value: 3,
                finalize: 1,
            },
            "value-offset RANGE FOLLOWING warm-up must visit the ordered prefix",
        );

        let ordered_range = open_mem().await;
        ordered_range
            .execute("CREATE TABLE udf_ordered_range (v INTEGER NOT NULL)")
            .await
            .expect("create ordered RANGE table");
        ordered_range
            .execute("INSERT INTO udf_ordered_range VALUES (1), (2), (3), (4)")
            .await
            .expect("seed ordered RANGE table");
        let range_events = Arc::new(Mutex::new(Vec::new()));
        ordered_range.register_window_function(EventLogWindow {
            name: "ordered_range_lifecycle",
            events: Arc::clone(&range_events),
        });
        assert_eq!(
            query_integer_column(
                &ordered_range,
                "SELECT ordered_range_lifecycle(v) OVER (\
                     ORDER BY v RANGE BETWEEN 1 FOLLOWING AND 2 FOLLOWING\
                 ) FROM udf_ordered_range ORDER BY v",
            )
            .await,
            vec![5, 7, 4, 0],
        );
        assert_eq!(
            *range_events.lock().unwrap(),
            vec![
                "initial",
                "step(1)",
                "step(2)",
                "step(3)",
                "inverse(1)",
                "value(5)",
                "step(4)",
                "inverse(2)",
                "value(7)",
                "inverse(3)",
                "value(4)",
                "inverse(4)",
                "value(0)",
                "finalize(0)",
            ],
            "numeric RANGE must enter through the ending cursor before advancing the start",
        );

        let preceding_range = open_mem().await;
        preceding_range
            .execute("CREATE TABLE udf_preceding_range (v INTEGER NOT NULL)")
            .await
            .expect("create preceding RANGE table");
        preceding_range
            .execute("INSERT INTO udf_preceding_range VALUES (1), (2), (3), (4)")
            .await
            .expect("seed preceding RANGE table");
        let preceding_range_events = Arc::new(Mutex::new(Vec::new()));
        preceding_range.register_window_function(EventLogWindow {
            name: "preceding_range_lifecycle",
            events: Arc::clone(&preceding_range_events),
        });
        assert_eq!(
            query_integer_column(
                &preceding_range,
                "SELECT preceding_range_lifecycle(v) OVER (\
                     ORDER BY v RANGE BETWEEN 1 PRECEDING AND CURRENT ROW\
                 ) FROM udf_preceding_range ORDER BY v",
            )
            .await,
            vec![1, 3, 5, 7],
        );
        assert_eq!(
            *preceding_range_events.lock().unwrap(),
            vec![
                "initial",
                "step(1)",
                "value(1)",
                "step(2)",
                "value(3)",
                "inverse(1)",
                "step(3)",
                "value(5)",
                "inverse(2)",
                "step(4)",
                "value(7)",
                "finalize(7)",
            ],
            "ordinary numeric RANGE transitions must remove before entering the next row",
        );

        let reversed_rows = open_mem().await;
        seed_precedence_values(&reversed_rows).await;
        let reversed_row_events = Arc::new(Mutex::new(Vec::new()));
        reversed_rows.register_window_function(EventLogWindow {
            name: "reversed_row_lifecycle",
            events: Arc::clone(&reversed_row_events),
        });
        assert_eq!(
            query_integer_column(
                &reversed_rows,
                "SELECT reversed_row_lifecycle(v) OVER (\
                     ORDER BY v ROWS BETWEEN 3 FOLLOWING AND 1 FOLLOWING\
                 ) FROM udf_precedence_values ORDER BY v",
            )
            .await,
            vec![0, 0, 0],
        );
        assert_eq!(
            *reversed_row_events.lock().unwrap(),
            vec!["initial", "value(0)", "value(0)", "value(0)", "finalize(0)"],
            "a statically reversed ROWS interval must not advance either callback cursor",
        );

        let reversed_groups = open_mem().await;
        reversed_groups
            .execute("CREATE TABLE udf_reversed_groups (v INTEGER NOT NULL)")
            .await
            .expect("create reversed GROUPS table");
        reversed_groups
            .execute("INSERT INTO udf_reversed_groups VALUES (1), (1), (2)")
            .await
            .expect("seed reversed GROUPS table");
        let reversed_group_events = Arc::new(Mutex::new(Vec::new()));
        reversed_groups.register_window_function(EventLogWindow {
            name: "reversed_group_lifecycle",
            events: Arc::clone(&reversed_group_events),
        });
        assert_eq!(
            query_integer_column(
                &reversed_groups,
                "SELECT reversed_group_lifecycle(v) OVER (\
                     ORDER BY v GROUPS BETWEEN 3 FOLLOWING AND 1 FOLLOWING\
                 ) FROM udf_reversed_groups ORDER BY v",
            )
            .await,
            vec![0, 0, 0],
        );
        assert_eq!(
            *reversed_group_events.lock().unwrap(),
            vec!["initial", "value(0)", "value(0)", "value(0)", "finalize(0)"],
            "a reversed GROUPS interval calls value per output row, not per peer group",
        );

        let truncated_rows = open_mem().await;
        seed_precedence_values(&truncated_rows).await;
        let truncated_row_events = Arc::new(Mutex::new(Vec::new()));
        truncated_rows.register_window_function(EventLogWindow {
            name: "truncated_row_lifecycle",
            events: Arc::clone(&truncated_row_events),
        });
        assert_eq!(
            query_integer_column(
                &truncated_rows,
                "SELECT truncated_row_lifecycle(v) OVER (\
                     ORDER BY v ROWS BETWEEN 5 FOLLOWING AND 6 FOLLOWING\
                 ) FROM udf_precedence_values ORDER BY v",
            )
            .await,
            vec![0, 0, 0],
        );
        assert_eq!(
            *truncated_row_events.lock().unwrap(),
            vec![
                "initial",
                "step(1)",
                "step(2)",
                "inverse(1)",
                "step(3)",
                "inverse(2)",
                "inverse(3)",
                "value(0)",
                "value(0)",
                "value(0)",
                "finalize(0)",
            ],
            "ROWS warm-up must preserve the unclamped nominal FOLLOWING width",
        );

        let truncated_groups = open_mem().await;
        seed_precedence_values(&truncated_groups).await;
        let truncated_group_events = Arc::new(Mutex::new(Vec::new()));
        truncated_groups.register_window_function(EventLogWindow {
            name: "truncated_group_lifecycle",
            events: Arc::clone(&truncated_group_events),
        });
        assert_eq!(
            query_integer_column(
                &truncated_groups,
                "SELECT truncated_group_lifecycle(v) OVER (\
                     ORDER BY v GROUPS BETWEEN 5 FOLLOWING AND 6 FOLLOWING\
                 ) FROM udf_precedence_values ORDER BY v",
            )
            .await,
            vec![0, 0, 0],
        );
        assert_eq!(
            *truncated_group_events.lock().unwrap(),
            vec![
                "initial",
                "step(1)",
                "step(2)",
                "inverse(1)",
                "step(3)",
                "inverse(2)",
                "inverse(3)",
                "value(0)",
                "value(0)",
                "value(0)",
                "finalize(0)",
            ],
            "GROUPS warm-up must preserve the unclamped nominal FOLLOWING width",
        );
    });
}

#[test]
fn test_application_window_range_preserves_sqlite_numeric_and_storage_order() {
    asupersync::test_utils::run_test(|| async {
        let precise = open_mem().await;
        precise
            .execute(
                "CREATE TABLE udf_precise_range \
                 (v INTEGER NOT NULL, marker INTEGER NOT NULL)",
            )
            .await
            .expect("create precise RANGE table");
        precise
            .execute(
                "INSERT INTO udf_precise_range VALUES \
                 (9007199254740992, 1), \
                 (9007199254740993, 10), \
                 (9007199254740994, 100)",
            )
            .await
            .expect("seed precise RANGE table");
        assert_eq!(
            query_integer_column(&precise, "SELECT v FROM udf_precise_range ORDER BY v",).await,
            vec![
                9_007_199_254_740_992,
                9_007_199_254_740_993,
                9_007_199_254_740_994,
            ],
            "the RANGE oracle inputs must remain exact INTEGER storage values",
        );
        precise.register_window_function(RunningSumWindow);
        assert_eq!(
            query_integer_column(
                &precise,
                "SELECT running_sum(marker) OVER (\
                     ORDER BY v RANGE BETWEEN 0 PRECEDING AND CURRENT ROW\
                 ) FROM udf_precise_range ORDER BY v",
            )
            .await,
            vec![1, 10, 100],
            "zero-offset RANGE must not collapse distinct integers above 2^53",
        );
        assert_eq!(
            query_integer_column(
                &precise,
                "SELECT running_sum(marker) OVER (\
                     ORDER BY v RANGE BETWEEN 1 PRECEDING AND CURRENT ROW\
                 ) FROM udf_precise_range ORDER BY v",
            )
            .await,
            vec![1, 11, 110],
            "integer RANGE arithmetic must remain exact until SQLite would promote it",
        );
        assert_eq!(
            query_integer_column(
                &precise,
                "SELECT running_sum(marker) OVER (\
                     ORDER BY v RANGE BETWEEN 0.0 PRECEDING AND CURRENT ROW\
                 ) FROM udf_precise_range ORDER BY v",
            )
            .await,
            vec![1, 10, 100],
            "this three-row precision prefix must retain SQLite's observed 0.0 PRECEDING boundaries",
        );
        for (offset, expected) in [
            ("1.0", vec![1, 10, 100]),
            ("'01'", vec![1, 11, 110]),
            ("'1.0'", vec![1, 10, 100]),
        ] {
            assert_eq!(
                query_integer_column(
                    &precise,
                    &format!(
                        "SELECT running_sum(marker) OVER (\
                             ORDER BY v RANGE BETWEEN {offset} PRECEDING AND CURRENT ROW\
                         ) FROM udf_precise_range ORDER BY v"
                    ),
                )
                .await,
                expected,
                "RANGE offset storage class must survive constant normalization: {offset}",
            );
        }
        assert_eq!(
            precise
                .query(
                    "SELECT running_sum(marker) OVER (\
                         ORDER BY v RANGE BETWEEN 1.0 PRECEDING AND 1.0 PRECEDING\
                     ) FROM udf_precise_range ORDER BY v",
                )
                .await
                .expect("query a REAL PRECEDING ending boundary")
                .into_iter()
                .map(|row| row.values()[0].clone())
                .collect::<Vec<_>>(),
            vec![
                SqliteValue::Integer(1),
                SqliteValue::Integer(10),
                SqliteValue::Integer(0),
            ],
            "RANGE must apply REAL arithmetic and SQLite's ordered-cursor guard to its end",
        );
        assert_eq!(
            query_integer_column(
                &precise,
                "SELECT running_sum(marker) OVER (\
                     ORDER BY v RANGE BETWEEN 0.0 FOLLOWING AND 0.0 FOLLOWING\
                 ) FROM udf_precise_range ORDER BY v",
            )
            .await,
            vec![1, 11, 100],
            "SQLite's pre-arithmetic guard and directional REAL rounding govern FOLLOWING bounds",
        );
        assert_eq!(
            query_integer_column(
                &precise,
                "SELECT running_sum(marker) OVER (\
                     ORDER BY v DESC RANGE BETWEEN 1.0 PRECEDING AND CURRENT ROW\
                 ) FROM udf_precise_range ORDER BY v",
            )
            .await,
            vec![111, 110, 100],
            "descending RANGE must subtract on the moving boundary cursor",
        );
        assert_eq!(
            query_integer_column(
                &precise,
                "SELECT running_sum(marker) OVER (\
                     ORDER BY v DESC RANGE BETWEEN 0.0 FOLLOWING AND 0.0 FOLLOWING\
                 ) FROM udf_precise_range ORDER BY v",
            )
            .await,
            vec![1, 1, 100],
            "descending FOLLOWING must reverse the comparison as well as the arithmetic",
        );

        let zero_precision = open_mem().await;
        zero_precision
            .execute(
                "CREATE TABLE udf_zero_precision_range \
                 (v INTEGER NOT NULL, marker INTEGER NOT NULL)",
            )
            .await
            .expect("create zero-offset precision RANGE table");
        zero_precision
            .execute(
                "INSERT INTO udf_zero_precision_range VALUES \
                 (9007199254740992, 1), \
                 (9007199254740993, 10), \
                 (9007199254740994, 100), \
                 (9007199254740995, 1000), \
                 (9007199254740996, 10000)",
            )
            .await
            .expect("seed zero-offset precision RANGE table");
        zero_precision.register_window_function(RunningSumWindow);
        for (frame, expected) in [
            (
                "ORDER BY v RANGE BETWEEN 0 PRECEDING AND 0 PRECEDING",
                vec![1, 10, 100, 1000, 10000],
            ),
            (
                "ORDER BY v RANGE BETWEEN 0.0 PRECEDING AND 0.0 PRECEDING",
                vec![1, 10, 100, 0, 11000],
            ),
            (
                "ORDER BY v RANGE BETWEEN 0.0 PRECEDING AND CURRENT ROW",
                vec![1, 10, 100, 1000, 11000],
            ),
            (
                "ORDER BY v RANGE BETWEEN 0.0 FOLLOWING AND 0.0 FOLLOWING",
                vec![1, 11, 100, 10000, 10000],
            ),
            (
                "ORDER BY v RANGE BETWEEN 1 PRECEDING AND CURRENT ROW",
                vec![1, 11, 110, 1100, 11000],
            ),
            (
                "ORDER BY v RANGE BETWEEN 0.3 PRECEDING AND CURRENT ROW",
                vec![1, 10, 100, 1000, 11000],
            ),
        ] {
            assert_eq!(
                query_integer_column(
                    &zero_precision,
                    &format!(
                        "SELECT running_sum(marker) OVER ({frame}) \
                         FROM udf_zero_precision_range ORDER BY v"
                    ),
                )
                .await,
                expected,
                "ascending RANGE must retain SQLite's directional numeric behavior: {frame}",
            );
        }
        for (frame, expected) in [
            (
                "ORDER BY v DESC RANGE BETWEEN 0.0 PRECEDING AND 0.0 PRECEDING",
                vec![10000, 1000, 100, 0, 11],
            ),
            (
                "ORDER BY v DESC RANGE BETWEEN 0.0 FOLLOWING AND 0.0 FOLLOWING",
                vec![10000, 11000, 100, 1, 1],
            ),
        ] {
            assert_eq!(
                query_integer_column(
                    &zero_precision,
                    &format!(
                        "SELECT running_sum(marker) OVER ({frame}) \
                         FROM udf_zero_precision_range ORDER BY v DESC"
                    ),
                )
                .await,
                expected,
                "descending RANGE must retain SQLite's directional numeric behavior: {frame}",
            );
        }

        let mixed = open_mem().await;
        mixed
            .execute("CREATE TABLE udf_mixed_range (v, marker INTEGER NOT NULL)")
            .await
            .expect("create mixed-storage RANGE table");
        mixed
            .execute(
                "INSERT INTO udf_mixed_range VALUES \
                 (NULL, 100), (1, 1), (2, 2), ('x', 1000)",
            )
            .await
            .expect("seed mixed-storage RANGE table");
        mixed.register_window_function(RunningSumWindow);
        assert_eq!(
            query_integer_column(
                &mixed,
                "SELECT running_sum(marker) OVER (\
                     ORDER BY v RANGE BETWEEN 10 FOLLOWING AND UNBOUNDED FOLLOWING\
                 ) FROM udf_mixed_range ORDER BY v",
            )
            .await,
            vec![1103, 1000, 1000, 1000],
            "RANGE boundaries must compare nonnumeric candidates by SQLite storage-class order",
        );
        assert_eq!(
            query_integer_column(
                &mixed,
                "SELECT running_sum(marker) OVER (\
                     ORDER BY v ASC NULLS LAST \
                     RANGE BETWEEN 10 FOLLOWING AND UNBOUNDED FOLLOWING\
                 ) FROM udf_mixed_range ORDER BY v ASC NULLS LAST",
            )
            .await,
            vec![1100, 1100, 1100, 100],
            "ascending RANGE must retain explicit NULLS LAST ordering for frame boundaries",
        );
        assert_eq!(
            query_integer_column(
                &mixed,
                "SELECT running_sum(marker) OVER (\
                     ORDER BY v DESC NULLS FIRST \
                     RANGE BETWEEN 10 FOLLOWING AND UNBOUNDED FOLLOWING\
                 ) FROM udf_mixed_range ORDER BY v DESC NULLS FIRST",
            )
            .await,
            vec![1103, 1003, 0, 0],
            "descending RANGE must retain explicit NULLS FIRST ordering for frame boundaries",
        );
        assert_eq!(
            query_integer_column(
                &mixed,
                "SELECT running_sum(marker) OVER mixed_order + 7 \
                 FROM udf_mixed_range \
                 WINDOW mixed_order AS (\
                     ORDER BY v ASC NULLS LAST \
                     ROWS BETWEEN 1 PRECEDING AND CURRENT ROW\
                 ) ORDER BY v ASC NULLS LAST",
            )
            .await,
            vec![8, 10, 1009, 1107],
            "named window resolution and wrapped application results must retain NULLS LAST",
        );

        let collated = open_mem().await;
        collated
            .execute("CREATE TABLE udf_collated_range (v TEXT, marker INTEGER NOT NULL)")
            .await
            .expect("create collated RANGE table");
        collated
            .execute("INSERT INTO udf_collated_range VALUES ('a', 1), ('B', 10), ('c', 100)")
            .await
            .expect("seed collated RANGE table");
        collated.register_window_function(RunningSumWindow);
        assert_eq!(
            query_integer_column(
                &collated,
                "SELECT running_sum(marker) OVER (\
                     ORDER BY v COLLATE NOCASE \
                     RANGE BETWEEN 1 PRECEDING AND CURRENT ROW\
                 ) FROM udf_collated_range ORDER BY v COLLATE NOCASE",
            )
            .await,
            vec![1, 10, 100],
            "TEXT RANGE boundaries must skip the numeric precheck and use ORDER collation",
        );
    });
}

#[test]
fn test_application_window_grouped_and_peer_frames_preserve_lifecycle() {
    asupersync::test_utils::run_test(|| async {
        let grouped = open_mem().await;
        grouped
            .execute(
                "CREATE TABLE udf_grouped_window_lifecycle \
                 (g INTEGER NOT NULL, v INTEGER NOT NULL)",
            )
            .await
            .expect("create grouped lifecycle table");
        grouped
            .execute(
                "INSERT INTO udf_grouped_window_lifecycle VALUES \
                 (1, 1), (1, 2), (2, 10), (2, 20), (3, 100), (3, 200)",
            )
            .await
            .expect("seed grouped lifecycle table");
        let grouped_counts = Arc::new(Mutex::new(WindowLifecycleCounts::default()));
        grouped.register_window_function(LifecycleWindow {
            name: "grouped_lifecycle_sum",
            counts: Arc::clone(&grouped_counts),
            fail_step: false,
            fail_finalize: false,
        });
        assert_eq!(
            query_integer_column(
                &grouped,
                "SELECT grouped_lifecycle_sum(sum(v)) OVER (\
                     ORDER BY g ROWS BETWEEN 1 PRECEDING AND CURRENT ROW\
                 ) FROM udf_grouped_window_lifecycle GROUP BY g ORDER BY g",
            )
            .await,
            vec![3, 33, 330],
        );
        assert_eq!(
            snapshot_lifecycle_counts(&grouped_counts),
            WindowLifecycleCounts {
                initial: 1,
                step: 3,
                inverse: 1,
                value: 3,
                finalize: 1,
            },
            "the grouped-window route must use the same sliding lifecycle",
        );

        let peers = open_mem().await;
        peers
            .execute("CREATE TABLE udf_window_peers (v INTEGER NOT NULL)")
            .await
            .expect("create peer table");
        peers
            .execute("INSERT INTO udf_window_peers VALUES (1), (1), (2)")
            .await
            .expect("seed peer table");
        let peer_counts = Arc::new(Mutex::new(WindowLifecycleCounts::default()));
        peers.register_window_function(LifecycleWindow {
            name: "peer_lifecycle_sum",
            counts: Arc::clone(&peer_counts),
            fail_step: false,
            fail_finalize: false,
        });
        assert_eq!(
            query_integer_column(
                &peers,
                "SELECT peer_lifecycle_sum(v) OVER (ORDER BY v) \
                 FROM udf_window_peers ORDER BY v",
            )
            .await,
            vec![2, 2, 4],
            "the default RANGE frame must reuse one application value across each peer group",
        );
        assert_eq!(
            snapshot_lifecycle_counts(&peer_counts),
            WindowLifecycleCounts {
                initial: 1,
                step: 3,
                inverse: 0,
                value: 2,
                finalize: 1,
            },
        );
    });
}

#[test]
fn test_application_grouped_window_routes_join_before_generic_aggregation() {
    asupersync::test_utils::run_test(|| async {
        let conn = open_mem().await;
        conn.execute("CREATE TABLE udf_window_route_anchor (g INTEGER NOT NULL)")
            .await
            .expect("create grouped-window route anchor");
        conn.execute(
            "CREATE TABLE udf_window_route_detail \
             (g INTEGER NOT NULL, v INTEGER NOT NULL)",
        )
        .await
        .expect("create grouped-window route detail");
        conn.execute("INSERT INTO udf_window_route_anchor VALUES (1), (2)")
            .await
            .expect("seed grouped-window route anchor");
        conn.execute(
            "INSERT INTO udf_window_route_detail VALUES \
             (1, 10), (1, 20), (2, 40)",
        )
        .await
        .expect("seed grouped-window route detail");

        let counts = Arc::new(Mutex::new(WindowLifecycleCounts::default()));
        conn.register_window_function(LifecycleWindow {
            name: "routed_lifecycle_sum",
            counts: Arc::clone(&counts),
            fail_step: false,
            fail_finalize: false,
        });
        let sql = "SELECT routed_lifecycle_sum(sum(d.v)) OVER (\
                       ORDER BY a.g ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW\
                   ) \
                   FROM udf_window_route_anchor AS a \
                   JOIN udf_window_route_detail AS d ON d.g = a.g \
                   GROUP BY a.g ORDER BY a.g";
        for prepared in [false, true] {
            *counts.lock().unwrap() = WindowLifecycleCounts::default();
            let rows = if prepared {
                conn.prepare(sql)
                    .await
                    .expect("prepare grouped-window JOIN")
                    .query()
                    .await
                    .expect("execute prepared grouped-window JOIN")
            } else {
                conn.query(sql).await.expect("execute grouped-window JOIN")
            };
            assert_eq!(
                rows.iter()
                    .map(|row| match row.values().first() {
                        Some(SqliteValue::Integer(value)) => *value,
                        other => panic!("expected grouped-window integer, got {other:?}"),
                    })
                    .collect::<Vec<_>>(),
                vec![30, 70],
                "prepared={prepared}",
            );
            assert_eq!(
                snapshot_lifecycle_counts(&counts),
                WindowLifecycleCounts {
                    initial: 1,
                    step: 2,
                    inverse: 0,
                    value: 2,
                    finalize: 1,
                },
                "prepared={prepared}",
            );
        }

        *counts.lock().unwrap() = WindowLifecycleCounts::default();
        assert_eq!(
            query_integer_column(
                &conn,
                "SELECT routed_lifecycle_sum(d.v) \
                 FROM udf_window_route_anchor AS a \
                 JOIN udf_window_route_detail AS d ON d.g = a.g \
                 GROUP BY a.g ORDER BY a.g",
            )
            .await,
            vec![30, 40],
            "a bare application-window name remains aggregate-callable without OVER",
        );
        assert_eq!(
            snapshot_lifecycle_counts(&counts),
            WindowLifecycleCounts {
                initial: 2,
                step: 3,
                inverse: 0,
                value: 0,
                finalize: 2,
            },
        );
    });
}

#[test]
fn test_application_window_exclusion_preserves_argument_order() {
    asupersync::test_utils::run_test(|| async {
        let fromless = open_mem().await;
        let fromless_counts = Arc::new(Mutex::new(WindowLifecycleCounts::default()));
        fromless.register_window_function(DistinctValueFinalizeWindow {
            name: "fromless_excluded_lifecycle",
            counts: Arc::clone(&fromless_counts),
        });
        assert_eq!(
            query_first_int(&fromless, "SELECT fromless_excluded_lifecycle(5) OVER ()").await,
            10_005,
            "an omitted EXCLUDE clause returns xValue",
        );
        assert_eq!(
            query_first_int(
                &fromless,
                "SELECT fromless_excluded_lifecycle(5) OVER (\
                     ROWS CURRENT ROW EXCLUDE NO OTHERS\
                 )",
            )
            .await,
            20_005,
            "an explicit EXCLUDE clause returns xFinal",
        );
        let empty_fromless = fromless
            .query(
                "SELECT fromless_excluded_lifecycle(5) OVER (\
                     ROWS CURRENT ROW EXCLUDE CURRENT ROW\
                 )",
            )
            .await
            .expect("query empty FROM-less excluded frame");
        assert_eq!(
            empty_fromless[0].values()[0],
            SqliteValue::Integer(20_000),
            "an empty explicit-EXCLUDE frame uses xFinal on a fresh empty state",
        );
        assert_eq!(
            snapshot_lifecycle_counts(&fromless_counts),
            WindowLifecycleCounts {
                initial: 3,
                step: 2,
                inverse: 0,
                value: 1,
                finalize: 3,
            },
            "an empty FROM-less excluded frame must finalize a fresh application state",
        );

        let conn = open_mem().await;
        seed_ordered_values(&conn).await;
        conn.register_window_function(OrderedArgsFunction {
            name: "ordered_window",
        });

        assert_eq!(
            query_integer_column(
                &conn,
                "SELECT ordered_window(v, marker) OVER (\
                     ORDER BY ord ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING \
                     EXCLUDE CURRENT ROW\
                 ) FROM udf_ordered_values ORDER BY ord",
            )
            .await,
            vec![28, 1_739, 28],
            "EXCLUDE must rebuild each output frame in frame order",
        );

        let lifecycle = open_mem().await;
        seed_precedence_values(&lifecycle).await;
        let counts = Arc::new(Mutex::new(WindowLifecycleCounts::default()));
        lifecycle.register_window_function(LifecycleWindow {
            name: "excluded_lifecycle",
            counts: Arc::clone(&counts),
            fail_step: false,
            fail_finalize: false,
        });
        assert_eq!(
            query_integer_column(
                &lifecycle,
                "SELECT excluded_lifecycle(v) OVER (\
                     ORDER BY v ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING \
                     EXCLUDE CURRENT ROW\
                 ) FROM udf_precedence_values ORDER BY v",
            )
            .await,
            vec![2, 4, 2],
        );
        assert_eq!(
            snapshot_lifecycle_counts(&counts),
            WindowLifecycleCounts {
                initial: 3,
                step: 4,
                inverse: 0,
                value: 0,
                finalize: 3,
            },
            "EXCLUDE must use one fresh xStep/xFinal aggregate lifecycle per nonempty output frame",
        );

        let empty_frame_rows = lifecycle
            .query(
                "SELECT excluded_lifecycle(v) OVER (\
                     ORDER BY v ROWS CURRENT ROW EXCLUDE CURRENT ROW\
                 ) FROM udf_precedence_values ORDER BY v",
            )
            .await
            .expect("query empty excluded frames");
        assert!(
            empty_frame_rows
                .iter()
                .all(|row| row.values()[0] == SqliteValue::Integer(0))
        );
        assert_eq!(
            snapshot_lifecycle_counts(&counts),
            WindowLifecycleCounts {
                initial: 6,
                step: 4,
                inverse: 0,
                value: 0,
                finalize: 6,
            },
            "each empty excluded frame must finalize its own fresh application state",
        );

        assert_eq!(
            query_integer_column(
                &lifecycle,
                "SELECT excluded_lifecycle(v) OVER (\
                     ORDER BY v ROWS CURRENT ROW EXCLUDE NO OTHERS\
                 ) FROM udf_precedence_values ORDER BY v",
            )
            .await,
            vec![1, 2, 3],
        );
        assert_eq!(
            snapshot_lifecycle_counts(&counts),
            WindowLifecycleCounts {
                initial: 9,
                step: 7,
                inverse: 0,
                value: 0,
                finalize: 9,
            },
            "explicit EXCLUDE NO OTHERS follows SQLite's per-frame xFinal lifecycle",
        );
    });
}

#[test]
fn test_application_window_finalize_errors_and_callback_failures_are_observable() {
    asupersync::test_utils::run_test(|| async {
        async fn assert_finalize_error(conn: &Connection, sql: &str) {
            let error = conn
                .query(sql)
                .await
                .expect_err("application window finalize failure must escape execution");
            assert!(
                error.to_string().contains("lifecycle finalize sentinel"),
                "unexpected finalize error: {error}",
            );
        }

        let fromless = open_mem().await;
        let fromless_counts = Arc::new(Mutex::new(WindowLifecycleCounts::default()));
        fromless.register_window_function(LifecycleWindow {
            name: "fromless_finalize_failure",
            counts: Arc::clone(&fromless_counts),
            fail_step: false,
            fail_finalize: true,
        });
        assert_finalize_error(&fromless, "SELECT fromless_finalize_failure(1) OVER ()").await;
        assert_eq!(
            snapshot_lifecycle_counts(&fromless_counts),
            WindowLifecycleCounts {
                initial: 1,
                step: 1,
                inverse: 0,
                value: 1,
                finalize: 1,
            },
        );

        let table = open_mem().await;
        seed_precedence_values(&table).await;
        let table_counts = Arc::new(Mutex::new(WindowLifecycleCounts::default()));
        table.register_window_function(LifecycleWindow {
            name: "table_finalize_failure",
            counts: Arc::clone(&table_counts),
            fail_step: false,
            fail_finalize: true,
        });
        assert_finalize_error(
            &table,
            "SELECT table_finalize_failure(v) OVER () FROM udf_precedence_values",
        )
        .await;
        assert_eq!(
            snapshot_lifecycle_counts(&table_counts),
            WindowLifecycleCounts {
                initial: 1,
                step: 3,
                inverse: 0,
                value: 1,
                finalize: 1,
            },
        );

        let grouped = open_mem().await;
        seed_precedence_values(&grouped).await;
        let grouped_counts = Arc::new(Mutex::new(WindowLifecycleCounts::default()));
        grouped.register_window_function(LifecycleWindow {
            name: "grouped_finalize_failure",
            counts: Arc::clone(&grouped_counts),
            fail_step: false,
            fail_finalize: true,
        });
        assert_finalize_error(
            &grouped,
            "SELECT grouped_finalize_failure(sum(v)) OVER () \
             FROM udf_precedence_values GROUP BY v",
        )
        .await;
        assert_eq!(
            snapshot_lifecycle_counts(&grouped_counts),
            WindowLifecycleCounts {
                initial: 1,
                step: 3,
                inverse: 0,
                value: 1,
                finalize: 1,
            },
        );

        let callback_failure = open_mem().await;
        seed_precedence_values(&callback_failure).await;
        let callback_counts = Arc::new(Mutex::new(WindowLifecycleCounts::default()));
        callback_failure.register_window_function(LifecycleWindow {
            name: "step_failure",
            counts: Arc::clone(&callback_counts),
            fail_step: true,
            fail_finalize: false,
        });
        let error = callback_failure
            .query(
                "SELECT step_failure(v) OVER (\
                     ORDER BY v ROWS BETWEEN CURRENT ROW AND CURRENT ROW\
                 ) \
                 FROM udf_precedence_values",
            )
            .await
            .expect_err("step failure must escape window execution");
        assert!(
            error.to_string().contains("lifecycle step sentinel"),
            "unexpected step error: {error}",
        );
        assert_eq!(
            snapshot_lifecycle_counts(&callback_counts),
            WindowLifecycleCounts {
                initial: 1,
                step: 1,
                inverse: 0,
                value: 0,
                finalize: 1,
            },
            "finalize must run exactly once even when step fails",
        );
    });
}

#[test]
fn test_application_window_filter_and_empty_input_lifecycles() {
    asupersync::test_utils::run_test(|| async {
        let filtered = open_mem().await;
        seed_precedence_values(&filtered).await;
        let filtered_counts = Arc::new(Mutex::new(WindowLifecycleCounts::default()));
        let expression_calls = Arc::new(Mutex::new(Vec::new()));
        filtered.register_window_function(LifecycleWindow {
            name: "filtered_lifecycle",
            counts: Arc::clone(&filtered_counts),
            fail_step: false,
            fail_finalize: false,
        });
        filtered.register_nondeterministic_scalar_function(RecordingWindowExpression {
            name: "record_window_argument",
            role: "argument",
            calls: Arc::clone(&expression_calls),
            is_filter: false,
        });
        filtered.register_nondeterministic_scalar_function(RecordingWindowExpression {
            name: "record_window_filter",
            role: "filter",
            calls: Arc::clone(&expression_calls),
            is_filter: true,
        });
        assert_eq!(
            query_integer_column(
                &filtered,
                "SELECT filtered_lifecycle(record_window_argument(v)) \
                 FILTER (WHERE record_window_filter(v)) OVER (\
                     ORDER BY v ROWS BETWEEN 1 PRECEDING AND CURRENT ROW\
                 ) FROM udf_precedence_values ORDER BY v",
            )
            .await,
            vec![0, 2, 2],
        );
        assert_eq!(
            snapshot_lifecycle_counts(&filtered_counts),
            WindowLifecycleCounts {
                initial: 1,
                step: 1,
                inverse: 0,
                value: 3,
                finalize: 1,
            },
            "FILTER gates step/inverse but not the partition lifecycle or per-row value",
        );
        assert_eq!(
            *expression_calls.lock().unwrap(),
            vec![
                ("argument", 1),
                ("filter", 1),
                ("argument", 2),
                ("filter", 2),
                ("argument", 3),
                ("filter", 3),
            ],
            "window arguments and FILTER must remain interleaved in input-row order",
        );

        let materialized = open_mem().await;
        materialized
            .execute("CREATE TABLE udf_window_materialization (v INTEGER NOT NULL)")
            .await
            .expect("create window-materialization table");
        materialized
            .execute("INSERT INTO udf_window_materialization VALUES (2), (1)")
            .await
            .expect("seed window-materialization table");
        let materialization_calls = Arc::new(Mutex::new(Vec::new()));
        materialized.register_window_function(RunningSumWindow);
        for (name, role, is_filter) in [
            ("record_row_argument", "argument", false),
            ("record_row_filter", "filter", true),
            ("record_row_partition", "partition", false),
            ("record_row_order", "order", false),
        ] {
            materialized.register_nondeterministic_scalar_function(RecordingWindowExpression {
                name,
                role,
                calls: Arc::clone(&materialization_calls),
                is_filter,
            });
        }
        assert_eq!(
            query_integer_column(
                &materialized,
                "SELECT running_sum(record_row_argument(v)) \
                 FILTER (WHERE record_row_filter(v)) OVER (\
                     PARTITION BY record_row_partition(v) \
                     ORDER BY record_row_order(v) \
                     ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING\
                 ) FROM udf_window_materialization",
            )
            .await,
            vec![0, 2],
            "without an outer ORDER BY, output follows the window's partition/order traversal",
        );
        assert_eq!(
            *materialization_calls.lock().unwrap(),
            vec![
                ("argument", 2),
                ("filter", 2),
                ("partition", 2),
                ("order", 2),
                ("argument", 1),
                ("filter", 1),
                ("partition", 1),
                ("order", 1),
            ],
            "table-backed windows materialize args, FILTER, PARTITION BY, and ORDER BY row-major",
        );

        let empty = open_mem().await;
        empty
            .execute("CREATE TABLE udf_empty_window (v INTEGER NOT NULL)")
            .await
            .expect("create empty window table");
        let empty_counts = Arc::new(Mutex::new(WindowLifecycleCounts::default()));
        empty.register_window_function(LifecycleWindow {
            name: "empty_lifecycle",
            counts: Arc::clone(&empty_counts),
            fail_step: false,
            fail_finalize: false,
        });
        assert!(
            empty
                .query("SELECT empty_lifecycle(v) OVER () FROM udf_empty_window")
                .await
                .expect("query empty application window")
                .is_empty(),
        );
        assert_eq!(
            snapshot_lifecycle_counts(&empty_counts),
            WindowLifecycleCounts::default(),
            "an empty input has zero logical partitions and must create no application state",
        );
    });
}

#[test]
fn test_json_aggregate_names_respect_scalar_aggregate_and_window_overrides() {
    asupersync::test_utils::run_test(|| async {
        let scalar = open_mem().await;
        seed_precedence_values(&scalar).await;
        scalar.register_deterministic_scalar_function(TaggedScalar {
            name: "json_group_array",
            num_args: 1,
            tag: 160_000,
        });
        assert_eq!(
            query_integer_column(
                &scalar,
                "SELECT json_group_array(v) FROM udf_precedence_values ORDER BY v",
            )
            .await,
            vec![160_001, 160_002, 160_003],
            "a scalar json_group_array/1 must outrank the extension aggregate",
        );
        scalar.register_deterministic_scalar_function(TaggedScalar {
            name: "json_group_array",
            num_args: 2,
            tag: 165_000,
        });
        assert_eq!(
            query_integer_column(
                &scalar,
                "SELECT json_group_array(v, v + 10) \
                 FROM udf_precedence_values ORDER BY v",
            )
            .await,
            vec![165_012, 165_014, 165_016],
            "application resolution must precede JSON's built-in-only arity validation",
        );

        let aggregate = open_mem().await;
        seed_precedence_values(&aggregate).await;
        aggregate.register_aggregate_function(TaggedAggregate {
            name: "json_group_object",
            num_args: 2,
            tag: 170_000,
        });
        assert_eq!(
            query_integer_column(
                &aggregate,
                "SELECT json_group_object(v, v + 10) FROM udf_precedence_values",
            )
            .await,
            vec![170_003],
            "an aggregate json_group_object/2 must outrank the extension aggregate",
        );

        let window = open_mem().await;
        seed_precedence_values(&window).await;
        window.register_window_function(TaggedWindow {
            name: "json_group_array",
            num_args: 1,
            tag: 180_000,
        });
        assert_eq!(
            query_integer_column(
                &window,
                "SELECT json_group_array(v) FROM udf_precedence_values",
            )
            .await,
            vec![180_006],
            "a window json_group_array/1 must retain its custom aggregate-call behavior",
        );
        assert_eq!(
            query_integer_column(
                &window,
                "SELECT json_group_array(v) OVER (\
                     ORDER BY v ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW\
                 ) FROM udf_precedence_values ORDER BY v",
            )
            .await,
            vec![180_001, 180_003, 180_006],
            "a window json_group_array/1 must use its custom OVER lifecycle",
        );
    });
}

#[test]
fn test_json_operators_resolve_their_sql_visible_application_names() {
    asupersync::test_utils::run_test(|| async {
        let builtin = open_mem().await;
        assert_eq!(
            query_first_text(&builtin, r#"SELECT json_array('{"a":1}' -> '$')"#,).await,
            r#"[{"a":1}]"#,
            "the built-in -> result must retain the JSON subtype when nested",
        );
        builtin
            .execute("CREATE TABLE udf_json_scan (doc TEXT NOT NULL, v INTEGER NOT NULL)")
            .await
            .expect("create JSON scan-dependency table");
        builtin
            .execute(r#"INSERT INTO udf_json_scan VALUES ('{"a":7}', 1), ('{"a":7}', 2)"#)
            .await
            .expect("seed JSON scan-dependency table");
        let aggregate_json = builtin
            .prepare("SELECT doc ->> '$.a', sum(v) FROM udf_json_scan")
            .await
            .expect("prepare aggregate JSON scan query")
            .query_row()
            .await
            .expect("execute aggregate JSON scan query");
        assert_eq!(
            aggregate_json.values(),
            &[SqliteValue::Integer(7), SqliteValue::Integer(3)],
            "JSON operands in aggregate projections must retain their scan dependency",
        );

        let conn = open_mem().await;
        conn.register_deterministic_scalar_function(PositionalTaggedScalar {
            name: "->",
            tag: 181_000,
        });
        conn.register_deterministic_scalar_function(PositionalTaggedScalar {
            name: "->>",
            tag: 182_000,
        });

        assert_eq!(
            query_first_int(&conn, "SELECT '{}' -> '$'").await,
            181_000,
            "FROM-less operator lowering must resolve an application ->/2",
        );
        assert_eq!(
            query_first_int(&conn, "SELECT '{}' ->> '$'").await,
            182_000,
            "FROM-less operator lowering must resolve an application ->>/2",
        );

        conn.execute("CREATE TABLE udf_json_operator (v TEXT NOT NULL)")
            .await
            .expect("create JSON operator table");
        conn.execute("INSERT INTO udf_json_operator VALUES ('{}')")
            .await
            .expect("seed JSON operator table");
        assert_eq!(
            query_first_int(&conn, "SELECT v -> '$' FROM udf_json_operator").await,
            181_000,
            "table-backed codegen must resolve the SQL-visible operator identity",
        );
        let prepared = conn
            .prepare("SELECT v ->> '$' FROM udf_json_operator")
            .await
            .expect("prepare overridden JSON operator");
        assert_eq!(
            prepared
                .query_row()
                .await
                .expect("execute prepared JSON operator")
                .get(0),
            Some(&SqliteValue::Integer(182_000)),
            "prepared execution must retain the application ->>/2 binding",
        );

        conn.execute("CREATE TABLE udf_json_outer (v INTEGER NOT NULL)")
            .await
            .expect("create correlated JSON outer table");
        conn.execute("INSERT INTO udf_json_outer VALUES (1), (2), (3)")
            .await
            .expect("seed correlated JSON outer table");
        conn.execute("CREATE TABLE udf_json_inner (k INTEGER NOT NULL)")
            .await
            .expect("create correlated JSON inner table");
        conn.execute("INSERT INTO udf_json_inner VALUES (1)")
            .await
            .expect("seed correlated JSON inner table");
        const CORRELATED_SQL: &str = "SELECT \
             (SELECT o.v -> 10 FROM udf_json_inner), \
             (SELECT 10 -> o.v FROM udf_json_inner) \
             FROM udf_json_outer AS o ORDER BY o.v";
        let prepared = conn
            .prepare(CORRELATED_SQL)
            .await
            .expect("prepare correlated JSON operator query");
        let rows = prepared
            .query()
            .await
            .expect("execute prepared correlated JSON operator query");
        assert_eq!(
            rows.iter()
                .map(|row| row.values().to_vec())
                .collect::<Vec<_>>(),
            vec![
                vec![SqliteValue::Integer(181_110), SqliteValue::Integer(182_001),],
                vec![SqliteValue::Integer(181_210), SqliteValue::Integer(182_002),],
                vec![SqliteValue::Integer(181_310), SqliteValue::Integer(182_003),],
            ],
            "prepared correlated evaluation must preserve outer references in both JSON operands",
        );
    });
}

#[test]
fn test_application_json_operator_validates_and_receives_argument_collation() {
    struct JsonOperatorCollation;

    impl CollationFunction for JsonOperatorCollation {
        fn name(&self) -> &str {
            "UDF_JSON_OPERATOR"
        }

        fn compare(&self, left: &[u8], right: &[u8]) -> std::cmp::Ordering {
            left.cmp(right)
        }
    }

    struct CollationAwareJsonOperator;

    impl ScalarFunction for CollationAwareJsonOperator {
        fn invoke(&self, _args: &[SqliteValue]) -> fsqlite_error::Result<SqliteValue> {
            Ok(SqliteValue::Integer(0))
        }

        fn consumes_argument_collation(&self) -> bool {
            true
        }

        fn invoke_with_collation(
            &self,
            _args: &[SqliteValue],
            collation: Option<&dyn CollationFunction>,
        ) -> fsqlite_error::Result<SqliteValue> {
            Ok(SqliteValue::Integer(i64::from(
                collation.is_some_and(|value| value.name() == "UDF_JSON_OPERATOR"),
            )))
        }

        fn num_args(&self) -> i32 {
            2
        }

        fn name(&self) -> &str {
            "->"
        }
    }

    asupersync::test_utils::run_test(|| async {
        let conn = open_mem().await;
        conn.register_collation_function(JsonOperatorCollation);
        conn.register_deterministic_scalar_function(CollationAwareJsonOperator);
        conn.execute(
            "CREATE TABLE udf_json_operator_collation \
             (document TEXT COLLATE UDF_JSON_OPERATOR)",
        )
        .await
        .expect("create JSON-operator collation table");
        conn.execute("INSERT INTO udf_json_operator_collation VALUES ('{}')")
            .await
            .expect("seed JSON-operator collation table");
        assert_eq!(
            query_first_int(
                &conn,
                "SELECT document -> '$' FROM udf_json_operator_collation",
            )
            .await,
            1,
            "application-owned JSON operators must receive the defining argument collation",
        );

        let error = conn
            .prepare("SELECT ('{}' COLLATE missing_json_operator) -> '$'")
            .await
            .expect_err("a consumed JSON-operator collation must resolve during prepare");
        let message = error.to_string().to_ascii_lowercase();
        assert!(
            message.contains("collation") && message.contains("missing_json_operator"),
            "unexpected missing JSON-operator collation error: {error}",
        );

        conn.execute("CREATE TABLE udf_json_frame_collation (v INTEGER NOT NULL)")
            .await
            .expect("create JSON frame-collation table");
        conn.execute("INSERT INTO udf_json_frame_collation VALUES (1)")
            .await
            .expect("seed JSON frame-collation table");
        let _prepared = conn
            .prepare(
                "SELECT sum(v) OVER (\
                     ORDER BY v ROWS BETWEEN \
                     ('{}' ->> (0 = (0 COLLATE missing_json_frame))) PRECEDING \
                     AND CURRENT ROW\
                 ) FROM udf_json_frame_collation",
            )
            .await
            .expect("collations inside an opaque JSON frame expression stay unresolved");

        let error = conn
            .prepare(
                "SELECT sum(v) OVER (\
                     ORDER BY v ROWS BETWEEN \
                     (0 = (0 COLLATE missing_direct_frame)) PRECEDING \
                     AND CURRENT ROW\
                 ) FROM udf_json_frame_collation",
            )
            .await
            .expect_err("a direct frame comparison must still resolve its collation");
        let message = error.to_string().to_ascii_lowercase();
        assert!(
            message.contains("collation") && message.contains("missing_direct_frame"),
            "unexpected direct frame-collation error: {error}",
        );
    });
}

#[test]
fn test_application_scalars_disable_only_their_matching_record_shortcuts() {
    asupersync::test_utils::run_test(|| async {
        let conn = open_mem().await;
        conn.execute("CREATE TABLE udf_record_shortcuts (v TEXT NOT NULL)")
            .await
            .expect("create record-shortcut table");
        conn.execute("INSERT INTO udf_record_shortcuts VALUES ('12345')")
            .await
            .expect("seed record-shortcut table");
        conn.register_deterministic_scalar_function(TaggedScalar {
            name: "substr",
            num_args: 3,
            tag: 183_000,
        });
        conn.register_deterministic_scalar_function(TaggedScalar {
            name: "substring",
            num_args: 3,
            tag: 184_000,
        });
        conn.register_deterministic_scalar_function(TaggedScalar {
            name: "octet_length",
            num_args: 1,
            tag: 185_000,
        });

        assert_eq!(
            query_first_int(&conn, "SELECT substr(v, 1, 2) FROM udf_record_shortcuts").await,
            195_348,
            "ColumnSubstrPrefix must not bypass an application substr/3",
        );
        assert_eq!(
            query_first_int(&conn, "SELECT substring(v, 1, 2) FROM udf_record_shortcuts",).await,
            196_348,
            "ColumnSubstrPrefix must not bypass an application substring/3",
        );
        assert_eq!(
            query_first_int(&conn, "SELECT octet_length(v) FROM udf_record_shortcuts").await,
            197_345,
            "ColumnOctetLength must not bypass an application octet_length/1",
        );
        let prepared = conn
            .prepare("SELECT substr(v, 1, 2) FROM udf_record_shortcuts")
            .await
            .expect("prepare overridden substr/3");
        assert_eq!(
            prepared
                .query_row()
                .await
                .expect("execute prepared substr/3")
                .get(0),
            Some(&SqliteValue::Integer(195_348)),
        );
        assert_eq!(
            query_first_int(
                &conn,
                "SELECT (SELECT substring(o.v, 1, 2)) \
                 FROM udf_record_shortcuts AS o",
            )
            .await,
            196_348,
            "correlated fallback codegen must not bypass an application substring/3",
        );
    });
}

#[test]
fn test_application_bm25_scalar_runs_outside_fts_context() {
    asupersync::test_utils::run_test(|| async {
        let conn = open_mem().await;
        seed_precedence_values(&conn).await;
        conn.register_deterministic_scalar_function(TaggedScalar {
            name: "bm25",
            num_args: 1,
            tag: 190_000,
        });

        assert_eq!(
            query_integer_column(
                &conn,
                "SELECT bm25(v) FROM udf_precedence_values ORDER BY v",
            )
            .await,
            vec![190_001, 190_002, 190_003],
            "an application bm25/1 must run normally when no FTS table context is active",
        );
    });
}

#[test]
fn test_fromless_count_zero_uses_application_scalar_in_direct_and_prepared_execution() {
    asupersync::test_utils::run_test(|| async {
        let conn = open_mem().await;
        conn.register_deterministic_scalar_function(TaggedScalar {
            name: "count",
            num_args: 0,
            tag: 191_000,
        });

        assert_eq!(
            query_first_int(&conn, "SELECT count()").await,
            191_000,
            "the FROM-less COUNT shortcut must not bypass an application count/0 scalar",
        );
        let prepared = conn
            .prepare("SELECT count()")
            .await
            .expect("prepare application count/0");
        assert_eq!(
            prepared
                .query_row()
                .await
                .expect("execute prepared count/0")
                .get(0),
            Some(&SqliteValue::Integer(191_000)),
            "prepared execution must resolve the application count/0 scalar",
        );
    });
}

#[test]
fn test_having_fts_aux_names_respect_application_scalar_precedence() {
    asupersync::test_utils::run_test(|| async {
        let conn = open_mem().await;
        seed_precedence_values(&conn).await;
        conn.register_deterministic_scalar_function(TaggedScalar {
            name: "highlight",
            num_args: 3,
            tag: 192_000,
        });
        conn.register_deterministic_scalar_function(TaggedScalar {
            name: "snippet",
            num_args: 6,
            tag: 193_000,
        });

        assert_eq!(
            query_integer_column(
                &conn,
                "SELECT count(*) FROM udf_precedence_values GROUP BY v \
                 HAVING highlight(v, v, v) > 0 \
                    AND snippet(v, v, v, v, v, v) > 0 ORDER BY v",
            )
            .await,
            vec![1, 1, 1],
            "HAVING must resolve application highlight/snippet before the FTS-only fallback",
        );
    });
}

#[test]
fn test_nested_application_aggregates_in_modifiers_and_wrappers_are_rejected() {
    asupersync::test_utils::run_test(|| async {
        let conn = open_mem().await;
        seed_precedence_values(&conn).await;
        conn.register_aggregate_function(TaggedAggregate {
            name: "nested_app",
            num_args: 1,
            tag: 194_000,
        });

        for sql in [
            "SELECT sum(v) FILTER (WHERE nested_app(v)) FROM udf_precedence_values",
            "SELECT sum(v ORDER BY nested_app(v)) FROM udf_precedence_values",
            "SELECT sum(nested_app(v) IS NULL) FROM udf_precedence_values",
            "SELECT sum(nested_app(v) BETWEEN 0 AND 1) FROM udf_precedence_values",
            "SELECT sum(nested_app(v) IN (1)) FROM udf_precedence_values",
            "SELECT sum(CAST(nested_app(v) AS TEXT) LIKE '%') \
             FROM udf_precedence_values",
            "SELECT sum('{}' -> nested_app(v)) FROM udf_precedence_values",
        ] {
            let error = conn
                .query(sql)
                .await
                .expect_err("an aggregate nested in an aggregate call must be rejected");
            let message = error.to_string().to_ascii_lowercase();
            assert!(
                message.contains("aggregate") && message.contains("misuse"),
                "unexpected nested-aggregate error for `{sql}`: {error}",
            );
        }
    });
}

#[test]
fn test_pattern_operators_fail_closed_for_non_scalar_application_owners() {
    asupersync::test_utils::run_test(|| async {
        async fn assert_owner_error(conn: &Connection, sql: &str, function_name: &str) {
            let error = conn
                .query(sql)
                .await
                .expect_err("a non-scalar application owner must not fall through to a built-in");
            let message = error.to_string().to_ascii_lowercase();
            assert!(
                message.contains(function_name)
                    && (message.contains("aggregate") || message.contains("window")),
                "unexpected {function_name} owner error: {error}",
            );
        }

        let aggregate_like = open_mem().await;
        seed_precedence_values(&aggregate_like).await;
        aggregate_like.register_aggregate_function(TaggedAggregate {
            name: "like",
            num_args: 2,
            tag: 200_000,
        });
        assert_owner_error(
            &aggregate_like,
            "SELECT v FROM udf_precedence_values WHERE CAST(v AS TEXT) LIKE '1%'",
            "like",
        )
        .await;

        let window_like = open_mem().await;
        seed_precedence_values(&window_like).await;
        window_like.register_window_function(TaggedWindow {
            name: "like",
            num_args: 2,
            tag: 210_000,
        });
        assert_owner_error(
            &window_like,
            "SELECT v FROM udf_precedence_values WHERE CAST(v AS TEXT) LIKE '1%'",
            "like",
        )
        .await;

        let aggregate_glob = open_mem().await;
        seed_precedence_values(&aggregate_glob).await;
        aggregate_glob.register_aggregate_function(TaggedAggregate {
            name: "glob",
            num_args: 2,
            tag: 220_000,
        });
        assert_owner_error(
            &aggregate_glob,
            "SELECT v FROM udf_precedence_values WHERE CAST(v AS TEXT) GLOB '1*'",
            "glob",
        )
        .await;

        let window_glob = open_mem().await;
        seed_precedence_values(&window_glob).await;
        window_glob.register_window_function(TaggedWindow {
            name: "glob",
            num_args: 2,
            tag: 230_000,
        });
        assert_owner_error(
            &window_glob,
            "SELECT v FROM udf_precedence_values WHERE CAST(v AS TEXT) GLOB '1*'",
            "glob",
        )
        .await;
    });
}

#[test]
fn test_pattern_operator_scalar_honors_collation_and_schema_metadata() {
    struct MarkerCollation;

    impl CollationFunction for MarkerCollation {
        fn name(&self) -> &str {
            "UDF_PATTERN"
        }

        fn compare(&self, left: &[u8], right: &[u8]) -> std::cmp::Ordering {
            left.cmp(right)
        }
    }

    struct CollationAwareLike {
        observed: Arc<Mutex<Vec<String>>>,
    }

    impl ScalarFunction for CollationAwareLike {
        fn invoke(&self, _args: &[SqliteValue]) -> fsqlite_error::Result<SqliteValue> {
            self.observed.lock().unwrap().push("NONE".to_owned());
            Ok(SqliteValue::Integer(0))
        }

        fn consumes_argument_collation(&self) -> bool {
            true
        }

        fn invoke_with_collation(
            &self,
            _args: &[SqliteValue],
            collation: Option<&dyn CollationFunction>,
        ) -> fsqlite_error::Result<SqliteValue> {
            let name = collation.map_or("NONE", |collation| collation.name());
            self.observed.lock().unwrap().push(name.to_owned());
            Ok(SqliteValue::Integer(i64::from(name == "UDF_PATTERN")))
        }

        fn num_args(&self) -> i32 {
            2
        }

        fn name(&self) -> &str {
            "like"
        }
    }

    asupersync::test_utils::run_test(|| async {
        let conn = open_mem().await;
        conn.register_collation_function(MarkerCollation);
        conn.execute("CREATE TABLE udf_pattern_collation (v TEXT COLLATE UDF_PATTERN)")
            .await
            .expect("create collation-bearing table");
        conn.execute("INSERT INTO udf_pattern_collation VALUES ('source')")
            .await
            .expect("seed collation-bearing table");
        conn.execute("CREATE TABLE udf_pattern_join_anchor (k INTEGER NOT NULL)")
            .await
            .expect("create pattern join anchor");
        conn.execute("INSERT INTO udf_pattern_join_anchor VALUES (1)")
            .await
            .expect("seed pattern join anchor");

        let observed = Arc::new(Mutex::new(Vec::new()));
        conn.register_deterministic_scalar_function(CollationAwareLike {
            observed: Arc::clone(&observed),
        });

        assert_eq!(
            query_first_int(&conn, "SELECT v LIKE 'pattern' FROM udf_pattern_collation",).await,
            1,
            "ordinary VDBE codegen must attach the source column's collation",
        );
        let prepared = conn
            .prepare("SELECT v LIKE 'pattern' FROM udf_pattern_collation")
            .await
            .expect("prepare collation-consuming application LIKE");
        assert_eq!(
            prepared
                .query_row()
                .await
                .expect("execute prepared application LIKE")
                .get(0),
            Some(&SqliteValue::Integer(1)),
            "prepared VDBE execution must retain the selected argument collation",
        );
        assert_eq!(
            query_first_int(
                &conn,
                "SELECT (SELECT v LIKE 'pattern') FROM udf_pattern_collation",
            )
            .await,
            1,
            "the async subquery evaluator must pass the source column's collation",
        );
        let prepared = conn
            .prepare("SELECT (SELECT v LIKE 'pattern') FROM udf_pattern_collation")
            .await
            .expect("prepare correlated collation-consuming application LIKE");
        assert_eq!(
            prepared
                .query_row()
                .await
                .expect("execute prepared correlated application LIKE")
                .get(0),
            Some(&SqliteValue::Integer(1)),
            "prepared correlated evaluation must retain the outer column's declared collation",
        );
        assert_eq!(
            query_first_int(
                &conn,
                "SELECT p.v LIKE 'pattern' FROM udf_pattern_collation AS p \
                 CROSS JOIN udf_pattern_join_anchor AS a",
            )
            .await,
            1,
            "the synchronous JOIN evaluator must pass the source column's collation",
        );
        assert_eq!(
            query_first_int(
                &conn,
                "SELECT count(*) FROM udf_pattern_collation GROUP BY v \
                 HAVING v LIKE 'pattern'",
            )
            .await,
            1,
            "the HAVING evaluator must pass the source column's collation",
        );
        conn.execute(
            "CREATE TABLE udf_pattern_upsert \
             (id INTEGER PRIMARY KEY, v TEXT, matched INTEGER)",
        )
        .await
        .expect("create pattern UPSERT target");
        conn.execute("INSERT INTO udf_pattern_upsert VALUES (1, 'old', 0)")
            .await
            .expect("seed pattern UPSERT target");
        conn.execute(
            "INSERT INTO udf_pattern_upsert VALUES (1, 'new', 0) \
             ON CONFLICT(id) DO UPDATE SET matched = \
                 excluded.v COLLATE UDF_PATTERN LIKE 'pattern'",
        )
        .await
        .expect("execute collation-consuming application LIKE in UPSERT");
        assert_eq!(
            query_first_int(&conn, "SELECT matched FROM udf_pattern_upsert WHERE id = 1",).await,
            1,
            "UPSERT codegen must attach an explicit application-function collation",
        );
        let observations = observed.lock().unwrap();
        assert!(
            observations.len() >= 6 && observations.iter().all(|name| name == "UDF_PATTERN"),
            "every application LIKE dispatch must receive UDF_PATTERN, got {observations:?}",
        );
        drop(observations);

        let schema_conn = open_mem().await;
        schema_conn
            .execute("CREATE TABLE udf_pattern_schema (v TEXT)")
            .await
            .expect("create schema-safety table");
        schema_conn
            .execute(
                "CREATE INDEX udf_pattern_schema_idx ON udf_pattern_schema(v) \
                 WHERE v LIKE 'safe%'",
            )
            .await
            .expect("create index under the deterministic built-in LIKE");
        schema_conn.register_nondeterministic_scalar_function(CollationAwareLike {
            observed: Arc::new(Mutex::new(Vec::new())),
        });
        let error = schema_conn
            .execute("INSERT INTO udf_pattern_schema VALUES ('safe-value')")
            .await
            .expect_err("index maintenance must reject a non-deterministic LIKE replacement");
        let message = error.to_string().to_ascii_lowercase();
        assert!(
            message.contains("non-deterministic") && message.contains("like"),
            "unexpected application LIKE schema-safety error: {error}",
        );
    });
}

#[test]
fn test_correlated_outer_values_preserve_declared_collation_and_affinity() {
    asupersync::test_utils::run_test(|| async {
        let conn = open_mem().await;
        conn.execute(
            "CREATE TABLE udf_bound_outer_metadata (\
                 nocase_v TEXT COLLATE NOCASE, \
                 text_v TEXT, \
                 numeric_v NUMERIC, \
                 binary_v TEXT COLLATE BINARY, \
                 right_nocase_v TEXT COLLATE NOCASE\
             )",
        )
        .await
        .expect("create bound-outer metadata table");
        conn.execute("INSERT INTO udf_bound_outer_metadata VALUES ('A', '01', '01', 'A', 'a')")
            .await
            .expect("seed bound-outer metadata table");

        const METADATA_SQL: &str = "SELECT \
             (SELECT nocase_v = ('a' COLLATE RTRIM)), \
             (SELECT text_v = 1), \
             (SELECT numeric_v = 1), \
             (SELECT binary_v = right_nocase_v) \
             FROM udf_bound_outer_metadata";
        let expected = vec![
            SqliteValue::Integer(0),
            SqliteValue::Integer(0),
            SqliteValue::Integer(1),
            SqliteValue::Integer(0),
        ];
        let direct = conn
            .query(METADATA_SQL)
            .await
            .expect("execute direct bound-outer metadata query");
        assert_eq!(direct.len(), 1);
        assert_eq!(
            direct[0].values(),
            expected.as_slice(),
            "explicit collation precedence, affinity, and a declared BINARY winner must survive correlation",
        );
        let prepared = conn
            .prepare(METADATA_SQL)
            .await
            .expect("prepare bound-outer metadata query");
        assert_eq!(
            prepared
                .query_row()
                .await
                .expect("execute prepared bound-outer metadata query")
                .values(),
            expected.as_slice(),
            "prepared correlation must retain the same declared metadata",
        );

        conn.execute("CREATE TABLE udf_bound_using_left (k TEXT COLLATE BINARY)")
            .await
            .expect("create canonical USING left table");
        conn.execute("CREATE TABLE udf_bound_using_right (k TEXT COLLATE NOCASE)")
            .await
            .expect("create skipped USING right table");
        conn.execute("INSERT INTO udf_bound_using_right VALUES ('a')")
            .await
            .expect("seed skipped USING right table");
        const USING_SQL: &str = "SELECT \
             (SELECT k = 'A'), \
             (SELECT r.k = 'A') \
             FROM udf_bound_using_left AS l \
             FULL JOIN udf_bound_using_right AS r USING (k)";
        let using_expected = vec![SqliteValue::Integer(0), SqliteValue::Integer(1)];
        let direct = conn
            .query(USING_SQL)
            .await
            .expect("execute direct USING-provenance query");
        assert_eq!(direct.len(), 1);
        assert_eq!(
            direct[0].values(),
            using_expected.as_slice(),
            "an unqualified USING value must coalesce from the right while retaining canonical left metadata",
        );
        let prepared = conn
            .prepare(USING_SQL)
            .await
            .expect("prepare USING-provenance query");
        assert_eq!(
            prepared
                .query_row()
                .await
                .expect("execute prepared USING-provenance query")
                .values(),
            using_expected.as_slice(),
            "qualified and canonical USING metadata must remain distinct after prepare",
        );
    });
}

#[test]
fn test_ordered_group_concat_aliases_pass_all_arguments_to_application_functions() {
    asupersync::test_utils::run_test(|| async {
        let aggregate = open_mem().await;
        seed_ordered_values(&aggregate).await;
        aggregate.register_aggregate_function(OrderedArgsFunction {
            name: "group_concat",
        });
        assert_eq!(
            query_integer_column(
                &aggregate,
                "SELECT group_concat(v, marker ORDER BY ord) \
                 FROM udf_ordered_values GROUP BY grp",
            )
            .await,
            vec![172_839],
            "the single-table path must sort first and pass both application arguments",
        );

        let aggregate_join = open_mem().await;
        seed_ordered_values(&aggregate_join).await;
        aggregate_join.register_aggregate_function(OrderedArgsFunction { name: "string_agg" });
        assert_eq!(
            query_integer_column(
                &aggregate_join,
                "SELECT string_agg(v, marker ORDER BY ord DESC) \
                 FROM udf_ordered_values \
                 JOIN udf_ordered_anchor ON udf_ordered_anchor.id = grp \
                 GROUP BY udf_ordered_anchor.id",
            )
            .await,
            vec![392_817],
            "the grouped-join path must sort first and pass both application arguments",
        );

        let window = open_mem().await;
        seed_ordered_values(&window).await;
        window.register_window_function(OrderedArgsFunction {
            name: "group_concat",
        });
        assert_eq!(
            query_integer_column(
                &window,
                "SELECT group_concat(v, marker ORDER BY ord) \
                 FROM udf_ordered_values GROUP BY grp",
            )
            .await,
            vec![172_839],
            "the single-table path must preserve a window registration's aggregate-call form",
        );

        let window_join = open_mem().await;
        seed_ordered_values(&window_join).await;
        window_join.register_window_function(OrderedArgsFunction { name: "string_agg" });
        assert_eq!(
            query_integer_column(
                &window_join,
                "SELECT string_agg(v, marker ORDER BY ord DESC) \
                 FROM udf_ordered_values \
                 JOIN udf_ordered_anchor ON udf_ordered_anchor.id = grp \
                 GROUP BY udf_ordered_anchor.id",
            )
            .await,
            vec![392_817],
            "the grouped-join path must preserve a window registration's aggregate-call form",
        );
    });
}
