//! bd-7d3ys — Math scalar-function correctness vs documented SQLite semantics.
//!
//! NOTE ON THE ORACLE: this family CANNOT be checked against rusqlite — its
//! bundled SQLite is compiled WITHOUT `SQLITE_ENABLE_MATH_FUNCTIONS`, so
//! `sqrt`/`ln`/`sin`/`pi`/`log`/etc. raise "no such function" there. So instead
//! of a live oracle, the expected values below are SQLite's *documented*
//! results (the C reference build with math functions enabled); frank is checked
//! against those. Covered: ceil/floor/trunc direction + real result type,
//! sqrt/pow/exp exact values, trig at exact points, `pi()`, the base-10
//! single-arg `log()` (a common base-e reimpl trap), two-arg `log(base,x)`,
//! `mod()` as C `fmod` (sign follows the dividend, /0 -> NULL), out-of-domain
//! inputs -> NULL, and NULL-argument propagation. All inputs were chosen to have
//! exact f64 results so the assertions are independent of libm last-bit deltas.
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

/// Evaluate a single-row query and render its columns.
async fn row(conn: &Connection, sql: &str) -> Vec<String> {
    let rows = conn
        .query(sql)
        .await
        .unwrap_or_else(|e| panic!("frank `{sql}`: {e}"));
    assert_eq!(
        rows.len(),
        1,
        "`{sql}` expected exactly one row, got {}",
        rows.len()
    );
    rows[0].values().iter().map(render_frank).collect()
}

/// Assert each (query, expected-rendered-columns) pair.
async fn check(pairs: &[(&str, &[&str])], label: &str) {
    let conn = Connection::open(":memory:").await.expect("open frank");
    let mut failures = Vec::new();
    for (sql, expected) in pairs {
        let got = row(&conn, sql).await;
        let want: Vec<String> = expected.iter().map(|s| (*s).to_owned()).collect();
        if got != want {
            failures.push(format!("{sql}\n  got:  {got:?}\n  want: {want:?}"));
        }
    }
    assert!(
        failures.is_empty(),
        "{label}: {} mismatch(es)\n{}",
        failures.len(),
        failures.join("\n")
    );
}

#[test]
fn math_floor_ceil_trunc() {
    asupersync::test_utils::run_test(|| async {
        check(
            &[
                (
                    "SELECT ceil(2.1), ceil(2.9), ceil(-2.1), ceil(3.0)",
                    &["3", "3", "-2", "3"],
                ),
                ("SELECT ceiling(4.2)", &["5"]),
                (
                    "SELECT floor(2.1), floor(2.9), floor(-2.1)",
                    &["2", "2", "-3"],
                ),
                (
                    "SELECT trunc(2.9), trunc(-2.9), trunc(2.1)",
                    &["2", "-2", "2"],
                ),
                // SQLite returns REAL for these.
                (
                    "SELECT typeof(ceil(2.5)), typeof(floor(2.5)), typeof(trunc(2.5))",
                    &["'real'", "'real'", "'real'"],
                ),
            ],
            "math_floor_ceil_trunc",
        )
        .await;
    });
}

#[test]
fn math_sqrt_pow_exp_exact() {
    asupersync::test_utils::run_test(|| async {
        check(
            &[
                ("SELECT sqrt(16.0), sqrt(25.0), sqrt(0.0)", &["4", "5", "0"]),
                ("SELECT pow(2.0, 10.0), power(3.0, 2.0)", &["1024", "9"]),
                ("SELECT pow(2.0, 0.0), pow(5.0, 1.0)", &["1", "5"]),
                ("SELECT exp(0.0)", &["1"]),
                ("SELECT ln(1.0)", &["0"]),
                (
                    "SELECT typeof(sqrt(16.0)), typeof(pow(2.0,3.0))",
                    &["'real'", "'real'"],
                ),
            ],
            "math_sqrt_pow_exp_exact",
        )
        .await;
    });
}

#[test]
fn math_trig_exact_points_and_pi() {
    asupersync::test_utils::run_test(|| async {
        check(
            &[
                ("SELECT sin(0.0), cos(0.0), tan(0.0)", &["0", "1", "0"]),
                ("SELECT asin(0.0), acos(1.0), atan(0.0)", &["0", "0", "0"]),
                ("SELECT atan2(0.0, 1.0)", &["0"]),
                ("SELECT pi()", &["3.141592653589793"]),
            ],
            "math_trig_exact_points_and_pi",
        )
        .await;
    });
}

#[test]
fn math_log_base_semantics() {
    asupersync::test_utils::run_test(|| async {
        // Single-arg log() is base-10 in SQLite (NOT natural log).
        check(
            &[
                ("SELECT log(100.0)", &["2"]),
                ("SELECT log(1000.0)", &["3"]),
                ("SELECT log10(100.0)", &["2"]),
                ("SELECT log2(8.0)", &["3"]),
                ("SELECT log(2.0, 8.0)", &["3"]), // base-2 of 8
                ("SELECT log(0.0)", &["NULL"]),
                ("SELECT log(-5.0)", &["NULL"]),
            ],
            "math_log_base_semantics",
        )
        .await;
    });
}

#[test]
fn math_mod_is_fmod_sign_of_dividend() {
    asupersync::test_utils::run_test(|| async {
        check(
            &[
                ("SELECT mod(10.0, 3.0), mod(-10.0, 3.0)", &["1", "-1"]),
                ("SELECT mod(10.0, -3.0), mod(-10.0, -3.0)", &["1", "-1"]),
                ("SELECT mod(7.5, 2.0)", &["1.5"]),
                ("SELECT mod(10.0, 0.0)", &["NULL"]), // division by zero -> NULL
            ],
            "math_mod_is_fmod_sign_of_dividend",
        )
        .await;
    });
}

#[test]
fn math_domain_errors_yield_null() {
    asupersync::test_utils::run_test(|| async {
        check(
            &[
                ("SELECT sqrt(-1.0)", &["NULL"]),
                ("SELECT ln(-1.0)", &["NULL"]),
                ("SELECT ln(0.0)", &["NULL"]),
                ("SELECT acos(2.0)", &["NULL"]), // out of [-1,1]
                ("SELECT asin(2.0)", &["NULL"]),
                ("SELECT typeof(sqrt(-1.0))", &["'null'"]),
            ],
            "math_domain_errors_yield_null",
        )
        .await;
    });
}

#[test]
fn math_null_argument_propagates() {
    asupersync::test_utils::run_test(|| async {
        check(
            &[
                (
                    "SELECT sqrt(NULL), ceil(NULL), floor(NULL), exp(NULL)",
                    &["NULL", "NULL", "NULL", "NULL"],
                ),
                ("SELECT pow(NULL, 2.0), pow(2.0, NULL)", &["NULL", "NULL"]),
                ("SELECT mod(NULL, 3.0), mod(10.0, NULL)", &["NULL", "NULL"]),
                ("SELECT typeof(sqrt(NULL))", &["'null'"]),
            ],
            "math_null_argument_propagates",
        )
        .await;
    });
}
