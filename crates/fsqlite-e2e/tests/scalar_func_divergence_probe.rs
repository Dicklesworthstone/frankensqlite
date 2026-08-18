//! Broad oracle DIVERGENCE PROBE (not a keeper): run a large matrix of scalar
//! SQL expressions through frank and rusqlite (= C SQLite) and print every cell
//! where the two disagree — value-vs-value, or error-vs-value. Used to hunt for
//! fresh, unfiled correctness divergences in leaf scalar functions (date/time,
//! string, math, json) that are NOT blocked on the held connection.rs.
//!
//! `#[ignore]` by default; run with:
//!   cargo test -p fsqlite-e2e --test scalar_func_divergence_probe -- --ignored --nocapture
#![recursion_limit = "512"]

use fsqlite::Connection;
use fsqlite_types::SqliteValue;

fn render_frank(v: &SqliteValue) -> String {
    match v {
        SqliteValue::Null => "NULL".to_owned(),
        SqliteValue::Integer(n) => format!("int:{n}"),
        SqliteValue::Float(f) => format!("real:{f}"),
        SqliteValue::Text(s) => format!("txt:{s}"),
        SqliteValue::Blob(b) => format!("blob:{}", b.len()),
    }
}

async fn frank_eval(conn: &Connection, expr: &str) -> Result<String, String> {
    let sql = format!("SELECT {expr}");
    match conn.query(&sql).await {
        Ok(rs) => {
            let rows: Vec<String> = rs
                .iter()
                .map(|row| {
                    row.values()
                        .iter()
                        .map(render_frank)
                        .collect::<Vec<_>>()
                        .join(",")
                })
                .collect();
            Ok(rows.join(";"))
        }
        Err(e) => Err(e.to_string()),
    }
}

fn sqlite_eval(conn: &rusqlite::Connection, expr: &str) -> Result<String, String> {
    let sql = format!("SELECT {expr}");
    let mut stmt = match conn.prepare(&sql) {
        Ok(s) => s,
        Err(e) => return Err(format!("prep: {e}")),
    };
    let n = stmt.column_count();
    let out = stmt.query_map([], |row| {
        let mut cells = Vec::with_capacity(n);
        for i in 0..n {
            let cell = match row.get_ref(i)? {
                rusqlite::types::ValueRef::Null => "NULL".to_owned(),
                rusqlite::types::ValueRef::Integer(x) => format!("int:{x}"),
                rusqlite::types::ValueRef::Real(f) => format!("real:{f}"),
                rusqlite::types::ValueRef::Text(t) => {
                    format!("txt:{}", String::from_utf8_lossy(t))
                }
                rusqlite::types::ValueRef::Blob(b) => format!("blob:{}", b.len()),
            };
            cells.push(cell);
        }
        Ok(cells.join(","))
    });
    match out {
        Ok(iter) => {
            let rows: Result<Vec<String>, _> = iter.collect();
            match rows {
                Ok(r) => Ok(r.join(";")),
                Err(e) => Err(format!("run: {e}")),
            }
        }
        Err(e) => Err(format!("map: {e}")),
    }
}

/// Normalize float rendering differences that are cosmetic (e.g. 3 vs 3.0).
fn approx_equal(a: &str, b: &str) -> bool {
    if a == b {
        return true;
    }
    // Compare numeric payloads with tolerance for real:/int: rendering.
    let strip = |s: &str| -> Option<f64> {
        s.strip_prefix("real:")
            .or_else(|| s.strip_prefix("int:"))
            .and_then(|x| x.parse::<f64>().ok())
    };
    match (strip(a), strip(b)) {
        (Some(x), Some(y)) => (x - y).abs() < 1e-9 || (x - y).abs() < x.abs().max(y.abs()) * 1e-12,
        _ => false,
    }
}

const EXPRS: &[&str] = &[
    // ---- date/time ----
    "date('2024-02-29','+1 year')",
    "date('2024-01-31','+1 month')",
    "date('2020-01-31','+1 month','+1 month')",
    "date('now','start of month','+1 month','-1 day')",
    "strftime('%Y-%m-%d %H:%M:%f','2024-06-15 12:34:56.789')",
    "strftime('%j','2024-03-01')",
    "strftime('%W','2024-01-01')",
    "strftime('%s','1970-01-01 00:00:01')",
    "julianday('2000-01-01')",
    "julianday('2000-01-01 12:00:00')",
    "time('12:34:56','+1 hour')",
    "datetime('2024-12-31 23:59:59','+2 seconds')",
    "date('2024-06-15','weekday 0')",
    "date('2024-06-15','weekday 6')",
    "strftime('%Y','2024-06-15','+200 days')",
    "date('2024-02-30')",
    "date('2024-13-01')",
    "unixepoch('2024-06-15 00:00:00')",
    "datetime(0,'unixepoch')",
    "strftime('%f','2024-01-01 00:00:00.5')",
    // ---- string ----
    "printf('%5.2f', 3.14159)",
    "printf('%+d', 42)",
    "printf('%x', 255)",
    "printf('%o', 8)",
    "printf('%e', 12345.678)",
    "printf('%g', 0.0001)",
    "printf('%c', 65)",
    "printf('%-10s|', 'hi')",
    "printf('%!5.2f', 3.14159)",
    "printf('%,d', 1234567)",
    "quote('a''b')",
    "quote(x'00ff')",
    "quote(3.14)",
    "char(72,73)",
    "unicode('A')",
    "unicode('\u{1F600}')",
    "substr('hello', -3)",
    "substr('hello', -3, 2)",
    "substr('hello', 0)",
    "substr('hello', 2, -1)",
    "replace('aaa','a','bb')",
    "trim('  x  ')",
    "ltrim('xxhi','x')",
    "instr('hello','l')",
    "instr('hello',NULL)",
    "hex(zeroblob(3))",
    "length(x'0102')",
    "typeof(1/0)",
    "1/0",
    "9223372036854775807 + 1",
    "abs(-9223372036854775808)",
    "round(2.5)",
    "round(3.5)",
    "round(-2.5)",
    "round(2.567, 2)",
    "round(1234.5678, -2)",
    // ---- math (SQLite 3.35+ built-ins) ----
    "power(2, 10)",
    "pow(2, 0.5)",
    "sqrt(2)",
    "log(100)",
    "log(2, 8)",
    "log2(8)",
    "ln(2.718281828459045)",
    "exp(1)",
    "ceil(2.1)",
    "floor(-2.1)",
    "trunc(-2.7)",
    "mod(10, 3)",
    "mod(-10, 3)",
    "sin(0)",
    "acos(2)",
    "atan2(1, 1)",
    "pi()",
    "degrees(pi())",
    "radians(180)",
    "sign(-5)",
    "sign(0)",
    "power(-8, 0.3333333333333333)",
    "sqrt(-1)",
    // ---- misc / cast / coalesce ----
    "cast('12abc' as integer)",
    "cast('  3.5  ' as real)",
    "cast(3.99 as integer)",
    "cast('0x10' as integer)",
    "cast(x'41' as text)",
    "coalesce(NULL, NULL, 3)",
    "nullif(1, 1)",
    "ifnull(NULL, 'x')",
    "iif(1>2, 'a', 'b')",
    "min(3, 1, 2)",
    "max('a', 'b', 'c')",
    "likelihood(1, 0.5)",
    "likely(1)",
    "'a' || NULL",
    "5 & 3",
    "5 | 2",
    "~0",
    "1 << 40",
    "'abc' GLOB 'a[b-d]c'",
    "'ABC' LIKE 'a%'",
    "10 % 3",
    "'5' + '5'",
    "-'3'",
    // ---- json (json1) ----
    "json('{\"a\":1}')",
    "json_extract('{\"a\":[1,2,3]}','$.a[1]')",
    "json_type('[1,2,3]')",
    "json_type('{\"a\":1}','$.a')",
    "json_array(1,2,'x')",
    "json_object('k',1)",
    "json_valid('{bad}')",
    "json_quote(3.14)",
    "json_array_length('[1,2,3]')",
    "json_extract('{\"a\":null}','$.a')",
    "json_insert('{\"a\":1}','$.b',2)",
    "json_remove('[0,1,2]','$[1]')",
    "json_patch('{\"a\":1}','{\"b\":2}')",
];

#[test]
#[ignore = "divergence probe (not a keeper): prints frank-vs-sqlite3 scalar mismatches"]
fn scalar_func_divergence_probe() {
    asupersync::test_utils::run_test(|| async {
        let f = Connection::open(":memory:").await.expect("open frank");
        let r = rusqlite::Connection::open_in_memory().expect("open rusqlite");
        let mut diverged = 0usize;
        let mut both_err = 0usize;
        for expr in EXPRS {
            let fr = frank_eval(&f, expr).await;
            let sr = sqlite_eval(&r, expr);
            match (&fr, &sr) {
                (Ok(a), Ok(b)) => {
                    if !approx_equal(a, b) {
                        diverged += 1;
                        println!("DIVERGE  {expr}\n    frank: {a}\n    csql:  {b}");
                    }
                }
                (Err(_), Err(_)) => {
                    both_err += 1;
                }
                (Ok(a), Err(b)) => {
                    diverged += 1;
                    println!("F-OK/C-ERR  {expr}\n    frank: {a}\n    csql:  <err: {b}>");
                }
                (Err(a), Ok(b)) => {
                    diverged += 1;
                    println!("F-ERR/C-OK  {expr}\n    frank: <err: {a}>\n    csql:  {b}");
                }
            }
        }
        println!(
            "\nPROBE SUMMARY: {} exprs, {} diverged, {} both-error",
            EXPRS.len(),
            diverged,
            both_err
        );
    });
}
