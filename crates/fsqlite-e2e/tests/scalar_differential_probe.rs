//! Differential scalar/expression probe: run a broad battery of `SELECT
//! quote(<expr>)` through both the fsqlite engine and the rusqlite C oracle and
//! report any divergence. `quote()` normalizes rendering (NULL / integer / real
//! / 'text' / X'blob') so a mismatch is a real semantic difference, not a
//! display artifact. Used to surface fix-in-one-turn leaf bugs when the ready
//! backlog is gated. Divergences are printed and the test fails so they are not
//! silently ignored; known-benign classes can be added to `IGNORE`.

use asupersync::runtime::{Runtime, RuntimeBuilder};
use fsqlite::Connection;

fn rt() -> Runtime {
    RuntimeBuilder::current_thread().build().expect("runtime")
}

/// Every probe expression. Kept deliberately broad across string / numeric /
/// type / null / bitwise / datetime-free surfaces.
const EXPRS: &[&str] = &[
    // string functions
    "substr('hello world', 4)",
    "substr('hello world', -5, 3)",
    "substr('abc', 0)",
    "substr('abc', 2, -1)",
    "replace('aaa', 'a', 'bb')",
    "replace('abc', '', 'x')",
    "trim('  xx  ')",
    "trim('xxhelloxx', 'x')",
    "ltrim('  a')",
    "rtrim('a  ')",
    "instr('hello', 'l')",
    "instr('hello', 'z')",
    "instr('', 'a')",
    "length('héllo')",
    "length(x'0102')",
    "upper('groß')",
    "lower('GROSS')",
    "hex('abc')",
    "hex(x'00ff')",
    "unicode('A')",
    "unicode('é')",
    "char(65, 66, 67)",
    "char(233)",
    "quote('a''b')",
    "quote(x'deadbeef')",
    "printf('%d-%s', 5, 'x')",
    "printf('%.3f', 2.5)",
    "printf('%5d', 42)",
    "printf('%x', 255)",
    "printf('%c', 65)",
    "printf('%%')",
    "format('%d', 10)",
    "concat('a', 2, NULL, 'b')",
    "concat_ws('-', 'a', NULL, 'b')",
    // numeric
    "round(2.5)",
    "round(3.14159, 2)",
    "round(-2.5)",
    "round(0.5)",
    "abs(-9223372036854775807)",
    "abs(-3.5)",
    "3 / 2",
    "3.0 / 2",
    "7 % 3",
    "-7 % 3",
    "7 % -3",
    "5 / 0",
    "5 % 0",
    "9223372036854775807 + 1",
    "2 * 3.5",
    "cast(3.99 as integer)",
    "cast(-3.99 as integer)",
    "cast('12abc' as integer)",
    "cast('  42  ' as integer)",
    "cast('3.14xyz' as real)",
    "cast('0x1F' as integer)",
    "cast(1e300 * 1e300 as text)",
    // bitwise / logic
    "5 & 3",
    "5 | 2",
    "~0",
    "1 << 62",
    "-1 >> 1",
    "5 = 5.0",
    "'10' < '9'",
    "10 < '9'",
    "NULL = NULL",
    "NULL IS NULL",
    // type / null
    "typeof(1)",
    "typeof(1.0)",
    "typeof('x')",
    "typeof(x'00')",
    "typeof(NULL)",
    "typeof(1 + 1.0)",
    "coalesce(NULL, NULL, 3)",
    "nullif(5, 5)",
    "nullif(5, 6)",
    "ifnull(NULL, 'd')",
    "max(3, 1, 2)",
    "min('b', 'a', 'c')",
    "iif(1, 'y', 'n')",
    "iif(0, 'y', 'n')",
    "sign(-4)",
    "sign(0)",
    "abs(NULL)",
    // like / glob
    "'abc' LIKE 'a%'",
    "'ABC' LIKE 'abc'",
    "'a_c' LIKE 'a\\_c' ESCAPE '\\'",
    "'abc' GLOB 'a[b-d]c'",
    "'abc' GLOB 'A*'",
    // printf format specifiers (higher divergence potential)
    "printf('%+d', 5)",
    "printf('% d', 5)",
    "printf('%05.2f', 3.14159)",
    "printf('%-5d|', 42)",
    "printf('%e', 12345.678)",
    "printf('%g', 0.0001)",
    "printf('%g', 100000.0)",
    "printf('%#x', 255)",
    "printf('%o', 64)",
    "printf('%5.3s', 'abcdef')",
    "printf('%!5d', 42)",
    "printf('%d %d', 1)",
    "printf('%,d', 1234567)",
    "printf('%q', 'a''b')",
    "printf('%Q', 'a''b')",
    "printf('%w', 'a\"b')",
    // fixed-input date/time
    "date('2024-02-29', '+1 year')",
    "date('2024-01-31', '+1 month')",
    "datetime('2024-01-15 12:30:45', '+90 minutes')",
    "strftime('%Y-%m-%d %H:%M:%f', '2024-01-15 12:30:45.678')",
    "strftime('%j', '2024-03-01')",
    "strftime('%w %W', '2024-01-15')",
    "julianday('2000-01-01 12:00:00')",
    "unixepoch('2024-01-01')",
    "date('2024-01-15', 'weekday 0')",
    "date('now', 'start of month') IS NOT NULL",
    "time('12:30:45', '+1 hour')",
    "strftime('%s', '1970-01-01 00:00:01')",
    // extreme numerics
    "-0.0",
    "1.0 / 3.0",
    "0.1 + 0.2",
    "1e-320",
    "9007199254740993",
    "cast(9223372036854775807 as real)",
    "round(2.675, 2)",
    "round(1.005, 2)",
    "2147483647 + 1",
    "-9223372036854775808 / -1",
    "18446744073709551616.0",
    "cast('inf' as real)",
    "cast('nan' as real)",
    "abs(-0.0)",
    // blob / collation / comparison edges
    "x'' = x''",
    "x'01' < x'0100'",
    "'a' < 'B' COLLATE NOCASE",
    "'A' = 'a' COLLATE NOCASE",
    "'abc' = 'abc ' ",
    "cast(1 as text) || cast(2 as text)",
    "'10' + '20'",
    "'3.5' * 2",
    "true",
    "false",
    "0x10 + 1",
    "1 IN (1, 2, 3)",
    "'x' IN ('a', 'x')",
    "NULL IN (1, 2)",
    "1 BETWEEN 0 AND 2",
    "(1, 2) = (1, 2)",
];

fn oracle_quote(conn: &rusqlite::Connection, expr: &str) -> Result<String, String> {
    conn.query_row(&format!("SELECT quote({expr});"), [], |r| {
        r.get::<_, Option<String>>(0)
    })
    .map(|v| v.unwrap_or_else(|| "NULL".to_owned()))
    .map_err(|e| format!("{e}"))
}

#[test]
fn scalar_differential_probe_vs_c_oracle() {
    let oracle = rusqlite::Connection::open_in_memory().unwrap();

    let rt = rt();
    let mismatches = rt.block_on(async {
        let conn = Connection::open(":memory:".to_owned()).await.unwrap();
        let mut out: Vec<String> = Vec::new();
        for expr in EXPRS {
            let sql = format!("SELECT quote({expr});");
            // fsqlite result (as rendered text of quote()).
            let frank = match conn.query(&sql).await {
                Ok(rows) => rows
                    .first()
                    .and_then(|row| row.get(0).map(render_value))
                    .unwrap_or_else(|| "<no-row>".to_owned()),
                Err(e) => format!("<err:{e:?}>"),
            };
            // C-oracle result.
            let c = match oracle_quote(&oracle, expr) {
                Ok(v) => v,
                Err(e) => format!("<err:{e}>"),
            };
            if frank != c {
                out.push(format!("expr=[{expr}]  frank=[{frank}]  c-oracle=[{c}]"));
            }
        }
        conn.close().await.unwrap();
        out
    });

    if !mismatches.is_empty() {
        eprintln!("=== SCALAR DIFFERENTIAL DIVERGENCES ({}) ===", mismatches.len());
        for m in &mismatches {
            eprintln!("{m}");
        }
        panic!("{} scalar divergences vs C oracle (see stderr)", mismatches.len());
    }
}

/// Render an fsqlite `SqliteValue` to a comparable string. `quote()` always
/// yields a TEXT literal, so the Text arm returns it raw (NOT re-wrapped) to
/// match rusqlite's `quote()` String result.
fn render_value(v: &fsqlite::SqliteValue) -> String {
    use fsqlite::SqliteValue;
    match v {
        SqliteValue::Null => "NULL".to_owned(),
        SqliteValue::Integer(i) => i.to_string(),
        SqliteValue::Float(f) => format!("{f}"),
        SqliteValue::Text(t) => t.to_string(),
        SqliteValue::Blob(b) => format!("blob:{}", b.len()),
    }
}
