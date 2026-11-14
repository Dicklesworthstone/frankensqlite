#![recursion_limit = "512"]

//! Integer/numeric arithmetic-edge leaf-hunt (pane af49, 2026-08-21): frank vs
//! rusqlite over the corners of SQLite integer math — signed 64-bit overflow
//! promoting to REAL (e.g. 9223372036854775807 + 1), integer division
//! truncation-toward-zero with negative operands, modulo sign (result takes the
//! dividend's sign), division/modulo by zero yielding NULL, unary minus on
//! i64::MIN, hex-literal parsing, bitwise AND/OR/XOR/NOT and shifts (including
//! negative and large shift counts), and INT vs REAL result typing. Scalar
//! results compared via a value tag that distinguishes int from real.
//! Pass = coverage keeper; a mismatch is a leaf divergence.

use fsqlite_core::connection::Connection;
use fsqlite_types::value::SqliteValue;

/// bd-w4pr1.13.21 lever H: an INTEGER, REAL or TEXT literal operand of an
/// arithmetic, concatenation or bitwise operator is loaded once, in the
/// program prologue, and read from there on every row; a left literal is the
/// operator's second source, never its destination. Every operator with a
/// literal on either side (an Int64, a REAL, TEXT, a zero divisor) over 2,000
/// scanned rows of every storage class and NULL, in a projection, an
/// aggregate, a WHERE, an UPDATE and an INSERT ... SELECT, each query run
/// twice on the same connection, on `:memory:` and on a file. A version that
/// wrote a result into the prologue register would give every row after the
/// first a different operand.
#[test]
fn literal_operands_over_scanned_rows_match_rusqlite_oracle() {
    asupersync::test_utils::run_test(|| async {
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("literal_operands.db");
        for path in [":memory:".to_owned(), file.to_str().unwrap().to_owned()] {
            let f = Connection::open(&path).await.unwrap();
            let r = rusqlite::Connection::open_in_memory().unwrap();
            let setup = [
                "CREATE TABLE q(id INTEGER PRIMARY KEY, a, b TEXT, c REAL)",
                "WITH RECURSIVE n(i) AS (SELECT 1 UNION ALL SELECT i + 1 FROM n WHERE i < 2000) \
                 INSERT INTO q(a, b, c) SELECT \
                 CASE i % 6 WHEN 0 THEN NULL WHEN 1 THEN i WHEN 2 THEN i * 0.25 \
                 WHEN 3 THEN 'v' || i WHEN 4 THEN -i ELSE x'0a0b' END, \
                 CASE WHEN i % 5 = 0 THEN NULL ELSE 't' || i END, i * 1.5 FROM n",
                "CREATE TABLE dst(id INTEGER PRIMARY KEY, x INTEGER, y TEXT, z REAL, w)",
            ];
            for statement in setup {
                f.execute(statement).await.unwrap();
                r.execute_batch(statement).unwrap();
            }
            let mut queries = Vec::new();
            for op in ["+", "-", "*", "/", "%", "||", "&", "|", "<<", ">>"] {
                for literal in ["7", "0", "-3", "4294967296", "2.5", "'12'", "'x'"] {
                    queries.push(format!(
                        "SELECT id, a {op} {literal}, typeof(a {op} {literal}), \
                         {literal} {op} a, typeof({literal} {op} a), \
                         b {op} {literal}, {literal} {op} c FROM q ORDER BY id"
                    ));
                }
            }
            queries.push(
                "SELECT count(*), sum(a * 3), total(c * 1.5), sum(length('r-' || b)), \
                 max((id * 7919) % 1000) FROM q"
                    .to_owned(),
            );
            queries.push(
                "SELECT id FROM q WHERE (id * 7919) % 1000 < 10 AND 'k' || b <> 'kt5' ORDER BY id"
                    .to_owned(),
            );
            let mut diffs = Vec::new();
            for q in &queries {
                for round in 0..2 {
                    let fr = fq(&f, q).await;
                    let rr = rq(&r, q);
                    if fr != rr {
                        diffs.push(format!(
                            "  [{path} round {round}] `{q}`\n     frank= {fr:?}\n     stock= {rr:?}"
                        ));
                    }
                }
            }
            let writes = [
                "UPDATE q SET c = c * 2 + 1, b = 'u-' || b WHERE id % 3 = 0",
                "INSERT INTO dst(x, y, z, w) SELECT (id * 7919) % 1000, 'row-' || id, id * 1.5, \
                 a + 1 FROM q",
                "UPDATE dst SET x = 1000 - x, y = y || '!' WHERE id % 7 = 2",
            ];
            let states = [
                "SELECT id, a, b, typeof(b), c FROM q ORDER BY id",
                "SELECT id, x, typeof(x), y, z, typeof(z), w, typeof(w) FROM dst ORDER BY id",
            ];
            for write in writes {
                let fo = f
                    .execute(write)
                    .await
                    .map(|changed| changed.to_string())
                    .map_err(|e| e.to_string());
                let so = r
                    .execute(write, [])
                    .map(|changed| changed.to_string())
                    .map_err(|e| e.to_string());
                if fo.as_ref().ok() != so.as_ref().ok() {
                    diffs.push(format!("  [{path}] `{write}`: frank {fo:?} stock {so:?}"));
                }
                for state in states {
                    let fr = fq(&f, state).await;
                    let rr = rq(&r, state);
                    if fr != rr {
                        diffs.push(format!(
                            "  [{path}] after `{write}`, `{state}`\n     frank= {fr:?}\n     stock= {rr:?}"
                        ));
                    }
                }
            }
            f.close().await.unwrap();
            assert!(
                diffs.is_empty(),
                "{} literal-operand divergence(s) vs rusqlite:\n{}",
                diffs.len(),
                diffs.join("\n")
            );
        }
    });
}

/// bd-w4pr1.13.21 lever K: an arithmetic, `||` or bitwise operand that is a
/// column of a row held in registers (a table-valued function's row in
/// INSERT ... SELECT, the row a CHECK constraint is evaluated over) is read
/// from its register directly instead of being copied first. The source
/// column on both sides and twice, mixed with literals, over generate_series
/// and json_each rows, and a CHECK over two columns of the row being written
/// (OR IGNORE keeps the rows that pass), on `:memory:` and on a file. A
/// version that wrote a result into the source register would change every
/// later read of that column in the same row. rusqlite's bundled SQLite has
/// no generate_series, so stock reads the same rows from a recursive CTE.
#[test]
fn register_operands_over_function_rows_match_rusqlite_oracle() {
    const SERIES_3000: &str = "(WITH RECURSIVE n(value) AS (SELECT 1 UNION ALL \
                               SELECT value + 1 FROM n WHERE value < 3000) SELECT value FROM n)";
    const SERIES_200: &str = "(WITH RECURSIVE n(value) AS (SELECT 1 UNION ALL \
                              SELECT value + 1 FROM n WHERE value < 200) SELECT value FROM n)";
    asupersync::test_utils::run_test(|| async {
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("register_operands.db");
        for path in [":memory:".to_owned(), file.to_str().unwrap().to_owned()] {
            let f = Connection::open(&path).await.unwrap();
            let r = rusqlite::Connection::open_in_memory().unwrap();
            let bulk = "INSERT INTO g SELECT value, value * value - value, value || '-' || value, \
                        value * 1.5 + value, (value * 7919) % 1000 - value % 7, \
                        'row-' || value || value FROM ";
            let check = "INSERT OR IGNORE INTO ck(a, b) SELECT value, value * 2 FROM ";
            // (fsqlite, stock) forms of each statement.
            let statements = [
                (
                    "CREATE TABLE g(id INTEGER PRIMARY KEY, a INTEGER, b TEXT, c REAL, d, e)"
                        .to_owned(),
                    None,
                ),
                (
                    format!("{bulk}generate_series(1, 3000)"),
                    Some(format!("{bulk}{SERIES_3000}")),
                ),
                (
                    "INSERT INTO g(a, b, c, d, e) SELECT key * key + key, value || key, \
                     key - 0.5, key << 2 | key, value || value \
                     FROM json_each('[\"x\", \"yy\", 3, 4.5, null, \"zzz\"]')"
                        .to_owned(),
                    None,
                ),
                (
                    "CREATE TABLE ck(id INTEGER PRIMARY KEY, a INTEGER, b INTEGER, \
                     CHECK (a * 3 > b + 10 AND a - b < a * 2))"
                        .to_owned(),
                    None,
                ),
                (
                    format!("{check}generate_series(1, 200)"),
                    Some(format!("{check}{SERIES_200}")),
                ),
                (
                    "INSERT OR IGNORE INTO ck(a, b) VALUES (5, 1), (20, 3), (-4, -40)".to_owned(),
                    None,
                ),
            ];
            for (statement, stock_statement) in &statements {
                let fo = f
                    .execute(statement)
                    .await
                    .map(|changed| changed.to_string())
                    .map_err(|e| e.to_string());
                let so = r
                    .execute(stock_statement.as_deref().unwrap_or(statement), [])
                    .map(|changed| changed.to_string())
                    .map_err(|e| e.to_string());
                // Stock's `execute` reports the last DML statement's count
                // for a CREATE TABLE, so only an INSERT's count is compared.
                if statement.starts_with("INSERT") {
                    assert_eq!(
                        fo.as_ref().ok(),
                        so.as_ref().ok(),
                        "[{path}] `{statement}`: frank {fo:?} stock {so:?}"
                    );
                } else {
                    assert!(
                        fo.is_ok() && so.is_ok(),
                        "[{path}] `{statement}`: frank {fo:?} stock {so:?}"
                    );
                }
            }
            let mut diffs = Vec::new();
            for q in [
                "SELECT id, a, typeof(a), b, c, typeof(c), d, typeof(d), e FROM g ORDER BY id",
                "SELECT count(*), sum(a), total(c), sum(length(e)) FROM g",
                "SELECT id, a, b FROM ck ORDER BY id",
            ] {
                let fr = fq(&f, q).await;
                let rr = rq(&r, q);
                if fr != rr {
                    diffs.push(format!(
                        "  [{path}] `{q}`\n     frank= {fr:?}\n     stock= {rr:?}"
                    ));
                }
            }
            f.close().await.unwrap();
            assert!(
                diffs.is_empty(),
                "{} register-operand divergence(s) vs rusqlite:\n{}",
                diffs.len(),
                diffs.join("\n")
            );
        }
    });
}

fn tag_f(v: &SqliteValue) -> String {
    match v {
        SqliteValue::Null => "NULL".to_owned(),
        SqliteValue::Integer(n) => format!("int:{n}"),
        SqliteValue::Float(f) => format!("real:{f:?}"),
        SqliteValue::Text(s) => format!("text:{s}"),
        SqliteValue::Blob(b) => format!("blob:{b:?}"),
    }
}
fn tag_r(v: &rusqlite::types::Value) -> String {
    match v {
        rusqlite::types::Value::Null => "NULL".to_owned(),
        rusqlite::types::Value::Integer(n) => format!("int:{n}"),
        rusqlite::types::Value::Real(f) => format!("real:{f:?}"),
        rusqlite::types::Value::Text(s) => format!("text:{s}"),
        rusqlite::types::Value::Blob(b) => format!("blob:{b:?}"),
    }
}

async fn fq(conn: &Connection, sql: &str) -> Vec<Vec<String>> {
    match conn.query(sql).await {
        Ok(rows) => rows
            .iter()
            .map(|r| r.values().iter().map(tag_f).collect())
            .collect(),
        Err(_) => vec![vec!["ERR".to_owned()]],
    }
}
fn rq(conn: &rusqlite::Connection, sql: &str) -> Vec<Vec<String>> {
    let Ok(mut st) = conn.prepare(sql) else {
        return vec![vec!["ERR".to_owned()]];
    };
    let n = st.column_count();
    match st.query_map([], |row| {
        Ok((0..n)
            .map(|i| tag_r(&row.get_unwrap::<_, rusqlite::types::Value>(i)))
            .collect::<Vec<_>>())
    }) {
        Ok(rows) => rows
            .collect::<Result<Vec<_>, _>>()
            .unwrap_or_else(|_| vec![vec!["ERR".to_owned()]]),
        Err(_) => vec![vec!["ERR".to_owned()]],
    }
}

#[test]
fn integer_arithmetic_edges_match_rusqlite_oracle() {
    asupersync::test_utils::run_test(|| async {
        let f = Connection::open(":memory:").await.unwrap();
        let r = rusqlite::Connection::open_in_memory().unwrap();

        let exprs = [
            // signed 64-bit overflow -> promotes to REAL
            "SELECT 9223372036854775807 + 1",
            "SELECT 9223372036854775807 * 2",
            "SELECT -9223372036854775808 - 1",
            "SELECT 9223372036854775807 + 0", // stays int at the boundary
            // integer division truncates toward zero
            "SELECT 7 / 2, -7 / 2, 7 / -2, -7 / -2",
            "SELECT 1 / 2, -1 / 2",
            // modulo takes the dividend's sign
            "SELECT 7 % 3, -7 % 3, 7 % -3, -7 % -3",
            "SELECT 5 % 2, 5 % -2, -5 % 2",
            // division / modulo by zero -> NULL
            "SELECT 1 / 0, 1 % 0, 0 / 0, 5.0 / 0",
            // unary minus on i64::MIN -> overflow to REAL
            "SELECT -(-9223372036854775808)",
            "SELECT abs(-9223372036854775808)",
            // mixing int and real promotes to real
            "SELECT 3 + 0.0, 3 / 2.0, 10 * 1.0",
            // hex literals
            "SELECT 0xff, 0x10, 0xFFFFFFFF, 0x7fffffffffffffff",
            "SELECT 0xffffffffffffffff", // -1 as signed 64-bit
            // bitwise ops
            "SELECT 5 & 3, 5 | 2",
            "SELECT ~0, ~5, ~-1",
            "SELECT 1 << 4, 256 >> 2, 1 << 63",
            "SELECT -8 >> 1, -8 << 1",
            "SELECT 5 & NULL, 5 | NULL, NULL << 2", // NULL propagation
            // large / negative shift counts (SQLite: >=64 or negative -> 0 or sign-based)
            "SELECT 1 << 64, 1 << 100, 1 << -1, 255 >> 64",
            // comparison result typing (0/1 integers)
            "SELECT 2 > 1, 1 = 1, 3 <> 3, NULL = NULL, 1 < NULL",
            // string-to-number coercion in arithmetic
            "SELECT '5' + 3, '5abc' + 3, 'abc' + 3, '  12 ' + 0",
            "SELECT '3.14' * 2, '0x10' + 0",
            // round-half behavior and REAL formatting
            "SELECT 1.0/3.0, 2.0/3.0",
            // typeof across the boundary
            "SELECT typeof(9223372036854775807 + 1), typeof(9223372036854775807), typeof(1/2), typeof(1/2.0)",
        ];

        let mut diffs = Vec::new();
        for q in exprs {
            let fr = fq(&f, q).await;
            let rr = rq(&r, q);
            if fr != rr {
                diffs.push(format!("  `{q}`\n     frank= {fr:?}\n     stock= {rr:?}"));
            }
        }
        assert!(
            diffs.is_empty(),
            "{} integer-arithmetic divergence(s) vs rusqlite:\n{}",
            diffs.len(),
            diffs.join("\n")
        );
    });
}

/// `%` over operands read from rows, so it runs in the scan loop instead of
/// being folded: two INTEGER registers take the loop's integer path
/// (bd-w4pr1.13.20), every other pair `sql_rem`. The rows put each storage
/// class on both sides, a zero divisor, `i64::MIN % -1`, and result types
/// that change from row to row in the same result register (INTEGER after
/// REAL and NULL), compared with their `typeof` against stock.
#[test]
fn remainder_over_scanned_rows_matches_rusqlite_oracle() {
    asupersync::test_utils::run_test(|| async {
        let f = Connection::open(":memory:").await.unwrap();
        let r = rusqlite::Connection::open_in_memory().unwrap();
        let setup = [
            "CREATE TABLE p(id INTEGER PRIMARY KEY, x, y, s TEXT)",
            "INSERT INTO p(x, y, s) VALUES \
             (7, 3, 'a'), (-7, 3, 'b'), (7, -3, 'c'), (-7, -3, 'd'), (5, 0, 'e'), \
             (0, 5, 'f'), (-9223372036854775808, -1, 'g'), (-9223372036854775808, 7, 'h'), \
             (9223372036854775807, -2, 'i'), (7.5, 2, 'j'), (8, 3, 'k'), (7, 2.5, 'l'), \
             (NULL, 3, 'm'), (9, 4, 'n'), ('17', 5, 'o'), (17, '5', 'p'), \
             (3, NULL, 'r'), (X'3132', 5, 's'), (1e20, 7, 't'), (12, 5, 'u'), (12, 0.0, 'v')",
        ];
        for statement in setup {
            f.execute(statement).await.unwrap();
            r.execute_batch(statement).unwrap();
        }
        let queries = [
            "SELECT id, x % y, typeof(x % y) FROM p ORDER BY id",
            "SELECT id, s, x % 3, typeof(x % 3), y % x FROM p ORDER BY id",
            "SELECT id FROM p WHERE x % y = 1 ORDER BY id",
            "SELECT id FROM p WHERE x % y = 0 ORDER BY id",
            "SELECT id FROM p WHERE id % 3 = 0 ORDER BY id",
            "SELECT count(*), sum(id % 7), sum(rowid % 4), total(x % y) FROM p",
            "SELECT id, rowid, typeof(rowid), rowid % 5 FROM p ORDER BY id",
        ];
        let mut diffs = Vec::new();
        for q in queries {
            let fr = fq(&f, q).await;
            let rr = rq(&r, q);
            if fr != rr {
                diffs.push(format!("  `{q}`\n     frank= {fr:?}\n     stock= {rr:?}"));
            }
        }
        assert!(
            diffs.is_empty(),
            "{} remainder divergence(s) vs rusqlite:\n{}",
            diffs.len(),
            diffs.join("\n")
        );
    });
}

/// bd-5lrpw: arithmetic decides INTEGER or REAL per operand as stock's
/// `numericType()` does. Text or blob with no numeric prefix counts as INTEGER
/// 0 in every operator, so `'x' / 3` and `x'41' % 3` are INTEGER 0 (they were
/// REAL 0.0 in `/` and `%`); a REAL-looking or out-of-range prefix stays REAL
/// (`'2.5' % 2` is REAL 0.0, `'9223372036854775808' / 3` REAL), which a fix
/// treating all text as integer would get wrong. Each operand class is tried
/// on both sides, as literals and as values read from scanned rows.
#[test]
fn text_and_blob_operands_take_stock_numeric_type_in_every_operator() {
    asupersync::test_utils::run_test(|| async {
        let f = Connection::open(":memory:").await.unwrap();
        let r = rusqlite::Connection::open_in_memory().unwrap();
        let operands = [
            "'x'",
            "''",
            "' '",
            "'abc1'",
            "x'41'",
            "x''",
            "'12abc'",
            "' 7 '",
            "'7'",
            "'-3'",
            "'2.5'",
            "'1e3'",
            "'1.0'",
            "'9223372036854775807'",
            "'9223372036854775808'",
            "char(11) || '42'",
            "char(160) || '1.5'",
            "3",
            "2.5",
            "0",
        ];
        let setup = [
            "CREATE TABLE v(id INTEGER PRIMARY KEY, a)".to_owned(),
            format!(
                "INSERT INTO v(a) VALUES {}",
                operands
                    .iter()
                    .map(|o| format!("({o})"))
                    .collect::<Vec<_>>()
                    .join(", ")
            ),
        ];
        for statement in &setup {
            f.execute(statement).await.unwrap();
            r.execute_batch(statement).unwrap();
        }
        let mut queries = Vec::new();
        for op in ["/", "%", "+", "-", "*"] {
            queries.push(format!(
                "SELECT l.id, r.id, l.a {op} r.a, typeof(l.a {op} r.a) \
                 FROM v AS l, v AS r ORDER BY l.id, r.id"
            ));
            for operand in operands {
                queries.push(format!(
                    "SELECT {operand} {op} 3, typeof({operand} {op} 3), \
                     7 {op} {operand}, typeof(7 {op} {operand})"
                ));
            }
        }
        // HAVING is evaluated outside the VDBE for grouped scans and grouped
        // joins; before the fix it gave REAL for every `/` and `%` there
        // ('17' / 5 = 3.4), so these kept only the INTEGER operand row.
        f.execute("CREATE TABLE w(id INTEGER PRIMARY KEY, b)")
            .await
            .unwrap();
        r.execute_batch("CREATE TABLE w(id INTEGER PRIMARY KEY, b)")
            .unwrap();
        f.execute("INSERT INTO w(b) VALUES (1), (2)").await.unwrap();
        r.execute_batch("INSERT INTO w(b) VALUES (1), (2)").unwrap();
        for op in ["/", "%"] {
            queries.push(format!(
                "SELECT id, count(*) FROM v GROUP BY id \
                 HAVING typeof(a {op} 3) = 'integer' ORDER BY id"
            ));
            queries.push(format!(
                "SELECT l.id, count(*) FROM v AS l, w AS r GROUP BY l.id \
                 HAVING typeof(l.a {op} 3) = 'integer' ORDER BY l.id"
            ));
        }
        queries.push(
            "SELECT l.id, count(*) FROM v AS l, w AS r GROUP BY l.id \
             HAVING l.a / 5 = 3 ORDER BY l.id"
                .to_owned(),
        );
        let mut diffs = Vec::new();
        for q in &queries {
            let fr = fq(&f, q).await;
            let rr = rq(&r, q);
            if fr != rr {
                diffs.push(format!("  `{q}`\n     frank= {fr:?}\n     stock= {rr:?}"));
            }
        }
        assert!(
            diffs.is_empty(),
            "{} arithmetic operand-type divergence(s) vs rusqlite:\n{}",
            diffs.len(),
            diffs.join("\n")
        );
    });
}
