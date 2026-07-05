//! Broad differential RESULT-parity divergence hunt vs C SQLite (rusqlite,
//! bundled fixed version). Complements `scalar_result_diff_probe.rs` (scalar/
//! math/date/dtoa/aggregate/window) and `core_sql_rusqlite_conformance.rs`
//! (join/group). This gate sweeps less-covered surfaces that are plausible
//! clean-room divergence sources: CAST edge cases, integer-overflow arithmetic,
//! string-function corners, LIKE/GLOB escaping, ORDER BY / DISTINCT with mixed
//! storage classes and NULLS placement, IN / BETWEEN / coalesce, group_concat
//! ordering, quote/printf rendering, and typeof/affinity in comparisons.
//!
//! Parity rule per case: both engines accept and return identical tagged rows,
//! OR both reject. A divergence (different values, different storage class, or
//! one accepts while the other rejects) is recorded. Unlike the other gates,
//! this one collects ALL divergences and reports them together so a hunt
//! surfaces the full set, not just the first.

use fsqlite_core::connection::Connection;
use fsqlite_types::value::SqliteValue;

fn tag_franken(value: &SqliteValue) -> String {
    match value {
        SqliteValue::Null => "null".to_owned(),
        SqliteValue::Integer(n) => format!("int:{n}"),
        SqliteValue::Float(x) => format!("real:{x:?}"),
        SqliteValue::Text(t) => format!("text:{t}"),
        SqliteValue::Blob(b) => format!("blob:{}", hex(b)),
    }
}

fn tag_rusqlite(value: &rusqlite::types::Value) -> String {
    use rusqlite::types::Value;
    match value {
        Value::Null => "null".to_owned(),
        Value::Integer(n) => format!("int:{n}"),
        Value::Real(x) => format!("real:{x:?}"),
        Value::Text(t) => format!("text:{t}"),
        Value::Blob(b) => format!("blob:{}", hex(b)),
    }
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02X}")).collect()
}

fn frank_rows(setup: &[&str], sql: &str) -> Result<Vec<Vec<String>>, String> {
    let dir = tempfile::tempdir().map_err(|e| format!("tempdir: {e}"))?;
    let path = dir.path().join("probe.db");
    let conn = Connection::open(path.to_str().unwrap()).map_err(|e| format!("open: {e}"))?;
    for s in setup {
        conn.execute(s).map_err(|e| format!("setup `{s}`: {e}"))?;
    }
    conn.query(sql).map_err(|e| e.to_string()).map(|rows| {
        rows.iter()
            .map(|row| row.values().iter().map(tag_franken).collect())
            .collect()
    })
}

fn sqlite_rows(setup: &[&str], sql: &str) -> Result<Vec<Vec<String>>, String> {
    let conn = rusqlite::Connection::open_in_memory().map_err(|e| format!("open: {e}"))?;
    for s in setup {
        conn.execute_batch(s)
            .map_err(|e| format!("setup `{s}`: {e}"))?;
    }
    let mut stmt = conn.prepare(sql).map_err(|e| e.to_string())?;
    let ncol = stmt.column_count();
    let rows = stmt
        .query_map([], |row| {
            let mut out = Vec::with_capacity(ncol);
            for i in 0..ncol {
                let v: rusqlite::types::Value = row.get(i)?;
                out.push(tag_rusqlite(&v));
            }
            Ok(out)
        })
        .map_err(|e| e.to_string())?
        .collect::<Result<Vec<_>, _>>();
    rows.map_err(|e| e.to_string())
}

/// One probe case: optional schema setup + a query.
struct Case {
    setup: &'static [&'static str],
    sql: &'static str,
}

/// Compare frank vs sqlite for a query whose row ORDER is implementation-defined
/// (e.g. RETURNING from a multi-row DML). Rows are sorted before comparison so
/// only the multiset of returned rows is asserted, not their order.
fn check_unordered(divergences: &mut Vec<String>, c: &Case) {
    let norm = |mut v: Vec<Vec<String>>| {
        v.sort();
        v
    };
    let f = frank_rows(c.setup, c.sql);
    let s = sqlite_rows(c.setup, c.sql);
    match (f, s) {
        (Ok(fr), Ok(sr)) => {
            let (fr, sr) = (norm(fr), norm(sr));
            if fr != sr {
                divergences.push(format!(
                    "VALUE DIVERGENCE (unordered)\n  sql: {}\n  frank:  {:?}\n  sqlite: {:?}",
                    c.sql, fr, sr
                ));
            }
        }
        (Err(_), Err(_)) => {}
        (Ok(fr), Err(se)) => divergences.push(format!(
            "ACCEPT/REJECT DIVERGENCE (frank accepts, sqlite rejects)\n  sql: {}\n  frank:  {:?}\n  sqlite-err: {}",
            c.sql, fr, se
        )),
        (Err(fe), Ok(sr)) => divergences.push(format!(
            "ACCEPT/REJECT DIVERGENCE (frank rejects, sqlite accepts)\n  sql: {}\n  frank-err: {}\n  sqlite: {:?}",
            c.sql, fe, sr
        )),
    }
}

const NO_SETUP: &[&str] = &[];

fn check(divergences: &mut Vec<String>, c: &Case) {
    let f = frank_rows(c.setup, c.sql);
    let s = sqlite_rows(c.setup, c.sql);
    match (&f, &s) {
        (Ok(fr), Ok(sr)) => {
            if fr != sr {
                divergences.push(format!(
                    "VALUE DIVERGENCE\n  sql: {}\n  frank:  {:?}\n  sqlite: {:?}",
                    c.sql, fr, sr
                ));
            }
        }
        (Err(_), Err(_)) => { /* both reject: parity */ }
        (Ok(fr), Err(se)) => divergences.push(format!(
            "ACCEPT/REJECT DIVERGENCE (frank accepts, sqlite rejects)\n  sql: {}\n  frank:  {:?}\n  sqlite-err: {}",
            c.sql, fr, se
        )),
        (Err(fe), Ok(sr)) => divergences.push(format!(
            "ACCEPT/REJECT DIVERGENCE (frank rejects, sqlite accepts)\n  sql: {}\n  frank-err: {}\n  sqlite: {:?}",
            c.sql, fe, sr
        )),
    }
}

#[test]
fn divergence_hunt_broad_surface() {
    let cases: Vec<Case> = vec![
        // ---- CAST edge cases ----
        Case {
            setup: NO_SETUP,
            sql: "SELECT CAST('123abc' AS INTEGER)",
        },
        Case {
            setup: NO_SETUP,
            sql: "SELECT CAST('  -45  ' AS INTEGER)",
        },
        Case {
            setup: NO_SETUP,
            sql: "SELECT CAST('3.99' AS INTEGER)",
        },
        Case {
            setup: NO_SETUP,
            sql: "SELECT CAST('0x1F' AS INTEGER)",
        },
        Case {
            setup: NO_SETUP,
            sql: "SELECT CAST('1e3' AS INTEGER)",
        },
        Case {
            setup: NO_SETUP,
            sql: "SELECT CAST('1e3' AS REAL)",
        },
        Case {
            setup: NO_SETUP,
            sql: "SELECT CAST('abc' AS REAL)",
        },
        Case {
            setup: NO_SETUP,
            sql: "SELECT CAST('9999999999999999999999' AS INTEGER)",
        },
        Case {
            setup: NO_SETUP,
            sql: "SELECT CAST(3.9 AS INTEGER)",
        },
        Case {
            setup: NO_SETUP,
            sql: "SELECT CAST(-3.9 AS INTEGER)",
        },
        Case {
            setup: NO_SETUP,
            sql: "SELECT CAST(9223372036854775807 AS REAL)",
        },
        Case {
            setup: NO_SETUP,
            sql: "SELECT CAST(X'41' AS TEXT)",
        },
        Case {
            setup: NO_SETUP,
            sql: "SELECT CAST(123 AS TEXT)",
        },
        Case {
            setup: NO_SETUP,
            sql: "SELECT CAST(1.5 AS TEXT)",
        },
        Case {
            setup: NO_SETUP,
            sql: "SELECT CAST('  12.5xyz' AS REAL)",
        },
        // ---- integer overflow arithmetic (SQLite promotes to REAL on overflow) ----
        Case {
            setup: NO_SETUP,
            sql: "SELECT 9223372036854775807 + 1",
        },
        Case {
            setup: NO_SETUP,
            sql: "SELECT 9223372036854775807 * 2",
        },
        Case {
            setup: NO_SETUP,
            sql: "SELECT -9223372036854775808 - 1",
        },
        Case {
            setup: NO_SETUP,
            sql: "SELECT -9223372036854775808 / -1",
        },
        Case {
            setup: NO_SETUP,
            sql: "SELECT abs(-9223372036854775808)",
        },
        Case {
            setup: NO_SETUP,
            sql: "SELECT 5 / 2",
        },
        Case {
            setup: NO_SETUP,
            sql: "SELECT 5 % 0",
        },
        Case {
            setup: NO_SETUP,
            sql: "SELECT 5 / 0",
        },
        Case {
            setup: NO_SETUP,
            sql: "SELECT 5.0 / 0",
        },
        Case {
            setup: NO_SETUP,
            sql: "SELECT -5 % 3",
        },
        Case {
            setup: NO_SETUP,
            sql: "SELECT 5 % -3",
        },
        // ---- string functions ----
        Case {
            setup: NO_SETUP,
            sql: "SELECT substr('hello', -3)",
        },
        Case {
            setup: NO_SETUP,
            sql: "SELECT substr('hello', -3, 2)",
        },
        Case {
            setup: NO_SETUP,
            sql: "SELECT substr('hello', 0)",
        },
        Case {
            setup: NO_SETUP,
            sql: "SELECT substr('hello', 2, -1)",
        },
        Case {
            setup: NO_SETUP,
            sql: "SELECT substr('hello', 0, 2)",
        },
        Case {
            setup: NO_SETUP,
            sql: "SELECT replace('aaa', 'a', 'bb')",
        },
        Case {
            setup: NO_SETUP,
            sql: "SELECT replace('abc', '', 'X')",
        },
        Case {
            setup: NO_SETUP,
            sql: "SELECT trim('  xx  ')",
        },
        Case {
            setup: NO_SETUP,
            sql: "SELECT trim('xxhelloxx', 'x')",
        },
        Case {
            setup: NO_SETUP,
            sql: "SELECT ltrim('xxhello', 'x')",
        },
        Case {
            setup: NO_SETUP,
            sql: "SELECT rtrim('helloxx', 'x')",
        },
        Case {
            setup: NO_SETUP,
            sql: "SELECT instr('hello world', 'o')",
        },
        Case {
            setup: NO_SETUP,
            sql: "SELECT instr('hello', 'z')",
        },
        Case {
            setup: NO_SETUP,
            sql: "SELECT instr('hello', '')",
        },
        Case {
            setup: NO_SETUP,
            sql: "SELECT char(72, 105)",
        },
        Case {
            setup: NO_SETUP,
            sql: "SELECT unicode('A')",
        },
        Case {
            setup: NO_SETUP,
            sql: "SELECT unicode('')",
        },
        Case {
            setup: NO_SETUP,
            sql: "SELECT length('héllo')",
        },
        Case {
            setup: NO_SETUP,
            sql: "SELECT length(X'00010203')",
        },
        Case {
            setup: NO_SETUP,
            sql: "SELECT length(12345)",
        },
        Case {
            setup: NO_SETUP,
            sql: "SELECT length(1.5)",
        },
        Case {
            setup: NO_SETUP,
            sql: "SELECT quote('it''s')",
        },
        Case {
            setup: NO_SETUP,
            sql: "SELECT quote(X'DEADBEEF')",
        },
        Case {
            setup: NO_SETUP,
            sql: "SELECT quote(NULL)",
        },
        Case {
            setup: NO_SETUP,
            sql: "SELECT quote(3.14)",
        },
        Case {
            setup: NO_SETUP,
            sql: "SELECT hex('abc')",
        },
        Case {
            setup: NO_SETUP,
            sql: "SELECT hex(255)",
        },
        Case {
            setup: NO_SETUP,
            sql: "SELECT upper('héllo')",
        },
        Case {
            setup: NO_SETUP,
            sql: "SELECT lower('HÉLLO')",
        },
        Case {
            setup: NO_SETUP,
            sql: "SELECT printf('%d-%s-%.2f', 5, 'x', 3.14159)",
        },
        Case {
            setup: NO_SETUP,
            sql: "SELECT printf('%5d|%-5d|', 42, 42)",
        },
        Case {
            setup: NO_SETUP,
            sql: "SELECT printf('%x', 255)",
        },
        Case {
            setup: NO_SETUP,
            sql: "SELECT printf('%05.2f', 3.1)",
        },
        Case {
            setup: NO_SETUP,
            sql: "SELECT format('%d', 99)",
        },
        Case {
            setup: NO_SETUP,
            sql: "SELECT char(0x48) || 'i'",
        },
        // ---- round / numeric rendering ----
        Case {
            setup: NO_SETUP,
            sql: "SELECT round(2.5)",
        },
        Case {
            setup: NO_SETUP,
            sql: "SELECT round(3.5)",
        },
        Case {
            setup: NO_SETUP,
            sql: "SELECT round(-2.5)",
        },
        Case {
            setup: NO_SETUP,
            sql: "SELECT round(2.675, 2)",
        },
        Case {
            setup: NO_SETUP,
            sql: "SELECT round(1.0/3.0, 5)",
        },
        Case {
            setup: NO_SETUP,
            sql: "SELECT 0.1 + 0.2",
        },
        Case {
            setup: NO_SETUP,
            sql: "SELECT 1.0/3.0",
        },
        Case {
            setup: NO_SETUP,
            sql: "SELECT -0.0",
        },
        Case {
            setup: NO_SETUP,
            sql: "SELECT 1e308 * 10",
        },
        Case {
            setup: NO_SETUP,
            sql: "SELECT 2e-308 / 1e10",
        },
        // ---- typeof / affinity in comparisons ----
        Case {
            setup: NO_SETUP,
            sql: "SELECT typeof(1), typeof(1.0), typeof('1'), typeof(X'01'), typeof(NULL)",
        },
        Case {
            setup: NO_SETUP,
            sql: "SELECT 1 = 1.0, '1' = 1, '1.0' = 1.0, X'31' = '1'",
        },
        Case {
            setup: NO_SETUP,
            sql: "SELECT 10 < '9', '10' < '9', 10 < 9",
        },
        Case {
            setup: NO_SETUP,
            sql: "SELECT NULL = NULL, NULL IS NULL, 1 IS NOT NULL",
        },
        Case {
            setup: NO_SETUP,
            sql: "SELECT NULL + 1, NULL || 'x', NULL AND 0, NULL OR 1",
        },
        // ---- coalesce / ifnull / nullif ----
        Case {
            setup: NO_SETUP,
            sql: "SELECT coalesce(NULL, NULL, 3, 4)",
        },
        Case {
            setup: NO_SETUP,
            sql: "SELECT ifnull(NULL, 'x'), ifnull(5, 'x')",
        },
        Case {
            setup: NO_SETUP,
            sql: "SELECT nullif(5, 5), nullif(5, 6), nullif('a','a')",
        },
        Case {
            setup: NO_SETUP,
            sql: "SELECT max(1, 2.5, '3', NULL)",
        },
        Case {
            setup: NO_SETUP,
            sql: "SELECT min('b', 'a', 'c')",
        },
        // ---- ORDER BY / DISTINCT with mixed storage classes + NULLS ----
        Case {
            setup: &[
                "CREATE TABLE t(x)",
                "INSERT INTO t VALUES (3),(1.5),('apple'),(NULL),(X'01'),(2),('Banana')",
            ],
            sql: "SELECT typeof(x), x FROM t ORDER BY x",
        },
        Case {
            setup: &[
                "CREATE TABLE t(x)",
                "INSERT INTO t VALUES (3),(1.5),('apple'),(NULL),(X'01'),(2)",
            ],
            sql: "SELECT x FROM t ORDER BY x DESC",
        },
        Case {
            setup: &[
                "CREATE TABLE t(x)",
                "INSERT INTO t VALUES (3),(NULL),(1),(NULL),(2)",
            ],
            sql: "SELECT x FROM t ORDER BY x NULLS FIRST",
        },
        Case {
            setup: &[
                "CREATE TABLE t(x)",
                "INSERT INTO t VALUES (3),(NULL),(1),(NULL),(2)",
            ],
            sql: "SELECT x FROM t ORDER BY x DESC NULLS LAST",
        },
        Case {
            setup: &[
                "CREATE TABLE t(x)",
                "INSERT INTO t VALUES (1),(1.0),('1'),(1),('1')",
            ],
            sql: "SELECT DISTINCT typeof(x), x FROM t ORDER BY x, typeof(x)",
        },
        // ---- IN / BETWEEN / LIKE / GLOB ----
        Case {
            setup: NO_SETUP,
            sql: "SELECT 1 IN (1,2,3), 4 IN (1,2,3), NULL IN (1,2), 1 IN (NULL,1)",
        },
        Case {
            setup: NO_SETUP,
            sql: "SELECT 1 NOT IN (2,3), 1 NOT IN (NULL,2)",
        },
        Case {
            setup: NO_SETUP,
            sql: "SELECT 5 BETWEEN 1 AND 10, 5 BETWEEN 10 AND 1, 'b' BETWEEN 'a' AND 'c'",
        },
        Case {
            setup: NO_SETUP,
            sql: "SELECT 'abc' LIKE 'a%', 'abc' LIKE 'A%', 'a%c' LIKE 'a\\%c' ESCAPE '\\'",
        },
        Case {
            setup: NO_SETUP,
            sql: "SELECT 'abc' LIKE 'a_c', 'aXc' LIKE 'a_c', 'ac' LIKE 'a_c'",
        },
        Case {
            setup: NO_SETUP,
            sql: "SELECT 'Hello' GLOB 'H*o', 'hello' GLOB 'H*o', 'abc' GLOB 'a[bc]c'",
        },
        Case {
            setup: NO_SETUP,
            sql: "SELECT 'a.b' GLOB 'a?b', 'a%b' LIKE 'a[%]b'",
        },
        Case {
            setup: NO_SETUP,
            sql: "SELECT 'ABC' LIKE 'abc', 'straße' LIKE 'STRASSE'",
        },
        // ---- group_concat ordering & separator ----
        Case {
            setup: &["CREATE TABLE t(x)", "INSERT INTO t VALUES (3),(1),(2)"],
            sql: "SELECT group_concat(x) FROM t",
        },
        Case {
            setup: &["CREATE TABLE t(x)", "INSERT INTO t VALUES (3),(1),(2)"],
            sql: "SELECT group_concat(x, '|') FROM t",
        },
        Case {
            setup: &["CREATE TABLE t(x)", "INSERT INTO t VALUES (3),(1),(2),(1)"],
            sql: "SELECT group_concat(DISTINCT x) FROM t",
        },
        Case {
            setup: &["CREATE TABLE t(x)", "INSERT INTO t VALUES (3),(1),(2)"],
            sql: "SELECT group_concat(x ORDER BY x DESC) FROM t",
        },
        // ---- aggregate edge: sum overflow, total vs sum, count ----
        Case {
            setup: &[
                "CREATE TABLE t(x)",
                "INSERT INTO t VALUES (9223372036854775807),(1)",
            ],
            sql: "SELECT sum(x), total(x) FROM t",
        },
        Case {
            setup: &["CREATE TABLE t(x)", "INSERT INTO t VALUES (NULL),(NULL)"],
            sql: "SELECT sum(x), total(x), count(x), count(*), avg(x) FROM t",
        },
        Case {
            setup: &["CREATE TABLE t(x)", "INSERT INTO t VALUES (1),(2),(3)"],
            sql: "SELECT avg(x), sum(x)/count(x) FROM t",
        },
        // ---- CASE / boolean ----
        Case {
            setup: NO_SETUP,
            sql: "SELECT CASE WHEN NULL THEN 'a' ELSE 'b' END",
        },
        Case {
            setup: NO_SETUP,
            sql: "SELECT CASE 1 WHEN 1.0 THEN 'eq' ELSE 'ne' END",
        },
        Case {
            setup: NO_SETUP,
            sql: "SELECT CASE WHEN 1 THEN 'a' END, CASE WHEN 0 THEN 'a' END",
        },
        // ---- date/time corners ----
        Case {
            setup: NO_SETUP,
            sql: "SELECT date('2026-01-31', '+1 month')",
        },
        Case {
            setup: NO_SETUP,
            sql: "SELECT date('2026-03-31', '-1 month')",
        },
        Case {
            setup: NO_SETUP,
            sql: "SELECT date('2024-02-29', '+1 year')",
        },
        Case {
            setup: NO_SETUP,
            sql: "SELECT strftime('%Y-%m-%d %H:%M:%f', '2026-06-30 12:34:56.789')",
        },
        Case {
            setup: NO_SETUP,
            sql: "SELECT julianday('2000-01-01')",
        },
        Case {
            setup: NO_SETUP,
            sql: "SELECT strftime('%w %j', '2026-06-30')",
        },
        Case {
            setup: NO_SETUP,
            sql: "SELECT date('2026-06-30', 'weekday 0')",
        },
        Case {
            setup: NO_SETUP,
            sql: "SELECT time('12:00', '+90 minutes')",
        },
        Case {
            setup: NO_SETUP,
            sql: "SELECT datetime('2026-06-30', 'start of month')",
        },
        // ---- rowid / implicit columns ----
        Case {
            setup: &[
                "CREATE TABLE t(a)",
                "INSERT INTO t VALUES ('x'),('y'),('z')",
            ],
            sql: "SELECT rowid, a FROM t ORDER BY rowid",
        },
        Case {
            setup: &[
                "CREATE TABLE t(a INTEGER PRIMARY KEY, b)",
                "INSERT INTO t VALUES (10,'x'),(5,'y')",
            ],
            sql: "SELECT rowid, a, b FROM t ORDER BY a",
        },
    ];

    let mut divergences = Vec::new();
    for c in &cases {
        check(&mut divergences, c);
    }

    if !divergences.is_empty() {
        let report = divergences.join("\n\n");
        panic!(
            "\n===== {} DIVERGENCE(S) vs C SQLite (of {} cases) =====\n{}\n",
            divergences.len(),
            cases.len(),
            report
        );
    }
}

/// RETURNING on WITHOUT ROWID tables (bd-eja6l): INSERT/UPDATE/DELETE ... RETURNING
/// must produce the same rows C SQLite does (inserted image / new image / deleted
/// image), including `*`, expressions, OR IGNORE/REPLACE conflict semantics, and
/// composite primary keys. Row order is impl-defined, so compared as a multiset.
#[test]
fn without_rowid_returning_parity() {
    const WR1: &[&str] = &["CREATE TABLE t(k TEXT PRIMARY KEY, v INTEGER) WITHOUT ROWID"];
    const WR_SEED: &[&str] = &[
        "CREATE TABLE t(k TEXT PRIMARY KEY, v INTEGER) WITHOUT ROWID",
        "INSERT INTO t VALUES ('a', 1), ('b', 2), ('c', 3)",
    ];
    const WR_COMPOSITE: &[&str] = &[
        "CREATE TABLE t(a INTEGER, b INTEGER, payload TEXT, PRIMARY KEY(a, b)) WITHOUT ROWID",
        "INSERT INTO t VALUES (1, 1, 'x'), (1, 2, 'y'), (2, 1, 'z')",
    ];
    let cases: Vec<Case> = vec![
        // INSERT ... RETURNING (inserted image, in statement order)
        Case {
            setup: WR1,
            sql: "INSERT INTO t VALUES ('b', 2), ('a', 1) RETURNING k, v",
        },
        Case {
            setup: WR1,
            sql: "INSERT INTO t VALUES ('c', 3) RETURNING *",
        },
        Case {
            setup: WR1,
            sql: "INSERT INTO t VALUES ('d', 4) RETURNING k, v * 10, v || '!'",
        },
        Case {
            setup: WR1,
            sql: "INSERT INTO t(v, k) VALUES (7, 'g') RETURNING k, v",
        },
        // DEFAULT VALUES with defaults that satisfy the PK's implicit NOT NULL.
        Case {
            setup: &[
                "CREATE TABLE t(k TEXT PRIMARY KEY DEFAULT 'x', v INTEGER DEFAULT 9) WITHOUT ROWID",
            ],
            sql: "INSERT INTO t DEFAULT VALUES RETURNING *",
        },
        // UPDATE ... RETURNING (NEW image)
        Case {
            setup: WR_SEED,
            sql: "UPDATE t SET v = v + 100 WHERE k = 'a' RETURNING k, v",
        },
        Case {
            setup: WR_SEED,
            sql: "UPDATE t SET v = v * 2 RETURNING k, v",
        },
        Case {
            setup: WR_SEED,
            sql: "UPDATE t SET k = 'zz' WHERE k = 'b' RETURNING *",
        },
        Case {
            setup: WR_SEED,
            sql: "UPDATE t SET v = v + 1 WHERE v >= 2 RETURNING k, v, v - 1",
        },
        // DELETE ... RETURNING (OLD/deleted image)
        Case {
            setup: WR_SEED,
            sql: "DELETE FROM t WHERE k = 'b' RETURNING k, v",
        },
        Case {
            setup: WR_SEED,
            sql: "DELETE FROM t WHERE v > 1 RETURNING *",
        },
        Case {
            setup: WR_SEED,
            sql: "DELETE FROM t RETURNING k",
        },
        // conflict semantics: OR IGNORE skips the conflicting row (no RETURNING row)
        Case {
            setup: WR_SEED,
            sql: "INSERT OR IGNORE INTO t VALUES ('a', 999), ('z', 1) RETURNING k, v",
        },
        // OR REPLACE replaces and returns the new image
        Case {
            setup: WR_SEED,
            sql: "INSERT OR REPLACE INTO t VALUES ('a', 555) RETURNING k, v",
        },
        // composite PK
        Case {
            setup: WR_COMPOSITE,
            sql: "INSERT INTO t VALUES (3, 3, 'w') RETURNING a, b, payload",
        },
        Case {
            setup: WR_COMPOSITE,
            sql: "UPDATE t SET payload = 'NEW' WHERE a = 1 RETURNING *",
        },
        Case {
            setup: WR_COMPOSITE,
            sql: "DELETE FROM t WHERE a = 1 RETURNING a, b, payload",
        },
        // WITHOUT ROWID PK is implicitly NOT NULL (bd-0re6l): these must be
        // rejected by both engines (NULL primary key), and OR IGNORE must skip.
        Case {
            setup: WR1,
            sql: "INSERT INTO t DEFAULT VALUES RETURNING *",
        },
        Case {
            setup: WR1,
            sql: "INSERT INTO t VALUES (NULL, 5) RETURNING k, v",
        },
        Case {
            setup: WR_COMPOSITE,
            sql: "INSERT INTO t VALUES (NULL, 9, 'q') RETURNING *",
        },
        Case {
            setup: WR_SEED,
            sql: "UPDATE t SET k = NULL WHERE k = 'a' RETURNING k, v",
        },
        // OR IGNORE on a NULL PK skips the row (no error, no RETURNING row)
        Case {
            setup: WR1,
            sql: "INSERT OR IGNORE INTO t VALUES (NULL, 1), ('ok', 2) RETURNING k, v",
        },
    ];

    let mut divergences = Vec::new();
    for c in &cases {
        check_unordered(&mut divergences, c);
    }
    if !divergences.is_empty() {
        let report = divergences.join("\n\n");
        panic!(
            "\n===== {} WITHOUT ROWID RETURNING DIVERGENCE(S) vs C SQLite (of {} cases) =====\n{}\n",
            divergences.len(),
            cases.len(),
            report
        );
    }
}

/// INSERT ... SELECT into WITHOUT ROWID tables (bd-eja6l). The INSERT runs in
/// `setup`; the case query is an ordered SELECT verifying the resulting table
/// contents matches C SQLite. Covers different-table source, explicit column
/// lists with DEFAULT fill, WHERE, expression projections, FROM-less constant
/// SELECT, OR IGNORE/REPLACE conflict modes, composite PK, and NULL-PK rejection.
#[test]
fn without_rowid_insert_select_parity() {
    let cases: Vec<Case> = vec![
        // basic SELECT from a different table
        Case {
            setup: &[
                "CREATE TABLE src(k TEXT, v INTEGER)",
                "INSERT INTO src VALUES ('a', 1), ('b', 2), ('c', 3)",
                "CREATE TABLE t(k TEXT PRIMARY KEY, v INTEGER) WITHOUT ROWID",
                "INSERT INTO t SELECT k, v FROM src",
            ],
            sql: "SELECT k, v FROM t ORDER BY k",
        },
        // explicit column list + DEFAULT fill for unmentioned columns
        Case {
            setup: &[
                "CREATE TABLE src(k TEXT, v INTEGER)",
                "INSERT INTO src VALUES ('a', 1), ('b', 2)",
                "CREATE TABLE t(k TEXT PRIMARY KEY, v INTEGER DEFAULT 99, w INTEGER) WITHOUT ROWID",
                "INSERT INTO t(k, w) SELECT k, v FROM src",
            ],
            sql: "SELECT k, v, w FROM t ORDER BY k",
        },
        // WHERE filter on the source
        Case {
            setup: &[
                "CREATE TABLE src(k TEXT, v INTEGER)",
                "INSERT INTO src VALUES ('a', 1), ('b', 2), ('c', 3)",
                "CREATE TABLE t(k TEXT PRIMARY KEY, v INTEGER) WITHOUT ROWID",
                "INSERT INTO t SELECT k, v FROM src WHERE v >= 2",
            ],
            sql: "SELECT k, v FROM t ORDER BY k",
        },
        // expression projection
        Case {
            setup: &[
                "CREATE TABLE src(k TEXT, v INTEGER)",
                "INSERT INTO src VALUES ('a', 1), ('b', 2)",
                "CREATE TABLE t(k TEXT PRIMARY KEY, v INTEGER) WITHOUT ROWID",
                "INSERT INTO t SELECT k || 'x', v * 10 FROM src",
            ],
            sql: "SELECT k, v FROM t ORDER BY k",
        },
        // FROM-less constant SELECT
        Case {
            setup: &[
                "CREATE TABLE t(k TEXT PRIMARY KEY, v INTEGER) WITHOUT ROWID",
                "INSERT INTO t SELECT 'z', 26",
            ],
            sql: "SELECT k, v FROM t",
        },
        // FROM-less with explicit column list (DEFAULT fill)
        Case {
            setup: &[
                "CREATE TABLE t(k TEXT PRIMARY KEY, v INTEGER DEFAULT 7) WITHOUT ROWID",
                "INSERT INTO t(k) SELECT 'q'",
            ],
            sql: "SELECT k, v FROM t",
        },
        // OR IGNORE: conflicting PK skipped
        Case {
            setup: &[
                "CREATE TABLE src(k TEXT, v INTEGER)",
                "INSERT INTO src VALUES ('a', 100), ('z', 1)",
                "CREATE TABLE t(k TEXT PRIMARY KEY, v INTEGER) WITHOUT ROWID",
                "INSERT INTO t VALUES ('a', 1)",
                "INSERT OR IGNORE INTO t SELECT k, v FROM src",
            ],
            sql: "SELECT k, v FROM t ORDER BY k",
        },
        // OR REPLACE: conflicting PK replaced
        Case {
            setup: &[
                "CREATE TABLE src(k TEXT, v INTEGER)",
                "INSERT INTO src VALUES ('a', 100)",
                "CREATE TABLE t(k TEXT PRIMARY KEY, v INTEGER) WITHOUT ROWID",
                "INSERT INTO t VALUES ('a', 1)",
                "INSERT OR REPLACE INTO t SELECT k, v FROM src",
            ],
            sql: "SELECT k, v FROM t ORDER BY k",
        },
        // composite PK target
        Case {
            setup: &[
                "CREATE TABLE src(a INTEGER, b INTEGER, p TEXT)",
                "INSERT INTO src VALUES (1, 1, 'x'), (1, 2, 'y'), (2, 1, 'z')",
                "CREATE TABLE t(a INTEGER, b INTEGER, p TEXT, PRIMARY KEY(a, b)) WITHOUT ROWID",
                "INSERT INTO t SELECT a, b, p FROM src",
            ],
            sql: "SELECT a, b, p FROM t ORDER BY a, b",
        },
        // INSERT ... SELECT with RETURNING
        Case {
            setup: &[
                "CREATE TABLE src(k TEXT, v INTEGER)",
                "INSERT INTO src VALUES ('a', 1), ('b', 2)",
                "CREATE TABLE t(k TEXT PRIMARY KEY, v INTEGER) WITHOUT ROWID",
            ],
            sql: "INSERT INTO t SELECT k, v FROM src RETURNING k, v",
        },
        // NULL primary key produced by SELECT — both engines reject
        Case {
            setup: &[
                "CREATE TABLE src(k TEXT, v INTEGER)",
                "INSERT INTO src VALUES (NULL, 1)",
                "CREATE TABLE t(k TEXT PRIMARY KEY, v INTEGER) WITHOUT ROWID",
                "INSERT INTO t SELECT k, v FROM src",
            ],
            sql: "SELECT k, v FROM t",
        },
    ];

    let mut divergences = Vec::new();
    for c in &cases {
        // The RETURNING case has impl-defined order; the rest are ORDER BY'd.
        if c.sql.contains("RETURNING") {
            check_unordered(&mut divergences, c);
        } else {
            check(&mut divergences, c);
        }
    }
    if !divergences.is_empty() {
        let report = divergences.join("\n\n");
        panic!(
            "\n===== {} WITHOUT ROWID INSERT...SELECT DIVERGENCE(S) vs C SQLite (of {} cases) =====\n{}\n",
            divergences.len(),
            cases.len(),
            report
        );
    }
}

/// INSERT ... ON CONFLICT ... DO UPDATE / DO NOTHING into WITHOUT ROWID tables
/// (bd-eja6l). The conflict target is the PRIMARY KEY. Covers `excluded.*`
/// references, mixing existing + excluded values, the DO UPDATE WHERE guard,
/// DO NOTHING, the no-conflict insert path, composite PK, secondary-index
/// maintenance, RETURNING, and multi-row batches with mixed conflict/no-conflict.
/// Non-RETURNING cases run the upsert in `setup` and verify final table state
/// with an ordered SELECT; RETURNING cases compare rows as a multiset.
#[test]
fn without_rowid_upsert_parity() {
    let cases: Vec<Case> = vec![
        // basic DO UPDATE from excluded on PK conflict
        Case {
            setup: &[
                "CREATE TABLE t(k TEXT PRIMARY KEY, v INTEGER) WITHOUT ROWID",
                "INSERT INTO t VALUES ('a', 1), ('b', 2)",
                "INSERT INTO t VALUES ('a', 10) ON CONFLICT(k) DO UPDATE SET v = excluded.v",
            ],
            sql: "SELECT k, v FROM t ORDER BY k",
        },
        // DO UPDATE mixing existing + excluded
        Case {
            setup: &[
                "CREATE TABLE t(k TEXT PRIMARY KEY, v INTEGER) WITHOUT ROWID",
                "INSERT INTO t VALUES ('a', 1)",
                "INSERT INTO t VALUES ('a', 100) ON CONFLICT(k) DO UPDATE SET v = v + excluded.v",
            ],
            sql: "SELECT k, v FROM t ORDER BY k",
        },
        // explicit target, DO UPDATE to a literal (no excluded reference)
        Case {
            setup: &[
                "CREATE TABLE t(k TEXT PRIMARY KEY, v INTEGER) WITHOUT ROWID",
                "INSERT INTO t VALUES ('a', 1)",
                "INSERT INTO t VALUES ('a', 7) ON CONFLICT(k) DO UPDATE SET v = 7",
            ],
            sql: "SELECT k, v FROM t ORDER BY k",
        },
        // DO NOTHING on conflict leaves the row unchanged
        Case {
            setup: &[
                "CREATE TABLE t(k TEXT PRIMARY KEY, v INTEGER) WITHOUT ROWID",
                "INSERT INTO t VALUES ('a', 1)",
                "INSERT INTO t VALUES ('a', 999) ON CONFLICT(k) DO NOTHING",
            ],
            sql: "SELECT k, v FROM t ORDER BY k",
        },
        // no conflict -> plain insert via the DO UPDATE path
        Case {
            setup: &[
                "CREATE TABLE t(k TEXT PRIMARY KEY, v INTEGER) WITHOUT ROWID",
                "INSERT INTO t VALUES ('a', 1)",
                "INSERT INTO t VALUES ('z', 26) ON CONFLICT(k) DO UPDATE SET v = excluded.v",
            ],
            sql: "SELECT k, v FROM t ORDER BY k",
        },
        // DO UPDATE WHERE guard true -> update applied
        Case {
            setup: &[
                "CREATE TABLE t(k TEXT PRIMARY KEY, v INTEGER) WITHOUT ROWID",
                "INSERT INTO t VALUES ('a', 1)",
                "INSERT INTO t VALUES ('a', 10) ON CONFLICT(k) DO UPDATE SET v = excluded.v WHERE excluded.v > v",
            ],
            sql: "SELECT k, v FROM t ORDER BY k",
        },
        // DO UPDATE WHERE guard false -> row left unchanged
        Case {
            setup: &[
                "CREATE TABLE t(k TEXT PRIMARY KEY, v INTEGER) WITHOUT ROWID",
                "INSERT INTO t VALUES ('a', 5)",
                "INSERT INTO t VALUES ('a', 1) ON CONFLICT(k) DO UPDATE SET v = excluded.v WHERE excluded.v > v",
            ],
            sql: "SELECT k, v FROM t ORDER BY k",
        },
        // coalesce(excluded, existing)
        Case {
            setup: &[
                "CREATE TABLE t(k TEXT PRIMARY KEY, v INTEGER) WITHOUT ROWID",
                "INSERT INTO t VALUES ('a', 1)",
                "INSERT INTO t VALUES ('a', NULL) ON CONFLICT(k) DO UPDATE SET v = coalesce(excluded.v, v)",
            ],
            sql: "SELECT k, v FROM t ORDER BY k",
        },
        // multi-row batch: mixed conflict + new
        Case {
            setup: &[
                "CREATE TABLE t(k TEXT PRIMARY KEY, v INTEGER) WITHOUT ROWID",
                "INSERT INTO t VALUES ('a', 1), ('b', 2)",
                "INSERT INTO t VALUES ('a', 10), ('z', 5) ON CONFLICT(k) DO UPDATE SET v = excluded.v",
            ],
            sql: "SELECT k, v FROM t ORDER BY k",
        },
        // composite PK upsert
        Case {
            setup: &[
                "CREATE TABLE t(a INTEGER, b INTEGER, payload TEXT, PRIMARY KEY(a, b)) WITHOUT ROWID",
                "INSERT INTO t VALUES (1, 1, 'x'), (1, 2, 'y')",
                "INSERT INTO t VALUES (1, 1, 'NEW') ON CONFLICT(a, b) DO UPDATE SET payload = excluded.payload",
            ],
            sql: "SELECT a, b, payload FROM t ORDER BY a, b",
        },
        // secondary index maintenance across an upsert that changes an indexed
        // col: verify row content by a PRIMARY KEY-ordered scan (the table
        // b-tree, avoiding the WITHOUT ROWID index read accessor gap bd-rjaff),
        // and that the index b-tree stays consistent via integrity_check below.
        Case {
            setup: &[
                "CREATE TABLE t(k TEXT PRIMARY KEY, v INTEGER) WITHOUT ROWID",
                "CREATE INDEX iv ON t(v)",
                "INSERT INTO t VALUES ('a', 1), ('b', 2)",
                "INSERT INTO t VALUES ('a', 100) ON CONFLICT(k) DO UPDATE SET v = excluded.v",
            ],
            sql: "SELECT k, v FROM t ORDER BY k",
        },
        Case {
            setup: &[
                "CREATE TABLE t(k TEXT PRIMARY KEY, v INTEGER) WITHOUT ROWID",
                "CREATE INDEX iv2 ON t(v)",
                "INSERT INTO t VALUES ('a', 1), ('b', 2)",
                "INSERT INTO t VALUES ('a', 100) ON CONFLICT(k) DO UPDATE SET v = excluded.v",
            ],
            sql: "PRAGMA integrity_check",
        },
        // update a non-PK column, extra columns present
        Case {
            setup: &[
                "CREATE TABLE t(k TEXT PRIMARY KEY, a INTEGER, b TEXT) WITHOUT ROWID",
                "INSERT INTO t VALUES ('r', 1, 'old')",
                "INSERT INTO t VALUES ('r', 2, 'new') ON CONFLICT(k) DO UPDATE SET b = excluded.b",
            ],
            sql: "SELECT k, a, b FROM t ORDER BY k",
        },
        // aliased INSERT target: `x` qualifies the EXISTING row; `excluded` the
        // proposed row. Exercises target-alias resolution in the DO UPDATE.
        Case {
            setup: &[
                "CREATE TABLE t(k TEXT PRIMARY KEY, v INTEGER) WITHOUT ROWID",
                "INSERT INTO t VALUES ('a', 1)",
                "INSERT INTO t AS x VALUES ('a', 10) ON CONFLICT(k) DO UPDATE SET v = x.v + excluded.v",
            ],
            sql: "SELECT k, v FROM t ORDER BY k",
        },
        // aliased INSERT target in the DO UPDATE WHERE guard
        Case {
            setup: &[
                "CREATE TABLE t(k TEXT PRIMARY KEY, v INTEGER) WITHOUT ROWID",
                "INSERT INTO t VALUES ('a', 5)",
                "INSERT INTO t AS x VALUES ('a', 1) ON CONFLICT(k) DO UPDATE SET v = excluded.v WHERE excluded.v > x.v",
            ],
            sql: "SELECT k, v FROM t ORDER BY k",
        },
        // RETURNING on the conflict (update) path
        Case {
            setup: &[
                "CREATE TABLE t(k TEXT PRIMARY KEY, v INTEGER) WITHOUT ROWID",
                "INSERT INTO t VALUES ('a', 1)",
            ],
            sql: "INSERT INTO t VALUES ('a', 10) ON CONFLICT(k) DO UPDATE SET v = excluded.v RETURNING k, v",
        },
        // RETURNING on the no-conflict (insert) path
        Case {
            setup: &[
                "CREATE TABLE t(k TEXT PRIMARY KEY, v INTEGER) WITHOUT ROWID",
                "INSERT INTO t VALUES ('a', 1)",
            ],
            sql: "INSERT INTO t VALUES ('z', 5) ON CONFLICT(k) DO UPDATE SET v = excluded.v RETURNING k, v",
        },
        // RETURNING skipped when the DO UPDATE WHERE guard is false
        Case {
            setup: &[
                "CREATE TABLE t(k TEXT PRIMARY KEY, v INTEGER) WITHOUT ROWID",
                "INSERT INTO t VALUES ('a', 5)",
            ],
            sql: "INSERT INTO t VALUES ('a', 1) ON CONFLICT(k) DO UPDATE SET v = excluded.v WHERE excluded.v > v RETURNING k, v",
        },
    ];

    let mut divergences = Vec::new();
    for c in &cases {
        if c.sql.contains("RETURNING") {
            check_unordered(&mut divergences, c);
        } else {
            check(&mut divergences, c);
        }
    }
    if !divergences.is_empty() {
        let report = divergences.join("\n\n");
        panic!(
            "\n===== {} WITHOUT ROWID UPSERT DIVERGENCE(S) vs C SQLite (of {} cases) =====\n{}\n",
            divergences.len(),
            cases.len(),
            report
        );
    }
}

/// UPDATE ... FROM into WITHOUT ROWID tables (bd-eja6l). Inner-join semantics:
/// the target WITHOUT ROWID table joins the FROM source(s); matched target rows
/// are rewritten with assignments that may reference both target and source
/// columns. Covers single-source lookup, multi-column SET, expressions mixing
/// target+source, aliased source, explicit INNER JOIN, comma (cross) join,
/// composite PK, PK reassignment, WHERE filtering, no-match, and RETURNING.
/// Each case is a single-match join (deterministic); non-RETURNING cases run the
/// UPDATE in `setup` and verify via an ordered SELECT.
#[test]
fn without_rowid_update_from_parity() {
    let cases: Vec<Case> = vec![
        // single-source lookup: SET target col = source col
        Case {
            setup: &[
                "CREATE TABLE t(k TEXT PRIMARY KEY, v INTEGER) WITHOUT ROWID",
                "INSERT INTO t VALUES ('a', 1), ('b', 2), ('c', 3)",
                "CREATE TABLE s(k TEXT, x INTEGER)",
                "INSERT INTO s VALUES ('a', 100), ('b', 200)",
                "UPDATE t SET v = s.x FROM s WHERE t.k = s.k",
            ],
            sql: "SELECT k, v FROM t ORDER BY k",
        },
        // multi-column SET from the source
        Case {
            setup: &[
                "CREATE TABLE t(k TEXT PRIMARY KEY, a INTEGER, b INTEGER) WITHOUT ROWID",
                "INSERT INTO t VALUES ('r', 1, 1), ('q', 9, 9)",
                "CREATE TABLE s(k TEXT, a INTEGER, b INTEGER)",
                "INSERT INTO s VALUES ('r', 10, 20)",
                "UPDATE t SET a = s.a, b = s.b FROM s WHERE t.k = s.k",
            ],
            sql: "SELECT k, a, b FROM t ORDER BY k",
        },
        // expression mixing target + source columns
        Case {
            setup: &[
                "CREATE TABLE t(k TEXT PRIMARY KEY, v INTEGER) WITHOUT ROWID",
                "INSERT INTO t VALUES ('a', 1), ('b', 2)",
                "CREATE TABLE s(k TEXT, d INTEGER)",
                "INSERT INTO s VALUES ('a', 5), ('b', 10)",
                "UPDATE t SET v = v + s.d FROM s WHERE t.k = s.k",
            ],
            sql: "SELECT k, v FROM t ORDER BY k",
        },
        // aliased source
        Case {
            setup: &[
                "CREATE TABLE t(k TEXT PRIMARY KEY, v INTEGER) WITHOUT ROWID",
                "INSERT INTO t VALUES ('a', 1), ('b', 2)",
                "CREATE TABLE s(k TEXT, x INTEGER)",
                "INSERT INTO s VALUES ('a', 100)",
                "UPDATE t SET v = src.x FROM s AS src WHERE t.k = src.k",
            ],
            sql: "SELECT k, v FROM t ORDER BY k",
        },
        // explicit INNER JOIN with ON between two sources
        Case {
            setup: &[
                "CREATE TABLE t(k TEXT PRIMARY KEY, v INTEGER) WITHOUT ROWID",
                "INSERT INTO t VALUES ('a', 0), ('b', 0)",
                "CREATE TABLE s1(k TEXT, y INTEGER)",
                "INSERT INTO s1 VALUES ('a', 1), ('b', 2)",
                "CREATE TABLE s2(y INTEGER, z INTEGER)",
                "INSERT INTO s2 VALUES (1, 100), (2, 200)",
                "UPDATE t SET v = s2.z FROM s1 JOIN s2 ON s1.y = s2.y WHERE t.k = s1.k",
            ],
            sql: "SELECT k, v FROM t ORDER BY k",
        },
        // comma (cross) join filtered by WHERE
        Case {
            setup: &[
                "CREATE TABLE t(k TEXT PRIMARY KEY, v INTEGER) WITHOUT ROWID",
                "INSERT INTO t VALUES ('a', 0)",
                "CREATE TABLE s1(k TEXT, id INTEGER)",
                "INSERT INTO s1 VALUES ('a', 1)",
                "CREATE TABLE s2(id INTEGER, b INTEGER)",
                "INSERT INTO s2 VALUES (1, 50)",
                "UPDATE t SET v = s2.b FROM s1, s2 WHERE t.k = s1.k AND s1.id = s2.id",
            ],
            sql: "SELECT k, v FROM t ORDER BY k",
        },
        // composite PK target
        Case {
            setup: &[
                "CREATE TABLE t(a INTEGER, b INTEGER, payload TEXT, PRIMARY KEY(a, b)) WITHOUT ROWID",
                "INSERT INTO t VALUES (1, 1, 'x'), (1, 2, 'y'), (2, 1, 'z')",
                "CREATE TABLE s(a INTEGER, b INTEGER, p TEXT)",
                "INSERT INTO s VALUES (1, 1, 'NEW'), (2, 1, 'ALSO')",
                "UPDATE t SET payload = s.p FROM s WHERE t.a = s.a AND t.b = s.b",
            ],
            sql: "SELECT a, b, payload FROM t ORDER BY a, b",
        },
        // PK reassignment from the source (delete old key, insert new key)
        Case {
            setup: &[
                "CREATE TABLE t(k TEXT PRIMARY KEY, v INTEGER) WITHOUT ROWID",
                "INSERT INTO t VALUES ('a', 1), ('b', 2)",
                "CREATE TABLE s(oldk TEXT, newk TEXT)",
                "INSERT INTO s VALUES ('a', 'z')",
                "UPDATE t SET k = s.newk FROM s WHERE t.k = s.oldk",
            ],
            sql: "SELECT k, v FROM t ORDER BY k",
        },
        // WHERE filter also constrains the target; only some rows match
        Case {
            setup: &[
                "CREATE TABLE t(k TEXT PRIMARY KEY, v INTEGER) WITHOUT ROWID",
                "INSERT INTO t VALUES ('a', 1), ('b', 5)",
                "CREATE TABLE s(k TEXT, x INTEGER)",
                "INSERT INTO s VALUES ('a', 100), ('b', 200)",
                "UPDATE t SET v = s.x FROM s WHERE t.k = s.k AND t.v < 3",
            ],
            sql: "SELECT k, v FROM t ORDER BY k",
        },
        // no match -> table unchanged
        Case {
            setup: &[
                "CREATE TABLE t(k TEXT PRIMARY KEY, v INTEGER) WITHOUT ROWID",
                "INSERT INTO t VALUES ('a', 1), ('b', 2)",
                "CREATE TABLE s(k TEXT, x INTEGER)",
                "INSERT INTO s VALUES ('a', 100)",
                "UPDATE t SET v = s.x FROM s WHERE t.k = s.k AND s.x > 1000",
            ],
            sql: "SELECT k, v FROM t ORDER BY k",
        },
        // secondary index maintenance during UPDATE ... FROM (verify integrity)
        Case {
            setup: &[
                "CREATE TABLE t(k TEXT PRIMARY KEY, v INTEGER) WITHOUT ROWID",
                "CREATE INDEX ivf ON t(v)",
                "INSERT INTO t VALUES ('a', 1), ('b', 2)",
                "CREATE TABLE s(k TEXT, x INTEGER)",
                "INSERT INTO s VALUES ('a', 100)",
                "UPDATE t SET v = s.x FROM s WHERE t.k = s.k",
            ],
            sql: "PRAGMA integrity_check",
        },
        // RETURNING the new image (impl-defined order -> multiset compare)
        Case {
            setup: &[
                "CREATE TABLE t(k TEXT PRIMARY KEY, v INTEGER) WITHOUT ROWID",
                "INSERT INTO t VALUES ('a', 1), ('b', 2), ('c', 3)",
                "CREATE TABLE s(k TEXT, x INTEGER)",
                "INSERT INTO s VALUES ('a', 100), ('b', 200)",
            ],
            sql: "UPDATE t SET v = s.x FROM s WHERE t.k = s.k RETURNING k, v",
        },
    ];

    let mut divergences = Vec::new();
    for c in &cases {
        if c.sql.contains("RETURNING") {
            check_unordered(&mut divergences, c);
        } else {
            check(&mut divergences, c);
        }
    }
    if !divergences.is_empty() {
        let report = divergences.join("\n\n");
        panic!(
            "\n===== {} WITHOUT ROWID UPDATE...FROM DIVERGENCE(S) vs C SQLite (of {} cases) =====\n{}\n",
            divergences.len(),
            cases.len(),
            report
        );
    }
}

#[test]
fn divergence_hunt_hard_constructs() {
    let cases: Vec<Case> = vec![
        // ---- generated columns ----
        Case {
            setup: &[
                "CREATE TABLE t(a INTEGER, b INTEGER, c AS (a + b) VIRTUAL, d AS (a * b) STORED)",
                "INSERT INTO t(a, b) VALUES (3, 4),(5, 6)",
            ],
            sql: "SELECT a, b, c, d FROM t ORDER BY a",
        },
        Case {
            setup: &[
                "CREATE TABLE t(a INTEGER, b TEXT AS (a || 'x'))",
                "INSERT INTO t(a) VALUES (1),(2)",
            ],
            sql: "SELECT a, b FROM t ORDER BY a",
        },
        // ---- CHECK constraints ----
        Case {
            setup: &[
                "CREATE TABLE t(a INTEGER CHECK (a > 0))",
                "INSERT INTO t VALUES (5)",
            ],
            sql: "SELECT a FROM t",
        },
        Case {
            setup: &["CREATE TABLE t(a INTEGER CHECK (a > 0))"],
            sql: "INSERT INTO t VALUES (-1) RETURNING a",
        },
        // ---- UPSERT (ON CONFLICT DO UPDATE / DO NOTHING) ----
        Case {
            setup: &[
                "CREATE TABLE t(id INTEGER PRIMARY KEY, v INTEGER)",
                "INSERT INTO t VALUES (1, 10)",
                "INSERT INTO t VALUES (1, 20) ON CONFLICT(id) DO UPDATE SET v = v + excluded.v",
            ],
            sql: "SELECT id, v FROM t",
        },
        Case {
            setup: &[
                "CREATE TABLE t(id INTEGER PRIMARY KEY, v INTEGER)",
                "INSERT INTO t VALUES (1, 10)",
                "INSERT INTO t VALUES (1, 20) ON CONFLICT(id) DO NOTHING",
            ],
            sql: "SELECT id, v FROM t",
        },
        Case {
            setup: &[
                "CREATE TABLE t(a INTEGER, b INTEGER, UNIQUE(a))",
                "INSERT INTO t VALUES (1, 100)",
                "INSERT INTO t VALUES (1, 200) ON CONFLICT(a) DO UPDATE SET b = excluded.b WHERE excluded.b > t.b",
            ],
            sql: "SELECT a, b FROM t",
        },
        // ---- triggers ----
        Case {
            setup: &[
                "CREATE TABLE t(a INTEGER)",
                "CREATE TABLE log(msg TEXT)",
                "CREATE TRIGGER tr AFTER INSERT ON t BEGIN INSERT INTO log VALUES ('inserted ' || NEW.a); END",
                "INSERT INTO t VALUES (1),(2)",
            ],
            sql: "SELECT msg FROM log ORDER BY rowid",
        },
        Case {
            setup: &[
                "CREATE TABLE t(a INTEGER, b INTEGER)",
                "CREATE TRIGGER tr BEFORE INSERT ON t BEGIN SELECT RAISE(IGNORE) WHERE NEW.a < 0; END",
                "INSERT INTO t VALUES (1, 10),(-1, 20),(2, 30)",
            ],
            sql: "SELECT a, b FROM t ORDER BY a",
        },
        Case {
            setup: &[
                "CREATE TABLE t(a INTEGER)",
                "CREATE TABLE audit(old_a INTEGER, new_a INTEGER)",
                "CREATE TRIGGER tr AFTER UPDATE ON t BEGIN INSERT INTO audit VALUES (OLD.a, NEW.a); END",
                "INSERT INTO t VALUES (1)",
                "UPDATE t SET a = 99 WHERE a = 1",
            ],
            sql: "SELECT old_a, new_a FROM audit",
        },
        // ---- recursive CTE ----
        Case {
            setup: NO_SETUP,
            sql: "WITH RECURSIVE c(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM c WHERE n < 5) SELECT n FROM c",
        },
        Case {
            setup: NO_SETUP,
            sql: "WITH RECURSIVE c(n, f) AS (SELECT 1, 1 UNION ALL SELECT n+1, f*(n+1) FROM c WHERE n < 6) SELECT n, f FROM c",
        },
        Case {
            setup: NO_SETUP,
            sql: "WITH RECURSIVE c(x) AS (SELECT 'a' UNION SELECT x || 'a' FROM c WHERE length(x) < 4) SELECT x FROM c ORDER BY x",
        },
        // ---- partial / expression indexes (results must match; index is transparent) ----
        Case {
            setup: &[
                "CREATE TABLE t(a INTEGER, b INTEGER)",
                "INSERT INTO t VALUES (1, 10),(2, 20),(3, 30),(-1, 5)",
                "CREATE INDEX idx ON t(a) WHERE a > 0",
            ],
            sql: "SELECT a, b FROM t WHERE a > 1 ORDER BY a",
        },
        Case {
            setup: &[
                "CREATE TABLE t(a TEXT)",
                "INSERT INTO t VALUES ('Hello'),('WORLD'),('foo')",
                "CREATE INDEX idx ON t(lower(a))",
            ],
            sql: "SELECT a FROM t WHERE lower(a) = 'world'",
        },
        // ---- window frame edge cases ----
        Case {
            setup: &[
                "CREATE TABLE t(x)",
                "INSERT INTO t VALUES (1),(2),(3),(4),(5)",
            ],
            sql: "SELECT x, sum(x) OVER (ORDER BY x ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) FROM t ORDER BY x",
        },
        Case {
            setup: &[
                "CREATE TABLE t(x)",
                "INSERT INTO t VALUES (1),(1),(2),(2),(3)",
            ],
            sql: "SELECT x, sum(x) OVER (ORDER BY x RANGE BETWEEN 1 PRECEDING AND CURRENT ROW) FROM t ORDER BY x",
        },
        Case {
            setup: &["CREATE TABLE t(x)", "INSERT INTO t VALUES (1),(2),(3),(4)"],
            sql: "SELECT x, sum(x) OVER (ORDER BY x GROUPS BETWEEN 1 PRECEDING AND CURRENT ROW) FROM t ORDER BY x",
        },
        Case {
            setup: &[
                "CREATE TABLE t(x)",
                "INSERT INTO t VALUES (1),(2),(3),(4),(5)",
            ],
            sql: "SELECT x, sum(x) OVER (ORDER BY x ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW EXCLUDE CURRENT ROW) FROM t ORDER BY x",
        },
        Case {
            setup: &[
                "CREATE TABLE t(g, x)",
                "INSERT INTO t VALUES ('a',1),('a',2),('b',3),('b',4)",
            ],
            sql: "SELECT g, x, lag(x, 1, -1) OVER (PARTITION BY g ORDER BY x) FROM t ORDER BY g, x",
        },
        Case {
            setup: &[
                "CREATE TABLE t(x)",
                "INSERT INTO t VALUES (1),(2),(3),(4),(5),(6)",
            ],
            sql: "SELECT x, ntile(3) OVER (ORDER BY x) FROM t ORDER BY x",
        },
        // ---- type affinity round-trip on INSERT ----
        Case {
            setup: &[
                "CREATE TABLE t(i INTEGER, r REAL, t TEXT, b BLOB, n NUMERIC)",
                "INSERT INTO t VALUES ('42', '3.5', 99, 1.5, '7')",
            ],
            sql: "SELECT typeof(i), i, typeof(r), r, typeof(t), t, typeof(b), b, typeof(n), n FROM t",
        },
        Case {
            setup: &[
                "CREATE TABLE t(x INTEGER)",
                "INSERT INTO t VALUES (3.0),(3.5),('4'),('4.0')",
            ],
            sql: "SELECT typeof(x), x FROM t ORDER BY rowid",
        },
        // ---- correlated / scalar subqueries ----
        Case {
            setup: &[
                "CREATE TABLE a(id, v)",
                "CREATE TABLE b(aid, w)",
                "INSERT INTO a VALUES (1,'x'),(2,'y'),(3,'z')",
                "INSERT INTO b VALUES (1,10),(1,20),(2,30)",
            ],
            sql: "SELECT id, (SELECT sum(w) FROM b WHERE b.aid = a.id) FROM a ORDER BY id",
        },
        Case {
            setup: &[
                "CREATE TABLE a(id)",
                "CREATE TABLE b(aid)",
                "INSERT INTO a VALUES (1),(2),(3)",
                "INSERT INTO b VALUES (1),(3)",
            ],
            sql: "SELECT id FROM a WHERE EXISTS (SELECT 1 FROM b WHERE b.aid = a.id) ORDER BY id",
        },
        // ---- compound SELECT ORDER BY by alias/ordinal ----
        Case {
            setup: &[
                "CREATE TABLE t(a, b)",
                "INSERT INTO t VALUES (3,'c'),(1,'a'),(2,'b')",
            ],
            sql: "SELECT a AS k, b FROM t ORDER BY k DESC",
        },
        Case {
            setup: &[
                "CREATE TABLE t(a, b)",
                "INSERT INTO t VALUES (3,'c'),(1,'a'),(2,'b')",
            ],
            sql: "SELECT a, b FROM t ORDER BY 2 DESC",
        },
        Case {
            setup: NO_SETUP,
            sql: "SELECT 1 AS x UNION SELECT 3 UNION SELECT 2 ORDER BY x DESC",
        },
        // ---- VALUES as a standalone query ----
        Case {
            setup: NO_SETUP,
            sql: "VALUES (1,2),(3,4),(5,6)",
        },
        Case {
            setup: NO_SETUP,
            sql: "SELECT * FROM (VALUES (1),(2),(3)) ORDER BY 1 DESC",
        },
        // ---- numeric literal parsing ----
        Case {
            setup: NO_SETUP,
            sql: "SELECT 0x1F, 0xFF, .5, 5., 1_000",
        },
        Case {
            setup: NO_SETUP,
            sql: "SELECT 1.5e2, 1E3, 0xABCDEF",
        },
        // ---- digit separators (SQLite 3.46+): underscore must be between two digits ----
        Case {
            setup: NO_SETUP,
            sql: "SELECT 1_000_000",
        },
        Case {
            setup: NO_SETUP,
            sql: "SELECT 1_0_0",
        },
        Case {
            setup: NO_SETUP,
            sql: "SELECT 1_000.5",
        },
        Case {
            setup: NO_SETUP,
            sql: "SELECT 1.0_5",
        },
        Case {
            setup: NO_SETUP,
            sql: "SELECT .5_0",
        },
        Case {
            setup: NO_SETUP,
            sql: "SELECT 1_0e2",
        },
        Case {
            setup: NO_SETUP,
            sql: "SELECT 1e1_0",
        },
        Case {
            setup: NO_SETUP,
            sql: "SELECT 0x1_F",
        },
        Case {
            setup: NO_SETUP,
            sql: "SELECT 0xFF_FF",
        },
        Case {
            setup: NO_SETUP,
            sql: "SELECT 9_223_372_036_854_775_807",
        },
        // these must be REJECTED by both engines (underscore not between two digits)
        Case {
            setup: NO_SETUP,
            sql: "SELECT 1__0",
        },
        Case {
            setup: NO_SETUP,
            sql: "SELECT 100_",
        },
        Case {
            setup: NO_SETUP,
            sql: "SELECT 5_.0",
        },
        Case {
            setup: NO_SETUP,
            sql: "SELECT 1_.5",
        },
        Case {
            setup: NO_SETUP,
            sql: "SELECT 1._5",
        },
        Case {
            setup: NO_SETUP,
            sql: "SELECT 0x_1F",
        },
        Case {
            setup: NO_SETUP,
            sql: "SELECT 1e_2",
        },
        Case {
            setup: NO_SETUP,
            sql: "SELECT 1_e2",
        },
        Case {
            setup: NO_SETUP,
            sql: "SELECT 0x1F_",
        },
    ];

    let mut divergences = Vec::new();
    for c in &cases {
        check(&mut divergences, c);
    }

    if !divergences.is_empty() {
        let report = divergences.join("\n\n");
        panic!(
            "\n===== {} DIVERGENCE(S) vs C SQLite (of {} cases) =====\n{}\n",
            divergences.len(),
            cases.len(),
            report
        );
    }
}

/// printf/format and date-time edge parity: non-finite float rendering
/// (`Inf`/`-Inf`), sign/zero-pad flags on `%e`/`%g`, argument-supplied
/// precision (`.*`), byte-counted `%s` precision, and numeric date() inputs
/// outside the valid Julian-day range (NULL, unless a reinterpreting
/// modifier like 'unixepoch'/'auto'/'julianday' comes first).
#[test]
fn printf_and_datetime_edge_parity() {
    let cases: Vec<Case> = vec![
        // overflowing float literals are ±Infinity, not f64::MAX
        Case {
            setup: NO_SETUP,
            sql: "SELECT 9e999",
        },
        Case {
            setup: NO_SETUP,
            sql: "SELECT -9e999",
        },
        Case {
            setup: NO_SETUP,
            sql: "SELECT 9e999 = 1e999",
        },
        Case {
            setup: NO_SETUP,
            sql: "SELECT 9e999 + 1",
        },
        Case {
            setup: NO_SETUP,
            sql: "SELECT 9e999 / 9e999",
        },
        Case {
            setup: NO_SETUP,
            sql: "SELECT typeof(9e999)",
        },
        Case {
            setup: NO_SETUP,
            sql: "SELECT printf('%f', 9e999)",
        },
        Case {
            setup: NO_SETUP,
            sql: "SELECT printf('%f', -9e999)",
        },
        Case {
            setup: NO_SETUP,
            sql: "SELECT printf('%+f', 9e999)",
        },
        Case {
            setup: NO_SETUP,
            sql: "SELECT printf('%10f', 9e999)",
        },
        Case {
            setup: NO_SETUP,
            sql: "SELECT printf('%e', 9e999)",
        },
        Case {
            setup: NO_SETUP,
            sql: "SELECT printf('%g', -9e999)",
        },
        Case {
            setup: NO_SETUP,
            sql: "SELECT printf('%+e', 1.5)",
        },
        Case {
            setup: NO_SETUP,
            sql: "SELECT printf('% e', 1.5)",
        },
        Case {
            setup: NO_SETUP,
            sql: "SELECT printf('%015e', 1.5)",
        },
        Case {
            setup: NO_SETUP,
            sql: "SELECT printf('%+015e', 1.5)",
        },
        Case {
            setup: NO_SETUP,
            sql: "SELECT printf('%015g', 1.5)",
        },
        Case {
            setup: NO_SETUP,
            sql: "SELECT printf('%+g', 1.5)",
        },
        Case {
            setup: NO_SETUP,
            sql: "SELECT printf('%+e', -1.5)",
        },
        Case {
            setup: NO_SETUP,
            sql: "SELECT printf('%.*f', 2, 3.14159)",
        },
        Case {
            setup: NO_SETUP,
            sql: "SELECT printf('%.*e', 2, 12345.678)",
        },
        Case {
            setup: NO_SETUP,
            sql: "SELECT printf('%.*s', 3, 'abcdef')",
        },
        Case {
            setup: NO_SETUP,
            sql: "SELECT printf('%.3s', 'héllo')",
        },
        Case {
            setup: NO_SETUP,
            sql: "SELECT printf('%.4s', 'héllo')",
        },
        // date/time numeric-range edges
        Case {
            setup: NO_SETUP,
            sql: "SELECT date(99999999999)",
        },
        Case {
            setup: NO_SETUP,
            sql: "SELECT date(-1)",
        },
        Case {
            setup: NO_SETUP,
            sql: "SELECT date(-0.5)",
        },
        Case {
            setup: NO_SETUP,
            sql: "SELECT date(0)",
        },
        Case {
            setup: NO_SETUP,
            sql: "SELECT date(5373484.4)",
        },
        Case {
            setup: NO_SETUP,
            sql: "SELECT date(5373484.5)",
        },
        Case {
            setup: NO_SETUP,
            sql: "SELECT datetime(1700000000, 'unixepoch')",
        },
        Case {
            setup: NO_SETUP,
            sql: "SELECT date(1700000000, 'auto')",
        },
        Case {
            setup: NO_SETUP,
            sql: "SELECT date(2460000, 'julianday')",
        },
        Case {
            setup: NO_SETUP,
            sql: "SELECT unixepoch(2460000.5, 'julianday')",
        },
    ];

    let mut divergences = Vec::new();
    for c in &cases {
        check(&mut divergences, c);
    }
    if !divergences.is_empty() {
        let report = divergences.join("\n\n");
        panic!(
            "\n===== {} PRINTF/DATETIME DIVERGENCE(S) vs C SQLite (of {} cases) =====\n{}\n",
            divergences.len(),
            cases.len(),
            report
        );
    }
}

/// WITHOUT ROWID secondary-index READ accessors (bd-rjaff). Index entries on
/// WITHOUT ROWID tables are `(key terms..., PK cols...)` with no trailing
/// rowid; the index-driven read path must extract the PK suffix and seek the
/// table b-tree by PK instead of `IdxRowid` + `SeekRowid`. `INDEXED BY`
/// forces the accessor, which previously failed with "index key record
/// missing trailing integer rowid". Multi-row cases rely on index scan order
/// (ascending key, PK tiebreak), which is identical in both engines.
#[test]
fn without_rowid_index_read_parity() {
    let cases: Vec<Case> = vec![
        // the bd-rjaff repro: forced index range read
        Case {
            setup: &[
                "CREATE TABLE t(k TEXT PRIMARY KEY, v INTEGER) WITHOUT ROWID",
                "CREATE INDEX iv ON t(v)",
                "INSERT INTO t VALUES ('a', 1), ('b', 2)",
            ],
            sql: "SELECT k, v FROM t INDEXED BY iv WHERE v >= 2",
        },
        // forced index equality read
        Case {
            setup: &[
                "CREATE TABLE t(k TEXT PRIMARY KEY, v INTEGER) WITHOUT ROWID",
                "CREATE INDEX iv ON t(v)",
                "INSERT INTO t VALUES ('a', 1), ('b', 2), ('c', 3)",
            ],
            sql: "SELECT k, v FROM t INDEXED BY iv WHERE v = 2",
        },
        // duplicate index keys: full duplicate run must be returned
        Case {
            setup: &[
                "CREATE TABLE t(k TEXT PRIMARY KEY, v INTEGER) WITHOUT ROWID",
                "CREATE INDEX iv ON t(v)",
                "INSERT INTO t VALUES ('a', 2), ('b', 2), ('c', 3), ('d', 1)",
            ],
            sql: "SELECT k, v FROM t INDEXED BY iv WHERE v = 2",
        },
        // range with both bounds
        Case {
            setup: &[
                "CREATE TABLE t(k TEXT PRIMARY KEY, v INTEGER) WITHOUT ROWID",
                "CREATE INDEX iv ON t(v)",
                "INSERT INTO t VALUES ('a', 1), ('b', 2), ('c', 3), ('d', 4)",
            ],
            sql: "SELECT k, v FROM t INDEXED BY iv WHERE v > 1 AND v < 4",
        },
        // planner-chosen access (no hint) must also be correct
        Case {
            setup: &[
                "CREATE TABLE t(k TEXT PRIMARY KEY, v INTEGER) WITHOUT ROWID",
                "CREATE INDEX iv ON t(v)",
                "INSERT INTO t VALUES ('a', 1), ('b', 2), ('c', 3)",
            ],
            sql: "SELECT k, v FROM t WHERE v >= 2",
        },
        // composite PK suffix: index must round-trip both PK columns
        Case {
            setup: &[
                "CREATE TABLE t(a INTEGER, b INTEGER, v INTEGER, PRIMARY KEY(a, b)) WITHOUT ROWID",
                "CREATE INDEX iv ON t(v)",
                "INSERT INTO t VALUES (1, 1, 10), (1, 2, 20), (2, 1, 30)",
            ],
            sql: "SELECT a, b, v FROM t INDEXED BY iv WHERE v >= 20",
        },
        // TEXT PK with rows inserted out of index order
        Case {
            setup: &[
                "CREATE TABLE t(k TEXT PRIMARY KEY, v INTEGER) WITHOUT ROWID",
                "CREATE INDEX iv ON t(v)",
                "INSERT INTO t VALUES ('z', 5), ('m', 9), ('a', 7)",
            ],
            sql: "SELECT k, v FROM t INDEXED BY iv WHERE v > 5",
        },
        // no matching rows
        Case {
            setup: &[
                "CREATE TABLE t(k TEXT PRIMARY KEY, v INTEGER) WITHOUT ROWID",
                "CREATE INDEX iv ON t(v)",
                "INSERT INTO t VALUES ('a', 1)",
            ],
            sql: "SELECT k, v FROM t INDEXED BY iv WHERE v > 100",
        },
        // UNIQUE secondary index read
        Case {
            setup: &[
                "CREATE TABLE t(k TEXT PRIMARY KEY, v INTEGER) WITHOUT ROWID",
                "CREATE UNIQUE INDEX iv ON t(v)",
                "INSERT INTO t VALUES ('a', 1), ('b', 2)",
            ],
            sql: "SELECT k, v FROM t INDEXED BY iv WHERE v = 2",
        },
        // ORDER BY satisfiable by the index (index-ordered scan lane)
        Case {
            setup: &[
                "CREATE TABLE t(k TEXT PRIMARY KEY, v INTEGER) WITHOUT ROWID",
                "CREATE INDEX iv ON t(v)",
                "INSERT INTO t VALUES ('z', 5), ('m', 9), ('a', 7)",
            ],
            sql: "SELECT k, v FROM t ORDER BY v",
        },
        // ORDER BY DESC via the index
        Case {
            setup: &[
                "CREATE TABLE t(k TEXT PRIMARY KEY, v INTEGER) WITHOUT ROWID",
                "CREATE INDEX iv ON t(v)",
                "INSERT INTO t VALUES ('z', 5), ('m', 9), ('a', 7)",
            ],
            sql: "SELECT k, v FROM t ORDER BY v DESC",
        },
        // covering-shaped output (only indexed column selected) + ORDER BY
        Case {
            setup: &[
                "CREATE TABLE t(k TEXT PRIMARY KEY, v INTEGER) WITHOUT ROWID",
                "CREATE INDEX iv ON t(v)",
                "INSERT INTO t VALUES ('z', 5), ('m', 9), ('a', 7)",
            ],
            sql: "SELECT v FROM t ORDER BY v",
        },
        // join with a WITHOUT ROWID table on the lookup side (join fast paths
        // must fall back to the generic path)
        Case {
            setup: &[
                "CREATE TABLE w(k TEXT PRIMARY KEY, v INTEGER) WITHOUT ROWID",
                "CREATE INDEX wv ON w(v)",
                "INSERT INTO w VALUES ('a', 1), ('b', 2)",
                "CREATE TABLE r(id INTEGER PRIMARY KEY, v INTEGER)",
                "INSERT INTO r VALUES (10, 1), (20, 2)",
            ],
            sql: "SELECT r.id, w.k FROM r JOIN w ON r.v = w.v ORDER BY r.id",
        },
        // grouped count/sum join shape with a WITHOUT ROWID side
        Case {
            setup: &[
                "CREATE TABLE w(k TEXT PRIMARY KEY, g INTEGER, v INTEGER) WITHOUT ROWID",
                "INSERT INTO w VALUES ('a', 1, 10), ('b', 1, 20), ('c', 2, 30)",
                "CREATE TABLE r(id INTEGER PRIMARY KEY, g INTEGER)",
                "INSERT INTO r VALUES (1, 1), (2, 1), (3, 2)",
            ],
            sql: "SELECT r.g, count(*), sum(w.v) FROM r JOIN w ON r.g = w.g GROUP BY r.g ORDER BY r.g",
        },
    ];

    let mut divergences = Vec::new();
    for c in &cases {
        check(&mut divergences, c);
    }
    if !divergences.is_empty() {
        let report = divergences.join("\n\n");
        panic!(
            "\n===== {} WITHOUT ROWID INDEX-READ DIVERGENCE(S) vs C SQLite (of {} cases) =====\n{}\n",
            divergences.len(),
            cases.len(),
            report
        );
    }
}

/// `INSERT ... ON CONFLICT DO UPDATE` with an OMITTED conflict target
/// (SQLite 3.35+; bd-6geae). The upsert must fire on whichever uniqueness
/// constraint the new row violates — the rowid/INTEGER PRIMARY KEY *or* any
/// UNIQUE column/index — not just the PK. Also pins the parse rule that a
/// targetless ON CONFLICT clause must be the last one.
///
/// Known remaining gap (kept out of this gate, documented on bd-6geae):
/// WITHOUT ROWID tables with UNIQUE secondary indexes reject the omitted
/// target loudly instead of probing the secondary index.
#[test]
fn omitted_conflict_target_upsert_parity() {
    let cases: Vec<Case> = vec![
        // IPK conflict, omitted target
        Case {
            setup: &[
                "CREATE TABLE t(id INTEGER PRIMARY KEY, v INTEGER)",
                "INSERT INTO t VALUES (1, 10)",
                "INSERT INTO t VALUES (1, 99) ON CONFLICT DO UPDATE SET v = excluded.v",
            ],
            sql: "SELECT id, v FROM t ORDER BY id",
        },
        // UNIQUE column conflict, omitted target (the bd-6geae repro shape)
        Case {
            setup: &[
                "CREATE TABLE t(k TEXT UNIQUE, v INTEGER)",
                "INSERT INTO t VALUES ('a', 1)",
                "INSERT INTO t VALUES ('a', 7) ON CONFLICT DO UPDATE SET v = excluded.v",
            ],
            sql: "SELECT k, v FROM t ORDER BY k",
        },
        // UNIQUE index (CREATE UNIQUE INDEX) conflict, omitted target
        Case {
            setup: &[
                "CREATE TABLE t(id INTEGER PRIMARY KEY, k TEXT, v INTEGER)",
                "CREATE UNIQUE INDEX tk ON t(k)",
                "INSERT INTO t VALUES (1, 'a', 1)",
                "INSERT INTO t(k, v) VALUES ('a', 7) ON CONFLICT DO UPDATE SET v = excluded.v",
            ],
            sql: "SELECT id, k, v FROM t ORDER BY id",
        },
        // two UNIQUE constraints; the conflict lands on the SECOND one
        Case {
            setup: &[
                "CREATE TABLE t(a TEXT UNIQUE, b TEXT UNIQUE, v INTEGER)",
                "INSERT INTO t VALUES ('a1', 'b1', 1)",
                "INSERT INTO t VALUES ('aX', 'b1', 7) ON CONFLICT DO UPDATE SET v = excluded.v",
            ],
            sql: "SELECT a, b, v FROM t ORDER BY a",
        },
        // conflict on the IPK when a UNIQUE column also exists (PK checked first)
        Case {
            setup: &[
                "CREATE TABLE t(id INTEGER PRIMARY KEY, k TEXT UNIQUE, v INTEGER)",
                "INSERT INTO t VALUES (1, 'a', 1)",
                "INSERT INTO t VALUES (1, 'z', 7) ON CONFLICT DO UPDATE SET v = excluded.v",
            ],
            sql: "SELECT id, k, v FROM t ORDER BY id",
        },
        // no conflict anywhere -> plain insert through the omitted-target path
        Case {
            setup: &[
                "CREATE TABLE t(id INTEGER PRIMARY KEY, k TEXT UNIQUE, v INTEGER)",
                "INSERT INTO t VALUES (1, 'a', 1)",
                "INSERT INTO t VALUES (2, 'b', 2) ON CONFLICT DO UPDATE SET v = excluded.v",
            ],
            sql: "SELECT id, k, v FROM t ORDER BY id",
        },
        // NULL in a UNIQUE column never conflicts -> plain insert
        Case {
            setup: &[
                "CREATE TABLE t(k TEXT UNIQUE, v INTEGER)",
                "INSERT INTO t VALUES (NULL, 1)",
                "INSERT INTO t VALUES (NULL, 2) ON CONFLICT DO UPDATE SET v = excluded.v",
            ],
            sql: "SELECT k, v FROM t ORDER BY v",
        },
        // omitted target + DO UPDATE ... WHERE guard true
        Case {
            setup: &[
                "CREATE TABLE t(k TEXT UNIQUE, v INTEGER)",
                "INSERT INTO t VALUES ('a', 1)",
                "INSERT INTO t VALUES ('a', 10) ON CONFLICT DO UPDATE SET v = excluded.v WHERE excluded.v > v",
            ],
            sql: "SELECT k, v FROM t ORDER BY k",
        },
        // omitted target + DO UPDATE ... WHERE guard false -> unchanged
        Case {
            setup: &[
                "CREATE TABLE t(k TEXT UNIQUE, v INTEGER)",
                "INSERT INTO t VALUES ('a', 5)",
                "INSERT INTO t VALUES ('a', 1) ON CONFLICT DO UPDATE SET v = excluded.v WHERE excluded.v > v",
            ],
            sql: "SELECT k, v FROM t ORDER BY k",
        },
        // assignments mixing existing + excluded values
        Case {
            setup: &[
                "CREATE TABLE t(k TEXT UNIQUE, v INTEGER)",
                "INSERT INTO t VALUES ('a', 1)",
                "INSERT INTO t VALUES ('a', 100) ON CONFLICT DO UPDATE SET v = v + excluded.v",
            ],
            sql: "SELECT k, v FROM t ORDER BY k",
        },
        // multi-row batch: conflicting + fresh rows through the same statement
        Case {
            setup: &[
                "CREATE TABLE t(k TEXT UNIQUE, v INTEGER)",
                "INSERT INTO t VALUES ('a', 1), ('b', 2)",
                "INSERT INTO t VALUES ('a', 10), ('z', 26) ON CONFLICT DO UPDATE SET v = excluded.v",
            ],
            sql: "SELECT k, v FROM t ORDER BY k",
        },
        // composite UNIQUE index, omitted target
        Case {
            setup: &[
                "CREATE TABLE t(a INTEGER, b INTEGER, v TEXT)",
                "CREATE UNIQUE INDEX tab ON t(a, b)",
                "INSERT INTO t VALUES (1, 1, 'x'), (1, 2, 'y')",
                "INSERT INTO t VALUES (1, 1, 'NEW') ON CONFLICT DO UPDATE SET v = excluded.v",
            ],
            sql: "SELECT a, b, v FROM t ORDER BY a, b",
        },
        // secondary-index consistency after an omitted-target upsert
        Case {
            setup: &[
                "CREATE TABLE t(k TEXT UNIQUE, v INTEGER)",
                "INSERT INTO t VALUES ('a', 1)",
                "INSERT INTO t VALUES ('a', 42) ON CONFLICT DO UPDATE SET v = excluded.v",
            ],
            sql: "PRAGMA integrity_check",
        },
        // WITHOUT ROWID: PK conflict with omitted target
        Case {
            setup: &[
                "CREATE TABLE t(k TEXT PRIMARY KEY, v INTEGER) WITHOUT ROWID",
                "INSERT INTO t VALUES ('a', 1)",
                "INSERT INTO t VALUES ('a', 7) ON CONFLICT DO UPDATE SET v = excluded.v",
            ],
            sql: "SELECT k, v FROM t ORDER BY k",
        },
        // WITHOUT ROWID composite PK, omitted target
        Case {
            setup: &[
                "CREATE TABLE t(a INTEGER, b INTEGER, v TEXT, PRIMARY KEY(a, b)) WITHOUT ROWID",
                "INSERT INTO t VALUES (1, 1, 'x')",
                "INSERT INTO t VALUES (1, 1, 'NEW') ON CONFLICT DO UPDATE SET v = excluded.v",
            ],
            sql: "SELECT a, b, v FROM t ORDER BY a, b",
        },
        // omitted-target DO NOTHING still works (pre-existing surface)
        Case {
            setup: &[
                "CREATE TABLE t(k TEXT UNIQUE, v INTEGER)",
                "INSERT INTO t VALUES ('a', 1)",
                "INSERT INTO t VALUES ('a', 999) ON CONFLICT DO NOTHING",
            ],
            sql: "SELECT k, v FROM t ORDER BY k",
        },
        // parse rule: targetless clause must be the LAST ON CONFLICT clause
        // (both engines reject)
        Case {
            setup: &["CREATE TABLE t(k TEXT UNIQUE, v INTEGER)"],
            sql: "INSERT INTO t VALUES ('a', 1) ON CONFLICT DO NOTHING ON CONFLICT(k) DO NOTHING",
        },
        // RETURNING through the omitted-target conflict path
        Case {
            setup: &[
                "CREATE TABLE t(k TEXT UNIQUE, v INTEGER)",
                "INSERT INTO t VALUES ('a', 1)",
            ],
            sql: "INSERT INTO t VALUES ('a', 7) ON CONFLICT DO UPDATE SET v = excluded.v RETURNING k, v",
        },
    ];

    let mut divergences = Vec::new();
    for c in &cases {
        if c.sql.contains("RETURNING") {
            check_unordered(&mut divergences, c);
        } else {
            check(&mut divergences, c);
        }
    }
    if !divergences.is_empty() {
        let report = divergences.join("\n\n");
        panic!(
            "\n===== {} OMITTED-TARGET UPSERT DIVERGENCE(S) vs C SQLite (of {} cases) =====\n{}\n",
            divergences.len(),
            cases.len(),
            report
        );
    }
}
