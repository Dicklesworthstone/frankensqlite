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
        conn.execute_batch(s).map_err(|e| format!("setup `{s}`: {e}"))?;
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
        Case { setup: NO_SETUP, sql: "SELECT CAST('123abc' AS INTEGER)" },
        Case { setup: NO_SETUP, sql: "SELECT CAST('  -45  ' AS INTEGER)" },
        Case { setup: NO_SETUP, sql: "SELECT CAST('3.99' AS INTEGER)" },
        Case { setup: NO_SETUP, sql: "SELECT CAST('0x1F' AS INTEGER)" },
        Case { setup: NO_SETUP, sql: "SELECT CAST('1e3' AS INTEGER)" },
        Case { setup: NO_SETUP, sql: "SELECT CAST('1e3' AS REAL)" },
        Case { setup: NO_SETUP, sql: "SELECT CAST('abc' AS REAL)" },
        Case { setup: NO_SETUP, sql: "SELECT CAST('9999999999999999999999' AS INTEGER)" },
        Case { setup: NO_SETUP, sql: "SELECT CAST(3.9 AS INTEGER)" },
        Case { setup: NO_SETUP, sql: "SELECT CAST(-3.9 AS INTEGER)" },
        Case { setup: NO_SETUP, sql: "SELECT CAST(9223372036854775807 AS REAL)" },
        Case { setup: NO_SETUP, sql: "SELECT CAST(X'41' AS TEXT)" },
        Case { setup: NO_SETUP, sql: "SELECT CAST(123 AS TEXT)" },
        Case { setup: NO_SETUP, sql: "SELECT CAST(1.5 AS TEXT)" },
        Case { setup: NO_SETUP, sql: "SELECT CAST('  12.5xyz' AS REAL)" },
        // ---- integer overflow arithmetic (SQLite promotes to REAL on overflow) ----
        Case { setup: NO_SETUP, sql: "SELECT 9223372036854775807 + 1" },
        Case { setup: NO_SETUP, sql: "SELECT 9223372036854775807 * 2" },
        Case { setup: NO_SETUP, sql: "SELECT -9223372036854775808 - 1" },
        Case { setup: NO_SETUP, sql: "SELECT -9223372036854775808 / -1" },
        Case { setup: NO_SETUP, sql: "SELECT abs(-9223372036854775808)" },
        Case { setup: NO_SETUP, sql: "SELECT 5 / 2" },
        Case { setup: NO_SETUP, sql: "SELECT 5 % 0" },
        Case { setup: NO_SETUP, sql: "SELECT 5 / 0" },
        Case { setup: NO_SETUP, sql: "SELECT 5.0 / 0" },
        Case { setup: NO_SETUP, sql: "SELECT -5 % 3" },
        Case { setup: NO_SETUP, sql: "SELECT 5 % -3" },
        // ---- string functions ----
        Case { setup: NO_SETUP, sql: "SELECT substr('hello', -3)" },
        Case { setup: NO_SETUP, sql: "SELECT substr('hello', -3, 2)" },
        Case { setup: NO_SETUP, sql: "SELECT substr('hello', 0)" },
        Case { setup: NO_SETUP, sql: "SELECT substr('hello', 2, -1)" },
        Case { setup: NO_SETUP, sql: "SELECT substr('hello', 0, 2)" },
        Case { setup: NO_SETUP, sql: "SELECT replace('aaa', 'a', 'bb')" },
        Case { setup: NO_SETUP, sql: "SELECT replace('abc', '', 'X')" },
        Case { setup: NO_SETUP, sql: "SELECT trim('  xx  ')" },
        Case { setup: NO_SETUP, sql: "SELECT trim('xxhelloxx', 'x')" },
        Case { setup: NO_SETUP, sql: "SELECT ltrim('xxhello', 'x')" },
        Case { setup: NO_SETUP, sql: "SELECT rtrim('helloxx', 'x')" },
        Case { setup: NO_SETUP, sql: "SELECT instr('hello world', 'o')" },
        Case { setup: NO_SETUP, sql: "SELECT instr('hello', 'z')" },
        Case { setup: NO_SETUP, sql: "SELECT instr('hello', '')" },
        Case { setup: NO_SETUP, sql: "SELECT char(72, 105)" },
        Case { setup: NO_SETUP, sql: "SELECT unicode('A')" },
        Case { setup: NO_SETUP, sql: "SELECT unicode('')" },
        Case { setup: NO_SETUP, sql: "SELECT length('héllo')" },
        Case { setup: NO_SETUP, sql: "SELECT length(X'00010203')" },
        Case { setup: NO_SETUP, sql: "SELECT length(12345)" },
        Case { setup: NO_SETUP, sql: "SELECT length(1.5)" },
        Case { setup: NO_SETUP, sql: "SELECT quote('it''s')" },
        Case { setup: NO_SETUP, sql: "SELECT quote(X'DEADBEEF')" },
        Case { setup: NO_SETUP, sql: "SELECT quote(NULL)" },
        Case { setup: NO_SETUP, sql: "SELECT quote(3.14)" },
        Case { setup: NO_SETUP, sql: "SELECT hex('abc')" },
        Case { setup: NO_SETUP, sql: "SELECT hex(255)" },
        Case { setup: NO_SETUP, sql: "SELECT upper('héllo')" },
        Case { setup: NO_SETUP, sql: "SELECT lower('HÉLLO')" },
        Case { setup: NO_SETUP, sql: "SELECT printf('%d-%s-%.2f', 5, 'x', 3.14159)" },
        Case { setup: NO_SETUP, sql: "SELECT printf('%5d|%-5d|', 42, 42)" },
        Case { setup: NO_SETUP, sql: "SELECT printf('%x', 255)" },
        Case { setup: NO_SETUP, sql: "SELECT printf('%05.2f', 3.1)" },
        Case { setup: NO_SETUP, sql: "SELECT format('%d', 99)" },
        Case { setup: NO_SETUP, sql: "SELECT char(0x48) || 'i'" },
        // ---- round / numeric rendering ----
        Case { setup: NO_SETUP, sql: "SELECT round(2.5)" },
        Case { setup: NO_SETUP, sql: "SELECT round(3.5)" },
        Case { setup: NO_SETUP, sql: "SELECT round(-2.5)" },
        Case { setup: NO_SETUP, sql: "SELECT round(2.675, 2)" },
        Case { setup: NO_SETUP, sql: "SELECT round(1.0/3.0, 5)" },
        Case { setup: NO_SETUP, sql: "SELECT 0.1 + 0.2" },
        Case { setup: NO_SETUP, sql: "SELECT 1.0/3.0" },
        Case { setup: NO_SETUP, sql: "SELECT -0.0" },
        Case { setup: NO_SETUP, sql: "SELECT 1e308 * 10" },
        Case { setup: NO_SETUP, sql: "SELECT 2e-308 / 1e10" },
        // ---- typeof / affinity in comparisons ----
        Case { setup: NO_SETUP, sql: "SELECT typeof(1), typeof(1.0), typeof('1'), typeof(X'01'), typeof(NULL)" },
        Case { setup: NO_SETUP, sql: "SELECT 1 = 1.0, '1' = 1, '1.0' = 1.0, X'31' = '1'" },
        Case { setup: NO_SETUP, sql: "SELECT 10 < '9', '10' < '9', 10 < 9" },
        Case { setup: NO_SETUP, sql: "SELECT NULL = NULL, NULL IS NULL, 1 IS NOT NULL" },
        Case { setup: NO_SETUP, sql: "SELECT NULL + 1, NULL || 'x', NULL AND 0, NULL OR 1" },
        // ---- coalesce / ifnull / nullif ----
        Case { setup: NO_SETUP, sql: "SELECT coalesce(NULL, NULL, 3, 4)" },
        Case { setup: NO_SETUP, sql: "SELECT ifnull(NULL, 'x'), ifnull(5, 'x')" },
        Case { setup: NO_SETUP, sql: "SELECT nullif(5, 5), nullif(5, 6), nullif('a','a')" },
        Case { setup: NO_SETUP, sql: "SELECT max(1, 2.5, '3', NULL)" },
        Case { setup: NO_SETUP, sql: "SELECT min('b', 'a', 'c')" },
        // ---- ORDER BY / DISTINCT with mixed storage classes + NULLS ----
        Case {
            setup: &["CREATE TABLE t(x)", "INSERT INTO t VALUES (3),(1.5),('apple'),(NULL),(X'01'),(2),('Banana')"],
            sql: "SELECT typeof(x), x FROM t ORDER BY x",
        },
        Case {
            setup: &["CREATE TABLE t(x)", "INSERT INTO t VALUES (3),(1.5),('apple'),(NULL),(X'01'),(2)"],
            sql: "SELECT x FROM t ORDER BY x DESC",
        },
        Case {
            setup: &["CREATE TABLE t(x)", "INSERT INTO t VALUES (3),(NULL),(1),(NULL),(2)"],
            sql: "SELECT x FROM t ORDER BY x NULLS FIRST",
        },
        Case {
            setup: &["CREATE TABLE t(x)", "INSERT INTO t VALUES (3),(NULL),(1),(NULL),(2)"],
            sql: "SELECT x FROM t ORDER BY x DESC NULLS LAST",
        },
        Case {
            setup: &["CREATE TABLE t(x)", "INSERT INTO t VALUES (1),(1.0),('1'),(1),('1')"],
            sql: "SELECT DISTINCT typeof(x), x FROM t ORDER BY x, typeof(x)",
        },
        // ---- IN / BETWEEN / LIKE / GLOB ----
        Case { setup: NO_SETUP, sql: "SELECT 1 IN (1,2,3), 4 IN (1,2,3), NULL IN (1,2), 1 IN (NULL,1)" },
        Case { setup: NO_SETUP, sql: "SELECT 1 NOT IN (2,3), 1 NOT IN (NULL,2)" },
        Case { setup: NO_SETUP, sql: "SELECT 5 BETWEEN 1 AND 10, 5 BETWEEN 10 AND 1, 'b' BETWEEN 'a' AND 'c'" },
        Case { setup: NO_SETUP, sql: "SELECT 'abc' LIKE 'a%', 'abc' LIKE 'A%', 'a%c' LIKE 'a\\%c' ESCAPE '\\'" },
        Case { setup: NO_SETUP, sql: "SELECT 'abc' LIKE 'a_c', 'aXc' LIKE 'a_c', 'ac' LIKE 'a_c'" },
        Case { setup: NO_SETUP, sql: "SELECT 'Hello' GLOB 'H*o', 'hello' GLOB 'H*o', 'abc' GLOB 'a[bc]c'" },
        Case { setup: NO_SETUP, sql: "SELECT 'a.b' GLOB 'a?b', 'a%b' LIKE 'a[%]b'" },
        Case { setup: NO_SETUP, sql: "SELECT 'ABC' LIKE 'abc', 'straße' LIKE 'STRASSE'" },
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
            setup: &["CREATE TABLE t(x)", "INSERT INTO t VALUES (9223372036854775807),(1)"],
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
        Case { setup: NO_SETUP, sql: "SELECT CASE WHEN NULL THEN 'a' ELSE 'b' END" },
        Case { setup: NO_SETUP, sql: "SELECT CASE 1 WHEN 1.0 THEN 'eq' ELSE 'ne' END" },
        Case { setup: NO_SETUP, sql: "SELECT CASE WHEN 1 THEN 'a' END, CASE WHEN 0 THEN 'a' END" },
        // ---- date/time corners ----
        Case { setup: NO_SETUP, sql: "SELECT date('2026-01-31', '+1 month')" },
        Case { setup: NO_SETUP, sql: "SELECT date('2026-03-31', '-1 month')" },
        Case { setup: NO_SETUP, sql: "SELECT date('2024-02-29', '+1 year')" },
        Case { setup: NO_SETUP, sql: "SELECT strftime('%Y-%m-%d %H:%M:%f', '2026-06-30 12:34:56.789')" },
        Case { setup: NO_SETUP, sql: "SELECT julianday('2000-01-01')" },
        Case { setup: NO_SETUP, sql: "SELECT strftime('%w %j', '2026-06-30')" },
        Case { setup: NO_SETUP, sql: "SELECT date('2026-06-30', 'weekday 0')" },
        Case { setup: NO_SETUP, sql: "SELECT time('12:00', '+90 minutes')" },
        Case { setup: NO_SETUP, sql: "SELECT datetime('2026-06-30', 'start of month')" },
        // ---- rowid / implicit columns ----
        Case {
            setup: &["CREATE TABLE t(a)", "INSERT INTO t VALUES ('x'),('y'),('z')"],
            sql: "SELECT rowid, a FROM t ORDER BY rowid",
        },
        Case {
            setup: &["CREATE TABLE t(a INTEGER PRIMARY KEY, b)", "INSERT INTO t VALUES (10,'x'),(5,'y')"],
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
        Case { setup: WR1, sql: "INSERT INTO t VALUES ('b', 2), ('a', 1) RETURNING k, v" },
        Case { setup: WR1, sql: "INSERT INTO t VALUES ('c', 3) RETURNING *" },
        Case { setup: WR1, sql: "INSERT INTO t VALUES ('d', 4) RETURNING k, v * 10, v || '!'" },
        Case { setup: WR1, sql: "INSERT INTO t(v, k) VALUES (7, 'g') RETURNING k, v" },
        // DEFAULT VALUES with defaults that satisfy the PK's implicit NOT NULL.
        Case {
            setup: &["CREATE TABLE t(k TEXT PRIMARY KEY DEFAULT 'x', v INTEGER DEFAULT 9) WITHOUT ROWID"],
            sql: "INSERT INTO t DEFAULT VALUES RETURNING *",
        },
        // UPDATE ... RETURNING (NEW image)
        Case { setup: WR_SEED, sql: "UPDATE t SET v = v + 100 WHERE k = 'a' RETURNING k, v" },
        Case { setup: WR_SEED, sql: "UPDATE t SET v = v * 2 RETURNING k, v" },
        Case { setup: WR_SEED, sql: "UPDATE t SET k = 'zz' WHERE k = 'b' RETURNING *" },
        Case { setup: WR_SEED, sql: "UPDATE t SET v = v + 1 WHERE v >= 2 RETURNING k, v, v - 1" },
        // DELETE ... RETURNING (OLD/deleted image)
        Case { setup: WR_SEED, sql: "DELETE FROM t WHERE k = 'b' RETURNING k, v" },
        Case { setup: WR_SEED, sql: "DELETE FROM t WHERE v > 1 RETURNING *" },
        Case { setup: WR_SEED, sql: "DELETE FROM t RETURNING k" },
        // conflict semantics: OR IGNORE skips the conflicting row (no RETURNING row)
        Case { setup: WR_SEED, sql: "INSERT OR IGNORE INTO t VALUES ('a', 999), ('z', 1) RETURNING k, v" },
        // OR REPLACE replaces and returns the new image
        Case { setup: WR_SEED, sql: "INSERT OR REPLACE INTO t VALUES ('a', 555) RETURNING k, v" },
        // composite PK
        Case { setup: WR_COMPOSITE, sql: "INSERT INTO t VALUES (3, 3, 'w') RETURNING a, b, payload" },
        Case { setup: WR_COMPOSITE, sql: "UPDATE t SET payload = 'NEW' WHERE a = 1 RETURNING *" },
        Case { setup: WR_COMPOSITE, sql: "DELETE FROM t WHERE a = 1 RETURNING a, b, payload" },
        // WITHOUT ROWID PK is implicitly NOT NULL (bd-0re6l): these must be
        // rejected by both engines (NULL primary key), and OR IGNORE must skip.
        Case { setup: WR1, sql: "INSERT INTO t DEFAULT VALUES RETURNING *" },
        Case { setup: WR1, sql: "INSERT INTO t VALUES (NULL, 5) RETURNING k, v" },
        Case { setup: WR_COMPOSITE, sql: "INSERT INTO t VALUES (NULL, 9, 'q') RETURNING *" },
        Case { setup: WR_SEED, sql: "UPDATE t SET k = NULL WHERE k = 'a' RETURNING k, v" },
        // OR IGNORE on a NULL PK skips the row (no error, no RETURNING row)
        Case { setup: WR1, sql: "INSERT OR IGNORE INTO t VALUES (NULL, 1), ('ok', 2) RETURNING k, v" },
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
            setup: &["CREATE TABLE t(a INTEGER CHECK (a > 0))", "INSERT INTO t VALUES (5)"],
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
            setup: &["CREATE TABLE t(x)", "INSERT INTO t VALUES (1),(2),(3),(4),(5)"],
            sql: "SELECT x, sum(x) OVER (ORDER BY x ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) FROM t ORDER BY x",
        },
        Case {
            setup: &["CREATE TABLE t(x)", "INSERT INTO t VALUES (1),(1),(2),(2),(3)"],
            sql: "SELECT x, sum(x) OVER (ORDER BY x RANGE BETWEEN 1 PRECEDING AND CURRENT ROW) FROM t ORDER BY x",
        },
        Case {
            setup: &["CREATE TABLE t(x)", "INSERT INTO t VALUES (1),(2),(3),(4)"],
            sql: "SELECT x, sum(x) OVER (ORDER BY x GROUPS BETWEEN 1 PRECEDING AND CURRENT ROW) FROM t ORDER BY x",
        },
        Case {
            setup: &["CREATE TABLE t(x)", "INSERT INTO t VALUES (1),(2),(3),(4),(5)"],
            sql: "SELECT x, sum(x) OVER (ORDER BY x ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW EXCLUDE CURRENT ROW) FROM t ORDER BY x",
        },
        Case {
            setup: &["CREATE TABLE t(g, x)", "INSERT INTO t VALUES ('a',1),('a',2),('b',3),('b',4)"],
            sql: "SELECT g, x, lag(x, 1, -1) OVER (PARTITION BY g ORDER BY x) FROM t ORDER BY g, x",
        },
        Case {
            setup: &["CREATE TABLE t(x)", "INSERT INTO t VALUES (1),(2),(3),(4),(5),(6)"],
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
            setup: &["CREATE TABLE t(a, b)", "INSERT INTO t VALUES (3,'c'),(1,'a'),(2,'b')"],
            sql: "SELECT a AS k, b FROM t ORDER BY k DESC",
        },
        Case {
            setup: &["CREATE TABLE t(a, b)", "INSERT INTO t VALUES (3,'c'),(1,'a'),(2,'b')"],
            sql: "SELECT a, b FROM t ORDER BY 2 DESC",
        },
        Case {
            setup: NO_SETUP,
            sql: "SELECT 1 AS x UNION SELECT 3 UNION SELECT 2 ORDER BY x DESC",
        },
        // ---- VALUES as a standalone query ----
        Case { setup: NO_SETUP, sql: "VALUES (1,2),(3,4),(5,6)" },
        Case { setup: NO_SETUP, sql: "SELECT * FROM (VALUES (1),(2),(3)) ORDER BY 1 DESC" },
        // ---- numeric literal parsing ----
        Case { setup: NO_SETUP, sql: "SELECT 0x1F, 0xFF, .5, 5., 1_000" },
        Case { setup: NO_SETUP, sql: "SELECT 1.5e2, 1E3, 0xABCDEF" },
        // ---- digit separators (SQLite 3.46+): underscore must be between two digits ----
        Case { setup: NO_SETUP, sql: "SELECT 1_000_000" },
        Case { setup: NO_SETUP, sql: "SELECT 1_0_0" },
        Case { setup: NO_SETUP, sql: "SELECT 1_000.5" },
        Case { setup: NO_SETUP, sql: "SELECT 1.0_5" },
        Case { setup: NO_SETUP, sql: "SELECT .5_0" },
        Case { setup: NO_SETUP, sql: "SELECT 1_0e2" },
        Case { setup: NO_SETUP, sql: "SELECT 1e1_0" },
        Case { setup: NO_SETUP, sql: "SELECT 0x1_F" },
        Case { setup: NO_SETUP, sql: "SELECT 0xFF_FF" },
        Case { setup: NO_SETUP, sql: "SELECT 9_223_372_036_854_775_807" },
        // these must be REJECTED by both engines (underscore not between two digits)
        Case { setup: NO_SETUP, sql: "SELECT 1__0" },
        Case { setup: NO_SETUP, sql: "SELECT 100_" },
        Case { setup: NO_SETUP, sql: "SELECT 5_.0" },
        Case { setup: NO_SETUP, sql: "SELECT 1_.5" },
        Case { setup: NO_SETUP, sql: "SELECT 1._5" },
        Case { setup: NO_SETUP, sql: "SELECT 0x_1F" },
        Case { setup: NO_SETUP, sql: "SELECT 1e_2" },
        Case { setup: NO_SETUP, sql: "SELECT 1_e2" },
        Case { setup: NO_SETUP, sql: "SELECT 0x1F_" },
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
