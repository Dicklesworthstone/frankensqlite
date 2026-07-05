//! Differential RESULT parity gate: assert that the actual *values* returned by
//! FrankenSQLite match C SQLite (rusqlite, bundled = fixed version) for scalar /
//! numeric / CAST / string / math / date-time edge cases, plus a harder batch
//! covering float-to-text rendering, aggregate value semantics, window-function
//! values, and NULL-bearing subquery logic.
//!
//! This complements `codegen_gap_probe.rs`, which only classifies execution
//! SUCCESS vs FAILURE. A construct that *runs* on both engines but returns a
//! different value (or a different storage class) is invisible to that probe —
//! this gate catches exactly those wrong-answer divergences, which are the most
//! likely clean-room divergence for hand-rolled scalar/math/date/dtoa semantics.
//! This result-asserting surface (scalar/numeric/cast/string/math/date/float
//! rendering/aggregate/window/subquery) previously had no parity gate; the
//! join/aggregate/grouping surface is covered by core_sql_rusqlite_conformance.
//!
//! Frank runs on a real FILE-BACKED database (the production VDBE + pager +
//! btree path), not `:memory:`, so the gate exercises the lowering the engine
//! uses in practice. rusqlite (in-memory) is the oracle; SQL scalar semantics
//! are storage-path-independent in real SQLite.
//!
//! Parity rule per case: both engines accept and return identical tagged rows;
//! or both reject (a query that errors on C SQLite is allowed to error on Frank
//! — observable outcome matches). A divergence (different values, different
//! storage class, or one accepts while the other rejects) fails the gate.

use fsqlite_core::connection::Connection;
use fsqlite_types::value::SqliteValue;

/// Render one value with an explicit storage-class tag so a type divergence
/// (e.g. Frank Integer where SQLite promotes to Real) is caught as a mismatch,
/// not a silent match.
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

/// Run `sql` on a fresh file-backed FrankenSQLite db; return tagged rows or an
/// error string.
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
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| e.to_string())?;
    Ok(rows)
}

/// Assert FrankenSQLite and C SQLite agree on `sql`.
#[track_caller]
fn check(label: &str, setup: &[&str], sql: &str) {
    let f = frank_rows(setup, sql);
    let s = sqlite_rows(setup, sql);
    match (f, s) {
        (Ok(fr), Ok(sr)) => assert_eq!(
            fr, sr,
            "PARITY MISMATCH [{label}]: {sql}\n  frank  = {fr:?}\n  sqlite = {sr:?}"
        ),
        // Both engines reject: observable outcome matches (correct rejection).
        (Err(_), Err(_)) => {}
        (Ok(fr), Err(se)) => panic!(
            "PARITY MISMATCH [{label}]: {sql}\n  frank ACCEPTED = {fr:?}\n  sqlite REJECTED = {se}"
        ),
        (Err(fe), Ok(sr)) => panic!(
            "PARITY MISMATCH [{label}]: {sql}\n  frank REJECTED = {fe}\n  sqlite ACCEPTED = {sr:?}"
        ),
    }
}

/// Parity gate for higher-complexity surfaces: collation (NOCASE/RTRIM) in
/// GROUP BY / DISTINCT / comparison, generated columns, recursive CTEs,
/// compound set ops, UPSERT, PRAGMA introspection, and JSON operators.
///
/// Includes the regression guard for the GROUP BY-ignores-COLLATE bug
/// (bd-cdl4w): the VDBE storage-substrate GROUP BY path keyed grouping on raw
/// BINARY values, so `GROUP BY t` on a `COLLATE NOCASE` column grouped
/// 'Apple'/'apple' separately. Fixed by disqualifying that fast path when a
/// GROUP BY key has a non-BINARY collation.
#[test]
fn collation_complex_parity() {
    let none: &[&str] = &[];

    // ── generated columns (VIRTUAL + STORED) ──
    let gc = &[
        "CREATE TABLE gc (a INT, b INT, c INT GENERATED ALWAYS AS (a + b) VIRTUAL, d INT AS (a * b) STORED)",
        "INSERT INTO gc (a, b) VALUES (2, 3),(4, 5),(0, 7)",
    ][..];
    check("gencol_select", gc, "SELECT a, b, c, d FROM gc ORDER BY a");
    check(
        "gencol_where",
        gc,
        "SELECT a FROM gc WHERE c > 6 ORDER BY a",
    );
    check(
        "gencol_typeof",
        gc,
        "SELECT typeof(c), typeof(d) FROM gc LIMIT 1",
    );
    let gci = &[
        "CREATE TABLE gci (a INT, b INT, s INT AS (a + b) STORED)",
        "CREATE INDEX gci_s ON gci(s)",
        "INSERT INTO gci (a, b) VALUES (1, 1),(2, 2),(3, 3)",
    ][..];
    check("gencol_indexed", gci, "SELECT a FROM gci WHERE s = 4");

    // ── recursive CTEs ──
    check(
        "rcte_count",
        none,
        "WITH RECURSIVE cnt(x) AS (SELECT 1 UNION ALL SELECT x + 1 FROM cnt WHERE x < 5) SELECT x FROM cnt ORDER BY x",
    );
    check(
        "rcte_fib",
        none,
        "WITH RECURSIVE fib(a, b) AS (SELECT 0, 1 UNION ALL SELECT b, a + b FROM fib WHERE b < 50) SELECT a FROM fib ORDER BY a",
    );
    check(
        "rcte_limit",
        none,
        "WITH RECURSIVE cnt(x) AS (SELECT 1 UNION ALL SELECT x + 1 FROM cnt) SELECT x FROM cnt LIMIT 4",
    );
    let tree = &[
        "CREATE TABLE org (id INT PRIMARY KEY, mgr INT, name TEXT)",
        "INSERT INTO org VALUES (1,NULL,'ceo'),(2,1,'vp'),(3,2,'dir'),(4,1,'cfo')",
    ][..];
    check(
        "rcte_tree",
        tree,
        "WITH RECURSIVE chain(id, name, depth) AS (SELECT id, name, 0 FROM org WHERE mgr IS NULL UNION ALL SELECT o.id, o.name, c.depth + 1 FROM org o JOIN chain c ON o.mgr = c.id) SELECT name, depth FROM chain ORDER BY depth, name",
    );

    // ── collation (NOCASE / RTRIM) ──
    let col = &[
        "CREATE TABLE col (t TEXT COLLATE NOCASE)",
        "INSERT INTO col VALUES ('Apple'),('apple'),('BANANA'),('banana'),('Cherry')",
    ][..];
    check(
        "coll_distinct_nocase",
        col,
        "SELECT DISTINCT t FROM col ORDER BY t",
    );
    check(
        "coll_groupby_nocase",
        col,
        "SELECT t, count(*) FROM col GROUP BY t ORDER BY t",
    );
    check(
        "coll_eq_explicit",
        none,
        "SELECT 'ABC' = 'abc' COLLATE NOCASE",
    );
    check(
        "coll_order_explicit",
        col,
        "SELECT t FROM col ORDER BY t COLLATE BINARY",
    );
    check(
        "coll_rtrim_eq",
        none,
        "SELECT 'abc' = 'abc   ' COLLATE RTRIM",
    );
    check(
        "coll_in_nocase",
        col,
        "SELECT count(*) FROM col WHERE t = 'APPLE'",
    );
    // GROUP BY collation regression variants (bd-cdl4w): each routes through the
    // VDBE-substrate eligibility guard to the collation-aware grouping path.
    let coln = &[
        "CREATE TABLE coln (t TEXT COLLATE NOCASE, n INT)",
        "INSERT INTO coln VALUES ('Apple',1),('apple',2),('BANANA',3),('banana',4),('Cherry',5)",
    ][..];
    check(
        "coll_groupby_sum_nocase",
        coln,
        "SELECT t, sum(n) FROM coln GROUP BY t ORDER BY t",
    );
    let colb = &[
        "CREATE TABLE colb (t TEXT, n INT)",
        "INSERT INTO colb VALUES ('Apple',1),('apple',2),('BANANA',3),('banana',4)",
    ][..];
    check(
        "coll_groupby_explicit_collate",
        colb,
        "SELECT t COLLATE NOCASE, sum(n) FROM colb GROUP BY t COLLATE NOCASE ORDER BY 1",
    );
    let colr = &[
        "CREATE TABLE colr (t TEXT COLLATE RTRIM)",
        "INSERT INTO colr VALUES ('ab'),('ab  '),('ab '),('cd')",
    ][..];
    check(
        "coll_groupby_rtrim",
        colr,
        "SELECT t, count(*) FROM colr GROUP BY t ORDER BY t",
    );

    // ── compound set operations ──
    let cs = &[
        "CREATE TABLE x (v INT)",
        "CREATE TABLE y (v INT)",
        "INSERT INTO x VALUES (1),(2),(2),(3)",
        "INSERT INTO y VALUES (2),(3),(4)",
    ][..];
    check(
        "compound_union",
        cs,
        "SELECT v FROM x UNION SELECT v FROM y ORDER BY v",
    );
    check(
        "compound_union_all",
        cs,
        "SELECT v FROM x UNION ALL SELECT v FROM y ORDER BY v",
    );
    check(
        "compound_intersect",
        cs,
        "SELECT v FROM x INTERSECT SELECT v FROM y ORDER BY v",
    );
    check(
        "compound_except",
        cs,
        "SELECT v FROM x EXCEPT SELECT v FROM y ORDER BY v",
    );
    check(
        "compound_coerce",
        none,
        "SELECT 1 UNION SELECT 1.0 UNION SELECT '1' ORDER BY 1",
    );

    // ── UPSERT ──
    let up = &[
        "CREATE TABLE up (id INTEGER PRIMARY KEY, k TEXT UNIQUE, n INT)",
        "INSERT INTO up VALUES (1,'a',10),(2,'b',20)",
    ][..];
    check(
        "upsert_do_update",
        up,
        "INSERT INTO up VALUES (3,'a',5) ON CONFLICT(k) DO UPDATE SET n = n + excluded.n RETURNING id, k, n",
    );
    check(
        "upsert_do_nothing",
        up,
        "INSERT INTO up VALUES (4,'b',99) ON CONFLICT(k) DO NOTHING RETURNING id",
    );

    // ── PRAGMA introspection ──
    let pi =
        &["CREATE TABLE pt (id INTEGER PRIMARY KEY, name TEXT NOT NULL, val REAL DEFAULT 1.5)"][..];
    check(
        "pragma_table_info",
        pi,
        "SELECT cid, name, type, \"notnull\", dflt_value, pk FROM pragma_table_info('pt') ORDER BY cid",
    );

    // ── JSON operators (extension; may be unwired) ──
    check(
        "json_extract",
        none,
        "SELECT json_extract('{\"a\":1,\"b\":[2,3]}', '$.a')",
    );
    check("json_arrow", none, "SELECT '{\"a\":1}' -> '$.a'");
    check("json_arrow2", none, "SELECT '{\"a\":1}' ->> '$.a'");
    check(
        "json_array_len",
        none,
        "SELECT json_array_length('[1,2,3,4]')",
    );
    check("json_type", none, "SELECT json_type('{\"a\":1}', '$.a')");
}

/// Parity gate for collation PROPAGATION beyond GROUP BY (the family of the
/// bd-cdl4w bug): ORDER BY / range comparison / IN / BETWEEN / min-max /
/// DISTINCT / index lookup / UNION survivor-case on `COLLATE NOCASE` and
/// `COLLATE RTRIM` columns. (UNION survivor case fixed under bd-a6mlo.)
#[test]
fn collation_propagation_parity() {
    let p = check;
    let nc = &[
        "CREATE TABLE nc (t TEXT COLLATE NOCASE, n INT)",
        "INSERT INTO nc VALUES ('Banana',1),('apple',2),('Apple',3),('CHERRY',4),('banana',5)",
    ][..];
    p(
        "order_by_nocase_implicit",
        nc,
        "SELECT t FROM nc ORDER BY t",
    );
    p(
        "order_by_nocase_then_n",
        nc,
        "SELECT t, n FROM nc ORDER BY t, n",
    );
    p(
        "distinct_nocase_multi",
        nc,
        "SELECT DISTINCT t FROM nc ORDER BY t",
    );
    p(
        "range_gt_nocase",
        nc,
        "SELECT t FROM nc WHERE t > 'b' ORDER BY n",
    );
    p(
        "between_nocase",
        nc,
        "SELECT t FROM nc WHERE t BETWEEN 'a' AND 'c' ORDER BY n",
    );
    p(
        "in_list_nocase",
        nc,
        "SELECT n FROM nc WHERE t IN ('APPLE','cherry') ORDER BY n",
    );
    p("min_max_nocase", nc, "SELECT min(t), max(t) FROM nc");
    p(
        "count_eq_nocase",
        nc,
        "SELECT count(*) FROM nc WHERE t = 'BANANA'",
    );
    p(
        "groupby_having_nocase",
        nc,
        "SELECT t, count(*) FROM nc GROUP BY t HAVING count(*) >= 2 ORDER BY t",
    );

    // bd-a6mlo: UNION survivor case under NOCASE. C SQLite keeps the value from
    // the LAST compound arm containing the key; within that arm the FIRST
    // occurrence wins when an outer ORDER BY is present, the LAST when it is not.
    p(
        "union_nocase_order_by",
        nc,
        "SELECT t FROM nc UNION SELECT t FROM nc ORDER BY t",
    );
    p(
        "union_nocase_no_order",
        nc,
        "SELECT t FROM nc UNION SELECT t FROM nc",
    );
    p(
        "union_nocase_desc",
        nc,
        "SELECT t FROM nc UNION SELECT t FROM nc ORDER BY t DESC",
    );
    p(
        "union_nocase_key_left_only",
        nc,
        "SELECT t FROM nc UNION SELECT 'zzz' ORDER BY t",
    );
    p(
        "union_nocase_three_arm",
        nc,
        "SELECT t FROM nc UNION SELECT upper(t) FROM nc UNION SELECT lower(t) FROM nc ORDER BY t",
    );
    p(
        "union_nocase_three_arm_noord",
        nc,
        "SELECT t FROM nc UNION SELECT upper(t) FROM nc UNION SELECT lower(t) FROM nc",
    );
    p(
        "union_nocase_swap_arms",
        nc,
        "SELECT t FROM nc WHERE n<=3 UNION SELECT t FROM nc WHERE n>=4 ORDER BY t",
    );
    p(
        "union_nocase_swap_noord",
        nc,
        "SELECT t FROM nc WHERE n<=3 UNION SELECT t FROM nc WHERE n>=4",
    );

    // index on a NOCASE column
    let idx = &[
        "CREATE TABLE idx (id INTEGER PRIMARY KEY, t TEXT COLLATE NOCASE)",
        "CREATE INDEX idx_t ON idx(t)",
        "INSERT INTO idx (t) VALUES ('Apple'),('apple'),('Banana'),('BANANA')",
    ][..];
    p(
        "index_eq_nocase",
        idx,
        "SELECT count(*) FROM idx WHERE t = 'apple'",
    );
    p("index_order_nocase", idx, "SELECT t FROM idx ORDER BY t");

    // RTRIM column
    let rt = &[
        "CREATE TABLE rt (t TEXT COLLATE RTRIM)",
        "INSERT INTO rt VALUES ('ab'),('ab  '),('cd '),('cd')",
    ][..];
    p("order_by_rtrim", rt, "SELECT t FROM rt ORDER BY t, rowid");
    p("distinct_rtrim", rt, "SELECT DISTINCT t FROM rt ORDER BY t");
    p("eq_rtrim", rt, "SELECT count(*) FROM rt WHERE t = 'ab'");
}

#[test]
fn scalar_parity_basic() {
    let none: &[&str] = &[];

    // ───────────────────────── integer / real arithmetic ─────────────────────
    check("int_overflow_add", none, "SELECT 9223372036854775807 + 1");
    check("int_overflow_sub", none, "SELECT -9223372036854775807 - 2");
    check("int_overflow_mul", none, "SELECT 9223372036854775807 * 2");
    check("int_div_floor", none, "SELECT 5 / 2");
    check("int_div_neg", none, "SELECT -5 / 2");
    check("real_div", none, "SELECT 5.0 / 2");
    check("div_by_zero_int", none, "SELECT 5 / 0");
    check("div_by_zero_real", none, "SELECT 5.0 / 0");
    check("mod_pos", none, "SELECT 7 % 3");
    check("mod_neg_dividend", none, "SELECT -7 % 3");
    check("mod_neg_divisor", none, "SELECT 7 % -3");
    check("mod_by_zero", none, "SELECT 7 % 0");
    check("shift_left_63", none, "SELECT 1 << 63");
    check("shift_left_64", none, "SELECT 1 << 64");
    check("shift_right_neg", none, "SELECT -8 >> 1");
    check("bitand", none, "SELECT 12 & 10");
    check("bitor", none, "SELECT 12 | 10");
    check("bitnot", none, "SELECT ~0");
    check("hex_literal", none, "SELECT 0x7fffffff");
    check("unary_minus_real", none, "SELECT -3.5");
    check("concat_with_null", none, "SELECT 'a' || 1 || NULL");
    check("concat_num", none, "SELECT 1 || 2");

    // ───────────────────────── typeof / storage class ────────────────────────
    check("typeof_int", none, "SELECT typeof(1)");
    check("typeof_real", none, "SELECT typeof(1.0)");
    check("typeof_div", none, "SELECT typeof(1/2)");
    check("typeof_realdiv", none, "SELECT typeof(3/2.0)");
    check(
        "typeof_overflow",
        none,
        "SELECT typeof(9223372036854775807+1)",
    );
    check("typeof_concat", none, "SELECT typeof(1||2)");
    check("typeof_null", none, "SELECT typeof(NULL)");

    // ───────────────────────── CAST semantics ────────────────────────────────
    check(
        "cast_text_int_partial",
        none,
        "SELECT CAST('123abc' AS INTEGER)",
    );
    check("cast_text_int_none", none, "SELECT CAST('abc' AS INTEGER)");
    check("cast_text_int_ws", none, "SELECT CAST('  12 ' AS INTEGER)");
    check("cast_text_int_float", none, "SELECT CAST('3.9' AS INTEGER)");
    check("cast_real_int_trunc", none, "SELECT CAST(3.9 AS INTEGER)");
    check(
        "cast_real_int_negtrunc",
        none,
        "SELECT CAST(-3.9 AS INTEGER)",
    );
    check("cast_text_int_exp", none, "SELECT CAST('1e3' AS INTEGER)");
    check("cast_text_real_exp", none, "SELECT CAST('1e3' AS REAL)");
    check(
        "cast_text_int_hexstr",
        none,
        "SELECT CAST('0x1F' AS INTEGER)",
    );
    check(
        "cast_huge_int",
        none,
        "SELECT CAST('99999999999999999999999' AS INTEGER)",
    );
    check("cast_int_text", none, "SELECT CAST(123 AS TEXT)");
    check("cast_real_text", none, "SELECT CAST(1.5 AS TEXT)");
    check("cast_blob_text", none, "SELECT CAST(x'414243' AS TEXT)");
    check("cast_int_real", none, "SELECT CAST(7 AS REAL)");
    check("cast_text_numeric", none, "SELECT CAST('3.0' AS NUMERIC)");
    check("cast_real_numeric", none, "SELECT CAST(3.0 AS NUMERIC)");

    // ───────────────────────── comparison / affinity ─────────────────────────
    check("cmp_int_lt_text", none, "SELECT 1 < 'a'");
    check("cmp_text_numbers", none, "SELECT '10' < '9'");
    check("cmp_int_numbers", none, "SELECT 10 < 9");
    check("cmp_null_eq", none, "SELECT NULL = NULL");
    check("cmp_null_is", none, "SELECT NULL IS NULL");
    check("and_null_false", none, "SELECT NULL AND 0");
    check("and_null_true", none, "SELECT NULL AND 1");
    check("or_null_true", none, "SELECT NULL OR 1");
    check("in_affinity", none, "SELECT '2' IN (2)");
    check("in_int_reals", none, "SELECT 2 IN (1.0, 2.0)");
    check("between_text", none, "SELECT 'b' BETWEEN 'a' AND 'c'");

    // ───────────────────────── NULL / conditional builtins ───────────────────
    check("coalesce", none, "SELECT coalesce(NULL, NULL, 3)");
    check("nullif_eq", none, "SELECT nullif(1, 1)");
    check("nullif_ne", none, "SELECT nullif(1, 2)");
    check("ifnull", none, "SELECT ifnull(NULL, 5)");
    check("iif_null_cond", none, "SELECT iif(NULL, 'a', 'b')");
    check("scalar_max_null", none, "SELECT max(1, NULL, 3)");
    check("scalar_min_null", none, "SELECT min(3, NULL, 1)");
    check("scalar_max", none, "SELECT max(1, 5, 3)");

    // ───────────────────────── string functions ──────────────────────────────
    check("substr_neg_start", none, "SELECT substr('hello', -3)");
    check(
        "substr_neg_start_len",
        none,
        "SELECT substr('hello', -3, 2)",
    );
    check("substr_zero_start", none, "SELECT substr('hello', 0, 2)");
    check("substr_neg_len", none, "SELECT substr('hello', 4, -2)");
    check("substr_past_end", none, "SELECT substr('hello', 10)");
    check("instr_found", none, "SELECT instr('hello', 'l')");
    check("instr_missing", none, "SELECT instr('hello', 'x')");
    check("instr_empty", none, "SELECT instr('hello', '')");
    check("replace_grow", none, "SELECT replace('aaa', 'a', 'bb')");
    check("replace_empty_pat", none, "SELECT replace('abc', '', 'x')");
    check("length_unicode", none, "SELECT length('héllo')");
    check("length_blob", none, "SELECT length(x'0102')");
    check("upper_unicode", none, "SELECT upper('héllo')");
    check("trim_custom", none, "SELECT trim('xxhixx', 'x')");
    check("ltrim_default", none, "SELECT ltrim('   hi')");
    check("hex_int", none, "SELECT hex(12345)");
    check("hex_blob", none, "SELECT hex(x'00ff')");
    check("char_fn", none, "SELECT char(72, 105)");
    check("unicode_fn", none, "SELECT unicode('A')");
    check("quote_text", none, "SELECT quote('a''b')");
    check("quote_blob", none, "SELECT quote(x'00ff')");
    check("quote_null", none, "SELECT quote(NULL)");
    check("quote_real", none, "SELECT quote(3.5)");
    check("printf_int_str", none, "SELECT printf('%d-%s', 5, 'x')");
    check("printf_float", none, "SELECT printf('%.2f', 3.14159)");
    check("printf_width", none, "SELECT printf('%5d', 3)");
    check("printf_hex", none, "SELECT printf('%x', 255)");
    check("printf_pct", none, "SELECT printf('%d%%', 50)");
    check("printf_neg", none, "SELECT printf('%05.1f', -2.5)");

    // ───────────────────────── math functions ────────────────────────────────
    check("round_half_even25", none, "SELECT round(2.5)");
    check("round_half_35", none, "SELECT round(3.5)");
    check("round_neg_half", none, "SELECT round(-2.5)");
    check("round_ndigits", none, "SELECT round(2.567, 2)");
    check("round_int", none, "SELECT round(5)");
    check("abs_neg", none, "SELECT abs(-5)");
    check("abs_real", none, "SELECT abs(-5.5)");
    check("sign_neg", none, "SELECT sign(-3)");
    check("sign_zero", none, "SELECT sign(0)");
    check("ceil_pos", none, "SELECT ceil(2.1)");
    check("floor_neg", none, "SELECT floor(-2.1)");
    check("trunc_neg", none, "SELECT trunc(-2.9)");
    check("pow_fn", none, "SELECT pow(2, 10)");
    check("power_op", none, "SELECT 2 * pow(2, 3)");
    check("sqrt_fn", none, "SELECT sqrt(2)");
    check("log10_default", none, "SELECT log(100)");
    check("log_base", none, "SELECT log(2, 8)");
    check("ln_fn", none, "SELECT ln(exp(1))");
    check("mod_math", none, "SELECT mod(7, 3)");
    check("pi_fn", none, "SELECT pi()");

    // ───────────────────────── date / time functions ─────────────────────────
    check(
        "date_add_year_leap",
        none,
        "SELECT date('2024-02-29', '+1 year')",
    );
    check(
        "date_add_month_overflow",
        none,
        "SELECT date('2024-01-31', '+1 month')",
    );
    check("date_sub_day", none, "SELECT date('2024-03-01', '-1 day')");
    check(
        "datetime_unixepoch",
        none,
        "SELECT datetime(0, 'unixepoch')",
    );
    check("strftime_dow", none, "SELECT strftime('%w', '2024-06-16')");
    check(
        "strftime_doy_leap",
        none,
        "SELECT strftime('%j', '2024-03-01')",
    );
    check(
        "strftime_full",
        none,
        "SELECT strftime('%Y-%m-%d %H:%M:%S', '2024-06-15 09:30:45')",
    );
    check(
        "julianday_epoch",
        none,
        "SELECT julianday('1970-01-01 00:00:00')",
    );
    check(
        "unixepoch_fn",
        none,
        "SELECT unixepoch('2024-01-01 00:00:00')",
    );
    check(
        "time_modifier",
        none,
        "SELECT time('2024-06-15 09:30:45', '+90 minutes')",
    );
    check(
        "date_weekday",
        none,
        "SELECT date('2024-06-10', 'weekday 0')",
    );
    check(
        "date_start_of_month",
        none,
        "SELECT date('2024-06-15', 'start of month')",
    );

    // ───────────────────────── stored-value affinity round-trips ─────────────
    let aff = &[
        "CREATE TABLE af (i INTEGER, r REAL, t TEXT, n NUMERIC, b BLOB)",
        "INSERT INTO af VALUES ('123', '4.5', 678, '9.0', 'x')",
        "INSERT INTO af VALUES (4.0, 5, '6', 7.5, 99)",
    ][..];
    check(
        "affinity_typeof_cols",
        aff,
        "SELECT typeof(i), typeof(r), typeof(t), typeof(n), typeof(b) FROM af ORDER BY rowid",
    );
    check(
        "affinity_values",
        aff,
        "SELECT i, r, t, n, b FROM af ORDER BY rowid",
    );

    // mixed-type ORDER BY (NULL < INT/REAL < TEXT < BLOB)
    let mixed = &[
        "CREATE TABLE m (v)",
        "INSERT INTO m VALUES (NULL),(2),(1.5),('apple'),(x'01'),(10),('9')",
    ][..];
    check(
        "mixed_order_by",
        mixed,
        "SELECT v, typeof(v) FROM m ORDER BY v",
    );
}

#[test]
fn scalar_parity_hard() {
    let none: &[&str] = &[];

    // ───────────────────────── float → text rendering ────────────────────────
    check("f2t_concat_01", none, "SELECT '' || 0.1");
    check("f2t_concat_10", none, "SELECT '' || 1.0");
    check("f2t_concat_1000", none, "SELECT '' || 100.0");
    check("f2t_concat_1e20", none, "SELECT '' || 1e20");
    check("f2t_concat_1e-7", none, "SELECT '' || 1e-7");
    check("f2t_concat_0001", none, "SELECT '' || 0.0001");
    check("f2t_concat_third", none, "SELECT '' || (1.0/3.0)");
    check("f2t_concat_sum", none, "SELECT '' || (0.1 + 0.2)");
    check("f2t_concat_big", none, "SELECT '' || 123456789012345.0");
    check("f2t_concat_neg", none, "SELECT '' || -2.5");
    check("f2t_concat_small", none, "SELECT '' || 2.5e-300");
    check("f2t_concat_large", none, "SELECT '' || 1.5e300");
    check("f2t_cast_01", none, "SELECT CAST(0.1 AS TEXT)");
    check("f2t_cast_third", none, "SELECT CAST(1.0/3.0 AS TEXT)");
    check("f2t_printf_s", none, "SELECT printf('%s', 0.1)");
    check("f2t_printf_g", none, "SELECT printf('%g', 0.1)");
    check("f2t_printf_e", none, "SELECT printf('%e', 12345.678)");
    check("f2t_printf_f", none, "SELECT printf('%f', 0.1)");
    check("f2t_quote_third", none, "SELECT quote(1.0/3.0)");
    check("f2t_round_third", none, "SELECT round(1.0/3.0, 5)");
    check("f2t_round_text", none, "SELECT '' || round(2.0/3.0, 10)");
    check("f2t_real_neg0", none, "SELECT '' || (0.0 * -1)");

    // ───────────────────────── aggregate value semantics ─────────────────────
    let agg = &[
        "CREATE TABLE g (k INTEGER, v)",
        "INSERT INTO g VALUES (1, 10),(1, 20),(2, NULL),(2, 5),(3, 3.5),(3, 'txt')",
    ][..];
    check(
        "agg_sum_mixed",
        agg,
        "SELECT k, sum(v), typeof(sum(v)) FROM g GROUP BY k ORDER BY k",
    );
    check("agg_total", agg, "SELECT total(v) FROM g");
    check(
        "agg_avg",
        agg,
        "SELECT k, avg(v) FROM g GROUP BY k ORDER BY k",
    );
    check(
        "agg_count_v",
        agg,
        "SELECT k, count(v), count(*) FROM g GROUP BY k ORDER BY k",
    );
    check("agg_minmax_mixed", agg, "SELECT min(v), max(v) FROM g");
    check("agg_sum_empty", agg, "SELECT sum(v) FROM g WHERE k = 99");
    check(
        "agg_total_empty",
        agg,
        "SELECT total(v) FROM g WHERE k = 99",
    );
    check("agg_avg_empty", agg, "SELECT avg(v) FROM g WHERE k = 99");
    check(
        "agg_group_concat",
        agg,
        "SELECT k, group_concat(v) FROM g GROUP BY k ORDER BY k",
    );
    check(
        "agg_group_concat_sep",
        agg,
        "SELECT group_concat(v, '|') FROM g WHERE v IS NOT NULL",
    );
    check("agg_count_distinct", agg, "SELECT count(DISTINCT k) FROM g");
    check(
        "agg_bare_col",
        agg,
        "SELECT k, v FROM g GROUP BY k ORDER BY k",
    );

    // sum() integer overflow → SQLite errors ("integer overflow"); Frank rejects too.
    let ov = &[
        "CREATE TABLE o (v INTEGER)",
        "INSERT INTO o VALUES (9223372036854775807),(9223372036854775807)",
    ][..];
    check("agg_sum_overflow", ov, "SELECT sum(v) FROM o");
    check("agg_total_overflow", ov, "SELECT total(v) FROM o");

    // ───────────────────────── window-function values ────────────────────────
    let w = &[
        "CREATE TABLE w (id INTEGER PRIMARY KEY, p INTEGER, v INTEGER)",
        "INSERT INTO w VALUES (1,1,10),(2,1,20),(3,1,30),(4,2,40),(5,2,50)",
    ][..];
    check(
        "win_lag_default",
        w,
        "SELECT id, lag(v) OVER (ORDER BY id) FROM w ORDER BY id",
    );
    check(
        "win_lag_n_def",
        w,
        "SELECT id, lag(v, 2, -1) OVER (ORDER BY id) FROM w ORDER BY id",
    );
    check(
        "win_lead",
        w,
        "SELECT id, lead(v) OVER (ORDER BY id) FROM w ORDER BY id",
    );
    check(
        "win_nth",
        w,
        "SELECT id, nth_value(v, 2) OVER (ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) FROM w ORDER BY id",
    );
    check(
        "win_ntile",
        w,
        "SELECT id, ntile(3) OVER (ORDER BY id) FROM w ORDER BY id",
    );
    check(
        "win_running_sum",
        w,
        "SELECT id, sum(v) OVER (ORDER BY id) FROM w ORDER BY id",
    );
    check(
        "win_partition_sum",
        w,
        "SELECT id, sum(v) OVER (PARTITION BY p) FROM w ORDER BY id",
    );
    check(
        "win_first_last",
        w,
        "SELECT id, first_value(v) OVER (PARTITION BY p ORDER BY id), last_value(v) OVER (PARTITION BY p ORDER BY id) FROM w ORDER BY id",
    );
    check(
        "win_rank_dense",
        w,
        "SELECT id, rank() OVER (ORDER BY p), dense_rank() OVER (ORDER BY p) FROM w ORDER BY id",
    );
    check(
        "win_pct_rank",
        w,
        "SELECT id, round(percent_rank() OVER (ORDER BY v), 4), round(cume_dist() OVER (ORDER BY v), 4) FROM w ORDER BY id",
    );
    check(
        "win_range_frame",
        w,
        "SELECT id, sum(v) OVER (ORDER BY v RANGE BETWEEN 15 PRECEDING AND 15 FOLLOWING) FROM w ORDER BY id",
    );

    // ───────────────────────── NULL-bearing subquery logic ───────────────────
    let s = &[
        "CREATE TABLE s (id INTEGER PRIMARY KEY, v INTEGER)",
        "INSERT INTO s VALUES (1, 10),(2, NULL),(3, 30)",
    ][..];
    check(
        "sub_in_with_null",
        s,
        "SELECT id FROM s WHERE v IN (SELECT v FROM s) ORDER BY id",
    );
    check(
        "sub_not_in_null",
        s,
        "SELECT 5 WHERE 5 NOT IN (SELECT v FROM s)",
    );
    check(
        "sub_exists",
        s,
        "SELECT id FROM s WHERE EXISTS (SELECT 1 FROM s s2 WHERE s2.v > s.v) ORDER BY id",
    );
    check(
        "sub_corr_count",
        s,
        "SELECT id, (SELECT count(*) FROM s s2 WHERE s2.v < s.v) FROM s ORDER BY id",
    );
    check(
        "sub_scalar_null",
        s,
        "SELECT (SELECT v FROM s WHERE id = 2)",
    );
    check(
        "sub_in_empty",
        s,
        "SELECT 1 WHERE 1 IN (SELECT v FROM s WHERE id = 99)",
    );
}
