//! Wider oracle DIVERGENCE PROBE (not a keeper): exercise aggregate, window, and
//! table-valued / JSON functions against a fixture table in both frank and
//! rusqlite (= C SQLite), printing every query whose result set (or error
//! behavior) diverges. Complements scalar_func_divergence_probe.rs, which only
//! covers single-expression scalars. Hunts for fresh, unfiled correctness
//! divergences in the higher-complexity function surface that scalar probing
//! cannot reach — none of which are blocked on the held connection.rs.
//!
//! Result sets are compared as SORTED multisets of rendered rows, so a query
//! without a deterministic output order never produces a false positive; a wrong
//! per-row value (e.g. a bad window computation) still diverges because the
//! sorted multiset changes.
//!
//! `#[ignore]` by default; run with:
//!   cargo test -p fsqlite-e2e --test aggregate_window_tvf_divergence_probe -- --ignored --nocapture
#![recursion_limit = "512"]

use fsqlite::Connection;
use fsqlite_types::SqliteValue;

fn render_frank(v: &SqliteValue) -> String {
    match v {
        SqliteValue::Null => "NULL".to_owned(),
        SqliteValue::Integer(n) => format!("i:{n}"),
        SqliteValue::Float(f) => format!("f:{f}"),
        SqliteValue::Text(s) => format!("t:{s}"),
        SqliteValue::Blob(b) => format!("b:{}", b.len()),
    }
}

/// Run a query on frank; return sorted rendered rows, or an error string.
async fn frank_rows(conn: &Connection, sql: &str) -> Result<Vec<String>, String> {
    match conn.query(sql).await {
        Ok(rs) => {
            let mut rows: Vec<String> = rs
                .iter()
                .map(|row| {
                    row.values()
                        .iter()
                        .map(render_frank)
                        .collect::<Vec<_>>()
                        .join(",")
                })
                .collect();
            rows.sort();
            Ok(rows)
        }
        Err(e) => Err(e.to_string()),
    }
}

/// Run a query on rusqlite (= C SQLite); return sorted rendered rows or an error.
fn sqlite_rows(conn: &rusqlite::Connection, sql: &str) -> Result<Vec<String>, String> {
    let mut stmt = match conn.prepare(sql) {
        Ok(s) => s,
        Err(e) => return Err(format!("prep: {e}")),
    };
    let n = stmt.column_count();
    let out = stmt.query_map([], |row| {
        let mut cells = Vec::with_capacity(n);
        for i in 0..n {
            let cell = match row.get_ref(i)? {
                rusqlite::types::ValueRef::Null => "NULL".to_owned(),
                rusqlite::types::ValueRef::Integer(x) => format!("i:{x}"),
                rusqlite::types::ValueRef::Real(f) => format!("f:{f}"),
                rusqlite::types::ValueRef::Text(t) => {
                    format!("t:{}", String::from_utf8_lossy(t))
                }
                rusqlite::types::ValueRef::Blob(b) => format!("b:{}", b.len()),
            };
            cells.push(cell);
        }
        Ok(cells.join(","))
    });
    match out {
        Ok(iter) => {
            let collected: Result<Vec<String>, _> = iter.collect();
            match collected {
                Ok(mut rows) => {
                    rows.sort();
                    Ok(rows)
                }
                Err(e) => Err(format!("run: {e}")),
            }
        }
        Err(e) => Err(format!("map: {e}")),
    }
}

const SETUP: &[&str] = &[
    "CREATE TABLE t(id INTEGER PRIMARY KEY, grp TEXT, x INTEGER, y REAL)",
    "INSERT INTO t(grp, x, y) VALUES \
     ('a', 10, 1.5), ('a', 20, 2.5), ('b', 30, 3.5), \
     ('b', 40, 4.5), ('b', 50, 5.5), ('c', 60, 6.5), ('c', 5, 0.5)",
];

const QUERIES: &[&str] = &[
    // ---- plain aggregates ----
    "SELECT count(*), sum(x), avg(x), min(x), max(x), total(x) FROM t",
    "SELECT count(DISTINCT grp) FROM t",
    "SELECT sum(x) FROM t WHERE x > 1000",
    "SELECT total(x) FROM t WHERE x > 1000",
    "SELECT avg(x) FROM t WHERE x > 1000",
    "SELECT group_concat(x) FROM t",
    "SELECT group_concat(x, '|') FROM t",
    "SELECT group_concat(DISTINCT grp) FROM t",
    "SELECT sum(x) FROM t",
    "SELECT max(y), min(y) FROM t",
    "SELECT count(y), count(*) FROM t",
    // ---- GROUP BY / HAVING ----
    "SELECT grp, count(*), sum(x) FROM t GROUP BY grp ORDER BY grp",
    "SELECT grp, avg(x) FROM t GROUP BY grp HAVING count(*) > 1 ORDER BY grp",
    "SELECT x % 2 AS parity, count(*) FROM t GROUP BY parity ORDER BY parity",
    "SELECT grp, max(x) - min(x) AS spread FROM t GROUP BY grp ORDER BY grp",
    "SELECT grp, group_concat(x ORDER BY x DESC) FROM t GROUP BY grp ORDER BY grp",
    "SELECT count(*), sum(x) FROM t GROUP BY grp ORDER BY sum(x) DESC",
    // ---- ordered-set / newer aggregates ----
    "SELECT string_agg(grp, '-') FROM t",
    "SELECT group_concat(x ORDER BY x) FROM t",
    // ---- window functions ----
    "SELECT x, row_number() OVER (ORDER BY x) FROM t",
    "SELECT x, rank() OVER (ORDER BY x), dense_rank() OVER (ORDER BY x) FROM t",
    "SELECT x, sum(x) OVER (ORDER BY x ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) FROM t",
    "SELECT x, lag(x) OVER (ORDER BY x), lead(x) OVER (ORDER BY x) FROM t",
    "SELECT x, lag(x, 1, -1) OVER (ORDER BY x) FROM t",
    "SELECT x, first_value(x) OVER w, last_value(x) OVER w FROM t \
     WINDOW w AS (ORDER BY x ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)",
    "SELECT x, ntile(3) OVER (ORDER BY x) FROM t",
    "SELECT x, percent_rank() OVER (ORDER BY x) FROM t",
    "SELECT x, cume_dist() OVER (ORDER BY x) FROM t",
    "SELECT grp, x, sum(x) OVER (PARTITION BY grp ORDER BY x) FROM t",
    "SELECT x, nth_value(x, 2) OVER (ORDER BY x) FROM t",
    "SELECT x, avg(x) OVER (ORDER BY x ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) FROM t",
    "SELECT x, count(*) OVER () FROM t",
    // ---- table-valued: json_each / json_tree ----
    "SELECT value FROM json_each('[10,20,30]')",
    "SELECT key, value FROM json_each('{\"a\":1,\"b\":2}')",
    "SELECT value FROM json_each('[1,2,3,4]') WHERE value > 2",
    "SELECT count(*), sum(value) FROM json_each('[1,2,3,4,5]')",
    "SELECT type, atom FROM json_tree('{\"a\":[1,2],\"b\":3}')",
    "SELECT fullkey FROM json_tree('[10,[20,30]]')",
    "SELECT value FROM json_each('[1,2,3]') ORDER BY value DESC",
    // ---- json aggregates ----
    "SELECT json_group_array(x) FROM t WHERE grp='a'",
    "SELECT json_group_object(grp, x) FROM (SELECT grp, x FROM t WHERE id <= 2)",
    // ---- generate_series (may be unsupported) ----
    "SELECT count(*) FROM generate_series(1,5)",
    "SELECT value FROM generate_series(2,10,2)",
    // ---- subquery / correlated aggregates ----
    "SELECT grp, (SELECT count(*) FROM t t2 WHERE t2.grp = t.grp) FROM t GROUP BY grp ORDER BY grp",
    "SELECT x FROM t WHERE x > (SELECT avg(x) FROM t) ORDER BY x",
    // ---- type affinity in comparisons (column has affinity, literal coerces) ----
    "SELECT id FROM t WHERE x = '10' ORDER BY id",
    "SELECT id FROM t WHERE grp = 30 ORDER BY id",
    "SELECT id FROM t WHERE y = '1.5' ORDER BY id",
    "SELECT count(*) FROM t WHERE x BETWEEN '10' AND '30'",
    "SELECT count(*) FROM t WHERE grp IN (30, 40)",
    "SELECT count(*) FROM t WHERE x IN ('10', '20')",
    "SELECT id FROM t WHERE x > '9' ORDER BY id",
    "SELECT id FROM t WHERE CAST(x AS TEXT) < '30' ORDER BY id",
    // ---- literal comparison / coercion semantics ----
    "SELECT '10' < '9'",
    "SELECT 10 < 9",
    "SELECT '10' < 9",
    "SELECT 10 = '10'",
    "SELECT '10' = '10.0'",
    "SELECT 1 = 1.0",
    "SELECT '1' = 1",
    "SELECT NULL = NULL",
    "SELECT NULL IS NULL",
    "SELECT 'a' BETWEEN 'A' AND 'z'",
    "SELECT 5 BETWEEN '1' AND '9'",
    "SELECT x'01' < x'0100'",
    "SELECT 2 IN (1,2,3)",
    "SELECT '2' IN (1,2,3)",
    "SELECT 2 IN ('1','2','3')",
    // ---- collation / LIKE / GLOB / RTRIM ----
    "SELECT 'abc' = 'ABC' COLLATE NOCASE",
    "SELECT 'abc' < 'ABC'",
    "SELECT 'a' = 'a '",
    "SELECT 'a' = 'a ' COLLATE RTRIM",
    "SELECT count(*) FROM t WHERE grp LIKE 'A%'",
    "SELECT count(*) FROM t WHERE grp GLOB 'a*'",
    "SELECT 'abc' LIKE 'a_c'",
    "SELECT 'a%c' LIKE 'a\\%c' ESCAPE '\\'",
    "SELECT 'ABC' GLOB '[A-C][A-C][A-C]'",
    "SELECT 'aXbXc' LIKE 'a%c'",
    "SELECT lower('İ')",
    "SELECT upper('ß')",
    "SELECT 'foo' || 'bar' = 'foobar'",
    // ---- CAST edge cases ----
    "SELECT CAST(3.99 AS INTEGER)",
    "SELECT CAST(-3.99 AS INTEGER)",
    "SELECT CAST('12abc' AS INTEGER)",
    "SELECT CAST('abc' AS INTEGER)",
    "SELECT CAST('  3.5 ' AS REAL)",
    "SELECT CAST(9223372036854775808 AS INTEGER)",
    "SELECT CAST('0x1A' AS INTEGER)",
    "SELECT CAST(x'41424300' AS TEXT)",
    "SELECT CAST('1e3' AS INTEGER)",
    "SELECT CAST(1.9999999999999999 AS INTEGER)",
    "SELECT typeof(1), typeof(1.0), typeof('1'), typeof(x'01'), typeof(NULL)",
    "SELECT typeof(1 + 1.0), typeof(5 / 2), typeof(5.0 / 2), typeof('5' + 0)",
];

#[test]
#[ignore = "wider divergence probe (not a keeper): aggregate/window/TVF frank-vs-sqlite3 mismatches"]
fn aggregate_window_tvf_divergence_probe() {
    asupersync::test_utils::run_test(|| async {
        let f = Connection::open(":memory:").await.expect("open frank");
        let r = rusqlite::Connection::open_in_memory().expect("open rusqlite");
        for s in SETUP {
            f.execute(s)
                .await
                .unwrap_or_else(|e| panic!("frank setup `{s}`: {e}"));
            r.execute_batch(s)
                .unwrap_or_else(|e| panic!("csql setup `{s}`: {e}"));
        }
        let mut diverged = 0usize;
        let mut both_err = 0usize;
        for q in QUERIES {
            let fr = frank_rows(&f, q).await;
            let sr = sqlite_rows(&r, q);
            match (&fr, &sr) {
                (Ok(a), Ok(b)) => {
                    if a != b {
                        diverged += 1;
                        println!("DIVERGE  {q}\n    frank: {a:?}\n    csql:  {b:?}");
                    }
                }
                (Err(_), Err(_)) => both_err += 1,
                (Ok(a), Err(b)) => {
                    diverged += 1;
                    println!("F-OK/C-ERR  {q}\n    frank: {a:?}\n    csql:  <err: {b}>");
                }
                (Err(a), Ok(b)) => {
                    diverged += 1;
                    println!("F-ERR/C-OK  {q}\n    frank: <err: {a}>\n    csql:  {b:?}");
                }
            }
        }
        println!(
            "\nPROBE SUMMARY: {} queries, {} diverged, {} both-error",
            QUERIES.len(),
            diverged,
            both_err
        );
    });
}
