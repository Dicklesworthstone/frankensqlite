#![recursion_limit = "512"]

//! bd-w4pr1.13.9 (lever TB): an ephemeral index's in-memory shortcuts are
//! bounded by the temp budget instead of being refused under it.
//!
//! A join's automatic index stages its records and is built from them in one
//! sorted pass (bd-w4pr1.13.15), and an IN / DISTINCT index keeps its keys in
//! a hash set. Since d081a0f87 both were refused whenever the FILE temp budget
//! was under 100 MiB, which is every default connection (`cache_size` -2000
//! gives 2,048,000 bytes), so both went to the spillable b-tree key by key.
//! Now both are admitted, hold at most the budget, and past it move what they
//! hold into that b-tree, which spills to a temp file as stock's does.
//!
//! Every answer is compared with stock SQLite (rusqlite) on the same data, in
//! row order (none of these queries has ORDER BY: stock emits the outer scan's
//! order, and for each outer row the automatic index's (key, rowid) order),
//! in memory and on a file. On the file, the temp-store counters are the
//! evidence of which path ran: a staged build, a set, a budget spill, and the
//! most bytes one arena or set counted against the budget. In memory a join
//! may run the connection's in-memory hash join instead
//! (`prefer_memdb_hash_join_dispatch`), so there only the answers are held to.

use fsqlite_core::connection::Connection;
use fsqlite_types::value::SqliteValue;
use fsqlite_vdbe::temp_store::TempStoreStatsSnapshot;

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

async fn rows_f(conn: &Connection, sql: &str) -> Vec<Vec<String>> {
    conn.query(sql)
        .await
        .unwrap_or_else(|e| panic!("franken `{sql}`: {e:?}"))
        .iter()
        .map(|row| row.values().iter().map(tag_f).collect())
        .collect()
}

fn rows_r(conn: &rusqlite::Connection, sql: &str) -> Vec<Vec<String>> {
    let mut stmt = conn.prepare(sql).expect("rusqlite prepare");
    let ncol = stmt.column_count();
    stmt.query_map([], |row| {
        Ok((0..ncol)
            .map(|i| tag_r(&row.get::<_, rusqlite::types::Value>(i).unwrap()))
            .collect())
    })
    .expect("rusqlite query")
    .map(|r| r.unwrap())
    .collect()
}

/// `n` rows `1..=n` of a recursive CTE, for both engines alike.
fn series(n: u32) -> String {
    format!("WITH RECURSIVE c(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM c WHERE n < {n})")
}

/// The scorecard's `join_no_index_5k_x_5k` tables, and a table of keys that
/// mix NULL, INTEGER, REAL equal and unequal to an INTEGER, TEXT in two cases
/// and BLOB, with a NOCASE column and a REAL column.
fn base_setup() -> Vec<String> {
    vec![
        "CREATE TABLE v1(k INTEGER, x INTEGER)".to_owned(),
        "CREATE TABLE v2(k INTEGER, y INTEGER)".to_owned(),
        format!("{} INSERT INTO v1 SELECT (n * 31) % 5000, n FROM c", series(5000)),
        format!("{} INSERT INTO v2 SELECT (n * 17) % 5000, n FROM c", series(5000)),
        "CREATE TABLE m(k, t TEXT COLLATE NOCASE, r REAL)".to_owned(),
        format!(
            "{} INSERT INTO m SELECT CASE n % 7 WHEN 0 THEN NULL WHEN 1 THEN n % 50 \
             WHEN 2 THEN (n % 50) * 1.0 WHEN 3 THEN (n % 50) + 0.5 WHEN 4 THEN 'K' || (n % 40) \
             WHEN 5 THEN 'k' || (n % 40) ELSE zeroblob(n % 5) END, \
             CASE n % 3 WHEN 0 THEN 'Name' || (n % 30) WHEN 1 THEN 'NAME' || (n % 30) \
             ELSE NULL END, (n % 100) * 1.0 FROM c",
            series(3000)
        ),
    ]
}

/// Two 200,000-row tables whose join builds a 200,000-entry automatic index.
fn large_setup() -> Vec<String> {
    vec![
        "CREATE TABLE w1(k INTEGER, x INTEGER)".to_owned(),
        "CREATE TABLE w2(k INTEGER, y INTEGER)".to_owned(),
        format!("{} INSERT INTO w1 SELECT (n * 7919) % 200000, n FROM c", series(200_000)),
        format!("{} INSERT INTO w2 SELECT (n * 104729) % 200000, n FROM c", series(200_000)),
    ]
}

/// The scorecard's `join_no_index_5k_x_5k`: its join program builds a staged
/// automatic index (`OPFLAG_STAGED_BUILD`), which the counters record.
const JNI: &str = "SELECT count(*) FROM v1 JOIN v2 ON v1.k = v2.k";

/// A count over a 200,000-entry automatic index, the same program shape.
const W_COUNT: &str = "SELECT count(*) FROM w2 JOIN w1 ON w1.k = w2.k";

/// More unindexed equi-joins, held to stock's answers. Their counters are not
/// asserted: a filtered row join may run without the automatic index although
/// EXPLAIN shows one (2026-11-27 gate: `... WHERE v1.x % 97 = 0` recorded no
/// staged build). Row order is compared only where the inner keys are unique or
/// the rows are aggregated: stock's covering automatic index orders a key's
/// duplicates by the columns it covers before the rowid.
const JOINS: &[&str] = &[
    "SELECT v1.x, v2.y FROM v1 JOIN v2 ON v1.k = v2.k WHERE v1.x % 97 = 0",
    "SELECT v2.y, v1.x FROM v2 JOIN v1 ON v2.k = v1.k WHERE v2.y % 89 = 0",
    "SELECT count(*), sum(v1.x) FROM m JOIN v1 ON v1.k = m.k",
    "SELECT m.r, v1.x FROM m JOIN v1 ON v1.k = m.k WHERE m.r < 3",
    "SELECT count(*), sum(a.r) FROM m a JOIN m b ON b.t = a.t",
];

/// IN and DISTINCT shapes that open a membership set (`OPFLAG_MEMBERSHIP_SET`).
const SETS: &[&str] = &[
    "SELECT DISTINCT x FROM v1",
    "SELECT count(*), sum(x) FROM v1 WHERE x IN (SELECT y FROM v2)",
    "SELECT count(*) FROM v1 WHERE k NOT IN (SELECT y FROM v2 WHERE y % 3 = 0)",
    "SELECT DISTINCT k FROM m",
    "SELECT DISTINCT t FROM m",
    "SELECT DISTINCT r FROM m",
    "SELECT count(*) FROM m WHERE k IN (SELECT k FROM v1)",
    "SELECT count(*), sum(x) FROM v1 WHERE k IN (SELECT r FROM m)",
];

const DEFAULT_BUDGET: usize = 2_048_000;
const TIGHT_BUDGET: usize = 64 * 1024;

fn bytes(n: usize) -> u64 {
    u64::try_from(n).unwrap()
}

struct Arms {
    _dir: tempfile::TempDir,
    franken: Connection,
    stock: rusqlite::Connection,
}

async fn open_arms(file_backed: bool, tight: bool, setup: &[String]) -> Arms {
    let dir = tempfile::tempdir().unwrap();
    let target = if file_backed {
        dir.path()
            .join("bounded_autoindex.db")
            .to_string_lossy()
            .into_owned()
    } else {
        ":memory:".to_owned()
    };
    let franken = Connection::open(&target).await.unwrap();
    let stock = rusqlite::Connection::open_in_memory().unwrap();
    for sql in setup {
        franken.execute(sql).await.unwrap();
        stock.execute(sql, []).unwrap();
    }
    if tight {
        franken.execute("PRAGMA cache_size = -64").await.unwrap();
        stock.execute_batch("PRAGMA cache_size = -64").unwrap();
    }
    let budget = franken.temp_store_stats().budget_bytes;
    assert_eq!(
        budget,
        if tight { TIGHT_BUDGET } else { DEFAULT_BUDGET },
        "the temp budget comes from cache_size"
    );
    Arms {
        _dir: dir,
        franken,
        stock,
    }
}

/// Run `sql` on both arms, require stock's rows in stock's order, and return
/// the temp-store counters before and after the franken run.
async fn same_rows(arms: &Arms, sql: &str) -> (TempStoreStatsSnapshot, TempStoreStatsSnapshot) {
    let expected = rows_r(&arms.stock, sql);
    assert!(!expected.is_empty(), "fixture must give `{sql}` rows");
    let before = arms.franken.temp_store_stats();
    let actual = rows_f(&arms.franken, sql).await;
    let after = arms.franken.temp_store_stats();
    assert_eq!(actual, expected, "`{sql}` (stock's rows, in stock's order)");
    (before, after)
}

/// Keeper 1. Under the default budget a 5k x 5k unindexed equi-join builds
/// its automatic index from a staged arena, in one statement, with no budget
/// spill and no page spill; the arena counted something, and at most the
/// budget. The refusal this replaces (d081a0f87) records no staged build.
#[test]
fn default_budget_join_autoindex_is_built_staged_without_spilling() {
    for file_backed in [false, true] {
        asupersync::test_utils::run_test(|| async move {
            let arms = open_arms(file_backed, false, &base_setup()).await;
            let (before, after) = same_rows(&arms, JNI).await;
            if file_backed {
                assert!(
                    after.staged_index_builds > before.staged_index_builds,
                    "`{JNI}` must build its automatic index staged: {before:?} -> {after:?}"
                );
                assert_eq!(
                    after.staged_index_budget_spills, before.staged_index_budget_spills,
                    "`{JNI}` fits the default budget"
                );
                assert_eq!(
                    after.temp_index_spill_events, before.temp_index_spill_events,
                    "`{JNI}` must not spill index pages"
                );
            }
            for sql in JOINS {
                same_rows(&arms, sql).await;
            }
            if file_backed {
                let stats = arms.franken.temp_store_stats();
                assert!(
                    stats.in_memory_index_bytes_peak > 0
                        && stats.in_memory_index_bytes_peak <= bytes(DEFAULT_BUDGET),
                    "{stats:?}"
                );
            }
        });
    }
}

/// Keeper 2, the negative case. Under a 64 KiB budget the same joins, and a
/// join over a 200,000-entry automatic index, build from what the arena
/// holds when the next record would pass the budget and insert the rest into
/// the spillable b-tree. Rows and their order equal stock's; the large index
/// spills pages; no arena counted past the budget. An arena with no bound
/// records no budget spill and counts far past 64 KiB; a spill that laid the
/// held records out unsorted loses rows.
#[test]
fn tight_budget_join_autoindex_spills_within_the_budget_like_stock() {
    for file_backed in [false, true] {
        asupersync::test_utils::run_test(|| async move {
            let mut setup = base_setup();
            setup.extend(large_setup());
            let arms = open_arms(file_backed, true, &setup).await;
            let (before, after) = same_rows(&arms, JNI).await;
            if file_backed {
                assert!(
                    after.staged_index_builds > before.staged_index_builds
                        && after.staged_index_budget_spills > before.staged_index_budget_spills,
                    "`{JNI}` must stage and outgrow a 64 KiB arena: {before:?} -> {after:?}"
                );
            }
            let (before, after) = same_rows(&arms, W_COUNT).await;
            if file_backed {
                assert!(
                    after.staged_index_budget_spills > before.staged_index_budget_spills,
                    "`{W_COUNT}` must outgrow a 64 KiB arena: {before:?} -> {after:?}"
                );
                assert!(
                    after.temp_index_spill_events > before.temp_index_spill_events,
                    "`{W_COUNT}`: a 200,000-entry index must spill pages past 64 KiB: \
                     {before:?} -> {after:?}"
                );
            }
            for sql in JOINS.iter().copied().chain([
                "SELECT count(*), sum(w1.x), sum(w2.y) FROM w2 JOIN w1 ON w1.k = w2.k",
                "SELECT w2.y, w1.x FROM w2 JOIN w1 ON w1.k = w2.k WHERE w2.y % 5003 = 0",
            ]) {
                same_rows(&arms, sql).await;
            }
            let stats = arms.franken.temp_store_stats();
            if file_backed {
                assert!(
                    stats.in_memory_index_bytes_peak > 0
                        && stats.in_memory_index_bytes_peak <= bytes(TIGHT_BUDGET),
                    "no arena may count past the budget: {stats:?}"
                );
            }
            assert_eq!(stats.active_temp_files, 0, "file {file_backed}: {stats:?}");
        });
    }
}

/// Keeper 3. IN (subquery), NOT IN and DISTINCT open a membership set under
/// the default budget and never spill it for the budget there; under 64 KiB
/// the 5,000-key sets spill into the b-tree. Keys mix NULL, INTEGER, REAL
/// equal to an INTEGER (1 = 1.0), REAL, TEXT under BINARY and NOCASE (which
/// has no canonical bytes and leaves set mode at once) and BLOB. Rows and
/// their order equal stock's under both budgets.
#[test]
fn in_and_distinct_sets_are_admitted_and_spill_within_the_budget_like_stock() {
    for file_backed in [false, true] {
        for tight in [false, true] {
            asupersync::test_utils::run_test(|| async move {
                let arms = open_arms(file_backed, tight, &base_setup()).await;
                let start = arms.franken.temp_store_stats();
                for sql in SETS {
                    let (before, after) = same_rows(&arms, sql).await;
                    if file_backed {
                        assert!(
                            after.membership_sets_opened > before.membership_sets_opened,
                            "tight {tight} `{sql}` must open a set: {before:?} -> {after:?}"
                        );
                    }
                }
                if !file_backed {
                    return;
                }
                let end = arms.franken.temp_store_stats();
                let budget = if tight { TIGHT_BUDGET } else { DEFAULT_BUDGET };
                if tight {
                    assert!(
                        end.membership_set_budget_spills >= start.membership_set_budget_spills + 2,
                        "file {file_backed}: the 5,000-key sets must spill past 64 KiB: \
                         {start:?} -> {end:?}"
                    );
                } else {
                    assert_eq!(
                        end.membership_set_budget_spills, start.membership_set_budget_spills,
                        "file {file_backed}: every set fits the default budget"
                    );
                }
                assert!(
                    end.in_memory_index_bytes_peak > 0
                        && end.in_memory_index_bytes_peak <= bytes(budget),
                    "file {file_backed} tight {tight}: {end:?}"
                );
            });
        }
    }
}
