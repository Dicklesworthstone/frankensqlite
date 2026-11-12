#![recursion_limit = "512"]

//! bd-w4pr1.13.1: the interpreter runs `Next` and `Column` without awaiting
//! when the row is at hand, and falls back to the async path otherwise. This
//! keeper drives both sides of every boundary those lanes have and compares
//! each result with stock SQLite (rusqlite), file-backed and `:memory:`:
//!
//! - `Next` inside a leaf page and across leaf and interior boundaries, on
//!   multi-level trees, including scans that delete or update as they go;
//! - `Column` on payloads local to the leaf, local but longer than the
//!   layout prefix, and spilled to overflow pages, read in every order;
//! - the INTEGER PRIMARY KEY alias first, in the middle and absent;
//! - rows shorter than the table (ALTER TABLE ADD COLUMN defaults);
//! - index cursors, WITHOUT ROWID tables and records wider than 64 columns;
//! - rows that change between local and overflow storage.
//!
//! bd-w4pr1.13.4 added a `Column` lane that decodes a value from the leaf
//! page when it needs no heap, and leaves the rest to the copying lane. Its
//! boundaries are covered here as well:
//!
//! - text on both sides of the inline limit, with every column read twice
//!   per row, beside NULL, 0, 1, each integer width, reals and blobs;
//! - a file written by stock SQLite, whose records store a NULL in the
//!   rowid-alias slot, read and then mutated;
//! - rows one column short of the table that hold their own rowid in the
//!   alias slot (the shape old FrankenSQLite files have).
//!
//! bd-w4pr1.13 added a top-N preflight that compares a candidate's packed sort
//! key with the heap's worst row without decoding it; `ORDER BY ... LIMIT`
//! over every key class, ties, NULLS FIRST/LAST, NOCASE and BLOB keys (which
//! take the decoding path) are compared here too.
//!
//! A lane that skipped or repeated a cell at a page boundary, read a stale
//! row image, or mis-mapped a column past the rowid alias fails here.

use std::fmt::Write as _;

use fsqlite_core::connection::Connection;
use fsqlite_types::value::SqliteValue;

fn tag_f(v: &SqliteValue) -> String {
    match v {
        SqliteValue::Null => "NULL".to_owned(),
        SqliteValue::Integer(n) => n.to_string(),
        SqliteValue::Float(f) => format!("{f:?}"),
        SqliteValue::Text(s) => format!("'{s}'"),
        SqliteValue::Blob(b) => format!(
            "X'{}'",
            b.iter().map(|x| format!("{x:02X}")).collect::<String>()
        ),
    }
}

fn tag_r(v: &rusqlite::types::Value) -> String {
    match v {
        rusqlite::types::Value::Null => "NULL".to_owned(),
        rusqlite::types::Value::Integer(n) => n.to_string(),
        rusqlite::types::Value::Real(f) => format!("{f:?}"),
        rusqlite::types::Value::Text(s) => format!("'{s}'"),
        rusqlite::types::Value::Blob(b) => format!(
            "X'{}'",
            b.iter().map(|x| format!("{x:02X}")).collect::<String>()
        ),
    }
}

async fn frank_rows(f: &Connection, sql: &str) -> Vec<Vec<String>> {
    match f.query(sql).await {
        Ok(rows) => rows
            .iter()
            .map(|r| r.values().iter().map(tag_f).collect())
            .collect(),
        Err(e) => vec![vec![format!("<ERR {e}>")]],
    }
}

fn stock_rows(r: &rusqlite::Connection, sql: &str) -> Vec<Vec<String>> {
    let mut st = match r.prepare(sql) {
        Ok(st) => st,
        Err(e) => return vec![vec![format!("<ERR {e}>")]],
    };
    let n = st.column_count();
    st.query_map([], |row| {
        Ok((0..n)
            .map(|i| tag_r(&row.get_unwrap::<_, rusqlite::types::Value>(i)))
            .collect())
    })
    .unwrap()
    .map(Result::unwrap)
    .collect()
}

const ROWS: i64 = 3000;
/// Longer than the 256-byte layout prefix, still local to a 4096-byte page.
const LONG_LOCAL: usize = 400;
/// Spills to overflow pages.
const OVERFLOW: usize = 6000;

/// TEXT for row `i`: short for most rows, long-but-local for every 10th, and
/// overflowing for every 97th.
fn text_for(i: i64) -> String {
    if i % 97 == 0 {
        format!("ov-{i}-{}", "y".repeat(OVERFLOW))
    } else if i % 10 == 0 {
        format!("ll-{i}-{}", "x".repeat(LONG_LOCAL))
    } else {
        format!("row-{i}")
    }
}

/// A literal for column `d`: NULL, a small blob, or an integer of each width.
fn mixed_for(i: i64) -> String {
    match i % 8 {
        0 => "NULL".to_owned(),
        1 => format!("X'{:04X}'", i & 0xFFFF),
        2 => "0".to_owned(),
        3 => "1".to_owned(),
        4 => (i % 100 - 50).to_string(),
        5 => (i * 40_000).to_string(),
        6 => (i * 5_000_000_000).to_string(),
        _ => "''".to_owned(),
    }
}

/// `INSERT` statements for `rows`, 100 rows per statement.
fn inserts(table: &str, rows: &[String]) -> String {
    let mut sql = String::new();
    for chunk in rows.chunks(100) {
        write!(sql, "INSERT INTO {table} VALUES {};", chunk.join(",")).unwrap();
    }
    sql
}

fn setup_sql() -> String {
    let mut sql = String::from(
        "CREATE TABLE t1(id INTEGER PRIMARY KEY, a INTEGER, b TEXT, c REAL, d);\
         CREATE TABLE t2(a INTEGER, id INTEGER PRIMARY KEY, b TEXT, c);\
         CREATE TABLE t3(a, b, c);\
         CREATE TABLE t4(k1 INTEGER, k2 TEXT, v TEXT, w INTEGER, PRIMARY KEY(k2, k1)) WITHOUT ROWID;",
    );
    let wide_cols: Vec<String> = (0..70).map(|c| format!("c{c}")).collect();
    write!(sql, "CREATE TABLE t5({});", wide_cols.join(", ")).unwrap();

    let t1: Vec<String> = (1..=ROWS)
        .map(|i| {
            // c = 1.5 * i, written out so no float formatting is involved.
            format!(
                "({i}, {}, '{}', {}.{}, {})",
                (i * 7919) % 1000,
                text_for(i),
                i * 3 / 2,
                if i % 2 == 0 { 0 } else { 5 },
                mixed_for(i)
            )
        })
        .collect();
    let t2: Vec<String> = (1..=ROWS / 2)
        .map(|i| format!("({}, {}, '{}', {})", i % 13, i * 2, text_for(i), mixed_for(i)))
        .collect();
    let t3: Vec<String> = (1..=ROWS / 2)
        .map(|i| format!("({}, 'k-{}', '{}')", (i * 31) % ROWS + 1, i % 40, text_for(i * 3)))
        .collect();
    let t4: Vec<String> = (1..=ROWS / 3)
        .map(|i| format!("({i}, 'k-{}', '{}', {})", i % 25, text_for(i * 7), i * 3))
        .collect();
    let t5: Vec<String> = (1..=60_i64)
        .map(|i| {
            let cells: Vec<String> = (0..70_i64)
                .map(|c| match c {
                    0 => i.to_string(),
                    64 => format!("'{}'", text_for(i * 10)),
                    _ if (i + c) % 9 == 0 => "NULL".to_owned(),
                    _ if c % 3 == 0 => format!("'t{i}-{c}'"),
                    _ => (i * 100 + c).to_string(),
                })
                .collect();
            format!("({})", cells.join(", "))
        })
        .collect();

    sql.push_str("BEGIN;");
    sql.push_str(&inserts("t1", &t1));
    sql.push_str(&inserts("t2", &t2));
    sql.push_str(&inserts("t3", &t3));
    sql.push_str(&inserts("t4", &t4));
    sql.push_str(&inserts("t5", &t5));
    sql.push_str("COMMIT;");
    // Rows written before these ALTERs stay two columns short of the table.
    sql.push_str(
        "ALTER TABLE t2 ADD COLUMN e DEFAULT 'dflt';\
         ALTER TABLE t2 ADD COLUMN f INTEGER DEFAULT 7;\
         INSERT INTO t2 VALUES (3, 3001, 'new-1', 1.5, 'e1', 11), (4, 3003, 'new-2', NULL, NULL, 12);\
         CREATE INDEX t1_a ON t1(a);\
         CREATE INDEX t3_b ON t3(b);",
    );
    sql
}

const T1_QUERIES: [&str; 9] = [
    "SELECT count(*), sum(a), sum(length(b)), sum(c), count(d) FROM t1",
    "SELECT id, a, length(b), substr(b, 1, 8), c, typeof(d), hex(d) FROM t1 WHERE a > 900 ORDER BY id",
    "SELECT c, b, a, id FROM t1 WHERE id IN (1, 5, 10, 97, 194, 970, 2910, 3000) ORDER BY id",
    "SELECT id, length(b), d FROM t1 ORDER BY id LIMIT 400 OFFSET 2000",
    "SELECT a, count(*), min(id), max(id), sum(length(b)) FROM t1 GROUP BY a ORDER BY a",
    "SELECT id FROM t1 WHERE b LIKE 'row-12%' ORDER BY id",
    "SELECT rowid, id, a, d FROM t1 WHERE id BETWEEN 90 AND 100 ORDER BY rowid DESC",
    "SELECT d, c, id FROM t1 WHERE a = 77 ORDER BY id",
    "SELECT count(*), sum(t1.a), sum(length(t1.b)) FROM t3 JOIN t1 ON t1.id = t3.a",
];

const OTHER_QUERIES: [&str; 12] = [
    "SELECT a, id, b, c, e, f FROM t2 WHERE id % 50 = 0 OR id > 2990 ORDER BY id",
    "SELECT f, e, c, length(b), id, a FROM t2 ORDER BY id DESC LIMIT 60",
    "SELECT count(*), sum(id), sum(f), count(e), sum(length(b)), sum(a) FROM t2",
    "SELECT id, typeof(c), e FROM t2 WHERE a = 5 ORDER BY id",
    "SELECT rowid, a, b, length(c) FROM t3 WHERE a % 7 = 0 ORDER BY rowid",
    "SELECT b, count(*) FROM t3 WHERE b >= 'k-2' AND b < 'k-3' GROUP BY b ORDER BY b",
    "SELECT count(*), sum(a), sum(length(c)) FROM t3",
    "SELECT k1, k2, length(v), w FROM t4 ORDER BY k2, k1",
    "SELECT w, substr(v, 1, 10) FROM t4 WHERE k2 = 'k-17' ORDER BY k1",
    "SELECT sum(w), count(v), sum(length(v)), sum(k1) FROM t4",
    "SELECT c0, c1, c33, length(c64), c65, c69, c2, c63 FROM t5 ORDER BY c0",
    "SELECT sum(c69), sum(length(c64)), count(c1), count(c66) FROM t5",
];

/// Scans that write as they go, then rows that move between local and
/// overflow storage. The t1 queries run again after each step.
fn mutations() -> [String; 5] {
    [
        "UPDATE t1 SET a = a + 1, b = b || '!' WHERE id % 10 = 0".to_owned(),
        "DELETE FROM t1 WHERE id % 3 = 0".to_owned(),
        format!("UPDATE t1 SET b = '{}' WHERE id = 5", "z".repeat(OVERFLOW)),
        "UPDATE t1 SET b = 'tiny' WHERE id = 97".to_owned(),
        "DELETE FROM t2 WHERE id % 4 = 0; UPDATE t2 SET f = f + id WHERE a < 3".to_owned(),
    ]
}

async fn compare(
    label: &str,
    frank: &Connection,
    stock: &rusqlite::Connection,
    queries: &[&str],
    failures: &mut Vec<String>,
) {
    for query in queries {
        let (f, s) = (frank_rows(frank, query).await, stock_rows(stock, query));
        if f != s {
            let first = f
                .iter()
                .zip(&s)
                .position(|(a, b)| a != b)
                .unwrap_or_else(|| f.len().min(s.len()));
            let show = |rows: &[Vec<String>]| {
                rows.get(first).map_or_else(
                    || "<no row>".to_owned(),
                    |row| format!("{row:?}").chars().take(300).collect(),
                )
            };
            failures.push(format!(
                "{label}: `{query}`: {} rows vs SQLite {}; first difference at row {first}:\n  frank {}\n  stock {}",
                f.len(),
                s.len(),
                show(&f),
                show(&s)
            ));
        }
    }
}

async fn run_case(file_backed: bool, failures: &mut Vec<String>) {
    let mode = if file_backed { "file" } else { "memory" };
    let dir = tempfile::tempdir().expect("temp dir");
    let path = dir.path().join("frank.db");
    let frank = if file_backed {
        Connection::open(path.to_str().expect("utf-8 path"))
            .await
            .expect("open")
    } else {
        Connection::open(":memory:").await.expect("open")
    };
    let stock = rusqlite::Connection::open_in_memory().expect("stock open");
    let setup = setup_sql();
    frank.execute_batch(&setup).await.expect("frank setup");
    stock.execute_batch(&setup).expect("stock setup");
    let edge_setup = inline_edge_setup();
    frank.execute_batch(&edge_setup).await.expect("frank edge setup");
    stock.execute_batch(&edge_setup).expect("stock edge setup");

    compare(&format!("[{mode}] loaded"), &frank, &stock, &T1_QUERIES, failures).await;
    compare(&format!("[{mode}] loaded"), &frank, &stock, &OTHER_QUERIES, failures).await;
    compare(&format!("[{mode}] loaded"), &frank, &stock, &INLINE_EDGE_QUERIES, failures).await;
    compare(&format!("[{mode}] loaded"), &frank, &stock, &TOP_N_QUERIES, failures).await;
    compare(&format!("[{mode}] loaded"), &frank, &stock, &TYPED_AGG_QUERIES, failures).await;
    for (step, mutation) in mutations().iter().enumerate() {
        frank.execute_batch(mutation).await.expect("frank mutation");
        stock.execute_batch(mutation).expect("stock mutation");
        let label = format!("[{mode}] after mutation {step}");
        compare(&label, &frank, &stock, &T1_QUERIES, failures).await;
        compare(&label, &frank, &stock, &OTHER_QUERIES[..4], failures).await;
    }
    frank.close().await.expect("close");
}

/// Text on both sides of the 23-byte inline limit, each column read twice per
/// row, next to values of every class the in-place lane decodes.
const INLINE_EDGE_QUERIES: [&str; 4] = [
    "SELECT id, s, length(s), s || '|', n, r, n + 0, typeof(z) FROM edge ORDER BY id",
    "SELECT s, s, id, id, n, n FROM edge WHERE length(s) BETWEEN 22 AND 25 ORDER BY id",
    "SELECT count(*), sum(n), sum(r), sum(length(s)), count(z) FROM edge WHERE s >= 'a'",
    "SELECT z, r, n, s, id FROM edge WHERE id % 7 = 3 ORDER BY id DESC",
];

/// `ORDER BY ... LIMIT` over `edge`: the top-N preflight compares each
/// candidate's packed key with the heap's worst row without decoding it when
/// the key is TEXT under BINARY, an integer, a real or NULL, and decodes it
/// otherwise (BLOB keys, NOCASE, TEXT against a number). The first queries
/// are decided by ties (`s` repeats every 41 rows), where only a strict
/// "better than the worst row" admits a candidate.
const TOP_N_QUERIES: [&str; 13] = [
    "SELECT id, s FROM edge ORDER BY s DESC LIMIT 7",
    "SELECT id, s FROM edge ORDER BY s LIMIT 5 OFFSET 30",
    "SELECT count(*), sum(id) FROM (SELECT id FROM edge ORDER BY s DESC LIMIT 10)",
    "SELECT id, n FROM edge ORDER BY n LIMIT 9",
    "SELECT id, n FROM edge ORDER BY n DESC NULLS FIRST LIMIT 5",
    "SELECT id, n FROM edge ORDER BY n NULLS LAST LIMIT 6",
    "SELECT id, r FROM edge ORDER BY r DESC LIMIT 4",
    "SELECT id, z FROM edge ORDER BY z DESC LIMIT 3",
    "SELECT id, s FROM edge ORDER BY s COLLATE NOCASE DESC LIMIT 6",
    "SELECT id FROM edge ORDER BY coalesce(n, r, s) LIMIT 8",
    "SELECT id FROM edge ORDER BY coalesce(z, s, n) DESC LIMIT 8",
    "SELECT id, s, n FROM edge ORDER BY length(s) DESC, n, id DESC LIMIT 10",
    "SELECT id, s FROM edge ORDER BY s, r DESC LIMIT 12",
];

/// `sum`, `total`, `count` and `avg` over `edge`: a plain `AggStep` of the
/// built-in `sum`, `count(*)` or `count(X)` steps its accumulator's typed state
/// itself (bd-w4pr1.13). Integers of every width with NULLs, reals, numeric and
/// non-numeric text, blobs, an all-NULL input (`sum` NULL, `total` 0.0) and a
/// column that mixes classes from row to row, grouped and not.
const TYPED_AGG_QUERIES: [&str; 8] = [
    "SELECT sum(n), total(n), count(n), count(*), avg(n) FROM edge",
    "SELECT sum(r), total(r), sum(n + r), count(r) FROM edge",
    "SELECT sum(s), total(s), sum(z), count(z), count(s) FROM edge",
    "SELECT sum(CASE WHEN id % 3 = 0 THEN NULL END), total(CASE WHEN id < 0 THEN 1 END), \
     count(CASE WHEN id < 0 THEN 1 END) FROM edge",
    "SELECT id % 7, count(*), count(n), sum(n), sum(r), total(n) FROM edge GROUP BY 1 ORDER BY 1",
    "SELECT sum(id), count(*), sum(length(s)) FROM edge WHERE n IS NULL",
    "SELECT sum('12' || id), sum(' 7 '), sum('1e3'), sum(id || 'x') FROM edge",
    "SELECT sum(CASE WHEN id % 2 THEN n ELSE r END), sum(CASE WHEN id % 5 = 0 THEN z ELSE id END) \
     FROM edge",
];

fn inline_edge_setup() -> String {
    let rows: Vec<String> = (1..=600_i64)
        .map(|i| {
            // Lengths 0..=40 cycle through the inline limit; `n` cycles
            // through NULL, 0, 1 and every integer width, both signs. The
            // 8-byte values stay small enough that `sum(n)` does not overflow.
            let len = usize::try_from(i % 41).unwrap();
            let n = match i % 9 {
                0 => "NULL".to_owned(),
                1 => "0".to_owned(),
                2 => "1".to_owned(),
                3 => "-1".to_owned(),
                4 => (i * 100).to_string(),
                5 => (-i * 40_000).to_string(),
                6 => (i * 3_000_000).to_string(),
                7 => (-i * 5_000_000_000).to_string(),
                _ => (150_000_000_000_000 + i).to_string(),
            };
            let z = if i % 5 == 0 { "X'00FF'" } else { "NULL" };
            format!(
                "({i}, '{}', {n}, {}.25, {z})",
                "abcdefghijklmnopqrstuvwxyz0123456789ABCDEFGHIJ"[..len].to_owned(),
                i - 300
            )
        })
        .collect();
    format!(
        "CREATE TABLE edge(id INTEGER PRIMARY KEY, s TEXT, n INTEGER, r REAL, z);BEGIN;{}COMMIT;",
        inserts("edge", &rows)
    )
}

/// Rows of a rowid-alias table that are one column short of the table and
/// hold their own rowid in the alias slot: the shape old FrankenSQLite files
/// have after ALTER TABLE ADD COLUMN. Built with stock through
/// `writable_schema`, since no current writer produces it.
const LEGACY_QUERIES: [&str; 4] = [
    "SELECT a, id, b, c FROM legacy ORDER BY id",
    "SELECT b, a FROM legacy WHERE id IN (1, 7, 200) ORDER BY id",
    "SELECT count(*), sum(id), sum(a), count(b), count(c) FROM legacy",
    "SELECT id, b FROM legacy WHERE a % 5 = 2 ORDER BY id DESC",
];

fn legacy_setup(stock: &rusqlite::Connection) {
    let rows: Vec<String> = (1..=200_i64)
        .map(|i| format!("({}, {i}, 'b-{i}')", i * 3))
        .collect();
    stock
        .execute_batch(&format!(
            "CREATE TABLE legacy(a INTEGER, id INTEGER, b TEXT);BEGIN;{}COMMIT;\
             PRAGMA writable_schema=ON;\
             UPDATE sqlite_master \
                SET sql='CREATE TABLE legacy(a INTEGER, id INTEGER PRIMARY KEY, b TEXT, c TEXT)' \
              WHERE name='legacy';\
             PRAGMA writable_schema=OFF;",
            inserts("legacy", &rows)
        ))
        .expect("legacy setup");
}

/// A file written by stock SQLite, which stores a NULL in the rowid-alias
/// slot of every record, read and then mutated by FrankenSQLite. Stock runs
/// the same statements on its own copy of the file.
async fn run_stock_written_case(failures: &mut Vec<String>) {
    let dir = tempfile::tempdir().expect("temp dir");
    let seed = dir.path().join("seed.db");
    {
        let writer = rusqlite::Connection::open(&seed).expect("stock seed open");
        writer.execute_batch(&setup_sql()).expect("stock seed setup");
        writer
            .execute_batch(&inline_edge_setup())
            .expect("stock seed edge setup");
        legacy_setup(&writer);
    }
    let frank_path = dir.path().join("frank.db");
    let stock_path = dir.path().join("stock.db");
    std::fs::copy(&seed, &frank_path).expect("copy seed for frank");
    std::fs::copy(&seed, &stock_path).expect("copy seed for stock");

    let frank = Connection::open(frank_path.to_str().expect("utf-8 path"))
        .await
        .expect("open stock-written file");
    let stock = rusqlite::Connection::open(&stock_path).expect("stock open");

    let label = "[stock-written file] loaded";
    compare(label, &frank, &stock, &T1_QUERIES, failures).await;
    compare(label, &frank, &stock, &OTHER_QUERIES, failures).await;
    compare(label, &frank, &stock, &INLINE_EDGE_QUERIES, failures).await;
    compare(label, &frank, &stock, &TOP_N_QUERIES, failures).await;
    compare(label, &frank, &stock, &TYPED_AGG_QUERIES, failures).await;
    compare(label, &frank, &stock, &LEGACY_QUERIES, failures).await;
    for (step, mutation) in mutations().iter().enumerate() {
        frank.execute_batch(mutation).await.expect("frank mutation");
        stock.execute_batch(mutation).expect("stock mutation");
        let label = format!("[stock-written file] after mutation {step}");
        compare(&label, &frank, &stock, &T1_QUERIES, failures).await;
        compare(&label, &frank, &stock, &OTHER_QUERIES[..4], failures).await;
    }
    frank.close().await.expect("close");
}

#[test]
fn scans_and_column_reads_match_sqlite_on_both_sides_of_every_sync_lane_boundary() {
    asupersync::test_utils::run_test(|| async {
        let mut failures = Vec::new();
        for file_backed in [true, false] {
            run_case(file_backed, &mut failures).await;
        }
        run_stock_written_case(&mut failures).await;
        assert!(
            failures.is_empty(),
            "{} queries differ from SQLite:\n{}",
            failures.len(),
            failures.join("\n")
        );
    });
}
