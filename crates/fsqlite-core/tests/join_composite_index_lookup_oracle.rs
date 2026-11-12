#![recursion_limit = "512"]

//! A two-table join whose ON clause is a conjunction (`t.a = q.a AND t.k =
//! q.k`), or a single equality whose only index is composite, ran as a nested
//! loop that scanned the whole right table per left row (20k x 200k rows:
//! over 90 s against stock's 0.02 s). The join lookup now seeks the right
//! table by rowid or by the index whose leading key terms the equalities pin
//! exactly, and checks every other ON conjunct on each looked-up row, as
//! SQLite's `SEARCH t USING INDEX t_ak (a=? AND k=?)` does.
//!
//! Joins are compared with rusqlite (rows), ad hoc and prepared, in memory
//! and file-backed: inner and LEFT joins, residual conjuncts on either side,
//! NULL and duplicate keys, affinity coercion, a NOCASE comparison against a
//! BINARY key term, a DESC index, aggregates, WHERE and ORDER BY.

use fsqlite_core::connection::Connection;
use fsqlite_types::value::SqliteValue;

fn tag_f(v: &SqliteValue) -> String {
    match v {
        SqliteValue::Null => "NULL".to_owned(),
        SqliteValue::Integer(n) => n.to_string(),
        SqliteValue::Float(f) => format!("{f}"),
        SqliteValue::Text(s) => format!("'{s}'"),
        SqliteValue::Blob(b) => format!("blob{b:?}"),
    }
}

fn tag_r(v: &rusqlite::types::Value) -> String {
    match v {
        rusqlite::types::Value::Null => "NULL".to_owned(),
        rusqlite::types::Value::Integer(n) => n.to_string(),
        rusqlite::types::Value::Real(f) => format!("{f}"),
        rusqlite::types::Value::Text(s) => format!("'{s}'"),
        rusqlite::types::Value::Blob(b) => format!("blob{b:?}"),
    }
}

const SETUP: &[&str] = &[
    "CREATE TABLE t(id INTEGER PRIMARY KEY, a INTEGER, k TEXT, v)",
    "CREATE INDEX t_ak ON t(a, k)",
    "CREATE TABLE d(id INTEGER PRIMARY KEY, a INTEGER, k TEXT, v)",
    "CREATE INDEX d_ak ON d(a DESC, k)",
    "CREATE TABLE q(id INTEGER PRIMARY KEY, a INTEGER, k TEXT, w)",
    "CREATE TABLE u(id INTEGER PRIMARY KEY, a TEXT, k, w)",
    "INSERT INTO t VALUES (1, 1, 'x', 1), (2, 1, 'x', 2), (3, 1, 'y', 3), (4, 2, 'x', 4), \
     (5, 2, NULL, 5), (6, NULL, 'x', 6), (7, 3, 'X', 7), (8, 3, 'x', 8), (9, 1, 'z', 9), \
     (10, 2, '10', 10)",
    "INSERT INTO d SELECT * FROM t",
    "INSERT INTO q VALUES (1, 1, 'x', 1), (2, 1, 'y', 0), (3, 2, 'x', 1), (4, 2, NULL, 1), \
     (5, NULL, 'x', 0), (6, 3, 'x', 1), (7, 9, 'x', 1), (8, 1, 'z', 0), (9, 3, 'X', 1), \
     (10, 2, '10', 1)",
    "INSERT INTO u VALUES (1, '1', 'x', 1), (2, '2', 'x', 0), (3, ' 1', 'x', 1), (4, 'x', 'x', 1), \
     (5, '2', 10, 1), (6, '1.0', 'y', 1), (7, NULL, 'x', 1)",
];

const QUERIES: &[&str] = &[
    // Both key terms, either operand order.
    "SELECT q.id, t.id, t.v FROM q JOIN t ON t.a = q.a AND t.k = q.k ORDER BY q.id, t.id",
    "SELECT q.id, t.id FROM q JOIN t ON q.k = t.k AND q.a = t.a ORDER BY q.id, t.id",
    "SELECT q.id, t.id FROM q LEFT JOIN t ON t.a = q.a AND t.k = q.k ORDER BY q.id, t.id",
    // Residual conjuncts on the right row, on the left row, never true.
    "SELECT q.id, t.id FROM q JOIN t ON t.a = q.a AND t.k = q.k AND t.v > 1 ORDER BY q.id, t.id",
    "SELECT q.id, t.id FROM q LEFT JOIN t ON t.a = q.a AND t.k = q.k AND t.v > 1 \
     ORDER BY q.id, t.id",
    "SELECT q.id, t.id FROM q LEFT JOIN t ON t.a = q.a AND q.w = 1 ORDER BY q.id, t.id",
    "SELECT q.id, t.id FROM q LEFT JOIN t ON t.a = q.a AND t.k = q.k AND 0 ORDER BY q.id, t.id",
    // One equality on a composite index's leading term.
    "SELECT q.id, t.id FROM q JOIN t ON t.a = q.a ORDER BY q.id, t.id",
    "SELECT q.id, t.id FROM q LEFT JOIN t ON t.a = q.a AND t.v <> 2 ORDER BY q.id, t.id",
    // A rowid equality with a residual.
    "SELECT q.id, t.id FROM q JOIN t ON t.id = q.id AND t.k = q.k ORDER BY q.id",
    "SELECT q.id, t.id FROM q LEFT JOIN t ON t.id = q.id AND t.k = q.k ORDER BY q.id",
    // Affinity: TEXT and typeless probes into INTEGER / TEXT key terms.
    "SELECT u.id, t.id FROM u JOIN t ON t.a = u.a AND t.k = u.k ORDER BY u.id, t.id",
    "SELECT u.id, t.id FROM u LEFT JOIN t ON t.a = u.a AND t.k = u.k ORDER BY u.id, t.id",
    // A NOCASE comparison may not seek the BINARY key term.
    "SELECT q.id, t.id FROM q JOIN t ON t.a = q.a AND t.k = q.k COLLATE NOCASE \
     ORDER BY q.id, t.id",
    // A DESC key term.
    "SELECT q.id, d.id FROM q JOIN d ON d.a = q.a AND d.k = q.k ORDER BY q.id, d.id",
    // Aggregates, WHERE, ORDER BY.
    "SELECT count(*), sum(t.v), count(t.id) FROM q JOIN t ON t.a = q.a AND t.k = q.k",
    "SELECT count(*), count(t.id) FROM q LEFT JOIN t ON t.a = q.a AND t.k = q.k",
    "SELECT q.id, t.v FROM q JOIN t ON t.a = q.a AND t.k = q.k WHERE t.v < 8 ORDER BY t.v DESC",
    "SELECT q.id, t.id FROM q JOIN t ON t.a = q.a AND t.k = q.k WHERE q.w = 1 ORDER BY t.id DESC",
];

async fn rows_f(conn: &Connection, sql: &str) -> Vec<Vec<String>> {
    conn.query(sql)
        .await
        .unwrap_or_else(|e| panic!("franken `{sql}`: {e:?}"))
        .iter()
        .map(|row| row.values().iter().map(tag_f).collect())
        .collect()
}

async fn rows_prepared(conn: &Connection, sql: &str) -> Vec<Vec<String>> {
    conn.prepare(sql)
        .await
        .unwrap_or_else(|e| panic!("franken prepare `{sql}`: {e:?}"))
        .query()
        .await
        .unwrap_or_else(|e| panic!("franken prepared `{sql}`: {e:?}"))
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

async fn opcodes(conn: &Connection, sql: &str) -> Vec<String> {
    conn.query(&format!("EXPLAIN {sql}"))
        .await
        .unwrap_or_else(|e| panic!("franken EXPLAIN `{sql}`: {e:?}"))
        .iter()
        .filter_map(|row| match row.values().get(1) {
            Some(SqliteValue::Text(opcode)) => Some(opcode.to_string()),
            _ => None,
        })
        .collect()
}

#[test]
fn conjunctive_join_keys_seek_the_composite_index_like_sqlite() {
    for file_backed in [false, true] {
        asupersync::test_utils::run_test(|| async move {
            let dir = tempfile::tempdir().unwrap();
            let target = if file_backed {
                dir.path()
                    .join("join_composite.db")
                    .to_string_lossy()
                    .into_owned()
            } else {
                ":memory:".to_owned()
            };
            let f = Connection::open(&target).await.unwrap();
            let r = rusqlite::Connection::open_in_memory().unwrap();
            for sql in SETUP {
                f.execute(sql).await.unwrap();
                r.execute(sql, []).unwrap();
            }
            for sql in QUERIES {
                let stock = rows_r(&r, sql);
                assert_eq!(rows_f(&f, sql).await, stock, "query `{sql}`");
                assert_eq!(rows_prepared(&f, sql).await, stock, "prepared `{sql}`");
            }
            // The right table is sought, not scanned per left row.
            for sql in [
                "SELECT q.id, t.id FROM q JOIN t ON t.a = q.a AND t.k = q.k",
                "SELECT q.id, t.id FROM q LEFT JOIN t ON t.a = q.a AND t.k = q.k AND t.v > 1",
                "SELECT count(*), sum(t.v) FROM q JOIN t ON t.a = q.a AND t.k = q.k",
            ] {
                let ops = opcodes(&f, sql).await;
                assert!(
                    ops.iter().filter(|op| *op == "Rewind").count() == 1
                        && ops.iter().any(|op| op == "SeekGE"),
                    "`{sql}` must scan q once and seek t's index: {ops:?}"
                );
            }
        });
    }
}

/// bd-w4pr1.10: an equi-join with no index on either join column ran as a
/// nested loop that rescanned the right table per left row on a file-backed
/// database (4k x 4k rows: 3.65 s, stock 2 ms). It now builds a transient
/// `(key, rowid)` index over the right join column once, like stock's
/// AUTOMATIC COVERING INDEX, and probes it. `q` and `u` have no indexes; the
/// rows hold NULL and duplicate keys, mixed affinities and case variants.
#[test]
fn unindexed_equi_join_builds_an_automatic_index_like_sqlite() {
    const ORDERED: &[&str] = &[
        "SELECT q.id, u.id FROM q JOIN u ON u.k = q.k ORDER BY q.id, u.id",
        "SELECT q.id, u.id FROM q JOIN u ON q.k = u.k ORDER BY q.id, u.id",
        "SELECT q.id, u.id FROM q LEFT JOIN u ON u.k = q.k ORDER BY q.id, u.id",
        "SELECT q.id, u.id FROM q JOIN u ON u.a = q.a ORDER BY q.id, u.id",
        "SELECT u.id, q.id FROM u LEFT JOIN q ON q.a = u.a ORDER BY u.id, q.id",
        "SELECT u.id, q.id FROM u JOIN q ON q.k = u.k ORDER BY u.id, q.id",
        "SELECT q.id, u.id FROM q JOIN u ON u.k = q.k COLLATE NOCASE ORDER BY q.id, u.id",
        "SELECT q.id, u.id FROM q LEFT JOIN u ON u.w = q.w * 1 ORDER BY q.id, u.id",
        "SELECT q.id, u.id FROM q JOIN u ON u.k = q.k AND u.w = 1 ORDER BY q.id, u.id",
        "SELECT q.id, u.id FROM q LEFT JOIN u ON u.k = q.k AND q.w = 1 ORDER BY q.id, u.id",
        "SELECT q.id, u.id FROM q JOIN u ON u.k = q.k WHERE q.w = 1 ORDER BY u.id DESC, q.id",
        "SELECT q.id, u.id FROM q JOIN u ON u.k = q.k ORDER BY q.id, u.id LIMIT 3 OFFSET 2",
        "SELECT a.id, b.id FROM q AS a JOIN q AS b ON b.k = a.k ORDER BY a.id, b.id",
        "SELECT count(*), sum(u.w), count(u.id) FROM q LEFT JOIN u ON u.k = q.k",
        "SELECT count(*), max(q.id) FROM q JOIN u ON u.a = q.a",
        "SELECT q.k, count(u.id) FROM q LEFT JOIN u ON u.k = q.k GROUP BY q.k ORDER BY 1",
    ];
    // Row order without ORDER BY is unspecified; compare these as multisets.
    const UNORDERED: &[&str] = &[
        "SELECT q.id, u.id, u.k FROM q JOIN u ON u.k = q.k",
        "SELECT q.id, u.id FROM q LEFT JOIN u ON u.a = q.a",
        "SELECT * FROM q JOIN u ON u.k = q.k COLLATE NOCASE",
    ];
    for file_backed in [false, true] {
        asupersync::test_utils::run_test(|| async move {
            let dir = tempfile::tempdir().unwrap();
            let target = if file_backed {
                dir.path()
                    .join("join_autoindex.db")
                    .to_string_lossy()
                    .into_owned()
            } else {
                ":memory:".to_owned()
            };
            let f = Connection::open(&target).await.unwrap();
            let r = rusqlite::Connection::open_in_memory().unwrap();
            for sql in SETUP {
                f.execute(sql).await.unwrap();
                r.execute(sql, []).unwrap();
            }
            for sql in ORDERED {
                let stock = rows_r(&r, sql);
                assert!(!stock.is_empty(), "fixture must give `{sql}` rows");
                assert_eq!(rows_f(&f, sql).await, stock, "query `{sql}`");
                assert_eq!(rows_prepared(&f, sql).await, stock, "prepared `{sql}`");
            }
            for sql in UNORDERED {
                let mut stock = rows_r(&r, sql);
                stock.sort();
                let mut ours = rows_f(&f, sql).await;
                ours.sort();
                assert_eq!(ours, stock, "query `{sql}`");
            }

            // Many duplicate keys on both sides.
            let rows = |table: &str, n: i64, modulus: i64| {
                let values: Vec<String> = (1..=n).map(|i| format!("({i}, {})", i % modulus)).collect();
                format!("INSERT INTO {table} VALUES {}", values.join(", "))
            };
            for sql in [
                "CREATE TABLE bl(id INTEGER PRIMARY KEY, k INTEGER)".to_owned(),
                "CREATE TABLE br(id INTEGER PRIMARY KEY, k INTEGER)".to_owned(),
                rows("bl", 3000, 211),
                rows("br", 3000, 307),
            ] {
                f.execute(&sql).await.unwrap();
                r.execute(&sql, []).unwrap();
            }
            for sql in [
                "SELECT count(*), sum(bl.id), sum(br.id) FROM bl JOIN br ON br.k = bl.k",
                "SELECT count(*), count(br.id) FROM bl LEFT JOIN br ON br.k = bl.k + 250",
            ] {
                assert_eq!(rows_f(&f, sql).await, rows_r(&r, sql), "query `{sql}`");
            }

            if file_backed {
                // u is read once to build the index and q once to drive the
                // probes; nothing rescans u per q row.
                for sql in [
                    "SELECT q.id, u.id FROM q JOIN u ON u.k = q.k",
                    "SELECT q.id, u.id FROM q LEFT JOIN u ON u.k = q.k ORDER BY q.id",
                    "SELECT count(*), sum(u.w) FROM q JOIN u ON u.k = q.k",
                ] {
                    let ops = opcodes(&f, sql).await;
                    assert!(
                        ops.iter().any(|op| op == "OpenAutoindex")
                            && ops.iter().filter(|op| *op == "Rewind").count() == 2
                            && ops.iter().any(|op| op == "SeekGE"),
                        "`{sql}` must index u once and probe it: {ops:?}"
                    );
                }
            }
        });
    }
}

/// bd-w4pr1.13.15: the automatic index of an unindexed equi-join stages its
/// `(key, rowid)` entries and is built from them in one pass, sorted with the
/// index's own comparator. The probes must find exactly stock's matches:
/// hundreds of rows whose keys mix integers, equal reals, text that differs
/// only in case (one run per NOCASE key), NULL and long runs of duplicates; an
/// empty and a one-row right table; and an INSERT ... SELECT over the join
/// whose UNIQUE conflicts are ignored while the index exists. On UTF-8 and
/// UTF-16le databases, in memory and on a file.
#[test]
fn unindexed_equi_join_index_built_in_one_pass_matches_sqlite() {
    const QUERIES: &[&str] = &[
        "SELECT count(*), sum(l.id), sum(r.id) FROM l JOIN r ON r.k = l.k",
        "SELECT count(*), sum(l.id * r.id) FROM l JOIN r ON r.k = l.k COLLATE NOCASE",
        "SELECT count(*), count(r.id) FROM l LEFT JOIN r ON r.k = l.k",
        "SELECT l.id, r.id FROM l JOIN r ON r.k = l.k WHERE l.id % 41 = 0 ORDER BY l.id, r.id",
        "SELECT l.id, r.id FROM l JOIN r ON r.k = l.k COLLATE NOCASE \
         WHERE l.id % 37 = 0 ORDER BY l.id, r.id",
        "SELECT l.k, count(r.id) FROM l LEFT JOIN r ON r.k = l.k GROUP BY l.k ORDER BY 1",
        "SELECT count(*), count(e.id) FROM l LEFT JOIN e ON e.k = l.k",
        "SELECT count(*), sum(o.id) FROM l JOIN o ON o.k = l.k",
        "SELECT count(*), sum(l.id) FROM o JOIN l ON l.k = o.k",
    ];
    // Key of row i: an integer, the same value as a REAL, two case variants of
    // one text key, or NULL.
    let key = |i: i64| match i % 6 {
        0 => format!("{}", i % 23),
        1 => format!("{}.0", i % 23),
        2 => format!("'Key{}'", i % 17),
        3 => format!("'kEY{}'", i % 17),
        4 if i % 4 == 0 => "NULL".to_owned(),
        _ => format!("{}", i % 29),
    };
    let rows = |table: &str, n: i64, step: i64| {
        let values: Vec<String> = (1..=n)
            .map(|i| format!("({i}, {})", key(i * step)))
            .collect();
        format!("INSERT INTO {table} VALUES {}", values.join(", "))
    };
    let setup = [
        "CREATE TABLE l(id INTEGER PRIMARY KEY, k)".to_owned(),
        "CREATE TABLE r(id INTEGER PRIMARY KEY, k)".to_owned(),
        "CREATE TABLE e(id INTEGER PRIMARY KEY, k)".to_owned(),
        "CREATE TABLE o(id INTEGER PRIMARY KEY, k)".to_owned(),
        "CREATE TABLE sink(l_id INTEGER UNIQUE, r_id INTEGER)".to_owned(),
        rows("l", 400, 7),
        rows("r", 900, 5),
        "INSERT INTO o VALUES (5, 4)".to_owned(),
    ];
    for encoding in ["UTF-8", "UTF-16le"] {
        for file_backed in [false, true] {
            let setup = setup.clone();
            asupersync::test_utils::run_test(|| async move {
                let dir = tempfile::tempdir().unwrap();
                let target = if file_backed {
                    dir.path()
                        .join("join_autoindex_staged.db")
                        .to_string_lossy()
                        .into_owned()
                } else {
                    ":memory:".to_owned()
                };
                let f = Connection::open(&target).await.unwrap();
                let r = rusqlite::Connection::open_in_memory().unwrap();
                let pragma = format!("PRAGMA encoding = '{encoding}'");
                f.execute(&pragma).await.unwrap();
                r.execute_batch(&pragma).unwrap();
                for sql in &setup {
                    f.execute(sql).await.unwrap();
                    r.execute(sql, []).unwrap();
                }
                for sql in QUERIES {
                    let stock = rows_r(&r, sql);
                    assert!(!stock.is_empty(), "fixture must give `{sql}` rows");
                    assert_eq!(rows_f(&f, sql).await, stock, "{encoding} query `{sql}`");
                    assert_eq!(
                        rows_prepared(&f, sql).await,
                        stock,
                        "{encoding} prepared `{sql}`"
                    );
                }
                // Many joined rows share an l.id; all but the first of each
                // conflict on UNIQUE(l_id) and are ignored.
                let insert = "INSERT OR IGNORE INTO sink SELECT l.id, r.id FROM l JOIN r ON r.k = l.k";
                f.execute(insert).await.unwrap();
                r.execute(insert, []).unwrap();
                let sink = "SELECT count(*), sum(l_id), count(DISTINCT r_id) FROM sink";
                assert_eq!(rows_f(&f, sink).await, rows_r(&r, sink), "{encoding} `{insert}`");
                let ops = opcodes(&f, QUERIES[0]).await;
                assert!(
                    ops.iter().any(|op| op == "OpenAutoindex")
                        && ops.iter().any(|op| op == "IdxBulkLoad"),
                    "`{}` must build its automatic index in one pass: {ops:?}",
                    QUERIES[0]
                );
            });
        }
    }
}

/// GH#502: v0.4.10 returned no rows for a join whose OUTER table is filtered
/// through an index with a DESC column (`c(a, s DESC)`) while the inner table
/// is looked up through its own index. Inner and LEFT joins, with or without
/// GROUP BY, came back empty; the single-table query was right. The fix
/// landed on main before this keeper; it pins the shape against stock.
#[test]
fn join_driven_by_a_desc_column_index_returns_rows_like_sqlite() {
    const SETUP_502: &[&str] = &[
        "CREATE TABLE c (id INTEGER PRIMARY KEY, a INTEGER, s INTEGER, x TEXT)",
        "INSERT INTO c VALUES (1, 1, 1000, 'one'), (2, 1, 2000, 'two'), (3, 2, 3000, 'three')",
        "CREATE TABLE m (id INTEGER PRIMARY KEY, cid INTEGER)",
        "CREATE INDEX im ON m(cid)",
        "INSERT INTO m VALUES (10, 1), (11, 1), (12, 3)",
        "CREATE INDEX ia ON c(a, s DESC)",
    ];
    const QUERIES_502: &[&str] = &[
        "SELECT c.x, COUNT(m.id) FROM c LEFT JOIN m ON m.cid = c.id WHERE c.a = 1 GROUP BY c.id",
        "SELECT c.x, m.id FROM c LEFT JOIN m ON m.cid = c.id WHERE c.a = 1 ORDER BY c.id, m.id",
        "SELECT c.x, m.id FROM c JOIN m ON m.cid = c.id WHERE c.a = 1 ORDER BY c.id, m.id",
        "SELECT c.x, m.id FROM c JOIN m ON m.cid = c.id WHERE c.a = 1 AND c.s < 1500",
        "SELECT c.x FROM c WHERE c.a = 1 ORDER BY c.id",
    ];
    for file_backed in [false, true] {
        asupersync::test_utils::run_test(|| async move {
            let dir = tempfile::tempdir().unwrap();
            let target = if file_backed {
                dir.path().join("gh502.db").to_string_lossy().into_owned()
            } else {
                ":memory:".to_owned()
            };
            let f = Connection::open(&target).await.unwrap();
            let r = rusqlite::Connection::open_in_memory().unwrap();
            for sql in SETUP_502 {
                f.execute(sql).await.unwrap();
                r.execute(sql, []).unwrap();
            }
            for sql in QUERIES_502 {
                let stock = rows_r(&r, sql);
                assert!(!stock.is_empty(), "fixture must give `{sql}` rows");
                assert_eq!(rows_f(&f, sql).await, stock, "query `{sql}`");
                assert_eq!(rows_prepared(&f, sql).await, stock, "prepared `{sql}`");
            }
        });
    }
}
