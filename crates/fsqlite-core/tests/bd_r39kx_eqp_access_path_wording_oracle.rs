//! bd-r39kx: `EXPLAIN QUERY PLAN` names the access path fsqlite runs in stock's
//! words, for the single-table shapes whose program and stock's plan agree:
//! a rowid range the program seeks prints `SEARCH t USING INTEGER PRIMARY KEY
//! (rowid>? AND rowid<?)`, an index range prints its bounds (`(a>?)`,
//! `(a>? AND a<?)`) instead of `(a range)`, an index seek that never reads the
//! table is `COVERING`, an alias names the source, and the sorter an ORDER BY
//! needs is reported. Each statement compares with the bundled SQLite 3.54.1
//! on `:memory:` and on a file, and returns the same rows. Negatives: a real
//! full scan still prints `SCAN t`, a seek that looks the row up is not
//! `COVERING`, and an ORDER BY the index already satisfies adds no sorter row.
//!
//! Round 12 (SQL lane): a walk from the first row that stops at an upper
//! bound prints `(rowid<?)` / `(a<?)`; a min or max read at one end of an
//! index is `SEARCH t USING COVERING INDEX ta`; a count of the index's
//! entries and a walk of the whole index are one row, `SCAN <t or alias>
//! USING [COVERING] INDEX ta`, where fsqlite printed `SCAN INDEX ta ...` (and
//! a separate `SCAN t` when it looked rows up). Negative: a whole-index walk
//! that looks the row up is not `COVERING`.

use fsqlite_core::connection::Connection;
use fsqlite_types::value::SqliteValue;

const SETUP: &[&str] = &[
    "CREATE TABLE t(id INTEGER PRIMARY KEY, a INTEGER, b TEXT, c INTEGER)",
    "CREATE INDEX ta ON t(a)",
    "CREATE INDEX tbc ON t(b, c)",
    "CREATE TABLE n(x, y)",
    "CREATE INDEX nx ON n(x)",
    "INSERT INTO t VALUES (1, 10, 'x', 1), (2, 20, 'y', 2), (3, 30, 'x', 3)",
    "INSERT INTO n VALUES (4, 'p'), (6, 'q')",
];

/// Statements whose `EXPLAIN QUERY PLAN` rows must equal stock's.
const SHAPES: &[&str] = &[
    // Rowid ranges the program seeks.
    "SELECT * FROM t WHERE id > 1",
    "SELECT * FROM t WHERE id >= 1",
    "SELECT * FROM t WHERE id BETWEEN 1 AND 2",
    "SELECT * FROM t WHERE id > 1 AND id < 3",
    "SELECT * FROM t WHERE rowid > 1",
    "SELECT b FROM t WHERE id > 1 AND c = 3",
    // Index ranges.
    "SELECT * FROM t WHERE a > 15",
    "SELECT * FROM t WHERE a >= 15",
    "SELECT a FROM t WHERE a BETWEEN 15 AND 40",
    "SELECT a FROM t WHERE a > 15 AND a < 40",
    "SELECT * FROM t WHERE b > 'w'",
    "SELECT * FROM n WHERE x > 5",
    "SELECT * FROM t AS q WHERE q.a > 15",
    "SELECT * FROM t WHERE a > 15 LIMIT 1",
    // Covering and not.
    "SELECT id FROM t WHERE a = 20",
    "SELECT id, a FROM t WHERE a = 20",
    "SELECT * FROM t WHERE a = 20",
    "SELECT * FROM t AS q WHERE q.a = 20",
    // The ORDER BY sorter, and an ORDER BY the index satisfies.
    "SELECT * FROM t WHERE a = 10 ORDER BY b",
    "SELECT * FROM t WHERE a = 20 ORDER BY id",
    // Real scans and point lookups.
    "SELECT * FROM t",
    "SELECT * FROM t WHERE id = 2",
    "SELECT * FROM t WHERE a + 0 = 20",
    // Round 12: walks from the first row that stop at an upper bound.
    "SELECT * FROM t WHERE id < 3",
    "SELECT * FROM t WHERE id <= 3",
    "SELECT * FROM t WHERE a < 25",
    "SELECT * FROM t WHERE a <= 25",
    "SELECT id FROM t WHERE a < 25",
    // min and max at one end of an index, and a count of its entries.
    "SELECT max(a) FROM t",
    "SELECT min(a) FROM t",
    "SELECT count(*) FROM t",
    // Whole-index walks for an ORDER BY or a GROUP BY, covering or looking
    // the row up, by the table or its alias.
    "SELECT a FROM t ORDER BY a",
    "SELECT a FROM t AS q ORDER BY a DESC",
    "SELECT * FROM t ORDER BY a",
    "SELECT * FROM t AS q ORDER BY a",
    "SELECT a, count(*) FROM t GROUP BY a",
];

/// Stock's text for the slice, pinned so a change in the oracle shows here.
const PINNED: &[(&str, &[&str])] = &[
    (
        "SELECT * FROM t WHERE id > 1",
        &["SEARCH t USING INTEGER PRIMARY KEY (rowid>?)"],
    ),
    (
        "SELECT * FROM t WHERE id BETWEEN 1 AND 2",
        &["SEARCH t USING INTEGER PRIMARY KEY (rowid>? AND rowid<?)"],
    ),
    ("SELECT * FROM t WHERE a > 15", &["SEARCH t USING INDEX ta (a>?)"]),
    (
        "SELECT a FROM t WHERE a > 15 AND a < 40",
        &["SEARCH t USING COVERING INDEX ta (a>? AND a<?)"],
    ),
    ("SELECT * FROM t AS q WHERE q.a > 15", &["SEARCH q USING INDEX ta (a>?)"]),
    ("SELECT id FROM t WHERE a = 20", &["SEARCH t USING COVERING INDEX ta (a=?)"]),
    ("SELECT * FROM t WHERE a = 20", &["SEARCH t USING INDEX ta (a=?)"]),
    (
        "SELECT * FROM t WHERE a = 10 ORDER BY b",
        &["SEARCH t USING INDEX ta (a=?)", "USE TEMP B-TREE FOR ORDER BY"],
    ),
    ("SELECT * FROM t WHERE a = 20 ORDER BY id", &["SEARCH t USING INDEX ta (a=?)"]),
    ("SELECT * FROM t", &["SCAN t"]),
    (
        "SELECT * FROM t WHERE id <= 3",
        &["SEARCH t USING INTEGER PRIMARY KEY (rowid<?)"],
    ),
    ("SELECT * FROM t WHERE a < 25", &["SEARCH t USING INDEX ta (a<?)"]),
    (
        "SELECT id FROM t WHERE a < 25",
        &["SEARCH t USING COVERING INDEX ta (a<?)"],
    ),
    ("SELECT max(a) FROM t", &["SEARCH t USING COVERING INDEX ta"]),
    ("SELECT count(*) FROM t", &["SCAN t USING COVERING INDEX ta"]),
    ("SELECT a FROM t AS q ORDER BY a DESC", &["SCAN q USING COVERING INDEX ta"]),
    ("SELECT * FROM t ORDER BY a", &["SCAN t USING INDEX ta"]),
];

async fn frank_eqp(conn: &Connection, sql: &str) -> Vec<String> {
    conn.query(&format!("EXPLAIN QUERY PLAN {sql}"))
        .await
        .unwrap_or_else(|error| panic!("fsqlite EQP `{sql}`: {error}"))
        .iter()
        .map(|row| match row.values().get(3) {
            Some(SqliteValue::Text(detail)) => detail.to_string(),
            other => panic!("EQP `{sql}`: detail column {other:?}"),
        })
        .collect()
}

fn stock_eqp(conn: &rusqlite::Connection, sql: &str) -> Vec<String> {
    let mut statement = conn
        .prepare(&format!("EXPLAIN QUERY PLAN {sql}"))
        .unwrap_or_else(|error| panic!("stock EQP `{sql}`: {error}"));
    statement
        .query_map([], |row| row.get::<_, String>(3))
        .unwrap_or_else(|error| panic!("stock EQP `{sql}`: {error}"))
        .map(|detail| detail.expect("stock EQP row"))
        .collect()
}

fn render(value: &SqliteValue) -> String {
    match value {
        SqliteValue::Null => "NULL".to_owned(),
        SqliteValue::Integer(integer) => integer.to_string(),
        SqliteValue::Float(float) => format!("{float}r"),
        SqliteValue::Text(text) => format!("'{text}'"),
        SqliteValue::Blob(blob) => format!("{blob:?}"),
    }
}

fn render_stock(value: rusqlite::types::ValueRef<'_>) -> String {
    use rusqlite::types::ValueRef;
    match value {
        ValueRef::Null => "NULL".to_owned(),
        ValueRef::Integer(integer) => integer.to_string(),
        ValueRef::Real(float) => format!("{float}r"),
        ValueRef::Text(text) => format!("'{}'", String::from_utf8_lossy(text)),
        ValueRef::Blob(blob) => format!("{blob:?}"),
    }
}

async fn frank_rows(conn: &Connection, sql: &str) -> Vec<Vec<String>> {
    conn.query(sql)
        .await
        .unwrap_or_else(|error| panic!("fsqlite `{sql}`: {error}"))
        .iter()
        .map(|row| row.values().iter().map(render).collect())
        .collect()
}

fn stock_rows(conn: &rusqlite::Connection, sql: &str) -> Vec<Vec<String>> {
    let mut statement = conn
        .prepare(sql)
        .unwrap_or_else(|error| panic!("stock `{sql}`: {error}"));
    let width = statement.column_count();
    statement
        .query_map([], |row| {
            (0..width)
                .map(|index| row.get_ref(index).map(render_stock))
                .collect::<rusqlite::Result<Vec<_>>>()
        })
        .unwrap_or_else(|error| panic!("stock `{sql}`: {error}"))
        .map(|row| row.expect("stock row"))
        .collect()
}

#[test]
fn eqp_names_rowid_ranges_index_bounds_covering_and_sorter_as_stock() {
    asupersync::test_utils::run_test(|| async {
        let stock = rusqlite::Connection::open_in_memory().expect("stock");
        for sql in SETUP {
            stock.execute_batch(sql).expect("stock setup");
        }
        for (sql, expected) in PINNED {
            assert_eq!(stock_eqp(&stock, sql), *expected, "stock EQP `{sql}`");
        }
        let dir = tempfile::tempdir().expect("tempdir");
        let file = dir.path().join("eqp.db");
        for path in [":memory:", file.to_str().expect("utf-8 path")] {
            let conn = Connection::open(path).await.expect("fsqlite open");
            for sql in SETUP {
                conn.execute(sql).await.expect("fsqlite setup");
            }
            for sql in SHAPES {
                assert_eq!(
                    frank_eqp(&conn, sql).await,
                    stock_eqp(&stock, sql),
                    "{path}: EQP `{sql}`"
                );
                let mut frank = frank_rows(&conn, sql).await;
                let mut oracle = stock_rows(&stock, sql);
                if !sql.contains("ORDER BY") {
                    frank.sort();
                    oracle.sort();
                }
                assert_eq!(frank, oracle, "{path}: rows of `{sql}`");
            }
            conn.close().await.expect("fsqlite close");
        }
    });
}

async fn frank_eqp_rows(conn: &Connection, sql: &str) -> Vec<(i64, i64, String)> {
    let eqp_sql = format!("EXPLAIN QUERY PLAN {sql}");
    let rows = conn
        .query(&eqp_sql)
        .await
        .unwrap_or_else(|error| panic!("fsqlite `{eqp_sql}`: {error}"));
    rows.iter()
        .map(|row| {
            let id = row.values()[0].to_integer();
            let parent = row.values()[1].to_integer();
            let detail = match row.values().get(3) {
                Some(SqliteValue::Text(detail)) => detail.to_string(),
                other => panic!("EQP `{sql}`: detail column {other:?}"),
            };
            (id, parent, detail)
        })
        .collect()
}

async fn frank_rows_mode(conn: &Connection, sql: &str, prepared: bool) -> Vec<Vec<String>> {
    let rows = if prepared {
        let stmt = conn
            .prepare(sql)
            .await
            .unwrap_or_else(|error| panic!("fsqlite prepare `{sql}`: {error}"));
        stmt.query()
            .await
            .unwrap_or_else(|error| panic!("fsqlite prepared `{sql}`: {error}"))
    } else {
        conn.query(sql)
            .await
            .unwrap_or_else(|error| panic!("fsqlite `{sql}`: {error}"))
    };
    rows.iter()
        .map(|row| row.values().iter().map(render).collect())
        .collect()
}

fn assert_eqp_tree_valid(rows: &[(i64, i64, String)], context: &str) {
    assert!(!rows.is_empty(), "{context}: empty EQP");
    let mut ids = Vec::with_capacity(rows.len());
    for &(id, parent, _) in rows {
        assert!(id > 0, "{context}: non-positive id {id} in {rows:?}");
        if parent != 0 {
            assert!(
                ids.contains(&parent),
                "{context}: parent {parent} must precede child {id} in {rows:?}"
            );
        }
        assert!(!ids.contains(&id), "{context}: duplicate id {id} in {rows:?}");
        ids.push(id);
    }
}

/// bd-oi8tv (GH#432 finding 4): `EXPLAIN QUERY PLAN` emits structured
/// `SCALAR SUBQUERY` / `LIST SUBQUERY` / `CORRELATED ... SUBQUERY` trees with
/// truthful inner `SEARCH`/`SCAN` steps and fallback-routing visibility when
/// a correlated subquery routes to a per-row fallback, across `:memory:` and
/// file-backed databases, direct and prepared, with live `rusqlite` 3.54.1
/// result-row parity.
#[test]
#[allow(clippy::too_many_lines)]
fn bd_oi8tv_eqp_reports_scalar_list_and_correlated_subqueries() {
    const SUBQUERY_SETUP: &[&str] = &[
        "CREATE TABLE repos (id INTEGER PRIMARY KEY, path TEXT NOT NULL, owner TEXT)",
        "CREATE INDEX idx_repos_path ON repos(path)",
        "CREATE TABLE repos_pk (path TEXT PRIMARY KEY)",
        "CREATE TABLE repos_nc (path TEXT COLLATE NOCASE PRIMARY KEY)",
        "CREATE TABLE t_txt (s TEXT UNIQUE)",
        "CREATE TABLE repos3 (path TEXT, n INTEGER)",
        "CREATE INDEX repos3_path ON repos3(path)",
        "CREATE TABLE evidence (id INTEGER PRIMARY KEY, repo TEXT NOT NULL, score INTEGER, n INTEGER)",
        "CREATE INDEX idx_ev_repo ON evidence(repo)",
        "CREATE TABLE notes (id INTEGER PRIMARY KEY, ref_id INTEGER, body TEXT)",
        "INSERT INTO repos VALUES (1, '/a', 'u1'), (2, '/b', 'u2')",
        "INSERT INTO repos_pk VALUES ('/a'), ('/b')",
        "INSERT INTO repos_nc VALUES ('/a'), ('/b')",
        "INSERT INTO t_txt VALUES ('1'), ('2')",
        "INSERT INTO repos3 VALUES ('/a', 1), ('/a', 2), ('/b', 3)",
        "INSERT INTO evidence VALUES (1, '/a', 10, 1), (2, '/missing', 20, 2), (3, '/A', 30, 5)",
        "INSERT INTO notes VALUES (1, 1, 'ok'), (2, 99, 'orphan')",
    ];

    asupersync::test_utils::run_test(|| async {
        let stock = rusqlite::Connection::open_in_memory().expect("stock");
        for sql in SUBQUERY_SETUP {
            stock.execute_batch(sql).expect("stock setup");
        }

        let dir = tempfile::tempdir().expect("tempdir");
        let file = dir.path().join("bd_oi8tv.db");
        let file_str = file.to_str().expect("utf-8 path").to_owned();

        // Populate and reopen the file-backed DB so `!join_mem_scan_safe()`
        // exercises the compiled GH#432 index-probe route (not the MemDB
        // fallback).
        {
            let seed = Connection::open(&file_str).await.expect("seed open");
            for sql in SUBQUERY_SETUP {
                seed.execute(sql).await.expect("seed setup");
            }
            seed.close().await.expect("seed close");
        }

        for (label, is_file) in [(":memory:", false), (file_str.as_str(), true)] {
            let conn = Connection::open(label).await.expect("fsqlite open");
            if !is_file {
                for sql in SUBQUERY_SETUP {
                    conn.execute(sql).await.expect("memory setup");
                }
            }

            let ctx = label.to_owned();

            // 1. Exact GH#432 nested EXISTS repro: before MemDB hydration
            //    both :memory: and file-backed take the compiled index
            //    probe (no fallback).
            let gh432 = "SELECT EXISTS(SELECT 1 FROM evidence WHERE NOT EXISTS(SELECT 1 FROM repos WHERE repos.path = evidence.repo))";
            let rows = frank_eqp_rows(&conn, gh432).await;
            assert_eqp_tree_valid(&rows, &format!("{ctx}: gh432"));
            let details: Vec<&str> = rows.iter().map(|(_, _, d)| d.as_str()).collect();
            assert_eq!(rows.len(), 5, "{ctx}: gh432 rows={rows:?}");
            assert_eq!(rows[0].1, 0);
            assert_eq!(rows[1].1, 0);
            assert_eq!(rows[2].1, rows[1].0);
            assert_eq!(rows[3].1, rows[1].0);
            assert_eq!(rows[4].1, rows[3].0);
            assert_eq!(details[0], "SCAN CONSTANT ROW", "{ctx}");
            assert_eq!(details[1], "SCALAR SUBQUERY 2", "{ctx}");
            assert_eq!(details[2], "SCAN evidence", "{ctx}");
            assert_eq!(details[3], "CORRELATED SCALAR SUBQUERY 1", "{ctx}");
            assert_eq!(
                details[4],
                "SEARCH repos USING COVERING INDEX idx_repos_path (path=?)",
                "{ctx}"
            );
            // Stock also reports the same 5-node tree shape.
            let stock_details = stock_eqp(&stock, gh432);
            assert_eq!(stock_details.len(), 5);
            assert_eq!(stock_details[1], "SCALAR SUBQUERY 2");
            assert_eq!(stock_details[3], "CORRELATED SCALAR SUBQUERY 1");
            assert_eq!(stock_details[4], details[4]);

            // 2. Sibling uncorrelated scalar subqueries: numbering 1, 2.
            let siblings =
                "SELECT (SELECT count(*) FROM repos), (SELECT max(score) FROM evidence)";
            let rows = frank_eqp_rows(&conn, siblings).await;
            assert_eqp_tree_valid(&rows, &format!("{ctx}: siblings"));
            let details: Vec<&str> = rows.iter().map(|(_, _, d)| d.as_str()).collect();
            assert_eq!(
                details,
                vec![
                    "SCAN CONSTANT ROW",
                    "SCALAR SUBQUERY 1",
                    "SCAN repos USING COVERING INDEX idx_repos_path",
                    "SCALAR SUBQUERY 2",
                    "SCAN evidence",
                ],
                "{ctx}: siblings"
            );

            // 3. Correlated scalar subquery with non-covering index (compiled path).
            let corr_scalar = "SELECT id, (SELECT owner FROM repos WHERE repos.path = evidence.repo) FROM evidence";
            let rows = frank_eqp_rows(&conn, corr_scalar).await;
            assert_eqp_tree_valid(&rows, &format!("{ctx}: corr_scalar"));
            let details: Vec<&str> = rows.iter().map(|(_, _, d)| d.as_str()).collect();
            assert_eq!(
                details,
                vec![
                    "SCAN evidence",
                    "CORRELATED SCALAR SUBQUERY 1",
                    "SEARCH repos USING INDEX idx_repos_path (path=?)",
                ],
                "{ctx}: corr_scalar"
            );

            // 4. Correlated EXISTS with alias + TEXT PK autoindex + residual vs no residual.
            let alias_exists = "SELECT id FROM evidence e WHERE NOT EXISTS(SELECT 1 FROM repos_pk r WHERE r.path = e.repo)";
            let rows = frank_eqp_rows(&conn, alias_exists).await;
            assert_eqp_tree_valid(&rows, &format!("{ctx}: alias_exists"));
            let details: Vec<&str> = rows.iter().map(|(_, _, d)| d.as_str()).collect();
            assert_eq!(
                details,
                vec![
                    "SCAN e",
                    "CORRELATED SCALAR SUBQUERY 1",
                    "SEARCH r USING COVERING INDEX sqlite_autoindex_repos_pk_1 (path=?)",
                ],
                "{ctx}: alias_exists"
            );

            let residual_exists = "SELECT id FROM evidence WHERE EXISTS(SELECT 1 FROM repos3 WHERE repos3.path = evidence.repo AND repos3.n > 1)";
            let rows = frank_eqp_rows(&conn, residual_exists).await;
            assert_eqp_tree_valid(&rows, &format!("{ctx}: residual_exists"));
            let details: Vec<&str> = rows.iter().map(|(_, _, d)| d.as_str()).collect();
            assert_eq!(
                details,
                vec![
                    "SCAN evidence",
                    "CORRELATED SCALAR SUBQUERY 1",
                    "SEARCH repos3 USING INDEX repos3_path (path=?)",
                ],
                "{ctx}: residual must NOT be covering"
            );

            // 5. Rowid correlated EXISTS compiles on BOTH :memory: and file (no fallback!).
            let rowid_exists =
                "SELECT id FROM evidence WHERE EXISTS(SELECT 1 FROM repos3 WHERE repos3.rowid = evidence.n)";
            let rows = frank_eqp_rows(&conn, rowid_exists).await;
            assert_eqp_tree_valid(&rows, &format!("{ctx}: rowid_exists"));
            let details: Vec<&str> = rows.iter().map(|(_, _, d)| d.as_str()).collect();
            assert_eq!(
                details,
                vec![
                    "SCAN evidence",
                    "CORRELATED SCALAR SUBQUERY 1",
                    "SEARCH repos3 USING INTEGER PRIMARY KEY (rowid=?)",
                ],
                "{ctx}: rowid_exists"
            );

            // 6. Negative controls for seek eligibility: unindexed, collation mismatch,
            //    and affinity mismatch must truthfully report SCAN + fallback.
            for (name, sql, expected_inner) in [
                (
                    "unindexed",
                    "SELECT id FROM evidence WHERE EXISTS(SELECT 1 FROM notes WHERE notes.ref_id = evidence.id)",
                    "SCAN notes",
                ),
                (
                    "collation_mismatch",
                    "SELECT id FROM evidence WHERE NOT EXISTS(SELECT 1 FROM repos_nc WHERE evidence.repo = repos_nc.path)",
                    "SCAN repos_nc",
                ),
                (
                    "affinity_mismatch",
                    "SELECT id FROM evidence WHERE EXISTS(SELECT 1 FROM t_txt WHERE t_txt.s = evidence.n)",
                    "SCAN t_txt",
                ),
            ] {
                let rows = frank_eqp_rows(&conn, sql).await;
                assert_eqp_tree_valid(&rows, &format!("{ctx}: {name}"));
                let details: Vec<&str> = rows.iter().map(|(_, _, d)| d.as_str()).collect();
                assert_eq!(
                    details,
                    vec![
                        "SCAN evidence",
                        "CORRELATED SCALAR SUBQUERY 1 (fallback: correlated_exists_fallback)",
                        expected_inner,
                    ],
                    "{ctx}: {name}"
                );
            }

            // 7. Uncorrelated IN subquery (outer index seek, NOT bogus IPK seek) and
            //    correlated IN subquery (`CORRELATED LIST SUBQUERY 1`).
            let uncorr_in = "SELECT score FROM evidence WHERE repo IN (SELECT path FROM repos)";
            let rows = frank_eqp_rows(&conn, uncorr_in).await;
            assert_eqp_tree_valid(&rows, &format!("{ctx}: uncorr_in"));
            let details: Vec<&str> = rows.iter().map(|(_, _, d)| d.as_str()).collect();
            assert_eq!(
                details[0],
                "SEARCH evidence USING INDEX idx_ev_repo (repo=?)",
                "{ctx}: uncorr_in outer"
            );
            assert_eq!(details[1], "LIST SUBQUERY 1", "{ctx}: uncorr_in header");
            assert!(
                details[2].starts_with("SCAN repos"),
                "{ctx}: uncorr_in inner={details:?}"
            );

            let corr_in = "SELECT score FROM evidence WHERE id IN (SELECT ref_id FROM notes WHERE notes.ref_id = evidence.id)";
            let rows = frank_eqp_rows(&conn, corr_in).await;
            assert_eqp_tree_valid(&rows, &format!("{ctx}: corr_in"));
            let details: Vec<&str> = rows.iter().map(|(_, _, d)| d.as_str()).collect();
            assert_eq!(
                details,
                vec![
                    "SCAN evidence",
                    "CORRELATED LIST SUBQUERY 1 (fallback: correlated_in_subquery_fallback)",
                    "SCAN notes",
                ],
                "{ctx}: corr_in"
            );

            // 8. GROUP BY ... HAVING correlated subquery: outer covering walk, no `SCAN Int(...)` leak.
            let having_corr = "SELECT repo, count(*) FROM evidence GROUP BY repo HAVING count(*) > (SELECT count(*) FROM repos WHERE repos.path = evidence.repo)";
            let rows = frank_eqp_rows(&conn, having_corr).await;
            assert_eqp_tree_valid(&rows, &format!("{ctx}: having_corr"));
            let details: Vec<&str> = rows.iter().map(|(_, _, d)| d.as_str()).collect();
            assert!(
                !details.iter().any(|d| d.contains("Int(")),
                "{ctx}: leaked raw inline cursor into HAVING EQP: {details:?}"
            );
            assert_eq!(
                details,
                vec![
                    "SCAN evidence USING COVERING INDEX idx_ev_repo",
                    "CORRELATED SCALAR SUBQUERY 1 (fallback: correlated_scalar_subquery_fallback)",
                    "SEARCH repos USING COVERING INDEX idx_repos_path (path=?)",
                ],
                "{ctx}: having_corr"
            );

            // 9. 3-level nested correlated subquery (`notes` referencing grandparent `evidence`).
            let three_level = "SELECT id FROM evidence WHERE EXISTS(SELECT 1 FROM repos WHERE repos.path = evidence.repo AND EXISTS(SELECT 1 FROM notes WHERE notes.ref_id = evidence.id))";
            let rows = frank_eqp_rows(&conn, three_level).await;
            assert_eqp_tree_valid(&rows, &format!("{ctx}: three_level"));
            let details: Vec<&str> = rows.iter().map(|(_, _, d)| d.as_str()).collect();
            assert_eq!(rows.len(), 5, "{ctx}: three_level rows={rows:?}");
            assert_eq!(rows[0].1, 0);
            assert_eq!(rows[1].1, 0);
            assert_eq!(rows[2].1, rows[1].0);
            assert_eq!(rows[3].1, rows[1].0);
            assert_eq!(rows[4].1, rows[3].0);
            assert_eq!(
                details,
                vec![
                    "SCAN evidence",
                    "CORRELATED SCALAR SUBQUERY 2 (fallback: correlated_exists_fallback)",
                    "SEARCH repos USING COVERING INDEX idx_repos_path (path=?)",
                    "CORRELATED SCALAR SUBQUERY 1 (fallback: correlated_exists_fallback)",
                    "SCAN notes",
                ],
                "{ctx}: three_level"
            );

            // 10. UPDATE / DELETE with correlated EXISTS.
            let upd = "UPDATE evidence SET score = score + 1 WHERE EXISTS(SELECT 1 FROM repos WHERE repos.path = evidence.repo)";
            let rows = frank_eqp_rows(&conn, upd).await;
            assert_eqp_tree_valid(&rows, &format!("{ctx}: update"));
            let details: Vec<&str> = rows.iter().map(|(_, _, d)| d.as_str()).collect();
            assert_eq!(details[0], "SCAN evidence (UPDATE)", "{ctx}");
            assert_eq!(details[1], "CORRELATED SCALAR SUBQUERY 1", "{ctx}");
            assert_eq!(
                details[2],
                "SEARCH repos USING COVERING INDEX idx_repos_path (path=?)",
                "{ctx}"
            );

            let del = "DELETE FROM evidence WHERE NOT EXISTS(SELECT 1 FROM repos WHERE repos.path = evidence.repo)";
            let rows = frank_eqp_rows(&conn, del).await;
            assert_eqp_tree_valid(&rows, &format!("{ctx}: delete"));
            let details: Vec<&str> = rows.iter().map(|(_, _, d)| d.as_str()).collect();
            assert_eq!(details[0], "SCAN evidence (DELETE)", "{ctx}");
            assert_eq!(details[1], "CORRELATED SCALAR SUBQUERY 1", "{ctx}");
            assert_eq!(
                details[2],
                "SEARCH repos USING COVERING INDEX idx_repos_path (path=?)",
                "{ctx}"
            );

            // 11. Runtime truthfulness check: every query whose EQP reported NO
            //     fallback executes cleanly under strict mem-fallback rejection,
            //     both direct and prepared, matching live bundled rusqlite 3.54.1!
            let compiled_sqls = [
                gh432,
                corr_scalar,
                alias_exists,
                residual_exists,
                rowid_exists,
                uncorr_in,
            ];
            conn.set_reject_mem_fallback(true);
            conn.set_strict_mem_fallback_rejection(true);
            for prepared in [false, true] {
                for sql in compiled_sqls {
                    let mut frank = frank_rows_mode(&conn, sql, prepared).await;
                    let mut oracle = stock_rows(&stock, sql);
                    frank.sort();
                    oracle.sort();
                    assert_eq!(
                        frank, oracle,
                        "{label} (prepared={prepared}): compiled result rows of `{sql}`"
                    );
                }
            }
            conn.set_strict_mem_fallback_rejection(false);
            conn.set_reject_mem_fallback(false);

            // 12. Converse runtime truthfulness check: every query whose EQP
            //     reported `(fallback: ...)` actually routes through a mem
            //     fallback (rejected under strict mem-fallback rejection), and
            //     once allowed matches live bundled rusqlite 3.54.1 both direct
            //     and prepared.
            let fallback_sqls = [
                "SELECT id FROM evidence WHERE EXISTS(SELECT 1 FROM notes WHERE notes.ref_id = evidence.id)",
                "SELECT id FROM evidence WHERE NOT EXISTS(SELECT 1 FROM repos_nc WHERE evidence.repo = repos_nc.path)",
                "SELECT id FROM evidence WHERE EXISTS(SELECT 1 FROM t_txt WHERE t_txt.s = evidence.n)",
                corr_in,
                having_corr,
                three_level,
            ];
            conn.set_reject_mem_fallback(true);
            conn.set_strict_mem_fallback_rejection(true);
            for sql in fallback_sqls {
                assert!(
                    conn.query(sql).await.is_err(),
                    "{label}: EQP reported fallback for `{sql}`, so strict mem-fallback rejection must refuse it"
                );
            }
            conn.set_strict_mem_fallback_rejection(false);
            conn.set_reject_mem_fallback(false);

            for prepared in [false, true] {
                for sql in std::iter::once(siblings).chain(fallback_sqls) {
                    let mut frank = frank_rows_mode(&conn, sql, prepared).await;
                    let mut oracle = stock_rows(&stock, sql);
                    frank.sort();
                    oracle.sort();
                    assert_eq!(
                        frank, oracle,
                        "{label} (prepared={prepared}): fallback result rows of `{sql}`"
                    );
                }
            }

            conn.close().await.expect("fsqlite close");
        }
    });
}
