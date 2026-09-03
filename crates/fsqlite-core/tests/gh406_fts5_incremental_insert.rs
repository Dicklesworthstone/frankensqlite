//! GH #406 keepers — a content-backed FTS5 INSERT persists O(rows in the
//! statement), not O(rows in the table).
//!
//! Before the fix every `INSERT` into a content-backed `fts5` table re-encoded
//! the whole in-memory inverted index and rewrote every
//! `_data`/`_idx`/`_config`/`_content`/`_docsize` shadow row, so loading N rows
//! in k statements cost O(N^2/k). These keepers pin the three properties that
//! together prove the append is incremental and still correct:
//!
//! 1. structural — a leaf written by an earlier statement is still byte-identical
//!    after later statements (a full re-encode would have rewritten it), the
//!    structure grows a segment per statement, and stock C SQLite reads the
//!    multi-segment image;
//! 2. scaling — the per-row cost of the last quarter of a bulk load is not
//!    materially worse than the first quarter (quadratic growth would make it
//!    ~7x);
//! 3. transactional — `ROLLBACK TO SAVEPOINT`, nested savepoints and a whole
//!    `ROLLBACK` still restore exactly, in memory and after reopen.

use fsqlite_core::connection::{Connection, Row};
use fsqlite_types::value::SqliteValue;
use std::time::Instant;

fn integers(rows: &[Row]) -> Vec<i64> {
    rows.iter()
        .map(|row| match row.values()[0] {
            SqliteValue::Integer(n) => n,
            ref other => panic!("expected an integer, got {other:?}"),
        })
        .collect()
}

fn scalar_i64(rows: &[Row]) -> i64 {
    match rows[0].values()[0] {
        SqliteValue::Integer(n) => n,
        ref other => panic!("expected an integer, got {other:?}"),
    }
}

/// Every `%_data` row that is a segment leaf (i.e. not the averages or
/// structure metadata row), as `(id, block bytes)`.
async fn segment_leaves(conn: &Connection) -> Vec<(i64, Vec<u8>)> {
    let rows = conn
        .query("SELECT id, block FROM t_data ORDER BY id;")
        .await
        .unwrap();
    rows.iter()
        .filter_map(|row| {
            let id = match row.values()[0] {
                SqliteValue::Integer(n) => n,
                ref other => panic!("expected id integer, got {other:?}"),
            };
            // rowid 1 is `averages`, rowid 10 is `structure`; everything else is
            // a segment leaf (or a dlidx page).
            if id == 1 || id == 10 {
                return None;
            }
            let block = match row.values()[1] {
                SqliteValue::Blob(ref bytes) => bytes.to_vec(),
                ref other => panic!("expected block blob, got {other:?}"),
            };
            Some((id, block))
        })
        .collect()
}

#[test]
fn gh406_content_backed_insert_appends_a_segment_instead_of_rewriting_the_shadow() {
    asupersync::test_utils::run_test(|| async {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("gh406_append.db");
        let db_str = db_path.to_string_lossy().into_owned();

        let conn = Connection::open(&db_str).await.unwrap();
        conn.execute("CREATE VIRTUAL TABLE t USING fts5(body);")
            .await
            .unwrap();

        // Statement 1 lays down the first segment (and the config/idx/docsize
        // shadows) through the full encode.
        conn.execute(
            "INSERT INTO t(rowid, body) VALUES \
             (1, 'alpha common one'), (2, 'alpha common two');",
        )
        .await
        .unwrap();
        let after_first = segment_leaves(&conn).await;
        assert!(
            !after_first.is_empty(),
            "the first statement must persist at least one segment leaf"
        );

        // Statements 2 and 3 must APPEND: their rows land in fresh segments and
        // the first statement's leaves are never touched again. `automerge`
        // defaults to 4, so three level-0 segments do not trigger a merge.
        conn.execute(
            "INSERT INTO t(rowid, body) VALUES \
             (3, 'beta common three'), (4, 'beta common four');",
        )
        .await
        .unwrap();
        conn.execute(
            "INSERT INTO t(rowid, body) VALUES \
             (5, 'gamma common five'), (6, 'gamma common six');",
        )
        .await
        .unwrap();

        let after_third = segment_leaves(&conn).await;
        for (id, block) in &after_first {
            let found = after_third
                .iter()
                .find(|(later_id, _)| later_id == id)
                .unwrap_or_else(|| panic!("segment leaf {id} disappeared after later inserts"));
            assert_eq!(
                &found.1, block,
                "GH#406: leaf {id} was rewritten by a later INSERT — the persist \
                 path is still re-encoding the whole shadow table"
            );
        }
        assert!(
            after_third.len() > after_first.len(),
            "later statements must append new leaves ({} -> {})",
            after_first.len(),
            after_third.len()
        );

        // Every row is still reachable, in memory and from a stock reader.
        let matched = conn
            .query("SELECT rowid FROM t WHERE t MATCH 'common' ORDER BY rowid;")
            .await
            .unwrap();
        assert_eq!(integers(&matched), vec![1, 2, 3, 4, 5, 6]);
        let beta = conn
            .query("SELECT rowid FROM t WHERE t MATCH 'beta' ORDER BY rowid;")
            .await
            .unwrap();
        assert_eq!(integers(&beta), vec![3, 4]);

        // `_content` must carry every row exactly once (the incremental append
        // writes only the statement's rows, so a double-write or a missing row
        // would show up here).
        let content_count = conn.query("SELECT count(*) FROM t_content;").await.unwrap();
        assert_eq!(scalar_i64(&content_count), 6, "one _content row per insert");
        conn.close().await.unwrap();

        let stock = rusqlite::Connection::open(&db_path).unwrap();
        let integrity: String = stock
            .query_row("PRAGMA integrity_check;", [], |r| r.get(0))
            .unwrap();
        assert_eq!(integrity, "ok", "stock integrity_check on the appended image");
        let stock_matched: Vec<i64> = stock
            .prepare("SELECT rowid FROM t WHERE t MATCH 'common' ORDER BY rowid")
            .unwrap()
            .query_map([], |r| r.get(0))
            .unwrap()
            .collect::<std::result::Result<Vec<_>, _>>()
            .unwrap();
        assert_eq!(
            stock_matched,
            vec![1, 2, 3, 4, 5, 6],
            "stock reads every appended segment"
        );
        let stock_gamma: Vec<i64> = stock
            .prepare("SELECT rowid FROM t WHERE t MATCH 'gamma' ORDER BY rowid")
            .unwrap()
            .query_map([], |r| r.get(0))
            .unwrap()
            .collect::<std::result::Result<Vec<_>, _>>()
            .unwrap();
        assert_eq!(stock_gamma, vec![5, 6], "stock reads the newest segment");
    });
}

#[test]
fn gh406_content_backed_bulk_load_is_subquadratic() {
    asupersync::test_utils::run_test(|| async {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("gh406_scaling.db");
        let db_str = db_path.to_string_lossy().into_owned();

        let conn = Connection::open(&db_str).await.unwrap();
        conn.execute("CREATE VIRTUAL TABLE t USING fts5(body);")
            .await
            .unwrap();

        const QUARTERS: usize = 4;
        const STATEMENTS_PER_QUARTER: usize = 10;
        const ROWS_PER_STATEMENT: usize = 100;

        conn.execute("BEGIN;").await.unwrap();
        let mut rowid = 0_i64;
        let mut quarter_times = Vec::with_capacity(QUARTERS);
        for _ in 0..QUARTERS {
            let started = Instant::now();
            for _ in 0..STATEMENTS_PER_QUARTER {
                let mut sql = String::from("INSERT INTO t(rowid, body) VALUES ");
                for row in 0..ROWS_PER_STATEMENT {
                    rowid += 1;
                    if row > 0 {
                        sql.push(',');
                    }
                    // A shared term plus a per-row term: the shared term keeps
                    // every doclist long (the shape that made the full re-encode
                    // expensive), the unique term keeps the vocabulary growing.
                    sql.push_str(&format!("({rowid}, 'common corpus token{rowid}')"));
                }
                sql.push(';');
                conn.execute(&sql).await.unwrap();
            }
            quarter_times.push(started.elapsed());
        }
        conn.execute("COMMIT;").await.unwrap();

        let total_rows = i64::try_from(QUARTERS * STATEMENTS_PER_QUARTER * ROWS_PER_STATEMENT)
            .expect("row count fits in i64");
        let count = conn.query("SELECT count(*) FROM t;").await.unwrap();
        assert_eq!(scalar_i64(&count), total_rows, "every row was inserted");

        // Under the pre-fix persist path each statement re-encoded the whole
        // shadow table, so the last quarter costs ~7x the first. An incremental
        // append is O(rows in the statement) plus bounded merge work, so the
        // ratio stays near 1; 3.0 leaves generous headroom for timer noise and
        // for the merge that lands in a later quarter.
        let first = quarter_times[0].as_secs_f64().max(1e-6);
        let last = quarter_times[QUARTERS - 1].as_secs_f64();
        let ratio = last / first;
        eprintln!(
            "GH#406 bulk load: {total_rows} rows in {} statements of {ROWS_PER_STATEMENT}; \
             quarters (s) = {:?}; last/first = {ratio:.2}",
            QUARTERS * STATEMENTS_PER_QUARTER,
            quarter_times
                .iter()
                .map(|d| format!("{:.3}", d.as_secs_f64()))
                .collect::<Vec<_>>()
        );
        assert!(
            ratio < 3.0,
            "GH#406: bulk INSERT cost is growing with table size — quarter timings {quarter_times:?}, \
             last/first = {ratio:.2}"
        );

        // The load is still correct end to end.
        let matched = conn
            .query("SELECT count(*) FROM t WHERE t MATCH 'common';")
            .await
            .unwrap();
        assert_eq!(scalar_i64(&matched), total_rows, "every row matches 'common'");
        let unique = conn
            .query("SELECT rowid FROM t WHERE t MATCH 'token7' ORDER BY rowid;")
            .await
            .unwrap();
        assert_eq!(integers(&unique), vec![7], "per-row terms survive the append");
        conn.close().await.unwrap();

        let stock = rusqlite::Connection::open(&db_path).unwrap();
        let integrity: String = stock
            .query_row("PRAGMA integrity_check;", [], |r| r.get(0))
            .unwrap();
        assert_eq!(integrity, "ok", "stock integrity_check after the bulk load");
        let stock_count: i64 = stock
            .query_row("SELECT count(*) FROM t WHERE t MATCH 'common'", [], |r| {
                r.get(0)
            })
            .unwrap();
        assert_eq!(
            stock_count, total_rows,
            "stock reads every row of the incrementally appended index"
        );
        let stock_unique: i64 = stock
            .query_row("SELECT rowid FROM t WHERE t MATCH 'token1234'", [], |r| {
                r.get(0)
            })
            .unwrap();
        assert_eq!(stock_unique, 1234, "stock seeks a term in a later segment");
    });
}

#[test]
fn gh406_incremental_append_still_rolls_back_to_savepoint() {
    asupersync::test_utils::run_test(|| async {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("gh406_savepoint.db");
        let db_str = db_path.to_string_lossy().into_owned();

        let conn = Connection::open(&db_str).await.unwrap();
        conn.execute("CREATE VIRTUAL TABLE t USING fts5(body);")
            .await
            .unwrap();

        conn.execute("BEGIN;").await.unwrap();
        // Enough statements that the keepers below run against appended
        // segments rather than the first-statement full encode.
        for id in 1..=6_i64 {
            conn.execute(&format!(
                "INSERT INTO t(rowid, body) VALUES ({id}, 'kept common row{id}');"
            ))
            .await
            .unwrap();
        }

        conn.execute("SAVEPOINT outer;").await.unwrap();
        for id in 7..=9_i64 {
            conn.execute(&format!(
                "INSERT INTO t(rowid, body) VALUES ({id}, 'dropped common row{id}');"
            ))
            .await
            .unwrap();
        }
        conn.execute("SAVEPOINT inner;").await.unwrap();
        conn.execute("INSERT INTO t(rowid, body) VALUES (10, 'inner common row10');")
            .await
            .unwrap();

        // Nested rollback: only row 10 goes away.
        conn.execute("ROLLBACK TO inner;").await.unwrap();
        let after_inner = conn
            .query("SELECT rowid FROM t WHERE t MATCH 'common' ORDER BY rowid;")
            .await
            .unwrap();
        assert_eq!(
            integers(&after_inner),
            vec![1, 2, 3, 4, 5, 6, 7, 8, 9],
            "ROLLBACK TO inner drops only the inner statement"
        );

        // Outer rollback: rows 7-9 go away too, and the savepoint survives.
        conn.execute("ROLLBACK TO outer;").await.unwrap();
        let after_outer = conn
            .query("SELECT rowid FROM t WHERE t MATCH 'common' ORDER BY rowid;")
            .await
            .unwrap();
        assert_eq!(
            integers(&after_outer),
            vec![1, 2, 3, 4, 5, 6],
            "ROLLBACK TO outer restores the pre-savepoint corpus exactly"
        );
        assert!(
            conn.query("SELECT rowid FROM t WHERE t MATCH 'dropped';")
                .await
                .unwrap()
                .is_empty(),
            "the rolled-back rows' postings are gone"
        );

        // Inserting after the rollback must reuse the freed rowids cleanly.
        conn.execute("INSERT INTO t(rowid, body) VALUES (7, 'redone common row7');")
            .await
            .unwrap();
        conn.execute("RELEASE outer;").await.unwrap();
        conn.execute("COMMIT;").await.unwrap();

        let committed = conn
            .query("SELECT rowid FROM t WHERE t MATCH 'common' ORDER BY rowid;")
            .await
            .unwrap();
        assert_eq!(integers(&committed), vec![1, 2, 3, 4, 5, 6, 7]);
        let content_count = conn.query("SELECT count(*) FROM t_content;").await.unwrap();
        assert_eq!(
            scalar_i64(&content_count),
            7,
            "_content holds exactly the committed rows"
        );
        conn.close().await.unwrap();

        // Reopen: the committed image round-trips, including the redone row.
        let reopened = Connection::open(&db_str).await.unwrap();
        let after_reopen = reopened
            .query("SELECT rowid FROM t WHERE t MATCH 'common' ORDER BY rowid;")
            .await
            .unwrap();
        assert_eq!(
            integers(&after_reopen),
            vec![1, 2, 3, 4, 5, 6, 7],
            "the committed corpus survives reopen"
        );
        let redone = reopened
            .query("SELECT rowid FROM t WHERE t MATCH 'redone';")
            .await
            .unwrap();
        assert_eq!(integers(&redone), vec![7]);
        assert!(
            reopened
                .query("SELECT rowid FROM t WHERE t MATCH 'dropped';")
                .await
                .unwrap()
                .is_empty(),
            "no rolled-back posting reached the durable index"
        );
        let body = reopened
            .query("SELECT body FROM t WHERE rowid = 7;")
            .await
            .unwrap();
        assert_eq!(
            body[0].values()[0],
            SqliteValue::Text("redone common row7".into()),
            "_content round-trips the post-rollback value"
        );
        reopened.close().await.unwrap();

        let stock = rusqlite::Connection::open(&db_path).unwrap();
        let integrity: String = stock
            .query_row("PRAGMA integrity_check;", [], |r| r.get(0))
            .unwrap();
        assert_eq!(integrity, "ok");
        let stock_matched: Vec<i64> = stock
            .prepare("SELECT rowid FROM t WHERE t MATCH 'common' ORDER BY rowid")
            .unwrap()
            .query_map([], |r| r.get(0))
            .unwrap()
            .collect::<std::result::Result<Vec<_>, _>>()
            .unwrap();
        assert_eq!(stock_matched, vec![1, 2, 3, 4, 5, 6, 7]);
    });
}

#[test]
fn gh406_incremental_append_rolls_back_a_whole_transaction() {
    asupersync::test_utils::run_test(|| async {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("gh406_rollback.db");
        let db_str = db_path.to_string_lossy().into_owned();

        let conn = Connection::open(&db_str).await.unwrap();
        conn.execute("CREATE VIRTUAL TABLE t USING fts5(body);")
            .await
            .unwrap();
        conn.execute("INSERT INTO t(rowid, body) VALUES (1, 'kept common row1');")
            .await
            .unwrap();

        conn.execute("BEGIN;").await.unwrap();
        for id in 2..=40_i64 {
            conn.execute(&format!(
                "INSERT INTO t(rowid, body) VALUES ({id}, 'dropped common row{id}');"
            ))
            .await
            .unwrap();
        }
        conn.execute("ROLLBACK;").await.unwrap();

        let after = conn
            .query("SELECT rowid FROM t WHERE t MATCH 'common' ORDER BY rowid;")
            .await
            .unwrap();
        assert_eq!(
            integers(&after),
            vec![1],
            "ROLLBACK after many appended segments restores the pre-transaction corpus"
        );
        let content_count = conn.query("SELECT count(*) FROM t_content;").await.unwrap();
        assert_eq!(scalar_i64(&content_count), 1, "_content rolled back too");
        conn.close().await.unwrap();

        let reopened = Connection::open(&db_str).await.unwrap();
        let after_reopen = reopened
            .query("SELECT rowid FROM t WHERE t MATCH 'common' ORDER BY rowid;")
            .await
            .unwrap();
        assert_eq!(integers(&after_reopen), vec![1]);
        reopened.close().await.unwrap();

        let stock = rusqlite::Connection::open(&db_path).unwrap();
        let integrity: String = stock
            .query_row("PRAGMA integrity_check;", [], |r| r.get(0))
            .unwrap();
        assert_eq!(integrity, "ok", "stock integrity_check after the rollback");
    });
}

#[test]
fn gh406_insert_or_replace_still_retires_the_replaced_postings() {
    asupersync::test_utils::run_test(|| async {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("gh406_replace.db");
        let db_str = db_path.to_string_lossy().into_owned();

        let conn = Connection::open(&db_str).await.unwrap();
        conn.execute("CREATE VIRTUAL TABLE t USING fts5(body);")
            .await
            .unwrap();
        for id in 1..=4_i64 {
            conn.execute(&format!(
                "INSERT INTO t(rowid, body) VALUES ({id}, 'original common row{id}');"
            ))
            .await
            .unwrap();
        }

        // `INSERT OR REPLACE` re-uses a rowid whose postings already live in an
        // older segment, so it takes the full re-encode rather than the append.
        conn.execute("INSERT OR REPLACE INTO t(rowid, body) VALUES (2, 'replaced common row2');")
            .await
            .unwrap();

        let original = conn
            .query("SELECT rowid FROM t WHERE t MATCH 'original' ORDER BY rowid;")
            .await
            .unwrap();
        assert_eq!(
            integers(&original),
            vec![1, 3, 4],
            "the replaced row's old postings are retired"
        );
        let replaced = conn
            .query("SELECT rowid FROM t WHERE t MATCH 'replaced';")
            .await
            .unwrap();
        assert_eq!(integers(&replaced), vec![2]);

        // A plain INSERT after the replace goes back to the append path.
        conn.execute("INSERT INTO t(rowid, body) VALUES (5, 'appended common row5');")
            .await
            .unwrap();
        let all = conn
            .query("SELECT rowid FROM t WHERE t MATCH 'common' ORDER BY rowid;")
            .await
            .unwrap();
        assert_eq!(integers(&all), vec![1, 2, 3, 4, 5]);
        conn.close().await.unwrap();

        let stock = rusqlite::Connection::open(&db_path).unwrap();
        let integrity: String = stock
            .query_row("PRAGMA integrity_check;", [], |r| r.get(0))
            .unwrap();
        assert_eq!(integrity, "ok");
        let stock_original: Vec<i64> = stock
            .prepare("SELECT rowid FROM t WHERE t MATCH 'original' ORDER BY rowid")
            .unwrap()
            .query_map([], |r| r.get(0))
            .unwrap()
            .collect::<std::result::Result<Vec<_>, _>>()
            .unwrap();
        assert_eq!(
            stock_original,
            vec![1, 3, 4],
            "stock agrees the replaced posting is gone"
        );
    });
}
