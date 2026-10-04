//! Keepers for one `:memory:` bug found by the
//! `memory_mirror_snapshot_differential` review, all present before
//! fc1f6a537:
//!
//! - A private `:memory:` pager never reused its freelist (the
//!   `memory_db_bump_alloc` fast path skipped it). Freed pages (a DELETE's
//!   overflow chain, a dropped index) were never handed out again, and the
//!   unused tail of a concurrent-mode page lease went back to the freelist
//!   above db_size while `next_page` stayed past it, so the next allocation
//!   left "page N is never used" holes inside the grown database. Later reuse
//!   of such a page then corrupted data. A rollback that rewound `next_page`
//!   also left those above-db_size pages on the freelist, so once the freelist
//!   was used one page could be granted twice.

use fsqlite_core::connection::{Connection, Row};
use fsqlite_types::value::SqliteValue;

fn render(rows: &[Row]) -> Vec<Vec<String>> {
    rows.iter()
        .map(|row| {
            row.values()
                .iter()
                .map(|value| match value {
                    SqliteValue::Null => "null".to_owned(),
                    SqliteValue::Integer(i) => i.to_string(),
                    SqliteValue::Float(f) => f.to_string(),
                    SqliteValue::Text(t) => t.to_string(),
                    SqliteValue::Blob(b) => format!("blob:{}", b.len()),
                })
                .collect()
        })
        .collect()
}

fn stock_rows(stock: &rusqlite::Connection, sql: &str) -> Vec<Vec<String>> {
    let mut stmt = stock.prepare(sql).expect("stock prepare");
    let columns = stmt.column_count();
    stmt.query_map([], |row| {
        (0..columns)
            .map(|i| {
                Ok(match row.get_ref(i)? {
                    rusqlite::types::ValueRef::Null => "null".to_owned(),
                    rusqlite::types::ValueRef::Integer(i) => i.to_string(),
                    rusqlite::types::ValueRef::Real(f) => f.to_string(),
                    rusqlite::types::ValueRef::Text(t) => String::from_utf8_lossy(t).into_owned(),
                    rusqlite::types::ValueRef::Blob(b) => format!("blob:{}", b.len()),
                })
            })
            .collect::<rusqlite::Result<Vec<String>>>()
    })
    .expect("stock query")
    .collect::<rusqlite::Result<Vec<_>>>()
    .expect("stock rows")
}

async fn one_int(conn: &Connection, sql: &str) -> i64 {
    let rows = conn
        .query(sql)
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e}"));
    match rows.first().and_then(|row| row.get(0)) {
        Some(SqliteValue::Integer(i)) => *i,
        other => panic!("{sql}: expected one integer, got {other:?}"),
    }
}

async fn assert_integrity_ok(conn: &Connection, context: &str) {
    let rows = conn
        .query("PRAGMA integrity_check")
        .await
        .unwrap_or_else(|e| panic!("{context}: integrity_check: {e}"));
    assert_eq!(render(&rows), vec![vec!["ok".to_owned()]], "{context}");
}

async fn run_both(conn: &Connection, stock: &rusqlite::Connection, sql: &str) {
    conn.execute(sql)
        .await
        .unwrap_or_else(|e| panic!("fsqlite {sql}: {e}"));
    stock
        .execute_batch(sql)
        .unwrap_or_else(|e| panic!("stock {sql}: {e}"));
}

/// Grow `t` past one page, then insert in an explicit (concurrent-mode)
/// transaction whose splits take a page lease. The unused lease tail must not
/// become a hole: the next CREATE TABLE gets the page right after the end of
/// the database, and integrity_check stays clean.
#[test]
fn explicit_transaction_page_lease_tail_is_reused_not_leaked() {
    asupersync::test_utils::run_test(|| async {
        let conn = Connection::open(":memory:").await.expect("open");
        conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, a TEXT)")
            .await
            .expect("create");
        conn.execute(
            "WITH RECURSIVE c(i) AS (SELECT 1 UNION ALL SELECT i+1 FROM c WHERE i<40) \
             INSERT INTO t SELECT i, printf('%0100d', i) FROM c",
        )
        .await
        .expect("seed");
        conn.execute("BEGIN").await.expect("begin");
        conn.execute(
            "WITH RECURSIVE c(i) AS (SELECT 41 UNION ALL SELECT i+1 FROM c WHERE i<80) \
             INSERT INTO t SELECT i, printf('%0100d', i) FROM c",
        )
        .await
        .expect("txn insert");
        conn.execute("COMMIT").await.expect("commit");
        assert_integrity_ok(&conn, "after explicit-transaction splits").await;

        for name in ["x", "y", "z"] {
            let before = one_int(&conn, "PRAGMA page_count").await;
            conn.execute(&format!("CREATE TABLE {name}(a)"))
                .await
                .expect("create table");
            let root = one_int(
                &conn,
                &format!("SELECT rootpage FROM sqlite_master WHERE name = '{name}'"),
            )
            .await;
            let after = one_int(&conn, "PRAGMA page_count").await;
            assert_eq!(
                (root, after),
                (before + 1, before + 1),
                "CREATE TABLE {name} must take the page right after the database end \
                 (page_count was {before}); a larger root means skipped page numbers"
            );
            assert_integrity_ok(&conn, &format!("after CREATE TABLE {name}")).await;
        }
        assert_eq!(one_int(&conn, "SELECT count(*) FROM t").await, 80);
    });
}

/// Pages freed by deleting a row whose payload spilled to overflow pages, and
/// by dropping an index, are reused by later allocations as stock reuses them:
/// a CREATE INDEX that fits in the freed pages does not grow the database.
#[test]
fn freed_overflow_and_index_pages_are_reused_like_stock() {
    asupersync::test_utils::run_test(|| async {
        let conn = Connection::open(":memory:").await.expect("open");
        let stock = rusqlite::Connection::open_in_memory().expect("stock");
        for sql in [
            "CREATE TABLE t(id INTEGER PRIMARY KEY, a BLOB)",
            "INSERT INTO t VALUES (1, zeroblob(20000))",
            "INSERT INTO t VALUES (2, zeroblob(20000))",
            "INSERT INTO t VALUES (3, 'small')",
            "DELETE FROM t WHERE id = 1",
        ] {
            run_both(&conn, &stock, sql).await;
        }
        let stock_int = |sql: &str| -> i64 {
            stock
                .query_row(sql, [], |row| row.get(0))
                .unwrap_or_else(|e| panic!("stock {sql}: {e}"))
        };
        let ours_free = one_int(&conn, "PRAGMA freelist_count").await;
        assert!(ours_free > 0, "the overflow DELETE must free pages");
        assert_integrity_ok(&conn, "after overflow DELETE").await;

        // Stock's CREATE INDEX here fits entirely in the freed pages.
        let stock_free = stock_int("PRAGMA freelist_count");
        run_both(&conn, &stock, "CREATE INDEX ti ON t(a)").await;
        assert!(
            stock_int("PRAGMA freelist_count") < stock_free,
            "oracle: stock's CREATE INDEX reuses the freed pages"
        );
        let ours_free_after = one_int(&conn, "PRAGMA freelist_count").await;
        assert!(
            ours_free_after < ours_free,
            "CREATE INDEX must take pages from the freelist ({ours_free} free before, \
             {ours_free_after} after)"
        );
        assert_integrity_ok(&conn, "after CREATE INDEX").await;

        // Churn: drop and recreate indexes, delete and reinsert overflow rows,
        // in autocommit and in explicit transactions. Every round must stay
        // internally consistent, and the database must stop growing once the
        // churn is steady: pages freed in one round are reused by later ones.
        // (Within one explicit transaction, pages it freed itself stay
        // quarantined until commit by design, so a single round may grow past
        // stock; repeated rounds must not keep growing.)
        let mut sizes = Vec::new();
        for round in 0..12 {
            let explicit = round % 2 == 1;
            if explicit {
                run_both(&conn, &stock, "BEGIN").await;
            }
            for sql in [
                "DROP INDEX ti",
                &format!("INSERT INTO t VALUES ({}, zeroblob(15000))", 100 + round),
                &format!("DELETE FROM t WHERE id = {}", 99 + round),
                "CREATE INDEX ti ON t(a)",
            ] {
                run_both(&conn, &stock, sql).await;
            }
            if explicit {
                run_both(&conn, &stock, "COMMIT").await;
            }
            assert_integrity_ok(&conn, &format!("churn round {round}")).await;
            sizes.push(one_int(&conn, "PRAGMA page_count").await);
        }
        // Rounds 4..12 repeat rounds 0..4's shapes on a database that already
        // holds their freed pages, so they must fit without growing.
        assert_eq!(
            sizes[11], sizes[3],
            "the database kept growing under steady churn: page_count per round {sizes:?}"
        );
        let rows = "SELECT id, length(a) FROM t ORDER BY id";
        assert_eq!(
            render(&conn.query(rows).await.expect("rows")),
            stock_rows(&stock, rows)
        );
    });
}

/// `BEGIN; CREATE INDEX ...; COMMIT;` followed by a statement that fails after
/// parsing and more DDL: no root page may end up both on the freelist and in
/// sqlite_master.
#[test]
fn ddl_in_explicit_transaction_then_failed_statement_keeps_roots_off_freelist() {
    asupersync::test_utils::run_test(|| async {
        let conn = Connection::open(":memory:").await.expect("open");
        conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, a TEXT)")
            .await
            .expect("create");
        conn.execute(
            "WITH RECURSIVE c(i) AS (SELECT 1 UNION ALL SELECT i+1 FROM c WHERE i<300) \
             INSERT INTO t SELECT i, printf('%0200d', i) FROM c",
        )
        .await
        .expect("seed");
        for round in 0..4 {
            conn.execute("BEGIN").await.expect("begin");
            conn.execute(&format!("CREATE INDEX i{round} ON t(a)"))
                .await
                .expect("create index");
            conn.execute("COMMIT").await.expect("commit");
            for failing in [
                "INSERT INTO missing VALUES (1)",
                "INSERT INTO t VALUES (1, 'duplicate rowid')",
                "INSERT INTO t VALUES (1, 2, 3)",
            ] {
                assert!(conn.execute(failing).await.is_err(), "{failing} must fail");
            }
            conn.execute(&format!("CREATE TABLE extra{round}(x)"))
                .await
                .expect("create table");
            conn.execute(&format!("DROP INDEX i{round}"))
                .await
                .expect("drop index");
            assert_integrity_ok(&conn, &format!("round {round}")).await;
        }
        assert_eq!(
            one_int(&conn, "SELECT count(*) FROM t WHERE a > ''").await,
            300
        );
    });
}
