//! Keepers for three `:memory:` / attached-database bugs found by the
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
//! - RELEASE of the savepoint that implicitly began a transaction, and the
//!   public `commit_transaction()`, committed main but left an enrolled attached
//!   database's transaction open, so its writes vanished at the next ROLLBACK.
//! - `INSERT ... SELECT` into a missing table said `internal error: table not
//!   found` instead of `no such table`.
//!
//! GH#503 additionally covers a dropped index reused by an overflow chain,
//! followed by another schema allocation with autocommit retention disabled.

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

/// RELEASE of the savepoint that began the transaction commits attached
/// participants too: a later `BEGIN; ROLLBACK` must not discard their rows.
/// Same for the public `commit_transaction()`. Both attached kinds.
#[test]
fn implicit_release_and_commit_transaction_commit_attached_participants() {
    asupersync::test_utils::run_test(|| async {
        let dir = tempfile::tempdir().expect("tempdir");
        let file = dir.path().join("aux.db");
        for target in [":memory:".to_owned(), file.to_string_lossy().into_owned()] {
            let conn = Connection::open(":memory:").await.expect("open");
            conn.execute("CREATE TABLE m(x)").await.expect("main table");
            conn.execute(&format!("ATTACH '{target}' AS aux"))
                .await
                .expect("attach");
            conn.execute("CREATE TABLE aux.t(id INTEGER PRIMARY KEY, a)")
                .await
                .expect("aux table");

            conn.execute("SAVEPOINT s").await.expect("savepoint");
            conn.execute("INSERT INTO aux.t VALUES (16, 1)")
                .await
                .expect("aux insert");
            conn.execute("INSERT INTO m VALUES (1)")
                .await
                .expect("main insert");
            conn.execute("RELEASE s").await.expect("release");
            conn.execute("BEGIN").await.expect("begin");
            conn.execute("ROLLBACK").await.expect("rollback");
            assert_eq!(
                one_int(&conn, "SELECT count(*) FROM aux.t").await,
                1,
                "{target}: RELEASE-as-COMMIT lost the attached row"
            );

            conn.begin_transaction().await.expect("begin api");
            conn.execute("INSERT INTO aux.t VALUES (17, 1)")
                .await
                .expect("aux insert");
            conn.commit_transaction().await.expect("commit api");
            conn.execute("BEGIN").await.expect("begin");
            conn.execute("ROLLBACK").await.expect("rollback");
            assert_eq!(
                render(
                    &conn
                        .query("SELECT id FROM aux.t ORDER BY id")
                        .await
                        .expect("rows")
                ),
                vec![vec!["16".to_owned()], vec!["17".to_owned()]],
                "{target}: commit_transaction() lost the attached row"
            );

            // Nested savepoints with a partial rollback still match stock.
            conn.execute("SAVEPOINT a").await.expect("savepoint a");
            conn.execute("INSERT INTO m VALUES (2)")
                .await
                .expect("m insert");
            conn.execute("SAVEPOINT b").await.expect("savepoint b");
            conn.execute("INSERT INTO aux.t VALUES (18, 1)")
                .await
                .expect("aux insert");
            conn.execute("RELEASE b").await.expect("release b");
            conn.execute("ROLLBACK TO a").await.expect("rollback to a");
            conn.execute("INSERT INTO aux.t VALUES (19, 1)")
                .await
                .expect("aux insert");
            conn.execute("RELEASE a").await.expect("release a");
            conn.execute("BEGIN").await.expect("begin");
            conn.execute("ROLLBACK").await.expect("rollback");
            assert_eq!(
                render(
                    &conn
                        .query("SELECT id FROM aux.t ORDER BY id")
                        .await
                        .expect("rows")
                ),
                vec![
                    vec!["16".to_owned()],
                    vec!["17".to_owned()],
                    vec!["19".to_owned()]
                ],
                "{target}: nested savepoints"
            );
            assert_eq!(
                one_int(&conn, "SELECT count(*) FROM m").await,
                1,
                "{target}"
            );
        }
    });
}

const GH503_CREATE_T: &str = "CREATE TABLE t (id INTEGER PRIMARY KEY, a INTEGER, b BLOB)";
const GH503_CREATE_INDEX: &str = "CREATE INDEX ta ON t(a)";
const GH503_CREATE_U: &str = "CREATE TABLE u (k TEXT PRIMARY KEY, v INTEGER)";

async fn gh503_open_pair(
    file_backed: bool,
    dir: &std::path::Path,
) -> (Connection, rusqlite::Connection) {
    let target = if file_backed {
        dir.join("fsqlite.db").to_string_lossy().into_owned()
    } else {
        ":memory:".to_owned()
    };
    let conn = Connection::open(&target).await.expect("GH#503 open");
    let stock = if file_backed {
        rusqlite::Connection::open(dir.join("stock.db")).expect("GH#503 stock file")
    } else {
        rusqlite::Connection::open_in_memory().expect("GH#503 stock memory")
    };
    stock
        .execute_batch("PRAGMA page_size = 4096")
        .expect("GH#503 stock page size");
    (conn, stock)
}

async fn gh503_create_index_then_set_retention(
    conn: &Connection,
    stock: &rusqlite::Connection,
    retain: bool,
) {
    // Preserve the report's ordering: changing retention before either DDL
    // statement exercises a different cached/retained-transaction history.
    run_both(conn, stock, GH503_CREATE_T).await;
    run_both(conn, stock, GH503_CREATE_INDEX).await;
    conn.execute(if retain {
        "PRAGMA fsqlite.autocommit_retain = ON"
    } else {
        "PRAGMA fsqlite.autocommit_retain = OFF"
    })
    .await
    .expect("GH#503 set retention");
}

fn gh503_stock_int(stock: &rusqlite::Connection, sql: &str) -> i64 {
    stock
        .query_row(sql, [], |row| row.get(0))
        .unwrap_or_else(|error| panic!("GH#503 stock {sql}: {error}"))
}

fn gh503_pattern(len: usize, salt: u8) -> Vec<u8> {
    // Adjacent overflow pages have different contents, so exchanging two
    // same-sized pages cannot pass a length-only or repeated-byte assertion.
    (0..len)
        .map(|offset| {
            u8::try_from((offset * 37 + (offset / 4092) * 17 + usize::from(salt)) % 256)
                .expect("pattern byte")
        })
        .collect()
}

async fn gh503_insert_blob(conn: &Connection, stock: &rusqlite::Connection, blob: &[u8]) {
    let sql = "INSERT INTO t (id, a, b) VALUES (1, 100, ?1)";
    assert_eq!(
        conn.execute_with_params(sql, &[SqliteValue::Blob(blob.into())])
            .await
            .expect("GH#503 insert bound blob"),
        1
    );
    assert_eq!(
        stock
            .execute(sql, rusqlite::params![blob])
            .expect("GH#503 stock insert bound blob"),
        1
    );
}

fn gh503_assert_blob(actual: &[u8], expected: &[u8], context: &str) {
    assert_eq!(actual.len(), expected.len(), "{context}: BLOB length");
    let mismatch = actual
        .iter()
        .zip(expected)
        .position(|(actual, expected)| actual != expected);
    assert!(
        mismatch.is_none(),
        "{context}: BLOB differs at byte {mismatch:?}"
    );
}

async fn gh503_assert_contents(
    conn: &Connection,
    stock: &rusqlite::Connection,
    expected_blob: Option<&[u8]>,
    context: &str,
) {
    let sql = "SELECT id, a, b FROM t ORDER BY id";
    let ours = conn
        .query(sql)
        .await
        .unwrap_or_else(|error| panic!("{context}: fsqlite rows: {error}"));
    let mut statement = stock.prepare(sql).expect("GH#503 stock prepare rows");
    let theirs = statement
        .query_map([], |row| {
            Ok((
                row.get::<_, i64>(0)?,
                row.get::<_, i64>(1)?,
                row.get::<_, Vec<u8>>(2)?,
            ))
        })
        .expect("GH#503 stock query rows")
        .collect::<rusqlite::Result<Vec<_>>>()
        .expect("GH#503 stock rows");
    let expected_count = usize::from(expected_blob.is_some());
    assert_eq!(ours.len(), expected_count, "{context}: fsqlite row count");
    assert_eq!(theirs.len(), expected_count, "{context}: stock row count");
    if let Some(expected) = expected_blob {
        let [
            SqliteValue::Integer(id),
            SqliteValue::Integer(a),
            SqliteValue::Blob(blob),
        ] = ours[0].values()
        else {
            panic!("{context}: fsqlite row must contain INTEGER, INTEGER, BLOB");
        };
        assert_eq!((*id, *a), (1, 100), "{context}: fsqlite row values");
        assert_eq!((theirs[0].0, theirs[0].1), (1, 100), "{context}: stock row");
        gh503_assert_blob(blob, expected, &format!("{context}: fsqlite"));
        gh503_assert_blob(&theirs[0].2, expected, &format!("{context}: stock"));
    }

    let schema = "SELECT type, name, tbl_name FROM sqlite_master ORDER BY name";
    assert_eq!(
        render(&conn.query(schema).await.expect("GH#503 schema")),
        stock_rows(stock, schema),
        "{context}: schema"
    );
    assert_integrity_ok(conn, context).await;
    assert_eq!(
        stock_rows(stock, "PRAGMA integrity_check"),
        vec![vec!["ok".to_owned()]],
        "{context}: stock integrity"
    );
}

async fn gh503_assert_published_state(
    conn: &Connection,
    stock: &rusqlite::Connection,
    expected_blob: Option<&[u8]>,
    expected_overflow_pages: i64,
    context: &str,
) {
    gh503_assert_contents(conn, stock, expected_blob, context).await;
    assert_eq!(one_int(conn, "PRAGMA page_size").await, 4096, "{context}");
    assert_eq!(
        gh503_stock_int(stock, "PRAGMA page_size"),
        4096,
        "{context}"
    );

    let roots_sql = "SELECT rootpage FROM sqlite_master WHERE rootpage > 0 ORDER BY rootpage";
    let ours = conn.query(roots_sql).await.expect("GH#503 roots");
    let ours: Vec<i64> = ours
        .iter()
        .map(|row| match row.get(0) {
            Some(SqliteValue::Integer(root)) => *root,
            other => panic!("{context}: non-integer root {other:?}"),
        })
        .collect();
    let mut statement = stock.prepare(roots_sql).expect("GH#503 stock roots");
    let theirs = statement
        .query_map([], |row| row.get::<_, i64>(0))
        .expect("GH#503 stock query roots")
        .collect::<rusqlite::Result<Vec<_>>>()
        .expect("GH#503 stock collect roots");

    for (engine, roots, pages, free) in [
        (
            "fsqlite",
            ours,
            one_int(conn, "PRAGMA page_count").await,
            one_int(conn, "PRAGMA freelist_count").await,
        ),
        (
            "stock",
            theirs,
            gh503_stock_int(stock, "PRAGMA page_count"),
            gh503_stock_int(stock, "PRAGMA freelist_count"),
        ),
    ] {
        assert!(free >= 0, "{context}: {engine} negative freelist count");
        assert!(
            roots.iter().all(|&root| root > 1 && root <= pages),
            "{context}: {engine} root outside pages 2..={pages}: {roots:?}"
        );
        assert!(
            roots.windows(2).all(|pair| pair[0] != pair[1]),
            "{context}: {engine} shared root page: {roots:?}"
        );
        // The schema fits on page 1; t has at most one row, and every other
        // B-tree is empty. Account for every page independently of the
        // integrity walk, including the dropped index's former root. The
        // fixture's overflow counts come from SQLite's documented record and
        // cell layout, not FrankenSQLite's record/overflow implementation.
        assert_eq!(
            pages,
            1 + i64::try_from(roots.len()).expect("root count") + expected_overflow_pages + free,
            "{context}: {engine} page ownership: roots={roots:?}, \
             overflow={expected_overflow_pages}, free={free}"
        );
    }
}

/// A=100 occupies one record byte, and the BLOB serial-type varint grows at
/// length 8186. At 4096-byte pages the record's first three overflow thresholds
/// therefore fall between BLOB lengths 4055/4056, 8147/8148, and 12238/12239.
#[test]
fn gh503_overflow_boundaries_preserve_contents_and_page_ownership() {
    asupersync::test_utils::run_test(|| async {
        let cases = [
            (4054, 0),
            (4055, 0),
            (4056, 1),
            (8146, 1),
            (8147, 1),
            (8148, 2),
            (12237, 2),
            (12238, 2),
            (12239, 3),
            (86016, 21),
        ];
        for file_backed in [false, true] {
            for retain in [false, true] {
                for (len, overflow_pages) in cases {
                    let dir = tempfile::tempdir().expect("GH#503 boundary tempdir");
                    let (conn, stock) = gh503_open_pair(file_backed, dir.path()).await;
                    let context = format!(
                        "GH#503 boundary: bytes={len}, file={file_backed}, retain={retain}"
                    );
                    gh503_create_index_then_set_retention(&conn, &stock, retain).await;
                    gh503_assert_published_state(&conn, &stock, None, 0, &context).await;
                    if !file_backed {
                        assert_eq!(
                            one_int(
                                &conn,
                                "SELECT rootpage FROM sqlite_master WHERE name = 'ta'"
                            )
                            .await,
                            3,
                            "{context}: index starts at page 3"
                        );
                    }
                    let free_before_drop = one_int(&conn, "PRAGMA freelist_count").await;
                    run_both(&conn, &stock, "DROP INDEX ta").await;
                    gh503_assert_published_state(
                        &conn,
                        &stock,
                        None,
                        0,
                        &format!("{context}: drop index"),
                    )
                    .await;
                    assert!(
                        one_int(&conn, "PRAGMA freelist_count").await > free_before_drop,
                        "{context}: dropping the index must free its root"
                    );
                    if !file_backed {
                        assert_eq!(
                            one_int(&conn, "PRAGMA freelist_count").await,
                            1,
                            "{context}"
                        );
                    }
                    let blob = gh503_pattern(len, 29);
                    gh503_insert_blob(&conn, &stock, &blob).await;
                    gh503_assert_published_state(
                        &conn,
                        &stock,
                        Some(&blob),
                        overflow_pages,
                        &format!("{context}: insert"),
                    )
                    .await;
                    if !file_backed {
                        assert_eq!(
                            one_int(&conn, "PRAGMA freelist_count").await,
                            i64::from(overflow_pages == 0),
                            "{context}: an overflow chain must consume the freed index page"
                        );
                        assert_eq!(
                            one_int(&conn, "PRAGMA page_count").await,
                            (2 + overflow_pages).max(3),
                            "{context}: reuse page 3 before extending the database"
                        );
                    }
                    run_both(&conn, &stock, GH503_CREATE_U).await;
                    gh503_assert_published_state(
                        &conn,
                        &stock,
                        Some(&blob),
                        overflow_pages,
                        &format!("{context}: create table and primary-key index"),
                    )
                    .await;
                }
            }
        }
    });
}

#[test]
fn gh503_repeated_overflow_and_schema_allocations_reuse_pages_without_aliasing() {
    asupersync::test_utils::run_test(|| async {
        for file_backed in [false, true] {
            for retain in [false, true] {
                let dir = tempfile::tempdir().expect("GH#503 churn tempdir");
                let (conn, stock) = gh503_open_pair(file_backed, dir.path()).await;
                gh503_create_index_then_set_retention(&conn, &stock, retain).await;
                run_both(&conn, &stock, "DROP INDEX ta").await;
                let mut previous: Option<Vec<u8>> = None;
                let mut page_counts = Vec::new();
                for round in 0_u8..6 {
                    let context =
                        format!("GH#503 churn: round={round}, file={file_backed}, retain={retain}");
                    if let Some(blob) = &previous {
                        run_both(&conn, &stock, "DROP TABLE u").await;
                        gh503_assert_published_state(
                            &conn,
                            &stock,
                            Some(blob),
                            21,
                            &format!("{context}: drop table and index"),
                        )
                        .await;
                        run_both(&conn, &stock, "DELETE FROM t WHERE id = 1").await;
                        gh503_assert_published_state(
                            &conn,
                            &stock,
                            None,
                            0,
                            &format!("{context}: free overflow chain"),
                        )
                        .await;
                        for sql in [GH503_CREATE_INDEX, "DROP INDEX ta"] {
                            run_both(&conn, &stock, sql).await;
                            gh503_assert_published_state(&conn, &stock, None, 0, &context).await;
                        }
                    }
                    let blob = gh503_pattern(86016, round);
                    gh503_insert_blob(&conn, &stock, &blob).await;
                    gh503_assert_published_state(
                        &conn,
                        &stock,
                        Some(&blob),
                        21,
                        &format!("{context}: insert"),
                    )
                    .await;
                    run_both(&conn, &stock, GH503_CREATE_U).await;
                    gh503_assert_published_state(&conn, &stock, Some(&blob), 21, &context).await;
                    page_counts.push(one_int(&conn, "PRAGMA page_count").await);
                    previous = Some(blob);
                }
                if !file_backed {
                    // Private-memory allocation has no concurrent EOF lease:
                    // identical committed churn must stabilize after warm-up.
                    // File-backed lease schedules may retain free slack, whose
                    // ownership is checked after every transition above.
                    assert!(
                        page_counts[3..]
                            .iter()
                            .all(|&pages| pages == page_counts[3]),
                        "GH#503 churn: retain={retain}: \
                         continued growth after warm-up: {page_counts:?}"
                    );
                }
            }
        }
    });
}

#[test]
fn gh503_transaction_and_savepoint_rollback_preserve_overflow_ownership() {
    asupersync::test_utils::run_test(|| async {
        for file_backed in [false, true] {
            for retain in [false, true] {
                let dir = tempfile::tempdir().expect("GH#503 rollback tempdir");
                let (conn, stock) = gh503_open_pair(file_backed, dir.path()).await;
                let context = format!("GH#503 rollback: file={file_backed}, retain={retain}");
                gh503_create_index_then_set_retention(&conn, &stock, retain).await;
                run_both(&conn, &stock, "DROP INDEX ta").await;
                gh503_assert_published_state(&conn, &stock, None, 0, &context).await;

                let original = gh503_pattern(86016, 53);
                run_both(&conn, &stock, "BEGIN").await;
                gh503_insert_blob(&conn, &stock, &original).await;
                // The active transaction's page-1 freelist header is a deferred
                // commit-time projection (GH#113). The integrity walker checks
                // live page ownership here; exact header accounting follows
                // ROLLBACK/COMMIT instead of treating that stale header as live.
                gh503_assert_contents(&conn, &stock, Some(&original), &context).await;
                run_both(&conn, &stock, GH503_CREATE_U).await;
                gh503_assert_contents(&conn, &stock, Some(&original), &context).await;
                run_both(&conn, &stock, "ROLLBACK").await;
                gh503_assert_published_state(
                    &conn,
                    &stock,
                    None,
                    0,
                    &format!("{context}: full rollback"),
                )
                .await;

                gh503_insert_blob(&conn, &stock, &original).await;
                gh503_assert_published_state(&conn, &stock, Some(&original), 21, &context).await;
                run_both(&conn, &stock, GH503_CREATE_U).await;
                gh503_assert_published_state(&conn, &stock, Some(&original), 21, &context).await;

                run_both(&conn, &stock, "BEGIN").await;
                run_both(&conn, &stock, "SAVEPOINT replace_blob").await;
                run_both(&conn, &stock, "DELETE FROM t WHERE id = 1").await;
                gh503_assert_contents(&conn, &stock, None, &context).await;
                let replacement = gh503_pattern(86016, 197);
                gh503_insert_blob(&conn, &stock, &replacement).await;
                gh503_assert_contents(&conn, &stock, Some(&replacement), &context).await;
                run_both(&conn, &stock, "CREATE TABLE rolled_back (x)").await;
                gh503_assert_contents(&conn, &stock, Some(&replacement), &context).await;
                run_both(&conn, &stock, "ROLLBACK TO replace_blob").await;
                gh503_assert_contents(&conn, &stock, Some(&original), &context).await;
                run_both(&conn, &stock, "RELEASE replace_blob").await;
                gh503_assert_contents(&conn, &stock, Some(&original), &context).await;
                run_both(&conn, &stock, "COMMIT").await;
                gh503_assert_published_state(
                    &conn,
                    &stock,
                    Some(&original),
                    21,
                    &format!("{context}: savepoint rollback then commit"),
                )
                .await;
                // Reallocate after both rollback kinds; discarded reservations
                // must not let a new root overwrite the restored overflow chain.
                for sql in ["CREATE TABLE rolled_back (x)", "DROP TABLE rolled_back"] {
                    run_both(&conn, &stock, sql).await;
                    gh503_assert_published_state(&conn, &stock, Some(&original), 21, &context)
                        .await;
                }
            }
        }
    });
}

/// `INSERT ... SELECT` into a missing table reports `no such table`, like
/// every other INSERT form and like stock.
#[test]
fn insert_select_into_missing_table_says_no_such_table() {
    asupersync::test_utils::run_test(|| async {
        let conn = Connection::open(":memory:").await.expect("open");
        conn.execute("CREATE TABLE t1(id INTEGER PRIMARY KEY, a)")
            .await
            .expect("create");
        conn.execute("INSERT INTO t1 VALUES (1, 2)")
            .await
            .expect("insert");
        for sql in [
            "INSERT INTO extra SELECT id, a FROM t1",
            "INSERT OR REPLACE INTO extra SELECT id, a FROM t1",
            "INSERT INTO extra VALUES (1, 2)",
        ] {
            let error = conn.execute(sql).await.expect_err(sql).to_string();
            assert_eq!(error, "no such table: extra", "{sql}");
        }
    });
}
