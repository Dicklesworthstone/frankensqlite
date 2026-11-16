#![recursion_limit = "512"]

use fsqlite_core::connection::Connection;
use fsqlite_types::SqliteValue;
use std::error::Error;

type TestResult<T = ()> = Result<T, Box<dyn Error>>;

async fn open_wal_db(path: &str) -> TestResult<Connection> {
    let conn = Connection::open(path).await?;
    conn.execute("PRAGMA journal_mode = WAL").await?;
    Ok(conn)
}

async fn setup_table(conn: &Connection) -> TestResult {
    conn.execute("CREATE TABLE t(pk INTEGER PRIMARY KEY, v TEXT)")
        .await?;
    Ok(())
}

async fn commit_insert(conn: &Connection, pk: i64, value: &str) -> TestResult {
    conn.execute("BEGIN").await?;
    conn.execute_with_params(
        "INSERT INTO t(pk, v) VALUES(?1, ?2)",
        &[
            SqliteValue::Integer(pk),
            SqliteValue::Text(value.to_owned().into()),
        ],
    )
    .await?;
    conn.execute("COMMIT").await?;
    Ok(())
}

async fn commit_update(conn: &Connection, pk: i64, value: &str) -> TestResult {
    conn.execute("BEGIN").await?;
    conn.execute_with_params(
        "UPDATE t SET v = ?1 WHERE pk = ?2",
        &[
            SqliteValue::Text(value.to_owned().into()),
            SqliteValue::Integer(pk),
        ],
    )
    .await?;
    conn.execute("COMMIT").await?;
    Ok(())
}

async fn commit_delete(conn: &Connection, pk: i64) -> TestResult {
    conn.execute("BEGIN").await?;
    conn.execute_with_params("DELETE FROM t WHERE pk = ?1", &[SqliteValue::Integer(pk)])
        .await?;
    conn.execute("COMMIT").await?;
    Ok(())
}

fn text_at(row: &fsqlite_core::connection::Row, column: usize) -> TestResult<String> {
    match &row.values()[column] {
        SqliteValue::Text(value) => Ok(value.to_string()),
        other => Err(format!("expected text column {column}, got {other:?}").into()),
    }
}

// Regression test for issue #72: prepared-statement plan cache was not
// invalidated at commit time, so a `SELECT ... WHERE pk = ?` fired on the
// same connection immediately after an UPDATE returned stale (pre-UPDATE)
// rows — or zero rows when the UPDATE shifted the row image on a path the
// cached plan no longer matched. Prior to the fix downstream crates
// (beads_rust) worked around this by wrapping re-reads in CTEs; with the
// commit-time `clear_compilation_reuse_caches` call the plain shape works.
#[test]
fn prepared_select_sees_update_from_same_connection_after_cache_warmup() -> TestResult {
    let mut outcome: TestResult = Ok(());
    asupersync::test_utils::run_test(|| async {
        outcome = async {
            let dir = tempfile::tempdir()?;
            let db_path = dir.path().join("same-conn-update-staleness.db");
            let db_path = db_path.to_string_lossy().into_owned();

            let conn = open_wal_db(&db_path).await?;
            setup_table(&conn).await?;
            commit_insert(&conn, 1, "before").await?;

            // Warm the prepared-statement / planner-directive caches with a read.
            let sql = "SELECT v FROM t WHERE pk = ?1";
            let warm_rows = conn
                .query_with_params(sql, &[SqliteValue::Integer(1)])
                .await?;
            assert_eq!(warm_rows.len(), 1);
            assert_eq!(text_at(&warm_rows[0], 0)?, "before");

            // Commit a write on the same handle.
            commit_update(&conn, 1, "after").await?;

            // The critical assertion: a plain `SELECT ... WHERE pk = ?` reissued on
            // the same handle must see the post-UPDATE value. Without
            // clear_compilation_reuse_caches at commit time this returned "before"
            // (or zero rows, depending on which cache was consulted).
            let rows = conn
                .query_with_params(sql, &[SqliteValue::Integer(1)])
                .await?;
            assert_eq!(rows.len(), 1, "post-UPDATE re-read must find the row");
            assert_eq!(text_at(&rows[0], 0)?, "after");

            // Also verify the prepared path explicitly.
            let stmt = conn.prepare(sql).await?;
            let prepared_rows = stmt.query_with_params(&[SqliteValue::Integer(1)]).await?;
            assert_eq!(prepared_rows.len(), 1);
            assert_eq!(text_at(&prepared_rows[0], 0)?, "after");

            Ok(())
        }
        .await;
    });
    outcome
}

// Regression test for the CTE-shaped same-connection variant documented in
// issue #72. beads_rust wrapped `get_issue_from_conn` in a CTE to escape the
// bare fast-path cache. After an UPDATE on the same handle, the CTE shape
// then returned zero rows while the bare path was correct — confirming the
// cache-invalidation bug could shift between query shapes. With commit-time
// cache clear, both shapes see post-UPDATE state.
#[test]
fn cte_wrapped_select_sees_update_from_same_connection_after_cache_warmup() -> TestResult {
    let mut outcome: TestResult = Ok(());
    asupersync::test_utils::run_test(|| async {
        outcome = async {
            let dir = tempfile::tempdir()?;
            let db_path = dir.path().join("same-conn-cte-update-staleness.db");
            let db_path = db_path.to_string_lossy().into_owned();

            let conn = open_wal_db(&db_path).await?;
            setup_table(&conn).await?;
            commit_insert(&conn, 1, "before").await?;

            let cte_sql = "WITH target(id_value) AS (SELECT ?1)
                   SELECT t.v FROM t, target WHERE t.pk = target.id_value";

            let warm_rows = conn
                .query_with_params(cte_sql, &[SqliteValue::Integer(1)])
                .await?;
            assert_eq!(warm_rows.len(), 1);
            assert_eq!(text_at(&warm_rows[0], 0)?, "before");

            commit_update(&conn, 1, "after").await?;

            let rows = conn
                .query_with_params(cte_sql, &[SqliteValue::Integer(1)])
                .await?;
            assert_eq!(
                rows.len(),
                1,
                "CTE-wrapped re-read must still find the row after commit-time cache clear"
            );
            assert_eq!(text_at(&rows[0], 0)?, "after");

            Ok(())
        }
        .await;
    });
    outcome
}

#[test]
fn prepared_select_sees_row_inserted_by_other_connection_after_cache_warmup() -> TestResult {
    let mut outcome: TestResult = Ok(());
    asupersync::test_utils::run_test(|| async {
        outcome = async {
            let dir = tempfile::tempdir()?;
            let db_path = dir.path().join("insert-staleness.db");
            let db_path = db_path.to_string_lossy().into_owned();

            let conn_a = open_wal_db(&db_path).await?;
            setup_table(&conn_a).await?;
            commit_insert(&conn_a, 1, "one").await?;

            let conn_b = open_wal_db(&db_path).await?;
            let sql = "SELECT pk, v FROM t WHERE pk = ?1";
            let stmt = conn_b.prepare(sql).await?;
            let warm_rows = stmt.query_with_params(&[SqliteValue::Integer(1)]).await?;
            assert_eq!(warm_rows.len(), 1);
            assert_eq!(text_at(&warm_rows[0], 1)?, "one");

            commit_insert(&conn_a, 99, "ninety-nine").await?;

            let rows = stmt.query_with_params(&[SqliteValue::Integer(99)]).await?;
            assert_eq!(rows.len(), 1);
            assert_eq!(text_at(&rows[0], 1)?, "ninety-nine");

            let reprepared = conn_b.prepare(sql).await?;
            let reprepared_rows = reprepared
                .query_with_params(&[SqliteValue::Integer(99)])
                .await?;
            assert_eq!(reprepared_rows.len(), 1);
            assert_eq!(text_at(&reprepared_rows[0], 1)?, "ninety-nine");
            Ok(())
        }
        .await;
    });
    outcome
}

#[test]
fn newly_prepared_select_sees_row_inserted_by_other_connection() -> TestResult {
    let mut outcome: TestResult = Ok(());
    asupersync::test_utils::run_test(|| async {
        outcome = async {
            let dir = tempfile::tempdir()?;
            let db_path = dir.path().join("new-prepare-insert-staleness.db");
            let db_path = db_path.to_string_lossy().into_owned();

            let conn_a = open_wal_db(&db_path).await?;
            setup_table(&conn_a).await?;
            commit_insert(&conn_a, 1, "one").await?;

            let conn_b = open_wal_db(&db_path).await?;
            let warm_rows = conn_b
                .query_with_params("SELECT v FROM t WHERE pk = ?1", &[SqliteValue::Integer(1)])
                .await?;
            assert_eq!(warm_rows.len(), 1);
            assert_eq!(text_at(&warm_rows[0], 0)?, "one");

            commit_insert(&conn_a, 2, "two").await?;

            let stmt = conn_b.prepare("SELECT v FROM t WHERE pk = ?1").await?;
            let rows = stmt.query_with_params(&[SqliteValue::Integer(2)]).await?;
            assert_eq!(
                rows.len(),
                1,
                "prepare() must not mark a stale row image as current via lightweight metadata refresh"
            );
            assert_eq!(text_at(&rows[0], 0)?, "two");
            Ok(())
        }
        .await;
    });
    outcome
}

#[test]
fn prepared_select_sees_row_updated_by_other_connection_after_cache_warmup() -> TestResult {
    let mut outcome: TestResult = Ok(());
    asupersync::test_utils::run_test(|| async {
        outcome = async {
            let dir = tempfile::tempdir()?;
            let db_path = dir.path().join("update-staleness.db");
            let db_path = db_path.to_string_lossy().into_owned();

            let conn_a = open_wal_db(&db_path).await?;
            setup_table(&conn_a).await?;
            commit_insert(&conn_a, 1, "before").await?;

            let conn_b = open_wal_db(&db_path).await?;
            let sql = "SELECT v FROM t WHERE pk = ?1";
            let stmt = conn_b.prepare(sql).await?;
            let warm_rows = stmt.query_with_params(&[SqliteValue::Integer(1)]).await?;
            assert_eq!(warm_rows.len(), 1);
            assert_eq!(text_at(&warm_rows[0], 0)?, "before");

            commit_update(&conn_a, 1, "after").await?;

            let rows = stmt.query_with_params(&[SqliteValue::Integer(1)]).await?;
            assert_eq!(rows.len(), 1);
            assert_eq!(text_at(&rows[0], 0)?, "after");

            let reprepared = conn_b.prepare(sql).await?;
            let reprepared_rows = reprepared
                .query_with_params(&[SqliteValue::Integer(1)])
                .await?;
            assert_eq!(reprepared_rows.len(), 1);
            assert_eq!(text_at(&reprepared_rows[0], 0)?, "after");
            Ok(())
        }
        .await;
    });
    outcome
}

#[test]
fn prepared_select_stops_returning_row_deleted_by_other_connection_after_cache_warmup() -> TestResult
{
    let mut outcome: TestResult = Ok(());
    asupersync::test_utils::run_test(|| async {
        outcome = async {
            let dir = tempfile::tempdir()?;
            let db_path = dir.path().join("delete-staleness.db");
            let db_path = db_path.to_string_lossy().into_owned();

            let conn_a = open_wal_db(&db_path).await?;
            setup_table(&conn_a).await?;
            commit_insert(&conn_a, 1, "doomed").await?;

            let conn_b = open_wal_db(&db_path).await?;
            let sql = "SELECT v FROM t WHERE pk = ?1";
            let stmt = conn_b.prepare(sql).await?;
            let warm_rows = stmt.query_with_params(&[SqliteValue::Integer(1)]).await?;
            assert_eq!(warm_rows.len(), 1);
            assert_eq!(text_at(&warm_rows[0], 0)?, "doomed");

            commit_delete(&conn_a, 1).await?;

            let rows = stmt.query_with_params(&[SqliteValue::Integer(1)]).await?;
            assert!(
                rows.is_empty(),
                "deleted row must not be returned through stale cursor state: {rows:?}"
            );

            let reprepared = conn_b.prepare(sql).await?;
            let reprepared_rows = reprepared
                .query_with_params(&[SqliteValue::Integer(1)])
                .await?;
            assert!(
                reprepared_rows.is_empty(),
                "deleted row must not be returned from prepared cache: {reprepared_rows:?}"
            );
            Ok(())
        }
        .await;
    });
    outcome
}

// --- Memory-backend prepared statements warmed before the FIRST write ---
//
// Regression tests for bd-md5pf (root-caused from downstream
// hedge_fund_data_tool live persistence): on the MEMORY backend, a prepared
// statement whose shape is an index-seek equality on a TEXT column (TEXT
// PRIMARY KEY or UNIQUE index) that is first EXECUTED before the connection's
// first write transaction served frozen (pre-transaction) results forever
// after that transaction committed — and could not see the transaction's own
// inserts while it was open. A second write transaction on the connection
// flipped the statement fully current. Fresh statements, non-indexed shapes,
// scans, and other connections were all correct: the defect lived in the
// clean-memory prepared indexed-equality fast path, whose
// `PreparedIndexedEqualityCacheKey` is derived from `memdb_visible_commit_seq`
// plus the MemDatabase mirror's `undo_version()` — neither of which advanced
// on the first same-connection write commit, so the pre-transaction rowid
// cache and last-result memo still matched.

fn text_value(value: &str) -> SqliteValue {
    SqliteValue::Text(value.to_owned().into())
}

async fn open_memory_artifact_db() -> TestResult<Connection> {
    let conn = Connection::open(":memory:").await?;
    // The field trigger ran with foreign keys enforced; the FK-checked insert
    // into `runs` is what rehydrates the MemDatabase mirror from the ACTIVE
    // transaction (bound at the pre-transaction snapshot seq), which is the
    // condition that freezes pre-warmed statements after COMMIT. A plain
    // single-table insert abandons the mirror instead and heals on the next
    // read boundary, masking the defect.
    conn.execute("PRAGMA foreign_keys = ON").await?;
    conn.execute_batch(
        "CREATE TABLE artifact_refs (
             artifact_id TEXT PRIMARY KEY,
             content_hash TEXT NOT NULL,
             kind TEXT NOT NULL
         );
         CREATE UNIQUE INDEX idx_artifact_refs_content_hash
             ON artifact_refs(content_hash);
         CREATE TABLE runs (
             run_id TEXT PRIMARY KEY,
             artifact_id TEXT REFERENCES artifact_refs(artifact_id)
         );",
    )
    .await?;
    Ok(conn)
}

async fn insert_artifact(conn: &Connection, artifact_id: &str, content_hash: &str) -> TestResult {
    conn.execute_with_params(
        "INSERT INTO artifact_refs (artifact_id, content_hash, kind) VALUES (?1, ?2, ?3)",
        &[
            text_value(artifact_id),
            text_value(content_hash),
            text_value("filing_diff"),
        ],
    )
    .await?;
    conn.execute_with_params(
        "INSERT INTO runs (run_id, artifact_id) VALUES (?1, ?2)",
        &[text_value("run-00000"), text_value(artifact_id)],
    )
    .await?;
    Ok(())
}

const ARTIFACT_ID: &str = "art-00000";
const CONTENT_HASH: &str = "00000000000000000000000000000000000000000000feedbeef000000000000";

#[test]
fn memory_text_pk_statement_warmed_before_first_write_tx_sees_committed_row() -> TestResult {
    let mut outcome: TestResult = Ok(());
    asupersync::test_utils::run_test(|| async {
        outcome = async {
            let conn = open_memory_artifact_db().await?;

            let sql = "SELECT artifact_id, kind FROM artifact_refs WHERE artifact_id = ?1";
            let stmt = conn.prepare(sql).await?;
            // Warm the statement on the empty table so its result shape predates the
            // connection's first write transaction.
            let warm = stmt.query_with_params(&[text_value(ARTIFACT_ID)]).await?;
            assert!(warm.is_empty(), "warm run on empty table must be empty");

            conn.execute("BEGIN IMMEDIATE").await?;
            insert_artifact(&conn, ARTIFACT_ID, CONTENT_HASH).await?;
            conn.execute("COMMIT").await?;

            let rows = stmt.query_with_params(&[text_value(ARTIFACT_ID)]).await?;
            assert_eq!(
                rows.len(),
                1,
                "pre-warmed TEXT-PK equality statement must see the first write \
         transaction's committed row"
            );
            assert_eq!(text_at(&rows[0], 0)?, ARTIFACT_ID);

            // Repeat executions must stay current (the field failure was frozen
            // forever, not a single stale read).
            let again = stmt.query_with_params(&[text_value(ARTIFACT_ID)]).await?;
            assert_eq!(again.len(), 1, "re-execution must remain current");

            // A fresh statement was always correct; keep that pinned.
            let fresh = conn
                .query_with_params(sql, &[text_value(ARTIFACT_ID)])
                .await?;
            assert_eq!(fresh.len(), 1, "fresh statement must see the committed row");
            Ok(())
        }
        .await;
    });
    outcome
}

#[test]
fn memory_text_pk_statement_warmed_before_first_write_tx_sees_own_insert_inside_tx() -> TestResult {
    let mut outcome: TestResult = Ok(());
    asupersync::test_utils::run_test(|| async {
        outcome = async {
            let conn = open_memory_artifact_db().await?;

            let stmt = conn
                .prepare("SELECT artifact_id, kind FROM artifact_refs WHERE artifact_id = ?1")
                .await?;
            let warm = stmt.query_with_params(&[text_value(ARTIFACT_ID)]).await?;
            assert!(warm.is_empty(), "warm run on empty table must be empty");

            conn.execute("BEGIN IMMEDIATE").await?;
            let before_insert = stmt.query_with_params(&[text_value(ARTIFACT_ID)]).await?;
            assert!(
                before_insert.is_empty(),
                "row must not exist before the transaction's insert"
            );
            insert_artifact(&conn, ARTIFACT_ID, CONTENT_HASH).await?;
            let inside = stmt.query_with_params(&[text_value(ARTIFACT_ID)]).await?;
            assert_eq!(
                inside.len(),
                1,
                "pre-warmed statement must see the open transaction's own insert"
            );
            conn.execute("COMMIT").await?;

            let after = stmt.query_with_params(&[text_value(ARTIFACT_ID)]).await?;
            assert_eq!(
                after.len(),
                1,
                "pre-warmed statement must stay current after COMMIT"
            );
            Ok(())
        }
        .await;
    });
    outcome
}

#[test]
fn memory_unique_text_statement_warmed_before_first_write_tx_sees_committed_row() -> TestResult {
    let mut outcome: TestResult = Ok(());
    asupersync::test_utils::run_test(|| async {
        outcome = async {
            let conn = open_memory_artifact_db().await?;

            let stmt = conn
                .prepare(
                    "SELECT artifact_id, content_hash FROM artifact_refs WHERE content_hash = ?1",
                )
                .await?;
            let warm = stmt.query_with_params(&[text_value(CONTENT_HASH)]).await?;
            assert!(warm.is_empty(), "warm run on empty table must be empty");

            conn.execute("BEGIN IMMEDIATE").await?;
            insert_artifact(&conn, ARTIFACT_ID, CONTENT_HASH).await?;
            conn.execute("COMMIT").await?;

            let rows = stmt.query_with_params(&[text_value(CONTENT_HASH)]).await?;
            assert_eq!(
                rows.len(),
                1,
                "pre-warmed UNIQUE-index equality statement must see the first write \
         transaction's committed row"
            );
            assert_eq!(text_at(&rows[0], 0)?, ARTIFACT_ID);
            Ok(())
        }
        .await;
    });
    outcome
}

#[test]
fn stale_prepared_statement_reprepares_after_schema_change_from_other_connection() -> TestResult {
    let mut outcome: TestResult = Ok(());
    asupersync::test_utils::run_test(|| async {
        outcome = async {
            let dir = tempfile::tempdir()?;
            let db_path = dir.path().join("schema-staleness.db");
            let db_path = db_path.to_string_lossy().into_owned();

            let conn_a = open_wal_db(&db_path).await?;
            setup_table(&conn_a).await?;
            commit_insert(&conn_a, 1, "one").await?;

            let conn_b = open_wal_db(&db_path).await?;
            let stmt = conn_b.prepare("SELECT v FROM t WHERE pk = ?1").await?;
            let warm_rows = stmt.query_with_params(&[SqliteValue::Integer(1)]).await?;
            assert_eq!(warm_rows.len(), 1);
            assert_eq!(text_at(&warm_rows[0], 0)?, "one");

            conn_a
                .execute("ALTER TABLE t ADD COLUMN extra TEXT DEFAULT 'fresh'")
                .await?;

            // bd-9zuif: outside an explicit transaction the stale statement
            // re-prepares against the new schema, as sqlite3_prepare_v2 does.
            let reprepared_rows = stmt.query_with_params(&[SqliteValue::Integer(1)]).await?;
            assert_eq!(reprepared_rows.len(), 1);
            assert_eq!(text_at(&reprepared_rows[0], 0)?, "one");

            let fresh_stmt = conn_b.prepare("SELECT extra FROM t WHERE pk = ?1").await?;
            let fresh_rows = fresh_stmt
                .query_with_params(&[SqliteValue::Integer(1)])
                .await?;
            assert_eq!(fresh_rows.len(), 1);
            assert_eq!(text_at(&fresh_rows[0], 0)?, "fresh");
            Ok(())
        }
        .await;
    });
    outcome
}

async fn row_count_and_sum(conn: &Connection) -> TestResult<(i64, i64)> {
    let rows = conn.query("SELECT count(*), coalesce(sum(pk), 0) FROM t").await?;
    match rows[0].values() {
        [SqliteValue::Integer(count), SqliteValue::Integer(sum)] => Ok((*count, *sum)),
        other => Err(format!("expected two integers, got {other:?}").into()),
    }
}

/// bd-w4pr1.7: a begin skips re-deriving committed state only when the WAL-index
/// header its reader admission bound is the one this connection's last refresh
/// or own commit left behind. Its own autocommit inserts and a repeated read
/// take that path; a peer's commit between two statements moves the header, so
/// the next statement must see the peer's row (a begin that skipped anyway would
/// answer from its stale committed state and page cache).
#[test]
fn autocommit_statements_see_each_peer_commit_and_their_own() -> TestResult {
    let mut outcome: TestResult = Ok(());
    asupersync::test_utils::run_test(|| async {
        outcome = async {
            let dir = tempfile::tempdir()?;
            let db_path = dir.path().join("begin-signature.db");
            let db_path = db_path.to_string_lossy().into_owned();

            let conn_a = open_wal_db(&db_path).await?;
            setup_table(&conn_a).await?;
            let conn_b = open_wal_db(&db_path).await?;
            let mut expected = (0_i64, 0_i64);
            for round in 0..6_i64 {
                let own = round * 2;
                conn_a
                    .execute_with_params(
                        "INSERT INTO t(pk, v) VALUES(?1, 'a')",
                        &[SqliteValue::Integer(own)],
                    )
                    .await?;
                expected = (expected.0 + 1, expected.1 + own);
                assert_eq!(
                    row_count_and_sum(&conn_a).await?,
                    expected,
                    "own commit, round {round}"
                );
                assert_eq!(
                    row_count_and_sum(&conn_a).await?,
                    expected,
                    "an unchanged header keeps the answer, round {round}"
                );

                let peer = own + 1;
                conn_b
                    .execute_with_params(
                        "INSERT INTO t(pk, v) VALUES(?1, 'b')",
                        &[SqliteValue::Integer(peer)],
                    )
                    .await?;
                expected = (expected.0 + 1, expected.1 + peer);
                assert_eq!(
                    row_count_and_sum(&conn_a).await?,
                    expected,
                    "the peer's commit is seen by the next statement, round {round}"
                );
                assert_eq!(
                    row_count_and_sum(&conn_b).await?,
                    expected,
                    "peer view, round {round}"
                );
            }
            assert_eq!(expected, (12, 66));
            Ok(())
        }
        .await;
    });
    outcome
}

/// One step of [`shared_page_image_steps`]: which connection runs `sql`.
#[derive(Clone, Copy)]
enum Peer {
    A,
    B,
}

/// bd-05xlf: a page a transaction fetches from the WAL is the image the WAL
/// frame cache keeps of that committed frame, shared rather than copied.
/// These steps read a table and an index through it, let a peer commit newer
/// frames of the same pages while a reader holds the old images, write
/// through a shared image and roll it back, and reset the WAL (the peer's
/// TRUNCATE checkpoint) so the next commits reuse its frame indexes for other
/// pages. Every answer must equal stock SQLite's on the same steps: a reader
/// handed a newer frame by page number, a cache that kept a rolled-back write,
/// or a frame cache that served an old generation's frame answers differently.
fn shared_page_image_steps() -> Vec<(Peer, &'static str)> {
    vec![
        // Warm-up reads put the pages in A's caches.
        (Peer::A, "SELECT v FROM t WHERE pk = 5"),
        (Peer::A, "SELECT pk FROM t WHERE v = 'x5'"),
        (Peer::A, "SELECT count(*), sum(length(v)) FROM t"),
        // A's transaction reads the shared images; B commits newer ones.
        (Peer::A, "BEGIN"),
        (Peer::A, "SELECT v FROM t WHERE pk = 5"),
        (Peer::A, "SELECT pk FROM t WHERE v = 'x5'"),
        (Peer::B, "UPDATE t SET v = 'y5' WHERE pk = 5"),
        (
            Peer::B,
            "UPDATE t SET v = 'y' || pk WHERE pk BETWEEN 200 AND 210",
        ),
        (Peer::A, "SELECT v FROM t WHERE pk = 5"),
        (Peer::A, "SELECT pk FROM t WHERE v = 'x5'"),
        (Peer::A, "SELECT pk FROM t WHERE v = 'y5'"),
        (
            Peer::A,
            "SELECT count(*), sum(length(v)) FROM t WHERE v LIKE 'y%'",
        ),
        (Peer::A, "COMMIT"),
        // The next statements see B's commits.
        (Peer::A, "SELECT v FROM t WHERE pk = 5"),
        (Peer::A, "SELECT pk FROM t WHERE v = 'y5'"),
        (
            Peer::A,
            "SELECT count(*), sum(length(v)) FROM t WHERE v LIKE 'y%'",
        ),
        // A writes through images it shares with its caches, then rolls back.
        (Peer::A, "BEGIN"),
        (Peer::A, "SELECT v FROM t WHERE pk = 5"),
        (Peer::A, "UPDATE t SET v = 'z5' WHERE pk = 5"),
        (
            Peer::A,
            "UPDATE t SET v = 'z' || pk WHERE pk BETWEEN 200 AND 205",
        ),
        (Peer::A, "SELECT v FROM t WHERE pk = 5"),
        (Peer::A, "ROLLBACK"),
        (Peer::A, "SELECT v FROM t WHERE pk = 5"),
        (Peer::A, "SELECT pk FROM t WHERE v = 'z5'"),
        (
            Peer::A,
            "SELECT count(*), sum(length(v)) FROM t WHERE v LIKE 'z%'",
        ),
        (Peer::B, "SELECT v FROM t WHERE pk = 5"),
        // ...and commits, which B sees.
        (Peer::A, "UPDATE t SET v = 'w5' WHERE pk = 5"),
        (Peer::B, "SELECT v FROM t WHERE pk = 5"),
        (Peer::B, "SELECT pk FROM t WHERE v = 'w5'"),
        (Peer::A, "SELECT pk FROM t WHERE v = 'y5'"),
        (Peer::A, "SELECT count(*), sum(pk), sum(length(v)) FROM t"),
        // B resets the WAL; its next commits reuse the frame indexes A's
        // frame cache holds images of, for other pages and newer images.
        (Peer::B, "PRAGMA wal_checkpoint(TRUNCATE)"),
        (
            Peer::B,
            "UPDATE t SET v = 'q' || pk WHERE pk BETWEEN 300 AND 320",
        ),
        (Peer::B, "UPDATE t SET v = 'r5' WHERE pk = 5"),
        (Peer::A, "SELECT v FROM t WHERE pk = 5"),
        (Peer::A, "SELECT pk FROM t WHERE v = 'r5'"),
        (Peer::A, "SELECT pk FROM t WHERE v = 'w5'"),
        (Peer::A, "SELECT count(*), sum(pk) FROM t WHERE v LIKE 'q%'"),
        (Peer::A, "SELECT count(*), sum(pk), sum(length(v)) FROM t"),
        (Peer::B, "PRAGMA integrity_check"),
    ]
}

/// Whether a step's rows are compared with stock's. A checkpoint's row
/// counts frames, which the two engines lay out differently.
fn step_is_compared(sql: &str) -> bool {
    sql.starts_with("SELECT") || sql == "PRAGMA integrity_check"
}

fn stock_answer(conn: &rusqlite::Connection, sql: &str) -> Vec<Vec<String>> {
    let mut stmt = conn.prepare(sql).expect("stock prepare");
    let columns = stmt.column_count();
    let rows = stmt
        .query_map([], |row| {
            Ok((0..columns)
                .map(|i| match row.get::<_, rusqlite::types::Value>(i) {
                    Ok(rusqlite::types::Value::Integer(n)) => n.to_string(),
                    Ok(rusqlite::types::Value::Text(s)) => s,
                    Ok(rusqlite::types::Value::Null) => "NULL".to_owned(),
                    Ok(other) => format!("{other:?}"),
                    Err(error) => format!("<ERR {error}>"),
                })
                .collect())
        })
        .expect("stock query");
    rows.map(|row| row.expect("stock row")).collect()
}

fn frank_answer(rows: &[fsqlite_core::connection::Row]) -> Vec<Vec<String>> {
    rows.iter()
        .map(|row| {
            row.values()
                .iter()
                .map(|value| match value {
                    SqliteValue::Integer(n) => n.to_string(),
                    SqliteValue::Text(s) => s.to_string(),
                    SqliteValue::Null => "NULL".to_owned(),
                    other => format!("{other:?}"),
                })
                .collect()
        })
        .collect()
}

/// Rows of the shared-image table: enough 200-byte rows for several table
/// leaves and index pages.
const SHARED_IMAGE_ROWS: i64 = 400;
const SHARED_IMAGE_SCHEMA: [&str; 2] = [
    "CREATE TABLE t(pk INTEGER PRIMARY KEY, v TEXT, pad TEXT)",
    "CREATE INDEX t_v ON t(v)",
];

/// The stock answers of [`shared_page_image_steps`] on a WAL file, optionally
/// checkpointed (TRUNCATE) after the load.
fn stock_shared_image_answers(path: &std::path::Path, checkpoint: bool) -> Vec<Vec<Vec<String>>> {
    let a = rusqlite::Connection::open(path).expect("stock open a");
    a.execute_batch("PRAGMA journal_mode = WAL;")
        .expect("stock wal");
    for statement in SHARED_IMAGE_SCHEMA {
        a.execute_batch(statement).expect("stock schema");
    }
    {
        let mut insert = a
            .prepare("INSERT INTO t(pk, v, pad) VALUES (?1, ?2, ?3)")
            .expect("stock insert");
        for pk in 1..=SHARED_IMAGE_ROWS {
            insert
                .execute(rusqlite::params![pk, format!("x{pk}"), "p".repeat(200)])
                .expect("stock row");
        }
    }
    if checkpoint {
        a.execute_batch("PRAGMA wal_checkpoint(TRUNCATE);")
            .expect("stock checkpoint");
    }
    let b = rusqlite::Connection::open(path).expect("stock open b");
    shared_page_image_steps()
        .into_iter()
        .map(|(peer, sql)| {
            let conn = match peer {
                Peer::A => &a,
                Peer::B => &b,
            };
            if step_is_compared(sql) {
                stock_answer(conn, sql)
            } else {
                conn.execute_batch(sql).expect("stock statement");
                Vec::new()
            }
        })
        .collect()
}

fn run_shared_page_image_steps(checkpoint: bool) -> TestResult {
    let dir = tempfile::tempdir()?;
    let stock = stock_shared_image_answers(&dir.path().join("stock.db"), checkpoint);
    let mut outcome: TestResult = Ok(());
    asupersync::test_utils::run_test(|| async {
        outcome = async {
            let db_path = dir.path().join("shared-image.db");
            let db_path = db_path.to_string_lossy().into_owned();
            let conn_a = open_wal_db(&db_path).await?;
            for statement in SHARED_IMAGE_SCHEMA {
                conn_a.execute(statement).await?;
            }
            for pk in 1..=SHARED_IMAGE_ROWS {
                conn_a
                    .execute_with_params(
                        "INSERT INTO t(pk, v, pad) VALUES (?1, ?2, ?3)",
                        &[
                            SqliteValue::Integer(pk),
                            SqliteValue::Text(format!("x{pk}").into()),
                            SqliteValue::Text("p".repeat(200).into()),
                        ],
                    )
                    .await?;
            }
            if checkpoint {
                conn_a.query("PRAGMA wal_checkpoint(TRUNCATE)").await?;
            }
            let conn_b = open_wal_db(&db_path).await?;
            for (step, ((peer, sql), expected)) in shared_page_image_steps()
                .into_iter()
                .zip(&stock)
                .enumerate()
            {
                let conn = match peer {
                    Peer::A => &conn_a,
                    Peer::B => &conn_b,
                };
                let answer = if sql.starts_with("SELECT") || sql.starts_with("PRAGMA") {
                    let rows = conn.query(sql).await?;
                    if step_is_compared(sql) {
                        frank_answer(&rows)
                    } else {
                        Vec::new()
                    }
                } else {
                    conn.execute(sql).await?;
                    Vec::new()
                };
                assert_eq!(
                    &answer, expected,
                    "step {step} ({sql}), checkpointed {checkpoint}: fsqlite differs from stock"
                );
            }
            Ok(())
        }
        .await;
    });
    outcome
}

#[test]
fn transactions_keep_the_shared_page_images_they_fetched_from_the_wal() -> TestResult {
    run_shared_page_image_steps(false)
}

#[test]
fn transactions_keep_the_shared_page_images_they_fetched_from_the_main_file() -> TestResult {
    run_shared_page_image_steps(true)
}
