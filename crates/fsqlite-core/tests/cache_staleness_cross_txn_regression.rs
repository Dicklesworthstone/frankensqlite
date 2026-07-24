use fsqlite_core::connection::Connection;
use fsqlite_error::FrankenError;
use fsqlite_types::SqliteValue;
use std::error::Error;

type TestResult<T = ()> = Result<T, Box<dyn Error>>;

fn open_wal_db(path: &str) -> TestResult<Connection> {
    let conn = Connection::open(path)?;
    conn.execute("PRAGMA journal_mode = WAL")?;
    Ok(conn)
}

fn setup_table(conn: &Connection) -> TestResult {
    conn.execute("CREATE TABLE t(pk INTEGER PRIMARY KEY, v TEXT)")?;
    Ok(())
}

fn commit_insert(conn: &Connection, pk: i64, value: &str) -> TestResult {
    conn.execute("BEGIN")?;
    conn.execute_with_params(
        "INSERT INTO t(pk, v) VALUES(?1, ?2)",
        &[
            SqliteValue::Integer(pk),
            SqliteValue::Text(value.to_owned().into()),
        ],
    )?;
    conn.execute("COMMIT")?;
    Ok(())
}

fn commit_update(conn: &Connection, pk: i64, value: &str) -> TestResult {
    conn.execute("BEGIN")?;
    conn.execute_with_params(
        "UPDATE t SET v = ?1 WHERE pk = ?2",
        &[
            SqliteValue::Text(value.to_owned().into()),
            SqliteValue::Integer(pk),
        ],
    )?;
    conn.execute("COMMIT")?;
    Ok(())
}

fn commit_delete(conn: &Connection, pk: i64) -> TestResult {
    conn.execute("BEGIN")?;
    conn.execute_with_params("DELETE FROM t WHERE pk = ?1", &[SqliteValue::Integer(pk)])?;
    conn.execute("COMMIT")?;
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
    let dir = tempfile::tempdir()?;
    let db_path = dir.path().join("same-conn-update-staleness.db");
    let db_path = db_path.to_string_lossy().into_owned();

    let conn = open_wal_db(&db_path)?;
    setup_table(&conn)?;
    commit_insert(&conn, 1, "before")?;

    // Warm the prepared-statement / planner-directive caches with a read.
    let sql = "SELECT v FROM t WHERE pk = ?1";
    let warm_rows = conn.query_with_params(sql, &[SqliteValue::Integer(1)])?;
    assert_eq!(warm_rows.len(), 1);
    assert_eq!(text_at(&warm_rows[0], 0)?, "before");

    // Commit a write on the same handle.
    commit_update(&conn, 1, "after")?;

    // The critical assertion: a plain `SELECT ... WHERE pk = ?` reissued on
    // the same handle must see the post-UPDATE value. Without
    // clear_compilation_reuse_caches at commit time this returned "before"
    // (or zero rows, depending on which cache was consulted).
    let rows = conn.query_with_params(sql, &[SqliteValue::Integer(1)])?;
    assert_eq!(rows.len(), 1, "post-UPDATE re-read must find the row");
    assert_eq!(text_at(&rows[0], 0)?, "after");

    // Also verify the prepared path explicitly.
    let stmt = conn.prepare(sql)?;
    let prepared_rows = stmt.query_with_params(&[SqliteValue::Integer(1)])?;
    assert_eq!(prepared_rows.len(), 1);
    assert_eq!(text_at(&prepared_rows[0], 0)?, "after");

    Ok(())
}

// Regression test for the CTE-shaped same-connection variant documented in
// issue #72. beads_rust wrapped `get_issue_from_conn` in a CTE to escape the
// bare fast-path cache. After an UPDATE on the same handle, the CTE shape
// then returned zero rows while the bare path was correct — confirming the
// cache-invalidation bug could shift between query shapes. With commit-time
// cache clear, both shapes see post-UPDATE state.
#[test]
fn cte_wrapped_select_sees_update_from_same_connection_after_cache_warmup() -> TestResult {
    let dir = tempfile::tempdir()?;
    let db_path = dir.path().join("same-conn-cte-update-staleness.db");
    let db_path = db_path.to_string_lossy().into_owned();

    let conn = open_wal_db(&db_path)?;
    setup_table(&conn)?;
    commit_insert(&conn, 1, "before")?;

    let cte_sql = "WITH target(id_value) AS (SELECT ?1)
                   SELECT t.v FROM t, target WHERE t.pk = target.id_value";

    let warm_rows = conn.query_with_params(cte_sql, &[SqliteValue::Integer(1)])?;
    assert_eq!(warm_rows.len(), 1);
    assert_eq!(text_at(&warm_rows[0], 0)?, "before");

    commit_update(&conn, 1, "after")?;

    let rows = conn.query_with_params(cte_sql, &[SqliteValue::Integer(1)])?;
    assert_eq!(
        rows.len(),
        1,
        "CTE-wrapped re-read must still find the row after commit-time cache clear"
    );
    assert_eq!(text_at(&rows[0], 0)?, "after");

    Ok(())
}

#[test]
fn prepared_select_sees_row_inserted_by_other_connection_after_cache_warmup() -> TestResult {
    let dir = tempfile::tempdir()?;
    let db_path = dir.path().join("insert-staleness.db");
    let db_path = db_path.to_string_lossy().into_owned();

    let conn_a = open_wal_db(&db_path)?;
    setup_table(&conn_a)?;
    commit_insert(&conn_a, 1, "one")?;

    let conn_b = open_wal_db(&db_path)?;
    let sql = "SELECT pk, v FROM t WHERE pk = ?1";
    let stmt = conn_b.prepare(sql)?;
    let warm_rows = stmt.query_with_params(&[SqliteValue::Integer(1)])?;
    assert_eq!(warm_rows.len(), 1);
    assert_eq!(text_at(&warm_rows[0], 1)?, "one");

    commit_insert(&conn_a, 99, "ninety-nine")?;

    let rows = stmt.query_with_params(&[SqliteValue::Integer(99)])?;
    assert_eq!(rows.len(), 1);
    assert_eq!(text_at(&rows[0], 1)?, "ninety-nine");

    let reprepared = conn_b.prepare(sql)?;
    let reprepared_rows = reprepared.query_with_params(&[SqliteValue::Integer(99)])?;
    assert_eq!(reprepared_rows.len(), 1);
    assert_eq!(text_at(&reprepared_rows[0], 1)?, "ninety-nine");
    Ok(())
}

#[test]
fn newly_prepared_select_sees_row_inserted_by_other_connection() -> TestResult {
    let dir = tempfile::tempdir()?;
    let db_path = dir.path().join("new-prepare-insert-staleness.db");
    let db_path = db_path.to_string_lossy().into_owned();

    let conn_a = open_wal_db(&db_path)?;
    setup_table(&conn_a)?;
    commit_insert(&conn_a, 1, "one")?;

    let conn_b = open_wal_db(&db_path)?;
    let warm_rows =
        conn_b.query_with_params("SELECT v FROM t WHERE pk = ?1", &[SqliteValue::Integer(1)])?;
    assert_eq!(warm_rows.len(), 1);
    assert_eq!(text_at(&warm_rows[0], 0)?, "one");

    commit_insert(&conn_a, 2, "two")?;

    let stmt = conn_b.prepare("SELECT v FROM t WHERE pk = ?1")?;
    let rows = stmt.query_with_params(&[SqliteValue::Integer(2)])?;
    assert_eq!(
        rows.len(),
        1,
        "prepare() must not mark a stale row image as current via lightweight metadata refresh"
    );
    assert_eq!(text_at(&rows[0], 0)?, "two");
    Ok(())
}

#[test]
fn prepared_select_sees_row_updated_by_other_connection_after_cache_warmup() -> TestResult {
    let dir = tempfile::tempdir()?;
    let db_path = dir.path().join("update-staleness.db");
    let db_path = db_path.to_string_lossy().into_owned();

    let conn_a = open_wal_db(&db_path)?;
    setup_table(&conn_a)?;
    commit_insert(&conn_a, 1, "before")?;

    let conn_b = open_wal_db(&db_path)?;
    let sql = "SELECT v FROM t WHERE pk = ?1";
    let stmt = conn_b.prepare(sql)?;
    let warm_rows = stmt.query_with_params(&[SqliteValue::Integer(1)])?;
    assert_eq!(warm_rows.len(), 1);
    assert_eq!(text_at(&warm_rows[0], 0)?, "before");

    commit_update(&conn_a, 1, "after")?;

    let rows = stmt.query_with_params(&[SqliteValue::Integer(1)])?;
    assert_eq!(rows.len(), 1);
    assert_eq!(text_at(&rows[0], 0)?, "after");

    let reprepared = conn_b.prepare(sql)?;
    let reprepared_rows = reprepared.query_with_params(&[SqliteValue::Integer(1)])?;
    assert_eq!(reprepared_rows.len(), 1);
    assert_eq!(text_at(&reprepared_rows[0], 0)?, "after");
    Ok(())
}

#[test]
fn prepared_select_stops_returning_row_deleted_by_other_connection_after_cache_warmup() -> TestResult
{
    let dir = tempfile::tempdir()?;
    let db_path = dir.path().join("delete-staleness.db");
    let db_path = db_path.to_string_lossy().into_owned();

    let conn_a = open_wal_db(&db_path)?;
    setup_table(&conn_a)?;
    commit_insert(&conn_a, 1, "doomed")?;

    let conn_b = open_wal_db(&db_path)?;
    let sql = "SELECT v FROM t WHERE pk = ?1";
    let stmt = conn_b.prepare(sql)?;
    let warm_rows = stmt.query_with_params(&[SqliteValue::Integer(1)])?;
    assert_eq!(warm_rows.len(), 1);
    assert_eq!(text_at(&warm_rows[0], 0)?, "doomed");

    commit_delete(&conn_a, 1)?;

    let rows = stmt.query_with_params(&[SqliteValue::Integer(1)])?;
    assert!(
        rows.is_empty(),
        "deleted row must not be returned through stale cursor state: {rows:?}"
    );

    let reprepared = conn_b.prepare(sql)?;
    let reprepared_rows = reprepared.query_with_params(&[SqliteValue::Integer(1)])?;
    assert!(
        reprepared_rows.is_empty(),
        "deleted row must not be returned from prepared cache: {reprepared_rows:?}"
    );
    Ok(())
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

fn open_memory_artifact_db() -> TestResult<Connection> {
    let conn = Connection::open(":memory:")?;
    // The field trigger ran with foreign keys enforced; the FK-checked insert
    // into `runs` is what rehydrates the MemDatabase mirror from the ACTIVE
    // transaction (bound at the pre-transaction snapshot seq), which is the
    // condition that freezes pre-warmed statements after COMMIT. A plain
    // single-table insert abandons the mirror instead and heals on the next
    // read boundary, masking the defect.
    conn.execute("PRAGMA foreign_keys = ON")?;
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
    )?;
    Ok(conn)
}

fn insert_artifact(conn: &Connection, artifact_id: &str, content_hash: &str) -> TestResult {
    conn.execute_with_params(
        "INSERT INTO artifact_refs (artifact_id, content_hash, kind) VALUES (?1, ?2, ?3)",
        &[
            text_value(artifact_id),
            text_value(content_hash),
            text_value("filing_diff"),
        ],
    )?;
    conn.execute_with_params(
        "INSERT INTO runs (run_id, artifact_id) VALUES (?1, ?2)",
        &[text_value("run-00000"), text_value(artifact_id)],
    )?;
    Ok(())
}

const ARTIFACT_ID: &str = "art-00000";
const CONTENT_HASH: &str = "00000000000000000000000000000000000000000000feedbeef000000000000";

#[test]
fn memory_text_pk_statement_warmed_before_first_write_tx_sees_committed_row() -> TestResult {
    let conn = open_memory_artifact_db()?;

    let sql = "SELECT artifact_id, kind FROM artifact_refs WHERE artifact_id = ?1";
    let stmt = conn.prepare(sql)?;
    // Warm the statement on the empty table so its result shape predates the
    // connection's first write transaction.
    let warm = stmt.query_with_params(&[text_value(ARTIFACT_ID)])?;
    assert!(warm.is_empty(), "warm run on empty table must be empty");

    conn.execute("BEGIN IMMEDIATE")?;
    insert_artifact(&conn, ARTIFACT_ID, CONTENT_HASH)?;
    conn.execute("COMMIT")?;

    let rows = stmt.query_with_params(&[text_value(ARTIFACT_ID)])?;
    assert_eq!(
        rows.len(),
        1,
        "pre-warmed TEXT-PK equality statement must see the first write \
         transaction's committed row"
    );
    assert_eq!(text_at(&rows[0], 0)?, ARTIFACT_ID);

    // Repeat executions must stay current (the field failure was frozen
    // forever, not a single stale read).
    let again = stmt.query_with_params(&[text_value(ARTIFACT_ID)])?;
    assert_eq!(again.len(), 1, "re-execution must remain current");

    // A fresh statement was always correct; keep that pinned.
    let fresh = conn.query_with_params(sql, &[text_value(ARTIFACT_ID)])?;
    assert_eq!(fresh.len(), 1, "fresh statement must see the committed row");
    Ok(())
}

#[test]
fn memory_text_pk_statement_warmed_before_first_write_tx_sees_own_insert_inside_tx() -> TestResult {
    let conn = open_memory_artifact_db()?;

    let stmt =
        conn.prepare("SELECT artifact_id, kind FROM artifact_refs WHERE artifact_id = ?1")?;
    let warm = stmt.query_with_params(&[text_value(ARTIFACT_ID)])?;
    assert!(warm.is_empty(), "warm run on empty table must be empty");

    conn.execute("BEGIN IMMEDIATE")?;
    let before_insert = stmt.query_with_params(&[text_value(ARTIFACT_ID)])?;
    assert!(
        before_insert.is_empty(),
        "row must not exist before the transaction's insert"
    );
    insert_artifact(&conn, ARTIFACT_ID, CONTENT_HASH)?;
    let inside = stmt.query_with_params(&[text_value(ARTIFACT_ID)])?;
    assert_eq!(
        inside.len(),
        1,
        "pre-warmed statement must see the open transaction's own insert"
    );
    conn.execute("COMMIT")?;

    let after = stmt.query_with_params(&[text_value(ARTIFACT_ID)])?;
    assert_eq!(
        after.len(),
        1,
        "pre-warmed statement must stay current after COMMIT"
    );
    Ok(())
}

#[test]
fn memory_unique_text_statement_warmed_before_first_write_tx_sees_committed_row() -> TestResult {
    let conn = open_memory_artifact_db()?;

    let stmt = conn
        .prepare("SELECT artifact_id, content_hash FROM artifact_refs WHERE content_hash = ?1")?;
    let warm = stmt.query_with_params(&[text_value(CONTENT_HASH)])?;
    assert!(warm.is_empty(), "warm run on empty table must be empty");

    conn.execute("BEGIN IMMEDIATE")?;
    insert_artifact(&conn, ARTIFACT_ID, CONTENT_HASH)?;
    conn.execute("COMMIT")?;

    let rows = stmt.query_with_params(&[text_value(CONTENT_HASH)])?;
    assert_eq!(
        rows.len(),
        1,
        "pre-warmed UNIQUE-index equality statement must see the first write \
         transaction's committed row"
    );
    assert_eq!(text_at(&rows[0], 0)?, ARTIFACT_ID);
    Ok(())
}

#[test]
fn stale_prepared_statement_rejects_schema_change_from_other_connection() -> TestResult {
    let dir = tempfile::tempdir()?;
    let db_path = dir.path().join("schema-staleness.db");
    let db_path = db_path.to_string_lossy().into_owned();

    let conn_a = open_wal_db(&db_path)?;
    setup_table(&conn_a)?;
    commit_insert(&conn_a, 1, "one")?;

    let conn_b = open_wal_db(&db_path)?;
    let stmt = conn_b.prepare("SELECT v FROM t WHERE pk = ?1")?;
    let warm_rows = stmt.query_with_params(&[SqliteValue::Integer(1)])?;
    assert_eq!(warm_rows.len(), 1);
    assert_eq!(text_at(&warm_rows[0], 0)?, "one");

    conn_a.execute("ALTER TABLE t ADD COLUMN extra TEXT DEFAULT 'fresh'")?;

    let schema_result = stmt.query_with_params(&[SqliteValue::Integer(1)]);
    assert!(
        matches!(schema_result, Err(FrankenError::SchemaChanged)),
        "stale prepared statement should reject schema change, got {schema_result:?}"
    );

    let fresh_stmt = conn_b.prepare("SELECT extra FROM t WHERE pk = ?1")?;
    let fresh_rows = fresh_stmt.query_with_params(&[SqliteValue::Integer(1)])?;
    assert_eq!(fresh_rows.len(), 1);
    assert_eq!(text_at(&fresh_rows[0], 0)?, "fresh");
    Ok(())
}
