//! bd-fcof5: beads_rust JSONL auto-import fails with a PHANTOM
//! "PRIMARY KEY constraint failed" during a bulk upsert of ~3200 rows into a
//! TEXT-PRIMARY-KEY table carrying several partial (and one UNIQUE partial)
//! indexes.
//!
//! Faithful to `upsert_issue_for_import_in_tx`: the whole import runs in ONE
//! transaction, and per issue does an existence-probe
//! `SELECT 1 FROM issues WHERE id = ? LIMIT 1`, then UPDATE if present or INSERT
//! if new — all via REUSED prepared statements. Because the index grows and
//! splits leaves WITHIN the transaction, a stale seek-cache cursor / index root
//! can make a genuinely-new id (the probe said NOT-present) fail INSERT with a
//! phantom PK conflict (class of bd-h1kvh / GH#123). br embeds a pinned
//! crates.io fsqlite, so this reproduces the shape against HEAD directly.

use fsqlite_core::connection::Connection;
use fsqlite_types::value::SqliteValue;

const N: usize = 3200;
const BEADS_SHAPED_N: usize = 1200;

fn text(s: impl Into<String>) -> SqliteValue {
    SqliteValue::Text(s.into().into())
}

const PROBE: &str = "SELECT 1 FROM issues WHERE id = ?1 LIMIT 1;";
const INSERT: &str = "INSERT INTO issues (id, external_ref, status, priority, updated_at) \
     VALUES (?1, ?2, ?3, ?4, ?5);";
const UPDATE: &str = "UPDATE issues SET status = ?2, priority = ?3, updated_at = ?4 \
     WHERE id = ?1;";

fn ext_for(i: usize) -> SqliteValue {
    // ~1/4 of rows carry a non-null external_ref (exercises the UNIQUE partial
    // index); the rest are NULL and excluded from it.
    if i.is_multiple_of(4) {
        text(format!("ext-{i:05}"))
    } else {
        SqliteValue::Null
    }
}

fn deterministic_payload(label: &str, row: usize, len: usize) -> String {
    let prefix = format!("{label}:{row:05}:");
    let mut payload = String::with_capacity(len);
    while payload.len() < len {
        payload.push_str(&prefix);
    }
    payload.truncate(len);
    payload
}

async fn probe_exists(conn: &Connection, id: &str) -> bool {
    !conn
        .query_with_params(PROBE, &[text(id)])
        .await
        .unwrap_or_else(|e| panic!("existence probe failed for {id}: {e:?}"))
        .is_empty()
}

// bd-fcof5: GREEN regression guard (was a known-red anchor). A single-writer
// file-backed BEGIN IMMEDIATE bulk import (~3200 rows, TEXT PK + partial
// indexes) spills across multiple group-commit flushes. It once failed COMMIT
// with a self-conflict `BusySnapshot { conflicting_pages: "8,9,10" }`: the
// o81ov cross-connection EOF-alias guard (wal_adapter.rs
// conflicting_pages_since_snapshot) misclassified the transaction's OWN
// spilled fresh pages as peer aliases, because a later flush re-derived its
// allocator base from the connection's stale begin-time db_size.
//
// Fixed at HEAD by the bd-vnxjd db_size-monotonicity floor in the group-commit
// flusher (pager.rs: flush_base_db_size = max(inner.db_size, latest durable
// certificate db_size)). Each spill flush now bases its snapshot_db_size on the
// transaction's OWN prior committed growth, so the second import flush runs
// with snapshot_db_size = 812 (covering the first flush's 812 pages) and those
// pages are no longer "fresh past the allocator base" — the o81ov guard no
// longer fires on them, while genuine cross-connection EOF double-allocation
// (a peer's frame the committing connection never wrote) is still caught.
//
// This keeper locks the beads-shaped import contract green. If it regresses to
// BusySnapshot, the db_size floor / alias-guard interaction has broken again.
#[test]
fn bulk_import_upsert_text_pk_has_no_phantom_pk_conflict() {
    asupersync::test_utils::run_test(|| async {
        // File-backed (like beads' io_uring backend) so the WAL + seek-cache +
        // statement-reuse fast path — the suspected mechanism — is exercised.
        let dir = tempfile::tempdir().expect("temp dir");
        let db = dir.path().join("issues.db").to_string_lossy().into_owned();
        let conn = Connection::open(&db).await.expect("open");
        for ddl in [
            "CREATE TABLE issues (\
                 id TEXT PRIMARY KEY, \
                 external_ref TEXT, \
                 status TEXT NOT NULL DEFAULT 'open', \
                 priority INTEGER NOT NULL DEFAULT 2, \
                 updated_at TEXT NOT NULL DEFAULT '');",
            "CREATE UNIQUE INDEX idx_ext ON issues(external_ref) WHERE external_ref IS NOT NULL;",
            "CREATE INDEX idx_status ON issues(status);",
            "CREATE INDEX idx_priority ON issues(priority);",
            "CREATE INDEX idx_ready ON issues(status, priority) WHERE status = 'open';",
        ] {
            conn.execute(ddl).await.expect("ddl");
        }

        // ── Initial import: ONE transaction, probe-then-INSERT every new id. ──
        conn.execute("BEGIN IMMEDIATE;")
            .await
            .expect("begin initial import");
        for i in 0..N {
            let id = format!("bd-{i:05}");
            assert!(
                !probe_exists(&conn, &id).await,
                "fresh DB: {id} must not pre-exist"
            );
            conn.execute_with_params(
                INSERT,
                &[
                    text(id.clone()),
                    ext_for(i),
                    text(if i % 3 == 0 { "closed" } else { "open" }),
                    SqliteValue::Integer((i % 5) as i64),
                    text("t0"),
                ],
            )
            .await
            .unwrap_or_else(|e| {
                panic!(
                    "INITIAL-import PHANTOM 'PRIMARY KEY constraint failed' on new id {id} \
                     (probe said NOT-present) at iteration {i}: {e:?}"
                )
            });
        }
        conn.execute("COMMIT;")
            .await
            .expect("commit initial import");

        // ── Re-import: ONE transaction, probe-then-UPDATE existing, with 16 new
        // ids scattered among them (unsorted-JSONL re-import that adds rows). ──
        conn.execute("BEGIN IMMEDIATE;")
            .await
            .expect("begin re-import");
        let mut inserted_new = 0usize;
        for i in 0..N {
            let id = format!("bd-{i:05}");
            assert!(
                probe_exists(&conn, &id).await,
                "re-import: existing {id} must be present"
            );
            let rows = conn
                .execute_with_params(
                    UPDATE,
                    &[
                        text(id.clone()),
                        text(if i % 2 == 0 { "open" } else { "closed" }),
                        SqliteValue::Integer((i % 4) as i64),
                        text("t1"),
                    ],
                )
                .await
                .unwrap_or_else(|e| panic!("re-import UPDATE failed at {i} (id={id}): {e:?}"));
            assert!(rows > 0, "existing id {id} must UPDATE, got {rows} rows");

            // Scatter a new id after every ~200th existing row.
            if i % 200 == 199 && inserted_new < 16 {
                let nid = format!("bd-{:05}", N + inserted_new);
                assert!(
                    !probe_exists(&conn, &nid).await,
                    "new id {nid} must not pre-exist"
                );
                conn.execute_with_params(
                    INSERT,
                    &[
                        text(nid.clone()),
                        SqliteValue::Null,
                        text("open"),
                        SqliteValue::Integer(1),
                        text("t2"),
                    ],
                )
                .await
                .unwrap_or_else(|e| {
                    panic!(
                        "RE-import PHANTOM 'PRIMARY KEY constraint failed' on new id {nid} \
                         (probe said NOT-present) after {i} existing rows: {e:?}"
                    )
                });
                inserted_new += 1;
            }
        }
        conn.execute("COMMIT;").await.expect("commit re-import");
        assert_eq!(
            inserted_new, 16,
            "all 16 scattered new ids must insert without phantom conflict"
        );

        // ── Durable-state checks ──
        let integrity = conn
            .query("PRAGMA integrity_check;")
            .await
            .expect("integrity_check");
        assert!(
            matches!(integrity[0].values()[0], SqliteValue::Text(ref s) if s.as_ref() == "ok"),
            "integrity_check must be ok, got {:?}",
            integrity[0].values()[0]
        );
        let count = conn
            .query("SELECT COUNT(*) FROM issues;")
            .await
            .expect("count");
        assert_eq!(
            count[0].values()[0],
            SqliteValue::Integer((N + 16) as i64),
            "row count must equal all inserted ids"
        );
    });
}

/// GH #399 / beads_rust #458: a fresh JSONL rebuild reported an `issues`
/// overflow page aliased as a `comments` B-tree page while all table counts
/// still matched. Exercise that exact insert-only shape: one large transaction,
/// roughly 1,200 wide issue rows, 10-15 KiB overflow payloads, and interleaved
/// comments/events relation inserts with their secondary indexes.
///
/// Count equality is deliberately insufficient here. The keeper proves every
/// source primary key is point-addressable, every normalized payload round-
/// trips, relation rows remain unique and exact, and both FrankenSQLite's
/// ownership walk and stock SQLite's independent integrity checker accept the
/// final physical image.
#[test]
fn beads_shaped_large_payload_import_has_unique_page_ownership_and_exact_rows() {
    asupersync::test_utils::run_test(|| async {
        let dir = tempfile::tempdir().expect("temp dir");
        let db_path = dir.path().join("beads-shaped.db");
        let db = db_path.to_string_lossy().into_owned();
        let conn = Connection::open(&db).await.expect("open");

        for ddl in [
            "CREATE TABLE issues (\
                 id TEXT PRIMARY KEY, \
                 title TEXT NOT NULL, \
                 description TEXT NOT NULL DEFAULT '', \
                 close_reason TEXT NOT NULL DEFAULT '', \
                 status TEXT NOT NULL DEFAULT 'open', \
                 priority INTEGER NOT NULL DEFAULT 2, \
                 created_at TEXT NOT NULL, \
                 created_by TEXT NOT NULL DEFAULT '', \
                 updated_at TEXT NOT NULL);",
            "CREATE INDEX idx_issues_status ON issues(status);",
            "CREATE INDEX idx_issues_priority ON issues(priority);",
            "CREATE INDEX idx_issues_created_at ON issues(created_at);",
            "CREATE INDEX idx_issues_updated_at ON issues(updated_at);",
            "CREATE INDEX idx_issues_ready ON issues(status, priority, created_at) \
                 WHERE status = 'open';",
            "CREATE TABLE comments (\
                 id INTEGER PRIMARY KEY AUTOINCREMENT, \
                 issue_id TEXT NOT NULL, \
                 author TEXT NOT NULL, \
                 text TEXT NOT NULL, \
                 created_at TEXT NOT NULL);",
            "CREATE INDEX idx_comments_issue ON comments(issue_id);",
            "CREATE INDEX idx_comments_created_at ON comments(created_at);",
            "CREATE TABLE events (\
                 id INTEGER PRIMARY KEY AUTOINCREMENT, \
                 issue_id TEXT NOT NULL, \
                 event_type TEXT NOT NULL, \
                 actor TEXT NOT NULL DEFAULT '', \
                 comment TEXT, \
                 created_at TEXT NOT NULL);",
            "CREATE INDEX idx_events_issue ON events(issue_id);",
            "CREATE INDEX idx_events_type ON events(event_type);",
            "CREATE INDEX idx_events_created_at ON events(created_at);",
        ] {
            conn.execute(ddl).await.expect("create beads-shaped schema");
        }

        let mut expected_issues = Vec::with_capacity(BEADS_SHAPED_N);
        let mut expected_comments = Vec::new();
        let mut expected_events = Vec::new();

        conn.execute("BEGIN IMMEDIATE;")
            .await
            .expect("begin beads-shaped import");
        for row in 0..BEADS_SHAPED_N {
            let id = format!("zz-{row:05}");
            let title = format!("Synthetic issue {row:05}");
            let description_len = if row.is_multiple_of(29) {
                14_999
            } else {
                1_600 + (row % 9) * 173
            };
            let close_reason_len = if row.is_multiple_of(41) { 11_003 } else { 0 };
            let description = deterministic_payload("description", row, description_len);
            let close_reason = deterministic_payload("close", row, close_reason_len);
            let status = if row.is_multiple_of(5) {
                "closed"
            } else {
                "open"
            };
            let priority = i64::try_from(row % 5).expect("priority fits i64");
            let created_at = format!("2026-08-27T12:{:02}:{:02}Z", row % 60, row / 60 % 60);
            let updated_at = format!("2026-08-27T13:{:02}:{:02}Z", row % 60, row / 60 % 60);

            conn.execute_with_params(
                "INSERT INTO issues (\
                     id, title, description, close_reason, status, priority, \
                     created_at, created_by, updated_at\
                 ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9);",
                &[
                    text(id.clone()),
                    text(title.clone()),
                    text(description.clone()),
                    text(close_reason.clone()),
                    text(status),
                    SqliteValue::Integer(priority),
                    text(created_at.clone()),
                    text("ubuntu"),
                    text(updated_at.clone()),
                ],
            )
            .await
            .unwrap_or_else(|error| panic!("issue insert {row} failed: {error:?}"));

            expected_issues.push((
                id.clone(),
                title,
                description,
                close_reason,
                status.to_owned(),
                priority,
                created_at.clone(),
                "ubuntu".to_owned(),
                updated_at,
            ));

            if row.is_multiple_of(3) {
                let comment = deterministic_payload("comment", row, 401 + row % 211);
                conn.execute_with_params(
                    "INSERT INTO comments (issue_id, author, text, created_at) \
                     VALUES (?1, ?2, ?3, ?4);",
                    &[
                        text(id.clone()),
                        text("reporter"),
                        text(comment.clone()),
                        text(created_at.clone()),
                    ],
                )
                .await
                .unwrap_or_else(|error| panic!("comment insert {row} failed: {error:?}"));
                expected_comments.push((id.clone(), "reporter".to_owned(), comment, created_at.clone()));
            }

            if row.is_multiple_of(4) {
                let event_comment = format!("created {id}");
                conn.execute_with_params(
                    "INSERT INTO events (issue_id, event_type, actor, comment, created_at) \
                     VALUES (?1, ?2, ?3, ?4, ?5);",
                    &[
                        text(id.clone()),
                        text("created"),
                        text("ubuntu"),
                        text(event_comment.clone()),
                        text(created_at.clone()),
                    ],
                )
                .await
                .unwrap_or_else(|error| panic!("event insert {row} failed: {error:?}"));
                expected_events.push((
                    id,
                    "created".to_owned(),
                    "ubuntu".to_owned(),
                    event_comment,
                    created_at,
                ));
            }
        }
        conn.execute("COMMIT;")
            .await
            .expect("commit beads-shaped import");

        let integrity = conn
            .query("PRAGMA integrity_check;")
            .await
            .expect("FrankenSQLite integrity_check");
        assert_eq!(
            integrity.len(),
            1,
            "physical ownership walk returned {integrity:?}"
        );
        assert_eq!(integrity[0].values()[0], text("ok"));

        let imported = conn
            .query(
                "SELECT id, title, description, close_reason, status, priority, \
                        created_at, created_by, updated_at \
                 FROM issues ORDER BY id;",
            )
            .await
            .expect("read every imported issue");
        assert_eq!(imported.len(), expected_issues.len());
        for (row, expected) in imported.iter().zip(&expected_issues) {
            assert_eq!(
                row.values(),
                &[
                    text(expected.0.clone()),
                    text(expected.1.clone()),
                    text(expected.2.clone()),
                    text(expected.3.clone()),
                    text(expected.4.clone()),
                    SqliteValue::Integer(expected.5),
                    text(expected.6.clone()),
                    text(expected.7.clone()),
                    text(expected.8.clone()),
                ]
            );

            let point = conn
                .query_with_params("SELECT id FROM issues WHERE id = ?1;", &[text(&expected.0)])
                .await
                .unwrap_or_else(|error| panic!("point lookup {} failed: {error:?}", expected.0));
            assert_eq!(point.len(), 1, "source id {} is not uniquely addressable", expected.0);
            assert_eq!(point[0].values()[0], text(expected.0.clone()));
        }

        let comments = conn
            .query("SELECT issue_id, author, text, created_at FROM comments ORDER BY id;")
            .await
            .expect("read comments");
        assert_eq!(comments.len(), expected_comments.len());
        for (row, expected) in comments.iter().zip(&expected_comments) {
            assert_eq!(
                row.values(),
                &[
                    text(expected.0.clone()),
                    text(expected.1.clone()),
                    text(expected.2.clone()),
                    text(expected.3.clone()),
                ]
            );
        }

        let events = conn
            .query(
                "SELECT issue_id, event_type, actor, comment, created_at \
                 FROM events ORDER BY id;",
            )
            .await
            .expect("read events");
        assert_eq!(events.len(), expected_events.len());
        for (row, expected) in events.iter().zip(&expected_events) {
            assert_eq!(
                row.values(),
                &[
                    text(expected.0.clone()),
                    text(expected.1.clone()),
                    text(expected.2.clone()),
                    text(expected.3.clone()),
                    text(expected.4.clone()),
                ]
            );
        }

        drop(conn);
        let sqlite = rusqlite::Connection::open(&db_path).expect("stock SQLite open");
        let stock_integrity: String = sqlite
            .query_row("PRAGMA integrity_check;", [], |row| row.get(0))
            .expect("stock SQLite integrity_check");
        assert_eq!(stock_integrity, "ok");
        let stock_issue_count: i64 = sqlite
            .query_row("SELECT COUNT(*) FROM issues;", [], |row| row.get(0))
            .expect("stock issue count");
        let stock_comment_count: i64 = sqlite
            .query_row("SELECT COUNT(*) FROM comments;", [], |row| row.get(0))
            .expect("stock comment count");
        let stock_event_count: i64 = sqlite
            .query_row("SELECT COUNT(*) FROM events;", [], |row| row.get(0))
            .expect("stock event count");
        assert_eq!(stock_issue_count, BEADS_SHAPED_N as i64);
        assert_eq!(stock_comment_count, expected_comments.len() as i64);
        assert_eq!(stock_event_count, expected_events.len() as i64);
    });
}

// ────────────────────────────────────────────────────────────────────────────
// GH #399 / beads_rust #458 — faithful `br sync --import-only` sequence.
//
// The insert-only keeper above never exercised the shapes beads actually
// writes: the real 38-column `issues` row behind fifteen indexes, random TEXT
// primary keys (mid-tree autoindex inserts), comment rows carrying explicit
// non-monotonic ids taken from the JSONL, `INSERT OR IGNORE` label rows,
// multi-row `INSERT OR IGNORE` dependency rows, per-issue relation probes,
// batched export-hash `INSERT OR REPLACE`, orphan-cleanup deletes, beads'
// runtime PRAGMAs (WAL, synchronous=NORMAL, cache_size=-8000,
// wal_autocheckpoint=0), and a second upsert pass that rewrites large
// overflow rows and DELETE+reinserts every relation row.
// ────────────────────────────────────────────────────────────────────────────

const BEADS_ISSUE_COUNT: usize = 1_200;
const BEADS_EXPORT_HASH_BATCH: usize = 512;

const BEADS_SCHEMA: &[&str] = &[
    "CREATE TABLE IF NOT EXISTS issues (
        id TEXT PRIMARY KEY,
        content_hash TEXT,
        title TEXT NOT NULL CHECK(length(title) <= 500),
        description TEXT NOT NULL DEFAULT '',
        design TEXT NOT NULL DEFAULT '',
        acceptance_criteria TEXT NOT NULL DEFAULT '',
        notes TEXT NOT NULL DEFAULT '',
        status TEXT NOT NULL DEFAULT 'open',
        priority INTEGER NOT NULL DEFAULT 2 CHECK(priority >= 0 AND priority <= 4),
        issue_type TEXT NOT NULL DEFAULT 'task',
        assignee TEXT,
        owner TEXT DEFAULT '',
        estimated_minutes INTEGER,
        created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
        created_by TEXT DEFAULT '',
        updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
        closed_at DATETIME,
        close_reason TEXT DEFAULT '',
        closed_by_session TEXT DEFAULT '',
        due_at DATETIME,
        defer_until DATETIME,
        external_ref TEXT,
        source_system TEXT DEFAULT '',
        source_repo TEXT NOT NULL DEFAULT '.',
        deleted_at DATETIME,
        deleted_by TEXT DEFAULT '',
        delete_reason TEXT DEFAULT '',
        original_type TEXT DEFAULT '',
        compaction_level INTEGER DEFAULT 0,
        compacted_at DATETIME,
        compacted_at_commit TEXT,
        original_size INTEGER,
        sender TEXT DEFAULT '',
        ephemeral INTEGER NOT NULL DEFAULT 0,
        pinned INTEGER NOT NULL DEFAULT 0,
        is_template INTEGER NOT NULL DEFAULT 0,
        source_repo_path TEXT,
        agent_context TEXT,
        CHECK (
            (status = 'closed' AND closed_at IS NOT NULL) OR
            (status = 'tombstone') OR
            (status NOT IN ('closed', 'tombstone') AND closed_at IS NULL)
        )
    )",
    "CREATE INDEX IF NOT EXISTS idx_issues_status ON issues(status)",
    "CREATE INDEX IF NOT EXISTS idx_issues_priority ON issues(priority)",
    "CREATE INDEX IF NOT EXISTS idx_issues_issue_type ON issues(issue_type)",
    "CREATE INDEX IF NOT EXISTS idx_issues_assignee ON issues(assignee) WHERE assignee IS NOT NULL",
    "CREATE INDEX IF NOT EXISTS idx_issues_created_at ON issues(created_at)",
    "CREATE INDEX IF NOT EXISTS idx_issues_updated_at ON issues(updated_at)",
    "CREATE INDEX IF NOT EXISTS idx_issues_content_hash ON issues(content_hash)",
    "CREATE UNIQUE INDEX IF NOT EXISTS idx_issues_external_ref_unique ON issues(external_ref) \
     WHERE external_ref IS NOT NULL",
    "CREATE INDEX IF NOT EXISTS idx_issues_ephemeral ON issues(ephemeral) WHERE ephemeral = 1",
    "CREATE INDEX IF NOT EXISTS idx_issues_pinned ON issues(pinned) WHERE pinned = 1",
    "CREATE INDEX IF NOT EXISTS idx_issues_tombstone ON issues(status) WHERE status = 'tombstone'",
    "CREATE INDEX IF NOT EXISTS idx_issues_due_at ON issues(due_at) WHERE due_at IS NOT NULL",
    "CREATE INDEX IF NOT EXISTS idx_issues_defer_until ON issues(defer_until) \
     WHERE defer_until IS NOT NULL",
    "CREATE INDEX IF NOT EXISTS idx_issues_ready ON issues(status, priority, created_at) \
     WHERE status = 'open' AND ephemeral = 0 AND pinned = 0 AND is_template = 0",
    "CREATE INDEX IF NOT EXISTS idx_issues_status_priority_created \
     ON issues(status, priority, created_at)",
    "CREATE INDEX IF NOT EXISTS idx_issues_list_active_order ON issues(priority, created_at) \
     WHERE status NOT IN ('closed', 'tombstone') AND (is_template = 0 OR is_template IS NULL)",
    "CREATE TABLE IF NOT EXISTS dependencies (
        issue_id TEXT NOT NULL,
        depends_on_id TEXT NOT NULL,
        type TEXT NOT NULL DEFAULT 'blocks',
        created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
        created_by TEXT NOT NULL DEFAULT '',
        metadata TEXT DEFAULT '{}',
        thread_id TEXT DEFAULT '',
        PRIMARY KEY (issue_id, depends_on_id),
        FOREIGN KEY (issue_id) REFERENCES issues(id) ON DELETE CASCADE
    )",
    "CREATE INDEX IF NOT EXISTS idx_dependencies_issue ON dependencies(issue_id)",
    "CREATE INDEX IF NOT EXISTS idx_dependencies_depends_on ON dependencies(depends_on_id)",
    "CREATE INDEX IF NOT EXISTS idx_dependencies_type ON dependencies(type)",
    "CREATE INDEX IF NOT EXISTS idx_dependencies_depends_on_type \
     ON dependencies(depends_on_id, type)",
    "CREATE INDEX IF NOT EXISTS idx_dependencies_thread ON dependencies(thread_id) \
     WHERE thread_id != ''",
    "CREATE INDEX IF NOT EXISTS idx_dependencies_blocking ON dependencies(depends_on_id, issue_id) \
     WHERE (type = 'blocks' OR type = 'parent-child' OR type = 'conditional-blocks' \
     OR type = 'waits-for')",
    "CREATE TABLE IF NOT EXISTS labels (
        issue_id TEXT NOT NULL,
        label TEXT NOT NULL,
        PRIMARY KEY (issue_id, label),
        FOREIGN KEY (issue_id) REFERENCES issues(id) ON DELETE CASCADE
    )",
    "CREATE INDEX IF NOT EXISTS idx_labels_label ON labels(label)",
    "CREATE INDEX IF NOT EXISTS idx_labels_issue ON labels(issue_id)",
    "CREATE TABLE IF NOT EXISTS comments (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        issue_id TEXT NOT NULL,
        author TEXT NOT NULL,
        text TEXT NOT NULL,
        created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (issue_id) REFERENCES issues(id) ON DELETE CASCADE
    )",
    "CREATE INDEX IF NOT EXISTS idx_comments_issue ON comments(issue_id)",
    "CREATE INDEX IF NOT EXISTS idx_comments_created_at ON comments(created_at)",
    "CREATE TABLE IF NOT EXISTS events (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        issue_id TEXT NOT NULL,
        event_type TEXT NOT NULL,
        actor TEXT NOT NULL DEFAULT '',
        old_value TEXT,
        new_value TEXT,
        comment TEXT,
        created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
        agent_name TEXT,
        harness TEXT,
        model TEXT,
        FOREIGN KEY (issue_id) REFERENCES issues(id) ON DELETE CASCADE
    )",
    "CREATE INDEX IF NOT EXISTS idx_events_issue ON events(issue_id)",
    "CREATE INDEX IF NOT EXISTS idx_events_type ON events(event_type)",
    "CREATE INDEX IF NOT EXISTS idx_events_created_at ON events(created_at)",
    "CREATE INDEX IF NOT EXISTS idx_events_actor ON events(actor) WHERE actor != ''",
    "CREATE TABLE IF NOT EXISTS config (key TEXT NOT NULL, value TEXT NOT NULL)",
    "CREATE INDEX IF NOT EXISTS idx_config_key ON config(key)",
    "CREATE TABLE IF NOT EXISTS metadata (key TEXT NOT NULL, value TEXT NOT NULL)",
    "CREATE INDEX IF NOT EXISTS idx_metadata_key ON metadata(key)",
    "CREATE TABLE IF NOT EXISTS dirty_issues (
        issue_id TEXT PRIMARY KEY,
        marked_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (issue_id) REFERENCES issues(id) ON DELETE CASCADE
    )",
    "CREATE INDEX IF NOT EXISTS idx_dirty_issues_marked_at ON dirty_issues(marked_at)",
    "CREATE TABLE IF NOT EXISTS export_hashes (
        issue_id TEXT PRIMARY KEY,
        content_hash TEXT NOT NULL,
        exported_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (issue_id) REFERENCES issues(id) ON DELETE CASCADE
    )",
    "CREATE TABLE IF NOT EXISTS blocked_issues_cache (
        issue_id TEXT PRIMARY KEY,
        blocked_by TEXT NOT NULL,
        blocked_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (issue_id) REFERENCES issues(id) ON DELETE CASCADE
    )",
    "CREATE INDEX IF NOT EXISTS idx_blocked_cache_blocked_at ON blocked_issues_cache(blocked_at)",
    "CREATE TABLE IF NOT EXISTS child_counters (
        parent_id TEXT PRIMARY KEY,
        last_child INTEGER NOT NULL DEFAULT 0,
        FOREIGN KEY (parent_id) REFERENCES issues(id) ON DELETE CASCADE
    )",
    "CREATE TABLE IF NOT EXISTS close_metadata (
        issue_id TEXT PRIMARY KEY,
        closed_by_agent_name TEXT,
        closed_by_harness TEXT,
        closed_by_model TEXT,
        bypassed_policy INTEGER NOT NULL DEFAULT 0,
        bypass_reason TEXT,
        policy_gates_fired TEXT,
        recorded_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (issue_id) REFERENCES issues(id) ON DELETE CASCADE
    )",
    "CREATE INDEX IF NOT EXISTS idx_close_metadata_recorded_at ON close_metadata(recorded_at)",
    "CREATE INDEX IF NOT EXISTS idx_close_metadata_bypassed ON close_metadata(bypassed_policy) \
     WHERE bypassed_policy = 1",
];

const BEADS_ISSUE_COLUMNS: &str = "id, content_hash, title, description, design, \
     acceptance_criteria, notes, status, priority, issue_type, assignee, owner, \
     estimated_minutes, created_at, created_by, updated_at, closed_at, close_reason, \
     closed_by_session, due_at, defer_until, external_ref, source_system, source_repo, \
     source_repo_path, deleted_at, deleted_by, delete_reason, original_type, \
     compaction_level, compacted_at, compacted_at_commit, original_size, sender, ephemeral, \
     pinned, is_template, agent_context";

const BEADS_ISSUE_INSERT: &str = "INSERT INTO issues (id, content_hash, title, description, \
     design, acceptance_criteria, notes, status, priority, issue_type, assignee, owner, \
     estimated_minutes, created_at, created_by, updated_at, closed_at, close_reason, \
     closed_by_session, due_at, defer_until, external_ref, source_system, source_repo, \
     source_repo_path, deleted_at, deleted_by, delete_reason, original_type, \
     compaction_level, compacted_at, compacted_at_commit, original_size, sender, ephemeral, \
     pinned, is_template, agent_context) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, \
     ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

const BEADS_ISSUE_UPDATE: &str = "UPDATE issues SET content_hash = ?, title = ?, \
     description = ?, design = ?, acceptance_criteria = ?, notes = ?, status = ?, \
     priority = ?, issue_type = ?, assignee = ?, owner = ?, estimated_minutes = ?, \
     created_at = ?, created_by = ?, updated_at = ?, closed_at = ?, close_reason = ?, \
     closed_by_session = ?, due_at = ?, defer_until = ?, external_ref = ?, \
     source_system = ?, source_repo = ?, source_repo_path = ?, deleted_at = ?, \
     deleted_by = ?, delete_reason = ?, original_type = ?, compaction_level = ?, \
     compacted_at = ?, compacted_at_commit = ?, original_size = ?, sender = ?, \
     ephemeral = ?, pinned = ?, is_template = ?, agent_context = ? WHERE id = ?";

/// Deterministic xorshift64* generator so the synthetic corpus is reproducible.
struct BeadsRng(u64);

impl BeadsRng {
    fn next_u64(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x >> 12;
        x ^= x << 25;
        x ^= x >> 27;
        self.0 = x;
        x.wrapping_mul(0x2545_F491_4F6C_DD1D)
    }

    fn below(&mut self, bound: usize) -> usize {
        usize::try_from(self.next_u64() % bound as u64).expect("bounded index fits usize")
    }
}

#[derive(Clone, Debug)]
struct BeadsComment {
    id: i64,
    author: String,
    text: String,
    created_at: String,
}

#[derive(Clone, Debug)]
struct BeadsIssue {
    id: String,
    content_hash: String,
    title: String,
    description: String,
    design: String,
    acceptance_criteria: String,
    notes: String,
    status: String,
    priority: i64,
    issue_type: String,
    assignee: Option<String>,
    owner: String,
    estimated_minutes: Option<i64>,
    created_at: String,
    created_by: String,
    updated_at: String,
    closed_at: Option<String>,
    close_reason: String,
    closed_by_session: String,
    due_at: Option<String>,
    external_ref: Option<String>,
    labels: Vec<String>,
    dependencies: Vec<(String, String)>,
    comments: Vec<BeadsComment>,
}

fn opt_text(value: &Option<String>) -> SqliteValue {
    value.as_deref().map_or(SqliteValue::Null, text)
}

impl BeadsIssue {
    /// The 37 non-key column values in beads' `import_issue_field_values` order.
    fn field_values(&self) -> Vec<SqliteValue> {
        vec![
            text(self.content_hash.clone()),
            text(self.title.clone()),
            text(self.description.clone()),
            text(self.design.clone()),
            text(self.acceptance_criteria.clone()),
            text(self.notes.clone()),
            text(self.status.clone()),
            SqliteValue::Integer(self.priority),
            text(self.issue_type.clone()),
            opt_text(&self.assignee),
            text(self.owner.clone()),
            self.estimated_minutes
                .map_or(SqliteValue::Null, SqliteValue::Integer),
            text(self.created_at.clone()),
            text(self.created_by.clone()),
            text(self.updated_at.clone()),
            opt_text(&self.closed_at),
            text(self.close_reason.clone()),
            text(self.closed_by_session.clone()),
            opt_text(&self.due_at),
            SqliteValue::Null,
            opt_text(&self.external_ref),
            text(""),
            text("."),
            SqliteValue::Null,
            SqliteValue::Null,
            text(""),
            text(""),
            text(""),
            SqliteValue::Integer(0),
            SqliteValue::Null,
            SqliteValue::Null,
            SqliteValue::Integer(0),
            text(""),
            SqliteValue::Integer(0),
            SqliteValue::Integer(0),
            SqliteValue::Integer(0),
            SqliteValue::Null,
        ]
    }

    fn row_values(&self) -> Vec<SqliteValue> {
        let mut values = vec![text(self.id.clone())];
        values.extend(self.field_values());
        values
    }

    fn insert_params(&self) -> Vec<SqliteValue> {
        self.row_values()
    }

    fn update_params(&self) -> Vec<SqliteValue> {
        let mut values = self.field_values();
        values.push(text(self.id.clone()));
        values
    }
}

fn beads_timestamp(base_minute: usize) -> String {
    let minute = base_minute % 60;
    let hour = (base_minute / 60) % 24;
    let day = 1 + (base_minute / 1440) % 27;
    format!("2026-08-{day:02}T{hour:02}:{minute:02}:00+00:00")
}

fn beads_issue_id(rng: &mut BeadsRng, taken: &mut std::collections::HashSet<String>) -> String {
    const ALPHABET: &[u8] = b"0123456789abcdefghijklmnopqrstuvwxyz";
    loop {
        let mut id = String::from("zz-");
        for _ in 0..5 {
            id.push(char::from(ALPHABET[rng.below(ALPHABET.len())]));
        }
        if taken.insert(id.clone()) {
            return id;
        }
    }
}

fn beads_content_hash(rng: &mut BeadsRng) -> String {
    format!("{:016x}{:016x}{:016x}{:016x}", rng.next_u64(), rng.next_u64(), rng.next_u64(), rng.next_u64())
}

/// Build a corpus shaped like the reporter's workspace: ~1,200 mostly-closed
/// issues, a handful of 10-15 KiB text fields, ~380 comments whose ids are a
/// global sequence assigned in JSONL order (so they arrive non-monotonically),
/// 0-3 labels and 0-2 dependencies per issue.
fn build_beads_corpus(seed: u64, variant: u16) -> Vec<BeadsIssue> {
    let mut rng = BeadsRng(seed);
    let mut taken = std::collections::HashSet::new();
    let ids: Vec<String> = (0..BEADS_ISSUE_COUNT)
        .map(|_| beads_issue_id(&mut rng, &mut taken))
        .collect();
    let label_pool = [
        "engine", "vfs", "pager", "btree", "mvcc", "parser", "release", "perf", "docs",
        "harness", "ci", "windows",
    ];

    // Global comment ids assigned in a permuted order so file order is not
    // monotonic, exactly like a real beads JSONL (each issue carries its own
    // slice of a global AUTOINCREMENT sequence).
    let comment_slots: Vec<usize> = (0..BEADS_ISSUE_COUNT).filter(|row| row % 3 == 0).collect();
    let mut comment_ids: Vec<i64> = (1..=i64::try_from(comment_slots.len()).expect("fits"))
        .collect();
    for i in (1..comment_ids.len()).rev() {
        let j = rng.below(i + 1);
        comment_ids.swap(i, j);
    }

    let mut issues = Vec::with_capacity(BEADS_ISSUE_COUNT);
    let mut next_comment = 0usize;
    for (row, id) in ids.iter().enumerate() {
        let closed = row % 20 != 7;
        let big_description = row % 23 == 0;
        let description_len = if big_description {
            // The reporter's largest field is exactly 14,999 characters.
            if variant == 0 { 10_000 + (row % 5) * 1_250 - 1 } else { 12_500 + (row % 3) * 833 }
        } else {
            120 + (row % 17) * 140 + usize::from(variant) * 37
        };
        let close_reason_len = if closed {
            if row % 41 == 0 { 8_000 + (row % 7) * 999 } else { (row % 9) * 130 }
        } else {
            0
        };
        let description = deterministic_payload(&format!("desc{variant}"), row, description_len);
        let close_reason = deterministic_payload(&format!("close{variant}"), row, close_reason_len);
        let design = if row % 11 == 0 {
            deterministic_payload("design", row, 900 + (row % 5) * 400)
        } else {
            String::new()
        };
        let notes = if row % 13 == 0 {
            deterministic_payload(&format!("notes{variant}"), row, 300 + (row % 4) * 250)
        } else {
            String::new()
        };
        let created_at = beads_timestamp(row * 7 + 3);
        let updated_at = beads_timestamp(row * 7 + 40 + usize::from(variant) * 500);
        let closed_at = closed.then(|| beads_timestamp(row * 7 + 41 + usize::from(variant) * 500));

        let label_count = row % 4;
        let labels: Vec<String> = (0..label_count)
            .map(|k| label_pool[(row * 5 + k * 7) % label_pool.len()].to_owned())
            .collect();

        let mut dependencies = Vec::new();
        if row % 5 == 1 {
            dependencies.push((ids[(row * 37 + 11) % BEADS_ISSUE_COUNT].clone(), "blocks".to_owned()));
        }
        if row % 9 == 4 {
            dependencies.push((
                ids[(row * 53 + 29) % BEADS_ISSUE_COUNT].clone(),
                "parent-child".to_owned(),
            ));
        }
        if row % 97 == 5 {
            dependencies.push((format!("external:gh-{row}"), "blocks".to_owned()));
        }
        dependencies.retain(|(target, _)| target != id);

        let mut comments = Vec::new();
        if row % 3 == 0 {
            let comment_total = 1 + usize::from(row % 7 == 0);
            for k in 0..comment_total {
                let cid = comment_ids[next_comment % comment_ids.len()];
                next_comment += 1;
                comments.push(BeadsComment {
                    id: cid + i64::try_from(k * comment_ids.len()).expect("fits"),
                    author: if k == 0 { "ubuntu".to_owned() } else { "reviewer".to_owned() },
                    text: deterministic_payload(
                        &format!("comment{variant}"),
                        row * 10 + k,
                        90 + (row % 29) * 23,
                    ),
                    created_at: beads_timestamp(row * 7 + 20 + k),
                });
            }
        }

        issues.push(BeadsIssue {
            id: id.clone(),
            content_hash: beads_content_hash(&mut rng),
            title: format!("Synthetic beads issue {row:05} v{variant}"),
            description,
            design,
            acceptance_criteria: String::new(),
            notes,
            status: if closed { "closed".to_owned() } else { "open".to_owned() },
            priority: i64::try_from(row % 5).expect("priority fits"),
            issue_type: ["task", "bug", "feature", "epic"][row % 4].to_owned(),
            assignee: (row % 15 == 0).then(|| "ubuntu".to_owned()),
            owner: String::new(),
            estimated_minutes: (row % 31 == 0).then_some(45),
            created_at,
            created_by: "ubuntu".to_owned(),
            updated_at,
            closed_at,
            close_reason,
            closed_by_session: if closed { "session-1".to_owned() } else { String::new() },
            due_at: None,
            external_ref: (row % 61 == 3).then(|| format!("github:{}", 400 + row)),
            labels,
            dependencies,
            comments,
        });
    }
    issues
}

async fn beads_apply_schema_and_pragmas(conn: &Connection) {
    conn.execute("PRAGMA busy_timeout=5000").await.expect("busy_timeout");
    for ddl in BEADS_SCHEMA {
        conn.execute(ddl).await.unwrap_or_else(|error| panic!("schema statement failed: {error:?}\n{ddl}"));
    }
    conn.execute("PRAGMA user_version = 17").await.expect("user_version");
    conn.execute("PRAGMA journal_mode = WAL").await.expect("journal_mode");
    conn.execute("PRAGMA foreign_keys = ON").await.expect("foreign_keys");
    conn.execute("PRAGMA synchronous = NORMAL").await.expect("synchronous");
    conn.execute("PRAGMA temp_store = MEMORY").await.expect("temp_store");
    conn.execute("PRAGMA cache_size = -8000").await.expect("cache_size");
    conn.execute("PRAGMA journal_size_limit = 33554432").await.expect("journal_size_limit");
    conn.execute("PRAGMA wal_autocheckpoint = 0").await.expect("wal_autocheckpoint");
    conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").await.expect("fresh bootstrap checkpoint");
}

/// Insert an issue's relation rows the way beads 0.5.3 does and return the
/// comment rows as they actually landed.
///
/// `comment_seq` mirrors `sqlite_sequence` for `comments`: a comment with
/// `id <= 0` is inserted without an id (AUTOINCREMENT assigns `seq + 1`), an
/// explicit id that collides with an existing row is retried without an id
/// (beads' `insert_comment_for_import` fallback), and every explicit insert
/// bumps the sequence to `max(seq, id)`.
async fn beads_insert_relations(
    conn: &Connection,
    issue: &BeadsIssue,
    comment_seq: &mut i64,
) -> Vec<BeadsComment> {
    for label in &issue.labels {
        conn.execute_with_params(
            "INSERT OR IGNORE INTO labels (issue_id, label) VALUES (?, ?)",
            &[text(issue.id.clone()), text(label.clone())],
        )
        .await
        .unwrap_or_else(|error| panic!("label insert for {} failed: {error:?}", issue.id));
    }
    if !issue.dependencies.is_empty() {
        let placeholders = issue
            .dependencies
            .iter()
            .map(|_| "(?, ?, ?, ?, ?, ?, ?)")
            .collect::<Vec<_>>()
            .join(", ");
        let sql = format!(
            "INSERT OR IGNORE INTO dependencies (issue_id, depends_on_id, type, created_at, \
             created_by, metadata, thread_id) VALUES {placeholders}"
        );
        let mut params = Vec::with_capacity(issue.dependencies.len() * 7);
        for (target, dep_type) in &issue.dependencies {
            params.push(text(issue.id.clone()));
            params.push(text(target.clone()));
            params.push(text(dep_type.clone()));
            params.push(text(issue.created_at.clone()));
            params.push(text("ubuntu"));
            params.push(text("{}"));
            params.push(text(""));
        }
        conn.execute_with_params(&sql, &params)
            .await
            .unwrap_or_else(|error| panic!("dependency insert for {} failed: {error:?}", issue.id));
    }
    let mut landed = Vec::with_capacity(issue.comments.len());
    for comment in &issue.comments {
        let explicit = if comment.id > 0 {
            match conn
                .execute_with_params(
                    "INSERT INTO comments (id, issue_id, author, text, created_at) \
                     VALUES (?, ?, ?, ?, ?)",
                    &[
                        SqliteValue::Integer(comment.id),
                        text(issue.id.clone()),
                        text(comment.author.clone()),
                        text(comment.text.clone()),
                        text(comment.created_at.clone()),
                    ],
                )
                .await
            {
                Ok(_) => {
                    *comment_seq = (*comment_seq).max(comment.id);
                    Some(comment.id)
                }
                Err(
                    fsqlite_error::FrankenError::PrimaryKeyViolation
                    | fsqlite_error::FrankenError::UniqueViolation { .. },
                ) => None,
                Err(error) => {
                    panic!("comment {} insert for {} failed: {error:?}", comment.id, issue.id)
                }
            }
        } else {
            None
        };
        let id = match explicit {
            Some(id) => id,
            None => {
                conn.execute_with_params(
                    "INSERT INTO comments (issue_id, author, text, created_at) VALUES (?, ?, ?, ?)",
                    &[
                        text(issue.id.clone()),
                        text(comment.author.clone()),
                        text(comment.text.clone()),
                        text(comment.created_at.clone()),
                    ],
                )
                .await
                .unwrap_or_else(|error| {
                    panic!("id-less comment insert for {} failed: {error:?}", issue.id)
                });
                *comment_seq += 1;
                *comment_seq
            }
        };
        landed.push(BeadsComment {
            id,
            author: comment.author.clone(),
            text: comment.text.clone(),
            created_at: comment.created_at.clone(),
        });
    }
    landed
}

async fn beads_sync_relations(
    conn: &Connection,
    issue: &BeadsIssue,
    comment_seq: &mut i64,
) -> Vec<BeadsComment> {
    for sql in [
        "DELETE FROM labels WHERE issue_id = ?",
        "DELETE FROM dependencies WHERE issue_id = ?",
        "DELETE FROM comments WHERE issue_id = ?",
    ] {
        conn.execute_with_params(sql, &[text(issue.id.clone())])
            .await
            .unwrap_or_else(|error| panic!("relation delete for {} failed: {error:?}", issue.id));
    }
    beads_insert_relations(conn, issue, comment_seq).await
}

/// One `br sync --import-only` write transaction, following beads 0.5.3's
/// `stream_import_actions_in_tx` order. `fresh` selects the insert-only path
/// taken on an empty database; otherwise every issue goes through the
/// probe → UPDATE → DELETE+reinsert relation path.
/// Run one `br sync --import-only` write transaction and return the corpus
/// as it effectively landed: one entry per primary key (a duplicated id in
/// the stream is applied through beads' PK-violation → upsert fallback, so
/// the later record wins) with comment ids as AUTOINCREMENT assigned them.
async fn beads_import_transaction(
    conn: &Connection,
    issues: &[BeadsIssue],
    fresh: bool,
) -> Vec<BeadsIssue> {
    conn.execute("PRAGMA foreign_keys = OFF").await.expect("foreign_keys off");
    conn.execute("BEGIN IMMEDIATE").await.expect("begin import");
    conn.execute("DELETE FROM export_hashes").await.expect("clear export hashes");

    // Mirror `sqlite_sequence` for the AUTOINCREMENT comments table.
    let mut comment_seq = conn
        .query("SELECT seq FROM sqlite_sequence WHERE name = 'comments'")
        .await
        .expect("sqlite_sequence probe")
        .first()
        .and_then(|row| match row.values()[0] {
            SqliteValue::Integer(seq) => Some(seq),
            _ => None,
        })
        .unwrap_or(0);

    let mut effective: Vec<BeadsIssue> = Vec::with_capacity(issues.len());
    let mut effective_index: std::collections::HashMap<String, usize> =
        std::collections::HashMap::with_capacity(issues.len());
    let mut hash_batch: Vec<(String, String)> = Vec::new();
    let exported_at = beads_timestamp(9_000 + usize::from(!fresh));
    for (row, issue) in issues.iter().enumerate() {
        let seen_before = effective_index.contains_key(&issue.id);
        let landed_comments = if fresh && !seen_before {
            let inserted = match conn
                .execute_with_params(BEADS_ISSUE_INSERT, &issue.insert_params())
                .await
            {
                Ok(inserted) => {
                    assert_eq!(inserted, 1, "fresh insert must add exactly one row");
                    true
                }
                Err(
                    fsqlite_error::FrankenError::PrimaryKeyViolation
                    | fsqlite_error::FrankenError::UniqueViolation { .. },
                ) => false,
                Err(error) => panic!("fresh issue insert {row} ({}) failed: {error:?}", issue.id),
            };
            assert!(
                inserted,
                "{} is new in this stream and must not collide on a fresh database",
                issue.id
            );
            let owned = conn
                .query_with_params(
                    "SELECT EXISTS(SELECT 1 FROM labels WHERE issue_id = ?) \
                     OR EXISTS(SELECT 1 FROM dependencies WHERE issue_id = ?) \
                     OR EXISTS(SELECT 1 FROM comments WHERE issue_id = ?)",
                    &[text(issue.id.clone()), text(issue.id.clone()), text(issue.id.clone())],
                )
                .await
                .expect("owned relation probe");
            assert_eq!(
                owned[0].values()[0],
                SqliteValue::Integer(0),
                "{} must own no relations yet",
                issue.id
            );
            beads_insert_relations(conn, issue, &mut comment_seq).await
        } else {
            if fresh {
                // beads' `insert_new_import_issue`: the duplicated id fails its
                // INSERT with a PK violation (rolled back through the statement
                // savepoint) and falls back to the upsert path.
                let collided = conn
                    .execute_with_params(BEADS_ISSUE_INSERT, &issue.insert_params())
                    .await;
                assert!(
                    matches!(
                        collided,
                        Err(fsqlite_error::FrankenError::PrimaryKeyViolation
                            | fsqlite_error::FrankenError::UniqueViolation { .. })
                    ),
                    "duplicate id {} must fail its INSERT with a key violation, got {collided:?}",
                    issue.id
                );
            }
            let exists = conn
                .query_with_params(
                    "SELECT 1 FROM issues WHERE id = ? LIMIT 1",
                    &[text(issue.id.clone())],
                )
                .await
                .expect("existence probe");
            assert_eq!(exists.len(), 1, "upsert: {} must already exist", issue.id);
            let updated = conn
                .execute_with_params(BEADS_ISSUE_UPDATE, &issue.update_params())
                .await
                .unwrap_or_else(|error| panic!("issue update {row} ({}) failed: {error:?}", issue.id));
            assert_eq!(updated, 1, "upsert UPDATE for {} must touch one row", issue.id);
            beads_sync_relations(conn, issue, &mut comment_seq).await
        };
        let mut landed = issue.clone();
        landed.comments = landed_comments;
        match effective_index.get(&issue.id) {
            Some(&index) => effective[index] = landed,
            None => {
                effective_index.insert(issue.id.clone(), effective.len());
                effective.push(landed);
            }
        }

        hash_batch.push((issue.id.clone(), issue.content_hash.clone()));
        if hash_batch.len() >= BEADS_EXPORT_HASH_BATCH {
            for (issue_id, content_hash) in hash_batch.drain(..) {
                conn.execute_with_params(
                    "INSERT OR REPLACE INTO export_hashes (issue_id, content_hash, exported_at) \
                     VALUES (?, ?, ?)",
                    &[text(issue_id), text(content_hash), text(exported_at.clone())],
                )
                .await
                .expect("export hash insert");
            }
        }
    }
    for (issue_id, content_hash) in hash_batch.drain(..) {
        conn.execute_with_params(
            "INSERT OR REPLACE INTO export_hashes (issue_id, content_hash, exported_at) VALUES (?, ?, ?)",
            &[text(issue_id), text(content_hash), text(exported_at.clone())],
        )
        .await
        .expect("export hash insert");
    }

    for (table, column, filter) in [
        ("dependencies", "issue_id", " AND issue_id NOT LIKE 'external:%'"),
        ("dependencies", "depends_on_id", " AND depends_on_id NOT LIKE 'external:%'"),
        ("labels", "issue_id", ""),
        ("comments", "issue_id", ""),
        ("events", "issue_id", ""),
        ("dirty_issues", "issue_id", ""),
        ("blocked_issues_cache", "issue_id", ""),
        ("child_counters", "parent_id", ""),
    ] {
        conn.execute(&format!(
            "DELETE FROM {table} WHERE {column} NOT IN (SELECT id FROM issues){filter}"
        ))
        .await
        .unwrap_or_else(|error| panic!("orphan cleanup on {table}.{column} failed: {error:?}"));
    }

    conn.execute_with_params(
        "UPDATE blocked_issues_cache SET blocked_at = ?",
        &[text(exported_at.clone())],
    )
    .await
    .expect("blocked cache stamp");
    conn.execute("DELETE FROM blocked_issues_cache").await.expect("blocked cache clear");
    let open_ids: std::collections::HashSet<&str> = issues
        .iter()
        .filter(|issue| issue.status == "open")
        .map(|issue| issue.id.as_str())
        .collect();
    for issue in issues {
        let blockers: Vec<&str> = issue
            .dependencies
            .iter()
            .filter(|(target, dep_type)| dep_type == "blocks" && open_ids.contains(target.as_str()))
            .map(|(target, _)| target.as_str())
            .collect();
        if !blockers.is_empty() {
            conn.execute_with_params(
                "INSERT INTO blocked_issues_cache (issue_id, blocked_by, blocked_at) VALUES (?, ?, ?)",
                &[
                    text(issue.id.clone()),
                    text(format!("[\"{}\"]", blockers.join("\",\""))),
                    text(exported_at.clone()),
                ],
            )
            .await
            .expect("blocked cache insert");
        }
    }
    conn.execute("DELETE FROM child_counters").await.expect("child counters clear");
    let ids = conn.query("SELECT id FROM issues").await.expect("child counter scan");
    assert_eq!(ids.len(), issues.len(), "child counter scan must see every issue");

    for (key, value) in [
        ("last_import_time", exported_at.clone()),
        ("jsonl_content_hash", format!("sha256:{}", issues.len())),
        ("last_observed_jsonl", exported_at.clone()),
    ] {
        conn.execute_with_params("DELETE FROM metadata WHERE key = ?", &[text(key)])
            .await
            .expect("metadata delete");
        conn.execute_with_params(
            "INSERT INTO metadata (key, value) VALUES (?, ?)",
            &[text(key), text(value)],
        )
        .await
        .expect("metadata insert");
    }
    conn.execute("COMMIT").await.expect("commit import");
    conn.execute("PRAGMA foreign_keys = ON").await.expect("foreign_keys on");
    let violations = conn.query("PRAGMA foreign_key_check").await.expect("foreign_key_check");
    assert!(violations.is_empty(), "post-import foreign_key_check reported {violations:?}");
    conn.execute("PRAGMA wal_checkpoint(PASSIVE)").await.expect("passive checkpoint");
    effective
}

async fn beads_verify_with_fsqlite(conn: &Connection, issues: &[BeadsIssue], phase: &str) {
    let integrity = conn.query("PRAGMA integrity_check").await.expect("integrity_check");
    assert_eq!(
        integrity.len(),
        1,
        "[{phase}] FrankenSQLite integrity_check returned {integrity:?}"
    );
    assert_eq!(integrity[0].values()[0], text("ok"), "[{phase}] FrankenSQLite integrity_check");

    let count = conn.query("SELECT COUNT(*) FROM issues").await.expect("count");
    assert_eq!(count[0].values()[0], SqliteValue::Integer(issues.len() as i64), "[{phase}] issue count");

    let select = format!("SELECT {BEADS_ISSUE_COLUMNS} FROM issues WHERE id = ?");
    let mut expected_comments: Vec<(i64, String, String, String, String)> = Vec::new();
    let mut expected_labels: Vec<(String, String)> = Vec::new();
    let mut expected_deps: Vec<(String, String, String)> = Vec::new();
    for issue in issues {
        let rows = conn
            .query_with_params(&select, &[text(issue.id.clone())])
            .await
            .unwrap_or_else(|error| panic!("[{phase}] point lookup {} failed: {error:?}", issue.id));
        assert_eq!(rows.len(), 1, "[{phase}] source id {} must be addressable exactly once", issue.id);
        let expected = issue.row_values();
        let actual = rows[0].values();
        assert_eq!(actual.len(), expected.len(), "[{phase}] column count for {}", issue.id);
        for (index, (got, want)) in actual.iter().zip(&expected).enumerate() {
            assert!(
                got == want,
                "[{phase}] issue {} column #{index} mismatch: got {:?}, want {:?}",
                issue.id,
                summarize_value(got),
                summarize_value(want)
            );
        }
        for comment in &issue.comments {
            expected_comments.push((
                comment.id,
                issue.id.clone(),
                comment.author.clone(),
                comment.text.clone(),
                comment.created_at.clone(),
            ));
        }
        for label in &issue.labels {
            expected_labels.push((issue.id.clone(), label.clone()));
        }
        for (target, dep_type) in &issue.dependencies {
            expected_deps.push((issue.id.clone(), target.clone(), dep_type.clone()));
        }
    }

    expected_comments.sort();
    let comments = conn
        .query("SELECT id, issue_id, author, text, created_at FROM comments ORDER BY id")
        .await
        .expect("comments");
    assert_eq!(comments.len(), expected_comments.len(), "[{phase}] comment row count");
    for (row, expected) in comments.iter().zip(&expected_comments) {
        assert_eq!(
            row.values(),
            &[
                SqliteValue::Integer(expected.0),
                text(expected.1.clone()),
                text(expected.2.clone()),
                text(expected.3.clone()),
                text(expected.4.clone()),
            ],
            "[{phase}] comment {} mismatch",
            expected.0
        );
    }
    let by_issue_index = conn
        .query("SELECT COUNT(*) FROM comments WHERE issue_id IN (SELECT id FROM issues)")
        .await
        .expect("comment index walk");
    assert_eq!(by_issue_index[0].values()[0], SqliteValue::Integer(expected_comments.len() as i64), "[{phase}] idx_comments_issue-driven count");

    expected_labels.sort();
    let labels = conn
        .query("SELECT issue_id, label FROM labels ORDER BY issue_id, label")
        .await
        .expect("labels");
    assert_eq!(labels.len(), expected_labels.len(), "[{phase}] label row count");
    for (row, expected) in labels.iter().zip(&expected_labels) {
        assert_eq!(row.values(), &[text(expected.0.clone()), text(expected.1.clone())], "[{phase}] label mismatch");
    }

    expected_deps.sort();
    let deps = conn
        .query("SELECT issue_id, depends_on_id, type FROM dependencies ORDER BY issue_id, depends_on_id, type")
        .await
        .expect("dependencies");
    assert_eq!(deps.len(), expected_deps.len(), "[{phase}] dependency row count");
    for (row, expected) in deps.iter().zip(&expected_deps) {
        assert_eq!(
            row.values(),
            &[text(expected.0.clone()), text(expected.1.clone()), text(expected.2.clone())],
            "[{phase}] dependency mismatch"
        );
    }

    let hashes = conn.query("SELECT COUNT(*) FROM export_hashes").await.expect("export hashes");
    assert_eq!(hashes[0].values()[0], SqliteValue::Integer(issues.len() as i64), "[{phase}] export hash count");
}

fn summarize_value(value: &SqliteValue) -> String {
    match value {
        SqliteValue::Text(s) if s.len() > 48 => format!("Text(len={}, head={:?})", s.len(), &s[..32]),
        other => format!("{other:?}"),
    }
}

fn beads_verify_with_stock_sqlite(db_path: &std::path::Path, issues: &[BeadsIssue], phase: &str) {
    let sqlite = rusqlite::Connection::open(db_path).expect("stock SQLite open");
    let mut stmt = sqlite.prepare("PRAGMA integrity_check").expect("prepare integrity_check");
    let report: Vec<String> = stmt
        .query_map([], |row| row.get::<_, String>(0))
        .expect("run integrity_check")
        .map(|row| row.expect("integrity row"))
        .collect();
    assert_eq!(report, vec!["ok".to_owned()], "[{phase}] stock SQLite integrity_check");
    drop(stmt);

    let issue_count: i64 = sqlite
        .query_row("SELECT COUNT(*) FROM issues", [], |row| row.get(0))
        .expect("stock issue count");
    assert_eq!(issue_count, issues.len() as i64, "[{phase}] stock issue count");
    let mut point = sqlite
        .prepare("SELECT title, description, close_reason, created_by, status FROM issues WHERE id = ?1")
        .expect("prepare point lookup");
    for issue in issues {
        let row: (String, String, String, String, String) = point
            .query_row([issue.id.as_str()], |row| {
                Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?, row.get(4)?))
            })
            .unwrap_or_else(|error| panic!("[{phase}] stock lookup of {} failed: {error}", issue.id));
        assert_eq!(row.0, issue.title, "[{phase}] stock title for {}", issue.id);
        assert_eq!(row.1, issue.description, "[{phase}] stock description for {}", issue.id);
        assert_eq!(row.2, issue.close_reason, "[{phase}] stock close_reason for {}", issue.id);
        assert_eq!(row.3, issue.created_by, "[{phase}] stock created_by for {}", issue.id);
        assert_eq!(row.4, issue.status, "[{phase}] stock status for {}", issue.id);
    }
    drop(point);
    let blank_ids: i64 = sqlite
        .query_row("SELECT COUNT(*) FROM issues WHERE id = '' OR id IS NULL", [], |row| row.get(0))
        .expect("blank id scan");
    assert_eq!(blank_ids, 0, "[{phase}] field-shifted issue rows with a blank id");
    let duplicate_comments: i64 = sqlite
        .query_row(
            "SELECT COUNT(*) FROM (SELECT id FROM comments GROUP BY id HAVING COUNT(*) > 1)",
            [],
            |row| row.get(0),
        )
        .expect("duplicate comment scan");
    assert_eq!(duplicate_comments, 0, "[{phase}] byte-identical repeated comment ids");
    let expected_comments: i64 = issues.iter().map(|issue| issue.comments.len() as i64).sum();
    let stock_comments: i64 = sqlite
        .query_row("SELECT COUNT(*) FROM comments", [], |row| row.get(0))
        .expect("stock comment count");
    assert_eq!(stock_comments, expected_comments, "[{phase}] stock comment count");
}

#[test]
fn beads_rust_import_sequence_round_trips_and_keeps_unique_page_ownership() {
    asupersync::test_utils::run_test(|| async {
        let dir = tempfile::tempdir().expect("temp dir");
        let db_path = dir.path().join("beads.db");
        let db = db_path.to_string_lossy().into_owned();

        // ── Fresh rebuild: `br` opens an empty file, applies SCHEMA_SQL, then
        //    imports every JSONL record in one BEGIN IMMEDIATE transaction. ──
        let corpus_v0 = build_beads_corpus(0x9E37_79B9_7F4A_7C15, 0);
        let conn = Connection::open(&db).await.expect("open fresh");
        beads_apply_schema_and_pragmas(&conn).await;
        let landed_v0 = beads_import_transaction(&conn, &corpus_v0, true).await;
        beads_verify_with_fsqlite(&conn, &landed_v0, "fresh-import").await;
        drop(conn);
        beads_verify_with_stock_sqlite(&db_path, &landed_v0, "fresh-import");

        // ── Re-import onto the existing database (the reporter's earlier
        //    occurrences): every row is UPDATEd with resized overflow payloads
        //    and every relation row is DELETEd and reinserted. ──
        let corpus_v1 = build_beads_corpus(0x9E37_79B9_7F4A_7C15, 1);
        assert_eq!(
            corpus_v1.iter().map(|issue| issue.id.as_str()).collect::<Vec<_>>(),
            corpus_v0.iter().map(|issue| issue.id.as_str()).collect::<Vec<_>>(),
            "re-import corpus must keep the same primary keys"
        );
        let conn = Connection::open(&db).await.expect("reopen existing");
        conn.execute("PRAGMA busy_timeout=5000").await.expect("busy_timeout");
        conn.execute("PRAGMA foreign_keys = ON").await.expect("foreign_keys");
        conn.execute("PRAGMA wal_autocheckpoint = 0").await.expect("wal_autocheckpoint");
        let landed_v1 = beads_import_transaction(&conn, &corpus_v1, false).await;
        beads_verify_with_fsqlite(&conn, &landed_v1, "re-import").await;
        drop(conn);
        beads_verify_with_stock_sqlite(&db_path, &landed_v1, "re-import");

        // ── One more fresh-shaped pass on the now-churned file: freelist
        //    pages from the rewrite are reused by new overflow chains. ──
        let corpus_v2 = build_beads_corpus(0x9E37_79B9_7F4A_7C15, 2);
        let conn = Connection::open(&db).await.expect("reopen churned");
        conn.execute("PRAGMA busy_timeout=5000").await.expect("busy_timeout");
        conn.execute("PRAGMA foreign_keys = ON").await.expect("foreign_keys");
        conn.execute("PRAGMA wal_autocheckpoint = 0").await.expect("wal_autocheckpoint");
        let landed_v2 = beads_import_transaction(&conn, &corpus_v2, false).await;
        beads_verify_with_fsqlite(&conn, &landed_v2, "churned-re-import").await;
        conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").await.expect("final checkpoint");
        drop(conn);
        beads_verify_with_stock_sqlite(&db_path, &landed_v2, "churned-re-import");
    });
}

/// Shape a corpus the way merge-scarred beads JSONL files arrive: some ids
/// appear twice in the stream (the later record must win through beads'
/// PK-violation → upsert fallback, which rolls the failed INSERT back through
/// the statement savepoint and then DELETE+reinserts every relation row),
/// some comments carry no id (legacy records: AUTOINCREMENT assigns one), and
/// some explicit comment ids collide with values AUTOINCREMENT already handed
/// out (beads retries those without an id).
fn with_beads_collisions(mut issues: Vec<BeadsIssue>, variant: u16) -> Vec<BeadsIssue> {
    let original_len = issues.len();
    for (row, issue) in issues.iter_mut().enumerate() {
        for (k, comment) in issue.comments.iter_mut().enumerate() {
            if row % 9 == 0 {
                // Legacy comment without an id.
                comment.id = 0;
            } else if row % 11 == 0 {
                // Explicit id sitting in the low range AUTOINCREMENT will
                // reach after the id-less inserts above: a real collision.
                comment.id = 1 + i64::try_from((row / 11 + k) % 40).expect("fits");
            }
        }
    }
    for row in (0..original_len).filter(|row| row % 37 == 5) {
        let mut duplicate = issues[row].clone();
        duplicate.description = deterministic_payload(
            &format!("dup{variant}"),
            row,
            if row % 74 == 5 { 14_999 } else { 700 + (row % 5) * 900 },
        );
        duplicate.close_reason = if duplicate.status == "closed" {
            deterministic_payload(&format!("dupclose{variant}"), row, 9_500 + (row % 3) * 1_100)
        } else {
            String::new()
        };
        duplicate.title = format!("{} (merged copy)", duplicate.title);
        duplicate.updated_at = beads_timestamp(row * 7 + 90 + usize::from(variant) * 500);
        duplicate.labels.push("merged".to_owned());
        for (k, comment) in duplicate.comments.iter_mut().enumerate() {
            comment.text = deterministic_payload(
                &format!("dupcomment{variant}"),
                row * 10 + k,
                140 + (row % 13) * 31,
            );
        }
        issues.push(duplicate);
    }
    issues
}

/// GH #399: the same import with beads' real fallback shapes — duplicated
/// issue ids, id-less comments, and colliding explicit comment ids — so the
/// statement-savepoint rollback, AUTOINCREMENT, and DELETE+reinsert paths
/// interleave with large overflow allocations inside one transaction.
#[test]
fn beads_rust_import_with_key_collisions_round_trips_and_keeps_unique_page_ownership() {
    asupersync::test_utils::run_test(|| async {
        let dir = tempfile::tempdir().expect("temp dir");
        let db_path = dir.path().join("beads-collisions.db");
        let db = db_path.to_string_lossy().into_owned();

        let corpus_v0 = with_beads_collisions(build_beads_corpus(0x5851_F42D_4C95_7F2D, 0), 0);
        assert!(
            corpus_v0.len() > BEADS_ISSUE_COUNT,
            "collision corpus must carry duplicated ids"
        );
        let conn = Connection::open(&db).await.expect("open fresh");
        beads_apply_schema_and_pragmas(&conn).await;
        let landed_v0 = beads_import_transaction(&conn, &corpus_v0, true).await;
        assert_eq!(landed_v0.len(), BEADS_ISSUE_COUNT, "duplicates collapse onto their id");
        assert!(
            landed_v0
                .iter()
                .any(|issue| issue.title.ends_with("(merged copy)")),
            "the later duplicate record must win"
        );
        beads_verify_with_fsqlite(&conn, &landed_v0, "collisions-fresh").await;
        drop(conn);
        beads_verify_with_stock_sqlite(&db_path, &landed_v0, "collisions-fresh");

        let corpus_v1 = with_beads_collisions(build_beads_corpus(0x5851_F42D_4C95_7F2D, 1), 1);
        let conn = Connection::open(&db).await.expect("reopen existing");
        conn.execute("PRAGMA busy_timeout=5000").await.expect("busy_timeout");
        conn.execute("PRAGMA foreign_keys = ON").await.expect("foreign_keys");
        conn.execute("PRAGMA wal_autocheckpoint = 0").await.expect("wal_autocheckpoint");
        let landed_v1 = beads_import_transaction(&conn, &corpus_v1, false).await;
        beads_verify_with_fsqlite(&conn, &landed_v1, "collisions-re-import").await;
        conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").await.expect("final checkpoint");
        drop(conn);
        beads_verify_with_stock_sqlite(&db_path, &landed_v1, "collisions-re-import");
    });
}
