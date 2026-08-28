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
