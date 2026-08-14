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
    if i % 4 == 0 {
        text(format!("ext-{i:05}"))
    } else {
        SqliteValue::Null
    }
}

async fn probe_exists(conn: &Connection, id: &str) -> bool {
    !conn
        .query_with_params(PROBE, &[text(id)])
        .await
        .unwrap_or_else(|e| panic!("existence probe failed for {id}: {e:?}"))
        .is_empty()
}

// bd-fcof5: KNOWN-RED reproduction anchor. A single-writer file-backed
// BEGIN IMMEDIATE bulk import (~3200 rows, TEXT PK + partial indexes)
// deterministically fails COMMIT with a self-conflict
// `BusySnapshot { conflicting_pages: "8,9,10" }` — the o81ov cross-connection
// EOF-alias guard (wal_adapter.rs conflicting_pages_since_snapshot) flags the
// transaction's OWN spilled fresh pages as peer aliases. Un-ignore once the
// guard learns to exclude the committing transaction's own frames.
#[ignore = "bd-fcof5: reproduces the file-backed bulk-import BusySnapshot self-conflict; un-ignore when the alias-guard fix lands"]
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
