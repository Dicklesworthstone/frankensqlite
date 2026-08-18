//! bd-85x9y / GH#364 — a stale parallel-WAL commit-certificate handoff
//! (`-wal-cert-head`) must not be replayed across a database-FILE replacement.
//!
//! # The bug
//!
//! `checkpoint_certificate_handoff` returned the certificate carried by a
//! `-wal-cert-head` sidecar with **no binding to the database file it was
//! written for**. When a database file was replaced by a fresh, smaller file at
//! the same path while the stale sidecar survived, the old file's committed
//! `db_size_pages` was applied as a floor, re-extending the fresh database to
//! the replaced file's page count — an orphaned-page spray (46 MB → 378 MB in
//! the field report), and each subsequent commit re-wrote the inflated size.
//!
//! # The fix
//!
//! Every commit certificate is now bound to the creating database's 16-byte
//! creation-stable identity (page-1 header bytes 76..92). A certificate bound to
//! a *different* identity is treated as absent, so it can neither re-extend nor
//! corrupt the fresh database.
//!
//! # Durability guardrail
//!
//! The certificate's `db_size_pages` is a FLOOR that prevents truncation of
//! committed growth. Wrongly rejecting a valid SAME-FILE certificate would let a
//! checkpoint truncate committed pages = data loss. These tests therefore also
//! prove that a same-file reopen and a committed-but-uncheckpointed WAL are
//! recovered intact.

use std::fs;
use std::path::{Path, PathBuf};

use fsqlite::Connection;
use fsqlite_types::SqliteValue;

// ── helpers ────────────────────────────────────────────────────────────────

async fn open(path: &Path) -> Connection {
    Connection::open(path.to_str().expect("utf-8 path"))
        .await
        .unwrap_or_else(|e| panic!("open {}: {e}", path.display()))
}

async fn exec(conn: &Connection, sql: &str) {
    conn.execute(sql)
        .await
        .unwrap_or_else(|e| panic!("execute `{sql}`: {e}"));
}

async fn scalar_i64(conn: &Connection, sql: &str) -> i64 {
    let rows = conn
        .query(sql)
        .await
        .unwrap_or_else(|e| panic!("query `{sql}`: {e}"));
    match rows.first().map(fsqlite::Row::values) {
        Some([SqliteValue::Integer(n), ..]) => *n,
        other => panic!("expected integer scalar from `{sql}`, got {other:?}"),
    }
}

async fn scalar_text(conn: &Connection, sql: &str) -> String {
    let rows = conn
        .query(sql)
        .await
        .unwrap_or_else(|e| panic!("query `{sql}`: {e}"));
    match rows.first().map(fsqlite::Row::values) {
        Some([SqliteValue::Text(s), ..]) => s.to_string(),
        other => panic!("expected text scalar from `{sql}`, got {other:?}"),
    }
}

async fn page_count(conn: &Connection) -> i64 {
    scalar_i64(conn, "PRAGMA page_count;").await
}

async fn integrity_ok(conn: &Connection) -> bool {
    scalar_text(conn, "PRAGMA integrity_check;").await == "ok"
}

fn sidecar_path(db: &Path, suffix: &str) -> PathBuf {
    let mut p = db.as_os_str().to_owned();
    p.push(suffix);
    PathBuf::from(p)
}

/// Read the 16-byte creation-stable identity a database stamped into page-1
/// header bytes 76..92 (bd-85x9y / GH#364).
fn read_db_file_id(db: &Path) -> [u8; 16] {
    let bytes = fs::read(db).unwrap_or_else(|e| panic!("read {}: {e}", db.display()));
    assert!(bytes.len() >= 92, "db file too small to hold a header");
    let mut id = [0u8; 16];
    id.copy_from_slice(&bytes[76..92]);
    id
}

/// Build a real, LARGE durable certificate by driving parallel-WAL commits in a
/// throwaway "old" database (the file that is later replaced), then return the
/// newest sidecar record re-serialized as a stand-alone `-wal-cert-head` handoff
/// — its own committed `db_size_pages` intact — together with that record's
/// bound identity. This is the exact shape of a sidecar left behind by a
/// replaced file.
///
/// The record is captured from `-wal-cert` BEFORE any `wal_checkpoint(TRUNCATE)`
/// (which would reset the per-generation sidecar) so a large, authorized record
/// is guaranteed to be present.
async fn stale_handoff_from_replaced_file(dir: &Path) -> (Vec<u8>, [u8; 16], i64) {
    let old_db = dir.join("replaced_old.db");
    let stale_bytes;
    {
        let conn = open(&old_db).await;
        exec(&conn, "PRAGMA journal_mode=WAL;").await;
        exec(&conn, "CREATE TABLE big (id INTEGER PRIMARY KEY, payload TEXT);").await;
        // Grow the old file well past a handful of pages so a re-extension to
        // its size would be unmistakable. ~400 rows of ~900-byte payload lands
        // near ~100 database pages.
        for row in 0..400_i64 {
            exec(
                &conn,
                &format!("INSERT INTO big VALUES ({row}, '{}');", "x".repeat(900)),
            )
            .await;
        }
        // Capture the newest durable record while the per-generation sidecar is
        // still intact (before any checkpoint reset).
        let cert_path = sidecar_path(&old_db, "-wal-cert");
        let records = fsqlite_wal::decode_parallel_wal_durable_certificate_records(
            &fs::read(&cert_path).expect("read old -wal-cert sidecar"),
        )
        .expect("decode old durable certificate sidecar");
        let newest = records
            .last()
            .expect("old sidecar must carry at least one durable record")
            .clone();
        stale_bytes = newest.to_bytes();
        // Match the field repro: a TRUNCATE checkpoint also publishes the
        // `-wal-cert-head` handoff anchor. It is not required for the synthetic
        // plant below, but exercises the natural production path.
        exec(&conn, "PRAGMA wal_checkpoint(TRUNCATE);").await;
        conn.close_without_checkpoint()
            .await
            .expect("close old db without resetting its certificate-bound WAL");
    }

    let old_id = read_db_file_id(&old_db);
    let record = fsqlite_wal::ParallelWalDurableCertificateRecord::from_bytes(&stale_bytes)
        .expect("re-decode captured stale record");
    assert_eq!(
        record.db_file_id, old_id,
        "the old file's certificate must be bound to the old file's identity"
    );
    let inflated_pages = i64::from(record.certificate.db_size_pages);
    assert!(
        inflated_pages > 40,
        "old certificate must carry a large committed db size (got {inflated_pages})"
    );
    (stale_bytes, old_id, inflated_pages)
}

// ── the regression ───────────────────────────────────────────────────────────

#[test]
fn gh364_stale_handoff_from_replaced_file_does_not_reextend_fresh_db() {
    asupersync::test_utils::run_test(|| async {
        let dir = tempfile::tempdir().unwrap();

        // A stale, LARGE handoff bound to a REPLACED file's identity.
        let (stale_handoff, old_id, inflated_pages) = stale_handoff_from_replaced_file(dir.path()).await;

        // A fresh, SMALL database created at a different path (its own new id).
        let fresh_db = dir.path().join("fresh_small.db");
        let fresh_pages_before;
        let fresh_id;
        {
            let conn = open(&fresh_db).await;
            exec(&conn, "PRAGMA journal_mode=WAL;").await;
            exec(&conn, "CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT);").await;
            exec(&conn, "INSERT INTO t VALUES (1, 'a');").await;
            exec(&conn, "PRAGMA wal_checkpoint(TRUNCATE);").await;
            fresh_pages_before = page_count(&conn).await;
            conn.close_without_checkpoint()
                .await
                .expect("close fresh db");
        }
        fresh_id = read_db_file_id(&fresh_db);
        assert_ne!(
            fresh_id, old_id,
            "a freshly created database must have a distinct creation-stable identity"
        );
        assert_ne!(fresh_id, [0u8; 16], "the fresh database must be stamped");
        assert!(
            fresh_pages_before < inflated_pages,
            "fresh db ({fresh_pages_before} pages) must start far smaller than the stale \
             certificate's db size ({inflated_pages} pages)"
        );

        // Plant the stale foreign handoff sidecar onto the fresh database, exactly
        // as a database-file replacement would leave it behind.
        fs::write(sidecar_path(&fresh_db, "-wal-cert-head"), &stale_handoff)
            .expect("plant stale -wal-cert-head");

        // Reopen and do light writes. Each commit consults the durable
        // certificate floor; a stale FOREIGN certificate must be ignored.
        let fresh_pages_after;
        {
            let conn = open(&fresh_db).await;
            for row in 2..12_i64 {
                exec(&conn, &format!("INSERT INTO t VALUES ({row}, 'r{row}');")).await;
            }
            fresh_pages_after = page_count(&conn).await;
            assert!(
                integrity_ok(&conn).await,
                "fresh database must pass integrity_check after light writes"
            );
            conn.close_without_checkpoint()
                .await
                .expect("close fresh db after writes");
        }

        // The core assertion: the fresh database was NOT re-extended toward the
        // replaced file's page count. A handful of tiny inserts adds a page or
        // two at most — never a jump toward the inflated size.
        assert!(
            fresh_pages_after < inflated_pages,
            "GH#364 regression: fresh db re-extended to {fresh_pages_after} pages toward the \
             stale certificate's {inflated_pages} pages"
        );
        assert!(
            fresh_pages_after <= fresh_pages_before + 8,
            "fresh db grew unexpectedly ({fresh_pages_before} -> {fresh_pages_after} pages) — \
             the stale foreign certificate leaked its db_size floor"
        );

        // Stock SQLite must agree the file is healthy with no orphaned pages.
        let oracle = rusqlite::Connection::open(&fresh_db).expect("stock reopen of fresh db");
        let integrity: String = oracle
            .query_row("PRAGMA integrity_check;", [], |r| r.get(0))
            .expect("stock integrity_check");
        assert_eq!(
            integrity, "ok",
            "stock SQLite integrity_check must be ok (zero 'never used' pages)"
        );
    });
}

// ── durability guardrail: valid SAME-FILE certificates are never dropped ─────

#[test]
fn gh364_same_file_reopen_preserves_committed_growth() {
    // Guardrail (a): a database's own certificate (identity matches) must still
    // be honored across a plain reopen — its committed growth survives.
    asupersync::test_utils::run_test(|| async {
        let dir = tempfile::tempdir().unwrap();
        let db = dir.path().join("same_file.db");

        let committed_rows = 250_i64;
        let pages_before;
        {
            let conn = open(&db).await;
            exec(&conn, "PRAGMA journal_mode=WAL;").await;
            exec(&conn, "CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT);").await;
            for row in 0..committed_rows {
                exec(
                    &conn,
                    &format!("INSERT INTO t VALUES ({row}, '{}');", "y".repeat(120)),
                )
                .await;
            }
            pages_before = page_count(&conn).await;
            // Close WITHOUT checkpoint: the committed growth lives in the
            // certificate-bound WAL, exactly where the db_size floor matters.
            conn.close_without_checkpoint()
                .await
                .expect("close same-file db without checkpoint");
        }
        assert!(pages_before > 10, "table should have grown ({pages_before})");

        // Reopen the SAME file (identity matches → certificate applies).
        {
            let conn = open(&db).await;
            let rows = scalar_i64(&conn, "SELECT COUNT(*) FROM t;").await;
            assert_eq!(
                rows, committed_rows,
                "same-file reopen must recover every committed row"
            );
            let pages_after = page_count(&conn).await;
            assert!(
                pages_after >= pages_before,
                "same-file reopen must not shrink committed growth ({pages_before} -> {pages_after})"
            );
            assert!(
                integrity_ok(&conn).await,
                "same-file reopen must pass integrity_check"
            );
            conn.close_without_checkpoint()
                .await
                .expect("close same-file db after verification");
        }
    });
}

#[test]
fn gh364_committed_uncheckpointed_wal_recovers_intact() {
    // Guardrail (b): committed-but-uncheckpointed data (a valid same-file
    // certificate) must be recovered on reopen — the fix must not drop it.
    asupersync::test_utils::run_test(|| async {
        let dir = tempfile::tempdir().unwrap();
        let db = dir.path().join("uncheckpointed.db");

        let committed_rows = 64_i64;
        {
            let conn = open(&db).await;
            exec(&conn, "PRAGMA journal_mode=WAL;").await;
            exec(&conn, "CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER);").await;
            for row in 0..committed_rows {
                exec(&conn, &format!("INSERT INTO t VALUES ({row}, {});", row * 7)).await;
            }
            // Simulate a crash before checkpoint: drop the WAL on disk, no reset.
            conn.close_without_checkpoint()
                .await
                .expect("close before checkpoint");
        }

        {
            let conn = open(&db).await;
            let rows = scalar_i64(&conn, "SELECT COUNT(*) FROM t;").await;
            assert_eq!(
                rows, committed_rows,
                "committed-but-uncheckpointed rows must survive reopen"
            );
            let sum = scalar_i64(&conn, "SELECT COALESCE(SUM(v), 0) FROM t;").await;
            let expected: i64 = (0..committed_rows).map(|r| r * 7).sum();
            assert_eq!(sum, expected, "recovered row values must be intact");
            assert!(
                page_count(&conn).await >= 2,
                "recovered database must retain its committed pages"
            );
            assert!(integrity_ok(&conn).await, "recovered database integrity");
            conn.close_without_checkpoint()
                .await
                .expect("close after recovery");
        }

        // Stock SQLite oracle agrees the recovered file is healthy.
        let oracle = rusqlite::Connection::open(&db).expect("stock reopen");
        let integrity: String = oracle
            .query_row("PRAGMA integrity_check;", [], |r| r.get(0))
            .expect("stock integrity_check");
        assert_eq!(integrity, "ok", "stock integrity_check after recovery");
        let count: i64 = oracle
            .query_row("SELECT COUNT(*) FROM t;", [], |r| r.get(0))
            .expect("stock count");
        assert_eq!(count, committed_rows, "stock reader sees every committed row");
    });
}
