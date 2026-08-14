//! bd-vacuum-desc-index-self-reject-y2aog: VACUUM INTO must not emit a
//! candidate that fsqlite's own integrity checker rejects.
//!
//! Downstream shape (hfdt store): a composite index whose SECOND key is DESC
//! (`CREATE INDEX ... ON source_handles(provider_subject_id, known_at DESC)`),
//! TEXT data, no NULLs. fsqlite's VACUUM INTO rewrote the index in an order
//! its own checker then rejected ("entries are out of order for their
//! declared key directions"), while stock sqlite3 called the same candidate
//! consistent. Writer and checker must agree on DESC key direction.

use fsqlite::{Connection, SqliteValue};

fn scratch_path(tag: &str) -> std::path::PathBuf {
    std::env::temp_dir().join(format!(
        "y2aog-{tag}-{}-{}.db",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ))
}

fn first_text(rows: &[fsqlite::Row]) -> String {
    match &rows[0].values()[0] {
        SqliteValue::Text(s) => s.as_ref().to_string(),
        other => panic!("unexpected value: {other:?}"),
    }
}

#[test]
fn vacuum_into_desc_composite_index_candidate_passes_own_integrity_check() {
    asupersync::test_utils::run_test(move || async move {
        let src = scratch_path("src");
        let dst = scratch_path("dst");
        let conn = Connection::open(src.to_str().unwrap())
            .await
            .expect("open source");

        conn.execute(
            "CREATE TABLE source_handles (
                id INTEGER PRIMARY KEY,
                provider_subject_id TEXT NOT NULL,
                known_at TEXT NOT NULL
            );
            CREATE INDEX idx_source_handles_provider_subject_id
                ON source_handles(provider_subject_id, known_at DESC);",
        )
        .await
        .expect("create schema");

        // 48 rows like the hfdt repro: repeated provider ids so the DESC
        // second column actually orders within equal first keys.
        for i in 0..48 {
            conn.execute(&format!(
                "INSERT INTO source_handles (provider_subject_id, known_at)
                 VALUES ('provider-{}', '2026-07-{:02}T{:02}:00:00Z');",
                i % 6,
                (i % 28) + 1,
                i % 24
            ))
            .await
            .expect("insert row");
        }

        let src_check = conn
            .query("PRAGMA integrity_check;")
            .await
            .expect("source integrity_check");
        assert_eq!(first_text(&src_check), "ok", "source must be healthy");

        conn.execute(&format!("VACUUM INTO '{}';", dst.to_str().unwrap()))
            .await
            .expect("VACUUM INTO must succeed from a healthy source (bd-y2aog)");

        let cand = Connection::open(dst.to_str().unwrap())
            .await
            .expect("open candidate");
        let cand_check = cand
            .query("PRAGMA integrity_check;")
            .await
            .expect("candidate integrity_check");
        assert_eq!(
            first_text(&cand_check),
            "ok",
            "fsqlite's checker must accept fsqlite's own VACUUM INTO output (bd-y2aog)"
        );

        let rows = cand
            .query("SELECT COUNT(*) FROM source_handles;")
            .await
            .expect("count candidate rows");
        match &rows[0].values()[0] {
            SqliteValue::Integer(48) => {}
            other => panic!("candidate lost rows: {other:?}"),
        }

        let _ = std::fs::remove_file(&src);
        let _ = std::fs::remove_file(&dst);
    });
}
