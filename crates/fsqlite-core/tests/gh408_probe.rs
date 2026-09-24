#![recursion_limit = "512"]

//! Scratch measurement probe for GH#408 (not a keeper).

use fsqlite_core::connection::{
    Connection, hot_path_profile_snapshot, reset_hot_path_profile, set_hot_path_profile_enabled,
};
use std::time::Instant;

async fn probe(rows: usize, external: bool) -> (u64, u64, u64, f64) {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("p.db");
    let db_str = db_path.to_string_lossy().into_owned();
    let conn = Connection::open(&db_str).await.unwrap();
    conn.execute("CREATE TABLE msgs(id INTEGER PRIMARY KEY, t TEXT);")
        .await
        .unwrap();
    conn.execute("CREATE TABLE src(id INTEGER PRIMARY KEY, body TEXT);")
        .await
        .unwrap();
    if external {
        conn.execute("CREATE VIRTUAL TABLE f USING fts5(body, content='src', content_rowid='id');")
            .await
            .unwrap();
    } else {
        conn.execute("CREATE VIRTUAL TABLE f USING fts5(body, content='');")
            .await
            .unwrap();
    }
    conn.execute("BEGIN;").await.unwrap();
    let mut id = 0;
    while id < rows {
        let mut sql = String::from("INSERT INTO f(rowid, body) VALUES ");
        let mut src_sql = String::from("INSERT INTO src(id, body) VALUES ");
        for k in 0..100 {
            id += 1;
            if k > 0 {
                sql.push(',');
                src_sql.push(',');
            }
            sql.push_str(&format!("({id}, 'common corpus token{id}')"));
            src_sql.push_str(&format!("({id}, 'common corpus token{id}')"));
        }
        sql.push(';');
        src_sql.push(';');
        if external {
            conn.execute(&src_sql).await.unwrap();
        }
        conn.execute(&sql).await.unwrap();
    }
    conn.execute("COMMIT;").await.unwrap();

    reset_hot_path_profile();
    let rebuilds_before = conn.fts5_reload_rebuild_count();
    const K: usize = 8;
    let started = Instant::now();
    for i in 0..K {
        conn.execute("BEGIN;").await.unwrap();
        conn.execute(&format!(
            "INSERT INTO msgs(id, t) VALUES ({}, 'x');",
            i + 1
        ))
        .await
        .unwrap();
        conn.execute("COMMIT;").await.unwrap();
        conn.query("SELECT rowid FROM f WHERE f MATCH 'token7';")
            .await
            .unwrap();
    }
    let elapsed = started.elapsed().as_secs_f64() / K as f64;
    let snap = hot_path_profile_snapshot();
    let rebuilds = conn.fts5_reload_rebuild_count() - rebuilds_before;
    conn.close().await.unwrap();
    (
        snap.fts5_reload_documents_retokenized + snap.fts5_reload_shadow_rows_decoded,
        snap.fts5_reload_lazy_binds,
        rebuilds,
        elapsed,
    )
}

#[test]
fn gh408_probe_scaling() {
    asupersync::test_utils::run_test(|| async {
        set_hot_path_profile_enabled(true);
        for external in [false, true] {
            for n in [2000usize, 8000] {
                let (hydrated, lazy, rebuilds, secs) = probe(n, external).await;
                eprintln!(
                    "external={external} rows={n} hydrated_rows={hydrated} lazy_binds={lazy} \
                     reload_rebuilds={rebuilds} per_boundary={secs:.6}s"
                );
            }
        }
    });
}
