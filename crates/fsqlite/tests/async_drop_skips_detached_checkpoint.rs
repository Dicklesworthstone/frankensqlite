//! bd-daqmp keeper: a READ-ONLY open of an fsqlite-native database
//! family whose -wal still holds unpublished staged frames must NOT rewrite
//! the main database file. Stock sqlite3 `mode=ro` leaves main + -wal
//! byte-identical (only an -shm rebuild is permitted).
//!
//! Recipe (downstream-decisive shape, sqlmodel FrankenConnection lineage):
//!   1. AsyncConnection::open_sync + ONE multi-hundred-KB DDL/INSERT batch via
//!      execute_sync, then DROP without a clean close (main stays 1 page,
//!      -wal keeps the staged frames).
//!   2. Reopen via each read-only surface and assert the main file's byte
//!      length never changes.

use fsqlite::AsyncConnection;
use fsqlite::compat::OpenFlags;

fn file_len(path: &std::path::Path) -> u64 {
    std::fs::metadata(path).map(|m| m.len()).unwrap_or(0)
}

fn family(db: &std::path::Path) -> (u64, u64, u64) {
    let sibling = |suffix: &str| {
        let mut s = db.as_os_str().to_owned();
        s.push(suffix);
        std::path::PathBuf::from(s)
    };
    (
        file_len(db),
        file_len(&sibling("-wal")),
        file_len(&sibling("-shm")),
    )
}

fn big_ddl_batch() -> String {
    let mut sql = String::new();
    let filler = "x".repeat(400);
    for i in 0..120 {
        sql.push_str(&format!(
            "CREATE TABLE t{i} (id INTEGER PRIMARY KEY, a TEXT, b TEXT, c TEXT, \
             d INTEGER, e REAL);\n\
             CREATE INDEX idx_t{i}_a ON t{i}(a);\n\
             INSERT INTO t{i} (a, b, c, d, e) VALUES ('{filler}', '{filler}', \
             '{filler}', {i}, {i}.5);\n"
        ));
    }
    sql
}

#[test]
fn readonly_open_must_not_rewrite_main_db() {
    let db_path = std::env::temp_dir().join(format!(
        "daqmp-ro-open-{}-{}.db",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let db_path_str = db_path.to_str().expect("utf8 path").to_owned();

    // ── Stage 1: build the family and drop WITHOUT a clean close ──────
    {
        let conn = AsyncConnection::open_sync(db_path_str.clone()).expect("open rw");
        let batch = big_ddl_batch();
        eprintln!("STAGE1 ddl batch bytes={}", batch.len());
        conn.execute_sync(&batch).expect("execute ddl batch");
        // Deliberately DROP without close(): staged frames stay in -wal.
        drop(conn);
    }

    let immediate = family(&db_path);
    eprintln!(
        "STAGE1 after dirty drop (immediate): main={} wal={} shm={}",
        immediate.0, immediate.1, immediate.2
    );
    assert!(immediate.1 > 0, "fixture must leave staged frames in -wal");

    // ── Stage 1b: poll WITHOUT opening anything. If the main file grows on
    // its own, the writer's detached async-worker cleanup (close_in_place →
    // passive checkpoint) is the mutator, not the read-only open. ──────────
    let mut last = immediate;
    let mut background_mutation_at = None;
    for tick in 0..100 {
        std::thread::sleep(std::time::Duration::from_millis(50));
        let now = family(&db_path);
        if now != last {
            eprintln!(
                "STAGE1b background change at ~{}ms: main={} wal={} shm={}",
                (tick + 1) * 50,
                now.0,
                now.1,
                now.2
            );
            background_mutation_at.get_or_insert((tick + 1) * 50);
            last = now;
        }
    }
    let baseline = family(&db_path);
    eprintln!(
        "STAGE1b settled (no opens performed): main={} wal={} shm={} background_mutation_at={:?}ms",
        baseline.0, baseline.1, baseline.2, background_mutation_at
    );

    // ── Stage 2a: bare open_with_flags_sync(SQLITE_OPEN_READ_ONLY) ────
    let during_a;
    {
        let conn =
            AsyncConnection::open_with_flags_sync(&db_path_str, OpenFlags::SQLITE_OPEN_READ_ONLY)
                .expect("bare readonly open");
        during_a = family(&db_path);
        drop(conn);
    }
    let after_open_a = family(&db_path);
    eprintln!(
        "STAGE2a bare RO open: during main={} wal={} shm={} | after drop main={} wal={} shm={}",
        during_a.0, during_a.1, during_a.2, after_open_a.0, after_open_a.1, after_open_a.2
    );

    // ── Stage 2b: open_schema_only_sync + one SELECT ──────────────────
    let during_b;
    {
        let conn =
            AsyncConnection::open_schema_only_sync(db_path_str.clone()).expect("schema-only open");
        let rows = conn
            .query_sync("SELECT count(*) FROM sqlite_master;")
            .expect("select");
        assert!(!rows.is_empty());
        during_b = family(&db_path);
        drop(conn);
    }
    let after_open_b = family(&db_path);
    eprintln!(
        "STAGE2b schema-only+SELECT: during main={} wal={} shm={} | after drop main={} wal={} shm={}",
        during_b.0, during_b.1, during_b.2, after_open_b.0, after_open_b.1, after_open_b.2
    );

    let cleanup = |suffix: &str| {
        let mut s = db_path.as_os_str().to_owned();
        s.push(suffix);
        let _ = std::fs::remove_file(std::path::PathBuf::from(s));
    };
    let _ = std::fs::remove_file(&db_path);
    cleanup("-wal");
    cleanup("-shm");

    // ── Oracle assertions (stock sqlite3 mode=ro semantics) ───────────
    // The decisive assertion: NOTHING may rewrite the family after a dirty
    // drop with no opens in flight. Pre-fix, the detached async-worker
    // shutdown cleanup ran close_in_place() -> passive checkpoint and grew
    // the main db ~50ms after Drop returned (misattributed downstream to
    // the read-only open that happened to run inside that window).
    assert_eq!(
        baseline, immediate,
        "background mutation after dirty drop (detached worker cleanup wrote the family \
         with no opens in flight; first change at {background_mutation_at:?}ms)"
    );
    assert_eq!(
        during_a.0, baseline.0,
        "MUTATION AT OPEN (bare RO open_with_flags): main db grew {} -> {} while the \
         connection was still open",
        baseline.0, during_a.0
    );
    assert_eq!(
        after_open_a.0, baseline.0,
        "MUTATION AT DROP (bare RO open_with_flags): main db grew {} -> {} after drop",
        baseline.0, after_open_a.0
    );
    assert_eq!(during_a.1, baseline.1, "bare RO open must not rewrite -wal");
    assert_eq!(
        during_b.0, baseline.0,
        "MUTATION AT OPEN (open_schema_only + SELECT): main db grew {} -> {}",
        baseline.0, during_b.0
    );
    assert_eq!(
        after_open_b.0, baseline.0,
        "MUTATION AT DROP (open_schema_only + SELECT): main db grew {} -> {}",
        baseline.0, after_open_b.0
    );
    assert_eq!(
        during_b.1, baseline.1,
        "schema-only open must not rewrite -wal"
    );
}
