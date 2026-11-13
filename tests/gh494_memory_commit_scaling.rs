#![recursion_limit = "512"]
#![cfg(all(feature = "native", not(target_arch = "wasm32")))]

//! GH494: publication must not scale with unrelated committed resident rows.
//!
//! The ordinary tests compare commit, savepoint, rollback, trigger and index
//! semantics with stock SQLite. The ignored release-only probes independently
//! grow a resident table to 32 MiB while repeatedly updating ONE fixed-size row.
//! BEGIN, write, finalization, first reads and the full envelope are measured
//! separately so moving work out of COMMIT cannot disguise the regression.
//!
//! Run with `cargo test --release -p fsqlite-core --no-default-features
//! --features native --test gh494_memory_commit_scaling -- --include-ignored
//! --nocapture --test-threads=1`. Wall-clock thresholds are coarse guards, not
//! a proof of O(touched-state) work; internal publication counters and a native
//! profile are still needed. Do not disable time-travel capture to pass these
//! probes: also run `bd_zjocc_time_travel_history_semantics`.
//!
//! GH503 extends the publication controls below: freed index pages reused by
//! overflow rows must remain owned across autocommit, DDL and rollback. These
//! deterministic stock-SQLite oracles run without the ignored timing probes.

use std::time::{Duration, Instant};

use fsqlite_core::connection::{Connection, Row};
use fsqlite_types::value::SqliteValue;

const PAYLOAD_BYTES: usize = 4096;
const RESIDENT_STAGES: &[usize] = &[0, 128, 512, 2048, 8192];
const WARMUP_TRANSACTIONS: usize = 8;
const SAMPLES: usize = 101;

fn render(rows: &[Row]) -> Vec<Vec<String>> {
    rows.iter()
        .map(|row| {
            row.values()
                .iter()
                .map(|value| match value {
                    SqliteValue::Null => "null".to_owned(),
                    SqliteValue::Integer(value) => format!("i:{value}"),
                    SqliteValue::Float(value) => format!("r:{value}"),
                    SqliteValue::Text(value) => format!("t:{value}"),
                    SqliteValue::Blob(value) => format!("b:{value:?}"),
                })
                .collect()
        })
        .collect()
}

fn stock_rows(conn: &rusqlite::Connection, sql: &str) -> Vec<Vec<String>> {
    let mut statement = conn.prepare(sql).expect("stock prepare");
    let columns = statement.column_count();
    statement
        .query_map([], |row| {
            (0..columns)
                .map(|column| {
                    Ok(match row.get_ref(column)? {
                        rusqlite::types::ValueRef::Null => "null".to_owned(),
                        rusqlite::types::ValueRef::Integer(value) => format!("i:{value}"),
                        rusqlite::types::ValueRef::Real(value) => format!("r:{value}"),
                        rusqlite::types::ValueRef::Text(value) => {
                            format!("t:{}", String::from_utf8_lossy(value))
                        }
                        rusqlite::types::ValueRef::Blob(value) => format!("b:{value:?}"),
                    })
                })
                .collect::<rusqlite::Result<Vec<_>>>()
        })
        .expect("stock query")
        .collect::<rusqlite::Result<Vec<_>>>()
        .expect("stock rows")
}

async fn execute_pair(conn: &Connection, stock: &rusqlite::Connection, sql: &str) {
    stock
        .execute_batch(sql)
        .unwrap_or_else(|error| panic!("stock `{sql}`: {error}"));
    conn.execute(sql)
        .await
        .unwrap_or_else(|error| panic!("fsqlite `{sql}`: {error}"));
}

const CONTROL_SCHEMA: &[&str] = &[
    "CREATE TABLE resident(id INTEGER PRIMARY KEY, payload BLOB NOT NULL)",
    "CREATE TABLE hot(id INTEGER PRIMARY KEY, k INTEGER NOT NULL)",
    "CREATE UNIQUE INDEX hot_k ON hot(k)",
    "CREATE TABLE audit(seq INTEGER PRIMARY KEY AUTOINCREMENT, id INTEGER, event INTEGER, k INTEGER)",
    "CREATE INDEX audit_id_event ON audit(id,event)",
    "CREATE TRIGGER hot_insert AFTER INSERT ON hot BEGIN INSERT INTO audit(id,event,k) VALUES(NEW.id,1,NEW.k); END",
    "CREATE TRIGGER hot_update AFTER UPDATE ON hot BEGIN INSERT INTO audit(id,event,k) VALUES(NEW.id,2,NEW.k); END",
    "CREATE TRIGGER hot_delete AFTER DELETE ON hot BEGIN INSERT INTO audit(id,event,k) VALUES(OLD.id,3,OLD.k); END",
    "CREATE TRIGGER hot_reject BEFORE INSERT ON hot WHEN NEW.k < 0 BEGIN SELECT RAISE(ABORT,'negative key'); END",
];

const CONTROL_READS: &[&str] = &[
    "SELECT id,k FROM hot ORDER BY id",
    "SELECT seq,id,event,k FROM audit ORDER BY seq",
    "SELECT count(*),sum(length(payload)) FROM resident",
    "SELECT seq,id,event,k FROM audit INDEXED BY audit_id_event WHERE id=1 ORDER BY event,seq",
    // INDEXED BY makes stale or missing secondary-index entries observable.
    "SELECT id,k FROM hot INDEXED BY hot_k ORDER BY k",
    "SELECT id FROM hot INDEXED BY hot_k WHERE k = 10",
    "SELECT id FROM hot INDEXED BY hot_k WHERE k = 20",
    "SELECT id FROM hot INDEXED BY hot_k WHERE k = 30",
    "SELECT id FROM hot INDEXED BY hot_k WHERE k = 40",
    "SELECT id FROM hot INDEXED BY hot_k WHERE k = 50",
    "SELECT id FROM hot INDEXED BY hot_k WHERE k = 60",
    "SELECT id FROM hot INDEXED BY hot_k WHERE k = 80",
    "SELECT id FROM hot INDEXED BY hot_k WHERE k = 99",
];

// The multi-row cases write a row AND an AFTER-trigger audit row before the
// second row fails. Both direct and indirect writes must be rolled back.
const CONTROL_FAILURES: &[&str] = &[
    "INSERT INTO hot VALUES(4,60)",
    "INSERT INTO hot VALUES(5,-1)",
    "INSERT INTO hot VALUES(4,80),(5,60)",
    "INSERT INTO hot VALUES(4,80),(5,-1)",
];

async fn compare_controls(conn: &Connection, stock: &rusqlite::Connection) {
    for sql in CONTROL_READS {
        let actual = conn
            .query(sql)
            .await
            .unwrap_or_else(|error| panic!("fsqlite query `{sql}`: {error}"));
        assert_eq!(render(&actual), stock_rows(stock, sql), "query: {sql}");
    }
}

async fn correctness_controls(file_backed: bool, wal: bool) {
    let directory = tempfile::tempdir().expect("temporary directory");
    let path = if file_backed {
        directory
            .path()
            .join("controls.db")
            .to_string_lossy()
            .into_owned()
    } else {
        ":memory:".to_owned()
    };
    let conn = Connection::open(&path).await.expect("open fsqlite");
    let stock = rusqlite::Connection::open_in_memory().expect("open stock");
    if wal {
        conn.execute("PRAGMA journal_mode=WAL")
            .await
            .expect("enable WAL");
    }
    for sql in CONTROL_SCHEMA {
        execute_pair(&conn, &stock, sql).await;
    }
    let blob = format!("X'{}'", "ab".repeat(PAYLOAD_BYTES));
    execute_pair(&conn, &stock, "BEGIN").await;
    for id in 1..=64 {
        execute_pair(
            &conn,
            &stock,
            &format!("INSERT INTO resident VALUES({id},{blob})"),
        )
        .await;
    }
    execute_pair(&conn, &stock, "COMMIT").await;
    // Retain a prepared reader across publication and rollback.
    let reader = conn
        .prepare("SELECT id,k FROM hot ORDER BY id")
        .await
        .expect("prepare reader");

    for sql in ["BEGIN", "INSERT INTO hot VALUES(1,10)", "COMMIT"] {
        execute_pair(&conn, &stock, sql).await;
    }
    compare_controls(&conn, &stock).await;
    assert_eq!(
        render(&reader.query().await.expect("prepared read after commit")),
        stock_rows(&stock, "SELECT id,k FROM hot ORDER BY id")
    );

    for sql in [
        "BEGIN",
        "UPDATE hot SET k=20 WHERE id=1",
        "INSERT INTO hot VALUES(2,30)",
        "SAVEPOINT nested",
        "UPDATE hot SET k=99 WHERE id=1",
        "INSERT INTO hot VALUES(3,40)",
        "DELETE FROM hot WHERE id=2",
    ] {
        execute_pair(&conn, &stock, sql).await;
    }
    compare_controls(&conn, &stock).await;
    for sql in ["ROLLBACK TO nested", "RELEASE nested"] {
        execute_pair(&conn, &stock, sql).await;
    }
    compare_controls(&conn, &stock).await;
    execute_pair(&conn, &stock, "COMMIT").await;
    compare_controls(&conn, &stock).await;

    for sql in [
        "BEGIN",
        "UPDATE hot SET k=50 WHERE id=1",
        "DELETE FROM hot WHERE id=2",
        "INSERT INTO hot VALUES(3,60)",
    ] {
        execute_pair(&conn, &stock, sql).await;
    }
    compare_controls(&conn, &stock).await;
    execute_pair(&conn, &stock, "ROLLBACK").await;
    compare_controls(&conn, &stock).await;
    assert_eq!(
        render(&reader.query().await.expect("prepared read after rollback")),
        stock_rows(&stock, "SELECT id,k FROM hot ORDER BY id")
    );

    // Both failure paths must leave the successful statement in the
    // same transaction intact and publish none of the failed writes.
    execute_pair(&conn, &stock, "BEGIN").await;
    execute_pair(&conn, &stock, "INSERT INTO hot VALUES(3,60)").await;
    for sql in CONTROL_FAILURES {
        let before: Vec<_> = CONTROL_READS
            .iter()
            .map(|query| stock_rows(&stock, query))
            .collect();
        assert!(stock.execute_batch(sql).is_err(), "stock must reject {sql}");
        let after: Vec<_> = CONTROL_READS
            .iter()
            .map(|query| stock_rows(&stock, query))
            .collect();
        assert_eq!(before, after, "stock statement atomicity: {sql}");
        assert!(conn.execute(sql).await.is_err(), "fsqlite must reject {sql}");
        compare_controls(&conn, &stock).await;
    }
    execute_pair(&conn, &stock, "COMMIT").await;
    compare_controls(&conn, &stock).await;
    drop(reader);
    if wal {
        let checkpoint = conn
            .query("PRAGMA wal_checkpoint(TRUNCATE)")
            .await
            .expect("checkpoint WAL");
        assert_eq!(checkpoint.len(), 1);
        assert!(matches!(
            checkpoint[0].values().first(),
            Some(SqliteValue::Integer(0))
        ));
    }
    drop(conn);

    if file_backed {
        // Verify the persisted image BEFORE another engine open could repair it.
        let persisted = rusqlite::Connection::open_with_flags(
            &path,
            rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
        )
        .expect("stock read-only persisted database");
        let integrity: String = persisted
            .query_row("PRAGMA integrity_check", [], |row| row.get(0))
            .expect("integrity check");
        assert_eq!(integrity, "ok");
        for sql in CONTROL_READS {
            assert_eq!(
                stock_rows(&persisted, sql),
                stock_rows(&stock, sql),
                "persisted: {sql}"
            );
        }
        drop(persisted);
        let reopened = Connection::open(&path).await.expect("reopen file");
        compare_controls(&reopened, &stock).await;
        // Check persisted trigger/index definitions, not just rows.
        for sql in ["BEGIN", "INSERT INTO hot VALUES(4,70)", "COMMIT"] {
            execute_pair(&reopened, &stock, sql).await;
        }
        compare_controls(&reopened, &stock).await;
    }
}

#[test]
fn gh494_memory_commit_rollback_trigger_index_controls() {
    asupersync::test_utils::run_test(|| async { correctness_controls(false, false).await });
}

#[test]
fn gh494_file_commit_rollback_trigger_index_controls() {
    asupersync::test_utils::run_test(|| async { correctness_controls(true, false).await });
}

#[test]
fn gh494_wal_file_commit_rollback_trigger_index_controls() {
    asupersync::test_utils::run_test(|| async { correctness_controls(true, true).await });
}

const GH503_OVERFLOW_BYTES: usize = 86_016;
const GH503_TABLE: &str = "CREATE TABLE t(id INTEGER PRIMARY KEY,a INTEGER,b BLOB)";
const GH503_LATER_TABLE: &str = "CREATE TABLE u(k TEXT PRIMARY KEY,v INTEGER)";

async fn gh503_compare_query(conn: &Connection, stock: &rusqlite::Connection, sql: &str) {
    let actual = conn.query(sql).await.expect(sql);
    assert_eq!(render(&actual), stock_rows(stock, sql), "GH503: {sql}");
}

async fn gh503_integrity(conn: &Connection, stock: &rusqlite::Connection) {
    let sql = "PRAGMA integrity_check";
    assert_eq!(stock_rows(stock, sql), vec![vec!["t:ok".to_owned()]]);
    gh503_compare_query(conn, stock, sql).await;
}

async fn gh503_scalar(conn: &Connection, sql: &str) -> i64 {
    let rows = conn.query(sql).await.expect(sql);
    assert_eq!(rows.len(), 1, "GH503 scalar: {sql}");
    match rows[0].values() {
        [SqliteValue::Integer(value)] => *value,
        other => panic!("GH503 expected integer for {sql}, got {other:?}"),
    }
}

fn gh503_payload(bytes: usize, salt: u8) -> Vec<u8> {
    (0..bytes)
        .map(|offset| {
            // Distinguish positions within and between overflow pages; an
            // all-zero fixture alone cannot reveal a zeroed or aliased page.
            u8::try_from((offset * 37 + offset / 4092) % 256)
                .expect("byte pattern")
                .wrapping_add(salt)
        })
        .collect()
}

async fn gh503_insert_payload(
    conn: &Connection,
    stock: &rusqlite::Connection,
    id: i64,
    payload: &[u8],
) {
    let sql = "INSERT INTO t(id,a,b) VALUES(?1,100,?2)";
    stock
        .execute(sql, rusqlite::params![id, payload])
        .expect("stock patterned overflow insert");
    conn.execute_with_params(
        sql,
        &[SqliteValue::Integer(id), SqliteValue::Blob(payload.into())],
    )
    .await
    .expect("fsqlite patterned overflow insert");
}

async fn gh503_check_payload(
    conn: &Connection,
    stock: &rusqlite::Connection,
    id: i64,
    expected: &[u8],
) {
    let sql = format!("SELECT b FROM t WHERE id={id}");
    let oracle: Vec<u8> = stock
        .query_row(&sql, [], |row| row.get(0))
        .expect("stock overflow row");
    assert_eq!(oracle, expected, "stock payload at id={id}");
    let rows = conn.query(&sql).await.expect("fsqlite overflow row");
    assert_eq!(rows.len(), 1, "GH503 lost overflow row id={id}");
    match rows[0].values() {
        [SqliteValue::Blob(bytes)] => {
            assert_eq!(
                bytes.as_ref(),
                oracle.as_slice(),
                "GH503 payload at id={id}"
            );
        }
        other => panic!("GH503 expected blob at id={id}, got {other:?}"),
    }
}

async fn gh503_reported_sequence(file_backed: bool, disable_retention: bool) {
    let directory = tempfile::tempdir().expect("GH503 controls directory");
    let path = if file_backed {
        directory
            .path()
            .join("gh503.db")
            .to_string_lossy()
            .into_owned()
    } else {
        ":memory:".to_owned()
    };
    let conn = Connection::open(&path).await.expect("open GH503 control");
    let stock = rusqlite::Connection::open_in_memory().expect("stock GH503 control");
    execute_pair(&conn, &stock, GH503_TABLE).await;
    execute_pair(&conn, &stock, "CREATE INDEX ta ON t(a)").await;
    if disable_retention {
        conn.execute("PRAGMA fsqlite.autocommit_retain=OFF")
            .await
            .expect("disable retained autocommit");
    }
    // Keep the reported statement ordering with no diagnostic reads or extra
    // transaction boundaries between freeing the index and the later DDL.
    for sql in [
        "DROP INDEX ta",
        "INSERT INTO t(id,a,b) VALUES(1,100,zeroblob(86016))",
        GH503_LATER_TABLE,
    ] {
        execute_pair(&conn, &stock, sql).await;
    }
    // Collect both symptoms before asserting, so a baseline failure preserves
    // the lost-row count and the unused-page diagnostic in the same receipt.
    let count_rows = conn.query("SELECT count(*) FROM t").await;
    let integrity_rows = conn.query("PRAGMA integrity_check").await;
    let count_rows = count_rows.expect("GH503 count after final DDL");
    let integrity_rows = integrity_rows.expect("GH503 integrity after final DDL");
    let actual_count = match count_rows[0].values() {
        [SqliteValue::Integer(value)] => *value,
        other => panic!("GH503 expected count, got {other:?}"),
    };
    let oracle_integrity = stock_rows(&stock, "PRAGMA integrity_check");
    assert_eq!(oracle_integrity, vec![vec!["t:ok".to_owned()]]);
    assert_eq!(
        (actual_count, render(&integrity_rows)),
        (1, oracle_integrity),
        "GH503 final DDL: file_backed={file_backed}, disable_retention={disable_retention}"
    );
    let expected = vec![0; GH503_OVERFLOW_BYTES];
    gh503_compare_query(&conn, &stock, "SELECT id,a,length(b) FROM t").await;
    gh503_check_payload(&conn, &stock, 1, &expected).await;
    gh503_integrity(&conn, &stock).await;
    conn.close().await.expect("close GH503 control");
    if file_backed {
        // Inspect the written file with stock SQLite before a FrankenSQLite
        // reopen can refresh or repair any in-memory bookkeeping.
        let persisted = rusqlite::Connection::open_with_flags(
            &path,
            rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
        )
        .expect("stock read-only GH503 image");
        for sql in ["SELECT id,a,b FROM t", "PRAGMA integrity_check"] {
            assert_eq!(stock_rows(&persisted, sql), stock_rows(&stock, sql));
        }
        drop(persisted);
        let reopened = Connection::open(&path).await.expect("reopen GH503 image");
        gh503_check_payload(&reopened, &stock, 1, &expected).await;
        gh503_integrity(&reopened, &stock).await;
        reopened.close().await.expect("close reopened GH503 image");
    }
}

#[test]
fn gh503_reported_memory_row_loss_matches_sqlite() {
    asupersync::test_utils::run_test(|| async {
        gh503_reported_sequence(false, true).await;
    });
}

#[test]
fn gh503_default_retention_and_file_backed_controls_match_sqlite() {
    asupersync::test_utils::run_test(|| async {
        for (file_backed, disable_retention) in [(false, false), (true, true), (true, false)] {
            gh503_reported_sequence(file_backed, disable_retention).await;
        }
    });
}

#[test]
fn gh503_freelist_and_overflow_boundaries_preserve_patterned_payloads() {
    asupersync::test_utils::run_test(|| async {
        // For this schema and a 4096-byte page, 4055/4056 straddle the
        // table-leaf local-payload limit (six bytes of record overhead).
        for bytes in [4055, 4056, 8192, GH503_OVERFLOW_BYTES] {
            for (free_pages, interior) in [(1, false), (3, false), (3, true)] {
                let conn = Connection::open(":memory:")
                    .await
                    .expect("open boundary case");
                let stock = rusqlite::Connection::open_in_memory().expect("stock boundary case");
                execute_pair(&conn, &stock, GH503_TABLE).await;
                for index in 0..free_pages {
                    execute_pair(&conn, &stock, &format!("CREATE INDEX ta{index} ON t(a)")).await;
                }
                if interior {
                    execute_pair(
                        &conn,
                        &stock,
                        "CREATE TABLE anchor(id INTEGER PRIMARY KEY,v TEXT)",
                    )
                    .await;
                    execute_pair(
                        &conn,
                        &stock,
                        "INSERT INTO anchor VALUES(1,'live EOF page')",
                    )
                    .await;
                }
                conn.execute("PRAGMA fsqlite.autocommit_retain=OFF")
                    .await
                    .expect("disable retained boundary transactions");
                let before = gh503_scalar(&conn, "PRAGMA page_count").await;
                for index in (0..free_pages).rev() {
                    execute_pair(&conn, &stock, &format!("DROP INDEX ta{index}")).await;
                }
                let payload = gh503_payload(bytes, 17);
                gh503_insert_payload(&conn, &stock, 1, &payload).await;
                execute_pair(&conn, &stock, GH503_LATER_TABLE).await;
                gh503_check_payload(&conn, &stock, 1, &payload).await;
                gh503_integrity(&conn, &stock).await;
                if interior {
                    gh503_compare_query(&conn, &stock, "SELECT * FROM anchor").await;
                }
                if free_pages == 3 && bytes == 4055 {
                    assert!(
                        gh503_scalar(&conn, "PRAGMA page_count").await <= before,
                        "reusable pages must satisfy the later table/autoindex allocation"
                    );
                }
                conn.close().await.expect("close boundary case");
            }
        }
    });
}

async fn gh503_rollback_allocation(file_backed: bool, savepoint: bool) {
    let directory = tempfile::tempdir().expect("GH503 rollback directory");
    let path = if file_backed {
        directory
            .path()
            .join("rollback.db")
            .to_string_lossy()
            .into_owned()
    } else {
        ":memory:".to_owned()
    };
    let conn = Connection::open(&path).await.expect("open rollback case");
    let stock = rusqlite::Connection::open_in_memory().expect("stock rollback case");
    execute_pair(&conn, &stock, GH503_TABLE).await;
    execute_pair(&conn, &stock, "CREATE INDEX ta ON t(a)").await;
    conn.execute("PRAGMA fsqlite.autocommit_retain=OFF")
        .await
        .expect("disable retained rollback transactions");
    execute_pair(&conn, &stock, "DROP INDEX ta").await;
    let survivor = gh503_payload(GH503_OVERFLOW_BYTES, 31);
    let abandoned = gh503_payload(GH503_OVERFLOW_BYTES + 4092, 83);
    let outer = gh503_payload(257, 129);
    gh503_insert_payload(&conn, &stock, 1, &survivor).await;
    execute_pair(&conn, &stock, "BEGIN").await;
    if savepoint {
        gh503_insert_payload(&conn, &stock, 3, &outer).await;
        execute_pair(&conn, &stock, "SAVEPOINT reuse").await;
    }
    gh503_insert_payload(&conn, &stock, 2, &abandoned).await;
    for sql in [
        "UPDATE t SET b=zeroblob(172032) WHERE id=1",
        "CREATE TABLE abandoned(k TEXT PRIMARY KEY,v INTEGER)",
    ] {
        execute_pair(&conn, &stock, sql).await;
    }
    if savepoint {
        for sql in ["ROLLBACK TO reuse", "RELEASE reuse", "COMMIT"] {
            execute_pair(&conn, &stock, sql).await;
        }
    } else {
        execute_pair(&conn, &stock, "ROLLBACK").await;
    }
    // Force reuse after rollback, including two roots (TEXT PK autoindex).
    execute_pair(&conn, &stock, GH503_LATER_TABLE).await;
    gh503_compare_query(&conn, &stock, "SELECT id,a,length(b) FROM t ORDER BY id").await;
    gh503_compare_query(
        &conn,
        &stock,
        "SELECT name FROM sqlite_master WHERE name='abandoned'",
    )
    .await;
    gh503_check_payload(&conn, &stock, 1, &survivor).await;
    if savepoint {
        gh503_check_payload(&conn, &stock, 3, &outer).await;
    }
    gh503_integrity(&conn, &stock).await;
    conn.close().await.expect("close rollback case");
}

#[test]
fn gh503_rollback_and_savepoint_reuse_match_sqlite() {
    asupersync::test_utils::run_test(|| async {
        for file_backed in [false, true] {
            for savepoint in [false, true] {
                gh503_rollback_allocation(file_backed, savepoint).await;
            }
        }
    });
}

// Relevant SQL from CASS src/storage/sqlite.rs, blob
// b71ee5f6e6ab6ceffa0526afd373485d8c284472: the reported
// migration_v21_adds_covering_context_index_without_changing_archive_rows
// test, MIGRATION_V21 and MIGRATION_V22. Unrelated archive/FTS tables are
// omitted; migration timestamps are fixed to keep the oracle deterministic.
const GH503_CASS_SCHEMA: &[&str] = &[
    "CREATE TABLE meta(key TEXT PRIMARY KEY,value TEXT NOT NULL)",
    "CREATE TABLE agents(id INTEGER PRIMARY KEY,slug TEXT NOT NULL UNIQUE,name TEXT NOT NULL,version TEXT,kind TEXT NOT NULL,created_at INTEGER NOT NULL,updated_at INTEGER NOT NULL)",
    "CREATE TABLE workspaces(id INTEGER PRIMARY KEY,path TEXT NOT NULL UNIQUE,display_name TEXT)",
    "CREATE TABLE sources(id TEXT PRIMARY KEY)",
    "INSERT INTO sources VALUES('local')",
    "CREATE TABLE conversations(id INTEGER PRIMARY KEY,agent_id INTEGER NOT NULL REFERENCES agents(id),workspace_id INTEGER REFERENCES workspaces(id),source_id TEXT NOT NULL DEFAULT 'local' REFERENCES sources(id),external_id TEXT,title TEXT,source_path TEXT NOT NULL,started_at INTEGER,ended_at INTEGER,approx_tokens INTEGER,metadata_json TEXT,origin_host TEXT,metadata_bin BLOB,total_input_tokens INTEGER,total_output_tokens INTEGER,total_cache_read_tokens INTEGER,total_cache_creation_tokens INTEGER,grand_total_tokens INTEGER,estimated_cost_usd REAL,primary_model TEXT,api_call_count INTEGER,tool_call_count INTEGER,user_message_count INTEGER,assistant_message_count INTEGER,last_message_idx INTEGER,last_message_created_at INTEGER)",
    "CREATE UNIQUE INDEX idx_conversations_provenance ON conversations(source_id,agent_id,external_id)",
    "CREATE TABLE IF NOT EXISTS _schema_migrations(version INTEGER PRIMARY KEY,name TEXT NOT NULL,applied_at TEXT NOT NULL)",
    "CREATE INDEX IF NOT EXISTS idx_conversations_context ON conversations(started_at DESC,workspace_id,agent_id)",
    "CREATE TABLE IF NOT EXISTS forgotten_sources(source_path TEXT PRIMARY KEY,size_bytes INTEGER,mtime_ms INTEGER,forgotten_at_ms INTEGER NOT NULL)",
    "INSERT INTO meta VALUES('schema_version','22')",
    "INSERT INTO _schema_migrations VALUES(20,'conversation_external_tail_lookup','2026-10-09'),(21,'conversation_context_index','2026-10-09'),(22,'forgotten_sources','2026-10-09')",
];

#[test]
fn gh503_cass_v21_migration_preserves_wide_archive_row() {
    asupersync::test_utils::run_test(|| async {
        let conn = Connection::open(":memory:").await.expect("open CASS shape");
        let stock = rusqlite::Connection::open_in_memory().expect("stock CASS shape");
        for sql in GH503_CASS_SCHEMA {
            execute_pair(&conn, &stock, sql).await;
        }
        // CASS applies these after its initial migrations. Let each engine
        // apply its normal in-memory journal-mode semantics, as CASS does.
        for sql in [
            "PRAGMA journal_mode=WAL",
            "PRAGMA synchronous=NORMAL",
            "PRAGMA cache_size=-65536",
            "PRAGMA foreign_keys=ON",
            "PRAGMA busy_timeout=5000",
        ] {
            execute_pair(&conn, &stock, sql).await;
        }
        for sql in [
            "PRAGMA fsqlite.concurrent_mode=ON",
            "PRAGMA fsqlite.autocommit_retain=OFF",
        ] {
            conn.execute(sql)
                .await
                .expect("CASS apply_config engine setting");
        }
        for sql in [
            "DROP INDEX idx_conversations_context",
            "DELETE FROM _schema_migrations WHERE version>=21",
            "UPDATE meta SET value='20' WHERE key='schema_version'",
            "INSERT INTO agents(id,slug,name,kind,created_at,updated_at) VALUES(1,'codex','Codex','cli',0,0)",
            "INSERT INTO conversations(id,agent_id,source_path,started_at,metadata_bin) VALUES(1,1,'/context.jsonl',100,zeroblob(86016))",
        ] {
            execute_pair(&conn, &stock, sql).await;
        }
        let archive_sql = "SELECT * FROM conversations WHERE id=1";
        let before = stock_rows(&stock, archive_sql);
        assert_eq!(before.len(), 1, "CASS oracle seeded one archive row");
        gh503_compare_query(&conn, &stock, archive_sql).await;
        for _ in 0..2 {
            for sql in [
                "CREATE TABLE IF NOT EXISTS _schema_migrations(version INTEGER PRIMARY KEY,name TEXT NOT NULL,applied_at TEXT NOT NULL)",
                "BEGIN",
                "CREATE INDEX IF NOT EXISTS idx_conversations_context ON conversations(started_at DESC,workspace_id,agent_id)",
                "INSERT OR IGNORE INTO _schema_migrations VALUES(21,'conversation_context_index','2026-10-09')",
                "COMMIT",
                "BEGIN",
                "CREATE TABLE IF NOT EXISTS forgotten_sources(source_path TEXT PRIMARY KEY,size_bytes INTEGER,mtime_ms INTEGER,forgotten_at_ms INTEGER NOT NULL)",
                "INSERT OR IGNORE INTO _schema_migrations VALUES(22,'forgotten_sources','2026-10-09')",
                "COMMIT",
                "UPDATE meta SET value='22' WHERE key='schema_version'",
            ] {
                execute_pair(&conn, &stock, sql).await;
            }
        }
        // CASS calls run_migrations twice before observing the archive. This
        // reduced fixture stress-tests the relevant idempotent SQL on both
        // passes; the real runner skips migrations already in its ledger.
        // Avoid extra archive/integrity reads between the passes, which could
        // refresh retained state and conceal a publication defect.
        assert_eq!(stock_rows(&stock, archive_sql), before);
        for sql in [
            archive_sql,
            "SELECT value FROM meta WHERE key='schema_version'",
            "SELECT count(*) FROM _schema_migrations WHERE version=21",
            "PRAGMA index_info(idx_conversations_context)",
            "SELECT id,started_at,workspace_id,agent_id FROM conversations INDEXED BY idx_conversations_context ORDER BY started_at DESC",
        ] {
            gh503_compare_query(&conn, &stock, sql).await;
        }
        assert_eq!(
            gh503_scalar(
                &conn,
                "SELECT count(*) FROM _schema_migrations WHERE version=21"
            )
            .await,
            1
        );
        gh503_integrity(&conn, &stock).await;
        conn.close().await.expect("close CASS shape");
    });
}

#[derive(Clone, Copy, Debug)]
enum Finish {
    Commit,
    Rollback,
}

impl Finish {
    const fn sql(self) -> &'static str {
        match self {
            Self::Commit => "COMMIT",
            Self::Rollback => "ROLLBACK",
        }
    }
}

#[derive(Clone, Copy)]
struct Sample {
    begin: Duration,
    write: Duration,
    finish: Duration,
    first_reads: Duration,
    envelope: Duration,
}

fn quantile(
    values: impl Iterator<Item = Duration>,
    numerator: usize,
    denominator: usize,
) -> Duration {
    let mut values: Vec<_> = values.collect();
    values.sort_unstable();
    assert!(!values.is_empty());
    values[(values.len() - 1) * numerator / denominator]
}

fn median(samples: &[Sample], field: fn(&Sample) -> Duration) -> Duration {
    quantile(samples.iter().map(field), 1, 2)
}

fn within_timing_budget(baseline: Duration, observed: Duration) -> bool {
    // A deliberately coarse, explicitly non-deterministic guard against the
    // reported ~19x regression. Internal work counters are still required.
    let budget = (baseline * 4).max(baseline + Duration::from_millis(2));
    observed <= budget
}

async fn grow_resident(conn: &Connection, from: usize, to: usize, blob: &str) {
    if from == to {
        return;
    }
    conn.execute("BEGIN").await.expect("begin resident growth");
    for first in ((from + 1)..=to).step_by(16) {
        let last = (first + 15).min(to);
        let rows = (first..=last)
            .map(|id| format!("({id},{blob})"))
            .collect::<Vec<_>>()
            .join(",");
        conn.execute(&format!("INSERT INTO resident VALUES {rows}"))
            .await
            .expect("grow unrelated resident table");
    }
    conn.execute("COMMIT").await.expect("commit resident growth");
}

async fn sample_one_row(
    conn: &Connection,
    finish: Finish,
    previous: i64,
    resident_rows: usize,
) -> Sample {
    let attempted = 1 - previous;
    let expected = match finish {
        Finish::Commit => attempted,
        Finish::Rollback => previous,
    };
    let write_sql = format!("UPDATE hot SET version={attempted} WHERE id=1");
    let resident_sql = format!(
        "SELECT length(payload) FROM resident WHERE id={}",
        resident_rows.max(1)
    );
    let start = Instant::now();
    conn.execute("BEGIN").await.expect("begin sample");
    let begin = start.elapsed();
    let tick = Instant::now();
    conn.execute(&write_sql).await.expect("one-row update");
    let write = tick.elapsed();
    let tick = Instant::now();
    conn.execute(finish.sql()).await.expect("finish sample");
    let finish_elapsed = tick.elapsed();
    let tick = Instant::now();
    let hot = conn
        .query("SELECT version,length(payload) FROM hot WHERE id=1")
        .await
        .expect("first read of touched table");
    let resident = conn.query(&resident_sql).await.expect("first unrelated read");
    let first_reads = tick.elapsed();
    let envelope = start.elapsed();
    assert_eq!(
        render(&hot),
        vec![vec![format!("i:{expected}"), format!("i:{PAYLOAD_BYTES}")]]
    );
    if resident_rows == 0 {
        assert!(resident.is_empty());
    } else {
        assert_eq!(render(&resident), vec![vec![format!("i:{PAYLOAD_BYTES}")]]);
    }
    Sample {
        begin,
        write,
        finish: finish_elapsed,
        first_reads,
        envelope,
    }
}

async fn unrelated_resident_scaling(file_backed: bool, finish: Finish) {
    assert!(!cfg!(debug_assertions), "run this timing test with --release");
    let directory = tempfile::tempdir().expect("temporary directory");
    let path = if file_backed {
        directory
            .path()
            .join("scaling.db")
            .to_string_lossy()
            .into_owned()
    } else {
        ":memory:".to_owned()
    };
    let conn = Connection::open(&path).await.expect("open probe");
    conn.execute("CREATE TABLE resident(id INTEGER PRIMARY KEY, payload BLOB NOT NULL)")
        .await
        .expect("create resident");
    conn.execute("CREATE TABLE hot(id INTEGER PRIMARY KEY, version INTEGER, payload BLOB NOT NULL)")
        .await
        .expect("create hot");
    let blob = format!("X'{}'", "ab".repeat(PAYLOAD_BYTES));
    conn.execute(&format!("INSERT INTO hot VALUES(1,0,{blob})"))
        .await
        .expect("seed fixed-size hot table");
    let mut resident_rows = 0;
    let mut previous = 0;
    let mut measurements = Vec::new();
    for &size in RESIDENT_STAGES {
        grow_resident(&conn, resident_rows, size, &blob).await;
        resident_rows = size;
        // Traverse payloads outside the timed region. Memory-mode
        // storage is resident; the file case is a backend control.
        let warm = conn
            .query("SELECT payload FROM resident")
            .await
            .expect("warm resident");
        assert_eq!(warm.len(), size);
        drop(warm);
        let mut samples = Vec::with_capacity(SAMPLES);
        for iteration in 0..(WARMUP_TRANSACTIONS + SAMPLES) {
            let sample = sample_one_row(&conn, finish, previous, size).await;
            if matches!(finish, Finish::Commit) {
                previous = 1 - previous;
            }
            if iteration >= WARMUP_TRANSACTIONS {
                samples.push(sample);
            }
        }
        let finish_median = median(&samples, |sample| sample.finish);
        let envelope_median = median(&samples, |sample| sample.envelope);
        println!(
            "backend={} finish={finish:?} resident_rows={size} resident_bytes={} begin_p50_us={} write_p50_us={} finish_p50_us={} finish_p90_us={} first_reads_p50_us={} envelope_p50_us={}",
            if file_backed { "file" } else { "memory" },
            size * PAYLOAD_BYTES,
            median(&samples, |sample| sample.begin).as_micros(),
            median(&samples, |sample| sample.write).as_micros(),
            finish_median.as_micros(),
            quantile(samples.iter().map(|sample| sample.finish), 9, 10).as_micros(),
            median(&samples, |sample| sample.first_reads).as_micros(),
            envelope_median.as_micros(),
        );
        measurements.push((size, finish_median, envelope_median));
    }
    let (_, base_finish, base_envelope) = measurements[0];
    for &(size, observed_finish, observed_envelope) in &measurements[1..] {
        assert!(
            within_timing_budget(base_finish, observed_finish),
            "{finish:?} scales with unrelated rows: file={file_backed}, rows={size}, baseline={base_finish:?}, observed={observed_finish:?}"
        );
        assert!(
            within_timing_budget(base_envelope, observed_envelope),
            "work moved outside finalization: file={file_backed}, finish={finish:?}, rows={size}, baseline={base_envelope:?}, observed={observed_envelope:?}"
        );
    }
}

#[test]
#[ignore = "release-only timing regression; use --release --ignored --nocapture --test-threads=1"]
fn gh494_memory_unrelated_resident_commit_scaling() {
    asupersync::test_utils::run_test(|| async {
        unrelated_resident_scaling(false, Finish::Commit).await;
    });
}

#[test]
#[ignore = "release-only timing regression; use --release --ignored --nocapture --test-threads=1"]
fn gh494_memory_unrelated_resident_rollback_scaling() {
    asupersync::test_utils::run_test(|| async {
        unrelated_resident_scaling(false, Finish::Rollback).await;
    });
}

#[test]
#[ignore = "release-only timing regression; use --release --ignored --nocapture --test-threads=1"]
fn gh494_file_unrelated_resident_commit_scaling() {
    asupersync::test_utils::run_test(|| async {
        unrelated_resident_scaling(true, Finish::Commit).await;
    });
}

#[test]
#[ignore = "release-only timing regression; use --release --ignored --nocapture --test-threads=1"]
fn gh494_file_unrelated_resident_rollback_scaling() {
    asupersync::test_utils::run_test(|| async {
        unrelated_resident_scaling(true, Finish::Rollback).await;
    });
}

async fn original_single_insert_scaling(file_backed: bool) {
    assert!(!cfg!(debug_assertions), "run this timing test with --release");
    let directory = tempfile::tempdir().expect("temporary directory");
    let path = if file_backed {
        directory
            .path()
            .join("original.db")
            .to_string_lossy()
            .into_owned()
    } else {
        ":memory:".to_owned()
    };
    let conn = Connection::open(&path).await.expect("open original repro");
    conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, payload BLOB NOT NULL)")
        .await
        .expect("create original table");
    let blob = format!("X'{}'", "ab".repeat(PAYLOAD_BYTES));
    let mut id = 0;
    let mut windows = Vec::new();
    for _ in 0..8 {
        let before = id;
        let mut insert_time = Duration::ZERO;
        let mut commit_time = Duration::ZERO;
        for _ in 0..200 {
            id += 1;
            // Keep SQL formatting outside the timed INSERT interval.
            let sql = format!("INSERT INTO t VALUES({id},{blob})");
            conn.execute("BEGIN").await.expect("begin insert transaction");
            let tick = Instant::now();
            conn.execute(&sql).await.expect("insert payload");
            insert_time += tick.elapsed();
            let tick = Instant::now();
            conn.execute("COMMIT").await.expect("commit payload");
            commit_time += tick.elapsed();
        }
        let mean_commit = commit_time / 200;
        println!(
            "original backend={} rows_before={before} insert_mean_us={} commit_mean_us={}",
            if file_backed { "file" } else { "memory" },
            (insert_time / 200).as_micros(),
            mean_commit.as_micros(),
        );
        windows.push(mean_commit);
    }
    let rows = conn
        .query("SELECT count(*),sum(length(payload)) FROM t")
        .await
        .expect("verify committed payloads");
    assert_eq!(
        render(&rows),
        vec![vec!["i:1600".to_owned(), "i:6553600".to_owned()]]
    );
    for (window, &observed) in windows.iter().enumerate().skip(1) {
        assert!(
            within_timing_budget(windows[0], observed),
            "original GH494 slope: file={file_backed}, window={window}, baseline={:?}, observed={observed:?}",
            windows[0]
        );
    }
}

#[test]
#[ignore = "release-only timing regression; use --release --ignored --nocapture --test-threads=1"]
fn gh494_original_memory_single_insert_commit_scaling() {
    asupersync::test_utils::run_test(|| async { original_single_insert_scaling(false).await });
}

#[test]
#[ignore = "release-only timing regression; use --release --ignored --nocapture --test-threads=1"]
fn gh494_original_file_single_insert_commit_scaling() {
    asupersync::test_utils::run_test(|| async { original_single_insert_scaling(true).await });
}

async fn gh503_timed_frank_batch(
    conn: &Connection,
    insert: &fsqlite_core::connection::PreparedStatement<'_>,
    delete: &fsqlite_core::connection::PreparedStatement<'_>,
    params: &[SqliteValue],
    pairs: usize,
) -> Duration {
    let tick = Instant::now();
    for _ in 0..pairs {
        insert
            .execute_with_params(params)
            .await
            .expect("timed fsqlite insert");
        delete.execute().await.expect("timed fsqlite delete");
    }
    // execute_begin flushes and commits any retained autocommit transaction
    // before opening the explicit transaction. Include that completed-work
    // boundary in the sample; a below-threshold batch must not park its commit
    // cost until the untimed verification reads or connection close.
    conn.execute("BEGIN")
        .await
        .expect("timed fsqlite publication boundary");
    conn.execute("COMMIT")
        .await
        .expect("timed fsqlite boundary commit");
    tick.elapsed()
}

fn gh503_timed_stock_batch(
    stock: &rusqlite::Connection,
    insert: &mut rusqlite::Statement<'_>,
    delete: &mut rusqlite::Statement<'_>,
    payload: &[u8],
    pairs: usize,
) -> Duration {
    let tick = Instant::now();
    for _ in 0..pairs {
        insert
            .execute(rusqlite::params![2_i64, payload])
            .expect("timed stock insert");
        delete.execute([]).expect("timed stock delete");
    }
    stock
        .execute_batch("BEGIN")
        .expect("timed stock publication boundary");
    stock
        .execute_batch("COMMIT")
        .expect("timed stock boundary commit");
    tick.elapsed()
}

async fn gh503_paired_overflow_reuse(file_backed: bool, disable_retention: bool) {
    const WARMUP_PAIRS: usize = 4;
    const PAIRS_PER_SAMPLE: usize = 8;
    const BATCHES: usize = 9;
    const INSERT: &str = "INSERT INTO t(id,a,b) VALUES(?1,100,?2)";
    const DELETE: &str = "DELETE FROM t WHERE id=2";

    let directory = tempfile::tempdir().expect("paired GH503 directory");
    let path = if file_backed {
        directory
            .path()
            .join("frank.db")
            .to_string_lossy()
            .into_owned()
    } else {
        ":memory:".to_owned()
    };
    let conn = Connection::open(&path).await.expect("paired fsqlite open");
    let stock = if file_backed {
        rusqlite::Connection::open(directory.path().join("stock.db"))
    } else {
        rusqlite::Connection::open_in_memory()
    }
    .expect("paired stock open");
    if file_backed {
        for sql in ["PRAGMA journal_mode=WAL", "PRAGMA synchronous=NORMAL"] {
            execute_pair(&conn, &stock, sql).await;
        }
    }
    let journal_rows = conn
        .query("PRAGMA journal_mode")
        .await
        .expect("fsqlite journal mode");
    let fsqlite_journal_mode = match journal_rows[0].values() {
        [SqliteValue::Text(mode)] => mode.to_string(),
        other => panic!("expected journal mode, got {other:?}"),
    };
    let sqlite_journal_mode: String = stock
        .query_row("PRAGMA journal_mode", [], |row| row.get(0))
        .expect("stock journal mode");
    let fsqlite_synchronous = gh503_scalar(&conn, "PRAGMA synchronous").await;
    let sqlite_synchronous: i64 = stock
        .query_row("PRAGMA synchronous", [], |row| row.get(0))
        .expect("stock synchronous mode");
    execute_pair(&conn, &stock, GH503_TABLE).await;
    execute_pair(&conn, &stock, "CREATE INDEX ta ON t(a)").await;
    if disable_retention {
        conn.execute("PRAGMA fsqlite.autocommit_retain=OFF")
            .await
            .expect("paired retention OFF");
    }
    execute_pair(&conn, &stock, "DROP INDEX ta").await;
    let survivor = gh503_payload(GH503_OVERFLOW_BYTES, 41);
    let churn = gh503_payload(GH503_OVERFLOW_BYTES, 173);
    gh503_insert_payload(&conn, &stock, 1, &survivor).await;
    gh503_insert_payload(&conn, &stock, 2, &churn).await;
    gh503_check_payload(&conn, &stock, 1, &survivor).await;
    gh503_check_payload(&conn, &stock, 2, &churn).await;
    assert_eq!(gh503_scalar(&conn, "SELECT count(*) FROM t").await, 2);
    gh503_compare_query(&conn, &stock, "SELECT count(*) FROM t").await;
    gh503_integrity(&conn, &stock).await;
    execute_pair(&conn, &stock, DELETE).await;

    let insert = conn.prepare(INSERT).await.expect("prepare fsqlite insert");
    let delete = conn.prepare(DELETE).await.expect("prepare fsqlite delete");
    let mut stock_insert = stock.prepare(INSERT).expect("prepare stock insert");
    let mut stock_delete = stock.prepare(DELETE).expect("prepare stock delete");
    let params = [
        SqliteValue::Integer(2),
        SqliteValue::Blob(churn.as_slice().into()),
    ];
    gh503_timed_frank_batch(&conn, &insert, &delete, &params, WARMUP_PAIRS).await;
    gh503_timed_stock_batch(
        &stock,
        &mut stock_insert,
        &mut stock_delete,
        &churn,
        WARMUP_PAIRS,
    );

    let mut frank_samples = Vec::with_capacity(BATCHES);
    let mut stock_samples = Vec::with_capacity(BATCHES);
    for sample in 0..BATCHES {
        // Alternate order within an invocation. Baseline/candidate binaries
        // must also be alternated externally on the same host and flags.
        if sample.is_multiple_of(2) {
            frank_samples.push(
                gh503_timed_frank_batch(&conn, &insert, &delete, &params, PAIRS_PER_SAMPLE).await,
            );
            stock_samples.push(gh503_timed_stock_batch(
                &stock,
                &mut stock_insert,
                &mut stock_delete,
                &churn,
                PAIRS_PER_SAMPLE,
            ));
        } else {
            stock_samples.push(gh503_timed_stock_batch(
                &stock,
                &mut stock_insert,
                &mut stock_delete,
                &churn,
                PAIRS_PER_SAMPLE,
            ));
            frank_samples.push(
                gh503_timed_frank_batch(&conn, &insert, &delete, &params, PAIRS_PER_SAMPLE).await,
            );
        }
    }
    drop(insert);
    drop(delete);
    drop(stock_insert);
    drop(stock_delete);

    // Reads and structural checks are deliberately outside the timed region.
    // There is no DDL during churn, so the pre-fix engine can participate in
    // an honest timing comparison without accepting the known row-loss case.
    assert_eq!(gh503_scalar(&conn, "SELECT count(*) FROM t").await, 1);
    gh503_compare_query(&conn, &stock, "SELECT id,a,length(b) FROM t ORDER BY id").await;
    gh503_check_payload(&conn, &stock, 1, &survivor).await;
    gh503_integrity(&conn, &stock).await;
    gh503_insert_payload(&conn, &stock, 2, &churn).await;
    gh503_check_payload(&conn, &stock, 2, &churn).await;
    gh503_check_payload(&conn, &stock, 1, &survivor).await;
    gh503_integrity(&conn, &stock).await;
    execute_pair(&conn, &stock, DELETE).await;
    conn.close().await.expect("close paired fsqlite");

    let statements_per_sample = (PAIRS_PER_SAMPLE * 2 + 2) as u128;
    println!(
        "GH503_PERF {}",
        serde_json::json!({
            "probe": "overflow_freelist_reuse",
            "backend": if file_backed { "file" } else { "memory" },
            "retention": if disable_retention { "off" } else { "default" },
            "sqlite_version": rusqlite::version(),
            "fsqlite_journal_mode": fsqlite_journal_mode,
            "sqlite_journal_mode": sqlite_journal_mode,
            "fsqlite_synchronous": fsqlite_synchronous,
            "sqlite_synchronous": sqlite_synchronous,
            "payload_bytes": GH503_OVERFLOW_BYTES,
            "warmup_pairs": WARMUP_PAIRS,
            "pairs_per_sample": PAIRS_PER_SAMPLE,
            "completion_boundary": "BEGIN;COMMIT",
            "boundary_statements_per_sample": 2,
            "statements_per_sample": statements_per_sample,
            "samples": BATCHES,
            "fsqlite_p50_ns_per_statement": quantile(frank_samples.iter().copied(), 1, 2).as_nanos() / statements_per_sample,
            "sqlite_p50_ns_per_statement": quantile(stock_samples.iter().copied(), 1, 2).as_nanos() / statements_per_sample,
            "fsqlite_batch_ns": frank_samples.iter().map(Duration::as_nanos).collect::<Vec<_>>(),
            "sqlite_batch_ns": stock_samples.iter().map(Duration::as_nanos).collect::<Vec<_>>(),
        })
    );
}

#[test]
#[ignore = "diagnostic paired timing probe; use --release --ignored --exact --nocapture --test-threads=1"]
fn gh503_live_stock_overflow_reuse_probe() {
    assert!(
        !cfg!(debug_assertions),
        "run paired timing probe with --release"
    );
    asupersync::test_utils::run_test(|| async {
        for (file_backed, disable_retention) in [(false, true), (false, false), (true, true)] {
            gh503_paired_overflow_reuse(file_backed, disable_retention).await;
        }
    });
}
