//! bd-og0dj — release-risk keeper for GitHub issue #290.
//!
//! Issue #290 (cross-repo incident from coding_agent_session_search#345)
//! reported two independent failure mechanisms on a 22 GB contentless FTS5
//! database:
//!
//! 1. Schema-only opening was read-only, and contentless FTS5 reconnect
//!    eagerly hydrated the historical corpus, so registering/repairing a
//!    multi-million-row index grew memory with corpus size and could OOM
//!    before the first bounded batch committed.
//! 2. FrankenSQLite-created FTS5 shadow tables (`%_idx`, `%_config`) bypassed
//!    ordinary CREATE TABLE allocation and were not stock-SQLite compatible
//!    (`WITHOUT ROWID` layouts, catalog metadata).
//! 3. TEMP tables and their implicit indexes allocated roots from the *main*
//!    database pager without a `sqlite_master` owner; after close, stock
//!    SQLite reported orphaned pages ("Page 19 is never used").
//!
//! This keeper reproduces each reported sequence on current main and proves:
//! bounded (non-corpus-proportional) resident memory across an
//! `open_existing_schema_only` registration + incremental appends, unchanged
//! main-database page accounting (`page_count` / `freelist_count`) across
//! TEMP DDL, and stock SQLite (`rusqlite`) `quick_check` + `integrity_check`
//! acceptance of every resulting database file.
//!
//! Run: `cargo test -p fsqlite-e2e --test bd_og0dj_issue290_keeper`

use std::fmt::Write as _;
use std::sync::{Mutex, PoisonError};

use fsqlite::{Connection, SqliteValue};

/// The RSS-based memory bound is only meaningful process-wide, so the three
/// keeper tests serialize on this guard instead of running concurrently.
static KEEPER_SERIAL: Mutex<()> = Mutex::new(());

/// Seeded corpus rows. At ~1 KiB of body text per row this puts well over
/// 40 MiB of postings/text behind the contentless index, so an eager
/// corpus-proportional hydration (the reported OOM mechanism) must exceed the
/// resident-memory bound below by a wide margin.
const SEED_ROWS: i64 = 40_000;

/// Maximum permitted resident-set growth (bytes) across the schema-only open
/// plus all incremental append batches. Eager hydration of the seeded corpus
/// would add at least the corpus size (> 40 MiB); the bound sits at half that
/// to keep clear air between legitimate bounded allocation and regression.
const MAX_RESIDENT_GROWTH_BYTES: u64 = 24 * 1024 * 1024;

/// Rows appended per incremental batch, mirroring the bounded CASS repair
/// driver's explicit-rowid batches.
const APPEND_BATCH_ROWS: i64 = 40;
const APPEND_BATCHES: i64 = 3;

#[cfg(target_os = "linux")]
fn resident_bytes() -> u64 {
    let statm = std::fs::read_to_string("/proc/self/statm").expect("read /proc/self/statm");
    let pages: u64 = statm
        .split_whitespace()
        .nth(1)
        .expect("statm resident field")
        .parse()
        .expect("statm resident field parses");
    let page_size = 4096;
    pages * page_size
}

fn seed_body(i: i64) -> String {
    // ~1 KiB of searchable text per row. `needlealpha` is planted on exactly
    // two seeded rows so MATCH results stay small and deterministic.
    let needle = if i == 7 || i == 4242 {
        "needlealpha"
    } else {
        "fillerbeta"
    };
    let mut body = String::with_capacity(1100);
    for w in 0..48 {
        let _ = write!(body, "corpusword{w} ");
    }
    let _ = write!(body, "uniq{i} {needle}");
    body
}

fn sqlite_ok_pragmas(path: &std::path::Path) -> rusqlite::Connection {
    let sqlite = rusqlite::Connection::open(path).expect("stock sqlite open");
    let quick: String = sqlite
        .query_row("PRAGMA quick_check", [], |row| row.get(0))
        .expect("quick_check");
    let integrity: String = sqlite
        .query_row("PRAGMA integrity_check", [], |row| row.get(0))
        .expect("integrity_check");
    assert_eq!(quick, "ok", "stock SQLite quick_check must pass");
    assert_eq!(integrity, "ok", "stock SQLite integrity_check must pass");
    sqlite
}

/// Mechanism 1: contentless FTS5 registration through the writable
/// existing-only schema-load path must stay lazy (bounded memory) across
/// incremental explicit-rowid appends, and the appended segments must be
/// readable by stock SQLite afterwards.
#[test]
fn gh290_contentless_fts5_registration_appends_stay_bounded() {
    let _serial = KEEPER_SERIAL.lock().unwrap_or_else(PoisonError::into_inner);
    asupersync::test_utils::run_test(|| async {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("gh290_contentless.db");
        let db_str = db_path.to_string_lossy().into_owned();

        // Seed a substantial contentless corpus with stock SQLite, exactly as
        // the downstream CASS database was produced.
        {
            let sqlite = rusqlite::Connection::open(&db_path).unwrap();
            sqlite
                .execute_batch("CREATE VIRTUAL TABLE messages_fts USING fts5(body, content='');")
                .unwrap();
            let tx = sqlite.unchecked_transaction().unwrap();
            {
                let mut stmt = tx
                    .prepare("INSERT INTO messages_fts(rowid, body) VALUES (?1, ?2)")
                    .unwrap();
                for i in 1..=SEED_ROWS {
                    stmt.execute(rusqlite::params![i, seed_body(i)]).unwrap();
                }
            }
            tx.commit().unwrap();
            sqlite
                .execute_batch("INSERT INTO messages_fts(messages_fts) VALUES('optimize');")
                .unwrap();
        }

        // The existing-only API must never create a missing database.
        let missing = dir.path().join("gh290_never_created.db");
        let missing_str = missing.to_string_lossy().into_owned();
        assert!(
            Connection::open_existing_schema_only(&missing_str)
                .await
                .is_err(),
            "open_existing_schema_only must refuse to create a missing database"
        );
        assert!(
            !missing.exists(),
            "open_existing_schema_only must not leave a created file behind"
        );

        #[cfg(target_os = "linux")]
        let resident_before = resident_bytes();

        // Registration: writable schema-only open of the seeded database.
        let conn = Connection::open_existing_schema_only(&db_str)
            .await
            .expect("writable existing-only schema open");

        // Bounded incremental appends with explicit rowids (the CASS repair
        // driver sequence), interleaved with the connection-local TEMP probe
        // table that driver uses.
        conn.execute("CREATE TEMP TABLE repair_probe_ids(id INTEGER PRIMARY KEY);")
            .await
            .unwrap();
        for batch in 0..APPEND_BATCHES {
            let mut insert = String::from("INSERT INTO messages_fts(rowid, body) VALUES ");
            let mut probe = String::from("INSERT INTO repair_probe_ids(id) VALUES ");
            for row in 0..APPEND_BATCH_ROWS {
                let rowid = SEED_ROWS + batch * APPEND_BATCH_ROWS + row + 1;
                if row > 0 {
                    insert.push_str(", ");
                    probe.push_str(", ");
                }
                let _ = write!(insert, "({rowid}, 'appended{rowid} needleappend')");
                let _ = write!(probe, "({rowid})");
            }
            insert.push(';');
            probe.push(';');
            conn.execute("DELETE FROM repair_probe_ids;").await.unwrap();
            conn.execute(&probe).await.unwrap();
            conn.execute(&insert).await.unwrap();
        }

        #[cfg(target_os = "linux")]
        {
            let resident_after = resident_bytes();
            let growth = resident_after.saturating_sub(resident_before);
            assert!(
                growth < MAX_RESIDENT_GROWTH_BYTES,
                "schema-only registration + appends must not hydrate the \
                 historical corpus: resident grew by {growth} bytes \
                 (bound {MAX_RESIDENT_GROWTH_BYTES}); this is the GH #290 OOM \
                 mechanism"
            );
        }

        // Appending an already-persisted rowid must be rejected without
        // hydrating or corrupting anything.
        assert!(
            conn.execute("INSERT INTO messages_fts(rowid, body) VALUES (7, 'duplicate');")
                .await
                .is_err(),
            "lazy append must reject an existing persisted rowid"
        );

        // MATCH must see both historical and appended postings.
        let historical = conn
            .query(
                "SELECT rowid FROM messages_fts WHERE messages_fts MATCH 'needlealpha' \
                 ORDER BY rowid;",
            )
            .await
            .unwrap();
        assert_eq!(
            historical
                .iter()
                .map(|row| row.values()[0].to_integer())
                .collect::<Vec<_>>(),
            vec![7, 4242],
            "historical postings must stay queryable after lazy registration"
        );
        let appended = conn
            .query("SELECT count(*) FROM messages_fts WHERE messages_fts MATCH 'needleappend';")
            .await
            .unwrap();
        assert_eq!(
            appended[0].values()[0].to_integer(),
            APPEND_BATCHES * APPEND_BATCH_ROWS,
            "all appended rows must be MATCH-visible"
        );
        drop(conn);

        // The file FrankenSQLite wrote must remain fully stock-compatible.
        let sqlite = sqlite_ok_pragmas(&db_path);
        let stock_appended: i64 = sqlite
            .query_row(
                "SELECT count(*) FROM messages_fts WHERE messages_fts MATCH 'needleappend'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(
            stock_appended,
            APPEND_BATCHES * APPEND_BATCH_ROWS,
            "stock SQLite must read the segments FrankenSQLite appended"
        );
        let stock_total: i64 = sqlite
            .query_row("SELECT count(*) FROM messages_fts", [], |row| row.get(0))
            .unwrap();
        assert_eq!(stock_total, SEED_ROWS + APPEND_BATCHES * APPEND_BATCH_ROWS);
        let leaked_probe: i64 = sqlite
            .query_row(
                "SELECT count(*) FROM sqlite_master WHERE name = 'repair_probe_ids'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(
            leaked_probe, 0,
            "the driver's TEMP probe table must never reach the main catalog"
        );
    });
}

/// Mechanism 3: the full TEMP DDL sequence (tables with implicit UNIQUE
/// indexes, explicit TEMP indexes, drops, and a TEMP table still live at
/// close) must leave main-database page accounting untouched and the file
/// must pass stock SQLite integrity checks — no "Page N is never used".
#[test]
fn gh290_temp_ddl_leaves_main_page_accounting_untouched() {
    let _serial = KEEPER_SERIAL.lock().unwrap_or_else(PoisonError::into_inner);
    asupersync::test_utils::run_test(|| async {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("gh290_temp_ddl.db");
        let db_str = db_path.to_string_lossy().into_owned();

        let conn = Connection::open(&db_str).await.unwrap();
        conn.execute(
            "CREATE TABLE durable(id INTEGER PRIMARY KEY, key TEXT NOT NULL UNIQUE, value TEXT);",
        )
        .await
        .unwrap();
        conn.execute("INSERT INTO durable VALUES (1, 'seed', 'main');")
            .await
            .unwrap();

        let page_count_before =
            conn.query("PRAGMA page_count;").await.unwrap()[0].values()[0].to_integer();
        let freelist_before =
            conn.query("PRAGMA freelist_count;").await.unwrap()[0].values()[0].to_integer();

        // The reported sequence: TEMP table with implicit indexes, rows,
        // explicit TEMP indexes, index drop, table drop, and a second TEMP
        // table deliberately left alive at close.
        conn.execute(
            "CREATE TEMP TABLE repair_state(
                id INTEGER PRIMARY KEY,
                external_id TEXT UNIQUE,
                ordinal INTEGER,
                tag TEXT,
                UNIQUE(ordinal, external_id)
            );",
        )
        .await
        .unwrap();
        conn.execute(
            "INSERT INTO repair_state VALUES
                (1000000, 'high', 2, 'alpha'),
                (1, 'low', 1, 'beta');",
        )
        .await
        .unwrap();
        conn.execute("CREATE INDEX temp.idx_repair_ordinal ON repair_state(ordinal);")
            .await
            .unwrap();
        conn.execute("CREATE UNIQUE INDEX temp.idx_repair_tag ON repair_state(tag);")
            .await
            .unwrap();
        assert!(
            conn.execute("INSERT INTO repair_state VALUES (2, 'other', 3, 'alpha');")
                .await
                .is_err(),
            "TEMP UNIQUE index must enforce uniqueness"
        );
        conn.execute("DROP INDEX temp.idx_repair_tag;")
            .await
            .unwrap();
        conn.execute("INSERT INTO repair_state VALUES (2, 'other', 3, 'alpha');")
            .await
            .unwrap();
        conn.execute("DROP TABLE repair_state;").await.unwrap();
        conn.execute("CREATE TEMP TABLE still_open(id INTEGER PRIMARY KEY, note TEXT UNIQUE);")
            .await
            .unwrap();
        conn.execute("INSERT INTO still_open VALUES (1, 'left alive at close');")
            .await
            .unwrap();

        let page_count_after =
            conn.query("PRAGMA page_count;").await.unwrap()[0].values()[0].to_integer();
        let freelist_after =
            conn.query("PRAGMA freelist_count;").await.unwrap()[0].values()[0].to_integer();
        assert_eq!(
            page_count_after, page_count_before,
            "TEMP DDL must not allocate main-database pages"
        );
        assert_eq!(
            freelist_after, freelist_before,
            "TEMP DDL must not disturb the main-database freelist"
        );
        for pragma in ["PRAGMA quick_check;", "PRAGMA integrity_check;"] {
            let result = conn.query(pragma).await.unwrap();
            assert_eq!(
                result[0].values(),
                &[SqliteValue::Text("ok".into())],
                "in-connection integrity checks must ignore TEMP roots"
            );
        }
        drop(conn);

        // Stock SQLite must agree: identical accounting, clean integrity
        // (an orphaned root would surface as "Page N is never used"), and no
        // TEMP catalog leakage.
        let sqlite = sqlite_ok_pragmas(&db_path);
        let stock_page_count: i64 = sqlite
            .query_row("PRAGMA page_count", [], |row| row.get(0))
            .unwrap();
        let stock_freelist: i64 = sqlite
            .query_row("PRAGMA freelist_count", [], |row| row.get(0))
            .unwrap();
        assert_eq!(stock_page_count, page_count_before);
        assert_eq!(stock_freelist, freelist_before);
        let leaked: i64 = sqlite
            .query_row(
                "SELECT count(*) FROM sqlite_master \
                 WHERE name IN ('repair_state', 'still_open') \
                    OR name LIKE 'idx_repair%'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(leaked, 0, "TEMP objects must stay out of the main catalog");
        let durable: i64 = sqlite
            .query_row("SELECT count(*) FROM durable", [], |row| row.get(0))
            .unwrap();
        assert_eq!(durable, 1, "main data must survive the TEMP DDL sequence");
    });
}

/// Mechanism 2: FTS5 shadow tables created *by FrankenSQLite* must use the
/// canonical stock layouts (`WITHOUT ROWID` for `%_idx` / `%_config`) and the
/// resulting file must be fully readable by stock SQLite.
#[test]
fn gh290_fsqlite_created_fts5_shadows_are_stock_compatible() {
    let _serial = KEEPER_SERIAL.lock().unwrap_or_else(PoisonError::into_inner);
    asupersync::test_utils::run_test(|| async {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("gh290_shadow_ddl.db");
        let db_str = db_path.to_string_lossy().into_owned();

        let conn = Connection::open(&db_str).await.unwrap();
        conn.execute("CREATE VIRTUAL TABLE notes USING fts5(body, content='');")
            .await
            .unwrap();
        conn.execute(
            "INSERT INTO notes(rowid, body) VALUES
                (1, 'alpha rust'),
                (2, 'beta search'),
                (3, 'gamma rust search');",
        )
        .await
        .unwrap();
        let rows = conn
            .query("SELECT rowid FROM notes WHERE notes MATCH 'rust' ORDER BY rowid;")
            .await
            .unwrap();
        assert_eq!(
            rows.iter()
                .map(|row| row.values()[0].to_integer())
                .collect::<Vec<_>>(),
            vec![1, 3]
        );
        drop(conn);

        let sqlite = sqlite_ok_pragmas(&db_path);
        for shadow in ["notes_idx", "notes_config"] {
            let sql: String = sqlite
                .query_row(
                    "SELECT sql FROM sqlite_master WHERE name = ?1",
                    [shadow],
                    |row| row.get(0),
                )
                .unwrap_or_else(|e| panic!("{shadow} must exist in sqlite_master: {e}"));
            assert!(
                sql.to_ascii_uppercase().contains("WITHOUT ROWID"),
                "{shadow} must use the stock WITHOUT ROWID layout, got: {sql}"
            );
        }
        let matched: Vec<i64> = sqlite
            .prepare("SELECT rowid FROM notes WHERE notes MATCH 'rust' ORDER BY rowid")
            .unwrap()
            .query_map([], |row| row.get(0))
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        assert_eq!(
            matched,
            vec![1, 3],
            "stock SQLite must query the FrankenSQLite-created FTS5 index"
        );
    });
}
