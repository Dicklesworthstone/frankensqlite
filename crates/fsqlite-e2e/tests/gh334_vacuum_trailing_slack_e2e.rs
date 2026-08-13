//! bd-e26jr — GH #334 signature 2 keeper: `VACUUM INTO` (and in-place
//! `VACUUM`) must tolerate a source database with deliberate trailing slack —
//! file bytes beyond `header.page_count * page_size`.
//!
//! Stock SQLite treats the header page count as authoritative and ignores
//! trailing garbage; fsqlite 0.1.19 vacuumed such files, and beads_rust's
//! doctor-repair/migration paths depend on `VACUUM INTO` as the repair step
//! for exactly these files. On the 0.2 line the vacuum source-image receipt
//! (`database_image_receipt_for_open_file`) started requiring the file length
//! to be an exact page multiple matching the header page count, so repair of
//! a slack-bearing file fails with `DatabaseCorrupt` before any work happens.
//!
//! Two slack shapes are pinned: page-aligned slack (a whole trailing page of
//! zeros) and unaligned slack (a partial-page tail), both after a clean close.

use asupersync::runtime::RuntimeBuilder;

const SEED_ROWS: i64 = 8;

/// Build a db at `path`, close it cleanly, and return its logical row count.
fn seed_db(path: &str) {
    let rt = RuntimeBuilder::current_thread()
        .build()
        .expect("seed runtime");
    let conn = rt
        .block_on(fsqlite::Connection::open(path.to_owned()))
        .expect("seed open");
    rt.block_on(conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT NOT NULL)"))
        .expect("seed create table");
    for i in 0..SEED_ROWS {
        rt.block_on(conn.execute(&format!("INSERT INTO t (id, v) VALUES ({i}, 'row-{i}')")))
            .expect("seed insert");
    }
    // Truncate-checkpoint so the main file holds the full committed image and
    // the slack appended below is unambiguously beyond the logical database.
    rt.block_on(conn.execute("PRAGMA wal_checkpoint(TRUNCATE)"))
        .expect("seed checkpoint");
    rt.block_on(conn.close()).expect("seed close");
}

fn append_slack(path: &str, slack: &[u8]) {
    use std::io::Write as _;
    let mut file = std::fs::OpenOptions::new()
        .append(true)
        .open(path)
        .expect("open db file for slack append");
    file.write_all(slack).expect("append trailing slack");
    file.sync_all().expect("sync slack");
}

fn count_rows(path: &str) -> i64 {
    let rt = RuntimeBuilder::current_thread()
        .build()
        .expect("count runtime");
    let conn = rt
        .block_on(fsqlite::Connection::open(path.to_owned()))
        .expect("count open");
    let rows = rt
        .block_on(conn.query("SELECT count(*) FROM t"))
        .expect("count query");
    let count = match rows[0].values()[0] {
        fsqlite_types::SqliteValue::Integer(n) => n,
        ref other => panic!("unexpected count value: {other:?}"),
    };
    rt.block_on(conn.close()).expect("count close");
    count
}

fn vacuum_into_with_slack(slack: &[u8], label: &str) {
    let dir = tempfile::tempdir().expect("tempdir");
    let src = dir
        .path()
        .join(format!("slack-src-{label}.db"))
        .to_string_lossy()
        .into_owned();
    let dst = dir
        .path()
        .join(format!("slack-dst-{label}.db"))
        .to_string_lossy()
        .into_owned();

    seed_db(&src);
    append_slack(&src, slack);

    let rt = RuntimeBuilder::current_thread()
        .build()
        .expect("vacuum runtime");
    let conn = rt
        .block_on(fsqlite::Connection::open(src.clone()))
        .expect("open slack-bearing source");
    rt.block_on(conn.execute(&format!("VACUUM INTO '{dst}'")))
        .unwrap_or_else(|error| {
            panic!(
                "VACUUM INTO must tolerate {label} trailing slack on the source \
                 (0.1.19 did; repair paths depend on it), got: {error:?}"
            )
        });
    rt.block_on(conn.close()).expect("close source");
    drop(rt);

    assert_eq!(
        count_rows(&dst),
        SEED_ROWS,
        "vacuumed output must carry the full logical database ({label})"
    );
}

#[test]
fn gh334_vacuum_into_tolerates_page_aligned_trailing_slack() {
    // One whole page of zero slack (page-size multiple keeps the length an
    // exact multiple, but longer than the header page count).
    vacuum_into_with_slack(&[0_u8; 4096], "aligned");
}

#[test]
fn gh334_vacuum_into_tolerates_unaligned_trailing_slack() {
    // A partial-page tail: the file length is not even a page multiple.
    vacuum_into_with_slack(&[0xA5_u8; 37], "unaligned");
}

#[test]
fn gh334_in_place_vacuum_tolerates_trailing_slack() {
    // Same source-receipt mechanism as VACUUM INTO: in-place VACUUM must also
    // repair (and normalize) a slack-bearing file rather than refuse it.
    let dir = tempfile::tempdir().expect("tempdir");
    let src = dir
        .path()
        .join("slack-inplace.db")
        .to_string_lossy()
        .into_owned();
    seed_db(&src);
    append_slack(&src, &[0x5A_u8; 4096 + 37]);

    let rt = RuntimeBuilder::current_thread()
        .build()
        .expect("vacuum runtime");
    let conn = rt
        .block_on(fsqlite::Connection::open(src.clone()))
        .expect("open slack-bearing source");
    rt.block_on(conn.execute("VACUUM")).unwrap_or_else(|error| {
        panic!("in-place VACUUM must tolerate trailing slack on the source, got: {error:?}")
    });
    rt.block_on(conn.close()).expect("close source");
    drop(rt);

    assert_eq!(
        count_rows(&src),
        SEED_ROWS,
        "vacuumed database must retain the full logical database"
    );
}
