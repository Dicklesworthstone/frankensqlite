//! Freelist trunk capacity on databases with reserved bytes per page.
//!
//! A trunk page holds at most `usable_size / 4 - 2` leaf entries, where
//! `usable_size = page_size - reserved_bytes` (header byte 20). Stock SQLite
//! enforces that bound in `allocateBtreePage` and in `PRAGMA integrity_check`
//! ("freelist leaf count too big on page N"). The pager used to size trunks
//! from the full page size, so a bulk DELETE on a reserved-bytes database
//! (checksum VFS, SQLCipher-style layouts) wrote trunks with up to
//! `page_size / 4 - 2` entries whose tail overwrote the reserved trailer; stock
//! SQLite then reported the file corrupt. fsqlite's own integrity check parsed
//! the full page and could not see the defect.
//!
//! Stock SQLite (rusqlite) is the oracle for both directions: what fsqlite
//! writes must pass stock `integrity_check`, and an over-full trunk that stock
//! rejects must be rejected by fsqlite too.

// Integration tests are their own crate root and do not inherit the lib's
// `#![recursion_limit]`; match the 512 used by the other oracle suites.
#![recursion_limit = "512"]

use fsqlite_core::connection::Connection;
use fsqlite_types::value::SqliteValue;

const PAGE_SIZE: usize = 4096;
const RESERVED: u8 = 32;
const USABLE: usize = PAGE_SIZE - RESERVED as usize;
/// `usable / 4 - 2`: the most leaves a trunk may legally hold.
const TRUNK_CAPACITY: usize = USABLE / 4 - 2;
const ROWS: usize = 1200;

fn stock_integrity(path: &std::path::Path) -> Vec<String> {
    let conn = rusqlite::Connection::open(path).expect("stock open");
    let mut stmt = conn
        .prepare("PRAGMA integrity_check")
        .expect("stock prepare integrity_check");
    stmt.query_map([], |row| row.get::<_, String>(0))
        .expect("stock integrity_check")
        .map(|line| line.expect("stock integrity row"))
        .collect()
}

/// Build, through stock SQLite, an empty 4096-byte-page database with 32
/// reserved bytes per page.
///
/// The rusqlite API cannot set `SQLITE_FCNTL_RESERVE_BYTES`, so the empty
/// one-page image is patched exactly as stock `zeroPage` lays it out for a
/// reserved-bytes database: header byte 20 and page 1's cell-content start at
/// the usable size. Stock then fills it, honoring the reserved trailer.
fn build_stock_reserved_empty(path: &std::path::Path) {
    {
        let conn = rusqlite::Connection::open(path).expect("stock create");
        conn.execute_batch(
            "PRAGMA page_size=4096; PRAGMA journal_mode=DELETE; \
             CREATE TABLE seed(x); DROP TABLE seed; VACUUM;",
        )
        .expect("stock empty image");
    }
    let mut bytes = std::fs::read(path).expect("read empty image");
    assert_eq!(bytes.len(), PAGE_SIZE, "empty image must be one page");
    bytes[20] = RESERVED;
    let content_start = u16::try_from(USABLE).expect("usable fits u16");
    bytes[105..107].copy_from_slice(&content_start.to_be_bytes());
    std::fs::write(path, &bytes).expect("write reserved image");

    assert_eq!(
        stock_integrity(path),
        vec!["ok".to_owned()],
        "patched empty reserved-bytes image must be valid to stock SQLite"
    );
}

/// [`build_stock_reserved_empty`] plus one ~page-sized row per page in `t`.
fn build_stock_reserved_db(path: &std::path::Path) {
    build_stock_reserved_empty(path);
    let conn = rusqlite::Connection::open(path).expect("stock reopen");
    conn.execute_batch(&format!(
        "CREATE TABLE t(x); \
         WITH RECURSIVE c(i) AS (SELECT 1 UNION ALL SELECT i + 1 FROM c WHERE i < {ROWS}) \
         INSERT INTO t SELECT randomblob(3900) FROM c;"
    ))
    .expect("stock fill");
    drop(conn);
    assert_eq!(
        std::fs::read(path).expect("reread")[20],
        RESERVED,
        "stock must preserve the reserved bytes"
    );
}

/// `(trunk page, leaf count)` for every trunk in the chain, head first.
fn trunk_chain(bytes: &[u8]) -> Vec<(usize, usize)> {
    let be32 = |at: usize| {
        usize::try_from(u32::from_be_bytes(
            bytes[at..at + 4].try_into().expect("4-byte slice"),
        ))
        .expect("u32 fits usize")
    };
    let mut chain = Vec::new();
    let mut trunk = be32(32);
    while trunk != 0 {
        let base = (trunk - 1) * PAGE_SIZE;
        chain.push((trunk, be32(base + 4)));
        trunk = be32(base);
    }
    chain
}

async fn fsqlite_integrity(path: &str) -> Result<Vec<String>, String> {
    let conn = Connection::open(path).await.map_err(|err| err.to_string())?;
    let result = conn.query("PRAGMA integrity_check;").await;
    let report = result.map(|rows| {
        rows.iter()
            .filter_map(|row| match &row.values()[0] {
                SqliteValue::Text(line) => Some(line.as_ref().to_owned()),
                _ => None,
            })
            .collect()
    });
    conn.close().await.map_err(|err| err.to_string())?;
    report.map_err(|err| err.to_string())
}

#[test]
fn bulk_delete_on_reserved_bytes_db_keeps_trunks_within_usable_capacity() {
    asupersync::test_utils::run_test(|| async {
        let dir = tempfile::tempdir().expect("temp dir");
        let path = dir.path().join("reserved_delete.db");
        build_stock_reserved_db(&path);
        let db = path.to_string_lossy().into_owned();

        let conn = Connection::open(&db).await.expect("fsqlite open");
        // Rollback-journal mode writes the freelist straight into the main
        // file, so the raw image read below is the committed state.
        conn.execute("PRAGMA journal_mode=DELETE;")
            .await
            .expect("journal_mode");
        conn.execute("DELETE FROM t;").await.expect("fsqlite delete");
        conn.close().await.expect("fsqlite close");

        let bytes = std::fs::read(&path).expect("read after delete");
        assert_eq!(bytes[20], RESERVED, "reserved bytes must be preserved");
        let chain = trunk_chain(&bytes);
        let free: usize = chain.iter().map(|&(_, leaves)| leaves + 1).sum();
        assert!(
            free > TRUNK_CAPACITY + 1,
            "the reproducer must free more pages than one trunk can hold, freed {free}"
        );
        for &(trunk, leaves) in &chain {
            assert!(
                leaves <= TRUNK_CAPACITY,
                "trunk page {trunk} holds {leaves} leaves; usable capacity is {TRUNK_CAPACITY}"
            );
        }
        assert_eq!(
            stock_integrity(&path),
            vec!["ok".to_owned()],
            "stock SQLite must accept the freelist fsqlite wrote"
        );
    });
}

#[test]
fn integrity_check_rejects_trunk_leaves_in_the_reserved_trailer() {
    asupersync::test_utils::run_test(|| async {
        let dir = tempfile::tempdir().expect("temp dir");
        let path = dir.path().join("reserved_overfull.db");
        build_stock_reserved_db(&path);
        {
            let conn = rusqlite::Connection::open(&path).expect("stock reopen");
            conn.execute_batch("DELETE FROM t;").expect("stock delete");
        }

        // Stock fills trunks to `usable / 4 - 8`. Move leaves from another
        // trunk until the fullest one holds exactly one more than the legal
        // capacity. The page-1 count and every page's ownership stay the same,
        // so the over-full trunk is the image's only defect.
        let mut bytes = std::fs::read(&path).expect("read stock freelist");
        let chain = trunk_chain(&bytes);
        let &(full, full_leaves) = chain
            .iter()
            .max_by_key(|&&(_, leaves)| leaves)
            .expect("stock delete must build a freelist");
        let needed = TRUNK_CAPACITY + 1 - full_leaves;
        let &(donor, donor_leaves) = chain
            .iter()
            .find(|&&(trunk, leaves)| trunk != full && leaves >= needed)
            .expect("a second trunk must have leaves to donate");
        let full_base = (full - 1) * PAGE_SIZE;
        let donor_base = (donor - 1) * PAGE_SIZE;
        for moved in 0..needed {
            let from = donor_base + 8 + (donor_leaves - 1 - moved) * 4;
            let to = full_base + 8 + (full_leaves + moved) * 4;
            let leaf: [u8; 4] = bytes[from..from + 4].try_into().expect("leaf");
            bytes[to..to + 4].copy_from_slice(&leaf);
            bytes[from..from + 4].fill(0);
        }
        let set_count = |bytes: &mut Vec<u8>, base: usize, count: usize| {
            let count = u32::try_from(count).expect("count fits u32");
            bytes[base + 4..base + 8].copy_from_slice(&count.to_be_bytes());
        };
        set_count(&mut bytes, full_base, TRUNK_CAPACITY + 1);
        set_count(&mut bytes, donor_base, donor_leaves - needed);
        std::fs::write(&path, &bytes).expect("write over-full trunk");

        let stock = stock_integrity(&path);
        assert!(
            stock
                .iter()
                .any(|line| line.contains("freelist leaf count too big")),
            "oracle: stock SQLite must reject the over-full trunk, got {stock:?}"
        );

        let db = path.to_string_lossy().into_owned();
        match fsqlite_integrity(&db).await {
            Ok(report) => assert_ne!(
                report,
                vec!["ok".to_owned()],
                "fsqlite integrity_check must reject what stock rejects"
            ),
            Err(err) => assert!(
                err.contains("freelist") || err.to_ascii_lowercase().contains("corrupt"),
                "fsqlite must fail on the freelist defect, got {err}"
            ),
        }
    });
}

/// Content digest, read through stock SQLite.
fn stock_digest(path: &std::path::Path) -> (i64, i64, i64) {
    let conn = rusqlite::Connection::open(path).expect("stock open");
    conn.query_row(
        "SELECT count(*), sum(length(a)), sum(length(b)) FROM t",
        [],
        |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
    )
    .expect("stock digest")
}

/// VACUUM rebuilds the image through a pager opened on a fresh file, whose
/// bootstrap header has no reserved bytes; the source header (with them) is
/// stamped into page 1 only inside the rebuild transaction. The rebuild's
/// freelist trunks must still be sized for the published usable size, or
/// the pre-publication integrity gate rejects the image (and, before that
/// gate parsed the usable prefix, stock-corrupt images were published).
#[test]
fn vacuum_on_reserved_bytes_db_keeps_trunks_within_usable_capacity() {
    asupersync::test_utils::run_test(|| async {
        let dir = tempfile::tempdir().expect("temp dir");
        let path = dir.path().join("reserved_vacuum.db");
        build_stock_reserved_empty(&path);
        {
            // Overflow chains, index churn and deletes leave the source with
            // free pages and the rebuild with more than one trunk's worth.
            let conn = rusqlite::Connection::open(&path).expect("stock reopen");
            conn.execute_batch(
                "CREATE TABLE t(id INTEGER PRIMARY KEY, a TEXT, b BLOB, c REAL); \
                 CREATE INDEX ti ON t(a); \
                 WITH RECURSIVE s(i) AS (SELECT 1 UNION ALL SELECT i + 1 FROM s WHERE i < 600) \
                 INSERT INTO t SELECT i, printf('text-%05d-%.*c', i, (i * 37) % 300, 'x'), \
                   zeroblob((i * 131) % 9000) || x'0102', i * 0.25 FROM s; \
                 UPDATE t SET b = zeroblob((id * 53) % 20000) || x'ff' WHERE id % 3 = 0; \
                 DELETE FROM t WHERE id % 5 = 0; \
                 WITH RECURSIVE s(i) AS (SELECT 601 UNION ALL SELECT i + 1 FROM s WHERE i < 900) \
                 INSERT INTO t SELECT i, printf('late-%05d', i), zeroblob(i * 17) || x'0a', \
                   i * 1.5 FROM s; \
                 DELETE FROM t WHERE id BETWEEN 100 AND 400;",
            )
            .expect("stock workload");
        }
        let before = stock_digest(&path);
        let db = path.to_string_lossy().into_owned();

        let conn = Connection::open(&db).await.expect("fsqlite open");
        conn.execute("PRAGMA journal_mode=DELETE;")
            .await
            .expect("journal_mode");
        conn.execute("VACUUM;").await.expect("fsqlite VACUUM");
        conn.close().await.expect("fsqlite close");

        let bytes = std::fs::read(&path).expect("read vacuumed image");
        assert_eq!(bytes[20], RESERVED, "VACUUM must keep the reserved bytes");
        for (trunk, leaves) in trunk_chain(&bytes) {
            assert!(
                leaves <= TRUNK_CAPACITY,
                "trunk page {trunk} holds {leaves} leaves; usable capacity is {TRUNK_CAPACITY}"
            );
        }
        assert_eq!(
            stock_integrity(&path),
            vec!["ok".to_owned()],
            "stock SQLite must accept fsqlite's VACUUMed image"
        );
        assert_eq!(stock_digest(&path), before, "VACUUM must not change content");
    });
}
