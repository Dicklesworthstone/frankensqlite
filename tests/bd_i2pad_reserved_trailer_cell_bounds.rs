//! B-tree cell decoders must stay inside a page's usable area (GH#426, bd-i2pad).
//!
//! A page is `usable_size` bytes of b-tree content followed by a reserved
//! trailer (`page_size - usable_size` bytes, header byte 20). The cursor's raw
//! fast paths (leaf binary search, interior descent, append hints, COUNT walk)
//! read rowids and child pointers straight from the page image. They used to
//! bound those reads by the full page length, so a cell pointer aimed into the
//! reserved trailer decoded trailer bytes as a cell: a point lookup silently
//! missed its row, or a descent followed a child pointer forged in the trailer.
//!
//! Stock SQLite (rusqlite) is the oracle: a valid reserved-bytes database must
//! read and write identically, and a pointer into the trailer must be reported
//! as corruption instead of being decoded.

// Integration tests are their own crate root and do not inherit the lib's
// `#![recursion_limit]`; match the 512 used by the other oracle suites.
#![recursion_limit = "512"]

use fsqlite_core::connection::Connection;
use fsqlite_error::ErrorCode;
use fsqlite_types::value::SqliteValue;

const PAGE_SIZE: usize = 4096;
const RESERVED: u8 = 32;
const USABLE: usize = PAGE_SIZE - RESERVED as usize;
const ROWS: i64 = 4000;

/// Where a crafted cell pointer lands: inside the reserved trailer.
const TRAILER_CELL: usize = USABLE + 4;

fn stock_rows(path: &std::path::Path, sql: &str) -> Vec<Vec<String>> {
    let conn = rusqlite::Connection::open(path).expect("stock open");
    let mut stmt = conn.prepare(sql).expect("stock prepare");
    let width = stmt.column_count();
    stmt.query_map([], |row| {
        (0..width)
            .map(|i| {
                let value: rusqlite::types::Value = row.get(i)?;
                Ok(match value {
                    rusqlite::types::Value::Null => "NULL".to_owned(),
                    rusqlite::types::Value::Integer(v) => v.to_string(),
                    rusqlite::types::Value::Real(v) => v.to_string(),
                    rusqlite::types::Value::Text(v) => v,
                    rusqlite::types::Value::Blob(v) => format!("{v:?}"),
                })
            })
            .collect::<rusqlite::Result<Vec<_>>>()
    })
    .expect("stock query")
    .map(|row| row.expect("stock row"))
    .collect()
}

fn render(value: &SqliteValue) -> String {
    match value {
        SqliteValue::Null => "NULL".to_owned(),
        SqliteValue::Integer(v) => v.to_string(),
        SqliteValue::Float(v) => v.to_string(),
        SqliteValue::Text(v) => v.as_ref().to_owned(),
        SqliteValue::Blob(v) => format!("{:?}", v.as_ref()),
    }
}

async fn fsqlite_rows(conn: &Connection, sql: &str) -> Result<Vec<Vec<String>>, fsqlite_error::FrankenError> {
    let rows = conn.query(sql).await?;
    Ok(rows
        .iter()
        .map(|row| row.values().iter().map(render).collect())
        .collect())
}

/// Stock-built 4096-byte-page database with 32 reserved bytes per page and a
/// two-level table `t` plus an index, so seeks descend an interior page.
///
/// The rusqlite API cannot set `SQLITE_FCNTL_RESERVE_BYTES`, so the empty
/// one-page image is patched exactly as stock `zeroPage` lays it out for a
/// reserved-bytes database (header byte 20, page 1 content start at the
/// usable size); stock then fills it and honors the trailer.
fn build_stock_reserved_db(path: &std::path::Path) {
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

    let conn = rusqlite::Connection::open(path).expect("stock reopen");
    conn.execute_batch(&format!(
        "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT, w INTEGER); \
         WITH RECURSIVE c(i) AS (SELECT 1 UNION ALL SELECT i + 1 FROM c WHERE i < {ROWS}) \
         INSERT INTO t SELECT i, printf('value-%06d-%s', i, hex(zeroblob(20))), (i * 7) % 1000 FROM c; \
         CREATE INDEX t_w ON t(w);"
    ))
    .expect("stock fill");
    drop(conn);
    assert_eq!(stock_rows(path, "PRAGMA integrity_check"), vec![vec!["ok".to_owned()]]);
    assert_eq!(std::fs::read(path).expect("reread")[20], RESERVED);
}

fn page_base(page_no: usize) -> usize {
    (page_no - 1) * PAGE_SIZE
}

fn be16(bytes: &[u8], at: usize) -> usize {
    usize::from(u16::from_be_bytes([bytes[at], bytes[at + 1]]))
}

fn be32(bytes: &[u8], at: usize) -> usize {
    usize::try_from(u32::from_be_bytes(bytes[at..at + 4].try_into().expect("4 bytes")))
        .expect("u32 fits usize")
}

fn varint(bytes: &[u8], at: usize) -> (u64, usize) {
    let mut value = 0u64;
    for i in 0..9 {
        let byte = bytes[at + i];
        if i == 8 {
            return ((value << 8) | u64::from(byte), 9);
        }
        value = (value << 7) | u64::from(byte & 0x7f);
        if byte & 0x80 == 0 {
            return (value, i + 1);
        }
    }
    unreachable!()
}

fn root_page_of_t(path: &std::path::Path) -> usize {
    stock_rows(path, "SELECT rootpage FROM sqlite_master WHERE name = 't'")[0][0]
        .parse()
        .expect("rootpage")
}

/// `(page, cell_count)` of an interior table page: type 0x05, 12-byte header.
fn interior_root(bytes: &[u8], root: usize) -> usize {
    let base = page_base(root);
    assert_eq!(bytes[base], 0x05, "t's root must be an interior table page");
    be16(bytes, base + 3)
}

fn assert_corrupt(result: Result<Vec<Vec<String>>, fsqlite_error::FrankenError>, what: &str) {
    match result {
        Err(err) => assert_eq!(
            err.error_code(),
            ErrorCode::Corrupt,
            "{what}: expected a corruption error, got {err}"
        ),
        Ok(rows) => panic!("{what}: decoded a cell pointer in the reserved trailer, returned {rows:?}"),
    }
}

#[test]
fn valid_reserved_bytes_database_reads_and_writes_like_stock() {
    asupersync::test_utils::run_test(|| async {
        let dir = tempfile::tempdir().expect("temp dir");
        let path = dir.path().join("reserved_valid.db");
        build_stock_reserved_db(&path);
        let db = path.to_string_lossy().into_owned();

        let queries = [
            "SELECT count(*), sum(id), sum(w), sum(length(v)) FROM t",
            "SELECT v FROM t WHERE id = 1",
            "SELECT v FROM t WHERE id = 2017",
            "SELECT v FROM t WHERE id = 4000",
            "SELECT count(*) FROM t WHERE id = 4001",
            "SELECT id FROM t WHERE id BETWEEN 1990 AND 2010 ORDER BY id",
            "SELECT id FROM t WHERE w = 77 ORDER BY id",
            "SELECT count(*), min(id), max(id) FROM t WHERE w BETWEEN 10 AND 20",
            "SELECT max(id), min(id) FROM t",
        ];

        let conn = Connection::open(&db).await.expect("fsqlite open");
        conn.execute("PRAGMA journal_mode=DELETE;").await.expect("journal_mode");
        for sql in queries {
            let got = fsqlite_rows(&conn, sql).await.expect(sql);
            assert_eq!(got, stock_rows(&path, sql), "read parity: {sql}");
        }

        // Append (rightmost-leaf hints and splits), point updates, deletes.
        conn.execute(&format!(
            "WITH RECURSIVE c(i) AS (SELECT {} UNION ALL SELECT i + 1 FROM c WHERE i < {}) \
             INSERT INTO t SELECT i, printf('more-%06d', i), i % 13 FROM c;",
            ROWS + 1,
            ROWS + 3000
        ))
        .await
        .expect("fsqlite append");
        conn.execute("UPDATE t SET v = v || '-u' WHERE id % 97 = 0;")
            .await
            .expect("fsqlite update");
        conn.execute("DELETE FROM t WHERE id % 5 = 0 AND id < 3000;")
            .await
            .expect("fsqlite delete");
        let after = "SELECT count(*), sum(id), sum(w), sum(length(v)) FROM t";
        let fsqlite_after = fsqlite_rows(&conn, after).await.expect("after");
        conn.close().await.expect("fsqlite close");

        assert_eq!(std::fs::read(&path).expect("reread")[20], RESERVED);
        assert_eq!(
            stock_rows(&path, "PRAGMA integrity_check"),
            vec![vec!["ok".to_owned()]],
            "stock must accept the reserved-bytes file fsqlite wrote"
        );
        assert_eq!(fsqlite_after, stock_rows(&path, after), "write parity");
    });
}

#[test]
fn leaf_cell_pointer_into_reserved_trailer_is_corruption() {
    asupersync::test_utils::run_test(|| async {
        let dir = tempfile::tempdir().expect("temp dir");
        let path = dir.path().join("reserved_leaf_ptr.db");
        build_stock_reserved_db(&path);

        let mut bytes = std::fs::read(&path).expect("read image");
        let root = root_page_of_t(&path);
        interior_root(&bytes, root);
        // Leftmost leaf: the left child of the root's first cell.
        let root_base = page_base(root);
        let leaf = be32(&bytes, root_base + be16(&bytes, root_base + 12));
        let leaf_base = page_base(leaf);
        assert_eq!(bytes[leaf_base], 0x0d, "page {leaf} must be a table leaf");
        let cells = be16(&bytes, leaf_base + 3);
        let last_slot = leaf_base + 8 + (cells - 1) * 2;
        let last_cell = leaf_base + be16(&bytes, last_slot);
        let (_, payload_len) = varint(&bytes, last_cell);
        let (last_rowid, _) = varint(&bytes, last_cell + payload_len);

        // Aim the last slot into the trailer and plant a plausible cell there
        // (payload 5, rowid 127). The old decoder read rowid 127 for this
        // slot, so the leaf's binary search lost the real last row.
        let trailer_ptr = u16::try_from(TRAILER_CELL).expect("fits u16");
        bytes[last_slot..last_slot + 2].copy_from_slice(&trailer_ptr.to_be_bytes());
        bytes[leaf_base + TRAILER_CELL] = 0x05;
        bytes[leaf_base + TRAILER_CELL + 1] = 0x7f;
        std::fs::write(&path, &bytes).expect("write corrupted image");
        assert_ne!(
            stock_rows(&path, "PRAGMA integrity_check"),
            vec![vec!["ok".to_owned()]],
            "the crafted pointer must be corruption to stock SQLite too"
        );

        let db = path.to_string_lossy().into_owned();
        let conn = Connection::open(&db).await.expect("fsqlite open");
        assert_corrupt(
            fsqlite_rows(&conn, &format!("SELECT v FROM t WHERE id = {last_rowid}")).await,
            "point lookup on the leaf",
        );
        assert_corrupt(
            fsqlite_rows(&conn, "SELECT count(*), sum(id) FROM t").await,
            "full scan over the leaf",
        );
        // The image is corrupt; only release the handle.
        let _ = conn.close().await;
    });
}

#[test]
fn interior_cell_pointer_into_reserved_trailer_is_corruption() {
    asupersync::test_utils::run_test(|| async {
        let dir = tempfile::tempdir().expect("temp dir");
        let path = dir.path().join("reserved_interior_ptr.db");
        build_stock_reserved_db(&path);

        let mut bytes = std::fs::read(&path).expect("read image");
        let root = root_page_of_t(&path);
        let cells = interior_root(&bytes, root);
        assert!(cells >= 4, "t must span several leaves, root has {cells} cells");
        let root_base = page_base(root);
        let first_child = be32(&bytes, root_base + be16(&bytes, root_base + 12));
        let mid = cells / 2;
        let mid_slot = root_base + 12 + mid * 2;
        let mid_cell = root_base + be16(&bytes, mid_slot);
        let (mid_key, _) = varint(&bytes, mid_cell + 4);

        // Forge an interior cell in the trailer that routes the middle key
        // range to the FIRST leaf. The old descent followed it and reported
        // the middle rows as missing.
        let trailer_ptr = u16::try_from(TRAILER_CELL).expect("fits u16");
        bytes[mid_slot..mid_slot + 2].copy_from_slice(&trailer_ptr.to_be_bytes());
        let forged_child = u32::try_from(first_child).expect("fits u32");
        let at = root_base + TRAILER_CELL;
        bytes[at..at + 4].copy_from_slice(&forged_child.to_be_bytes());
        let key = u16::try_from(mid_key).expect("mid key fits two varint bytes");
        bytes[at + 4] = 0x80 | u8::try_from(key >> 7).expect("high bits");
        bytes[at + 5] = u8::try_from(key & 0x7f).expect("low bits");
        std::fs::write(&path, &bytes).expect("write corrupted image");
        assert_ne!(
            stock_rows(&path, "PRAGMA integrity_check"),
            vec![vec!["ok".to_owned()]],
            "the crafted pointer must be corruption to stock SQLite too"
        );

        let db = path.to_string_lossy().into_owned();
        let conn = Connection::open(&db).await.expect("fsqlite open");
        assert_corrupt(
            fsqlite_rows(&conn, &format!("SELECT v FROM t WHERE id = {mid_key}")).await,
            "seek through the root",
        );
        // A rowid range walk crosses every root cell. (Plain count(*) is
        // answered from the smaller, intact index t_w.) The old decoder
        // counted the forged route's leaf twice: 4001 rows.
        assert_corrupt(
            fsqlite_rows(&conn, "SELECT count(*), sum(id) FROM t WHERE id > 0").await,
            "rowid range walk over the root",
        );
        // The image is corrupt; only release the handle.
        let _ = conn.close().await;
    });
}
