//! Regression oracle (GH #287 / #203 field corruption): repeated UPDATEs that
//! rewrite rows via the delete+reinsert path must keep every row reachable by
//! rowid POINT SEEK — not just by full scan — with `PRAGMA integrity_check`
//! clean, on a file-backed database driven through the real pager/B-tree
//! stack.
//!
//! The broken shape: a row that is the sole cell of its leaf AND the maximum
//! of a subtree whose separator lives at grandparent-or-higher level. Deleting
//! it drains the leaf, rebalances, and re-anchors the cursor across the
//! stale-but-legal ancestor separator; pre-fix, the prechecked-absent reinsert
//! blindly reused that position and committed an out-of-order table B-tree
//! (rows scan-visible but seek-unreachable; stock SQLite integrity_check says
//! "Rowid out of order"; REINDEX cannot repair it, only VACUUM can).
//!
//! Payload sizes are chosen so every row owns its own leaf (locally stored,
//! >half a 4096-byte page) and the row count forces a depth-3 tree so subtree
//! separators live at the root. The UPDATE changes the payload LENGTH so the
//! same-size in-place overwrite fast path must decline and take the
//! delete+reinsert route. Results are compared row-for-row against C SQLite.

use fsqlite::Connection;
use fsqlite_types::SqliteValue;

const ROWS: i64 = 700;
/// 2500 bytes: fully local on a 4096-byte page, but two cells cannot share a
/// leaf, so every row owns its own leaf.
const OLD_BYTES: usize = 2500;
/// Different length so `table_overwrite_current_payload_same_size_no_overflow`
/// declines and the UPDATE takes delete()+table_insert_prechecked_absent.
const NEW_BYTES: usize = 2600;

fn blob_hex(byte: u8, len: usize) -> String {
    format!("{byte:02X}").repeat(len)
}

fn count_f(c: &Connection, sql: &str) -> i64 {
    let rows = c
        .query(sql)
        .unwrap_or_else(|e| panic!("frank `{sql}`: {e}"));
    match rows.first().and_then(|r| r.values().first().cloned()) {
        Some(SqliteValue::Integer(n)) => n,
        other => panic!("expected integer from `{sql}`, got {other:?}"),
    }
}

fn count_r(c: &rusqlite::Connection, sql: &str) -> i64 {
    c.query_row(sql, [], |row| row.get::<_, i64>(0)).unwrap()
}

fn integrity_ok(c: &Connection, label: &str) {
    let rows = c.query("PRAGMA integrity_check").unwrap();
    let msgs: Vec<SqliteValue> = rows.iter().flat_map(|r| r.values().to_vec()).collect();
    assert_eq!(
        msgs,
        vec![SqliteValue::Text("ok".into())],
        "integrity_check after {label}"
    );
}

#[test]
fn update_rewrite_keeps_every_row_rowid_seek_reachable() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("rewrite_seek.db");
    let frank = Connection::open(db_path.to_str().unwrap()).unwrap();
    let oracle = rusqlite::Connection::open_in_memory().unwrap();

    let ddl = "CREATE TABLE t (id INTEGER PRIMARY KEY, v BLOB)";
    frank.execute(ddl).unwrap();
    oracle.execute_batch(ddl).unwrap();

    let old_hex = blob_hex(0x5A, OLD_BYTES);
    for i in 1..=ROWS {
        let sql = format!("INSERT INTO t VALUES ({i}, X'{old_hex}')");
        frank.execute(&sql).unwrap();
        oracle.execute_batch(&sql).unwrap();
    }
    integrity_ok(&frank, "load");

    // UPDATE-rewrite every row. The new payload length differs, so the
    // in-place same-size overwrite declines and the engine takes the
    // delete+reinsert route for each row — including the rows whose rowids
    // equal root/grandparent separator keys.
    let new_hex = blob_hex(0xC6, NEW_BYTES);
    for i in 1..=ROWS {
        let sql = format!("UPDATE t SET v = X'{new_hex}' WHERE id = {i}");
        frank.execute(&sql).unwrap();
        oracle.execute_batch(&sql).unwrap();
    }

    // Every row must remain reachable through a rowid POINT SEEK (the
    // pre-fix corruption left rows scan-visible but seek-unreachable), and
    // both engines must agree row-for-row.
    let mut unreachable = Vec::new();
    for i in 1..=ROWS {
        let probe = format!("SELECT count(*) FROM t WHERE id = {i} AND length(v) = {NEW_BYTES}");
        let f = count_f(&frank, &probe);
        let r = count_r(&oracle, &probe);
        assert_eq!(
            f, r,
            "engines diverged on `{probe}`: frank {f} vs sqlite {r}"
        );
        if f != 1 {
            unreachable.push(i);
        }
    }
    assert!(
        unreachable.is_empty(),
        "{} of {ROWS} rewritten rows are not reachable by rowid seek; first: {:?}",
        unreachable.len(),
        &unreachable[..unreachable.len().min(10)]
    );

    // Scan totals agree and the file structure is sound.
    assert_eq!(count_f(&frank, "SELECT count(*) FROM t"), ROWS);
    assert_eq!(count_r(&oracle, "SELECT count(*) FROM t"), ROWS);
    integrity_ok(&frank, "update rewrites");
}

#[test]
fn update_rewrite_overflow_rows_stay_seek_reachable() {
    // Overflow-chain variant: 14776-byte payloads spill to overflow pages
    // (local portion ~2500 bytes keeps leaves singleton). Guards the same
    // rewrite path where the deleted cell also frees an overflow chain.
    const OVERFLOW_ROWS: i64 = 160;
    const OVERFLOW_OLD: usize = 14_776;
    const OVERFLOW_NEW: usize = 14_900;

    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("rewrite_overflow.db");
    let frank = Connection::open(db_path.to_str().unwrap()).unwrap();
    let oracle = rusqlite::Connection::open_in_memory().unwrap();

    let ddl = "CREATE TABLE t (id INTEGER PRIMARY KEY, v BLOB)";
    frank.execute(ddl).unwrap();
    oracle.execute_batch(ddl).unwrap();

    let old_hex = blob_hex(0x3C, OVERFLOW_OLD);
    for i in 1..=OVERFLOW_ROWS {
        let sql = format!("INSERT INTO t VALUES ({i}, X'{old_hex}')");
        frank.execute(&sql).unwrap();
        oracle.execute_batch(&sql).unwrap();
    }
    integrity_ok(&frank, "overflow load");

    let new_hex = blob_hex(0x99, OVERFLOW_NEW);
    for i in 1..=OVERFLOW_ROWS {
        let sql = format!("UPDATE t SET v = X'{new_hex}' WHERE id = {i}");
        frank.execute(&sql).unwrap();
        oracle.execute_batch(&sql).unwrap();
    }

    let mut unreachable = Vec::new();
    for i in 1..=OVERFLOW_ROWS {
        let probe = format!("SELECT count(*) FROM t WHERE id = {i} AND length(v) = {OVERFLOW_NEW}");
        let f = count_f(&frank, &probe);
        let r = count_r(&oracle, &probe);
        assert_eq!(
            f, r,
            "engines diverged on `{probe}`: frank {f} vs sqlite {r}"
        );
        if f != 1 {
            unreachable.push(i);
        }
    }
    assert!(
        unreachable.is_empty(),
        "{} of {OVERFLOW_ROWS} rewritten overflow rows are not reachable by rowid seek; first: {:?}",
        unreachable.len(),
        &unreachable[..unreachable.len().min(10)]
    );
    assert_eq!(count_f(&frank, "SELECT count(*) FROM t"), OVERFLOW_ROWS);
    integrity_ok(&frank, "overflow update rewrites");
}
