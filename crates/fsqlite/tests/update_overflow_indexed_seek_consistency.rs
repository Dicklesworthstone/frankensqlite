//! Sibling audit of the prechecked-absent cursor-reuse corruption (table side
//! fixed by "fix(btree): revalidate reused cursor position before
//! prechecked-absent table insert"): a workload of repeated UPDATEs that
//! rewrite overflow-payload rows on INDEXED tables must keep every row
//! reachable through the index seek path (not just full scans), and
//! `PRAGMA integrity_check` must stay clean.
//!
//! Covers the three index-maintenance flows that reinsert entries after a
//! delete:
//!   * non-indexed-column UPDATE on an indexed rowid table (direct-update
//!     delete+reinsert on the TABLE btree, with the index left in place);
//!   * indexed-column UPDATE through a NON-UNIQUE index (IdxDelete +
//!     IdxInsert full-seek path);
//!   * indexed-column UPDATE through a UNIQUE index (IdxDelete + the
//!     unique-probe insert path that reuses its own fresh probe position);
//!   * WITHOUT ROWID table UPDATE (delete-at-position + unique reinsert on
//!     the index-structured table btree), including a PK rewrite pass.
//!
//! All data is synthetic. Payload sizes are chosen so both the table rows
//! and the index keys spill to overflow pages, and row counts are large
//! enough that the btrees are multi-level, so deletes drain leaves and
//! rebalance — the shape that broke the table-side prechecked reuse.

use fsqlite::Connection;
use fsqlite_types::SqliteValue;

const ROWS: i64 = 120;
const KEY_PAD: usize = 1100; // > index max-local for 4096-byte pages
const BLOB_HEX_BYTES: usize = 2600; // > table max-local once hex-expanded

fn key(i: i64, pass: usize) -> String {
    // Deterministic, strictly ordered by i within a pass; rotating the pad
    // character per pass forces real index-entry rewrites on k-updates.
    let pad_char = match pass {
        0 => 'a',
        1 => 'b',
        _ => 'c',
    };
    format!("k{i:06}-{}", pad_char.to_string().repeat(KEY_PAD))
}

fn blob_hex(i: i64, pass: usize) -> String {
    let byte = (i % 251 + pass as i64) % 256;
    format!("{byte:02X}").repeat(BLOB_HEX_BYTES)
}

fn has_op(c: &Connection, sql: &str, prefix: &str) -> bool {
    c.query(&format!("EXPLAIN {sql}")).unwrap().iter().any(
        |row| matches!(row.values().get(1), Some(SqliteValue::Text(o)) if o.to_string().starts_with(prefix)),
    )
}

fn assert_integrity_ok(c: &Connection, label: &str) {
    let rows = c.query("PRAGMA integrity_check").unwrap();
    let msgs: Vec<SqliteValue> = rows.iter().flat_map(|row| row.values().to_vec()).collect();
    assert_eq!(
        msgs,
        vec![SqliteValue::Text("ok".into())],
        "integrity_check after {label}"
    );
}

fn count_where(c: &Connection, sql: &str) -> i64 {
    let rows = c.query(sql).unwrap();
    match rows.first().and_then(|row| row.values().first().cloned()) {
        Some(SqliteValue::Integer(n)) => n,
        other => panic!("expected integer count from `{sql}`, got {other:?}"),
    }
}

fn assert_indexed_seek_finds_every_row(
    c: &Connection,
    table: &str,
    key_col: &str,
    pass: usize,
    expect_seek_plan: bool,
    label: &str,
) {
    // For indexed rowid tables the probe plan must contain the index seek
    // path (SeekGE). The engine's plan also carries a verify-by-scan
    // fallback branch, so the count alone cannot prove the seek found the
    // row; the `PRAGMA integrity_check` calls in each test are what would
    // flag an out-of-order entry. (WITHOUT ROWID SELECT probes currently
    // compile to a scan in this engine, so no plan assertion there — the
    // UPDATE statements' own NoConflict PK seeks, verified by re-reading
    // each rewritten row, are the seek-consistency check for that layout.)
    let probe = format!(
        "SELECT count(*) FROM {table} WHERE {key_col} = '{}'",
        key(1, pass)
    );
    if expect_seek_plan {
        assert!(
            has_op(c, &probe, "SeekGE"),
            "probe must plan an index seek (SeekGE): `{probe}`"
        );
    }
    for i in 1..=ROWS {
        let seek = count_where(
            c,
            &format!(
                "SELECT count(*) FROM {table} WHERE {key_col} = '{}'",
                key(i, pass)
            ),
        );
        assert_eq!(seek, 1, "{label}: index seek must find row {i}");
    }
    let total = count_where(c, &format!("SELECT count(*) FROM {table}"));
    assert_eq!(total, ROWS, "{label}: full scan row count");
}

/// Rowid table with a NON-UNIQUE index over an overflow-sized TEXT key.
#[test]
fn update_overflow_rows_nonunique_index_stays_seek_consistent() {
    let c = Connection::open(":memory:").unwrap();
    c.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, k TEXT, v BLOB)")
        .unwrap();
    c.execute("CREATE INDEX idx_t_k ON t(k)").unwrap();
    for i in 1..=ROWS {
        c.execute(&format!(
            "INSERT INTO t VALUES ({i}, '{}', X'{}')",
            key(i, 0),
            blob_hex(i, 0)
        ))
        .unwrap();
    }
    assert_indexed_seek_finds_every_row(&c, "t", "k", 0, true, "after load");

    // Pass 1: rewrite only the overflow BLOB (table-btree delete+reinsert;
    // index untouched). This is the SQL surface of the fixed table bug.
    for i in 1..=ROWS {
        assert_eq!(
            c.execute(&format!(
                "UPDATE t SET v = X'{}' WHERE id = {i}",
                blob_hex(i, 1)
            ))
            .unwrap(),
            1,
            "blob rewrite must update row {i}"
        );
    }
    assert_indexed_seek_finds_every_row(&c, "t", "k", 0, true, "after blob rewrites");
    assert_integrity_ok(&c, "blob rewrites (non-unique index)");

    // Pass 2: rewrite the INDEXED overflow key itself (IdxDelete+IdxInsert).
    for i in 1..=ROWS {
        assert_eq!(
            c.execute(&format!("UPDATE t SET k = '{}' WHERE id = {i}", key(i, 1)))
                .unwrap(),
            1,
            "key rewrite must update row {i}"
        );
    }
    assert_indexed_seek_finds_every_row(&c, "t", "k", 1, true, "after key rewrites");
    assert_integrity_ok(&c, "key rewrites (non-unique index)");
}

/// Rowid table with a UNIQUE index over an overflow-sized TEXT key.
#[test]
fn update_overflow_rows_unique_index_stays_seek_consistent() {
    let c = Connection::open(":memory:").unwrap();
    c.execute("CREATE TABLE u (id INTEGER PRIMARY KEY, k TEXT, v BLOB)")
        .unwrap();
    c.execute("CREATE UNIQUE INDEX idx_u_k ON u(k)").unwrap();
    for i in 1..=ROWS {
        c.execute(&format!(
            "INSERT INTO u VALUES ({i}, '{}', X'{}')",
            key(i, 0),
            blob_hex(i, 0)
        ))
        .unwrap();
    }
    assert_indexed_seek_finds_every_row(&c, "u", "k", 0, true, "after load");

    for i in 1..=ROWS {
        assert_eq!(
            c.execute(&format!(
                "UPDATE u SET v = X'{}' WHERE id = {i}",
                blob_hex(i, 1)
            ))
            .unwrap(),
            1
        );
    }
    assert_indexed_seek_finds_every_row(&c, "u", "k", 0, true, "after blob rewrites");
    assert_integrity_ok(&c, "blob rewrites (unique index)");

    for i in 1..=ROWS {
        assert_eq!(
            c.execute(&format!("UPDATE u SET k = '{}' WHERE id = {i}", key(i, 1)))
                .unwrap(),
            1
        );
    }
    assert_indexed_seek_finds_every_row(&c, "u", "k", 1, true, "after key rewrites");
    assert_integrity_ok(&c, "key rewrites (unique index)");
}

/// WITHOUT ROWID table: rows live in an index-structured btree keyed by an
/// overflow-sized TEXT primary key. UPDATEs are delete-at-position + unique
/// reinsert on that btree.
#[test]
fn update_overflow_rows_without_rowid_stays_seek_consistent() {
    let c = Connection::open(":memory:").unwrap();
    c.execute("CREATE TABLE w (k TEXT PRIMARY KEY, v BLOB) WITHOUT ROWID")
        .unwrap();
    for i in 1..=ROWS {
        c.execute(&format!(
            "INSERT INTO w VALUES ('{}', X'{}')",
            key(i, 0),
            blob_hex(i, 0)
        ))
        .unwrap();
    }
    assert_indexed_seek_finds_every_row(&c, "w", "k", 0, false, "after load");

    // NOTE: the WITHOUT ROWID UPDATE path currently reports 0 through
    // `changes()` even when it rewrites a row (observed at HEAD; rowid-table
    // UPDATEs report correctly). The assertions below therefore verify that
    // each UPDATE actually took effect by re-reading the row through its
    // PRIMARY KEY, which is also the seek-consistency check for this layout:
    // the UPDATE's own PK probe AND the verification probe must both find
    // the (re)inserted entry.

    // Pass 1: rewrite the non-PK payload (same PK reinserted).
    for i in 1..=ROWS {
        c.execute(&format!(
            "UPDATE w SET v = X'{}' WHERE k = '{}'",
            blob_hex(i, 1),
            key(i, 0)
        ))
        .unwrap();
        assert_eq!(
            count_where(
                &c,
                &format!(
                    "SELECT count(*) FROM w WHERE k = '{}' AND v = X'{}'",
                    key(i, 0),
                    blob_hex(i, 1)
                ),
            ),
            1,
            "payload rewrite must be visible for row {i}"
        );
    }
    assert_indexed_seek_finds_every_row(&c, "w", "k", 0, false, "after payload rewrites");
    assert_integrity_ok(&c, "payload rewrites (without rowid)");

    // Pass 2: rewrite the PRIMARY KEY itself (entry moves inside the btree).
    for i in 1..=ROWS {
        c.execute(&format!(
            "UPDATE w SET k = '{}' WHERE k = '{}'",
            key(i, 1),
            key(i, 0)
        ))
        .unwrap();
        assert_eq!(
            count_where(
                &c,
                &format!("SELECT count(*) FROM w WHERE k = '{}'", key(i, 1)),
            ),
            1,
            "pk rewrite must be visible for row {i}"
        );
    }
    assert_indexed_seek_finds_every_row(&c, "w", "k", 1, false, "after pk rewrites");
    assert_integrity_ok(&c, "pk rewrites (without rowid)");
}
