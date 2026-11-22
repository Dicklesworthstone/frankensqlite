#![recursion_limit = "512"]

//! bd-y5urj: `PRAGMA integrity_check` must flag an empty non-root B-tree leaf
//! that is still referenced by its parent — stock sqlite3 3.46.1 calls that
//! shape malformed (it cannot even parse the page: `integrity_check`,
//! `quick_check`, and a plain `SELECT` all raise "database disk image is
//! malformed"). The invariant is enforced in the integrity b-tree walk
//! (`connection.rs::walk_integrity_btree_pages`: `!is_root && cell_count == 0`
//! -> `DatabaseCorrupt`).
//!
//! Root-cause of the earlier "inconclusive" investigation (EmeraldOsprey,
//! 2026-08-15): the previous fixture set `PRAGMA page_size = 512` *after*
//! `Connection::open`, but fsqlite fixes the page size when it initialises the
//! file at open, so that pragma was a no-op and the database stayed at the
//! default page size. A handful of small rows then fit on a single leaf (the
//! table root stayed `is_root` with no non-root leaf at all), and the fixture's
//! hard-coded 512-byte stride corrupted arbitrary interior bytes rather than a
//! real leaf's cell count — so the checker correctly read "ok" because there was
//! no empty non-root leaf to find. The checker was never the bug; the fixture
//! never built the shape it claimed to test.
//!
//! This version forces a genuine split: enough moderate rows that the table
//! root becomes an interior page with several non-root leaves. It reads the
//! real page size from the file header, corrupts one genuine non-root
//! table-leaf's cell count to zero, and asserts fsqlite self-reports instead of
//! reading it as "ok".

use fsqlite_core::connection::Connection;
use fsqlite_types::serial_type::{read_varint, write_varint};
use fsqlite_types::value::SqliteValue;

/// Read the database page size from the file header (bytes 16..18, big-endian;
/// the value `1` encodes 65536 per the SQLite format).
fn header_page_size(bytes: &[u8]) -> usize {
    let raw = u16::from_be_bytes([bytes[16], bytes[17]]);
    if raw == 1 { 65_536 } else { usize::from(raw) }
}

/// B-tree page-type flag byte for a 1-based page. Page 1 carries the 100-byte
/// database header before its b-tree header; every other page starts with it.
fn page_type(bytes: &[u8], page_size: usize, page_1based: usize) -> u8 {
    let base = (page_1based - 1) * page_size;
    let hdr = if page_1based == 1 { base + 100 } else { base };
    bytes[hdr]
}

/// Zero the b-tree cell count (u16 at header offset 3..5) of a 1-based page.
fn set_cell_count_zero(bytes: &mut [u8], page_size: usize, page_1based: usize) {
    let base = (page_1based - 1) * page_size;
    let hdr = if page_1based == 1 { base + 100 } else { base };
    bytes[hdr + 3] = 0;
    bytes[hdr + 4] = 0;
}

#[test]
fn integrity_check_flags_empty_non_root_leaf() {
    asupersync::test_utils::run_test(|| async {
        let dir = tempfile::tempdir().expect("temp dir");
        let db = dir
            .path()
            .join("empty_leaf.db")
            .to_string_lossy()
            .into_owned();

        {
            let conn = Connection::open(&db).await.expect("open");
            // Keep the committed image in the main database file (no WAL
            // sidecar), so a byte edit to the file lands on the read path.
            conn.execute("PRAGMA journal_mode=DELETE;")
                .await
                .expect("journal_mode");
            conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT);")
                .await
                .expect("create");
            // Enough moderate rows to split the table root into an interior page
            // with several non-root leaf children at the default page size. Each
            // ~40-byte value keeps every row inline (no overflow pages to
            // confuse the leaf scan) while still overflowing a single leaf.
            for i in 0..400 {
                conn.execute(&format!(
                    "INSERT INTO t VALUES ({i}, 'row-{i:05}-padding-abcdefghijklmnopqrstuvwxyz');"
                ))
                .await
                .expect("insert");
            }
            let ic = conn.query("PRAGMA integrity_check;").await.expect("ic");
            assert!(
                matches!(ic[0].values()[0], SqliteValue::Text(ref s) if s.as_ref() == "ok"),
                "premise: the freshly-built table must be integrity_check-ok, got {:?}",
                ic[0].values()[0]
            );
            conn.close().await.expect("close");
        }

        let mut bytes = std::fs::read(&db).expect("read db");
        let page_size = header_page_size(&bytes);
        let page_count = bytes.len() / page_size;
        assert!(
            page_count >= 3,
            "db must have split into >= 3 pages (got {page_count} pages of {page_size} bytes)"
        );

        // The table root is page 2. After the split it is an interior page
        // (0x05), so any table-leaf (0x0D) page beyond it is a genuine non-root
        // leaf. Overflow pages begin with a next-page number whose high byte is
        // 0x00 for a database this small, so they never masquerade as 0x0D.
        assert_eq!(
            page_type(&bytes, page_size, 2),
            0x05,
            "premise: 400 rows must split the table root into an interior page"
        );
        let mut target = None;
        for p in 3..=page_count {
            if page_type(&bytes, page_size, p) == 0x0D {
                target = Some(p);
                break;
            }
        }
        let target = target.expect("expected a non-root table-leaf page after the root split");
        set_cell_count_zero(&mut bytes, page_size, target);
        std::fs::write(&db, &bytes).expect("write corrupted db");

        // Reopen and check: fsqlite must now self-report the malformed page
        // rather than reading the empty non-root leaf as "ok".
        let conn = Connection::open(&db).await.expect("reopen");
        let report = match conn.query("PRAGMA integrity_check;").await {
            Ok(rows) => match &rows[0].values()[0] {
                SqliteValue::Text(s) => s.as_ref().to_owned(),
                other => panic!("integrity_check did not return text: {other:?}"),
            },
            // A hard DatabaseCorrupt error surfaced through the query is also an
            // acceptable self-report (the checker refused the malformed image).
            Err(e) => format!("{e:?}"),
        };
        assert_ne!(
            report, "ok",
            "bd-y5urj: integrity_check must not read an empty non-root leaf as ok"
        );
        assert!(
            report.contains("empty non-root")
                || report.contains("never used")
                || report.to_lowercase().contains("malformed")
                || report.to_lowercase().contains("corrupt"),
            "expected an empty-non-root / malformed self-report, got: {report}"
        );
    });
}

fn stock_reports(path: &std::path::Path, pragma: &str) -> Vec<String> {
    let conn = rusqlite::Connection::open(path).expect("open stock fixture");
    conn.prepare(pragma)
        .expect("prepare stock check")
        .query_map([], |row| row.get(0))
        .expect("run stock check")
        .collect::<rusqlite::Result<_>>()
        .expect("read stock reports")
}

async fn native_reports(conn: &Connection, pragma: &str, prepared: bool) -> Vec<String> {
    let rows = if prepared {
        conn.prepare(pragma)
            .await
            .expect("prepare native check")
            .query()
            .await
            .expect("run prepared native check")
    } else {
        conn.query(pragma).await.expect("run direct native check")
    };
    rows.iter()
        .map(|row| match &row.values()[0] {
            SqliteValue::Text(text) => text.to_string(),
            other => panic!("non-text integrity report: {other:?}"),
        })
        .collect()
}

fn create_stock_table(path: &std::path::Path, first_rowid: i64, count: i64) -> u32 {
    let mut stock = rusqlite::Connection::open(path).expect("create stock fixture");
    stock
        .execute_batch("PRAGMA page_size=512; CREATE TABLE t(value TEXT);")
        .unwrap();
    let txn = stock.transaction().unwrap();
    {
        let mut insert = txn
            .prepare("INSERT INTO t(rowid,value) VALUES(?1,?2)")
            .unwrap();
        for rowid in first_rowid..first_rowid + count {
            insert
                .execute(rusqlite::params![rowid, format!("payload-{rowid:08}")])
                .unwrap();
        }
    }
    txn.commit().unwrap();
    stock
        .query_row(
            "SELECT rootpage FROM sqlite_schema WHERE name='t'",
            [],
            |r| r.get(0),
        )
        .unwrap()
}

/// Change only one leaf's rowid, keeping its record, cell size, and within-page
/// order valid. The key crosses its parent's upper or lower divider, so a
/// page-local validator (or a cursor walk without inherited bounds) misses it.
fn cross_table_divider(bytes: &mut [u8], root: u32, cross_upper: bool) -> (u32, usize, i64, usize) {
    let page_size = header_page_size(bytes);
    let usable_size = u32::try_from(page_size - usize::from(bytes[20])).unwrap();
    let mut pending = vec![(root, None::<i64>, None::<i64>, 1_usize)];
    while let Some((page_no, lower, upper, depth)) = pending.pop() {
        let start = (usize::try_from(page_no).unwrap() - 1) * page_size;
        let page = &bytes[start..start + page_size];
        let offset = if page_no == 1 { 100 } else { 0 };
        let header = fsqlite_btree::BtreePageHeader::parse(page, offset).unwrap();
        let pointers = fsqlite_btree::read_cell_pointers(page, &header, offset).unwrap();
        if header.page_type.is_leaf() {
            let bound = if cross_upper { upper } else { lower };
            let Some(bound) = bound else { continue };
            let cell_idx = if cross_upper { pointers.len() - 1 } else { 0 };
            let pointer = usize::from(pointers[cell_idx]);
            let (_, payload_len_bytes) = read_varint(&page[pointer..]).unwrap();
            let rowid_offset = pointer + payload_len_bytes;
            let (_, old_len) = read_varint(&page[rowid_offset..]).unwrap();
            let new_rowid = if cross_upper { bound + 1 } else { bound };
            // SQLite's signed rowids use the two's-complement u64 varint.
            let encoded = u64::from_ne_bytes(new_rowid.to_ne_bytes());
            let mut encoded_bytes = [0_u8; 9];
            let new_len = write_varint(&mut encoded_bytes, encoded);
            assert_eq!(new_len, old_len, "fixture must preserve cell size");
            bytes[start + rowid_offset..start + rowid_offset + new_len]
                .copy_from_slice(&encoded_bytes[..new_len]);
            return (page_no, cell_idx, new_rowid, depth);
        }
        let mut next = Vec::new();
        let mut previous = lower;
        for pointer in pointers {
            let cell = fsqlite_btree::CellRef::parse(
                page,
                usize::from(pointer),
                header.page_type,
                usable_size,
            )
            .unwrap();
            let rowid = cell.rowid.unwrap();
            next.push((
                cell.left_child.unwrap().get(),
                previous,
                Some(rowid),
                depth + 1,
            ));
            previous = Some(rowid);
        }
        next.push((
            header.right_child.unwrap().get(),
            previous,
            upper,
            depth + 1,
        ));
        pending.extend(next.into_iter().rev());
    }
    panic!("fixture has no non-root table leaf with the requested bound");
}

#[test]
fn bd_3koph_table_subtree_bounds_match_stock() {
    let dir = tempfile::tempdir().unwrap();
    for count in [240, 4_000] {
        for first_rowid in [1_000, -9_000] {
            for cross_upper in [false, true] {
                let path = dir
                    .path()
                    .join(format!("bounds-{count}-{first_rowid}-{cross_upper}.db"));
                let root = create_stock_table(&path, first_rowid, count);
                let mut bytes = std::fs::read(&path).unwrap();
                let (page, cell, rowid, depth) = cross_table_divider(&mut bytes, root, cross_upper);
                assert!(depth >= if count == 4_000 { 3 } else { 2 });
                std::fs::write(&path, bytes).unwrap();

                let diagnostic =
                    format!("Tree {root} page {page} cell {cell}: Rowid {rowid} out of order");
                for pragma in ["PRAGMA integrity_check", "PRAGMA quick_check"] {
                    let stock = stock_reports(&path, pragma);
                    assert!(
                        stock.iter().any(|line| line.contains(&diagnostic)),
                        "stock must identify the altered leaf cell: {stock:?}"
                    );
                }
                asupersync::test_utils::run_test(|| async {
                    let conn = Connection::open(path.to_str().unwrap()).await.unwrap();
                    for pragma in ["PRAGMA integrity_check", "PRAGMA quick_check"] {
                        for prepared in [false, true] {
                            let reports = native_reports(&conn, pragma, prepared).await;
                            // The public wrapper currently prefixes structural findings
                            // with `database disk image is malformed`; the diagnostic's
                            // tree/page/cell/rowid must match the live stock oracle.
                            assert!(
                                reports.iter().any(|line| line.contains(&diagnostic)),
                                "{pragma}, prepared={prepared}: expected {diagnostic}, got {reports:?}"
                            );
                        }
                    }
                    conn.close().await.unwrap();
                });
            }
        }
    }
}

#[test]
fn bd_3koph_healthy_multilevel_collated_trees_match_stock() {
    let dir = tempfile::tempdir().unwrap();
    for count in [0, 8, 240, 4_000] {
        let path = dir.path().join(format!("healthy-{count}.db"));
        create_stock_table(&path, -2_000, count);
        {
            let stock = rusqlite::Connection::open(&path).unwrap();
            stock.execute_batch(
                "UPDATE t SET value = CASE WHEN rowid % 2 = 0 THEN upper(value) ELSE value END; \
                 CREATE INDEX idx_nocase ON t(value COLLATE NOCASE DESC); \
                 CREATE INDEX idx_rtrim ON t(value COLLATE RTRIM);",
            ).unwrap();
        }
        for pragma in ["PRAGMA integrity_check", "PRAGMA quick_check"] {
            assert_eq!(stock_reports(&path, pragma), ["ok"]);
        }
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(path.to_str().unwrap()).await.unwrap();
            for pragma in ["PRAGMA integrity_check", "PRAGMA quick_check"] {
                for prepared in [false, true] {
                    assert_eq!(native_reports(&conn, pragma, prepared).await, ["ok"]);
                }
            }
            conn.close().await.unwrap();
        });
    }
}

#[test]
fn bd_3koph_without_rowid_primary_key_collations_match_stock() {
    let dir = tempfile::tempdir().unwrap();
    for count in [0, 8, 240] {
        let path = dir
            .path()
            .join(format!("without-rowid-collations-{count}.db"));
        {
            let mut stock = rusqlite::Connection::open(&path).unwrap();
            stock
                .execute_batch(
                    "PRAGMA page_size=512; \
                     CREATE TABLE column_pk( \
                         key TEXT COLLATE NOCASE PRIMARY KEY, payload TEXT \
                     ) WITHOUT ROWID; \
                     CREATE TABLE binary_pk( \
                         key TEXT COLLATE NOCASE, payload TEXT, \
                         PRIMARY KEY(key COLLATE BINARY) \
                     ) WITHOUT ROWID; \
                     CREATE TABLE composite_pk( \
                         tenant TEXT COLLATE NOCASE, revision INTEGER, payload TEXT, \
                         PRIMARY KEY(tenant DESC, revision ASC) \
                     ) WITHOUT ROWID; \
                     CREATE TABLE repeated_pk(key TEXT, PRIMARY KEY(key, key)) WITHOUT ROWID; \
                     CREATE TABLE repeated_direction_pk( \
                         key TEXT, PRIMARY KEY(key DESC, key ASC) \
                     ) WITHOUT ROWID; \
                     CREATE TABLE repeated_default_pk( \
                         key TEXT, PRIMARY KEY(key, key COLLATE binary DESC) \
                     ) WITHOUT ROWID; \
                     CREATE TABLE repeated_collation_pk( \
                         key TEXT COLLATE NOCASE, \
                         PRIMARY KEY(key COLLATE NOCASE DESC, key COLLATE nocase ASC, \
                                     key COLLATE BINARY ASC) \
                     ) WITHOUT ROWID;",
                )
                .unwrap();
            let transaction = stock.transaction().unwrap();
            {
                let mut column_pk = transaction
                    .prepare("INSERT INTO column_pk VALUES (?1, ?2)")
                    .unwrap();
                let mut binary_pk = transaction
                    .prepare("INSERT INTO binary_pk VALUES (?1, ?2)")
                    .unwrap();
                let mut composite_pk = transaction
                    .prepare("INSERT INTO composite_pk VALUES (?1, ?2, ?3)")
                    .unwrap();
                let mut repeated_primary_keys = [
                    "INSERT INTO repeated_pk VALUES (?1)",
                    "INSERT INTO repeated_direction_pk VALUES (?1)",
                    "INSERT INTO repeated_default_pk VALUES (?1)",
                ]
                .into_iter()
                .map(|sql| transaction.prepare(sql).unwrap())
                .collect::<Vec<_>>();
                let mut repeated_collation_pk = transaction
                    .prepare("INSERT INTO repeated_collation_pk VALUES (?1)")
                    .unwrap();
                for number in 0..count {
                    // BINARY puts uppercase B before lowercase a; NOCASE
                    // orders these prefixes in the opposite direction.
                    let prefix = if number % 2 == 0 { "alpha" } else { "BETA" };
                    let key = format!("{prefix}-{number:05}");
                    let payload = format!("payload-{:05}", count - number);
                    column_pk
                        .execute(rusqlite::params![&key, &payload])
                        .unwrap();
                    binary_pk
                        .execute(rusqlite::params![&key, &payload])
                        .unwrap();
                    // Repeated tenant values exercise the ASC tie-break
                    // under the descending NOCASE primary-key prefix.
                    let tenant = format!("{prefix}-{:02}", number % 6);
                    composite_pk
                        .execute(rusqlite::params![&tenant, number / 6, &payload])
                        .unwrap();
                    for insert in &mut repeated_primary_keys {
                        insert.execute([&key]).unwrap();
                    }
                    repeated_collation_pk.execute([&key]).unwrap();
                    // Both values are legal: they compare equal under the
                    // NOCASE prefix but the retained BINARY term distinguishes
                    // them. Deduplicating by column alone would lose that term.
                    let other_case = if number % 2 == 0 {
                        key.to_ascii_uppercase()
                    } else {
                        key.to_ascii_lowercase()
                    };
                    repeated_collation_pk.execute([&other_case]).unwrap();
                }
            }
            transaction.commit().unwrap();
        }
        for pragma in ["PRAGMA integrity_check", "PRAGMA quick_check"] {
            assert_eq!(stock_reports(&path, pragma), ["ok"]);
        }
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(path.to_str().unwrap()).await.unwrap();
            for pragma in ["PRAGMA integrity_check", "PRAGMA quick_check"] {
                for prepared in [false, true] {
                    assert_eq!(
                        native_reports(&conn, pragma, prepared).await,
                        ["ok"],
                        "healthy WITHOUT ROWID primary-key order, count={count}"
                    );
                }
            }
            conn.close().await.unwrap();
        });
    }
}

#[test]
fn bd_3koph_index_subtree_bounds_respect_quick_check_scope() {
    let dir = tempfile::tempdir().unwrap();
    for without_rowid in [false, true] {
        let path = dir
            .path()
            .join(format!("index-bounds-without-rowid-{without_rowid}.db"));
        if without_rowid {
            let mut stock = rusqlite::Connection::open(&path).unwrap();
            stock
                .execute_batch(
                    "PRAGMA page_size=512; \
                     CREATE TABLE t(id INTEGER PRIMARY KEY, value TEXT) WITHOUT ROWID;",
                )
                .unwrap();
            let transaction = stock.transaction().unwrap();
            {
                let mut insert = transaction
                    .prepare("INSERT INTO t VALUES (?1, ?2)")
                    .unwrap();
                for id in 1_000..1_120 {
                    insert
                        .execute(rusqlite::params![id, format!("payload-{id:08}")])
                        .unwrap();
                }
            }
            transaction.commit().unwrap();
        } else {
            create_stock_table(&path, 1_000, 120);
        }
        let root: u32 = {
            let stock = rusqlite::Connection::open(&path).unwrap();
            let row_key = if without_rowid { "id" } else { "rowid" };
            stock
                .execute_batch(&format!(
                    "UPDATE t SET value = CASE WHEN {row_key} % 2 = 0 THEN upper(value) ELSE value END; \
                     CREATE INDEX idx_nocase ON t(value COLLATE NOCASE DESC);"
                ))
                .unwrap();
            stock
                .query_row(
                    "SELECT rootpage FROM sqlite_schema WHERE name='idx_nocase'",
                    [],
                    |row| row.get(0),
                )
                .unwrap()
        };
        assert_eq!(stock_reports(&path, "PRAGMA integrity_check"), ["ok"]);
        let mut bytes = std::fs::read(&path).unwrap();
        let page_size = header_page_size(&bytes);
        let offset = (usize::try_from(root).unwrap() - 1) * page_size;
        let page = &bytes[offset..offset + page_size];
        let header = fsqlite_btree::BtreePageHeader::parse(page, 0).unwrap();
        assert_eq!(
            header.page_type,
            fsqlite_btree::BtreePageType::InteriorIndex
        );
        let pointers = fsqlite_btree::read_cell_pointers(page, &header, 0).unwrap();
        assert!(pointers.len() >= 2);
        let first = offset + usize::from(pointers[0]);
        let second = offset + usize::from(pointers[1]);
        let first_child: [u8; 4] = bytes[first..first + 4].try_into().unwrap();
        let second_child: [u8; 4] = bytes[second..second + 4].try_into().unwrap();
        let wrong_page = u32::from_be_bytes(second_child);
        assert_eq!(
            page_type(&bytes, page_size, usize::try_from(wrong_page).unwrap()),
            0x0A,
            "fixture needs leaf children"
        );
        // Every physical key stays byte-identical and every page stays locally
        // sorted. Only the subtree placement changes, with no duplicate ownership.
        bytes[first..first + 4].copy_from_slice(&second_child);
        bytes[second..second + 4].copy_from_slice(&first_child);
        std::fs::write(&path, bytes).unwrap();

        let stock = stock_reports(&path, "PRAGMA integrity_check");
        assert!(
            stock
                .iter()
                .any(|line| line.contains("missing from index idx_nocase"))
        );
        // Stock omits index ordering/concordance from quick_check. It does not
        // diagnose a physical page for this index defect, reporting failed row
        // lookups instead. Native's bound diagnostic identifies the misplaced
        // page and cell before attempting those lookups.
        assert_eq!(stock_reports(&path, "PRAGMA quick_check"), ["ok"]);
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(path.to_str().unwrap()).await.unwrap();
            for prepared in [false, true] {
                let reports = native_reports(&conn, "PRAGMA integrity_check", prepared).await;
                let location = format!("Tree {root} page {wrong_page} cell 0:");
                assert!(
                    reports
                        .iter()
                        .any(|line| line.contains(&location) && line.contains("out of order")),
                    "missing physical index bound diagnostic {location}: {reports:?}"
                );
                assert_eq!(
                    native_reports(&conn, "PRAGMA quick_check", prepared).await,
                    ["ok"]
                );
            }
            conn.close().await.unwrap();
        });
    }
}
