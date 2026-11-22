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
    stock_connection_reports(&conn, pragma)
}

fn stock_connection_reports(conn: &rusqlite::Connection, pragma: &str) -> Vec<String> {
    conn.prepare(pragma)
        .expect("prepare stock check")
        .query_map([], |row| row.get(0))
        .expect("run stock check")
        .collect::<rusqlite::Result<_>>()
        .expect("read stock reports")
}

async fn native_check(
    conn: &Connection,
    pragma: &str,
    prepared: bool,
) -> fsqlite_error::Result<Vec<fsqlite_core::connection::Row>> {
    if prepared {
        conn.prepare(pragma).await?.query().await
    } else {
        conn.query(pragma).await
    }
}

async fn native_reports(conn: &Connection, pragma: &str, prepared: bool) -> Vec<String> {
    let rows = native_check(conn, pragma, prepared)
        .await
        .expect("run native check");
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
    let mut pending = vec![(root, None, None, 1_usize)];
    while let Some((page_no, lower, upper, depth)) = pending.pop() {
        let start = (usize::try_from(page_no).unwrap() - 1) * page_size;
        let page = &bytes[start..start + page_size];
        let offset = if page_no == 1 { 100 } else { 0 };
        let header = fsqlite_btree::BtreePageHeader::parse(page, offset).unwrap();
        let pointers = fsqlite_btree::read_cell_pointers(page, &header, offset).unwrap();
        if header.page_type.is_leaf() {
            let bound = if cross_upper { upper } else { lower };
            let Some((bound, divider_page, divider_cell)) = bound else {
                continue;
            };
            let cell_idx = if cross_upper { pointers.len() - 1 } else { 0 };
            let pointer = usize::from(pointers[cell_idx]);
            let (_, payload_len_bytes) = read_varint(&page[pointer..]).unwrap();
            let rowid_offset = pointer + payload_len_bytes;
            let (_, old_len) = read_varint(&page[rowid_offset..]).unwrap();
            let new_rowid: i64 = if cross_upper { bound + 1 } else { bound };
            // SQLite's signed rowids use the two's-complement u64 varint.
            let encoded = u64::from_ne_bytes(new_rowid.to_ne_bytes());
            let mut encoded_bytes = [0_u8; 9];
            let new_len = write_varint(&mut encoded_bytes, encoded);
            assert_eq!(new_len, old_len, "fixture must preserve cell size");
            bytes[start + rowid_offset..start + rowid_offset + new_len]
                .copy_from_slice(&encoded_bytes[..new_len]);
            // The left-child maximum is checked on the leaf. For a
            // too-small right-child minimum, SQLite reports the separator.
            return if cross_upper {
                (page_no, cell_idx, new_rowid, depth)
            } else {
                (divider_page, divider_cell, bound, depth)
            };
        }
        let mut next = Vec::new();
        let mut previous = lower;
        for (cell_idx, pointer) in pointers.into_iter().enumerate() {
            let cell = fsqlite_btree::CellRef::parse(
                page,
                usize::from(pointer),
                header.page_type,
                usable_size,
            )
            .unwrap();
            let divider = (cell.rowid.unwrap(), page_no, cell_idx);
            next.push((
                cell.left_child.unwrap().get(),
                previous,
                Some(divider),
                depth + 1,
            ));
            previous = Some(divider);
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
                        "stock must identify the expected rowid boundary ({diagnostic}): {stock:?}"
                    );
                }
                asupersync::test_utils::run_test(|| async {
                    let conn = Connection::open(path.to_str().unwrap()).await.unwrap();
                    for pragma in ["PRAGMA integrity_check", "PRAGMA quick_check"] {
                        let oracle = stock_reports(&path, pragma);
                        for prepared in [false, true] {
                            let reports = native_reports(&conn, pragma, prepared).await;
                            assert_eq!(reports, oracle, "{pragma}, prepared={prepared}");
                        }
                    }
                    conn.close().await.unwrap();
                });
            }
        }
    }
}

#[test]
fn bd_vojth_rowid_reports_match_stock_across_attached_databases() {
    let dir = tempfile::tempdir().unwrap();
    let main = dir.path().join("main.db");
    let second = dir.path().join("attachment.db");
    for (path, start) in [(&main, -4_000), (&second, 1_000)] {
        create_stock_table(path, start, 240);
        assert_eq!(stock_reports(path, "PRAGMA integrity_check"), ["ok"]);
        let mut bytes = std::fs::read(path).unwrap();
        let size = header_page_size(&bytes);
        let mut changed = 0;
        for page in bytes.chunks_exact_mut(size).skip(2) {
            if page[0] == 0x0d && u16::from_be_bytes([page[3], page[4]]) >= 4 {
                // Exchange the second and third cell pointers. Both records and
                // both subtree bounds stay intact. Each leaf has one inversion.
                page[10..14].rotate_left(2);
                changed += 1;
                if changed == 2 {
                    break;
                }
            }
        }
        assert_eq!(changed, 2);
        std::fs::write(path, bytes).unwrap();
        let single = stock_reports(path, "PRAGMA quick_check");
        assert_eq!(single.len(), 1);
        assert_eq!(single[0].matches("out of order").count(), 2, "{single:?}");
    }

    let attach = "ATTACH DATABASE ?1 AS SeCond";
    let second = second.to_str().unwrap();
    let expected = {
        let stock = rusqlite::Connection::open(&main).unwrap();
        stock.execute(attach, [second]).unwrap();
        let mut cases = Vec::new();
        for kind in ["integrity_check", "quick_check"] {
            for schema in ["", "main.", "SECOND."] {
                for limit in ["", "(1)", "(2)", "(3)", "(4)"] {
                    let sql = format!("PRAGMA {schema}{kind}{limit}");
                    let rows = stock_connection_reports(&stock, &sql);
                    cases.push((sql, rows));
                }
            }
        }
        cases
    };
    asupersync::test_utils::run_test(|| async {
        let conn = Connection::open(main.to_str().unwrap()).await.unwrap();
        conn.execute_with_params(attach, &[SqliteValue::Text(second.into())])
            .await
            .unwrap();
        for (sql, oracle) in &expected {
            for prepared in [false, true] {
                assert_eq!(
                    native_reports(&conn, sql, prepared).await,
                    *oracle,
                    "{sql}, prepared={prepared}"
                );
            }
        }
        // Linux OFD pager locks conflict with stock's POSIX byte-range
        // locks even when each engine opens its handle in this process.
        #[cfg(target_os = "linux")]
        {
            let reader = rusqlite::Connection::open(&main).unwrap();
            reader.execute(attach, [second]).unwrap();
            reader.busy_timeout(std::time::Duration::ZERO).unwrap();
            conn.execute("PRAGMA busy_timeout=0").await.unwrap();
            let holder = rusqlite::Connection::open(second).unwrap();
            holder.execute_batch("BEGIN EXCLUSIVE").unwrap();
            for kind in ["integrity_check", "quick_check"] {
                let sql = format!("PRAGMA {kind}(1)");
                let error = reader
                    .query_row(&sql, [], |row| row.get::<_, String>(0))
                    .unwrap_err();
                assert_eq!(
                    error.sqlite_error_code(),
                    Some(rusqlite::ErrorCode::DatabaseBusy)
                );
                let qualified = format!("PRAGMA main.{kind}(1)");
                let oracle = stock_connection_reports(&reader, &qualified);
                for prepared in [false, true] {
                    let error = native_check(&conn, &sql, prepared).await.unwrap_err();
                    assert!(error.is_transient(), "{sql}: {error}");
                    assert_eq!(native_reports(&conn, &qualified, prepared).await, oracle);
                }
            }
            // Release the lock immediately after our check, with a fallback
            // before a fresh child's 5 s default expires. A child that ignores
            // the connection's timeout can then return a false success.
            let (release, requested) = std::sync::mpsc::channel::<()>();
            let unlock = std::thread::spawn(move || {
                let _ = requested.recv_timeout(std::time::Duration::from_secs(2));
                holder.execute_batch("ROLLBACK").unwrap();
            });
            let result = native_check(&conn, "PRAGMA quick_check(1)", true).await;
            let _ = release.send(());
            unlock.join().unwrap();
            assert!(result.unwrap_err().is_transient());
            assert_eq!(
                native_reports(&conn, "PRAGMA quick_check(1)", false).await,
                stock_connection_reports(&reader, "PRAGMA quick_check(1)")
            );
        }
        conn.close().await.unwrap();
    });
}

#[test]
fn bd_vojth_checks_stored_rows_after_rowid_findings() {
    let dir = tempfile::tempdir().unwrap();
    for damaged in ["t", "q"] {
        let path = dir.path().join(format!("{damaged}.db"));
        let stock = rusqlite::Connection::open(&path).unwrap();
        stock
            .execute_batch(
                "CREATE TABLE t(x CHECK(x>0)); CREATE TABLE q(x);
                 PRAGMA ignore_check_constraints=ON;
                 INSERT INTO t VALUES(1),(-2),(-3),(4);
                 INSERT INTO q VALUES(1),(2),(3),(4);",
            )
            .unwrap();
        let root: u32 = stock
            .query_row(
                "SELECT rootpage FROM sqlite_schema WHERE name=?1",
                [damaged],
                |row| row.get(0),
            )
            .unwrap();
        drop(stock);
        let mut image = std::fs::read(&path).unwrap();
        let page = usize::try_from(root - 1).unwrap() * header_page_size(&image);
        assert_eq!(image[page], 0x0d);
        image[page + 10..page + 14].rotate_left(2);
        std::fs::write(&path, image).unwrap();
        asupersync::test_utils::run_test(|| async {
            let conn = Connection::open(path.to_str().unwrap()).await.unwrap();
            for kind in ["quick_check", "integrity_check"] {
                for limit in 1..=4 {
                    let sql = format!("PRAGMA {kind}({limit})");
                    let oracle = stock_reports(&path, &sql);
                    assert!(oracle[0].contains("Rowid 3 out of order"));
                    assert_eq!(oracle.len(), limit.min(3));
                    assert!(
                        oracle[1..]
                            .iter()
                            .all(|s| s == "CHECK constraint failed in t")
                    );
                    for prepared in [false, true] {
                        assert_eq!(
                            native_reports(&conn, &sql, prepared).await,
                            oracle,
                            "{damaged}: {sql}"
                        );
                    }
                }
                for table in ["t", "q", "sqlite_schema"] {
                    let sql = format!("PRAGMA {kind}({table})");
                    let oracle = stock_reports(&path, &sql);
                    for prepared in [false, true] {
                        assert_eq!(
                            native_reports(&conn, &sql, prepared).await,
                            oracle,
                            "{damaged}: {sql}"
                        );
                    }
                }
            }
            conn.close().await.unwrap();
        });
    }
}

#[test]
fn partial_integrity_checks_resolve_tables_in_sqlite_search_order() {
    let dir = tempfile::tempdir().unwrap();
    for (name, invalid) in [("main", 2), ("First", 1), ("second", 3)] {
        let conn = rusqlite::Connection::open(dir.path().join(name)).unwrap();
        conn.execute_batch(
            "CREATE TABLE t(x CHECK(x>0)); CREATE VIEW v AS SELECT * FROM t;
             PRAGMA ignore_check_constraints=ON;",
        )
        .unwrap();
        if name != "main" {
            conn.execute_batch("CREATE TABLE child(x CHECK(x>0))")
                .unwrap();
        }
        for _ in 0..invalid {
            conn.execute("INSERT INTO t VALUES(-1)", []).unwrap();
            if name != "main" {
                conn.execute("INSERT INTO child VALUES(-1)", []).unwrap();
            }
        }
    }
    let main = dir.path().join("main");
    let stock = rusqlite::Connection::open(&main).unwrap();
    asupersync::test_utils::run_test(|| async {
        let native = Connection::open(main.to_str().unwrap()).await.unwrap();
        for schema in ["First", "second"] {
            let sql = format!("ATTACH ?1 AS {schema}");
            let path = dir.path().join(schema);
            let path = path.to_str().unwrap();
            stock.execute(&sql, [path]).unwrap();
            native
                .execute_with_params(&sql, &[SqliteValue::Text(path.into())])
                .await
                .unwrap();
        }
        for temp in [false, true] {
            if temp {
                let ddl =
                    "CREATE TEMP TABLE t(label TEXT NOT NULL); INSERT INTO temp.t VALUES('ok')";
                stock.execute_batch(ddl).unwrap();
                native.execute_batch(ddl).await.unwrap();
            }
            for kind in ["integrity_check", "quick_check"] {
                for (schema, target) in [
                    ("", "t"),
                    ("main.", "t"),
                    ("", "child"),
                    ("fIrSt.", "child"),
                    ("second.", "child"),
                    ("main.", "v"),
                    ("", "sqlite_schema"),
                    ("temp.", "sqlite_schema"),
                    ("", "absent"),
                    ("main.", "absent"),
                    ("first.", "absent"),
                    ("temp.", "t"),
                ] {
                    let sql = format!("PRAGMA {schema}{kind}('{target}')");
                    let oracle = stock.prepare(&sql);
                    for prepared in [false, true] {
                        if let Err(error) = &oracle {
                            let actual = native_check(&native, &sql, prepared).await.unwrap_err();
                            assert_eq!(actual.to_string(), error.to_string(), "{sql}");
                        } else {
                            let expected = stock_connection_reports(&stock, &sql);
                            assert_eq!(
                                native_reports(&native, &sql, prepared).await,
                                expected,
                                "{sql}"
                            );
                        }
                    }
                }
            }
        }
        // Changing LIKE semantics invalidates a stored CHECK without needing
        // to modify the connection-local TEMP database outside either engine.
        let ddl = "CREATE TEMP TABLE temp_violation(x CHECK(x LIKE 'a'));
                   INSERT INTO temp_violation VALUES('A'); PRAGMA case_sensitive_like=ON";
        stock.execute_batch(ddl).unwrap();
        native.execute_batch(ddl).await.unwrap();
        // Both attached tables violate CHECK. The order of tables inside one
        // schema is unspecified. Compare every finding before isolating the
        // schema boundary for a capped check below.
        for prepared in [false, true] {
            let sql = "PRAGMA integrity_check";
            let mut expected = stock_connection_reports(&stock, sql);
            let mut actual = native_reports(&native, sql, prepared).await;
            expected.sort();
            actual.sort();
            assert_eq!(actual, expected);
        }
        // Leave exactly one invalid table in the first attachment. The fourth
        // result can then only come from that database on either engine.
        stock.execute("DELETE FROM First.t", []).unwrap();
        for kind in ["integrity_check", "quick_check"] {
            for limit in [2, 3, 4] {
                let sql = format!("PRAGMA {kind}({limit})");
                let oracle = stock_connection_reports(&stock, &sql);
                assert_eq!(oracle.len(), limit);
                if limit >= 3 {
                    assert_eq!(oracle[2], "CHECK constraint failed in temp_violation");
                }
                if limit == 4 {
                    assert_eq!(oracle[3], "CHECK constraint failed in child");
                }
                for prepared in [false, true] {
                    assert_eq!(native_reports(&native, &sql, prepared).await, oracle, "{sql}");
                }
            }
        }
        native.close().await.unwrap();
    });
}

#[test]
fn quick_check_counts_unconditional_index_entries_in_the_selected_database() {
    let dir = tempfile::tempdir().unwrap();
    for without_rowid in [false, true] {
        let path = dir.path().join(format!("index-count-{without_rowid}.db"));
        let stock = rusqlite::Connection::open(&path).unwrap();
        let layout = if without_rowid { " WITHOUT ROWID" } else { "" };
        stock
            .execute_batch(&format!(
                "PRAGMA page_size=512;
                 CREATE TABLE t(id INTEGER PRIMARY KEY, x INTEGER){layout};
                 CREATE TABLE q(y);
                 WITH RECURSIVE seq(n) AS (VALUES(1) UNION ALL SELECT n+1 FROM seq WHERE n<900)
                 INSERT INTO t SELECT n*3,n FROM seq;
                 CREATE INDEX a ON t(x) WHERE x<>2;
                 CREATE INDEX b ON t(x+1) WHERE x<>3;
                 CREATE INDEX partial_idx ON t(x) WHERE x<100;
                 PRAGMA writable_schema=ON;
                 UPDATE sqlite_schema SET sql='CREATE INDEX a ON t(x)' WHERE name='a';
                 UPDATE sqlite_schema SET sql='CREATE INDEX b ON t(x+1)' WHERE name='b';
                 PRAGMA writable_schema=RESET;"
            ))
            .unwrap();
        let first = "wrong # of entries in index b";
        let second = "wrong # of entries in index a";
        assert_eq!(
            stock_connection_reports(&stock, "PRAGMA quick_check(t)"),
            [first, second]
        );
        asupersync::test_utils::run_test(|| async {
            let native = Connection::open(path.to_str().unwrap()).await.unwrap();
            for sql in [
                "PRAGMA quick_check", "PRAGMA quick_check(1)", "PRAGMA quick_check(2)",
                "PRAGMA quick_check(t)", "PRAGMA main.quick_check(q)",
            ] {
                let oracle = stock_connection_reports(&stock, sql);
                for prepared in [false, true] {
                    assert_eq!(native_reports(&native, sql, prepared).await, oracle, "{sql}");
                }
            }
            if without_rowid {
                for sql in ["PRAGMA integrity_check", "PRAGMA integrity_check(1)", "PRAGMA integrity_check(2)", "PRAGMA integrity_check(t)", "PRAGMA integrity_check(q)"] {
                    let oracle = stock_connection_reports(&stock, sql);
                    for prepared in [false, true] {
                        assert_eq!(native_reports(&native, sql, prepared).await, oracle, "{sql}");
                    }
                }
            }
            native.close().await.unwrap();
        });
        stock.execute_batch("REINDEX a; REINDEX b;").unwrap();
        assert_eq!(stock_connection_reports(&stock, "PRAGMA quick_check(t)"), ["ok"]);
        asupersync::test_utils::run_test(|| async {
            let native = Connection::open(path.to_str().unwrap()).await.unwrap();
            assert_eq!(native_reports(&native, "PRAGMA quick_check(t)", false).await, ["ok"]);
            native.close().await.unwrap();
        });
        if without_rowid {
            stock.execute_batch(
                "PRAGMA writable_schema=ON;
                 UPDATE sqlite_schema SET sql='CREATE INDEX partial_idx ON t(x) WHERE x<=100 AND x<>1'
                 WHERE name='partial_idx'; PRAGMA writable_schema=RESET;"
            ).unwrap();
            assert_eq!(stock_connection_reports(&stock, "PRAGMA integrity_check(t)"), ["row 100 missing from index partial_idx"]);
            asupersync::test_utils::run_test(|| async {
                let native = Connection::open(path.to_str().unwrap()).await.unwrap();
                for sql in ["PRAGMA integrity_check(t)", "PRAGMA integrity_check(q)", "PRAGMA quick_check(t)"] {
                    let oracle = stock_connection_reports(&stock, sql);
                    for prepared in [false, true] {
                        assert_eq!(native_reports(&native, sql, prepared).await, oracle, "{sql}");
                    }
                }
                native.close().await.unwrap();
            });
        }
    }
}

#[test]
fn bd_vojth_rowid_and_orphan_share_one_structural_report() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("mixed.db");
    let root = create_stock_table(&path, 1, 4);
    let mut bytes = std::fs::read(&path).unwrap();
    let size = header_page_size(&bytes);
    let page = (usize::try_from(root).unwrap() - 1) * size;
    assert_eq!(bytes[page], 0x0d);
    bytes[page + 10..page + 14].rotate_left(2);
    let orphan = u32::try_from(bytes.len() / size + 1).unwrap();
    bytes.resize(bytes.len() + size, 0);
    bytes[28..32].copy_from_slice(&orphan.to_be_bytes());
    std::fs::write(&path, bytes).unwrap();

    let stock = stock_reports(&path, "PRAGMA integrity_check(2)");
    assert_eq!(stock.len(), 1);
    let lines: Vec<_> = stock[0].lines().collect();
    assert_eq!(lines.len(), 3);
    assert!(lines[1].contains("Rowid 3 out of order"));
    assert_eq!(lines[2], format!("Page {orphan}: never used"));
    asupersync::test_utils::run_test(|| async {
        // Keep first-open repair from reclaiming the orphan before the check.
        let conn = Connection::open_schema_only(path.to_str().unwrap())
            .await
            .unwrap();
        for prepared in [false, true] {
            let native = native_reports(&conn, "PRAGMA integrity_check(2)", prepared).await;
            assert_eq!(native.len(), 1, "{native:?}");
            let grouped: Vec<_> = native[0].lines().collect();
            assert_eq!(grouped.len(), 3, "{native:?}");
            assert_eq!(grouped[..2], lines[..2]);
            assert!(grouped[2].contains(&format!("page {orphan} is never used")));
            for sql in ["PRAGMA integrity_check(1)", "PRAGMA quick_check(2)"] {
                assert_eq!(
                    native_reports(&conn, sql, prepared).await,
                    [lines[..2].join("\n")]
                );
            }
        }
        conn.close().await.unwrap();
    });
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
fn without_rowid_partial_blob_cast_uses_database_encoding() {
    let dir = tempfile::tempdir().unwrap();
    for (encoding, mixed) in [
        ("UTF-8", "4D69586544"),
        ("UTF-16le", "4D006900580065004400"),
        ("UTF-16be", "004D0069005800650044"),
    ] {
        let path = dir.path().join(format!("partial-cast-{encoding}.db"));
        let stock = rusqlite::Connection::open(&path).unwrap();
        stock.execute_batch(&format!(
            "PRAGMA encoding='{encoding}';
             CREATE TABLE t(k INTEGER PRIMARY KEY, v TEXT) WITHOUT ROWID;
             CREATE INDEX utf8 ON t(v) WHERE CAST(v AS BLOB)=X'4D69586544';
             CREATE INDEX changed ON t(v) WHERE k=2;
             INSERT INTO t VALUES(1,'MiXeD'),(2,'other');"
        )).unwrap();
        for damaged in [false, true] {
            if damaged {
                stock.execute_batch(&format!(
                    "PRAGMA writable_schema=ON;
                     UPDATE sqlite_schema SET sql='CREATE INDEX changed ON t(v) WHERE CAST(v AS BLOB)=X''{mixed}''' WHERE name='changed';
                     PRAGMA writable_schema=RESET;"
                )).unwrap();
            }
            let full = stock_connection_reports(&stock, "PRAGMA integrity_check(t)");
            assert_eq!(full, [if damaged { "row 1 missing from index changed" } else { "ok" }]);
            asupersync::test_utils::run_test(|| async {
                let conn = Connection::open(path.to_str().unwrap()).await.unwrap();
                for prepared in [false, true] {
                    assert_eq!(native_reports(&conn, "PRAGMA integrity_check(t)", prepared).await, full);
                    assert_eq!(native_reports(&conn, "PRAGMA quick_check(t)", prepared).await, ["ok"]);
                }
                conn.close().await.unwrap();
            });
        }
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
                     CREATE INDEX binary_key_covered ON binary_pk(key COLLATE BINARY,payload); \
                     CREATE INDEX binary_key_appended ON binary_pk(key COLLATE NOCASE,payload); \
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
