//! bd-bld9w.7 capstone WRITE oracle — the corruption-safety proof.
//!
//! FrankenSQLite CREATES a UTF-16LE/BE database (via `PRAGMA encoding` on an empty
//! DB), writes ASCII + non-ASCII TEXT, and then stock `rusqlite` (C SQLite 3.46.x)
//! REOPENS the exact file and must read every value back identically, with a
//! passing `integrity_check` and the correct on-disk header encoding. This is the
//! reverse of the read oracle (`utf16_database_oracle_bld9w.rs`) and proves the
//! write-encode sweep produces byte-correct UTF-16 images, not just self-consistent
//! ones.

use fsqlite_core::connection::Connection;
use fsqlite_types::value::SqliteValue;

const ASCII_ROWS: &[(i64, &str, &str)] = &[(1, "Alice", "Paris"), (2, "Bob", "Berlin")];
const UNICODE_ROWS: &[(i64, &str, &str)] = &[
    (3, "Élise", "Zürich"),
    (4, "名前", "東京"),
    (5, "Ømega", "Þórshöfn"),
];

fn frank_text(v: &SqliteValue) -> String {
    match v {
        SqliteValue::Text(s) => s.to_string(),
        other => panic!("expected TEXT, got {other:?}"),
    }
}

/// The two UTF-16 byte orders and their on-disk header code (bytes 56..60).
const UTF16_VARIANTS: &[(&str, u32)] = &[("UTF-16le", 2), ("UTF-16be", 3)];

/// Stock C SQLite reopens the FrankenSQLite image and confirms the header
/// encoding + `integrity_check` — the corruption tripwire for the write-path
/// decode-site fixes (bd-o3rz4). A UTF-8-hardcoded decode paired with an
/// encoding-aware encode leaves stale index entries / mojibake that stock's own
/// integrity_check flags as corruption.
fn assert_stock_image_ok(db_path: &std::path::Path, expected_header: u32, encoding: &str) {
    let image = std::fs::read(db_path).unwrap();
    let header_encoding = u32::from_be_bytes(image[56..60].try_into().unwrap());
    assert_eq!(
        header_encoding, expected_header,
        "{encoding}: on-disk header byte 56..60"
    );
    let stock = rusqlite::Connection::open(db_path)
        .unwrap_or_else(|e| panic!("{encoding}: rusqlite open: {e:?}"));
    let stock_enc: String = stock
        .query_row("PRAGMA encoding;", [], |r| r.get(0))
        .unwrap();
    assert_eq!(stock_enc, encoding, "{encoding}: stock reads the encoding");
    let integrity: String = stock
        .query_row("PRAGMA integrity_check;", [], |r| r.get(0))
        .unwrap();
    assert_eq!(
        integrity, "ok",
        "{encoding}: stock integrity_check on fsqlite image"
    );
}

/// Site 1 (connection.rs:29328 prepared direct UPDATE): a single-column UPDATE
/// on a two-TEXT-column row must not corrupt the untouched TEXT column on a
/// UTF-16 DB. Before bd-o3rz4 the old row was decoded with a UTF-8-hardcoded
/// `parse_record_into` then re-serialized in the DB encoding, double-encoding
/// every column NOT in the SET clause. A single-TEXT-column table (the capstone)
/// cannot catch this — there is no untouched TEXT column.
#[test]
fn bd_o3rz4_utf16_prepared_update_preserves_untouched_text_column() {
    asupersync::test_utils::run_test(|| async {
        let dir = tempfile::tempdir().unwrap();
        for &(encoding, expected_header) in UTF16_VARIANTS {
            let db_path = dir.path().join(format!("upd_{encoding}.db"));
            let db_str = db_path.to_string_lossy().into_owned();
            let conn = Connection::open(&db_str).await.unwrap();
            conn.execute(&format!("PRAGMA encoding = '{encoding}';"))
                .await
                .unwrap();
            conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, a TEXT, b TEXT);")
                .await
                .unwrap();
            conn.execute("INSERT INTO t VALUES (1, 'Zürich', 'Élise');")
                .await
                .unwrap();
            // Prepared direct UPDATE of ONLY column a; b must survive the
            // decode/re-encode of the untouched columns. The `WHERE id = ?1`
            // placeholder shape is what routes through the prepared-direct fast
            // path (connection.rs:29328); assigning a strict subset of columns
            // forces it to decode the old payload (can_skip_old_payload_decode
            // is false), exercising the fixed decode site.
            conn.execute_with_params(
                "UPDATE t SET a = 'München' WHERE id = ?1",
                &[SqliteValue::Integer(1)],
            )
            .await
            .unwrap();
            let row = conn
                .query("SELECT a, b FROM t WHERE id = 1;")
                .await
                .unwrap();
            assert_eq!(
                frank_text(&row[0].values()[0]),
                "München",
                "{encoding}: updated col a"
            );
            assert_eq!(
                frank_text(&row[0].values()[1]),
                "Élise",
                "{encoding}: untouched col b preserved (not mojibaked)"
            );
            conn.close().await.unwrap();

            assert_stock_image_ok(&db_path, expected_header, encoding);
            let stock = rusqlite::Connection::open(&db_path).unwrap();
            let (a, b): (String, String) = stock
                .query_row("SELECT a, b FROM t WHERE id = 1;", [], |r| {
                    Ok((r.get(0)?, r.get(1)?))
                })
                .unwrap();
            assert_eq!(a, "München", "{encoding}: stock col a");
            assert_eq!(b, "Élise", "{encoding}: stock untouched col b");
        }
    });
}

/// Sites 2a/2b (engine.rs:7850 native_replace_row + 8013 UPDATE-conflict
/// restore): INSERT OR REPLACE that conflicts on a UNIQUE TEXT index must delete
/// the old row's index entry. Before bd-o3rz4 the old row was decoded UTF-8 and
/// the delete probe re-encoded in the DB encoding never matched the stored
/// UTF-16 key, orphaning the entry — stock's integrity_check flags the stale
/// index entry.
#[test]
fn bd_o3rz4_utf16_or_replace_conflict_leaves_no_stale_index_entry() {
    asupersync::test_utils::run_test(|| async {
        let dir = tempfile::tempdir().unwrap();
        for &(encoding, expected_header) in UTF16_VARIANTS {
            let db_path = dir.path().join(format!("orr_{encoding}.db"));
            let db_str = db_path.to_string_lossy().into_owned();
            let conn = Connection::open(&db_str).await.unwrap();
            conn.execute(&format!("PRAGMA encoding = '{encoding}';"))
                .await
                .unwrap();
            conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, city TEXT UNIQUE);")
                .await
                .unwrap();
            conn.execute("INSERT INTO t VALUES (1, 'Zürich');")
                .await
                .unwrap();
            conn.execute("INSERT INTO t VALUES (2, 'Berlin');")
                .await
                .unwrap();
            // city 'Zürich' conflicts with row 1 on the UNIQUE index → row 1 is
            // replaced; its old index entry must be removed, not orphaned.
            conn.execute("INSERT OR REPLACE INTO t VALUES (3, 'Zürich');")
                .await
                .unwrap();

            let hit = conn
                .query("SELECT id FROM t WHERE city = 'Zürich';")
                .await
                .unwrap();
            assert_eq!(
                hit.len(),
                1,
                "{encoding}: exactly one 'Zürich' after replace"
            );
            assert_eq!(
                hit[0].values()[0],
                SqliteValue::Integer(3),
                "{encoding}: replacement row"
            );
            conn.close().await.unwrap();

            // The strongest proof: stock's own integrity_check catches a stale
            // index entry that points at the deleted rowid.
            assert_stock_image_ok(&db_path, expected_header, encoding);
            let stock = rusqlite::Connection::open(&db_path).unwrap();
            let ids: Vec<i64> = stock
                .prepare("SELECT id FROM t WHERE city = 'Zürich' ORDER BY id")
                .unwrap()
                .query_map([], |r| r.get(0))
                .unwrap()
                .collect::<std::result::Result<Vec<_>, _>>()
                .unwrap();
            assert_eq!(
                ids,
                vec![3],
                "{encoding}: stock sees only the replacement via the index"
            );
        }
    });
}

/// Site 5 (engine.rs:11697 IdxDelete REPLACE-victim capture, family b): a WITHOUT
/// ROWID INSERT OR REPLACE must delete the replaced row's SECONDARY-index entry.
/// The victim is captured by decoding the stored WITHOUT ROWID record; before
/// bd-o3rz4 that decode was UTF-8-hardcoded, so on a UTF-16 DB the victim's
/// re-encoded index-delete probe missed the stored key and orphaned the entry.
#[test]
fn bd_o3rz4_utf16_without_rowid_replace_cleans_secondary_index() {
    asupersync::test_utils::run_test(|| async {
        let dir = tempfile::tempdir().unwrap();
        for &(encoding, expected_header) in UTF16_VARIANTS {
            let db_path = dir.path().join(format!("wr_{encoding}.db"));
            let db_str = db_path.to_string_lossy().into_owned();
            let conn = Connection::open(&db_str).await.unwrap();
            conn.execute(&format!("PRAGMA encoding = '{encoding}';"))
                .await
                .unwrap();
            conn.execute("CREATE TABLE t(city TEXT PRIMARY KEY, note TEXT) WITHOUT ROWID;")
                .await
                .unwrap();
            conn.execute("CREATE INDEX idx_note ON t(note);")
                .await
                .unwrap();
            conn.execute("INSERT INTO t VALUES ('Zürich', 'Übung');")
                .await
                .unwrap();
            // PK 'Zürich' conflicts → the old row (note 'Übung') is the replace
            // victim; its idx_note='Übung' entry must be removed, not orphaned.
            conn.execute("INSERT OR REPLACE INTO t VALUES ('Zürich', 'Neu');")
                .await
                .unwrap();

            let stale = conn
                .query("SELECT city FROM t WHERE note = 'Übung';")
                .await
                .unwrap();
            assert!(
                stale.is_empty(),
                "{encoding}: replaced note is not matchable via the index"
            );
            let cur = conn
                .query("SELECT note FROM t WHERE city = 'Zürich';")
                .await
                .unwrap();
            assert_eq!(
                frank_text(&cur[0].values()[0]),
                "Neu",
                "{encoding}: current note"
            );
            conn.close().await.unwrap();

            // Stock integrity_check catches an orphaned secondary-index entry.
            assert_stock_image_ok(&db_path, expected_header, encoding);
            let stock = rusqlite::Connection::open(&db_path).unwrap();
            let by_note: Vec<String> = stock
                .prepare("SELECT city FROM t WHERE note = 'Übung'")
                .unwrap()
                .query_map([], |r| r.get(0))
                .unwrap()
                .collect::<std::result::Result<Vec<_>, _>>()
                .unwrap();
            assert!(
                by_note.is_empty(),
                "{encoding}: stock finds no stale idx_note entry"
            );
        }
    });
}

/// Site 3 (engine.rs:14435 TEMP FusedAppendInsert): a TEMP row serialized in the
/// DB encoding must be decoded in the same encoding — before bd-o3rz4 it was
/// parsed UTF-8, mojibaking the TEMP row and lossily re-encoding it into main.
#[test]
fn bd_o3rz4_utf16_temp_table_round_trip_into_main() {
    asupersync::test_utils::run_test(|| async {
        let dir = tempfile::tempdir().unwrap();
        for &(encoding, expected_header) in UTF16_VARIANTS {
            let db_path = dir.path().join(format!("temp_{encoding}.db"));
            let db_str = db_path.to_string_lossy().into_owned();
            let conn = Connection::open(&db_str).await.unwrap();
            conn.execute(&format!("PRAGMA encoding = '{encoding}';"))
                .await
                .unwrap();
            conn.execute("CREATE TABLE main_t(id INTEGER PRIMARY KEY, v TEXT);")
                .await
                .unwrap();
            conn.execute("CREATE TEMP TABLE tmp(v TEXT);")
                .await
                .unwrap();
            // Append-inserts into a TEMP table exercise the FusedAppendInsert
            // TEMP branch.
            conn.execute("INSERT INTO tmp(v) VALUES ('Zürich');")
                .await
                .unwrap();
            conn.execute("INSERT INTO tmp(v) VALUES ('東京');")
                .await
                .unwrap();

            let temp_read = conn.query("SELECT v FROM tmp ORDER BY v;").await.unwrap();
            let mut got: Vec<String> = temp_read
                .iter()
                .map(|r| frank_text(&r.values()[0]))
                .collect();
            got.sort();
            let mut want = vec!["Zürich".to_owned(), "東京".to_owned()];
            want.sort();
            assert_eq!(
                got, want,
                "{encoding}: TEMP round-trip preserves non-ASCII TEXT"
            );

            // Flow TEMP → main; the copy must not lossily re-encode.
            conn.execute("INSERT INTO main_t(v) SELECT v FROM tmp;")
                .await
                .unwrap();
            conn.close().await.unwrap();

            assert_stock_image_ok(&db_path, expected_header, encoding);
            let stock = rusqlite::Connection::open(&db_path).unwrap();
            let mut main_vals: Vec<String> = stock
                .prepare("SELECT v FROM main_t")
                .unwrap()
                .query_map([], |r| r.get(0))
                .unwrap()
                .collect::<std::result::Result<Vec<_>, _>>()
                .unwrap();
            main_vals.sort();
            assert_eq!(
                main_vals, want,
                "{encoding}: stock reads TEMP→main copy intact"
            );
        }
    });
}

/// Site 4 (engine.rs:12409 IdxGT/GE/LT/LE probe): a range seek over a UTF-16
/// TEXT index must decode the probe key in the cursor's encoding — before
/// bd-o3rz4 the probe was parsed UTF-8 while the cursor key used the DB
/// encoding, so every TEXT index seek mis-compared. Stock on the identical
/// image is the oracle for the expected result set.
#[test]
fn bd_o3rz4_utf16_range_seek_matches_stock() {
    asupersync::test_utils::run_test(|| async {
        let dir = tempfile::tempdir().unwrap();
        // ASCII cities: the seek PROBE ('Berlin') is still UTF-16-encoded on a
        // UTF-16 DB (so a UTF-8-hardcoded probe decode still mis-compares and is
        // exercised), while ASCII keys keep BINARY ordering identical between
        // frank (UTF-8 internal) and stock (UTF-16), so the oracle comparison is
        // not confounded by cross-encoding collation differences.
        let cities = ["Alice", "Berlin", "Cairo", "Delhi", "Zanzibar", "Boston"];
        for &(encoding, expected_header) in UTF16_VARIANTS {
            let db_path = dir.path().join(format!("range_{encoding}.db"));
            let db_str = db_path.to_string_lossy().into_owned();
            let conn = Connection::open(&db_str).await.unwrap();
            conn.execute(&format!("PRAGMA encoding = '{encoding}';"))
                .await
                .unwrap();
            conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, city TEXT);")
                .await
                .unwrap();
            conn.execute("CREATE INDEX idx_city ON t(city);")
                .await
                .unwrap();
            for (i, c) in cities.iter().enumerate() {
                conn.execute(&format!("INSERT INTO t VALUES ({}, '{c}');", i + 1))
                    .await
                    .unwrap();
            }
            const RANGE_SQL: &str = "SELECT city FROM t WHERE city > 'Berlin' ORDER BY city;";
            let frank_rows = conn.query(RANGE_SQL).await.unwrap();
            let frank_cities: Vec<String> = frank_rows
                .iter()
                .map(|r| frank_text(&r.values()[0]))
                .collect();
            conn.close().await.unwrap();

            assert_stock_image_ok(&db_path, expected_header, encoding);
            let stock = rusqlite::Connection::open(&db_path).unwrap();
            let stock_cities: Vec<String> = stock
                .prepare(RANGE_SQL)
                .unwrap()
                .query_map([], |r| r.get(0))
                .unwrap()
                .collect::<std::result::Result<Vec<_>, _>>()
                .unwrap();
            assert_eq!(
                frank_cities, stock_cities,
                "{encoding}: range-seek result set matches stock (probe decoded in cursor encoding)"
            );
        }
    });
}

#[test]
fn bd_bld9w_utf16_write_oracle_fsqlite_writes_rusqlite_validates() {
    asupersync::test_utils::run_test(|| async {
        let dir = tempfile::tempdir().unwrap();
        for (encoding, expected_header) in [("UTF-16le", 2_u32), ("UTF-16be", 3_u32)] {
            let db_path = dir.path().join(format!("write_oracle_{encoding}.db"));
            let db_str = db_path.to_string_lossy().into_owned();

            // ── FrankenSQLite writes a fresh UTF-16 database. ──────────────────
            let conn = Connection::open(&db_str)
                .await
                .expect("open fresh file-backed DB");
            conn.execute(&format!("PRAGMA encoding = '{encoding}';"))
                .await
                .expect("set UTF-16 encoding on the empty database");

            // The setter took effect (header-backed read-back).
            let enc_row = conn.query("PRAGMA encoding;").await.unwrap();
            assert_eq!(
                frank_text(&enc_row[0].values()[0]),
                encoding,
                "{encoding}: PRAGMA encoding read-back after set"
            );

            conn.execute("CREATE TABLE people(id INTEGER PRIMARY KEY, name TEXT, city TEXT);")
                .await
                .unwrap();
            conn.execute("CREATE INDEX idx_city ON people(city);")
                .await
                .unwrap();
            for (id, name, city) in ASCII_ROWS.iter().chain(UNICODE_ROWS.iter()) {
                conn.execute(&format!(
                    "INSERT INTO people VALUES ({id}, '{name}', '{city}');"
                ))
                .await
                .unwrap_or_else(|e| panic!("{encoding}: INSERT ({id},{name}) failed: {e:?}"));
            }

            // Round-trip inside FrankenSQLite: values come back as canonical UTF-8.
            for (id, name, city) in ASCII_ROWS.iter().chain(UNICODE_ROWS.iter()) {
                let row = conn
                    .query(&format!("SELECT name, city FROM people WHERE id = {id};"))
                    .await
                    .unwrap();
                assert_eq!(row.len(), 1, "{encoding}: row {id} readable");
                assert_eq!(
                    frank_text(&row[0].values()[0]),
                    *name,
                    "{encoding}: name {id}"
                );
                assert_eq!(
                    frank_text(&row[0].values()[1]),
                    *city,
                    "{encoding}: city {id}"
                );
            }
            // Index-backed lookup on freshly-written UTF-16 keys.
            let by_city = conn
                .query("SELECT id FROM people WHERE city = 'Zürich';")
                .await
                .unwrap();
            assert_eq!(
                by_city.len(),
                1,
                "{encoding}: index WHERE finds the UTF-16 key"
            );
            assert_eq!(by_city[0].values()[0], SqliteValue::Integer(3));

            // VACUUM must preserve the encoding AND produce a stock-decodable image
            // (family e serialize + the UTF-16 MemDatabase-hydration decode fix —
            // without the decode fix VACUUM double-encodes UTF-16 TEXT).
            conn.execute("VACUUM;").await.expect("VACUUM a UTF-16 DB");
            let after_vacuum = conn.query("PRAGMA encoding;").await.unwrap();
            assert_eq!(
                frank_text(&after_vacuum[0].values()[0]),
                encoding,
                "{encoding}: VACUUM preserves the header encoding"
            );

            conn.close().await.expect("flush + close");

            // Reopen with FrankenSQLite and confirm it still decodes after VACUUM.
            {
                let reopened = Connection::open(&db_str).await.expect("fsqlite reopen");
                let r = reopened
                    .query("SELECT name, city FROM people WHERE id = 3;")
                    .await
                    .unwrap();
                assert_eq!(
                    frank_text(&r[0].values()[0]),
                    "Élise",
                    "{encoding}: fsqlite reopen decodes non-ASCII TEXT after VACUUM"
                );
                reopened.close().await.unwrap();
            }

            // ── On-disk header records the UTF-16 encoding. ───────────────────
            let image = std::fs::read(&db_path).unwrap();
            let header_encoding = u32::from_be_bytes(image[56..60].try_into().unwrap());
            assert_eq!(
                header_encoding, expected_header,
                "{encoding}: on-disk header byte 56..60"
            );

            // ── Stock C SQLite reopens and validates the FrankenSQLite image. ─
            let stock = rusqlite::Connection::open(&db_path)
                .unwrap_or_else(|e| panic!("{encoding}: rusqlite open of fsqlite image: {e:?}"));
            let stock_enc: String = stock
                .query_row("PRAGMA encoding;", [], |r| r.get(0))
                .unwrap();
            assert_eq!(stock_enc, encoding, "{encoding}: stock reads the encoding");
            let integrity: String = stock
                .query_row("PRAGMA integrity_check;", [], |r| r.get(0))
                .unwrap();
            assert_eq!(
                integrity, "ok",
                "{encoding}: stock integrity_check on the fsqlite image"
            );
            for (id, name, city) in ASCII_ROWS.iter().chain(UNICODE_ROWS.iter()) {
                let (got_name, got_city): (String, String) = stock
                    .query_row("SELECT name, city FROM people WHERE id = ?1;", [*id], |r| {
                        Ok((r.get(0)?, r.get(1)?))
                    })
                    .unwrap_or_else(|e| panic!("{encoding}: stock read row {id}: {e:?}"));
                assert_eq!(&got_name, name, "{encoding}: stock name {id}");
                assert_eq!(&got_city, city, "{encoding}: stock city {id}");
            }
        }
    });
}
