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

const ASCII_ROWS: &[(i64, &str, &str)] = &[
    (1, "Alice", "Paris"),
    (2, "Bob", "Berlin"),
];
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

            conn.execute(
                "CREATE TABLE people(id INTEGER PRIMARY KEY, name TEXT, city TEXT);",
            )
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
                assert_eq!(frank_text(&row[0].values()[0]), *name, "{encoding}: name {id}");
                assert_eq!(frank_text(&row[0].values()[1]), *city, "{encoding}: city {id}");
            }
            // Index-backed lookup on freshly-written UTF-16 keys.
            let by_city = conn
                .query("SELECT id FROM people WHERE city = 'Zürich';")
                .await
                .unwrap();
            assert_eq!(by_city.len(), 1, "{encoding}: index WHERE finds the UTF-16 key");
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
            assert_eq!(integrity, "ok", "{encoding}: stock integrity_check on the fsqlite image");
            for (id, name, city) in ASCII_ROWS.iter().chain(UNICODE_ROWS.iter()) {
                let (got_name, got_city): (String, String) = stock
                    .query_row(
                        "SELECT name, city FROM people WHERE id = ?1;",
                        [*id],
                        |r| Ok((r.get(0)?, r.get(1)?)),
                    )
                    .unwrap_or_else(|e| panic!("{encoding}: stock read row {id}: {e:?}"));
                assert_eq!(&got_name, name, "{encoding}: stock name {id}");
                assert_eq!(&got_city, city, "{encoding}: stock city {id}");
            }
        }
    });
}
