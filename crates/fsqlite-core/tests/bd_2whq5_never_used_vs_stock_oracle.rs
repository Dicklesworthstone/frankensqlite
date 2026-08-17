//! bd-2whq5 (P1): fsqlite `integrity_check` reports "page N is never used" on a
//! (reconstruct/churn) database that stock sqlite3 accepts as `ok`.
//!
//! ORACLE INVARIANT under test: for any database file, if bundled stock sqlite
//! (`rusqlite`) `PRAGMA integrity_check` == `ok`, then fsqlite's own
//! `PRAGMA integrity_check` MUST also be `ok`. A divergence (stock ok, fsqlite
//! "never used") is a live defect — either a checker false-positive (an
//! ownership-walk gap: stale `self.schema` cache, freelist-walk gap, overflow
//! gap) or a reconstruct-writer bug (compat_persist leaves a page unlinked that
//! stock tolerates, e.g. a trailing page beyond `page_count`).
//!
//! We drive fsqlite through reconstruct (VACUUM INTO) and runtime-churn paths
//! that build a real freelist / force b-tree splits / overflow chains, then
//! reopen the produced image and compare. VACUUM INTO already self-validates
//! with fsqlite's checker on a fresh reload, so these tests additionally re-run
//! the comparison through independent fresh reopens.

use fsqlite_core::connection::Connection;
use fsqlite_types::value::SqliteValue;

/// Stock (bundled sqlite3 via rusqlite) integrity_check joined into one string.
fn stock_integrity(path: &std::path::Path) -> String {
    let conn = rusqlite::Connection::open(path).expect("stock open");
    let mut stmt = conn.prepare("PRAGMA integrity_check;").expect("stock prepare");
    let rows: Vec<String> = stmt
        .query_map([], |r| r.get::<_, String>(0))
        .expect("stock query")
        .collect::<std::result::Result<_, _>>()
        .expect("stock rows");
    rows.join("; ")
}

/// fsqlite integrity_check text rows.
async fn fsqlite_integrity(conn: &Connection) -> Vec<String> {
    let rows = conn
        .query("PRAGMA integrity_check;")
        .await
        .expect("fsqlite integrity_check");
    rows.iter()
        .filter_map(|row| match row.values().first() {
            Some(SqliteValue::Text(s)) => Some(s.as_ref().to_owned()),
            _ => None,
        })
        .collect()
}

/// Assert the oracle invariant on a produced image at `path`, opening a FRESH
/// fsqlite connection so the schema cache is loaded from disk exactly like a
/// "promoted family copy" would be.
async fn assert_oracle_invariant(path: &std::path::Path, scenario: &str) {
    let stock = stock_integrity(path);
    let conn = Connection::open(path.to_string_lossy().as_ref())
        .await
        .expect("fsqlite reopen");
    let frank = fsqlite_integrity(&conn).await;
    conn.close().await.expect("close");

    let frank_ok = frank.len() == 1 && frank[0].eq_ignore_ascii_case("ok");
    let stock_ok = stock == "ok";

    eprintln!("[{scenario}] stock={stock:?} fsqlite={frank:?}");

    if stock_ok {
        assert!(
            frank_ok,
            "[{scenario}] DIVERGENCE: stock=ok but fsqlite integrity_check={frank:?}"
        );
    }
}

fn tmp() -> tempfile::TempDir {
    tempfile::tempdir().expect("temp dir")
}

/// RAW, UNMASKED bd-2whq5 divergence.
///
/// `Connection::open` runs the first-open migration pass (migration.rs), which
/// on a checker failure BACKS UP and REPAIRS the image — so the plain-open
/// oracle tests above end at "ok" only because fsqlite silently re-freed the
/// trailing slack into the freelist (see the "applied migration repairs" lines
/// they print). `open_schema_only` does NOT run that migration, so it exposes
/// what fsqlite's `PRAGMA integrity_check` actually reports — exactly the bead's
/// "integrity probe on a promoted family copy".
///
/// A canonical stock image is built, then ONE zero-filled physical page is
/// appended beyond the header `page_count`. Stock ignores it (header size is
/// authoritative when `version_valid_for == change_counter`) and reports `ok`;
/// fsqlite's pager adopts `max(header.page_count, file_pages)` (pager.rs ~8672)
/// and its orphan scan flags the slack page as "page N is never used".
///
/// REGRESSION GUARD (bd-2whq5 fix landed): with the orphan scan bounded by the
/// header page_count when it is not stale (connection.rs
/// `validate_database_integrity`), fsqlite must now MATCH stock and report `ok`
/// for stock-legal trailing slack, even on the migration-bypassing
/// `open_schema_only` path. (Pre-fix this asserted the divergence was present.)
#[test]
fn raw_trailing_slack_divergence_via_schema_only_open() {
    asupersync::test_utils::run_test(|| async {
        let dir = tmp();
        let db = dir.path().join("raw_slack.db");
        let page_size = {
            let stock = rusqlite::Connection::open(&db).expect("stock open");
            stock.pragma_update(None, "journal_mode", "DELETE").ok();
            stock
                .execute_batch(
                    "CREATE TABLE t(id INTEGER PRIMARY KEY, a INTEGER, b TEXT);\
                     WITH RECURSIVE c(x) AS (SELECT 1 UNION ALL SELECT x+1 FROM c WHERE x<600) \
                     INSERT INTO t(id,a,b) SELECT x, x%29, 'v'||x FROM c;\
                     CREATE INDEX ta ON t(a);\
                     DELETE FROM t WHERE id % 3 = 0;",
                )
                .expect("stock build");
            let ps: i64 = stock.query_row("PRAGMA page_size;", [], |r| r.get(0)).unwrap();
            usize::try_from(ps).unwrap()
        };

        let mut bytes = std::fs::read(&db).unwrap();
        let header_page_count = u32::from_be_bytes([bytes[28], bytes[29], bytes[30], bytes[31]]);
        let file_pages_before = bytes.len() / page_size;
        bytes.extend(std::iter::repeat(0u8).take(page_size)); // one trailing slack page
        std::fs::write(&db, &bytes).unwrap();
        let file_pages_after = bytes.len() / page_size;

        let stock = stock_integrity(&db);
        assert_eq!(stock, "ok", "stock must ignore trailing slack, got {stock:?}");

        // Schema-only open bypasses the migration auto-repair.
        let conn = Connection::open_schema_only(db.to_string_lossy().into_owned())
            .await
            .expect("schema-only open");
        let frank = fsqlite_integrity(&conn).await;
        conn.close().await.ok();

        eprintln!(
            "[raw_slack] header_page_count={header_page_count} file_pages_before={file_pages_before} \
             file_pages_after={file_pages_after} page_size={page_size} stock={stock:?} fsqlite={frank:?}"
        );

        // Post-fix invariant: stock=ok on stock-legal trailing slack, and
        // fsqlite must MATCH (the orphan scan is now bounded by the header
        // page_count, so it never looks at the slack page stock ignores).
        assert_eq!(
            stock, "ok",
            "precondition: stock must accept trailing slack, got {stock:?}"
        );
        assert!(
            frank.len() == 1 && frank[0].eq_ignore_ascii_case("ok"),
            "bd-2whq5 REGRESSION: stock=ok on trailing slack but fsqlite \
             integrity_check={frank:?} (expected [\"ok\"]). The orphan scan must be \
             bounded by the header page_count so stock-legal slack is not flagged \
             'never used'."
        );
    });
}

/// Churn a table with a UNIQUE (autoindex) + two secondary indexes, then VACUUM
/// INTO a target and compare on a fresh reopen.
#[test]
fn churn_vacuum_into_indexed_table() {
    asupersync::test_utils::run_test(|| async {
        let dir = tmp();
        let src = dir.path().join("src.db");
        let tgt = dir.path().join("tgt.db");

        let conn = Connection::open(src.to_string_lossy().as_ref()).await.unwrap();
        conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, u TEXT UNIQUE, a INTEGER, b TEXT);")
            .await
            .unwrap();
        conn.execute("CREATE INDEX ta ON t(a);").await.unwrap();
        conn.execute("CREATE INDEX tb ON t(b);").await.unwrap();
        for i in 0..3000i64 {
            conn.execute(&format!(
                "INSERT INTO t(id,u,a,b) VALUES ({i}, 'u{i}', {}, 'b{}');",
                i % 500,
                i % 700
            ))
            .await
            .unwrap();
        }
        // Delete a large, scattered chunk to build a freelist and index churn.
        conn.execute("DELETE FROM t WHERE id % 3 = 0;").await.unwrap();
        conn.execute("DELETE FROM t WHERE id % 5 = 1;").await.unwrap();
        conn.execute_with_params(
            "VACUUM INTO ?1;",
            &[SqliteValue::Text(tgt.to_string_lossy().into_owned().into())],
        )
        .await
        .unwrap();
        conn.close().await.unwrap();

        assert_oracle_invariant(&tgt, "churn_vacuum_into_indexed_table").await;
    });
}

/// Rows with large blobs force overflow chains; churn + VACUUM INTO.
#[test]
fn churn_vacuum_into_overflow_rows() {
    asupersync::test_utils::run_test(|| async {
        let dir = tmp();
        let src = dir.path().join("src.db");
        let tgt = dir.path().join("tgt.db");

        let conn = Connection::open(src.to_string_lossy().as_ref()).await.unwrap();
        conn.execute("CREATE TABLE big(id INTEGER PRIMARY KEY, blob TEXT, k INTEGER);")
            .await
            .unwrap();
        conn.execute("CREATE INDEX bigk ON big(k);").await.unwrap();
        for i in 0..800i64 {
            let payload = "x".repeat(2500); // > typical usable page -> overflow
            conn.execute(&format!(
                "INSERT INTO big(id,blob,k) VALUES ({i}, '{payload}', {});",
                i % 100
            ))
            .await
            .unwrap();
        }
        conn.execute("DELETE FROM big WHERE id % 2 = 0;").await.unwrap();
        conn.execute_with_params(
            "VACUUM INTO ?1;",
            &[SqliteValue::Text(tgt.to_string_lossy().into_owned().into())],
        )
        .await
        .unwrap();
        conn.close().await.unwrap();

        assert_oracle_invariant(&tgt, "churn_vacuum_into_overflow_rows").await;
    });
}

/// WITHOUT ROWID churn + VACUUM INTO (index-btree table storage).
#[test]
fn churn_vacuum_into_without_rowid() {
    asupersync::test_utils::run_test(|| async {
        let dir = tmp();
        let src = dir.path().join("src.db");
        let tgt = dir.path().join("tgt.db");

        let conn = Connection::open(src.to_string_lossy().as_ref()).await.unwrap();
        conn.execute("CREATE TABLE w(k TEXT PRIMARY KEY, v TEXT) WITHOUT ROWID;")
            .await
            .unwrap();
        conn.execute("CREATE INDEX wv ON w(v);").await.unwrap();
        for i in 0..2500i64 {
            conn.execute(&format!(
                "INSERT INTO w(k,v) VALUES ('k{:06}', 'v{}');",
                i,
                i % 300
            ))
            .await
            .unwrap();
        }
        conn.execute("DELETE FROM w WHERE k LIKE 'k0000%';").await.unwrap();
        conn.execute("DELETE FROM w WHERE k LIKE 'k001%';").await.unwrap();
        conn.execute_with_params(
            "VACUUM INTO ?1;",
            &[SqliteValue::Text(tgt.to_string_lossy().into_owned().into())],
        )
        .await
        .unwrap();
        conn.close().await.unwrap();

        assert_oracle_invariant(&tgt, "churn_vacuum_into_without_rowid").await;
    });
}

/// Runtime-churn path (no VACUUM): build a freelist on the MAIN db, close,
/// reopen, compare. This is the bd-84rh4 / GH#346 territory — a residual FP or
/// leak would surface as stock=ok / fsqlite="never used".
#[test]
fn churn_main_db_reopen_no_vacuum() {
    asupersync::test_utils::run_test(|| async {
        let dir = tmp();
        let db = dir.path().join("main.db");

        let conn = Connection::open(db.to_string_lossy().as_ref()).await.unwrap();
        conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, u TEXT UNIQUE, a INTEGER);")
            .await
            .unwrap();
        conn.execute("CREATE INDEX ta ON t(a);").await.unwrap();
        conn.execute("CREATE TABLE dropme(id INTEGER PRIMARY KEY, big TEXT);")
            .await
            .unwrap();
        for i in 0..2000i64 {
            conn.execute(&format!("INSERT INTO t(id,u,a) VALUES ({i},'u{i}',{});", i % 400))
                .await
                .unwrap();
            conn.execute(&format!(
                "INSERT INTO dropme(id,big) VALUES ({i}, '{}');",
                "y".repeat(200)
            ))
            .await
            .unwrap();
        }
        // Free many pages into the freelist.
        conn.execute("DROP TABLE dropme;").await.unwrap();
        conn.execute("DELETE FROM t WHERE id % 4 = 0;").await.unwrap();
        conn.close().await.unwrap();

        assert_oracle_invariant(&db, "churn_main_db_reopen_no_vacuum").await;
    });
}

/// Writer-INDEPENDENT probe (the bead's real shape: a db written elsewhere,
/// then opened by fsqlite as a "promoted family copy"). Bundled stock sqlite
/// builds canonical images with exotic index kinds; fsqlite must reach every
/// root stock's on-disk sqlite_master carries. A dropped/missing root in
/// fsqlite's ownership walk (`self.schema` cache) surfaces as a false
/// "page N is never used" while stock says ok. This isolates loader/checker
/// coverage from fsqlite's own reconstruct writer entirely.
#[test]
fn stock_built_exotic_indexes_open_in_fsqlite() {
    asupersync::test_utils::run_test(|| async {
        // (label, DDL+DML script executed by bundled stock sqlite)
        let cases: &[(&str, &[&str])] = &[
            (
                "partial_index",
                &[
                    "CREATE TABLE p(id INTEGER PRIMARY KEY, a INTEGER, b TEXT);",
                    "WITH RECURSIVE c(x) AS (SELECT 1 UNION ALL SELECT x+1 FROM c WHERE x<1500) \
                     INSERT INTO p(id,a,b) SELECT x, x%50, 'v'||x FROM c;",
                    "CREATE INDEX pa ON p(a) WHERE a > 10;",
                    "DELETE FROM p WHERE id % 3 = 0;",
                ],
            ),
            (
                "expression_index",
                &[
                    "CREATE TABLE e(id INTEGER PRIMARY KEY, a INTEGER, b TEXT);",
                    "WITH RECURSIVE c(x) AS (SELECT 1 UNION ALL SELECT x+1 FROM c WHERE x<1500) \
                     INSERT INTO e(id,a,b) SELECT x, x%50, 'v'||x FROM c;",
                    "CREATE INDEX eabs ON e(abs(a) + length(b));",
                    "DELETE FROM e WHERE id % 4 = 1;",
                ],
            ),
            (
                "desc_collate_index",
                &[
                    "CREATE TABLE d(id INTEGER PRIMARY KEY, a TEXT, b INTEGER);",
                    "WITH RECURSIVE c(x) AS (SELECT 1 UNION ALL SELECT x+1 FROM c WHERE x<1500) \
                     INSERT INTO d(id,a,b) SELECT x, 'T'||x, x%77 FROM c;",
                    "CREATE INDEX da ON d(a COLLATE NOCASE DESC, b ASC);",
                    "DELETE FROM d WHERE id % 5 = 2;",
                ],
            ),
            (
                "without_rowid_secondary",
                &[
                    "CREATE TABLE wr(k TEXT PRIMARY KEY, v INTEGER, w TEXT) WITHOUT ROWID;",
                    "WITH RECURSIVE c(x) AS (SELECT 1 UNION ALL SELECT x+1 FROM c WHERE x<1500) \
                     INSERT INTO wr(k,v,w) SELECT 'k'||printf('%06d',x), x%40, 'w'||x FROM c;",
                    "CREATE INDEX wrv ON wr(v);",
                    "CREATE UNIQUE INDEX wrw ON wr(w);",
                    "DELETE FROM wr WHERE v % 2 = 0;",
                ],
            ),
            (
                "composite_unique_autoindex",
                &[
                    "CREATE TABLE cu(id INTEGER PRIMARY KEY, a INTEGER, b INTEGER, UNIQUE(a,b));",
                    "WITH RECURSIVE c(x) AS (SELECT 1 UNION ALL SELECT x+1 FROM c WHERE x<1500) \
                     INSERT INTO cu(id,a,b) SELECT x, x, x*2 FROM c;",
                    "DELETE FROM cu WHERE id % 3 = 0;",
                ],
            ),
        ];

        for (label, script) in cases {
            let dir = tmp();
            let db = dir.path().join(format!("{label}.db"));
            {
                let stock = rusqlite::Connection::open(&db).expect("stock open");
                // Ensure a WAL-free, self-contained image, then run a checkpoint
                // so everything is in the main file for fsqlite's reader.
                stock.pragma_update(None, "journal_mode", "DELETE").ok();
                for sql in *script {
                    stock.execute_batch(sql).expect("stock ddl/dml");
                }
                let chk: String = stock
                    .query_row("PRAGMA integrity_check;", [], |r| r.get(0))
                    .expect("stock integrity");
                assert_eq!(chk, "ok", "[{label}] stock built a non-ok image: {chk}");
            }
            assert_oracle_invariant(&db, label).await;
        }
    });
}

/// DECISIVE writer-independent probe for the ONLY mechanism that lets stock say
/// `ok` while fsqlite says "page N is never used": trailing physical pages
/// beyond the header `page_count`. Stock scans only `1..=page_count` (header
/// field 28) and ignores physical slack past it; if fsqlite derives its
/// `total_pages` from the FILE SIZE (or otherwise adopts the slack), it scans a
/// page stock never looks at and reports it "never used".
///
/// Build a canonical image with bundled stock sqlite, then append N zero-filled
/// (and one garbage) trailing page(s) WITHOUT touching the header `page_count`.
/// Stock must stay `ok`; fsqlite must match.
#[test]
fn trailing_slack_pages_beyond_page_count_oracle() {
    asupersync::test_utils::run_test(|| async {
        for (label, extra_pages, garbage) in
            [("one_zero_page", 1usize, false), ("three_zero_pages", 3, false), ("one_garbage_page", 1, true)]
        {
            let dir = tmp();
            let db = dir.path().join(format!("slack_{label}.db"));
            let page_size: usize = {
                let stock = rusqlite::Connection::open(&db).expect("stock open");
                stock.pragma_update(None, "journal_mode", "DELETE").ok();
                stock
                    .execute_batch(
                        "CREATE TABLE t(id INTEGER PRIMARY KEY, a INTEGER, b TEXT);\
                         WITH RECURSIVE c(x) AS (SELECT 1 UNION ALL SELECT x+1 FROM c WHERE x<800) \
                         INSERT INTO t(id,a,b) SELECT x, x%37, 'v'||x FROM c;\
                         CREATE INDEX ta ON t(a);\
                         DELETE FROM t WHERE id % 3 = 0;",
                    )
                    .expect("stock build");
                let ps: i64 = stock
                    .query_row("PRAGMA page_size;", [], |r| r.get(0))
                    .expect("page_size");
                let chk: String = stock
                    .query_row("PRAGMA integrity_check;", [], |r| r.get(0))
                    .expect("stock integrity");
                assert_eq!(chk, "ok", "[{label}] stock pre-append not ok: {chk}");
                usize::try_from(ps).unwrap()
            };

            // Append trailing physical pages beyond the header page_count.
            let mut bytes = std::fs::read(&db).expect("read image");
            let before_pages = bytes.len() / page_size;
            let header_page_count =
                u32::from_be_bytes([bytes[28], bytes[29], bytes[30], bytes[31]]);
            for p in 0..extra_pages {
                let fill = if garbage { 0xABu8.wrapping_add(p as u8) } else { 0u8 };
                bytes.extend(std::iter::repeat(fill).take(page_size));
            }
            std::fs::write(&db, &bytes).expect("write image");
            eprintln!(
                "[slack_{label}] header_page_count={header_page_count} before_file_pages={before_pages} after_file_pages={} page_size={page_size}",
                bytes.len() / page_size
            );

            assert_oracle_invariant(&db, &format!("trailing_slack_{label}")).await;
        }
    });
}

/// Sanity anchor: a GENUINE in-range freelist hole (freed pages recorded free
/// nowhere, still <= page_count) is reported by BOTH engines. This proves a
/// plain freelist hole is NOT the bd-2whq5 divergence (it is not a case where
/// stock says ok). Confirms the oracle gate is meaningful.
#[test]
fn genuine_freelist_hole_flagged_by_both_engines() {
    asupersync::test_utils::run_test(|| async {
        let dir = tmp();
        let db = dir.path().join("hole.db");
        {
            let conn = Connection::open(db.to_string_lossy().as_ref()).await.unwrap();
            conn.execute("PRAGMA journal_mode=DELETE;").await.unwrap();
            conn.execute("CREATE TABLE keep(x INTEGER PRIMARY KEY, v TEXT);").await.unwrap();
            conn.execute("CREATE TABLE dropme(x INTEGER PRIMARY KEY, v TEXT);").await.unwrap();
            for i in 0..64i64 {
                conn.execute(&format!("INSERT INTO keep VALUES ({i}, 'k{i}');")).await.unwrap();
                conn.execute(&format!("INSERT INTO dropme VALUES ({i}, 'd{i}');")).await.unwrap();
            }
            conn.execute("DROP TABLE dropme;").await.unwrap();
            conn.close().await.unwrap();
        }
        let mut bytes = std::fs::read(&db).unwrap();
        let freelist_count = u32::from_be_bytes([bytes[36], bytes[37], bytes[38], bytes[39]]);
        assert!(freelist_count > 0, "DROP must durably free pages");
        for b in &mut bytes[32..40] {
            *b = 0; // erase freelist header -> in-range pages recorded free nowhere
        }
        std::fs::write(&db, &bytes).unwrap();

        let stock = stock_integrity(&db);
        let conn = Connection::open(db.to_string_lossy().as_ref()).await.unwrap();
        let frank = fsqlite_integrity(&conn).await;
        conn.close().await.unwrap();
        eprintln!("[genuine_hole] stock={stock:?} fsqlite={frank:?}");
        assert_ne!(stock, "ok", "stock must ALSO flag a genuine in-range hole");
        assert!(
            frank.iter().any(|l| l.to_ascii_lowercase().contains("never used")),
            "fsqlite must flag the genuine hole, got {frank:?}"
        );
    });
}

/// Many UNIQUE columns -> many autoindexes reconstructed; VACUUM INTO.
#[test]
fn churn_vacuum_into_many_autoindexes() {
    asupersync::test_utils::run_test(|| async {
        let dir = tmp();
        let src = dir.path().join("src.db");
        let tgt = dir.path().join("tgt.db");

        let conn = Connection::open(src.to_string_lossy().as_ref()).await.unwrap();
        conn.execute(
            "CREATE TABLE m(id INTEGER PRIMARY KEY, c1 TEXT UNIQUE, c2 TEXT UNIQUE, c3 INTEGER UNIQUE);",
        )
        .await
        .unwrap();
        for i in 0..2200i64 {
            conn.execute(&format!(
                "INSERT INTO m(id,c1,c2,c3) VALUES ({i},'a{i}','b{i}',{i});"
            ))
            .await
            .unwrap();
        }
        conn.execute("DELETE FROM m WHERE id % 3 = 2;").await.unwrap();
        conn.execute_with_params(
            "VACUUM INTO ?1;",
            &[SqliteValue::Text(tgt.to_string_lossy().into_owned().into())],
        )
        .await
        .unwrap();
        conn.close().await.unwrap();

        assert_oracle_invariant(&tgt, "churn_vacuum_into_many_autoindexes").await;
    });
}
