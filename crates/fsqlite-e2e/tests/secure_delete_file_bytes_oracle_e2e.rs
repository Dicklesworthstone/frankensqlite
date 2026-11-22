//! bd-i2u1c keeper: `PRAGMA secure_delete` decides, as stock SQLite decides,
//! whether removed content survives in the bytes a database leaves on disk.
//!
//! Each case runs one script on a fresh file database through FrankenSQLite and
//! through rusqlite's bundled SQLite (`secure_delete` compiled at upstream's
//! default, OFF), checkpoints a WAL database with TRUNCATE, closes, and then
//! searches every file the engine left in the case's directory for a marker the
//! script wrote and then removed. The engines must agree on every case.
//!
//! Stock's answers were also pinned live against the 3.46.1 and 3.54.1 shells
//! (bulk-write lane, 2026-11-20 and 21), identical in WAL and rollback mode, and
//! are asserted here so an oracle change cannot move the expectation silently:
//! - OFF: a deleted cell, a freed overflow chain and a dropped table's rows stay.
//! - ON: nothing stays.
//! - FAST: cells and dropped rows are erased; a freed overflow chain (freelist
//!   leaf pages, which FAST never writes) stays.
//! - An UPDATE rewriting the marker in place, and VACUUM, leave nothing.
//!
//! One case differs from stock by design, and is pinned as such: OFF on
//! `cell`, a deleted cell with other cells below it on the leaf. FrankenSQLite's
//! leaf delete repacks the page (`remove_cell_from_leaf` keeps no freeblock
//! list), so the cells below move over the deleted bytes; stock leaves them in a
//! freeblock. OFF promises nothing either way. `cell_top` deletes the lowest
//! cell instead, which no repacking covers, so OFF keeps it in both engines.
//!
//! The negatives a naive implementation fails: OFF keeps the marker on
//! `cell_top`, `overflow` and `drop` (an implementation that always zeroes
//! fails), ON erases it (one that never zeroes fails), FAST keeps the overflow
//! chain (one that treats FAST as ON fails). The connection cases pin the
//! default and a change mid-connection: between statements, inside an open
//! transaction, and back to OFF, each on a lowest cell, so only the setting
//! decides whether it stays.

use std::fmt::Write as _;
use std::path::Path;

use fsqlite::Connection;

const JOURNAL_MODES: [&str; 2] = ["WAL", "DELETE"];
const SETTINGS: [&str; 3] = ["0", "1", "FAST"];
const SHAPES: [&str; 6] = ["cell", "cell_top", "overflow", "update", "drop", "vacuum"];

fn filler_rows() -> Vec<String> {
    (2..30)
        .map(|i| format!("INSERT INTO t VALUES({i}, '{}{i}');", "x".repeat(60)))
        .collect()
}

/// The statements that write `marker` and then remove it.
fn shape_statements(shape: &str, marker: &str) -> Vec<String> {
    let mut sql = Vec::new();
    match shape {
        "cell" | "update" | "vacuum" => {
            sql.push("CREATE TABLE t(id INTEGER PRIMARY KEY, v);".to_owned());
            sql.push(format!("INSERT INTO t VALUES(1, '{marker}');"));
            sql.extend(filler_rows());
            match shape {
                "cell" => sql.push("DELETE FROM t WHERE id = 1;".to_owned()),
                "update" => sql.push(format!(
                    "UPDATE t SET v = '{}' WHERE id = 1;",
                    "y".repeat(marker.len())
                )),
                _ => {
                    sql.push("DELETE FROM t WHERE id = 1;".to_owned());
                    sql.push("VACUUM;".to_owned());
                }
            }
        }
        "cell_top" => {
            // Written after the filler, so its cell is the lowest on the leaf.
            sql.push("CREATE TABLE t(id INTEGER PRIMARY KEY, v);".to_owned());
            sql.extend(filler_rows());
            sql.push(format!("INSERT INTO t VALUES(100, '{marker}');"));
            sql.push("DELETE FROM t WHERE id = 100;".to_owned());
        }
        "overflow" => {
            let mut blob = String::with_capacity(2 * 12_288);
            for byte in marker.as_bytes().iter().cycle().take(12_288) {
                write!(blob, "{byte:02x}").expect("hex into a String");
            }
            sql.push("CREATE TABLE t(id INTEGER PRIMARY KEY, v);".to_owned());
            sql.push(format!("INSERT INTO t VALUES(1, x'{blob}');"));
            sql.extend(filler_rows());
            sql.push("DELETE FROM t WHERE id = 1;".to_owned());
        }
        "drop" => {
            sql.push("CREATE TABLE keep(x);".to_owned());
            sql.push("INSERT INTO keep VALUES(1);".to_owned());
            sql.push("CREATE TABLE t(id INTEGER PRIMARY KEY, v);".to_owned());
            sql.extend((1..40).map(|i| format!("INSERT INTO t VALUES({i}, '{marker}{i}');")));
            sql.push("DROP TABLE t;".to_owned());
        }
        other => panic!("unknown shape {other}"),
    }
    sql
}

/// Stock's pinned answer: does the marker survive?
fn stock_keeps_marker(setting: &str, shape: &str) -> bool {
    match (setting, shape) {
        ("0", "cell" | "cell_top" | "overflow" | "drop") | ("FAST", "overflow") => true,
        (_, "cell" | "cell_top" | "overflow" | "update" | "drop" | "vacuum") => false,
        other => panic!("unknown case {other:?}"),
    }
}

/// FrankenSQLite's answer where it differs from stock by design (see the
/// module docs): OFF on `cell`, whose bytes the leaf repack covers.
fn fsqlite_keeps_marker(setting: &str, shape: &str) -> bool {
    if (setting, shape) == ("0", "cell") {
        return false;
    }
    stock_keeps_marker(setting, shape)
}

/// Every regular file in `dir` holding `needle`, by name.
fn files_holding(dir: &Path, needle: &[u8]) -> Vec<String> {
    let mut found = Vec::new();
    for entry in std::fs::read_dir(dir).expect("read case dir") {
        let entry = entry.expect("dir entry");
        let path = entry.path();
        if !path.is_file() {
            continue;
        }
        let bytes = std::fs::read(&path).expect("read case file");
        if bytes.windows(needle.len()).any(|window| window == needle) {
            found.push(entry.file_name().to_string_lossy().into_owned());
        }
    }
    found.sort();
    found
}

async fn run_frank(db: &Path, script: &[String]) {
    let conn = Connection::open(db.to_string_lossy().into_owned())
        .await
        .expect("open fsqlite");
    for sql in script {
        conn.execute_batch(sql)
            .await
            .unwrap_or_else(|error| panic!("fsqlite: {}: {error}", &sql[..sql.len().min(80)]));
    }
    conn.close().await.expect("close fsqlite");
}

fn run_stock(db: &Path, script: &[String]) {
    let conn = rusqlite::Connection::open(db).expect("open rusqlite");
    for sql in script {
        conn.execute_batch(sql)
            .unwrap_or_else(|error| panic!("rusqlite: {}: {error}", &sql[..sql.len().min(80)]));
    }
    conn.close().expect("close rusqlite");
}

/// Run `script` on both engines in fresh directories under `root`; return,
/// per marker, the files each engine left holding it.
async fn both_engines(
    root: &Path,
    case: &str,
    script: &[String],
    markers: &[&str],
) -> Vec<(Vec<String>, Vec<String>)> {
    let frank_dir = root.join(format!("{case}-fsqlite"));
    let stock_dir = root.join(format!("{case}-stock"));
    std::fs::create_dir(&frank_dir).expect("fsqlite case dir");
    std::fs::create_dir(&stock_dir).expect("stock case dir");
    run_frank(&frank_dir.join("case.db"), script).await;
    run_stock(&stock_dir.join("case.db"), script);
    markers
        .iter()
        .map(|marker| {
            (
                files_holding(&frank_dir, marker.as_bytes()),
                files_holding(&stock_dir, marker.as_bytes()),
            )
        })
        .collect()
}

fn checkpoint_if_wal(journal_mode: &str, script: &mut Vec<String>) {
    if journal_mode == "WAL" {
        script.push("PRAGMA wal_checkpoint(TRUNCATE);".to_owned());
    }
}

#[test]
fn secure_delete_matches_stock_file_bytes_for_every_mode_and_shape() {
    asupersync::test_utils::run_test(|| async {
        let root = tempfile::tempdir().expect("tempdir");
        let mut mismatches = Vec::new();
        for journal_mode in JOURNAL_MODES {
            for setting in SETTINGS {
                for shape in SHAPES {
                    let case = format!("{journal_mode}-{setting}-{shape}");
                    let marker = format!("SECDEL-MARKER-{case}-ZQX");
                    let mut script = vec![
                        format!("PRAGMA journal_mode={journal_mode};"),
                        format!("PRAGMA secure_delete={setting};"),
                    ];
                    script.extend(shape_statements(shape, &marker));
                    checkpoint_if_wal(journal_mode, &mut script);
                    let found = both_engines(root.path(), &case, &script, &[&marker]).await;
                    let (frank, stock) = &found[0];
                    let expected = stock_keeps_marker(setting, shape);
                    if stock.is_empty() == expected {
                        mismatches.push(format!(
                            "{case}: the oracle moved: stock files holding the marker {stock:?}, \
                             pinned {}",
                            if expected { "kept" } else { "erased" }
                        ));
                    }
                    if frank.is_empty() == fsqlite_keeps_marker(setting, shape) {
                        mismatches.push(format!(
                            "{case}: fsqlite files holding the marker {frank:?}, stock {stock:?}"
                        ));
                    }
                }
            }
        }
        assert!(
            mismatches.is_empty(),
            "secure_delete file bytes differ from stock in {} of {} cases:\n{}",
            mismatches.len(),
            JOURNAL_MODES.len() * SETTINGS.len() * SHAPES.len(),
            mismatches.join("\n")
        );
    });
}

#[test]
fn secure_delete_applies_to_existing_and_later_attachments_like_stock() {
    asupersync::test_utils::run_test(|| async {
        for (assignment, initial, expected) in [
            ("secure_delete=ON", 0, [1, 1, 1, 1]),
            ("main.secure_delete=ON", 0, [0, 0, 1, 1]),
            ("early.secure_delete=ON", 0, [1, 0, 0, 0]),
            ("secure_delete=FAST", 0, [2, 2, 2, 2]),
            ("secure_delete=OFF", 1, [0, 0, 0, 0]),
        ] {
            for journal in JOURNAL_MODES {
                for stock in [true, false] {
                    let dir = tempfile::tempdir().unwrap();
                    let open_attachment = |name: &str| {
                        let db = dir.path().join(format!("{name}.db"));
                        let path = db.to_str().unwrap().replace('\'', "''");
                        format!("ATTACH '{path}' AS {name}; PRAGMA {name}.journal_mode={journal};")
                    };
                    let early = open_attachment("early");
                    let sibling = open_attachment("sibling");
                    let late = open_attachment("late");
                    let checkpoint = if journal == "WAL" {
                        "PRAGMA early.wal_checkpoint(TRUNCATE); PRAGMA late.wal_checkpoint(TRUNCATE);"
                    } else {
                        ""
                    };
                    let script = format!(
                        "PRAGMA secure_delete={initial}; {early} {sibling}
                         CREATE TABLE early.t(id INTEGER PRIMARY KEY, v);
                         INSERT INTO early.t VALUES(1,'keep');
                         BEGIN;
                         INSERT INTO early.t VALUES(2,'SECDEL-attached-early-marker');
                         PRAGMA {assignment};
                         DELETE FROM early.t WHERE id=2;
                         COMMIT;
                         {late}
                         CREATE TABLE late.t(id INTEGER PRIMARY KEY, v);
                         INSERT INTO late.t VALUES(1,'keep');
                         INSERT INTO late.t VALUES(2,'SECDEL-attached-late-marker');
                         DELETE FROM late.t WHERE id=2;
                         {checkpoint}"
                    );
                    let queries = ["early", "sibling", "late", "main"]
                        .map(|name| format!("PRAGMA {name}.secure_delete"));
                    let main = dir.path().join("main.db");
                    let modes = if stock {
                        let conn = rusqlite::Connection::open(main).unwrap();
                        conn.execute_batch(&script).unwrap();
                        queries.map(|sql| {
                            conn.query_row(&sql, [], |row| row.get::<_, i64>(0))
                                .unwrap()
                        })
                    } else {
                        let conn = Connection::open(main.to_str().unwrap()).await.unwrap();
                        conn.execute_batch(&script).await.unwrap();
                        let mut modes = [0; 4];
                        for (mode, sql) in modes.iter_mut().zip(queries) {
                            let rows = conn.prepare(&sql).await.unwrap().query().await.unwrap();
                            let [fsqlite::SqliteValue::Integer(value)] = rows[0].values() else {
                                panic!("unexpected {sql} result {rows:?}");
                            };
                            *mode = *value;
                        }
                        conn.close().await.unwrap();
                        modes
                    };
                    assert_eq!(modes, expected, "{assignment} {journal} stock={stock}");
                    for (schema, mode) in [("early", modes[0]), ("late", modes[2])] {
                        let marker = format!("SECDEL-attached-{schema}-marker");
                        let files = files_holding(dir.path(), marker.as_bytes());
                        if mode == 0 {
                            assert!(
                                files.contains(&format!("{schema}.db")),
                                "{assignment} {journal} {files:?}"
                            );
                        } else {
                            assert!(files.is_empty(), "{assignment} {journal} {files:?}");
                        }
                    }
                }
            }
        }
    });
}

#[test]
fn secure_delete_default_and_mid_connection_changes_match_stock() {
    asupersync::test_utils::run_test(|| async {
        let root = tempfile::tempdir().expect("tempdir");
        // The marker rows are written after the filler, so each sits at the
        // bottom of the leaf, where no repack covers its bytes: only the
        // setting decides whether a deleted one stays (see the module docs).
        let setup = |marker: &str, extra: Option<&str>| {
            let mut sql = vec![
                "PRAGMA journal_mode=WAL;".to_owned(),
                "CREATE TABLE t(id INTEGER PRIMARY KEY, v);".to_owned(),
            ];
            sql.extend(filler_rows());
            // `extra` first, so `marker` (row 1) is the lowest cell and its
            // deletion is the first.
            if let Some(extra) = extra {
                sql.push(format!("INSERT INTO t VALUES(100, '{extra}');"));
            }
            sql.push(format!("INSERT INTO t VALUES(1, '{marker}');"));
            sql
        };
        let mut mismatches = Vec::new();
        let mut check = |case: &str, found: &[(Vec<String>, Vec<String>)], pinned: &[bool]| {
            for (index, ((frank, stock), keeps)) in found.iter().zip(pinned).enumerate() {
                if stock.is_empty() == *keeps {
                    mismatches.push(format!(
                        "{case} marker {index}: the oracle moved: {stock:?}"
                    ));
                }
                if frank.is_empty() != stock.is_empty() {
                    mismatches.push(format!(
                        "{case} marker {index}: fsqlite {frank:?}, stock {stock:?}"
                    ));
                }
            }
        };

        // No PRAGMA: upstream's default is OFF, so the deleted cell stays.
        let marker = "SECDEL-MARKER-default-ZQX";
        let mut script = setup(marker, None);
        script.push("DELETE FROM t WHERE id = 1;".to_owned());
        checkpoint_if_wal("WAL", &mut script);
        let found = both_engines(root.path(), "default", &script, &[marker]).await;
        check("default", &found, &[true]);

        // Turned ON between autocommit statements, after the rows were written.
        let marker = "SECDEL-MARKER-between-ZQX";
        let mut script = setup(marker, None);
        script.push("PRAGMA secure_delete=1;".to_owned());
        script.push("DELETE FROM t WHERE id = 1;".to_owned());
        checkpoint_if_wal("WAL", &mut script);
        let found = both_engines(root.path(), "between", &script, &[marker]).await;
        check("between", &found, &[false]);

        // Turned ON inside an open transaction: it governs that transaction's
        // next deletion, as stock's b-tree flag does.
        let marker = "SECDEL-MARKER-in-txn-ZQX";
        let mut script = setup(marker, None);
        script.push("BEGIN;".to_owned());
        script.push("UPDATE t SET v = v WHERE id = 2;".to_owned());
        script.push("PRAGMA secure_delete=1;".to_owned());
        script.push("DELETE FROM t WHERE id = 1;".to_owned());
        script.push("COMMIT;".to_owned());
        checkpoint_if_wal("WAL", &mut script);
        let found = both_engines(root.path(), "in-txn", &script, &[marker]).await;
        check("in-txn", &found, &[false]);

        // ON for one deletion, back to OFF for the next: the first marker is
        // erased and the second stays.
        let first = "SECDEL-MARKER-toggle-on-ZQX";
        let second = "SECDEL-MARKER-toggle-off-ZQX";
        let mut script = setup(first, Some(second));
        script.push("PRAGMA secure_delete=1;".to_owned());
        script.push("DELETE FROM t WHERE id = 1;".to_owned());
        script.push("PRAGMA secure_delete=0;".to_owned());
        script.push("DELETE FROM t WHERE id = 100;".to_owned());
        checkpoint_if_wal("WAL", &mut script);
        let found = both_engines(root.path(), "toggle", &script, &[first, second]).await;
        check("toggle", &found, &[false, true]);

        assert!(
            mismatches.is_empty(),
            "secure_delete connection cases differ from stock:\n{}",
            mismatches.join("\n")
        );
    });
}
