#![recursion_limit = "512"]

//! GH #186 (bd-gh-autoincrement-sequence) HEAD probe: an INSERT OR IGNORE that
//! allocates an AUTOINCREMENT rowid but discards the row on a UNIQUE conflict
//! must still advance sqlite_sequence (stock sqlite3), so the next insert skips
//! the burned rowid. Differential vs rusqlite.

use fsqlite_core::connection::Connection;
use fsqlite_types::value::SqliteValue;

fn tag_f(v: &SqliteValue) -> String {
    match v {
        SqliteValue::Null => "NULL".to_owned(),
        SqliteValue::Integer(n) => n.to_string(),
        SqliteValue::Float(f) => format!("{f}"),
        SqliteValue::Text(s) => format!("'{s}'"),
        SqliteValue::Blob(b) => format!(
            "X'{}'",
            b.iter().map(|x| format!("{x:02X}")).collect::<String>()
        ),
    }
}
fn tag_r(v: &rusqlite::types::Value) -> String {
    match v {
        rusqlite::types::Value::Null => "NULL".to_owned(),
        rusqlite::types::Value::Integer(n) => n.to_string(),
        rusqlite::types::Value::Real(f) => format!("{f}"),
        rusqlite::types::Value::Text(s) => format!("'{s}'"),
        rusqlite::types::Value::Blob(b) => format!(
            "X'{}'",
            b.iter().map(|x| format!("{x:02X}")).collect::<String>()
        ),
    }
}

async fn assert_agree(fconn: &Connection, rconn: &rusqlite::Connection, sql: &str) {
    let fr: Vec<Vec<String>> = fconn
        .query(sql)
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e:?}"))
        .iter()
        .map(|r| r.values().iter().map(tag_f).collect())
        .collect();
    let mut st = rconn.prepare(sql).unwrap();
    let n = st.column_count();
    let rr: Vec<Vec<String>> = st
        .query_map([], |row| {
            Ok((0..n)
                .map(|i| tag_r(&row.get_unwrap::<_, rusqlite::types::Value>(i)))
                .collect())
        })
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(fr, rr, "autoincrement seq mismatch on `{sql}`");
}

async fn run_both(fconn: &Connection, rconn: &rusqlite::Connection, stmts: &[&str]) {
    for s in stmts {
        let _ = fconn.execute(s).await;
        let _ = rconn.execute_batch(s);
    }
}

#[test]
fn autoincrement_or_ignore_advances_seq_gh186() {
    asupersync::test_utils::run_test(|| async {
        let f = Connection::open(":memory:").await.unwrap();
        let r = rusqlite::Connection::open_in_memory().unwrap();
        run_both(
            &f,
            &r,
            &[
                "CREATE TABLE t (id INTEGER PRIMARY KEY AUTOINCREMENT, v UNIQUE)",
                "INSERT INTO t(v) VALUES ('a')",
                "INSERT OR IGNORE INTO t(v) VALUES ('a')", // conflict: row ignored, rowid 2 burned
            ],
        )
        .await;
        // sqlite3: seq advanced to 2 after the ignored insert.
        assert_agree(&f, &r, "SELECT seq FROM sqlite_sequence WHERE name='t'").await;
        run_both(&f, &r, &["INSERT INTO t(v) VALUES ('b')"]).await;
        // Next insert gets id 3 (2 was burned), seq -> 3.
        assert_agree(&f, &r, "SELECT id, v FROM t ORDER BY id").await;
        assert_agree(&f, &r, "SELECT seq FROM sqlite_sequence WHERE name='t'").await;
    });
}

#[test]
fn autoincrement_plain_sequence_control_gh186() {
    asupersync::test_utils::run_test(|| async {
        let f = Connection::open(":memory:").await.unwrap();
        let r = rusqlite::Connection::open_in_memory().unwrap();
        // Control: no conflict — the sequence tracks the max inserted rowid.
        run_both(
            &f,
            &r,
            &[
                "CREATE TABLE t (id INTEGER PRIMARY KEY AUTOINCREMENT, v UNIQUE)",
                "INSERT INTO t(v) VALUES ('a')",
                "INSERT INTO t(v) VALUES ('b')",
                "INSERT INTO t(id, v) VALUES (10, 'c')",
            ],
        )
        .await;
        assert_agree(&f, &r, "SELECT seq FROM sqlite_sequence WHERE name='t'").await;
        run_both(&f, &r, &["INSERT INTO t(v) VALUES ('d')"]).await;
        assert_agree(&f, &r, "SELECT id, v FROM t ORDER BY id").await;
    });
}

// ── Rowid tables must not burn discarded rowids (companion to GH #186) ──
//
// Stock recomputes `max(rowid)+1` on every OP_NewRowid, so a row the same
// statement discarded (OR IGNORE on a NOT NULL, CHECK, or UNIQUE violation, on
// an ordinary or a VIRTUAL generated column) leaves no gap on a rowid table:
// after ids 1,2,3 the surviving row gets 4. An explicit rowid landed
// mid-statement moves the next implicit one past it (4, 100, 101), and a
// discarded row followed by an explicit one yields (100, 101). AUTOINCREMENT
// burns the discarded value (5) and its sequence follows the explicit rowid
// (101). Every expectation is compared live against rusqlite; the ids pinned
// below are what stock 3.53.4 returned for these exact statements. Before the
// fix, fsqlite's per-cursor allocation cache advanced past the discarded value
// and the surviving row got 5.

async fn run_both_ok(fconn: &Connection, rconn: &rusqlite::Connection, stmts: &[&str]) {
    for s in stmts {
        fconn
            .execute(s)
            .await
            .unwrap_or_else(|e| panic!("fsqlite `{s}`: {e}"));
        rconn
            .execute_batch(s)
            .unwrap_or_else(|e| panic!("stock `{s}`: {e}"));
    }
}

const PLAIN_ROWID_SEED: &str =
    "CREATE TABLE g(id INTEGER PRIMARY KEY, v INTEGER NOT NULL CHECK(v > 0), u INTEGER UNIQUE); \
     INSERT INTO g(v, u) VALUES (7, 1), (11, 2), (13, 3);";
const VIRTUAL_ROWID_SEED: &str = "CREATE TABLE g(id INTEGER PRIMARY KEY, v INTEGER DEFAULT 3, \
     n INTEGER GENERATED ALWAYS AS (NULLIF(v, 0) + 1) VIRTUAL NOT NULL CHECK(n > 0)); \
     CREATE UNIQUE INDEX g_n ON g(n); \
     INSERT INTO g(v) VALUES (7), (11), (13);";
const AUTOINC_ROWID_SEED: &str = "CREATE TABLE g(id INTEGER PRIMARY KEY AUTOINCREMENT, \
     v INTEGER NOT NULL CHECK(v > 0), u INTEGER UNIQUE); \
     INSERT INTO g(v, u) VALUES (7, 1), (11, 2), (13, 3);";

#[test]
fn rowid_table_or_ignore_does_not_burn_discarded_rowids() {
    asupersync::test_utils::run_test(|| async {
        let dir = tempfile::tempdir().unwrap();
        let cases: [(&str, &str, &str, &[i64]); 11] = [
            (
                "plain-not-null",
                PLAIN_ROWID_SEED,
                "INSERT OR IGNORE INTO g(v, u) VALUES (NULL, 4), (19, 5)",
                &[1, 2, 3, 4],
            ),
            (
                "plain-check",
                PLAIN_ROWID_SEED,
                "INSERT OR IGNORE INTO g(v, u) VALUES (-5, 4), (19, 5)",
                &[1, 2, 3, 4],
            ),
            (
                "plain-unique",
                PLAIN_ROWID_SEED,
                "INSERT OR IGNORE INTO g(v, u) VALUES (21, 1), (19, 5)",
                &[1, 2, 3, 4],
            ),
            (
                "plain-two-discards",
                PLAIN_ROWID_SEED,
                "INSERT OR IGNORE INTO g(v, u) VALUES (NULL, 4), (NULL, 6), (19, 5)",
                &[1, 2, 3, 4],
            ),
            (
                "plain-explicit-mid-statement",
                PLAIN_ROWID_SEED,
                "INSERT INTO g(id, v, u) VALUES (NULL, 21, 7), (100, 22, 8), (NULL, 23, 9)",
                &[1, 2, 3, 4, 100, 101],
            ),
            (
                "plain-discard-then-explicit",
                PLAIN_ROWID_SEED,
                "INSERT OR IGNORE INTO g(id, v, u) VALUES (NULL, NULL, 4), (100, 22, 8), (NULL, 23, 9)",
                &[1, 2, 3, 100, 101],
            ),
            (
                "virtual-not-null",
                VIRTUAL_ROWID_SEED,
                "INSERT OR IGNORE INTO g(v) VALUES (0), (19)",
                &[1, 2, 3, 4],
            ),
            (
                "virtual-check",
                VIRTUAL_ROWID_SEED,
                "INSERT OR IGNORE INTO g(v) VALUES (-5), (19)",
                &[1, 2, 3, 4],
            ),
            (
                "virtual-unique",
                VIRTUAL_ROWID_SEED,
                "INSERT OR IGNORE INTO g(v) VALUES (7), (19)",
                &[1, 2, 3, 4],
            ),
            (
                "autoinc-not-null-burns",
                AUTOINC_ROWID_SEED,
                "INSERT OR IGNORE INTO g(v, u) VALUES (NULL, 4), (19, 5)",
                &[1, 2, 3, 5],
            ),
            (
                "autoinc-explicit-mid-statement",
                AUTOINC_ROWID_SEED,
                "INSERT INTO g(id, v, u) VALUES (NULL, 21, 7), (100, 22, 8), (NULL, 23, 9)",
                &[1, 2, 3, 4, 100, 101],
            ),
        ];
        for (label, seed, statement, expected_ids) in cases {
            for file_backed in [false, true] {
                let path = dir.path().join(format!("{label}-{file_backed}.db"));
                let f = Connection::open(if file_backed {
                    path.to_str().unwrap()
                } else {
                    ":memory:"
                })
                .await
                .unwrap();
                let r = rusqlite::Connection::open_in_memory().unwrap();
                run_both_ok(&f, &r, &[seed, statement]).await;
                let context = format!("{label}, file={file_backed}");
                let read = if seed.contains("VIRTUAL") {
                    "SELECT id, v, n FROM g ORDER BY id"
                } else {
                    "SELECT id, v, u FROM g ORDER BY id"
                };
                let fr: Vec<Vec<String>> = f
                    .query(read)
                    .await
                    .unwrap_or_else(|e| panic!("{context}: {e}"))
                    .iter()
                    .map(|row| row.values().iter().map(tag_f).collect())
                    .collect();
                let mut st = r.prepare(read).unwrap();
                let n = st.column_count();
                let rr: Vec<Vec<String>> = st
                    .query_map([], |row| {
                        Ok((0..n)
                            .map(|i| tag_r(&row.get_unwrap::<_, rusqlite::types::Value>(i)))
                            .collect())
                    })
                    .unwrap()
                    .collect::<Result<Vec<_>, _>>()
                    .unwrap();
                assert_eq!(fr, rr, "{context}: rows differ from stock");
                // Pin the stock premise so a shared drift cannot pass as agreement.
                let ids: Vec<String> = rr.iter().map(|row| row[0].clone()).collect();
                let expected: Vec<String> = expected_ids.iter().map(ToString::to_string).collect();
                assert_eq!(ids, expected, "{context}: stock premise");
                if seed.contains("AUTOINCREMENT") {
                    assert_agree(&f, &r, "SELECT seq FROM sqlite_sequence WHERE name='g'").await;
                }
                f.close().await.unwrap();
            }
        }
    });
}
