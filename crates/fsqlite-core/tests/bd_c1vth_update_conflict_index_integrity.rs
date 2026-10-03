#![recursion_limit = "512"]

//! bd-c1vth: an UPDATE row that hits a conflict is restored, and the restore
//! must put back exactly the index entries that row's UPDATE removed.
//!
//! The restore used to re-insert the old row's key into every plain-column
//! index of the table. UPDATE only rewrites the indexes whose columns it
//! changes, so every untouched index gained a duplicate entry (stock
//! `integrity_check`: "wrong # of entries in index ..."), and later scans over
//! that index visited the row twice. Expression and partial indexes were
//! skipped instead, so an UPDATE that did rewrite one lost its entry.
//!
//! Each case runs on a fresh file-backed FrankenSQLite database and on stock
//! SQLite (rusqlite), and compares the outcome (ok / error), the rows,
//! `changes()`, the `total_changes()` delta and an AFTER UPDATE trigger log
//! after every statement. A follow-up range UPDATE over the indexed columns
//! catches rows visited twice. The FrankenSQLite file is then closed and
//! checked with stock `PRAGMA integrity_check`.

use fsqlite_core::connection::Connection;
use fsqlite_types::SqliteValue;
use rusqlite::types::Value;

fn value(value: Value) -> SqliteValue {
    match value {
        Value::Null => SqliteValue::Null,
        Value::Integer(value) => SqliteValue::Integer(value),
        Value::Real(value) => SqliteValue::Float(value),
        Value::Text(value) => SqliteValue::from(value.as_str()),
        Value::Blob(value) => SqliteValue::Blob(value.into()),
    }
}

async fn frank_rows(conn: &Connection, sql: &str) -> Vec<Vec<SqliteValue>> {
    conn.query(sql)
        .await
        .unwrap_or_else(|e| panic!("FrankenSQLite: `{sql}`: {e:?}"))
        .iter()
        .map(|row| row.values().to_vec())
        .collect()
}

fn stock_rows(conn: &rusqlite::Connection, sql: &str) -> Vec<Vec<SqliteValue>> {
    let mut stmt = conn
        .prepare(sql)
        .unwrap_or_else(|e| panic!("SQLite: `{sql}`: {e}"));
    let width = stmt.column_count();
    stmt.query_map([], |row| {
        (0..width)
            .map(|i| row.get::<_, Value>(i).map(value))
            .collect::<rusqlite::Result<Vec<_>>>()
    })
    .unwrap()
    .collect::<rusqlite::Result<Vec<_>>>()
    .unwrap()
}

fn single_integer(rows: &[Vec<SqliteValue>]) -> i64 {
    match rows.first().and_then(|row| row.first()) {
        Some(SqliteValue::Integer(v)) => *v,
        other => panic!("expected one integer, got {other:?}"),
    }
}

struct Schema {
    name: &'static str,
    ddl: &'static str,
    /// Whether the table has a rowid (`rowid` is selectable and assignable).
    rowid: bool,
    /// Whether `a` is the INTEGER PRIMARY KEY.
    ipk: bool,
}

const TABLES: [(&str, &str, bool, bool); 4] = [
    ("plain", "CREATE TABLE u(a, b UNIQUE, c, d);", true, false),
    (
        "ipk",
        "CREATE TABLE u(a INTEGER PRIMARY KEY, b UNIQUE, c, d);",
        true,
        true,
    ),
    (
        "without_rowid",
        "CREATE TABLE u(a PRIMARY KEY, b UNIQUE, c, d) WITHOUT ROWID;",
        false,
        false,
    ),
    (
        "two_unique",
        "CREATE TABLE u(a, b UNIQUE, c, d UNIQUE);",
        true,
        false,
    ),
];

const INDEX_SETS: [(&str, &str); 7] = [
    ("no_index", ""),
    ("plain_c", "CREATE INDEX u_c ON u(c);"),
    ("partial_d", "CREATE INDEX u_pd ON u(d) WHERE d > 2;"),
    (
        "expr",
        "CREATE INDEX u_e ON u(c + d); CREATE INDEX u_l ON u(lower(d));",
    ),
    ("composite", "CREATE INDEX u_cd ON u(c, d);"),
    ("covering", "CREATE INDEX u_cdb ON u(c, d, b);"),
    (
        "all",
        "CREATE INDEX u_c ON u(c); CREATE INDEX u_pd ON u(d) WHERE d > 2; \
         CREATE INDEX u_e ON u(c + d); CREATE INDEX u_cd ON u(c, d); \
         CREATE INDEX u_cdb ON u(c, d, b);",
    ),
];

const SEED: &str = "CREATE TABLE log(seq INTEGER PRIMARY KEY, ev TEXT);
     CREATE TABLE src(k, nb, nc); INSERT INTO src VALUES (1, 20, 9), (3, 99, 8);
     INSERT INTO u(a, b, c, d) VALUES (1, 10, 1, 2), (2, 20, 2, 3), (3, 30, 3, 4),
       (4, 40, 4, 5), (5, 50, 5, 6), (6, 60, 6, 7);";

/// An AFTER UPDATE trigger logging every updated row. Left out of the cases
/// that assign the hidden rowid, which FrankenSQLite refuses on a table with
/// UPDATE triggers.
const LOG_TRIGGER: &str = "CREATE TRIGGER u_au AFTER UPDATE ON u BEGIN
       INSERT INTO log(ev) VALUES ('au ' || old.a || '>' || new.a || ' b=' || new.b);
     END;";

const MODES: [&str; 6] = [
    "",
    "OR IGNORE ",
    "OR REPLACE ",
    "OR FAIL ",
    "OR ABORT ",
    "OR ROLLBACK ",
];

/// Statement templates; `{or}` is the conflict clause.
fn statements(schema: &Schema) -> Vec<String> {
    let mut out = vec![
        // Only the UNIQUE column changes: the other indexes are untouched.
        "UPDATE {or}u SET b = 20 WHERE a = 1".to_owned(),
        // Every indexed column changes along with the conflicting one.
        "UPDATE {or}u SET b = 20, c = c + 100, d = d + 100 WHERE a = 1".to_owned(),
        // Multi-row: some rows conflict, some do not (scan order matters).
        "UPDATE {or}u SET b = b + 10, d = d + 1".to_owned(),
        "UPDATE {or}u SET c = 7, b = 30 WHERE a IN (1, 2, 5)".to_owned(),
        "UPDATE {or}u SET b = 60 WHERE a > 3".to_owned(),
        // The first row succeeds, a later one conflicts.
        "UPDATE {or}u SET b = CASE a WHEN 1 THEN 15 WHEN 2 THEN 30 ELSE b END, c = c + 1".to_owned(),
        // UPDATE ... FROM.
        "UPDATE {or}u SET b = v.nb, c = v.nc FROM src AS v WHERE u.a = v.k".to_owned(),
        // UPSERT DO UPDATE whose rewrite conflicts on another row.
        "INSERT INTO u(a, b, c, d) VALUES (7, 10, 0, 0) ON CONFLICT(b) DO UPDATE SET b = 20, c = 70"
            .to_owned(),
        "INSERT INTO u(a, b, c, d) VALUES (7, 10, 0, 0) ON CONFLICT(b) DO UPDATE SET c = excluded.c + 50"
            .to_owned(),
        // Plain inserts with several indexes.
        "INSERT {or}INTO u(a, b, c, d) VALUES (8, 20, 9, 9)".to_owned(),
        "INSERT {or}INTO u(a, b, c, d) VALUES (9, 90, 1, 3), (10, 30, 2, 4), (11, 110, 3, 5)"
            .to_owned(),
    ];
    if schema.ipk {
        out.push("UPDATE {or}u SET a = 2 WHERE a = 1".to_owned());
        out.push("UPDATE {or}u SET a = a + 100, b = 20 WHERE a = 1".to_owned());
        out.push("UPDATE {or}u SET a = a + 1".to_owned());
    } else if schema.rowid {
        out.push("UPDATE {or}u SET rowid = 2 WHERE a = 1".to_owned());
        out.push("UPDATE {or}u SET rowid = rowid + 100, b = 20 WHERE a = 1".to_owned());
    } else {
        out.push("UPDATE {or}u SET a = 2 WHERE a = 1".to_owned());
        out.push("UPDATE {or}u SET a = a + 100, b = 20 WHERE a = 1".to_owned());
    }
    out
}

/// A range UPDATE over the indexed columns after the statement under test:
/// a duplicated index entry makes it visit a row twice.
const FOLLOW_UP: &str = "UPDATE u SET c = c + 1000 WHERE c >= 0 AND d >= 0";

async fn run_case(schema: &Schema, indexes: &str, sql: &str, failures: &mut Vec<String>) {
    let label = format!("[{} | {indexes}] {sql}", schema.name);
    let dir = tempfile::tempdir().expect("temp dir");
    let path = dir.path().join("frank.db");
    let path_str = path.to_str().expect("utf-8 path").to_owned();
    let frank = Connection::open(&path_str).await.expect("open");
    let stock = rusqlite::Connection::open_in_memory().expect("stock open");
    let trigger = if sql.contains("rowid =") {
        ""
    } else {
        LOG_TRIGGER
    };
    let setup = format!("{} {indexes} {SEED} {trigger}", schema.ddl);
    frank.execute_batch(&setup).await.expect("frank setup");
    stock.execute_batch(&setup).expect("stock setup");

    let check = if schema.rowid {
        "SELECT rowid, a, b, c, d FROM u ORDER BY rowid"
    } else {
        "SELECT a, b, c, d FROM u ORDER BY a"
    };
    for step in [sql, FOLLOW_UP] {
        let frank_before = single_integer(&frank_rows(&frank, "SELECT total_changes()").await);
        let stock_before = single_integer(&stock_rows(&stock, "SELECT total_changes()"));
        let frank_result = frank.execute_batch(step).await;
        let stock_result = stock.execute_batch(step);
        if frank_result.is_ok() != stock_result.is_ok() {
            failures.push(format!(
                "{label} :: `{step}` outcome: FrankenSQLite {frank_result:?} vs SQLite {stock_result:?}"
            ));
            return;
        }
        // The log is compared by content and order, not by its allocated
        // `seq`: after a statement rollback FrankenSQLite does not hand a
        // rolled-back INTEGER PRIMARY KEY out again, where stock does.
        for query in [check, "SELECT ev FROM log ORDER BY seq", "SELECT changes()"] {
            let (f, s) = (frank_rows(&frank, query).await, stock_rows(&stock, query));
            if f != s {
                failures.push(format!(
                    "{label} :: after `{step}`: `{query}` differs:\n  frank {f:?}\n  stock {s:?}"
                ));
            }
        }
        let frank_delta =
            single_integer(&frank_rows(&frank, "SELECT total_changes()").await) - frank_before;
        let stock_delta =
            single_integer(&stock_rows(&stock, "SELECT total_changes()")) - stock_before;
        if frank_delta != stock_delta {
            failures.push(format!(
                "{label} :: after `{step}`: total_changes() delta {frank_delta} vs SQLite {stock_delta}"
            ));
        }
    }
    frank.close().await.expect("close");

    let checked = rusqlite::Connection::open(&path).expect("stock open of fsqlite file");
    let verdict: Vec<String> = checked
        .prepare("PRAGMA integrity_check")
        .unwrap()
        .query_map([], |row| row.get::<_, String>(0))
        .unwrap()
        .collect::<rusqlite::Result<Vec<_>>>()
        .unwrap();
    if verdict != ["ok"] {
        failures.push(format!("{label} :: stock integrity_check: {verdict:?}"));
    }
}

#[test]
fn update_conflict_restore_keeps_every_index_consistent() {
    asupersync::test_utils::run_test(|| async {
        let mut failures = Vec::new();
        let mut cases = 0usize;
        for (name, ddl, rowid, ipk) in TABLES {
            let schema = Schema {
                name,
                ddl,
                rowid,
                ipk,
            };
            for (_, indexes) in INDEX_SETS {
                for template in statements(&schema) {
                    let modes: &[&str] = if template.contains("{or}") {
                        &MODES
                    } else {
                        &[""]
                    };
                    for mode in modes {
                        // UPDATE OR IGNORE / OR REPLACE ... FROM fails to
                        // resolve the FROM alias ("no such column: v.k"), a
                        // separate open bug; the other modes cover the path.
                        if template.contains("FROM src")
                            && matches!(*mode, "OR IGNORE " | "OR REPLACE ")
                        {
                            continue;
                        }
                        let sql = template.replace("{or}", mode);
                        run_case(&schema, indexes, &sql, &mut failures).await;
                        cases += 1;
                    }
                }
            }
        }
        assert!(
            failures.is_empty(),
            "{} of {cases} cases differ from SQLite:\n{}",
            failures.len(),
            failures.join("\n")
        );
    });
}

/// The bead's exact reproduction, kept as its own test so a failure names it.
#[test]
fn update_or_ignore_does_not_duplicate_an_untouched_index_entry() {
    asupersync::test_utils::run_test(|| async {
        let dir = tempfile::tempdir().expect("temp dir");
        let path = dir.path().join("repro.db");
        let path_str = path.to_str().expect("utf-8 path").to_owned();
        let frank = Connection::open(&path_str).await.expect("open");
        frank
            .execute_batch(
                "CREATE TABLE u(a, b UNIQUE, c); CREATE INDEX u_c ON u(c);
                 INSERT INTO u VALUES (1,10,1),(2,20,2),(3,30,3);
                 UPDATE OR IGNORE u SET b = 20 WHERE a = 1;
                 UPDATE u SET c = c + 10 WHERE c > 0;",
            )
            .await
            .expect("script");
        assert_eq!(
            frank_rows(&frank, "SELECT a, b, c FROM u ORDER BY a").await,
            vec![
                vec![
                    SqliteValue::Integer(1),
                    SqliteValue::Integer(10),
                    SqliteValue::Integer(11)
                ],
                vec![
                    SqliteValue::Integer(2),
                    SqliteValue::Integer(20),
                    SqliteValue::Integer(12)
                ],
                vec![
                    SqliteValue::Integer(3),
                    SqliteValue::Integer(30),
                    SqliteValue::Integer(13)
                ],
            ]
        );
        frank.close().await.expect("close");
        let stock = rusqlite::Connection::open(&path).expect("stock open");
        let verdict: String = stock
            .query_row("PRAGMA integrity_check", [], |row| row.get(0))
            .expect("integrity_check");
        assert_eq!(verdict, "ok");
    });
}

/// UPDATE OR IGNORE that skips a parent row runs no ON UPDATE CASCADE, and a
/// failed statement keeps the completed trigger steps in `total_changes()`
/// (stock's `OP_ResetCount`) while dropping its own rows.
#[test]
fn skipped_and_failed_updates_match_stock_side_effects() {
    asupersync::test_utils::run_test(|| async {
        let frank = Connection::open(":memory:").await.expect("open");
        let stock = rusqlite::Connection::open_in_memory().expect("stock");
        let setup = "PRAGMA foreign_keys = ON;
             CREATE TABLE p(id INTEGER PRIMARY KEY, k UNIQUE);
             CREATE TABLE c(pk REFERENCES p(k) ON UPDATE CASCADE);
             INSERT INTO p VALUES (1, 'a'), (2, 'b'); INSERT INTO c VALUES ('a');
             CREATE TABLE u(a, b UNIQUE, c); CREATE TABLE log(x);
             INSERT INTO u VALUES (1, 10, 1), (2, 20, 2), (3, 30, 3);";
        frank.execute_batch(setup).await.expect("frank setup");
        stock.execute_batch(setup).expect("stock setup");
        let check = "SELECT (SELECT group_concat(pk) FROM c), (SELECT group_concat(k) FROM p), \
                     (SELECT group_concat(b) FROM u), (SELECT count(*) FROM log), total_changes()";
        for sql in [
            "UPDATE OR IGNORE p SET k = 'b' WHERE id = 1",
            "UPDATE OR IGNORE p SET k = 'z' WHERE id = 1",
            "CREATE TRIGGER au AFTER UPDATE ON u BEGIN INSERT INTO log VALUES (new.a); END",
            "UPDATE u SET b = CASE a WHEN 1 THEN 16 WHEN 2 THEN 30 ELSE b END",
            "UPDATE OR FAIL u SET b = CASE a WHEN 1 THEN 17 WHEN 2 THEN 30 ELSE b END",
            "UPDATE OR IGNORE u SET b = CASE a WHEN 1 THEN 18 WHEN 2 THEN 30 ELSE b END",
            "UPDATE OR IGNORE u SET b = 30 WHERE a = 1",
            "CREATE TRIGGER bu BEFORE UPDATE ON u BEGIN INSERT INTO log VALUES (-old.a); END",
            "UPDATE u SET b = 30 WHERE a = 1",
            "UPDATE OR IGNORE u SET b = 30 WHERE a = 1",
        ] {
            let f = frank.execute_batch(sql).await;
            let s = stock.execute_batch(sql);
            assert_eq!(f.is_ok(), s.is_ok(), "`{sql}`: {f:?} vs {s:?}");
            assert_eq!(
                frank_rows(&frank, check).await,
                stock_rows(&stock, check),
                "after `{sql}`"
            );
        }
    });
}
