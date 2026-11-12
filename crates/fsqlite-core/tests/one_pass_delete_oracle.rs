#![recursion_limit = "512"]

//! bd-w4pr1.13.20: a DELETE that scans its table deletes each matching row
//! while the scan stands on it (one pass) when nothing can observe the rows
//! deleted so far, and keeps the two passes (collect the rowids, then delete)
//! otherwise. Compared with stock SQLite (rusqlite, bundled): the changed-row
//! count or RETURNING rows, then every listed state query, on `:memory:` and
//! on a file, through `Connection::execute` and a prepared statement.
//!
//! The shapes a wrong one-pass lowering gets wrong, each covered below:
//! consecutive matching rows (a Next after the Delete skips the successor),
//! runs that empty leaves and rebalance the tree, the last row of a leaf and
//! of the table, index entries of the deleted rows, a WHERE that reads the
//! target table through a subquery (it must see the table as it was before
//! the statement), BEFORE / AFTER triggers and foreign-key actions (run around
//! the program in this engine), and RETURNING.

use fsqlite_core::connection::Connection;
use fsqlite_types::value::SqliteValue;

struct Case {
    setup: &'static [&'static str],
    statement: &'static str,
    states: &'static [&'static str],
}

const SMALL: &[&str] = &[
    "CREATE TABLE t(id INTEGER PRIMARY KEY, k INTEGER, v TEXT)",
    "INSERT INTO t VALUES (1, 1, 'a'), (2, 2, 'b'), (3, 3, 'c'), (4, 4, 'd'), (5, 5, 'e'), (6, 6, 'f'), (7, 7, 'g')",
];
const ROWS_STATE: &[&str] = &["SELECT rowid, * FROM t ORDER BY rowid"];

/// 3,000 rows of about 120 bytes: about 100 leaves, so long runs of deletes
/// empty whole leaves and rebalance interior pages.
const MANY: &[&str] = &[
    "CREATE TABLE t(id INTEGER PRIMARY KEY, a INTEGER, b TEXT)",
    "WITH RECURSIVE n(i) AS (SELECT 1 UNION ALL SELECT i + 1 FROM n WHERE i < 3000) \
     INSERT INTO t SELECT i, (i * 7919) % 1000, printf('%0100d', i) FROM n",
];
const MANY_STATE: &[&str] = &[
    "SELECT count(*), sum(id), sum(a), min(id), max(id) FROM t",
    "SELECT group_concat(id) FROM (SELECT id FROM t ORDER BY id LIMIT 40)",
    "PRAGMA integrity_check",
];

const INDEXED: &[&str] = &[
    "CREATE TABLE t(id INTEGER PRIMARY KEY, a INTEGER, b TEXT)",
    "CREATE INDEX t_a ON t(a)",
    "CREATE UNIQUE INDEX t_b ON t(b)",
    "WITH RECURSIVE n(i) AS (SELECT 1 UNION ALL SELECT i + 1 FROM n WHERE i < 2000) \
     INSERT INTO t SELECT i, i % 13, 'k' || i FROM n",
];
const INDEXED_STATE: &[&str] = &[
    "SELECT count(*), sum(id) FROM t",
    "SELECT a, count(*) FROM t INDEXED BY t_a WHERE a >= 0 GROUP BY a ORDER BY a",
    "SELECT count(*) FROM t INDEXED BY t_b WHERE b > ''",
    "PRAGMA integrity_check",
];

const TRIGGERS: &[&str] = &[
    "CREATE TABLE t(id INTEGER PRIMARY KEY, k INTEGER, v TEXT)",
    "CREATE TABLE log(seq INTEGER PRIMARY KEY, what TEXT, id INTEGER, live INTEGER)",
    "INSERT INTO t VALUES (1, 1, 'a'), (2, 2, 'b'), (3, 3, 'c'), (4, 4, 'd'), (5, 5, 'e'), (6, 6, 'f')",
    "CREATE TRIGGER t_bd BEFORE DELETE ON t BEGIN \
     INSERT INTO log(what, id, live) VALUES ('before', old.id, (SELECT count(*) FROM t)); END",
    "CREATE TRIGGER t_ad AFTER DELETE ON t BEGIN \
     INSERT INTO log(what, id, live) VALUES ('after', old.id, (SELECT count(*) FROM t)); END",
];
const TRIGGER_STATE: &[&str] = &[
    "SELECT rowid, * FROM t ORDER BY rowid",
    "SELECT what, id, live FROM log ORDER BY seq",
];

/// An AFTER trigger that deletes the next row: the second delete of each
/// pair must see what the first one left.
const TRIGGER_DELETES_TARGET: &[&str] = &[
    "CREATE TABLE t(id INTEGER PRIMARY KEY, k INTEGER, v TEXT)",
    "INSERT INTO t VALUES (1, 1, 'a'), (2, 2, 'b'), (3, 3, 'c'), (4, 4, 'd'), (5, 5, 'e'), (6, 6, 'f')",
    "CREATE TRIGGER t_ad AFTER DELETE ON t BEGIN DELETE FROM t WHERE id = old.id + 1; END",
];

const FK_CASCADE: &[&str] = &[
    "PRAGMA foreign_keys = ON",
    "CREATE TABLE p(id INTEGER PRIMARY KEY, k INTEGER)",
    "CREATE TABLE c(id INTEGER PRIMARY KEY, pid INTEGER REFERENCES p(id) ON DELETE CASCADE, \
     nid INTEGER REFERENCES p(id) ON DELETE SET NULL)",
    "INSERT INTO p VALUES (1, 1), (2, 2), (3, 3), (4, 4), (5, 5)",
    "INSERT INTO c VALUES (10, 1, 2), (11, 2, 3), (12, 3, 4), (13, 4, 5), (14, 5, 1)",
];
const FK_STATE: &[&str] = &["SELECT * FROM p ORDER BY id", "SELECT * FROM c ORDER BY id"];

/// A self-referencing foreign key: deleting a row cascades to rows of the
/// same table, some of which the scan has not reached yet.
const FK_SELF: &[&str] = &[
    "PRAGMA foreign_keys = ON",
    "CREATE TABLE t(id INTEGER PRIMARY KEY, parent INTEGER REFERENCES t(id) ON DELETE CASCADE, k INTEGER)",
    "INSERT INTO t VALUES (1, NULL, 0), (2, 1, 1), (3, 2, 0), (4, NULL, 1), (5, 4, 0), (6, 5, 1), (7, NULL, 0)",
];

const WITHOUT_ROWID: &[&str] = &[
    "CREATE TABLE t(k INTEGER PRIMARY KEY, v TEXT) WITHOUT ROWID",
    "INSERT INTO t VALUES (1, 'a'), (2, 'b'), (3, 'c'), (4, 'd'), (5, 'e'), (6, 'f')",
];
const WITHOUT_ROWID_STATE: &[&str] = &["SELECT * FROM t ORDER BY k"];

const CASES: &[Case] = &[
    // Consecutive matches: a Next after each Delete would skip every other row.
    Case {
        setup: SMALL,
        statement: "DELETE FROM t WHERE k > 2",
        states: ROWS_STATE,
    },
    Case {
        setup: SMALL,
        statement: "DELETE FROM t WHERE v <> 'zz'",
        states: ROWS_STATE,
    },
    // The first row, the last row, alternate rows, no row.
    Case {
        setup: SMALL,
        statement: "DELETE FROM t WHERE k % 7 = 1",
        states: ROWS_STATE,
    },
    Case {
        setup: SMALL,
        statement: "DELETE FROM t WHERE k = 7",
        states: ROWS_STATE,
    },
    Case {
        setup: SMALL,
        statement: "DELETE FROM t WHERE k % 2 = 0",
        states: ROWS_STATE,
    },
    Case {
        setup: SMALL,
        statement: "DELETE FROM t WHERE k > 100",
        states: ROWS_STATE,
    },
    Case {
        setup: SMALL,
        statement: "DELETE FROM t WHERE length(v) = 1 AND k BETWEEN 3 AND 5",
        states: ROWS_STATE,
    },
    // A WHERE that reads the target table: it must see the table as it was.
    // Deleting row 1 (no row 0) must not make row 2 deletable.
    Case {
        setup: SMALL,
        statement: "DELETE FROM t WHERE NOT EXISTS (SELECT 1 FROM t AS u WHERE u.id = t.id - 1)",
        states: ROWS_STATE,
    },
    Case {
        setup: SMALL,
        statement: "DELETE FROM t WHERE k <= (SELECT count(*) FROM t) - 4",
        states: ROWS_STATE,
    },
    Case {
        setup: SMALL,
        statement: "DELETE FROM t WHERE id IN (SELECT id + 1 FROM t WHERE k % 2 = 1)",
        states: ROWS_STATE,
    },
    Case {
        setup: SMALL,
        statement: "DELETE FROM t WHERE k > 4 RETURNING id, v",
        states: ROWS_STATE,
    },
    // Runs that empty leaves and rebalance; the table's last rows; all rows.
    Case {
        setup: MANY,
        statement: "DELETE FROM t WHERE id % 50 < 45",
        states: MANY_STATE,
    },
    Case {
        setup: MANY,
        statement: "DELETE FROM t WHERE id % 7 = 0",
        states: MANY_STATE,
    },
    Case {
        setup: MANY,
        statement: "DELETE FROM t WHERE id > 2900 OR id < 40",
        states: MANY_STATE,
    },
    Case {
        setup: MANY,
        statement: "DELETE FROM t WHERE a >= 0",
        states: MANY_STATE,
    },
    // Index entries of the deleted rows.
    Case {
        setup: INDEXED,
        statement: "DELETE FROM t WHERE id % 3 = 0",
        states: INDEXED_STATE,
    },
    Case {
        setup: INDEXED,
        statement: "DELETE FROM t WHERE substr(b, 2) + 0 > 1500",
        states: INDEXED_STATE,
    },
    // Triggers and foreign keys run around the program.
    Case {
        setup: TRIGGERS,
        statement: "DELETE FROM t WHERE k % 2 = 1",
        states: TRIGGER_STATE,
    },
    Case {
        setup: TRIGGER_DELETES_TARGET,
        statement: "DELETE FROM t WHERE k % 2 = 1",
        states: ROWS_STATE,
    },
    Case {
        setup: FK_CASCADE,
        statement: "DELETE FROM p WHERE k % 2 = 0",
        states: FK_STATE,
    },
    Case {
        setup: FK_SELF,
        statement: "DELETE FROM t WHERE k = 1",
        states: ROWS_STATE,
    },
    // The cascade reaches rows ahead of the scan that the statement does not
    // match (3 under 2, 5 and 6 under 4), so the count is comparable.
    Case {
        setup: FK_SELF,
        statement: "DELETE FROM t WHERE k = 1 AND id < 5",
        states: ROWS_STATE,
    },
    Case {
        setup: WITHOUT_ROWID,
        statement: "DELETE FROM t WHERE k % 2 = 0",
        states: WITHOUT_ROWID_STATE,
    },
];

fn tag_f(v: &SqliteValue) -> String {
    match v {
        SqliteValue::Null => "NULL".to_owned(),
        SqliteValue::Integer(n) => n.to_string(),
        SqliteValue::Float(f) => format!("{f:?}"),
        SqliteValue::Text(s) => format!("'{s}'"),
        SqliteValue::Blob(b) => format!("X'{}'", b.len()),
    }
}

fn tag_r(v: &rusqlite::types::Value) -> String {
    match v {
        rusqlite::types::Value::Null => "NULL".to_owned(),
        rusqlite::types::Value::Integer(n) => n.to_string(),
        rusqlite::types::Value::Real(f) => format!("{f:?}"),
        rusqlite::types::Value::Text(s) => format!("'{s}'"),
        rusqlite::types::Value::Blob(b) => format!("X'{}'", b.len()),
    }
}

fn stock_message(error: rusqlite::Error) -> String {
    match error {
        rusqlite::Error::SqliteFailure(_, Some(m))
        | rusqlite::Error::SqlInputError { msg: m, .. } => m,
        other => other.to_string(),
    }
}

fn stock_rows(r: &rusqlite::Connection, sql: &str) -> Result<Vec<Vec<String>>, String> {
    let mut statement = r.prepare(sql).map_err(stock_message)?;
    let n = statement.column_count();
    statement
        .query_map([], |row| {
            Ok((0..n)
                .map(|i| tag_r(&row.get_unwrap::<_, rusqlite::types::Value>(i)))
                .collect::<Vec<_>>())
        })
        .and_then(Iterator::collect)
        .map_err(stock_message)
}

fn frank_rows(rows: &[fsqlite_core::connection::Row]) -> Vec<Vec<String>> {
    rows.iter()
        .map(|row| row.values().iter().map(tag_f).collect())
        .collect()
}

fn stock_outcome(r: &rusqlite::Connection, sql: &str) -> String {
    if sql.contains("RETURNING") {
        return match stock_rows(r, sql) {
            Ok(rows) => format!("rows {rows:?}"),
            Err(m) => format!("error: {m}"),
        };
    }
    match r
        .prepare(sql)
        .and_then(|mut statement| statement.execute([]))
    {
        Ok(changed) => format!("changed {changed}"),
        Err(e) => format!("error: {}", stock_message(e)),
    }
}

async fn frank_outcome(f: &Connection, sql: &str, prepared: bool) -> String {
    if sql.contains("RETURNING") {
        return match f.query(sql).await {
            Ok(rows) => format!("rows {:?}", frank_rows(&rows)),
            Err(e) => format!("error: {e}"),
        };
    }
    let changed = if prepared {
        match f.prepare(sql).await {
            Ok(statement) => statement.execute().await,
            Err(e) => Err(e),
        }
    } else {
        f.execute(sql).await
    };
    match changed {
        Ok(changed) => format!("changed {changed}"),
        Err(e) => format!("error: {e}"),
    }
}

/// The one statement whose change count differs from stock for a reason
/// older than the one-pass delete (bd-n8g76, the same on the two-pass base):
/// a self-referencing cascade removes a row the statement also matches, stock
/// does not count that row and this engine, which runs FK actions after the
/// program, does. Its table state is still compared.
fn count_differs_by_bd_n8g76(case: &Case) -> bool {
    // By content: a `const` slice has no single address to compare.
    case.setup == FK_SELF && case.statement == "DELETE FROM t WHERE k = 1"
}

async fn check_case(case: &Case, file_backed: bool, prepared: bool) -> Vec<String> {
    let dir = tempfile::tempdir().expect("temp dir");
    let path = if file_backed {
        dir.path()
            .join("one_pass_delete.db")
            .to_str()
            .expect("utf-8 path")
            .to_owned()
    } else {
        ":memory:".to_owned()
    };
    let f = Connection::open(&path).await.expect("open");
    let r = rusqlite::Connection::open_in_memory().expect("stock open");
    for setup in case.setup {
        f.execute(setup).await.expect("frank setup");
        r.execute_batch(setup).expect("stock setup");
    }
    let mut failures = Vec::new();
    let label = format!(
        "[file_backed={file_backed} prepared={prepared}] `{}`",
        case.statement
    );
    let fo = frank_outcome(&f, case.statement, prepared).await;
    let so = stock_outcome(&r, case.statement);
    if fo != so && !count_differs_by_bd_n8g76(case) {
        failures.push(format!("{label}: frank {fo:?} vs stock {so:?}"));
    }
    for state in case.states {
        let fv = match f.query(state).await {
            Ok(rows) => Ok(frank_rows(&rows)),
            Err(e) => Err(e.to_string()),
        };
        let sv = stock_rows(&r, state);
        if fv != sv {
            failures.push(format!(
                "{label}, then `{state}`: frank {fv:?} vs stock {sv:?}"
            ));
        }
    }
    f.close().await.expect("close");
    failures
}

#[test]
fn scan_delete_matches_stock_in_one_pass_and_two() {
    asupersync::test_utils::run_test(|| async {
        let mut failures = Vec::new();
        for case in CASES {
            // RETURNING rows of a prepared DML statement are not readable
            // through `PreparedStatement::query`; that case runs directly.
            let modes: &[bool] = if case.statement.contains("RETURNING") {
                &[false]
            } else {
                &[false, true]
            };
            for file_backed in [false, true] {
                for &prepared in modes {
                    failures.extend(check_case(case, file_backed, prepared).await);
                }
            }
        }
        assert!(
            failures.is_empty(),
            "{} differences from SQLite:\n{}",
            failures.len(),
            failures.join("\n")
        );
    });
}

/// The program shape this test file exists for: with no subquery and no
/// RETURNING the scan deletes in place (no RowSet); a subquery that reads the
/// target table, or RETURNING, keeps the two passes.
#[test]
fn scan_delete_is_one_pass_only_where_nothing_can_observe_it() {
    asupersync::test_utils::run_test(|| async {
        let f = Connection::open(":memory:").await.expect("open");
        for setup in SMALL {
            f.execute(setup).await.expect("setup");
        }
        let opcodes = |rows: Vec<fsqlite_core::connection::Row>| -> Vec<String> {
            rows.iter()
                .map(|row| match &row.values()[1] {
                    SqliteValue::Text(name) => name.to_string(),
                    other => format!("{other:?}"),
                })
                .collect()
        };
        for (statement, one_pass) in [
            ("DELETE FROM t WHERE k % 2 = 0", true),
            ("DELETE FROM t WHERE v <> 'x' AND k > 1", true),
            (
                "DELETE FROM t WHERE k <= (SELECT count(*) FROM t) - 4",
                false,
            ),
            ("DELETE FROM t WHERE k > 4 RETURNING id", false),
        ] {
            let program = opcodes(
                f.query(&format!("EXPLAIN {statement}"))
                    .await
                    .expect("explain"),
            );
            let uses_rowset = program.iter().any(|op| op == "RowSetAdd");
            assert_eq!(
                !uses_rowset, one_pass,
                "`{statement}` one pass expected {one_pass}: {program:?}"
            );
            if one_pass {
                let delete = program
                    .iter()
                    .position(|op| op == "Delete")
                    .expect("Delete");
                let next = program.iter().rposition(|op| op == "Next").expect("Next");
                assert!(
                    delete < next,
                    "`{statement}` deletes inside the scan: {program:?}"
                );
            }
        }
    });
}
