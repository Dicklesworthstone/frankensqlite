#![recursion_limit = "512"]

//! bd-2jbl5: `UPDATE ... FROM` on a table with UPDATE triggers (or with
//! foreign-key work) failed with "no such column: v.k".
//!
//! The trigger OLD/NEW collector and the row-by-row replay locators matched
//! the target rows with a SELECT over the target table alone, so a WHERE or a
//! SET value naming a FROM source could not resolve. Every conflict mode was
//! affected; the bead first surfaced under OR IGNORE / OR REPLACE.
//!
//! Each case runs on a fresh file-backed FrankenSQLite database and on stock
//! SQLite (rusqlite) and compares the outcome (ok / error), the target rows,
//! the BEFORE/AFTER trigger log, RETURNING rows, `changes()` and the
//! `total_changes()` delta. The FrankenSQLite file is then checked with stock
//! `PRAGMA integrity_check`.

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

/// Run `sql` as a query on both engines so RETURNING rows are compared too.
async fn frank_run(conn: &Connection, sql: &str) -> Result<Vec<Vec<SqliteValue>>, String> {
    conn.query(sql)
        .await
        .map(|rows| rows.iter().map(|row| row.values().to_vec()).collect())
        .map_err(|e| e.to_string())
}

fn stock_run(conn: &rusqlite::Connection, sql: &str) -> Result<Vec<Vec<SqliteValue>>, String> {
    let mut stmt = conn.prepare(sql).map_err(|e| e.to_string())?;
    let width = stmt.column_count();
    let rows = stmt
        .query_map([], |row| {
            (0..width)
                .map(|i| row.get::<_, Value>(i).map(value))
                .collect::<rusqlite::Result<Vec<_>>>()
        })
        .map_err(|e| e.to_string())?
        .collect::<rusqlite::Result<Vec<_>>>()
        .map_err(|e| e.to_string())?;
    Ok(rows)
}

struct Schema {
    name: &'static str,
    ddl: &'static str,
    check: &'static str,
}

const SCHEMAS: [Schema; 3] = [
    Schema {
        name: "rowid",
        ddl: "CREATE TABLE u(a, b UNIQUE, c, d); CREATE INDEX u_c ON u(c);",
        check: "SELECT rowid, a, b, c, d FROM u ORDER BY rowid",
    },
    Schema {
        name: "ipk",
        ddl: "CREATE TABLE u(a INTEGER PRIMARY KEY, b UNIQUE, c, d); CREATE INDEX u_c ON u(c);",
        check: "SELECT a, b, c, d FROM u ORDER BY a",
    },
    Schema {
        name: "without_rowid",
        ddl: "CREATE TABLE u(a PRIMARY KEY, b UNIQUE, c, d) WITHOUT ROWID; \
              CREATE INDEX u_c ON u(c);",
        check: "SELECT a, b, c, d FROM u ORDER BY a",
    },
];

/// Source keys are unique, so no target row has more than one FROM match
/// (which row stock picks then is unspecified).
const SEED: &str = "CREATE TABLE log(seq INTEGER PRIMARY KEY, ev TEXT);
     CREATE TABLE src(k, nb, nc); INSERT INTO src VALUES (1, 20, 9), (3, 99, 8), (5, 55, 7);
     CREATE TABLE extra(k, w); INSERT INTO extra VALUES (1, 'x1'), (3, 'x3');
     INSERT INTO u(a, b, c, d) VALUES (1, 10, 1, 2), (2, 20, 2, 3), (3, 30, 3, 4),
       (4, 40, 4, 5), (5, 50, 5, 6);";

const TRIGGERS: [(&str, &str); 3] = [
    ("none", ""),
    (
        "after",
        "CREATE TRIGGER u_au AFTER UPDATE ON u BEGIN
           INSERT INTO log(ev) VALUES ('au ' || old.a || ' b=' || old.b || '>' || new.b
             || ' c=' || old.c || '>' || new.c);
         END;",
    ),
    (
        "before+after",
        "CREATE TRIGGER u_bu BEFORE UPDATE ON u BEGIN
           INSERT INTO log(ev) VALUES ('bu ' || old.a || ' n=' || (SELECT count(*) FROM log));
         END;
         CREATE TRIGGER u_au AFTER UPDATE ON u BEGIN
           INSERT INTO log(ev) VALUES ('au ' || old.a || ' b=' || old.b || '>' || new.b
             || ' c=' || old.c || '>' || new.c);
         END;",
    ),
];

const MODES: [&str; 6] = [
    "",
    "OR IGNORE ",
    "OR REPLACE ",
    "OR FAIL ",
    "OR ABORT ",
    "OR ROLLBACK ",
];

/// UPDATE ... FROM shapes; `{or}` is the conflict clause. Several make row 1
/// collide with row 2 on UNIQUE(b), so the conflict modes differ.
const STATEMENTS: [&str; 9] = [
    "UPDATE {or}u SET b = v.nb, c = v.nc FROM src AS v WHERE u.a = v.k",
    "UPDATE {or}u SET b = src.nb, c = src.nc FROM src WHERE u.a = src.k",
    "UPDATE {or}u AS t SET c = v.nc + t.c FROM src AS v WHERE t.a = v.k",
    "UPDATE {or}u SET b = v.nb FROM (SELECT k, nb FROM src WHERE k < 4) AS v WHERE u.a = v.k",
    "WITH s AS (SELECT k, nb + 1 AS nb FROM src) UPDATE {or}u SET b = s.nb FROM s WHERE u.a = s.k",
    "UPDATE {or}u SET c = v.nc, d = x.w FROM src AS v JOIN extra AS x ON x.k = v.k \
     WHERE u.a = v.k",
    "UPDATE {or}u SET (b, c) = (v.nb, v.nc) FROM src AS v WHERE u.a = v.k AND v.nc > 7",
    "UPDATE {or}u SET b = v.nb FROM src AS v WHERE u.a = v.k RETURNING a, b, c",
    "UPDATE {or}u SET c = c + v.nc FROM src AS v WHERE v.k = 3 AND u.a > 3",
];

async fn run_case(
    schema: &Schema,
    trigger: (&str, &str),
    fk: bool,
    sql: &str,
    failures: &mut Vec<String>,
) {
    let label = format!("[{} | {} | fk={fk}] {sql}", schema.name, trigger.0);
    let dir = tempfile::tempdir().expect("temp dir");
    let path = dir.path().join("frank.db");
    let path_str = path.to_str().expect("utf-8 path").to_owned();
    let frank = Connection::open(&path_str).await.expect("open");
    let stock = rusqlite::Connection::open_in_memory().expect("stock open");
    let fk_setup = if fk {
        "PRAGMA foreign_keys = ON;
         CREATE TABLE child(cb REFERENCES u(b) ON UPDATE CASCADE);
         INSERT INTO child VALUES (10), (30);"
    } else {
        ""
    };
    let setup = format!("{} {SEED} {} {fk_setup}", schema.ddl, trigger.1);
    frank.execute_batch(&setup).await.expect("frank setup");
    stock.execute_batch(&setup).expect("stock setup");

    let frank_before = single_integer(&frank_rows(&frank, "SELECT total_changes()").await);
    let stock_before = single_integer(&stock_rows(&stock, "SELECT total_changes()"));
    let frank_result = frank_run(&frank, sql).await;
    let stock_result = stock_run(&stock, sql);
    match (&frank_result, &stock_result) {
        (Ok(f), Ok(s)) if f != s => failures.push(format!(
            "{label} :: RETURNING differs:\n  frank {f:?}\n  stock {s:?}"
        )),
        (Ok(_), Ok(_)) => {}
        (Err(f), Err(s)) if f.contains("UNIQUE") == s.contains("UNIQUE") => {}
        _ => {
            failures.push(format!(
                "{label} :: outcome: FrankenSQLite {frank_result:?} vs SQLite {stock_result:?}"
            ));
            return;
        }
    }
    let mut queries = vec![
        schema.check,
        "SELECT ev FROM log ORDER BY seq",
        "SELECT changes()",
    ];
    if fk {
        queries.push("SELECT cb FROM child ORDER BY rowid");
    }
    for query in queries {
        let (f, s) = (frank_rows(&frank, query).await, stock_rows(&stock, query));
        if f != s {
            failures.push(format!(
                "{label} :: `{query}` differs:\n  frank {f:?}\n  stock {s:?}"
            ));
        }
    }
    let frank_delta =
        single_integer(&frank_rows(&frank, "SELECT total_changes()").await) - frank_before;
    let stock_delta = single_integer(&stock_rows(&stock, "SELECT total_changes()")) - stock_before;
    if frank_delta != stock_delta {
        failures.push(format!(
            "{label} :: total_changes() delta {frank_delta} vs SQLite {stock_delta}"
        ));
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
fn update_from_with_triggers_and_fks_matches_stock_in_every_conflict_mode() {
    asupersync::test_utils::run_test(|| async {
        let mut failures = Vec::new();
        let mut cases = 0usize;
        for schema in &SCHEMAS {
            for trigger in TRIGGERS {
                for fk in [false, true] {
                    for template in STATEMENTS {
                        for mode in MODES {
                            let sql = template.replace("{or}", mode);
                            run_case(schema, trigger, fk, &sql, &mut failures).await;
                            cases += 1;
                        }
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

/// The bead's shape, with a bound parameter in the WHERE, kept as its own test
/// so a failure names it.
#[test]
fn update_or_ignore_from_alias_with_trigger_and_parameter() {
    asupersync::test_utils::run_test(|| async {
        let frank = Connection::open(":memory:").await.expect("open");
        frank
            .execute_batch(
                "CREATE TABLE u(a, b UNIQUE, c); CREATE TABLE log(ev);
                 CREATE TABLE src(k, nb, nc); INSERT INTO src VALUES (1, 20, 9), (3, 99, 8);
                 INSERT INTO u VALUES (1, 10, 1), (2, 20, 2), (3, 30, 3);
                 CREATE TRIGGER au AFTER UPDATE ON u BEGIN
                   INSERT INTO log VALUES (old.a || ':' || new.b);
                 END;",
            )
            .await
            .expect("setup");
        let changed = frank
            .execute_with_params(
                "UPDATE OR IGNORE u SET b = v.nb, c = v.nc FROM src AS v \
                 WHERE u.a = v.k AND v.nc >= ?1",
                &[SqliteValue::Integer(8)],
            )
            .await
            .expect("UPDATE OR IGNORE ... FROM with a trigger");
        assert_eq!(changed, 1, "row 1 is skipped on UNIQUE(b); row 3 updates");
        assert_eq!(
            frank_rows(&frank, "SELECT a, b, c FROM u ORDER BY a").await,
            vec![
                vec![
                    SqliteValue::Integer(1),
                    SqliteValue::Integer(10),
                    SqliteValue::Integer(1)
                ],
                vec![
                    SqliteValue::Integer(2),
                    SqliteValue::Integer(20),
                    SqliteValue::Integer(2)
                ],
                vec![
                    SqliteValue::Integer(3),
                    SqliteValue::Integer(99),
                    SqliteValue::Integer(8)
                ],
            ]
        );
        assert_eq!(
            frank_rows(&frank, "SELECT ev FROM log").await,
            vec![vec![SqliteValue::from("3:99")]]
        );
    });
}

/// A multi-row UPDATE OR ROLLBACK replayed row by row (here because of an
/// AFTER trigger) reports the UNIQUE violation, as stock does. The nested row
/// statement used to attempt a full ROLLBACK outside any explicit
/// transaction and report "cannot rollback - no transaction is active".
#[test]
fn replayed_update_or_rollback_reports_the_constraint_error() {
    asupersync::test_utils::run_test(|| async {
        let setup = "CREATE TABLE u(a, b UNIQUE); CREATE TABLE log(x);
             INSERT INTO u VALUES (1, 10), (2, 20), (3, 30);
             CREATE TRIGGER au AFTER UPDATE ON u BEGIN INSERT INTO log VALUES (new.a); END;";
        let sql = "UPDATE OR ROLLBACK u SET b = CASE a WHEN 1 THEN 15 WHEN 2 THEN 30 ELSE b END";
        let frank = Connection::open(":memory:").await.expect("open");
        frank.execute_batch(setup).await.expect("frank setup");
        let stock = rusqlite::Connection::open_in_memory().expect("stock");
        stock.execute_batch(setup).expect("stock setup");
        let frank_err = frank.execute(sql).await.expect_err("UNIQUE violation").to_string();
        let stock_err = stock.execute_batch(sql).expect_err("UNIQUE violation").to_string();
        assert!(
            frank_err.contains("UNIQUE constraint failed: u.b"),
            "FrankenSQLite: {frank_err} (SQLite: {stock_err})"
        );
        let check = "SELECT (SELECT group_concat(b) FROM u), (SELECT count(*) FROM log)";
        assert_eq!(frank_rows(&frank, check).await, stock_rows(&stock, check));
    });
}
