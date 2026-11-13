#![recursion_limit = "512"]

//! bd-b5j26: `UPDATE ... FROM` on a rowid table rewrote the target while its
//! own scan cursor walked it.
//!
//! - On a file-backed table only the first matching target row was updated;
//!   the rest were silently skipped.
//! - A target row matched by several FROM rows was rewritten once per match,
//!   compounding `SET c = c + ...` and over-counting `changes()`. SQLite
//!   updates each target row once.
//! - An INTEGER PRIMARY KEY rewrite could move a row ahead of the scan.
//!
//! The VDBE lane now collects the matches first and rewrites in a second pass,
//! as SQLite does. Each case runs on a fresh FrankenSQLite database (in memory
//! and file-backed) and on stock SQLite (rusqlite), with multi-page tables, and
//! compares the outcome, the table contents, RETURNING rows, `changes()` and
//! the `total_changes()` delta. File-backed results are also checked with
//! stock `PRAGMA integrity_check`.

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

fn single_integer(rows: &[Vec<SqliteValue>]) -> i64 {
    match rows.first().and_then(|row| row.first()) {
        Some(SqliteValue::Integer(v)) => *v,
        other => panic!("expected one integer, got {other:?}"),
    }
}

struct Schema {
    name: &'static str,
    ddl: &'static str,
    check: &'static str,
}

const SCHEMAS: [Schema; 7] = [
    Schema {
        name: "no_index",
        ddl: "CREATE TABLE u(a, b, c, d);",
        check: "SELECT rowid, a, b, c, d FROM u ORDER BY rowid",
    },
    Schema {
        name: "index_on_assigned",
        ddl: "CREATE TABLE u(a, b, c, d); CREATE INDEX u_c ON u(c);",
        check: "SELECT rowid, a, b, c, d FROM u ORDER BY rowid",
    },
    Schema {
        name: "index_on_where",
        ddl: "CREATE TABLE u(a, b, c, d); CREATE INDEX u_a ON u(a);",
        check: "SELECT rowid, a, b, c, d FROM u ORDER BY rowid",
    },
    Schema {
        name: "index_on_both",
        ddl: "CREATE TABLE u(a, b, c, d); CREATE INDEX u_a ON u(a); CREATE INDEX u_c ON u(c);
              CREATE INDEX u_dc ON u(substr(d, 1, 3), c);",
        check: "SELECT rowid, a, b, c, d FROM u ORDER BY rowid",
    },
    Schema {
        name: "unique",
        ddl: "CREATE TABLE u(a, b UNIQUE, c, d); CREATE INDEX u_c ON u(c);",
        check: "SELECT rowid, a, b, c, d FROM u ORDER BY rowid",
    },
    Schema {
        name: "ipk",
        ddl: "CREATE TABLE u(a INTEGER PRIMARY KEY, b, c, d); CREATE INDEX u_c ON u(c);",
        check: "SELECT a, b, c, d FROM u ORDER BY a",
    },
    Schema {
        name: "without_rowid",
        ddl: "CREATE TABLE u(a PRIMARY KEY, b, c, d) WITHOUT ROWID; CREATE INDEX u_c ON u(c);",
        check: "SELECT a, b, c, d FROM u ORDER BY a",
    },
];

/// 2,000 rows with ~200-byte payloads, so the table and its indexes span many
/// pages and matches cross page boundaries. `src` keys are unique; `dup` has
/// several rows per key (every match assigns the same value, so the outcome
/// does not depend on which match SQLite applies).
const SEED: &str = "
    CREATE TABLE seq(value INTEGER PRIMARY KEY);
    INSERT INTO seq
      WITH RECURSIVE n(value) AS (SELECT 1 UNION ALL SELECT value + 1 FROM n WHERE value < 2000)
      SELECT value FROM n;
    INSERT INTO u(a, b, c, d) SELECT value, value * 10, value, printf('%.200c', 'd') FROM seq;
    CREATE TABLE src(k, nc, w);
    INSERT INTO src VALUES (0, 100, 'x0'), (1, 9, 'x1'), (2, 7, 'x2'), (3, 8, 'x3'),
      (4, 6, 'x4'), (5, 5, 'x5'), (6, 4, 'x6');
    CREATE TABLE dup(k, nc);
    INSERT INTO dup SELECT value % 5, 1 FROM seq WHERE value <= 15;";

const STATEMENTS: [&str; 16] = [
    // The bead's shape: one FROM row matches a run of target rows.
    "UPDATE u SET c = c + v.nc FROM src AS v WHERE v.k = 3 AND u.a > 1900",
    "UPDATE u SET c = c + v.nc FROM src AS v WHERE v.k = 3",
    // A join: every target row matches exactly one FROM row.
    "UPDATE u SET c = c + v.nc FROM src AS v WHERE v.k = u.a % 7",
    // Several FROM rows per target row: applied (and counted) once.
    "UPDATE u SET c = c + d.nc FROM dup AS d WHERE d.k = u.a % 5",
    "UPDATE u SET c = c + 1 FROM dup WHERE dup.k = 2 AND u.a <= 600",
    // Growing, shrinking and same-size rewrites.
    "UPDATE u SET d = d || printf('%.300c', 'g') FROM src AS v WHERE v.k = u.a % 7 AND v.k < 3",
    "UPDATE u SET d = substr(d, 1, 5) FROM src AS v WHERE v.k = 4 AND u.a % 3 = 0",
    "UPDATE u SET b = b + 1 FROM src AS v WHERE v.k = 1 AND u.c BETWEEN 500 AND 1500",
    // Keys that change: the indexed column, and the IPK on the `ipk` schema.
    "UPDATE u SET c = -c FROM src AS v WHERE v.k = 5 AND u.c > 100",
    "UPDATE u SET a = a + 100000 FROM src AS v WHERE v.k = 2 AND u.a > 1000",
    // Two FROM sources joined to each other.
    "UPDATE u SET c = c + v.nc, d = x.w FROM src AS v JOIN src AS x ON x.k = v.k + 1 \
     WHERE v.k = u.a % 7",
    // Plain UPDATE shapes for the Halloween problem.
    "UPDATE u SET c = c + 1 WHERE a > 1900",
    "UPDATE u SET c = c * 2 WHERE c > 10",
    "UPDATE u SET a = a + 1 WHERE a > 100",
    "UPDATE u SET d = d || 'z' WHERE c % 2 = 0",
    // RETURNING rows (sorted so the comparison is order-free).
    "UPDATE u SET c = c + v.nc FROM src AS v WHERE v.k = u.a % 7 RETURNING a, c",
];

async fn run_case(schema: &Schema, file_backed: bool, sql: &str, failures: &mut Vec<String>) {
    let label = format!(
        "[{} | {}] {sql}",
        schema.name,
        if file_backed { "file" } else { "memory" }
    );
    let dir = tempfile::tempdir().expect("temp dir");
    let path = dir.path().join("frank.db");
    let path_str = path.to_str().expect("utf-8 path").to_owned();
    let frank = if file_backed {
        Connection::open(&path_str).await.expect("open")
    } else {
        Connection::open(":memory:").await.expect("open")
    };
    let stock = rusqlite::Connection::open_in_memory().expect("stock open");
    let setup = format!("{} {SEED}", schema.ddl);
    frank.execute_batch(&setup).await.expect("frank setup");
    stock.execute_batch(&setup).expect("stock setup");

    let frank_before = single_integer(&frank_run(&frank, "SELECT total_changes()").await.unwrap());
    let stock_before = single_integer(&stock_run(&stock, "SELECT total_changes()").unwrap());
    let mut frank_result = frank_run(&frank, sql).await;
    let mut stock_result = stock_run(&stock, sql);
    if let (Ok(f), Ok(s)) = (&mut frank_result, &mut stock_result) {
        f.sort_by(|x, y| format!("{x:?}").cmp(&format!("{y:?}")));
        s.sort_by(|x, y| format!("{x:?}").cmp(&format!("{y:?}")));
    }
    match (&frank_result, &stock_result) {
        (Ok(f), Ok(s)) if f != s => failures.push(format!(
            "{label} :: RETURNING differs:\n  frank {f:?}\n  stock {s:?}"
        )),
        (Ok(_), Ok(_)) | (Err(_), Err(_)) => {}
        _ => {
            failures.push(format!(
                "{label} :: outcome: FrankenSQLite {frank_result:?} vs SQLite {stock_result:?}"
            ));
            return;
        }
    }
    for query in [schema.check, "SELECT changes()"] {
        let f = frank_run(&frank, query).await.expect("frank query");
        let s = stock_run(&stock, query).expect("stock query");
        if f != s {
            let first_diff = f.iter().zip(&s).position(|(x, y)| x != y);
            failures.push(format!(
                "{label} :: `{query}` differs ({} vs {} rows, first difference at {first_diff:?}): \
                 frank {:?} stock {:?}",
                f.len(),
                s.len(),
                first_diff.map(|i| &f[i]),
                first_diff.map(|i| &s[i]),
            ));
        }
    }
    let frank_delta =
        single_integer(&frank_run(&frank, "SELECT total_changes()").await.unwrap()) - frank_before;
    let stock_delta =
        single_integer(&stock_run(&stock, "SELECT total_changes()").unwrap()) - stock_before;
    if frank_delta != stock_delta {
        failures.push(format!(
            "{label} :: total_changes() delta {frank_delta} vs SQLite {stock_delta}"
        ));
    }
    frank.close().await.expect("close");

    if file_backed {
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
}

#[test]
fn update_from_and_plain_update_match_stock_across_schemas_and_storage() {
    asupersync::test_utils::run_test(|| async {
        let mut failures = Vec::new();
        let mut cases = 0usize;
        for schema in &SCHEMAS {
            for file_backed in [false, true] {
                for sql in STATEMENTS {
                    run_case(schema, file_backed, sql, &mut failures).await;
                    cases += 1;
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

/// The bead's exact repro, file-backed, kept as its own test so a failure
/// names it.
#[test]
fn bead_repro_updates_both_matching_rows() {
    asupersync::test_utils::run_test(|| async {
        let dir = tempfile::tempdir().expect("temp dir");
        let path = dir.path().join("repro.db");
        let frank = Connection::open(path.to_str().unwrap()).await.expect("open");
        frank
            .execute_batch(
                "CREATE TABLE u(a, b UNIQUE, c, d); CREATE INDEX u_c ON u(c);
                 INSERT INTO u VALUES (1,10,1,0),(2,20,2,0),(3,30,3,0),(4,40,4,0),(5,50,5,0);
                 CREATE TABLE src(k, v, nc); INSERT INTO src VALUES (1,20,9),(3,99,8),(5,55,7);",
            )
            .await
            .expect("setup");
        let changed = frank
            .execute("UPDATE u SET c = c + v.nc FROM src AS v WHERE v.k = 3 AND u.a > 3")
            .await
            .expect("update");
        assert_eq!(changed, 2);
        let rows = frank_run(&frank, "SELECT group_concat(c) FROM u").await.unwrap();
        assert_eq!(rows, vec![vec![SqliteValue::from("1,2,3,12,13")]]);
    });
}
