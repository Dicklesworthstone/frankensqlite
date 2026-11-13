#![recursion_limit = "512"]

//! A caller-supplied rowid or INTEGER PRIMARY KEY value goes through stock's
//! `OP_MustBeInt` gate on every write path.
//!
//! INSERT (VALUES, SELECT, DEFAULT VALUES, an explicit `rowid` column), UPDATE,
//! UPDATE ... FROM and UPSERT DO UPDATE used to copy the value into the rowid
//! register unconverted. The table row landed at the value truncated to an
//! integer (`1.5` -> 1, `'abc'` -> 0, `'1e1'` -> 1), where stock either converts
//! it exactly (`'7'`, `8.0`, `'1e1'` -> 10) or fails with "datatype mismatch".
//! Every secondary-index entry also carried the raw REAL or TEXT value as its
//! trailing rowid, so a single `INSERT INTO t VALUES (201.0, 'x')` into a table
//! with a UNIQUE column left a file stock `integrity_check` calls malformed
//! ("index key record missing trailing integer rowid"). Shipped since at least
//! 0.3.9.
//!
//! Each case runs on FrankenSQLite (in memory and file-backed) and on stock
//! SQLite (rusqlite), and compares the outcome and error message, the rows with
//! their storage classes, and `changes()`. The in-memory database must pass
//! FrankenSQLite's `integrity_check`; the file must pass stock's.

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

struct Table {
    name: &'static str,
    ddl: &'static str,
    /// The table has a rowid, so `INSERT INTO t(rowid, ...)` is valid.
    rowid: bool,
    /// `b` alone is UNIQUE, so `ON CONFLICT(b)` names a real constraint.
    unique_b: bool,
}

const TABLES: [Table; 6] = [
    Table {
        name: "ipk_unique",
        ddl: "CREATE TABLE t(id INTEGER PRIMARY KEY, a, b UNIQUE);",
        rowid: true,
        unique_b: true,
    },
    Table {
        name: "ipk_composite_unique",
        ddl: "CREATE TABLE t(id INTEGER PRIMARY KEY, a, b, UNIQUE(a, b));",
        rowid: true,
        unique_b: false,
    },
    Table {
        name: "ipk_expr_partial",
        ddl: "CREATE TABLE t(id INTEGER PRIMARY KEY, a, b);
              CREATE INDEX t_lower_b ON t(lower(b));
              CREATE INDEX t_a_nn ON t(a) WHERE a IS NOT NULL;",
        rowid: true,
        unique_b: false,
    },
    // `INTEGER PRIMARY KEY DESC` is not a rowid alias: `id` is an ordinary
    // column with its own UNIQUE index.
    Table {
        name: "ipk_desc_not_alias",
        ddl: "CREATE TABLE t(id INTEGER PRIMARY KEY DESC, a, b UNIQUE);",
        rowid: true,
        unique_b: true,
    },
    Table {
        name: "plain_rowid",
        ddl: "CREATE TABLE t(id, a, b UNIQUE);",
        rowid: true,
        unique_b: true,
    },
    Table {
        name: "without_rowid",
        ddl: "CREATE TABLE t(id PRIMARY KEY, a, b UNIQUE) WITHOUT ROWID;",
        rowid: false,
        unique_b: true,
    },
];

const SEED: &str = "CREATE TABLE s(k, a, b);
     INSERT INTO s VALUES (1, 1.5, 'x'), (2, 2.0, 'y'), (3, '3', 'z'), (4, NULL, 'x'), (5, 2.0, 'w');
     INSERT INTO t(id, a, b) VALUES (1, 1, 'x'), (2, 2, 'y'), (3, 3, 'z');";

const MODES: [&str; 4] = ["", "OR REPLACE ", "OR IGNORE ", "OR ABORT "];

/// Statement templates; `<or>` is the conflict clause.
const STATEMENTS: [&str; 16] = [
    // The reported shape: an aggregate whose key expression is REAL.
    "INSERT <or>INTO t(id, a, b) SELECT 200 + abs(coalesce(a, 0)) % 50, count(*), \
     'agg-' || coalesce(a, 'n') FROM s GROUP BY a",
    "INSERT <or>INTO t(id, a, b) SELECT DISTINCT a, k, b || '-d' FROM s WHERE a IS NOT NULL",
    "INSERT <or>INTO t(id, a, b) SELECT k + 0.0, a, b FROM s ORDER BY k DESC",
    "INSERT <or>INTO t(id, a, b) SELECT max(k) * 1.0, sum(k), group_concat(b) FROM s",
    "INSERT <or>INTO t(id, a, b) SELECT row_number() OVER (ORDER BY k) * 1.0 + 10, a, b || '-w' FROM s",
    "INSERT <or>INTO t(id, a, b) VALUES ('7', 1, 'p'), (8.0, 2, 'q'), ('1e1', 3, 'r'), (' 30 ', 4, 's')",
    "INSERT <or>INTO t(id, a, b) VALUES (2.5, 1, 'm')",
    "INSERT <or>INTO t(id, a, b) VALUES ('abc', 1, 'm')",
    "INSERT <or>INTO t(id, a, b) VALUES (x'01', 1, 'm')",
    "INSERT <or>INTO t(id, a, b) VALUES (9223372036854775807.0, 1, 'm')",
    "INSERT <or>INTO t(id, a, b) VALUES (NULL, 9, 'auto')",
    "UPDATE <or>t SET id = id + 100.0",
    "UPDATE <or>t SET id = '42' WHERE b = 'x'",
    "UPDATE <or>t SET id = NULL WHERE b = 'y'",
    "UPDATE <or>t SET id = s.k * 1.0 + 300 FROM s WHERE s.b = t.b",
    "UPDATE <or>t SET id = 2.5 WHERE b = 'z'",
];

/// Statements that only exist for some table shapes.
fn extra_statements(table: &Table) -> Vec<String> {
    let mut out = Vec::new();
    if table.rowid {
        out.push("INSERT INTO t(rowid, a, b) VALUES ('11', 1, 'r1'), (12.0, 2, 'r2')".to_owned());
        out.push("INSERT INTO t(rowid, a, b) VALUES ('abc', 1, 'r3')".to_owned());
        out.push("INSERT INTO t(rowid, a, b) VALUES (NULL, 1, 'r4')".to_owned());
    }
    if table.unique_b {
        out.push(
            "INSERT INTO t(id, a, b) VALUES (50, 0, 'x') \
             ON CONFLICT(b) DO UPDATE SET id = excluded.id + 0.0 + 50"
                .to_owned(),
        );
        out.push(
            "INSERT INTO t(id, a, b) VALUES (51, 0, 'y') ON CONFLICT(b) DO UPDATE SET id = '61'"
                .to_owned(),
        );
        out.push(
            "INSERT INTO t(id, a, b) VALUES (52, 0, 'z') ON CONFLICT(b) DO UPDATE SET id = NULL"
                .to_owned(),
        );
        out.push(
            "INSERT INTO t(id, a, b) VALUES (53, 0, 'x') ON CONFLICT(b) DO UPDATE SET id = 1.5"
                .to_owned(),
        );
        out.push(
            "INSERT INTO t(id, a, b) VALUES (54, 0, 'y') ON CONFLICT(b) DO UPDATE SET a = 7"
                .to_owned(),
        );
    }
    out.push("INSERT INTO t DEFAULT VALUES".to_owned());
    out
}

const CHECK: &str = "SELECT id, typeof(id), a, typeof(a), b FROM t ORDER BY b, id";

fn frank_message(err: &fsqlite_error::FrankenError) -> String {
    err.to_string()
}

fn stock_message(err: &rusqlite::Error) -> String {
    match err {
        rusqlite::Error::SqliteFailure(_, Some(message)) => message.clone(),
        other => other.to_string(),
    }
}

async fn run_case(table: &Table, sql: &str, file_backed: bool, failures: &mut Vec<String>) {
    let label = format!(
        "[{} | {}] {sql}",
        table.name,
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
    let setup = format!("{} {SEED}", table.ddl);
    frank.execute_batch(&setup).await.expect("frank setup");
    stock.execute_batch(&setup).expect("stock setup");

    let frank_result = frank.execute_batch(sql).await;
    let stock_result = stock.execute_batch(sql);
    match (&frank_result, &stock_result) {
        (Ok(()), Ok(())) => {}
        (Err(f), Err(s)) => {
            let (f, s) = (frank_message(f), stock_message(s));
            if !f.contains(&s) {
                failures.push(format!("{label} :: error message: `{f}` vs SQLite `{s}`"));
            }
        }
        _ => {
            failures.push(format!(
                "{label} :: outcome: FrankenSQLite {frank_result:?} vs SQLite {stock_result:?}"
            ));
            return;
        }
    }
    for query in [CHECK, "SELECT changes()"] {
        let (f, s) = (frank_rows(&frank, query).await, stock_rows(&stock, query));
        if f != s {
            failures.push(format!(
                "{label} :: `{query}` differs:\n  frank {f:?}\n  stock {s:?}"
            ));
        }
    }
    let own = frank.query("PRAGMA integrity_check").await.map(|rows| {
        rows.iter()
            .map(|row| row.values().to_vec())
            .collect::<Vec<_>>()
    });
    if !matches!(&own, Ok(rows) if *rows == [vec![SqliteValue::from("ok")]]) {
        failures.push(format!("{label} :: FrankenSQLite integrity_check: {own:?}"));
    }
    frank.close().await.expect("close");

    if file_backed {
        let verdict = stock_integrity_check(&path);
        if verdict != ["ok"] {
            failures.push(format!("{label} :: stock integrity_check: {verdict:?}"));
        }
    }
}

/// Stock `PRAGMA integrity_check` on a file; a file stock cannot even scan
/// reports its error as the verdict.
fn stock_integrity_check(path: &std::path::Path) -> Vec<String> {
    let checked = rusqlite::Connection::open(path).expect("stock open of fsqlite file");
    let mut stmt = match checked.prepare("PRAGMA integrity_check") {
        Ok(stmt) => stmt,
        Err(e) => return vec![e.to_string()],
    };
    let rows = stmt.query_map([], |row| row.get::<_, String>(0));
    match rows.and_then(Iterator::collect::<rusqlite::Result<Vec<_>>>) {
        Ok(verdict) => verdict,
        Err(e) => vec![e.to_string()],
    }
}

#[test]
fn caller_supplied_rowids_pass_must_be_int_on_every_write_path() {
    asupersync::test_utils::run_test(|| async {
        let mut failures = Vec::new();
        let mut cases = 0_usize;
        for table in &TABLES {
            let mut sqls: Vec<String> = STATEMENTS
                .iter()
                .flat_map(|template| MODES.iter().map(move |or| template.replace("<or>", or)))
                .collect();
            sqls.extend(extra_statements(table));
            for sql in &sqls {
                for file_backed in [false, true] {
                    run_case(table, sql, file_backed, &mut failures).await;
                    cases += 1;
                }
            }
        }
        assert!(
            failures.is_empty(),
            "{} of {cases} cases diverge from SQLite:\n{}",
            failures.len(),
            failures.join("\n")
        );
    });
}

fn stock_param(value: &SqliteValue) -> Value {
    match value {
        SqliteValue::Null => Value::Null,
        SqliteValue::Integer(v) => Value::Integer(*v),
        SqliteValue::Float(v) => Value::Real(*v),
        SqliteValue::Text(v) => Value::Text(v.to_string()),
        SqliteValue::Blob(v) => Value::Blob(v.to_vec()),
    }
}

/// Bound parameters reach the same gate, through a one-shot
/// `execute_with_params` and through one prepared statement reused across
/// executions.
#[test]
fn bound_rowid_parameters_pass_must_be_int() {
    asupersync::test_utils::run_test(|| async {
        let params: Vec<(&str, Vec<SqliteValue>)> = vec![
            (
                "INSERT INTO t(id, a, b) VALUES (?1, 1, ?2)",
                vec![SqliteValue::Float(201.0), "p1".into()],
            ),
            (
                "INSERT INTO t(id, a, b) VALUES (?1, 1, ?2)",
                vec!["202".into(), "p2".into()],
            ),
            (
                "INSERT INTO t(id, a, b) VALUES (?1, 1, ?2)",
                vec!["1e1".into(), "p3".into()],
            ),
            (
                "INSERT INTO t(id, a, b) VALUES (?1, 1, ?2)",
                vec![SqliteValue::Float(2.5), "p4".into()],
            ),
            (
                "INSERT INTO t(id, a, b) VALUES (?1, 1, ?2)",
                vec!["abc".into(), "p5".into()],
            ),
            (
                "INSERT INTO t(id, a, b) VALUES (?1, 1, ?2)",
                vec![SqliteValue::Null, "p6".into()],
            ),
            (
                "INSERT INTO t(id, a, b) VALUES (?1, 1, ?2)",
                vec![SqliteValue::Integer(300), "p7".into()],
            ),
            (
                "UPDATE t SET id = ?1 WHERE b = ?2",
                vec![SqliteValue::Float(400.0), "p7".into()],
            ),
            (
                "UPDATE t SET id = ?1 WHERE b = ?2",
                vec!["401".into(), "p1".into()],
            ),
            (
                "UPDATE t SET id = ?1 WHERE b = ?2",
                vec![SqliteValue::Float(0.5), "p2".into()],
            ),
            (
                "UPDATE t SET id = ?1 WHERE b = ?2",
                vec![SqliteValue::Null, "p3".into()],
            ),
        ];
        let mut failures = Vec::new();
        for file_backed in [false, true] {
            for prepared in [false, true] {
                let label = format!("file={file_backed} prepared={prepared}");
                let dir = tempfile::tempdir().expect("temp dir");
                let path = dir.path().join("frank.db");
                let frank = if file_backed {
                    Connection::open(path.to_str().expect("utf-8 path"))
                        .await
                        .expect("open")
                } else {
                    Connection::open(":memory:").await.expect("open")
                };
                let stock = rusqlite::Connection::open_in_memory().expect("stock open");
                let ddl = "CREATE TABLE t(id INTEGER PRIMARY KEY, a, b UNIQUE);";
                frank.execute_batch(ddl).await.expect("frank ddl");
                stock.execute_batch(ddl).expect("stock ddl");
                let insert = if prepared {
                    Some(frank.prepare(params[0].0).await.expect("prepare insert"))
                } else {
                    None
                };
                for (sql, values) in &params {
                    let frank_result = match (&insert, *sql == params[0].0) {
                        (Some(stmt), true) => stmt.execute_with_params(values).await,
                        _ => frank.execute_with_params(sql, values).await,
                    };
                    let stock_values: Vec<Value> = values.iter().map(stock_param).collect();
                    let stock_result =
                        stock.execute(sql, rusqlite::params_from_iter(stock_values.iter()));
                    if frank_result.is_ok() != stock_result.is_ok() {
                        failures.push(format!(
                            "{label} `{sql}` {values:?}: FrankenSQLite {frank_result:?} vs SQLite {stock_result:?}"
                        ));
                    }
                }
                drop(insert);
                let (f, s) = (frank_rows(&frank, CHECK).await, stock_rows(&stock, CHECK));
                if f != s {
                    failures.push(format!("{label} rows:\n  frank {f:?}\n  stock {s:?}"));
                }
                let own = frank_rows(&frank, "PRAGMA integrity_check").await;
                if own != [vec![SqliteValue::from("ok")]] {
                    failures.push(format!("{label} FrankenSQLite integrity_check: {own:?}"));
                }
                frank.close().await.expect("close");
                if file_backed {
                    let verdict = stock_integrity_check(&path);
                    if verdict != ["ok"] {
                        failures.push(format!("{label} stock integrity_check: {verdict:?}"));
                    }
                }
            }
        }
        assert!(failures.is_empty(), "{}", failures.join("\n"));
    });
}

/// The reported reproducer: one aggregate INSERT OR REPLACE ... SELECT into a
/// table with an INTEGER PRIMARY KEY and a UNIQUE column.
#[test]
fn aggregate_insert_or_replace_with_real_key_leaves_a_valid_file() {
    asupersync::test_utils::run_test(|| async {
        let dir = tempfile::tempdir().expect("temp dir");
        let path = dir.path().join("frank.db");
        let path_str = path.to_str().expect("utf-8 path").to_owned();
        let frank = Connection::open(&path_str).await.expect("open");
        frank
            .execute_batch(
                "CREATE TABLE t1 (id INTEGER PRIMARY KEY, a INT, b TEXT, c BLOB);
                 CREATE TABLE t3 (id INTEGER PRIMARY KEY, a, b UNIQUE);
                 INSERT OR REPLACE INTO t1 (id, a, b, c) VALUES (3, 1.5, 'stuvw', x'0102');
                 INSERT OR REPLACE INTO t3 (id, a, b)
                   SELECT 200 + abs(coalesce(a, 0)) % 50, count(*), 'agg-' || coalesce(a, 'n')
                   FROM t1 GROUP BY a;",
            )
            .await
            .expect("frank script");
        assert_eq!(
            frank_rows(&frank, "SELECT id, typeof(id), a, b FROM t3").await,
            vec![vec![
                SqliteValue::Integer(201),
                SqliteValue::from("integer"),
                SqliteValue::Integer(1),
                SqliteValue::from("agg-1.5"),
            ]]
        );
        frank.close().await.expect("close");
        assert_eq!(stock_integrity_check(&path), ["ok"]);
    });
}
