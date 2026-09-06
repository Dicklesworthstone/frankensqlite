//! GH #415 — `<rowid> IN (?1, ?2, …)` must plan as rowid seeks, not a full scan.
//!
//! `SELECT … FROM t WHERE id IN (1, 2, 3)` and `WHERE id = ?1` already seek the
//! INTEGER PRIMARY KEY, but the parameterized list `id IN (?1, ?2, ?3)` (and the
//! `rowid IN (?, …)` / `id = ?1 OR id = ?2` spellings) walked the whole table:
//! codegen's rowid IN-list detector only admitted integer literals. Stock
//! SQLite materializes the bind-time-constant list into an ephemeral index and
//! loops `SeekRowid` over it, so a 5-id lookup on a 12.9M-row table is five
//! seeks, not a scan.
//!
//! This file pins (a) the plan text for every spelling, (b) result parity with
//! real SQLite (via rusqlite) for NULL / duplicate / affinity / large-list
//! parameter sets, and (c) a perf-sanity check on the emitted bytecode: the
//! parameterized form must not `Rewind`/`Next` the table cursor.

use fsqlite::Connection;
use fsqlite_types::SqliteValue;

fn render_frank(v: &SqliteValue) -> String {
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

fn render_sqlite(v: rusqlite::types::Value) -> String {
    match v {
        rusqlite::types::Value::Null => "NULL".to_owned(),
        rusqlite::types::Value::Integer(x) => x.to_string(),
        rusqlite::types::Value::Real(f) => format!("{f}"),
        rusqlite::types::Value::Text(s) => format!("'{s}'"),
        rusqlite::types::Value::Blob(b) => format!(
            "X'{}'",
            b.iter().map(|x| format!("{x:02X}")).collect::<String>()
        ),
    }
}

fn to_rusqlite(v: &SqliteValue) -> rusqlite::types::Value {
    match v {
        SqliteValue::Null => rusqlite::types::Value::Null,
        SqliteValue::Integer(n) => rusqlite::types::Value::Integer(*n),
        SqliteValue::Float(f) => rusqlite::types::Value::Real(*f),
        SqliteValue::Text(s) => rusqlite::types::Value::Text(s.to_string()),
        SqliteValue::Blob(b) => rusqlite::types::Value::Blob(b.to_vec()),
    }
}

async fn frank_rows(
    conn: &Connection,
    sql: &str,
    params: &[SqliteValue],
) -> Result<Vec<Vec<String>>, String> {
    let rows = conn
        .query_with_params(sql, params)
        .await
        .map_err(|e| e.to_string())?;
    Ok(rows
        .iter()
        .map(|row| row.values().iter().map(render_frank).collect())
        .collect())
}

fn sqlite_rows(
    conn: &rusqlite::Connection,
    sql: &str,
    params: &[SqliteValue],
) -> Result<Vec<Vec<String>>, String> {
    let mut stmt = conn.prepare(sql).map_err(|e| e.to_string())?;
    let n = stmt.column_count();
    let bound: Vec<rusqlite::types::Value> = params.iter().map(to_rusqlite).collect();
    stmt.query_map(rusqlite::params_from_iter(bound.iter()), |row| {
        let mut out = Vec::with_capacity(n);
        for i in 0..n {
            let v: rusqlite::types::Value = row.get_unwrap(i);
            out.push(render_sqlite(v));
        }
        Ok(out)
    })
    .map_err(|e| e.to_string())?
    .collect::<Result<Vec<_>, _>>()
    .map_err(|e| e.to_string())
}

async fn setup(stmts: &[&str]) -> (Connection, rusqlite::Connection) {
    let f = Connection::open(":memory:").await.expect("open frank");
    let r = rusqlite::Connection::open_in_memory().expect("open rusqlite");
    for s in stmts {
        f.execute(s)
            .await
            .unwrap_or_else(|e| panic!("frank `{s}`: {e}"));
        r.execute_batch(s)
            .unwrap_or_else(|e| panic!("rusqlite `{s}`: {e}"));
    }
    (f, r)
}

/// `EXPLAIN QUERY PLAN` detail column(s) for `sql` (compile-only; nothing bound).
async fn eqp(conn: &Connection, sql: &str) -> Vec<String> {
    let rows = conn
        .query(&format!("EXPLAIN QUERY PLAN {sql}"))
        .await
        .unwrap_or_else(|e| panic!("EQP `{sql}`: {e}"));
    rows.iter()
        .map(|row| match row.values().get(3) {
            Some(SqliteValue::Text(s)) => s.to_string(),
            other => panic!("EQP `{sql}`: unexpected detail column {other:?}"),
        })
        .collect()
}

/// `(opcode, p1)` pairs of the compiled program for `sql`.
async fn explain_ops(conn: &Connection, sql: &str) -> Vec<(String, i64)> {
    let rows = conn
        .query(&format!("EXPLAIN {sql}"))
        .await
        .unwrap_or_else(|e| panic!("EXPLAIN `{sql}`: {e}"));
    rows.iter()
        .map(|row| {
            let opcode = match row.values().get(1) {
                Some(SqliteValue::Text(s)) => s.to_string(),
                other => panic!("EXPLAIN `{sql}`: unexpected opcode column {other:?}"),
            };
            let p1 = match row.values().get(2) {
                Some(SqliteValue::Integer(n)) => *n,
                other => panic!("EXPLAIN `{sql}`: unexpected p1 column {other:?}"),
            };
            (opcode, p1)
        })
        .collect()
}

const SCHEMA: &[&str] = &[
    "CREATE TABLE t (id INTEGER PRIMARY KEY, k INTEGER, v TEXT);",
    "INSERT INTO t VALUES (-5, 1, 'neg'), (1, 2, 'a'), (2, 3, 'b'), (3, 4, 'c'), (5, 6, 'e'), (100, 7, 'f');",
];

const ROWID_SEEK: &str = "SEARCH t USING INTEGER PRIMARY KEY (rowid=?)";

/// Every bind-time-constant spelling of a rowid membership test plans as a
/// rowid seek — the same text the literal list and the single equality get.
#[test]
fn gh415_parameterized_rowid_in_plans_as_rowid_seek() {
    asupersync::test_utils::run_test(|| async {
        let (f, _r) = setup(SCHEMA).await;

        // Reference shapes that already seeked before the fix.
        assert_eq!(eqp(&f, "SELECT v FROM t WHERE id = ?1").await, [ROWID_SEEK]);
        assert_eq!(
            eqp(&f, "SELECT v FROM t WHERE id IN (1, 2, 3)").await,
            [ROWID_SEEK]
        );

        // GH #415: parameterized / mixed / OR-chain spellings.
        for sql in [
            "SELECT v FROM t WHERE id IN (?1, ?2, ?3)",
            "SELECT v FROM t WHERE id IN (?, ?, ?)",
            "SELECT v FROM t WHERE id IN (?1)",
            "SELECT v FROM t WHERE rowid IN (?1, ?2)",
            "SELECT v FROM t WHERE t.id IN (:a, :b)",
            "SELECT v FROM t WHERE id IN (?1, 5, abs(?2), -3, '7', 2.0)",
            "SELECT v FROM t WHERE id = ?1 OR id = ?2",
            "SELECT v FROM t WHERE id = ?1 OR ?2 = id OR rowid = 5",
            "SELECT v FROM t WHERE id IN (?1, ?2) AND v <> 'zz'",
            "SELECT v FROM t WHERE v <> ? AND id IN (?, ?)",
            "SELECT v FROM t WHERE id IN (?1, ?2) ORDER BY id",
            "SELECT v FROM t WHERE id IN (?1, ?2) ORDER BY id DESC",
            "SELECT COUNT(*) FROM t WHERE id IN (?1, ?2)",
            "SELECT COUNT(*) FROM t WHERE id IN (?1, ?2) AND k > ?3",
        ] {
            assert_eq!(eqp(&f, sql).await, [ROWID_SEEK], "plan for `{sql}`");
        }

        // Shapes that must still scan: NOT IN, a list member that is not a
        // bind-time constant, and a plain (non-rowid) column.
        for sql in [
            "SELECT v FROM t WHERE id NOT IN (?1, ?2)",
            "SELECT v FROM t WHERE id IN (?1, k)",
            "SELECT v FROM t WHERE k IN (?1, ?2)",
        ] {
            assert_eq!(eqp(&f, sql).await, ["SCAN t"], "plan for `{sql}`");
        }
    });
}

/// Perf sanity (not a benchmark): the parameterized form must not walk the
/// table b-tree. The table is cursor 0; no `Rewind`/`Next`/`Last`/`Prev` may
/// target it, and at least one `SeekRowid` must.
#[test]
fn gh415_parameterized_rowid_in_bytecode_never_walks_the_table() {
    asupersync::test_utils::run_test(|| async {
        let (f, _r) = setup(SCHEMA).await;
        for sql in [
            "SELECT v FROM t WHERE id IN (?1, ?2, ?3)",
            "SELECT v FROM t WHERE rowid IN (?1, ?2)",
            "SELECT v FROM t WHERE id = ?1 OR id = ?2",
            "SELECT v FROM t WHERE id IN (?1, ?2) AND v <> 'zz'",
            "SELECT v FROM t WHERE id IN (?1, ?2) ORDER BY id DESC",
            "SELECT COUNT(*) FROM t WHERE id IN (?1, ?2)",
            "UPDATE t SET v = ?1 WHERE id IN (?2, ?3)",
            "DELETE FROM t WHERE id IN (?1, ?2)",
        ] {
            let ops = explain_ops(&f, sql).await;
            let table_cursor = ops
                .iter()
                .find(|(op, _)| op == "OpenRead" || op == "OpenWrite")
                .map(|(_, p1)| *p1)
                .unwrap_or_else(|| panic!("`{sql}`: no table cursor opened\n{ops:?}"));
            let walks = ops
                .iter()
                .filter(|(op, p1)| {
                    *p1 == table_cursor
                        && matches!(op.as_str(), "Rewind" | "Next" | "Last" | "Prev")
                })
                .count();
            assert_eq!(walks, 0, "`{sql}` walks the table cursor\n{ops:?}");
            let seeks = ops
                .iter()
                .filter(|(op, p1)| *p1 == table_cursor && op == "SeekRowid")
                .count();
            assert!(
                seeks >= 1,
                "`{sql}` never seeks the table by rowid\n{ops:?}"
            );
        }
    });
}

fn int(n: i64) -> SqliteValue {
    SqliteValue::Integer(n)
}

fn text(s: &str) -> SqliteValue {
    SqliteValue::Text(s.into())
}

/// Result parity with real SQLite across the semantic edges the seek path
/// must preserve: NULL members, duplicates, mixed/loose affinity, negatives,
/// misses, residual conjuncts, ORDER BY, and DML by parameterized list.
#[test]
fn gh415_parameterized_rowid_in_matches_sqlite() {
    asupersync::test_utils::run_test(|| async {
        let (f, r) = setup(SCHEMA).await;

        let sorted = |res: Result<Vec<Vec<String>>, String>| -> Result<Vec<Vec<String>>, String> {
            res.map(|mut rows| {
                rows.sort();
                rows
            })
        };

        // (sql, params). No ORDER BY -> compare as sets.
        let set_cases: Vec<(&str, Vec<SqliteValue>)> = vec![
            (
                "SELECT id, v FROM t WHERE id IN (?1, ?2, ?3)",
                vec![int(3), int(1), int(3)],
            ),
            (
                "SELECT id, v FROM t WHERE id IN (?1, ?2)",
                vec![SqliteValue::Null, int(3)],
            ),
            (
                "SELECT id, v FROM t WHERE id IN (?1)",
                vec![SqliteValue::Null],
            ),
            ("SELECT id, v FROM t WHERE id IN (?1)", vec![text("3")]),
            ("SELECT id, v FROM t WHERE id IN (?1)", vec![text(" 3")]),
            ("SELECT id, v FROM t WHERE id IN (?1)", vec![text("3 ")]),
            ("SELECT id, v FROM t WHERE id IN (?1)", vec![text("3abc")]),
            ("SELECT id, v FROM t WHERE id IN (?1)", vec![text("0x3")]),
            ("SELECT id, v FROM t WHERE id IN (?1)", vec![text("3.0")]),
            ("SELECT id, v FROM t WHERE id IN (?1)", vec![text("3e0")]),
            ("SELECT id, v FROM t WHERE id IN (?1)", vec![text("")]),
            (
                "SELECT id, v FROM t WHERE id IN (?1)",
                vec![SqliteValue::Float(2.5)],
            ),
            (
                "SELECT id, v FROM t WHERE id IN (?1)",
                vec![SqliteValue::Float(3.0)],
            ),
            (
                "SELECT id, v FROM t WHERE id IN (?1)",
                vec![SqliteValue::Float(-5.0)],
            ),
            (
                "SELECT id, v FROM t WHERE id IN (?1)",
                vec![SqliteValue::Blob(vec![3].into())],
            ),
            (
                "SELECT id, v FROM t WHERE id IN (?1, ?2)",
                vec![int(i64::MIN), int(i64::MAX)],
            ),
            (
                "SELECT id, v FROM t WHERE id IN (?1, ?2, ?3)",
                vec![int(-5), int(999), int(100)],
            ),
            (
                "SELECT id, v FROM t WHERE id IN (?1, ?2)",
                vec![int(999), int(1000)],
            ),
            (
                "SELECT id, v FROM t WHERE rowid IN (?1, ?2)",
                vec![int(5), text("2")],
            ),
            (
                "SELECT id, v FROM t WHERE id IN (?1, 5, abs(?2), -5, '1', 2.0)",
                vec![int(100), int(-3)],
            ),
            (
                "SELECT id, v FROM t WHERE id = ?1 OR id = ?2",
                vec![int(2), int(2)],
            ),
            (
                "SELECT id, v FROM t WHERE id = ?1 OR ?2 = id OR rowid = 5",
                vec![text("1"), SqliteValue::Null],
            ),
            (
                "SELECT id, v FROM t WHERE id IN (?1, ?2) AND v <> ?3",
                vec![int(1), int(2), text("b")],
            ),
            (
                "SELECT id, v FROM t WHERE v <> ? AND id IN (?, ?)",
                vec![text("a"), int(1), int(2)],
            ),
            (
                "SELECT id, v FROM t WHERE id IN (?, ?) AND k = ?",
                vec![int(1), int(2), int(3)],
            ),
            (
                "SELECT COUNT(*) FROM t WHERE id IN (?1, ?2, ?3)",
                vec![int(3), int(3), text("1")],
            ),
            (
                "SELECT COUNT(*) FROM t WHERE id IN (?1, ?2) AND k > ?3",
                vec![int(1), int(2), int(2)],
            ),
            (
                "SELECT COUNT(*) FROM t WHERE id IN (?1, ?2)",
                vec![SqliteValue::Null, SqliteValue::Float(2.5)],
            ),
        ];
        for (sql, params) in &set_cases {
            assert_eq!(
                sorted(frank_rows(&f, sql, params).await),
                sorted(sqlite_rows(&r, sql, params)),
                "row set diverged from SQLite for `{sql}` with {params:?}"
            );
        }

        // ORDER BY -> exact sequence.
        let exact_cases: Vec<(&str, Vec<SqliteValue>)> = vec![
            (
                "SELECT id FROM t WHERE id IN (?1, ?2, ?3) ORDER BY id",
                vec![int(5), int(1), int(3)],
            ),
            (
                "SELECT id FROM t WHERE id IN (?1, ?2, ?3) ORDER BY id DESC",
                vec![int(5), int(1), int(3)],
            ),
            (
                "SELECT id FROM t WHERE id IN (?1, ?2, ?3, ?4) ORDER BY id DESC",
                vec![int(5), SqliteValue::Null, text("1"), int(5)],
            ),
            (
                "SELECT id FROM t WHERE id IN (?1, ?2) ORDER BY id LIMIT 1",
                vec![int(5), int(1)],
            ),
            (
                "SELECT id FROM t WHERE id NOT IN (?1, ?2) ORDER BY id",
                vec![int(5), int(1)],
            ),
            (
                "SELECT id FROM t WHERE id IN (?1, ?2) ORDER BY v",
                vec![int(5), int(1)],
            ),
        ];
        for (sql, params) in &exact_cases {
            assert_eq!(
                frank_rows(&f, sql, params).await,
                sqlite_rows(&r, sql, params),
                "ordered rows diverged from SQLite for `{sql}` with {params:?}"
            );
        }

        // Re-executing the same statement with different bindings must not
        // reuse a stale value set (prepared-statement cache).
        for params in [
            vec![int(1), int(2)],
            vec![int(3), int(100)],
            vec![SqliteValue::Null, SqliteValue::Null],
            vec![int(2), int(1)],
        ] {
            let sql = "SELECT id FROM t WHERE id IN (?1, ?2) ORDER BY id";
            assert_eq!(
                frank_rows(&f, sql, &params).await,
                sqlite_rows(&r, sql, &params),
                "re-bound rows diverged from SQLite for `{sql}` with {params:?}"
            );
        }

        // DML by parameterized rowid list.
        let dml: Vec<(&str, Vec<SqliteValue>)> = vec![
            (
                "UPDATE t SET v = ?1 WHERE id IN (?2, ?3, ?4)",
                vec![text("upd"), int(1), text("2"), SqliteValue::Null],
            ),
            (
                "UPDATE t SET v = ? WHERE k > ? AND id IN (?, ?)",
                vec![text("upd2"), int(2), int(2), int(3)],
            ),
            (
                "DELETE FROM t WHERE id IN (?1, ?2, ?3)",
                vec![int(5), int(5), SqliteValue::Float(100.0)],
            ),
            (
                "DELETE FROM t WHERE id = ?1 OR id = ?2",
                vec![int(-5), int(999)],
            ),
            (
                "DELETE FROM t WHERE id IN (?1, ?2) AND v = ?3",
                vec![int(1), int(2), text("upd")],
            ),
        ];
        for (sql, params) in &dml {
            let frank_changed = f
                .execute_with_params(sql, params)
                .await
                .unwrap_or_else(|e| panic!("frank `{sql}`: {e}"));
            let bound: Vec<rusqlite::types::Value> = params.iter().map(to_rusqlite).collect();
            let sqlite_changed = r
                .execute(sql, rusqlite::params_from_iter(bound.iter()))
                .unwrap_or_else(|e| panic!("rusqlite `{sql}`: {e}"));
            assert_eq!(
                frank_changed, sqlite_changed,
                "changed-row count diverged for `{sql}` with {params:?}"
            );
            let snapshot = "SELECT id, k, v FROM t ORDER BY id";
            assert_eq!(
                frank_rows(&f, snapshot, &[]).await,
                sqlite_rows(&r, snapshot, &[]),
                "table state diverged after `{sql}` with {params:?}"
            );
        }
    });
}

/// Hundreds of parameters (well past any straight-line unrolling budget)
/// stay correct, deduplicate, and still plan as a rowid seek.
#[test]
fn gh415_large_parameter_list_matches_sqlite() {
    asupersync::test_utils::run_test(|| async {
        let mut schema = vec!["CREATE TABLE big (id INTEGER PRIMARY KEY, v TEXT);".to_owned()];
        for i in 0..2000_i64 {
            schema.push(format!("INSERT INTO big VALUES ({i}, 'v{i}');"));
        }
        let schema_refs: Vec<&str> = schema.iter().map(String::as_str).collect();
        let (f, r) = setup(&schema_refs).await;

        let n = 600_usize;
        let placeholders = (1..=n)
            .map(|i| format!("?{i}"))
            .collect::<Vec<_>>()
            .join(", ");
        let sql = format!("SELECT id, v FROM big WHERE id IN ({placeholders}) ORDER BY id");
        // Every third value is a duplicate, every tenth misses, a few NULLs.
        let params: Vec<SqliteValue> = (0..n)
            .map(|i| {
                if i % 10 == 9 {
                    int(5000 + i as i64)
                } else if i % 37 == 0 {
                    SqliteValue::Null
                } else if i % 3 == 0 {
                    int((i as i64 * 7) % 2000)
                } else {
                    int((i as i64 * 13) % 2000)
                }
            })
            .collect();

        assert_eq!(
            eqp(&f, &sql).await,
            ["SEARCH big USING INTEGER PRIMARY KEY (rowid=?)"],
            "large list plan"
        );
        let frank = frank_rows(&f, &sql, &params).await;
        let sqlite = sqlite_rows(&r, &sql, &params);
        assert_eq!(frank, sqlite, "large parameterized IN diverged from SQLite");
        let rows = frank.expect("frank rows");
        assert!(rows.len() > 100, "expected many hits, got {}", rows.len());
        let mut ids: Vec<&String> = rows.iter().map(|row| &row[0]).collect();
        let before = ids.len();
        ids.dedup();
        assert_eq!(
            before,
            ids.len(),
            "duplicate parameters must not duplicate rows"
        );
    });
}
