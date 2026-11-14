//! bd-d2jwe: `x IN (SELECT ...)` builds its probe set by walking only the subquery rows its WHERE
//! can reach: a safe rowid range (`WHERE pid < 100`, stock's `SEARCH p USING INTEGER PRIMARY KEY
//! (rowid<?)`) or an equality prefix of an index on the subquery's table (`WHERE org = ?1`), instead
//! of reading the whole table. The WHERE still filters every subquery row. Rows match rusqlite as sets
//! (the outer seek order follows the probe set), in memory and on a file database in strict mode.

use fsqlite::Connection;
use fsqlite_types::SqliteValue;

fn render(v: &SqliteValue) -> String {
    match v {
        SqliteValue::Null => "NULL".to_owned(),
        SqliteValue::Integer(n) => n.to_string(),
        SqliteValue::Float(f) => format!("{f:?}"),
        SqliteValue::Text(s) => format!("'{s}'"),
        SqliteValue::Blob(b) => format!(
            "X'{}'",
            b.iter().map(|x| format!("{x:02X}")).collect::<String>()
        ),
    }
}

fn to_rusqlite(value: &SqliteValue) -> rusqlite::types::Value {
    match value {
        SqliteValue::Null => rusqlite::types::Value::Null,
        SqliteValue::Integer(n) => rusqlite::types::Value::Integer(*n),
        SqliteValue::Float(f) => rusqlite::types::Value::Real(*f),
        SqliteValue::Text(s) => rusqlite::types::Value::Text(s.to_string()),
        SqliteValue::Blob(b) => rusqlite::types::Value::Blob(b.to_vec()),
    }
}

async fn frank_rows(c: &Connection, sql: &str, params: &[SqliteValue]) -> Vec<Vec<String>> {
    let mut rows: Vec<Vec<String>> = c
        .query_with_params(sql, params)
        .await
        .unwrap_or_else(|e| panic!("frank `{sql}` {params:?}: {e}"))
        .iter()
        .map(|row| row.values().iter().map(render).collect())
        .collect();
    rows.sort();
    rows
}

fn sqlite_rows(c: &rusqlite::Connection, sql: &str, params: &[SqliteValue]) -> Vec<Vec<String>> {
    let mut stmt = c.prepare(sql).unwrap();
    let n = stmt.column_count();
    let mut rows: Vec<Vec<String>> = stmt
        .query_map(
            rusqlite::params_from_iter(params.iter().map(to_rusqlite)),
            |row| {
                Ok((0..n)
                    .map(|i| match row.get_unwrap::<_, rusqlite::types::Value>(i) {
                        rusqlite::types::Value::Null => "NULL".to_owned(),
                        rusqlite::types::Value::Integer(x) => x.to_string(),
                        rusqlite::types::Value::Real(f) => format!("{f:?}"),
                        rusqlite::types::Value::Text(s) => format!("'{s}'"),
                        rusqlite::types::Value::Blob(b) => format!(
                            "X'{}'",
                            b.iter().map(|x| format!("{x:02X}")).collect::<String>()
                        ),
                    })
                    .collect())
            },
        )
        .unwrap()
        .map(Result::unwrap)
        .collect();
    rows.sort();
    rows
}

/// One EXPLAIN row: address, opcode, p1, p2, p4.
#[derive(Debug)]
struct Op {
    addr: i64,
    opcode: String,
    p1: i64,
    p2: i64,
    p4: String,
}

async fn program(c: &Connection, sql: &str) -> Vec<Op> {
    c.query(&format!("EXPLAIN {sql}"))
        .await
        .unwrap_or_else(|e| panic!("EXPLAIN `{sql}`: {e}"))
        .iter()
        .map(|row| {
            let values = row.values();
            let text = |i: usize| match values.get(i) {
                Some(SqliteValue::Text(s)) => s.to_string(),
                _ => String::new(),
            };
            let int = |i: usize| match values.get(i) {
                Some(SqliteValue::Integer(n)) => *n,
                _ => -1,
            };
            Op {
                addr: int(0),
                opcode: text(1),
                p1: int(2),
                p2: int(3),
                p4: text(5),
            }
        })
        .collect()
}

/// Whether the build walks only part of p: it seeks the walk's start on p's
/// cursor, seeks an index on p, or leaves the walk early (a comparison that
/// jumps past the walk's `Next`, as an upper rowid bound does; the WHERE
/// filter's comparisons jump to the `Next` itself).
fn source_walk_is_bounded(ops: &[Op]) -> bool {
    let Some(source) = ops
        .iter()
        .find(|op| op.opcode == "OpenRead" && op.p4 == "p")
        .map(|op| op.p1)
    else {
        return false;
    };
    let seeks_start = ops
        .iter()
        .any(|op| matches!(op.opcode.as_str(), "SeekGE" | "SeekGT") && op.p1 == source);
    let seeks_index = ops
        .iter()
        .any(|op| op.opcode == "OpenRead" && op.p4.starts_with("p_"));
    let leaves_early = ops
        .iter()
        .find(|op| op.opcode == "Next" && op.p1 == source)
        .is_some_and(|next| {
            ops.iter()
                .any(|op| matches!(op.opcode.as_str(), "Gt" | "Ge") && op.p2 == next.addr + 1)
        });
    seeks_start || seeks_index || leaves_early
}

#[test]
fn in_subquery_probe_set_walks_only_the_rows_its_where_reaches() {
    asupersync::test_utils::run_test(|| async {
        let int = SqliteValue::Integer;
        let text = |s: &str| SqliteValue::Text(s.into());
        let mut setup = vec![
            "CREATE TABLE t (id INTEGER PRIMARY KEY, a INTEGER, s TEXT);".to_owned(),
            "CREATE INDEX t_a ON t(a);".to_owned(),
            "CREATE INDEX t_s ON t(s);".to_owned(),
            "CREATE TABLE p (pid INTEGER PRIMARY KEY, w INTEGER, org INTEGER, tag TEXT, \
             nc TEXT COLLATE NOCASE, u INTEGER);"
                .to_owned(),
            "CREATE INDEX p_org ON p(org);".to_owned(),
            "CREATE INDEX p_tag ON p(tag);".to_owned(),
            "CREATE INDEX p_nc ON p(nc);".to_owned(),
        ];
        for i in 1..=2000_i64 {
            setup.push(format!(
                "INSERT INTO t VALUES ({i}, {}, 'x{}');",
                i % 40,
                i % 30
            ));
        }
        for i in 1..=3000_i64 {
            // NULL and duplicate members; text in the INTEGER column.
            let w = if i % 17 == 0 {
                "NULL".to_owned()
            } else if i % 41 == 0 {
                "'abc'".to_owned()
            } else {
                format!("{}", i % 13)
            };
            let nc = if i % 2 == 0 { "X" } else { "x" };
            setup.push(format!(
                "INSERT INTO p VALUES ({i}, {w}, {}, 'x{}', '{nc}{}', {});",
                i % 50,
                i % 30,
                i % 7,
                i % 11
            ));
        }
        // (sql, params, the build seeks its source)
        let cases: Vec<(&str, Vec<SqliteValue>, bool)> = vec![
            (
                "SELECT id FROM t WHERE a IN (SELECT w FROM p WHERE pid < 100)",
                vec![],
                true,
            ),
            (
                "SELECT id FROM t WHERE a IN (SELECT w FROM p WHERE pid <= 100)",
                vec![],
                true,
            ),
            (
                "SELECT id FROM t WHERE a IN (SELECT w FROM p WHERE pid > 2900)",
                vec![],
                true,
            ),
            (
                "SELECT id FROM t WHERE a IN (SELECT w FROM p WHERE pid >= 2990)",
                vec![],
                true,
            ),
            (
                "SELECT id FROM t WHERE a IN (SELECT w FROM p WHERE pid BETWEEN 50 AND 60)",
                vec![],
                true,
            ),
            (
                "SELECT id FROM t WHERE a IN (SELECT w FROM p WHERE pid > 10 AND pid < 20)",
                vec![],
                true,
            ),
            (
                "SELECT id FROM t WHERE a IN (SELECT w FROM p WHERE pid < 0)",
                vec![],
                true,
            ),
            (
                // A NULL literal is no seek bound: the scan stays (and finds nothing).
                "SELECT id FROM t WHERE a IN (SELECT w FROM p WHERE pid < NULL)",
                vec![],
                false,
            ),
            (
                "SELECT id FROM t WHERE a IN (SELECT w FROM p WHERE pid < ?1)",
                vec![int(120)],
                true,
            ),
            (
                "SELECT id FROM t WHERE a IN (SELECT w FROM p WHERE pid < ?1)",
                vec![SqliteValue::Null],
                true,
            ),
            // A lower bound bound to text or a real: the seek must not start
            // past a row the comparison accepts.
            (
                "SELECT id FROM t WHERE a IN (SELECT w FROM p WHERE pid > ?1)",
                vec![text("2950")],
                true,
            ),
            (
                "SELECT id FROM t WHERE a IN (SELECT w FROM p WHERE pid > ?1)",
                vec![SqliteValue::Float(2950.5)],
                true,
            ),
            (
                "SELECT id FROM t WHERE a IN (SELECT w FROM p WHERE pid >= ?1)",
                vec![text("abc")],
                true,
            ),
            (
                "SELECT id FROM t WHERE a IN (SELECT w FROM p WHERE org = 7)",
                vec![],
                true,
            ),
            (
                "SELECT id FROM t WHERE a IN (SELECT w FROM p WHERE org = '7')",
                vec![],
                true,
            ),
            (
                "SELECT id FROM t WHERE a IN (SELECT w FROM p WHERE org = NULL)",
                vec![],
                true,
            ),
            (
                "SELECT id FROM t WHERE a IN (SELECT w FROM p WHERE org = 7 AND u > 4)",
                vec![],
                true,
            ),
            (
                "SELECT id FROM t WHERE a IN (SELECT w FROM p WHERE tag = 'x3')",
                vec![],
                true,
            ),
            (
                "SELECT id FROM t WHERE a IN (SELECT w FROM p WHERE nc = 'X3')",
                vec![],
                true,
            ),
            (
                "SELECT id FROM t WHERE a IN (SELECT w FROM p WHERE org = ?1)",
                vec![int(9)],
                true,
            ),
            (
                "SELECT id FROM t WHERE a IN (SELECT w FROM p WHERE org = ?1)",
                vec![text("9")],
                true,
            ),
            (
                "SELECT id FROM t WHERE a IN (SELECT pid FROM p WHERE org = 7)",
                vec![],
                true,
            ),
            (
                "SELECT id, s FROM t WHERE s IN (SELECT tag FROM p WHERE org = 3)",
                vec![],
                true,
            ),
            // An unindexed predicate keeps the scan of p.
            (
                "SELECT id FROM t WHERE a IN (SELECT w FROM p WHERE u = 3)",
                vec![],
                false,
            ),
        ];
        // NOT IN and IN under other predicates take the filter form, whose
        // probe set is built once with the same walk.
        let rows_only: Vec<&str> = vec![
            "SELECT count(*) FROM t WHERE a NOT IN (SELECT w FROM p WHERE pid < 100)",
            "SELECT count(*) FROM t WHERE a NOT IN (SELECT w FROM p WHERE pid < 300 AND w IS NOT NULL)",
            "SELECT id FROM t WHERE id < 50 AND a IN (SELECT w FROM p WHERE pid > 2950)",
            "SELECT id FROM t WHERE a IN (SELECT w FROM p WHERE pid < 100) OR s = 'x7'",
        ];
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("in_subquery_source_access.db");
        for path in [":memory:", file.to_str().unwrap()] {
            let f = Connection::open(path).await.unwrap();
            let r = rusqlite::Connection::open_in_memory().unwrap();
            for sql in &setup {
                f.execute(sql).await.unwrap();
                r.execute_batch(sql).unwrap();
            }
            f.execute("PRAGMA fsqlite.parity_cert_strict = ON;")
                .await
                .unwrap();
            for (sql, params, seeks) in &cases {
                assert_eq!(
                    frank_rows(&f, sql, params).await,
                    sqlite_rows(&r, sql, params),
                    "`{sql}` {params:?} at {path}"
                );
                let ops = program(&f, sql).await;
                assert_eq!(
                    source_walk_is_bounded(&ops),
                    *seeks,
                    "`{sql}`: the build must {} p: {ops:?}",
                    if *seeks {
                        "walk only part of"
                    } else {
                        "scan all of"
                    }
                );
            }
            for sql in &rows_only {
                assert_eq!(
                    frank_rows(&f, sql, &[]).await,
                    sqlite_rows(&r, sql, &[]),
                    "`{sql}` at {path}"
                );
            }
            f.close().await.unwrap();
        }
    });
}
