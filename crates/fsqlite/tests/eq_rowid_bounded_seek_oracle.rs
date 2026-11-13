//! bd-z3a05: `SELECT ... FROM t WHERE <indexed col> = <literal> AND <rowid range>` — keyset
//! pagination (`a = 5 AND id > 100 ORDER BY id LIMIT 10`) — seeks the `a = 5` block of a
//! single-column index bounded by the rowid range, as stock's
//! `SEARCH t USING INDEX t_a (a=? AND rowid>?)` does, instead of walking the rowid slice (usually
//! most of the table) and filtering on `a`. The block is in rowid order, so the seek also serves
//! `ORDER BY <rowid>` ASC and DESC. Every case compares rows in the order each engine returns them
//! against C SQLite; the plan check proves the index is opened for the bounded shapes, and a
//! composite-only table proves the lane declines when its entries are not in rowid order.
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

async fn frank_rows(c: &Connection, sql: &str) -> Vec<Vec<String>> {
    c.query(sql)
        .await
        .unwrap_or_else(|e| panic!("frank `{sql}`: {e}"))
        .iter()
        .map(|row| row.values().iter().map(render).collect())
        .collect()
}

fn sqlite_rows(c: &rusqlite::Connection, sql: &str) -> Vec<Vec<String>> {
    let mut stmt = c.prepare(sql).unwrap();
    let n = stmt.column_count();
    stmt.query_map([], |row| {
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
    })
    .unwrap()
    .map(Result::unwrap)
    .collect()
}

/// Whether the program opens the index `index` (a read cursor on it).
async fn opens_index(c: &Connection, sql: &str, index: &str) -> bool {
    c.query(&format!("EXPLAIN {sql}"))
        .await
        .unwrap()
        .iter()
        .any(|row| {
            let values = row.values();
            matches!(values.get(1), Some(SqliteValue::Text(op)) if op.to_string() == "OpenRead")
                && values.iter().any(|value| {
                    matches!(value, SqliteValue::Text(text)
                        if text.to_string().eq_ignore_ascii_case(index))
                })
        })
}

async fn setup() -> (Connection, rusqlite::Connection) {
    let f = Connection::open(":memory:").await.unwrap();
    let r = rusqlite::Connection::open_in_memory().unwrap();
    let mut ddl = vec![
        "CREATE TABLE t (id INTEGER PRIMARY KEY, a INTEGER, c INTEGER, x TEXT);".to_owned(),
        "CREATE INDEX t_a ON t(a);".to_owned(),
        "CREATE INDEX t_x ON t(x);".to_owned(),
        // Only a composite index on (a, c): its `a = 5` entries are ordered by c, not rowid.
        "CREATE TABLE u (id INTEGER PRIMARY KEY, a INTEGER, c INTEGER);".to_owned(),
        "CREATE INDEX u_ac ON u(a, c);".to_owned(),
    ];
    for i in 1..=1500_i64 {
        let a = if i % 37 == 0 {
            "NULL".to_owned()
        } else {
            format!("{}", i % 20)
        };
        ddl.push(format!(
            "INSERT INTO t VALUES ({i}, {a}, {}, 'v{}');",
            i % 12,
            i % 7
        ));
        ddl.push(format!(
            "INSERT INTO u VALUES ({i}, {a}, {});",
            (1500 - i) % 9
        ));
    }
    for statement in &ddl {
        f.execute(statement).await.unwrap();
        r.execute_batch(statement).unwrap();
    }
    (f, r)
}

#[test]
fn eq_rowid_bounded_seek_matches_sqlite_in_order() {
    asupersync::test_utils::run_test(|| async {
        let (f, r) = setup().await;
        // Bounded shapes on a single-column index: the lane must take the index.
        let bounded = [
            "SELECT id, c FROM t WHERE a = 5 AND id > 100 ORDER BY id LIMIT 10",
            "SELECT id, c FROM t WHERE a = 5 AND id > 100 ORDER BY id DESC LIMIT 10",
            "SELECT id FROM t WHERE a = 5 AND id >= 100 AND id < 400 ORDER BY id",
            "SELECT id FROM t WHERE a = 5 AND id > 100 AND id <= 400 ORDER BY id DESC",
            "SELECT id FROM t WHERE a = 5 AND id BETWEEN 50 AND 300 ORDER BY id DESC",
            "SELECT id FROM t WHERE a = 5 AND id BETWEEN 300 AND 50 ORDER BY id",
            "SELECT id, c FROM t WHERE a = 5 AND id > 100 ORDER BY id LIMIT 5 OFFSET 3",
            "SELECT id, c FROM t WHERE a = 5 AND id < 900 ORDER BY id DESC LIMIT 4 OFFSET 2",
            "SELECT id, x FROM t WHERE x = 'v3' AND id < 200 ORDER BY id DESC LIMIT 7",
            "SELECT id, x FROM t WHERE x = 'v3' AND id >= 1400 ORDER BY id",
            "SELECT id FROM t WHERE a = 5 AND id > 100 AND c = 3 ORDER BY id",
            "SELECT id FROM t WHERE a = 99 AND id > 0 ORDER BY id",
            "SELECT id FROM t WHERE a = 5 AND id > 100000 ORDER BY id",
            "SELECT id FROM t WHERE a = 5 AND id < 0 ORDER BY id DESC",
            "SELECT id FROM t WHERE a = 5 AND rowid > 700 ORDER BY rowid LIMIT 3",
            "SELECT id FROM t WHERE id > 1000 AND a = 7 ORDER BY id",
            "SELECT * FROM t WHERE a = 0 AND id > 1460 ORDER BY id",
        ];
        for sql in bounded {
            assert_eq!(frank_rows(&f, sql).await, sqlite_rows(&r, sql), "`{sql}`");
            assert!(
                opens_index(&f, sql, "t_a").await || opens_index(&f, sql, "t_x").await,
                "bd-z3a05: `{sql}` must seek the equality index, not walk the rowid slice"
            );
        }
        // Shapes outside the lane must stay correct: a composite index (entries not in rowid
        // order within `a`), a bound that is not an integer literal, no rowid bound at all.
        let other = [
            "SELECT id, c FROM u WHERE a = 5 AND id > 100 ORDER BY id LIMIT 10",
            "SELECT id, c FROM u WHERE a = 5 AND id > 100 ORDER BY id DESC LIMIT 10",
            "SELECT id FROM t WHERE a = 5 AND id > -5 ORDER BY id LIMIT 6",
            "SELECT id FROM t WHERE a = 5 AND id > 10.5 ORDER BY id LIMIT 6",
            "SELECT id FROM t WHERE a = 5 ORDER BY id LIMIT 6",
            "SELECT id FROM t WHERE a = '5' AND id > 100 ORDER BY id LIMIT 6",
            "SELECT id FROM t WHERE a IS NULL AND id > 100 ORDER BY id LIMIT 6",
        ];
        for sql in other {
            assert_eq!(frank_rows(&f, sql).await, sqlite_rows(&r, sql), "`{sql}`");
        }
    });
}

async fn frank_rows_with(c: &Connection, sql: &str, params: &[SqliteValue]) -> Vec<Vec<String>> {
    c.query_with_params(sql, params)
        .await
        .unwrap_or_else(|e| panic!("frank `{sql}` {params:?}: {e}"))
        .iter()
        .map(|row| row.values().iter().map(render).collect())
        .collect()
}

fn sqlite_rows_with(
    c: &rusqlite::Connection,
    sql: &str,
    params: &[SqliteValue],
) -> Vec<Vec<String>> {
    let values: Vec<rusqlite::types::Value> = params
        .iter()
        .map(|value| match value {
            SqliteValue::Null => rusqlite::types::Value::Null,
            SqliteValue::Integer(n) => rusqlite::types::Value::Integer(*n),
            SqliteValue::Float(f) => rusqlite::types::Value::Real(*f),
            SqliteValue::Text(s) => rusqlite::types::Value::Text(s.to_string()),
            SqliteValue::Blob(b) => rusqlite::types::Value::Blob(b.to_vec()),
        })
        .collect();
    let mut stmt = c.prepare(sql).unwrap();
    let n = stmt.column_count();
    stmt.query_map(rusqlite::params_from_iter(values), |row| {
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
    })
    .unwrap()
    .map(Result::unwrap)
    .collect()
}

/// The program's `(opcode, p2)` pairs in address order.
async fn explain_ops(c: &Connection, sql: &str) -> Vec<(String, i64)> {
    c.query(&format!("EXPLAIN {sql}"))
        .await
        .unwrap()
        .iter()
        .map(|row| {
            let values = row.values();
            let opcode = match values.get(1) {
                Some(SqliteValue::Text(op)) => op.to_string(),
                _ => String::new(),
            };
            let p2 = match values.get(3) {
                Some(SqliteValue::Integer(n)) => *n,
                _ => -1,
            };
            (opcode, p2)
        })
        .collect()
}

/// The field count of the index seek's probe record: the last `MakeRecord`
/// before the first `SeekGE`/`SeekLE`.
fn seek_probe_fields(ops: &[(String, i64)]) -> Option<i64> {
    let seek = ops
        .iter()
        .position(|(op, _)| op == "SeekGE" || op == "SeekLE")?;
    ops[..seek]
        .iter()
        .rev()
        .find(|(op, _)| op == "MakeRecord")
        .map(|(_, fields)| *fields)
}

/// Whether the walk leaves the key's block once the rowid passes a ceiling:
/// an `IdxRowid` immediately followed by a `Gt` jump.
fn has_rowid_ceiling(ops: &[(String, i64)]) -> bool {
    ops.windows(2)
        .any(|pair| pair[0].0 == "IdxRowid" && pair[1].0 == "Gt")
}

/// Whether the index seek's probe record carries a second field (the rowid
/// bound) after a one-column key.
async fn seek_probe_has_rowid_field(c: &Connection, sql: &str) -> bool {
    seek_probe_fields(&explain_ops(c, sql).await) == Some(2)
}

/// bd-z3a05, bound parameters: keyset pagination as applications write it.
/// The ordered index scan seeks `(key, bound)` instead of the start of the
/// key's block; the bound takes the rowid's INTEGER affinity, and values that
/// stay non-numeric sort as SQLite orders them.
#[test]
fn eq_rowid_bound_parameters_start_the_seek_and_match_sqlite() {
    asupersync::test_utils::run_test(|| async {
        let (f, r) = setup().await;
        let text = |s: &str| SqliteValue::Text(s.into());
        let cases: Vec<(&str, Vec<SqliteValue>)> = vec![
            (
                "SELECT id, c FROM t WHERE a = ?1 AND id > ?2 ORDER BY id LIMIT 10",
                vec![SqliteValue::Integer(5), SqliteValue::Integer(100)],
            ),
            (
                "SELECT id, c FROM t WHERE a = ?1 AND id >= ?2 ORDER BY id LIMIT 10",
                vec![SqliteValue::Integer(5), SqliteValue::Integer(105)],
            ),
            (
                "SELECT id, c FROM t WHERE a = ?1 AND id < ?2 ORDER BY id DESC LIMIT 10",
                vec![SqliteValue::Integer(5), SqliteValue::Integer(900)],
            ),
            (
                "SELECT id FROM t WHERE a = ?1 AND id > ?2 ORDER BY id LIMIT 5",
                vec![SqliteValue::Integer(5), text("100")],
            ),
            (
                "SELECT id FROM t WHERE a = ?1 AND id > ?2 ORDER BY id LIMIT 5",
                vec![SqliteValue::Integer(5), SqliteValue::Float(100.5)],
            ),
            (
                "SELECT id FROM t WHERE a = ?1 AND id > ?2 ORDER BY id LIMIT 5",
                vec![SqliteValue::Integer(5), SqliteValue::Null],
            ),
            (
                "SELECT id FROM t WHERE a = ?1 AND id > ?2 ORDER BY id LIMIT 5",
                vec![SqliteValue::Integer(5), text("abc")],
            ),
            (
                "SELECT id FROM t WHERE a = ?1 AND id < ?2 ORDER BY id DESC LIMIT 5",
                vec![SqliteValue::Integer(5), text("abc")],
            ),
            (
                "SELECT id FROM t WHERE a = ?1 AND id < ?2 ORDER BY id DESC LIMIT 5",
                vec![SqliteValue::Integer(5), SqliteValue::Null],
            ),
            (
                "SELECT id, x FROM t WHERE x = ?1 AND id > ?2 ORDER BY id LIMIT 7",
                vec![text("v3"), SqliteValue::Integer(700)],
            ),
            (
                "SELECT id FROM t WHERE a = :a AND id > :after ORDER BY id LIMIT 4",
                vec![SqliteValue::Integer(9), SqliteValue::Integer(1200)],
            ),
            (
                "SELECT id FROM t WHERE a = ?1 AND id > ?2 ORDER BY id LIMIT 4 OFFSET 2",
                vec![SqliteValue::Integer(9), SqliteValue::Integer(10)],
            ),
        ];
        for (sql, params) in &cases {
            assert_eq!(
                frank_rows_with(&f, sql, params).await,
                sqlite_rows_with(&r, sql, params),
                "`{sql}` {params:?}"
            );
            assert!(
                seek_probe_has_rowid_field(&f, sql).await,
                "bd-z3a05: `{sql}` must seek (key, rowid bound), not the start of the key's block"
            );
        }
        // A composite index's entries are not in rowid order within `a`:
        // no rowid field is added there, and the rows still match stock.
        let composite = "SELECT id, c FROM u WHERE a = ?1 AND id > ?2 ORDER BY id LIMIT 10";
        let params = [SqliteValue::Integer(5), SqliteValue::Integer(100)];
        assert_eq!(
            frank_rows_with(&f, composite, &params).await,
            sqlite_rows_with(&r, composite, &params),
            "`{composite}`"
        );
        assert!(!seek_probe_has_rowid_field(&f, composite).await);
    });
}

/// bd-z3a05, aggregates: `SUM(c) WHERE a = 5 AND id < 900` walked the rowid
/// slice and filtered on `a`. When literal equalities pin every key column of
/// an index, the aggregate seeks `(key, floor)` and leaves the block past the
/// ceiling, as stock's `SEARCH t USING INDEX t_a (a=? AND rowid<?)` does.
/// `group_concat` pins the visiting order (rowid order, as stock's).
#[test]
fn eq_rowid_bounded_aggregates_seek_the_index_and_match_sqlite() {
    asupersync::test_utils::run_test(|| async {
        let (f, r) = setup().await;
        // (query, index, probe fields when the rowid floor is in the probe, ceiling check)
        let bounded: [(&str, &str, i64, bool); 12] = [
            (
                "SELECT sum(c) FROM t WHERE a = 5 AND id < 900",
                "t_a",
                1,
                true,
            ),
            (
                "SELECT count(*) FROM t WHERE a = 5 AND id > 100",
                "t_a",
                2,
                false,
            ),
            (
                "SELECT sum(c), count(*), min(id), max(id), avg(c) FROM t \
                 WHERE a = 5 AND id BETWEEN 100 AND 900",
                "t_a",
                2,
                true,
            ),
            (
                "SELECT group_concat(id) FROM t WHERE a = 5 AND id > 1300",
                "t_a",
                2,
                false,
            ),
            (
                "SELECT group_concat(id) FROM t WHERE a = 5 AND id <= 300",
                "t_a",
                1,
                true,
            ),
            (
                "SELECT total(c), count(x) FROM t WHERE x = 'v3' AND id >= 1400",
                "t_x",
                2,
                false,
            ),
            (
                "SELECT sum(c) FROM t WHERE a = 5 AND id > 100 AND id < 700 AND c = 3",
                "t_a",
                2,
                true,
            ),
            (
                "SELECT count(*), sum(c) FROM t WHERE a = 99 AND id > 0",
                "t_a",
                2,
                false,
            ),
            (
                "SELECT count(*), sum(c) FROM t WHERE a = 5 AND id > 100000",
                "t_a",
                2,
                false,
            ),
            (
                "SELECT count(*), sum(c) FROM t WHERE a = 5 AND id BETWEEN 300 AND 50",
                "t_a",
                2,
                true,
            ),
            (
                "SELECT max(c) FROM t WHERE id > 1000 AND a = 7",
                "t_a",
                2,
                false,
            ),
            // Both key columns pinned: the next field is the rowid here too.
            (
                "SELECT sum(id) FROM u WHERE a = 5 AND c = 3 AND id > 100",
                "u_ac",
                3,
                false,
            ),
        ];
        for (sql, index, probe_fields, ceiling) in bounded {
            assert_eq!(frank_rows(&f, sql).await, sqlite_rows(&r, sql), "`{sql}`");
            let ops = explain_ops(&f, sql).await;
            assert!(
                opens_index(&f, sql, index).await,
                "bd-z3a05: `{sql}` must seek {index}, not walk the rowid slice"
            );
            assert_eq!(
                seek_probe_fields(&ops),
                Some(probe_fields),
                "bd-z3a05: `{sql}` probe record"
            );
            assert_eq!(
                has_rowid_ceiling(&ops),
                ceiling,
                "bd-z3a05: `{sql}` rowid ceiling check"
            );
        }
        // A composite index listed first, which `a = 5` does not fully pin, must
        // not hide the one-column index that qualifies.
        for statement in [
            "CREATE TABLE w (id INTEGER PRIMARY KEY, a INTEGER, c INTEGER)",
            "CREATE INDEX w_ac ON w(a, c)",
            "CREATE INDEX w_a ON w(a)",
            "INSERT INTO w SELECT id, a, c FROM t",
        ] {
            f.execute(statement).await.unwrap();
            r.execute_batch(statement).unwrap();
        }
        let shadowed = "SELECT sum(c), group_concat(id) FROM w WHERE a = 5 AND id < 900";
        assert_eq!(
            frank_rows(&f, shadowed).await,
            sqlite_rows(&r, shadowed),
            "`{shadowed}`"
        );
        assert!(
            opens_index(&f, shadowed, "w_a").await,
            "`{shadowed}` must seek w_a"
        );
        assert!(has_rowid_ceiling(&explain_ops(&f, shadowed).await));

        // Outside the lane, answers must still match. `u`'s `a = 5` entries are
        // ordered by c, so leaving at the first rowid past 900 would drop rows:
        // the lane must not claim it. (No group_concat here: this plan visits
        // in rowid order while stock walks u_ac, so its order differs, on main
        // as well.)
        let composite = "SELECT count(*), sum(c), sum(id) FROM u WHERE a = 5 AND id < 900";
        assert_eq!(
            frank_rows(&f, composite).await,
            sqlite_rows(&r, composite),
            "`{composite}`"
        );
        assert!(!has_rowid_ceiling(&explain_ops(&f, composite).await));
        let other = [
            "SELECT sum(c) FROM t WHERE a = 5 AND id > 10.5",
            "SELECT sum(c) FROM t WHERE a = 5 AND id > -5",
            "SELECT sum(c) FROM t WHERE a = '5' AND id > 100",
            "SELECT sum(c) FROM t WHERE a IS NULL AND id > 100",
            "SELECT sum(c) FROM t WHERE (a = 5 OR a = 6) AND id < 900",
            "SELECT c, sum(id) FROM t WHERE a = 5 AND id < 900 GROUP BY c",
            "SELECT sum(c) FROM t WHERE a = 5 AND id < 900 HAVING count(*) > 3",
            "SELECT count(*) FROM t WHERE a = 5 AND id < 900 AND id > 950",
        ];
        for sql in other {
            assert_eq!(frank_rows(&f, sql).await, sqlite_rows(&r, sql), "`{sql}`");
        }
        let param = "SELECT sum(c), count(*) FROM t WHERE a = 5 AND id < ?1";
        let params = [SqliteValue::Integer(900)];
        assert_eq!(
            frank_rows_with(&f, param, &params).await,
            sqlite_rows_with(&r, param, &params),
            "`{param}`"
        );
    });
}

/// The P4 operands of the program's `Int64` instructions.
async fn int64_operands(c: &Connection, sql: &str) -> Vec<i64> {
    c.query(&format!("EXPLAIN {sql}"))
        .await
        .unwrap()
        .iter()
        .filter_map(|row| {
            let values = row.values();
            if !matches!(values.get(1), Some(SqliteValue::Text(op)) if op.to_string() == "Int64") {
                return None;
            }
            match values.get(5)? {
                SqliteValue::Integer(n) => Some(*n),
                SqliteValue::Text(text) => {
                    text.to_string().trim_start_matches("(i64)").parse().ok()
                }
                _ => None,
            }
        })
        .collect()
}

/// bd-z3a05, the IN-list form: `SUM(c) WHERE a IN (5, 7) AND id < 900` walked
/// the rowid slice. Each listed value's run of a one-column index is in rowid
/// order, so each seek starts at `(value, floor)` and leaves past the ceiling.
/// The values are sorted, so group_concat visits 5's rows, then 7's, as stock
/// does.
#[test]
fn eq_rowid_bounded_in_list_aggregates_seek_each_value_and_match_sqlite() {
    asupersync::test_utils::run_test(|| async {
        let (f, r) = setup().await;
        // (query, floor in the probe, ceiling check)
        let bounded = [
            (
                "SELECT sum(c) FROM t WHERE a IN (5, 7) AND id < 900",
                false,
                true,
            ),
            (
                "SELECT count(*), sum(c) FROM t WHERE a IN (7, 5, 5) AND id BETWEEN 100 AND 900",
                true,
                true,
            ),
            (
                "SELECT group_concat(id) FROM t WHERE a IN (7, 5) AND id > 1300",
                true,
                false,
            ),
            (
                "SELECT group_concat(id) FROM t WHERE a IN (5, 7) AND id <= 300",
                false,
                true,
            ),
            (
                "SELECT count(*), sum(c) FROM t WHERE a IN (98, 99) AND id > 0",
                true,
                false,
            ),
            (
                "SELECT count(*), sum(c) FROM t WHERE a IN (5, 7) AND id > 100000",
                true,
                false,
            ),
            (
                "SELECT max(c), min(id) FROM t WHERE a IN (5, 7) AND id > 100 AND id < 700 AND c = 3",
                true,
                true,
            ),
        ];
        for (sql, floor, ceiling) in bounded {
            assert_eq!(frank_rows(&f, sql).await, sqlite_rows(&r, sql), "`{sql}`");
            assert!(opens_index(&f, sql, "t_a").await, "`{sql}` must seek t_a");
            assert_eq!(
                !int64_operands(&f, sql).await.contains(&i64::MIN),
                floor,
                "bd-z3a05: `{sql}` per-value rowid floor"
            );
            assert_eq!(
                has_rowid_ceiling(&explain_ops(&f, sql).await),
                ceiling,
                "bd-z3a05: `{sql}` rowid ceiling check"
            );
        }
        // Outside the lane: a composite index's runs are ordered by c, so it
        // keeps its unbounded prefix probe; non-integer members decline; and
        // `a = 5 OR (a = 7 AND id < 900)` has no rowid range over the whole WHERE.
        for sql in [
            "SELECT count(*), sum(c), sum(id) FROM u WHERE a IN (5, 7) AND id < 900",
            "SELECT total(c) FROM t WHERE a = 5 OR a = 7 AND id < 900",
            "SELECT sum(c) FROM t WHERE a IN ('5', 7) AND id < 900",
            "SELECT sum(c) FROM t WHERE a IN (5, NULL) AND id < 900",
            "SELECT sum(c) FROM t WHERE a NOT IN (5, 7) AND id < 900",
        ] {
            assert_eq!(frank_rows(&f, sql).await, sqlite_rows(&r, sql), "`{sql}`");
            assert!(!has_rowid_ceiling(&explain_ops(&f, sql).await), "`{sql}`");
        }
    });
}
