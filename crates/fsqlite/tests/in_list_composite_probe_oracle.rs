//! bd-hyoms: an IN list on one key term of a composite index, with `col = constant` conjuncts
//! pinning the terms before it (and any right after it), seeks `(prefix.., member, suffix..)` per
//! member, as stock's `SEARCH t USING INDEX t_ab (a=? AND b=?)` does, instead of walking the IN
//! column's (or the leading equality's) whole run. The rows match rusqlite's as a set (without an
//! ORDER BY the order follows the chosen index), on a file database in strict mode so they come
//! from the program under test; the opcode checks read the probe record's field count.

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
    let mut rows = frank_rows_in_order(c, sql, params).await;
    rows.sort();
    rows
}

async fn frank_rows_in_order(
    c: &Connection,
    sql: &str,
    params: &[SqliteValue],
) -> Vec<Vec<String>> {
    c.query_with_params(sql, params)
        .await
        .unwrap_or_else(|e| panic!("frank `{sql}` {params:?}: {e}"))
        .iter()
        .map(|row| row.values().iter().map(render).collect())
        .collect()
}

fn sqlite_rows(c: &rusqlite::Connection, sql: &str, params: &[SqliteValue]) -> Vec<Vec<String>> {
    let mut rows = sqlite_rows_in_order(c, sql, params);
    rows.sort();
    rows
}

fn sqlite_rows_in_order(
    c: &rusqlite::Connection,
    sql: &str,
    params: &[SqliteValue],
) -> Vec<Vec<String>> {
    let mut stmt = c.prepare(sql).unwrap();
    let n = stmt.column_count();
    stmt.query_map(
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
    .collect()
}

/// EXPLAIN rows as (opcode, p1, p2, p3).
async fn program(c: &Connection, sql: &str) -> Vec<(String, i64, i64, i64)> {
    let int = |v: Option<&SqliteValue>| match v {
        Some(SqliteValue::Integer(n)) => *n,
        _ => -1,
    };
    c.query(&format!("EXPLAIN {sql}"))
        .await
        .unwrap_or_else(|e| panic!("EXPLAIN `{sql}`: {e}"))
        .iter()
        .map(|row| {
            let values = row.values();
            let op = match values.get(1) {
                Some(SqliteValue::Text(op)) => op.to_string(),
                _ => String::new(),
            };
            (
                op,
                int(values.get(2)),
                int(values.get(3)),
                int(values.get(4)),
            )
        })
        .collect()
}

/// The widest record a `SeekGE` probes with: the field count of the
/// `MakeRecord` that writes its key register.
fn widest_probe(ops: &[(String, i64, i64, i64)]) -> i64 {
    ops.iter()
        .filter(|(op, ..)| op == "SeekGE")
        .filter_map(|(_, _, _, key)| {
            ops.iter()
                .filter(|(op, _, _, dest)| op == "MakeRecord" && dest == key)
                .map(|(_, _, fields, _)| *fields)
                .max()
        })
        .max()
        .unwrap_or(0)
}

async fn setup(path: &str) -> (Connection, rusqlite::Connection) {
    let f = Connection::open(path).await.unwrap();
    let r = rusqlite::Connection::open_in_memory().unwrap();
    let mut ddl = vec![
        "CREATE TABLE t (id INTEGER PRIMARY KEY, a INTEGER, b INTEGER, c INTEGER, s TEXT, \
         x TEXT COLLATE NOCASE, r REAL, u INTEGER);"
            .to_owned(),
        "CREATE INDEX t_ab ON t(a, b);".to_owned(),
        "CREATE INDEX t_asd ON t(a, s DESC);".to_owned(),
        "CREATE INDEX t_ax ON t(a, x);".to_owned(),
        "CREATE INDEX t_ar ON t(a, r);".to_owned(),
        "CREATE INDEX t_uvw ON t(u, b, c);".to_owned(),
    ];
    for i in 1..=3000_i64 {
        // NULLs and text in the INTEGER columns; reals with integral values.
        let a = if i % 97 == 0 {
            "NULL".to_owned()
        } else if i % 101 == 0 {
            "'abc'".to_owned()
        } else {
            format!("{}", i % 40)
        };
        let b = if i % 89 == 0 {
            "NULL".to_owned()
        } else {
            format!("{}", i % 7)
        };
        let s = if i % 83 == 0 {
            "NULL".to_owned()
        } else {
            format!("'k{}'", i % 60)
        };
        let x = format!("'{}{}'", if i % 2 == 0 { "K" } else { "k" }, i % 5);
        let r = if i % 3 == 0 {
            format!("{}", i % 4)
        } else {
            format!("{}.5", i % 4)
        };
        ddl.push(format!(
            "INSERT INTO t VALUES ({i}, {a}, {b}, {}, {s}, {x}, {r}, {});",
            i % 11,
            i % 13
        ));
    }
    for statement in &ddl {
        f.execute(statement).await.unwrap();
        r.execute_batch(statement).unwrap();
    }
    (f, r)
}

#[test]
fn in_list_with_key_equalities_probes_the_composite_prefix_and_matches_sqlite() {
    asupersync::test_utils::run_test(|| async {
        let int = SqliteValue::Integer;
        let text = |s: &str| SqliteValue::Text(s.into());
        // (sql, params, probe width the seek must reach)
        let probes: Vec<(&str, Vec<SqliteValue>, i64)> = vec![
            (
                "SELECT id FROM t WHERE a IN (3, 17, 39) AND b = 3",
                vec![],
                2,
            ),
            (
                "SELECT id, c FROM t WHERE a = 5 AND b IN (1, 2, 3)",
                vec![],
                2,
            ),
            ("SELECT id FROM t WHERE b IN (1, 2, 3) AND a = 5", vec![], 2),
            (
                "SELECT id FROM t WHERE a = 5 AND (b = 1 OR b = 4)",
                vec![],
                2,
            ),
            // A residual, duplicate and equal-after-affinity members, a NULL member.
            (
                "SELECT id FROM t WHERE a = 5 AND b IN (1, 2, 3) AND c > 4",
                vec![],
                2,
            ),
            (
                "SELECT id FROM t WHERE a = 5 AND b IN (1, 1, '1', 1.0, 6)",
                vec![],
                2,
            ),
            ("SELECT id FROM t WHERE a = 5 AND b IN (NULL, 2)", vec![], 2),
            (
                "SELECT id FROM t WHERE a = 'abc' AND b IN (1, 5)",
                vec![],
                2,
            ),
            (
                "SELECT id FROM t WHERE a = '5' AND b IN ('1', 2)",
                vec![],
                2,
            ),
            ("SELECT id FROM t WHERE a = NULL AND b IN (1, 2)", vec![], 2),
            (
                "SELECT id FROM t WHERE a = 5.0 AND b IN (2.0, 3)",
                vec![],
                2,
            ),
            (
                "SELECT id FROM t WHERE a = 5 AND b IN (100, 200)",
                vec![],
                2,
            ),
            // A DESC second key term; TEXT affinity on the members.
            (
                "SELECT id, s FROM t WHERE a = 5 AND s IN ('k5', 'k45', 5)",
                vec![],
                2,
            ),
            (
                "SELECT id, s FROM t WHERE a IN (5, 7, 9) AND s = 'k5'",
                vec![],
                2,
            ),
            // A NOCASE key term pinned under its own collation.
            (
                "SELECT id, x FROM t WHERE a IN (5, 7) AND x = 'K0'",
                vec![],
                2,
            ),
            // REAL: integral members and equalities.
            (
                "SELECT id, r FROM t WHERE a = 5 AND r IN (1.5, 3, 2)",
                vec![],
                2,
            ),
            (
                "SELECT id, r FROM t WHERE a IN (4, 5, 6) AND r = 1",
                vec![],
                2,
            ),
            // Three key terms, and the IN in the middle.
            (
                "SELECT id FROM t WHERE u = 3 AND b IN (2, 3) AND c = 6",
                vec![],
                3,
            ),
            (
                "SELECT id FROM t WHERE u IN (3, 4) AND b = 2 AND c = 9",
                vec![],
                3,
            ),
            // Numbered parameters.
            (
                "SELECT id FROM t WHERE a = ?1 AND b IN (?2, ?3)",
                vec![int(5), int(1), int(3)],
                2,
            ),
            (
                "SELECT id FROM t WHERE a IN (?1, ?2) AND b = ?3",
                vec![int(5), text("7"), int(2)],
                2,
            ),
            (
                "SELECT id FROM t WHERE a = ?1 AND b IN (?2, ?3)",
                vec![SqliteValue::Null, int(1), int(3)],
                2,
            ),
            // Aggregates; the first two enforce the whole WHERE from the index.
            (
                "SELECT count(*) FROM t WHERE a = 5 AND b IN (1, 2, 3)",
                vec![],
                2,
            ),
            (
                "SELECT count(*) FROM t WHERE a IN (3, 17, 39) AND b = 3",
                vec![],
                2,
            ),
            (
                "SELECT sum(c), max(id) FROM t WHERE a IN (3, 17) AND b = 3",
                vec![],
                2,
            ),
            (
                "SELECT count(*) FROM t WHERE a = 5 AND b IN (1, 2) AND c < 5",
                vec![],
                2,
            ),
        ];
        // The probe cannot pin a second term: the IN column has a NOCASE
        // collation, the equality compares under another collation than the
        // key term's, or a key term between them is not pinned. (A range on
        // the next key term is a probe since bd-5i6zm, checked below.)
        let one_field: Vec<&str> = vec![
            "SELECT id, x FROM t WHERE a = 5 AND x IN ('K1', 'k2')",
            "SELECT id, x FROM t WHERE a IN (5, 7) AND x = 'K0' COLLATE BINARY",
            "SELECT id FROM t WHERE u IN (3, 4) AND c = 9",
        ];
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("in_list_composite_probe.db");
        for path in [":memory:", file.to_str().unwrap()] {
            let (f, r) = setup(path).await;
            // Strict mode turns any fallback into an error.
            f.execute("PRAGMA fsqlite.parity_cert_strict = ON;")
                .await
                .unwrap();
            for (sql, params, width) in &probes {
                assert_eq!(
                    frank_rows(&f, sql, params).await,
                    sqlite_rows(&r, sql, params),
                    "`{sql}` {params:?} at {path}"
                );
                let ops = program(&f, sql).await;
                assert_eq!(
                    widest_probe(&ops),
                    *width,
                    "`{sql}` must probe {width} key terms: {ops:?}"
                );
                assert!(
                    !ops.iter().any(|(op, ..)| op == "Rewind"),
                    "`{sql}` must not scan: {ops:?}"
                );
            }
            for sql in &one_field {
                assert_eq!(
                    frank_rows(&f, sql, &[]).await,
                    sqlite_rows(&r, sql, &[]),
                    "`{sql}` at {path}"
                );
                let ops = program(&f, sql).await;
                assert!(
                    widest_probe(&ops) < 2,
                    "`{sql}` must not pin a second key term: {ops:?}"
                );
            }
            // A COUNT(*) the probe answers alone reads no table row.
            for sql in [
                "SELECT count(*) FROM t WHERE a = 5 AND b IN (1, 2, 3)",
                "SELECT count(*) FROM t WHERE a IN (3, 17, 39) AND b = 3",
            ] {
                let ops = program(&f, sql).await;
                assert!(
                    !ops.iter().any(|(op, ..)| op == "SeekRowid"),
                    "`{sql}` must count index entries: {ops:?}"
                );
            }
            // GROUP BY: on a file database the substrate runs the program
            // (strict refuses its in-memory fallback); in memory the mirror
            // groups, so only the rows are compared there.
            for sql in [
                "SELECT b, count(*) FROM t WHERE a = 5 AND b IN (1, 2, 3) GROUP BY b",
                "SELECT a, sum(c) FROM t WHERE a IN (3, 17, 39) AND b = 3 GROUP BY a",
            ] {
                assert_eq!(
                    frank_rows(&f, sql, &[]).await,
                    sqlite_rows(&r, sql, &[]),
                    "`{sql}` at {path}"
                );
                assert_eq!(
                    widest_probe(&program(&f, sql).await),
                    2,
                    "`{sql}` must probe two key terms"
                );
            }
            // A prepared statement runs twice with fresh probe sets.
            let sql = "SELECT id FROM t WHERE a = ?1 AND b IN (?2, ?3)";
            let stmt = f.prepare(sql).await.unwrap();
            for params in [[int(5), int(1), int(3)], [int(7), int(2), int(2)]] {
                let mut got: Vec<Vec<String>> = stmt
                    .query_with_params(&params)
                    .await
                    .unwrap()
                    .iter()
                    .map(|row| row.values().iter().map(render).collect())
                    .collect();
                got.sort();
                assert_eq!(
                    got,
                    sqlite_rows(&r, sql, &params),
                    "prepared {params:?} at {path}"
                );
            }
            drop(stmt);
            f.close().await.unwrap();
        }
    });
}

/// bd-5i6zm: a second IN list on a pinned key term multiplies the probe set
/// (one seek per member combination, at most 64), and a range on the key term
/// after the pinned ones bounds each probe's run, as stock's
/// `SEARCH t USING COVERING INDEX t_ab (a=? AND b=?)` / `(a=? AND b>?)` do.
/// Covered outputs are answered from the index entry. Rows match rusqlite's,
/// in its order where both walk the same index.
#[test]
fn second_in_list_or_range_on_the_next_key_term_extends_the_composite_probe() {
    asupersync::test_utils::run_test(|| async {
        let int = SqliteValue::Integer;
        let text = |s: &str| SqliteValue::Text(s.into());
        // (sql, params, probe width the seek must reach, outputs and filter covered by the index)
        let probes: Vec<(&str, Vec<SqliteValue>, i64, bool)> = vec![
            // Two IN lists: duplicates, equal-after-affinity, NULL, text and
            // absent members, an OR chain, and three key terms.
            (
                "SELECT id FROM t WHERE a IN (3, 17, 39) AND b IN (3, 4)",
                vec![],
                2,
                true,
            ),
            (
                "SELECT id, c FROM t WHERE a IN (5, 7) AND b IN (1, 2, 3)",
                vec![],
                2,
                false,
            ),
            (
                "SELECT id FROM t WHERE a IN (5, 5.0, '5', NULL, 7) AND b IN (1, '1', NULL, 2)",
                vec![],
                2,
                true,
            ),
            (
                "SELECT id, a FROM t WHERE a IN ('abc', 5) AND b IN (1, 5)",
                vec![],
                2,
                true,
            ),
            (
                "SELECT id FROM t WHERE a IN (100, 200) AND b IN (1, 2)",
                vec![],
                2,
                true,
            ),
            (
                "SELECT id FROM t WHERE a IN (5, 7) AND (b = 1 OR b = 4)",
                vec![],
                2,
                true,
            ),
            (
                "SELECT id, s FROM t WHERE a IN (5, 7) AND s IN ('k5', 'k45', 'k7')",
                vec![],
                2,
                true,
            ),
            (
                "SELECT id FROM t WHERE u IN (3, 4) AND b IN (2, 3) AND c IN (6, 9)",
                vec![],
                3,
                true,
            ),
            (
                "SELECT id FROM t WHERE u IN (3, 4) AND b = 2 AND c IN (6, 9)",
                vec![],
                3,
                true,
            ),
            (
                "SELECT id FROM t WHERE a IN (5, 7) AND b IN (1, 2) AND c > 4",
                vec![],
                2,
                false,
            ),
            (
                "SELECT id FROM t WHERE a IN (?1, ?2) AND b IN (?3, ?4)",
                vec![int(5), text("7"), int(1), SqliteValue::Null],
                2,
                true,
            ),
            // A range on the next key term: exclusive and inclusive bounds,
            // BETWEEN, an empty range, REAL keys, parameters, and a NULL bound.
            (
                "SELECT id, a FROM t WHERE a IN (3, 17) AND b > 4",
                vec![],
                2,
                true,
            ),
            (
                "SELECT id, b FROM t WHERE a IN (17, 3) AND b BETWEEN 2 AND 4",
                vec![],
                2,
                true,
            ),
            (
                "SELECT id FROM t WHERE a IN (3, 17) AND b >= 2 AND b < 5",
                vec![],
                2,
                true,
            ),
            (
                "SELECT id FROM t WHERE a IN (3, 17) AND b > 100",
                vec![],
                2,
                true,
            ),
            // A REAL filter column is not read from the entry (fails closed).
            (
                "SELECT id, r FROM t WHERE a IN (4, 5) AND r > 1.5",
                vec![],
                2,
                false,
            ),
            (
                "SELECT id FROM t WHERE a IN (?1, ?2) AND b > ?3",
                vec![int(3), int(17), int(4)],
                2,
                true,
            ),
            (
                "SELECT id FROM t WHERE a IN (?1, ?2) AND b > ?3",
                vec![int(3), int(17), SqliteValue::Null],
                2,
                true,
            ),
            (
                "SELECT id, c FROM t WHERE a IN (3, 17) AND b > 2 AND c < 5",
                vec![],
                2,
                false,
            ),
            // Pinned terms then a range: three key terms.
            (
                "SELECT id FROM t WHERE u IN (3, 4) AND b = 2 AND c > 5",
                vec![],
                3,
                true,
            ),
            (
                "SELECT id FROM t WHERE u IN (3, 4) AND b IN (2, 3) AND c BETWEEN 2 AND 8",
                vec![],
                3,
                true,
            ),
            // No lower bound: the seek is the pinned prefix, NULL keys are
            // skipped and the upper bound ends each run.
            (
                "SELECT id FROM t WHERE a IN (3, 17) AND b < 3",
                vec![],
                1,
                true,
            ),
            (
                "SELECT id FROM t WHERE a IN (3, 17) AND b <= 3",
                vec![],
                1,
                true,
            ),
            // Aggregates the probe answers alone, and one with a residual.
            (
                "SELECT count(*) FROM t WHERE a IN (3, 17, 39) AND b IN (3, 4)",
                vec![],
                2,
                true,
            ),
            (
                "SELECT count(*) FROM t WHERE a IN (3, 17, 39) AND b BETWEEN 2 AND 4",
                vec![],
                2,
                true,
            ),
            (
                "SELECT count(*) FROM t WHERE a IN (3, 17) AND b < 3",
                vec![],
                1,
                true,
            ),
            (
                "SELECT sum(c) FROM t WHERE a IN (3, 17) AND b > 2",
                vec![],
                2,
                false,
            ),
        ];
        // Not extended: a cross product past 64 combinations, a text bound on
        // the INTEGER column, a DESC or NOCASE range term.
        let unchanged: Vec<&str> = vec![
            "SELECT id FROM t WHERE a IN (0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, \
             17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32) AND b IN (1, 2)",
            "SELECT id FROM t WHERE a IN (3, 17) AND b > '4'",
            "SELECT id, s FROM t WHERE a IN (5, 7) AND s > 'k4'",
            "SELECT id, x FROM t WHERE a IN (5, 7) AND x > 'k2'",
        ];
        // Stock walks the same index: same rows, in the same order.
        let ordered = [
            "SELECT id FROM t WHERE a IN (3, 17, 39) AND b IN (3, 4)",
            "SELECT id, a FROM t WHERE a IN (3, 17) AND b > 4",
            "SELECT id, b FROM t WHERE a IN (17, 3) AND b BETWEEN 2 AND 4",
            "SELECT id, s FROM t WHERE a IN (5, 7) AND s IN ('k5', 'k45', 'k7')",
            "SELECT id FROM t WHERE u IN (3, 4) AND b IN (2, 3) AND c BETWEEN 2 AND 8",
            "SELECT id FROM t WHERE a IN (3, 17) AND b < 3",
        ];
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("in_list_composite_cross.db");
        for path in [":memory:", file.to_str().unwrap()] {
            let (f, r) = setup(path).await;
            f.execute("PRAGMA fsqlite.parity_cert_strict = ON;")
                .await
                .unwrap();
            for (sql, params, width, covered) in &probes {
                assert_eq!(
                    frank_rows(&f, sql, params).await,
                    sqlite_rows(&r, sql, params),
                    "`{sql}` {params:?} at {path}"
                );
                let ops = program(&f, sql).await;
                assert_eq!(
                    widest_probe(&ops),
                    *width,
                    "`{sql}` must probe {width} key terms: {ops:?}"
                );
                assert!(
                    !ops.iter().any(|(op, ..)| op == "Rewind"),
                    "`{sql}` must not scan: {ops:?}"
                );
                assert_eq!(
                    !ops.iter().any(|(op, ..)| op == "SeekRowid"),
                    *covered,
                    "`{sql}`: the index {} every column read: {ops:?}",
                    if *covered { "holds" } else { "lacks" }
                );
                // Without a lower bound the seek key is the pinned prefix; the
                // composite probe set serves it, not the integer IN walk that
                // filters every entry of each member's run.
                if *width == 1 {
                    assert!(
                        ops.iter().any(|(op, ..)| op == "SorterOpen"),
                        "`{sql}` must take the composite probe: {ops:?}"
                    );
                }
            }
            for sql in &unchanged {
                assert_eq!(
                    frank_rows(&f, sql, &[]).await,
                    sqlite_rows(&r, sql, &[]),
                    "`{sql}` at {path}"
                );
                let ops = program(&f, sql).await;
                assert!(
                    widest_probe(&ops) < 2,
                    "`{sql}` must keep its one-field probe: {ops:?}"
                );
            }
            for sql in ordered {
                assert_eq!(
                    frank_rows_in_order(&f, sql, &[]).await,
                    sqlite_rows_in_order(&r, sql, &[]),
                    "`{sql}` rows in stock's order at {path}"
                );
            }
            // GROUP BY over a cross product and over a range (rows only in
            // memory, where the mirror groups).
            for sql in [
                "SELECT a, b, count(*) FROM t WHERE a IN (5, 7) AND b IN (1, 2) GROUP BY a, b",
                "SELECT a, sum(c) FROM t WHERE a IN (3, 17, 39) AND b > 3 GROUP BY a",
            ] {
                assert_eq!(
                    frank_rows(&f, sql, &[]).await,
                    sqlite_rows(&r, sql, &[]),
                    "`{sql}` at {path}"
                );
                assert_eq!(
                    widest_probe(&program(&f, sql).await),
                    2,
                    "`{sql}` must probe two key terms"
                );
            }
            // A prepared statement runs again with other members and bound.
            let sql = "SELECT id FROM t WHERE a IN (?1, ?2) AND b > ?3";
            let stmt = f.prepare(sql).await.unwrap();
            for params in [[int(3), int(17), int(4)], [int(17), int(17), int(0)]] {
                let mut got: Vec<Vec<String>> = stmt
                    .query_with_params(&params)
                    .await
                    .unwrap()
                    .iter()
                    .map(|row| row.values().iter().map(render).collect())
                    .collect();
                got.sort();
                assert_eq!(
                    got,
                    sqlite_rows(&r, sql, &params),
                    "prepared {params:?} at {path}"
                );
            }
            drop(stmt);
            f.close().await.unwrap();
        }
    });
}

/// bd-odxzh + bd-mdnfh: IN list + ORDER BY covering probes, probe-order
/// streaming without an ORDER BY sorter (ASC/DESC + LIMIT/OFFSET), composite
/// and range probes, DISTINCT, NULL/duplicate members, INDEXED BY / NOT INDEXED,
/// and density-gated sparse vs dense LIMIT choice (both with and without ANALYZE).
#[test]
fn in_list_order_by_covering_probe_order_and_limit_choice_match_sqlite() {
    asupersync::test_utils::run_test(|| async {
        let int = SqliteValue::Integer;
        let text = |s: &str| SqliteValue::Text(s.into());

        // Helper to count SorterOpen opcodes (1 = probe set only, 2 = probe set + ORDER BY sort).
        let sorter_count = |ops: &[(String, i64, i64, i64)]| {
            ops.iter().filter(|(op, ..)| op == "SorterOpen").count()
        };
        let has_op =
            |ops: &[(String, i64, i64, i64)], name: &str| ops.iter().any(|(op, ..)| op == name);

        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("in_list_order_by_oracle.db");
        for path in [":memory:", file.to_str().unwrap()] {
            let (f, r) = setup(path).await;
            // Add single-column indexes mirroring the 200k benchmark schema so
            // index ranking (t_a vs t_ab, t_s, t_c) is exercised directly.
            for ddl in [
                "CREATE INDEX t_a ON t(a);",
                "CREATE INDEX t_c ON t(c);",
                "CREATE INDEX t_s ON t(s);",
                "ANALYZE;",
            ] {
                f.execute(ddl).await.unwrap();
                r.execute_batch(ddl).unwrap();
            }
            f.execute("PRAGMA fsqlite.parity_cert_strict = ON;")
                .await
                .unwrap();

            // 1. Covering probe-and-sort (bd-odxzh + bd-mdnfh part 1):
            //    NO SeekRowid, NO Rewind, probes index + sorts in ORDER BY sorter (2 SorterOpen).
            let covering_sort_cases: Vec<(&str, Vec<SqliteValue>, i64)> = vec![
                (
                    "SELECT id FROM t WHERE a IN (3, 17, 39) ORDER BY id",
                    vec![],
                    1,
                ),
                (
                    "SELECT id FROM t WHERE a IN (3, 17, 39) ORDER BY id DESC",
                    vec![],
                    1,
                ),
                (
                    "SELECT id FROM t WHERE a IN (3, 17, 39) AND b BETWEEN 2 AND 4 ORDER BY id",
                    vec![],
                    2,
                ),
                (
                    "SELECT id FROM t WHERE s IN ('k5', 'k45') ORDER BY id",
                    vec![],
                    1,
                ),
                (
                    "SELECT id, b FROM t WHERE a IN (3, 17, 39) ORDER BY id",
                    vec![],
                    1,
                ),
                (
                    "SELECT a FROM t WHERE a IN (3, 17, 39) ORDER BY id DESC",
                    vec![],
                    1,
                ),
                (
                    "SELECT DISTINCT id FROM t WHERE a IN (3, 3, '3', 3.0, NULL, 17) ORDER BY id DESC",
                    vec![],
                    1,
                ),
                (
                    "SELECT id FROM t WHERE a = ?1 AND b IN (?2, ?3) ORDER BY id DESC",
                    vec![int(5), int(1), int(3)],
                    2,
                ),
                (
                    "SELECT id FROM t WHERE a IN (?1, ?2) AND b = ?3 ORDER BY id",
                    vec![int(5), text("7"), int(2)],
                    2,
                ),
                (
                    "SELECT id FROM t WHERE (a = 3 OR a = 17) ORDER BY id DESC",
                    vec![],
                    1,
                ),
            ];
            for (sql, params, width) in &covering_sort_cases {
                assert_eq!(
                    frank_rows_in_order(&f, sql, params).await,
                    sqlite_rows_in_order(&r, sql, params),
                    "covering sort `{sql}` {params:?} at {path}"
                );
                let ops = program(&f, sql).await;
                assert!(
                    !has_op(&ops, "Rewind"),
                    "covering sort `{sql}` must not table-scan: {ops:?}"
                );
                assert!(
                    !has_op(&ops, "SeekRowid"),
                    "covering sort `{sql}` must be covering (no SeekRowid): {ops:?}"
                );
                assert_eq!(
                    widest_probe(&ops),
                    *width,
                    "covering sort `{sql}` probe width: {ops:?}"
                );
                assert_eq!(
                    sorter_count(&ops),
                    2,
                    "covering sort `{sql}` expects probe + ORDER BY sorter: {ops:?}"
                );
            }

            // 2. Non-covering probe-and-sort (bd-odxzh): SELECT * / non-indexed column
            //    (and REAL column failing closed) must still probe (no Rewind), with SeekRowid.
            let noncov_sort_cases: Vec<(&str, Vec<SqliteValue>)> = vec![
                ("SELECT * FROM t WHERE a IN (3, 17, 39) ORDER BY id", vec![]),
                (
                    "SELECT id, c FROM t WHERE a IN (3, 17, 39) ORDER BY c, id",
                    vec![],
                ),
                ("SELECT id, r FROM t WHERE a IN (4, 5) ORDER BY id", vec![]),
                (
                    "SELECT DISTINCT c FROM t WHERE a IN (3, 17) ORDER BY c DESC",
                    vec![],
                ),
            ];
            for (sql, params) in &noncov_sort_cases {
                assert_eq!(
                    frank_rows_in_order(&f, sql, params).await,
                    sqlite_rows_in_order(&r, sql, params),
                    "noncov sort `{sql}` at {path}"
                );
                let ops = program(&f, sql).await;
                assert!(
                    !has_op(&ops, "Rewind"),
                    "noncov sort `{sql}` must not table-scan: {ops:?}"
                );
                assert!(
                    has_op(&ops, "SeekRowid"),
                    "noncov sort `{sql}` needs SeekRowid: {ops:?}"
                );
                assert_eq!(
                    sorter_count(&ops),
                    2,
                    "noncov sort `{sql}` expects probe + ORDER BY sorter: {ops:?}"
                );
            }

            // 3. Probe-order streaming WITHOUT an ORDER BY sorter (bd-mdnfh part 2):
            //    only 1 SorterOpen (the IN probe set), NO Rewind, SeekRowid iff non-covering.
            let stream_cases: Vec<(&str, Vec<SqliteValue>, bool, bool)> = vec![
                // (sql, params, covering, reverse)
                (
                    "SELECT id, a, b FROM t WHERE a IN (3, 17, 39) ORDER BY a, b",
                    vec![],
                    true,
                    false,
                ),
                (
                    "SELECT id, a, b FROM t WHERE a IN (39, 3, 17, 3, NULL) ORDER BY a, b, id",
                    vec![],
                    true,
                    false,
                ),
                (
                    "SELECT * FROM t WHERE a IN (3, 17, 39) ORDER BY a, b",
                    vec![],
                    false,
                    false,
                ),
                (
                    "SELECT id, a, b FROM t WHERE a IN (3, 17, 39) ORDER BY a DESC, b DESC",
                    vec![],
                    true,
                    true,
                ),
                (
                    "SELECT id, a, b FROM t WHERE a IN (3, 17, 39) ORDER BY a DESC, b DESC, id DESC",
                    vec![],
                    true,
                    true,
                ),
                (
                    "SELECT id, a, b FROM t WHERE a IN (3, 17, 39) ORDER BY a, b LIMIT 5 OFFSET 2",
                    vec![],
                    true,
                    false,
                ),
                (
                    "SELECT id, a, b FROM t WHERE a IN (3, 17, 39) ORDER BY a DESC, b DESC LIMIT 5 OFFSET 2",
                    vec![],
                    true,
                    true,
                ),
                (
                    "SELECT id, a, b FROM t WHERE a IN (3, 17, 39) ORDER BY a, b LIMIT 0",
                    vec![],
                    true,
                    false,
                ),
                (
                    "SELECT id, a, b FROM t WHERE a = 5 AND b IN (1, 2, 3) ORDER BY b, id",
                    vec![],
                    true,
                    false,
                ),
                (
                    "SELECT id, a, b FROM t WHERE a = 5 AND b IN (1, 2, 3) ORDER BY a DESC, b ASC, id ASC",
                    vec![],
                    true,
                    false,
                ),
                (
                    "SELECT id, a, b FROM t WHERE a IN (3, 17) AND b BETWEEN 2 AND 4 ORDER BY a, b, id",
                    vec![],
                    true,
                    false,
                ),
                (
                    "SELECT id, a, s FROM t WHERE a IN (5, 7) ORDER BY a, s DESC, id",
                    vec![],
                    true,
                    false,
                ),
                (
                    "SELECT id, a, s FROM t WHERE a IN (5, 7) ORDER BY a DESC, s ASC, id DESC",
                    vec![],
                    true,
                    true,
                ),
                (
                    "SELECT id, a, b FROM t WHERE a IN (?1, ?2) ORDER BY a, b LIMIT ?3 OFFSET ?4",
                    vec![int(17), int(3), int(4), int(1)],
                    true,
                    false,
                ),
            ];
            for (sql, params, covering, reverse) in &stream_cases {
                assert_eq!(
                    frank_rows_in_order(&f, sql, params).await,
                    sqlite_rows_in_order(&r, sql, params),
                    "probe-order stream `{sql}` {params:?} at {path}"
                );
                let ops = program(&f, sql).await;
                assert!(
                    !has_op(&ops, "Rewind"),
                    "probe-order stream `{sql}` must not table-scan: {ops:?}"
                );
                assert_eq!(
                    sorter_count(&ops),
                    1,
                    "probe-order stream `{sql}` must skip the ORDER BY sorter (only 1 SorterOpen for probes): {ops:?}"
                );
                assert_eq!(
                    !has_op(&ops, "SeekRowid"),
                    *covering,
                    "probe-order stream `{sql}` covering={covering}: {ops:?}"
                );
                if *reverse {
                    assert!(
                        has_op(&ops, "SeekLE") && has_op(&ops, "Prev"),
                        "reverse probe-order stream `{sql}` must use SeekLE+Prev: {ops:?}"
                    );
                }
            }

            // Negative controls for probe-order streaming:
            // mixed direction `a ASC, b DESC` on `t_ab(a, b)` and `NULLS LAST` MUST keep ORDER BY sorter.
            for sql in [
                "SELECT id, a, b FROM t WHERE a IN (3, 17) ORDER BY a ASC, b DESC",
                "SELECT id, a, b FROM t WHERE a IN (3, 17) ORDER BY a NULLS LAST, b",
                "SELECT id, a, b FROM t WHERE a IN (3, 17) ORDER BY b, a",
            ] {
                assert_eq!(
                    frank_rows_in_order(&f, sql, &[]).await,
                    sqlite_rows_in_order(&r, sql, &[]),
                    "negative stream control `{sql}` at {path}"
                );
                let ops = program(&f, sql).await;
                assert_eq!(
                    sorter_count(&ops),
                    2,
                    "negative stream control `{sql}` must keep ORDER BY sorter: {ops:?}"
                );
            }

            // 4. Density-gated LIMIT choice (bd-mdnfh part 3):
            //    Sparse LIMIT probes and sorts (covering on t_ab, no Rewind/Last);
            //    Dense LIMIT keeps the early-stopping walk (0 SorterOpen)!
            //    Also verify NOT INDEXED declines the probe completely.
            let sparse_limit =
                "SELECT id, b FROM t WHERE a IN (3, 17) AND b = 5 ORDER BY id DESC LIMIT 20";
            assert_eq!(
                frank_rows_in_order(&f, sparse_limit, &[]).await,
                sqlite_rows_in_order(&r, sparse_limit, &[]),
                "sparse LIMIT at {path}"
            );
            let sparse_ops = program(&f, sparse_limit).await;
            assert!(
                !has_op(&sparse_ops, "Last") && !has_op(&sparse_ops, "Rewind"),
                "sparse LIMIT must probe, not walk table: {sparse_ops:?}"
            );
            assert!(
                !has_op(&sparse_ops, "SeekRowid"),
                "sparse LIMIT on t_ab must be covering: {sparse_ops:?}"
            );
            assert_eq!(sorter_count(&sparse_ops), 2, "sparse LIMIT uses probe+sort");

            // Dense LIMIT: 20 of 40 distinct `a` values (~50% density) with LIMIT 5
            // MUST keep the early-stopping walk (0 SorterOpen) for both ORDER BY id and ORDER BY c, id.
            let dense_id_limit = "SELECT id, b FROM t WHERE a IN (0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19) ORDER BY id LIMIT 5";
            let dense_c_limit = "SELECT id, b FROM t WHERE a IN (0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19) ORDER BY c, id LIMIT 5";
            for sql in [dense_id_limit, dense_c_limit] {
                assert_eq!(
                    frank_rows_in_order(&f, sql, &[]).await,
                    sqlite_rows_in_order(&r, sql, &[]),
                    "dense LIMIT `{sql}` at {path}"
                );
                let ops = program(&f, sql).await;
                assert_eq!(
                    sorter_count(&ops),
                    0,
                    "dense LIMIT `{sql}` must keep early-stopping walk (0 SorterOpen): {ops:?}"
                );
            }

            // Explicit NOT INDEXED must keep full table scan; INDEXED BY t_a must use t_a.
            let not_indexed = "SELECT id FROM t NOT INDEXED WHERE a IN (3, 17) ORDER BY id";
            assert_eq!(
                frank_rows_in_order(&f, not_indexed, &[]).await,
                sqlite_rows_in_order(&r, not_indexed, &[]),
                "NOT INDEXED at {path}"
            );
            let ni_ops = program(&f, not_indexed).await;
            assert!(
                has_op(&ni_ops, "Rewind") && sorter_count(&ni_ops) == 0,
                "NOT INDEXED must stay a table walk: {ni_ops:?}"
            );

            // Control: equality seek `a = 5 ORDER BY id` streams t_a with 0 sorters;
            // `a = 5 ORDER BY c` uses GH#498's eq-seek + 1 ORDER BY sorter (no IN probe).
            let eq_id_ctrl = "SELECT * FROM t WHERE a = 5 ORDER BY id";
            assert_eq!(
                frank_rows_in_order(&f, eq_id_ctrl, &[]).await,
                sqlite_rows_in_order(&r, eq_id_ctrl, &[]),
                "eq id control at {path}"
            );
            assert_eq!(
                sorter_count(&program(&f, eq_id_ctrl).await),
                0,
                "eq id control streams t_a with 0 sorters"
            );
            let eq_c_ctrl = "SELECT * FROM t WHERE a = 5 ORDER BY c, id";
            assert_eq!(
                frank_rows_in_order(&f, eq_c_ctrl, &[]).await,
                sqlite_rows_in_order(&r, eq_c_ctrl, &[]),
                "eq c control at {path}"
            );
            let eq_c_ops = program(&f, eq_c_ctrl).await;
            assert!(
                !has_op(&eq_c_ops, "Rewind") && sorter_count(&eq_c_ops) == 1,
                "eq c control must keep GH#498 seek + 1 ORDER BY sorter: {eq_c_ops:?}"
            );

            // Prepared reuse across fresh parameter sets.
            let prep_sql =
                "SELECT id, a, b FROM t WHERE a IN (?1, ?2) ORDER BY a DESC, b DESC LIMIT ?3";
            let stmt = f.prepare(prep_sql).await.unwrap();
            for params in [[int(3), int(17), int(5)], [int(39), int(39), int(3)]] {
                let got: Vec<Vec<String>> = stmt
                    .query_with_params(&params)
                    .await
                    .unwrap()
                    .iter()
                    .map(|row| row.values().iter().map(render).collect())
                    .collect();
                assert_eq!(
                    got,
                    sqlite_rows_in_order(&r, prep_sql, &params),
                    "prepared stream {params:?} at {path}"
                );
            }
            drop(stmt);
            f.close().await.unwrap();
        }

        // 5. Without ANALYZE on a file database: bd-prkvt b-tree sampling still
        //    picks covering probe-and-sort for a sparse LIMIT and keeps the
        //    early-stopping walk for a dense LIMIT.
        let no_analyze_file = dir.path().join("in_list_order_by_no_analyze.db");
        let (f_na, r_na) = setup(no_analyze_file.to_str().unwrap()).await;
        for ddl in ["CREATE INDEX t_a ON t(a);", "CREATE INDEX t_c ON t(c);"] {
            f_na.execute(ddl).await.unwrap();
            r_na.execute_batch(ddl).unwrap();
        }
        f_na.execute("PRAGMA fsqlite.parity_cert_strict = ON;")
            .await
            .unwrap();
        let sparse_na =
            "SELECT id, b FROM t WHERE a IN (3, 17) AND b = 5 ORDER BY id DESC LIMIT 20";
        assert_eq!(
            frank_rows_in_order(&f_na, sparse_na, &[]).await,
            sqlite_rows_in_order(&r_na, sparse_na, &[]),
            "no-ANALYZE sparse LIMIT"
        );
        let sparse_na_ops = program(&f_na, sparse_na).await;
        assert!(
            !has_op(&sparse_na_ops, "Last")
                && !has_op(&sparse_na_ops, "Rewind")
                && !has_op(&sparse_na_ops, "SeekRowid")
                && sorter_count(&sparse_na_ops) == 2,
            "no-ANALYZE sparse LIMIT must take covering probe-and-sort: {sparse_na_ops:?}"
        );
        let dense_na = "SELECT id, b FROM t WHERE a IN (0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19) ORDER BY c, id LIMIT 5";
        assert_eq!(
            frank_rows_in_order(&f_na, dense_na, &[]).await,
            sqlite_rows_in_order(&r_na, dense_na, &[]),
            "no-ANALYZE dense LIMIT"
        );
        assert_eq!(
            sorter_count(&program(&f_na, dense_na).await),
            0,
            "no-ANALYZE dense LIMIT must keep early-stopping walk"
        );
        f_na.close().await.unwrap();
    });
}

/// Live release-perf A/B probe for bd-odxzh + bd-mdnfh over a 200,000-row file
/// database against bundled `rusqlite` in the same process. Verifies exact
/// ordered row equality on every shape and prints per-shape median latencies.
#[test]
#[ignore = "live 200k-row A/B benchmark for bd-odxzh + bd-mdnfh; run with --profile release-perf -- --ignored --nocapture"]
fn bd_mdnfh_in_list_order_by_ab_live() {
    use std::fmt::Write as _;
    use std::time::Instant;

    const ROWS: i64 = 200_000;
    const ITERS: usize = 9;

    asupersync::test_utils::run_test(|| async {
        let dir = tempfile::tempdir().unwrap();
        let f_path = dir.path().join("bd_mdnfh_200k_f.db");
        let r_path = dir.path().join("bd_mdnfh_200k_r.db");

        // Build identical 200,000-row file databases in rusqlite, then open the
        // FrankenSQLite copy directly from the same bytes so setup is fast and
        // both engines read identical b-tree data.
        let r = rusqlite::Connection::open(&r_path).unwrap();
        r.execute_batch(
            "PRAGMA journal_mode = WAL;\n\
             PRAGMA synchronous = NORMAL;\n\
             CREATE TABLE t (id INTEGER PRIMARY KEY, a INTEGER, b INTEGER, c INTEGER, s TEXT, pad TEXT);\n\
             BEGIN;",
        )
        .unwrap();
        {
            let mut ins = r
                .prepare("INSERT INTO t VALUES (?1, ?2, ?3, ?4, ?5, '0123456789abcdef')")
                .unwrap();
            for i in 1..=ROWS {
                let a = i % 1000;
                let b = (i / 1000) % 100;
                let c = (i.wrapping_mul(2_654_435_761) & 0x7fff_ffff) % 10_000;
                let s = format!("k{}", i % 1000);
                ins.execute(rusqlite::params![i, a, b, c, s]).unwrap();
            }
        }
        r.execute_batch(
            "COMMIT;\n\
             CREATE INDEX t_a ON t(a);\n\
             CREATE INDEX t_ab ON t(a, b);\n\
             CREATE INDEX t_c ON t(c);\n\
             CREATE INDEX t_s ON t(s);\n\
             PRAGMA wal_checkpoint(TRUNCATE);",
        )
        .unwrap();

        // Snapshot a no-ANALYZE copy before running ANALYZE on the main fixture.
        let f_no_analyze_path = dir.path().join("bd_mdnfh_200k_no_analyze_f.db");
        let r_no_analyze_path = dir.path().join("bd_mdnfh_200k_no_analyze_r.db");
        std::fs::copy(&r_path, &f_no_analyze_path).unwrap();
        std::fs::copy(&r_path, &r_no_analyze_path).unwrap();

        r.execute_batch("ANALYZE; PRAGMA wal_checkpoint(TRUNCATE);")
            .unwrap();
        drop(r);
        std::fs::copy(&r_path, &f_path).unwrap();

        let r = rusqlite::Connection::open(&r_path).unwrap();
        let f = Connection::open(f_path.to_str().unwrap()).await.unwrap();
        f.execute("PRAGMA fsqlite.parity_cert_strict = ON;")
            .await
            .unwrap();

        let mut hundred_in = String::from("0");
        for v in 1..100 {
            let _ = write!(hundred_in, ", {v}");
        }
        let dense_c_limit =
            format!("SELECT id, b FROM t WHERE a IN ({hundred_in}) ORDER BY c, id LIMIT 5");
        let dense_id_limit =
            format!("SELECT id, b FROM t WHERE a IN ({hundred_in}) ORDER BY id LIMIT 5");

        let shapes: Vec<(&str, &str)> = vec![
            (
                "covering_a_order_id",
                "SELECT id FROM t WHERE a IN (3, 77, 500) ORDER BY id",
            ),
            (
                "covering_ab_between_order_id",
                "SELECT id FROM t WHERE a IN (3, 77, 500) AND b BETWEEN 1 AND 50 ORDER BY id",
            ),
            (
                "covering_s_order_id",
                "SELECT id FROM t WHERE s IN ('k1', 'k2') ORDER BY id",
            ),
            (
                "noncov_star_a_order_id",
                "SELECT * FROM t WHERE a IN (3, 77, 500) ORDER BY id",
            ),
            (
                "probe_order_cov_a_b",
                "SELECT id, a, b FROM t WHERE a IN (3, 77, 500) ORDER BY a, b",
            ),
            (
                "probe_order_star_a_b",
                "SELECT * FROM t WHERE a IN (3, 77, 500) ORDER BY a, b",
            ),
            (
                "probe_order_desc_a_b",
                "SELECT id, a, b FROM t WHERE a IN (3, 77, 500) ORDER BY a DESC, b DESC",
            ),
            (
                "nonrowid_order_c",
                "SELECT id, b FROM t WHERE a IN (3, 77, 500) ORDER BY c, id",
            ),
            (
                "sparse_limit_ab_id_desc_5",
                "SELECT id, b FROM t WHERE a IN (3, 77) AND b = 5 ORDER BY id DESC LIMIT 5",
            ),
            (
                "sparse_limit_ab_c_5",
                "SELECT id, b FROM t WHERE a IN (3, 77) AND b = 5 ORDER BY c, id LIMIT 5",
            ),
            (
                "dense_limit_a3_id_5",
                "SELECT id, b FROM t WHERE a IN (3, 77, 500) ORDER BY id LIMIT 5",
            ),
            ("dense_limit_a100_c_5", &dense_c_limit),
            ("dense_limit_a100_id_5", &dense_id_limit),
            (
                "control_eq_a77_order_id",
                "SELECT * FROM t WHERE a = 77 ORDER BY id",
            ),
        ];

        println!(
            "\n=== bd-odxzh + bd-mdnfh 200k ANALYZE live A/B (sqlite={}) ===",
            rusqlite::version()
        );
        println!(
            "{:<30} {:>6} {:>10} {:>10} {:>7} {:>10} {:>10} {:>7}",
            "shape", "rows", "prep_f_us", "prep_s_us", "p_x", "dir_f_us", "dir_s_us", "d_x"
        );

        for &(label, sql) in &shapes {
            let expected = sqlite_rows_in_order(&r, sql, &[]);
            let got = frank_rows_in_order(&f, sql, &[]).await;
            assert_eq!(got, expected, "ANALYZE row order mismatch for `{label}`");

            // Prepared timing
            let f_stmt = f.prepare(sql).await.unwrap();
            let mut r_stmt = r.prepare(sql).unwrap();
            let _ = f_stmt.query().await.unwrap();
            let _ = r_stmt
                .query_map([], |row| row.get::<_, rusqlite::types::Value>(0))
                .unwrap()
                .count();

            let mut f_prep = Vec::with_capacity(ITERS);
            let mut r_prep = Vec::with_capacity(ITERS);
            for _ in 0..ITERS {
                let t0 = Instant::now();
                let rows = f_stmt.query().await.unwrap();
                let dt = t0.elapsed().as_nanos();
                assert_eq!(rows.len(), expected.len());
                f_prep.push(dt);

                let t1 = Instant::now();
                let cnt = r_stmt
                    .query_map([], |row| {
                        let mut sum = 0_usize;
                        for idx in 0..row.as_ref().column_count() {
                            sum += usize::from(!matches!(
                                row.get_ref_unwrap(idx),
                                rusqlite::types::ValueRef::Null
                            ));
                        }
                        Ok(sum)
                    })
                    .unwrap()
                    .count();
                let dr = t1.elapsed().as_nanos();
                assert_eq!(cnt, expected.len());
                r_prep.push(dr);
            }
            f_prep.sort_unstable();
            r_prep.sort_unstable();
            drop(f_stmt);
            drop(r_stmt);

            // Direct (ad-hoc) timing
            let mut f_dir = Vec::with_capacity(ITERS);
            let mut r_dir = Vec::with_capacity(ITERS);
            for _ in 0..ITERS {
                let t0 = Instant::now();
                let rows = f.query(sql).await.unwrap();
                let dt = t0.elapsed().as_nanos();
                assert_eq!(rows.len(), expected.len());
                f_dir.push(dt);

                let t1 = Instant::now();
                let mut stmt = r.prepare(sql).unwrap();
                let cnt = stmt
                    .query_map([], |row| {
                        let mut sum = 0_usize;
                        for idx in 0..row.as_ref().column_count() {
                            sum += usize::from(!matches!(
                                row.get_ref_unwrap(idx),
                                rusqlite::types::ValueRef::Null
                            ));
                        }
                        Ok(sum)
                    })
                    .unwrap()
                    .count();
                let dr = t1.elapsed().as_nanos();
                assert_eq!(cnt, expected.len());
                r_dir.push(dr);
            }
            f_dir.sort_unstable();
            r_dir.sort_unstable();

            let fp = f_prep[ITERS / 2] as f64 / 1000.0;
            let rp = r_prep[ITERS / 2] as f64 / 1000.0;
            let fd = f_dir[ITERS / 2] as f64 / 1000.0;
            let rd = r_dir[ITERS / 2] as f64 / 1000.0;
            println!(
                "{label:<30} {:>6} {:>10.1} {:>10.1} {:>6.2}x {:>10.1} {:>10.1} {:>6.2}x",
                expected.len(),
                fp,
                rp,
                fp / rp.max(0.001),
                fd,
                rd,
                fd / rd.max(0.001),
            );
        }
        f.close().await.unwrap();

        // Also check sparse vs dense LIMIT WITHOUT ANALYZE (bd-prkvt b-tree sample).
        let r_na = rusqlite::Connection::open(&r_no_analyze_path).unwrap();
        let f_na = Connection::open(f_no_analyze_path.to_str().unwrap())
            .await
            .unwrap();
        f_na.execute("PRAGMA fsqlite.parity_cert_strict = ON;")
            .await
            .unwrap();
        println!("\n=== bd-mdnfh 200k NO-ANALYZE sparse vs dense LIMIT ===");
        for &(label, sql) in &[
            (
                "no_analyze_sparse_ab_id_desc_5",
                "SELECT id, b FROM t WHERE a IN (3, 77) AND b = 5 ORDER BY id DESC LIMIT 5",
            ),
            ("no_analyze_dense_a100_c_5", dense_c_limit.as_str()),
            ("no_analyze_dense_a100_id_5", dense_id_limit.as_str()),
        ] {
            let expected = sqlite_rows_in_order(&r_na, sql, &[]);
            let got = frank_rows_in_order(&f_na, sql, &[]).await;
            assert_eq!(got, expected, "NO-ANALYZE row order mismatch for `{label}`");
            let f_stmt = f_na.prepare(sql).await.unwrap();
            let mut r_stmt = r_na.prepare(sql).unwrap();
            let _ = f_stmt.query().await.unwrap();
            let mut f_prep = Vec::with_capacity(ITERS);
            let mut r_prep = Vec::with_capacity(ITERS);
            for _ in 0..ITERS {
                let t0 = Instant::now();
                let rows = f_stmt.query().await.unwrap();
                f_prep.push(t0.elapsed().as_nanos());
                assert_eq!(rows.len(), expected.len());
                let t1 = Instant::now();
                let cnt = r_stmt
                    .query_map([], |row| Ok(row.get_ref_unwrap(0).data_type()))
                    .unwrap()
                    .count();
                r_prep.push(t1.elapsed().as_nanos());
                assert_eq!(cnt, expected.len());
            }
            f_prep.sort_unstable();
            r_prep.sort_unstable();
            let fp = f_prep[ITERS / 2] as f64 / 1000.0;
            let rp = r_prep[ITERS / 2] as f64 / 1000.0;
            println!(
                "{label:<30} {:>6} {:>10.1} {:>10.1} {:>6.2}x",
                expected.len(),
                fp,
                rp,
                fp / rp.max(0.001),
            );
            drop(f_stmt);
        }
        f_na.close().await.unwrap();
    });
}
