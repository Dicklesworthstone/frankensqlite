//! bd-agg-index-range: `SELECT COUNT(*)/SUM(x) FROM t WHERE a <range>` on a single-column index now
//! seeks the bound and walks the range instead of full-scanning — for BOTH `SUM` (via
//! `codegen_select_aggregate`) and `COUNT(*)` (which now yields out of `codegen_select_count_star`).
//! HARD GATE: byte-identical to C SQLite (rusqlite) across `>`/`>=`/`<`/`<=`/`BETWEEN`, reversed
//! operand order, NULLs (excluded by the predicate), empty ranges (→ COUNT=0 / SUM=NULL), the COALESCE
//! wrapper, mixed storage classes in the indexed column, and a residual-predicate DECLINE
//! (`a > c AND x = k` must not drop `x = k`). Opcode gate: a lower-bounded range SeekGEs the index.

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

async fn frank_rows(conn: &Connection, sql: &str, params: &[i64]) -> Vec<Vec<String>> {
    let params: Vec<SqliteValue> = params.iter().map(|&n| SqliteValue::Integer(n)).collect();
    conn.query_with_params(sql, &params)
        .await
        .unwrap_or_else(|e| panic!("frank `{sql}` {params:?}: {e}"))
        .iter()
        .map(|row| row.values().iter().map(render).collect())
        .collect()
}

fn sqlite_rows(conn: &rusqlite::Connection, sql: &str, params: &[i64]) -> Vec<Vec<String>> {
    let mut stmt = conn.prepare(sql).unwrap();
    let n = stmt.column_count();
    stmt.query_map(rusqlite::params_from_iter(params), |row| {
        let mut out = Vec::with_capacity(n);
        for i in 0..n {
            out.push(match row.get_unwrap::<_, rusqlite::types::Value>(i) {
                rusqlite::types::Value::Null => "NULL".to_owned(),
                rusqlite::types::Value::Integer(x) => x.to_string(),
                rusqlite::types::Value::Real(f) => format!("{f:?}"),
                rusqlite::types::Value::Text(s) => format!("'{s}'"),
                rusqlite::types::Value::Blob(b) => format!(
                    "X'{}'",
                    b.iter().map(|x| format!("{x:02X}")).collect::<String>()
                ),
            });
        }
        Ok(out)
    })
    .unwrap()
    .map(Result::unwrap)
    .collect()
}

async fn has_op(conn: &Connection, sql: &str, want: &str) -> bool {
    conn.query(&format!("EXPLAIN {sql}"))
        .await
        .unwrap()
        .iter()
        .any(
            |row| matches!(row.values().get(1), Some(SqliteValue::Text(op)) if op.to_string() == want),
        )
}

/// `t(id, a, x)` with `idx_a` at `path` (`:memory:` or a file) on fsqlite and
/// in memory on stock: NULL keys, integers over [-400, 600), and reals and
/// text in the INTEGER column.
async fn seeded(path: &str, extra: &[&str]) -> (Connection, rusqlite::Connection) {
    let f = Connection::open(path).await.expect("frank");
    let r = rusqlite::Connection::open_in_memory().expect("sqlite");
    for stmt in [
        "CREATE TABLE t (id INTEGER PRIMARY KEY, a INTEGER, x INTEGER);",
        "CREATE INDEX idx_a ON t(a);",
    ]
    .iter()
    .chain(extra)
    {
        f.execute(stmt).await.unwrap();
        r.execute_batch(stmt).unwrap();
    }
    // ids 1..=4 have NULL a (excluded by every range); the rest spread over [-400, 600).
    for i in 1..=500_i64 {
        let a = if i <= 4 {
            "NULL".to_owned()
        } else {
            format!("{}", ((i.wrapping_mul(7)) % 1000) - 400)
        };
        let stmt = format!("INSERT INTO t VALUES ({i}, {a}, {});", i % 100);
        f.execute(&stmt).await.unwrap();
        r.execute_batch(&stmt).unwrap();
    }
    // Mixed storage classes in the indexed column (real + text stay off-integer under INTEGER affinity).
    for (id, val) in [
        (9001, "7.5"),
        (9002, "250.25"),
        (9003, "'apple'"),
        (9004, "'zzz'"),
    ] {
        let stmt = format!("INSERT INTO t VALUES ({id}, {val}, 0);");
        f.execute(&stmt).await.unwrap();
        r.execute_batch(&stmt).unwrap();
    }
    (f, r)
}

#[test]
fn agg_index_range_matches_sqlite() {
    asupersync::test_utils::run_test(|| async {
        let (f, r) = seeded(":memory:", &[]).await;

        for sql in [
            // All four operators, both aggregates.
            "SELECT COUNT(*) FROM t WHERE a > 100",
            "SELECT SUM(x) FROM t WHERE a > 100",
            "SELECT COUNT(*) FROM t WHERE a >= 100",
            "SELECT COUNT(*) FROM t WHERE a < 100",
            "SELECT SUM(x) FROM t WHERE a <= 100",
            // BETWEEN (two-sided) and its desugaring.
            "SELECT COUNT(*) FROM t WHERE a BETWEEN 0 AND 200",
            "SELECT SUM(x) FROM t WHERE a > 0 AND a < 200",
            "SELECT COUNT(*) FROM t WHERE a >= -100 AND a <= 100",
            // Reversed operand order.
            "SELECT COUNT(*) FROM t WHERE 100 < a",
            "SELECT SUM(x) FROM t WHERE 100 > a",
            // Empty ranges -> COUNT=0 / SUM=NULL, and the COALESCE wrapper.
            "SELECT COUNT(*) FROM t WHERE a > 100000",
            "SELECT SUM(x) FROM t WHERE a < -100000",
            "SELECT COALESCE(SUM(x), -1) FROM t WHERE a > 100000",
            // Boundaries where reals/text interleave the integer range.
            "SELECT COUNT(*) FROM t WHERE a > 249",
            "SELECT COUNT(*) FROM t WHERE a <= 600",
            // Covering SUM of the *indexed* column (read straight from the index, no table lookup).
            "SELECT SUM(a) FROM t WHERE a > 100",
            "SELECT SUM(a) FROM t WHERE a BETWEEN 0 AND 200",
            // Range + residual (placeholder-free): seek the range as a superset, re-apply the whole WHERE.
            // MUST NOT drop the residual `x = 5` / `x > 3` / `x IN (...)`.
            "SELECT COUNT(*) FROM t WHERE a > 100 AND x = 5",
            "SELECT SUM(x) FROM t WHERE a BETWEEN 0 AND 200 AND x > 3",
            "SELECT COUNT(*) FROM t WHERE a > 100 AND x IN (2, 5)",
            "SELECT COUNT(*) FROM t WHERE a >= -100 AND a <= 100 AND x <> 5",
        ] {
            assert_eq!(
                frank_rows(&f, sql, &[]).await,
                sqlite_rows(&r, sql, &[]),
                "diverged: `{sql}`"
            );
        }

        // Range + residual now SEEKS (superset) instead of scanning; the residual is enforced by the filter.
        assert!(
            has_op(
                &f,
                "SELECT COUNT(*) FROM t WHERE a > 100 AND x = 5",
                "SeekGE"
            )
            .await,
            "range + residual must seek the range (SeekGE) and filter the residual"
        );
        assert!(
            has_op(
                &f,
                "SELECT COUNT(*) FROM t WHERE a > 100 AND x = 5",
                "SeekRowid"
            )
            .await,
            "range + residual is non-covering (SeekRowid to read the residual column)"
        );

        // Opcode gate: a lower-bounded range seeks the index for both aggregates.
        assert!(
            has_op(&f, "SELECT COUNT(*) FROM t WHERE a > 100", "SeekGE").await,
            "COUNT(*) WHERE a>c must seek the index"
        );
        assert!(
            has_op(&f, "SELECT SUM(x) FROM t WHERE a > 100", "SeekGE").await,
            "SUM(x) WHERE a>c must seek the index"
        );
        // Covering gate: COUNT(*) and SUM(indexed col) read straight from the index (no SeekRowid);
        // SUM of a non-indexed column still needs the table lookup.
        assert!(
            !has_op(&f, "SELECT COUNT(*) FROM t WHERE a > 100", "SeekRowid").await,
            "COUNT(*) range walk must be covering (no SeekRowid)"
        );
        assert!(
            !has_op(&f, "SELECT SUM(a) FROM t WHERE a > 100", "SeekRowid").await,
            "SUM(indexed col) range walk must be covering (no SeekRowid)"
        );
        assert!(
            has_op(&f, "SELECT SUM(x) FROM t WHERE a > 100", "SeekRowid").await,
            "SUM(non-indexed col) range walk must open the table (SeekRowid)"
        );
    });
}

/// GROUP BY over a range on an index's leading key walks that range into the
/// grouping sorter instead of scanning the table, and matches stock in memory
/// and on a file database in strict mode: NULL keys and NULL bounds, reals and
/// text in the INTEGER column, a text bound taking the column's affinity,
/// parameters, HAVING, and residual conjuncts the range does not cover.
#[test]
fn grouped_index_range_matches_sqlite() {
    asupersync::test_utils::run_test(|| async {
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("grouped_index_range.db");
        // A file database runs GROUP BY as a VDBE program over the pager, and
        // strict mode turns a fallback to the connection's grouped evaluator
        // into an error, so its rows come from the program under test. A
        // `:memory:` connection groups through its in-memory mirror.
        for path in [":memory:", file.to_str().unwrap()] {
            let (f, r) = seeded(path, &["CREATE INDEX idx_ax ON t(a, x);"]).await;
            // A HAVING keeps the grouped evaluator (bd-w4pr1.13.32): before strict mode.
            let having = "SELECT x, sum(a) FROM t WHERE a >= -100 AND a <= 100 AND x <> 5 \
                      GROUP BY x HAVING count(*) > 2";
            // Ad hoc: until bd-ro43z a prepared HAVING on a file ran the substrate.
            let ad_hoc: Vec<Vec<String>> = f
                .query(having)
                .await
                .unwrap()
                .iter()
                .map(|row| row.values().iter().map(render).collect())
                .collect();
            assert_eq!(
                ad_hoc,
                sqlite_rows(&r, having, &[]),
                "diverged: `{having}` at {path}"
            );
            if path != ":memory:" {
                f.execute("PRAGMA fsqlite.parity_cert_strict = ON;")
                    .await
                    .unwrap();
                assert!(
                    f.query(having).await.is_err(),
                    "strict mode must refuse the grouped evaluator, which a HAVING still takes \
                     (bd-w4pr1.13.32); otherwise the rows below need not come from the program"
                );
            }
            let cases: &[(&str, &[i64])] = &[
                ("SELECT a, count(*) FROM t WHERE a < 100 GROUP BY a", &[]),
                ("SELECT a, count(*) FROM t WHERE a > 249 GROUP BY a", &[]),
                ("SELECT a, count(*) FROM t WHERE 100 > a GROUP BY a", &[]),
                (
                    "SELECT a, count(*) FROM t WHERE a BETWEEN -50 AND 50 GROUP BY a",
                    &[],
                ),
                ("SELECT a, count(*) FROM t WHERE a >= 590 GROUP BY a", &[]),
                ("SELECT a, count(*) FROM t WHERE a > '590' GROUP BY a", &[]),
                ("SELECT a, count(*) FROM t WHERE a > 100000 GROUP BY a", &[]),
                ("SELECT a, count(*) FROM t WHERE a < NULL GROUP BY a", &[]),
                (
                    "SELECT x, count(*), sum(a) FROM t WHERE a BETWEEN 0 AND 200 GROUP BY x",
                    &[],
                ),
                ("SELECT x, count(*) FROM t WHERE a > 100 GROUP BY x", &[]),
                (
                    "SELECT x % 7, max(a), min(id) FROM t WHERE a <= -300 GROUP BY x % 7",
                    &[],
                ),
                (
                    "SELECT a, count(*) FROM t WHERE a > ?1 AND a < ?2 GROUP BY a",
                    &[100, 200],
                ),
                ("SELECT x, count(*) FROM t WHERE a > ?1 GROUP BY x", &[400]),
                // The range alone is not the WHERE: the residual, the second
                // lower bound and the OR still filter every row.
                (
                    "SELECT a, count(*) FROM t WHERE a < 100 AND x = 5 GROUP BY a",
                    &[],
                ),
                (
                    "SELECT a, count(*) FROM t WHERE a > 10 AND a > 300 GROUP BY a",
                    &[],
                ),
                (
                    "SELECT a, count(*) FROM t WHERE a < 10 OR a > 500 GROUP BY a",
                    &[],
                ),
                (
                    "SELECT a, count(*) FROM t WHERE a > CAST('100' AS INTEGER) GROUP BY a",
                    &[],
                ),
            ];
            for &(sql, params) in cases {
                assert_eq!(
                    frank_rows(&f, sql, params).await,
                    sqlite_rows(&r, sql, params),
                    "diverged: `{sql}` {params:?} at {path}"
                );
            }

            // An exact range under a COUNT(*) grouping reads its keys from the
            // index: past the NULL keys, no table scan, no row lookup.
            let covering = "SELECT a, count(*) FROM t WHERE a < 100 GROUP BY a";
            assert!(
                has_op(&f, covering, "SeekGT").await,
                "`{covering}` must seek past NULL keys"
            );
            assert!(
                !has_op(&f, covering, "Rewind").await,
                "`{covering}` must not scan the table"
            );
            assert!(
                !has_op(&f, covering, "SeekRowid").await,
                "`{covering}` must be covering"
            );
            // The index that stores the group key wins: idx_ax covers `GROUP BY x`.
            let other_key = "SELECT x, count(*) FROM t WHERE a > 100 GROUP BY x";
            assert!(
                !has_op(&f, other_key, "SeekRowid").await,
                "`{other_key}` must use idx_ax"
            );
            // idx_ax stores x and a, so a plain-column aggregate reads the index
            // too; a residual conjunct reads the row.
            for (sql, reads_row) in [
                (
                    "SELECT x, count(*), sum(a) FROM t WHERE a BETWEEN 0 AND 200 GROUP BY x",
                    false,
                ),
                (
                    "SELECT a, count(*) FROM t WHERE a < 100 AND x = 5 GROUP BY a",
                    true,
                ),
            ] {
                assert!(
                    !has_op(&f, sql, "Rewind").await,
                    "`{sql}` must walk the range"
                );
                assert_eq!(
                    has_op(&f, sql, "SeekRowid").await,
                    reads_row,
                    "`{sql}` row lookup"
                );
            }
            f.close().await.unwrap();
        }
    });
}

/// GROUP BY whose keys an index orders forms its groups as the index's rows
/// arrive, with no sorter, and a LIMIT ends the walk: a whole-index walk when
/// the index covers the query or there is a LIMIT, a range on the leading key,
/// and an equality prefix with the keys next in the index. Rows match stock in
/// order, in memory and on a file database in strict mode (where the program
/// under test must produce them). Keys no index orders, expression keys and
/// arguments, IN lists and keys out of index order keep the sorter.
#[test]
fn grouped_index_order_streams_and_matches_sqlite() {
    asupersync::test_utils::run_test(|| async {
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("grouped_index_order.db");
        let streamed: &[(&str, &[i64])] = &[
            ("SELECT a, count(*) FROM t GROUP BY a", &[]),
            (
                "SELECT a, max(x), min(x), count(x), sum(x), total(x), avg(x) FROM t GROUP BY a",
                &[],
            ),
            ("SELECT a, max(x) FROM t GROUP BY a ORDER BY a LIMIT 5", &[]),
            ("SELECT a, count(*) FROM t GROUP BY a LIMIT 3 OFFSET 2", &[]),
            ("SELECT a, count(*) FROM t GROUP BY a LIMIT ?1", &[4]),
            ("SELECT a, sum(id) FROM t GROUP BY a LIMIT 4", &[]),
            ("SELECT a, count(DISTINCT x) FROM t GROUP BY a", &[]),
            (
                "SELECT a, count(*) FROM t WHERE x > 50 GROUP BY a LIMIT 5",
                &[],
            ),
            ("SELECT a, count(*) FROM t WHERE a < 100 GROUP BY a", &[]),
            (
                "SELECT a, max(x) FROM t WHERE a BETWEEN 0 AND 300 GROUP BY a",
                &[],
            ),
            ("SELECT a, count(*) FROM t WHERE a = 5 GROUP BY a", &[]),
            ("SELECT a, x, count(*) FROM t GROUP BY a, x LIMIT 7", &[]),
            ("SELECT a, sum(x) FROM t GROUP BY a ORDER BY a", &[]),
            ("SELECT a, count(*) FROM t GROUP BY a LIMIT 0", &[]),
        ];
        // The equality prefix takes the first index that pins it (idx_a), whose
        // order does not group x: either way the rows must match.
        let either = "SELECT x, count(*) FROM t WHERE a = 5 GROUP BY x";
        let sorted: &[&str] = &[
            "SELECT x, count(*) FROM t GROUP BY x",
            // (CAST: `a % 7` puts 7.5's 0.0 in the integer 0 group, whose
            // output value the in-memory mirror takes from another row than
            // stock, bd-t92q0.)
            "SELECT CAST(a AS INTEGER) % 7, count(*) FROM t GROUP BY CAST(a AS INTEGER) % 7",
            "SELECT a, sum(id * 2) FROM t GROUP BY a",
            "SELECT a, count(*) FROM t WHERE a IN (5, 3, 9) GROUP BY a",
            "SELECT x, a, count(*) FROM t GROUP BY x, a ORDER BY x, a LIMIT 5",
        ];
        for path in [":memory:", file.to_str().unwrap()] {
            let (f, r) = seeded(path, &["CREATE INDEX idx_ax ON t(a, x);"]).await;
            if path != ":memory:" {
                f.execute("PRAGMA fsqlite.parity_cert_strict = ON;")
                    .await
                    .unwrap();
                let having = "SELECT a, count(*) FROM t GROUP BY a HAVING count(*) > 1";
                assert!(
                    f.query(having).await.is_err(),
                    "strict mode must refuse the grouped evaluator, which a HAVING still takes \
                     (bd-w4pr1.13.32); otherwise the rows below need not come from the program"
                );
            }
            for &(sql, params) in streamed {
                assert_eq!(
                    frank_rows(&f, sql, params).await,
                    sqlite_rows(&r, sql, params),
                    "diverged: `{sql}` {params:?} at {path}"
                );
                assert!(
                    !has_op(&f, sql, "SorterOpen").await,
                    "`{sql}` must form its groups in index order, without a sorter"
                );
            }
            assert_eq!(
                frank_rows(&f, either, &[]).await,
                sqlite_rows(&r, either, &[]),
                "diverged: `{either}` at {path}"
            );
            for &sql in sorted {
                assert_eq!(
                    frank_rows(&f, sql, &[]).await,
                    sqlite_rows(&r, sql, &[]),
                    "diverged: `{sql}` at {path}"
                );
                assert!(
                    has_op(&f, sql, "SorterOpen").await,
                    "`{sql}` must keep the sorter"
                );
            }
            // An index that stores every input value is read alone.
            for sql in [
                "SELECT a, count(*) FROM t GROUP BY a",
                "SELECT a, max(x), min(x), count(x), sum(x), total(x), avg(x) FROM t GROUP BY a",
                "SELECT a, x, count(*) FROM t GROUP BY a, x LIMIT 7",
            ] {
                assert!(
                    !has_op(&f, sql, "SeekRowid").await,
                    "`{sql}` must read only the index"
                );
            }
            // A residual or a rowid argument reads the row.
            for sql in [
                "SELECT a, count(*) FROM t WHERE x > 50 GROUP BY a LIMIT 5",
                "SELECT a, sum(id) FROM t GROUP BY a LIMIT 4",
            ] {
                assert!(
                    has_op(&f, sql, "SeekRowid").await,
                    "`{sql}` must read the row"
                );
            }
            f.close().await.unwrap();
        }
    });
}
