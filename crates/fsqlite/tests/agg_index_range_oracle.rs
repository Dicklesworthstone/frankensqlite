//! bd-agg-index-range: `SELECT COUNT(*)/SUM(x) FROM t WHERE a <range>` on a single-column index now
//! seeks the bound and walks the range instead of full-scanning — for BOTH `SUM` (via
//! `codegen_select_aggregate`) and `COUNT(*)` (which now yields out of `codegen_select_count_star`).
//! HARD GATE: byte-identical to C SQLite (rusqlite) across `>`/`>=`/`<`/`<=`/`BETWEEN`, reversed
//! operand order, NULLs (excluded by the predicate), empty ranges (→ COUNT=0 / SUM=NULL), the COALESCE
//! wrapper, mixed storage classes in the indexed column, and a residual-predicate DECLINE
//! (`a > c AND x = k` must not drop `x = k`). Opcode gate: a lower-bounded range seeks the index;
//! a lone COUNT(*) over an exact range counts it from the index's pages (`IdxCountBefore`,
//! bd-2fb53).

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

/// The opcodes of the program, in address order.
async fn opcodes(conn: &Connection, sql: &str) -> Vec<String> {
    conn.query(&format!("EXPLAIN {sql}"))
        .await
        .unwrap()
        .iter()
        .map(|row| match row.values().get(1) {
            Some(SqliteValue::Text(op)) => op.to_string(),
            _ => String::new(),
        })
        .collect()
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

        // Opcode gate: a lower-bounded range seeks the index for both aggregates. COUNT(*)
        // seeks past the bound's own keys and counts the range from the index's pages
        // (bd-2fb53).
        assert!(
            has_op(&f, "SELECT COUNT(*) FROM t WHERE a > 100", "SeekGT").await
                && has_op(&f, "SELECT COUNT(*) FROM t WHERE a > 100", "IdxCountBefore").await,
            "COUNT(*) WHERE a>c must seek the index and count from it"
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
            // HAVING references a count absent from the result columns.
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
                let error = f.query(having).await.expect_err("unprojected HAVING count");
                assert!(
                    error
                        .to_string()
                        .contains("decision_reason=group_by_fallback"),
                    "{error}"
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
            // The keys in another order than idx_ax's: stock walks idx_ax and
            // emits (a, x) order, and so does the stream.
            ("SELECT x, a, count(*) FROM t GROUP BY x, a LIMIT 5", &[]),
            ("SELECT x, a, count(*) FROM t GROUP BY x, a", &[]),
        ];
        // The equality prefix takes the first index that pins it (idx_a), whose
        // order does not group x: either way the rows must match.
        let either = "SELECT x, count(*) FROM t WHERE a = 5 GROUP BY x";
        let sorted: &[&str] = &[
            "SELECT x, count(*) FROM t GROUP BY x",
            "SELECT a % 7, count(*) FROM t GROUP BY a % 7",
            "SELECT a, sum(id * 2) FROM t GROUP BY a",
            // An ORDER BY on x first needs the sorter.
            "SELECT x, a, count(*) FROM t GROUP BY x, a ORDER BY x, a LIMIT 5",
        ];
        for path in [":memory:", file.to_str().unwrap()] {
            let (f, r) = seeded(path, &["CREATE INDEX idx_ax ON t(a, x);"]).await;
            if path != ":memory:" {
                f.execute("PRAGMA fsqlite.parity_cert_strict = ON;")
                    .await
                    .unwrap();
                let having = "SELECT a, count(*) FROM t GROUP BY a HAVING count(*) > 1";
                let fallback = "SELECT a, count(*) FROM t GROUP BY a HAVING count(*) + 0 > 1";
                let expected = sqlite_rows(&r, having, &[]);
                assert_eq!(expected, sqlite_rows(&r, fallback, &[]));
                assert_eq!(frank_rows(&f, having, &[]).await, expected);
                let error = f.query(fallback).await.expect_err("HAVING arithmetic");
                assert!(
                    error
                        .to_string()
                        .contains("decision_reason=group_by_fallback"),
                    "{error}"
                );
            }
            for &(sql, params) in streamed {
                let mut got = frank_rows(&f, sql, params).await;
                let mut want = sqlite_rows(&r, sql, params);
                if path == ":memory:" && !sql.contains("ORDER BY") {
                    // The in-memory mirror orders groups by key, not by the
                    // index (`GROUP BY x, a` comes out in (x, a) order there):
                    // compare the groups only, and skip a LIMIT, which keeps
                    // the first groups in that order.
                    if !sql.contains("LIMIT") {
                        got.sort();
                        want.sort();
                        assert_eq!(got, want, "diverged: `{sql}` {params:?} at {path}");
                    }
                } else {
                    assert_eq!(got, want, "diverged: `{sql}` {params:?} at {path}");
                }
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
            // IN probes run in ascending value order, so their groups stream:
            // the only sorter left orders the probes.
            let in_list = "SELECT a, count(*) FROM t WHERE a IN (5, 3, 9, 3) GROUP BY a";
            assert_eq!(
                frank_rows(&f, in_list, &[]).await,
                sqlite_rows(&r, in_list, &[]),
                "diverged: `{in_list}` at {path}"
            );
            let sorters = opcodes(&f, in_list)
                .await
                .iter()
                .filter(|op| *op == "SorterOpen")
                .count();
            assert_eq!(sorters, 1, "`{in_list}` sorts its probes only");
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

/// With no ORDER BY, groups stream in a DESC index key's order, which is the
/// order stock emits them in (bd-gazf2): an equality prefix, an IN list and a
/// whole-index walk over `(k, suffix DESC)`; and in an index's key order when
/// the GROUP BY names its keys in another order (`GROUP BY x, a` over
/// `(a, x)`). An ORDER BY that is an ASC prefix of the keys keeps the
/// ascending sorter. Rows match stock in order, in memory (where the mirror's
/// grouped evaluator would sort them ascending by key) and on a file database
/// in strict mode.
#[test]
fn grouped_desc_index_keys_stream_in_index_order() {
    asupersync::test_utils::run_test(|| async {
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("grouped_desc_order.db");
        let schema = [
            "CREATE TABLE c (id INTEGER PRIMARY KEY, k INTEGER, suffix TEXT, n INTEGER);",
            "CREATE INDEX c_k_suffix ON c(k, suffix DESC);",
            "CREATE TABLE p (id INTEGER PRIMARY KEY, a INTEGER, x INTEGER);",
            "CREATE INDEX p_a_x ON p(a, x);",
        ];
        let mut rows = Vec::new();
        for i in 1..=60_i64 {
            rows.push(format!(
                "INSERT INTO p VALUES ({i}, {}, {});",
                i % 5,
                (i * 7) % 4
            ));
        }
        for i in 1..=400_i64 {
            let suffix = match i % 6 {
                0 => "NULL".to_owned(),
                1 => "'a'".to_owned(),
                2 => "'z'".to_owned(),
                3 => "'m'".to_owned(),
                4 => "'B'".to_owned(),
                _ => format!("'s{}'", i % 4),
            };
            rows.push(format!(
                "INSERT INTO c VALUES ({i}, {}, {suffix}, {i});",
                i % 9
            ));
        }
        let streamed = [
            "SELECT k, suffix, count(*) FROM c WHERE k = 2 GROUP BY k, suffix",
            "SELECT k, suffix, count(*) FROM c WHERE k IN (2, 3, 2) GROUP BY k, suffix",
            "SELECT k, suffix, count(*) FROM c GROUP BY k, suffix",
            "SELECT k, suffix, count(*) FROM c GROUP BY k, suffix LIMIT 7",
            "SELECT k, count(*) FROM c GROUP BY k",
            // The ORDER BY orders k only: suffix still streams descending.
            "SELECT k, suffix, count(*) FROM c GROUP BY k, suffix ORDER BY k",
        ];
        let sorted = [
            "SELECT k, suffix, count(*) FROM c WHERE k = 2 GROUP BY k, suffix ORDER BY k, suffix",
            "SELECT k, suffix, count(*) FROM c GROUP BY k, suffix ORDER BY k, suffix",
        ];
        // The GROUP BY names the index's keys in another order: stock walks
        // `(a, x)` and emits a-major order, where ascending key order is
        // x-major.
        let permuted = [
            "SELECT x, a, count(*) FROM p GROUP BY x, a",
            "SELECT x, a, sum(id) FROM p WHERE a IN (1, 3) GROUP BY x, a",
        ];
        for path in [":memory:", file.to_str().unwrap()] {
            let f = Connection::open(path).await.unwrap();
            let r = rusqlite::Connection::open_in_memory().unwrap();
            for stmt in schema
                .iter()
                .copied()
                .chain(rows.iter().map(String::as_str))
            {
                f.execute(stmt).await.unwrap();
                r.execute_batch(stmt).unwrap();
            }
            if path == ":memory:" {
                // Arithmetic on the HAVING aggregate loads the mirror's
                // rows. The shapes below then meet the mirror's grouped
                // evaluator, which orders groups by key whatever the index
                // (bd-gazf2), and not only the storage substrate a mirror
                // whose rows are not loaded hands them to.
                for sql in [
                    "SELECT k, count(*) FROM c GROUP BY k HAVING count(*) + 0 > 0",
                    "SELECT a, count(*) FROM p GROUP BY a HAVING count(*) + 0 > 0",
                    "SELECT k, count(*) FROM c GROUP BY k HAVING count(*) > 0",
                    "SELECT a, count(*) FROM p GROUP BY a HAVING count(*) > 0",
                ] {
                    let got = frank_rows(&f, sql, &[]).await;
                    assert_eq!(
                        got,
                        sqlite_rows(&r, sql, &[]),
                        "diverged: `{sql}` at {path}"
                    );
                }
            } else {
                f.execute("PRAGMA fsqlite.parity_cert_strict = ON;")
                    .await
                    .unwrap();
            }
            for sql in streamed.iter().chain(&sorted).chain(&permuted) {
                let got = frank_rows(&f, sql, &[]).await;
                let want = sqlite_rows(&r, sql, &[]);
                assert_eq!(got, want, "diverged: `{sql}` at {path}");
                // A prepared statement chooses its route when it is prepared.
                let prepared = f.prepare(sql).await.unwrap();
                let via_prepared: Vec<Vec<String>> = prepared
                    .query()
                    .await
                    .unwrap_or_else(|e| panic!("prepared `{sql}` at {path}: {e}"))
                    .iter()
                    .map(|row| row.values().iter().map(render).collect())
                    .collect();
                assert_eq!(via_prepared, want, "diverged prepared: `{sql}` at {path}");
            }
            // (The IN list keeps the sorter that orders its probes.)
            for sql in [
                streamed[0],
                streamed[2],
                streamed[3],
                streamed[4],
                streamed[5],
            ] {
                assert!(
                    !has_op(&f, sql, "SorterOpen").await,
                    "`{sql}` must stream its groups in index order"
                );
            }
            for sql in sorted {
                assert!(
                    has_op(&f, sql, "SorterOpen").await,
                    "`{sql}` must sort its groups ascending"
                );
            }
            f.close().await.unwrap();
        }
    });
}

/// bd-vbqub: `GROUP BY` on an indexed column (or rowid) with `ORDER BY ... ASC`
/// or `ORDER BY ... DESC` and `LIMIT` / `OFFSET` / `HAVING` streams directly
/// from the index (`Rewind`/`Next` or `Last`/`Prev`) with no sorter, stopping
/// at `LIMIT`, while matching live bundled stock SQLite 3.54.1 across `:memory:`
/// and file-backed × direct and prepared statements (including NULLs, one-sided
/// and two-sided ranges, composite mixed-direction indexes, unindexed DESC
/// fallback, and `NOT INDEXED`).
#[test]
fn bd_vbqub_grouped_order_by_asc_and_desc_limit_streams_and_matches_stock() {
    asupersync::test_utils::run_test(|| async {
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("bd_vbqub_order_limit.db");
        let ddl = [
            "CREATE TABLE t (id INTEGER PRIMARY KEY, a INTEGER, b INTEGER, c INTEGER);",
            "CREATE INDEX t_a ON t(a);",
            "CREATE INDEX t_ab_desc ON t(a, b DESC);",
            "CREATE INDEX t_a_desc ON t(a DESC);",
        ];
        let mut inserts = Vec::new();
        for i in 1..=300_i64 {
            let a = if i % 25 == 0 {
                "NULL".to_owned()
            } else {
                (i % 40).to_string()
            };
            let b = if i % 17 == 0 {
                "NULL".to_owned()
            } else {
                ((i * 7) % 10).to_string()
            };
            inserts.push(format!(
                "INSERT INTO t VALUES ({i}, {a}, {b}, {});",
                (i * 13) % 97
            ));
        }

        let streamed_no_sorter = [
            // Core bd-vbqub ASC & DESC LIMIT shapes on ASC index.
            "SELECT a, count(*), sum(c) FROM t GROUP BY a ORDER BY a LIMIT 5",
            "SELECT a, count(*), sum(c) FROM t GROUP BY a ORDER BY a DESC LIMIT 5",
            "SELECT a, count(*), sum(c) FROM t GROUP BY a ORDER BY a LIMIT 5 OFFSET 2",
            "SELECT a, count(*), sum(c) FROM t GROUP BY a ORDER BY a DESC LIMIT 5 OFFSET 2",
            // All groups (including trailing NULL group in DESC, leading NULL group in ASC).
            "SELECT a, count(*), sum(c) FROM t GROUP BY a ORDER BY a LIMIT 100",
            "SELECT a, count(*), sum(c) FROM t GROUP BY a ORDER BY a DESC LIMIT 100",
            // HAVING skips non-matching groups while streaming.
            "SELECT a, count(*), sum(c) FROM t GROUP BY a HAVING sum(c) > 300 ORDER BY a DESC LIMIT 5",
            // LIMIT 0 edge case.
            "SELECT a, count(*), sum(c) FROM t GROUP BY a ORDER BY a DESC LIMIT 0",
            // Covering index (no table lookup) DESC without LIMIT.
            "SELECT a, count(*) FROM t GROUP BY a ORDER BY a DESC",
            // Bounded range ASC & DESC with LIMIT, plus one-sided upper/lower bounds (NULL safety!).
            "SELECT a, count(*), sum(c) FROM t WHERE a BETWEEN 10 AND 25 GROUP BY a ORDER BY a LIMIT 5",
            "SELECT a, count(*), sum(c) FROM t WHERE a BETWEEN 10 AND 25 GROUP BY a ORDER BY a DESC LIMIT 5",
            "SELECT a, count(*), sum(c) FROM t WHERE a <= 15 GROUP BY a ORDER BY a DESC LIMIT 5",
            "SELECT a, count(*), sum(c) FROM t WHERE a <= 15 GROUP BY a ORDER BY a DESC LIMIT 100",
            "SELECT a, count(*), sum(c) FROM t WHERE a < 15 GROUP BY a ORDER BY a DESC LIMIT 5",
            "SELECT a, count(*), sum(c) FROM t WHERE a >= 25 GROUP BY a ORDER BY a DESC LIMIT 5",
            "SELECT a, count(*), sum(c) FROM t WHERE a > 25 GROUP BY a ORDER BY a DESC LIMIT 5",
            // Explicit INDEXED BY on ASC and DESC indexes.
            "SELECT a, count(*), sum(c) FROM t INDEXED BY t_a GROUP BY a ORDER BY a DESC LIMIT 5",
            "SELECT a, count(*), sum(c) FROM t INDEXED BY t_a_desc GROUP BY a ORDER BY a DESC LIMIT 5",
            "SELECT a, count(*), sum(c) FROM t INDEXED BY t_a_desc GROUP BY a ORDER BY a LIMIT 5",
            // Composite mixed-direction index reversed: (a ASC, b DESC) walked backward -> (a DESC, b ASC).
            "SELECT a, b, count(*), sum(c) FROM t GROUP BY a, b ORDER BY a DESC, b ASC LIMIT 12",
            "SELECT a, b, count(*), sum(c) FROM t GROUP BY a, b ORDER BY a ASC, b DESC LIMIT 12",
            // Rowid GROUP BY ORDER BY id DESC LIMIT 5.
            "SELECT id, count(*), sum(c) FROM t GROUP BY id ORDER BY id DESC LIMIT 5",
            "SELECT id, count(*), sum(c) FROM t WHERE c > 50 GROUP BY id ORDER BY id DESC LIMIT 5",
        ];

        let sorted_substrate = [
            // Unindexed column DESC + LIMIT (uses descending sorter, still matches stock).
            "SELECT c, count(*) FROM t GROUP BY c ORDER BY c DESC LIMIT 8",
            // NOT INDEXED negative control (must not open index, still matches stock).
            "SELECT a, count(*), sum(c) FROM t NOT INDEXED GROUP BY a ORDER BY a DESC LIMIT 5",
            // Composite direction mismatch against (a ASC, b DESC): neither forward nor reverse matches.
            "SELECT a, b, count(*) FROM t GROUP BY a, b ORDER BY a DESC, b DESC LIMIT 10",
            // Empty range (no matching rows).
            "SELECT a, count(*), sum(c) FROM t WHERE a BETWEEN 500 AND 600 GROUP BY a ORDER BY a DESC LIMIT 5",
        ];

        let fallback_nulls_order = [
            // NULLS FIRST / NULLS LAST overrides (falls back cleanly to core, matches stock).
            "SELECT a, count(*), sum(c) FROM t GROUP BY a ORDER BY a DESC NULLS FIRST LIMIT 5",
            "SELECT a, count(*), sum(c) FROM t GROUP BY a ORDER BY a ASC NULLS LAST LIMIT 5",
        ];

        for path in [":memory:", file.to_str().unwrap()] {
            let f = Connection::open(path).await.unwrap();
            let r = rusqlite::Connection::open_in_memory().unwrap();
            for stmt in ddl
                .iter()
                .copied()
                .chain(inserts.iter().map(String::as_str))
            {
                f.execute(stmt).await.unwrap();
                r.execute_batch(stmt).unwrap();
            }
            if path != ":memory:" {
                f.execute("PRAGMA fsqlite.parity_cert_strict = ON;")
                    .await
                    .unwrap();
            }

            for sql in streamed_no_sorter.iter().chain(&sorted_substrate) {
                let want = sqlite_rows(&r, sql, &[]);
                let got = frank_rows(&f, sql, &[]).await;
                assert_eq!(got, want, "direct mismatch on `{sql}` ({path})");

                let prepared = f.prepare(sql).await.unwrap();
                let got_prep: Vec<Vec<String>> = prepared
                    .query()
                    .await
                    .unwrap_or_else(|e| panic!("prepared `{sql}` ({path}): {e}"))
                    .iter()
                    .map(|row| row.values().iter().map(render).collect())
                    .collect();
                assert_eq!(got_prep, want, "prepared mismatch on `{sql}` ({path})");
            }

            if path != ":memory:" {
                f.execute("PRAGMA fsqlite.parity_cert_strict = OFF;")
                    .await
                    .unwrap();
            }
            for sql in fallback_nulls_order {
                let want = sqlite_rows(&r, sql, &[]);
                let got = frank_rows(&f, sql, &[]).await;
                assert_eq!(got, want, "fallback direct mismatch on `{sql}` ({path})");

                let prepared = f.prepare(sql).await.unwrap();
                let got_prep: Vec<Vec<String>> = prepared
                    .query()
                    .await
                    .unwrap_or_else(|e| panic!("fallback prepared `{sql}` ({path}): {e}"))
                    .iter()
                    .map(|row| row.values().iter().map(render).collect())
                    .collect();
                assert_eq!(
                    got_prep, want,
                    "fallback prepared mismatch on `{sql}` ({path})"
                );
            }

            // Prove no SorterOpen on streamed shapes.
            for sql in streamed_no_sorter {
                assert!(
                    !has_op(&f, sql, "SorterOpen").await,
                    "expected streamed GROUP BY without SorterOpen for `{sql}` ({path})"
                );
            }
            // Prove NOT INDEXED negative control opens no secondary index.
            assert!(
                !has_op(
                    &f,
                    "SELECT a, count(*), sum(c) FROM t NOT INDEXED GROUP BY a ORDER BY a DESC LIMIT 5",
                    "IdxRowid",
                )
                .await
            );

            f.close().await.unwrap();
        }
    });
}

/// bd-2fb53: a lone COUNT(*) whose WHERE is exactly a range on an index's key,
/// `IS NULL` or `IS NOT NULL` counts from the index's pages (`IdxCountBefore`,
/// whole leaves at a time) and matches stock: on a file database with indexes
/// of many leaves and interior levels, inclusive and exclusive bounds, ranges
/// starting and ending inside one leaf, across leaves and at both ends,
/// empty ranges, NULL bounds, reals and text in the INTEGER column, bound
/// parameters, BINARY text ranges over mixed-case keys with an integer and a
/// blob at the ends, a composite index's leading NULLs, and an empty table. A
/// residual, a DESC index, a NOCASE text range (which the range lane declines)
/// and a non-COUNT aggregate keep their programs.
#[test]
fn count_star_over_an_exact_index_range_counts_leaves_and_matches_sqlite() {
    asupersync::test_utils::run_test(|| async {
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("count_index_range.db");
        let f = Connection::open(file.to_str().unwrap()).await.unwrap();
        let r = rusqlite::Connection::open_in_memory().unwrap();
        for stmt in [
            "CREATE TABLE t (id INTEGER PRIMARY KEY, a INTEGER, s TEXT COLLATE NOCASE, x INTEGER, \
             d INTEGER, w TEXT);",
            "CREATE INDEX idx_a ON t(a);",
            "CREATE INDEX idx_s ON t(s);",
            "CREATE INDEX idx_w ON t(w);",
            "CREATE INDEX idx_xa ON t(x, a);",
            "CREATE INDEX idx_d ON t(d DESC);",
            "CREATE TABLE empty (id INTEGER PRIMARY KEY, a INTEGER);",
            "CREATE INDEX idx_empty ON empty(a);",
        ] {
            f.execute(stmt).await.unwrap();
            r.execute_batch(stmt).unwrap();
        }
        let mut batch = String::from("BEGIN;");
        for i in 1..=12_000_i64 {
            // Every 9th row has a NULL key; the rest repeat each value about 12 times.
            let a = if i % 9 == 0 {
                "NULL".to_owned()
            } else {
                format!("{}", (i * 37) % 1000 - 300)
            };
            let x = if i % 5 == 0 {
                "NULL".to_owned()
            } else {
                format!("{}", i % 3)
            };
            let s = if i % 4 == 0 { "K" } else { "k" };
            batch.push_str(&format!(
                "INSERT INTO t VALUES ({i}, {a}, '{s}{}', {x}, {}, '{s}{}');",
                i % 50,
                i % 70,
                i % 60
            ));
        }
        batch.push_str(
            "INSERT INTO t VALUES (90001, 7.5, NULL, 1, 1, NULL), (90002, 'apple', NULL, 1, 1, 7), \
             (90003, 'zzz', NULL, 1, 1, X'01'); COMMIT;",
        );
        f.execute_batch(&batch).await.unwrap();
        r.execute_batch(&batch).unwrap();
        f.execute("PRAGMA fsqlite.parity_cert_strict = ON;")
            .await
            .unwrap();

        let counted: &[(&str, &[i64])] = &[
            ("SELECT count(*) FROM t WHERE a > 100", &[]),
            ("SELECT count(*) FROM t WHERE a >= 100", &[]),
            ("SELECT count(*) FROM t WHERE a < 100", &[]),
            ("SELECT count(*) FROM t WHERE a <= 100", &[]),
            ("SELECT count(*) FROM t WHERE a BETWEEN -50 AND 50", &[]),
            ("SELECT count(*) FROM t WHERE a > -300 AND a < 699", &[]),
            ("SELECT count(*) FROM t WHERE a BETWEEN 10 AND 10", &[]),
            ("SELECT count(*) FROM t WHERE a > 10 AND a < 11", &[]),
            ("SELECT count(*) FROM t WHERE a >= 698", &[]),
            ("SELECT count(*) FROM t WHERE a <= -300", &[]),
            ("SELECT count(*) FROM t WHERE a > 100000", &[]),
            ("SELECT count(*) FROM t WHERE a < -100000", &[]),
            ("SELECT count(*) FROM t WHERE a > 249", &[]),
            ("SELECT count(*) FROM t WHERE 100 < a", &[]),
            ("SELECT count(*) FROM t WHERE a IS NULL", &[]),
            ("SELECT count(*) FROM t WHERE a IS NOT NULL", &[]),
            ("SELECT count(*) FROM t WHERE x IS NULL", &[]),
            ("SELECT count(*) FROM t WHERE x IS NOT NULL", &[]),
            (
                "SELECT count(*) AS n FROM t WHERE a BETWEEN ?1 AND ?2",
                &[-20, 400],
            ),
            ("SELECT count(*) FROM t WHERE a > ?1", &[650]),
            // BINARY text keys: 'K…' sort before 'k…'; an integer and a blob sit at the ends.
            ("SELECT count(*) FROM t WHERE w >= 'k1' AND w < 'k2'", &[]),
            ("SELECT count(*) FROM t WHERE w > 'K30'", &[]),
            ("SELECT count(*) FROM t WHERE w IS NOT NULL", &[]),
            ("SELECT count(*) FROM empty WHERE a > 0", &[]),
            ("SELECT count(*) FROM empty WHERE a IS NULL", &[]),
        ];
        for (sql, params) in counted {
            assert_eq!(
                frank_rows(&f, sql, params).await,
                sqlite_rows(&r, sql, params),
                "diverged: `{sql}` {params:?}"
            );
            assert!(
                has_op(&f, sql, "IdxCountBefore").await,
                "an exact index range counts from the index's pages: `{sql}`"
            );
        }
        // A NULL bound matches no row.
        for sql in [
            "SELECT count(*) FROM t WHERE a > NULL",
            "SELECT count(*) FROM t WHERE a BETWEEN NULL AND 5",
        ] {
            assert_eq!(
                frank_rows(&f, sql, &[]).await,
                sqlite_rows(&r, sql, &[]),
                "diverged: `{sql}`"
            );
        }

        let walked = [
            "SELECT count(*) FROM t WHERE a > 100 AND x = 1",
            "SELECT count(*) FROM t WHERE d > 30",
            // A NOCASE text range is not an exact range for this lane (the range target declines
            // it); its answer must still be stock's.
            "SELECT count(*) FROM t WHERE s >= 'k1' AND s < 'k2'",
            "SELECT count(*) FROM t WHERE s > 'K30'",
            "SELECT sum(a) FROM t WHERE a > 100",
            "SELECT count(a) FROM t WHERE a > 100",
        ];
        for sql in walked {
            assert_eq!(
                frank_rows(&f, sql, &[]).await,
                sqlite_rows(&r, sql, &[]),
                "diverged: `{sql}`"
            );
            assert!(
                !has_op(&f, sql, "IdxCountBefore").await,
                "not an exact lone COUNT(*) range: `{sql}`"
            );
        }
        f.close().await.unwrap();
    });
}

/// bd-zgbhb / bd-text-probe-numeric-index-qk7vd: a lone COUNT(*) whose WHERE
/// is exactly an equality prefix of an index, with every value exact after
/// index-key affinity coercion, counts the pinned entries from the index's
/// pages: values whose runs span leaves, an absent value, a two-column prefix,
/// text, typeless, REAL and cross-type numeric-text keys, and parameters,
/// against stock on a file database in strict mode. A suppressed key (`+a`),
/// a mismatched-affinity CAST on a TEXT key (`s = CAST(3 AS INTEGER)`), and an
/// equality beside a residual keep their programs.
#[test]
fn count_star_over_an_exact_equality_prefix_counts_leaves_and_matches_sqlite() {
    asupersync::test_utils::run_test(|| async {
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("count_equality_prefix.db");
        let f = Connection::open(file.to_str().unwrap()).await.unwrap();
        let r = rusqlite::Connection::open_in_memory().unwrap();
        for stmt in [
            "CREATE TABLE t (id INTEGER PRIMARY KEY, a INTEGER, x INTEGER, s TEXT, u, w REAL);",
            "CREATE INDEX idx_a ON t(a);",
            "CREATE INDEX idx_xa ON t(x, a);",
            "CREATE INDEX idx_s ON t(s);",
            "CREATE INDEX idx_u ON t(u);",
            "CREATE INDEX idx_w ON t(w);",
        ] {
            f.execute(stmt).await.unwrap();
            r.execute_batch(stmt).unwrap();
        }
        let mut batch = String::from("BEGIN;");
        for i in 1..=12_000_i64 {
            let a = if i % 13 == 0 {
                "NULL".to_owned()
            } else {
                format!("{}", i % 7)
            };
            // The typeless column holds 5 as an integer and as text.
            let u = if i % 2 == 0 {
                format!("{}", i % 9)
            } else {
                format!("'{}'", i % 9)
            };
            batch.push_str(&format!(
                "INSERT INTO t VALUES ({i}, {a}, {}, 'k{}', {u}, {});",
                i % 3,
                i % 11,
                f64::from(u8::try_from(i % 6).unwrap()) / 2.0
            ));
        }
        batch.push_str("COMMIT;");
        f.execute_batch(&batch).await.unwrap();
        r.execute_batch(&batch).unwrap();
        f.execute("PRAGMA fsqlite.parity_cert_strict = ON;")
            .await
            .unwrap();

        let counted: &[(&str, &[i64])] = &[
            ("SELECT count(*) FROM t WHERE a = 3", &[]),
            ("SELECT count(*) FROM t WHERE a = '3'", &[]),
            ("SELECT count(*) FROM t WHERE 3 = a", &[]),
            ("SELECT count(*) FROM t WHERE a = 999", &[]),
            ("SELECT count(*) FROM t WHERE x = 1 AND a = 4", &[]),
            ("SELECT count(*) FROM t WHERE a = 4 AND x = 1", &[]),
            ("SELECT count(*) FROM t WHERE s = 'k3'", &[]),
            ("SELECT count(*) FROM t WHERE u = 5", &[]),
            ("SELECT count(*) FROM t WHERE w = 1.5", &[]),
            ("SELECT count(*) FROM t WHERE w = 2", &[]),
            ("SELECT count(*) FROM t WHERE a = ?1", &[6]),
            ("SELECT count(*) FROM t WHERE x = ?1 AND a = ?2", &[2, 0]),
        ];
        for (sql, params) in counted {
            assert_eq!(
                frank_rows(&f, sql, params).await,
                sqlite_rows(&r, sql, params),
                "diverged: `{sql}` {params:?}"
            );
            assert!(
                has_op(&f, sql, "IdxCountBefore").await,
                "an exact equality prefix counts from the index's pages: `{sql}`"
            );
        }
        for sql in [
            "SELECT count(*) FROM t WHERE +a = 3",
            "SELECT count(*) FROM t WHERE s = CAST(3 AS INTEGER)",
            "SELECT count(*) FROM t WHERE a = 3 AND s = 'k1'",
        ] {
            assert_eq!(
                frank_rows(&f, sql, &[]).await,
                sqlite_rows(&r, sql, &[]),
                "diverged: `{sql}`"
            );
            assert!(
                !has_op(&f, sql, "IdxCountBefore").await,
                "not an exact equality prefix: `{sql}`"
            );
        }
        f.close().await.unwrap();
    });
}

/// bd-qyd3s: a lone COUNT(*) whose WHERE is exactly `col IN (<integer
/// literals>)` on an INTEGER column counts each distinct member's run from the
/// index's pages and adds the counts: runs spanning leaves on a single-column
/// index and on a composite one with NULL trailing keys, duplicate, absent and
/// past-the-end members, and a one-member list, against stock on a file
/// database in strict mode. A text member, a residual and a TEXT column keep
/// their programs.
#[test]
fn count_star_over_an_integer_in_list_counts_each_member_and_matches_sqlite() {
    asupersync::test_utils::run_test(|| async {
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("count_in_list.db");
        let f = Connection::open(file.to_str().unwrap()).await.unwrap();
        let r = rusqlite::Connection::open_in_memory().unwrap();
        for stmt in [
            "CREATE TABLE t (id INTEGER PRIMARY KEY, a INTEGER, x INTEGER, s TEXT);",
            "CREATE INDEX idx_a ON t(a);",
            "CREATE INDEX idx_xa ON t(x, a);",
            "CREATE INDEX idx_s ON t(s);",
        ] {
            f.execute(stmt).await.unwrap();
            r.execute_batch(stmt).unwrap();
        }
        let mut batch = String::from("BEGIN;");
        for i in 1..=12_000_i64 {
            let a = if i % 13 == 0 {
                "NULL".to_owned()
            } else {
                format!("{}", i % 7)
            };
            batch.push_str(&format!(
                "INSERT INTO t VALUES ({i}, {a}, {}, 'k{}');",
                i % 3,
                i % 11
            ));
        }
        batch.push_str("COMMIT;");
        f.execute_batch(&batch).await.unwrap();
        r.execute_batch(&batch).unwrap();
        f.execute("PRAGMA fsqlite.parity_cert_strict = ON;")
            .await
            .unwrap();

        // (sql, distinct members, each counted by its own IdxCountBefore)
        let counted: &[(&str, usize)] = &[
            ("SELECT count(*) FROM t WHERE a IN (1, 3, 5)", 3),
            ("SELECT count(*) FROM t WHERE a IN (3, 3, 5, 3)", 2),
            ("SELECT count(*) FROM t WHERE a IN (999, 3)", 2),
            ("SELECT count(*) FROM t WHERE a IN (6, 100)", 2),
            ("SELECT count(*) FROM t WHERE a IN (4)", 1),
            ("SELECT count(*) FROM t WHERE a IN (0, 1, 2, 3, 4, 5, 6)", 7),
            ("SELECT count(*) FROM t WHERE x IN (0, 2)", 2),
            ("SELECT count(*) FROM t WHERE a = 1 OR a = 3 OR a = 1", 2),
        ];
        for (sql, members) in counted {
            assert_eq!(
                frank_rows(&f, sql, &[]).await,
                sqlite_rows(&r, sql, &[]),
                "diverged: `{sql}`"
            );
            let ops = opcodes(&f, sql).await;
            assert_eq!(
                ops.iter().filter(|op| *op == "IdxCountBefore").count(),
                *members,
                "each distinct member's run is counted from the index's pages: `{sql}` {ops:?}"
            );
            assert!(
                !ops.iter().any(|op| op == "AggStep"),
                "no entry is stepped: `{sql}` {ops:?}"
            );
        }
        for sql in [
            "SELECT count(*) FROM t WHERE a IN (3, '5')",
            "SELECT count(*) FROM t WHERE a IN (1, 3) AND s = 'k1'",
            "SELECT count(*) FROM t WHERE s IN ('k1', 'k2')",
        ] {
            assert_eq!(
                frank_rows(&f, sql, &[]).await,
                sqlite_rows(&r, sql, &[]),
                "diverged: `{sql}`"
            );
            assert!(
                !has_op(&f, sql, "IdxCountBefore").await,
                "not an integer-literal IN list over an INTEGER column: `{sql}`"
            );
        }
        f.close().await.unwrap();
    });
}

/// bd-t92q0: in `:memory:` (both unloaded mirror and hydrated mirror, with and
/// without `HAVING`, ad hoc and prepared) and on file, `GROUP BY a % 7` with
/// mixed INTEGER / REAL / TEXT rows must print the exact key storage class
/// (INTEGER `0` vs REAL `0.0`) that stock SQLite prints.
#[test]
fn bd_t92q0_memory_group_by_expression_key_spelling_matches_stock() {
    asupersync::test_utils::run_test(|| async {
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("t92q0.db");
        for path in [":memory:", file.to_str().unwrap()] {
            let (f, r) = seeded(path, &[]).await;
            let queries = [
                "SELECT a % 7, count(*) FROM t GROUP BY a % 7",
                "SELECT a % 7, count(*) FROM t GROUP BY a % 7 HAVING count(*) > 0",
                "SELECT a % 7, count(*) FROM t WHERE id > 0 GROUP BY a % 7",
            ];
            // First pass: immediately after seeding (unloaded mirror on :memory:).
            for sql in queries {
                let want = sqlite_rows(&r, sql, &[]);
                assert_eq!(
                    frank_rows(&f, sql, &[]).await,
                    want,
                    "unloaded mirror diverged: `{sql}` at {path}"
                );
                let stmt = f.prepare(sql).await.unwrap();
                let prepared: Vec<Vec<String>> = stmt
                    .query()
                    .await
                    .unwrap()
                    .iter()
                    .map(|row| row.values().iter().map(render).collect())
                    .collect();
                assert_eq!(prepared, want, "prepared diverged: `{sql}` at {path}");
            }
            // Second pass: force the in-memory mirror to hydrate via a CTE/temp
            // boundary so the in-memory streaming/fallback GROUP BY paths are
            // also exercised directly.
            let _ = f
                .query("WITH c AS (SELECT id FROM t LIMIT 1) SELECT * FROM c")
                .await
                .unwrap();
            for sql in queries {
                let want = sqlite_rows(&r, sql, &[]);
                assert_eq!(
                    frank_rows(&f, sql, &[]).await,
                    want,
                    "hydrated mirror diverged: `{sql}` at {path}"
                );
            }
            f.close().await.unwrap();
        }

        // Small mixed-class tables where rowid order puts REAL 7.5 before
        // INTEGER 0 / -7 / 14 (table scan order, matching stock under the same
        // table scan plan; both unloaded and hydrated :memory: mirrors).
        for (name, rows) in [
            ("7.5_then_0", &[(1_i64, "7.5"), (2, "0")][..]),
            ("0_then_7.5", &[(1, "0"), (2, "7.5")][..]),
            ("7.5_then_14", &[(1, "7.5"), (2, "14")][..]),
            ("7.5_then_neg7", &[(1, "7.5"), (2, "-7")][..]),
            (
                "7.5_first_in_small",
                &[(0, "7.5"), (1, "-7"), (2, "0"), (3, "7")][..],
            ),
        ] {
            let f = Connection::open(":memory:").await.unwrap();
            let r = rusqlite::Connection::open_in_memory().unwrap();
            let mut setup =
                vec!["CREATE TABLE m (id INTEGER PRIMARY KEY, a INTEGER, x INTEGER)".to_owned()];
            for &(id, val) in rows {
                setup.push(format!("INSERT INTO m VALUES ({id}, {val}, 0)"));
            }
            for sql in &setup {
                f.execute(sql).await.unwrap();
                r.execute_batch(sql).unwrap();
            }
            for pass in ["unloaded", "hydrated"] {
                if pass == "hydrated" {
                    let _ = f
                        .query("WITH c AS (SELECT id FROM m LIMIT 1) SELECT * FROM c")
                        .await
                        .unwrap();
                }
                for sql in [
                    "SELECT a % 7, count(*) FROM m GROUP BY a % 7",
                    "SELECT a % 7, count(*) FROM m GROUP BY a % 7 HAVING count(*) > 0",
                ] {
                    assert_eq!(
                        frank_rows(&f, sql, &[]).await,
                        sqlite_rows(&r, sql, &[]),
                        "case={name} pass={pass} `{sql}`"
                    );
                }
            }
            f.close().await.unwrap();
        }
    });
}
