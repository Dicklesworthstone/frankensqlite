//! bd-nonagg-eq-residual: `SELECT <cols> FROM t WHERE <indexed col> = <lit> AND <residual>`
//! seeks the exact `col = lit` block on its index and filters the residual per row (heuristic-chain
//! fallback, after rowid/range/index_eq) via codegen_select_index_equality_scan with residual_filter=true,
//! instead of full-scanning. The eq block is emitted in rowid order (= the full scan's order within it),
//! so byte-identical. The residual-aware emitter opens the table even when the projection itself is
//! index-covering, because the residual may read any table column.
use fsqlite::Connection;
use fsqlite_func::ScalarFunction;
use fsqlite_types::SqliteValue;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

struct CountingIdentity {
    calls: Arc<AtomicUsize>,
}

impl ScalarFunction for CountingIdentity {
    fn invoke(&self, args: &[SqliteValue]) -> fsqlite_error::Result<SqliteValue> {
        let [value] = args else {
            return Err(fsqlite_error::FrankenError::function_error(
                "tick() expects exactly one argument",
            ));
        };
        self.calls.fetch_add(1, Ordering::Relaxed);
        Ok(value.clone())
    }

    fn num_args(&self) -> i32 {
        1
    }

    fn name(&self) -> &str {
        "tick"
    }

    fn is_deterministic(&self) -> bool {
        false
    }
}

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
    let mut rows = frank_rows_ordered(c, sql).await;
    rows.sort();
    rows
}
async fn frank_rows_ordered(c: &Connection, sql: &str) -> Vec<Vec<String>> {
    c.query(sql)
        .await
        .unwrap_or_else(|e| panic!("frank `{sql}`: {e}"))
        .iter()
        .map(|row| row.values().iter().map(render).collect())
        .collect()
}
async fn frank_rows_params(
    c: &Connection,
    sql: &str,
    params: &[SqliteValue],
) -> Result<Vec<Vec<String>>, String> {
    let stmt = c
        .prepare(sql)
        .await
        .map_err(|error| format!("frank prepare `{sql}`: {error}"))?;
    let mut rows: Vec<Vec<String>> = stmt
        .query_with_params(params)
        .await
        .map_err(|error| format!("frank query `{sql}`: {error}"))?
        .iter()
        .map(|row| row.values().iter().map(render).collect())
        .collect();
    rows.sort();
    Ok(rows)
}
fn sqlite_rows(c: &rusqlite::Connection, sql: &str) -> Vec<Vec<String>> {
    let mut rows = sqlite_rows_ordered(c, sql);
    rows.sort();
    rows
}
fn sqlite_rows_ordered(c: &rusqlite::Connection, sql: &str) -> Vec<Vec<String>> {
    let mut stmt = c.prepare(sql).unwrap();
    let n = stmt.column_count();
    stmt.query_map([], |row| {
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
fn sqlite_rows_params(
    c: &rusqlite::Connection,
    sql: &str,
    params: &[rusqlite::types::Value],
) -> Result<Vec<Vec<String>>, String> {
    let mut stmt = c
        .prepare(sql)
        .map_err(|error| format!("sqlite prepare `{sql}`: {error}"))?;
    let column_count = stmt.column_count();
    let mapped = stmt
        .query_map(rusqlite::params_from_iter(params.iter()), |row| {
            let mut out = Vec::with_capacity(column_count);
            for i in 0..column_count {
                out.push(match row.get_unwrap::<_, rusqlite::types::Value>(i) {
                    rusqlite::types::Value::Null => "NULL".to_owned(),
                    rusqlite::types::Value::Integer(value) => value.to_string(),
                    rusqlite::types::Value::Real(value) => format!("{value:?}"),
                    rusqlite::types::Value::Text(value) => format!("'{value}'"),
                    rusqlite::types::Value::Blob(value) => format!(
                        "X'{}'",
                        value
                            .iter()
                            .map(|byte| format!("{byte:02X}"))
                            .collect::<String>()
                    ),
                });
            }
            Ok(out)
        })
        .map_err(|error| format!("sqlite query `{sql}`: {error}"))?;
    let mut rows = Vec::new();
    for row in mapped {
        rows.push(row.map_err(|error| format!("sqlite step `{sql}`: {error}"))?);
    }
    rows.sort();
    Ok(rows)
}
async fn has_seek(c: &Connection, sql: &str) -> bool {
    c.query(&format!("EXPLAIN {sql}")).await.unwrap().iter().any(|row| matches!(row.values().get(1), Some(SqliteValue::Text(op)) if op.to_string().starts_with("Seek")))
}
async fn has_seek_params(c: &Connection, sql: &str, params: &[SqliteValue]) -> bool {
    let explain_sql = format!("EXPLAIN {sql}");
    c.query_with_params(&explain_sql, params)
        .await
        .unwrap()
        .iter()
        .any(|row| matches!(row.values().get(1), Some(SqliteValue::Text(op)) if op.to_string().starts_with("Seek")))
}
async fn setup(ddl: &[&str]) -> (Connection, rusqlite::Connection) {
    let f = Connection::open(":memory:").await.unwrap();
    let r = rusqlite::Connection::open_in_memory().unwrap();
    for s in ddl {
        f.execute(s).await.unwrap();
        r.execute_batch(s).unwrap();
    }
    (f, r)
}
async fn ins(f: &Connection, r: &rusqlite::Connection, s: &str) {
    f.execute(s).await.unwrap();
    r.execute_batch(s).unwrap();
}
async fn cmp(f: &Connection, r: &rusqlite::Connection, sql: &str, l: &str) {
    assert_eq!(
        frank_rows(f, sql).await,
        sqlite_rows(r, sql),
        "[{l}] diverged: `{sql}`"
    );
}
async fn cmp_ordered(f: &Connection, r: &rusqlite::Connection, sql: &str, l: &str) {
    assert_eq!(
        frank_rows_ordered(f, sql).await,
        sqlite_rows_ordered(r, sql),
        "[{l}] ordered rows diverged: `{sql}`"
    );
}
async fn cmp_params(
    f: &Connection,
    r: &rusqlite::Connection,
    sql: &str,
    frank_params: &[SqliteValue],
    sqlite_params: &[rusqlite::types::Value],
    label: &str,
) {
    assert_eq!(
        frank_rows_params(f, sql, frank_params)
            .await
            .unwrap_or_else(|error| panic!("[{label}] {error}")),
        sqlite_rows_params(r, sql, sqlite_params)
            .unwrap_or_else(|error| panic!("[{label}] {error}")),
        "[{label}] parameterized query diverged: `{sql}`"
    );
}
async fn both_reject_params(
    f: &Connection,
    r: &rusqlite::Connection,
    sql: &str,
    frank_params: &[SqliteValue],
    sqlite_params: &[rusqlite::types::Value],
    label: &str,
) {
    let frank_error = match frank_rows_params(f, sql, frank_params).await {
        Ok(rows) => {
            panic!("[{label}] FrankenSQLite accepted invalid SQL `{sql}` with rows {rows:?}")
        }
        Err(error) => error,
    };
    let sqlite_error = match sqlite_rows_params(r, sql, sqlite_params) {
        Ok(rows) => panic!("[{label}] SQLite accepted invalid SQL `{sql}` with rows {rows:?}"),
        Err(error) => error,
    };
    assert!(
        !frank_error.is_empty() && !sqlite_error.is_empty(),
        "[{label}] both engines must report a concrete error"
    );
}

fn install_counting_identity(
    f: &Connection,
    r: &rusqlite::Connection,
) -> (Arc<AtomicUsize>, Arc<AtomicUsize>) {
    let frank_calls = Arc::new(AtomicUsize::new(0));
    f.register_nondeterministic_scalar_function(CountingIdentity {
        calls: Arc::clone(&frank_calls),
    });

    let sqlite_calls = Arc::new(AtomicUsize::new(0));
    let sqlite_counter = Arc::clone(&sqlite_calls);
    r.create_scalar_function(
        "tick",
        1,
        rusqlite::functions::FunctionFlags::SQLITE_UTF8,
        move |context| {
            sqlite_counter.fetch_add(1, Ordering::Relaxed);
            context.get::<i64>(0)
        },
    )
    .unwrap();

    (frank_calls, sqlite_calls)
}

async fn cmp_counting_identity(
    f: &Connection,
    r: &rusqlite::Connection,
    frank_calls: &AtomicUsize,
    sqlite_calls: &AtomicUsize,
    sql: &str,
    expected_calls: usize,
    label: &str,
) {
    frank_calls.store(0, Ordering::Relaxed);
    sqlite_calls.store(0, Ordering::Relaxed);
    assert_eq!(
        frank_rows(f, sql).await,
        sqlite_rows(r, sql),
        "[{label}] result diverged: `{sql}`"
    );
    let frank_count = frank_calls.load(Ordering::Relaxed);
    let sqlite_count = sqlite_calls.load(Ordering::Relaxed);
    assert_eq!(
        frank_count, sqlite_count,
        "[{label}] tick() call count diverged: `{sql}`"
    );
    assert_eq!(
        frank_count, expected_calls,
        "[{label}] unexpected tick() call count: `{sql}`"
    );
}

async fn cmp_counting_identity_reference_may_reproject(
    f: &Connection,
    r: &rusqlite::Connection,
    frank_calls: &AtomicUsize,
    sqlite_calls: &AtomicUsize,
    sql: &str,
    expected_source_rows: usize,
    label: &str,
) {
    frank_calls.store(0, Ordering::Relaxed);
    sqlite_calls.store(0, Ordering::Relaxed);
    assert_eq!(
        frank_rows(f, sql).await,
        sqlite_rows(r, sql),
        "[{label}] result diverged: `{sql}`"
    );
    let frank_count = frank_calls.load(Ordering::Relaxed);
    let sqlite_count = sqlite_calls.load(Ordering::Relaxed);
    assert_eq!(
        frank_count, expected_source_rows,
        "[{label}] FrankenSQLite must evaluate tick() once per source row: `{sql}`"
    );
    assert!(
        (expected_source_rows..=expected_source_rows + 1).contains(&sqlite_count),
        "[{label}] unexpected SQLite tick() call count {sqlite_count}: `{sql}`"
    );
}

#[allow(clippy::too_many_arguments)]
async fn cmp_counting_identity_params(
    f: &Connection,
    r: &rusqlite::Connection,
    frank_calls: &AtomicUsize,
    sqlite_calls: &AtomicUsize,
    sql: &str,
    frank_params: &[SqliteValue],
    sqlite_params: &[rusqlite::types::Value],
    expected_calls: usize,
    label: &str,
) {
    frank_calls.store(0, Ordering::Relaxed);
    sqlite_calls.store(0, Ordering::Relaxed);
    assert_eq!(
        frank_rows_params(f, sql, frank_params)
            .await
            .unwrap_or_else(|error| panic!("[{label}] {error}")),
        sqlite_rows_params(r, sql, sqlite_params)
            .unwrap_or_else(|error| panic!("[{label}] {error}")),
        "[{label}] prepared result diverged: `{sql}`"
    );
    let frank_count = frank_calls.load(Ordering::Relaxed);
    let sqlite_count = sqlite_calls.load(Ordering::Relaxed);
    assert_eq!(
        frank_count, sqlite_count,
        "[{label}] prepared tick() call count diverged: `{sql}`"
    );
    assert_eq!(
        frank_count, expected_calls,
        "[{label}] unexpected prepared tick() call count: `{sql}`"
    );
}

async fn check(label: &str, ddl: &[&str]) {
    let (f, r) = setup(ddl).await;
    for i in 1..=600_i64 {
        let a = if i % 17 == 0 {
            "NULL".to_owned()
        } else {
            format!("{}", i % 20)
        };
        let c = if i % 19 == 0 {
            "NULL".to_owned()
        } else {
            format!("{}", i % 12)
        };
        ins(
            &f,
            &r,
            &format!(
                "INSERT INTO t VALUES ({i}, {a}, {c}, 'v{}', 'k{}');",
                i % 7,
                i % 9
            ),
        )
        .await;
    }
    // Every residual-aware shape must seek. Some projections are index-covering
    // (`id`/`a`), but the residual still requires the table cursor.
    let seeks = [
        "SELECT * FROM t WHERE a = 5 AND c = 5",
        "SELECT c FROM t WHERE a = 5 AND c = 5",
        "SELECT id FROM t WHERE a = 5 AND c = 5",
        "SELECT a FROM t WHERE a = 5 AND c = 5",
        "SELECT x FROM t WHERE a = 5 AND c = 5",
        "SELECT q.id FROM t AS q WHERE q.a = 5 AND q.c = 5",
        "SELECT c, id FROM t WHERE a = 5 AND c > 6",
        "SELECT c FROM t WHERE a = 5 AND c = 5 AND s = 'k3'",
        "SELECT c FROM t WHERE a = 5 AND c != 5",
        "SELECT * FROM t WHERE a = 999 AND c = 5",
        "SELECT c FROM t WHERE a = 0 AND c = 0",
        "SELECT c, s FROM t WHERE a = 3 AND c BETWEEN 2 AND 8",
        // OFFSET counts rows after WHERE filtering, not every candidate in the
        // `a = 5` index run.
        "SELECT id, c FROM t WHERE a = 5 AND c = 5 LIMIT 3 OFFSET 2",
        "SELECT id FROM t WHERE a = 5 AND c = 5 LIMIT 3 OFFSET 2",
    ];
    for sql in seeks {
        cmp(&f, &r, sql, label).await;
        assert!(
            has_seek(&f, sql).await,
            "[{label}] eq+residual must seek: `{sql}`"
        );
    }
    cmp(&f, &r, "SELECT id FROM t WHERE a = 5", label).await;

    let ordered_params_sql = "SELECT id, c FROM t WHERE a = 5 AND c > ? LIMIT ? OFFSET ?";
    cmp_params(
        &f,
        &r,
        ordered_params_sql,
        &[
            SqliteValue::Integer(4),
            SqliteValue::Integer(3),
            SqliteValue::Integer(2),
        ],
        &[
            rusqlite::types::Value::Integer(4),
            rusqlite::types::Value::Integer(3),
            rusqlite::types::Value::Integer(2),
        ],
        label,
    )
    .await;
    assert!(
        has_seek_params(
            &f,
            ordered_params_sql,
            &[
                SqliteValue::Integer(4),
                SqliteValue::Integer(3),
                SqliteValue::Integer(2),
            ],
        )
        .await,
        "[{label}] residual/LIMIT/OFFSET parameter-order fixture must retain the equality seek"
    );

    let invalid_limit_sql = "SELECT id FROM t WHERE a = 5 AND c > ? LIMIT (? + 0) OFFSET ?";
    both_reject_params(
        &f,
        &r,
        invalid_limit_sql,
        &[
            SqliteValue::Integer(4),
            SqliteValue::Float(1.5),
            SqliteValue::Integer(0),
        ],
        &[
            rusqlite::types::Value::Integer(4),
            rusqlite::types::Value::Real(1.5),
            rusqlite::types::Value::Integer(0),
        ],
        label,
    )
    .await;

    let invalid_offset_sql = "SELECT id FROM t WHERE a = 5 AND c > ? LIMIT ? OFFSET (? + 0)";
    both_reject_params(
        &f,
        &r,
        invalid_offset_sql,
        &[
            SqliteValue::Integer(4),
            SqliteValue::Integer(3),
            SqliteValue::Float(1.5),
        ],
        &[
            rusqlite::types::Value::Integer(4),
            rusqlite::types::Value::Integer(3),
            rusqlite::types::Value::Real(1.5),
        ],
        label,
    )
    .await;

    // SQLite does not evaluate OFFSET when LIMIT is already zero. This catches
    // the bytecode-ordering bug where an invalid OFFSET raised before the
    // LIMIT-zero guard could suppress execution.
    cmp_params(
        &f,
        &r,
        invalid_offset_sql,
        &[
            SqliteValue::Integer(4),
            SqliteValue::Integer(0),
            SqliteValue::Float(1.5),
        ],
        &[
            rusqlite::types::Value::Integer(4),
            rusqlite::types::Value::Integer(0),
            rusqlite::types::Value::Real(1.5),
        ],
        label,
    )
    .await;
}
#[test]
fn nonagg_eq_residual_matches_sqlite() {
    asupersync::test_utils::run_test(|| async {
        check(
            "single idx_a",
            &[
                "CREATE TABLE t (id INTEGER PRIMARY KEY, a INTEGER, c INTEGER, x TEXT, s TEXT);",
                "CREATE INDEX idx_a ON t(a);",
                "CREATE INDEX idx_c ON t(c);",
            ],
        )
        .await;
        check(
            "shadowed idx_a",
            &[
                "CREATE TABLE t (id INTEGER PRIMARY KEY, a INTEGER, c INTEGER, x TEXT, s TEXT);",
                "CREATE INDEX idx_ax ON t(a, x);",
                "CREATE INDEX idx_a ON t(a);",
            ],
        )
        .await;
        check(
            "partial idx_a before full idx_a",
            &[
                "CREATE TABLE t (id INTEGER PRIMARY KEY, a INTEGER, c INTEGER, x TEXT, s TEXT);",
                "CREATE INDEX idx_a_c5 ON t(a) WHERE c = 5;",
                "CREATE INDEX idx_a ON t(a);",
            ],
        )
        .await;
    });
}

#[test]
fn grouped_aggregate_limit_offset_matches_sqlite() {
    asupersync::test_utils::run_test(|| async {
        let (f, r) =
            setup(&["CREATE TABLE t (id INTEGER PRIMARY KEY, a INTEGER, c INTEGER);"]).await;
        for sql in [
            "INSERT INTO t VALUES (1, 1, 1);",
            "INSERT INTO t VALUES (2, 1, 2);",
            "INSERT INTO t VALUES (3, 2, 3);",
            "INSERT INTO t VALUES (4, 2, 4);",
            "INSERT INTO t VALUES (5, 3, 5);",
            "INSERT INTO t VALUES (6, 4, 6);",
        ] {
            ins(&f, &r, sql).await;
        }

        let sql = "SELECT a, COUNT(*) FROM t WHERE c > ? GROUP BY a LIMIT (? + 0) OFFSET (? + 0)";
        cmp_params(
            &f,
            &r,
            sql,
            &[
                SqliteValue::Integer(0),
                SqliteValue::Integer(2),
                SqliteValue::Integer(1),
            ],
            &[
                rusqlite::types::Value::Integer(0),
                rusqlite::types::Value::Integer(2),
                rusqlite::types::Value::Integer(1),
            ],
            "grouped-limit-offset",
        )
        .await;

        both_reject_params(
            &f,
            &r,
            sql,
            &[
                SqliteValue::Integer(0),
                SqliteValue::Float(1.5),
                SqliteValue::Integer(0),
            ],
            &[
                rusqlite::types::Value::Integer(0),
                rusqlite::types::Value::Real(1.5),
                rusqlite::types::Value::Integer(0),
            ],
            "grouped-invalid-limit",
        )
        .await;
        both_reject_params(
            &f,
            &r,
            sql,
            &[
                SqliteValue::Integer(0),
                SqliteValue::Integer(2),
                SqliteValue::Float(1.5),
            ],
            &[
                rusqlite::types::Value::Integer(0),
                rusqlite::types::Value::Integer(2),
                rusqlite::types::Value::Real(1.5),
            ],
            "grouped-invalid-offset",
        )
        .await;

        // As in SQLite, a zero LIMIT suppresses evaluation of an otherwise
        // invalid OFFSET expression.
        cmp_params(
            &f,
            &r,
            sql,
            &[
                SqliteValue::Integer(0),
                SqliteValue::Integer(0),
                SqliteValue::Float(1.5),
            ],
            &[
                rusqlite::types::Value::Integer(0),
                rusqlite::types::Value::Integer(0),
                rusqlite::types::Value::Real(1.5),
            ],
            "grouped-zero-limit-short-circuit",
        )
        .await;
    });
}

#[test]
fn ordered_top_n_reuses_exact_output_and_skips_rejected_payloads() {
    asupersync::test_utils::run_test(|| async {
        let connection = Connection::open(":memory:").await.unwrap();
        connection
            .execute("CREATE TABLE tick_rows (v INTEGER, k INTEGER)")
            .await
            .unwrap();
        for sql in [
            "INSERT INTO tick_rows VALUES (1, 1)",
            "INSERT INTO tick_rows VALUES (2, 2)",
            "INSERT INTO tick_rows VALUES (3, 3)",
        ] {
            connection.execute(sql).await.unwrap();
        }

        let calls = Arc::new(AtomicUsize::new(0));
        connection.register_nondeterministic_scalar_function(CountingIdentity {
            calls: Arc::clone(&calls),
        });

        let rows = connection
            .query("SELECT tick(v) AS x FROM tick_rows ORDER BY x LIMIT 1")
            .await
            .unwrap();
        assert_eq!(rows[0].get(0), Some(&SqliteValue::Integer(1)));
        assert_eq!(
            calls.load(Ordering::Relaxed),
            3,
            "an exact ORDER BY output reference must evaluate tick() once, not once for the key and again for the payload"
        );

        calls.store(0, Ordering::Relaxed);
        let statement = connection
            .prepare("SELECT tick(v) FROM tick_rows ORDER BY k LIMIT ?1")
            .await
            .unwrap();
        let rows = statement
            .query_with_params(&[SqliteValue::Integer(1)])
            .await
            .unwrap();
        assert_eq!(rows[0].get(0), Some(&SqliteValue::Integer(1)));
        assert_eq!(
            calls.load(Ordering::Relaxed),
            1,
            "runtime top-N preflight must skip payload evaluation for later rejected rows"
        );
    });
}

#[test]
fn ordered_distinct_deduplicates_output_tuple_instead_of_sort_key() {
    asupersync::test_utils::run_test(|| async {
        let (f, r) = setup(&["CREATE TABLE distinct_order_rows \
             (a INTEGER, b INTEGER, c TEXT COLLATE NOCASE);"])
        .await;
        for sql in [
            "INSERT INTO distinct_order_rows VALUES (2, 0, 'x');",
            "INSERT INTO distinct_order_rows VALUES (1, 1, 'A');",
            "INSERT INTO distinct_order_rows VALUES (2, 2, 'X');",
        ] {
            ins(&f, &r, sql).await;
        }

        for (label, sql) in [
            (
                "ordered-distinct-output-separated-by-keys",
                "SELECT DISTINCT a FROM distinct_order_rows ORDER BY b",
            ),
            (
                "ordered-distinct-output-declared-collation",
                "SELECT DISTINCT c FROM distinct_order_rows ORDER BY b",
            ),
            (
                "ordered-distinct-mixed-output-collations",
                "SELECT DISTINCT a, c FROM distinct_order_rows ORDER BY b",
            ),
            (
                "ordered-distinct-offset-after-dedup",
                "SELECT DISTINCT a FROM distinct_order_rows ORDER BY b LIMIT 1 OFFSET 1",
            ),
            (
                "ordered-distinct-duplicate-skips-independent-order-error",
                "SELECT DISTINCT a FROM distinct_order_rows \
                 ORDER BY CASE WHEN b = 2 \
                     THEN json_extract('x', '$') ELSE b END",
            ),
        ] {
            cmp_ordered(&f, &r, sql, label).await;
        }

        cmp_params(
            &f,
            &r,
            "SELECT DISTINCT a FROM distinct_order_rows \
             ORDER BY b LIMIT ?1 OFFSET ?2",
            &[SqliteValue::Integer(1), SqliteValue::Integer(1)],
            &[
                rusqlite::types::Value::Integer(1),
                rusqlite::types::Value::Integer(1),
            ],
            "ordered-distinct-membership-prepared",
        )
        .await;
    });
}

#[test]
fn ordered_distinct_reuses_function_output_for_exact_ascending_tuple() {
    asupersync::test_utils::run_test(|| async {
        let (f, r) =
            setup(&["CREATE TABLE merged_rows (id INTEGER PRIMARY KEY, v INTEGER, k INTEGER);"])
                .await;
        for sql in [
            "INSERT INTO merged_rows VALUES (10, 1, 30);",
            "INSERT INTO merged_rows VALUES (20, 2, 10);",
            "INSERT INTO merged_rows VALUES (30, 3, 20);",
        ] {
            ins(&f, &r, sql).await;
        }
        let (frank_calls, sqlite_calls) = install_counting_identity(&f, &r);

        for (label, sql) in [
            (
                "ordered-distinct-merged-alias",
                "SELECT DISTINCT tick(v) AS projected \
                 FROM merged_rows ORDER BY projected LIMIT 1",
            ),
            (
                "ordered-distinct-merged-ordinal",
                "SELECT DISTINCT tick(v) \
                 FROM merged_rows ORDER BY 1 LIMIT 1",
            ),
            (
                "ordered-distinct-merged-explicit-asc-nulls-first",
                "SELECT DISTINCT tick(v) AS projected \
                 FROM merged_rows ORDER BY projected ASC NULLS FIRST LIMIT 1",
            ),
            (
                "ordered-distinct-merged-structural-expression",
                "SELECT DISTINCT tick(v) \
                 FROM merged_rows ORDER BY tick(v) LIMIT 1",
            ),
            (
                "ordered-distinct-merged-offset-reuses-output",
                "SELECT DISTINCT tick(v) AS projected \
                 FROM merged_rows ORDER BY projected LIMIT 1 OFFSET 1",
            ),
            (
                "ordered-distinct-merged-hidden-rowid",
                "SELECT DISTINCT tick(rowid) AS projected \
                 FROM merged_rows ORDER BY projected LIMIT 1",
            ),
            (
                "ordered-distinct-merged-ipk",
                "SELECT DISTINCT tick(id) AS projected \
                 FROM merged_rows ORDER BY projected LIMIT 1",
            ),
            (
                "ordered-distinct-merged-exact-multi-output-tuple",
                "SELECT DISTINCT tick(v) AS projected, k \
                 FROM merged_rows ORDER BY projected, k LIMIT 1",
            ),
        ] {
            cmp_counting_identity(
                &f,
                &r,
                frank_calls.as_ref(),
                sqlite_calls.as_ref(),
                sql,
                3,
                label,
            )
            .await;
        }

        for (label, sql) in [
            (
                "ordered-distinct-stored-desc",
                "SELECT DISTINCT tick(v) AS projected \
                 FROM merged_rows ORDER BY projected DESC LIMIT 1",
            ),
            (
                "ordered-distinct-stored-explicit-collation",
                "SELECT DISTINCT tick(v) AS projected \
                 FROM merged_rows ORDER BY projected COLLATE BINARY LIMIT 1",
            ),
            (
                "ordered-distinct-stored-nulls-last",
                "SELECT DISTINCT tick(v) AS projected \
                 FROM merged_rows ORDER BY projected NULLS LAST LIMIT 1",
            ),
            (
                "ordered-distinct-stored-source-order",
                "SELECT DISTINCT tick(v) AS projected \
                 FROM merged_rows ORDER BY k LIMIT 1",
            ),
            (
                "ordered-distinct-stored-permuted-output",
                "SELECT DISTINCT tick(v) AS projected, k \
                 FROM merged_rows ORDER BY k, projected LIMIT 1",
            ),
        ] {
            cmp_counting_identity(
                &f,
                &r,
                frank_calls.as_ref(),
                sqlite_calls.as_ref(),
                sql,
                3,
                label,
            )
            .await;
        }

        cmp_counting_identity_params(
            &f,
            &r,
            frank_calls.as_ref(),
            sqlite_calls.as_ref(),
            "SELECT DISTINCT tick(v) AS projected \
             FROM merged_rows ORDER BY projected LIMIT ?1 OFFSET ?2",
            &[SqliteValue::Integer(1), SqliteValue::Integer(1)],
            &[
                rusqlite::types::Value::Integer(1),
                rusqlite::types::Value::Integer(1),
            ],
            3,
            "ordered-distinct-merged-prepared-offset",
        )
        .await;
        cmp_counting_identity_params(
            &f,
            &r,
            frank_calls.as_ref(),
            sqlite_calls.as_ref(),
            "SELECT DISTINCT tick(v) AS projected \
             FROM merged_rows ORDER BY projected DESC LIMIT ?1",
            &[SqliteValue::Integer(1)],
            &[rusqlite::types::Value::Integer(1)],
            3,
            "ordered-distinct-stored-prepared-desc",
        )
        .await;
    });
}

#[test]
fn ordered_distinct_function_output_reuse_covers_generated_without_rowid_and_complex_in() {
    asupersync::test_utils::run_test(|| async {
        let (f, r) = setup(&[
            "CREATE TABLE star_rows (v INTEGER);",
            "CREATE TABLE generated_rows \
             (id INTEGER PRIMARY KEY, v INTEGER, \
              g INTEGER GENERATED ALWAYS AS (v * 2) VIRTUAL);",
            "CREATE TABLE without_rowid_rows \
             (id INTEGER PRIMARY KEY, v INTEGER, \
              g INTEGER GENERATED ALWAYS AS (v * 3) VIRTUAL) WITHOUT ROWID;",
            "CREATE TABLE duplicate_rows (v INTEGER);",
        ])
        .await;
        for sql in [
            "INSERT INTO star_rows VALUES (3);",
            "INSERT INTO star_rows VALUES (1);",
            "INSERT INTO star_rows VALUES (2);",
            "INSERT INTO generated_rows(id, v) VALUES (10, 3);",
            "INSERT INTO generated_rows(id, v) VALUES (20, 1);",
            "INSERT INTO generated_rows(id, v) VALUES (30, 2);",
            "INSERT INTO without_rowid_rows(id, v) VALUES (10, 3);",
            "INSERT INTO without_rowid_rows(id, v) VALUES (20, 1);",
            "INSERT INTO without_rowid_rows(id, v) VALUES (30, 2);",
            "INSERT INTO duplicate_rows VALUES (3);",
            "INSERT INTO duplicate_rows VALUES (1);",
            "INSERT INTO duplicate_rows VALUES (1);",
            "INSERT INTO duplicate_rows VALUES (1);",
            "INSERT INTO duplicate_rows VALUES (2);",
            "INSERT INTO duplicate_rows VALUES (2);",
            "INSERT INTO duplicate_rows VALUES (4);",
            "INSERT INTO duplicate_rows VALUES (4);",
        ] {
            ins(&f, &r, sql).await;
        }
        let (frank_calls, sqlite_calls) = install_counting_identity(&f, &r);

        cmp(
            &f,
            &r,
            "SELECT DISTINCT * FROM star_rows ORDER BY 1 LIMIT 1",
            "ordered-distinct-merged-single-star",
        )
        .await;
        cmp(
            &f,
            &r,
            "SELECT DISTINCT * FROM generated_rows ORDER BY 1, 2, 3 LIMIT 1",
            "ordered-distinct-merged-ipk-generated-star",
        )
        .await;
        cmp(
            &f,
            &r,
            "SELECT DISTINCT * FROM without_rowid_rows ORDER BY 1, 2, 3 LIMIT 1",
            "ordered-distinct-merged-without-rowid-star",
        )
        .await;

        for (label, sql) in [
            (
                "ordered-distinct-merged-virtual-generated",
                "SELECT DISTINCT tick(g) AS projected \
                 FROM generated_rows ORDER BY projected LIMIT 1",
            ),
            (
                "ordered-distinct-merged-without-rowid-expression",
                "SELECT DISTINCT tick(v + g) AS projected \
                 FROM without_rowid_rows ORDER BY projected LIMIT 1",
            ),
            (
                "complex-in-ordered-distinct-merged-offset",
                "SELECT 2 IN \
                 (SELECT DISTINCT tick(v) AS projected \
                  FROM generated_rows ORDER BY projected LIMIT 1 OFFSET 1) \
                 FROM star_rows LIMIT 1",
            ),
            (
                "complex-in-ordered-distinct-merged-without-rowid",
                "SELECT 4 IN \
                 (SELECT DISTINCT tick(v + g) AS projected \
                  FROM without_rowid_rows ORDER BY projected LIMIT 1) \
                 FROM star_rows LIMIT 1",
            ),
        ] {
            cmp_counting_identity(
                &f,
                &r,
                frank_calls.as_ref(),
                sqlite_calls.as_ref(),
                sql,
                3,
                label,
            )
            .await;
        }

        for (label, sql) in [
            (
                "ordered-distinct-topn-duplicate-heavy-offset",
                "SELECT DISTINCT tick(v) AS projected \
                 FROM duplicate_rows ORDER BY projected LIMIT 1 OFFSET 1",
            ),
            (
                "complex-in-ordered-distinct-topn-duplicate-heavy-offset",
                "SELECT 2 IN \
                 (SELECT DISTINCT tick(v) AS projected \
                  FROM duplicate_rows ORDER BY projected LIMIT 1 OFFSET 1) \
                 FROM star_rows LIMIT 1",
            ),
        ] {
            cmp_counting_identity_reference_may_reproject(
                &f,
                &r,
                frank_calls.as_ref(),
                sqlite_calls.as_ref(),
                sql,
                8,
                label,
            )
            .await;
        }

        cmp_counting_identity_params(
            &f,
            &r,
            frank_calls.as_ref(),
            sqlite_calls.as_ref(),
            "SELECT ?1 IN \
             (SELECT DISTINCT tick(v) AS projected \
              FROM generated_rows ORDER BY projected LIMIT ?2 OFFSET ?3) \
             FROM star_rows LIMIT 1",
            &[
                SqliteValue::Integer(2),
                SqliteValue::Integer(1),
                SqliteValue::Integer(1),
            ],
            &[
                rusqlite::types::Value::Integer(2),
                rusqlite::types::Value::Integer(1),
                rusqlite::types::Value::Integer(1),
            ],
            3,
            "complex-in-ordered-distinct-merged-prepared-offset",
        )
        .await;
    });
}

#[test]
fn in_subquery_selection_and_three_valued_semantics_match_sqlite() {
    asupersync::test_utils::run_test(|| async {
        let (f, r) = setup(&[
            "CREATE TABLE qn (id INTEGER PRIMARY KEY, x INTEGER);",
            "CREATE TABLE qt (id INTEGER PRIMARY KEY, x TEXT);",
            "CREATE TABLE empty_rhs (v INTEGER);",
            "CREATE TABLE scan_rhs (id INTEGER PRIMARY KEY, v TEXT);",
            "CREATE TABLE window_rhs (id INTEGER PRIMARY KEY, v TEXT, k INTEGER);",
            "CREATE TABLE distinct_rhs (id INTEGER PRIMARY KEY, v TEXT, k INTEGER);",
            "CREATE TABLE nocase_rhs (id INTEGER PRIMARY KEY, v TEXT COLLATE NOCASE, k INTEGER);",
            "CREATE TABLE binary_rhs (id INTEGER PRIMARY KEY, v TEXT, k INTEGER);",
            "CREATE TABLE single_nocase (v TEXT COLLATE NOCASE);",
            "CREATE TABLE alias_rhs (id INTEGER PRIMARY KEY, v TEXT);",
            "CREATE TABLE collision_rhs (id INTEGER PRIMARY KEY, v INTEGER, k INTEGER);",
            "CREATE TABLE alias_collision_rhs \
             (k TEXT COLLATE BINARY, v TEXT COLLATE NOCASE);",
            "CREATE TABLE order_expr_rhs (v TEXT);",
            "CREATE TABLE lhs_nocase (x TEXT COLLATE NOCASE);",
            "CREATE TABLE single_ipk (id INTEGER PRIMARY KEY);",
            "CREATE TABLE multi_rhs (a INTEGER, b INTEGER);",
            "CREATE TABLE scalar_lhs_integer (x INTEGER);",
            "CREATE TABLE scalar_lhs_nocase (x TEXT COLLATE NOCASE);",
            "CREATE TABLE scalar_rhs_numeric_text (v TEXT);",
            "CREATE TABLE scalar_rhs_binary_text (v TEXT);",
            "CREATE TABLE topn_rhs (v INTEGER, k INTEGER);",
            "CREATE TABLE topn_equal_rhs (v INTEGER, k INTEGER);",
            "CREATE TABLE topn_offset_rhs (v INTEGER, k INTEGER);",
        ])
        .await;
        for sql in [
            "INSERT INTO qn VALUES (1, 1);",
            "INSERT INTO qn VALUES (2, 2);",
            "INSERT INTO qn VALUES (3, 3);",
            "INSERT INTO qn VALUES (4, NULL);",
            "INSERT INTO qt VALUES (1, 'a');",
            "INSERT INTO qt VALUES (2, 'B');",
            "INSERT INTO qt VALUES (3, 'z');",
            "INSERT INTO qt VALUES (4, NULL);",
            "INSERT INTO scan_rhs VALUES (1, '2');",
            "INSERT INTO scan_rhs VALUES (2, '1');",
            "INSERT INTO window_rhs VALUES (1, '2', 10);",
            "INSERT INTO window_rhs VALUES (2, NULL, 20);",
            "INSERT INTO window_rhs VALUES (3, '3', 30);",
            "INSERT INTO distinct_rhs VALUES (1, '1', 30);",
            "INSERT INTO distinct_rhs VALUES (2, '1', 10);",
            "INSERT INTO distinct_rhs VALUES (3, '2', 20);",
            "INSERT INTO distinct_rhs VALUES (4, NULL, 40);",
            "INSERT INTO nocase_rhs VALUES (1, 'A', 10);",
            "INSERT INTO nocase_rhs VALUES (2, 'c', 20);",
            "INSERT INTO binary_rhs VALUES (1, 'A', 10);",
            "INSERT INTO binary_rhs VALUES (2, 'c', 20);",
            "INSERT INTO single_nocase VALUES ('a');",
            "INSERT INTO single_nocase VALUES ('B');",
            "INSERT INTO alias_rhs VALUES (1, 'b');",
            "INSERT INTO alias_rhs VALUES (2, 'A');",
            "INSERT INTO collision_rhs VALUES (1, 9, 1);",
            "INSERT INTO collision_rhs VALUES (2, 1, 0);",
            "INSERT INTO alias_collision_rhs VALUES ('x', 'a');",
            "INSERT INTO alias_collision_rhs VALUES ('y', 'B');",
            "INSERT INTO order_expr_rhs VALUES ('a');",
            "INSERT INTO order_expr_rhs VALUES ('B');",
            "INSERT INTO lhs_nocase VALUES ('a');",
            "INSERT INTO single_ipk VALUES (5);",
            "INSERT INTO multi_rhs VALUES (1, 2);",
            "INSERT INTO scalar_lhs_integer VALUES (1);",
            "INSERT INTO scalar_lhs_nocase VALUES ('a');",
            "INSERT INTO scalar_rhs_numeric_text VALUES ('01');",
            "INSERT INTO scalar_rhs_binary_text VALUES ('A');",
            "INSERT INTO topn_rhs VALUES (1, 1);",
            "INSERT INTO topn_rhs VALUES (2, 2);",
            "INSERT INTO topn_equal_rhs VALUES (1, 1);",
            "INSERT INTO topn_equal_rhs VALUES (2, 1);",
            "INSERT INTO topn_offset_rhs VALUES (1, 1);",
            "INSERT INTO topn_offset_rhs VALUES (2, 2);",
            "INSERT INTO topn_offset_rhs VALUES (3, 3);",
        ] {
            ins(&f, &r, sql).await;
        }

        // LIMIT without ORDER BY must retain table-scan order. The TEXT RHS
        // simultaneously checks comparison affinity against the INTEGER LHS.
        for (label, sql) in [
            (
                "limit-scan-order",
                "SELECT id, x IN (SELECT v FROM scan_rhs LIMIT 1) FROM qn",
            ),
            (
                "not-in-limit-scan-order",
                "SELECT id, x NOT IN (SELECT v FROM scan_rhs LIMIT 1) FROM qn",
            ),
            (
                "limit-offset-scan-order",
                "SELECT id, x IN (SELECT v FROM scan_rhs LIMIT 1 OFFSET 1) FROM qn",
            ),
            (
                "simple-empty-null-lhs",
                "SELECT id, x IN (SELECT v FROM empty_rhs) FROM qn",
            ),
            (
                "simple-empty-null-lhs-not-in",
                "SELECT id, x NOT IN (SELECT v FROM empty_rhs) FROM qn",
            ),
            (
                "simple-filtered-empty-null-lhs",
                "SELECT id, x IN (SELECT v FROM scan_rhs WHERE 0) FROM qn",
            ),
            (
                "simple-nonempty-null-lhs",
                "SELECT id, x IN (SELECT v FROM scan_rhs) FROM qn",
            ),
            (
                "complex-zero-limit-null-lhs",
                "SELECT id, x IN (SELECT v FROM scan_rhs ORDER BY id LIMIT 0) FROM qn",
            ),
            (
                "complex-zero-limit-null-lhs-not-in",
                "SELECT id, x NOT IN (SELECT v FROM scan_rhs ORDER BY id LIMIT 0) FROM qn",
            ),
            (
                "selected-null-in",
                "SELECT id, x IN \
                 (SELECT v FROM window_rhs ORDER BY k LIMIT 1 OFFSET 1) FROM qn",
            ),
            (
                "selected-null-not-in",
                "SELECT id, x NOT IN \
                 (SELECT v FROM window_rhs ORDER BY k LIMIT 1 OFFSET 1) FROM qn",
            ),
            (
                "distinct-before-limit",
                "SELECT id, x IN (SELECT DISTINCT v FROM distinct_rhs LIMIT 2) FROM qn",
            ),
            (
                "distinct-before-offset",
                "SELECT id, x IN \
                 (SELECT DISTINCT v FROM distinct_rhs LIMIT 1 OFFSET 1) FROM qn",
            ),
            (
                "distinct-before-order-limit",
                "SELECT id, x IN \
                 (SELECT DISTINCT v FROM distinct_rhs ORDER BY k LIMIT 2) FROM qn",
            ),
            (
                "rhs-declared-nocase",
                "SELECT id, 'a' IN \
                 (SELECT v FROM nocase_rhs ORDER BY k LIMIT 1) FROM qn",
            ),
            (
                "lhs-explicit-nocase",
                "SELECT id, 'a' COLLATE NOCASE IN \
                 (SELECT v FROM binary_rhs ORDER BY k LIMIT 1) FROM qn",
            ),
            (
                "rhs-explicit-nocase",
                "SELECT id, 'a' IN \
                 (SELECT v COLLATE NOCASE FROM binary_rhs ORDER BY k LIMIT 1) FROM qn",
            ),
            (
                "lhs-nested-explicit-nocase",
                "SELECT id, (('a' COLLATE NOCASE) || '') IN \
                 (SELECT v FROM binary_rhs ORDER BY k LIMIT 1) FROM qn",
            ),
            (
                "rhs-nested-explicit-nocase",
                "SELECT id, 'a' IN \
                 (SELECT (v COLLATE NOCASE) || '' \
                  FROM binary_rhs ORDER BY k LIMIT 1) FROM qn",
            ),
            (
                "order-nested-explicit-nocase",
                "SELECT id, 'a' IN \
                 (SELECT v FROM order_expr_rhs \
                  ORDER BY (v COLLATE NOCASE) || '' LIMIT 1) FROM qn",
            ),
            (
                "rhs-unary-plus-preserves-declared-nocase",
                "SELECT id, 'a' IN \
                 (SELECT +v FROM single_nocase ORDER BY rowid LIMIT 1) FROM qn",
            ),
            (
                "rhs-cast-preserves-declared-nocase",
                "SELECT id, 'a' IN \
                 (SELECT CAST(v AS TEXT) FROM single_nocase ORDER BY rowid LIMIT 1) FROM qn",
            ),
            (
                "lhs-unary-plus-preserves-declared-nocase",
                "SELECT +x IN \
                 (SELECT v FROM binary_rhs ORDER BY k LIMIT 1) FROM lhs_nocase",
            ),
            (
                "lhs-cast-preserves-declared-nocase",
                "SELECT CAST(x AS TEXT) IN \
                 (SELECT v FROM binary_rhs ORDER BY k LIMIT 1) FROM lhs_nocase",
            ),
            (
                "star-ordinal-inherits-collation",
                "SELECT id, 'a' IN \
                 (SELECT * FROM single_nocase ORDER BY 1 LIMIT 1) FROM qn",
            ),
            (
                "star-ipk-reads-rowid",
                "SELECT id, 5 IN \
                 (SELECT * FROM single_ipk ORDER BY +1 LIMIT 1) FROM qn",
            ),
            (
                "signed-positive-order-ordinal",
                "SELECT id, 1 IN \
                 (SELECT v FROM scan_rhs ORDER BY +1 LIMIT 1) FROM qn",
            ),
            (
                "alias-collate-order",
                "SELECT id, 'A' IN \
                 (SELECT v AS k FROM alias_rhs ORDER BY k COLLATE NOCASE LIMIT 1) FROM qn",
            ),
            (
                "alias-collision-inherits-output-collation",
                "SELECT id, 'a' IN \
                 (SELECT v AS k FROM alias_collision_rhs ORDER BY k LIMIT 1) FROM qn",
            ),
            (
                "source-inside-order-expression",
                "SELECT id, 1 IN \
                 (SELECT v FROM scan_rhs ORDER BY v + 0 LIMIT 1) FROM qn",
            ),
            (
                "alias-inside-order-expression",
                "SELECT id, 1 IN \
                 (SELECT v AS k FROM scan_rhs ORDER BY k + 0 LIMIT 1) FROM qn",
            ),
            (
                "where-output-alias",
                "SELECT id, 1 IN \
                 (SELECT v AS k FROM scan_rhs WHERE k = 1 LIMIT 1) FROM qn",
            ),
            (
                "where-real-column-precedes-alias",
                "SELECT id, 9 IN \
                 (SELECT v AS k FROM collision_rhs WHERE k = 1 LIMIT 1) FROM qn",
            ),
            (
                "correlated-simple-probe",
                "SELECT qn.id, qn.x IN \
                 (SELECT v FROM scan_rhs WHERE v = qn.x) FROM qn",
            ),
            (
                "correlated-complex-projection",
                "SELECT qn.id, qn.x IN \
                 (SELECT qn.x FROM scan_rhs LIMIT 1) FROM qn",
            ),
            (
                "aggregate-complex-projection",
                "SELECT id, x IN \
                 (SELECT max(id) FROM scan_rhs ORDER BY 1 LIMIT 1) FROM qn",
            ),
            (
                "uncorrelated-scalar-lhs-column-affinity",
                "SELECT id, (SELECT x FROM scalar_lhs_integer) IN \
                 (SELECT v FROM scalar_rhs_numeric_text ORDER BY rowid LIMIT 1) FROM qn",
            ),
            (
                "uncorrelated-scalar-lhs-cast-affinity",
                "SELECT id, (SELECT CAST(1 AS INTEGER)) IN \
                 (SELECT v FROM scalar_rhs_numeric_text ORDER BY rowid LIMIT 1) FROM qn",
            ),
            (
                "uncorrelated-scalar-lhs-no-affinity",
                "SELECT id, (SELECT 1) IN \
                 (SELECT v FROM scalar_rhs_numeric_text ORDER BY rowid LIMIT 1) FROM qn",
            ),
            (
                "scalar-lhs-declared-collation-stops-at-boundary",
                "SELECT id, (SELECT x FROM scalar_lhs_nocase) IN \
                 (SELECT v FROM scalar_rhs_binary_text ORDER BY rowid LIMIT 1) FROM qn",
            ),
            (
                "scalar-lhs-inner-explicit-collation-stops-at-boundary",
                "SELECT id, (SELECT x COLLATE NOCASE FROM scalar_lhs_nocase) IN \
                 (SELECT v FROM scalar_rhs_binary_text ORDER BY rowid LIMIT 1) FROM qn",
            ),
            (
                "generic-topn-rejected-projection-is-lazy",
                "SELECT CASE WHEN v = 1 THEN 1 ELSE json_extract('x', '$') END \
                 FROM topn_rhs ORDER BY k LIMIT 1",
            ),
            (
                "generic-topn-equal-later-projection-is-lazy",
                "SELECT CASE WHEN v = 1 THEN 1 ELSE json_extract('x', '$') END \
                 FROM topn_equal_rhs ORDER BY k LIMIT 1",
            ),
            (
                "generic-topn-offset-rejected-projection-is-lazy",
                "SELECT CASE WHEN v <= 2 THEN v ELSE json_extract('x', '$') END \
                 FROM topn_offset_rhs ORDER BY k LIMIT 1 OFFSET 1",
            ),
            (
                "complex-in-topn-rejected-projection-is-lazy",
                "SELECT 1 IN \
                 (SELECT CASE WHEN v = 1 THEN 1 ELSE json_extract('x', '$') END \
                  FROM topn_rhs ORDER BY k LIMIT 1)",
            ),
            (
                "complex-in-topn-equal-later-projection-is-lazy",
                "SELECT 1 IN \
                 (SELECT CASE WHEN v = 1 THEN 1 ELSE json_extract('x', '$') END \
                  FROM topn_equal_rhs ORDER BY k LIMIT 1)",
            ),
            (
                "complex-in-topn-offset-rejected-projection-is-lazy",
                "SELECT 2 IN \
                 (SELECT CASE WHEN v <= 2 THEN v ELSE json_extract('x', '$') END \
                  FROM topn_offset_rhs ORDER BY k LIMIT 1 OFFSET 1)",
            ),
        ] {
            cmp(&f, &r, sql, label).await;
        }

        for (label, sql) in [
            (
                "generic-topn-competitive-projection-errors",
                "SELECT CASE WHEN v = 1 THEN 1 ELSE json_extract('x', '$') END \
                 FROM topn_rhs ORDER BY k DESC LIMIT 1",
            ),
            (
                "complex-in-topn-competitive-projection-errors",
                "SELECT 1 IN \
                 (SELECT CASE WHEN v = 1 THEN 1 ELSE json_extract('x', '$') END \
                  FROM topn_rhs ORDER BY k DESC LIMIT 1)",
            ),
        ] {
            both_reject_params(&f, &r, sql, &[], &[], label).await;
        }

        let scalar_lhs_prepared = "SELECT id, (SELECT x FROM scalar_lhs_integer) IN \
            (SELECT v FROM scalar_rhs_numeric_text ORDER BY rowid LIMIT 1) FROM qn";
        cmp_params(
            &f,
            &r,
            scalar_lhs_prepared,
            &[],
            &[],
            "uncorrelated-scalar-lhs-column-affinity-prepared",
        )
        .await;
        cmp_params(
            &f,
            &r,
            "SELECT CASE WHEN v = 1 THEN 1 ELSE json_extract('x', '$') END \
             FROM topn_rhs ORDER BY k LIMIT ?1 OFFSET ?2",
            &[SqliteValue::Integer(1), SqliteValue::Integer(-5)],
            &[
                rusqlite::types::Value::Integer(1),
                rusqlite::types::Value::Integer(-5),
            ],
            "generic-dynamic-topn-negative-offset-clamps-to-zero",
        )
        .await;
        cmp_params(
            &f,
            &r,
            "SELECT 1 IN \
             (SELECT CASE WHEN v = 1 THEN 1 ELSE json_extract('x', '$') END \
              FROM topn_rhs ORDER BY k LIMIT ?1 OFFSET ?2)",
            &[SqliteValue::Integer(1), SqliteValue::Integer(-5)],
            &[
                rusqlite::types::Value::Integer(1),
                rusqlite::types::Value::Integer(-5),
            ],
            "complex-in-dynamic-topn-negative-offset-clamps-to-zero",
        )
        .await;

        let parameterized = "SELECT id, x IN \
            (SELECT v FROM window_rhs ORDER BY k LIMIT (? + 0) OFFSET (? + 0)) FROM qn";
        cmp_params(
            &f,
            &r,
            parameterized,
            &[SqliteValue::Integer(1), SqliteValue::Integer(1)],
            &[
                rusqlite::types::Value::Integer(1),
                rusqlite::types::Value::Integer(1),
            ],
            "complex-parameter-slots",
        )
        .await;
        both_reject_params(
            &f,
            &r,
            parameterized,
            &[SqliteValue::Float(1.5), SqliteValue::Integer(0)],
            &[
                rusqlite::types::Value::Real(1.5),
                rusqlite::types::Value::Integer(0),
            ],
            "complex-invalid-limit",
        )
        .await;
        both_reject_params(
            &f,
            &r,
            parameterized,
            &[SqliteValue::Integer(1), SqliteValue::Float(1.5)],
            &[
                rusqlite::types::Value::Integer(1),
                rusqlite::types::Value::Real(1.5),
            ],
            "complex-invalid-offset",
        )
        .await;
        cmp_params(
            &f,
            &r,
            parameterized,
            &[SqliteValue::Integer(0), SqliteValue::Float(1.5)],
            &[
                rusqlite::types::Value::Integer(0),
                rusqlite::types::Value::Real(1.5),
            ],
            "complex-zero-limit-skips-invalid-offset",
        )
        .await;

        let reordered_bind_slots =
            "SELECT ? IN (SELECT v FROM window_rhs ORDER BY k LIMIT ?) FROM qn LIMIT 1";
        cmp_params(
            &f,
            &r,
            reordered_bind_slots,
            &[SqliteValue::Text("2".into()), SqliteValue::Integer(1)],
            &[
                rusqlite::types::Value::Text("2".to_owned()),
                rusqlite::types::Value::Integer(1),
            ],
            "complex-lhs-before-rhs-parameter-slots",
        )
        .await;

        for (label, sql) in [
            (
                "multi-column-select-star",
                "SELECT x IN (SELECT * FROM multi_rhs LIMIT 1) FROM qn",
            ),
            ("multi-column-in-table", "SELECT x IN multi_rhs FROM qn"),
            (
                "unknown-subquery-projection",
                "SELECT x IN (SELECT nope FROM scan_rhs LIMIT 1) FROM qn",
            ),
            (
                "unknown-subquery-where-name",
                "SELECT x IN \
                 (SELECT v FROM scan_rhs WHERE nope = 1 LIMIT 1) FROM qn",
            ),
            (
                "unknown-subquery-order-name",
                "SELECT x IN \
                 (SELECT v FROM scan_rhs ORDER BY nope LIMIT 1) FROM qn",
            ),
            (
                "out-of-range-subquery-order-ordinal",
                "SELECT x IN \
                 (SELECT v FROM scan_rhs ORDER BY 2 LIMIT 1) FROM qn",
            ),
            (
                "negative-subquery-order-ordinal",
                "SELECT x IN \
                 (SELECT v FROM scan_rhs ORDER BY -1 LIMIT 1) FROM qn",
            ),
            (
                "row-value-subquery-projection",
                "SELECT x IN \
                 (SELECT (a, b) FROM multi_rhs ORDER BY a LIMIT 1) FROM qn",
            ),
            (
                "invalid-scalar-filter",
                "SELECT x IN \
                 (SELECT abs(id) FILTER (WHERE 0) \
                  FROM scan_rhs ORDER BY id LIMIT 1) FROM qn",
            ),
            (
                "invalid-scalar-in-call-order",
                "SELECT x IN \
                 (SELECT abs(id ORDER BY id) \
                  FROM scan_rhs ORDER BY id LIMIT 1) FROM qn",
            ),
        ] {
            both_reject_params(&f, &r, sql, &[], &[], label).await;
        }
    });
}

#[test]
fn nonagg_eq_residual_text_prefix() {
    asupersync::test_utils::run_test(|| async {
        let (f, r) = setup(&[
            "CREATE TABLE t (id INTEGER PRIMARY KEY, s TEXT, c INTEGER, x TEXT);",
            "CREATE INDEX idx_s ON t(s);",
        ])
        .await;
        for i in 1..=300_i64 {
            ins(
                &f,
                &r,
                &format!(
                    "INSERT INTO t VALUES ({i}, 'k{}', {}, 'p{}');",
                    i % 8,
                    i % 10,
                    i % 4
                ),
            )
            .await;
        }
        for sql in [
            "SELECT c FROM t WHERE s = 'k3' AND c = 4",
            "SELECT * FROM t WHERE s = 'k3' AND c > 5",
        ] {
            cmp(&f, &r, sql, "text-prefix").await;
            assert!(
                has_seek(&f, sql).await,
                "text eq+residual must seek: `{sql}`"
            );
        }
    });
}

#[test]
fn nonagg_eq_residual_rejects_binary_index_for_nocase_column() {
    asupersync::test_utils::run_test(|| async {
        let (f, r) = setup(&[
            "CREATE TABLE t (id INTEGER PRIMARY KEY, s TEXT COLLATE NOCASE, c INTEGER);",
            "CREATE INDEX idx_s_binary ON t(s COLLATE BINARY);",
        ])
        .await;
        for sql in [
            "INSERT INTO t VALUES (1, 'k3', 1);",
            "INSERT INTO t VALUES (2, 'K3', 1);",
            "INSERT INTO t VALUES (3, 'k3', 0);",
            "INSERT INTO t VALUES (4, 'other', 1);",
        ] {
            ins(&f, &r, sql).await;
        }

        let sql = "SELECT id, s FROM t WHERE s = 'k3' AND c = 1";
        cmp(&f, &r, sql, "nocase-column-binary-index").await;
        assert!(
            !has_seek(&f, sql).await,
            "a BINARY index cannot serve a bare equality whose NOCASE column semantics \
             also match differently-cased keys"
        );
    });
}
