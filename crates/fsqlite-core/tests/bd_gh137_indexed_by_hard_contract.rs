#![recursion_limit = "512"]

//! GH #137 (bd-gh-indexed-by-hard-contract-qd1cs): `INDEXED BY <name>` is a hard
//! planning contract — the named index must be usable, or prepare must fail with
//! `no query solution`. The bead's evidence predates the bd-wlo29 / bd-0fz9d
//! partial-index cover work; this is a census-vs-HEAD keeper for the two
//! confirmed sub-issues (oracle: sqlite3 3.46.1).

use fsqlite_core::connection::Connection;
use fsqlite_types::value::SqliteValue;

/// Sub-issue 1: a forced PARTIAL index that the query cannot use (no WHERE that
/// implies the partial predicate) must ERROR like stock — not silently
/// table-scan and return rows the index can't represent.
#[test]
fn gh137_sub1_unusable_partial_index_errors() {
    asupersync::test_utils::run_test(|| async {
        let conn = Connection::open(":memory:").await.unwrap();
        conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, k INTEGER);")
            .await
            .unwrap();
        conn.execute("CREATE INDEX p ON t(k) WHERE k > 2;")
            .await
            .unwrap();
        conn.execute("INSERT INTO t VALUES (1,1),(2,3);")
            .await
            .unwrap();

        // Stock: "no query solution". frank must also refuse (not return 1,2).
        let res = conn.query("SELECT id FROM t INDEXED BY p;").await;
        assert!(
            res.is_err(),
            "forced unusable partial index must error like stock (no query solution); got rows {res:?}"
        );
    });
}

/// Sub-issue 2: an aggregate over `INDEXED BY <idx>` must scan in the INDEX
/// order, which changes SUM overflow semantics. Rows summed in `ord` order are
/// MAX + (-1) + 1 = MAX (no overflow); in rowid order MAX + 1 overflows. Stock
/// returns 9223372036854775807; frank must honor the hard hint, not scan rowid
/// order and raise "integer overflow".
///
/// FIXED (bd-gh-indexed-by-hard-contract-qd1cs sub-issue 2): codegen_select_aggregate
/// now takes an index-ORDERED full-scan path for a forced `INDEXED BY` (the
/// `forced_index_ordered` branch), so the aggregate accumulates in index order.
#[test]
fn gh137_sub2_aggregate_honors_indexed_by_scan_order() {
    asupersync::test_utils::run_test(|| async {
        let conn = Connection::open(":memory:").await.unwrap();
        conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, ord INTEGER, x INTEGER);")
            .await
            .unwrap();
        conn.execute("CREATE INDEX idx_ord ON t(ord);")
            .await
            .unwrap();
        conn.execute("INSERT INTO t VALUES (1,1,9223372036854775807),(2,3,1),(3,2,-1);")
            .await
            .unwrap();

        let res = conn.query("SELECT sum(x) FROM t INDEXED BY idx_ord;").await;
        let got = res.expect("sum honoring INDEXED BY must not overflow");
        assert_eq!(
            got[0].values()[0],
            SqliteValue::Integer(9_223_372_036_854_775_807),
            "aggregate must scan in idx_ord order (MAX-1+1=MAX), matching stock"
        );
    });
}

/// Sub-2 order-INSENSITIVE aggregates over a forced index stay correct: the
/// COVERING path (count(*) + sum(ord), both readable from idx_ord) and the
/// non-covering path (min/max(x), x looked up in the table by rowid).
#[test]
fn gh137_sub2_covering_and_noncovering_unchanged() {
    asupersync::test_utils::run_test(|| async {
        let conn = Connection::open(":memory:").await.unwrap();
        conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, ord INTEGER, x INTEGER);")
            .await
            .unwrap();
        conn.execute("CREATE INDEX idx_ord ON t(ord);")
            .await
            .unwrap();
        conn.execute("INSERT INTO t VALUES (1,3,10),(2,1,20),(3,2,30);")
            .await
            .unwrap();

        // Covering: count(*) needs no column; sum(ord) reads ord from the index.
        let cov = conn
            .query("SELECT count(*), sum(ord) FROM t INDEXED BY idx_ord;")
            .await
            .expect("covering forced-index aggregate");
        assert_eq!(cov[0].values()[0], SqliteValue::Integer(3));
        assert_eq!(cov[0].values()[1], SqliteValue::Integer(6));

        // Non-covering: min/max(x) require a table lookup by rowid.
        let mm = conn
            .query("SELECT min(x), max(x) FROM t INDEXED BY idx_ord;")
            .await
            .expect("non-covering forced-index aggregate");
        assert_eq!(mm[0].values()[0], SqliteValue::Integer(10));
        assert_eq!(mm[0].values()[1], SqliteValue::Integer(30));
    });
}

/// Sub-2 with a residual WHERE: the forced full index scan enforces no
/// predicate, so the whole WHERE is re-applied per row (non-covering).
#[test]
fn gh137_sub2_where_residual() {
    asupersync::test_utils::run_test(|| async {
        let conn = Connection::open(":memory:").await.unwrap();
        conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, ord INTEGER, x INTEGER);")
            .await
            .unwrap();
        conn.execute("CREATE INDEX idx_ord ON t(ord);")
            .await
            .unwrap();
        conn.execute("INSERT INTO t VALUES (1,3,10),(2,1,20),(3,2,30);")
            .await
            .unwrap();

        let res = conn
            .query("SELECT sum(x) FROM t INDEXED BY idx_ord WHERE x > 15;")
            .await
            .expect("forced-index aggregate with residual WHERE");
        // x in {20,30} pass the filter -> 50.
        assert_eq!(res[0].values()[0], SqliteValue::Integer(50));
    });
}

/// Sub-2 × sub-4: a WITHOUT ROWID table has no rowid, so the forced-index
/// aggregate must fetch the row via the PK-suffix seek. Same index-order overflow
/// semantics as the rowid case (MAX + -1 + 1 = MAX, no intermediate overflow).
#[test]
fn gh137_sub2_without_rowid() {
    asupersync::test_utils::run_test(|| async {
        let conn = Connection::open(":memory:").await.unwrap();
        conn.execute("CREATE TABLE w(k TEXT PRIMARY KEY, ord INTEGER, x INTEGER) WITHOUT ROWID;")
            .await
            .unwrap();
        conn.execute("CREATE INDEX wi ON w(ord);").await.unwrap();
        conn.execute("INSERT INTO w VALUES ('a',1,9223372036854775807),('b',3,1),('c',2,-1);")
            .await
            .unwrap();

        let res = conn
            .query("SELECT sum(x) FROM w INDEXED BY wi;")
            .await
            .expect("WITHOUT ROWID forced-index aggregate must not overflow");
        assert_eq!(
            res[0].values()[0],
            SqliteValue::Integer(9_223_372_036_854_775_807),
            "WITHOUT ROWID aggregate must scan wi order (PK-suffix seek), matching stock"
        );
    });
}

/// Sub-2 empty-table edge: the forced index is empty, so Rewind jumps straight to
/// finalize with still-Null accumulators — sum -> NULL, count(*) -> 0 (stock).
#[test]
fn gh137_sub2_empty_table() {
    asupersync::test_utils::run_test(|| async {
        let conn = Connection::open(":memory:").await.unwrap();
        conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, ord INTEGER, x INTEGER);")
            .await
            .unwrap();
        conn.execute("CREATE INDEX idx_ord ON t(ord);")
            .await
            .unwrap();

        let res = conn
            .query("SELECT sum(x), count(*) FROM t INDEXED BY idx_ord;")
            .await
            .expect("empty forced-index aggregate");
        assert_eq!(res[0].values()[0], SqliteValue::Null);
        assert_eq!(res[0].values()[1], SqliteValue::Integer(0));
    });
}

/// Sub-4 facet (a) — WITHOUT ROWID + INDEXED BY on a NON-aggregate query (the
/// codegen_select_index_ordered_scan path): covering (b in the index) and
/// non-covering (k via the PK-suffix table seek) both scan in wi (index) order.
/// Appears resolved at HEAD (bd-rjaff PK-suffix seek); this is a regression guard.
#[test]
fn gh137_sub4_without_rowid_nonaggregate() {
    asupersync::test_utils::run_test(|| async {
        let conn = Connection::open(":memory:").await.unwrap();
        conn.execute("CREATE TABLE w(k TEXT PRIMARY KEY, b INTEGER) WITHOUT ROWID;")
            .await
            .unwrap();
        conn.execute("CREATE INDEX wi ON w(b);").await.unwrap();
        conn.execute("INSERT INTO w VALUES ('c',30),('a',10),('b',20);")
            .await
            .unwrap();

        // Covering (b is the index key): forced index wi -> ascending b order.
        let cov = conn
            .query("SELECT b FROM w INDEXED BY wi;")
            .await
            .expect("WITHOUT ROWID covering forced index");
        let bs: Vec<SqliteValue> = cov.iter().map(|r| r.values()[0].clone()).collect();
        assert_eq!(
            bs,
            vec![
                SqliteValue::Integer(10),
                SqliteValue::Integer(20),
                SqliteValue::Integer(30)
            ],
            "WITHOUT ROWID covering forced index must scan wi order"
        );

        // Non-covering (k needs the PK-suffix table seek), same wi order.
        let non = conn
            .query("SELECT k, b FROM w INDEXED BY wi;")
            .await
            .expect("WITHOUT ROWID non-covering forced index");
        let ks: Vec<SqliteValue> = non.iter().map(|r| r.values()[0].clone()).collect();
        assert_eq!(
            ks,
            vec![
                SqliteValue::Text("a".into()),
                SqliteValue::Text("b".into()),
                SqliteValue::Text("c".into())
            ],
            "WITHOUT ROWID non-covering forced index must fetch k via PK-suffix seek in wi order"
        );
    });
}

/// Sub-4 facet (b) — parameter lowering across a forced-index seek + residual: a
/// bound `?` in the seek key and another in the residual WHERE must map to the
/// right parameter (the base-reset in codegen_select_index_ordered_scan). Appears
/// resolved at HEAD; this is a regression guard.
#[test]
fn gh137_sub4_param_lowering() {
    asupersync::test_utils::run_test(|| async {
        let conn = Connection::open(":memory:").await.unwrap();
        conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, k INTEGER, v INTEGER);")
            .await
            .unwrap();
        conn.execute("CREATE INDEX ik ON t(k);").await.unwrap();
        conn.execute("INSERT INTO t VALUES (1,5,100),(2,5,200),(3,9,300);")
            .await
            .unwrap();

        // k = ?1 (seek key), v > ?2 (residual). k=5 -> v in {100,200}; v>150 -> 200.
        let res = conn
            .query_with_params(
                "SELECT v FROM t INDEXED BY ik WHERE k = ? AND v > ?;",
                &[SqliteValue::Integer(5), SqliteValue::Integer(150)],
            )
            .await
            .expect("forced-index param lowering");
        let vs: Vec<SqliteValue> = res.iter().map(|r| r.values()[0].clone()).collect();
        assert_eq!(
            vs,
            vec![SqliteValue::Integer(200)],
            "both bound params must map correctly across the forced-index seek + residual"
        );
    });
}

fn render_fsqlite_val(value: &SqliteValue) -> String {
    match value {
        SqliteValue::Null => "NULL".to_owned(),
        SqliteValue::Integer(n) => n.to_string(),
        SqliteValue::Float(f) => format!("{f:?}"),
        SqliteValue::Text(s) => format!("'{s}'"),
        SqliteValue::Blob(b) => format!("x{b:02x?}"),
    }
}

fn render_stock_val(value: &rusqlite::types::Value) -> String {
    match value {
        rusqlite::types::Value::Null => "NULL".to_owned(),
        rusqlite::types::Value::Integer(n) => n.to_string(),
        rusqlite::types::Value::Real(f) => format!("{f:?}"),
        rusqlite::types::Value::Text(s) => format!("'{s}'"),
        rusqlite::types::Value::Blob(b) => format!("x{b:02x?}"),
    }
}

fn stock_query_rows(conn: &rusqlite::Connection, sql: &str) -> Result<Vec<Vec<String>>, String> {
    let mut stmt = conn.prepare(sql).map_err(|e| e.to_string())?;
    let ncols = stmt.column_count();
    let mut rows = Vec::new();
    let mut cursor = stmt.query([]).map_err(|e| e.to_string())?;
    while let Some(row) = cursor.next().map_err(|e| e.to_string())? {
        rows.push(
            (0..ncols)
                .map(|i| render_stock_val(&row.get_unwrap::<_, rusqlite::types::Value>(i)))
                .collect(),
        );
    }
    Ok(rows)
}

async fn fsqlite_query_rows(
    conn: &Connection,
    sql: &str,
    prepared: bool,
) -> Result<Vec<Vec<String>>, String> {
    let rows = if prepared {
        let stmt = conn.prepare(sql).await.map_err(|e| e.to_string())?;
        stmt.query().await.map_err(|e| e.to_string())?
    } else {
        conn.query(sql).await.map_err(|e| e.to_string())?
    };
    Ok(rows
        .iter()
        .map(|row| row.values().iter().map(render_fsqlite_val).collect())
        .collect())
}

/// bd-aj9v4: `INDEXED BY` a covering index on an aggregate must seek only the
/// constrained range of that covering index without table lookups, matching
/// live bundled SQLite 3.54.1 across `:memory:` and file-backed x direct and
/// prepared, while preserving `NOT INDEXED`, DESC scan order, and partial-index
/// refusal contracts.
#[test]
fn bd_aj9v4_indexed_by_covering_aggregates_match_stock_and_seek() {
    let setup: &[&str] = &[
        "CREATE TABLE s(id INTEGER PRIMARY KEY, g INTEGER, h INTEGER, val INTEGER, pad TEXT)",
        "INSERT INTO s VALUES \
            (1, NULL, 10, 5, 'n1'), \
            (2, NULL, 20, NULL, 'n2'), \
            (3, 1, 100, 10, 'a'), \
            (4, 1, 200, 25, 'b'), \
            (5, 2, 100, 30, 'c'), \
            (6, 2, 300, NULL, 'd'), \
            (7, 3, 150, 40, 'e'), \
            (8, 3, 250, 55, 'f'), \
            (9, 4, 100, 70, 'g'), \
            (10, 5, 500, 90, 'h')",
        "CREATE INDEX s_g ON s(g)",
        "CREATE INDEX s_gh ON s(g, h)",
        "CREATE INDEX s_gv ON s(g, val)",
        "CREATE INDEX s_gv_desc ON s(g DESC, val DESC)",
        "CREATE INDEX s_partial ON s(g, val) WHERE g >= 3",
        // Overflow-order table to verify DESC forced-index scan order stays intact.
        "CREATE TABLE ord_t(id INTEGER PRIMARY KEY, k INTEGER, x INTEGER)",
        "CREATE INDEX ord_k_desc ON ord_t(k DESC)",
        "INSERT INTO ord_t VALUES (1, 3, 9223372036854775807), (2, 1, 1), (3, 2, -1)",
    ];

    let covering_seek_cases: &[(&str, &str)] = &[
        (
            "range_covering_gv",
            "SELECT count(*), sum(val) FROM s INDEXED BY s_gv WHERE g BETWEEN 2 AND 4",
        ),
        (
            "eq_covering_gv",
            "SELECT count(*), sum(val), min(val), max(val) FROM s INDEXED BY s_gv WHERE g = 3",
        ),
        (
            "eq_empty_covering_gv",
            "SELECT count(*), sum(val) FROM s INDEXED BY s_gv WHERE g = 999",
        ),
        (
            "composite_prefix_range_gv",
            "SELECT count(*), sum(val) FROM s INDEXED BY s_gv WHERE g = 3 AND val BETWEEN 35 AND 60",
        ),
        (
            "in_list_covering_gv",
            "SELECT count(*), sum(val) FROM s INDEXED BY s_gv WHERE g IN (1, 3, 5)",
        ),
        (
            "prefix_residual_covering_gv",
            "SELECT count(*), sum(val) FROM s INDEXED BY s_gv WHERE g = 3 AND val % 2 = 0",
        ),
        (
            "count_star_eq_gv",
            "SELECT count(*) FROM s INDEXED BY s_gv WHERE g = 2",
        ),
        (
            "count_star_is_null_gv",
            "SELECT count(*) FROM s INDEXED BY s_gv WHERE g IS NULL",
        ),
        (
            "count_star_is_not_null_gv",
            "SELECT count(*) FROM s INDEXED BY s_gv WHERE g IS NOT NULL",
        ),
        (
            "minmax_prefix_gv",
            "SELECT max(val) FROM s INDEXED BY s_gv WHERE g = 3",
        ),
        (
            "groupby_range_covering_gv",
            "SELECT g, count(*), sum(val) FROM s INDEXED BY s_gv WHERE g BETWEEN 1 AND 4 GROUP BY g",
        ),
        (
            "groupby_eq_covering_gv",
            "SELECT g, count(*), sum(val) FROM s INDEXED BY s_gv WHERE g = 3 GROUP BY g",
        ),
    ];

    let extra_parity_cases: &[(&str, &str)] = &[
        (
            "unseekable_where_covering_gv",
            "SELECT count(*), sum(val) FROM s INDEXED BY s_gv WHERE val > 35",
        ),
        (
            "non_covering_forced_gh",
            "SELECT count(*), sum(val) FROM s INDEXED BY s_gh WHERE g BETWEEN 2 AND 4",
        ),
        (
            "not_indexed_negative",
            "SELECT count(*), sum(val) FROM s NOT INDEXED WHERE g BETWEEN 2 AND 4",
        ),
        (
            "count_star_no_where_gv",
            "SELECT count(*) FROM s INDEXED BY s_gv",
        ),
        (
            "count_star_no_where_not_indexed",
            "SELECT count(*) FROM s NOT INDEXED",
        ),
        (
            "groupby_no_where_gv",
            "SELECT g, count(*), sum(val) FROM s INDEXED BY s_gv GROUP BY g",
        ),
        (
            "desc_forced_index_range",
            "SELECT count(*), sum(val) FROM s INDEXED BY s_gv_desc WHERE g BETWEEN 2 AND 4",
        ),
        (
            "desc_forced_index_overflow_order",
            "SELECT sum(x) FROM ord_t INDEXED BY ord_k_desc",
        ),
        (
            "partial_forced_index_covered",
            "SELECT count(*), sum(val) FROM s INDEXED BY s_partial WHERE g >= 3",
        ),
    ];

    let stock = rusqlite::Connection::open_in_memory().unwrap();
    for stmt in setup {
        stock.execute(stmt, []).unwrap();
    }
    let expected_covering: Vec<(&str, &str, Vec<Vec<String>>)> = covering_seek_cases
        .iter()
        .map(|&(name, sql)| (name, sql, stock_query_rows(&stock, sql).unwrap()))
        .collect();
    let expected_extra: Vec<(&str, &str, Vec<Vec<String>>)> = extra_parity_cases
        .iter()
        .map(|&(name, sql)| (name, sql, stock_query_rows(&stock, sql).unwrap()))
        .collect();
    assert!(
        stock_query_rows(
            &stock,
            "SELECT count(*), sum(val) FROM s INDEXED BY s_partial WHERE g = 1"
        )
        .is_err(),
        "stock must reject unusable forced partial index"
    );

    asupersync::test_utils::run_test(|| async {
        for file_backed in [false, true] {
            for prepared in [false, true] {
                let dir = tempfile::tempdir().unwrap();
                let path = if file_backed {
                    dir.path()
                        .join("bd_aj9v4.db")
                        .to_str()
                        .unwrap()
                        .to_owned()
                } else {
                    ":memory:".to_owned()
                };
                let conn = Connection::open(&path).await.unwrap();
                for stmt in setup {
                    conn.execute(stmt).await.unwrap();
                }

                for (name, sql, expected) in &expected_covering {
                    let got = fsqlite_query_rows(&conn, sql, prepared).await.unwrap();
                    assert_eq!(
                        &got, expected,
                        "covering case={name} file_backed={file_backed} prepared={prepared}\nsql: {sql}"
                    );
                }
                for (name, sql, expected) in &expected_extra {
                    let got = fsqlite_query_rows(&conn, sql, prepared).await.unwrap();
                    assert_eq!(
                        &got, expected,
                        "extra case={name} file_backed={file_backed} prepared={prepared}\nsql: {sql}"
                    );
                }

                // Bound parameter parity on INDEXED BY covering aggregate.
                let bound = conn
                    .query_with_params(
                        "SELECT count(*), sum(val) FROM s INDEXED BY s_gv WHERE g = ?1",
                        &[SqliteValue::Integer(3)],
                    )
                    .await
                    .unwrap();
                assert_eq!(bound[0].values()[0], SqliteValue::Integer(2));
                assert_eq!(bound[0].values()[1], SqliteValue::Integer(95));

                // Unusable forced partial index must still error.
                assert!(
                    fsqlite_query_rows(
                        &conn,
                        "SELECT count(*), sum(val) FROM s INDEXED BY s_partial WHERE g = 1",
                        prepared,
                    )
                    .await
                    .is_err(),
                    "unusable forced partial index must error (file_backed={file_backed} prepared={prepared})"
                );

                // Opcode & EQP verification (once per DB backing).
                if !prepared {
                    for (name, sql) in covering_seek_cases {
                        let explain_rows = conn.query(&format!("EXPLAIN {sql}")).await.unwrap();
                        let ops: Vec<(String, String)> = explain_rows
                            .iter()
                            .map(|r| {
                                (
                                    render_fsqlite_val(&r.values()[1]),
                                    render_fsqlite_val(&r.values()[5]),
                                )
                            })
                            .collect();
                        assert!(
                            ops.iter().any(|(op, p4)| op == "'OpenRead'" && p4 == "'s_gv'"),
                            "case={name}: expected OpenRead on s_gv, got {ops:?}"
                        );
                        assert!(
                            !ops.iter().any(|(op, p4)| op == "'OpenRead'" && p4 == "'s'"),
                            "case={name}: expected covering s_gv with no table OpenRead, got {ops:?}"
                        );
                        assert!(
                            !ops.iter().any(|(op, _)| op == "'SeekRowid'"),
                            "case={name}: expected no SeekRowid, got {ops:?}"
                        );
                    }

                    // EXPLAIN QUERY PLAN matches covering SEARCH on s_gv.
                    let eqp_range = conn
                        .query(
                            "EXPLAIN QUERY PLAN SELECT count(*), sum(val) FROM s INDEXED BY s_gv WHERE g BETWEEN 2 AND 4",
                        )
                        .await
                        .unwrap();
                    let eqp_text: Vec<String> = eqp_range
                        .iter()
                        .map(|r| render_fsqlite_val(&r.values()[3]))
                        .collect();
                    assert!(
                        eqp_text
                            .iter()
                            .any(|d| d.contains("SEARCH s USING COVERING INDEX s_gv")),
                        "expected COVERING INDEX s_gv in EQP, got {eqp_text:?}"
                    );

                    // Unseekable covering WHERE avoids table OpenRead and SeekRowid.
                    let explain_full_cover = conn
                        .query("EXPLAIN SELECT count(*), sum(val) FROM s INDEXED BY s_gv WHERE val > 35")
                        .await
                        .unwrap();
                    assert!(
                        !explain_full_cover
                            .iter()
                            .any(|r| render_fsqlite_val(&r.values()[1]) == "'SeekRowid'"),
                        "unseekable covering WHERE on INDEXED BY s_gv should avoid SeekRowid"
                    );

                    // NOT INDEXED negative control opens table, never any index.
                    let explain_not_indexed = conn
                        .query("EXPLAIN SELECT count(*), sum(val) FROM s NOT INDEXED WHERE g BETWEEN 2 AND 4")
                        .await
                        .unwrap();
                    assert!(
                        !explain_not_indexed.iter().any(|r| {
                            render_fsqlite_val(&r.values()[1]) == "'OpenRead'"
                                && matches!(
                                    render_fsqlite_val(&r.values()[5]).as_str(),
                                    "'s_g'" | "'s_gh'" | "'s_gv'" | "'s_gv_desc'" | "'s_partial'"
                                )
                        }),
                        "NOT INDEXED must never open a secondary index"
                    );
                }
            }
        }
    });
}
