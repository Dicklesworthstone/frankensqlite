#![recursion_limit = "512"]

//! An index that serves `WHERE col = value ORDER BY <next key column>` seeks its
//! equality prefix with SQLite's comparison affinity and collation.
//!
//! The ORDER BY index lane (`codegen_select_index_ordered_scan`) seeked the
//! equality prefix with the raw value and walked only that block, with no
//! fallback scan. So `nu = '1' ORDER BY id` over `INDEX(nu, id)` on a NUMERIC,
//! INTEGER or REAL column returned no rows: the TEXT probe '1' lands among the
//! TEXT keys, while SQLite converts it to the number 1 first. The same held for
//! a bound TEXT parameter, and `k = 'v' COLLATE NOCASE` seeked a BINARY index
//! for 'v' only. Found by the fresh-eyes review of 71556d531 (the lane predates
//! it). Results are compared with rusqlite, in memory and file-backed, as
//! literals and as parameters.

use fsqlite_core::connection::Connection;
use fsqlite_types::value::SqliteValue;

fn tag_f(v: &SqliteValue) -> String {
    match v {
        SqliteValue::Null => "NULL".to_owned(),
        SqliteValue::Integer(n) => format!("i{n}"),
        SqliteValue::Float(f) => format!("r{f}"),
        SqliteValue::Text(s) => format!("'{s}'"),
        SqliteValue::Blob(b) => format!("x{b:?}"),
    }
}

fn tag_r(v: &rusqlite::types::Value) -> String {
    match v {
        rusqlite::types::Value::Null => "NULL".to_owned(),
        rusqlite::types::Value::Integer(n) => format!("i{n}"),
        rusqlite::types::Value::Real(f) => format!("r{f}"),
        rusqlite::types::Value::Text(s) => format!("'{s}'"),
        rusqlite::types::Value::Blob(b) => format!("x{b:?}"),
    }
}

fn to_r(v: &SqliteValue) -> rusqlite::types::Value {
    match v {
        SqliteValue::Null => rusqlite::types::Value::Null,
        SqliteValue::Integer(n) => rusqlite::types::Value::Integer(*n),
        SqliteValue::Float(f) => rusqlite::types::Value::Real(*f),
        SqliteValue::Text(s) => rusqlite::types::Value::Text(s.to_string()),
        SqliteValue::Blob(b) => rusqlite::types::Value::Blob(b.to_vec()),
    }
}

const SETUP: &[&str] = &[
    "CREATE TABLE p(id INTEGER PRIMARY KEY, a TEXT, k TEXT, n INTEGER, r REAL, nu NUMERIC, x, \
     kn TEXT COLLATE NOCASE)",
    "INSERT INTO p VALUES (1,'A','v',1,1,1,1,'v'),(2,'A','V','1','1','1','1','V'),\
     (3,'a','1',1.0,1.0,1.0,1.0,'1'),(4,'A',NULL,NULL,NULL,NULL,NULL,NULL),\
     (5,'B','abc','2.5','2.5','2.5','2.5','ABC'),(6,'A',' 1',' 1',' 1',' 1',' 1',' 1'),\
     (7,'A','1.0','1.0','1.0','1.0','1.0','1.0'),(8,'A',x'31',x'31',x'31',x'31',x'31',x'31'),\
     (9,'A','2',2,2,2,2,'2'),(10,'A',2.5,2.5,2.5,2.5,2.5,2.5),(11,'A','abc','abc','abc',\
     'abc','abc','abc')",
    "CREATE INDEX p_kid ON p(k, id)",
    "CREATE INDEX p_nid ON p(n, id)",
    "CREATE INDEX p_rid ON p(r, id)",
    "CREATE INDEX p_nuid ON p(nu, id)",
    "CREATE INDEX p_xid ON p(x, id)",
    "CREATE INDEX p_knid ON p(kn, id)",
    "CREATE INDEX p_akid ON p(a, k, id)",
];

const VALUES: &[&str] = &[
    "1", "'1'", "1.0", "'1.0'", "' 1'", "2.5", "'2.5'", "'v'", "'V'", "'abc'", "x'31'", "NULL",
];

fn params() -> Vec<SqliteValue> {
    vec![
        SqliteValue::Integer(1),
        SqliteValue::Float(1.0),
        SqliteValue::Float(2.5),
        SqliteValue::Text("1".into()),
        SqliteValue::Text(" 1".into()),
        SqliteValue::Text("1.0".into()),
        SqliteValue::Text("2.5".into()),
        SqliteValue::Text("v".into()),
        SqliteValue::Text("V".into()),
        SqliteValue::Blob(b"1".to_vec().into()),
        SqliteValue::Null,
    ]
}

fn queries() -> Vec<String> {
    let mut queries = Vec::new();
    for column in ["k", "n", "r", "nu", "x", "kn"] {
        for value in VALUES.iter().copied().chain(["?1"]) {
            for suffix in ["", " COLLATE NOCASE", " COLLATE BINARY"] {
                for order in ["id", "id DESC"] {
                    queries.push(format!(
                        "SELECT id FROM p WHERE {column} = {value}{suffix} ORDER BY {order}"
                    ));
                }
            }
            queries.push(format!(
                "SELECT id FROM p WHERE {value} = {column} ORDER BY id"
            ));
        }
    }
    for value in VALUES.iter().copied().chain(["?1"]) {
        queries.push(format!(
            "SELECT id FROM p WHERE a = 'A' AND k = {value} ORDER BY id"
        ));
        queries.push(format!(
            "SELECT id FROM p WHERE a = 'a' COLLATE NOCASE AND k = {value} ORDER BY id DESC"
        ));
    }
    queries
}

async fn franken_rows(
    conn: &Connection,
    sql: &str,
    params: &[SqliteValue],
) -> Result<Vec<String>, String> {
    conn.query_with_params(sql, params)
        .await
        .map(|rows| {
            rows.iter()
                .map(|row| row.values().iter().map(tag_f).collect::<Vec<_>>().join("|"))
                .collect()
        })
        .map_err(|error| error.to_string())
}

fn stock_rows(
    conn: &rusqlite::Connection,
    sql: &str,
    params: &[SqliteValue],
) -> Result<Vec<String>, String> {
    let mut stmt = conn.prepare(sql).map_err(|error| error.to_string())?;
    let ncol = stmt.column_count();
    let params: Vec<rusqlite::types::Value> = params.iter().map(to_r).collect();
    stmt.query_map(rusqlite::params_from_iter(params.iter()), |row| {
        Ok((0..ncol)
            .map(|i| tag_r(&row.get::<_, rusqlite::types::Value>(i).unwrap()))
            .collect::<Vec<_>>()
            .join("|"))
    })
    .map_err(|error| error.to_string())?
    .collect::<Result<_, _>>()
    .map_err(|error| error.to_string())
}

#[test]
fn ordered_index_equality_prefix_matches_sqlite() {
    for file_backed in [false, true] {
        asupersync::test_utils::run_test(|| async move {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("ordered_index_prefix.db");
            let f = if file_backed {
                Connection::open(path.to_str().unwrap()).await.unwrap()
            } else {
                Connection::open(":memory:").await.unwrap()
            };
            let r = rusqlite::Connection::open_in_memory().unwrap();
            for sql in SETUP {
                f.execute(sql).await.unwrap();
                r.execute_batch(sql).unwrap();
            }
            let mut found = Vec::new();
            for sql in queries() {
                let param_sets: Vec<Vec<SqliteValue>> = if sql.contains("?1") {
                    params().into_iter().map(|value| vec![value]).collect()
                } else {
                    vec![Vec::new()]
                };
                for params in param_sets {
                    let ff = franken_rows(&f, &sql, &params).await;
                    let rr = stock_rows(&r, &sql, &params);
                    if ff != rr {
                        found.push(format!(
                            "[file_backed={file_backed}] {sql} params={params:?}\n  \
                             fsqlite: {ff:?}\n  stock:   {rr:?}"
                        ));
                    }
                }
            }
            assert!(
                found.is_empty(),
                "{} mismatches vs stock:\n{}",
                found.len(),
                found.join("\n")
            );
        });
    }
}

/// The ORDER BY index lane still serves the shape after the fix: the prefix
/// seek remains (no table scan, no sorter) and the TEXT literal is converted
/// before it.
#[test]
fn ordered_index_prefix_keeps_the_seek_and_converts_the_probe() {
    asupersync::test_utils::run_test(|| async {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("ordered_index_prefix_plan.db");
        let f = Connection::open(path.to_str().unwrap()).await.unwrap();
        for sql in SETUP {
            f.execute(sql).await.unwrap();
        }
        let rows = f
            .query("EXPLAIN SELECT id FROM p WHERE nu = '1' ORDER BY id")
            .await
            .unwrap();
        let opcodes: Vec<String> = rows
            .iter()
            .map(|row| match &row.values()[1] {
                SqliteValue::Text(op) => op.to_string(),
                other => format!("{other:?}"),
            })
            .collect();
        for expected in ["Affinity", "SeekGE", "IdxGT"] {
            assert!(
                opcodes.iter().any(|op| op == expected),
                "expected {expected} in {opcodes:?}"
            );
        }
        for absent in ["SorterOpen", "Rewind"] {
            assert!(
                !opcodes.iter().any(|op| op == absent),
                "unexpected {absent} in {opcodes:?}"
            );
        }
    });
}

/// bd-text-probe-numeric-index-qk7vd: cross-type equality literals (`t = 7920`
/// on TEXT, `c = '7920'` on REAL, `i = '7920'` on INTEGER, `nu = '7920'` on
/// NUMERIC) and bound parameters must apply column-appropriate index affinity
/// BEFORE `SeekGE`, and neither hits nor 0-match misses may execute the
/// full-table scan fallback at runtime.
#[test]
fn bd_qk7vd_cross_type_equality_seeks_index_without_scan_fallback() {
    use fsqlite_vdbe::engine::{
        reset_vdbe_metrics, set_vdbe_metrics_thread_override, vdbe_metrics_snapshot,
    };

    const ROWS: i64 = 2_000;
    const SEEK_OPCODE_LIMIT: u64 = 600;
    // A tight full-table scan executes ~3 opcodes per row (~6,000 for 2,000 rows).
    const SCAN_CONTROL_MIN_OPCODES: u64 = 5_000;

    asupersync::test_utils::run_test(|| async {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("qk7vd_plan.db");
        let f = Connection::open(path.to_str().unwrap()).await.unwrap();
        for sql in [
            "CREATE TABLE s(id INTEGER PRIMARY KEY, t TEXT, c REAL, i INTEGER, nu NUMERIC, u, \
             kn TEXT COLLATE NOCASE, v INTEGER)",
            "CREATE TABLE wr(k TEXT PRIMARY KEY, c REAL, v INTEGER) WITHOUT ROWID",
            &format!(
                "INSERT INTO s WITH RECURSIVE g(x) AS (SELECT 1 UNION ALL SELECT x + 1 FROM g \
                 WHERE x < {ROWS}) SELECT x, CAST(x + 7000 AS TEXT), CAST(x + 7000 AS REAL), \
                 x + 7000, x + 7000, CASE WHEN x % 2 = 0 THEN CAST(x + 7000 AS TEXT) ELSE x + 7000 END, \
                 CAST(x + 7000 AS TEXT), x FROM g"
            ),
            &format!(
                "INSERT INTO wr WITH RECURSIVE g(x) AS (SELECT 1 UNION ALL SELECT x + 1 FROM g \
                 WHERE x < {ROWS}) SELECT 'k' || x, CAST(x + 7000 AS REAL), x FROM g"
            ),
            "CREATE INDEX s_t ON s(t)",
            "CREATE INDEX s_c ON s(c)",
            "CREATE INDEX s_i ON s(i)",
            "CREATE INDEX s_nu ON s(nu)",
            "CREATE INDEX s_u ON s(u)",
            "CREATE INDEX s_kn ON s(kn)",
            "CREATE INDEX s_ti ON s(t, i)",
            "CREATE INDEX wr_c ON wr(c)",
        ] {
            f.execute(sql).await.unwrap();
        }

        // Every query below must emit `Affinity` before `SeekGE` AND execute in
        // O(log n) opcodes (< SEEK_OPCODE_LIMIT) for both matching and absent keys.
        let must_seek_with_affinity: Vec<(&str, Vec<SqliteValue>)> = vec![
            // Covering & non-covering SELECT with cross-type literals (hits & misses)
            ("SELECT id FROM s WHERE t = 7920", vec![]),
            ("SELECT id FROM s WHERE t = 999999", vec![]),
            ("SELECT id, v FROM s WHERE t = 7920", vec![]),
            ("SELECT id FROM s WHERE t = 7920.5", vec![]),
            ("SELECT id FROM s WHERE c = '7920'", vec![]),
            ("SELECT id FROM s WHERE c = '999999'", vec![]),
            ("SELECT id, v FROM s WHERE c = '7920'", vec![]),
            ("SELECT id FROM s WHERE i = '7920'", vec![]),
            ("SELECT id FROM s WHERE i = '999999'", vec![]),
            ("SELECT id, v FROM s WHERE i = '7920'", vec![]),
            ("SELECT id FROM s WHERE nu = '7920'", vec![]),
            ("SELECT id FROM s WHERE kn = 7920", vec![]),
            // Parameterized probes (cross-type hits & 0-match misses)
            (
                "SELECT id FROM s WHERE t = ?1",
                vec![SqliteValue::Integer(7920)],
            ),
            (
                "SELECT id FROM s WHERE t = ?1",
                vec![SqliteValue::Integer(999_999)],
            ),
            (
                "SELECT id FROM s WHERE c = ?1",
                vec![SqliteValue::Text("7920".into())],
            ),
            (
                "SELECT id FROM s WHERE c = ?1",
                vec![SqliteValue::Text("999999".into())],
            ),
            (
                "SELECT id FROM s WHERE i = ?1",
                vec![SqliteValue::Text("7920".into())],
            ),
            (
                "SELECT id FROM s WHERE i = ?1",
                vec![SqliteValue::Text("999999".into())],
            ),
            // Equality + residual filter
            ("SELECT id FROM s WHERE t = 7920 AND v > 0", vec![]),
            ("SELECT id FROM s WHERE c = '7920' AND v > 0", vec![]),
            ("SELECT id FROM s WHERE i = '7920' AND v > 0", vec![]),
            // Composite prefix with cross-type literals
            ("SELECT id FROM s WHERE t = 7920 AND i = '7920'", vec![]),
            // Aggregates (COUNT(*) and SUM — hits & 0-match misses)
            ("SELECT count(*) FROM s WHERE t = 7920", vec![]),
            ("SELECT count(*) FROM s WHERE t = 999999", vec![]),
            ("SELECT count(*) FROM s WHERE c = '7920'", vec![]),
            ("SELECT count(*) FROM s WHERE i = '7920'", vec![]),
            (
                "SELECT count(*) FROM s WHERE t = ?1",
                vec![SqliteValue::Integer(7920)],
            ),
            ("SELECT sum(v) FROM s WHERE t = 7920", vec![]),
            ("SELECT sum(v) FROM s WHERE t = 999999", vec![]),
            ("SELECT sum(v) FROM s WHERE c = '7920'", vec![]),
            ("SELECT sum(v) FROM s WHERE i = '7920'", vec![]),
            (
                "SELECT sum(v) FROM s WHERE c = ?1",
                vec![SqliteValue::Text("7920".into())],
            ),
            // ORDER BY non-indexed column (outer_index_equality_seek)
            ("SELECT id, v FROM s WHERE t = 7920 ORDER BY v", vec![]),
            ("SELECT id, v FROM s WHERE c = '7920' ORDER BY v", vec![]),
            ("SELECT id, v FROM s WHERE i = '7920' ORDER BY v", vec![]),
            // WITHOUT ROWID secondary index
            ("SELECT k FROM wr WHERE c = '7920'", vec![]),
            ("SELECT k FROM wr WHERE c = '999999'", vec![]),
        ];

        let mut failures = Vec::new();
        for (sql, params) in &must_seek_with_affinity {
            let rows = f.query(&format!("EXPLAIN {sql}")).await.unwrap();
            let opcodes: Vec<String> = rows
                .iter()
                .map(|row| match &row.values()[1] {
                    SqliteValue::Text(op) => op.to_string(),
                    other => format!("{other:?}"),
                })
                .collect();
            let aff_pos = opcodes.iter().position(|op| op == "Affinity");
            let seek_pos = opcodes.iter().position(|op| op == "SeekGE");
            if !matches!((aff_pos, seek_pos), (Some(a), Some(s)) if a < s) {
                failures.push(format!(
                    "  [plan] `{sql}`: aff={aff_pos:?} seek={seek_pos:?}\n    opcodes={opcodes:?}"
                ));
                continue;
            }

            set_vdbe_metrics_thread_override(Some(true));
            reset_vdbe_metrics();
            let res = if params.is_empty() {
                f.query(sql).await
            } else {
                f.query_with_params(sql, params).await
            };
            let executed = vdbe_metrics_snapshot().opcodes_executed_total;
            set_vdbe_metrics_thread_override(None);
            res.unwrap();
            if executed > SEEK_OPCODE_LIMIT {
                failures.push(format!(
                    "  [cost] `{sql}` params={params:?}: executed {executed} opcodes (limit {SEEK_OPCODE_LIMIT})"
                ));
            }
        }

        // Positive full-scan controls: `+t = 7920` and `t = CAST(7920 AS INTEGER)`
        // cannot use `s_t` and must genuinely scan (proving the opcode gate works).
        for control_sql in [
            "SELECT id FROM s WHERE +t = 7920",
            "SELECT id FROM s WHERE t = CAST(7920 AS INTEGER)",
        ] {
            set_vdbe_metrics_thread_override(Some(true));
            reset_vdbe_metrics();
            f.query(control_sql).await.unwrap();
            let executed = vdbe_metrics_snapshot().opcodes_executed_total;
            set_vdbe_metrics_thread_override(None);
            if executed < SCAN_CONTROL_MIN_OPCODES {
                failures.push(format!(
                    "  [control] `{control_sql}` executed only {executed} opcodes (expected >= {SCAN_CONTROL_MIN_OPCODES})"
                ));
            }
        }

        f.close().await.unwrap();
        assert!(
            failures.is_empty(),
            "{} bd-qk7vd plan/cost failure(s):\n{}",
            failures.len(),
            failures.join("\n")
        );
    });
}

/// bd-text-probe-numeric-index-qk7vd: live bundled rusqlite differential oracle
/// across TEXT / REAL / INTEGER / NUMERIC / untyped / NOCASE / WITHOUT ROWID /
/// collation-mismatched indexes, including negative controls (`CAST` affinity
/// overrides, `i64::MAX` vs REAL precision, `-0.0`, `9e999`, BLOBs, missing
/// keys, aggregates, residuals, `ORDER BY`), across `:memory:` & file-backed
/// and ad hoc & prepared execution.
#[test]
fn bd_qk7vd_cross_type_equality_oracle_matches_stock() {
    let setup = [
        "CREATE TABLE s(id INTEGER PRIMARY KEY, t TEXT, c REAL, i INTEGER, nu NUMERIC, u, \
         kn TEXT COLLATE NOCASE, v INTEGER)",
        "INSERT INTO s VALUES \
         (1, '7920', 7920.0, 7920, 7920, '7920', '7920', 10), \
         (2, '07920', 7920.5, 7920, 7920.5, 7920, '07920', 20), \
         (3, '7920.0', 7920.0, 7921, 7920.0, 7920.0, 'AbC', 30), \
         (4, '7920.5', 'abc', 'abc', 'abc', 'abc', 'abc', 40), \
         (5, ' 7920 ', 0.0, 0, 0, 0, '7920.5', 50), \
         (6, '0', -0.0, 0, -0.0, -0.0, '0', 60), \
         (7, '0.0', 9e999, 9223372036854775807, 9223372036854775807, 9223372036854775807, 'x', 70), \
         (8, x'37393230', 9223372036854775807, x'37393230', x'37393230', x'37393230', x'37393230', 80), \
         (9, NULL, NULL, NULL, NULL, NULL, NULL, 90), \
         (10, '7920x', '7920x', '7920x', '7920x', '7920x', '7920X', 100)",
        "CREATE INDEX s_t ON s(t)",
        "CREATE INDEX s_c ON s(c)",
        "CREATE INDEX s_i ON s(i)",
        "CREATE INDEX s_nu ON s(nu)",
        "CREATE INDEX s_u ON s(u)",
        "CREATE INDEX s_kn ON s(kn)",
        "CREATE INDEX s_ti ON s(t, i)",
        // Collation-mismatch control tables
        "CREATE TABLE cm_bin_col(id INTEGER PRIMARY KEY, a TEXT, n INTEGER)",
        "CREATE INDEX cm_bin_col_nocase ON cm_bin_col(a COLLATE NOCASE)",
        "CREATE INDEX cm_bin_col_rtrim ON cm_bin_col(n COLLATE RTRIM)",
        "INSERT INTO cm_bin_col VALUES (1, 'a', 'x'), (2, 'A', 'x '), (3, '7920', 7920)",
        "CREATE TABLE cm_nc_col(id INTEGER PRIMARY KEY, a TEXT COLLATE NOCASE)",
        "CREATE INDEX cm_nc_col_bin ON cm_nc_col(a COLLATE BINARY)",
        "INSERT INTO cm_nc_col VALUES (1, 'a'), (2, 'A'), (3, '7920')",
        // WITHOUT ROWID table
        "CREATE TABLE wr(k TEXT PRIMARY KEY, c REAL, t TEXT, v INTEGER) WITHOUT ROWID",
        "CREATE INDEX wr_c ON wr(c)",
        "CREATE INDEX wr_t ON wr(t)",
        "INSERT INTO wr VALUES ('a', 7920.0, '7920', 1), ('b', 7920.5, '07920', 2), ('c', 'abc', 'abc', 3)",
    ];

    let literal_queries = [
        // Cross-type literals on TEXT, REAL, INTEGER, NUMERIC, untyped, NOCASE
        "SELECT id FROM s WHERE t = 7920 ORDER BY id",
        "SELECT id, v FROM s WHERE t = 7920 ORDER BY id",
        "SELECT id FROM s WHERE t = 7920.0 ORDER BY id",
        "SELECT id FROM s WHERE t = 7920.5 ORDER BY id",
        "SELECT id FROM s WHERE t = 0 ORDER BY id",
        "SELECT id FROM s WHERE t = 0.0 ORDER BY id",
        "SELECT id FROM s WHERE t = 999999 ORDER BY id",
        "SELECT id FROM s WHERE t = x'37393230' ORDER BY id",
        "SELECT id FROM s WHERE c = '7920' ORDER BY id",
        "SELECT id, v FROM s WHERE c = '7920' ORDER BY id",
        "SELECT id FROM s WHERE c = '07920' ORDER BY id",
        "SELECT id FROM s WHERE c = ' 7920 ' ORDER BY id",
        "SELECT id FROM s WHERE c = '7920.0' ORDER BY id",
        "SELECT id FROM s WHERE c = '7920.5' ORDER BY id",
        "SELECT id FROM s WHERE c = '0' ORDER BY id",
        "SELECT id FROM s WHERE c = '-0.0' ORDER BY id",
        "SELECT id FROM s WHERE c = 'abc' ORDER BY id",
        "SELECT id FROM s WHERE c = '7920x' ORDER BY id",
        "SELECT id FROM s WHERE c = '999999' ORDER BY id",
        "SELECT id FROM s WHERE c = '9e999' ORDER BY id",
        // i64::MAX vs REAL precision (must cap index affinity at NUMERIC 'C')
        "SELECT id FROM s WHERE c = '9223372036854775807' ORDER BY id",
        "SELECT id FROM s WHERE c = 9223372036854775807 ORDER BY id",
        "SELECT id FROM s WHERE i = '7920' ORDER BY id",
        "SELECT id, v FROM s WHERE i = '7920' ORDER BY id",
        "SELECT id FROM s WHERE i = '07920' ORDER BY id",
        "SELECT id FROM s WHERE i = '7920.0' ORDER BY id",
        "SELECT id FROM s WHERE i = '7920.5' ORDER BY id",
        "SELECT id FROM s WHERE i = 'abc' ORDER BY id",
        "SELECT id FROM s WHERE i = '999999' ORDER BY id",
        "SELECT id FROM s WHERE i = '9223372036854775807' ORDER BY id",
        "SELECT id FROM s WHERE i = x'37393230' ORDER BY id",
        "SELECT id FROM s WHERE nu = '7920' ORDER BY id",
        "SELECT id FROM s WHERE nu = '7920.5' ORDER BY id",
        "SELECT id FROM s WHERE nu = '0' ORDER BY id",
        "SELECT id FROM s WHERE nu = 'abc' ORDER BY id",
        "SELECT id FROM s WHERE nu = '999999' ORDER BY id",
        "SELECT id FROM s WHERE u = 7920 ORDER BY id",
        "SELECT id FROM s WHERE u = '7920' ORDER BY id",
        "SELECT id FROM s WHERE u = 7920.0 ORDER BY id",
        "SELECT id FROM s WHERE u = '999999' ORDER BY id",
        "SELECT id FROM s WHERE kn = 7920 ORDER BY id",
        "SELECT id FROM s WHERE kn = 'abc' ORDER BY id",
        "SELECT id FROM s WHERE kn = '7920x' ORDER BY id",
        // Covering (no ORDER BY) + non-indexed ORDER BY + residual + composite
        "SELECT id FROM s WHERE t = 7920",
        "SELECT id FROM s WHERE c = '7920'",
        "SELECT id FROM s WHERE i = '7920'",
        "SELECT id, v FROM s WHERE t = 7920 ORDER BY v",
        "SELECT id, v FROM s WHERE c = '7920' ORDER BY v",
        "SELECT id, v FROM s WHERE i = '7920' ORDER BY v",
        "SELECT id FROM s WHERE t = 7920 AND v >= 10",
        "SELECT id FROM s WHERE c = '7920' AND v >= 20",
        "SELECT id FROM s WHERE i = '7920' AND v >= 15",
        "SELECT id FROM s WHERE t = 7920 AND i = '7920'",
        "SELECT id FROM s WHERE t = 7920 AND i = '999'",
        // Aggregates
        "SELECT count(*) FROM s WHERE t = 7920",
        "SELECT count(*) FROM s WHERE t = 999999",
        "SELECT count(*) FROM s WHERE c = '7920'",
        "SELECT count(*) FROM s WHERE c = '999999'",
        "SELECT count(*) FROM s WHERE c = '9223372036854775807'",
        "SELECT count(*) FROM s WHERE i = '7920'",
        "SELECT count(*) FROM s WHERE i = '999999'",
        "SELECT sum(v) FROM s WHERE t = 7920",
        "SELECT sum(v) FROM s WHERE c = '7920'",
        "SELECT sum(v) FROM s WHERE i = '7920'",
        "SELECT sum(v) FROM s WHERE t = 999999",
        "SELECT sum(v) FROM s WHERE c = '999999'",
        "SELECT count(*) FROM s WHERE t = 7920 AND i = '7920'",
        // Negative controls: explicit CAST changes comparison affinity
        "SELECT id FROM s WHERE t = CAST(7920 AS INTEGER) ORDER BY id",
        "SELECT count(*) FROM s WHERE t = CAST(7920 AS INTEGER)",
        "SELECT id FROM s WHERE u = CAST(7920 AS INTEGER) ORDER BY id",
        "SELECT count(*) FROM s WHERE u = CAST(7920 AS INTEGER)",
        "SELECT id FROM s WHERE t = CAST(7920 AS BLOB) ORDER BY id",
        "SELECT id FROM s WHERE c = CAST('7920' AS TEXT) ORDER BY id",
        "SELECT id FROM s WHERE t = 'abc' COLLATE NOCASE ORDER BY id",
        // Collation-mismatched indexes
        "SELECT id FROM cm_bin_col WHERE a = 'a' ORDER BY id",
        "SELECT id FROM cm_bin_col WHERE a = 7920 ORDER BY id",
        "SELECT id FROM cm_bin_col WHERE n = 'x' ORDER BY id",
        "SELECT id FROM cm_bin_col WHERE n = 'x ' ORDER BY id",
        "SELECT id FROM cm_bin_col WHERE n = '7920' ORDER BY id",
        "SELECT id FROM cm_nc_col WHERE a = 'a' ORDER BY id",
        "SELECT id FROM cm_nc_col WHERE a = 7920 ORDER BY id",
        // WITHOUT ROWID
        "SELECT k FROM wr WHERE c = '7920' ORDER BY k",
        "SELECT k FROM wr WHERE c = '7920.5' ORDER BY k",
        "SELECT k FROM wr WHERE c = '999999' ORDER BY k",
        "SELECT k FROM wr WHERE t = 7920 ORDER BY k",
        "SELECT k FROM wr WHERE t = CAST(7920 AS INTEGER) ORDER BY k",
    ];

    let param_queries = [
        "SELECT id FROM s WHERE t = ?1 ORDER BY id",
        "SELECT id, v FROM s WHERE t = ?1 ORDER BY v",
        "SELECT id FROM s WHERE c = ?1 ORDER BY id",
        "SELECT id, v FROM s WHERE c = ?1 ORDER BY v",
        "SELECT id FROM s WHERE i = ?1 ORDER BY id",
        "SELECT id FROM s WHERE nu = ?1 ORDER BY id",
        "SELECT id FROM s WHERE u = ?1 ORDER BY id",
        "SELECT id FROM s WHERE kn = ?1 ORDER BY id",
        "SELECT count(*) FROM s WHERE t = ?1",
        "SELECT count(*) FROM s WHERE c = ?1",
        "SELECT count(*) FROM s WHERE i = ?1",
        "SELECT sum(v) FROM s WHERE t = ?1",
        "SELECT sum(v) FROM s WHERE c = ?1",
        "SELECT sum(v) FROM s WHERE i = ?1",
        "SELECT k FROM wr WHERE c = ?1 ORDER BY k",
        "SELECT k FROM wr WHERE t = ?1 ORDER BY k",
    ];

    let bind_values = vec![
        SqliteValue::Integer(7920),
        SqliteValue::Integer(0),
        SqliteValue::Integer(999_999),
        SqliteValue::Integer(i64::MAX),
        SqliteValue::Float(7920.0),
        SqliteValue::Float(7920.5),
        SqliteValue::Float(-0.0),
        SqliteValue::Text("7920".into()),
        SqliteValue::Text("07920".into()),
        SqliteValue::Text(" 7920 ".into()),
        SqliteValue::Text("7920.0".into()),
        SqliteValue::Text("7920.5".into()),
        SqliteValue::Text("abc".into()),
        SqliteValue::Text("7920x".into()),
        SqliteValue::Text("9223372036854775807".into()),
        SqliteValue::Text("999999".into()),
        SqliteValue::Blob(b"7920".as_slice().into()),
        SqliteValue::Null,
    ];

    for file_backed in [false, true] {
        asupersync::test_utils::run_test(|| async {
            let dir = tempfile::tempdir().unwrap();
            let (f, r) = if file_backed {
                (
                    Connection::open(dir.path().join("f.db").to_str().unwrap())
                        .await
                        .unwrap(),
                    rusqlite::Connection::open(dir.path().join("r.db")).unwrap(),
                )
            } else {
                (
                    Connection::open(":memory:").await.unwrap(),
                    rusqlite::Connection::open_in_memory().unwrap(),
                )
            };
            for sql in setup {
                f.execute(sql).await.unwrap();
                r.execute_batch(sql).unwrap();
            }

            let mut diffs = Vec::new();
            for sql in literal_queries {
                let stock = stock_rows(&r, sql, &[]);
                let ad_hoc = franken_rows(&f, sql, &[]).await;
                if ad_hoc != stock {
                    diffs.push(format!(
                        "[file={file_backed}, ad_hoc] `{sql}`\n  fsqlite: {ad_hoc:?}\n  stock:   {stock:?}"
                    ));
                }
                let prep_stmt = f.prepare(sql).await.unwrap();
                let prepared: Result<Vec<String>, String> = prep_stmt
                    .query()
                    .await
                    .map(|rows| {
                        rows.iter()
                            .map(|row| row.values().iter().map(tag_f).collect::<Vec<_>>().join("|"))
                            .collect()
                    })
                    .map_err(|error| error.to_string());
                drop(prep_stmt);
                if prepared != stock {
                    diffs.push(format!(
                        "[file={file_backed}, prepared] `{sql}`\n  fsqlite: {prepared:?}\n  stock:   {stock:?}"
                    ));
                }
            }

            for sql in param_queries {
                let prep_stmt = f.prepare(sql).await.unwrap();
                for val in &bind_values {
                    let params = [val.clone()];
                    let stock = stock_rows(&r, sql, &params);
                    let ad_hoc = franken_rows(&f, sql, &params).await;
                    if ad_hoc != stock {
                        diffs.push(format!(
                            "[file={file_backed}, ad_hoc] `{sql}` param={val:?}\n  fsqlite: {ad_hoc:?}\n  stock:   {stock:?}"
                        ));
                    }
                    let prepared: Result<Vec<String>, String> = prep_stmt
                        .query_with_params(&params)
                        .await
                        .map(|rows| {
                            rows.iter()
                                .map(|row| {
                                    row.values().iter().map(tag_f).collect::<Vec<_>>().join("|")
                                })
                                .collect()
                        })
                        .map_err(|error| error.to_string());
                    if prepared != stock {
                        diffs.push(format!(
                            "[file={file_backed}, prepared] `{sql}` param={val:?}\n  fsqlite: {prepared:?}\n  stock:   {stock:?}"
                        ));
                    }
                }
                drop(prep_stmt);
            }

            f.close().await.unwrap();
            assert!(
                diffs.is_empty(),
                "{} bd-qk7vd oracle divergence(s):\n{}",
                diffs.len(),
                diffs.iter().take(40).cloned().collect::<Vec<_>>().join("\n")
            );
        });
    }
}
