#![recursion_limit = "512"]

//! Implicit aggregates over lookup joins (bd-41o3t, GH#527):
//! `SELECT count(*), sum(t.c) FROM u JOIN t ON t.a = u.a` on a file-backed
//! database compiles into a rowid or index lookup loop that feeds each match
//! to AggStep, instead of materializing every joined row first. count(*)
//! over an index lookup reads only the index.
//!
//! Each query is compared with rusqlite, with and without indexes on the
//! lookup columns. The cases cover INNER and LEFT joins, either FROM order,
//! expression keys, WHERE and extra ON terms, typed and untyped keys,
//! collated columns, an empty table, and a bound parameter. Shapes the lookup
//! loop does not handle (OR in ON, DISTINCT aggregates) must still answer
//! correctly through the general path.
//! Multi-table chains additionally cover unique lookups, terminal LEFT JOIN
//! null extension, empty inputs, prepared execution, and snapshot visibility.

use fsqlite_core::connection::Connection;
use fsqlite_types::value::SqliteValue;

fn tag_f(v: &SqliteValue) -> String {
    match v {
        SqliteValue::Null => "NULL".to_owned(),
        SqliteValue::Integer(n) => n.to_string(),
        SqliteValue::Float(f) => format!("{f}"),
        SqliteValue::Text(s) => format!("'{s}'"),
        SqliteValue::Blob(b) => format!("blob{}", b.len()),
    }
}

fn tag_r(v: &rusqlite::types::Value) -> String {
    match v {
        rusqlite::types::Value::Null => "NULL".to_owned(),
        rusqlite::types::Value::Integer(n) => n.to_string(),
        rusqlite::types::Value::Real(f) => format!("{f}"),
        rusqlite::types::Value::Text(s) => format!("'{s}'"),
        rusqlite::types::Value::Blob(b) => format!("blob{}", b.len()),
    }
}

const SETUP: &[&str] = &[
    "CREATE TABLE t(id INTEGER PRIMARY KEY, a INTEGER, b TEXT, c REAL, d, n TEXT COLLATE NOCASE)",
    "INSERT INTO t VALUES (1,1,'1',1.0,'1','A'),(2,2,'2',2.5,2,'b'),(3,NULL,'x',3.0,NULL,'C'),\
     (4,4,'4',4.0,4.0,'d'),(5,5,'05',5.0,'5','e'),(6,6,'6',6.0,x'36','F'),(10,10,'10',10.0,10,'aa'),\
     (12,2,'2',12.0,'2','B')",
    "CREATE TABLE u(id INTEGER PRIMARY KEY, a INTEGER, b TEXT, r REAL, d)",
    "INSERT INTO u VALUES (1,1,'1',0.5,1),(2,2,'2',1.0,'2'),(3,3,'3',1.5,NULL),(4,NULL,NULL,2.0,4),\
     (5,5,'5',2.5,'05'),(6,6,'a',3.0,6.0),(7,7,'7',5.0,'x')",
    "CREATE TABLE e(id INTEGER PRIMARY KEY, a INTEGER)",
];

const INDEXES: &[&str] = &[
    "CREATE INDEX t_a ON t(a)",
    "CREATE INDEX t_b ON t(b)",
    "CREATE INDEX t_n ON t(n)",
    "CREATE INDEX t_d ON t(d)",
    "CREATE INDEX e_a ON e(a)",
];

const QUERIES: &[&str] = &[
    "SELECT count(*) FROM u JOIN t ON t.a = u.a",
    "SELECT count(*) FROM t JOIN u ON t.a = u.a",
    "SELECT count(*) FROM u JOIN t ON t.id = u.id",
    "SELECT count(*) FROM u JOIN t ON t.id = u.a * 2",
    "SELECT count(*), sum(t.c), total(t.c), avg(t.c), min(t.c), max(t.c) FROM u JOIN t ON t.a = u.a",
    "SELECT count(t.b), count(u.b), min(t.b), max(u.b) FROM u JOIN t ON t.a = u.a",
    "SELECT sum(t.c * u.r), sum(t.a + u.a), max(t.b || u.b) FROM u JOIN t ON t.a = u.a",
    "SELECT count(*), count(t.id), sum(t.a) FROM u LEFT JOIN t ON t.a = u.a",
    "SELECT count(*), count(t.id), sum(t.c) FROM u LEFT JOIN t ON t.id = u.a + 3",
    "SELECT count(*) FROM u LEFT JOIN t ON t.a = u.a WHERE t.id IS NULL",
    "SELECT count(*), sum(u.r) FROM u JOIN t ON t.a = u.a WHERE u.r > 1",
    "SELECT count(*) FROM u JOIN t ON t.a = u.a WHERE t.c > 3",
    "SELECT count(*) FROM u JOIN t ON t.b = u.b",
    "SELECT count(*) FROM u JOIN t ON t.d = u.d",
    "SELECT count(*), min(t.n), max(t.n) FROM u JOIN t ON t.n = u.b",
    "SELECT min(t.n), max(t.n) FROM u JOIN t ON t.a = u.a",
    "SELECT count(*), sum(u.a), avg(u.a) FROM e JOIN u ON e.a = u.a",
    "SELECT count(*), sum(e.a) FROM u LEFT JOIN e ON e.a = u.a",
    "SELECT count(*) FROM u JOIN t ON t.id = u.b || ''",
    "SELECT count(*) FROM u JOIN t ON t.id = u.r * 2",
    "SELECT sum(t.id), max(u.id) FROM u JOIN t ON t.a = u.a AND t.c > 2",
    "SELECT count(*) FROM u JOIN t ON t.a = u.a OR t.id = u.id",
    "SELECT count(DISTINCT t.a) FROM u JOIN t ON t.a = u.a",
    "SELECT count(*) FROM u AS x JOIN t AS y ON y.a = x.a",
    "SELECT count(*) FROM t a JOIN t b ON b.a = a.id",
    "SELECT sum(t.rowid), count(u.rowid) FROM u JOIN t ON t.a = u.a",
];

async fn assert_agree(
    fconn: &Connection,
    rconn: &rusqlite::Connection,
    sql: &str,
    params: &[SqliteValue],
    rparams: &[&dyn rusqlite::ToSql],
) {
    let ff: Vec<Vec<String>> = fconn
        .query_with_params(sql, params)
        .await
        .unwrap_or_else(|e| panic!("franken `{sql}`: {e:?}"))
        .iter()
        .map(|row| row.values().iter().map(tag_f).collect())
        .collect();
    let mut stmt = rconn.prepare(sql).expect("rusqlite prepare");
    let ncol = stmt.column_count();
    let rr: Vec<Vec<String>> = stmt
        .query_map(rparams, |row| {
            Ok((0..ncol)
                .map(|i| tag_r(&row.get::<_, rusqlite::types::Value>(i).unwrap()))
                .collect())
        })
        .expect("rusqlite query")
        .map(|r| r.unwrap())
        .collect();
    assert_eq!(ff, rr, "mismatch on `{sql}`");
}

async fn opcodes(conn: &Connection, sql: &str) -> Vec<String> {
    conn.query(&format!("EXPLAIN {sql}"))
        .await
        .unwrap()
        .iter()
        .filter_map(|row| match row.values().get(1) {
            Some(SqliteValue::Text(op)) => Some(op.to_string()),
            _ => None,
        })
        .collect()
}

/// bd-24pa2: an aggregate over a join that names a column no source has
/// reports SQLite's `no such column`, not an internal error.
#[test]
fn join_aggregate_unknown_column_reports_no_such_column() {
    for path in [None, Some("join_aggregate_unknown_column.db")] {
        asupersync::test_utils::run_test(|| async move {
            let dir = tempfile::tempdir().unwrap();
            let target = path.map_or_else(
                || ":memory:".to_owned(),
                |name| dir.path().join(name).to_string_lossy().into_owned(),
            );
            let f = Connection::open(&target).await.unwrap();
            for sql in SETUP {
                f.execute(sql).await.unwrap();
            }
            for sql in [
                "SELECT count(*), sum(zz.c) FROM u JOIN e ON e.a = u.a",
                "SELECT u.b, max(zz.c) FROM u JOIN t ON t.a = u.a GROUP BY u.b",
            ] {
                let err = f.query(sql).await.expect_err(sql).to_string();
                assert!(
                    err.contains("no such column: zz.c") && !err.contains("internal"),
                    "`{sql}`: {err}"
                );
            }
        });
    }
}

/// On a file and on `:memory:`, where these shapes take the same lookup loop
/// since bd-w4pr1.13.23.
#[test]
fn join_aggregates_over_lookups_match_sqlite() {
    for (indexed, file_backed) in [(false, true), (true, true), (false, false), (true, false)] {
        asupersync::test_utils::run_test(|| async move {
            let dir = tempfile::tempdir().unwrap();
            let target = if file_backed {
                dir.path()
                    .join("join_aggregate_lookup.db")
                    .to_string_lossy()
                    .into_owned()
            } else {
                ":memory:".to_owned()
            };
            let f = Connection::open(&target).await.unwrap();
            let r = rusqlite::Connection::open_in_memory().unwrap();
            let indexes: &[&str] = if indexed { INDEXES } else { &[] };
            for sql in SETUP.iter().chain(indexes) {
                f.execute(sql).await.unwrap();
                r.execute(sql, []).unwrap();
            }
            for sql in QUERIES {
                assert_agree(&f, &r, sql, &[], &[]).await;
            }
            assert_agree(
                &f,
                &r,
                "SELECT count(*), sum(t.c) FROM u JOIN t ON t.a = u.a WHERE t.c > ?1",
                &[SqliteValue::Float(2.0)],
                &[&2.0_f64],
            )
            .await;

            if indexed {
                let ops = opcodes(&f, "SELECT count(*) FROM u JOIN t ON t.a = u.a").await;
                assert!(ops.iter().any(|op| op == "AggStep"), "{ops:?}");
                assert!(
                    !ops.iter().any(|op| op == "SeekRowid"),
                    "count(*) over an index lookup reads only the index: {ops:?}"
                );
                let ops = opcodes(&f, "SELECT sum(t.c) FROM u JOIN t ON t.a = u.a").await;
                assert!(ops.iter().any(|op| op == "SeekRowid"), "{ops:?}");
            }

            if !file_backed {
                // A rowid-lookup aggregate runs the lookup loop on `:memory:`
                // as well; a DISTINCT aggregate is not that loop's and still
                // takes the general join path, which records its decision.
                f.set_fallback_capture_enabled(true);
                for (sql, general_path) in [
                    ("SELECT count(*), sum(t.c) FROM u JOIN t ON t.id = u.id", false),
                    ("SELECT count(*), sum(t.c) FROM u JOIN t ON t.id = u.a * 2", false),
                    ("SELECT count(DISTINCT t.a) FROM u JOIN t ON t.a = u.a", true),
                ] {
                    f.reset_fallback_decision_evidence();
                    f.query(sql).await.unwrap();
                    let went_general = f
                        .fallback_decision_snapshot()
                        .decisions
                        .iter()
                        .any(|decision| decision.decision_reason == "group_by_join_fallback");
                    assert_eq!(went_general, general_path, "`{sql}` on :memory:");
                }
                f.set_fallback_capture_enabled(false);
            }
        });
    }
}

/// bd-w4pr1.13.6: an inner or cross join written without a constraint is
/// joined by its WHERE clause. These compile to the lookup loop.
const JOINED_BY_WHERE_QUERIES: &[&str] = &[
    "SELECT count(*), sum(t.c) FROM u, t WHERE t.id = u.a",
    "SELECT count(*), sum(t.c) FROM u CROSS JOIN t WHERE t.id = u.a",
    "SELECT count(*), sum(t.c) FROM u JOIN t WHERE t.id = u.a",
    "SELECT count(*), sum(t.c) FROM u INNER JOIN t WHERE u.a = t.id",
    "SELECT count(*), sum(u.r) FROM u, t WHERE t.a = u.a",
    "SELECT count(*), min(t.b), max(u.b) FROM t, u WHERE t.a = u.a",
    "SELECT count(*), sum(u.r) FROM t, u WHERE u.a = t.id",
    "SELECT count(*), sum(u.r), sum(t.c) FROM u, t WHERE u.a = t.a AND t.c > 2 AND u.r < 3",
    "SELECT count(*) FROM u, t WHERE t.id = u.a AND u.b IS NOT NULL",
    "SELECT count(*), sum(t.c) FROM u, t WHERE t.id = u.id AND t.a = u.a",
    "SELECT count(*), total(t.c) FROM u, t WHERE u.id = t.id AND t.id > 2",
    "SELECT count(*), sum(t.c) FROM u, t WHERE t.id = u.id + 1",
    "SELECT count(*) FROM u, t WHERE t.b = u.b",
    "SELECT count(*) FROM u, t WHERE t.d = u.d",
    "SELECT count(*), min(t.n), max(t.n) FROM u, t WHERE t.n = u.b",
    "SELECT count(*), sum(y.c) FROM u AS x, t AS y WHERE y.id = x.a",
    "SELECT count(*) FROM t a, t b WHERE b.a = a.id",
    "SELECT count(*), sum(e.a) FROM u, e WHERE e.a = u.a",
    "SELECT count(*), sum(u.a) FROM e, u WHERE e.a = u.a",
];

/// Shapes a lookup must not be taken from, or that are not this lane's. A
/// join condition read as "seek the equality" would drop the rows the OR arm
/// or the missing equality admits.
const NOT_JOINED_BY_A_LOOKUP_QUERIES: &[&str] = &[
    "SELECT count(*) FROM u, t WHERE t.id = u.a OR u.a IS NULL",
    "SELECT count(*), sum(t.c) FROM u, t WHERE u.r > 1",
    "SELECT count(*), sum(t.c) FROM u, t",
    "SELECT count(*), sum(t.c) FROM u CROSS JOIN t",
    "SELECT count(*) FROM u, t WHERE NOT (t.id = u.a)",
    "SELECT count(*) FROM u, t WHERE t.id <> u.a",
    "SELECT count(*), count(t.id) FROM u LEFT JOIN t WHERE t.id = u.a",
    "SELECT count(*), count(t.id) FROM u LEFT JOIN t WHERE t.id IS NULL",
    "SELECT count(*) FROM u NATURAL JOIN t",
    "SELECT count(*) FROM u, t, e WHERE t.id = u.a AND e.a = u.a",
];

#[test]
fn join_aggregates_joined_by_where_match_sqlite() {
    for indexed in [false, true] {
        asupersync::test_utils::run_test(|| async move {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("join_aggregate_joined_by_where.db");
            let f = Connection::open(path.to_str().unwrap()).await.unwrap();
            let r = rusqlite::Connection::open_in_memory().unwrap();
            let indexes: &[&str] = if indexed { INDEXES } else { &[] };
            for sql in SETUP.iter().chain(indexes) {
                f.execute(sql).await.unwrap();
                r.execute(sql, []).unwrap();
            }
            for sql in JOINED_BY_WHERE_QUERIES
                .iter()
                .chain(NOT_JOINED_BY_A_LOOKUP_QUERIES)
            {
                assert_agree(&f, &r, sql, &[], &[]).await;
            }
            assert_agree(
                &f,
                &r,
                "SELECT count(*), sum(t.c) FROM u, t WHERE t.id = u.a AND t.c > ?1",
                &[SqliteValue::Float(2.0)],
                &[&2.0_f64],
            )
            .await;

            // The rowid equality in WHERE is the lookup, in each spelling.
            for sql in [
                "SELECT count(*), sum(t.c) FROM u, t WHERE t.id = u.a",
                "SELECT count(*), sum(t.c) FROM u CROSS JOIN t WHERE t.id = u.a",
                "SELECT count(*), sum(t.c) FROM u JOIN t WHERE t.id = u.a",
            ] {
                let ops = opcodes(&f, sql).await;
                assert!(
                    ops.iter().any(|op| op == "AggStep") && ops.iter().any(|op| op == "SeekRowid"),
                    "`{sql}` must run as a rowid lookup loop: {ops:?}"
                );
            }
            // An explicit CROSS JOIN keeps its written order: `t` drives and
            // `u.a`, which has no index, is looked up through an automatic
            // index rather than `u` driving lookups into `t`. A comma join
            // parses as INNER JOIN (bd-hexz0) and reorders to a rowid lookup,
            // matching stock SQLite.
            let ops = opcodes(&f, "SELECT count(*), sum(u.r) FROM t CROSS JOIN u WHERE u.a = t.id").await;
            assert!(
                ops.iter().any(|op| op == "AggStep") && ops.iter().any(|op| op == "OpenAutoindex"),
                "a cross join must not be reordered into a rowid lookup: {ops:?}"
            );
            let comma_ops = opcodes(&f, "SELECT count(*), sum(u.r) FROM t, u WHERE u.a = t.id").await;
            assert!(
                comma_ops.iter().any(|op| op == "AggStep")
                    && comma_ops.iter().any(|op| op == "SeekRowid"),
                "a comma join reorders into a rowid lookup like stock SQLite: {comma_ops:?}"
            );
        });
    }
}

/// bd-w4pr1.13.6: plain scalar function calls in the aggregate arguments
/// compile into the lookup loop.
const SCALAR_CALL_ARGUMENT_QUERIES: &[&str] = &[
    "SELECT count(*), sum(length(t.b)) FROM u JOIN t ON t.a = u.a",
    "SELECT sum(length(u.b)), sum(length(t.b) + length(u.b)), sum(length(t.b) * 2 - 1) \
     FROM u JOIN t ON t.a = u.a",
    "SELECT sum(abs(t.c - u.r)), max(abs(t.a)), min(length(t.b)) FROM u JOIN t ON t.a = u.a",
    "SELECT count(length(t.b)), total(length(t.d)), sum(coalesce(t.c, 0.5)) \
     FROM u LEFT JOIN t ON t.a = u.a",
    "SELECT sum(length(substr(t.b, 1, 1))), sum(instr(t.b, '0')), max(upper(t.b)) \
     FROM u JOIN t ON t.a = u.a",
    "SELECT max(typeof(t.d)), min(typeof(u.d)), sum(length(t.d)) FROM u JOIN t ON t.id = u.id",
    "SELECT sum(round(t.c * u.r, 1)), sum(ifnull(u.a, 0)) FROM u JOIN t ON t.id = u.id",
    "SELECT sum(length(t.rowid)), sum(abs(u.rowid - t.rowid)) FROM u JOIN t ON t.a = u.a",
    "SELECT sum(length(t.b)) FROM u, t WHERE t.id = u.a",
    "SELECT count(*), sum(length(e.a)) FROM u JOIN e ON e.a = u.a",
];

/// Calls the join loop must leave to the general path: a scalar min/max
/// compares under its arguments' collations, a NOCASE column under an
/// aggregate min/max needs that collation, `random()` is volatile, and a
/// comparison or a call in ON is not an aggregate argument.
const SCALAR_CALL_GENERAL_PATH_QUERIES: &[&str] = &[
    "SELECT max(max(t.a, u.a)), min(min(t.c, u.r)) FROM u JOIN t ON t.a = u.a",
    "SELECT min(max(t.n, u.b)), max(min(t.n, u.b)) FROM u JOIN t ON t.a = u.a",
    "SELECT min(lower(t.n)), max(upper(t.n)) FROM u JOIN t ON t.a = u.a",
    "SELECT count(random()), count(abs(random())) FROM u JOIN t ON t.a = u.a",
    "SELECT sum(nullif(t.a, u.a)), count(nullif(t.b, u.b)) FROM u JOIN t ON t.id = u.id",
    "SELECT sum(iif(t.a > 2, 1, 0)), sum(typeof(t.d) = 'integer') FROM u JOIN t ON t.a = u.a",
    "SELECT sum(length(t.b)) FROM u JOIN t ON length(t.b) = u.a",
];

/// bd-w4pr1.13.6: ON, WHERE and aggregate arguments may use every binary
/// operator the join emitter compiles. Division and modulo by zero are NULL
/// (a NULL key matches nothing; a LEFT JOIN still returns its row), `%` on a
/// real truncates its operands, and `||` makes a text key.
const OPERATOR_CONDITION_QUERIES: &[&str] = &[
    "SELECT count(*), count(t.id) FROM u LEFT JOIN t ON t.id = u.id % 4",
    "SELECT count(*), sum(t.c) FROM u JOIN t ON t.id = u.id * 10 / 5",
    "SELECT count(*) FROM u JOIN t ON t.b = u.a || ''",
    "SELECT count(*) FROM u JOIN t ON t.d = u.d || ''",
    "SELECT count(*), sum(t.a) FROM u JOIN t ON t.id = (u.id & 7) | 1",
    "SELECT count(*), sum(t.id) FROM u JOIN t ON t.id = u.id << 1",
    "SELECT count(*), sum(t.id) FROM u JOIN t ON t.id = u.id >> 1",
    "SELECT count(*) FROM u JOIN t ON t.id = u.r % 3",
    "SELECT count(*) FROM u JOIN t ON t.id = u.id / 0",
    "SELECT count(*), count(t.id) FROM u LEFT JOIN t ON t.id = u.id / 0",
    "SELECT count(*), count(t.id) FROM u LEFT JOIN t ON t.id = u.id % 0",
    "SELECT count(*) FROM u JOIN t ON t.a = u.a WHERE t.c / 2 > 1 AND u.id % 2 = 1",
    "SELECT count(*), sum(u.r) FROM u JOIN t ON t.a = u.a WHERE (t.id % 0) IS NULL",
    "SELECT count(*), sum(u.r) FROM u JOIN t ON t.a = u.a WHERE t.b || u.b <> '11'",
    "SELECT sum(t.c / u.r), sum(t.id % 3), max(t.b || '-' || u.b) FROM u JOIN t ON t.a = u.a",
    "SELECT sum(t.a / 0), total(t.a % 0), count(t.b || NULL) FROM u JOIN t ON t.a = u.a",
    "SELECT count(*), sum(t.c) FROM u, t WHERE t.id = u.id % 5 AND t.a | 1 = t.a",
];

#[test]
fn join_aggregates_over_operator_conditions_match_sqlite() {
    for indexed in [false, true] {
        asupersync::test_utils::run_test(|| async move {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("join_aggregate_operators.db");
            let f = Connection::open(path.to_str().unwrap()).await.unwrap();
            let r = rusqlite::Connection::open_in_memory().unwrap();
            let indexes: &[&str] = if indexed { INDEXES } else { &[] };
            for sql in SETUP.iter().chain(indexes) {
                f.execute(sql).await.unwrap();
                r.execute(sql, []).unwrap();
            }
            for sql in OPERATOR_CONDITION_QUERIES {
                assert_agree(&f, &r, sql, &[], &[]).await;
            }
            let lookup_sql = "SELECT count(*), count(t.id) FROM u LEFT JOIN t ON t.id = u.id % 4";
            let ops = opcodes(&f, lookup_sql).await;
            assert!(
                ops.iter().any(|op| op == "AggStep") && ops.iter().any(|op| op == "SeekRowid"),
                "`{lookup_sql}` must run as a rowid lookup loop: {ops:?}"
            );
        });
    }
}

/// bd-w4pr1.13.16: a WHERE term that reads only the outer table is tested
/// before the lookup, and so is such an ON term of an inner join. An ON term
/// of a LEFT JOIN must not be: the outer row it fails still comes out, with
/// NULLs for the inner table. Terms joined by OR, and terms that read the
/// inner table, stay after the lookup.
const OUTER_ROW_FILTER_QUERIES: &[&str] = &[
    "SELECT u.id, t.id FROM u JOIN t ON t.a = u.a WHERE u.r > 1 ORDER BY u.id, t.id",
    "SELECT u.id, t.id FROM u JOIN t ON t.id = u.id WHERE u.r > 1 AND u.id <> 5 ORDER BY u.id",
    "SELECT u.id, t.id FROM u LEFT JOIN t ON t.a = u.a WHERE u.r > 1 ORDER BY u.id, t.id",
    "SELECT u.id, t.id FROM u LEFT JOIN t ON t.a = u.a AND u.r > 1 ORDER BY u.id, t.id",
    "SELECT u.id, t.id FROM u JOIN t ON t.a = u.a AND u.r > 1 ORDER BY u.id, t.id",
    "SELECT u.id, t.id FROM u LEFT JOIN t ON t.a = u.a WHERE u.b IS NULL ORDER BY u.id",
    "SELECT u.id, t.id FROM u LEFT JOIN t ON t.a = u.a WHERE u.a IS NULL OR t.id IS NULL \
     ORDER BY u.id",
    "SELECT u.id, t.id FROM u JOIN t ON t.a = u.a WHERE u.r > 1 AND t.c > 2 AND u.id <> 5 \
     ORDER BY u.id, t.id",
    "SELECT u.id, t.id FROM u JOIN t ON t.id = u.id WHERE NULL ORDER BY u.id",
    "SELECT u.id, t.id FROM u LEFT JOIN t ON t.id = u.id WHERE 0 ORDER BY u.id",
    "SELECT u.id, t.id FROM u JOIN t ON t.id = u.id WHERE 1 ORDER BY u.id",
    "SELECT a.id, b.id FROM t a JOIN t b ON b.id = a.a WHERE a.c > 2 AND b.c < 10 ORDER BY a.id",
    "SELECT count(*), sum(t.c) FROM u JOIN t ON t.a = u.a WHERE u.r > 1",
    "SELECT count(*), count(t.id) FROM u LEFT JOIN t ON t.a = u.a WHERE u.r < 3",
    "SELECT count(*), sum(t.c) FROM u, t WHERE t.id = u.a AND u.r > 1 AND u.id < 6",
];

/// `(address, opcode, p1, p2)` of each instruction of `sql`'s program.
async fn program(conn: &Connection, sql: &str) -> Vec<(i64, String, i64, i64)> {
    let integer = |value: Option<&SqliteValue>| match value {
        Some(SqliteValue::Integer(n)) => *n,
        _ => -1,
    };
    conn.query(&format!("EXPLAIN {sql}"))
        .await
        .unwrap()
        .iter()
        .map(|row| {
            let values = row.values();
            let opcode = match values.get(1) {
                Some(SqliteValue::Text(op)) => op.to_string(),
                _ => String::new(),
            };
            (integer(values.first()), opcode, integer(values.get(2)), integer(values.get(3)))
        })
        .collect()
}

#[test]
fn join_lookups_test_outer_row_terms_first_and_match_sqlite() {
    for indexed in [false, true] {
        asupersync::test_utils::run_test(|| async move {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("join_outer_row_filter.db");
            let f = Connection::open(path.to_str().unwrap()).await.unwrap();
            let r = rusqlite::Connection::open_in_memory().unwrap();
            let indexes: &[&str] = if indexed { INDEXES } else { &[] };
            for sql in SETUP.iter().chain(indexes) {
                f.execute(sql).await.unwrap();
                r.execute(sql, []).unwrap();
            }
            for sql in OUTER_ROW_FILTER_QUERIES {
                assert_agree(&f, &r, sql, &[], &[]).await;
            }
            assert_agree(
                &f,
                &r,
                "SELECT u.id, t.id FROM u JOIN t ON t.id = u.id WHERE u.r > ?1 ORDER BY u.id",
                &[SqliteValue::Float(1.0)],
                &[&1.0_f64],
            )
            .await;

            // The outer-only term is decided before the inner table is sought.
            let ops =
                program(&f, "SELECT u.id, t.id FROM u JOIN t ON t.id = u.id WHERE u.r > 1").await;
            let first = |name: &str| ops.iter().position(|(_, op, _, _)| op == name);
            assert!(
                matches!((first("IfNot"), first("SeekRowid")), (Some(test), Some(seek)) if test < seek),
                "the outer row must be tested before the lookup: {ops:?}"
            );
        });
    }
}

/// bd-w4pr1.13.16: an upper bound on the outer table's rowid ends the outer
/// scan the first time it fails. The bound may be a number, text that
/// converts, text that does not (every integer sorts before it: always true),
/// a blob (always true) or NULL (no rows). A bound on any other column, a
/// lower bound, and a bound under OR must not end the scan: `t.a <= 2` holds
/// again at t.id 12 after failing from t.id 4 on.
const OUTER_ROWID_BOUND_QUERIES: &[&str] = &[
    "SELECT u.id, t.id FROM u JOIN t ON t.a = u.a WHERE u.id <= 3 ORDER BY u.id, t.id",
    "SELECT u.id, t.id FROM u JOIN t ON t.a = u.a WHERE u.id < 3 ORDER BY u.id, t.id",
    "SELECT u.id, t.id FROM u JOIN t ON t.a = u.a WHERE 3 >= u.id ORDER BY u.id, t.id",
    "SELECT u.id, t.id FROM u JOIN t ON t.a = u.a WHERE 3 > u.rowid ORDER BY u.id, t.id",
    "SELECT u.id, t.id FROM u LEFT JOIN t ON t.a = u.a WHERE u.id <= 4 ORDER BY u.id, t.id",
    "SELECT u.id, t.id FROM u JOIN t ON t.id = u.id WHERE u.id <= 3.5 ORDER BY u.id",
    "SELECT u.id, t.id FROM u JOIN t ON t.id = u.id WHERE u.id <= '3' ORDER BY u.id",
    "SELECT u.id, t.id FROM u JOIN t ON t.id = u.id WHERE u.id <= 'abc' ORDER BY u.id",
    "SELECT u.id, t.id FROM u JOIN t ON t.id = u.id WHERE u.id < x'00' ORDER BY u.id",
    "SELECT u.id, t.id FROM u JOIN t ON t.id = u.id WHERE u.id <= NULL ORDER BY u.id",
    "SELECT u.id, t.id FROM u JOIN t ON t.id = u.id WHERE u.id <= 0 ORDER BY u.id",
    "SELECT u.id, t.id FROM u JOIN t ON t.id = u.id WHERE u.id <= 3 AND u.r > 0.5 ORDER BY u.id",
    "SELECT u.id, t.id FROM u JOIN t ON t.id = u.id WHERE u.id <= 3 OR u.r > 2.9 ORDER BY u.id",
    "SELECT u.id, t.id FROM u JOIN t ON t.id = u.id WHERE u.id >= 3 ORDER BY u.id",
    "SELECT u.id, t.id FROM u JOIN t ON t.id = u.id WHERE u.id > 2 AND u.id <= 5 ORDER BY u.id",
    "SELECT t.id, u.id FROM t LEFT JOIN u ON u.id = t.id WHERE t.a <= 2 ORDER BY t.id",
    "SELECT t.id, u.id FROM t JOIN u ON u.id = t.a WHERE t.a <= 2 ORDER BY t.id",
    "SELECT u.id, t.id FROM u JOIN t ON t.id = u.id WHERE u.id <= 5 ORDER BY u.id DESC LIMIT 2",
    "SELECT count(*), sum(t.c) FROM u JOIN t ON t.a = u.a WHERE u.id <= 3",
    "SELECT count(*), count(t.id) FROM u LEFT JOIN t ON t.a = u.a WHERE u.id < 5",
    "SELECT count(*), sum(t.c) FROM u, t WHERE t.id = u.a AND u.id <= 5",
];

#[test]
fn join_lookups_stop_at_an_outer_rowid_bound_and_match_sqlite() {
    for indexed in [false, true] {
        asupersync::test_utils::run_test(|| async move {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("join_outer_rowid_bound.db");
            let f = Connection::open(path.to_str().unwrap()).await.unwrap();
            let r = rusqlite::Connection::open_in_memory().unwrap();
            let indexes: &[&str] = if indexed { INDEXES } else { &[] };
            for sql in SETUP.iter().chain(indexes) {
                f.execute(sql).await.unwrap();
                r.execute(sql, []).unwrap();
            }
            for sql in OUTER_ROWID_BOUND_QUERIES {
                assert_agree(&f, &r, sql, &[], &[]).await;
            }
            for bound in [0_i64, 3, 100] {
                assert_agree(
                    &f,
                    &r,
                    "SELECT u.id, t.id FROM u JOIN t ON t.id = u.id WHERE u.id <= ?1 ORDER BY u.id",
                    &[SqliteValue::Integer(bound)],
                    &[&bound],
                )
                .await;
            }

            // The failing bound leaves the outer loop: its jump lands past
            // the `Next` that advances the outer cursor. A bound on another
            // column only skips the row.
            let leaves_the_loop = |ops: &[(i64, String, i64, i64)]| {
                let seek = ops.iter().position(|(_, op, _, _)| op == "SeekRowid").unwrap();
                let advance = ops
                    .iter()
                    .find(|(_, op, cursor, _)| op == "Next" && *cursor == 0)
                    .map(|(address, _, _, _)| *address)
                    .unwrap();
                ops[..seek]
                    .iter()
                    .any(|(_, op, _, target)| op == "IfNot" && *target > advance)
            };
            let bounded =
                program(&f, "SELECT u.id, t.id FROM u JOIN t ON t.id = u.id WHERE u.id <= 3").await;
            assert!(leaves_the_loop(&bounded), "{bounded:?}");
            let other_column =
                program(&f, "SELECT u.id, t.id FROM u JOIN t ON t.id = u.id WHERE u.r <= 3").await;
            assert!(!leaves_the_loop(&other_column), "{other_column:?}");
            let lower_bound =
                program(&f, "SELECT u.id, t.id FROM u JOIN t ON t.id = u.id WHERE u.id >= 3").await;
            assert!(!leaves_the_loop(&lower_bound), "{lower_bound:?}");
        });
    }
}

/// bd-w4pr1.13.16: an integer lower bound on the outer table's rowid starts
/// the outer scan with a seek. Of several bounds the tightest starts it; a
/// bound on another column, a bound that is not an integer literal, and a
/// bound under OR start it at the first row.
const OUTER_ROWID_FLOOR_QUERIES: &[&str] = &[
    "SELECT u.id, t.id FROM u JOIN t ON t.id = u.id WHERE u.id > 2 ORDER BY u.id",
    "SELECT u.id, t.id FROM u JOIN t ON t.id = u.id WHERE u.id >= 3 ORDER BY u.id",
    "SELECT u.id, t.id FROM u JOIN t ON t.id = u.id WHERE 2 < u.id ORDER BY u.id",
    "SELECT u.id, t.id FROM u JOIN t ON t.id = u.id WHERE 3 <= u.rowid ORDER BY u.id",
    "SELECT u.id, t.id FROM u JOIN t ON t.id = u.id WHERE u.id > 2 AND u.id >= 5 ORDER BY u.id",
    "SELECT u.id, t.id FROM u JOIN t ON t.id = u.id WHERE u.id >= 5 AND u.id > 5 ORDER BY u.id",
    "SELECT u.id, t.id FROM u JOIN t ON t.id = u.id WHERE u.id > 100 ORDER BY u.id",
    "SELECT u.id, t.id FROM u JOIN t ON t.id = u.id WHERE u.id > 0 ORDER BY u.id",
    "SELECT u.id, t.id FROM u JOIN t ON t.id = u.id WHERE u.id > 9223372036854775807 ORDER BY u.id",
    "SELECT u.id, t.id FROM u JOIN t ON t.id = u.id WHERE u.id > 2 AND u.id <= 5 ORDER BY u.id",
    "SELECT u.id, t.id FROM u LEFT JOIN t ON t.a = u.a WHERE u.id >= 4 ORDER BY u.id, t.id",
    "SELECT u.id, t.id FROM u JOIN t ON t.id = u.id WHERE u.id > 2.5 ORDER BY u.id",
    "SELECT u.id, t.id FROM u JOIN t ON t.id = u.id WHERE u.id > '2' ORDER BY u.id",
    "SELECT u.id, t.id FROM u JOIN t ON t.id = u.id WHERE u.id > 5 OR u.r < 1 ORDER BY u.id",
    "SELECT u.id, t.id FROM u JOIN t ON t.id = u.id AND u.id > 2 ORDER BY u.id",
    "SELECT u.id, t.id FROM u LEFT JOIN t ON t.id = u.id AND u.id > 2 ORDER BY u.id",
    "SELECT t.id, u.id FROM t LEFT JOIN u ON u.id = t.id WHERE t.a >= 5 ORDER BY t.id",
    "SELECT count(*), sum(t.c) FROM u JOIN t ON t.a = u.a WHERE u.id > 2",
    "SELECT count(*), count(t.id) FROM u LEFT JOIN t ON t.a = u.a WHERE u.id >= 6",
    "SELECT count(*), sum(t.c) FROM u, t WHERE t.id = u.a AND u.id > 1 AND u.id < 6",
];

#[test]
fn join_lookups_start_at_an_outer_rowid_floor_and_match_sqlite() {
    for indexed in [false, true] {
        asupersync::test_utils::run_test(|| async move {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("join_outer_rowid_floor.db");
            let f = Connection::open(path.to_str().unwrap()).await.unwrap();
            let r = rusqlite::Connection::open_in_memory().unwrap();
            let indexes: &[&str] = if indexed { INDEXES } else { &[] };
            for sql in SETUP.iter().chain(indexes) {
                f.execute(sql).await.unwrap();
                r.execute(sql, []).unwrap();
            }
            for sql in OUTER_ROWID_FLOOR_QUERIES {
                assert_agree(&f, &r, sql, &[], &[]).await;
            }

            // How the outer cursor (0) is first positioned.
            let outer_start = |ops: &[(i64, String, i64, i64)]| {
                ops.iter()
                    .find(|(_, op, cursor, _)| {
                        *cursor == 0 && matches!(op.as_str(), "Rewind" | "SeekGE" | "SeekGT")
                    })
                    .map(|(_, op, _, _)| op.clone())
            };
            for (sql, start) in [
                ("SELECT u.id, t.id FROM u JOIN t ON t.id = u.id WHERE u.id > 2", "SeekGT"),
                ("SELECT u.id, t.id FROM u JOIN t ON t.id = u.id WHERE 3 <= u.id", "SeekGE"),
                ("SELECT u.id, t.id FROM u LEFT JOIN t ON t.id = u.id AND u.id > 2", "Rewind"),
                ("SELECT u.id, t.id FROM u JOIN t ON t.id = u.id WHERE u.r > 2", "Rewind"),
                ("SELECT u.id, t.id FROM u JOIN t ON t.id = u.id WHERE u.id > 2.5", "Rewind"),
                ("SELECT u.id, t.id FROM u JOIN t ON t.id = u.id WHERE u.id > 5 OR u.r < 1", "Rewind"),
            ] {
                let ops = program(&f, sql).await;
                assert_eq!(outer_start(&ops).as_deref(), Some(start), "`{sql}`: {ops:?}");
            }
        });
    }
}

/// bd-w4pr1.13.16: `[NOT] BETWEEN` in a join's WHERE or ON compiles as the two
/// comparisons it stands for, in the row lane and the aggregate lane. On the
/// outer rowid its lower bound starts the scan and its upper bound ends it.
/// The cases include a NULL bound (no rows; NOT BETWEEN keeps only rows the
/// other bound decides), bounds in the wrong order, a text bound against an
/// integer column, a NOCASE column, a real column, an inner-table operand, a
/// LEFT JOIN ON term (which must keep its unmatched rows) and a bound that
/// is another column.
const BETWEEN_QUERIES: &[&str] = &[
    "SELECT u.id, t.id FROM u JOIN t ON t.id = u.id WHERE u.id BETWEEN 2 AND 5 ORDER BY u.id",
    "SELECT u.id, t.id FROM u JOIN t ON t.a = u.a WHERE u.id BETWEEN 2 AND 5 ORDER BY u.id, t.id",
    "SELECT u.id, t.id FROM u JOIN t ON t.id = u.id WHERE u.id NOT BETWEEN 2 AND 5 ORDER BY u.id",
    "SELECT u.id, t.id FROM u JOIN t ON t.id = u.id WHERE u.id BETWEEN 5 AND 2 ORDER BY u.id",
    "SELECT u.id, t.id FROM u JOIN t ON t.id = u.id WHERE u.id BETWEEN NULL AND 5 ORDER BY u.id",
    "SELECT u.id, t.id FROM u JOIN t ON t.id = u.id WHERE u.id NOT BETWEEN NULL AND 5 ORDER BY u.id",
    "SELECT u.id, t.id FROM u JOIN t ON t.id = u.id WHERE u.id NOT BETWEEN 3 AND NULL ORDER BY u.id",
    "SELECT u.id, t.id FROM u JOIN t ON t.id = u.id WHERE u.id BETWEEN '2' AND 5.5 ORDER BY u.id",
    "SELECT u.id, t.id FROM u JOIN t ON t.id = u.id WHERE u.id BETWEEN 2 AND 'x' ORDER BY u.id",
    "SELECT u.id, t.id FROM u JOIN t ON t.id = u.id WHERE u.r BETWEEN 1 AND 2.5 ORDER BY u.id",
    "SELECT u.id, t.id FROM u JOIN t ON t.id = u.id WHERE u.b BETWEEN '2' AND '6' ORDER BY u.id",
    "SELECT u.id, t.id FROM u JOIN t ON t.id = u.id WHERE t.n BETWEEN 'B' AND 'd' ORDER BY u.id",
    "SELECT u.id, t.id FROM u JOIN t ON t.id = u.id WHERE t.c BETWEEN u.r AND u.r * 4 ORDER BY u.id",
    "SELECT u.id, t.id FROM u JOIN t ON t.id = u.id WHERE u.id BETWEEN 2 AND 6 AND t.c NOT BETWEEN 3 AND 5 \
     ORDER BY u.id",
    "SELECT u.id, t.id FROM u LEFT JOIN t ON t.id = u.id AND u.id BETWEEN 3 AND 5 ORDER BY u.id",
    "SELECT u.id, t.id FROM u LEFT JOIN t ON t.id = u.id WHERE u.id BETWEEN 3 AND 100 ORDER BY u.id",
    "SELECT u.id, t.id FROM u LEFT JOIN t ON t.a = u.a WHERE t.c BETWEEN 2 AND 5 ORDER BY u.id, t.id",
    "SELECT u.id, t.id FROM u JOIN t ON t.id = u.id AND t.c BETWEEN 2 AND 5 ORDER BY u.id",
    "SELECT u.id, u.id BETWEEN 2 AND 4, t.c NOT BETWEEN 2 AND 4 FROM u JOIN t ON t.id = u.id ORDER BY u.id",
    "SELECT u.id, t.id FROM u JOIN t ON t.id = u.id WHERE u.id BETWEEN 2 AND 5 OR u.r BETWEEN 2.9 AND 3.1 \
     ORDER BY u.id",
    "SELECT count(*), sum(t.c) FROM u JOIN t ON t.a = u.a WHERE u.id BETWEEN 2 AND 5",
    "SELECT count(*), sum(length(t.b)) FROM u JOIN t ON t.id = u.id WHERE u.id BETWEEN 3 AND 6",
    "SELECT count(*), count(t.id) FROM u LEFT JOIN t ON t.a = u.a WHERE u.id NOT BETWEEN 2 AND 5",
    "SELECT count(*), sum(t.c) FROM u, t WHERE t.id = u.a AND u.id BETWEEN 2 AND 6",
    "SELECT count(*), sum(u.r) FROM u JOIN t ON t.a = u.a WHERE t.c BETWEEN 2 AND 10",
];

#[test]
fn join_lanes_compile_between_and_match_sqlite() {
    for indexed in [false, true] {
        asupersync::test_utils::run_test(|| async move {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("join_between.db");
            let f = Connection::open(path.to_str().unwrap()).await.unwrap();
            let r = rusqlite::Connection::open_in_memory().unwrap();
            let indexes: &[&str] = if indexed { INDEXES } else { &[] };
            for sql in SETUP.iter().chain(indexes) {
                f.execute(sql).await.unwrap();
                r.execute(sql, []).unwrap();
            }
            for sql in BETWEEN_QUERIES {
                assert_agree(&f, &r, sql, &[], &[]).await;
            }
            for (low, high) in [(2_i64, 5_i64), (0, 0), (6, 100), (5, 2)] {
                assert_agree(
                    &f,
                    &r,
                    "SELECT count(*), sum(t.c) FROM u JOIN t ON t.id = u.id WHERE u.id BETWEEN ?1 AND ?2",
                    &[SqliteValue::Integer(low), SqliteValue::Integer(high)],
                    &[&low, &high],
                )
                .await;
            }

            // Both lanes run the lookup loop for BETWEEN, and a rowid range
            // starts the outer scan with a seek and ends it at its upper bound.
            for sql in [
                "SELECT count(*), sum(t.c) FROM u JOIN t ON t.id = u.id WHERE u.id BETWEEN 2 AND 5",
                "SELECT u.id, t.c FROM u JOIN t ON t.id = u.id WHERE u.id BETWEEN 2 AND 5",
            ] {
                let ops = program(&f, sql).await;
                let outer_start = ops
                    .iter()
                    .find(|(_, op, cursor, _)| {
                        *cursor == 0 && matches!(op.as_str(), "Rewind" | "SeekGE" | "SeekGT")
                    })
                    .map(|(_, op, _, _)| op.as_str());
                assert_eq!(outer_start, Some("SeekGE"), "`{sql}`: {ops:?}");
                let seek = ops.iter().position(|(_, op, _, _)| op == "SeekRowid").unwrap();
                let advance = ops
                    .iter()
                    .find(|(_, op, cursor, _)| op == "Next" && *cursor == 0)
                    .map(|(address, _, _, _)| *address)
                    .unwrap();
                assert!(
                    ops[..seek]
                        .iter()
                        .any(|(_, op, _, target)| op == "IfNot" && *target > advance),
                    "`{sql}` must leave the outer loop at the upper bound: {ops:?}"
                );
            }
        });
    }
}

/// bd-w4pr1.13.16: an inner or cross join written without a constraint and
/// joined by its WHERE clause returns rows through the lookup loop. Every
/// query is ordered, so the comparison does not depend on which table a plan
/// drives from.
const ROWS_JOINED_BY_WHERE_QUERIES: &[&str] = &[
    "SELECT u.id, t.id, t.c FROM u, t WHERE t.id = u.a ORDER BY u.id",
    "SELECT u.id, t.id, t.c FROM u CROSS JOIN t WHERE t.id = u.a ORDER BY u.id",
    "SELECT u.id, t.id, t.c FROM u JOIN t WHERE u.a = t.id ORDER BY u.id",
    "SELECT u.id, t.id FROM u, t WHERE t.a = u.a ORDER BY u.id, t.id",
    "SELECT t.id, u.id FROM t, u WHERE u.a = t.id ORDER BY t.id, u.id",
    "SELECT u.id, t.id FROM u, t WHERE t.a = u.a AND t.c > 2 AND u.r < 3 ORDER BY u.id, t.id",
    "SELECT u.id, t.id FROM u, t WHERE t.id = u.id AND t.a = u.a ORDER BY u.id",
    "SELECT u.id, t.id FROM u, t WHERE t.id = u.id + 1 ORDER BY u.id",
    "SELECT u.id, t.id FROM u, t WHERE t.b = u.b ORDER BY u.id, t.id",
    "SELECT u.id, t.id FROM u, t WHERE t.d = u.d ORDER BY u.id, t.id",
    "SELECT u.id, t.id FROM u, t WHERE t.n = u.b ORDER BY u.id, t.id",
    "SELECT x.id, y.c FROM u AS x, t AS y WHERE y.id = x.a ORDER BY x.id",
    "SELECT a.id, b.id FROM t a, t b WHERE b.a = a.id ORDER BY a.id, b.id",
    "SELECT * FROM u, t WHERE t.id = u.a ORDER BY u.id",
    "SELECT u.*, t.c FROM u, t WHERE t.id = u.a ORDER BY u.id",
    "SELECT u.id, t.id FROM u, t WHERE t.id = u.a AND u.id BETWEEN 2 AND 5 ORDER BY u.id",
    "SELECT u.id, t.id FROM u, t WHERE t.id = u.a ORDER BY t.c DESC, u.id",
    "SELECT u.id, e.id FROM u, e WHERE e.a = u.a ORDER BY u.id",
    // A cross join is never reordered, so these row orders are the plan's.
    "SELECT u.id, t.id, t.c FROM u CROSS JOIN t WHERE t.id = u.a",
    "SELECT u.id, t.id, t.c FROM u CROSS JOIN t WHERE t.id = u.a LIMIT 3",
    "SELECT u.id, t.id, t.c FROM u CROSS JOIN t WHERE t.id = u.a LIMIT 2 OFFSET 2",
];

/// Joined by WHERE with no equality the lookup can take: these stay on the
/// materializing executor and must still match.
const ROWS_NOT_JOINED_BY_A_LOOKUP_QUERIES: &[&str] = &[
    "SELECT u.id, t.id FROM u, t WHERE t.id <> u.a AND t.id < 3 ORDER BY u.id, t.id",
    "SELECT u.id, t.id FROM u, t WHERE t.id = u.a OR u.a IS NULL ORDER BY u.id, t.id",
    "SELECT u.id, t.id FROM u, t WHERE u.r > 2.9 AND t.c > 9 ORDER BY u.id, t.id",
    "SELECT count(*) FROM (SELECT u.id FROM u, t)",
    "SELECT u.id, t.id FROM u LEFT JOIN t WHERE t.id = u.a ORDER BY u.id, t.id",
    "SELECT u.id, t.id, e.id FROM u, t, e WHERE t.id = u.a AND e.a = u.a ORDER BY u.id",
];

#[test]
fn join_rows_joined_by_where_match_sqlite() {
    for indexed in [false, true] {
        asupersync::test_utils::run_test(|| async move {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("join_rows_joined_by_where.db");
            let f = Connection::open(path.to_str().unwrap()).await.unwrap();
            let r = rusqlite::Connection::open_in_memory().unwrap();
            let indexes: &[&str] = if indexed { INDEXES } else { &[] };
            for sql in SETUP.iter().chain(indexes) {
                f.execute(sql).await.unwrap();
                r.execute(sql, []).unwrap();
            }
            for sql in ROWS_JOINED_BY_WHERE_QUERIES
                .iter()
                .chain(ROWS_NOT_JOINED_BY_A_LOOKUP_QUERIES)
            {
                assert_agree(&f, &r, sql, &[], &[]).await;
            }
            assert_agree(
                &f,
                &r,
                "SELECT u.id, t.c FROM u, t WHERE t.id = u.a AND t.c > ?1 ORDER BY u.id",
                &[SqliteValue::Float(2.0)],
                &[&2.0_f64],
            )
            .await;
        });
    }
}

/// The routing itself: on an in-memory database with the interpreted
/// fallbacks refused, a join joined by WHERE through a lookup still answers
/// (it runs as bytecode), and one with no lookup is refused (it would have
/// taken the materializing executor).
#[test]
fn join_rows_joined_by_where_run_as_bytecode() {
    asupersync::test_utils::run_test(|| async {
        let f = Connection::open(":memory:").await.unwrap();
        let r = rusqlite::Connection::open_in_memory().unwrap();
        for sql in SETUP.iter().chain(INDEXES) {
            f.execute(sql).await.unwrap();
            r.execute(sql, []).unwrap();
        }
        f.set_reject_mem_fallback(true);
        f.set_strict_mem_fallback_rejection(true);
        for sql in [
            "SELECT u.id, t.id, t.c FROM u, t WHERE t.id = u.a ORDER BY u.id",
            "SELECT u.id, t.id, t.c FROM u CROSS JOIN t WHERE t.id = u.a ORDER BY u.id",
            "SELECT u.id, t.id FROM u, t WHERE t.a = u.a AND t.c > 2 ORDER BY u.id, t.id",
            "SELECT u.id, t.id, t.c FROM u CROSS JOIN t WHERE t.id = u.a LIMIT 3",
        ] {
            assert_agree(&f, &r, sql, &[], &[]).await;
        }
        for sql in [
            "SELECT u.id, t.id FROM u, t WHERE t.id <> u.a AND t.id < 3 ORDER BY u.id, t.id",
            "SELECT u.id, t.id FROM u, t WHERE u.r > 2.9 AND t.c > 9 ORDER BY u.id, t.id",
        ] {
            assert!(
                f.query(sql).await.is_err(),
                "`{sql}` has no lookup and must be refused with fallbacks off"
            );
        }
        f.set_strict_mem_fallback_rejection(false);
        f.set_reject_mem_fallback(false);
    });
}

/// A replacement for the builtin `length`: every value is 7 long.
struct SevenLong;

impl fsqlite_func::ScalarFunction for SevenLong {
    fn name(&self) -> &str {
        "length"
    }

    fn num_args(&self) -> i32 {
        1
    }

    fn invoke(&self, _args: &[SqliteValue]) -> fsqlite_error::Result<SqliteValue> {
        Ok(SqliteValue::Integer(7))
    }
}

#[test]
fn join_aggregates_over_scalar_calls_match_sqlite() {
    for indexed in [false, true] {
        asupersync::test_utils::run_test(|| async move {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("join_aggregate_scalar_calls.db");
            let f = Connection::open(path.to_str().unwrap()).await.unwrap();
            let r = rusqlite::Connection::open_in_memory().unwrap();
            let indexes: &[&str] = if indexed { INDEXES } else { &[] };
            for sql in SETUP.iter().chain(indexes) {
                f.execute(sql).await.unwrap();
                r.execute(sql, []).unwrap();
            }
            for sql in SCALAR_CALL_ARGUMENT_QUERIES
                .iter()
                .chain(SCALAR_CALL_GENERAL_PATH_QUERIES)
            {
                assert_agree(&f, &r, sql, &[], &[]).await;
            }

            let lookup_sql = "SELECT count(*), sum(length(t.b)) FROM u JOIN t ON t.a = u.a";
            let ops = opcodes(&f, lookup_sql).await;
            assert!(
                ops.iter().any(|op| op == "AggStep") && ops.iter().any(|op| op == "PureFunc"),
                "`{lookup_sql}` must run the call inside the lookup loop: {ops:?}"
            );

            // An unknown function and a wrong argument count are errors in
            // both engines, not a row.
            for sql in [
                "SELECT sum(nosuchfn(t.a)) FROM u JOIN t ON t.a = u.a",
                "SELECT sum(length(t.a, t.b)) FROM u JOIN t ON t.a = u.a",
            ] {
                assert!(f.query(sql).await.is_err(), "franken `{sql}` must fail");
                assert!(r.prepare(sql).is_err(), "rusqlite `{sql}` must fail");
            }
            let err = f
                .query("SELECT sum(nosuchfn(t.a)) FROM u JOIN t ON t.a = u.a")
                .await
                .expect_err("unknown function")
                .to_string();
            assert!(err.contains("no such function"), "{err}");

            // The call resolves in the connection's registry when it runs: a
            // replacement of the builtin is what the loop executes, whether
            // it is registered as deterministic (the loop) or not (the
            // general path).
            let rows = f.query(lookup_sql).await.unwrap();
            let SqliteValue::Integer(matches) = rows[0].values()[0] else {
                panic!("count(*) is an integer: {:?}", rows[0].values());
            };
            assert!(matches > 0);
            f.register_deterministic_scalar_function(SevenLong);
            let rows = f.query(lookup_sql).await.unwrap();
            assert_eq!(
                rows[0].values(),
                &[SqliteValue::Integer(matches), SqliteValue::Integer(7 * matches)],
                "a deterministic replacement of length() must be the one called"
            );
            let ops = opcodes(&f, lookup_sql).await;
            assert!(ops.iter().any(|op| op == "PureFunc"), "{ops:?}");
            f.register_nondeterministic_scalar_function(SevenLong);
            let rows = f.query(lookup_sql).await.unwrap();
            assert_eq!(
                rows[0].values(),
                &[SqliteValue::Integer(matches), SqliteValue::Integer(7 * matches)],
                "a volatile replacement of length() must be the one called"
            );
        });
    }
}

const MULTI_SETUP: &str = "
    CREATE TABLE driver(id INTEGER PRIMARY KEY, sid INTEGER, sk INTEGER, amount REAL);
    CREATE TABLE middle(id INTEGER PRIMARY KEY, k INTEGER UNIQUE, lid INTEGER, lk INTEGER);
    CREATE TABLE leaf(id INTEGER PRIMARY KEY, k INTEGER UNIQUE, amount REAL, name TEXT);
    CREATE TABLE tail(id INTEGER PRIMARY KEY, amount REAL);
    CREATE TABLE empty_driver(id INTEGER PRIMARY KEY, sid INTEGER, amount REAL);
    INSERT INTO driver VALUES
        (1,1,10,1.0),(2,2,20,2.5),(3,3,30,NULL),(4,2,20,-3.0),
        (5,4,40,5.5),(6,NULL,NULL,10.0),(7,999,999,1.0),(8,1,10,-2.0);
    INSERT INTO middle VALUES (1,10,1,10),(2,20,99,999),(3,30,NULL,NULL),(4,40,2,20);
    INSERT INTO leaf VALUES (1,10,12.5,'A'),(2,20,-2.0,'z'),(3,30,NULL,'b');
    INSERT INTO tail VALUES (1,2.0),(2,-4.0);
";

#[test]
fn multi_join_aggregates_stream_and_match_sqlite() {
    asupersync::test_utils::run_test(|| async {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("multi_join_aggregate.db");
        let f = Connection::open(path.to_str().unwrap()).await.unwrap();
        let r = rusqlite::Connection::open_in_memory().unwrap();
        f.execute_batch(MULTI_SETUP).await.unwrap();
        r.execute_batch(MULTI_SETUP).unwrap();

        // Alternate real matches, missing and NULL keys, and repeated driver
        // keys. In particular, a LEFT miss must not poison the next match.
        let chains = [
            "driver d JOIN middle m ON m.id=d.sid JOIN leaf l ON l.id=m.lid",
            "driver d JOIN middle m ON m.id=d.sid LEFT JOIN leaf l ON l.id=m.lid",
            "driver d JOIN middle m ON m.k=d.sk JOIN leaf l ON l.k=m.lk",
            "driver d JOIN middle m ON m.k=d.sk LEFT JOIN leaf l ON l.k=m.lk",
            "driver d JOIN middle m ON m.id=d.sid JOIN leaf l ON l.id=m.lid \
             LEFT JOIN tail t ON t.id=l.id",
        ];
        for chain in chains {
            for aggregates in [
                "count(*), count(l.id), count(d.amount), sum(l.amount), total(l.amount), \
                 avg(d.amount), min(l.amount), max(l.amount)",
                "sum(d.amount*l.amount), min(l.name), max(l.name), count(1)",
            ] {
                for predicate in [
                    "",
                    " WHERE d.id > 1",
                    " WHERE l.id IS NULL",
                    " WHERE d.id < 0",
                ] {
                    let sql = format!("SELECT {aggregates} FROM {chain}{predicate}");
                    assert_agree(&f, &r, &sql, &[], &[]).await;
                    let ops = opcodes(&f, &sql).await;
                    if predicate == " WHERE d.id > 1" {
                        assert_eq!(
                            ops.iter().filter(|op| *op == "Rewind").count(),
                            0,
                            "{sql} should seek driver via SeekGT instead of Rewind: {ops:?}"
                        );
                        assert_eq!(
                            ops.iter().filter(|op| *op == "SeekGT").count(),
                            1,
                            "{sql} should seek driver via SeekGT: {ops:?}"
                        );
                    } else {
                        assert_eq!(
                            ops.iter().filter(|op| *op == "Rewind").count(),
                            1,
                            "{sql}: {ops:?}"
                        );
                    }
                    assert_eq!(
                        ops.iter().filter(|op| *op == "ResultRow").count(),
                        1,
                        "{sql}: {ops:?}"
                    );
                    assert!(ops.iter().any(|op| op == "AggStep"), "{sql}: {ops:?}");
                    assert!(ops.iter().any(|op| op == "AggFinal"), "{sql}: {ops:?}");
                    assert!(!ops.iter().any(|op| op == "SorterInsert"), "{sql}: {ops:?}");
                }
            }
        }
        for chain in [
            "empty_driver d JOIN middle m ON m.id=d.sid JOIN leaf l ON l.id=m.lid",
            "empty_driver d JOIN middle m ON m.id=d.sid LEFT JOIN leaf l ON l.id=m.lid",
        ] {
            let sql = format!(
                "SELECT count(*), count(l.id), sum(l.amount), total(l.amount), \
                 avg(l.amount), min(l.amount), max(l.amount) FROM {chain}"
            );
            assert_agree(&f, &r, &sql, &[], &[]).await;
        }

        let parameterized = "SELECT count(*), sum(d.amount), sum(l.amount), max(l.id) FROM driver d \
            JOIN middle m ON m.id=d.sid LEFT JOIN leaf l ON l.id=m.lid WHERE d.id > ?1";
        let prepared = f.prepare(parameterized).await.unwrap();
        for bound in [0_i64, 3, 100, 0] {
            let params = [SqliteValue::Integer(bound)];
            assert_agree(&f, &r, parameterized, &params, &[&bound]).await;
            let prepared_rows = prepared.query_with_params(&params).await.unwrap();
            let direct_rows = f.query_with_params(parameterized, &params).await.unwrap();
            assert_eq!(
                prepared_rows
                    .iter()
                    .map(|row| row.values())
                    .collect::<Vec<_>>(),
                direct_rows
                    .iter()
                    .map(|row| row.values())
                    .collect::<Vec<_>>()
            );
        }
        drop(prepared);

        // A generated probe needs expression scratch registers. Those must
        // not separate the index key from its rowid-floor register.
        let generated = "ALTER TABLE middle ADD COLUMN generated_key INTEGER \
            GENERATED ALWAYS AS (id * 10) VIRTUAL";
        f.execute(generated).await.unwrap();
        r.execute(generated, []).unwrap();
        for sql in [
            "SELECT count(*), sum(l.amount) FROM driver d JOIN middle m ON m.id=d.sid \
             LEFT JOIN leaf l ON l.k=m.generated_key",
            "SELECT d.id, l.id FROM driver d JOIN middle m ON m.id=d.sid \
             LEFT JOIN leaf l ON l.k=m.generated_key ORDER BY d.id",
        ] {
            assert_agree(&f, &r, sql, &[], &[]).await;
            let ops = opcodes(&f, sql).await;
            assert_eq!(
                ops.iter().filter(|op| *op == "Rewind").count(),
                1,
                "{sql}: {ops:?}"
            );
        }

        // Read the explicit transaction's uncommitted pages, then the original
        // values after rollback, through the same compiled join.
        f.execute("BEGIN").await.unwrap();
        r.execute("BEGIN", []).unwrap();
        f.execute("UPDATE leaf SET amount=40.0 WHERE id=1")
            .await
            .unwrap();
        r.execute("UPDATE leaf SET amount=40.0 WHERE id=1", [])
            .unwrap();
        assert_agree(&f, &r, parameterized, &[SqliteValue::Integer(0)], &[&0_i64]).await;
        f.execute("ROLLBACK").await.unwrap();
        r.execute("ROLLBACK", []).unwrap();
        assert_agree(&f, &r, parameterized, &[SqliteValue::Integer(0)], &[&0_i64]).await;

        // Accumulator state belongs to one execution, including after an error.
        f.execute("UPDATE driver SET sid=1, amount=1.0")
            .await
            .unwrap();
        r.execute("UPDATE driver SET sid=1, amount=1.0", [])
            .unwrap();
        let overflow = "SELECT sum(9223372036854775807) FROM driver d \
            JOIN middle m ON m.id=d.sid JOIN leaf l ON l.id=m.lid";
        assert!(
            f.query(overflow)
                .await
                .unwrap_err()
                .to_string()
                .contains("overflow")
        );
        assert!(
            r.query_row(overflow, [], |row| row.get::<_, i64>(0))
                .unwrap_err()
                .to_string()
                .contains("overflow")
        );
        assert_agree(&f, &r, parameterized, &[SqliteValue::Integer(0)], &[&0_i64]).await;
    });
}

#[test]
fn unsupported_multi_join_aggregates_keep_sqlite_semantics() {
    for memory in [false, true] {
        asupersync::test_utils::run_test(|| async move {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("multi_join_fallback.db");
            let target = if memory {
                ":memory:"
            } else {
                path.to_str().unwrap()
            };
            let f = Connection::open(target).await.unwrap();
            let r = rusqlite::Connection::open_in_memory().unwrap();
            f.execute_batch(MULTI_SETUP).await.unwrap();
            r.execute_batch(MULTI_SETUP).unwrap();
            for sql in [
                "SELECT count(*), sum(l.amount) FROM driver d LEFT JOIN middle m ON m.id=d.sid \
                 JOIN leaf l ON l.id=m.lid",
                "SELECT count(*), count(l.id) FROM driver d LEFT JOIN middle m ON m.id=d.sid \
                 LEFT JOIN leaf l ON l.id=m.lid",
                "SELECT count(*), sum(l.amount) FROM driver d JOIN middle m ON m.id=d.sid AND d.id>1 \
                 LEFT JOIN leaf l ON l.id=m.lid",
                "SELECT count(*), sum(l.amount) FROM driver d JOIN middle m ON m.id=d.sid \
                 LEFT JOIN leaf l ON l.id=m.lid+0",
                "SELECT count(DISTINCT d.sid), min(l.name COLLATE NOCASE) FROM driver d \
                 JOIN middle m ON m.id=d.sid LEFT JOIN leaf l ON l.id=m.lid",
                "SELECT count(*) FILTER (WHERE l.id IS NULL), sum(d.amount) FROM driver d \
                 JOIN middle m ON m.id=d.sid LEFT JOIN leaf l ON l.id=m.lid",
                "SELECT count(*), sum(d.amount) FROM driver d \
                 JOIN middle m ON m.id=d.sid JOIN driver l ON l.sid=m.id",
            ] {
                assert_agree(&f, &r, sql, &[], &[]).await;
            }
        });
    }
}

/// Unlike the older lookup oracle's display tags, retain storage classes and
/// complete blobs: equal INTEGER/REAL group keys can select different bare-row
/// representatives when MIN/MAX is present.
#[cfg(feature = "ext-json")]
async fn assert_table_function_agree(
    f: &Connection,
    r: &rusqlite::Connection,
    sql: &str,
    unordered: bool,
) {
    let mut actual = f
        .query(sql)
        .await
        .unwrap_or_else(|error| panic!("franken `{sql}`: {error}"))
        .iter()
        .map(|row| row.values().to_vec())
        .collect::<Vec<_>>();
    let mut statement = r
        .prepare(sql)
        .unwrap_or_else(|error| panic!("stock `{sql}`: {error}"));
    let width = statement.column_count();
    let mut expected = statement
        .query_map([], |row| {
            (0..width)
                .map(|column| {
                    row.get::<_, rusqlite::types::Value>(column)
                        .map(|value| match value {
                            rusqlite::types::Value::Null => SqliteValue::Null,
                            rusqlite::types::Value::Integer(value) => SqliteValue::Integer(value),
                            rusqlite::types::Value::Real(value) => SqliteValue::Float(value),
                            rusqlite::types::Value::Text(value) => SqliteValue::Text(value.into()),
                            rusqlite::types::Value::Blob(value) => SqliteValue::Blob(value.into()),
                        })
                })
                .collect::<rusqlite::Result<Vec<_>>>()
        })
        .unwrap()
        .collect::<rusqlite::Result<Vec<_>>>()
        .unwrap();
    if unordered {
        actual.sort_by_cached_key(|row| format!("{row:?}"));
        expected.sort_by_cached_key(|row| format!("{row:?}"));
    }
    // SqliteValue equality deliberately equates INTEGER 1 and REAL 1.0.
    // Check discriminants separately so the oracle catches changed storage
    // classes in grouped representatives and aggregate result values.
    for (actual_row, expected_row) in actual.iter().zip(&expected) {
        for (actual_value, expected_value) in actual_row.iter().zip(expected_row) {
            assert_eq!(
                std::mem::discriminant(actual_value),
                std::mem::discriminant(expected_value),
                "storage class on `{sql}`: actual {actual_value:?}, expected {expected_value:?}"
            );
        }
    }
    assert_eq!(
        actual, expected,
        "typed table-function aggregate mismatch on `{sql}`"
    );
}

#[test]
#[ignore = "bd-star-hidden-reland-nxl8a: needs b56e7bc7c's Cartesian aggregate path"]
#[cfg(feature = "ext-json")]
fn cartesian_table_function_aggregates_match_typed_stock_results() {
    asupersync::test_utils::run_test(|| async {
        let f = Connection::open(":memory:").await.unwrap();
        let r = rusqlite::Connection::open_in_memory().unwrap();
        for sql in [
            "SELECT count(*),count(a.value),count(b.value),sum(a.value),avg(a.value), \
             min(a.value),max(a.value) FROM json_each('[1,null,3,4.5]')a, \
             json_each('[2,null]')b",
            "SELECT sum(a.value*b.value),sum(a.value+b.value),count(a.value/b.value) \
             FROM json_each('[1,null,3]')a CROSS JOIN json_each('[0,2,4]')b",
            "SELECT count(*),sum(a.value+b.value+c.value) FROM json_each('[1,2]')a \
             JOIN json_each('[3,4,5]')b CROSS JOIN json_each('[6,7]')c",
            "SELECT count(*),sum(a.value),avg(a.value),min(a.value),max(a.value) \
             FROM json_each('[1,2]')a,json_each('[]')b",
            "SELECT count(*),sum(b.value) FROM json_each('[]')a,json_each('[1,2]')b",
            "SELECT count(*),sum(a.atom),count(a.parent),sum(b.key) \
             FROM json_tree('[1,[2,null]]')a,json_each('[3,4]')b",
            "SELECT count(a.json),count(b.json),min(a.root),max(b.root),sum(a.key),sum(b.key) \
             FROM json_each('[1,2]')a,json_each('[3,4,5]')b",
            "SELECT sum(a.rowid),sum(b.rowid),count(a.parent),count(b.parent) \
             FROM json_each('[1,2,3]')a CROSS JOIN json_tree('[4,[5,null]]')b",
            "SELECT min(a.value),max(a.value),count(a.value) \
             FROM json_each('[1,1.0,\"a\",null]')a,json_each('[0,1]')b",
            "SELECT min(CAST(a.value AS BLOB)),max(CAST(a.value AS BLOB)) \
             FROM json_each('[\"ab\",\"ac\",null]')a,json_each('[0,1]')b",
            "SELECT sum(a.value),avg(a.value) FROM json_each('[\"2\",3.5,null,\"not numeric\"]')a, \
             json_each('[0,1]')b",
            "SELECT count(*),sum(a.value) FROM json_each('[1,2]')a,json_each('[3,4]')b \
             HAVING count(*)=4 AND sum(a.value)=6",
            "SELECT count(*) FROM json_each('[1,2]')a,json_each('[3,4]')b HAVING count(*)<0",
            "SELECT count(*) FROM json_each('[1,2,3]') HAVING count(*)=3",
            "SELECT count(*) FROM json_each('[]')a,json_each('[1]')b HAVING count(*)=0",
            "SELECT count(*) FROM json_each('[1,2]')a,json_each('[3,4]')b LIMIT 0",
            "SELECT count(*) FROM json_each('[1,2]')a,json_each('[3,4]')b LIMIT 1 OFFSET 1",
        ] {
            assert_table_function_agree(&f, &r, sql, false).await;
        }
        for sql in [
            "SELECT a.value%2 AS bucket,count(*),sum(a.value*b.value) \
             FROM json_each('[1,2,3,4]')a,json_each('[2,3]')b GROUP BY bucket",
            "SELECT a.value,count(*),sum(b.value) FROM json_each('[1,1.0,null,2]')a, \
             json_each('[3,4]')b GROUP BY a.value",
            "SELECT a.value,max(a.type='real') FROM json_each('[1,1.0]')a, \
             json_each('[0]')b GROUP BY a.value",
            "SELECT a.value,min(a.type='real') FROM json_each('[1.0,1]')a, \
             json_each('[0]')b GROUP BY a.value",
            "SELECT a.value,count(*) FROM json_each('[]')a,json_each('[1,2]')b GROUP BY a.value",
        ] {
            assert_table_function_agree(&f, &r, sql, true).await;
        }
        for sql in [
            "SELECT a.value%2 AS bucket,count(*),sum(b.value) FROM json_each('[1,2,3,4]')a, \
             json_each('[3,4]')b GROUP BY bucket HAVING sum(b.value)>10 ORDER BY bucket DESC LIMIT 1",
            "SELECT a.value,count(*) FROM json_each('[3,1,2]')a,json_each('[0,1]')b \
             GROUP BY a.value ORDER BY a.value DESC LIMIT 1 OFFSET 1",
        ] {
            assert_table_function_agree(&f, &r, sql, false).await;
        }
    });
}

#[test]
#[ignore = "bd-star-hidden-reland-nxl8a: needs b56e7bc7c's Cartesian aggregate path"]
#[cfg(feature = "ext-json")]
fn cartesian_aggregate_numeric_accumulators_match_stock() {
    asupersync::test_utils::run_test(|| async {
        let f = Connection::open(":memory:").await.unwrap();
        let r = rusqlite::Connection::open_in_memory().unwrap();
        f.execute("CREATE TABLE aggregate_numbers(value)")
            .await
            .unwrap();
        r.execute_batch("CREATE TABLE aggregate_numbers(value)")
            .unwrap();
        for json in [
            "[9007199254740993,1.0,-9007199254740992]",
            "[-9007199254740992,9007199254740993,1.0]",
            "[9223372036854775807,1.0,9223372036854775807]",
            "[9223372036854775807,1,1.0]",
            "[-9223372036854775808,-1,1.0]",
            "[1e999,2]",
            "[1e999,-1e999]",
            "[\"1x\",\"2.5x\",null,\"not numeric\"]",
        ] {
            let single = format!("SELECT sum(value),avg(value) FROM json_each('{json}')");
            assert_table_function_agree(&f, &r, &single, false).await;
            for copies in ["[0]", "[0,1]"] {
                let sql = format!(
                    "SELECT sum(a.value),avg(a.value) FROM json_each('{json}')a,json_each('{copies}')b"
                );
                assert_table_function_agree(&f, &r, &sql, false).await;
            }
            // Ordinary-table GROUP BY uses the same streaming accumulators.
            // Keep their existing call sites under the typed numeric oracle too.
            f.execute("DELETE FROM aggregate_numbers").await.unwrap();
            r.execute_batch("DELETE FROM aggregate_numbers").unwrap();
            let insert =
                format!("INSERT INTO aggregate_numbers SELECT value FROM json_each('{json}')");
            f.execute(&insert).await.unwrap();
            r.execute_batch(&insert).unwrap();
            assert_table_function_agree(
                &f,
                &r,
                "SELECT 0+0,sum(value),avg(value) FROM aggregate_numbers GROUP BY 0+0",
                false,
            )
            .await;
        }
        for json in ["[9223372036854775807,1]", "[-9223372036854775808,-1]"] {
            let from = format!("FROM json_each('{json}')a,json_each('[0]')b");
            let sum = format!("SELECT sum(a.value) {from}");
            let error = f.query(&sum).await.unwrap_err().to_string();
            assert!(error.contains("overflow"), "{error}");
            let error = r
                .query_row(&sum, [], |row| row.get::<_, f64>(0))
                .unwrap_err()
                .to_string();
            assert!(error.contains("overflow"), "{error}");
            assert_table_function_agree(&f, &r, &format!("SELECT avg(a.value) {from}"), false)
                .await;
        }
    });
}

#[test]
#[cfg(feature = "ext-json")]
fn cartesian_aggregate_preserves_nested_loop_order_for_integer_overflow() {
    asupersync::test_utils::run_test(|| async {
        let f = Connection::open(":memory:").await.unwrap();
        let r = rusqlite::Connection::open_in_memory().unwrap();
        // The two products contain the same multiset of values. Repeating a
        // value before its opposite overflows; completing the inner pair
        // before repeating it does not. CROSS JOIN fixes the source order.
        let overflow = "SELECT sum(a.value) \
            FROM json_each('[9223372036854775807,-9223372036854775807]')a \
            CROSS JOIN json_each('[0,1]')b";
        let actual = f.query(overflow).await.unwrap_err().to_string();
        let expected = r
            .query_row(overflow, [], |row| row.get::<_, i64>(0))
            .unwrap_err()
            .to_string();
        assert!(actual.contains("overflow"), "{actual}");
        assert!(expected.contains("overflow"), "{expected}");
        assert_table_function_agree(
            &f,
            &r,
            "SELECT sum(b.value) FROM json_each('[0,1]')a CROSS JOIN \
             json_each('[9223372036854775807,-9223372036854775807]')b",
            false,
        )
        .await;
    });
}

#[test]
#[ignore = "bd-star-hidden-reland-nxl8a: needs b56e7bc7c's Cartesian aggregate path"]
#[cfg(feature = "ext-json")]
fn cartesian_aggregate_expression_errors_and_empty_inputs_match_stock() {
    asupersync::test_utils::run_test(|| async {
        let f = Connection::open(":memory:").await.unwrap();
        let r = rusqlite::Connection::open_in_memory().unwrap();
        for (sql, message) in [
            (
                "SELECT count(abs(-9223372036854775808)) \
                 FROM json_each('[1,2]')a CROSS JOIN json_each('[0]')b",
                "overflow",
            ),
            (
                "SELECT sum(json_extract(a.value,'$')) \
                 FROM json_each('[\"1\",\"bad\"]')a CROSS JOIN json_each('[0,1]')b",
                "malformed JSON",
            ),
            (
                "SELECT count(*) FROM json_each('bad')a CROSS JOIN json_each('[0]')b",
                "malformed JSON",
            ),
            (
                "SELECT sum(abs(a.value)),sum(json_extract(b.value,'$')) \
                 FROM json_each('[1,-9223372036854775808]')a,json_each('[\"bad\"]')b",
                "malformed JSON",
            ),
            (
                "SELECT -a.key,sum(abs(a.value)),sum(json_extract(b.value,'$')) \
                 FROM json_each('[1,-9223372036854775808]')a,json_each('[\"bad\"]')b \
                 GROUP BY -a.key",
                "overflow",
            ),
        ] {
            let actual = f.query(sql).await.unwrap_err().to_string();
            let expected = r
                .query_row(sql, [], |row| row.get::<_, rusqlite::types::Value>(0))
                .unwrap_err()
                .to_string();
            assert!(actual.contains(message), "{sql}: {actual}");
            assert!(expected.contains(message), "{sql}: {expected}");
            // Each failure must leave the connection and aggregate state usable.
            assert_table_function_agree(
                &f,
                &r,
                "SELECT sum(a.value),count(*) FROM json_each('[1,2]')a,json_each('[0,1]')b",
                false,
            )
            .await;
        }
        for sql in [
            "SELECT count(abs(-9223372036854775808)),sum(abs(-9223372036854775808)) \
             FROM json_each('[]')a CROSS JOIN json_each('[0,1]')b",
            "SELECT sum(json_extract('bad','$')) \
             FROM json_each('[1,2]')a CROSS JOIN json_each('[]')b",
            "SELECT sum(coalesce(a.value,abs(-9223372036854775808))), \
             sum(ifnull(a.value,json_extract('bad','$'))) \
             FROM json_each('[1,2]')a CROSS JOIN json_each('[0,1]')b",
            "SELECT sum(coalesce(a.value,b.value,4)),sum(ifnull(a.value,b.value)) \
             FROM json_each('[null,2]')a CROSS JOIN json_each('[null,3]')b",
        ] {
            assert_table_function_agree(&f, &r, sql, false).await;
        }
        for sql in [
            "SELECT sum(coalesce(a.value,abs(-9223372036854775808))) \
             FROM json_each('[null]')a CROSS JOIN json_each('[0]')b",
            "SELECT sum(ifnull(a.value,json_extract('bad','$'))) \
             FROM json_each('[null]')a CROSS JOIN json_each('[0]')b",
        ] {
            assert!(f.query(sql).await.is_err(), "franken accepted {sql}");
            assert!(
                r.query_row(sql, [], |row| row.get::<_, i64>(0)).is_err(),
                "stock accepted {sql}"
            );
        }
    });
}

#[test]
#[ignore = "bd-star-hidden-reland-nxl8a: json and root are not columns without b56e7bc7c"]
#[cfg(feature = "ext-json")]
fn json_hidden_columns_preserve_star_widths_and_join_visibility() {
    asupersync::test_utils::run_test(|| async {
        let f = Connection::open(":memory:").await.unwrap();
        let r = rusqlite::Connection::open_in_memory().unwrap();
        for (sql, width) in [
            ("SELECT * FROM json_each('[1,2]')", 8),
            ("SELECT j.* FROM json_each('[1,2]')j", 8),
            ("SELECT * FROM json_each('[1,2]')a,json_each('[3,4]')b", 16),
            (
                "SELECT a.* FROM json_each('[1,2]')a JOIN json_each('[1,2]')b USING(json)",
                8,
            ),
            (
                "SELECT * FROM json_each('[1,2]')a JOIN json_each('[1,2]')b USING(json)",
                16,
            ),
            (
                "WITH q AS (SELECT * FROM json_each('[1,2]')) SELECT * FROM q",
                8,
            ),
            ("SELECT q.* FROM (SELECT * FROM json_each('[1,2]'))q", 8),
            (
                "SELECT * FROM json_each('[1,2]')j NATURAL JOIN \
              (SELECT 'different' AS json,'other' AS root)x",
                10,
            ),
            (
                "SELECT * FROM json_each('[1]')a JOIN (SELECT '$' AS root)b USING(root) \
                 NATURAL JOIN (SELECT '$' AS root)c",
                8,
            ),
        ] {
            let rows = f
                .query(sql)
                .await
                .unwrap_or_else(|error| panic!("{sql}: {error}"));
            let mut statement = r.prepare(sql).unwrap();
            assert_eq!(statement.column_count(), width, "stock width: {sql}");
            let expected_count = statement
                .query_map([], |_| Ok(()))
                .unwrap()
                .collect::<rusqlite::Result<Vec<_>>>()
                .unwrap()
                .len();
            assert_eq!(rows.len(), expected_count, "row count: {sql}");
            assert!(
                rows.iter().all(|row| row.values().len() == width),
                "width: {sql}"
            );
            // JSON1's id/parent values describe its private traversal and need
            // not equal another implementation's numbering. Stable projected
            // fields and explicit hidden columns are compared below.
        }
        for sql in [
            "WITH q(k,v,t,a,i,p,f,path) AS (SELECT * FROM json_each('[1,2]')) \
             SELECT k,v,t,a,p,f,path FROM q",
            "SELECT q.value,q.type,q.key,q.atom,q.fullkey,q.path,q.parent \
             FROM (SELECT * FROM json_each('[1,2]'))q",
            "SELECT count(*) FROM json_tree('[1]')a NATURAL JOIN json_tree('[ 1 ]')b",
            "SELECT count(*) FROM json_each('[1,2]')a NATURAL JOIN \
             (SELECT 'different' AS json,'other' AS root)b",
            "SELECT count(*),min(json),max(a.root) FROM json_each('[1,2]')a \
             JOIN json_each('[1,2]')b USING(json)",
            "SELECT count(*),min(root),max(b.json) FROM json_each('[1,2]','$')a \
             JOIN json_each('[3,4]','$')b USING(root)",
            "SELECT (1,2,3,4,5,6,7,8) IN (SELECT * FROM json_each('[]'))",
            "SELECT root FROM json_each('[1]')a JOIN (SELECT '$' AS root)b USING(root) \
             NATURAL JOIN (SELECT '$' AS root)c",
            "SELECT root FROM json_each('[1]')a JOIN (SELECT '$' AS root)b USING(root) \
             NATURAL RIGHT JOIN (SELECT 'different' AS root)c",
            "WITH q(k,v,t,a,i,p,f,path) AS (SELECT * FROM json_each('[1]')a \
             JOIN (SELECT '$' AS root)b USING(root) NATURAL JOIN (SELECT '$' AS root)c) \
             SELECT k,v,t,a,p,f,path FROM q",
        ] {
            assert_table_function_agree(&f, &r, sql, false).await;
        }
        let scalar = "SELECT (SELECT * FROM json_each('[]'))";
        let actual = f.query(scalar).await.unwrap_err().to_string();
        let expected = r
            .query_row(scalar, [], |row| row.get::<_, i64>(0))
            .unwrap_err()
            .to_string();
        assert!(
            actual.contains('8') && actual.contains("column"),
            "{actual}"
        );
        assert!(
            expected.contains('8') && expected.contains("column"),
            "{expected}"
        );
    });
}

#[test]
#[ignore = "bd-star-hidden-reland-nxl8a: needs b56e7bc7c's Cartesian aggregate path"]
#[cfg(feature = "ext-json")]
fn cartesian_aggregate_validates_column_bindings_before_scanning_empty_inputs() {
    asupersync::test_utils::run_test(|| async {
        let f = Connection::open(":memory:").await.unwrap();
        let r = rusqlite::Connection::open_in_memory().unwrap();
        for (projection, suffix, message) in [
            ("sum(a.missing)", "", "no such column"),
            ("sum(a.missing+1)", "", "no such column"),
            ("sum(value+1)", "", "ambiguous column"),
            ("count(*)", "GROUP BY missing", "no such column"),
            ("count(*)", "GROUP BY abs(missing)", "no such column"),
        ] {
            for source in ["[]", "[1]"] {
                let sql = format!(
                    "SELECT {projection} FROM json_each('{source}')a \
                     CROSS JOIN json_each('[0]')b {suffix}"
                );
                let actual = f.query(&sql).await.unwrap_err().to_string();
                let expected = match r.prepare(&sql) {
                    Ok(_) => panic!("stock accepted invalid column binding: {sql}"),
                    Err(error) => error.to_string(),
                };
                assert!(actual.contains(message), "{sql}: {actual}");
                assert!(expected.contains(message), "{sql}: {expected}");
            }
        }
        for source in ["[]", "[1]"] {
            let sql = format!(
                "SELECT count(*) FROM json_each('{source}')a \
                 CROSS JOIN json_each('[0]','$',1)b"
            );
            assert!(f.query(&sql).await.is_err(), "franken accepted {sql}");
            assert!(r.prepare(&sql).is_err(), "stock accepted {sql}");
        }
    });
}

#[test]
#[ignore = "bd-star-hidden-reland-nxl8a: needs b56e7bc7c's Cartesian aggregate path"]
#[cfg(feature = "ext-json")]
fn unsupported_cartesian_aggregate_forms_preserve_predicates_and_collations() {
    asupersync::test_utils::run_test(|| async {
        let f = Connection::open(":memory:").await.unwrap();
        let r = rusqlite::Connection::open_in_memory().unwrap();
        for sql in [
            "SELECT count(*) FROM json_each('[1]')a,json_each('[1]')b \
             WHERE a.value=b.value OR json_extract('bad','$')",
            "SELECT count(*) FROM json_each('[1]')a,json_each('[1]')b \
             WHERE CASE WHEN a.value=b.value THEN 1 ELSE json_extract('bad','$') END",
            "SELECT count(*),sum(a.value) FROM json_each('[1,2,3]')a,json_each('[2,3]')b \
             WHERE a.value IN (SELECT value FROM json_each('[1,3]')) AND b.value>2",
            "SELECT count(*),sum(b.value) FROM json_each('[1,2]')a \
             JOIN json_each('[2,3]')b ON a.value=b.value",
            "SELECT count(*),count(b.value),sum(a.value) FROM json_each('[1,2]')a \
             LEFT JOIN json_each('[2,3]')b ON a.value=b.value",
            "SELECT count(*),count(a.value),count(b.value),sum(b.value) FROM json_each('[1,2]')a \
             RIGHT JOIN json_each('[2,3]')b ON a.value=b.value",
            "SELECT count(*),count(a.value),count(b.value) FROM json_each('[1,2]')a \
             FULL OUTER JOIN json_each('[2,3]')b ON a.value=b.value",
            "SELECT count(*),sum(value) FROM json_each('[1,2]')a \
             NATURAL JOIN json_each('[1,2]')b",
            "SELECT count(*),sum(value) FROM json_each('[1,2]')a JOIN json_each('[2,3]')b USING(value)",
            "SELECT count(*),sum(b.value) FROM json_each('[[1,2],[3]]')a,json_each(a.value)b",
            "SELECT count(DISTINCT a.value),sum(DISTINCT a.value) FROM json_each('[1,1,2]')a, \
             json_each('[3,4]')b",
            "SELECT count(*) FILTER (WHERE a.value>1),sum(b.value) FILTER (WHERE a.value<2) \
             FROM json_each('[1,2]')a,json_each('[3,4]')b",
            "SELECT group_concat(a.value||b.value ORDER BY a.value DESC,b.value) \
             FROM json_each('[1,2]')a,json_each('[3,4]')b",
            "SELECT min(a.value COLLATE NOCASE),max(a.value COLLATE NOCASE) \
             FROM json_each('[\"a\",\"B\",\"A\"]')a,json_each('[0,1]')b",
            "SELECT count(a.value=b.value COLLATE NOCASE),sum(a.value=b.value COLLATE NOCASE) \
             FROM json_each('[\"a\",\"B\",null]')a,json_each('[\"A\",\"b\"]')b",
            "SELECT a.value COLLATE NOCASE,count(*) FROM json_each('[\"a\",\"B\",\"A\"]')a, \
             json_each('[0,1]')b GROUP BY a.value COLLATE NOCASE ORDER BY a.value COLLATE NOCASE",
        ] {
            assert_table_function_agree(&f, &r, sql, false).await;
        }
        for sql in [
            "SELECT sum(9223372036854775807) FROM json_each('[1,2]')a,json_each('[0]')b",
            "SELECT count(*) FROM json_each('[1]')a,json_each('[1]')b \
             WHERE a.value!=b.value OR json_extract('bad','$')",
        ] {
            assert!(f.query(sql).await.is_err(), "{sql}");
            let error = r.query_row(sql, [], |row| row.get::<_, i64>(0));
            assert!(error.is_err(), "{sql}");
        }
        // A later independent source is never opened when an earlier source
        // is empty. Argument evaluation and scan errors follow the same rule.
        for sql in [
            "SELECT count(*) FROM json_each('[]')a CROSS JOIN json_each('bad')b",
            "SELECT count(*) FROM json_each('[1]')a CROSS JOIN json_each('[]')b \
             CROSS JOIN json_each('bad')c",
            "SELECT count(*) FROM json_each('[]')a \
             CROSS JOIN json_each(json_extract('bad','$'))b",
        ] {
            assert_table_function_agree(&f, &r, sql, false).await;
        }
        for sql in [
            "SELECT count(*) FROM json_each('[1]')a CROSS JOIN json_each('bad')b",
            "SELECT count(*) FROM json_each('bad')a CROSS JOIN json_each('[]')b",
        ] {
            let actual = f.query(sql).await.unwrap_err().to_string();
            let expected = r
                .query_row(sql, [], |row| row.get::<_, i64>(0))
                .unwrap_err()
                .to_string();
            assert!(actual.contains("malformed JSON"), "{sql}: {actual}");
            assert!(expected.contains("malformed JSON"), "{sql}: {expected}");
        }
    });
}

#[cfg(feature = "ext-json")]
struct ReverseBinary;

#[cfg(feature = "ext-json")]
impl fsqlite_func::collation::CollationFunction for ReverseBinary {
    fn name(&self) -> &str {
        "BINARY"
    }

    fn compare(&self, left: &[u8], right: &[u8]) -> std::cmp::Ordering {
        right.cmp(left)
    }
}

#[cfg(feature = "ext-json")]
struct ReplacementSum;

#[cfg(feature = "ext-json")]
struct ReplacementAbs;

#[cfg(feature = "ext-json")]
impl fsqlite_func::ScalarFunction for ReplacementAbs {
    fn name(&self) -> &str {
        "abs"
    }

    fn num_args(&self) -> i32 {
        1
    }

    fn invoke(&self, args: &[SqliteValue]) -> fsqlite_error::Result<SqliteValue> {
        // Deliberately preserve the sign to distinguish the registration from
        // the built-in even though both are declared deterministic.
        Ok(args[0].clone())
    }
}

#[cfg(feature = "ext-json")]
impl fsqlite_func::AggregateFunction for ReplacementSum {
    type State = i64;

    fn initial_state(&self) -> Self::State {
        1000
    }

    fn step(&self, state: &mut Self::State, _args: &[SqliteValue]) -> fsqlite_error::Result<()> {
        *state += 1;
        Ok(())
    }

    fn finalize(&self, state: Self::State) -> fsqlite_error::Result<SqliteValue> {
        Ok(SqliteValue::Integer(state))
    }

    fn num_args(&self) -> i32 {
        1
    }

    fn name(&self) -> &str {
        "sum"
    }
}

#[test]
#[ignore = "bd-star-hidden-reland-nxl8a: needs b56e7bc7c's table function registry"]
#[cfg(feature = "ext-json")]
fn cartesian_table_functions_honor_module_aggregate_and_collation_replacements() {
    asupersync::test_utils::run_test(|| async {
        let f = Connection::open(":memory:").await.unwrap();
        f.register_module(
            "custom_tree",
            Box::new(fsqlite_func::vtab::module_factory_from::<
                fsqlite_ext_json::JsonTreeVtab,
            >()),
        );
        f.register_module(
            "JSON_EACH",
            Box::new(fsqlite_func::vtab::module_factory_from::<
                fsqlite_ext_json::JsonTreeVtab,
            >()),
        );
        for name in ["custom_tree", "json_each"] {
            let sql = format!("SELECT count(*) FROM {name}('[1,2]')a,{name}('[3,4]')b");
            let rows = f.query(&sql).await.unwrap();
            assert_eq!(rows[0].values(), &[SqliteValue::Integer(9)], "{sql}");
            let sql = format!("SELECT count(*) FROM json_tree('1')a,{name}('[3,4]')b");
            let rows = f.query(&sql).await.unwrap();
            assert_eq!(rows[0].values(), &[SqliteValue::Integer(3)], "{sql}");
        }

        let f = Connection::open(":memory:").await.unwrap();
        f.register_deterministic_scalar_function(ReplacementAbs);
        let rows = f
            .query(
                "SELECT sum(abs(a.value)),count(*) FROM json_each('[-2,-1]')a,json_each('[0,1]')b",
            )
            .await
            .unwrap();
        assert_eq!(
            rows[0].values(),
            &[SqliteValue::Integer(-6), SqliteValue::Integer(4)]
        );

        let f = Connection::open(":memory:").await.unwrap();
        f.register_collation_function(ReverseBinary);
        let rows = f.query(
            "SELECT min(a.value),max(a.value) FROM json_each('[\"a\",\"b\"]')a,json_each('[0,1]')b",
        ).await.unwrap();
        assert_eq!(
            rows[0].values(),
            &[SqliteValue::Text("b".into()), SqliteValue::Text("a".into())]
        );

        let f = Connection::open(":memory:").await.unwrap();
        f.register_aggregate_function(ReplacementSum);
        let rows = f
            .query("SELECT sum(a.value),count(*) FROM json_each('[1,2]')a,json_each('[3,4,5]')b")
            .await
            .unwrap();
        assert_eq!(
            rows[0].values(),
            &[SqliteValue::Integer(1006), SqliteValue::Integer(6)]
        );
    });
}

#[cfg(feature = "ext-json")]
mod cartesian_callback_regression {
    use super::{Connection, ReplacementSum, SqliteValue};
    use fsqlite_error::{FrankenError, Result};
    use fsqlite_func::vtab::{
        ColumnContext, IndexInfo, VirtualTable, VirtualTableCursor, module_factory_from,
    };
    use fsqlite_types::cx::Cx;
    use std::cell::RefCell;
    use std::rc::{Rc, Weak};

    thread_local! {
        static CALLBACK_CONNECTION: RefCell<Option<Weak<Connection>>> = const { RefCell::new(None) };
    }

    struct CallbackConnectionGuard;

    impl Drop for CallbackConnectionGuard {
        fn drop(&mut self) {
            CALLBACK_CONNECTION.with(|slot| *slot.borrow_mut() = None);
        }
    }

    struct RegisterSumOnColumn(bool);

    impl VirtualTable for RegisterSumOnColumn {
        type Cursor = Self;

        fn connect(_cx: &Cx, _args: &[&str]) -> Result<Self> {
            Ok(Self(false))
        }

        fn best_index(&self, _info: &mut IndexInfo) -> Result<()> {
            Ok(())
        }

        fn open(&self) -> Result<Self::Cursor> {
            Ok(Self(false))
        }
    }

    impl VirtualTableCursor for RegisterSumOnColumn {
        fn filter(
            &mut self,
            _cx: &Cx,
            _idx_num: i32,
            _idx_str: Option<&str>,
            _args: &[SqliteValue],
        ) -> Result<()> {
            self.0 = false;
            Ok(())
        }

        fn next(&mut self, _cx: &Cx) -> Result<()> {
            self.0 = true;
            Ok(())
        }

        fn eof(&self) -> bool {
            self.0
        }

        fn column(&self, _ctx: &mut ColumnContext, _col: i32) -> Result<()> {
            let connection = CALLBACK_CONNECTION.with(|slot| {
                slot.borrow()
                    .as_ref()
                    .and_then(Weak::upgrade)
                    .expect("callback connection must remain live")
            });
            connection.register_aggregate_function(ReplacementSum);
            Ok(())
        }

        fn rowid(&self) -> Result<i64> {
            Ok(1)
        }
    }

    #[test]
    fn cartesian_count_does_not_elide_custom_module_registry_callbacks() {
        asupersync::test_utils::run_test(|| async {
            let connection = Rc::new(Connection::open(":memory:").await.unwrap());
            CALLBACK_CONNECTION.with(|slot| {
                assert!(
                    slot.borrow_mut()
                        .replace(Rc::downgrade(&connection))
                        .is_none()
                );
            });
            let _guard = CallbackConnectionGuard;
            connection.register_module(
                "JSON_EACH",
                Box::new(module_factory_from::<RegisterSumOnColumn>()),
            );

            // The general executor observes xColumn even for COUNT(*). A
            // registry replacement during that callback invalidates the scan.
            // Admitting this factory by name would mask every column, skip the
            // callback entirely, and incorrectly return a successful count.
            let result = connection
                .query("SELECT count(*) FROM json_each('[1]')a,json_tree('2')b")
                .await;
            assert!(
                matches!(result, Err(FrankenError::SchemaChanged)),
                "{result:?}"
            );

            let rows = connection
                .query("SELECT sum(value) FROM json_tree('2')")
                .await
                .unwrap();
            assert_eq!(rows[0].values(), &[SqliteValue::Integer(1001)]);
        });
    }
}

#[cfg(feature = "ext-json")]
mod table_function_visibility_registration {
    use super::{Connection, SqliteValue};
    use fsqlite_error::{FrankenError, Result};
    use fsqlite_ext_json::JsonEachVtab;
    use fsqlite_func::vtab::{IndexInfo, VirtualTable, module_factory_from};
    use fsqlite_types::cx::Cx;
    use std::cell::RefCell;
    use std::rc::{Rc, Weak};

    #[derive(Clone, Copy)]
    enum MetadataMutation {
        ModuleOnce,
        ModuleAlways,
        ScalarOnce,
    }

    struct MetadataReplacement {
        connection: Weak<Connection>,
        calls: usize,
        mutation: MetadataMutation,
    }

    thread_local! {
        static METADATA_REPLACEMENT: RefCell<Option<MetadataReplacement>> = const { RefCell::new(None) };
    }

    struct MetadataReplacementGuard;

    impl MetadataReplacementGuard {
        fn install(connection: &Rc<Connection>, mutation: MetadataMutation) -> Self {
            METADATA_REPLACEMENT.with(|slot| {
                assert!(
                    slot.borrow_mut()
                        .replace(MetadataReplacement {
                            connection: Rc::downgrade(connection),
                            calls: 0,
                            mutation,
                        })
                        .is_none()
                );
            });
            Self
        }
    }

    impl Drop for MetadataReplacementGuard {
        fn drop(&mut self) {
            METADATA_REPLACEMENT.with(|slot| *slot.borrow_mut() = None);
        }
    }

    /// Same column layout and data as JSON_EACH, but explicitly declares all
    /// ten columns visible through the default metadata method.
    struct VisibleJsonArguments(JsonEachVtab);

    impl VirtualTable for VisibleJsonArguments {
        type Cursor = <JsonEachVtab as VirtualTable>::Cursor;

        fn column_info(args: &[&str]) -> Vec<(String, char)> {
            JsonEachVtab::column_info(args)
        }

        fn connect(cx: &Cx, args: &[&str]) -> Result<Self> {
            JsonEachVtab::connect(cx, args).map(Self)
        }

        fn best_index(&self, info: &mut IndexInfo) -> Result<()> {
            self.0.best_index(info)
        }

        fn open(&self) -> Result<Self::Cursor> {
            self.0.open()
        }
    }

    struct ReplaceEarlierOnMetadata(JsonEachVtab);

    struct ValuesVisibilityProbe;

    impl fsqlite_func::ScalarFunction for ValuesVisibilityProbe {
        fn name(&self) -> &str {
            "values_visibility_probe"
        }

        fn num_args(&self) -> i32 {
            0
        }

        fn invoke(&self, _args: &[SqliteValue]) -> Result<SqliteValue> {
            Ok(SqliteValue::Integer(1))
        }
    }

    impl VirtualTable for ReplaceEarlierOnMetadata {
        type Cursor = <JsonEachVtab as VirtualTable>::Cursor;

        fn column_info(args: &[&str]) -> Vec<(String, char)> {
            let replacement = METADATA_REPLACEMENT.with(|slot| {
                let mut slot = slot.borrow_mut();
                let state = slot.as_mut().expect("metadata fixture must be installed");
                state.calls += 1;
                let replace = match state.mutation {
                    MetadataMutation::ModuleOnce => state.calls == 2,
                    MetadataMutation::ModuleAlways => {
                        assert!(state.calls <= 16, "metadata retries must remain bounded");
                        true
                    }
                    MetadataMutation::ScalarOnce => state.calls == 1,
                };
                replace.then(|| {
                    (
                        state
                            .connection
                            .upgrade()
                            .expect("metadata callback connection must remain live"),
                        state.mutation,
                    )
                })
            });
            if let Some((connection, mutation)) = replacement {
                match mutation {
                    MetadataMutation::ModuleOnce | MetadataMutation::ModuleAlways => {
                        connection.register_module(
                            "custom_a",
                            Box::new(module_factory_from::<VisibleJsonArguments>()),
                        );
                    }
                    MetadataMutation::ScalarOnce => {
                        connection.register_nondeterministic_scalar_function(ValuesVisibilityProbe);
                    }
                }
            }
            JsonEachVtab::column_info(args)
        }

        fn connect(cx: &Cx, args: &[&str]) -> Result<Self> {
            JsonEachVtab::connect(cx, args).map(Self)
        }

        fn best_index(&self, info: &mut IndexInfo) -> Result<()> {
            self.0.best_index(info)
        }

        fn open(&self) -> Result<Self::Cursor> {
            self.0.open()
        }
    }

    #[test]
    #[ignore = "bd-star-hidden-reland-nxl8a: needs b56e7bc7c's table function registry"]
    fn table_function_star_retries_cross_source_metadata_replacement() {
        asupersync::test_utils::run_test(|| async {
            let connection = Rc::new(Connection::open(":memory:").await.unwrap());
            let _guard =
                MetadataReplacementGuard::install(&connection, MetadataMutation::ModuleOnce);
            connection.register_module("custom_a", Box::new(module_factory_from::<JsonEachVtab>()));
            connection.register_module(
                "custom_b",
                Box::new(module_factory_from::<ReplaceEarlierOnMetadata>()),
            );

            // Detection reads both sources once. During the expansion pass,
            // B replaces A after A's eight-column projection has been read.
            // B itself stays current, so only the whole-projection generation
            // check can prevent those stale eight columns from being used.
            let sql = "SELECT * FROM custom_a('[1]')a,custom_b('[2]')b";
            // The current statement retries from its original star and sees
            // A's ten visible columns plus B's eight. Reuse must stay current.
            for _ in 0..2 {
                let rows = connection.query(sql).await.unwrap();
                assert_eq!(rows.len(), 1);
                assert_eq!(rows[0].values().len(), 18);
                assert_eq!(rows[0].values()[8], SqliteValue::Text("[1]".into()));
                assert_eq!(rows[0].values()[9], SqliteValue::Text("$".into()));
            }
        });
    }

    #[test]
    #[ignore = "bd-star-hidden-reland-nxl8a: needs b56e7bc7c's table function registry"]
    fn table_function_star_bounds_perpetual_metadata_replacement() {
        asupersync::test_utils::run_test(|| async {
            let connection = Rc::new(Connection::open(":memory:").await.unwrap());
            let _guard =
                MetadataReplacementGuard::install(&connection, MetadataMutation::ModuleAlways);
            connection.register_module("custom_a", Box::new(module_factory_from::<JsonEachVtab>()));
            connection.register_module(
                "custom_b",
                Box::new(module_factory_from::<ReplaceEarlierOnMetadata>()),
            );
            let sql = "SELECT * FROM custom_a('[1]')a,custom_b('[2]')b";
            let result = connection.query(sql).await;
            assert!(
                matches!(result, Err(FrankenError::SchemaChanged)),
                "{result:?}"
            );
            METADATA_REPLACEMENT.with(|slot| {
                assert!(slot.borrow().as_ref().unwrap().calls >= 2);
            });

            // Exhaustion must not cache a partially expanded projection.
            connection.register_module("custom_b", Box::new(module_factory_from::<JsonEachVtab>()));
            let rows = connection.query(sql).await.unwrap();
            assert_eq!(rows.len(), 1);
            assert_eq!(rows[0].values().len(), 18);
        });
    }

    #[test]
    #[ignore = "bd-star-hidden-reland-nxl8a: needs b56e7bc7c's table function registry"]
    fn table_function_star_metadata_retry_preserves_values_donor_snapshot() {
        asupersync::test_utils::run_test(|| async {
            let connection = Rc::new(Connection::open(":memory:").await.unwrap());
            connection.register_deterministic_scalar_function(ValuesVisibilityProbe);
            let control = connection
                .query("SELECT (VALUES(1),(values_visibility_probe()),(CAST(1 AS NUMERIC))) = '1'")
                .await
                .unwrap();
            assert_eq!(control[0].values(), &[SqliteValue::Integer(0)]);
            let _guard =
                MetadataReplacementGuard::install(&connection, MetadataMutation::ScalarOnce);
            connection.register_module(
                "custom_b",
                Box::new(module_factory_from::<ReplaceEarlierOnMetadata>()),
            );
            let sql = "SELECT (VALUES(1),(values_visibility_probe()),(CAST(1 AS NUMERIC))) = '1', \
                       b.* FROM custom_b('[2]')b";
            let rows = connection.query(sql).await.unwrap();
            assert_eq!(rows.len(), 1);
            assert_eq!(rows[0].values().len(), 9);
            assert_eq!(rows[0].values()[0], SqliteValue::Integer(1));
            assert_eq!(
                connection
                    .prepare(sql)
                    .await
                    .unwrap()
                    .query()
                    .await
                    .unwrap()[0]
                    .values(),
                rows[0].values()
            );
        });
    }

    #[test]
    #[ignore = "bd-star-hidden-reland-nxl8a: needs b56e7bc7c's table function registry"]
    fn table_function_hidden_columns_follow_registered_metadata_not_module_names() {
        asupersync::test_utils::run_test(|| async {
            let f = Connection::open(":memory:").await.unwrap();
            f.register_module(
                "custom_json",
                Box::new(module_factory_from::<JsonEachVtab>()),
            );
            for sql in [
                "SELECT * FROM custom_json('[1]')",
                "WITH q AS (SELECT * FROM custom_json('[1]')) SELECT * FROM q",
            ] {
                let rows = f.query(sql).await.unwrap();
                assert_eq!(rows.len(), 1);
                assert_eq!(rows[0].values().len(), 8, "{sql}");
            }
            let rows = f
                .query("SELECT json,root,rowid FROM custom_json('[1]')")
                .await
                .unwrap();
            assert_eq!(
                rows[0].values(),
                &[
                    SqliteValue::Text("[1]".into()),
                    SqliteValue::Text("$".into()),
                    SqliteValue::Integer(0),
                ]
            );

            f.register_module(
                "JSON_EACH",
                Box::new(module_factory_from::<VisibleJsonArguments>()),
            );
            for sql in [
                "SELECT * FROM json_each('[1]')",
                "WITH q AS (SELECT * FROM json_each('[1]')) SELECT * FROM q",
            ] {
                let rows = f.query(sql).await.unwrap();
                assert_eq!(rows.len(), 1);
                assert_eq!(rows[0].values().len(), 10, "{sql}");
                assert_eq!(rows[0].values()[8], SqliteValue::Text("[1]".into()));
                assert_eq!(rows[0].values()[9], SqliteValue::Text("$".into()));
            }
            let rows = f
                .query(
                    "SELECT count(*) FROM json_each('[1]')j NATURAL JOIN \
                 (SELECT 'different' AS json,'other' AS root)x",
                )
                .await
                .unwrap();
            assert_eq!(rows[0].values(), &[SqliteValue::Integer(0)]);
        });
    }
}

/// A join written without a constraint (`FROM t, u WHERE u.id = t.a AND t.a = 2`)
/// is joined by its whole WHERE, so the outer-table equality was never offered
/// to the outer seek and the outer table was scanned (440x stock on 200k rows).
/// An inner join's ON terms that the lookup does not consume filter the outer
/// row as WHERE terms do, so they now seek it too. A LEFT JOIN's ON term does
/// not filter the outer row (it still comes out, null-extended) and must not
/// seek; nor may a term under OR.
#[test]
fn join_lookups_seek_the_outer_table_by_unconsumed_inner_join_terms() {
    // (query, must seek the outer index)
    let cases: &[(&str, bool)] = &[
        ("SELECT t.id, u.id FROM t, u WHERE u.id = t.a AND t.a = 2 ORDER BY t.id", true),
        ("SELECT t.id, u.r FROM t, u WHERE u.id = t.a AND t.b = '2' ORDER BY t.id", true),
        ("SELECT count(*), sum(u.r) FROM t, u WHERE u.id = t.a AND t.a = 2", true),
        ("SELECT t.id, u.id FROM t, u WHERE t.a = 4 AND u.id = t.a ORDER BY t.id", true),
        ("SELECT t.id, u.id FROM t JOIN u ON u.id = t.a AND t.a = 2 ORDER BY t.id", true),
        ("SELECT t.id, u.id FROM t CROSS JOIN u WHERE u.id = t.a AND t.a = 2 ORDER BY t.id", true),
        ("SELECT t.id, u.id FROM t LEFT JOIN u ON u.id = t.a AND t.a = 2 ORDER BY t.id", false),
        (
            "SELECT t.id, u.id FROM t, u WHERE u.id = t.a AND (t.a = 2 OR t.a = 4) ORDER BY t.id",
            false,
        ),
        // bd-qk7vd: text probe `'2'` against INTEGER index `t.a` coerces via
        // Affinity before SeekGE; `t.a + 0 = 2` disables the index seek.
        ("SELECT t.id, u.id FROM t, u WHERE u.id = t.a AND t.a = '2' ORDER BY t.id", true),
        ("SELECT t.id, u.id FROM t, u WHERE u.id = t.a AND t.a + 0 = 2 ORDER BY t.id", false),
        ("SELECT t.id, u.id FROM t, u WHERE u.id = t.a AND t.a = 9 ORDER BY t.id", true),
    ];
    for indexed in [false, true] {
        asupersync::test_utils::run_test(|| async move {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("join_outer_seek_by_on_terms.db");
            let f = Connection::open(path.to_str().unwrap()).await.unwrap();
            let r = rusqlite::Connection::open_in_memory().unwrap();
            let indexes: &[&str] = if indexed { INDEXES } else { &[] };
            for sql in SETUP.iter().chain(indexes) {
                f.execute(sql).await.unwrap();
                r.execute(sql, []).unwrap();
            }
            for (sql, seeks) in cases {
                assert_agree(&f, &r, sql, &[], &[]).await;
                if indexed {
                    let ops = opcodes(&f, sql).await;
                    assert_eq!(
                        ops.iter().any(|op| op == "SeekGE"),
                        *seeks,
                        "`{sql}` outer index seek: {ops:?}"
                    );
                }
            }
            for value in [2_i64, 7] {
                assert_agree(
                    &f,
                    &r,
                    "SELECT t.id, u.id FROM t, u WHERE u.id = t.a AND t.a = ?1 ORDER BY t.id",
                    &[SqliteValue::Integer(value)],
                    &[&value],
                )
                .await;
            }
        });
    }
}

/// bd-9uf3l: an inner join written with its unfiltered table first (`FROM u
/// JOIN t ON u.id = t.a WHERE t.b = 'k2'`) drove from u, reading all of it and
/// looking t up per row (413x stock on 200k rows). It drives from t, the table
/// its WHERE pins, and looks u up, as stock does, when the pinned rows are well
/// under u's rows, nothing seeks u and u has a rowid or index lookup on the
/// join key. The row counts come from sqlite_stat1, or without it (bd-prkvt,
/// 257x stock) from b-tree samples: u's rows, and per index of t its rows per
/// leading value, or SQLite's default of ten when the sampled keys mostly
/// differ. A small written outer table (5-row q) and an unselective pin
/// (`t.f = 1`, half of t, whose index repeats each value across its page)
/// keep the written order either way. So do a LEFT join, a comma join (stock
/// never reorders CROSS JOIN, which the parser does not tell from a comma),
/// `*` (cursor-ordered columns), a written outer table the WHERE already
/// seeks, and a join key with no lookup on the written outer table. Rows match
/// stock in order either way.
#[test]
fn inner_joins_drive_from_the_table_their_where_pins_by_row_estimates() {
    // (query, whether it reads the outer table in full `[without, with]`
    // ANALYZE: `false` drives from the pinned table, `true` keeps the written
    // outer table as a full scan; `None` checks rows only).
    let cases: &[(&str, Option<[bool; 2]>)] = &[
        (
            "SELECT t.id, u.r FROM u JOIN t ON u.id = t.a WHERE t.b = 'k2' ORDER BY t.id",
            Some([false, false]),
        ),
        (
            "SELECT count(*), sum(u.r) FROM u JOIN t ON u.id = t.a WHERE t.a = 2",
            Some([false, false]),
        ),
        (
            "SELECT count(*), max(u.r) FROM u JOIN t ON t.a = u.id WHERE t.b = 'k4'",
            Some([false, false]),
        ),
        (
            "SELECT t.id, u.a FROM u INNER JOIN t ON u.id = t.a AND t.b = 'k5' ORDER BY t.id",
            Some([false, false]),
        ),
        (
            "SELECT count(*) FROM u JOIN t ON u.id = t.a WHERE t.id = 9",
            Some([false, false]),
        ),
        // A range on u is no equality seek, so t still drives.
        (
            "SELECT t.id, u.r FROM u JOIN t ON u.id = t.a WHERE u.r > 1 AND t.b = 'k2' \
             ORDER BY t.id",
            Some([false, false]),
        ),
        // A pin matching half of t, and a 5-row written outer table.
        (
            "SELECT count(*), sum(u.r) FROM u JOIN t ON u.id = t.a WHERE t.f = 1",
            Some([true, true]),
        ),
        (
            "SELECT count(*) FROM q JOIN t ON t.id = q.id WHERE t.f = 1",
            Some([true, true]),
        ),
        (
            "SELECT count(*) FROM q JOIN t ON t.id = q.id WHERE t.b = 'k14'",
            Some([true, true]),
        ),
        (
            "SELECT t.id, u.r FROM u LEFT JOIN t ON u.id = t.a WHERE t.b = 'k2' ORDER BY t.id",
            Some([true, true]),
        ),
        // CROSS JOIN pins join order (full outer scan of u); comma join parses
        // as INNER JOIN (bd-hexz0) and reorders to drive from pinned t.b = 'k2'.
        (
            "SELECT count(*) FROM u CROSS JOIN t WHERE u.id = t.a AND t.b = 'k2'",
            Some([true, true]),
        ),
        (
            "SELECT count(*) FROM u, t WHERE u.id = t.a AND t.b = 'k2'",
            Some([false, false]),
        ),
        (
            "SELECT * FROM u JOIN t ON u.id = t.a WHERE t.b = 'k2' ORDER BY t.id, u.id",
            Some([true, true]),
        ),
        (
            "SELECT t.id FROM u JOIN t ON u.a = t.a WHERE t.b = 'k2' ORDER BY t.id",
            Some([true, true]),
        ),
        // The WHERE already seeks u, so u stays outer.
        (
            "SELECT t.id, u.r FROM u JOIN t ON u.id = t.a WHERE u.id = 2 AND t.b = 'k2' \
             ORDER BY t.id",
            None,
        ),
    ];
    let mut setup = vec![
        "CREATE TABLE u(id INTEGER PRIMARY KEY, a INTEGER, r REAL)".to_owned(),
        "CREATE TABLE t(id INTEGER PRIMARY KEY, a INTEGER, b TEXT, f INTEGER)".to_owned(),
        "CREATE TABLE q(id INTEGER PRIMARY KEY, w INTEGER)".to_owned(),
        "CREATE INDEX t_a ON t(a)".to_owned(),
        "CREATE INDEX t_b ON t(b)".to_owned(),
        "CREATE INDEX t_f ON t(f)".to_owned(),
    ];
    for i in 1..=400_i64 {
        setup.push(format!("INSERT INTO u VALUES ({i}, {}, {}.5)", i % 9, i % 13));
        setup.push(format!(
            "INSERT INTO t VALUES ({i}, {i}, 'k{}', {})",
            i % 100,
            i % 2
        ));
    }
    for i in 1..=5_i64 {
        setup.push(format!("INSERT INTO q VALUES ({}, {i})", i * 7));
    }
    for analyzed in [false, true] {
        let setup = setup.clone();
        asupersync::test_utils::run_test(|| async move {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("join_drives_pinned_table.db");
            let f = Connection::open(path.to_str().unwrap()).await.unwrap();
            let r = rusqlite::Connection::open_in_memory().unwrap();
            for sql in &setup {
                f.execute(sql).await.unwrap();
                r.execute(sql, []).unwrap();
            }
            if analyzed {
                f.execute("ANALYZE").await.unwrap();
                r.execute_batch("ANALYZE").unwrap();
            }
            for (sql, scans_outer) in cases {
                assert_agree(&f, &r, sql, &[], &[]).await;
                if let Some(scans_outer) = scans_outer {
                    let ops = opcodes(&f, sql).await;
                    assert_eq!(
                        ops.iter().any(|op| op == "Rewind"),
                        scans_outer[usize::from(analyzed)],
                        "`{sql}` full outer scan (analyzed: {analyzed}): {ops:?}"
                    );
                }
            }
            for value in [2_i64, 6, 999] {
                assert_agree(
                    &f,
                    &r,
                    "SELECT t.id, u.r FROM u JOIN t ON u.id = t.a WHERE t.a = ?1 ORDER BY t.id",
                    &[SqliteValue::Integer(value)],
                    &[&value],
                )
                .await;
            }
            f.close().await.unwrap();
        });
    }
}

/// bd-w4pr1.12 (lever SL1 v3): a rowid lookup whose target lies on the leaf
/// the cursor already stands on is answered by the b-tree without a wait
/// (`BtCursor::try_table_seek_within_leaf`); everything else takes the
/// general seek. The outer keys step through `u` by a large stride, as
/// join_1M_to_pk_1000's do, so lookups alternate between the current leaf
/// and other leaves. They also land on missing rowids inside a leaf (every
/// 13th row is absent), before the first and past the last row, and key on
/// NULL, REAL, TEXT and BLOB values.
const SAME_LEAF_SETUP: &[&str] = &[
    "CREATE TABLE u(a INTEGER PRIMARY KEY, name TEXT)",
    "WITH RECURSIVE s(v) AS (SELECT 0 UNION ALL SELECT v + 1 FROM s WHERE v < 2999) \
     INSERT INTO u SELECT v, 'name-' || v || '-' || substr('abcdefghijklmnopqrstuvwxyz', 1 + v % 26) \
     FROM s WHERE v % 13 <> 5",
    "CREATE TABLE t(id INTEGER PRIMARY KEY, a)",
    "WITH RECURSIVE s(v) AS (SELECT 1 UNION ALL SELECT v + 1 FROM s WHERE v < 6000) \
     INSERT INTO t SELECT v, (v * 7919) % 3000 FROM s",
    "INSERT INTO t VALUES (6001, NULL), (6002, -1), (6003, 3000), (6004, 99999), (6005, 17.0), \
     (6006, 17.5), (6007, '18'), (6008, ' 19 '), (6009, x'3230'), (6010, 2999), (6011, 5), (6012, 0)",
];

const SAME_LEAF_QUERIES: &[&str] = &[
    "SELECT count(*), sum(length(u.name)) FROM t JOIN u ON u.a = t.a",
    "SELECT count(*), sum(t.id), sum(length(u.name)) FROM t JOIN u ON u.a = t.a WHERE t.id > 5980",
    "SELECT count(*), sum(t.id), sum(u.a) FROM t, u WHERE u.rowid = t.a",
    "SELECT count(*), count(u.a), sum(t.id) FROM t LEFT JOIN u ON u.a = t.a",
    "SELECT t.id, u.a, u.name FROM t LEFT JOIN u ON u.a = t.a WHERE t.id > 5995 ORDER BY t.id",
    "SELECT t.id, (SELECT name FROM u WHERE u.a = t.a) FROM t WHERE t.id > 5990 ORDER BY t.id",
    "SELECT sum(length(x)) FROM (SELECT (SELECT name FROM u WHERE u.a = t.a) x FROM t)",
];

/// The negative cases write `u` and then seek it: inside one transaction
/// after an UPDATE dirtied its leaves, and inside one UPDATE whose subquery
/// reads the row that statement changed just before, on a leaf it has
/// dirtied. A fast path that skipped its dirty-leaf check would answer those
/// from a stale page. Compared with rusqlite, file-backed and in memory, ad
/// hoc and prepared.
#[test]
fn same_leaf_rowid_lookups_match_sqlite_across_leaves_and_after_writes() {
    for file_backed in [true, false] {
        asupersync::test_utils::run_test(|| async move {
            let dir = tempfile::tempdir().unwrap();
            let path = if file_backed {
                dir.path()
                    .join("same_leaf.db")
                    .to_string_lossy()
                    .into_owned()
            } else {
                ":memory:".to_owned()
            };
            let f = Connection::open(&path).await.unwrap();
            let r = rusqlite::Connection::open_in_memory().unwrap();
            for sql in SAME_LEAF_SETUP {
                f.execute(sql).await.unwrap();
                r.execute(sql, []).unwrap();
            }
            for sql in SAME_LEAF_QUERIES {
                assert_agree(&f, &r, sql, &[], &[]).await;
            }
            let bounded =
                "SELECT count(*), sum(length(u.name)) FROM t JOIN u ON u.a = t.a WHERE t.id <= ?1";
            let stmt = f.prepare(bounded).await.unwrap();
            for bound in [1_i64, 2, 81, 4000, 6012] {
                let ours: Vec<Vec<String>> = stmt
                    .query_with_params(&[SqliteValue::Integer(bound)])
                    .await
                    .unwrap()
                    .iter()
                    .map(|row| row.values().iter().map(tag_f).collect())
                    .collect();
                let theirs: Vec<Vec<String>> = r
                    .prepare(bounded)
                    .unwrap()
                    .query_map([bound], |row| {
                        Ok((0..2)
                            .map(|i| tag_r(&row.get::<_, rusqlite::types::Value>(i).unwrap()))
                            .collect())
                    })
                    .unwrap()
                    .map(Result::unwrap)
                    .collect();
                assert_eq!(
                    ours, theirs,
                    "prepared bound {bound}, file-backed {file_backed}"
                );
            }

            // Leaves this transaction has written.
            for sql in ["BEGIN", "UPDATE u SET name = name || '!' WHERE a % 50 = 0"] {
                f.execute(sql).await.unwrap();
                r.execute(sql, []).unwrap();
            }
            assert_agree(&f, &r, SAME_LEAF_QUERIES[0], &[], &[]).await;
            assert_agree(&f, &r, SAME_LEAF_QUERIES[5], &[], &[]).await;
            f.execute("COMMIT").await.unwrap();
            r.execute("COMMIT", []).unwrap();

            // One statement writes a leaf and then seeks it: row k's subquery
            // reads row k - 1, which this UPDATE has just changed.
            let chain = "UPDATE u SET name = coalesce((SELECT p.name FROM u AS p WHERE p.a = u.a - 1), \
                         'none') || '.' WHERE u.a % 3 <> 0";
            f.execute(chain).await.unwrap();
            r.execute(chain, []).unwrap();
            for sql in [
                "SELECT count(*), sum(length(name)), max(name), min(name) FROM u",
                "SELECT a, name FROM u WHERE a < 12 OR a > 2990 ORDER BY a",
                SAME_LEAF_QUERIES[0],
            ] {
                assert_agree(&f, &r, sql, &[], &[]).await;
            }

            // Non-vacuity: the join looks u up by rowid.
            let ops = opcodes(&f, SAME_LEAF_QUERIES[0]).await;
            assert!(
                ops.iter().any(|op| op == "SeekRowid"),
                "the join must look u up by rowid: {ops:?}"
            );
            f.close().await.unwrap();
        });
    }
}

/// bd-knibh: aggregates (and plain selects) over 3+ table joins whose middle
/// or trailing lookup uses a non-unique secondary index (single-column or
/// leading composite prefix) must compile directly to VDBE lookup-loop
/// bytecode (EXPLAIN works, zero fallback) and match bundled SQLite 3.54.1 on
/// both `:memory:` and file-backed databases, ad hoc and prepared.
#[test]
fn bd_knibh_multi_join_non_unique_index_aggregates_match_sqlite() {
    const SETUP_SQL: &str = "
        CREATE TABLE a(id INTEGER PRIMARY KEY, code TEXT, grp INTEGER, w REAL);
        CREATE TABLE b(id INTEGER PRIMARY KEY, a_id INTEGER, c_id INTEGER, tag TEXT, flag INTEGER);
        CREATE TABLE c(id INTEGER PRIMARY KEY, b_tag TEXT, val REAL, label TEXT);
        CREATE TABLE d(id INTEGER PRIMARY KEY, c_id INTEGER, bonus REAL);
        CREATE TABLE b_comp(id INTEGER PRIMARY KEY, a_id INTEGER, c_id INTEGER, extra INTEGER);
        CREATE TABLE empty_a(id INTEGER PRIMARY KEY, grp INTEGER);
        CREATE INDEX idx_b_a_id ON b(a_id);
        CREATE INDEX idx_c_b_tag ON c(b_tag);
        CREATE INDEX idx_d_c_id ON d(c_id);
        CREATE INDEX idx_b_comp_ac ON b_comp(a_id, c_id);
        INSERT INTO a VALUES
            (1,'1',10,1.5),(2,'2',20,2.5),(3,'3',10,NULL),(4,'4',30,-4.0),
            (5,'999',20,0.5),(6,'x',10,3.0);
        INSERT INTO b VALUES
            (10,1,100,'k1',1),(11,1,101,'k1',0),(12,1,999,'missing',1),
            (13,2,101,'k2',1),(14,2,NULL,'k1',1),(15,2,102,'k2',0),
            (16,4,100,'k3',1),(17,4,100,'k3',1),(18,NULL,100,'k1',1);
        INSERT INTO c VALUES
            (100,'k1',10.0,'alpha'),(101,'k1',20.0,'beta'),(102,'k2',NULL,'gamma'),
            (103,'k2',-5.0,'delta'),(104,'k3',7.5,'epsilon');
        INSERT INTO d VALUES
            (1,100,1.0),(2,100,2.0),(3,101,3.5),(4,104,-1.0);
        INSERT INTO b_comp VALUES
            (1,1,100,5),(2,1,101,6),(3,2,102,7),(4,2,100,8),(5,4,999,9);
    ";

    // Shapes that MUST compile to multi-join lookup bytecode (EXPLAIN works,
    // single outer Rewind, AggStep + AggFinal, zero SorterInsert).
    let compiled_aggregate_queries: &[&str] = &[
        // Core bd-knibh shape: 3-table join with non-unique secondary index on middle table + PK leaf.
        "SELECT count(*), sum(c.val), total(c.val), avg(c.val), min(c.val), max(c.val) \
         FROM a JOIN b ON b.a_id = a.id JOIN c ON c.id = b.c_id",
        // Count-only: trailing index lookup can even avoid table SeekRowid when unreferenced.
        "SELECT count(*) FROM a JOIN b ON b.a_id = a.id JOIN c ON c.b_tag = b.tag",
        // Non-unique secondary index on BOTH middle AND leaf tables.
        "SELECT count(*), sum(c.val), sum(a.w), min(c.label), max(c.label) \
         FROM a JOIN b ON b.a_id = a.id JOIN c ON c.b_tag = b.tag",
        // Composite leading-column non-unique index on middle table.
        "SELECT count(*), sum(c.val), sum(b_comp.extra) \
         FROM a JOIN b_comp ON b_comp.a_id = a.id JOIN c ON c.id = b_comp.c_id",
        // Non-unique middle index + terminal LEFT JOIN (PK leaf).
        "SELECT count(*), count(c.id), sum(c.val), sum(a.w) \
         FROM a JOIN b ON b.a_id = a.id LEFT JOIN c ON c.id = b.c_id",
        // Non-unique middle index + terminal LEFT JOIN (non-unique indexed leaf!).
        "SELECT count(*), count(c.id), sum(c.val) \
         FROM a JOIN b ON b.a_id = a.id LEFT JOIN c ON c.b_tag = b.tag",
        // Terminal LEFT JOIN with ON residual + WHERE filtering NULL matches.
        "SELECT count(*), count(c.id), sum(c.val) \
         FROM a JOIN b ON b.a_id = a.id AND b.flag = 1 LEFT JOIN c ON c.id = b.c_id AND c.val > 10",
        "SELECT count(*), sum(b.id) \
         FROM a JOIN b ON b.a_id = a.id LEFT JOIN c ON c.id = b.c_id WHERE c.id IS NULL",
        // Cross-affinity coercion (TEXT probe a.code into INTEGER index b.a_id).
        "SELECT count(*), sum(c.val) \
         FROM a JOIN b ON b.a_id = a.code JOIN c ON c.id = b.c_id",
        // WHERE filters at table 0, table 1, and table 2.
        "SELECT count(*), sum(c.val), sum(a.w * c.val) \
         FROM a JOIN b ON b.a_id = a.id JOIN c ON c.id = b.c_id \
         WHERE a.grp = 10 AND b.flag = 1 AND c.val >= 10",
        // 4-table chain: non-unique middle + PK + non-unique terminal LEFT JOIN.
        "SELECT count(*), count(d.id), sum(d.bonus), sum(c.val) \
         FROM a JOIN b ON b.a_id = a.id JOIN c ON c.id = b.c_id LEFT JOIN d ON d.c_id = c.id",
        // Comma-joined 3-table aggregate.
        "SELECT count(*), sum(c.val) FROM a, b, c WHERE b.a_id = a.id AND c.id = b.c_id",
        "SELECT count(*), sum(c.val) FROM a, b, c WHERE c.id = b.c_id AND b.a_id = a.id AND b.flag = 1",
        // Empty driver table.
        "SELECT count(*), count(c.id), sum(c.val) \
         FROM empty_a JOIN b ON b.a_id = empty_a.id JOIN c ON c.id = b.c_id",
        "SELECT count(*), count(c.id), sum(c.val) \
         FROM empty_a JOIN b ON b.a_id = empty_a.id LEFT JOIN c ON c.id = b.c_id",
        // Exact join3_secidx_30k shape with driver rowid upper bound (`WHERE a.id <= ...`).
        "SELECT count(*), sum(c.val) \
         FROM a JOIN b ON b.a_id = a.id JOIN c ON c.id = b.c_id WHERE a.id <= 3",
        "SELECT count(*), sum(c.val) \
         FROM a JOIN b ON b.a_id = a.id JOIN c ON c.id = b.c_id WHERE a.id <= 3 AND b.flag = 1",
    ];

    // Driver rowid lower-bounded range queries (use SeekGE instead of Rewind on driver).
    let driver_range_aggregate_queries: &[&str] = &[
        "SELECT count(*), sum(c.val) \
         FROM a JOIN b ON b.a_id = a.id JOIN c ON c.id = b.c_id WHERE a.id BETWEEN 2 AND 4",
        "SELECT count(*), sum(c.val) \
         FROM a JOIN b ON b.a_id = a.id JOIN c ON c.id = b.c_id WHERE a.id = 2",
        "SELECT count(*), sum(c.val) \
         FROM a JOIN b ON b.a_id = a.id LEFT JOIN c ON c.id = b.c_id WHERE a.id > 1 AND a.id < 5",
    ];

    // Non-aggregate multi-join selects with non-unique indexes (with ORDER BY / LIMIT).
    let non_aggregate_queries: &[&str] = &[
        "SELECT a.id, b.id, c.id, c.val FROM a JOIN b ON b.a_id = a.id JOIN c ON c.id = b.c_id \
         ORDER BY a.id, b.id, c.id",
        "SELECT a.id, b.id, c.id FROM a JOIN b ON b.a_id = a.id LEFT JOIN c ON c.b_tag = b.tag \
         ORDER BY a.id, b.id, c.id",
        "SELECT a.id, b.id, c.id FROM a JOIN b ON b.a_id = a.id JOIN c ON c.id = b.c_id \
         WHERE a.grp = 10 ORDER BY a.id, b.id LIMIT 2 OFFSET 1",
    ];

    // Negative controls that MUST remain accurate through fallback.
    let fallback_control_queries: &[&str] = &[
        "SELECT count(*), count(c.id), sum(c.val) \
         FROM a LEFT JOIN b ON b.a_id = a.id JOIN c ON c.id = b.c_id",
        "SELECT count(DISTINCT c.id), sum(c.val) \
         FROM a JOIN b ON b.a_id = a.id JOIN c ON c.id = b.c_id",
        "SELECT count(*) FROM a JOIN b ON b.a_id = a.id OR b.id = a.id JOIN c ON c.id = b.c_id",
    ];

    for file_backed in [false, true] {
        asupersync::test_utils::run_test(|| async move {
            let dir = tempfile::tempdir().unwrap();
            let path = if file_backed {
                dir.path()
                    .join("bd_knibh_multi_join.db")
                    .to_string_lossy()
                    .into_owned()
            } else {
                ":memory:".to_owned()
            };
            let f = Connection::open(&path).await.unwrap();
            let r = rusqlite::Connection::open_in_memory().unwrap();
            f.execute_batch(SETUP_SQL).await.unwrap();
            r.execute_batch(SETUP_SQL).unwrap();

            for &sql in compiled_aggregate_queries {
                assert_agree(&f, &r, sql, &[], &[]).await;
                // Prepared parity too.
                let prep = f.prepare(sql).await.unwrap();
                let prep_rows: Vec<Vec<String>> = prep
                    .query()
                    .await
                    .unwrap()
                    .iter()
                    .map(|row| row.values().iter().map(tag_f).collect())
                    .collect();
                let direct_rows: Vec<Vec<String>> = f
                    .query(sql)
                    .await
                    .unwrap()
                    .iter()
                    .map(|row| row.values().iter().map(tag_f).collect())
                    .collect();
                assert_eq!(prep_rows, direct_rows, "prepared vs direct on `{sql}`");

                // EXPLAIN must succeed and show the lookup-loop bytecode program!
                let ops = opcodes(&f, sql).await;
                assert_eq!(
                    ops.iter().filter(|op| *op == "Rewind").count(),
                    1,
                    "file={file_backed} `{sql}` expected 1 outer Rewind: {ops:?}"
                );
                assert!(
                    ops.iter().any(|op| op == "SeekGE"),
                    "file={file_backed} `{sql}` expected SeekGE: {ops:?}"
                );
                assert!(
                    ops.iter().any(|op| op == "AggStep") && ops.iter().any(|op| op == "AggFinal"),
                    "file={file_backed} `{sql}` expected AggStep+AggFinal: {ops:?}"
                );
                assert!(
                    !ops.iter().any(|op| op == "SorterInsert"),
                    "file={file_backed} `{sql}` unexpected SorterInsert: {ops:?}"
                );
            }

            // Verify driver rowid-range seek queries replace outer Rewind with SeekGE + upper exit.
            for &sql in driver_range_aggregate_queries {
                assert_agree(&f, &r, sql, &[], &[]).await;
                let ops = opcodes(&f, sql).await;
                assert_eq!(
                    ops.iter().filter(|op| *op == "Rewind").count(),
                    0,
                    "file={file_backed} `{sql}` expected SeekGE instead of Rewind: {ops:?}"
                );
                assert!(
                    ops.iter().filter(|op| *op == "SeekGE" || *op == "SeekGT").count() >= 2,
                    "file={file_backed} `{sql}` expected driver + index seeks: {ops:?}"
                );
                assert!(
                    ops.iter().any(|op| op == "Gt" || op == "Ge"),
                    "file={file_backed} `{sql}` expected driver rowid upper-bound exit: {ops:?}"
                );
                assert!(
                    ops.iter().any(|op| op == "AggStep") && ops.iter().any(|op| op == "AggFinal"),
                    "file={file_backed} `{sql}` expected AggStep+AggFinal: {ops:?}"
                );
            }

            // Anonymous `?` placeholders split across terminal LEFT JOIN ON residual and WHERE.
            let anon_left_sql = "SELECT count(*), count(c.id), sum(c.val) \
                                 FROM a JOIN b ON b.a_id = a.id \
                                 LEFT JOIN c ON c.id = b.c_id AND c.val > ? \
                                 WHERE a.id <= 4 AND a.grp >= ?";
            for (v, g) in [(10.0_f64, 10_i64), (50.0, 10), (0.0, 20)] {
                let fp = [SqliteValue::Float(v), SqliteValue::Integer(g)];
                assert_agree(&f, &r, anon_left_sql, &fp, &[&v, &g]).await;
            }

            // Verify zero fallback decisions on `:memory:` for the core shape!
            if !file_backed {
                f.set_fallback_capture_enabled(true);
                f.reset_fallback_decision_evidence();
                f.query(compiled_aggregate_queries[0]).await.unwrap();
                let fell_back = f
                    .fallback_decision_snapshot()
                    .decisions
                    .iter()
                    .any(|d| d.decision_reason == "group_by_join_fallback");
                assert!(
                    !fell_back,
                    "core 3-table non-unique index join aggregate must not fall back on :memory:"
                );
                f.set_fallback_capture_enabled(false);
            }

            // Parameterized query across both direct and prepared.
            let param_sql = "SELECT count(*), sum(c.val) \
                             FROM a JOIN b ON b.a_id = a.id LEFT JOIN c ON c.id = b.c_id \
                             WHERE a.grp >= ?1 AND (c.val IS NULL OR c.val >= ?2)";
            let prep = f.prepare(param_sql).await.unwrap();
            for (g, v) in [(10_i64, 0.0_f64), (20, 10.0), (99, 0.0)] {
                let fp = [SqliteValue::Integer(g), SqliteValue::Float(v)];
                assert_agree(&f, &r, param_sql, &fp, &[&g, &v]).await;
                let pr: Vec<Vec<String>> = prep
                    .query_with_params(&fp)
                    .await
                    .unwrap()
                    .iter()
                    .map(|row| row.values().iter().map(tag_f).collect())
                    .collect();
                let dr: Vec<Vec<String>> = f
                    .query_with_params(param_sql, &fp)
                    .await
                    .unwrap()
                    .iter()
                    .map(|row| row.values().iter().map(tag_f).collect())
                    .collect();
                assert_eq!(pr, dr, "param ({g}, {v}) prepared vs direct");
            }
            drop(prep);

            for &sql in non_aggregate_queries {
                assert_agree(&f, &r, sql, &[], &[]).await;
            }
            for &sql in fallback_control_queries {
                assert_agree(&f, &r, sql, &[], &[]).await;
            }
            f.close().await.unwrap();
        });
    }
}

/// bd-knibh live A/B benchmark harness: 30,000-row 3-table join with a
/// non-unique secondary index on the middle table (`join3_secidx_30k`),
/// compared directly against bundled SQLite 3.54.1 in the same invocation.
#[test]
#[ignore = "live release-perf A/B benchmark for bd-knibh (join3_secidx_30k)"]
fn bd_knibh_join3_secidx_30k_live_vs_sqlite() {
    use std::time::Instant;

    const SQL_FULL: &str = "SELECT count(*), sum(c.val) \
                            FROM a JOIN b ON b.a_id = a.id JOIN c ON c.id = b.c_id";
    const SQL_BOUNDED: &str = "SELECT count(*), sum(pr.price) \
                               FROM cust c JOIN ord o ON o.cid = c.id JOIN prod pr ON pr.id = o.pid \
                               WHERE c.id <= 2000";
    const SETUP_SQL: &[&str] = &[
        "CREATE TABLE a(id INTEGER PRIMARY KEY, grp INTEGER)",
        "CREATE TABLE b(id INTEGER PRIMARY KEY, a_id INTEGER, c_id INTEGER)",
        "CREATE TABLE c(id INTEGER PRIMARY KEY, val INTEGER)",
        "WITH RECURSIVE s(v) AS (SELECT 1 UNION ALL SELECT v + 1 FROM s WHERE v < 10000) \
         INSERT INTO a SELECT v, v % 100 FROM s",
        "WITH RECURSIVE s(v) AS (SELECT 1 UNION ALL SELECT v + 1 FROM s WHERE v < 30000) \
         INSERT INTO b SELECT v, 1 + ((v - 1) % 10000), 1 + ((v * 7) % 10000) FROM s",
        "WITH RECURSIVE s(v) AS (SELECT 1 UNION ALL SELECT v + 1 FROM s WHERE v < 10000) \
         INSERT INTO c SELECT v, (v * 13) % 1000 FROM s",
        "CREATE INDEX idx_b_a_id ON b(a_id)",
        // Exact bd-w4pr1.13.18 join3_secidx_30k shape: large driving table c with
        // `WHERE c.id <= 2000` joining 30k matching rows through non-unique `o_cid`.
        "CREATE TABLE cust(id INTEGER PRIMARY KEY, tier INTEGER)",
        "CREATE TABLE ord(id INTEGER PRIMARY KEY, cid INTEGER, pid INTEGER)",
        "CREATE TABLE prod(id INTEGER PRIMARY KEY, price INTEGER)",
        "WITH RECURSIVE s(v) AS (SELECT 1 UNION ALL SELECT v + 1 FROM s WHERE v < 20000) \
         INSERT INTO cust SELECT v, v % 10 FROM s",
        "WITH RECURSIVE s(v) AS (SELECT 1 UNION ALL SELECT v + 1 FROM s WHERE v < 300000) \
         INSERT INTO ord SELECT v, 1 + ((v - 1) % 20000), 1 + ((v * 7) % 10000) FROM s",
        "WITH RECURSIVE s(v) AS (SELECT 1 UNION ALL SELECT v + 1 FROM s WHERE v < 10000) \
         INSERT INTO prod SELECT v, (v * 13) % 1000 FROM s",
        "CREATE INDEX o_cid ON ord(cid)",
    ];

    asupersync::test_utils::run_test(|| async {
        for file_backed in [false, true] {
            let dir = tempfile::tempdir().unwrap();
            let path = if file_backed {
                dir.path()
                    .join("join3_secidx_30k.db")
                    .to_string_lossy()
                    .into_owned()
            } else {
                ":memory:".to_owned()
            };
            let f = Connection::open(&path).await.unwrap();
            let r = if file_backed {
                rusqlite::Connection::open(dir.path().join("sqlite_join3.db")).unwrap()
            } else {
                rusqlite::Connection::open_in_memory().unwrap()
            };
            for &stmt in SETUP_SQL {
                f.execute(stmt).await.unwrap();
                r.execute_batch(stmt).unwrap();
            }

            for (label, sql) in [("full_30k", SQL_FULL), ("join3_secidx_30k_bounded", SQL_BOUNDED)] {
                // Prove EXPLAIN compiles cleanly and emits lookup-loop bytecode.
                let ops = opcodes(&f, sql).await;
                assert_eq!(ops.iter().filter(|op| *op == "Rewind").count(), 1, "{label}: {ops:?}");
                assert!(ops.iter().any(|op| op == "SeekGE"), "{label}: {ops:?}");
                assert!(ops.iter().any(|op| op == "AggStep"), "{label}: {ops:?}");

                // Parity check before timing.
                assert_agree(&f, &r, sql, &[], &[]).await;

                let f_stmt = f.prepare(sql).await.unwrap();
                let mut r_stmt = r.prepare(sql).unwrap();

                // Warmup.
                for _ in 0..3 {
                    let _ = f_stmt.query().await.unwrap();
                    let _ = r_stmt
                        .query_row([], |row| Ok((row.get::<_, i64>(0)?, row.get::<_, i64>(1)?)))
                        .unwrap();
                }

                let iters = 15_usize;
                let mut f_times = Vec::with_capacity(iters);
                let mut r_times = Vec::with_capacity(iters);
                for _ in 0..iters {
                    let t0 = Instant::now();
                    let rows = f_stmt.query().await.unwrap();
                    f_times.push(t0.elapsed().as_nanos());
                    assert_eq!(rows.len(), 1);

                    let t1 = Instant::now();
                    let _ = r_stmt
                        .query_row([], |row| Ok((row.get::<_, i64>(0)?, row.get::<_, i64>(1)?)))
                        .unwrap();
                    r_times.push(t1.elapsed().as_nanos());
                }
                f_times.sort_unstable();
                r_times.sort_unstable();
                let f_med = f_times[iters / 2] as f64 / 1_000_000.0;
                let r_med = r_times[iters / 2] as f64 / 1_000_000.0;
                eprintln!(
                    "{label} file_backed={file_backed}: fsqlite={f_med:.3}ms rusqlite={r_med:.3}ms ratio={:.2}x",
                    f_med / r_med
                );
            }
            f.close().await.unwrap();
        }
    });
}

/// bd-obpoh: a lookup join whose outer loop seeks an index skips `OpenRead`
/// and `SeekRowid` on the outer table whenever the outer index (plus its
/// rowid) covers every outer column read by the statement, and evaluates
/// outer-only WHERE terms once per outer row rather than inside the inner
/// loop. Uncovered outer columns, REAL-affinity outer columns, and virtual
/// generated outer columns fail closed to `SeekRowid`.
#[test]
fn bd_obpoh_covering_outer_index_join_oracle() {
    let has_open_cursor =
        |ops: &[(i64, String, i64, i64)], cursor: i64| ops.iter().any(|(_, op, p1, _)| op == "OpenRead" && *p1 == cursor);
    let has_seek_rowid_cursor = |ops: &[(i64, String, i64, i64)], cursor: i64| {
        ops.iter()
            .any(|(_, op, p1, _)| op == "SeekRowid" && *p1 == cursor)
    };
    let inner_loop_has_comparison = |ops: &[(i64, String, i64, i64)]| {
        let Some(inner_seek) = ops.iter().rposition(|(_, op, p1, _)| op == "SeekGE" && *p1 == 2)
        else {
            return false;
        };
        let Some(inner_next) = ops[inner_seek..]
            .iter()
            .position(|(_, op, p1, _)| op == "Next" && *p1 == 2)
            .map(|rel| inner_seek + rel)
        else {
            return false;
        };
        // Skip the single `Ne` key-termination guard right after `Column 2, 0`.
        ops[inner_seek..inner_next]
            .iter()
            .any(|(_, op, _, _)| op == "Eq" || op == "IfNot")
    };

    // Unanalyzed `:memory:` with outer `t` written first still covers outer `t`.
    asupersync::test_utils::run_test(|| async move {
        let f = Connection::open(":memory:").await.unwrap();
        let r = rusqlite::Connection::open_in_memory().unwrap();
        for sql in [
            "CREATE TABLE t(id INTEGER PRIMARY KEY, a INTEGER, pad TEXT)",
            "CREATE TABLE p(id INTEGER PRIMARY KEY, tid INTEGER)",
            "CREATE INDEX t_a ON t(a)",
            "CREATE INDEX p_tid ON p(tid)",
            "INSERT INTO t VALUES (1, 5, 'x'), (2, 5, 'y'), (3, 7, 'z')",
            "INSERT INTO p VALUES (10, 1), (11, 1), (12, 3)",
        ] {
            f.execute(sql).await.unwrap();
            r.execute(sql, []).unwrap();
        }
        let sql = "SELECT count(*) FROM t JOIN p ON p.tid = t.id WHERE t.a = 5";
        assert_agree(&f, &r, sql, &[], &[]).await;
        let ops = program(&f, sql).await;
        assert!(
            !has_open_cursor(&ops, 0)
                && !has_open_cursor(&ops, 1)
                && !ops.iter().any(|(_, op, _, _)| op == "SeekRowid"),
            "unanalyzed :memory: `t JOIN p` must be double-covering: {ops:?}"
        );
        f.close().await.unwrap();
    });

    for (analyzed, file_backed) in [(true, false), (false, true), (true, true)] {
        asupersync::test_utils::run_test(|| async move {
            let dir = tempfile::tempdir().unwrap();
            let target = if file_backed {
                dir.path()
                    .join("bd_obpoh_covering_outer.db")
                    .to_string_lossy()
                    .into_owned()
            } else {
                ":memory:".to_owned()
            };
            let f = Connection::open(&target).await.unwrap();
            let r = rusqlite::Connection::open_in_memory().unwrap();

            let mut setup = vec![
                "CREATE TABLE t(id INTEGER PRIMARY KEY, a INTEGER, b INTEGER, pad TEXT)".to_owned(),
                "CREATE TABLE p(id INTEGER PRIMARY KEY, tid INTEGER, val INTEGER)".to_owned(),
                "CREATE INDEX t_a ON t(a)".to_owned(),
                "CREATE INDEX p_tid ON p(tid)".to_owned(),
                "CREATE TABLE t2(id INTEGER PRIMARY KEY, a INTEGER, b INTEGER, pad TEXT)".to_owned(),
                "CREATE INDEX t2_a ON t2(a)".to_owned(),
                "CREATE INDEX t2_ab ON t2(a, b)".to_owned(),
                "CREATE TABLE small_cov(id INTEGER PRIMARY KEY, a INTEGER, b TEXT)".to_owned(),
                "CREATE INDEX sc_ab ON small_cov(a, b)".to_owned(),
                "CREATE TABLE rt(id INTEGER PRIMARY KEY, a INTEGER, r REAL)".to_owned(),
                "CREATE INDEX rt_r ON rt(r)".to_owned(),
                "CREATE INDEX rt_ar ON rt(a, r)".to_owned(),
                "CREATE TABLE vt(id INTEGER PRIMARY KEY, a INTEGER, \
                 g INTEGER GENERATED ALWAYS AS (a * 10 + id) VIRTUAL)"
                    .to_owned(),
                "CREATE INDEX vt_a ON vt(a)".to_owned(),
                "CREATE TABLE o(id INTEGER PRIMARY KEY, k INTEGER, s TEXT, \
                 n TEXT COLLATE NOCASE, u, pad TEXT)"
                    .to_owned(),
                "CREATE INDEX o_k ON o(k)".to_owned(),
                "CREATE INDEX o_s ON o(s)".to_owned(),
                "CREATE INDEX o_n ON o(n)".to_owned(),
                "CREATE INDEX o_u ON o(u)".to_owned(),
            ];
            for i in 1..=200_i64 {
                setup.push(format!(
                    "INSERT INTO t VALUES ({i}, {}, {}, 'pad-{i}')",
                    i % 20,
                    i % 7
                ));
                setup.push(format!(
                    "INSERT INTO t2 VALUES ({i}, {}, {}, 'pad2-{i}')",
                    i % 20,
                    i % 7
                ));
            }
            for i in 1..=400_i64 {
                // Leave some t rows unmatched for LEFT JOIN / anti-join coverage.
                let tid = if i % 5 == 0 {
                    9999
                } else {
                    1 + ((i * 3) % 180)
                };
                setup.push(format!("INSERT INTO p VALUES ({i}, {tid}, {})", i % 25));
            }
            for i in 1..=40_i64 {
                setup.push(format!(
                    "INSERT INTO small_cov VALUES ({i}, {}, 's{}')",
                    i % 5,
                    i % 3
                ));
                setup.push(format!(
                    "INSERT INTO rt VALUES ({i}, {}, {}.0)",
                    i % 5,
                    i % 7
                ));
                setup.push(format!("INSERT INTO vt VALUES ({i}, {})", i % 5));
            }
            setup.push(
                "INSERT INTO o VALUES \
                 (1, 2, '1', 'Abc', 5, 'p1'), \
                 (2, '2', 'a', 'abc', '5', 'p2'), \
                 (3, 'x', 1, 'ABC', 5.0, 'p3'), \
                 (4, NULL, NULL, NULL, NULL, 'p4'), \
                 (5, 2.5, 'A', 'abd', x'05', 'p5'), \
                 (6, 7, 'a', 'Abc', 'x', 'p6'), \
                 (7, 2, '1', 'aBc', 5, 'p7')"
                    .to_owned(),
            );
            for sql in &setup {
                f.execute(sql).await.unwrap();
                r.execute(sql, []).unwrap();
            }
            if analyzed {
                f.execute("ANALYZE").await.unwrap();
                r.execute_batch("ANALYZE").unwrap();
            }

            // 1. Exact bd-obpoh target: count(*) over p JOIN t with t.a = 5.
            //    Both outer (t via t_a) and inner (p via p_tid) are covering:
            //    no OpenRead on cursor 0 or 1, no SeekRowid at all, and no
            //    redundant Eq/IfNot inside the inner p_tid loop.
            let target_sql = "SELECT count(*) FROM p JOIN t ON t.id = p.tid WHERE t.a = 5";
            assert_agree(&f, &r, target_sql, &[], &[]).await;
            let ops = program(&f, target_sql).await;
            assert!(
                !has_open_cursor(&ops, 0),
                "analyzed={analyzed} file={file_backed} target must skip OpenRead 0: {ops:?}"
            );
            assert!(
                !has_open_cursor(&ops, 1),
                "analyzed={analyzed} file={file_backed} target must skip OpenRead 1: {ops:?}"
            );
            assert!(
                !ops.iter().any(|(_, op, _, _)| op == "SeekRowid"),
                "analyzed={analyzed} file={file_backed} target must emit zero SeekRowid: {ops:?}"
            );
            assert!(
                !inner_loop_has_comparison(&ops),
                "analyzed={analyzed} file={file_backed} target must not re-evaluate outer WHERE inside inner loop: {ops:?}"
            );

            // 2. Covered outer queries (aggregate, row projection, ORDER BY,
            //    LIMIT/OFFSET, alias ORDER BY, composite covering preference,
            //    covered SELECT *, LEFT JOIN, anti-join, LEFT JOIN ON residual).
            let covered_outer_queries = [
                "SELECT count(*), sum(t.id), sum(t.a) FROM p JOIN t ON t.id = p.tid WHERE t.a = 5",
                "SELECT t.id, t.a, p.id FROM p JOIN t ON t.id = p.tid WHERE t.a = 5 ORDER BY t.id, p.id",
                "SELECT t.id, t.a, p.val FROM t JOIN p ON p.tid = t.id WHERE t.a = 5 AND p.val > 10 ORDER BY t.id, p.id",
                "SELECT t.rowid, t.a, p.val FROM t JOIN p ON p.tid = t.id WHERE t.a = 5 ORDER BY t.rowid, p.id LIMIT 5 OFFSET 1",
                "SELECT t.a AS ka, p.val FROM t JOIN p ON p.tid = t.id WHERE t.a = 5 ORDER BY ka, p.val, p.id",
                // Composite covering index preference: t2_ab(a, b) covers t2.b over t2_a(a).
                "SELECT count(*), sum(t2.b) FROM p JOIN t2 ON t2.id = p.tid WHERE t2.a = 5",
                "SELECT t2.id, t2.a, t2.b, p.val FROM t2 JOIN p ON p.tid = t2.id WHERE t2.a = 5 AND t2.b >= 3 ORDER BY t2.id, p.id",
                // Full-table-covered SELECT * / table.* via sc_ab(a, b) + IPK id.
                "SELECT * FROM small_cov JOIN p ON p.tid = small_cov.id WHERE small_cov.a = 2 ORDER BY small_cov.id, p.id",
                "SELECT small_cov.*, p.val FROM small_cov JOIN p ON p.tid = small_cov.id WHERE small_cov.a = 2 ORDER BY small_cov.id, p.id",
                // Covered LEFT JOIN shapes (matched, unmatched, anti-join, ON residual).
                "SELECT count(*), count(p.id), sum(t.id) FROM t LEFT JOIN p ON p.tid = t.id WHERE t.a = 5",
                "SELECT t.id, t.a, p.id FROM t LEFT JOIN p ON p.tid = t.id WHERE t.a = 5 ORDER BY t.id, p.id",
                "SELECT t.id, t.a FROM t LEFT JOIN p ON p.tid = t.id WHERE t.a = 5 AND p.id IS NULL ORDER BY t.id",
                "SELECT t2.id, t2.b, p.id FROM t2 LEFT JOIN p ON p.tid = t2.id AND t2.b > 3 WHERE t2.a = 5 ORDER BY t2.id, p.id",
                "SELECT t2.id, t2.b, p.id FROM t2 LEFT JOIN p ON p.tid = t2.id AND p.val > 12 WHERE t2.a = 5 AND t2.b >= 2 ORDER BY t2.id, p.id",
            ];
            for sql in covered_outer_queries {
                assert_agree(&f, &r, sql, &[], &[]).await;
                let ops = program(&f, sql).await;
                assert!(
                    !has_open_cursor(&ops, 0) && !has_seek_rowid_cursor(&ops, 0),
                    "analyzed={analyzed} file={file_backed} `{sql}` expected covering outer index (no OpenRead/SeekRowid on cursor 0): {ops:?}"
                );
            }
            // Outer-only WHERE `t2.a = 5 AND t2.b >= 3` must not re-run inside inner p_tid loop.
            let comp_ops = program(
                &f,
                "SELECT count(*), sum(t2.b) FROM p JOIN t2 ON t2.id = p.tid WHERE t2.a = 5 AND t2.b >= 3",
            )
            .await;
            assert!(
                !inner_loop_has_comparison(&comp_ops),
                "analyzed={analyzed} composite outer-only WHERE must not re-run inside inner loop: {comp_ops:?}"
            );

            // 3. Uncovered outer columns + fail-closed guards (payload, SELECT *,
            //    REAL-affinity column typeof(), virtual generated column, LEFT JOIN
            //    uncovered, OR across outer+inner).
            let uncovered_outer_queries = [
                "SELECT count(*), sum(length(t.pad)) FROM p JOIN t ON t.id = p.tid WHERE t.a = 5",
                "SELECT t.*, p.id FROM t JOIN p ON p.tid = t.id WHERE t.a = 5 ORDER BY t.id, p.id",
                "SELECT t.id, t.pad, p.id FROM t LEFT JOIN p ON p.tid = t.id WHERE t.a = 5 ORDER BY t.id, p.id",
                // REAL-affinity column MUST fail closed so typeof(rt.r) stays 'real'.
                "SELECT typeof(rt.r), rt.r, p.id FROM rt JOIN p ON p.tid = rt.id WHERE rt.r = 5 ORDER BY rt.id, p.id",
                "SELECT typeof(rt.r), rt.r, p.id FROM rt JOIN p ON p.tid = rt.id WHERE rt.a = 2 ORDER BY rt.id, p.id",
                // Virtual generated column MUST fail closed.
                "SELECT vt.id, vt.g, p.id FROM vt JOIN p ON p.tid = vt.id WHERE vt.a = 2 ORDER BY vt.id, p.id",
            ];
            for sql in uncovered_outer_queries {
                assert_agree(&f, &r, sql, &[], &[]).await;
                let ops = program(&f, sql).await;
                assert!(
                    has_open_cursor(&ops, 0) && has_seek_rowid_cursor(&ops, 0),
                    "analyzed={analyzed} file={file_backed} `{sql}` must fail closed to table SeekRowid on cursor 0: {ops:?}"
                );
            }
            // OR across outer+inner in LEFT JOIN stays semantically exact.
            assert_agree(
                &f,
                &r,
                "SELECT t.id, p.id FROM t LEFT JOIN p ON p.tid = t.id WHERE t.a = 5 OR p.val = 1 ORDER BY t.id, p.id",
                &[],
                &[],
            )
            .await;

            // 4. Cross-type literals, collations, and bound parameters (reusing a
            //    prepared statement across storage classes) for both covered and
            //    uncovered outer index joins.
            let literal_probe_queries = [
                "SELECT o.id, o.k, p.id FROM o JOIN p ON p.tid = o.id WHERE o.k = 2 ORDER BY o.id, p.id",
                "SELECT o.id, o.k, p.id FROM o JOIN p ON p.tid = o.id WHERE o.k = '2' ORDER BY o.id, p.id",
                "SELECT o.id, o.k, p.id FROM o JOIN p ON p.tid = o.id WHERE o.k = 2.0 ORDER BY o.id, p.id",
                "SELECT o.id, o.k, p.id FROM o JOIN p ON p.tid = o.id WHERE o.k = 2.5 ORDER BY o.id, p.id",
                "SELECT o.id, o.k, p.id FROM o JOIN p ON p.tid = o.id WHERE o.k = 'x' ORDER BY o.id, p.id",
                "SELECT o.id, o.s, p.id FROM o JOIN p ON p.tid = o.id WHERE o.s = 1 ORDER BY o.id, p.id",
                "SELECT o.id, o.s, p.id FROM o JOIN p ON p.tid = o.id WHERE o.s = '1' ORDER BY o.id, p.id",
                "SELECT o.id, o.n, p.id FROM o JOIN p ON p.tid = o.id WHERE o.n = 'abc' ORDER BY o.id, p.id",
                "SELECT o.id, o.n, p.id FROM o JOIN p ON p.tid = o.id WHERE o.n = 'ABC' ORDER BY o.id, p.id",
                "SELECT o.id, o.u, p.id FROM o JOIN p ON p.tid = o.id WHERE o.u = 5 ORDER BY o.id, p.id",
                "SELECT o.id, o.u, p.id FROM o JOIN p ON p.tid = o.id WHERE o.u = '5' ORDER BY o.id, p.id",
                "SELECT o.id, o.u, p.id FROM o JOIN p ON p.tid = o.id WHERE o.u = 5.0 ORDER BY o.id, p.id",
                "SELECT count(*), sum(o.id) FROM o LEFT JOIN p ON p.tid = o.id WHERE o.n = 'aBc'",
            ];
            for sql in literal_probe_queries {
                assert_agree(&f, &r, sql, &[], &[]).await;
                let ops = program(&f, sql).await;
                assert!(
                    !has_open_cursor(&ops, 0) && !has_seek_rowid_cursor(&ops, 0),
                    "analyzed={analyzed} file={file_backed} `{sql}` expected covering outer index: {ops:?}"
                );
            }

            let param_queries = [
                (
                    "SELECT o.id, o.k, p.id FROM o JOIN p ON p.tid = o.id WHERE o.k = ?1 ORDER BY o.id, p.id",
                    true,
                ),
                (
                    "SELECT o.id, o.k, o.pad, p.id FROM o JOIN p ON p.tid = o.id WHERE o.k = ?1 ORDER BY o.id, p.id",
                    false,
                ),
                (
                    "SELECT o.id, o.u, p.id FROM o LEFT JOIN p ON p.tid = o.id WHERE o.u = ?1 ORDER BY o.id, p.id",
                    true,
                ),
                (
                    "SELECT count(*), sum(o.id) FROM o JOIN p ON p.tid = o.id WHERE o.n = ?1",
                    true,
                ),
            ];
            let param_values: Vec<(SqliteValue, rusqlite::types::Value)> = vec![
                (SqliteValue::Integer(2), rusqlite::types::Value::Integer(2)),
                (SqliteValue::Text("2".into()), rusqlite::types::Value::Text("2".to_owned())),
                (SqliteValue::Float(2.0), rusqlite::types::Value::Real(2.0)),
                (SqliteValue::Float(2.5), rusqlite::types::Value::Real(2.5)),
                (SqliteValue::Text("x".into()), rusqlite::types::Value::Text("x".to_owned())),
                (SqliteValue::Integer(5), rusqlite::types::Value::Integer(5)),
                (SqliteValue::Text("5".into()), rusqlite::types::Value::Text("5".to_owned())),
                (SqliteValue::Float(5.0), rusqlite::types::Value::Real(5.0)),
                (SqliteValue::Text("abc".into()), rusqlite::types::Value::Text("abc".to_owned())),
                (SqliteValue::Text("ABC".into()), rusqlite::types::Value::Text("ABC".to_owned())),
                (SqliteValue::Blob(vec![5].into()), rusqlite::types::Value::Blob(vec![5])),
                (SqliteValue::Null, rusqlite::types::Value::Null),
            ];
            for (sql, expect_covering) in param_queries {
                let ops = program(&f, sql).await;
                assert_eq!(
                    !has_open_cursor(&ops, 0) && !has_seek_rowid_cursor(&ops, 0),
                    expect_covering,
                    "analyzed={analyzed} file={file_backed} `{sql}` covering expectation: {ops:?}"
                );
                let prep = f.prepare(sql).await.unwrap();
                for (fv, rv) in &param_values {
                    assert_agree(&f, &r, sql, std::slice::from_ref(fv), &[rv]).await;
                    let pr: Vec<Vec<String>> = prep
                        .query_with_params(std::slice::from_ref(fv))
                        .await
                        .unwrap()
                        .iter()
                        .map(|row| row.values().iter().map(tag_f).collect())
                        .collect();
                    let dr: Vec<Vec<String>> = f
                        .query_with_params(sql, std::slice::from_ref(fv))
                        .await
                        .unwrap()
                        .iter()
                        .map(|row| row.values().iter().map(tag_f).collect())
                        .collect();
                    assert_eq!(pr, dr, "prepared vs direct mismatch for `{sql}` with {fv:?}");
                }
                drop(prep);
            }

            f.close().await.unwrap();
        });
    }
}
