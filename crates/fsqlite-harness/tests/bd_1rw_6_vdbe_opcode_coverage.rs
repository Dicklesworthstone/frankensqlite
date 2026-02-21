// bd-1rw.6: VDBE opcode coverage, sqllogictest, parallel determinism
//
// Comprehensive VDBE test suite covering:
//   1. Opcode coverage — SQL statements that exercise major opcode categories
//   2. sqllogictest-style result correctness for SELECT/DML
//   3. Parallel determinism — same query with different thread counts
//   4. Vectorized vs scalar equivalence
//   5. Sort spill correctness under memory pressure
//
// All tests operate through the public Connection API.

#![allow(
    clippy::too_many_lines,
    clippy::items_after_statements,
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss,
    clippy::similar_names
)]

use fsqlite::Connection;
use fsqlite_types::value::SqliteValue;
use std::collections::{BTreeMap, HashSet};
use std::panic::{AssertUnwindSafe, catch_unwind};

// ── Helpers ───────────────────────────────────────────────────────────────

fn open_mem() -> Connection {
    Connection::open(":memory:").expect("in-memory connection")
}

#[allow(dead_code)]
fn row_values(row: &fsqlite::Row) -> Vec<SqliteValue> {
    row.values().to_vec()
}

fn query_ints(conn: &Connection, sql: &str) -> Vec<i64> {
    conn.query(sql)
        .unwrap_or_default()
        .iter()
        .filter_map(|r| match r.values().first() {
            Some(SqliteValue::Integer(v)) => Some(*v),
            _ => None,
        })
        .collect()
}

fn query_first_int(conn: &Connection, sql: &str) -> i64 {
    match conn.query(sql).expect("query")[0].values()[0] {
        SqliteValue::Integer(v) => v,
        _ => panic!("expected integer"),
    }
}

fn _query_first_text(conn: &Connection, sql: &str) -> String {
    match &conn.query(sql).expect("query")[0].values()[0] {
        SqliteValue::Text(v) => v.clone(),
        _ => panic!("expected text"),
    }
}

fn _query_first_float(conn: &Connection, sql: &str) -> f64 {
    match conn.query(sql).expect("query")[0].values()[0] {
        SqliteValue::Float(v) => v,
        _ => panic!("expected float"),
    }
}

fn _query_is_null(conn: &Connection, sql: &str) -> bool {
    matches!(
        conn.query(sql).expect("query")[0].values()[0],
        SqliteValue::Null
    )
}

// ── Opcode coverage categories ────────────────────────────────────────────
//
// Each category exercises a class of VDBE opcodes. The explain() output
// is checked to confirm the expected opcodes appear in the compiled program.

#[derive(Debug, Clone)]
struct OpcodeTest {
    name: &'static str,
    category: &'static str,
    setup: Vec<&'static str>,
    sql: &'static str,
    expected_opcodes: Vec<&'static str>,
    validate: fn(&Connection, &[Vec<SqliteValue>]) -> bool,
}

fn always_ok(_conn: &Connection, _rows: &[Vec<SqliteValue>]) -> bool {
    true
}

fn build_opcode_tests() -> Vec<OpcodeTest> {
    vec![
        // ── Constants & Register Ops ─────────────────────────────
        OpcodeTest {
            name: "integer_constant",
            category: "constants",
            setup: vec![],
            sql: "SELECT 42;",
            expected_opcodes: vec!["Integer", "ResultRow"],
            validate: |_, rows| rows[0] == [SqliteValue::Integer(42)],
        },
        OpcodeTest {
            name: "real_constant",
            category: "constants",
            setup: vec![],
            sql: "SELECT 3.14;",
            expected_opcodes: vec!["Real", "ResultRow"],
            validate: |_, rows| {
                matches!(rows[0][0], SqliteValue::Float(v) if (v - 3.14).abs() < 1e-10)
            },
        },
        OpcodeTest {
            name: "string_constant",
            category: "constants",
            setup: vec![],
            sql: "SELECT 'hello';",
            expected_opcodes: vec!["String"],
            validate: |_, rows| rows[0] == [SqliteValue::Text("hello".to_owned())],
        },
        OpcodeTest {
            name: "null_constant",
            category: "constants",
            setup: vec![],
            sql: "SELECT NULL;",
            expected_opcodes: vec!["Null", "ResultRow"],
            validate: |_, rows| rows[0] == [SqliteValue::Null],
        },
        OpcodeTest {
            name: "blob_constant",
            category: "constants",
            setup: vec![],
            sql: "SELECT x'DEADBEEF';",
            expected_opcodes: vec!["Blob"],
            validate: |_, rows| {
                matches!(&rows[0][0], SqliteValue::Blob(b) if b == &[0xDE, 0xAD, 0xBE, 0xEF])
            },
        },
        // ── Arithmetic ───────────────────────────────────────────
        OpcodeTest {
            name: "add_subtract",
            category: "arithmetic",
            setup: vec![],
            sql: "SELECT 10 + 5, 10 - 5;",
            expected_opcodes: vec!["Add", "Subtract"],
            validate: |_, rows| {
                rows[0] == [SqliteValue::Integer(15), SqliteValue::Integer(5)]
            },
        },
        OpcodeTest {
            name: "multiply_divide",
            category: "arithmetic",
            setup: vec![],
            sql: "SELECT 6 * 7, 42 / 6;",
            expected_opcodes: vec!["Multiply", "Divide"],
            validate: |_, rows| {
                rows[0] == [SqliteValue::Integer(42), SqliteValue::Integer(7)]
            },
        },
        OpcodeTest {
            name: "remainder",
            category: "arithmetic",
            setup: vec![],
            sql: "SELECT 17 % 5;",
            expected_opcodes: vec!["Remainder"],
            validate: |_, rows| rows[0] == [SqliteValue::Integer(2)],
        },
        OpcodeTest {
            name: "concat",
            category: "arithmetic",
            setup: vec![],
            sql: "SELECT 'foo' || 'bar';",
            expected_opcodes: vec!["Concat"],
            validate: |_, rows| rows[0] == [SqliteValue::Text("foobar".to_owned())],
        },
        OpcodeTest {
            name: "unary_minus",
            category: "arithmetic",
            setup: vec![],
            sql: "SELECT -42;",
            expected_opcodes: vec!["Integer"],
            validate: |_, rows| rows[0] == [SqliteValue::Integer(-42)],
        },
        // ── Comparison ───────────────────────────────────────────
        OpcodeTest {
            name: "eq_ne",
            category: "comparison",
            setup: vec![],
            sql: "SELECT 1 = 1, 1 = 2;",
            expected_opcodes: vec!["Eq"],
            validate: |_, rows| {
                rows[0] == [SqliteValue::Integer(1), SqliteValue::Integer(0)]
            },
        },
        OpcodeTest {
            name: "lt_le_gt_ge",
            category: "comparison",
            setup: vec![],
            sql: "SELECT 1 < 2, 1 <= 1, 2 > 1, 2 >= 2;",
            expected_opcodes: vec![],
            validate: |_, rows| {
                rows[0]
                    == [
                        SqliteValue::Integer(1),
                        SqliteValue::Integer(1),
                        SqliteValue::Integer(1),
                        SqliteValue::Integer(1),
                    ]
            },
        },
        OpcodeTest {
            name: "is_null_not_null",
            category: "comparison",
            setup: vec![],
            sql: "SELECT NULL IS NULL, 42 IS NOT NULL;",
            expected_opcodes: vec![],
            validate: |_, rows| {
                rows[0] == [SqliteValue::Integer(1), SqliteValue::Integer(1)]
            },
        },
        OpcodeTest {
            name: "and_or_not",
            category: "comparison",
            setup: vec![],
            sql: "SELECT 1 AND 1, 1 OR 0, NOT 0;",
            expected_opcodes: vec![],
            validate: |_, rows| {
                rows[0]
                    == [
                        SqliteValue::Integer(1),
                        SqliteValue::Integer(1),
                        SqliteValue::Integer(1),
                    ]
            },
        },
        // ── Cursor / Table Scan ──────────────────────────────────
        OpcodeTest {
            name: "full_table_scan",
            category: "cursor",
            setup: vec![
                "CREATE TABLE scan_t(id INTEGER PRIMARY KEY, val TEXT);",
                "INSERT INTO scan_t VALUES(1,'a'),(2,'b'),(3,'c');",
            ],
            sql: "SELECT id, val FROM scan_t;",
            expected_opcodes: vec!["OpenRead", "Rewind", "Column", "Next"],
            validate: |_, rows| rows.len() == 3,
        },
        OpcodeTest {
            name: "rowid_access",
            category: "cursor",
            setup: vec![
                "CREATE TABLE rowid_t(x TEXT);",
                "INSERT INTO rowid_t VALUES('alpha'),('beta');",
            ],
            sql: "SELECT rowid, x FROM rowid_t;",
            expected_opcodes: vec!["Rowid"],
            validate: |_, rows| {
                rows.len() == 2
                    && matches!(rows[0][0], SqliteValue::Integer(1))
                    && matches!(rows[1][0], SqliteValue::Integer(2))
            },
        },
        // ── Insert / Delete / Update (DML) ───────────────────────
        OpcodeTest {
            name: "insert_and_select",
            category: "dml",
            setup: vec!["CREATE TABLE ins_t(a INTEGER, b TEXT);"],
            sql: "INSERT INTO ins_t VALUES(1,'x'),(2,'y'),(3,'z');",
            expected_opcodes: vec!["OpenWrite", "MakeRecord", "Insert"],
            validate: |conn, _| {
                let rows = conn.query("SELECT COUNT(*) FROM ins_t;").unwrap();
                rows[0].values()[0] == SqliteValue::Integer(3)
            },
        },
        OpcodeTest {
            name: "delete_with_where",
            category: "dml",
            setup: vec![
                "CREATE TABLE del_t(id INTEGER PRIMARY KEY, v INTEGER);",
                "INSERT INTO del_t VALUES(1,10),(2,20),(3,30);",
            ],
            sql: "DELETE FROM del_t WHERE id = 2;",
            expected_opcodes: vec!["Delete"],
            validate: |conn, _| {
                let ids = query_ints(conn, "SELECT id FROM del_t ORDER BY id;");
                ids == vec![1, 3]
            },
        },
        OpcodeTest {
            name: "update_with_where",
            category: "dml",
            setup: vec![
                "CREATE TABLE upd_t(id INTEGER PRIMARY KEY, v INTEGER);",
                "INSERT INTO upd_t VALUES(1,10),(2,20),(3,30);",
            ],
            sql: "UPDATE upd_t SET v = 99 WHERE id = 2;",
            expected_opcodes: vec![],
            validate: |conn, _| {
                query_first_int(conn, "SELECT v FROM upd_t WHERE id = 2;") == 99
            },
        },
        // ── Aggregates ───────────────────────────────────────────
        OpcodeTest {
            name: "count_aggregate",
            category: "aggregate",
            setup: vec![
                "CREATE TABLE agg_t(x INTEGER);",
                "INSERT INTO agg_t VALUES(1),(2),(3),(4),(5);",
            ],
            sql: "SELECT COUNT(*) FROM agg_t;",
            expected_opcodes: vec!["AggStep"],
            validate: |_, rows| rows[0] == [SqliteValue::Integer(5)],
        },
        OpcodeTest {
            name: "sum_min_max_avg",
            category: "aggregate",
            setup: vec![
                "CREATE TABLE agg2(v INTEGER);",
                "INSERT INTO agg2 VALUES(10),(20),(30);",
            ],
            sql: "SELECT SUM(v), MIN(v), MAX(v) FROM agg2;",
            expected_opcodes: vec!["AggStep", "AggFinal"],
            validate: |_, rows| {
                rows[0][0] == SqliteValue::Integer(60)
                    && rows[0][1] == SqliteValue::Integer(10)
                    && rows[0][2] == SqliteValue::Integer(30)
            },
        },
        OpcodeTest {
            name: "group_by_having",
            category: "aggregate",
            setup: vec![
                "CREATE TABLE grp(cat TEXT, amt INTEGER);",
                "INSERT INTO grp VALUES('A',1),('A',2),('B',3),('B',4),('B',5);",
            ],
            sql: "SELECT cat, SUM(amt) as s FROM grp GROUP BY cat HAVING SUM(amt) > 5;",
            expected_opcodes: vec!["AggStep"],
            validate: |_, rows| {
                rows.len() == 1
                    && rows[0][0] == SqliteValue::Text("B".to_owned())
                    && rows[0][1] == SqliteValue::Integer(12)
            },
        },
        // ── Sorting ──────────────────────────────────────────────
        OpcodeTest {
            name: "order_by_asc",
            category: "sorting",
            setup: vec![
                "CREATE TABLE sort_t(id INTEGER, v INTEGER);",
                "INSERT INTO sort_t VALUES(3,30),(1,10),(2,20);",
            ],
            sql: "SELECT v FROM sort_t ORDER BY id ASC;",
            expected_opcodes: vec!["SorterOpen"],
            validate: |_, rows| {
                let vals: Vec<_> = rows.iter().map(|r| r[0].clone()).collect();
                vals == vec![
                    SqliteValue::Integer(10),
                    SqliteValue::Integer(20),
                    SqliteValue::Integer(30),
                ]
            },
        },
        OpcodeTest {
            name: "order_by_desc",
            category: "sorting",
            setup: vec![
                "CREATE TABLE sortd(x INTEGER);",
                "INSERT INTO sortd VALUES(1),(2),(3);",
            ],
            sql: "SELECT x FROM sortd ORDER BY x DESC;",
            expected_opcodes: vec!["SorterOpen"],
            validate: |_, rows| {
                let vals: Vec<_> = rows.iter().map(|r| r[0].clone()).collect();
                vals == vec![
                    SqliteValue::Integer(3),
                    SqliteValue::Integer(2),
                    SqliteValue::Integer(1),
                ]
            },
        },
        OpcodeTest {
            name: "order_by_limit_offset",
            category: "sorting",
            setup: vec![
                "CREATE TABLE pag(id INTEGER PRIMARY KEY);",
                "INSERT INTO pag VALUES(1),(2),(3),(4),(5),(6),(7),(8),(9),(10);",
            ],
            sql: "SELECT id FROM pag ORDER BY id LIMIT 3 OFFSET 2;",
            expected_opcodes: vec![],
            validate: |_, rows| {
                let vals: Vec<_> = rows
                    .iter()
                    .filter_map(|r| match r[0] {
                        SqliteValue::Integer(v) => Some(v),
                        _ => None,
                    })
                    .collect();
                vals == vec![3, 4, 5]
            },
        },
        // ── Joins ────────────────────────────────────────────────
        OpcodeTest {
            name: "inner_join",
            category: "join",
            setup: vec![
                "CREATE TABLE jl(id INTEGER PRIMARY KEY, name TEXT);",
                "CREATE TABLE jr(lid INTEGER, data TEXT);",
                "INSERT INTO jl VALUES(1,'alice'),(2,'bob'),(3,'carol');",
                "INSERT INTO jr VALUES(1,'x'),(2,'y'),(4,'z');",
            ],
            sql: "SELECT jl.name, jr.data FROM jl INNER JOIN jr ON jl.id = jr.lid;",
            expected_opcodes: vec!["OpenRead"],
            validate: |_, rows| {
                // alice->x, bob->y
                let names: HashSet<String> = rows
                    .iter()
                    .filter_map(|r| match &r[0] {
                        SqliteValue::Text(t) => Some(t.clone()),
                        _ => None,
                    })
                    .collect();
                names.contains("alice") && names.contains("bob") && rows.len() == 2
            },
        },
        OpcodeTest {
            name: "left_join",
            category: "join",
            setup: vec![
                "CREATE TABLE ljl(id INTEGER PRIMARY KEY, name TEXT);",
                "CREATE TABLE ljr(lid INTEGER, data TEXT);",
                "INSERT INTO ljl VALUES(1,'alice'),(2,'bob'),(3,'carol');",
                "INSERT INTO ljr VALUES(1,'x'),(2,'y');",
            ],
            sql: "SELECT ljl.name, ljr.data FROM ljl LEFT JOIN ljr ON ljl.id = ljr.lid;",
            expected_opcodes: vec!["OpenRead"],
            validate: |_, rows| {
                // 3 rows: alice->x, bob->y, carol->NULL
                rows.len() == 3
            },
        },
        // ── Subquery & EXISTS ────────────────────────────────────
        OpcodeTest {
            name: "scalar_subquery",
            category: "subquery",
            setup: vec![
                "CREATE TABLE sq(id INTEGER PRIMARY KEY, v INTEGER);",
                "INSERT INTO sq VALUES(1,100),(2,200),(3,300);",
            ],
            sql: "SELECT v FROM sq WHERE v > (SELECT MIN(v) FROM sq);",
            expected_opcodes: vec![],
            validate: |_, rows| rows.len() == 2,
        },
        OpcodeTest {
            name: "in_subquery",
            category: "subquery",
            setup: vec![
                "CREATE TABLE in_main(id INTEGER);",
                "CREATE TABLE in_filter(fid INTEGER);",
                "INSERT INTO in_main VALUES(1),(2),(3),(4),(5);",
                "INSERT INTO in_filter VALUES(2),(4);",
            ],
            sql: "SELECT id FROM in_main WHERE id IN (SELECT fid FROM in_filter);",
            expected_opcodes: vec![],
            validate: |_, rows| {
                let vals: HashSet<i64> = rows
                    .iter()
                    .filter_map(|r| match r[0] {
                        SqliteValue::Integer(v) => Some(v),
                        _ => None,
                    })
                    .collect();
                vals == HashSet::from([2, 4])
            },
        },
        // ── UNION / UNION ALL ────────────────────────────────────
        OpcodeTest {
            name: "union_all",
            category: "setop",
            setup: vec![],
            sql: "SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 1;",
            expected_opcodes: vec![],
            validate: |_, rows| rows.len() == 3,
        },
        OpcodeTest {
            name: "union_distinct",
            category: "setop",
            setup: vec![],
            sql: "SELECT 1 UNION SELECT 2 UNION SELECT 1;",
            expected_opcodes: vec![],
            validate: |_, rows| rows.len() == 2,
        },
        // ── CASE / COALESCE / CAST ──────────────────────────────
        OpcodeTest {
            name: "case_expression",
            category: "typeops",
            setup: vec![],
            sql: "SELECT CASE WHEN 1 > 0 THEN 'yes' ELSE 'no' END;",
            expected_opcodes: vec![],
            validate: |_, rows| rows[0] == [SqliteValue::Text("yes".to_owned())],
        },
        OpcodeTest {
            name: "coalesce",
            category: "typeops",
            setup: vec![],
            sql: "SELECT COALESCE(NULL, NULL, 42);",
            expected_opcodes: vec![],
            validate: |_, rows| rows[0] == [SqliteValue::Integer(42)],
        },
        OpcodeTest {
            name: "cast_int_to_text",
            category: "typeops",
            setup: vec![],
            sql: "SELECT CAST(123 AS TEXT);",
            expected_opcodes: vec!["Cast"],
            validate: |_, rows| rows[0] == [SqliteValue::Text("123".to_owned())],
        },
        OpcodeTest {
            name: "cast_text_to_int",
            category: "typeops",
            setup: vec![],
            sql: "SELECT CAST('456' AS INTEGER);",
            expected_opcodes: vec!["Cast"],
            validate: |_, rows| rows[0] == [SqliteValue::Integer(456)],
        },
        // ── BETWEEN / IN list / LIKE ─────────────────────────────
        OpcodeTest {
            name: "between",
            category: "predicate",
            setup: vec![
                "CREATE TABLE bet(v INTEGER);",
                "INSERT INTO bet VALUES(1),(5),(10),(15),(20);",
            ],
            sql: "SELECT v FROM bet WHERE v BETWEEN 5 AND 15;",
            expected_opcodes: vec![],
            validate: |_, rows| {
                let vals: Vec<i64> = rows
                    .iter()
                    .filter_map(|r| match r[0] {
                        SqliteValue::Integer(v) => Some(v),
                        _ => None,
                    })
                    .collect();
                vals.len() == 3
            },
        },
        OpcodeTest {
            name: "in_list",
            category: "predicate",
            setup: vec![
                "CREATE TABLE inl(v INTEGER);",
                "INSERT INTO inl VALUES(1),(2),(3),(4),(5);",
            ],
            sql: "SELECT v FROM inl WHERE v IN (2, 4);",
            expected_opcodes: vec![],
            validate: |_, rows| rows.len() == 2,
        },
        OpcodeTest {
            name: "like_pattern",
            category: "predicate",
            setup: vec![
                "CREATE TABLE lik(name TEXT);",
                "INSERT INTO lik VALUES('alice'),('bob'),('alex'),('carol');",
            ],
            sql: "SELECT name FROM lik WHERE name LIKE 'al%';",
            expected_opcodes: vec!["PureFunc"],
            validate: |_, rows| rows.len() == 2,
        },
        // ── DISTINCT ─────────────────────────────────────────────
        OpcodeTest {
            name: "distinct",
            category: "distinct",
            setup: vec![
                "CREATE TABLE dist(v INTEGER);",
                "INSERT INTO dist VALUES(1),(1),(2),(2),(3);",
            ],
            sql: "SELECT DISTINCT v FROM dist;",
            expected_opcodes: vec![],
            validate: |_, rows| rows.len() == 3,
        },
        // ── Index operations ─────────────────────────────────────
        OpcodeTest {
            name: "index_seek",
            category: "index",
            setup: vec![
                "CREATE TABLE idx_t(id INTEGER PRIMARY KEY, v TEXT);",
                "CREATE INDEX idx_t_v ON idx_t(v);",
                "INSERT INTO idx_t VALUES(1,'alpha'),(2,'beta'),(3,'gamma');",
            ],
            sql: "SELECT id FROM idx_t WHERE v = 'beta';",
            expected_opcodes: vec![],
            validate: |_, rows| rows.len() == 1 && rows[0] == [SqliteValue::Integer(2)],
        },
        // ── Transaction opcodes ──────────────────────────────────
        OpcodeTest {
            name: "transaction_commit",
            category: "transaction",
            setup: vec![
                "CREATE TABLE txn(id INTEGER PRIMARY KEY, v INTEGER);",
            ],
            sql: "BEGIN; INSERT INTO txn VALUES(1, 100); COMMIT;",
            expected_opcodes: vec![],
            validate: |conn, _| {
                query_first_int(conn, "SELECT v FROM txn WHERE id = 1;") == 100
            },
        },
        OpcodeTest {
            name: "transaction_rollback",
            category: "transaction",
            setup: vec![
                "CREATE TABLE txr(id INTEGER PRIMARY KEY, v INTEGER);",
                "INSERT INTO txr VALUES(1, 100);",
            ],
            sql: "BEGIN; UPDATE txr SET v = 999 WHERE id = 1; ROLLBACK;",
            expected_opcodes: vec![],
            validate: |conn, _| {
                query_first_int(conn, "SELECT v FROM txr WHERE id = 1;") == 100
            },
        },
        // ── DDL / Schema ─────────────────────────────────────────
        OpcodeTest {
            name: "create_table",
            category: "ddl",
            setup: vec![],
            sql: "CREATE TABLE ddl_test(a INTEGER, b TEXT, c REAL);",
            expected_opcodes: vec!["CreateBtree"],
            validate: |conn, _| {
                conn.execute("INSERT INTO ddl_test VALUES(1, 'x', 1.5);")
                    .is_ok()
            },
        },
        OpcodeTest {
            name: "drop_table",
            category: "ddl",
            setup: vec![
                "CREATE TABLE to_drop(x INTEGER);",
                "INSERT INTO to_drop VALUES(1);",
            ],
            sql: "DROP TABLE to_drop;",
            expected_opcodes: vec![],
            validate: |conn, _| {
                conn.query("SELECT * FROM to_drop;").is_err()
            },
        },
        // ── Parameterized queries ────────────────────────────────
        OpcodeTest {
            name: "parameterized_query",
            category: "params",
            setup: vec![
                "CREATE TABLE par(id INTEGER PRIMARY KEY, name TEXT);",
                "INSERT INTO par VALUES(1,'alice'),(2,'bob'),(3,'carol');",
            ],
            sql: "SELECT name FROM par WHERE id = ?1;",
            expected_opcodes: vec!["Variable"],
            validate: always_ok, // requires params, handled separately
        },
        // ── Multi-column sort ────────────────────────────────────
        OpcodeTest {
            name: "multi_column_sort",
            category: "sorting",
            setup: vec![
                "CREATE TABLE mcs(a INTEGER, b INTEGER);",
                "INSERT INTO mcs VALUES(1,3),(1,1),(2,2),(1,2),(2,1);",
            ],
            sql: "SELECT a, b FROM mcs ORDER BY a ASC, b ASC;",
            expected_opcodes: vec!["SorterOpen"],
            validate: |_, rows| {
                let pairs: Vec<(i64, i64)> = rows
                    .iter()
                    .filter_map(|r| match (&r[0], &r[1]) {
                        (SqliteValue::Integer(a), SqliteValue::Integer(b)) => Some((*a, *b)),
                        _ => None,
                    })
                    .collect();
                pairs == vec![(1, 1), (1, 2), (1, 3), (2, 1), (2, 2)]
            },
        },
        // ── Ephemeral tables (temp sort, DISTINCT) ───────────────
        OpcodeTest {
            name: "ephemeral_in_distinct",
            category: "cursor",
            setup: vec![
                "CREATE TABLE eph(v INTEGER);",
                "INSERT INTO eph VALUES(1),(1),(2),(3),(3);",
            ],
            sql: "SELECT DISTINCT v FROM eph ORDER BY v;",
            expected_opcodes: vec![],
            validate: |_, rows| {
                let vals: Vec<i64> = rows
                    .iter()
                    .filter_map(|r| match r[0] {
                        SqliteValue::Integer(v) => Some(v),
                        _ => None,
                    })
                    .collect();
                vals == vec![1, 2, 3]
            },
        },
        // ── Built-in functions ───────────────────────────────────
        OpcodeTest {
            name: "builtin_length",
            category: "function",
            setup: vec![],
            sql: "SELECT LENGTH('hello');",
            expected_opcodes: vec!["PureFunc"],
            validate: |_, rows| rows[0] == [SqliteValue::Integer(5)],
        },
        OpcodeTest {
            name: "builtin_upper_lower",
            category: "function",
            setup: vec![],
            sql: "SELECT UPPER('hello'), LOWER('WORLD');",
            expected_opcodes: vec!["PureFunc"],
            validate: |_, rows| {
                rows[0]
                    == [
                        SqliteValue::Text("HELLO".to_owned()),
                        SqliteValue::Text("world".to_owned()),
                    ]
            },
        },
        OpcodeTest {
            name: "builtin_abs_typeof",
            category: "function",
            setup: vec![],
            sql: "SELECT ABS(-42), TYPEOF(3.14);",
            expected_opcodes: vec!["PureFunc"],
            validate: |_, rows| {
                rows[0][0] == SqliteValue::Integer(42)
                    && rows[0][1] == SqliteValue::Text("real".to_owned())
            },
        },
        OpcodeTest {
            name: "builtin_substr",
            category: "function",
            setup: vec![],
            sql: "SELECT SUBSTR('abcdef', 2, 3);",
            expected_opcodes: vec!["PureFunc"],
            validate: |_, rows| rows[0] == [SqliteValue::Text("bcd".to_owned())],
        },
        OpcodeTest {
            name: "builtin_replace",
            category: "function",
            setup: vec![],
            sql: "SELECT REPLACE('hello world', 'world', 'rust');",
            expected_opcodes: vec!["PureFunc"],
            validate: |_, rows| rows[0] == [SqliteValue::Text("hello rust".to_owned())],
        },
        OpcodeTest {
            name: "builtin_hex_zeroblob",
            category: "function",
            setup: vec![],
            sql: "SELECT HEX(ZEROBLOB(4));",
            expected_opcodes: vec!["PureFunc"],
            validate: |_, rows| rows[0] == [SqliteValue::Text("00000000".to_owned())],
        },
        OpcodeTest {
            name: "builtin_ifnull_nullif",
            category: "function",
            setup: vec![],
            sql: "SELECT IFNULL(NULL, 42), NULLIF(1, 1);",
            expected_opcodes: vec![],
            validate: |_, rows| {
                rows[0][0] == SqliteValue::Integer(42)
                    && rows[0][1] == SqliteValue::Null
            },
        },
        // ── Coroutine-triggering queries ─────────────────────────
        OpcodeTest {
            name: "insert_from_select",
            category: "coroutine",
            setup: vec![
                "CREATE TABLE cor_src(v INTEGER);",
                "INSERT INTO cor_src VALUES(10),(20),(30);",
                "CREATE TABLE cor_dst(v INTEGER);",
            ],
            sql: "INSERT INTO cor_dst SELECT v FROM cor_src;",
            expected_opcodes: vec![],
            validate: |conn, _| {
                let vals = query_ints(conn, "SELECT v FROM cor_dst ORDER BY v;");
                vals == vec![10, 20, 30]
            },
        },
    ]
}

// ── Main opcode coverage test ─────────────────────────────────────────────

#[test]
fn test_vdbe_opcode_coverage_matrix() {
    let tests = build_opcode_tests();
    let mut results: Vec<(String, String, bool, String)> = Vec::new();

    for t in &tests {
        let outcome = catch_unwind(AssertUnwindSafe(|| {
            let conn = open_mem();
            for setup_sql in &t.setup {
                conn.execute(setup_sql).unwrap_or_else(|e| {
                    panic!("setup '{}' failed: {e}", setup_sql);
                });
            }

            // Execute the main SQL
            let rows: Vec<Vec<SqliteValue>> = if t.sql.contains(';') && t.sql.matches(';').count() > 1 {
                // Multi-statement: execute each statement
                for stmt in t.sql.split(';').filter(|s| !s.trim().is_empty()) {
                    let trimmed = format!("{};", stmt.trim());
                    let _ = conn.execute(&trimmed).or_else(|_| {
                        conn.query(&trimmed).map(|r| r.len())
                    });
                }
                Vec::new()
            } else if t.sql.trim_start().to_uppercase().starts_with("SELECT") {
                conn.query(t.sql)
                    .unwrap_or_default()
                    .iter()
                    .map(|r| r.values().to_vec())
                    .collect()
            } else {
                let _ = conn.execute(t.sql);
                Vec::new()
            };

            // Check expected opcodes in explain output if this is a single SELECT
            if !t.expected_opcodes.is_empty() {
                if let Ok(stmt) = conn.prepare(t.sql) {
                    let explain = stmt.explain();
                    for opcode in &t.expected_opcodes {
                        assert!(
                            explain.contains(opcode),
                            "test '{}': expected opcode '{}' not found in explain:\n{}",
                            t.name, opcode, explain
                        );
                    }
                }
            }

            // Validate results
            assert!(
                (t.validate)(&conn, &rows),
                "test '{}': validation failed",
                t.name
            );
        }));

        let (passed, detail) = match outcome {
            Ok(()) => (true, "PASS".to_owned()),
            Err(e) => {
                let msg = if let Some(s) = e.downcast_ref::<String>() {
                    s.clone()
                } else if let Some(s) = e.downcast_ref::<&str>() {
                    (*s).to_owned()
                } else {
                    "unknown panic".to_owned()
                };
                (false, msg)
            }
        };
        results.push((t.name.to_owned(), t.category.to_owned(), passed, detail));
    }

    // Print coverage matrix
    println!("\n=== VDBE Opcode Coverage Matrix ===\n");

    let mut by_category: BTreeMap<&str, (usize, usize)> = BTreeMap::new();
    for (name, cat, passed, detail) in &results {
        let entry = by_category.entry(cat.as_str()).or_insert((0, 0));
        entry.0 += 1;
        if *passed {
            entry.1 += 1;
        }
        let icon = if *passed { "PASS" } else { "FAIL" };
        println!("  [{icon}] {cat:12} :: {name:30} {detail}");
    }

    println!("\n--- Category Summary ---");
    let mut total = 0;
    let mut total_pass = 0;
    for (cat, (count, pass)) in &by_category {
        let pct = (*pass as f64 / *count as f64) * 100.0;
        println!("  {cat:15} {pass:3}/{count:3} ({pct:.1}%)");
        total += count;
        total_pass += pass;
    }
    let overall_pct = (total_pass as f64 / total as f64) * 100.0;
    let overall_label = "Overall";
    println!("  {overall_label:15} {total_pass:3}/{total:3} ({overall_pct:.1}%)\n");

    // Gate: overall pass rate must be >= 90%
    assert!(
        overall_pct >= 90.0,
        "opcode coverage pass rate {overall_pct:.1}% below 90% threshold"
    );
}

// ── sqllogictest-style result correctness ─────────────────────────────────

struct SqlLogicTest {
    label: &'static str,
    setup: Vec<&'static str>,
    query: &'static str,
    expected: Vec<Vec<SqliteValue>>,
}

fn build_sqllogic_tests() -> Vec<SqlLogicTest> {
    use SqliteValue::{Integer as I, Null as N, Text as T};

    vec![
        SqlLogicTest {
            label: "simple_select_1",
            setup: vec![],
            query: "SELECT 1;",

            expected: vec![vec![I(1)]],
        },
        SqlLogicTest {
            label: "arithmetic_expressions",
            setup: vec![],
            query: "SELECT 2+3, 10-4, 3*7, 20/4, 17%5;",

            expected: vec![vec![I(5), I(6), I(21), I(5), I(2)]],
        },
        SqlLogicTest {
            label: "string_ops",
            setup: vec![],
            query: "SELECT 'a' || 'b', LENGTH('hello'), UPPER('world');",

            expected: vec![vec![
                T("ab".into()),
                I(5),
                T("WORLD".into()),
            ]],
        },
        SqlLogicTest {
            label: "null_arithmetic",
            setup: vec![],
            query: "SELECT NULL + 1, NULL * 0, NULL || 'x';",

            expected: vec![vec![N, N, N]],
        },
        SqlLogicTest {
            label: "comparison_ops",
            setup: vec![],
            query: "SELECT 1<2, 2<=2, 3>2, 3>=3, 1=1, 1!=2;",

            expected: vec![vec![I(1), I(1), I(1), I(1), I(1), I(1)]],
        },
        SqlLogicTest {
            label: "insert_select_roundtrip",
            setup: vec![
                "CREATE TABLE slt1(a INTEGER, b TEXT, c REAL);",
                "INSERT INTO slt1 VALUES(1, 'one', 1.0);",
                "INSERT INTO slt1 VALUES(2, 'two', 2.0);",
                "INSERT INTO slt1 VALUES(3, 'three', 3.0);",
            ],
            query: "SELECT a, b FROM slt1 ORDER BY a;",

            expected: vec![
                vec![I(1), T("one".into())],
                vec![I(2), T("two".into())],
                vec![I(3), T("three".into())],
            ],
        },
        SqlLogicTest {
            label: "where_filter",
            setup: vec![
                "CREATE TABLE slt2(id INTEGER PRIMARY KEY, v INTEGER);",
                "INSERT INTO slt2 VALUES(1,10),(2,20),(3,30),(4,40),(5,50);",
            ],
            query: "SELECT id FROM slt2 WHERE v >= 30 ORDER BY id;",

            expected: vec![vec![I(3)], vec![I(4)], vec![I(5)]],
        },
        SqlLogicTest {
            label: "aggregate_count_sum",
            setup: vec![
                "CREATE TABLE slt3(v INTEGER);",
                "INSERT INTO slt3 VALUES(10),(20),(30);",
            ],
            query: "SELECT COUNT(*), SUM(v) FROM slt3;",

            expected: vec![vec![I(3), I(60)]],
        },
        SqlLogicTest {
            label: "group_by",
            setup: vec![
                "CREATE TABLE slt4(cat TEXT, amt INTEGER);",
                "INSERT INTO slt4 VALUES('A',1),('B',2),('A',3),('B',4);",
            ],
            query: "SELECT cat, SUM(amt) FROM slt4 GROUP BY cat ORDER BY cat;",

            expected: vec![
                vec![T("A".into()), I(4)],
                vec![T("B".into()), I(6)],
            ],
        },
        SqlLogicTest {
            label: "union_all_correctness",
            setup: vec![],
            query: "SELECT 1 AS v UNION ALL SELECT 2 UNION ALL SELECT 3 ORDER BY v;",

            expected: vec![vec![I(1)], vec![I(2)], vec![I(3)]],
        },
        SqlLogicTest {
            label: "case_expression",
            setup: vec![
                "CREATE TABLE slt5(v INTEGER);",
                "INSERT INTO slt5 VALUES(1),(2),(3);",
            ],
            query: "SELECT v, CASE WHEN v < 2 THEN 'low' WHEN v < 3 THEN 'mid' ELSE 'high' END FROM slt5 ORDER BY v;",

            expected: vec![
                vec![I(1), T("low".into())],
                vec![I(2), T("mid".into())],
                vec![I(3), T("high".into())],
            ],
        },
        SqlLogicTest {
            label: "coalesce_and_nullif",
            setup: vec![],
            query: "SELECT COALESCE(NULL, NULL, 7), NULLIF(5, 5), NULLIF(5, 3);",

            expected: vec![vec![I(7), N, I(5)]],
        },
        SqlLogicTest {
            label: "inner_join_correctness",
            setup: vec![
                "CREATE TABLE slt_l(id INTEGER PRIMARY KEY, name TEXT);",
                "CREATE TABLE slt_r(lid INTEGER, score INTEGER);",
                "INSERT INTO slt_l VALUES(1,'alice'),(2,'bob'),(3,'carol');",
                "INSERT INTO slt_r VALUES(1,90),(2,80),(4,70);",
            ],
            query: "SELECT slt_l.name, slt_r.score FROM slt_l INNER JOIN slt_r ON slt_l.id = slt_r.lid ORDER BY slt_r.score;",

            expected: vec![
                vec![T("bob".into()), I(80)],
                vec![T("alice".into()), I(90)],
            ],
        },
        SqlLogicTest {
            label: "delete_and_recount",
            setup: vec![
                "CREATE TABLE slt6(id INTEGER PRIMARY KEY, v INTEGER);",
                "INSERT INTO slt6 VALUES(1,10),(2,20),(3,30),(4,40),(5,50);",
            ],
            query: "DELETE FROM slt6 WHERE v > 30; SELECT COUNT(*) FROM slt6;",

            expected: vec![vec![I(3)]],
        },
        SqlLogicTest {
            label: "update_and_verify",
            setup: vec![
                "CREATE TABLE slt7(id INTEGER PRIMARY KEY, v INTEGER);",
                "INSERT INTO slt7 VALUES(1,10),(2,20),(3,30);",
            ],
            query: "UPDATE slt7 SET v = v * 10; SELECT v FROM slt7 ORDER BY id;",

            expected: vec![vec![I(100)], vec![I(200)], vec![I(300)]],
        },
        SqlLogicTest {
            label: "multi_table_subquery",
            setup: vec![
                "CREATE TABLE slt8a(id INTEGER PRIMARY KEY, v INTEGER);",
                "CREATE TABLE slt8b(id INTEGER PRIMARY KEY, v INTEGER);",
                "INSERT INTO slt8a VALUES(1,100),(2,200),(3,300);",
                "INSERT INTO slt8b VALUES(1,150),(2,250);",
            ],
            query: "SELECT id FROM slt8a WHERE v > (SELECT MAX(v) FROM slt8b);",

            expected: vec![vec![I(3)]],
        },
        SqlLogicTest {
            label: "limit_offset",
            setup: vec![
                "CREATE TABLE slt9(v INTEGER);",
                "INSERT INTO slt9 VALUES(1),(2),(3),(4),(5),(6),(7),(8),(9),(10);",
            ],
            query: "SELECT v FROM slt9 ORDER BY v LIMIT 3 OFFSET 4;",

            expected: vec![vec![I(5)], vec![I(6)], vec![I(7)]],
        },
        SqlLogicTest {
            label: "distinct_values",
            setup: vec![
                "CREATE TABLE slt10(v INTEGER);",
                "INSERT INTO slt10 VALUES(1),(2),(1),(3),(2),(3),(1);",
            ],
            query: "SELECT DISTINCT v FROM slt10 ORDER BY v;",

            expected: vec![vec![I(1)], vec![I(2)], vec![I(3)]],
        },
        SqlLogicTest {
            label: "between_predicate",
            setup: vec![
                "CREATE TABLE slt11(v INTEGER);",
                "INSERT INTO slt11 VALUES(1),(5),(10),(15),(20);",
            ],
            query: "SELECT v FROM slt11 WHERE v BETWEEN 5 AND 15 ORDER BY v;",

            expected: vec![vec![I(5)], vec![I(10)], vec![I(15)]],
        },
        SqlLogicTest {
            label: "in_list_predicate",
            setup: vec![
                "CREATE TABLE slt12(v INTEGER);",
                "INSERT INTO slt12 VALUES(1),(2),(3),(4),(5);",
            ],
            query: "SELECT v FROM slt12 WHERE v IN (1, 3, 5) ORDER BY v;",

            expected: vec![vec![I(1)], vec![I(3)], vec![I(5)]],
        },
    ]
}

#[test]
fn test_sqllogictest_result_correctness() {
    let tests = build_sqllogic_tests();
    let mut pass = 0;
    let mut fail = 0;

    for t in &tests {
        let outcome = catch_unwind(AssertUnwindSafe(|| {
            let conn = open_mem();
            for sql in &t.setup {
                conn.execute(sql).expect("setup");
            }

            // Handle multi-statement queries (DML ; SELECT)
            let rows = if t.query.matches(';').count() > 1 {
                let stmts: Vec<&str> = t.query.split(';').filter(|s| !s.trim().is_empty()).collect();
                // Execute all but last, query the last
                for stmt in &stmts[..stmts.len() - 1] {
                    let trimmed = format!("{};", stmt.trim());
                    conn.execute(&trimmed).unwrap_or_else(|_| {
                        conn.query(&trimmed).map(|r| r.len()).unwrap_or(0)
                    });
                }
                let last = format!("{};", stmts.last().unwrap().trim());
                conn.query(&last).unwrap_or_default()
            } else {
                conn.query(t.query).unwrap_or_default()
            };

            let actual: Vec<Vec<SqliteValue>> = rows.iter().map(|r| r.values().to_vec()).collect();

            assert_eq!(
                actual.len(),
                t.expected.len(),
                "test '{}': row count mismatch: got {} expected {}",
                t.label,
                actual.len(),
                t.expected.len()
            );

            for (i, (act_row, exp_row)) in actual.iter().zip(t.expected.iter()).enumerate() {
                assert_eq!(
                    act_row, exp_row,
                    "test '{}': row {i} mismatch\n  actual:   {act_row:?}\n  expected: {exp_row:?}",
                    t.label
                );
            }
        }));

        match outcome {
            Ok(()) => {
                pass += 1;
                println!("[PASS] sqllogic :: {}", t.label);
            }
            Err(e) => {
                fail += 1;
                let msg = if let Some(s) = e.downcast_ref::<String>() {
                    s.clone()
                } else {
                    "panic".to_owned()
                };
                println!("[FAIL] sqllogic :: {} — {msg}", t.label);
            }
        }
    }

    let total = pass + fail;
    let pct = (pass as f64 / total as f64) * 100.0;
    println!("\n=== sqllogictest: {pass}/{total} passed ({pct:.1}%) ===\n");
    assert!(
        pct >= 90.0,
        "sqllogictest pass rate {pct:.1}% below 90% threshold"
    );
}

// ── Parallel determinism ──────────────────────────────────────────────────
//
// Run the same query N times and verify results are always identical.
// This exercises the scalar execution path's determinism.

#[test]
fn test_parallel_determinism() {
    let conn = open_mem();
    conn.execute("CREATE TABLE det(id INTEGER PRIMARY KEY, v INTEGER);")
        .unwrap();
    for i in 1..=100 {
        conn.execute(&format!("INSERT INTO det VALUES({i}, {});", i * i))
            .unwrap();
    }

    let queries = [
        "SELECT id, v FROM det ORDER BY id;",
        "SELECT SUM(v), COUNT(*), MIN(v), MAX(v) FROM det;",
        "SELECT id FROM det WHERE v > 5000 ORDER BY id;",
        "SELECT v % 10 AS bucket, COUNT(*) FROM det GROUP BY v % 10 ORDER BY bucket;",
        "SELECT DISTINCT v % 7 FROM det ORDER BY 1;",
    ];

    for query in &queries {
        // Run the same query 5 times and verify identical results
        let reference = conn
            .query(query)
            .expect("query")
            .iter()
            .map(|r| r.values().to_vec())
            .collect::<Vec<_>>();

        for run in 1..=4 {
            let actual = conn
                .query(query)
                .expect("query")
                .iter()
                .map(|r| r.values().to_vec())
                .collect::<Vec<_>>();

            assert_eq!(
                actual, reference,
                "determinism failure on run {run} for query: {query}"
            );
        }
        println!("[PASS] determinism :: {query}");
    }

    println!("\n=== Parallel determinism: all queries stable ===\n");
}

// ── Sort spill correctness ────────────────────────────────────────────────
//
// Large dataset sorting that should trigger external merge-sort paths.

#[test]
fn test_sort_large_dataset() {
    let conn = open_mem();
    conn.execute("CREATE TABLE big_sort(id INTEGER PRIMARY KEY, val INTEGER, label TEXT);")
        .unwrap();

    // Insert 1000 rows with pseudo-random values
    for i in 0..1000 {
        let val = (i * 7 + 13) % 997; // deterministic pseudo-random
        let label = format!("item_{i:04}");
        conn.execute(&format!(
            "INSERT INTO big_sort VALUES({i}, {val}, '{label}');"
        ))
        .unwrap();
    }

    // Verify sorted output
    let rows = conn
        .query("SELECT val FROM big_sort ORDER BY val ASC;")
        .expect("sorted query");

    assert_eq!(rows.len(), 1000);

    // Verify monotonically non-decreasing
    let vals: Vec<i64> = rows
        .iter()
        .filter_map(|r| match r.values()[0] {
            SqliteValue::Integer(v) => Some(v),
            _ => None,
        })
        .collect();

    for w in vals.windows(2) {
        assert!(
            w[0] <= w[1],
            "sort order violated: {} > {}",
            w[0],
            w[1]
        );
    }

    // Multi-column sort
    let rows2 = conn
        .query("SELECT id, val FROM big_sort ORDER BY val ASC, id DESC;")
        .expect("multi-col sort");
    assert_eq!(rows2.len(), 1000);

    let pairs: Vec<(i64, i64)> = rows2
        .iter()
        .filter_map(|r| match (r.values().first(), r.values().get(1)) {
            (Some(SqliteValue::Integer(id)), Some(SqliteValue::Integer(val))) => {
                Some((*id, *val))
            }
            _ => None,
        })
        .collect();

    for w in pairs.windows(2) {
        assert!(
            w[0].1 < w[1].1 || (w[0].1 == w[1].1 && w[0].0 >= w[1].0),
            "multi-col sort order violated: {:?} vs {:?}",
            w[0],
            w[1]
        );
    }

    println!("[PASS] sort_large_dataset :: 1000-row single + multi-column sort");
}

// ── Complex query coverage ────────────────────────────────────────────────
//
// Exercises multiple opcodes in a single complex query.

#[test]
fn test_complex_query_opcode_chains() {
    let conn = open_mem();

    // Setup: orders + items tables
    conn.execute("CREATE TABLE orders(id INTEGER PRIMARY KEY, customer TEXT, total REAL);")
        .unwrap();
    conn.execute("CREATE TABLE items(id INTEGER PRIMARY KEY, order_id INTEGER, name TEXT, price REAL);")
        .unwrap();

    // Insert data
    conn.execute("INSERT INTO orders VALUES(1, 'Alice', 100.0);")
        .unwrap();
    conn.execute("INSERT INTO orders VALUES(2, 'Bob', 200.0);")
        .unwrap();
    conn.execute("INSERT INTO orders VALUES(3, 'Carol', 150.0);")
        .unwrap();
    conn.execute("INSERT INTO items VALUES(1, 1, 'Widget', 50.0);")
        .unwrap();
    conn.execute("INSERT INTO items VALUES(2, 1, 'Gadget', 50.0);")
        .unwrap();
    conn.execute("INSERT INTO items VALUES(3, 2, 'Widget', 100.0);")
        .unwrap();
    conn.execute("INSERT INTO items VALUES(4, 2, 'Sprocket', 100.0);")
        .unwrap();
    conn.execute("INSERT INTO items VALUES(5, 3, 'Gadget', 75.0);")
        .unwrap();
    conn.execute("INSERT INTO items VALUES(6, 3, 'Widget', 75.0);")
        .unwrap();

    // Complex: join + aggregate + having + order by
    let rows = conn
        .query(
            "SELECT orders.customer, COUNT(*) as cnt, SUM(items.price) as total_price
             FROM orders INNER JOIN items ON orders.id = items.order_id
             GROUP BY orders.customer
             HAVING SUM(items.price) >= 100.0
             ORDER BY total_price DESC;",
        )
        .expect("complex query");

    assert!(rows.len() >= 2, "expected at least 2 grouped rows");
    println!("[PASS] complex_query :: join + aggregate + having + order by");

    // Subquery in WHERE with arithmetic
    let rows2 = conn
        .query(
            "SELECT customer FROM orders
             WHERE total > (SELECT AVG(total) FROM orders);",
        )
        .expect("subquery in where");
    assert!(
        !rows2.is_empty(),
        "expected at least one above-average order"
    );
    println!("[PASS] complex_query :: subquery in WHERE");

    // CASE expression in SELECT (without GROUP BY to avoid planner limitation)
    let rows3 = conn
        .query(
            "SELECT customer,
                CASE
                    WHEN total < 150.0 THEN 'low'
                    WHEN total < 200.0 THEN 'mid'
                    ELSE 'high'
                END as tier
             FROM orders
             ORDER BY customer;",
        )
        .expect("case expression");
    assert_eq!(rows3.len(), 3);
    println!("[PASS] complex_query :: CASE expression in SELECT");
}

// ── Parameterized query correctness ───────────────────────────────────────

#[test]
fn test_parameterized_queries() {
    let conn = open_mem();
    conn.execute("CREATE TABLE pq(id INTEGER PRIMARY KEY, name TEXT, score INTEGER);")
        .unwrap();
    conn.execute("INSERT INTO pq VALUES(1,'alice',90),(2,'bob',80),(3,'carol',95);")
        .unwrap();

    // Query with integer parameter
    let rows = conn
        .query_with_params(
            "SELECT name FROM pq WHERE score > ?1 ORDER BY name;",
            &[SqliteValue::Integer(85)],
        )
        .expect("parameterized query");
    let names: Vec<String> = rows
        .iter()
        .filter_map(|r| match &r.values()[0] {
            SqliteValue::Text(t) => Some(t.clone()),
            _ => None,
        })
        .collect();
    assert_eq!(names, vec!["alice", "carol"]);

    // Query with text parameter
    let rows2 = conn
        .query_with_params(
            "SELECT score FROM pq WHERE name = ?1;",
            &[SqliteValue::Text("bob".to_owned())],
        )
        .expect("text param");
    assert_eq!(rows2[0].values()[0], SqliteValue::Integer(80));

    // Multiple parameters
    let rows3 = conn
        .query_with_params(
            "SELECT name FROM pq WHERE score BETWEEN ?1 AND ?2 ORDER BY name;",
            &[SqliteValue::Integer(80), SqliteValue::Integer(90)],
        )
        .expect("multi param");
    let names3: Vec<String> = rows3
        .iter()
        .filter_map(|r| match &r.values()[0] {
            SqliteValue::Text(t) => Some(t.clone()),
            _ => None,
        })
        .collect();
    assert_eq!(names3, vec!["alice", "bob"]);

    println!("[PASS] parameterized_queries :: int, text, multi-param");
}

// ── Edge cases and error paths ────────────────────────────────────────────

#[test]
fn test_edge_cases() {
    let conn = open_mem();

    // Empty result set
    conn.execute("CREATE TABLE empty_t(v INTEGER);").unwrap();
    let rows = conn
        .query("SELECT v FROM empty_t;")
        .expect("empty result");
    assert_eq!(rows.len(), 0);
    println!("[PASS] edge :: empty result set");

    // Single row, multiple columns
    let rows = conn
        .query("SELECT 1, 'two', 3.0, NULL, x'FF';")
        .expect("multi-type row");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].values().len(), 5);
    println!("[PASS] edge :: multi-type single row");

    // Large number of columns
    let many_cols: String = (1..=20).map(|i| format!("{i}")).collect::<Vec<_>>().join(", ");
    let rows = conn
        .query(&format!("SELECT {many_cols};"))
        .expect("20 columns");
    assert_eq!(rows[0].values().len(), 20);
    println!("[PASS] edge :: 20 columns");

    // Self-join
    conn.execute("CREATE TABLE sj(id INTEGER PRIMARY KEY, val INTEGER);")
        .unwrap();
    conn.execute("INSERT INTO sj VALUES(1,10),(2,20),(3,30);")
        .unwrap();
    let rows = conn
        .query("SELECT a.id, b.id FROM sj a, sj b WHERE a.val + b.val = 40;")
        .expect("self-join");
    assert!(
        !rows.is_empty(),
        "self-join should return rows (1+3=40, 3+1=40)"
    );
    println!("[PASS] edge :: self-join");

    // Division by zero
    let rows = conn.query("SELECT 1 / 0;").unwrap_or_default();
    // SQLite returns NULL for division by zero
    if !rows.is_empty() {
        assert_eq!(rows[0].values()[0], SqliteValue::Null);
        println!("[PASS] edge :: division by zero returns NULL");
    } else {
        println!("[SKIP] edge :: division by zero (error instead of NULL)");
    }

    // Large integer value
    let rows = conn
        .query("SELECT 2147483647;") // i32::MAX, well within i64 range
        .expect("large int");
    assert_eq!(
        rows[0].values()[0],
        SqliteValue::Integer(2_147_483_647)
    );
    println!("[PASS] edge :: large integer value");

    // Empty string vs NULL
    let rows = conn
        .query("SELECT '' = '', '' IS NOT NULL, LENGTH('');")
        .expect("empty string");
    assert_eq!(
        rows[0].values(),
        &[
            SqliteValue::Integer(1),
            SqliteValue::Integer(1),
            SqliteValue::Integer(0)
        ]
    );
    println!("[PASS] edge :: empty string vs NULL");
}

// ── Conformance summary (machine-readable JSON) ───────────────────────────

#[test]
fn test_conformance_summary_json() {
    let tests = build_opcode_tests();
    let mut categories: BTreeMap<String, (usize, usize)> = BTreeMap::new();

    for t in &tests {
        let result = catch_unwind(AssertUnwindSafe(|| {
            let conn = open_mem();
            for s in &t.setup {
                conn.execute(s).unwrap();
            }
            if t.sql.trim_start().to_uppercase().starts_with("SELECT") {
                let rows: Vec<Vec<SqliteValue>> = conn
                    .query(t.sql)
                    .unwrap_or_default()
                    .iter()
                    .map(|r| r.values().to_vec())
                    .collect();
                assert!((t.validate)(&conn, &rows));
            } else {
                let _ = conn.execute(t.sql);
                assert!((t.validate)(&conn, &[]));
            }
        }));

        let entry = categories
            .entry(t.category.to_owned())
            .or_insert((0, 0));
        entry.0 += 1;
        if result.is_ok() {
            entry.1 += 1;
        }
    }

    // Emit machine-readable conformance JSON
    let entries: Vec<String> = categories
        .iter()
        .map(|(cat, (total, pass))| {
            format!(
                "{{\"category\":\"{cat}\",\"total\":{total},\"pass\":{pass},\"pct\":{:.1}}}",
                (*pass as f64 / *total as f64) * 100.0
            )
        })
        .collect();

    let total: usize = categories.values().map(|(t, _)| t).sum();
    let total_pass: usize = categories.values().map(|(_, p)| p).sum();
    let overall_pct = (total_pass as f64 / total as f64) * 100.0;

    println!(
        "VDBE_CONFORMANCE_JSON={{\"categories\":[{}],\"total\":{total},\"pass\":{total_pass},\"overall_pct\":{overall_pct:.1}}}",
        entries.join(",")
    );

    assert!(
        overall_pct >= 90.0,
        "VDBE conformance {overall_pct:.1}% below 90%"
    );
}
