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

#[test]
fn join_aggregates_over_lookups_match_sqlite() {
    for indexed in [false, true] {
        asupersync::test_utils::run_test(|| async move {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("join_aggregate_lookup.db");
            let f = Connection::open(path.to_str().unwrap()).await.unwrap();
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
                    assert_eq!(
                        ops.iter().filter(|op| *op == "Rewind").count(),
                        1,
                        "{sql}: {ops:?}"
                    );
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

        fn hidden_column_names(args: &[&str]) -> Vec<String> {
            JsonEachVtab::hidden_column_names(args)
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
