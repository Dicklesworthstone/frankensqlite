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
