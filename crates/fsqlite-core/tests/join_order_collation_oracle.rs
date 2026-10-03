#![recursion_limit = "512"]

//! Join ORDER BY sorts under each key's collation, and a key may name a result
//! alias (bd-673gw).
//!
//! The VDBE join lanes (index lookup, multi-join chain, nested loop) opened
//! their sorter with directions only, so `ORDER BY a.n` on a NOCASE or RTRIM
//! column sorted BINARY, and a bare identifier naming a result alias failed
//! with "no such column". File-backed joins take those lanes; in-memory joins
//! mostly run interpreted, so both are compared with rusqlite.
//!
//! The same oracle also guards out-of-range ORDER BY ordinals in joins and
//! single-table comparisons between TEXT, typeless, BLOB and numeric columns,
//! which already matched stock.

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

const SETUP: &[&str] = &[
    "CREATE TABLE a(id INTEGER PRIMARY KEY, n TEXT COLLATE NOCASE, r TEXT COLLATE RTRIM, p TEXT, \
     k TEXT)",
    "CREATE TABLE b(id INTEGER PRIMARY KEY, aid INTEGER, w TEXT, wn TEXT COLLATE NOCASE)",
    "CREATE TABLE c(id INTEGER PRIMARY KEY, bid INTEGER, v TEXT COLLATE NOCASE)",
    "INSERT INTO a VALUES (1,'b','x ','b','m'),(2,'A','x','A','M'),(3,'a','y','a','n'),\
     (4,'B','x  ','B','N'),(5,'c','w','c',NULL),(6,NULL,NULL,NULL,'o')",
    "INSERT INTO b VALUES (10,1,'q','q'),(11,2,'Q','Q'),(12,3,'r','r'),(13,4,'R','R'),\
     (14,5,'s','S'),(15,6,'t','t'),(16,1,'Q','q')",
    "INSERT INTO c VALUES (20,10,'z'),(21,11,'Z'),(22,12,'y'),(23,13,'Y'),(24,14,'x'),\
     (25,15,'X'),(26,16,'w')",
    // Single-table comparison table: TEXT, typeless, INTEGER, REAL, NUMERIC, BLOB.
    "CREATE TABLE t(id INTEGER PRIMARY KEY, b TEXT, x, i INTEGER, r REAL, n NUMERIC, bl BLOB)",
    "INSERT INTO t VALUES (1,'1',1,'1',1,1,1),(2,'2','2',2,2,'2','2'),(3,'1.0',1.0,1,1.0,1.0,1.0),\
     (4,'abc','abc',4,4,4,x'616263'),(5,' 1','1',1,1,1,' 1'),(6,'01',1,1,1,1,1),\
     (7,NULL,NULL,NULL,NULL,NULL,NULL)",
];

const INDEXES: &[&str] = &[
    "CREATE INDEX b_aid ON b(aid)",
    "CREATE INDEX c_bid ON c(bid)",
    "CREATE INDEX t_x ON t(x)",
    "CREATE INDEX t_b ON t(b)",
    "CREATE INDEX t_bl ON t(bl)",
];

/// Join ORDER BY shapes. Every query orders totally, so rows compare in order.
const JOIN_QUERIES: &[&str] = &[
    // Declared collations of the key column.
    "SELECT a.n, b.id FROM a JOIN b ON b.aid = a.id ORDER BY a.n, b.id",
    "SELECT a.r, b.id FROM a JOIN b ON b.aid = a.id ORDER BY a.r, b.id",
    "SELECT a.n, b.id FROM a JOIN b ON b.aid = a.id ORDER BY a.n DESC, b.id",
    "SELECT a.n, b.id FROM a LEFT JOIN b ON b.aid = a.id ORDER BY a.n, b.id",
    "SELECT b.wn, a.id, b.id FROM a JOIN b ON b.aid = a.id ORDER BY b.wn, a.id, b.id",
    "SELECT a.n, b.id FROM a, b WHERE b.aid = a.id ORDER BY a.n, b.id",
    // Explicit COLLATE overrides the declared one, either way.
    "SELECT a.p, b.id FROM a JOIN b ON b.aid = a.id ORDER BY a.p COLLATE NOCASE, b.id",
    "SELECT a.n, b.id FROM a JOIN b ON b.aid = a.id ORDER BY a.n COLLATE BINARY, b.id",
    "SELECT b.w, a.id, b.id FROM a JOIN b ON b.aid = a.id \
     ORDER BY b.w COLLATE NOCASE DESC, a.id, b.id",
    "SELECT a.r, b.id FROM a JOIN b ON b.aid = a.id ORDER BY a.r COLLATE BINARY, b.id",
    // Ordinals and aliases take the result column's collation.
    "SELECT a.n, b.id FROM a JOIN b ON b.aid = a.id ORDER BY 1, 2",
    "SELECT a.n, b.id FROM a LEFT JOIN b ON b.aid = a.id ORDER BY 1 DESC, 2",
    "SELECT a.n, b.id FROM a JOIN b ON b.aid = a.id ORDER BY 1 COLLATE BINARY, 2",
    "SELECT a.n AS key, b.id FROM a JOIN b ON b.aid = a.id ORDER BY key, b.id",
    "SELECT a.n AS key, b.id AS bid FROM a JOIN b ON b.aid = a.id ORDER BY key DESC, bid DESC",
    "SELECT a.r AS rr, b.id FROM a LEFT JOIN b ON b.aid = a.id ORDER BY rr, 2",
    // An alias that is also a column name resolves to the result column.
    "SELECT a.p AS n, b.id FROM a JOIN b ON b.aid = a.id ORDER BY n, b.id",
    "SELECT a.n AS p, b.id FROM a JOIN b ON b.aid = a.id ORDER BY p, b.id",
    // `*` ordinals: a.n is result column 2.
    "SELECT * FROM a JOIN b ON b.aid = a.id ORDER BY 2, 6",
    "SELECT a.*, b.id FROM a JOIN b ON b.aid = a.id ORDER BY 3, 6",
    // Expressions drop or keep the collation as SQLite does.
    "SELECT a.r || '', b.id FROM a JOIN b ON b.aid = a.id ORDER BY 1, 2",
    "SELECT a.n, b.id FROM a JOIN b ON b.aid = a.id ORDER BY +a.n, b.id",
    "SELECT a.n, b.id FROM a JOIN b ON b.aid = a.id ORDER BY lower(a.n), a.n COLLATE BINARY, b.id",
    "SELECT a.n, b.id FROM a JOIN b ON b.aid = a.id ORDER BY a.n, b.id LIMIT 3",
    "SELECT a.n, b.id FROM a JOIN b ON b.aid = a.id ORDER BY a.n, b.id LIMIT 3 OFFSET 2",
    // Three-table chains.
    "SELECT a.n, c.v, c.id FROM a JOIN b ON b.aid = a.id JOIN c ON c.bid = b.id \
     ORDER BY c.v, a.n, c.id",
    "SELECT a.n, c.v, c.id FROM a JOIN b ON b.aid = a.id JOIN c ON c.bid = b.id \
     ORDER BY 2 DESC, 1, 3",
    "SELECT a.n AS an, c.v AS cv, c.id FROM a JOIN b ON b.aid = a.id LEFT JOIN c ON c.bid = b.id \
     ORDER BY an, cv, c.id",
    // Out-of-range ordinals are errors.
    "SELECT a.n, b.id FROM a JOIN b ON b.aid = a.id ORDER BY 3",
    "SELECT a.n, b.id FROM a LEFT JOIN b ON b.aid = a.id ORDER BY 0",
    "SELECT a.n, b.id FROM a JOIN b ON b.aid = a.id ORDER BY -1",
    "SELECT a.n, b.id FROM a JOIN b ON b.aid = a.id JOIN c ON c.bid = b.id ORDER BY 9",
];

/// Single-table comparisons between columns of different affinity.
fn comparison_queries() -> Vec<String> {
    let cols = ["b", "x", "i", "r", "n", "bl"];
    let mut queries = Vec::new();
    for left in cols {
        for right in cols {
            if left == right {
                continue;
            }
            queries.push(format!("SELECT id FROM t WHERE {left} = {right} ORDER BY id"));
            queries.push(format!("SELECT id FROM t WHERE {left} < {right} ORDER BY id"));
            queries.push(format!("SELECT id, {left} = {right} FROM t ORDER BY id"));
        }
    }
    for (left, right) in [("b", "x"), ("x", "b"), ("b", "bl"), ("i", "x"), ("x", "n")] {
        queries.push(format!(
            "SELECT t1.id, t2.id FROM t t1 JOIN t t2 ON t1.{left} = t2.{right} ORDER BY 1, 2"
        ));
        queries.push(format!(
            "SELECT t1.id, t2.id FROM t t1 LEFT JOIN t t2 ON t2.{right} = t1.{left} ORDER BY 1, 2"
        ));
    }
    queries
}

async fn franken_rows(conn: &Connection, sql: &str) -> Result<Vec<Vec<String>>, String> {
    conn.query(sql)
        .await
        .map(|rows| {
            rows.iter()
                .map(|row| row.values().iter().map(tag_f).collect())
                .collect()
        })
        .map_err(|error| error.to_string())
}

fn stock_rows(conn: &rusqlite::Connection, sql: &str) -> Result<Vec<Vec<String>>, String> {
    let mut stmt = conn.prepare(sql).map_err(|error| error.to_string())?;
    let ncol = stmt.column_count();
    stmt.query_map([], |row| {
        Ok((0..ncol)
            .map(|i| tag_r(&row.get::<_, rusqlite::types::Value>(i).unwrap()))
            .collect())
    })
    .map_err(|error| error.to_string())?
    .collect::<Result<_, _>>()
    .map_err(|error| error.to_string())
}

/// Rows must match exactly; errors must both be errors, and an out-of-range
/// ordinal must say so as SQLite does.
async fn mismatches(f: &Connection, r: &rusqlite::Connection, label: &str) -> Vec<String> {
    let mut found = Vec::new();
    let queries = JOIN_QUERIES
        .iter()
        .map(|sql| (*sql).to_owned())
        .chain(comparison_queries());
    for sql in queries {
        let ff = franken_rows(f, &sql).await;
        let rr = stock_rows(r, &sql);
        let same = match (&ff, &rr) {
            (Ok(f_rows), Ok(r_rows)) => f_rows == r_rows,
            (Err(f_err), Err(r_err)) => {
                !r_err.contains("ORDER BY term out of range")
                    || f_err.contains("ORDER BY term out of range")
            }
            _ => false,
        };
        if !same {
            found.push(format!("[{label}] {sql}\n  fsqlite: {ff:?}\n  stock:   {rr:?}"));
        }
    }
    found
}

/// A LEFT join of a number to an indexed typeless column checks once per
/// statement whether the index holds TEXT keys. With none (numbers, BLOBs and
/// NULLs only) it must still match numeric keys, and once a TEXT key such as
/// '2' arrives the next statement must find it.
#[test]
fn left_join_typeless_index_text_probe_tracks_index_contents() {
    asupersync::test_utils::run_test(|| async {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("join_typeless_text_probe.db");
        let f = Connection::open(path.to_str().unwrap()).await.unwrap();
        let r = rusqlite::Connection::open_in_memory().unwrap();
        let left = "SELECT parent.id, child.id FROM parent LEFT JOIN child \
                    ON child.parent_id = parent.id ORDER BY 1, 2";
        let steps: &[&[&str]] = &[
            // An empty child index.
            &[
                "CREATE TABLE parent(id INTEGER PRIMARY KEY, x)",
                "CREATE TABLE child(id INTEGER PRIMARY KEY, parent_id, y)",
                "CREATE INDEX child_p ON child(parent_id)",
                "INSERT INTO parent VALUES (1,7),(2,14),(3,21),(4,28)",
            ],
            // Numbers, a BLOB and a NULL, no TEXT.
            &["INSERT INTO child VALUES (10,1,0),(11,3.0,0),(12,x'32',0),(13,NULL,0)"],
            // A TEXT key that equals 2 under NUMERIC affinity.
            &["INSERT INTO child VALUES (20,'2',0)"],
            // More TEXT, one matching and one not.
            &["INSERT INTO child VALUES (30,' 4',0),(31,'abc',0)"],
            // Back to no TEXT keys.
            &["DELETE FROM child WHERE id IN (20, 30, 31)"],
        ];
        for (step, sqls) in steps.iter().enumerate() {
            for sql in *sqls {
                f.execute(sql).await.unwrap();
                r.execute(sql, []).unwrap();
            }
            assert_eq!(
                franken_rows(&f, left).await,
                stock_rows(&r, left),
                "mismatch after step {step}"
            );
        }
    });
}

#[test]
fn join_order_by_collations_and_aliases_match_sqlite() {
    for file_backed in [false, true] {
        for indexed in [false, true] {
            asupersync::test_utils::run_test(|| async move {
                let dir = tempfile::tempdir().unwrap();
                let path = dir.path().join("join_order_collation.db");
                let f = if file_backed {
                    Connection::open(path.to_str().unwrap()).await.unwrap()
                } else {
                    Connection::open(":memory:").await.unwrap()
                };
                let r = rusqlite::Connection::open_in_memory().unwrap();
                let ddl = SETUP
                    .iter()
                    .chain(if indexed { INDEXES } else { &[] }.iter());
                for sql in ddl {
                    f.execute(sql).await.unwrap();
                    r.execute_batch(sql).unwrap();
                }
                let label = format!("file_backed={file_backed} indexed={indexed}");
                let found = mismatches(&f, &r, &label).await;
                assert!(
                    found.is_empty(),
                    "{} mismatches vs stock:\n{}",
                    found.len(),
                    found.join("\n")
                );
            });
        }
    }
}
