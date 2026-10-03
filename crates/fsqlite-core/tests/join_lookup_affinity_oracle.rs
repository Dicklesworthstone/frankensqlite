#![recursion_limit = "512"]

//! Join lookups honor comparison affinity.
//!
//! `t.a = u.b` with `t.a` INTEGER and `u.b` TEXT `'1'` is true in SQLite: the
//! numeric column coerces the text operand. The direct rowid and index lookup
//! lanes used to probe with the raw value, so on a file-backed database they
//! dropped such rows (an index probe of `'1'` misses the integer key `1`) or
//! matched rows SQLite does not (`SeekRowid` truncated `1.5` to rowid 1). This
//! held for row output, for the single-lookup aggregate loop in either FROM
//! order, for grouped joins and for multi-join chains.
//!
//! Every query pairs columns of different affinities and is compared with
//! rusqlite, with and without indexes on the lookup columns. The data holds
//! values that coercion changes: integer-looking text with and without a
//! leading zero, `'1.0'`, a text prefix of a number, reals with and without a
//! fraction, blobs and NULLs.

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
    "CREATE TABLE t(id INTEGER PRIMARY KEY, a INTEGER, b TEXT, c REAL, d, n NUMERIC)",
    "INSERT INTO t VALUES (1,1,'1',1.0,1,1),(2,2,'2',2.5,'2','2'),(3,3,'01',3.0,'x',3.5),\
     (4,NULL,NULL,NULL,NULL,NULL),(5,5,'1.0',1.5,x'35',5),(6,6,'abc',6.0,6.0,'6'),(12,12,'12',12.0,'12',12)",
    "CREATE TABLE u(id INTEGER PRIMARY KEY, a INTEGER, b TEXT, r REAL, d, n NUMERIC)",
    "INSERT INTO u VALUES (1,1,'1',1.5,'1',1.0),(2,2,'01',2.0,2,'2'),(3,3,'2abc',3.0,'3',3),\
     (4,NULL,'12',12.0,NULL,NULL),(5,5,'1.0',5.0,x'31',5.5),(6,6,'abc',6.0,'6',6),(7,12,' 2',2.5,12,'12')",
    "CREATE TABLE v(id INTEGER PRIMARY KEY, k TEXT UNIQUE, m INTEGER UNIQUE)",
    "INSERT INTO v VALUES (1,'1',1),(2,'2',2),(3,'abc',3),(12,'12',12)",
];

const INDEXES: &[&str] = &[
    "CREATE INDEX t_a ON t(a)",
    "CREATE INDEX t_b ON t(b)",
    "CREATE INDEX t_c ON t(c)",
    "CREATE INDEX t_d ON t(d)",
    "CREATE INDEX t_n ON t(n)",
    "CREATE INDEX u_a ON u(a)",
    "CREATE INDEX u_b ON u(b)",
    "CREATE INDEX u_r ON u(r)",
    "CREATE INDEX u_d ON u(d)",
];

const T_COLUMNS: &[&str] = &["id", "a", "b", "c", "d", "n"];
const U_COLUMNS: &[&str] = &["id", "a", "b", "r", "d", "n"];

fn queries() -> Vec<String> {
    let mut queries = Vec::new();
    for tc in T_COLUMNS {
        for uc in U_COLUMNS {
            queries.push(format!("SELECT count(*) FROM u JOIN t ON t.{tc} = u.{uc}"));
            queries.push(format!("SELECT count(*) FROM t JOIN u ON t.{tc} = u.{uc}"));
            queries.push(format!("SELECT sum(t.id), max(u.id) FROM u JOIN t ON t.{tc} = u.{uc}"));
            queries.push(format!(
                "SELECT u.id, t.id FROM u JOIN t ON t.{tc} = u.{uc} ORDER BY 1, 2"
            ));
            queries.push(format!(
                "SELECT u.id, t.id FROM u LEFT JOIN t ON t.{tc} = u.{uc} ORDER BY 1, 2"
            ));
            queries.push(format!(
                "SELECT t.id, count(*) FROM u JOIN t ON t.{tc} = u.{uc} GROUP BY t.id ORDER BY 1"
            ));
        }
    }
    // The grouped count/sum lane (`SELECT k, count(*), sum(x) ... GROUP BY k`):
    // a NUMERIC probe holding 5.5 must not truncate to rowid 5.
    for key in ["t.id", "t.a", "t.c"] {
        queries.push(format!(
            "SELECT u.id, count(*), sum(t.id) FROM u JOIN t ON {key} = u.n GROUP BY u.id"
        ));
        queries.push(format!(
            "SELECT u.id, count(*), sum(t.id) FROM u JOIN t ON {key} = u.r GROUP BY u.id"
        ));
    }
    // Multi-join chains through UNIQUE indexes and the rowid. The typeless
    // probe `u.d` against the TEXT key `v.k` compares without conversion, so
    // integer 2 does not match '2' (bd-y5mc8).
    for probe in ["u.a", "u.b", "u.r", "u.d", "u.n"] {
        queries.push(format!(
            "SELECT u.id, v.id, t.id FROM u JOIN v ON v.k = {probe} JOIN t ON t.id = v.m ORDER BY 1, 2, 3"
        ));
        queries.push(format!(
            "SELECT u.id, v.id, t.id FROM u JOIN v ON v.m = {probe} JOIN t ON t.id = {probe} ORDER BY 1, 2, 3"
        ));
    }
    queries
}

async fn assert_agree(fconn: &Connection, rconn: &rusqlite::Connection, sql: &str) {
    let ff: Vec<Vec<String>> = fconn
        .query(sql)
        .await
        .unwrap_or_else(|e| panic!("franken `{sql}`: {e:?}"))
        .iter()
        .map(|row| row.values().iter().map(tag_f).collect())
        .collect();
    let mut stmt = rconn.prepare(sql).expect("rusqlite prepare");
    let ncol = stmt.column_count();
    let rr: Vec<Vec<String>> = stmt
        .query_map([], |row| {
            Ok((0..ncol)
                .map(|i| tag_r(&row.get::<_, rusqlite::types::Value>(i).unwrap()))
                .collect())
        })
        .expect("rusqlite query")
        .map(|r| r.unwrap())
        .collect();
    assert_eq!(ff, rr, "mismatch on `{sql}`");
}

#[test]
fn join_lookups_apply_comparison_affinity_like_sqlite() {
    for indexed in [false, true] {
        asupersync::test_utils::run_test(|| async move {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("join_lookup_affinity.db");
            let f = Connection::open(path.to_str().unwrap()).await.unwrap();
            let r = rusqlite::Connection::open_in_memory().unwrap();
            let indexes: &[&str] = if indexed { INDEXES } else { &[] };
            for sql in SETUP.iter().chain(indexes) {
                f.execute(sql).await.unwrap();
                r.execute(sql, []).unwrap();
            }
            for sql in queries() {
                assert_agree(&f, &r, &sql).await;
            }
        });
    }
}

/// The two shapes from the review that motivated this file, kept as named
/// regressions: an index probe that needs numeric coercion, and a rowid
/// probe with a fractional real.
#[test]
fn join_lookup_affinity_named_regressions() {
    asupersync::test_utils::run_test(|| async {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("join_lookup_affinity_named.db");
        let f = Connection::open(path.to_str().unwrap()).await.unwrap();
        let r = rusqlite::Connection::open_in_memory().unwrap();
        for sql in [
            "CREATE TABLE t(id INTEGER PRIMARY KEY, a INTEGER)",
            "CREATE INDEX t_a ON t(a)",
            "CREATE TABLE u(b TEXT, r REAL)",
            "INSERT INTO t VALUES (1,1),(2,2)",
            "INSERT INTO u VALUES ('1',1.5),('2',2.0)",
        ] {
            f.execute(sql).await.unwrap();
            r.execute(sql, []).unwrap();
        }
        for sql in [
            "SELECT count(*) FROM t JOIN u ON t.a = u.b",
            "SELECT count(*) FROM u JOIN t ON t.a = u.b",
            "SELECT count(*) FROM t JOIN u ON t.id = u.r",
            "SELECT count(*) FROM u JOIN t ON t.id = u.r",
            "SELECT t.id, u.r FROM u JOIN t ON t.id = u.r ORDER BY 1, 2",
            "SELECT t.id, u.b FROM u JOIN t ON t.a = u.b ORDER BY 1, 2",
        ] {
            assert_agree(&f, &r, sql).await;
        }
    });
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

/// The affinity fix must not turn common join shapes into nested loops: a
/// TEXT key joined to an INTEGER UNIQUE key seeks with the coerced probe, and
/// an untyped foreign-key column drives the join and looks the parent up by
/// rowid, as SQLite does (bd-kr6hf).
#[test]
fn coercible_join_keys_keep_the_index_lookup() {
    asupersync::test_utils::run_test(|| async {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("join_lookup_affinity_lanes.db");
        let f = Connection::open(path.to_str().unwrap()).await.unwrap();
        let r = rusqlite::Connection::open_in_memory().unwrap();
        for sql in [
            "CREATE TABLE ref(id INTEGER PRIMARY KEY, code INTEGER UNIQUE)",
            "CREATE TABLE item(id INTEGER PRIMARY KEY, code TEXT)",
            "CREATE TABLE parent(id INTEGER PRIMARY KEY, x)",
            "CREATE TABLE child(id INTEGER PRIMARY KEY, parent_id, y)",
            "CREATE INDEX child_p ON child(parent_id)",
            "INSERT INTO ref VALUES (1,100001),(2,100002),(3,100003)",
            "INSERT INTO item VALUES (1,'100001'),(2,'100002'),(3,'0100003'),(4,'x'),(5,NULL),\
             (6,'100001')",
            "INSERT INTO parent VALUES (1,7),(2,14),(3,21)",
            "INSERT INTO child VALUES (10,1,0),(11,1,1),(20,2,0),(30,3,0),(40,4,0),(50,NULL,0)",
        ] {
            f.execute(sql).await.unwrap();
            r.execute(sql, []).unwrap();
        }
        let text_key =
            "SELECT item.id, ref.id FROM item JOIN ref ON ref.code = item.code ORDER BY 1, 2";
        let untyped_fk = "SELECT parent.id, child.id FROM parent JOIN child \
                          ON child.parent_id = parent.id ORDER BY 1, 2";
        assert_agree(&f, &r, text_key).await;
        assert_agree(&f, &r, untyped_fk).await;
        let ops = opcodes(&f, text_key).await;
        assert!(
            ops.iter().any(|op| op == "SeekGE"),
            "the TEXT key must seek ref.code: {ops:?}"
        );
        assert!(
            ops.iter().any(|op| op == "Affinity"),
            "the probe takes INTEGER affinity: {ops:?}"
        );
        let ops = opcodes(&f, untyped_fk).await;
        assert!(
            ops.iter().any(|op| op == "SeekRowid")
                && !ops.iter().any(|op| op == "SeekGE")
                && ops.iter().filter(|op| *op == "Rewind").count() == 1,
            "the untyped foreign key must scan child once and seek parent by rowid: {ops:?}"
        );
    });
}
