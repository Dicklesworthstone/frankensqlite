#![recursion_limit = "512"]

//! ALTER TABLE / DDL schema-rewrite leaf-hunt (pane af49, 2026-08-21): frank vs
//! rusqlite over the schema-rewriting corners of ALTER TABLE — RENAME COLUMN
//! (which must propagate the new name into indexes, CHECK constraints, generated
//! columns, views, and triggers that reference the column), RENAME TO (which
//! rewrites the table name inside dependent view/trigger bodies), ADD COLUMN
//! with a DEFAULT / NOT NULL DEFAULT / generated expression, and DROP COLUMN
//! (3.35+, rejected when the column is used by an index/constraint). We compare
//! post-DDL query results AND the rewritten schema text from sqlite_master.
//! Pass = coverage keeper; a mismatch is a leaf divergence.

use fsqlite_core::connection::Connection;
use fsqlite_types::value::SqliteValue;

fn tag_f(v: &SqliteValue) -> String {
    match v {
        SqliteValue::Null => "NULL".to_owned(),
        SqliteValue::Integer(n) => format!("int:{n}"),
        SqliteValue::Float(f) => format!("real:{f}"),
        SqliteValue::Text(s) => format!("text:{s}"),
        SqliteValue::Blob(b) => format!("blob:{b:?}"),
    }
}
fn tag_r(v: &rusqlite::types::Value) -> String {
    match v {
        rusqlite::types::Value::Null => "NULL".to_owned(),
        rusqlite::types::Value::Integer(n) => format!("int:{n}"),
        rusqlite::types::Value::Real(f) => format!("real:{f}"),
        rusqlite::types::Value::Text(s) => format!("text:{s}"),
        rusqlite::types::Value::Blob(b) => format!("blob:{b:?}"),
    }
}

async fn fq(conn: &Connection, sql: &str) -> Vec<Vec<String>> {
    match conn.query(sql).await {
        Ok(rows) => rows.iter().map(|r| r.values().iter().map(tag_f).collect()).collect(),
        Err(_) => vec![vec!["ERR".to_owned()]],
    }
}
fn rq(conn: &rusqlite::Connection, sql: &str) -> Vec<Vec<String>> {
    let Ok(mut st) = conn.prepare(sql) else { return vec![vec!["ERR".to_owned()]] };
    let n = st.column_count();
    match st.query_map([], |row| {
        Ok((0..n).map(|i| tag_r(&row.get_unwrap::<_, rusqlite::types::Value>(i))).collect::<Vec<_>>())
    }) {
        Ok(rows) => rows.collect::<Result<Vec<_>, _>>().unwrap_or_else(|_| vec![vec!["ERR".to_owned()]]),
        Err(_) => vec![vec!["ERR".to_owned()]],
    }
}
async fn ex(f: &Connection, r: &rusqlite::Connection, sql: &str) {
    let _ = f.execute(sql).await;
    let _ = r.execute(sql, []);
}

#[test]
fn alter_table_schema_rewrite_match_rusqlite_oracle() {
    asupersync::test_utils::run_test(|| async {
        let f = Connection::open(":memory:").await.unwrap();
        let r = rusqlite::Connection::open_in_memory().unwrap();
        let mut diffs = Vec::new();
        let check = |label: &str, fr: Vec<Vec<String>>, rr: Vec<Vec<String>>, d: &mut Vec<String>| {
            if fr != rr { d.push(format!("  [{label}]\n     frank= {fr:?}\n     stock= {rr:?}")); }
        };

        // ── RENAME COLUMN: must propagate into index + view + trigger + CHECK ──
        for s in [
            "CREATE TABLE t(a INTEGER PRIMARY KEY, b INTEGER, c TEXT, CHECK (b >= 0))",
            "CREATE INDEX ix_b ON t(b)",
            "CREATE VIEW vt AS SELECT a, b AS bee FROM t WHERE b > 5",
            "CREATE TABLE log(msg TEXT)",
            "CREATE TRIGGER tr AFTER INSERT ON t WHEN NEW.b > 100 BEGIN INSERT INTO log VALUES ('big-'||NEW.b); END",
            "INSERT INTO t VALUES (1,10,'x'),(2,3,'y'),(3,200,'z')",
        ] {
            ex(&f, &r, s).await;
        }
        // rename b -> qty
        ex(&f, &r, "ALTER TABLE t RENAME COLUMN b TO qty").await;
        // new name works in queries, old name gone
        check("rename col new name query", fq(&f, "SELECT a, qty FROM t ORDER BY a").await,
              rq(&r, "SELECT a, qty FROM t ORDER BY a"), &mut diffs);
        check("rename col old name errors", fq(&f, "SELECT b FROM t").await,
              rq(&r, "SELECT b FROM t"), &mut diffs);
        // index still usable under the renamed column
        check("rename col index usable", fq(&f, "SELECT a FROM t WHERE qty > 5 ORDER BY a").await,
              rq(&r, "SELECT a FROM t WHERE qty > 5 ORDER BY a"), &mut diffs);
        // dependent view rewritten to use the new column name internally
        check("rename col view rewritten", fq(&f, "SELECT a, bee FROM vt ORDER BY a").await,
              rq(&r, "SELECT a, bee FROM vt ORDER BY a"), &mut diffs);
        // sqlite_master text for the view/trigger now mentions qty, not b
        check("rename col schema text", fq(&f,
            "SELECT type,name FROM sqlite_master WHERE sql LIKE '%qty%' AND type IN('view','trigger','index','table') ORDER BY type,name").await,
            rq(&r,
            "SELECT type,name FROM sqlite_master WHERE sql LIKE '%qty%' AND type IN('view','trigger','index','table') ORDER BY type,name"), &mut diffs);
        // trigger fires under the renamed column (WHEN NEW.b -> NEW.qty)
        ex(&f, &r, "INSERT INTO t VALUES (4,150,'w')").await;
        check("rename col trigger fires", fq(&f, "SELECT msg FROM log ORDER BY msg").await,
              rq(&r, "SELECT msg FROM log ORDER BY msg"), &mut diffs);
        // CHECK constraint still enforced under the new name (insert violating qty<0 fails)
        ex(&f, &r, "INSERT INTO t VALUES (5,-1,'bad')").await;
        check("rename col check enforced", fq(&f, "SELECT count(*) FROM t").await,
              rq(&r, "SELECT count(*) FROM t"), &mut diffs);

        // ── RENAME TO: rewrites dependent view/trigger bodies ──
        ex(&f, &r, "ALTER TABLE t RENAME TO items").await;
        check("rename table new name", fq(&f, "SELECT a, qty FROM items ORDER BY a").await,
              rq(&r, "SELECT a, qty FROM items ORDER BY a"), &mut diffs);
        check("rename table view follows", fq(&f, "SELECT a, bee FROM vt ORDER BY a").await,
              rq(&r, "SELECT a, bee FROM vt ORDER BY a"), &mut diffs);
        check("rename table old name errors", fq(&f, "SELECT * FROM t").await,
              rq(&r, "SELECT * FROM t"), &mut diffs);

        // ── ADD COLUMN variants ──
        ex(&f, &r, "ALTER TABLE items ADD COLUMN note TEXT DEFAULT 'none'").await;
        check("add col with default", fq(&f, "SELECT a, note FROM items ORDER BY a").await,
              rq(&r, "SELECT a, note FROM items ORDER BY a"), &mut diffs);
        ex(&f, &r, "ALTER TABLE items ADD COLUMN score INTEGER NOT NULL DEFAULT 0").await;
        check("add col not-null default", fq(&f, "SELECT a, score FROM items ORDER BY a").await,
              rq(&r, "SELECT a, score FROM items ORDER BY a"), &mut diffs);
        // generated column via ADD COLUMN
        ex(&f, &r, "ALTER TABLE items ADD COLUMN qty2 INTEGER GENERATED ALWAYS AS (qty*2) VIRTUAL").await;
        check("add generated col", fq(&f, "SELECT a, qty2 FROM items ORDER BY a").await,
              rq(&r, "SELECT a, qty2 FROM items ORDER BY a"), &mut diffs);

        // ── DROP COLUMN (3.35+) ──
        // dropping a free column succeeds
        ex(&f, &r, "ALTER TABLE items DROP COLUMN note").await;
        check("drop free col", fq(&f, "SELECT name FROM pragma_table_info('items') ORDER BY name").await,
              rq(&r, "SELECT name FROM pragma_table_info('items') ORDER BY name"), &mut diffs);
        // dropping a column an index depends on is rejected on both engines
        ex(&f, &r, "ALTER TABLE items DROP COLUMN qty").await;
        check("drop indexed col rejected", fq(&f, "SELECT a, qty FROM items ORDER BY a").await,
              rq(&r, "SELECT a, qty FROM items ORDER BY a"), &mut diffs);

        assert!(diffs.is_empty(), "{} ALTER-TABLE schema-rewrite divergence(s) vs rusqlite:\n{}", diffs.len(), diffs.join("\n"));
    });
}
