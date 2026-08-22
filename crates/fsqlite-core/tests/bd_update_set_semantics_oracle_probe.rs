#![recursion_limit = "512"]

//! UPDATE SET evaluation-semantics leaf-hunt (pane af49, 2026-08-21): frank vs
//! rusqlite over the rule that every SET right-hand-side in a single UPDATE is
//! evaluated against the OLD row image — so `SET a=a+1, b=a` gives b the OLD a
//! (not the just-assigned new a), and `SET a=b, b=a` swaps the two columns.
//! Also: SET referencing multiple columns, a correlated scalar subquery in SET,
//! a WHERE that references the column being updated (evaluated against OLD),
//! updating the same column twice is a parse error on both, and a generated
//! column recomputing from the new base values. Post-update state compared.
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
        Ok(rows) => rows
            .iter()
            .map(|r| r.values().iter().map(tag_f).collect())
            .collect(),
        Err(_) => vec![vec!["ERR".to_owned()]],
    }
}
fn rq(conn: &rusqlite::Connection, sql: &str) -> Vec<Vec<String>> {
    let Ok(mut st) = conn.prepare(sql) else {
        return vec![vec!["ERR".to_owned()]];
    };
    let n = st.column_count();
    match st.query_map([], |row| {
        Ok((0..n)
            .map(|i| tag_r(&row.get_unwrap::<_, rusqlite::types::Value>(i)))
            .collect::<Vec<_>>())
    }) {
        Ok(rows) => rows
            .collect::<Result<Vec<_>, _>>()
            .unwrap_or_else(|_| vec![vec!["ERR".to_owned()]]),
        Err(_) => vec![vec!["ERR".to_owned()]],
    }
}
async fn ex(f: &Connection, r: &rusqlite::Connection, sql: &str) {
    let _ = f.execute(sql).await;
    let _ = r.execute(sql, []);
}

#[test]
fn update_set_semantics_match_rusqlite_oracle() {
    asupersync::test_utils::run_test(|| async {
        let f = Connection::open(":memory:").await.unwrap();
        let r = rusqlite::Connection::open_in_memory().unwrap();
        let mut diffs = Vec::new();
        let check =
            |label: &str, fr: Vec<Vec<String>>, rr: Vec<Vec<String>>, d: &mut Vec<String>| {
                if fr != rr {
                    d.push(format!(
                        "  [{label}]\n     frank= {fr:?}\n     stock= {rr:?}"
                    ));
                }
            };

        // b = OLD a (not the new a) when both are set in one statement
        for s in [
            "CREATE TABLE t(id INTEGER PRIMARY KEY, a INTEGER, b INTEGER)",
            "INSERT INTO t VALUES (1,10,0),(2,20,0)",
        ] {
            ex(&f, &r, s).await;
        }
        ex(&f, &r, "UPDATE t SET a=a+1, b=a").await;
        check(
            "b gets old a",
            fq(&f, "SELECT id,a,b FROM t ORDER BY id").await,
            rq(&r, "SELECT id,a,b FROM t ORDER BY id"),
            &mut diffs,
        );

        // column swap using old values
        for s in [
            "CREATE TABLE sw(id INTEGER PRIMARY KEY, a INTEGER, b INTEGER)",
            "INSERT INTO sw VALUES (1,1,2),(2,3,4)",
        ] {
            ex(&f, &r, s).await;
        }
        ex(&f, &r, "UPDATE sw SET a=b, b=a").await;
        check(
            "swap old values",
            fq(&f, "SELECT id,a,b FROM sw ORDER BY id").await,
            rq(&r, "SELECT id,a,b FROM sw ORDER BY id"),
            &mut diffs,
        );

        // SET expression referencing several columns, all OLD
        for s in [
            "CREATE TABLE m(id INTEGER PRIMARY KEY, a INTEGER, b INTEGER, c INTEGER)",
            "INSERT INTO m VALUES (1,2,3,4)",
        ] {
            ex(&f, &r, s).await;
        }
        ex(&f, &r, "UPDATE m SET a=b+c, b=a*10, c=a+b+c").await; // all use old a=2,b=3,c=4
        check(
            "multi-col old refs",
            fq(&f, "SELECT a,b,c FROM m").await,
            rq(&r, "SELECT a,b,c FROM m"),
            &mut diffs,
        );

        // WHERE references the updated column -> evaluated against OLD image
        for s in [
            "CREATE TABLE w(id INTEGER PRIMARY KEY, v INTEGER)",
            "INSERT INTO w VALUES (1,5),(2,15),(3,25)",
        ] {
            ex(&f, &r, s).await;
        }
        ex(&f, &r, "UPDATE w SET v=v+100 WHERE v > 10").await;
        check(
            "where old image",
            fq(&f, "SELECT id,v FROM w ORDER BY id").await,
            rq(&r, "SELECT id,v FROM w ORDER BY id"),
            &mut diffs,
        );

        // correlated scalar subquery in SET
        for s in [
            "CREATE TABLE p(id INTEGER PRIMARY KEY, base INTEGER)",
            "CREATE TABLE q(pid INTEGER, amt INTEGER)",
            "INSERT INTO p VALUES (1,0),(2,0),(3,0)",
            "INSERT INTO q VALUES (1,10),(1,20),(2,5)",
        ] {
            ex(&f, &r, s).await;
        }
        ex(
            &f,
            &r,
            "UPDATE p SET base = (SELECT COALESCE(sum(amt),0) FROM q WHERE q.pid=p.id)",
        )
        .await;
        check(
            "subquery in set",
            fq(&f, "SELECT id,base FROM p ORDER BY id").await,
            rq(&r, "SELECT id,base FROM p ORDER BY id"),
            &mut diffs,
        );

        // generated column recomputes from new base values
        for s in [
            "CREATE TABLE g(id INTEGER PRIMARY KEY, base INTEGER, dbl INTEGER GENERATED ALWAYS AS (base*2) VIRTUAL)",
            "INSERT INTO g(id,base) VALUES (1,5),(2,7)",
        ] {
            ex(&f, &r, s).await;
        }
        ex(&f, &r, "UPDATE g SET base=base+10").await;
        check(
            "gencol after update",
            fq(&f, "SELECT id,base,dbl FROM g ORDER BY id").await,
            rq(&r, "SELECT id,base,dbl FROM g ORDER BY id"),
            &mut diffs,
        );

        // SET to DEFAULT-like literal / arithmetic with NULL
        for s in [
            "CREATE TABLE n(id INTEGER PRIMARY KEY, a INTEGER, b INTEGER)",
            "INSERT INTO n VALUES (1,10,NULL)",
        ] {
            ex(&f, &r, s).await;
        }
        ex(&f, &r, "UPDATE n SET a=a+b, b=a").await; // a=10+NULL=NULL; b=old a=10
        check(
            "null arithmetic in set",
            fq(&f, "SELECT a,b FROM n").await,
            rq(&r, "SELECT a,b FROM n"),
            &mut diffs,
        );

        // updating the same column twice is a parse error on both engines
        check(
            "dup column set errors",
            fq(&f, "UPDATE t SET a=1, a=2").await,
            rq(&r, "UPDATE t SET a=1, a=2"),
            &mut diffs,
        );

        assert!(
            diffs.is_empty(),
            "{} UPDATE-SET-semantics divergence(s) vs rusqlite:\n{}",
            diffs.len(),
            diffs.join("\n")
        );
    });
}
