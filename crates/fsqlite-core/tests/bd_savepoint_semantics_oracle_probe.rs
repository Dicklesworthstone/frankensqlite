#![recursion_limit = "512"]

//! SAVEPOINT / nested-transaction semantics leaf-hunt (pane af49, 2026-08-21):
//! frank vs rusqlite over single-connection savepoint mechanics — ROLLBACK TO a
//! savepoint undoes writes made since it but leaves the savepoint active (so it
//! can be rolled back to again), RELEASE commits a savepoint's work into the
//! enclosing scope, RELEASE of an OUTER savepoint discards its inner ones, a
//! nested savepoint stack rolls back level-by-level, and a savepoint wrapping a
//! constraint-violating statement. All within an outer BEGIN. Post-state after
//! each step is compared. Pass = coverage keeper; a mismatch is a leaf.

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
fn savepoint_semantics_match_rusqlite_oracle() {
    asupersync::test_utils::run_test(|| async {
        let f = Connection::open(":memory:").await.unwrap();
        let r = rusqlite::Connection::open_in_memory().unwrap();
        for s in [
            "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)",
            "INSERT INTO t VALUES (1,'a')",
        ] {
            ex(&f, &r, s).await;
        }
        let mut diffs = Vec::new();
        let check =
            |label: &str, fr: Vec<Vec<String>>, rr: Vec<Vec<String>>, d: &mut Vec<String>| {
                if fr != rr {
                    d.push(format!(
                        "  [{label}]\n     frank= {fr:?}\n     stock= {rr:?}"
                    ));
                }
            };

        // ── ROLLBACK TO leaves the savepoint active; can roll back to it twice ──
        ex(&f, &r, "SAVEPOINT sp1").await;
        ex(&f, &r, "INSERT INTO t VALUES (2,'b')").await;
        ex(&f, &r, "ROLLBACK TO sp1").await; // undoes id=2, sp1 still open
        check(
            "after first rollback-to",
            fq(&f, "SELECT id,v FROM t ORDER BY id").await,
            rq(&r, "SELECT id,v FROM t ORDER BY id"),
            &mut diffs,
        );
        ex(&f, &r, "INSERT INTO t VALUES (3,'c')").await;
        ex(&f, &r, "ROLLBACK TO sp1").await; // undoes id=3; sp1 still open
        check(
            "after second rollback-to",
            fq(&f, "SELECT id,v FROM t ORDER BY id").await,
            rq(&r, "SELECT id,v FROM t ORDER BY id"),
            &mut diffs,
        );
        ex(&f, &r, "INSERT INTO t VALUES (4,'d')").await;
        ex(&f, &r, "RELEASE sp1").await; // commits id=4 into the outer scope
        check(
            "after release keeps work",
            fq(&f, "SELECT id,v FROM t ORDER BY id").await,
            rq(&r, "SELECT id,v FROM t ORDER BY id"),
            &mut diffs,
        );

        // ── nested savepoints: RELEASE of the OUTER discards the inner ──
        ex(&f, &r, "SAVEPOINT outer").await;
        ex(&f, &r, "INSERT INTO t VALUES (5,'e')").await;
        ex(&f, &r, "SAVEPOINT inner").await;
        ex(&f, &r, "INSERT INTO t VALUES (6,'f')").await;
        ex(&f, &r, "ROLLBACK TO inner").await; // undo id=6
        check(
            "nested rollback inner",
            fq(&f, "SELECT id,v FROM t ORDER BY id").await,
            rq(&r, "SELECT id,v FROM t ORDER BY id"),
            &mut diffs,
        );
        ex(&f, &r, "INSERT INTO t VALUES (7,'g')").await;
        ex(&f, &r, "RELEASE outer").await; // commits id=5 and id=7 (and closes inner)
        check(
            "release outer keeps all",
            fq(&f, "SELECT id,v FROM t ORDER BY id").await,
            rq(&r, "SELECT id,v FROM t ORDER BY id"),
            &mut diffs,
        );

        // ── ROLLBACK TO an outer savepoint undoes an inner savepoint's work too ──
        ex(&f, &r, "SAVEPOINT s_a").await;
        ex(&f, &r, "INSERT INTO t VALUES (8,'h')").await;
        ex(&f, &r, "SAVEPOINT s_b").await;
        ex(&f, &r, "INSERT INTO t VALUES (9,'i')").await;
        ex(&f, &r, "ROLLBACK TO s_a").await; // undoes id=8 AND id=9
        check(
            "rollback outer undoes inner",
            fq(&f, "SELECT id,v FROM t ORDER BY id").await,
            rq(&r, "SELECT id,v FROM t ORDER BY id"),
            &mut diffs,
        );
        ex(&f, &r, "RELEASE s_a").await;

        // ── a savepoint wrapping a constraint violation: the failed stmt is undone,
        //    prior savepoint work survives, and we can still ROLLBACK TO / RELEASE ──
        ex(&f, &r, "SAVEPOINT s_c").await;
        ex(&f, &r, "INSERT INTO t VALUES (10,'j')").await;
        ex(&f, &r, "INSERT INTO t VALUES (1,'dup')").await; // PK conflict -> statement fails
        check(
            "savepoint after failed stmt",
            fq(&f, "SELECT id,v FROM t ORDER BY id").await,
            rq(&r, "SELECT id,v FROM t ORDER BY id"),
            &mut diffs,
        );
        ex(&f, &r, "ROLLBACK TO s_c").await; // undoes id=10
        check(
            "rollback after failed stmt",
            fq(&f, "SELECT id,v FROM t ORDER BY id").await,
            rq(&r, "SELECT id,v FROM t ORDER BY id"),
            &mut diffs,
        );
        ex(&f, &r, "RELEASE s_c").await;

        // final state
        check(
            "final state",
            fq(&f, "SELECT id,v FROM t ORDER BY id").await,
            rq(&r, "SELECT id,v FROM t ORDER BY id"),
            &mut diffs,
        );

        assert!(
            diffs.is_empty(),
            "{} savepoint divergence(s) vs rusqlite:\n{}",
            diffs.len(),
            diffs.join("\n")
        );
    });
}
