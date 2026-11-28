#![recursion_limit = "512"]

//! Foreign-key cascade-action leaf-hunt (pane af49, 2026-08-21): frank vs
//! rusqlite over referential actions with PRAGMA foreign_keys=ON — ON DELETE
//! CASCADE, ON DELETE SET NULL, ON DELETE SET DEFAULT, ON UPDATE CASCADE,
//! ON DELETE RESTRICT (immediate rejection), multi-level cascade chains
//! (grandparent->parent->child), a self-referential FK cascade, and a composite
//! (two-column) FK. After each mutation we compare surviving rows / nulled
//! columns / rejection outcome. Pass = coverage keeper; a mismatch is a leaf.

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
fn fk_cascade_actions_match_rusqlite_oracle() {
    asupersync::test_utils::run_test(|| async {
        let f = Connection::open(":memory:").await.unwrap();
        let r = rusqlite::Connection::open_in_memory().unwrap();
        ex(&f, &r, "PRAGMA foreign_keys=ON").await;

        let mut diffs = Vec::new();
        let check =
            |label: &str, fr: Vec<Vec<String>>, rr: Vec<Vec<String>>, d: &mut Vec<String>| {
                if fr != rr {
                    d.push(format!(
                        "  [{label}]\n     frank= {fr:?}\n     stock= {rr:?}"
                    ));
                }
            };

        // ── ON DELETE CASCADE + ON UPDATE CASCADE ──
        for s in [
            "CREATE TABLE p(id INTEGER PRIMARY KEY, name TEXT)",
            "CREATE TABLE c(id INTEGER PRIMARY KEY, pid INTEGER REFERENCES p(id) ON DELETE CASCADE ON UPDATE CASCADE, v TEXT)",
            "INSERT INTO p VALUES (1,'p1'),(2,'p2'),(3,'p3')",
            "INSERT INTO c VALUES (10,1,'c10'),(11,1,'c11'),(12,2,'c12'),(13,3,'c13')",
        ] {
            ex(&f, &r, s).await;
        }
        // update parent id -> cascades to children's pid
        ex(&f, &r, "UPDATE p SET id=100 WHERE id=1").await;
        check(
            "on update cascade",
            fq(&f, "SELECT id,pid FROM c ORDER BY id").await,
            rq(&r, "SELECT id,pid FROM c ORDER BY id"),
            &mut diffs,
        );
        // delete parent -> cascades delete of children
        ex(&f, &r, "DELETE FROM p WHERE id=100").await;
        check(
            "on delete cascade",
            fq(&f, "SELECT id,pid FROM c ORDER BY id").await,
            rq(&r, "SELECT id,pid FROM c ORDER BY id"),
            &mut diffs,
        );

        // ── ON DELETE SET NULL ──
        for s in [
            "CREATE TABLE p2(id INTEGER PRIMARY KEY)",
            "CREATE TABLE c2(id INTEGER PRIMARY KEY, pid INTEGER REFERENCES p2(id) ON DELETE SET NULL)",
            "INSERT INTO p2 VALUES (1),(2)",
            "INSERT INTO c2 VALUES (10,1),(11,1),(12,2)",
        ] {
            ex(&f, &r, s).await;
        }
        ex(&f, &r, "DELETE FROM p2 WHERE id=1").await;
        check(
            "on delete set null",
            fq(&f, "SELECT id,pid FROM c2 ORDER BY id").await,
            rq(&r, "SELECT id,pid FROM c2 ORDER BY id"),
            &mut diffs,
        );

        // ── ON DELETE SET DEFAULT ──
        for s in [
            "CREATE TABLE p3(id INTEGER PRIMARY KEY)",
            "CREATE TABLE c3(id INTEGER PRIMARY KEY, pid INTEGER DEFAULT 2 REFERENCES p3(id) ON DELETE SET DEFAULT)",
            "INSERT INTO p3 VALUES (1),(2)",
            "INSERT INTO c3 VALUES (10,1),(11,1)",
        ] {
            ex(&f, &r, s).await;
        }
        ex(&f, &r, "DELETE FROM p3 WHERE id=1").await;
        check(
            "on delete set default",
            fq(&f, "SELECT id,pid FROM c3 ORDER BY id").await,
            rq(&r, "SELECT id,pid FROM c3 ORDER BY id"),
            &mut diffs,
        );

        // ── ON DELETE RESTRICT (immediate rejection) ──
        for s in [
            "CREATE TABLE p4(id INTEGER PRIMARY KEY)",
            "CREATE TABLE c4(id INTEGER PRIMARY KEY, pid INTEGER REFERENCES p4(id) ON DELETE RESTRICT)",
            "INSERT INTO p4 VALUES (1),(2)",
            "INSERT INTO c4 VALUES (10,1)",
        ] {
            ex(&f, &r, s).await;
        }
        ex(&f, &r, "DELETE FROM p4 WHERE id=1").await; // rejected -> no-op
        check(
            "on delete restrict rejected",
            fq(&f, "SELECT id FROM p4 ORDER BY id").await,
            rq(&r, "SELECT id FROM p4 ORDER BY id"),
            &mut diffs,
        );
        ex(&f, &r, "DELETE FROM p4 WHERE id=2").await; // no children -> allowed
        check(
            "restrict allows free delete",
            fq(&f, "SELECT id FROM p4 ORDER BY id").await,
            rq(&r, "SELECT id FROM p4 ORDER BY id"),
            &mut diffs,
        );

        // ── multi-level cascade chain: gp -> par -> ch ──
        for s in [
            "CREATE TABLE gp(id INTEGER PRIMARY KEY)",
            "CREATE TABLE par(id INTEGER PRIMARY KEY, gpid INTEGER REFERENCES gp(id) ON DELETE CASCADE)",
            "CREATE TABLE ch(id INTEGER PRIMARY KEY, pid INTEGER REFERENCES par(id) ON DELETE CASCADE)",
            "INSERT INTO gp VALUES (1),(2)",
            "INSERT INTO par VALUES (10,1),(11,1),(12,2)",
            "INSERT INTO ch VALUES (100,10),(101,11),(102,12)",
        ] {
            ex(&f, &r, s).await;
        }
        ex(&f, &r, "DELETE FROM gp WHERE id=1").await; // cascades gp->par->ch
        check(
            "multi-level cascade par",
            fq(&f, "SELECT id,gpid FROM par ORDER BY id").await,
            rq(&r, "SELECT id,gpid FROM par ORDER BY id"),
            &mut diffs,
        );
        check(
            "multi-level cascade ch",
            fq(&f, "SELECT id,pid FROM ch ORDER BY id").await,
            rq(&r, "SELECT id,pid FROM ch ORDER BY id"),
            &mut diffs,
        );

        // ── self-referential FK cascade ──
        for s in [
            "CREATE TABLE tree(id INTEGER PRIMARY KEY, parent INTEGER REFERENCES tree(id) ON DELETE CASCADE)",
            "INSERT INTO tree VALUES (1,NULL),(2,1),(3,2),(4,1),(5,4)",
        ] {
            ex(&f, &r, s).await;
        }
        ex(&f, &r, "DELETE FROM tree WHERE id=1").await; // cascades the whole subtree
        check(
            "self-ref cascade",
            fq(&f, "SELECT id,parent FROM tree ORDER BY id").await,
            rq(&r, "SELECT id,parent FROM tree ORDER BY id"),
            &mut diffs,
        );

        // ── composite (two-column) FK ──
        for s in [
            "CREATE TABLE pk2(a INTEGER, b INTEGER, PRIMARY KEY(a,b))",
            "CREATE TABLE fk2(id INTEGER PRIMARY KEY, a INTEGER, b INTEGER, FOREIGN KEY(a,b) REFERENCES pk2(a,b) ON DELETE CASCADE)",
            "INSERT INTO pk2 VALUES (1,1),(1,2),(2,1)",
            "INSERT INTO fk2 VALUES (10,1,1),(11,1,2),(12,2,1)",
        ] {
            ex(&f, &r, s).await;
        }
        ex(&f, &r, "DELETE FROM pk2 WHERE a=1 AND b=1").await;
        check(
            "composite fk cascade",
            fq(&f, "SELECT id,a,b FROM fk2 ORDER BY id").await,
            rq(&r, "SELECT id,a,b FROM fk2 ORDER BY id"),
            &mut diffs,
        );
        // inserting a child with no matching composite parent is rejected
        ex(&f, &r, "INSERT INTO fk2 VALUES (99,9,9)").await;
        check(
            "composite fk insert rejected",
            fq(&f, "SELECT count(*) FROM fk2").await,
            rq(&r, "SELECT count(*) FROM fk2"),
            &mut diffs,
        );

        assert!(
            diffs.is_empty(),
            "{} FK cascade-action divergence(s) vs rusqlite:\n{}",
            diffs.len(),
            diffs.join("\n")
        );
    });
}

/// An inbound FK compares child values with the parent's collation. A NOCASE
/// child can legally reference two distinct BINARY parents with keys "a" and
/// "A". Only one family may change when a parent row is deleted or updated.
#[test]
fn parent_collation_governs_foreign_key_actions() {
    asupersync::test_utils::run_test(|| async {
        for (parent, child) in [("BINARY", "NOCASE"), ("NOCASE", "BINARY")] {
            for action in ["CASCADE", "SET NULL", "SET DEFAULT", "RESTRICT", "NO ACTION"] {
                for change in ["DELETE", "UPDATE"] {
                    let frank = Connection::open(":memory:").await.unwrap();
                    let stock = rusqlite::Connection::open_in_memory().unwrap();
                    let other = if parent == "BINARY" { "a" } else { "other" };
                    let referent = if parent == "BINARY" { "A" } else { "a" };
                    for sql in [
                        "PRAGMA foreign_keys=ON".to_owned(),
                        format!("CREATE TABLE p(k TEXT COLLATE {parent} PRIMARY KEY) WITHOUT ROWID"),
                        format!("CREATE TABLE c(id INTEGER PRIMARY KEY, k TEXT COLLATE {child} DEFAULT 'fallback' REFERENCES p(k) ON DELETE {action} ON UPDATE {action})"),
                        format!("INSERT INTO p VALUES ('A'),('{other}'),('fallback')"),
                        format!("INSERT INTO c VALUES (1,'{referent}'),(2,'{other}')"),
                    ] {
                        stock.execute_batch(&sql).unwrap();
                        frank.execute(&sql).await.unwrap();
                    }
                    let sql = if change == "DELETE" {
                        "DELETE FROM p WHERE k='A'"
                    } else {
                        "UPDATE p SET k='Z' WHERE k='A'"
                    };
                    let stock_result = stock.execute(sql, []).map_err(|e| e.to_string());
                    let frank_result = frank.execute(sql).await.map_err(|e| e.to_string());
                    assert_eq!(frank_result, stock_result, "{parent} {action} {sql}");
                    for probe in ["SELECT id,k FROM c ORDER BY id", "SELECT k FROM p ORDER BY k", "PRAGMA foreign_key_check"] {
                        assert_eq!(fq(&frank, probe).await, rq(&stock, probe), "{parent} {action} {sql}: {probe}");
                    }
                }
            }
        }
    });
}

#[test]
fn composite_fk_keeps_each_parent_collation_across_updates_and_triggers() {
    asupersync::test_utils::run_test(|| async {
        let f = Connection::open(":memory:").await.unwrap();
        let r = rusqlite::Connection::open_in_memory().unwrap();
        for sql in [
            "PRAGMA foreign_keys=ON",
            "CREATE TABLE p(a TEXT COLLATE NOCASE, b TEXT COLLATE BINARY, PRIMARY KEY(a,b))",
            "CREATE TABLE c(id INTEGER PRIMARY KEY,x TEXT COLLATE BINARY,y TEXT COLLATE NOCASE, FOREIGN KEY(x,y) REFERENCES p ON UPDATE CASCADE ON DELETE CASCADE)",
            "CREATE TABLE audit(id, x, y)",
            "CREATE TRIGGER cu AFTER UPDATE ON c BEGIN INSERT INTO audit VALUES (old.id,old.x,new.y); END",
            "INSERT INTO p VALUES ('A','b'),('a','B')",
            "INSERT INTO c VALUES (1,'a','b'),(2,'A','B')",
            // An equivalent spelling is a new stored parent value, but neither
            // CASCADE nor the attached child trigger should run.
            "UPDATE p SET a='a' WHERE b='b'",
            "UPDATE p SET a='new', b='z' WHERE b='b'",
            "DELETE FROM p WHERE b='B'",
        ] {
            r.execute_batch(sql).unwrap();
            f.execute(sql).await.unwrap();
            if sql.starts_with("UPDATE") || sql.starts_with("DELETE") {
                for probe in ["SELECT * FROM c ORDER BY id", "SELECT * FROM audit ORDER BY rowid", "PRAGMA foreign_key_check"] {
                    assert_eq!(fq(&f, probe).await, rq(&r, probe), "{sql}: {probe}");
                }
            }
        }
    });
}

#[test]
fn deferred_fk_and_self_reference_use_parent_collation() {
    asupersync::test_utils::run_test(|| async {
        let f = Connection::open(":memory:").await.unwrap();
        let r = rusqlite::Connection::open_in_memory().unwrap();
        for sql in [
            "PRAGMA foreign_keys=ON",
            "CREATE TABLE p(k TEXT COLLATE NOCASE PRIMARY KEY)",
            "CREATE TABLE c(k TEXT COLLATE BINARY REFERENCES p DEFERRABLE INITIALLY DEFERRED)",
            "INSERT INTO p VALUES ('A')",
            "INSERT INTO c VALUES ('a')",
            "BEGIN",
            "DELETE FROM p",
            "COMMIT",
            "INSERT INTO p VALUES ('a')",
            "COMMIT",
            "CREATE TABLE node(k TEXT COLLATE NOCASE PRIMARY KEY, parent TEXT COLLATE BINARY REFERENCES node(k) ON DELETE RESTRICT)",
            "PRAGMA foreign_keys=OFF",
            "INSERT INTO node VALUES ('X','x')",
            "PRAGMA foreign_keys=ON",
            "DELETE FROM node WHERE k='X'",
        ] {
            let expected = r.execute_batch(sql).map_err(|e| e.to_string());
            let actual = f.execute(sql).await.map(|_| ()).map_err(|e| e.to_string());
            assert_eq!(actual, expected, "{sql}");
        }
        for probe in ["SELECT * FROM p", "SELECT * FROM c", "SELECT * FROM node", "PRAGMA foreign_key_check"] {
            assert_eq!(fq(&f, probe).await, rq(&r, probe), "{probe}");
        }
    });
}
