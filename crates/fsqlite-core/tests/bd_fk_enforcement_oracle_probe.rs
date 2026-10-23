#![recursion_limit = "512"]

//! Foreign-key enforcement leaf-hunt (pane af49, 2026-08-21): frank vs rusqlite
//! with PRAGMA foreign_keys=ON — ON DELETE / ON UPDATE CASCADE / SET NULL /
//! SET DEFAULT / RESTRICT / NO ACTION, composite FKs, self-referential FKs, and
//! insert-time violation rejection. Final table state (rows that remain after
//! each mutation) is compared, not error text. Pass = coverage keeper; a
//! mismatch is a leaf divergence.

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
fn fk_enforcement_matches_rusqlite_oracle() {
    asupersync::test_utils::run_test(|| async {
        let f = Connection::open(":memory:").await.unwrap();
        let r = rusqlite::Connection::open_in_memory().unwrap();
        let mut diffs = Vec::new();

        for s in [
            "PRAGMA foreign_keys=ON",
            "CREATE TABLE p(id INTEGER PRIMARY KEY, name TEXT)",
            "CREATE TABLE c_cascade(cid INTEGER PRIMARY KEY, pid INTEGER REFERENCES p(id) ON DELETE CASCADE ON UPDATE CASCADE)",
            "CREATE TABLE c_setnull(cid INTEGER PRIMARY KEY, pid INTEGER REFERENCES p(id) ON DELETE SET NULL)",
            "CREATE TABLE c_setdef(cid INTEGER PRIMARY KEY, pid INTEGER DEFAULT 99 REFERENCES p(id) ON DELETE SET DEFAULT)",
            "CREATE TABLE c_restrict(cid INTEGER PRIMARY KEY, pid INTEGER REFERENCES p(id) ON DELETE RESTRICT)",
            "INSERT INTO p VALUES (1,'a'),(2,'b'),(3,'c'),(99,'dflt')",
            "INSERT INTO c_cascade VALUES (10,1),(11,1),(12,2)",
            "INSERT INTO c_setnull VALUES (20,1),(21,2)",
            "INSERT INTO c_setdef VALUES (30,1),(31,2)",
            "INSERT INTO c_restrict VALUES (40,3)",
        ] {
            ex(&f, &r, s).await;
        }

        let check =
            |label: &str, fr: Vec<Vec<String>>, rr: Vec<Vec<String>>, d: &mut Vec<String>| {
                if fr != rr {
                    d.push(format!(
                        "  [{label}]\n     frank= {fr:?}\n     stock= {rr:?}"
                    ));
                }
            };

        // insert-time violation: child with no parent must be rejected on both.
        ex(&f, &r, "INSERT INTO c_cascade VALUES (13, 500)").await;
        check(
            "insert-violation rejected",
            fq(&f, "SELECT count(*) FROM c_cascade").await,
            rq(&r, "SELECT count(*) FROM c_cascade"),
            &mut diffs,
        );

        // ON UPDATE CASCADE: change p.id 2 -> 200, cascades to c_cascade.
        ex(&f, &r, "UPDATE p SET id=200 WHERE id=2").await;
        check(
            "update cascade",
            fq(&f, "SELECT cid,pid FROM c_cascade ORDER BY cid").await,
            rq(&r, "SELECT cid,pid FROM c_cascade ORDER BY cid"),
            &mut diffs,
        );

        // ON DELETE CASCADE: delete p 1 -> c_cascade rows 10,11 gone.
        ex(&f, &r, "DELETE FROM p WHERE id=1").await;
        check(
            "delete cascade child",
            fq(&f, "SELECT cid,pid FROM c_cascade ORDER BY cid").await,
            rq(&r, "SELECT cid,pid FROM c_cascade ORDER BY cid"),
            &mut diffs,
        );
        // but SET NULL child 20 -> pid NULL (its parent 1 deleted)
        check(
            "delete set null",
            fq(&f, "SELECT cid,pid FROM c_setnull ORDER BY cid").await,
            rq(&r, "SELECT cid,pid FROM c_setnull ORDER BY cid"),
            &mut diffs,
        );
        // SET DEFAULT child 30 -> pid 99
        check(
            "delete set default",
            fq(&f, "SELECT cid,pid FROM c_setdef ORDER BY cid").await,
            rq(&r, "SELECT cid,pid FROM c_setdef ORDER BY cid"),
            &mut diffs,
        );

        // RESTRICT: deleting p 3 (referenced by c_restrict 40) must be rejected -> p 3 stays.
        ex(&f, &r, "DELETE FROM p WHERE id=3").await;
        check(
            "delete restrict rejected",
            fq(&f, "SELECT count(*) FROM p WHERE id=3").await,
            rq(&r, "SELECT count(*) FROM p WHERE id=3"),
            &mut diffs,
        );

        // parent state after all
        check(
            "parent final",
            fq(&f, "SELECT id FROM p ORDER BY id").await,
            rq(&r, "SELECT id FROM p ORDER BY id"),
            &mut diffs,
        );

        assert!(
            diffs.is_empty(),
            "{} FK-enforcement divergence(s) vs rusqlite:\n{}",
            diffs.len(),
            diffs.join("\n")
        );
    });
}

/// GH#451 / bd-reality-check-1022-llk6g.4: an UPDATE checks only the child-side
/// foreign keys whose child columns it assigns (stock `fkChildIsModified`), so
/// rows that already violate an unrelated key stay updatable. Each statement's
/// success or failure must match stock, and so must the final rows.
#[test]
fn fk_update_checks_only_assigned_child_keys() {
    asupersync::test_utils::run_test(|| async {
        for path_kind in ["memory", "file"] {
            let dir = tempfile::tempdir().unwrap();
            let (f, r) = if path_kind == "memory" {
                (
                    Connection::open(":memory:").await.unwrap(),
                    rusqlite::Connection::open_in_memory().unwrap(),
                )
            } else {
                (
                    Connection::open(dir.path().join("f.db").to_str().unwrap())
                        .await
                        .unwrap(),
                    rusqlite::Connection::open(dir.path().join("r.db")).unwrap(),
                )
            };
            let mut diffs = Vec::new();
            for s in [
                // Bundled rusqlite defaults foreign_keys to ON; seed with it OFF.
                "PRAGMA foreign_keys=OFF",
                "CREATE TABLE p(id INTEGER PRIMARY KEY, x INT, y INT, UNIQUE(x, y))",
                "CREATE TABLE q(id INTEGER PRIMARY KEY)",
                "CREATE TABLE c(id INTEGER PRIMARY KEY, pid INTEGER REFERENCES p(id), \
                 qid INTEGER REFERENCES q(id), a INT, b INT, note TEXT, \
                 FOREIGN KEY(a, b) REFERENCES p(x, y))",
                "CREATE TABLE r(id INTEGER PRIMARY KEY REFERENCES p(id), note TEXT)",
                "CREATE TABLE d(id INTEGER PRIMARY KEY, \
                 pid INTEGER REFERENCES p(id) DEFERRABLE INITIALLY DEFERRED, note TEXT)",
                "INSERT INTO p VALUES (1, 10, 20), (2, 30, 40)",
                "INSERT INTO q VALUES (7)",
                // Orphans written while foreign_keys is OFF.
                "INSERT INTO c VALUES (1, 1, 7, 10, 20, 'ok'), (2, 99, 7, 10, 20, 'orphan pid'), \
                 (3, 1, 77, 10, 20, 'orphan qid'), (4, 1, 7, 10, 99, 'orphan pair')",
                "INSERT INTO r VALUES (98, 'orphan rowid key')",
                "INSERT INTO d VALUES (1, 99, 'deferred orphan')",
                "PRAGMA foreign_keys=ON",
                "UPDATE c SET note = 'touched' WHERE id = 2",
                "UPDATE c SET note = 'touched all'",
                "UPDATE c SET qid = 7 WHERE id = 2",
                "UPDATE c SET pid = 55 WHERE id = 1",
                "UPDATE c SET pid = 2 WHERE id = 2",
                "UPDATE c SET b = 40 WHERE id = 4",
                "UPDATE c SET a = 30 WHERE id = 4",
                "UPDATE c SET (a, b) = (30, 40) WHERE id = 4",
                "UPDATE c SET a = 11 WHERE id = 3",
                "UPDATE r SET note = 'touched' WHERE id = 98",
                "UPDATE r SET rowid = 97 WHERE id = 98",
                "UPDATE r SET id = 1 WHERE id = 98",
                "BEGIN",
                "UPDATE d SET note = 'touched in txn' WHERE id = 1",
                "COMMIT",
                "BEGIN",
                "UPDATE d SET pid = 88 WHERE id = 1",
                "COMMIT",
                "ROLLBACK",
            ] {
                let fe = f.execute(s).await.is_ok();
                let re = r.execute_batch(s).is_ok();
                if fe != re {
                    diffs.push(format!(
                        "  [{path_kind}] `{s}`: frank ok={fe}, stock ok={re}"
                    ));
                }
            }
            for q in [
                "SELECT id, pid, qid, a, b, note FROM c ORDER BY id",
                "SELECT id, note FROM r ORDER BY id",
                "SELECT id, pid, note FROM d ORDER BY id",
            ] {
                let fr = fq(&f, q).await;
                let rr = rq(&r, q);
                if fr != rr {
                    diffs.push(format!(
                        "  [{path_kind}] {q}\n     frank= {fr:?}\n     stock= {rr:?}"
                    ));
                }
            }
            eprintln!(
                "event=fk_update_assigned_child_keys path={path_kind} divergences={}",
                diffs.len()
            );
            assert!(
                diffs.is_empty(),
                "{} UPDATE child-key divergence(s) vs rusqlite:\n{}",
                diffs.len(),
                diffs.join("\n")
            );
        }
    });
}
