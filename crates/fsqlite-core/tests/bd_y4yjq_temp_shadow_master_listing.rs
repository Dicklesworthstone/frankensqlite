#![recursion_limit = "512"]

//! bd-y4yjq regression: after `CREATE TEMP TABLE t` shadows a same-named main
//! table, `main.sqlite_master` must STILL list the main `t` (the shadowed main
//! table is parked in shadowed_main_tables but remains a real main object).
//! Frank formerly omitted it (build_sqlite_master_rows only iterated self.schema,
//! where the temp entry took over the name and got filtered). Asserted against
//! the rusqlite oracle. Also checks sqlite_temp_master and cross-checks that the
//! main table stays directly queryable.

use fsqlite_core::connection::Connection;
use fsqlite_types::value::SqliteValue;

#[test]
fn temp_renames_preserve_the_persistent_table_and_its_dependents_after_reopen() {
    asupersync::test_utils::run_test(|| async {
        let dir = tempfile::tempdir().unwrap();
        for native_origin in [false, true] {
            let path = dir.path().join(format!("rename-{native_origin}.db"));
            let persistent = [
                "CREATE TABLE t(m INTEGER PRIMARY KEY)",
                "INSERT INTO t VALUES(123)",
                "CREATE TABLE audit(i INTEGER)",
                "CREATE TRIGGER tr AFTER INSERT ON t BEGIN INSERT INTO audit VALUES(NEW.m); END",
                "CREATE VIEW main_view AS SELECT m FROM t",
                "CREATE TABLE child(k INT REFERENCES t(m))",
            ];
            if native_origin {
                let db = Connection::open(path.to_str().unwrap()).await.unwrap();
                for sql in persistent { db.execute(sql).await.unwrap(); }
                db.close().await.unwrap();
            } else {
                let db = rusqlite::Connection::open(&path).unwrap();
                for sql in persistent { db.execute_batch(sql).unwrap(); }
            }
            let main_schema = "SELECT type,name,tbl_name,sql FROM main.sqlite_master ORDER BY type,name";
            let original = rq(&rusqlite::Connection::open(&path).unwrap(), main_schema);
            assert_eq!(original.len(), 5);
            let db = Connection::open(path.to_str().unwrap()).await.unwrap();
            for sql in [
                "CREATE TEMP TABLE t(u TEXT PRIMARY KEY)",
                "CREATE INDEX temp.local_i ON t(u)",
                "INSERT INTO temp.t VALUES('temp-row')",
                "CREATE TEMP VIEW pinned AS SELECT m FROM main.t",
                "CREATE TEMP VIEW follows AS SELECT * FROM temp.t",
                "SAVEPOINT s",
                "ALTER TABLE temp.t RENAME COLUMN u TO j",
                "ALTER TABLE temp.t RENAME TO scratch",
            ] {
                db.execute(sql).await.unwrap();
            }
            let one_temp = vec![vec!["text:temp-row".to_owned()]];
            assert_eq!(fq(&db, "SELECT j FROM temp.scratch INDEXED BY local_i").await, one_temp);
            assert_eq!(fq(&db, "SELECT * FROM follows").await, one_temp);
            let main_row = vec![vec!["int:123".to_owned()]];
            assert_eq!(fq(&db, "SELECT m FROM pinned").await, main_row);
            let pinned_sql = fq(&db, "SELECT sql FROM sqlite_temp_master WHERE name='pinned'").await;
            assert_eq!(pinned_sql, [vec!["text:CREATE VIEW pinned AS SELECT m FROM main.t".to_owned()]]);
            assert_eq!(fq(&db, main_schema).await, original);
            db.execute("ROLLBACK TO s").await.unwrap();
            db.execute("RELEASE s").await.unwrap();
            assert_eq!(fq(&db, "SELECT u FROM temp.t INDEXED BY local_i").await, one_temp);
            assert_eq!(fq(&db, "SELECT u FROM follows").await, one_temp);
            db.execute("SAVEPOINT pinned_trigger").await.unwrap();
            db.execute("CREATE TEMP TRIGGER pinned_tr AFTER INSERT ON main.t BEGIN SELECT NEW.m; END").await.unwrap();
            assert!(db.execute("ALTER TABLE temp.t RENAME TO scratch").await.is_err());
            assert_eq!(fq(&db, "SELECT u FROM temp.t").await, one_temp);
            db.execute("ROLLBACK TO pinned_trigger").await.unwrap();
            db.execute("RELEASE pinned_trigger").await.unwrap();
            db.execute("ALTER TABLE temp.t RENAME COLUMN u TO j").await.unwrap();
            db.execute("ALTER TABLE temp.t RENAME TO scratch").await.unwrap();
            assert_eq!(fq(&db, "SELECT j FROM scratch").await, one_temp);
            assert_eq!(fq(&db, "SELECT m FROM t").await, [vec!["int:123".to_owned()]]);
            db.close().await.unwrap();

            let stock = rusqlite::Connection::open(&path).unwrap();
            assert_eq!(rq(&stock, main_schema), original);
            stock.execute("INSERT INTO t VALUES(456)", []).unwrap();
            assert_eq!(rq(&stock, "SELECT i FROM audit"), [vec!["int:456".to_owned()]]);
            assert_eq!(rq(&stock, "SELECT m FROM main_view ORDER BY m"),
                [vec!["int:123".to_owned()], vec!["int:456".to_owned()]]);
            assert_eq!(rq(&stock, "PRAGMA integrity_check"), [vec!["text:ok".to_owned()]]);
        }
    });
}

#[test]
fn before_and_after_triggers_follow_their_table_across_a_temp_shadow() {
    asupersync::test_utils::run_test(|| async {
        let f = Connection::open(":memory:").await.unwrap();
        let r = rusqlite::Connection::open_in_memory().unwrap();
        for sql in [
            "CREATE TABLE t(x TEXT)", "CREATE TABLE audit(v TEXT)",
            "CREATE TRIGGER m AFTER INSERT ON t BEGIN INSERT INTO audit VALUES('main:'||NEW.x); END",
            "CREATE TEMP TRIGGER early BEFORE INSERT ON t BEGIN INSERT INTO audit VALUES('early:'||NEW.x); END",
            "CREATE TEMP TABLE t(y TEXT)",
            "CREATE TEMP TRIGGER late AFTER INSERT ON t BEGIN INSERT INTO audit VALUES('temp:'||NEW.y); END",
            "CREATE TEMP TRIGGER pin BEFORE INSERT ON main.t BEGIN INSERT INTO audit VALUES('pin:'||NEW.x); END",
            "INSERT INTO temp.t VALUES('a')", "INSERT INTO main.t VALUES('b')", "INSERT INTO t VALUES('c')",
        ] {
            ex(&f, &r, sql).await;
        }
        // SQLite may change the order of same-timing sibling triggers. Check
        // exactly which bodies fired and which owner's NEW columns they read.
        let sql = "SELECT v FROM audit ORDER BY v";
        let expected = rq(&r, sql);
        assert_eq!(expected.len(), 5);
        assert_eq!(fq(&f, sql).await, expected);
        assert_eq!(fq(&f, "SELECT x FROM main.t").await, [vec!["text:b".to_owned()]]);
        assert_eq!(fq(&f, "SELECT y FROM temp.t ORDER BY y").await,
            [vec!["text:a".to_owned()], vec!["text:c".to_owned()]]);
    });
}

#[test]
fn temp_shadow_and_rename_preserve_the_main_autoincrement_high_water() {
    asupersync::test_utils::run_test(|| async {
        let dir = tempfile::tempdir().unwrap();
        for temp_key in ["id INTEGER PRIMARY KEY", "id INTEGER PRIMARY KEY AUTOINCREMENT"] {
            let file = dir.path().join(format!("shadow-{}.db", temp_key.len()));
            let stock = rusqlite::Connection::open(&file).unwrap();
            stock.execute_batch("CREATE TABLE t(id INTEGER PRIMARY KEY AUTOINCREMENT,v);
                INSERT INTO t VALUES(1,'first'),(100,'old'); DELETE FROM t WHERE id=100;").unwrap();
            drop(stock);
            let f = Connection::open(file.to_str().unwrap()).await.unwrap();
            f.execute(&format!("CREATE TEMP TABLE t({temp_key},v)")).await.unwrap();
            f.execute("INSERT INTO temp.t VALUES(8,'temporary')").await.unwrap();
            let stmt = f.prepare("INSERT INTO main.t(v) VALUES('shadowed')").await.unwrap();
            assert_eq!(stmt.execute().await.unwrap(), 1);
            drop(stmt);
            f.execute("ALTER TABLE temp.t RENAME TO moved").await.unwrap();
            f.execute("INSERT INTO main.t(v) VALUES('restored')").await.unwrap();
            f.execute("INSERT INTO moved(v) VALUES('still temporary')").await.unwrap();
            assert_eq!(fq(&f, "SELECT id FROM moved ORDER BY id").await,
                [vec!["int:8".to_owned()], vec!["int:9".to_owned()]]);
            f.close().await.unwrap();
            let stock = rusqlite::Connection::open(&file).unwrap();
            assert_eq!(rq(&stock, "SELECT id FROM t ORDER BY id"),
                [vec!["int:1".to_owned()], vec!["int:101".to_owned()], vec!["int:102".to_owned()]]);
            assert_eq!(rq(&stock, "SELECT name,seq FROM sqlite_sequence"),
                [vec!["text:t".to_owned(), "int:102".to_owned()]]);
        }
    });
}

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
fn temp_shadow_keeps_main_in_sqlite_master() {
    asupersync::test_utils::run_test(|| async {
        let f = Connection::open(":memory:").await.unwrap();
        let r = rusqlite::Connection::open_in_memory().unwrap();
        for s in [
            "CREATE TABLE t(id INTEGER PRIMARY KEY, name TEXT)",
            "CREATE INDEX ix_t_name ON t(name)",
            "CREATE TABLE only_main(id INTEGER, v TEXT)",
            "INSERT INTO t VALUES (1,'main-a'),(2,'main-b')",
            "CREATE TEMP TABLE t(id INTEGER PRIMARY KEY, tag TEXT)", // shadows main.t
            "INSERT INTO temp.t VALUES (10,'temp10')",
            "CREATE TEMP TABLE scratch(x INTEGER)",
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

        // THE FIX: main.sqlite_master lists the shadowed main table `t` + only_main
        check(
            "main tables listed",
            fq(
                &f,
                "SELECT name FROM main.sqlite_master WHERE type='table' ORDER BY name",
            )
            .await,
            rq(
                &r,
                "SELECT name FROM main.sqlite_master WHERE type='table' ORDER BY name",
            ),
            &mut diffs,
        );
        // the main table's index is still listed with tbl_name='t'
        check("main index listed", fq(&f, "SELECT name,tbl_name FROM main.sqlite_master WHERE type='index' AND tbl_name='t' ORDER BY name").await,
              rq(&r, "SELECT name,tbl_name FROM main.sqlite_master WHERE type='index' AND tbl_name='t' ORDER BY name"), &mut diffs);
        // sqlite_temp_master lists the temp objects
        check(
            "temp master",
            fq(
                &f,
                "SELECT name FROM sqlite_temp_master WHERE type='table' ORDER BY name",
            )
            .await,
            rq(
                &r,
                "SELECT name FROM sqlite_temp_master WHERE type='table' ORDER BY name",
            ),
            &mut diffs,
        );
        // the shadowed main table is still directly queryable via main.t
        check(
            "main.t queryable",
            fq(&f, "SELECT id,name FROM main.t ORDER BY id").await,
            rq(&r, "SELECT id,name FROM main.t ORDER BY id"),
            &mut diffs,
        );
        // unqualified t still resolves to the temp shadow
        check(
            "unqualified -> temp",
            fq(&f, "SELECT id,tag FROM t ORDER BY id").await,
            rq(&r, "SELECT id,tag FROM t ORDER BY id"),
            &mut diffs,
        );
        // total table count in main
        check(
            "main table count",
            fq(
                &f,
                "SELECT count(*) FROM main.sqlite_master WHERE type='table'",
            )
            .await,
            rq(
                &r,
                "SELECT count(*) FROM main.sqlite_master WHERE type='table'",
            ),
            &mut diffs,
        );
        // after DROP of the temp shadow, main.sqlite_master still lists t exactly once
        ex(&f, &r, "DROP TABLE temp.t").await;
        check(
            "after drop shadow",
            fq(
                &f,
                "SELECT name FROM main.sqlite_master WHERE type='table' ORDER BY name",
            )
            .await,
            rq(
                &r,
                "SELECT name FROM main.sqlite_master WHERE type='table' ORDER BY name",
            ),
            &mut diffs,
        );

        assert!(
            diffs.is_empty(),
            "{} bd-y4yjq divergence(s) vs rusqlite:\n{}",
            diffs.len(),
            diffs.join("\n")
        );
    });
}
