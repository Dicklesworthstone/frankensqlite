#![recursion_limit = "512"]

//! TEMP-schema DML-routing + objects leaf-hunt (pane af49, 2026-08-21): frank vs
//! rusqlite, following the two temp-schema leaves found earlier (bd-y4yjq,
//! bd-ghiey) with a deeper sweep that AVOIDS the known-broken same-name
//! co-occurrence cases — instead: unqualified INSERT/UPDATE/DELETE routing to
//! the TEMP shadow (leaving main.t untouched), CREATE TEMP TABLE AS SELECT from
//! main, a temp index used by a temp-table query, a temp trigger firing on a
//! temp-table write, temp AUTOINCREMENT/rowid, and DROP of the temp shadow
//! restoring the main table for bare-name DML. Post-state compared. Pass = keeper.

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
fn temp_schema_dml_and_objects_match_rusqlite_oracle() {
    asupersync::test_utils::run_test(|| async {
        let f = Connection::open(":memory:").await.unwrap();
        let r = rusqlite::Connection::open_in_memory().unwrap();
        for s in [
            "CREATE TABLE t(id INTEGER PRIMARY KEY, src TEXT)",
            "INSERT INTO t VALUES (1,'main1'),(2,'main2'),(3,'main3')",
            "CREATE TEMP TABLE t(id INTEGER PRIMARY KEY, src TEXT)",
            "INSERT INTO temp.t VALUES (10,'temp10'),(20,'temp20')",
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

        // unqualified INSERT routes to the TEMP shadow
        ex(&f, &r, "INSERT INTO t VALUES (30,'temp30')").await;
        check(
            "insert -> temp",
            fq(&f, "SELECT id,src FROM temp.t ORDER BY id").await,
            rq(&r, "SELECT id,src FROM temp.t ORDER BY id"),
            &mut diffs,
        );
        check(
            "insert leaves main",
            fq(&f, "SELECT id,src FROM main.t ORDER BY id").await,
            rq(&r, "SELECT id,src FROM main.t ORDER BY id"),
            &mut diffs,
        );
        // unqualified UPDATE routes to temp
        ex(&f, &r, "UPDATE t SET src='temp-upd' WHERE id=10").await;
        check(
            "update -> temp",
            fq(&f, "SELECT id,src FROM temp.t ORDER BY id").await,
            rq(&r, "SELECT id,src FROM temp.t ORDER BY id"),
            &mut diffs,
        );
        check(
            "update leaves main",
            fq(&f, "SELECT id,src FROM main.t ORDER BY id").await,
            rq(&r, "SELECT id,src FROM main.t ORDER BY id"),
            &mut diffs,
        );
        // unqualified DELETE routes to temp
        ex(&f, &r, "DELETE FROM t WHERE id=20").await;
        check(
            "delete -> temp",
            fq(&f, "SELECT id,src FROM temp.t ORDER BY id").await,
            rq(&r, "SELECT id,src FROM temp.t ORDER BY id"),
            &mut diffs,
        );
        check(
            "delete leaves main",
            fq(&f, "SELECT count(*) FROM main.t").await,
            rq(&r, "SELECT count(*) FROM main.t"),
            &mut diffs,
        );

        // CREATE TEMP TABLE AS SELECT from main
        ex(
            &f,
            &r,
            "CREATE TEMP TABLE derived AS SELECT id, src FROM main.t WHERE id >= 2",
        )
        .await;
        check(
            "ctas from main",
            fq(&f, "SELECT id,src FROM derived ORDER BY id").await,
            rq(&r, "SELECT id,src FROM derived ORDER BY id"),
            &mut diffs,
        );

        // temp index on a temp table, used by a range query
        ex(&f, &r, "CREATE INDEX temp.ix_derived_id ON derived(id)").await;
        check(
            "temp index query",
            fq(&f, "SELECT id FROM derived WHERE id > 2 ORDER BY id").await,
            rq(&r, "SELECT id FROM derived WHERE id > 2 ORDER BY id"),
            &mut diffs,
        );

        // temp trigger firing on a temp-table write
        ex(&f, &r, "CREATE TEMP TABLE audit(msg TEXT)").await;
        ex(&f, &r, "CREATE TEMP TRIGGER trg AFTER INSERT ON derived BEGIN INSERT INTO audit VALUES ('ins-'||NEW.id); END").await;
        ex(&f, &r, "INSERT INTO derived VALUES (99,'x')").await;
        check(
            "temp trigger fired",
            fq(&f, "SELECT msg FROM audit ORDER BY msg").await,
            rq(&r, "SELECT msg FROM audit ORDER BY msg"),
            &mut diffs,
        );

        // temp table AUTOINCREMENT / rowid behavior
        ex(
            &f,
            &r,
            "CREATE TEMP TABLE seq(id INTEGER PRIMARY KEY AUTOINCREMENT, v TEXT)",
        )
        .await;
        ex(&f, &r, "INSERT INTO seq(v) VALUES ('a'),('b'),('c')").await;
        ex(&f, &r, "DELETE FROM seq WHERE id=3").await;
        ex(&f, &r, "INSERT INTO seq(v) VALUES ('d')").await; // autoinc must not reuse 3
        check(
            "temp autoincrement",
            fq(&f, "SELECT id,v FROM seq ORDER BY id").await,
            rq(&r, "SELECT id,v FROM seq ORDER BY id"),
            &mut diffs,
        );

        // DROP the temp shadow -> bare-name DML now hits main.t
        ex(&f, &r, "DROP TABLE temp.t").await;
        ex(&f, &r, "INSERT INTO t VALUES (4,'main4')").await; // now targets main.t
        check(
            "post-drop insert -> main",
            fq(&f, "SELECT id,src FROM t ORDER BY id").await,
            rq(&r, "SELECT id,src FROM t ORDER BY id"),
            &mut diffs,
        );
        check(
            "post-drop main direct",
            fq(&f, "SELECT id,src FROM main.t ORDER BY id").await,
            rq(&r, "SELECT id,src FROM main.t ORDER BY id"),
            &mut diffs,
        );

        assert!(
            diffs.is_empty(),
            "{} TEMP-schema DML/objects divergence(s) vs rusqlite:\n{}",
            diffs.len(),
            diffs.join("\n")
        );
    });
}
