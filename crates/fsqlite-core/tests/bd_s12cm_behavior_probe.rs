//! Throwaway diagnostic (bd-s12cm / bd-pqauo, lane cc_5 2026-08-22): capture
//! frank's ACTUAL behavior at HEAD for the attached cross-DB write repros and
//! the unqualified-attached-name read, since the bead's root-cause hypothesis
//! (silent delegation + child misresolution) predates code evolution. Prints
//! Ok/Err + resulting state so the fix targets reality, not a stale note.

use fsqlite_core::connection::Connection;
use fsqlite_types::value::SqliteValue;

fn tag(v: &SqliteValue) -> String {
    match v {
        SqliteValue::Null => "NULL".to_owned(),
        SqliteValue::Integer(n) => format!("int:{n}"),
        SqliteValue::Float(f) => format!("real:{f}"),
        SqliteValue::Text(s) => format!("text:{s}"),
        SqliteValue::Blob(b) => format!("blob:{b:?}"),
    }
}

async fn dump(conn: &Connection, sql: &str) -> String {
    match conn.query(sql).await {
        Ok(rows) => rows
            .iter()
            .map(|r| {
                r.values()
                    .iter()
                    .map(tag)
                    .collect::<Vec<_>>()
                    .join(",")
            })
            .collect::<Vec<_>>()
            .join(" | "),
        Err(e) => format!("QUERY-ERR: {e}"),
    }
}

#[test]
fn bd_s12cm_pqauo_behavior_probe() {
    asupersync::test_utils::run_test(|| async {
        let f = Connection::open(":memory:").await.unwrap();
        for s in [
            "ATTACH ':memory:' AS aux",
            "CREATE TABLE t(id INTEGER PRIMARY KEY, name TEXT)",
            "CREATE TABLE aux.t(id INTEGER PRIMARY KEY, tag TEXT)",
            "CREATE TABLE aux.only(id INTEGER, note TEXT)",
            "INSERT INTO t VALUES (1,'main-a'),(2,'main-b'),(3,'main-c')",
            "INSERT INTO aux.t VALUES (2,'aux-two'),(3,'aux-three'),(4,'aux-four')",
            "INSERT INTO aux.only VALUES (1,'n1'),(3,'n3')",
            "INSERT INTO aux.t(id,tag) SELECT id, name FROM main.t WHERE id=1",
        ] {
            let r = f.execute(s).await;
            eprintln!("SETUP {s:?} -> {}", if r.is_ok() { "ok".to_owned() } else { format!("ERR: {}", r.unwrap_err()) });
        }
        eprintln!("--- initial aux.t ---");
        eprintln!("{}", dump(&f, "SELECT id,tag FROM aux.t ORDER BY id").await);

        // bd-pqauo: unqualified name living only in aux
        eprintln!("--- bd-pqauo: SELECT id,note FROM only ---");
        eprintln!("{}", dump(&f, "SELECT id,note FROM only ORDER BY id").await);

        // bd-s12cm (retraction test): UPDATE with non-correlated cross-schema WHERE subquery
        let u = "UPDATE aux.t SET tag='UPD' WHERE id IN (SELECT id FROM main.t)";
        let ur = f.execute(u).await;
        eprintln!("--- bd-s12cm UPDATE {u:?} -> {} ---", if ur.is_ok() { "ok".to_owned() } else { format!("ERR: {}", ur.unwrap_err()) });
        eprintln!("aux.t after UPDATE: {}", dump(&f, "SELECT id,tag FROM aux.t ORDER BY id").await);

        // reset tag values via a plain aux update so DELETE test is independent
        let _ = f.execute("UPDATE aux.t SET tag='r'||id").await;

        // bd-s12cm case B: DELETE anti-join against real main
        let d = "DELETE FROM aux.t WHERE id NOT IN (SELECT id FROM main.t)";
        let dr = f.execute(d).await;
        eprintln!("--- bd-s12cm DELETE {d:?} -> {} ---", if dr.is_ok() { "ok".to_owned() } else { format!("ERR: {}", dr.unwrap_err()) });
        eprintln!("aux.t after DELETE (expect id=4 gone): {}", dump(&f, "SELECT id FROM aux.t ORDER BY id").await);

        eprintln!("main.t untouched: {}", dump(&f, "SELECT id,name FROM main.t ORDER BY id").await);
    });
}
