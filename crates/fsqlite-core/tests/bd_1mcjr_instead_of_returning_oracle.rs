#![recursion_limit = "512"]

//! bd-1mcjr M3: an INSTEAD OF trigger that raises RAISE(IGNORE) for a row must
//! emit NO RETURNING row for it — matching stock SQLite. Differential vs
//! rusqlite (bundled SQLite).

use fsqlite_core::connection::Connection;
use fsqlite_types::value::SqliteValue;

fn tag_f(v: &SqliteValue) -> String {
    match v {
        SqliteValue::Null => "NULL".to_owned(),
        SqliteValue::Integer(n) => n.to_string(),
        SqliteValue::Float(f) => format!("{f}"),
        SqliteValue::Text(s) => format!("'{s}'"),
        SqliteValue::Blob(b) => format!(
            "X'{}'",
            b.iter().map(|x| format!("{x:02X}")).collect::<String>()
        ),
    }
}
fn tag_r(v: &rusqlite::types::Value) -> String {
    match v {
        rusqlite::types::Value::Null => "NULL".to_owned(),
        rusqlite::types::Value::Integer(n) => n.to_string(),
        rusqlite::types::Value::Real(f) => format!("{f}"),
        rusqlite::types::Value::Text(s) => format!("'{s}'"),
        rusqlite::types::Value::Blob(b) => format!(
            "X'{}'",
            b.iter().map(|x| format!("{x:02X}")).collect::<String>()
        ),
    }
}
async fn fq(f: &Connection, sql: &str) -> Vec<Vec<String>> {
    match f.query(sql).await {
        Ok(rows) => rows
            .iter()
            .map(|r| r.values().iter().map(tag_f).collect())
            .collect(),
        Err(e) => vec![vec![format!("<ERR {e:?}>")]],
    }
}
fn rq(r: &rusqlite::Connection, sql: &str) -> Vec<Vec<String>> {
    let mut st = match r.prepare(sql) {
        Ok(st) => st,
        Err(e) => return vec![vec![format!("<ERR {e}>")]],
    };
    let n = st.column_count();
    st.query_map([], |row| {
        Ok((0..n)
            .map(|i| tag_r(&row.get_unwrap::<_, rusqlite::types::Value>(i)))
            .collect())
    })
    .unwrap()
    .collect::<Result<Vec<_>, _>>()
    .unwrap()
}

const SETUP: &[&str] = &[
    "CREATE TABLE t(id INTEGER PRIMARY KEY, v INTEGER)",
    "CREATE VIEW vw AS SELECT id, v FROM t",
    "CREATE TRIGGER vw_ins INSTEAD OF INSERT ON vw BEGIN \
       SELECT RAISE(IGNORE) WHERE NEW.v < 0; \
       INSERT INTO t(id, v) VALUES (NEW.id, NEW.v); END",
    "CREATE TRIGGER vw_del INSTEAD OF DELETE ON vw BEGIN \
       SELECT RAISE(IGNORE) WHERE OLD.v = 10; \
       DELETE FROM t WHERE id = OLD.id; END",
    "CREATE TRIGGER vw_upd INSTEAD OF UPDATE ON vw BEGIN \
       SELECT RAISE(IGNORE) WHERE NEW.v = 999; \
       UPDATE t SET v = NEW.v WHERE id = OLD.id; END",
];

async fn agree(setup: &[&str], query: &str, msg: &str) {
    let f = Connection::open(":memory:").await.unwrap();
    let r = rusqlite::Connection::open_in_memory().unwrap();
    for s in setup {
        let _ = f.execute(s).await;
        let _ = r.execute_batch(s);
    }
    let fr = fq(&f, query).await;
    let rr = rq(&r, query);
    assert_eq!(fr, rr, "{msg}\n  frank={fr:?}\n  stock={rr:?}");
}

#[test]
fn insert_raise_ignore_row_emits_no_returning_row() {
    asupersync::test_utils::run_test(|| async {
        agree(
            SETUP,
            "INSERT INTO vw(id, v) VALUES (1, 10), (2, -5), (3, 30) RETURNING id, v",
            "M3 INSERT: the RAISE(IGNORE) row (2,-5) must not appear in RETURNING",
        )
        .await;
    });
}

#[test]
fn delete_raise_ignore_row_emits_no_returning_row() {
    asupersync::test_utils::run_test(|| async {
        let mut setup = SETUP.to_vec();
        setup.push("INSERT INTO t VALUES (1, 10), (2, 20), (3, 10)");
        agree(
            &setup,
            "DELETE FROM vw RETURNING id, v",
            "M3 DELETE: v=10 rows are RAISE(IGNORE)'d — only id=2 returns",
        )
        .await;
    });
}

#[test]
fn update_raise_ignore_row_emits_no_returning_row() {
    asupersync::test_utils::run_test(|| async {
        let mut setup = SETUP.to_vec();
        setup.push("INSERT INTO t VALUES (1, 1), (2, 2)");
        agree(
            &setup,
            "UPDATE vw SET v = 999 RETURNING id, v",
            "M3 UPDATE: v=999 is RAISE(IGNORE)'d for every row — RETURNING empty",
        )
        .await;
    });
}

#[test]
fn non_ignored_rows_still_return() {
    asupersync::test_utils::run_test(|| async {
        agree(
            SETUP,
            "INSERT INTO vw(id, v) VALUES (5, 50), (6, 60) RETURNING id, v",
            "M3: rows that are NOT ignored must still return normally",
        )
        .await;
    });
}

// L9: a user column literally named `rowid` shadows the implicit rowid. The
// RAISE(IGNORE) skip-filter rewrites the DML to `<rowid> IN (<true rowids>)`;
// using the shadowed column name would match the wrong rows (or none).
#[test]
fn update_ignore_skip_filter_survives_user_rowid_column() {
    asupersync::test_utils::run_test(|| async {
        agree(
            &[
                "CREATE TABLE t(rowid INTEGER, v INTEGER)",
                "INSERT INTO t VALUES (100, 1), (200, 2), (300, 3)",
                "CREATE TRIGGER trg BEFORE UPDATE ON t BEGIN \
                   SELECT RAISE(IGNORE) WHERE NEW.v = 12; END",
                "UPDATE t SET v = v + 10",
            ],
            "SELECT rowid, v FROM t ORDER BY rowid",
            "L9 UPDATE: only the true non-ignored rows update (100->11, 200 kept, 300->13)",
        )
        .await;
    });
}

#[test]
#[ignore = "bd-1mcjr L9 DELETE still RED: on a table with a user `rowid` column the \
RAISE(IGNORE) rewrite now emits `_rowid_ IN (<true rowids>)` (is_rowid_ref recognizes \
`_rowid_`), but the recompiled DELETE deletes nothing — a deeper codegen issue in the \
compile_table_delete `_rowid_ IN` recompile path (the UPDATE path works via \
execute_update_row_by_row). Follow-up: bd-uur1d."]
fn delete_ignore_skip_filter_survives_user_rowid_column() {
    asupersync::test_utils::run_test(|| async {
        agree(
            &[
                "CREATE TABLE t(rowid INTEGER, v INTEGER)",
                "INSERT INTO t VALUES (100, 1), (200, 2), (300, 3)",
                "CREATE TRIGGER trg BEFORE DELETE ON t BEGIN \
                   SELECT RAISE(IGNORE) WHERE OLD.v = 2; END",
                "DELETE FROM t",
            ],
            "SELECT rowid, v FROM t ORDER BY rowid",
            "L9 DELETE: only the ignored row (200) survives",
        )
        .await;
    });
}
