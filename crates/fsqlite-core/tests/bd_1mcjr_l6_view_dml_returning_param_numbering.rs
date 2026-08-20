#![recursion_limit = "512"]
// DRAFT (bd-1mcjr L6) — NOT compiled (.rs.draft), NOT run. Rename to `.rs` and
// run only alongside the L6 fix (bd_1mcjr_l6_view_dml_bind.rs.draft). This keeper
// is RED on current HEAD (per-clause ? renumbering) and GREEN after the fix.
//
//! bd-1mcjr L6: the interpreted INSTEAD-OF view-DML path must number `?`
//! placeholders GLOBALLY in SQL text order (WITH -> source/SET -> WHERE ->
//! RETURNING), exactly like C SQLite — not per-clause from 1. Param-bound
//! differential vs rusqlite (bundled SQLite 3.53). Each case binds params whose
//! values differ per position so a mis-numbered clause produces a wrong value or
//! matches the wrong rows.

use fsqlite_core::connection::Connection;
use fsqlite_types::value::SqliteValue;

fn tag_f(v: &SqliteValue) -> String {
    match v {
        SqliteValue::Null => "NULL".to_owned(),
        SqliteValue::Integer(n) => n.to_string(),
        SqliteValue::Float(f) => format!("{f}"),
        SqliteValue::Text(s) => format!("'{s}'"),
        SqliteValue::Blob(b) => format!("X'{}'", b.iter().map(|x| format!("{x:02X}")).collect::<String>()),
    }
}
fn tag_r(v: &rusqlite::types::Value) -> String {
    match v {
        rusqlite::types::Value::Null => "NULL".to_owned(),
        rusqlite::types::Value::Integer(n) => n.to_string(),
        rusqlite::types::Value::Real(f) => format!("{f}"),
        rusqlite::types::Value::Text(s) => format!("'{s}'"),
        rusqlite::types::Value::Blob(b) => format!("X'{}'", b.iter().map(|x| format!("{x:02X}")).collect::<String>()),
    }
}

async fn fq_p(f: &Connection, sql: &str, params: &[SqliteValue]) -> Vec<Vec<String>> {
    match f.query_with_params(sql, params).await {
        Ok(rows) => rows.iter().map(|r| r.values().iter().map(tag_f).collect()).collect(),
        Err(e) => vec![vec![format!("<ERR {e:?}>")]],
    }
}
fn rq_p(r: &rusqlite::Connection, sql: &str, vals: &[i64]) -> Vec<Vec<String>> {
    let mut st = match r.prepare(sql) {
        Ok(st) => st,
        Err(e) => return vec![vec![format!("<ERR {e}>")]],
    };
    let n = st.column_count();
    st.query_map(rusqlite::params_from_iter(vals.iter().copied()), |row| {
        Ok((0..n).map(|i| tag_r(&row.get_unwrap::<_, rusqlite::types::Value>(i))).collect())
    })
    .unwrap()
    .collect::<Result<Vec<_>, _>>()
    .unwrap()
}

fn iv(n: i64) -> SqliteValue {
    SqliteValue::Integer(n)
}

/// Set up an identical view + INSTEAD-OF triggers (+ optional seed rows) on both
/// engines, run one param-bound statement, and assert the projected rows agree.
async fn agree_p(setup: &[&str], sql: &str, fparams: &[SqliteValue], rvals: &[i64], msg: &str) {
    let f = Connection::open(":memory:").await.unwrap();
    let r = rusqlite::Connection::open_in_memory().unwrap();
    for s in setup {
        let _ = f.execute(s).await;
        r.execute(s, []).unwrap();
    }
    let fr = fq_p(&f, sql, fparams).await;
    let rr = rq_p(&r, sql, rvals);
    assert_eq!(fr, rr, "{msg}\n  frank ={fr:?}\n  sqlite={rr:?}");
}

const VIEW: &[&str] = &[
    "CREATE TABLE t(id INTEGER PRIMARY KEY, v INTEGER)",
    "CREATE VIEW vw AS SELECT id, v FROM t",
    "CREATE TRIGGER vw_ins INSTEAD OF INSERT ON vw BEGIN \
       INSERT INTO t(id, v) VALUES (NEW.id, NEW.v); END",
    "CREATE TRIGGER vw_upd INSTEAD OF UPDATE ON vw BEGIN \
       UPDATE t SET v = NEW.v WHERE id = OLD.id; END",
    "CREATE TRIGGER vw_del INSTEAD OF DELETE ON vw BEGIN \
       DELETE FROM t WHERE id = OLD.id; END",
];

fn view_seeded(rows: &[&'static str]) -> Vec<&'static str> {
    let mut s = VIEW.to_vec();
    s.extend_from_slice(rows);
    s
}

#[test]
fn l6_insert_returning_continues_global_param_sequence() {
    asupersync::test_utils::run_test(|| async {
        // INSERT ? (id=1), ? (v=20), then RETURNING ? must be param#3 (=99), not
        // param#1 (=1). RETURNING also projects the inserted v to prove numbering.
        agree_p(
            VIEW,
            "INSERT INTO vw(id, v) VALUES (?, ?) RETURNING v, ?",
            &[iv(1), iv(20), iv(99)],
            &[1, 20, 99],
            "INSERT RETURNING ? must be param#3",
        )
        .await;
    });
}

#[test]
fn l6_update_set_and_where_and_returning_all_global() {
    asupersync::test_utils::run_test(|| async {
        // The case that distinguishes pre-resolve from offset-only: SET v=? is
        // param#1 (=77), WHERE id=? is param#2 (=1), RETURNING ? is param#3 (=88).
        // An offset-only fix leaves WHERE numbering from 1 -> WHERE id=77 -> 0 rows
        // -> empty RETURNING; the correct fix binds WHERE id=1 and returns 88.
        agree_p(
            &view_seeded(&["INSERT INTO t VALUES (1, 20)"]),
            "UPDATE vw SET v = ? WHERE id = ? RETURNING ?",
            &[iv(77), iv(1), iv(88)],
            &[77, 1, 88],
            "UPDATE SET/WHERE/RETURNING must all number globally",
        )
        .await;
    });
}

#[test]
fn l6_delete_where_and_returning_global() {
    asupersync::test_utils::run_test(|| async {
        // WHERE id=? is param#1 (=1), RETURNING ? is param#2 (=55).
        agree_p(
            &view_seeded(&["INSERT INTO t VALUES (1, 20)"]),
            "DELETE FROM vw WHERE id = ? RETURNING ?",
            &[iv(1), iv(55)],
            &[1, 55],
            "DELETE WHERE=param#1, RETURNING=param#2",
        )
        .await;
    });
}

#[test]
fn l6_insert_subquery_source_placeholder_counts() {
    asupersync::test_utils::run_test(|| async {
        // Adversarial: a ? inside a subquery in the VALUES source must consume a
        // slot so RETURNING ? lands on param#3. VALUES (?=id=2, (SELECT ?)=v=30),
        // RETURNING ? = param#3 = 77.
        agree_p(
            VIEW,
            "INSERT INTO vw(id, v) VALUES (?, (SELECT ?)) RETURNING v, ?",
            &[iv(2), iv(30), iv(77)],
            &[2, 30, 77],
            "subquery ? in VALUES source must advance the global count",
        )
        .await;
    });
}

#[test]
fn l6_negative_control_dml_without_returning_unaffected() {
    asupersync::test_utils::run_test(|| async {
        // No RETURNING: SET v=? (param#1=41) WHERE id=? (param#2=1) — the row must
        // become v=41. Verified by a following (unparameterized) read agreeing.
        let f = Connection::open(":memory:").await.unwrap();
        let r = rusqlite::Connection::open_in_memory().unwrap();
        for s in view_seeded(&["INSERT INTO t VALUES (1, 20)"]) {
            let _ = f.execute(s).await;
            r.execute(s, []).unwrap();
        }
        let _ = f
            .query_with_params("UPDATE vw SET v = ? WHERE id = ?", &[iv(41), iv(1)])
            .await
            .unwrap();
        r.execute("UPDATE vw SET v = ? WHERE id = ?", rusqlite::params![41, 1]).unwrap();
        let fr = fq_p(&f, "SELECT id, v FROM t ORDER BY id", &[]).await;
        let rr = rq_p(&r, "SELECT id, v FROM t ORDER BY id", &[]);
        assert_eq!(fr, rr, "no-RETURNING param DML unaffected\n  frank ={fr:?}\n  sqlite={rr:?}");
    });
}

#[test]
fn l6_insert_subquery_non_view_general() {
    asupersync::test_utils::run_test(|| async {
        // bd-l6-view-insert-subquery-numbering-2qtn7: the VALUES-subquery global
        // `?` numbering must hold for a PLAIN table (non-view) too, not only the
        // interpreted view path — the fix is at the path-agnostic dispatch entry.
        // VALUES (?=id=2, (SELECT ?)=v=30), RETURNING v, ? = param#3 = 77.
        agree_p(
            &["CREATE TABLE t2(id INTEGER PRIMARY KEY, v INTEGER)"],
            "INSERT INTO t2(id, v) VALUES (?, (SELECT ?)) RETURNING v, ?",
            &[iv(2), iv(30), iv(77)],
            &[2, 30, 77],
            "non-view INSERT VALUES subquery ? must number globally",
        )
        .await;
    });
}

#[test]
fn l6_update_set_subquery_where_returning_global() {
    asupersync::test_utils::run_test(|| async {
        // bd-l6-view-insert-subquery-numbering-2qtn7 (UPDATE arm): SET b=(SELECT ?=88)
        // WHERE id=?=1 RETURNING b, ?=55. A per-scope renumber binds WHERE id to
        // param#1 (=88) → matches no row → zero rows updated (frank was []).
        agree_p(
            &[
                "CREATE TABLE t(id INTEGER PRIMARY KEY, a INTEGER, b INTEGER)",
                "INSERT INTO t VALUES (1, 10, 20)",
            ],
            "UPDATE t SET b = (SELECT ?) WHERE id = ? RETURNING b, ?",
            &[iv(88), iv(1), iv(55)],
            &[88, 1, 55],
            "UPDATE SET-subquery / WHERE / RETURNING must number ? globally",
        )
        .await;
    });
}

#[test]
fn l6_update_two_subqueries_global() {
    asupersync::test_utils::run_test(|| async {
        // SET a=(SELECT ?=41), b=(SELECT ?=42) WHERE id=?=1 RETURNING a, b, ?=43.
        agree_p(
            &[
                "CREATE TABLE t(id INTEGER PRIMARY KEY, a INTEGER, b INTEGER)",
                "INSERT INTO t VALUES (1, 10, 20)",
            ],
            "UPDATE t SET a = (SELECT ?), b = (SELECT ?) WHERE id = ? RETURNING a, b, ?",
            &[iv(41), iv(42), iv(1), iv(43)],
            &[41, 42, 1, 43],
            "UPDATE two SET-subqueries must number ? globally",
        )
        .await;
    });
}

#[test]
fn l6_delete_where_subquery_returning_global() {
    asupersync::test_utils::run_test(|| async {
        // WHERE a=(SELECT ?=10) RETURNING id, ?=77. A per-scope renumber gives
        // RETURNING ? param#1 (=10) instead of #2 (=77).
        agree_p(
            &[
                "CREATE TABLE t(id INTEGER PRIMARY KEY, a INTEGER)",
                "INSERT INTO t VALUES (1, 10)",
            ],
            "DELETE FROM t WHERE a = (SELECT ?) RETURNING id, ?",
            &[iv(10), iv(77)],
            &[10, 77],
            "DELETE WHERE-subquery / RETURNING must number ? globally",
        )
        .await;
    });
}
