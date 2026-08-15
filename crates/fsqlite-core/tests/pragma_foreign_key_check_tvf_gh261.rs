#![recursion_limit = "512"]

//! GH #261 (bd-gh-pragma-fk-check), TVF part: the table-valued form
//! `SELECT * FROM pragma_foreign_key_check([table])` must report the same FK
//! violation rows (table, rowid, parent, fkid) as `PRAGMA foreign_key_check`.
//! rusqlite is the oracle.

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

async fn assert_agree(fconn: &Connection, rconn: &rusqlite::Connection, sql: &str) {
    let mut fr: Vec<Vec<String>> = fconn.query(sql).await.unwrap_or_else(|e| panic!("{sql}: {e:?}")).iter().map(|r| r.values().iter().map(tag_f).collect()).collect();
    fr.sort();
    let mut st = rconn.prepare(sql).unwrap();
    let n = st.column_count();
    let mut rr: Vec<Vec<String>> = st.query_map([], |row| Ok((0..n).map(|i| tag_r(&row.get_unwrap::<_, rusqlite::types::Value>(i))).collect())).unwrap().collect::<Result<Vec<_>, _>>().unwrap();
    rr.sort();
    assert_eq!(fr, rr, "pragma_foreign_key_check TVF mismatch on `{sql}`");
}

#[test]
fn pragma_foreign_key_check_tvf_reports_violations_gh261() {
    asupersync::test_utils::run_test(|| async {
        let f = Connection::open(":memory:").await.unwrap();
        let r = rusqlite::Connection::open_in_memory().unwrap();
        // With FK enforcement off we can seed orphaned child rows that
        // foreign_key_check must then report.
        for s in [
            "PRAGMA foreign_keys = OFF",
            "CREATE TABLE p (id INTEGER PRIMARY KEY)",
            "CREATE TABLE c (id INTEGER PRIMARY KEY, pid INTEGER REFERENCES p(id))",
            "INSERT INTO p VALUES (1)",
            "INSERT INTO c VALUES (10, 1)",   // valid
            "INSERT INTO c VALUES (11, 99)",  // orphan
            "INSERT INTO c VALUES (12, 42)",  // orphan
        ] {
            f.execute(s).await.unwrap();
            r.execute_batch(s).unwrap();
        }
        // TVF, scoped to one table — SELECT * returns all four columns
        // (table, rowid, parent, fkid); order by the rowid column positionally
        // to avoid the separate `rowid`-name-resolution follow-up below.
        assert_agree(&f, &r, "SELECT * FROM pragma_foreign_key_check('c') ORDER BY 2").await;
        // Non-rowid projection (ordered by name) matches too.
        assert_agree(&f, &r, "SELECT \"table\", parent, fkid FROM pragma_foreign_key_check('c') ORDER BY \"table\", parent").await;
        // Whole-schema form + count, usable like any table-valued function.
        assert_agree(&f, &r, "SELECT * FROM pragma_foreign_key_check() ORDER BY 1, 2").await;
        assert_agree(&f, &r, "SELECT count(*) FROM pragma_foreign_key_check('c')").await;
    });
}
