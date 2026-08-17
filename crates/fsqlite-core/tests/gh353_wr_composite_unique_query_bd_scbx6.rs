//! bd-scbx6 (GH#353, P0): WITHOUT ROWID + composite UNIQUE auto-index maintenance.
//!
//! DML on a WITHOUT ROWID table must maintain the auto-indexes backing composite
//! UNIQUE constraints so that (a) index scans / `INDEXED BY` queries return the
//! correct rows and (b) the UNIQUE constraint actually fires on a duplicate.
//! This is a differential-vs-stock (rusqlite = bundled SQLite) oracle for the
//! query-visibility + phantom-constraint angle of GH#353 (bd-5ava1 fixed the
//! on-disk key layout / integrity_check angle in 3d3cdda45).

use fsqlite_core::connection::Connection;
use fsqlite_types::SqliteValue;

const SCHEMA: &str = "CREATE TABLE members(\
    a TEXT, b INTEGER, c INTEGER, \
    PRIMARY KEY(a, b), UNIQUE(a, c)\
) WITHOUT ROWID";

fn render(v: &SqliteValue) -> String {
    match v {
        SqliteValue::Null => "NULL".to_owned(),
        SqliteValue::Integer(n) => n.to_string(),
        SqliteValue::Float(f) => format!("{f}"),
        SqliteValue::Text(s) => format!("'{s}'"),
        SqliteValue::Blob(b) => format!("X'{}'", b.iter().map(|x| format!("{x:02X}")).collect::<String>()),
    }
}

async fn frank_rows(conn: &Connection, sql: &str) -> Result<Vec<Vec<String>>, String> {
    let rows = conn.query(sql).await.map_err(|e| format!("{e:?}"))?;
    Ok(rows
        .iter()
        .map(|row| row.values().iter().map(render).collect())
        .collect())
}

fn stock_rows(conn: &rusqlite::Connection, sql: &str) -> Result<Vec<Vec<String>>, String> {
    let mut stmt = conn.prepare(sql).map_err(|e| e.to_string())?;
    let n = stmt.column_count();
    stmt.query_map([], |row| {
        let mut out = Vec::with_capacity(n);
        for i in 0..n {
            out.push(match row.get_unwrap::<_, rusqlite::types::Value>(i) {
                rusqlite::types::Value::Null => "NULL".to_owned(),
                rusqlite::types::Value::Integer(x) => x.to_string(),
                rusqlite::types::Value::Real(f) => format!("{f}"),
                rusqlite::types::Value::Text(s) => format!("'{s}'"),
                rusqlite::types::Value::Blob(b) => {
                    format!("X'{}'", b.iter().map(|x| format!("{x:02X}")).collect::<String>())
                }
            });
        }
        Ok(out)
    })
    .map_err(|e| e.to_string())?
    .collect::<Result<Vec<_>, _>>()
    .map_err(|e| e.to_string())
}

#[test]
fn wr_composite_unique_index_scan_and_constraint_bd_scbx6() {
    asupersync::test_utils::run_test(|| async {
        let f = Connection::open(":memory:").await.unwrap();
        let r = rusqlite::Connection::open_in_memory().unwrap();
        for s in [
            SCHEMA,
            "INSERT INTO members VALUES ('shared', 0, 100)",
            "INSERT INTO members VALUES ('shared', 1, 101)",
            "INSERT INTO members VALUES ('shared', 2, 102)",
        ] {
            f.execute(s).await.unwrap_or_else(|e| panic!("frank `{s}`: {e:?}"));
            r.execute_batch(s).unwrap_or_else(|e| panic!("stock `{s}`: {e}"));
        }

        // (1) Index-scan / covering reads over the composite-UNIQUE(a,c) auto-index
        // must match stock. If the auto-index is not maintained, these return
        // wrong/empty rows.
        let mut mismatches = Vec::new();
        for q in [
            "SELECT b FROM members WHERE a = 'shared' AND c = 101",
            "SELECT b FROM members WHERE a = 'shared' AND c = 102",
            "SELECT a, b, c FROM members WHERE a = 'shared' AND c = 100",
            "SELECT c FROM members WHERE a = 'shared' ORDER BY c",
            "SELECT b FROM members INDEXED BY sqlite_autoindex_members_2 WHERE a = 'shared' AND c = 102",
        ] {
            match (frank_rows(&f, q).await, stock_rows(&r, q)) {
                (Ok(a), Ok(b)) if a == b => {}
                (fa, sb) => mismatches.push(format!("`{q}` -> frank={fa:?} stock={sb:?}")),
            }
        }
        assert!(
            mismatches.is_empty(),
            "bd-scbx6 index-scan divergence(s):\n{}",
            mismatches.join("\n")
        );

        // (2) Phantom UNIQUE(a,c): inserting a duplicate (a,c) pair must be
        // rejected. c=100 already exists under a='shared'.
        let dup = "INSERT INTO members VALUES ('shared', 3, 100)";
        let frank_err = f.execute(dup).await.is_err();
        let stock_err = r.execute_batch(dup).is_err();
        assert!(stock_err, "sanity: stock must reject the duplicate (a,c)");
        assert_eq!(
            frank_err, stock_err,
            "bd-scbx6 phantom UNIQUE(a,c): frank rejected={frank_err}, stock rejected={stock_err} \
             (a composite-UNIQUE auto-index that is not maintained lets the duplicate through)"
        );

        // A non-conflicting distinct (a,c) still inserts in both.
        let ok = "INSERT INTO members VALUES ('shared', 3, 103)";
        assert!(f.execute(ok).await.is_ok(), "frank must accept a distinct (a,c)");
        assert!(r.execute_batch(ok).is_ok(), "stock must accept a distinct (a,c)");
    });
}
