#![recursion_limit = "512"]

//! bd-5m52m (part 1): the table-valued `pragma_collation_list()` form — both
//! `SELECT * FROM pragma_collation_list()` and the parenless
//! `SELECT * FROM pragma_collation_list` — must be recognized and produce the
//! same rows as the `PRAGMA collation_list` statement, mirroring the existing
//! pragma-TVF pattern (database_list / function_list / compile_options).
//!
//! The registered collation SET is compared against rusqlite order-independently
//! (SQLite documents collation_list ordering as unspecified hash order). Any
//! remaining set/order divergence vs the sqlite3 CLI's decimal/uint extension
//! collations is tracked separately in bd-5m52m part 2 — NOT asserted here.

use fsqlite_core::connection::Connection;
use fsqlite_types::value::SqliteValue;

async fn fq(fconn: &Connection, sql: &str) -> Vec<Vec<String>> {
    fconn
        .query(sql)
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e:?}"))
        .iter()
        .map(|r| {
            r.values()
                .iter()
                .map(|v| match v {
                    SqliteValue::Null => "NULL".to_owned(),
                    SqliteValue::Integer(n) => n.to_string(),
                    SqliteValue::Float(f) => format!("{f}"),
                    SqliteValue::Text(s) => s.to_string(),
                    SqliteValue::Blob(b) => format!("{b:?}"),
                })
                .collect()
        })
        .collect()
}

fn names_sorted_f(rows: &[Vec<String>], name_col: usize) -> Vec<String> {
    let mut v: Vec<String> = rows.iter().map(|r| r[name_col].clone()).collect();
    v.sort();
    v
}

fn rusqlite_collation_names(rconn: &rusqlite::Connection) -> Vec<String> {
    let mut st = rconn
        .prepare("SELECT name FROM pragma_collation_list")
        .unwrap();
    let mut v: Vec<String> = st
        .query_map([], |row| row.get::<_, String>(0))
        .unwrap()
        .map(Result::unwrap)
        .collect();
    v.sort();
    v
}

#[test]
fn collation_list_tvf_form_works_and_matches_statement_bd_5m52m() {
    asupersync::test_utils::run_test(|| async {
        let f = Connection::open(":memory:").await.unwrap();

        // Statement form (already worked) — the reference rows.
        let stmt = fq(&f, "PRAGMA collation_list").await;
        assert!(!stmt.is_empty(), "PRAGMA collation_list returned no rows");
        // Each row is (seq, name).
        assert_eq!(stmt[0].len(), 2, "collation_list should have (seq, name)");

        // Table-valued form with parens must now be recognized and identical.
        let tvf_parens = fq(&f, "SELECT seq, name FROM pragma_collation_list()").await;
        assert_eq!(
            tvf_parens, stmt,
            "pragma_collation_list() must mirror PRAGMA collation_list"
        );

        // Parenless bare-table form must also be recognized and identical.
        let tvf_bare = fq(&f, "SELECT seq, name FROM pragma_collation_list").await;
        assert_eq!(
            tvf_bare, stmt,
            "bare pragma_collation_list must mirror PRAGMA collation_list"
        );

        // The TVF is usable in a real query context (WHERE / projection).
        let filtered = fq(
            &f,
            "SELECT name FROM pragma_collation_list WHERE name = 'BINARY'",
        )
        .await;
        assert_eq!(filtered, vec![vec!["BINARY".to_owned()]]);

        // Differential: the registered collation SET matches the rusqlite oracle
        // (order-independent). This confirms frank's collation registry is not
        // missing any collation the bundled SQLite exposes.
        let r = rusqlite::Connection::open_in_memory().unwrap();
        assert_eq!(
            names_sorted_f(&stmt, 1),
            rusqlite_collation_names(&r),
            "collation SET must match rusqlite (bundled SQLite)"
        );
    });
}
