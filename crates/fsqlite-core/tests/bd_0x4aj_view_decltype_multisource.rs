//! bd-pragma-table-info-view-decltype-multisource-0x4aj: PRAGMA table_info(view)
//! must resolve source declared types across joins, subqueries, and nested views
//! (not just a single bare base table). Types match stock sqlite3 verbatim.

use fsqlite_core::connection::{Connection, Row};
use fsqlite_types::value::SqliteValue;

fn name_type_pairs(rows: &[Row]) -> Vec<(String, String)> {
    let text = |v: &SqliteValue| match v {
        SqliteValue::Text(s) => s.to_string(),
        _ => String::new(),
    };
    rows.iter()
        .map(|r| (text(&r.values()[1]), text(&r.values()[2])))
        .collect()
}

#[test]
fn bd_0x4aj_view_table_info_decltype_across_joins_and_subqueries() {
    asupersync::test_utils::run_test(|| async {
        let conn = Connection::open(":memory:").await.unwrap();
        conn.execute("CREATE TABLE a(x INTEGER, y TEXT);")
            .await
            .unwrap();
        conn.execute("CREATE TABLE b(z VARCHAR(5));").await.unwrap();
        conn.execute("CREATE VIEW v AS SELECT a.x, a.y, b.z FROM a JOIN b ON 1;")
            .await
            .unwrap();
        conn.execute("CREATE VIEW v2 AS SELECT s.x FROM (SELECT x FROM a) s;")
            .await
            .unwrap();
        conn.execute("CREATE VIEW v3 AS SELECT * FROM v;")
            .await
            .unwrap();

        // Join view: each qualified column resolves to its own source's decltype.
        assert_eq!(
            name_type_pairs(&conn.query("PRAGMA table_info(v);").await.unwrap()),
            vec![
                ("x".to_owned(), "INTEGER".to_owned()),
                ("y".to_owned(), "TEXT".to_owned()),
                ("z".to_owned(), "VARCHAR(5)".to_owned()),
            ],
            "join view decltypes must resolve per-source"
        );

        // Subquery source: decltype recurses into the derived table.
        assert_eq!(
            name_type_pairs(&conn.query("PRAGMA table_info(v2);").await.unwrap()),
            vec![("x".to_owned(), "INTEGER".to_owned())],
            "subquery-source view decltype must recurse"
        );

        // Nested view (SELECT * over another view): decltypes recurse through it.
        assert_eq!(
            name_type_pairs(&conn.query("PRAGMA table_info(v3);").await.unwrap()),
            vec![
                ("x".to_owned(), "INTEGER".to_owned()),
                ("y".to_owned(), "TEXT".to_owned()),
                ("z".to_owned(), "VARCHAR(5)".to_owned()),
            ],
            "nested-view SELECT * decltypes must recurse"
        );
    });
}
