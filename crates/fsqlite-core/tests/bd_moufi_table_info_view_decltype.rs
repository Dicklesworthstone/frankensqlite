// Keeper for bd-pragma-table-info-view-decltype-moufi: PRAGMA table_info(<view>)
// must report one row per view OUTPUT column, with each column's DECLARED type
// resolved like SQLite's sqlite3ColumnType — a direct column reference reports
// the source column's declared type verbatim (e.g. VARCHAR(20), not its
// affinity), while an expression / literal / aggregate reports an empty type.
// notnull, dflt_value, pk are always 0 / NULL / 0 for a view.
// Oracle: sqlite3 3.46.1.
use fsqlite_core::connection::Connection;
use fsqlite_types::value::SqliteValue;

/// Return (name, type) pairs from `PRAGMA table_info(name)` in cid order.
async fn table_info_name_type(conn: &Connection, name: &str) -> Vec<(String, String)> {
    let rows = conn
        .query_with_params(&format!("PRAGMA table_info({name})"), &[])
        .await
        .unwrap();
    rows.iter()
        .map(|r| {
            let v = r.values();
            let n = match &v[1] {
                SqliteValue::Text(s) => s.to_string(),
                other => panic!("table_info name not TEXT: {other:?}"),
            };
            let t = match &v[2] {
                SqliteValue::Text(s) => s.to_string(),
                other => panic!("table_info type not TEXT: {other:?}"),
            };
            (n, t)
        })
        .collect()
}

/// Assert notnull=0, dflt_value=NULL, pk=0 for every column of the view.
async fn assert_view_trailing_cols_are_zero(conn: &Connection, name: &str) {
    let rows = conn
        .query_with_params(&format!("PRAGMA table_info({name})"), &[])
        .await
        .unwrap();
    for r in &rows {
        let v = r.values();
        assert_eq!(v[3], SqliteValue::Integer(0), "view notnull must be 0");
        assert_eq!(v[4], SqliteValue::Null, "view dflt_value must be NULL");
        assert_eq!(v[5], SqliteValue::Integer(0), "view pk must be 0");
    }
}

#[test]
fn pragma_table_info_view_decltype_moufi() {
    asupersync::test_utils::run_test(|| async {
        let c = Connection::open(":memory:").await.unwrap();

        // (1) Direct column passthrough with aliases -> source declared types.
        c.execute("CREATE TABLE base(a INTEGER, b TEXT)")
            .await
            .unwrap();
        c.execute("CREATE VIEW v_alias AS SELECT a AS x, b AS y FROM base")
            .await
            .unwrap();
        assert_eq!(
            table_info_name_type(&c, "v_alias").await,
            vec![("x".into(), "INTEGER".into()), ("y".into(), "TEXT".into())],
        );
        assert_view_trailing_cols_are_zero(&c, "v_alias").await;

        // (2) VARCHAR(N) must be reported VERBATIM (declared type, not affinity).
        c.execute("CREATE TABLE tv(a VARCHAR(20), b TEXT)")
            .await
            .unwrap();
        c.execute("CREATE VIEW v_varchar AS SELECT a AS x, b FROM tv")
            .await
            .unwrap();
        assert_eq!(
            table_info_name_type(&c, "v_varchar").await,
            vec![
                ("x".into(), "VARCHAR(20)".into()),
                ("b".into(), "TEXT".into())
            ],
        );

        // (3) Expression / literal / aggregate columns report an EMPTY type.
        c.execute("CREATE VIEW v_expr AS SELECT a+1 AS z, 5 AS n, upper(b) AS u FROM base")
            .await
            .unwrap();
        assert_eq!(
            table_info_name_type(&c, "v_expr").await,
            vec![
                ("z".into(), String::new()),
                ("n".into(), String::new()),
                ("u".into(), String::new()),
            ],
        );

        // (4) SELECT * expands to each source column's declared type.
        c.execute("CREATE TABLE tw(a INTEGER, b VARCHAR(9))")
            .await
            .unwrap();
        c.execute("CREATE VIEW v_star AS SELECT * FROM tw")
            .await
            .unwrap();
        assert_eq!(
            table_info_name_type(&c, "v_star").await,
            vec![
                ("a".into(), "INTEGER".into()),
                ("b".into(), "VARCHAR(9)".into())
            ],
        );

        // (5) An explicit CREATE VIEW v(p,q) column list renames the outputs but
        // the types still come from the underlying source columns.
        c.execute("CREATE VIEW v_cols(p, q) AS SELECT a, b FROM base")
            .await
            .unwrap();
        assert_eq!(
            table_info_name_type(&c, "v_cols").await,
            vec![("p".into(), "INTEGER".into()), ("q".into(), "TEXT".into())],
        );
    });
}
