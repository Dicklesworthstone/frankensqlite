#![recursion_limit = "512"]

// Keeper for bd-pragma-table-info-dflt-source-rqvvf: PRAGMA table_info's
// dflt_value must report a parenthesized DEFAULT's VERBATIM inner source (outer
// paren pair stripped, whitespace trimmed, exact text preserved) rather than an
// AST re-render. Non-parenthesized literal defaults are unaffected.
// Oracle: sqlite3 3.46.1.
use fsqlite_core::connection::Connection;
use fsqlite_types::value::SqliteValue;

#[test]
fn pragma_table_info_dflt_value_verbatim_rqvvf() {
    asupersync::test_utils::run_test(|| async {
        let c = Connection::open(":memory:").await.unwrap();
        // NOTE: a nested-paren default `DEFAULT ((1+1))` is a known remaining
        // edge — Expr::span() yields the innermost expression, so frank strips
        // the inner parens too (reports `1+1` vs stock `(1+1)`). Fixing it needs
        // the parser to capture the between-outer-parens span; tracked on
        // bd-pragma-table-info-dflt-source-rqvvf. All common shapes are covered.
        c.execute(
            "CREATE TABLE t(\
                a INTEGER DEFAULT (1+1), \
                b INTEGER DEFAULT (1 + 1), \
                d INTEGER DEFAULT ( 1+1 ), \
                e INTEGER DEFAULT (abs(-1)), \
                f INTEGER DEFAULT 0, \
                g TEXT DEFAULT 'hi'\
            )",
        )
        .await
        .unwrap();

        let rows = c
            .query_with_params("PRAGMA table_info(t)", &[])
            .await
            .unwrap();
        // columns: cid, name, type, notnull, dflt_value, pk
        let mut got: Vec<(String, String)> = Vec::new();
        for r in &rows {
            let v = r.values();
            let name = match &v[1] {
                SqliteValue::Text(s) => s.to_string(),
                other => panic!("name not TEXT: {other:?}"),
            };
            let dflt = match &v[4] {
                SqliteValue::Text(s) => s.to_string(),
                SqliteValue::Null => String::from("<NULL>"),
                other => panic!("dflt not TEXT/NULL: {other:?}"),
            };
            got.push((name, dflt));
        }

        let expected: Vec<(&str, &str)> = vec![
            ("a", "1+1"),
            ("b", "1 + 1"),
            ("d", "1+1"),
            ("e", "abs(-1)"),
            ("f", "0"),
            ("g", "'hi'"),
        ];
        let got_ref: Vec<(&str, &str)> =
            got.iter().map(|(n, d)| (n.as_str(), d.as_str())).collect();
        assert_eq!(got_ref, expected, "dflt_value must match SQLite verbatim");
    });
}

/// Unparenthesized defaults are reported as written too (`1.50`, `1e2`,
/// `0x10`, `00010`), both for a table fsqlite creates and for a schema loaded
/// from a file stock SQLite wrote. That text is also what a record predating
/// an ALTER-added column reads its value from. Oracle: rusqlite.
#[test]
fn pragma_table_info_dflt_value_keeps_literal_spelling() {
    const CREATE: &str = "CREATE TABLE t(a TEXT DEFAULT 1e2, b DEFAULT 1.50, \
        c DEFAULT -1.50, d DEFAULT 0x10, e DEFAULT +5, f DEFAULT 00010, \
        g DEFAULT -0.0, h DEFAULT (1.50), i DEFAULT 'x', \
        k DEFAULT TRUE, l DEFAULT CURRENT_TIMESTAMP, m DEFAULT x'0a', \
        n DEFAULT NULL, o DEFAULT (abs(-1)), p DEFAULT - 7)";
    const QUERY: &str = "SELECT name, dflt_value FROM pragma_table_info('t') ORDER BY cid";

    fn tag(value: &SqliteValue) -> String {
        match value {
            SqliteValue::Text(s) => s.to_string(),
            SqliteValue::Null => "<NULL>".to_owned(),
            other => format!("{other:?}"),
        }
    }

    asupersync::test_utils::run_test(|| async {
        let dir = tempfile::tempdir().unwrap();
        let stock_path = dir.path().join("stock_dflt.db");
        let stock = rusqlite::Connection::open(&stock_path).unwrap();
        stock.execute_batch(CREATE).unwrap();
        let expected: Vec<(String, String)> = stock
            .prepare(QUERY)
            .unwrap()
            .query_map([], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, Option<String>>(1)?
                        .unwrap_or_else(|| "<NULL>".to_owned()),
                ))
            })
            .unwrap()
            .collect::<rusqlite::Result<_>>()
            .unwrap();
        drop(stock);

        let created = Connection::open(":memory:").await.unwrap();
        created.execute(CREATE).await.unwrap();
        let loaded = Connection::open(stock_path.to_str().unwrap())
            .await
            .unwrap();
        for (label, conn) in [("created", &created), ("loaded", &loaded)] {
            let got: Vec<(String, String)> = conn
                .query(QUERY)
                .await
                .unwrap()
                .iter()
                .map(|row| (tag(&row.values()[0]), tag(&row.values()[1])))
                .collect();
            assert_eq!(got, expected, "{label}: dflt_value must match SQLite verbatim");
        }

        // The stored text is what INSERT evaluates, so new rows still take
        // stock's default values.
        const ROW: &str = "SELECT quote(a), quote(b), quote(c), quote(d), quote(e), \
            quote(f), typeof(g), quote(h), quote(i), quote(k), quote(m), \
            quote(n), quote(o), quote(p) FROM t";
        let stock_row = rusqlite::Connection::open_in_memory().unwrap();
        stock_row.execute_batch(CREATE).unwrap();
        stock_row.execute_batch("INSERT INTO t DEFAULT VALUES").unwrap();
        let expected_row: Vec<String> = stock_row
            .query_row(ROW, [], |row| {
                (0..14)
                    .map(|i| row.get::<_, String>(i))
                    .collect::<rusqlite::Result<Vec<_>>>()
            })
            .unwrap();
        for (label, conn) in [("created", &created), ("loaded", &loaded)] {
            conn.execute("INSERT INTO t DEFAULT VALUES").await.unwrap();
            let rows = conn.query(ROW).await.unwrap();
            let got: Vec<String> = rows[0].values().iter().map(tag).collect();
            assert_eq!(got, expected_row, "{label}: INSERT defaults must match SQLite");
        }
    });
}
