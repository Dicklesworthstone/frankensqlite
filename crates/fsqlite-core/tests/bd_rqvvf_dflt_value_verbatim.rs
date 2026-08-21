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

        let rows = c.query_with_params("PRAGMA table_info(t)", &[]).await.unwrap();
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
