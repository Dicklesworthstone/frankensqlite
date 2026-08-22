// Keeper (bd-parser-syntax-error-format-6w6kp, Part B: expression-position catch-all
// unexpected tokens). An unexpected token in expression position now reports stock's
// `near "<lexeme>": syntax error`; an unexpected end-of-input reports `incomplete
// input`; and (Part A, already landed) a lexer error token reports `unrecognized
// token: "X"` verbatim — all WITHOUT the old `SQL error at offset N:` wrapper.
// Oracle: sqlite3 3.46.1 (parser messages are version-stable). Other emit sites
// (e.g. `expected column name after '.'`, trigger-body restrictions) are deferred to
// later slices and intentionally not asserted here.
use fsqlite_core::connection::Connection;

async fn err_of(sql: &str) -> String {
    let c = Connection::open(":memory:").await.unwrap();
    c.query(sql)
        .await
        .expect_err("malformed SQL must be rejected")
        .to_string()
}

#[test]
fn parser_syntax_errors_match_stock_near_x() {
    asupersync::test_utils::run_test(|| async {
        // Part B: unexpected token in expression position -> near "<lexeme>": syntax error.
        assert_eq!(err_of("SELECT FROM t").await, "near \"FROM\": syntax error");
        assert_eq!(err_of("SELECT .").await, "near \".\": syntax error");
        assert_eq!(
            err_of("SELECT count(DISTINCT *)").await,
            "near \"*\": syntax error",
        );
        // Part B: unexpected end-of-input -> incomplete input.
        assert_eq!(err_of("SELECT").await, "incomplete input");
        assert_eq!(err_of("SELECT 1 +").await, "incomplete input");
        // Part A (already landed): lexer error token -> verbatim, no prefix.
        assert_eq!(err_of("SELECT 0xGG").await, "unrecognized token: \"0xGG\"");

        // Valid SQL is unaffected.
        let c = Connection::open(":memory:").await.unwrap();
        c.execute("CREATE TABLE t(a, b)").await.unwrap();
        c.query("SELECT a FROM t").await.expect("valid query must succeed");
    });
}
