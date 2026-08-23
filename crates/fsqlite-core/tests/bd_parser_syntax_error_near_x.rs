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

#[test]
fn parser_expected_family_and_generic_match_stock_near_x() {
    asupersync::test_utils::run_test(|| async {
        // Part B-ext: the "expected X" expression-grammar family (err_here) and the
        // generic statement-position unexpected-token sites (err_expected /
        // err_unexpected) now render as stock's `near "<lexeme>": syntax error`
        // (non-EOF) or `incomplete input` (EOF), with NO `SQL error at offset N:`
        // prefix. Oracle: rusqlite (bundled SQLite 3.53.2) + sqlite3 CLI 3.46.1
        // (parser diagnostics are version-stable). bd-parser-syntax-error-format-6w6kp.

        // ── expected-X expression family (err_here) → near "<lexeme>" ──
        assert_eq!(err_of("SELECT 1 BETWEEN 2 3").await, "near \"3\": syntax error");
        assert_eq!(err_of("SELECT CASE WHEN 1 2 END").await, "near \"2\": syntax error");
        assert_eq!(err_of("SELECT 1 COLLATE 2").await, "near \"2\": syntax error");
        assert_eq!(
            err_of("SELECT count(* ORDER BY 1)").await,
            "near \"ORDER\": syntax error",
        );

        // ── generic statement-position unexpected token (err_unexpected / err_expected) ──
        assert_eq!(err_of("SELECT 1 2").await, "near \"2\": syntax error");
        assert_eq!(
            err_of("WITH x AS (SELECT 1) FOO").await,
            "near \"FOO\": syntax error",
        );
        assert_eq!(err_of("CREATE FOO").await, "near \"FOO\": syntax error");
        assert_eq!(err_of("ALTER TABLE t FOO").await, "near \"FOO\": syntax error");

        // ── EOF within the expected-X family → incomplete input ──
        assert_eq!(err_of("SELECT 1 BETWEEN 2").await, "incomplete input");
        assert_eq!(err_of("SELECT a.").await, "incomplete input");

        // ── Part C: a NATURAL join with ON/USING → stock's fixed message VERBATIM ──
        assert_eq!(
            err_of("SELECT * FROM t NATURAL JOIN u ON t.a = u.a").await,
            "a NATURAL join may not have an ON or USING clause",
        );
    });
}
