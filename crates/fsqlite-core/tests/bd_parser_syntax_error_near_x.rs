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

/// Run `setup` then attempt `sql`; return the error text (DDL path).
async fn ddl_err(setup: &[&str], sql: &str) -> String {
    let c = Connection::open(":memory:").await.unwrap();
    for s in setup {
        let _ = c.execute(s).await;
    }
    c.execute(sql)
        .await
        .expect_err("malformed statement must be rejected")
        .to_string()
}

#[test]
fn parser_trigger_body_restrictions_match_stock() {
    asupersync::test_utils::run_test(|| async {
        // Part B-ext + C: trigger-body DML restrictions. Stock (bundled SQLite
        // 3.53.2) reports most of these as near-X grammar errors, and a few as
        // fixed verbatim messages. bd-parser-syntax-error-format-6w6kp.
        let setup = &["CREATE TABLE t(a, b)", "CREATE INDEX ix ON t(a)"];
        let tr = |body: &str| format!("CREATE TRIGGER tr AFTER INSERT ON t BEGIN {body} END");

        // ── near-X (UnexpectedToken) ──
        assert_eq!(
            ddl_err(setup, &tr("INSERT INTO t DEFAULT VALUES;")).await,
            "near \"DEFAULT\": syntax error",
        );
        assert_eq!(
            ddl_err(setup, &tr("UPDATE t SET a = 1 ORDER BY a;")).await,
            "near \"ORDER\": syntax error",
        );
        assert_eq!(
            ddl_err(setup, &tr("UPDATE t SET a = 1 LIMIT 1;")).await,
            "near \"LIMIT\": syntax error",
        );
        assert_eq!(
            ddl_err(setup, &tr("DELETE FROM t ORDER BY a;")).await,
            "near \"ORDER\": syntax error",
        );
        assert_eq!(
            ddl_err(setup, &tr("CREATE TABLE z(a);")).await,
            "near \"CREATE\": syntax error",
        );
        assert_eq!(ddl_err(setup, &tr("")).await, "near \"END\": syntax error");
        assert_eq!(
            ddl_err(setup, "EXPLAIN EXPLAIN SELECT 1").await,
            "near \"EXPLAIN\": syntax error",
        );
        // UPDATE/DELETE RETURNING have no grammar slot in a trigger → near-X.
        assert_eq!(
            ddl_err(setup, &tr("UPDATE t SET a = 1 RETURNING a;")).await,
            "near \"RETURNING\": syntax error",
        );
        assert_eq!(
            ddl_err(setup, &tr("DELETE FROM t RETURNING a;")).await,
            "near \"RETURNING\": syntax error",
        );

        // ── verbatim (Semantic) ──
        assert_eq!(
            ddl_err(setup, &tr("UPDATE main.t SET a = 1;")).await,
            "qualified table names are not allowed on INSERT, UPDATE, and DELETE \
             statements within triggers",
        );
        assert_eq!(
            ddl_err(setup, &tr("UPDATE t INDEXED BY ix SET a = 1;")).await,
            "the INDEXED BY clause is not allowed on UPDATE or DELETE statements within triggers",
        );
        assert_eq!(
            ddl_err(setup, &tr("UPDATE t NOT INDEXED SET a = 1;")).await,
            "the NOT INDEXED clause is not allowed on UPDATE or DELETE statements within triggers",
        );
        // A trigger-body INSERT parses RETURNING then rejects it verbatim.
        assert_eq!(
            ddl_err(setup, &tr("INSERT INTO t VALUES (1, 2) RETURNING a;")).await,
            "cannot use RETURNING in a trigger",
        );
    });
}

#[test]
fn parser_window_frame_bound_errors_match_stock() {
    asupersync::test_utils::run_test(|| async {
        // bd-parser-errfmt-residuals-3giu1: window-frame-bound rejections must
        // read as stock SQLite (sqlite3 3.51, verified; also 3.53), NOT as the
        // old frank-specific "window frame ... bound must ..." phrasings wrapped
        // in a `SQL error at offset N:` prefix. Stock splits these into two
        // shapes: grammar-level near-X (an UNBOUNDED bound in the wrong role)
        // and the fixed `unsupported frame specification` (a semantically
        // impossible start/end ordering). The frame is validated at parse time,
        // so a minimal aggregate over-clause is enough to reach it.
        let over =
            |frame: &str| format!("SELECT sum(x) OVER (ORDER BY x {frame}) FROM (SELECT 1 AS x)");

        // UNBOUNDED FOLLOWING as a frame START → near "FOLLOWING": syntax error.
        assert_eq!(
            err_of(&over("ROWS UNBOUNDED FOLLOWING")).await,
            "near \"FOLLOWING\": syntax error",
        );
        assert_eq!(
            err_of(&over("ROWS BETWEEN UNBOUNDED FOLLOWING AND UNBOUNDED FOLLOWING")).await,
            "near \"FOLLOWING\": syntax error",
        );
        // UNBOUNDED PRECEDING as a frame END → near "PRECEDING": syntax error.
        assert_eq!(
            err_of(&over("ROWS BETWEEN CURRENT ROW AND UNBOUNDED PRECEDING")).await,
            "near \"PRECEDING\": syntax error",
        );
        // A single-bound frame that starts after CURRENT ROW (e.g. `ROWS 1
        // FOLLOWING`) → unsupported frame specification.
        assert_eq!(
            err_of(&over("ROWS 1 FOLLOWING")).await,
            "unsupported frame specification",
        );
        // An end bound that precedes the start bound → unsupported frame
        // specification.
        assert_eq!(
            err_of(&over("ROWS BETWEEN CURRENT ROW AND 1 PRECEDING")).await,
            "unsupported frame specification",
        );
    });
}

#[test]
fn parser_semantically_empty_input_is_a_noop_like_stock() {
    asupersync::test_utils::run_test(|| async {
        // bd-parser-errfmt-residuals-3giu1: stock's sqlite3_prepare_v2 treats
        // empty / whitespace / comment / bare-`;` input as a no-op (SQLITE_OK
        // with a NULL statement) — the C API and CLI return success with no
        // rows, never a parse error. frank previously raised
        // `SQL error at offset 0: no SQL statement provided`; query()/execute()
        // must now agree with stock (matching execute_batch, which already
        // treated these as no-ops).
        let c = Connection::open(":memory:").await.unwrap();
        for sql in [
            "",
            "   ",
            "\n\t ",
            ";",
            " ; ; ",
            "-- just a comment",
            "/* block */",
        ] {
            let rows = c
                .query(sql)
                .await
                .unwrap_or_else(|e| panic!("empty input {sql:?} must be a no-op, got: {e}"));
            assert!(rows.is_empty(), "empty input {sql:?} must yield no rows");
            assert_eq!(
                c.execute(sql)
                    .await
                    .unwrap_or_else(|e| panic!("execute {sql:?} must be a no-op, got: {e}")),
                0,
                "empty input {sql:?} must affect zero rows",
            );
        }
    });
}
