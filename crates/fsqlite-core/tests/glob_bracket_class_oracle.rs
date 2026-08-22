//! bd-gh-glob-charclass-tobn9 (GH #257): GLOB bracket character classes must
//! match C SQLite on BOTH matcher paths.
//!
//! Two independent GLOB matchers exist: the function path (fsqlite-func
//! `glob_match_inner`, used by bare `SELECT ... GLOB ...`) and the interpreter
//! path (`simple_glob_match`/`glob_dp` in connection.rs, used when a GLOB
//! predicate is row-evaluated, e.g. a table WHERE scan). The function path was
//! fixed for unterminated classes (86923bb1c); the interpreter path had NO
//! bracket support at all and matched `[` literally. This keeper pins both
//! against the rusqlite oracle, including the unterminated-class edge that must
//! return no match.

use fsqlite_core::connection::Connection;
use fsqlite_types::SqliteValue;

// Rows chosen so bracket classes, ranges, negation, and unterminated classes
// all discriminate.
const ROWS: &[&str] = &["a", "b", "q", "Z", "5", "-", "]"];

// (pattern, ...) — evaluated as `x GLOB pattern` over ROWS (interpreter path via
// the table scan) and as `'<lit>' GLOB pattern` (function path). Both must agree
// with C SQLite.
const PATTERNS: &[&str] = &[
    "[ab]",   // simple class
    "[a-z]",  // range
    "[^a-z]", // negated range
    "[a-c-]", // trailing literal dash
    "[]]",    // ']' as first literal member
    "[a",     // unterminated: C SQLite -> no match
    "[^",     // unterminated negated
    "[a-z",   // unterminated range
    "a[b-d]", // class after a literal
];

fn oracle_where(pattern: &str) -> Vec<String> {
    let conn = rusqlite::Connection::open_in_memory().unwrap();
    conn.execute("CREATE TABLE t (x TEXT)", []).unwrap();
    for r in ROWS {
        conn.execute("INSERT INTO t VALUES (?1)", [r]).unwrap();
    }
    let mut stmt = conn
        .prepare("SELECT x FROM t WHERE x GLOB ?1 ORDER BY x")
        .unwrap();
    stmt.query_map([pattern], |row| row.get::<_, String>(0))
        .unwrap()
        .map(Result::unwrap)
        .collect()
}

fn oracle_scalar(lit: &str, pattern: &str) -> i64 {
    let conn = rusqlite::Connection::open_in_memory().unwrap();
    conn.query_row("SELECT ?1 GLOB ?2", [lit, pattern], |row| row.get(0))
        .unwrap()
}

#[test]
fn glob_bracket_classes_match_rusqlite_oracle_both_paths() {
    asupersync::test_utils::run_test(|| async {
        let conn = Connection::open(":memory:").await.unwrap();
        conn.execute("CREATE TABLE t (x TEXT)")
            .await
            .expect("create");
        for r in ROWS {
            conn.execute(&format!("INSERT INTO t VALUES ('{r}')"))
                .await
                .expect("insert");
        }

        for pattern in PATTERNS {
            // Interpreter path: table WHERE scan.
            let expected_where = oracle_where(pattern);
            let sql = format!("SELECT x FROM t WHERE x GLOB '{pattern}' ORDER BY x");
            let rows = conn
                .query(&sql)
                .await
                .unwrap_or_else(|e| panic!("`{sql}`: {e:?}"));
            let got_where: Vec<String> = rows
                .iter()
                .map(|r| match r.values()[0] {
                    SqliteValue::Text(ref s) => s.as_ref().to_owned(),
                    ref other => panic!("`{sql}` col 0 not text: {other:?}"),
                })
                .collect();
            assert_eq!(
                got_where, expected_where,
                "interpreter-path `x GLOB '{pattern}'` diverged from the C SQLite oracle"
            );

            // Function path: scalar `'<lit>' GLOB pattern` for each row literal.
            for lit in ROWS {
                let expected = oracle_scalar(lit, pattern);
                let sql = format!("SELECT '{lit}' GLOB '{pattern}'");
                let row = conn
                    .query(&sql)
                    .await
                    .unwrap_or_else(|e| panic!("`{sql}`: {e:?}"));
                let got = match row[0].values()[0] {
                    SqliteValue::Integer(n) => n,
                    ref other => panic!("`{sql}` not integer: {other:?}"),
                };
                assert_eq!(
                    got, expected,
                    "function-path `'{lit}' GLOB '{pattern}'` diverged from the C SQLite oracle"
                );
            }
        }
    });
}
