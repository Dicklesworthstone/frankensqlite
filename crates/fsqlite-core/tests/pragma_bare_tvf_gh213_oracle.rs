//! bd-xl98m (GH #207/#213 family): the *parenless* eponymous no-arg pragma
//! table-valued function form — `SELECT * FROM pragma_compile_options` (no
//! parens) — used to error `no such table`, while the call form
//! `SELECT * FROM pragma_compile_options()` worked. Bare-table resolution
//! consulted real tables and registered vtab modules but never the pragma-TVF
//! synthesize path, so every no-arg synthesize-path pragma TVF failed in bare
//! form.
//!
//! The fix routes a bare `pragma_<name>` whose name is a NO-ARG pragma TVF
//! through the same table-function execution path the call form already uses,
//! before the bare-table resolver can raise `NoSuchTable`.
//!
//! Oracle strategy: rusqlite (stock SQLite) is the oracle for the pragmas whose
//! contents are portable (`pragma_database_list`) and a name-set oracle for the
//! ones whose contents are build-specific (`pragma_function_list`,
//! `pragma_compile_options` — the compile flags and function catalog differ from
//! any reference build). For the build-specific pragmas the stronger assertion
//! is frank-internal: the bare form must return exactly what the call form and
//! the `PRAGMA` statement form return.

use fsqlite_core::connection::{Connection, Row};
use fsqlite_types::value::SqliteValue;
use std::collections::BTreeSet;

fn text(v: &SqliteValue) -> String {
    match v {
        SqliteValue::Text(s) => s.as_ref().to_owned(),
        other => panic!("expected TEXT, got {other:?}"),
    }
}

fn int(v: &SqliteValue) -> i64 {
    match v {
        SqliteValue::Integer(n) => *n,
        other => panic!("expected INTEGER, got {other:?}"),
    }
}

async fn frank_rows(conn: &Connection, sql: &str) -> Vec<Row> {
    conn.query(sql)
        .await
        .unwrap_or_else(|e| panic!("frank error on `{sql}`: {e:?}"))
}

/// Sorted first-column TEXT values from a frank query.
async fn frank_col0_text(conn: &Connection, sql: &str) -> Vec<String> {
    let mut out: Vec<String> = frank_rows(conn, sql)
        .await
        .iter()
        .map(|row| text(&row.values()[0]))
        .collect();
    out.sort();
    out
}

/// Sorted (seq, name) pairs from a frank `pragma_database_list`-shaped query.
async fn frank_seq_name(conn: &Connection, sql: &str) -> Vec<(i64, String)> {
    let mut out: Vec<(i64, String)> = frank_rows(conn, sql)
        .await
        .iter()
        .map(|row| (int(&row.values()[0]), text(&row.values()[1])))
        .collect();
    out.sort();
    out
}

/// Sorted first-column TEXT values from a stock rusqlite query against a fresh
/// in-memory database.
fn rusqlite_col0_text(sql: &str) -> Vec<String> {
    let r = rusqlite::Connection::open_in_memory().unwrap();
    let mut stmt = r
        .prepare(sql)
        .unwrap_or_else(|e| panic!("rusqlite prepare failed on `{sql}`: {e:?}"));
    let mut out: Vec<String> = stmt
        .query_map([], |row| row.get::<_, String>(0))
        .unwrap()
        .map(Result::unwrap)
        .collect();
    out.sort();
    out
}

/// Sorted (seq, name) pairs from a stock rusqlite `pragma_database_list` query.
fn rusqlite_seq_name(sql: &str) -> Vec<(i64, String)> {
    let r = rusqlite::Connection::open_in_memory().unwrap();
    let mut stmt = r.prepare(sql).unwrap();
    let mut out: Vec<(i64, String)> = stmt
        .query_map([], |row| {
            Ok((row.get::<_, i64>(0)?, row.get::<_, String>(1)?))
        })
        .unwrap()
        .map(Result::unwrap)
        .collect();
    out.sort();
    out
}

/// The bare form of every no-arg synthesize-path pragma TVF must return exactly
/// what the call form and the `PRAGMA` statement form return (frank-internal).
/// This is the primary regression proof: the bare form no longer errors.
#[test]
fn bare_form_matches_call_and_statement_forms() {
    asupersync::test_utils::run_test(|| async {
        let conn = Connection::open(":memory:").await.unwrap();

        // compile_options (single build-specific column).
        let bare = frank_col0_text(&conn, "SELECT * FROM pragma_compile_options").await;
        let call = frank_col0_text(&conn, "SELECT * FROM pragma_compile_options()").await;
        let stmt = frank_col0_text(&conn, "PRAGMA compile_options").await;
        assert!(
            !bare.is_empty(),
            "compile_options bare form must be non-empty"
        );
        assert_eq!(bare, call, "compile_options: bare must match call form");
        assert_eq!(bare, stmt, "compile_options: bare must match PRAGMA form");

        // function_list (build-specific catalog).
        let bare = frank_col0_text(&conn, "SELECT name FROM pragma_function_list").await;
        let call = frank_col0_text(&conn, "SELECT name FROM pragma_function_list()").await;
        let stmt = frank_col0_text(
            &conn,
            "SELECT name FROM (SELECT * FROM pragma_function_list())",
        )
        .await;
        assert!(
            !bare.is_empty(),
            "function_list bare form must be non-empty"
        );
        assert_eq!(bare, call, "function_list: bare must match call form");
        assert_eq!(
            bare, stmt,
            "function_list: bare must match derived call form"
        );

        // database_list (portable contents).
        let bare = frank_seq_name(&conn, "SELECT seq, name FROM pragma_database_list").await;
        let call = frank_seq_name(&conn, "SELECT seq, name FROM pragma_database_list()").await;
        assert_eq!(bare, call, "database_list: bare must match call form");
        assert_eq!(bare, vec![(0, "main".to_owned())]);
    });
}

/// Cross-engine oracle for the portable pragma: frank's bare
/// `pragma_database_list` must agree with stock SQLite's bare form.
#[test]
fn bare_database_list_matches_rusqlite() {
    asupersync::test_utils::run_test(|| async {
        let conn = Connection::open(":memory:").await.unwrap();
        let frank = frank_seq_name(&conn, "SELECT seq, name FROM pragma_database_list").await;
        let sqlite = rusqlite_seq_name("SELECT seq, name FROM pragma_database_list");
        assert_eq!(
            frank, sqlite,
            "bare pragma_database_list must match stock SQLite"
        );
    });
}

/// Name-set oracle for the build-specific catalog: every core, portable function
/// stock SQLite reports through the bare `pragma_function_list` must also appear
/// in frank's bare-form result (both engines accept the same SQL shape).
#[test]
fn bare_function_list_covers_core_functions_vs_rusqlite() {
    asupersync::test_utils::run_test(|| async {
        let conn = Connection::open(":memory:").await.unwrap();
        let frank: BTreeSet<String> =
            frank_col0_text(&conn, "SELECT name FROM pragma_function_list ORDER BY name")
                .await
                .into_iter()
                .map(|n| n.to_ascii_lowercase())
                .collect();
        let sqlite: BTreeSet<String> =
            rusqlite_col0_text("SELECT name FROM pragma_function_list ORDER BY name")
                .into_iter()
                .map(|n| n.to_ascii_lowercase())
                .collect();

        for f in [
            "abs", "coalesce", "length", "lower", "upper", "count", "sum", "min", "max",
        ] {
            assert!(sqlite.contains(f), "oracle drift: stock SQLite lacks `{f}`");
            assert!(
                frank.contains(f),
                "frank bare pragma_function_list missing core function `{f}`"
            );
        }
    });
}

/// A WHERE filter over the bare form must match stock SQLite.
#[test]
fn bare_form_where_filter_matches_rusqlite() {
    asupersync::test_utils::run_test(|| async {
        let conn = Connection::open(":memory:").await.unwrap();
        let frank =
            frank_col0_text(&conn, "SELECT name FROM pragma_database_list WHERE seq = 0").await;
        let sqlite = rusqlite_col0_text("SELECT name FROM pragma_database_list WHERE seq = 0");
        assert_eq!(frank, vec!["main".to_owned()]);
        assert_eq!(
            frank, sqlite,
            "bare-form WHERE filter must match stock SQLite"
        );
    });
}

/// A JOIN of two pragma TVFs in bare form: a self-join of `pragma_database_list`
/// exercises the bare rewrite on both the primary and the joined source, and its
/// full result must match stock SQLite. A join of two *different* bare pragmas
/// must also execute and match the call form (frank-internal, since one pragma's
/// contents are build-specific).
#[test]
fn bare_form_join_of_two_pragma_tvfs() {
    asupersync::test_utils::run_test(|| async {
        let conn = Connection::open(":memory:").await.unwrap();

        // Two pragma-TVF sources (self-join), cross-engine oracle.
        let frank: Vec<(String, String)> = frank_rows(
            &conn,
            "SELECT a.name, b.name FROM pragma_database_list a \
             JOIN pragma_database_list b ORDER BY 1, 2",
        )
        .await
        .iter()
        .map(|row| (text(&row.values()[0]), text(&row.values()[1])))
        .collect();
        let r = rusqlite::Connection::open_in_memory().unwrap();
        let mut stmt = r
            .prepare(
                "SELECT a.name, b.name FROM pragma_database_list a \
                 JOIN pragma_database_list b ORDER BY 1, 2",
            )
            .unwrap();
        let sqlite: Vec<(String, String)> = stmt
            .query_map([], |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
            })
            .unwrap()
            .map(Result::unwrap)
            .collect();
        assert_eq!(
            frank, sqlite,
            "bare self-join of pragma_database_list must match stock"
        );
        assert_eq!(frank, vec![("main".to_owned(), "main".to_owned())]);

        // A join of two *different* bare pragma TVFs must execute (no NoSuchTable)
        // and yield the same cardinality as the equivalent call form.
        let bare = frank_rows(
            &conn,
            "SELECT b.compile_options FROM pragma_database_list a \
             JOIN pragma_compile_options b",
        )
        .await;
        let call = frank_rows(
            &conn,
            "SELECT b.compile_options FROM pragma_database_list() a \
             JOIN pragma_compile_options() b",
        )
        .await;
        assert!(
            !bare.is_empty(),
            "cross join with compile_options must be non-empty"
        );
        assert_eq!(
            bare.len(),
            call.len(),
            "bare join of two different pragma TVFs must match the call form"
        );
    });
}

/// Control: the parenthesized call form still works (no regression), an unknown
/// bare name still raises `no such table` (the fix does not over-route), and an
/// arg-requiring pragma TVF is unaffected — its bare no-arg form still errors as
/// before, while its arg call form still succeeds.
#[test]
fn controls_call_form_unknown_name_and_arg_pragma() {
    asupersync::test_utils::run_test(|| async {
        let conn = Connection::open(":memory:").await.unwrap();

        // Call form still works.
        let call = frank_rows(&conn, "SELECT * FROM pragma_compile_options()").await;
        assert!(!call.is_empty(), "call form must still work");

        // A genuinely unknown bare name still errors (not over-routed).
        match conn
            .query("SELECT * FROM definitely_not_a_real_table_xyz")
            .await
        {
            Ok(rows) => panic!("unknown bare name unexpectedly succeeded: {rows:?}"),
            Err(e) => assert!(
                e.to_string().to_ascii_lowercase().contains("no such table"),
                "unknown bare name must raise `no such table`, got: {e}"
            ),
        }

        // Arg-requiring pragma TVF: its call-with-arg form still works ...
        conn.execute("CREATE TABLE t(a, b)").await.unwrap();
        let info: Vec<String> = frank_rows(&conn, "SELECT name FROM pragma_table_info('t')")
            .await
            .iter()
            .map(|row| text(&row.values()[0]))
            .collect();
        assert_eq!(
            info,
            vec!["a".to_owned(), "b".to_owned()],
            "arg call form must be unaffected"
        );

        // ... and its bare, argument-less form still behaves as before (errors);
        // this bead deliberately does NOT route arg-requiring pragma TVFs.
        match conn.query("SELECT * FROM pragma_table_info").await {
            Ok(rows) => {
                panic!("bare arg-requiring pragma_table_info unexpectedly succeeded: {rows:?}")
            }
            Err(e) => assert!(
                e.to_string().to_ascii_lowercase().contains("no such table"),
                "bare pragma_table_info must still error, got: {e}"
            ),
        }
    });
}
