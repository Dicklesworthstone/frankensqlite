//! GH #207 (bd-gh-pragma-tvf-ktwap): the table-valued form of compile-options
//! introspection was missing — `SELECT * FROM pragma_compile_options()` failed
//! with `no such table: pragma_compile_options`, even though the statement form
//! `PRAGMA compile_options` already worked.
//!
//! The TVF form is now routed through the same PRAGMA row generator (like
//! pragma_database_list, GH#213), so it must return exactly the rows the
//! statement form returns. The compile-option *values* are build-specific and
//! differ from any reference build, so the oracle here is frank-internal: the
//! TVF form must agree with the PRAGMA form.

use fsqlite_core::connection::Connection;
use fsqlite_types::value::SqliteValue;

fn as_text(v: &SqliteValue) -> String {
    match v {
        SqliteValue::Text(s) => s.to_string(),
        other => panic!("expected TEXT compile option, got {other:?}"),
    }
}

async fn one_col(conn: &Connection, sql: &str) -> Vec<String> {
    conn.query(sql)
        .await
        .unwrap_or_else(|e| panic!("frank error on `{sql}`: {e:?}"))
        .iter()
        .map(|row| as_text(&row.values()[0]))
        .collect()
}

/// The `pragma_compile_options()` TVF returns the same non-empty catalog as the
/// `PRAGMA compile_options` statement form, via both `SELECT *` and an explicit
/// column projection.
#[test]
fn pragma_compile_options_tvf_matches_statement_gh207() {
    asupersync::test_utils::run_test(|| async {
        let conn = Connection::open(":memory:").await.unwrap();

        let mut pragma_rows = one_col(&conn, "PRAGMA compile_options").await;
        pragma_rows.sort();
        assert!(
            !pragma_rows.is_empty(),
            "PRAGMA compile_options should return build options"
        );

        // The GH#207 fix: the TVF call form must no longer error and must match.
        let mut star_rows = one_col(&conn, "SELECT * FROM pragma_compile_options()").await;
        star_rows.sort();
        assert_eq!(
            star_rows, pragma_rows,
            "SELECT * FROM pragma_compile_options() must match PRAGMA compile_options"
        );

        let mut col_rows = one_col(
            &conn,
            "SELECT compile_options FROM pragma_compile_options()",
        )
        .await;
        col_rows.sort();
        assert_eq!(
            col_rows, pragma_rows,
            "explicit-column projection of the TVF must match PRAGMA compile_options"
        );
    });
}

// NOTE: the parenless eponymous form `FROM pragma_compile_options` (no `()`)
// still errors `no such table`. That is a *general* gap in bare-table
// resolution — it affects every synthesize-path pragma TVF (e.g.
// pragma_database_list) equally, because the bare-table resolver consults real
// tables and registered vtab modules but not pragma_table_function_columns.
// Fixing it touches the hot multi-site table-resolution path, so it is tracked
// as a separate follow-up bead rather than scoped into this TVF-call-form fix.
