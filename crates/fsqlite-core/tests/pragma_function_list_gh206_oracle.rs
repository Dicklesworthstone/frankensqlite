//! GH #206 (bd-gh-pragma-introspection-29g0a): `PRAGMA function_list` returned
//! an empty result — function introspection reported no registered SQL
//! functions. It must return a non-empty catalog of the built-in functions.
//!
//! The catalog rows are `name|builtin|type|enc|narg|flags`. rusqlite (stock
//! SQLite) is used as a name-set oracle: every function in a core, portable set
//! must appear in BOTH engines' catalogs. Exact `type`/`flags` parity is NOT
//! asserted — stock SQLite classifies window-capable aggregates (`count`,
//! `sum`, `group_concat`) as `w` and encodes internal flag bitmasks that have no
//! clean-room equivalent; the introspection contract this bead fixes is the
//! presence of the catalog, not those internal codes.

use fsqlite_core::connection::Connection;
use fsqlite_types::value::SqliteValue;
use std::collections::BTreeSet;

fn as_text(v: &SqliteValue) -> String {
    match v {
        SqliteValue::Text(s) => s.to_string(),
        other => panic!("expected TEXT, got {other:?}"),
    }
}

fn as_int(v: &SqliteValue) -> i64 {
    match v {
        SqliteValue::Integer(n) => *n,
        other => panic!("expected INTEGER, got {other:?}"),
    }
}

/// (name, builtin, type, enc, narg) tuples from frank's `PRAGMA function_list`.
async fn frank_function_list(conn: &Connection) -> Vec<(String, i64, String, String, i64)> {
    conn.query("PRAGMA function_list")
        .await
        .expect("PRAGMA function_list must succeed")
        .iter()
        .map(|row| {
            let v = row.values();
            assert_eq!(v.len(), 6, "function_list row must have 6 columns");
            (
                as_text(&v[0]),
                as_int(&v[1]),
                as_text(&v[2]),
                as_text(&v[3]),
                as_int(&v[4]),
            )
        })
        .collect()
}

fn sqlite_function_names() -> BTreeSet<String> {
    let r = rusqlite::Connection::open_in_memory().unwrap();
    let mut stmt = r.prepare("SELECT name FROM pragma_function_list").unwrap();
    let names = stmt
        .query_map([], |row| row.get::<_, String>(0))
        .unwrap()
        .map(Result::unwrap)
        .map(|n| n.to_ascii_lowercase())
        .collect();
    names
}

/// The catalog is non-empty and contains standard scalar functions classified
/// as scalar (`type = 's'`), `builtin = 1`, `enc = 'utf8'`.
#[test]
fn function_list_is_nonempty_with_scalars_gh206() {
    asupersync::test_utils::run_test(|| async {
        let conn = Connection::open(":memory:").await.unwrap();
        let rows = frank_function_list(&conn).await;
        assert!(
            !rows.is_empty(),
            "PRAGMA function_list must return a non-empty catalog"
        );

        for scalar in ["abs", "length", "upper", "lower", "typeof", "coalesce"] {
            let row = rows
                .iter()
                .find(|(name, ..)| name.eq_ignore_ascii_case(scalar))
                .unwrap_or_else(|| panic!("scalar `{scalar}` missing from function_list"));
            assert_eq!(row.1, 1, "`{scalar}` builtin flag must be 1");
            assert_eq!(row.2, "s", "`{scalar}` type must be scalar 's'");
            assert_eq!(row.3, "utf8", "`{scalar}` enc must be 'utf8'");
        }

        // `abs(x)` has arity 1; `coalesce(...)` is variadic (narg = -1).
        let abs = rows.iter().find(|(n, ..)| n == "abs").unwrap();
        assert_eq!(abs.4, 1, "abs narg must be 1");
        let coalesce = rows.iter().find(|(n, ..)| n == "coalesce").unwrap();
        assert_eq!(coalesce.4, -1, "coalesce narg must be -1 (variadic)");
    });
}

/// Aggregate functions are present in the catalog (as builtins), even though
/// their `type` classification is not asserted.
#[test]
fn function_list_contains_aggregates_gh206() {
    asupersync::test_utils::run_test(|| async {
        let conn = Connection::open(":memory:").await.unwrap();
        let rows = frank_function_list(&conn).await;
        for agg in ["count", "sum", "min", "max", "group_concat"] {
            let row = rows
                .iter()
                .find(|(name, ..)| name.eq_ignore_ascii_case(agg))
                .unwrap_or_else(|| panic!("aggregate `{agg}` missing from function_list"));
            assert_eq!(row.1, 1, "`{agg}` builtin flag must be 1");
        }
    });
}

/// Oracle tie: every function in a core, portable set that stock SQLite reports
/// must also appear in frank's catalog.
#[test]
fn function_list_covers_core_sqlite_functions_gh206() {
    asupersync::test_utils::run_test(|| async {
        let conn = Connection::open(":memory:").await.unwrap();
        let frank: BTreeSet<String> = frank_function_list(&conn)
            .await
            .into_iter()
            .map(|(name, ..)| name.to_ascii_lowercase())
            .collect();
        let sqlite = sqlite_function_names();

        let core = [
            "abs", "coalesce", "hex", "ifnull", "instr", "length", "lower", "ltrim", "max", "min",
            "nullif", "quote", "replace", "round", "rtrim", "substr", "trim", "typeof", "upper",
            "count", "sum", "total", "avg", "group_concat",
        ];
        for f in core {
            assert!(
                sqlite.contains(f),
                "oracle drift: stock SQLite lacks `{f}` (test premise wrong)"
            );
            assert!(
                frank.contains(f),
                "frank function_list is missing core function `{f}`"
            );
        }
    });
}
