// Keeper: GLOB char-class range `[lo-hi]` matches its lower-bound char `lo` as a
// LITERAL set member even when the range is reversed/empty (hi < lo), matching
// C SQLite's patternCompare (which tests `c2==c` for the range start before
// consuming `-`). The upper bound is NOT a literal member, and the range
// interior matches only `lo..=hi`. Oracle: sqlite3 3.46.1.
use fsqlite_core::connection::Connection;
use fsqlite_types::value::SqliteValue;

async fn glob(c: &Connection, expr: &str) -> i64 {
    let rows = c.query_with_params(&format!("SELECT {expr}"), &[]).await.unwrap();
    match rows.first().map(|r| r.values()[0].clone()) {
        Some(SqliteValue::Integer(n)) => n,
        other => panic!("expected integer for `{expr}`, got {other:?}"),
    }
}

#[test]
fn glob_reversed_range_matches_literal_lower_bound() {
    asupersync::test_utils::run_test(|| async {
        let c = Connection::open(":memory:").await.unwrap();

        // Reversed range [5-0]: matches the lower-bound '5' literally...
        assert_eq!(glob(&c, "'a5c' GLOB 'a[5-0]c'").await, 1);
        // ...but NOT the upper bound '0', and nothing in between.
        assert_eq!(glob(&c, "'a0c' GLOB 'a[5-0]c'").await, 0);
        assert_eq!(glob(&c, "'a3c' GLOB 'a[5-0]c'").await, 0);

        // Negated reversed range [^5-0]: excludes the literal '5', admits '0'.
        assert_eq!(glob(&c, "'a5c' GLOB 'a[^5-0]c'").await, 0);
        assert_eq!(glob(&c, "'a0c' GLOB 'a[^5-0]c'").await, 1);

        // Reversed alpha range [z-a] matches 'z' (lower bound) but not 'a'.
        assert_eq!(glob(&c, "'zz' GLOB '[z-a]z'").await, 1);
        assert_eq!(glob(&c, "'az' GLOB '[z-a]z'").await, 0);

        // Normal ranges are unaffected: lower/interior/upper all match.
        assert_eq!(glob(&c, "'a0c' GLOB 'a[0-9]c'").await, 1);
        assert_eq!(glob(&c, "'a5c' GLOB 'a[0-9]c'").await, 1);
        assert_eq!(glob(&c, "'a9c' GLOB 'a[0-9]c'").await, 1);
        assert_eq!(glob(&c, "'axc' GLOB 'a[0-9]c'").await, 0);
        // glob() function form routes through the same matcher.
        assert_eq!(glob(&c, "glob('a[5-0]c','a5c')").await, 1);
    });
}
