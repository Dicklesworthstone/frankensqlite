//! bd-gh-printf-negative-zero-era4w (GH #258): printf/format must normalize
//! signed zero for %f/%e/%g like C SQLite (`-0.0` renders as `0`, not `-0`),
//! including sign flags (`%+g` -> `+0`, `% g` -> ` 0`), width, and
//! arithmetic-produced negative zero. Real negatives must still render `-`.

use fsqlite_core::connection::Connection;
use fsqlite_types::SqliteValue;

// Each expression is `SELECT <expr>` returning one text value; fsqlite must
// match the rusqlite/C-SQLite oracle exactly.
const EXPRS: &[&str] = &[
    "printf('%g', -0.0)",
    "format('%g', -0.0)",
    "printf('%G', -0.0)",
    "printf('%f', -0.0)",
    "printf('%e', -0.0)",
    "printf('%E', -0.0)",
    // Sign flags applied to the normalized +0.0.
    "printf('%+g', -0.0)",
    "printf('% g', -0.0)",
    "printf('%+f', -0.0)",
    // Width / precision.
    "printf('%8.2f', -0.0)",
    "printf('%g', 0.0)",
    // Arithmetic-produced negative zero (underflow).
    "printf('%g', -1e-320 * 1e-10)",
    "printf('%f', -1.0 * 0.0)",
    // Real negatives MUST still show the minus sign (regression guard).
    "printf('%g', -1.5)",
    "printf('%f', -2.25)",
    "printf('%e', -3.0)",
    "printf('%+g', -1.5)",
];

fn oracle(expr: &str) -> String {
    let conn = rusqlite::Connection::open_in_memory().unwrap();
    conn.query_row(&format!("SELECT {expr}"), [], |row| row.get::<_, String>(0))
        .unwrap()
}

#[test]
fn printf_signed_zero_matches_rusqlite_oracle() {
    asupersync::test_utils::run_test(|| async {
        let conn = Connection::open(":memory:").await.unwrap();
        for expr in EXPRS {
            let expected = oracle(expr);
            let sql = format!("SELECT {expr}");
            let rows = conn.query(&sql).await.unwrap_or_else(|e| panic!("`{sql}`: {e:?}"));
            let got = match rows[0].values()[0] {
                SqliteValue::Text(ref s) => s.as_ref().to_owned(),
                ref other => panic!("`{sql}` not text: {other:?}"),
            };
            assert_eq!(
                got, expected,
                "`{expr}` diverged from the C SQLite oracle (got {got:?}, want {expected:?})"
            );
        }
    });
}
