//! SQL-level regressions for math coercion, result types, and non-finite values.
//! Expected results were checked against SQLite 3.46.1 with math enabled.

#![cfg(feature = "native")]
#![recursion_limit = "512"]

use fsqlite::{Connection, SqliteValue};

fn text(value: &str) -> SqliteValue {
    SqliteValue::Text(value.into())
}

fn null_math_result() -> [SqliteValue; 4] {
    [
        SqliteValue::Null,
        SqliteValue::Null,
        text("null"),
        SqliteValue::Null,
    ]
}

#[test]
fn prepared_math_rebinding_preserves_nulls_infinities_and_storage_classes() {
    asupersync::test_utils::run_test(|| async {
        let conn = Connection::open(":memory:").await.unwrap();
        {
            let stmt = conn
                .prepare("SELECT sqrt(?1), ceil(?1), typeof(ceil(?1)), pow(?1, 0)")
                .await
                .unwrap();
            for (input, expected) in [
                (
                    text("4"),
                    [
                        SqliteValue::Float(2.0),
                        SqliteValue::Integer(4),
                        text("integer"),
                        SqliteValue::Float(1.0),
                    ],
                ),
                (
                    text("1e999"),
                    [
                        SqliteValue::Float(f64::INFINITY),
                        SqliteValue::Float(f64::INFINITY),
                        text("real"),
                        SqliteValue::Float(1.0),
                    ],
                ),
                (text("\u{00a0}4"), null_math_result()),
                (SqliteValue::Float(f64::NAN), null_math_result()),
                (
                    text("4.0"),
                    [
                        SqliteValue::Float(2.0),
                        SqliteValue::Float(4.0),
                        text("real"),
                        SqliteValue::Float(1.0),
                    ],
                ),
                (SqliteValue::Blob(vec![b'4'].into()), null_math_result()),
                (
                    text("-1e999"),
                    [
                        SqliteValue::Null,
                        SqliteValue::Float(f64::NEG_INFINITY),
                        text("real"),
                        SqliteValue::Float(1.0),
                    ],
                ),
                (text("inf"), null_math_result()),
                (
                    text(" \t4e0\u{000b}\r"),
                    [
                        SqliteValue::Float(2.0),
                        SqliteValue::Float(4.0),
                        text("real"),
                        SqliteValue::Float(1.0),
                    ],
                ),
            ] {
                let rows = stmt
                    .query_with_params(std::slice::from_ref(&input))
                    .await
                    .unwrap();
                assert_eq!(rows.len(), 1, "input {input:?}");
                assert_eq!(rows[0].values(), &expected, "input {input:?}");
            }
        }
        conn.close().await.unwrap();
    });
}

#[test]
fn atanh_sql_preserves_endpoint_infinities_but_rejects_domain_errors() {
    asupersync::test_utils::run_test(|| async {
        let conn = Connection::open(":memory:").await.unwrap();
        let row = conn
            .query_row(
                "SELECT atanh(1), atanh(-1), atanh(2), atanh(-2), \
                 atanh('1'), atanh('-1'), typeof(atanh(1)), pow(NULL, 0), power(1, NULL)",
            )
            .await
            .unwrap();
        assert_eq!(
            row.values(),
            &[
                SqliteValue::Float(f64::INFINITY),
                SqliteValue::Float(f64::NEG_INFINITY),
                SqliteValue::Null,
                SqliteValue::Null,
                SqliteValue::Float(f64::INFINITY),
                SqliteValue::Float(f64::NEG_INFINITY),
                text("real"),
                SqliteValue::Null,
                SqliteValue::Null,
            ]
        );
        conn.close().await.unwrap();
    });
}

#[test]
fn math_rounding_keeps_exact_integer_text_through_sql_binding() {
    asupersync::test_utils::run_test(|| async {
        let conn = Connection::open(":memory:").await.unwrap();
        {
            let stmt = conn
                .prepare("SELECT ceil(?1), ceiling(?1), floor(?1), trunc(?1), typeof(ceil(?1))")
                .await
                .unwrap();
            for expected in [i64::MIN, i64::MAX, 9_007_199_254_740_993] {
                for space in [' ', '\t', '\n', '\r', '\u{000b}', '\u{000c}'] {
                    let input = text(&format!("{space}{expected}{space}"));
                    let rows = stmt.query_with_params(&[input]).await.unwrap();
                    assert_eq!(rows.len(), 1);
                    assert_eq!(
                        rows[0].values(),
                        &[
                            SqliteValue::Integer(expected),
                            SqliteValue::Integer(expected),
                            SqliteValue::Integer(expected),
                            SqliteValue::Integer(expected),
                            text("integer"),
                        ],
                        "integer {expected} with whitespace {space:?}"
                    );
                }
            }
        }
        conn.close().await.unwrap();
    });
}

#[test]
fn persisted_numeric_text_uses_the_same_math_rules_in_projection_and_filter() {
    asupersync::test_utils::run_test(|| async {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("math.db");
        let path = path.to_str().unwrap();
        let conn = Connection::open(path).await.unwrap();
        conn.execute("CREATE TABLE inputs (id INTEGER PRIMARY KEY, value TEXT)")
            .await
            .unwrap();
        conn.execute(
            "INSERT INTO inputs VALUES \
             (1, '1e999'), (2, 'NaN'), (3, char(160) || '4'), (4, ' 4.0 ')",
        )
        .await
        .unwrap();
        conn.close().await.unwrap();

        let conn = Connection::open(path).await.unwrap();
        let rows = conn
            .query("SELECT id, sqrt(value), typeof(sqrt(value)) FROM inputs ORDER BY id")
            .await
            .unwrap();
        assert_eq!(rows.len(), 4);
        for (row, expected) in rows.iter().zip([
            [
                SqliteValue::Integer(1),
                SqliteValue::Float(f64::INFINITY),
                text("real"),
            ],
            [SqliteValue::Integer(2), SqliteValue::Null, text("null")],
            [SqliteValue::Integer(3), SqliteValue::Null, text("null")],
            [SqliteValue::Integer(4), SqliteValue::Float(2.0), text("real")],
        ]) {
            assert_eq!(row.values(), &expected);
        }
        let selected = conn
            .query("SELECT id FROM inputs WHERE sqrt(value) IS NOT NULL ORDER BY id")
            .await
            .unwrap();
        let ids: Vec<_> = selected.iter().map(|row| row.get(0).cloned()).collect();
        assert_eq!(
            ids,
            vec![Some(SqliteValue::Integer(1)), Some(SqliteValue::Integer(4))]
        );
        conn.close().await.unwrap();
    });
}
