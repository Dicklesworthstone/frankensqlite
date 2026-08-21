//! bd-alter-add-column-stored-empty-and-notnull-defaul-05537: two behavioral
//! ALTER TABLE ADD COLUMN divergences, both row-gated exactly like stock
//! sqlite3 3.46.1:
//!
//! (A) A STORED generated column may be added on an EMPTY table (and computes
//!     on later inserts) but is rejected on a NON-EMPTY table at step time with
//!     the verbatim message `cannot add a STORED column` (SQLITE_ERROR).
//! (B) A NOT NULL column whose back-fill default is NULL — no default OR an
//!     explicit `DEFAULT NULL` — is rejected on a NON-EMPTY table with
//!     `Cannot add a NOT NULL column with default value NULL`, but allowed on an
//!     empty table.
//!
//! VIRTUAL generated adds and literal-default NOT NULL adds are stock-correct
//! regressions guarded here too.

use fsqlite_core::connection::{Connection, Row};
use fsqlite_error::FrankenError;
use fsqlite_types::value::SqliteValue;

async fn alter_error(conn: &Connection, sql: &str) -> String {
    match conn.execute(sql).await {
        Ok(_) => panic!("expected an error for `{sql}`"),
        Err(FrankenError::FunctionError(message)) => message,
        Err(other) => panic!("`{sql}` expected FunctionError (SQLITE_ERROR), got {other:?}"),
    }
}

fn int_row(rows: &[Row], col: usize) -> Vec<i64> {
    rows.iter()
        .map(|r| match &r.values()[col] {
            SqliteValue::Integer(i) => *i,
            other => panic!("expected integer, got {other:?}"),
        })
        .collect()
}

#[test]
fn bd_05537_alter_add_stored_and_notnull_null_row_gated() {
    asupersync::test_utils::run_test(|| async {
        let conn = Connection::open(":memory:").await.unwrap();

        // (A) STORED generated column on an EMPTY table: allowed, and computes
        // its value on subsequent inserts (stock: `5|5`).
        conn.execute("CREATE TABLE se(a);").await.unwrap();
        conn.execute("ALTER TABLE se ADD COLUMN c INT GENERATED ALWAYS AS (a) STORED;")
            .await
            .expect("STORED generated column must be addable to an empty table");
        conn.execute("INSERT INTO se(a) VALUES(5);").await.unwrap();
        let rows = conn.query("SELECT a, c FROM se;").await.unwrap();
        assert_eq!(int_row(&rows, 0), vec![5], "base column");
        assert_eq!(
            int_row(&rows, 1),
            vec![5],
            "ALTER-added STORED column must materialize on insert"
        );

        // (A) STORED generated column on a NON-EMPTY table: rejected verbatim.
        conn.execute("CREATE TABLE sn(a);").await.unwrap();
        conn.execute("INSERT INTO sn VALUES(1);").await.unwrap();
        assert_eq!(
            alter_error(
                &conn,
                "ALTER TABLE sn ADD COLUMN c INT GENERATED ALWAYS AS (a) STORED;"
            )
            .await,
            "cannot add a STORED column"
        );

        // (B) NOT NULL DEFAULT NULL on a NON-EMPTY table: rejected verbatim (the
        // explicit NULL literal must be treated like no default at all).
        conn.execute("CREATE TABLE nn(a);").await.unwrap();
        conn.execute("INSERT INTO nn VALUES(1);").await.unwrap();
        assert_eq!(
            alter_error(&conn, "ALTER TABLE nn ADD COLUMN b NOT NULL DEFAULT NULL;").await,
            "Cannot add a NOT NULL column with default value NULL"
        );
        // Same message when there is no default at all (regression guard).
        assert_eq!(
            alter_error(&conn, "ALTER TABLE nn ADD COLUMN d NOT NULL;").await,
            "Cannot add a NOT NULL column with default value NULL"
        );

        // (B) NOT NULL DEFAULT NULL on an EMPTY table: allowed (no rows to
        // back-fill), matching stock.
        conn.execute("CREATE TABLE ne(a);").await.unwrap();
        conn.execute("ALTER TABLE ne ADD COLUMN b NOT NULL DEFAULT NULL;")
            .await
            .expect("NOT NULL DEFAULT NULL is legal on an empty table");

        // Regression: VIRTUAL generated add on a non-empty table stays legal.
        conn.execute("CREATE TABLE vg(a);").await.unwrap();
        conn.execute("INSERT INTO vg VALUES(3);").await.unwrap();
        conn.execute("ALTER TABLE vg ADD COLUMN c INT GENERATED ALWAYS AS (a) VIRTUAL;")
            .await
            .expect("VIRTUAL generated column is legal on a non-empty table");
        let rows = conn.query("SELECT a, c FROM vg;").await.unwrap();
        assert_eq!(int_row(&rows, 1), vec![3], "VIRTUAL generated column value");

        // Regression: NOT NULL with a real literal default stays legal on a
        // non-empty table and back-fills with the constant.
        conn.execute("CREATE TABLE ld(a);").await.unwrap();
        conn.execute("INSERT INTO ld VALUES(7);").await.unwrap();
        conn.execute("ALTER TABLE ld ADD COLUMN b NOT NULL DEFAULT 0;")
            .await
            .expect("NOT NULL with a literal default is legal on a non-empty table");
        let rows = conn.query("SELECT a, b FROM ld;").await.unwrap();
        assert_eq!(int_row(&rows, 1), vec![0], "literal default back-fill");
    });
}
