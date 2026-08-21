//! bd-ic9nu: the FTS5 `rank` column must be usable in a WHERE predicate
//! (e.g. `WHERE t MATCH '...' AND rank < 0`), not only in SELECT / ORDER BY.
//!
//! `rank` equals the current row's bm25 score (negative for a match). Before the
//! fix, referencing it in the WHERE clause alone errored with
//! `internal error: column not found: rank`, because the FTS5 auxiliary context
//! (which supplies per-row rank values) was built only when `rank`/aux functions
//! appeared in the projected columns or ORDER BY — never from the WHERE clause.

use fsqlite_core::connection::Connection;
use fsqlite_types::value::SqliteValue;

fn rowids(rows: &[fsqlite_core::connection::Row]) -> Vec<i64> {
    rows.iter()
        .map(|row| match row.values()[0] {
            SqliteValue::Integer(n) => n,
            ref other => panic!("expected rowid integer, got {other:?}"),
        })
        .collect()
}

#[test]
fn bd_ic9nu_fts5_rank_usable_in_where_predicate() {
    asupersync::test_utils::run_test(|| async {
        let conn = Connection::open(":memory:").await.unwrap();
        conn.execute("CREATE VIRTUAL TABLE docs USING fts5(title, body);")
            .await
            .unwrap();
        conn.execute(
            "INSERT INTO docs VALUES \
             ('the quick brown fox','jumps over the lazy dog'),\
             ('quick start guide','a fast introduction to quick things'),\
             ('brown bear','the bear is brown and big');",
        )
        .await
        .unwrap();

        // Baseline: the plain MATCH returns rows 1 and 2.
        let matched = conn
            .query("SELECT rowid FROM docs WHERE docs MATCH 'quick' ORDER BY rowid;")
            .await
            .unwrap();
        assert_eq!(rowids(&matched), vec![1, 2]);

        // `rank < 0` in the WHERE clause alone (not in SELECT or ORDER BY) must
        // resolve to the bm25 score and keep every matched row (all scores < 0).
        let filtered = conn
            .query("SELECT rowid FROM docs WHERE docs MATCH 'quick' AND rank < 0 ORDER BY rowid;")
            .await
            .unwrap();
        assert_eq!(
            rowids(&filtered),
            vec![1, 2],
            "rank<0 in WHERE keeps all matched rows"
        );

        // An impossible rank predicate excludes every row (no error).
        let none = conn
            .query("SELECT rowid FROM docs WHERE docs MATCH 'quick' AND rank > 0 ORDER BY rowid;")
            .await
            .unwrap();
        assert!(none.is_empty(), "rank>0 excludes all matched rows");

        // `rank` still works when projected and ordered, together with a WHERE
        // predicate over it.
        let combined = conn
            .query(
                "SELECT rowid FROM docs WHERE docs MATCH 'quick' AND rank < 0 ORDER BY rank, rowid;",
            )
            .await
            .unwrap();
        assert_eq!(rowids(&combined).len(), 2, "rank in WHERE and ORDER BY");

        conn.close().await.unwrap();
    });
}
