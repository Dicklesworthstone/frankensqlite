//! bd-pulz6: the FTS5 `rank MATCH '<rankfn>'` (and `rank = '<rankfn>'`) idiom
//! binds a custom ranking function for the query — it is NOT a row filter.
//!
//! Before the fix, `SELECT rowid FROM docs WHERE docs MATCH 'quick'
//! AND rank MATCH 'bm25(...)' ORDER BY rank` returned EMPTY: `rank` is a
//! computed column, so the directive never mapped to a vtab constraint and fell
//! through to the residual WHERE, where evaluating `<float> MATCH '<text>'` is
//! falsy for every row and silently dropped the whole result. The directive is
//! now stripped from the per-row WHERE, and its bm25 column weights are applied
//! when computing `rank`, so the rows come back re-ranked with the supplied
//! weights — matching stock SQLite 3.46.1.
//!
//! Oracle (stock sqlite3 3.46.1) with docs(title, body):
//!   1: 'quick',              'nothing here at all just filler text'
//!   2: 'irrelevant heading', 'quick quick quick brown fox jumps quick'
//!   ORDER BY rank (default) ................ 2, 1
//!   rank MATCH 'bm25(10.0, 0.1)' (title) ... 1, 2
//!   rank MATCH 'bm25(0.1, 10.0)' (body) .... 2, 1
//!   rank = 'bm25(10.0, 0.1)' ............... 1, 2
//!   rank MATCH 'bm25(10.0)' (== 10.0, 1.0) . 1, 2

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
fn bd_pulz6_fts5_rank_custom_ranking_reranks() {
    asupersync::test_utils::run_test(|| async {
        let conn = Connection::open(":memory:").await.unwrap();
        conn.execute("CREATE VIRTUAL TABLE docs USING fts5(title, body);")
            .await
            .unwrap();
        conn.execute(
            "INSERT INTO docs(rowid, title, body) VALUES \
             (1, 'quick', 'nothing here at all just filler text'),\
             (2, 'irrelevant heading', 'quick quick quick brown fox jumps quick');",
        )
        .await
        .unwrap();

        // Baseline: default rank orders 2 before 1 (doc 2 has more hits).
        let default_order = conn
            .query("SELECT rowid FROM docs WHERE docs MATCH 'quick' ORDER BY rank;")
            .await
            .unwrap();
        assert_eq!(rowids(&default_order), vec![2, 1], "default ORDER BY rank");

        // Core bug: the directive must NOT empty the result.
        let with_directive = conn
            .query(
                "SELECT rowid FROM docs WHERE docs MATCH 'quick' \
                 AND rank MATCH 'bm25(10.0, 0.1)' ORDER BY rank;",
            )
            .await
            .unwrap();
        assert_eq!(
            rowids(&with_directive),
            vec![1, 2],
            "rank MATCH 'bm25(10.0,0.1)' must re-rank (title-heavy), not return empty",
        );

        // Weighting the body column heavily flips the order back to 2, 1.
        let body_heavy = conn
            .query(
                "SELECT rowid FROM docs WHERE docs MATCH 'quick' \
                 AND rank MATCH 'bm25(0.1, 10.0)' ORDER BY rank;",
            )
            .await
            .unwrap();
        assert_eq!(
            rowids(&body_heavy),
            vec![2, 1],
            "rank MATCH 'bm25(0.1,10.0)' must re-rank body-heavy",
        );

        // The `rank = '<rankfn>'` spelling is equivalent to `rank MATCH`.
        let eq_form = conn
            .query(
                "SELECT rowid FROM docs WHERE docs MATCH 'quick' \
                 AND rank = 'bm25(10.0, 0.1)' ORDER BY rank;",
            )
            .await
            .unwrap();
        assert_eq!(rowids(&eq_form), vec![1, 2], "rank = 'bm25(...)' form");

        // Fewer weights than columns: trailing columns default to 1.0, so
        // `bm25(10.0)` == `bm25(10.0, 1.0)` and still orders 1 before 2.
        let short_weights = conn
            .query(
                "SELECT rowid FROM docs WHERE docs MATCH 'quick' \
                 AND rank MATCH 'bm25(10.0)' ORDER BY rank;",
            )
            .await
            .unwrap();
        assert_eq!(
            rowids(&short_weights),
            vec![1, 2],
            "bm25(10.0) pads trailing weights to 1.0",
        );

        // The projected `rank` value itself must reflect the custom weights: the
        // title-heavy weighting makes doc 1 (title hit) the best (most negative)
        // score, strictly less than doc 2's.
        let scored = conn
            .query(
                "SELECT rowid, rank FROM docs WHERE docs MATCH 'quick' \
                 AND rank MATCH 'bm25(10.0, 0.1)' ORDER BY rank;",
            )
            .await
            .unwrap();
        let scores: Vec<(i64, f64)> = scored
            .iter()
            .map(|row| {
                let vals = row.values();
                let id = match vals[0] {
                    SqliteValue::Integer(n) => n,
                    ref other => panic!("expected rowid integer, got {other:?}"),
                };
                let rank = match vals[1] {
                    SqliteValue::Float(f) => f,
                    ref other => panic!("expected rank float, got {other:?}"),
                };
                (id, rank)
            })
            .collect();
        assert_eq!(scores.len(), 2, "custom-ranked rows must be returned");
        assert_eq!(scores[0].0, 1, "title-heavy weighting ranks doc 1 first");
        assert!(
            scores[0].1 < scores[1].1,
            "best row's rank score must be strictly smaller: {scores:?}",
        );
    });
}
