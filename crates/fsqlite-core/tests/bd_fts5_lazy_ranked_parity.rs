//! itcc4.5 differential harness for lazy FTS5 **ranked** reads: fsqlite's
//! `ORDER BY rank` / `bm25()` on a lazily-bound on-disk index must match stock
//! SQLite exactly — both the row ORDER and the bm25 score VALUES.
//!
//! Stock (`rusqlite`) writes a corpus with VARIED per-doc term frequencies (so
//! bm25 actually re-orders rows rather than returning a flat score), for BOTH a
//! regular-content and a `content=''` contentless table. FrankenSQLite reopens
//! the file — the on-disk segments bind lazily — and every ranked query must
//! return the same rowids in the same rank order with the same bm25 scores.
//!
//! This is the safety net for the ranked-lazy-scoring work (bd-fts5-lazy Fix C):
//! it stays green whether ranking is served by the (slow) promote path or by a
//! lazy no-promote score source, so it guards correctness across that change.

use fsqlite_core::connection::Connection;
use fsqlite_types::value::SqliteValue;

/// (rowid, bm25) pairs from a FrankenSQLite `SELECT rowid, rank ... ORDER BY rank`.
fn frank_ranked(rows: &[fsqlite_core::connection::Row]) -> Vec<(i64, f64)> {
    rows.iter()
        .map(|row| {
            let rowid = match row.values()[0] {
                SqliteValue::Integer(n) => n,
                ref other => panic!("expected rowid integer, got {other:?}"),
            };
            let score = match row.values()[1] {
                SqliteValue::Float(f) => f,
                SqliteValue::Integer(n) => n as f64,
                ref other => panic!("expected rank float, got {other:?}"),
            };
            (rowid, score)
        })
        .collect()
}

fn stock_ranked(stock: &rusqlite::Connection, table: &str, query: &str) -> Vec<(i64, f64)> {
    stock
        .prepare(&format!(
            "SELECT rowid, rank FROM {table} WHERE {table} MATCH ? ORDER BY rank, rowid"
        ))
        .unwrap()
        .query_map(rusqlite::params![query], |r| Ok((r.get(0)?, r.get(1)?)))
        .unwrap()
        .collect::<std::result::Result<Vec<_>, _>>()
        .unwrap()
}

/// Assert two ranked result sets agree on rowid rank ORDER, and (when
/// `check_values`) on the bm25 score VALUES to 1e-6. Ties on score are broken by
/// rowid on both sides so the comparison is stable.
///
/// `check_values` is currently gated to the regular-content table: fsqlite's
/// bm25 VALUE for a `content=''` contentless table diverges slightly from stock
/// (a small pre-existing scoring-precision difference on the promote path,
/// tracked separately) even though the rank ORDER is correct. The lazy-scoring
/// work must not change either, so ORDER is asserted for both schemas.
fn assert_ranked_eq(
    label: &str,
    mut frank: Vec<(i64, f64)>,
    expected: &[(i64, f64)],
    check_values: bool,
) {
    // Stable tie-break by (score, rowid) — matches the stock query's ORDER BY.
    frank.sort_by(|a, b| {
        a.1.partial_cmp(&b.1)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then_with(|| a.0.cmp(&b.0))
    });
    assert_eq!(
        frank.len(),
        expected.len(),
        "{label}: row count diverged (frank={}, stock={})",
        frank.len(),
        expected.len()
    );
    for (i, ((fr, fs), (sr, ss))) in frank.iter().zip(expected.iter()).enumerate() {
        assert_eq!(fr, sr, "{label}: rowid at rank {i} diverged");
        if check_values {
            assert!(
                (fs - ss).abs() < 1e-6,
                "{label}: bm25 at rank {i} (rowid {fr}) diverged: frank={fs} stock={ss}"
            );
        }
    }
}

#[test]
fn bd_fts5_lazy_ranked_parity_matches_stock() {
    asupersync::test_utils::run_test(|| async {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("fts5_ranked_parity.db");

        // --- Stock writes two corpora with VARIED term frequencies. ---
        {
            let stock = rusqlite::Connection::open(&db_path).unwrap();
            stock
                .execute_batch(
                    "CREATE VIRTUAL TABLE c USING fts5(x);\n\
                     CREATE VIRTUAL TABLE d USING fts5(x, content='', contentless_delete=1);\n\
                     INSERT INTO d(d, rank) VALUES('pgsz', 64);\n\
                     INSERT INTO c(c, rank) VALUES('pgsz', 64);",
                )
                .unwrap();
            // 120 docs. Terms are chosen so bm25 actually re-orders rows:
            // - 'beta' in every 3rd doc (df=40 < N), with VARYING term frequency
            //   (1..=4 copies) so beta-docs land in distinct score tiers.
            // - 'gamma' in every 7th doc (df~17), tf=2, a sparser/higher-idf term.
            // - 'delta' in every 3rd doc too (overlaps beta) for AND/OR shapes.
            // - a per-doc unique token so doc lengths vary (bm25's dl/avgdl term).
            // ('alpha'-in-every-doc is deliberately avoided: df==N gives idf~0 =
            // a degenerate flat ranking that is not a useful parity discriminator.)
            for id in 1..=120_i64 {
                let mut parts = vec![format!("uniq{id}"), format!("pad{}", id % 11)];
                if id % 3 == 0 {
                    let beta = vec!["beta"; ((id / 3 % 4) + 1) as usize].join(" ");
                    parts.push(beta);
                    parts.push("delta".to_owned());
                }
                if id % 7 == 0 {
                    parts.push("gamma gamma".to_owned());
                }
                let text = parts.join(" ");
                stock
                    .execute(
                        "INSERT INTO c(rowid, x) VALUES (?1, ?2)",
                        rusqlite::params![id, text],
                    )
                    .unwrap();
                stock
                    .execute(
                        "INSERT INTO d(rowid, x) VALUES (?1, ?2)",
                        rusqlite::params![id, text],
                    )
                    .unwrap();
            }
            // A couple of deletes on the contentless table -> tombstones.
            for id in [9_i64, 42] {
                stock
                    .execute("DELETE FROM d WHERE rowid = ?1", rusqlite::params![id])
                    .unwrap();
                stock
                    .execute("DELETE FROM c WHERE rowid = ?1", rusqlite::params![id])
                    .unwrap();
            }
            stock
                .execute_batch("INSERT INTO c(c) VALUES('optimize'); INSERT INTO d(d) VALUES('optimize');")
                .unwrap();
        }

        // --- FrankenSQLite lazy-reads and must match stock on every ranked query. ---
        let conn = Connection::open(db_path.to_str().unwrap()).await.unwrap();
        let stock = rusqlite::Connection::open(&db_path).unwrap();

        // (table, query) pairs. Same query set on both the content ('c') and
        // contentless ('d') tables.
        // Exact-term / boolean ranked shapes with df<N terms and varying term
        // frequency, so ORDER BY rank is a genuine discriminator. (Prefix ranked
        // is intentionally excluded: fsqlite's prefix bm25 diverges from stock
        // today — a separate pre-existing defect tracked on its own bead, not
        // something this lazy-scoring safety net should assert or gate.)
        let queries = [
            "beta",               // df=40, varying tf -> distinct score tiers
            "gamma",              // df~17, higher idf
            "beta AND gamma",     // intersection, re-ranked
            "beta OR gamma",      // union, mixed frequencies
            "delta",              // df=40, overlaps beta docs
            "uniq7",              // single doc
            "uniq9",              // deleted doc (must be empty)
        ];
        for table in ["c", "d"] {
            for q in queries {
                let frank = conn
                    .query(&format!(
                        "SELECT rowid, rank FROM {table} WHERE {table} MATCH '{q}' ORDER BY rank, rowid;"
                    ))
                    .await
                    .unwrap_or_else(|e| panic!("frank ranked query {table}/{q:?} failed: {e}"));
                let expected = stock_ranked(&stock, table, q);
                assert_ranked_eq(
                    &format!("{table} MATCH {q:?} ORDER BY rank"),
                    frank_ranked(&frank),
                    &expected,
                    table == "c",
                );
            }
        }

        // Also check a custom bm25() weighting parses+scores identically on the
        // lazy path (single-column tables: one weight).
        for table in ["c", "d"] {
            let frank = conn
                .query(&format!(
                    "SELECT rowid, rank FROM {table} WHERE {table} MATCH 'beta' \
                     AND rank MATCH 'bm25(2.0)' ORDER BY rank, rowid;"
                ))
                .await
                .unwrap_or_else(|e| panic!("frank bm25() query {table} failed: {e}"));
            let expected: Vec<(i64, f64)> = stock
                .prepare(&format!(
                    "SELECT rowid, rank FROM {table} WHERE {table} MATCH 'beta' \
                     AND rank MATCH 'bm25(2.0)' ORDER BY rank, rowid"
                ))
                .unwrap()
                .query_map([], |r| Ok((r.get(0)?, r.get(1)?)))
                .unwrap()
                .collect::<std::result::Result<Vec<_>, _>>()
                .unwrap();
            assert_ranked_eq(
                &format!("{table} MATCH 'beta' bm25(2.0)"),
                frank_ranked(&frank),
                &expected,
                table == "c",
            );
        }

        conn.close().await.unwrap();
    });
}
