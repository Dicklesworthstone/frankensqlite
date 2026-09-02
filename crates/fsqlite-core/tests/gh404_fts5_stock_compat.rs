//! GH#404 / bd-jdq9v — FTS5 `%_data` stock-compatibility keeper.
//!
//! An fsqlite-written multi-leaf-page FTS5 index must be readable and
//! verifiable by bundled stock C SQLite:
//!
//! - `PRAGMA integrity_check` = "ok" (previously: "malformed inverted index
//!   for FTS5 table main.t" because every leaf header carried a nonzero
//!   first-rowid offset pointing AFTER the first term, so stock misparsed
//!   every page >= 2 as a doclist continuation);
//! - stock `MATCH` finds terms whose postings live on leaf pages > 1
//!   (previously: silently 0 rows, because fsqlite never wrote `%_idx` rows,
//!   so stock's term seek never left page 1);
//! - an `fts5vocab` scan enumerates the vocabulary without error.
//!
//! The corpus is deliberately sized to force `pgno_last > 1` (the bug is
//! invisible on a single-leaf segment: stock never consults the first-rowid
//! offset or `%_idx` below one leaf page), and the test asserts that
//! precondition so a future corpus shrink cannot silently stop covering the
//! bug. fsqlite reopen parity guards the other direction: the added `%_idx`
//! rows must not break fsqlite's own lazy reader.

use fsqlite_core::connection::Connection;
use fsqlite_types::value::SqliteValue;

/// `id & ((1<<37)-1)` — the pgno half of stock's `%_data` segment-leaf rowid
/// encoding `(segid << 37) | pgno`.
const FTS5_DATA_PGNO_MASK: i64 = (1_i64 << 37) - 1;

fn first_integer(rows: &[fsqlite_core::connection::Row]) -> i64 {
    match rows
        .first()
        .and_then(|row| row.values().first().cloned())
    {
        Some(SqliteValue::Integer(n)) => n,
        other => panic!("expected a single integer result, got {other:?}"),
    }
}

async fn franken_count(conn: &Connection, sql: &str) -> i64 {
    first_integer(&conn.query(sql).await.unwrap_or_else(|e| {
        panic!("franken query failed: {sql}: {e}");
    }))
}

fn stock_count(stock: &rusqlite::Connection, sql: &str) -> i64 {
    stock
        .query_row(sql, [], |r| r.get(0))
        .unwrap_or_else(|e| panic!("stock query failed: {sql}: {e}"))
}

/// A porter-stable unique token: digit suffixes are never stemmed.
fn tok(n: usize) -> String {
    format!("tok{n:06}")
}

/// Build the GH#404-shaped table and load `rows` documents in multi-row
/// INSERT batches of `batch`, deleting a stripe of earlier rowids every few
/// batches (tombstone coverage). Returns the probe terms recorded while the
/// writer connection is still open, as (term, franken_match_count).
async fn build_corpus(conn: &Connection, rows: usize, batch: usize) -> Vec<(String, i64)> {
    conn.execute(
        "CREATE VIRTUAL TABLE t USING fts5(content, title, created_at UNINDEXED, \
         content='', contentless_delete=1, tokenize='porter');",
    )
    .await
    .expect("create fts5 table");

    let mut rowid = 0_usize;
    let mut batch_index = 0_usize;
    while rowid < rows {
        let mut stmt = String::from("INSERT INTO t(rowid, content, title, created_at) VALUES ");
        let mut first = true;
        for _ in 0..batch.min(rows - rowid) {
            rowid += 1;
            if !first {
                stmt.push(',');
            }
            first = false;
            // Every doc: one globally unique token + two shared words. The
            // unique tokens sort as tok000001..tokNNNNNN, spreading across the
            // whole term space so later ones land on leaf pages > 1.
            stmt.push_str(&format!(
                "({rowid}, 'shared corpus {unique}', 'title {unique}', '2026-09-01')",
                unique = tok(rowid),
            ));
        }
        stmt.push(';');
        conn.execute("BEGIN").await.expect("begin batch");
        conn.execute(&stmt).await.expect("insert batch");
        conn.execute("COMMIT").await.expect("commit batch");
        batch_index += 1;
        // Every 4th batch: delete a small stripe of older rows so the image
        // carries tombstones (the contentless_delete lazy-delete path).
        if batch_index.is_multiple_of(4) {
            let lo = (batch_index - 1) * 3;
            conn.execute(&format!(
                "DELETE FROM t WHERE rowid IN ({}, {}, {});",
                lo + 1,
                lo + 2,
                lo + 3
            ))
            .await
            .expect("delete stripe");
        }
    }

    // Probe terms: first / middle / last unique token plus a shared word with
    // a long doclist. Record fsqlite's own live answer for later parity.
    let mut probes = Vec::new();
    for term in [tok(1), tok(rows / 2), tok(rows), "shared".to_owned()] {
        let count = franken_count(
            conn,
            &format!("SELECT count(*) FROM t WHERE t MATCH '{term}';"),
        )
        .await;
        probes.push((term, count));
    }
    // The last unique token must still be live (never deleted): the deleted
    // stripes only touch low rowids.
    assert_eq!(
        probes[2].1, 1,
        "probe precondition: the last unique token matches exactly its row"
    );
    probes
}

#[test]
fn gh404_fts5_multi_leaf_index_is_stock_verifiable_and_searchable() {
    asupersync::test_utils::run_test(|| async {
        let dir = tempfile::tempdir().expect("temp dir");
        let db_path = dir.path().join("gh404_fts5.db");
        let db_str = db_path.to_string_lossy().into_owned();

        let conn = Connection::open(&db_str).await.expect("open franken");
        let probes = build_corpus(&conn, 1_200, 50).await;
        conn.execute("PRAGMA wal_checkpoint(TRUNCATE);")
            .await
            .expect("truncate checkpoint");
        conn.close().await.expect("close franken");

        let stock = rusqlite::Connection::open(&db_path).expect("stock open");

        // Precondition: the index actually spans multiple leaf pages — the
        // whole point of the corpus. If this fires, the corpus shrank below
        // the bug's visibility threshold and the keeper stopped covering it.
        let max_leaf_pgno = stock_count(
            &stock,
            &format!("SELECT max(id & {FTS5_DATA_PGNO_MASK}) FROM t_data WHERE id > 10"),
        );
        assert!(
            max_leaf_pgno >= 2,
            "corpus precondition: segment leaves must span multiple pages, got max pgno {max_leaf_pgno}"
        );

        // (1) Stock structural verification accepts the image. Before the
        // GH#404 fix this reported "malformed inverted index for FTS5 table
        // main.t".
        let integrity: String = stock
            .query_row("PRAGMA integrity_check;", [], |r| r.get(0))
            .expect("stock integrity_check");
        assert_eq!(integrity, "ok", "stock PRAGMA integrity_check");

        // (2) `%_idx` is populated stock-shaped: at least one row, encoded
        // pgno (low bit = doclist-index flag, which fsqlite never sets since
        // it never splits a doclist across leaves).
        let idx_rows = stock_count(&stock, "SELECT count(*) FROM t_idx");
        assert!(idx_rows >= 1, "%_idx must carry seek rows, got {idx_rows}");
        let dlidx_flagged = stock_count(&stock, "SELECT count(*) FROM t_idx WHERE (pgno & 1) != 0");
        assert_eq!(dlidx_flagged, 0, "no %_idx row may claim a doclist-index");

        // (3) Stock MATCH parity on every probe — including tokens whose
        // postings live on leaf pages > 1. Before the fix stock silently
        // returned 0 for every term not on page 1.
        for (term, franken_matches) in &probes {
            let stock_matches = stock_count(
                &stock,
                &format!("SELECT count(*) FROM t WHERE t MATCH '{term}'"),
            );
            assert_eq!(
                stock_matches, *franken_matches,
                "stock/franken MATCH parity for probe term {term:?}"
            );
        }

        // (4) The fts5 auxiliary surface walks the index: an fts5vocab scan
        // enumerates a real vocabulary (previously: "database disk image is
        // malformed").
        stock
            .execute_batch("CREATE VIRTUAL TABLE temp.v USING fts5vocab(main, 't', 'row');")
            .expect("create fts5vocab");
        let vocab_terms = stock_count(&stock, "SELECT count(*) FROM temp.v");
        assert!(
            vocab_terms >= 1_000,
            "fts5vocab must enumerate the unique-token vocabulary, got {vocab_terms}"
        );
        drop(stock);

        // (5) Reopen parity: the `%_idx` rows written for stock must not
        // mislead fsqlite's own lazy reader (it seeks via the same rows).
        let reopened = Connection::open(&db_str).await.expect("reopen franken");
        for (term, franken_matches) in &probes {
            let count = franken_count(
                &reopened,
                &format!("SELECT count(*) FROM t WHERE t MATCH '{term}';"),
            )
            .await;
            assert_eq!(
                count, *franken_matches,
                "fsqlite reopen MATCH parity for probe term {term:?}"
            );
        }
        reopened.close().await.expect("close reopened");
    });
}

/// The GH#404 reporter's deterministic repro (cass#438's `fts_messages`
/// shape), verbatim generator: a ~100-word vocabulary drawn by xorshift64*,
/// transactions of 200 rows in 50-row multi-row `VALUES` batches, and a
/// 50-row `DELETE` slice every 5th transaction (contentless_delete
/// tombstones across segment merges). The reporter ran 40 transactions:
/// fsqlite answered `MATCH 'segment'` with 2,759 rows while stock reported
/// "fts5: corruption on page 47, segment 1", stock `MATCH` silently returned
/// 0 (no `%_idx` rows) and `fts5vocab` errored — reproduced 2026-09-01 with
/// the 0.3.13 engine pinned from crates.io. The keeper runs the first 10
/// transactions of the same stream (2,000 rows, 100 deleted; the 40-txn shape
/// takes minutes and proves nothing extra) and pins that prefix's exact
/// count, 702, measured with the same generator.
const REPORTER_VOCAB: &[&str] = &[
    "agent", "session", "search", "index", "message", "conversation", "token", "prompt",
    "response", "error", "warning", "build", "cargo", "rustc", "compile", "worker", "dispatch",
    "queue", "socket", "daemon", "restart", "reload", "memory", "swap", "disk", "cache",
    "segment", "page", "corruption", "integrity", "check", "validate", "parse", "read", "write",
    "commit", "rollback", "transaction", "checkpoint", "journal", "wal", "frame", "btree",
    "leaf", "interior", "overflow", "freelist", "vacuum", "analyze", "optimize", "merge",
    "tokenize", "porter", "stemming", "relational", "hopeful", "revival", "allowance",
    "inference", "probate", "cease", "controlling", "titles", "flying", "crying", "skies",
    "troubled", "trouble", "relating", "connection", "database", "schema", "virtual", "table",
    "shadow", "content", "docsize", "config", "rowid", "column", "unindexed", "created",
    "workspace", "source", "path", "timestamp", "monday", "tuesday", "wednesday", "thursday",
    "friday", "abstraction", "capability", "deterministic", "asynchronous", "runtime",
    "future", "executor", "polling", "latency", "throughput", "benchmark", "profile",
];
const REPORTER_TXNS: i64 = 10;
const REPORTER_ROWS_PER_TXN: i64 = 200;
const REPORTER_SEED: u64 = 0x9E37_79B9_7F4A_7C15;
/// fsqlite's answer for `MATCH 'segment'` over the first 10 transactions of
/// the reporter's stream (2,000 rows written, 100 deleted). Pinned exactly: a
/// tokenizer, merge, or tombstone drift that changes the visible posting set
/// changes this number (the reporter's full 40-transaction run gives 2,759).
const REPORTER_SEGMENT_MATCHES: i64 = 702;
const REPORTER_LIVE_ROWS: i64 = REPORTER_TXNS * REPORTER_ROWS_PER_TXN
    - (REPORTER_TXNS / 5) * (REPORTER_ROWS_PER_TXN / 4);

fn reporter_word(state: &mut u64) -> &'static str {
    let mut x = *state;
    x ^= x >> 12;
    x ^= x << 25;
    x ^= x >> 27;
    *state = x;
    let idx = usize::try_from(x.wrapping_mul(0x2545_F491_4F6C_DD1D) >> 33).expect("u64 >> 33 fits")
        % REPORTER_VOCAB.len();
    REPORTER_VOCAB[idx]
}

fn reporter_doc(state: &mut u64, n_words: usize, salt: u64) -> String {
    let mut s = String::new();
    for i in 0..n_words {
        if i > 0 {
            s.push(' ');
        }
        s.push_str(reporter_word(state));
        if i % 11 == 7 {
            s.push_str(&format!(" uniq{salt}x{i}"));
        }
    }
    s
}

#[test]
fn gh404_reporter_repro_corpus_is_stock_verifiable_with_exact_match_parity() {
    asupersync::test_utils::run_test(|| async {
        let dir = tempfile::tempdir().expect("temp dir");
        let db_path = dir.path().join("gh404_reporter.db");
        let db_str = db_path.to_string_lossy().into_owned();

        let conn = Connection::open(&db_str).await.expect("open franken");
        conn.execute(
            "CREATE VIRTUAL TABLE fts_messages USING fts5(\
             content, title, agent, workspace, source_path, created_at UNINDEXED, \
             content='', contentless_delete=1, tokenize='porter');",
        )
        .await
        .expect("create reporter fts5 table");

        let mut state = REPORTER_SEED;
        let mut rowid = 0_i64;
        for txn in 0..REPORTER_TXNS {
            conn.execute("BEGIN").await.expect("begin txn");
            let mut remaining = REPORTER_ROWS_PER_TXN;
            while remaining > 0 {
                let batch = remaining.min(50);
                let mut sql = String::from(
                    "INSERT INTO fts_messages(rowid, content, title, agent, workspace, \
                     source_path, created_at) VALUES ",
                );
                for i in 0..batch {
                    rowid += 1;
                    let content =
                        reporter_doc(&mut state, 40, u64::try_from(rowid).expect("rowid"));
                    let title = reporter_doc(&mut state, 6, u64::try_from(txn).expect("txn"));
                    if i > 0 {
                        sql.push(',');
                    }
                    sql.push_str(&format!(
                        "({rowid}, '{content}', '{title}', 'claude-code', 'ws{w}', \
                         '/data/projects/p{p}/session-{rowid}.jsonl', {ts})",
                        w = txn % 7,
                        p = rowid % 13,
                        ts = 1_756_600_000 + rowid,
                    ));
                }
                sql.push(';');
                conn.execute(&sql).await.expect("insert batch");
                remaining -= batch;
            }
            if txn % 5 == 4 {
                let lo = (txn - 4) * REPORTER_ROWS_PER_TXN + 1;
                let hi = lo + REPORTER_ROWS_PER_TXN / 4;
                conn.execute(&format!(
                    "DELETE FROM fts_messages WHERE rowid >= {lo} AND rowid < {hi};"
                ))
                .await
                .expect("delete slice");
            }
            conn.execute("COMMIT").await.expect("commit txn");
        }

        // fsqlite's own answers while the writer is still open.
        let live = franken_count(
            &conn,
            "SELECT count(*) FROM fts_messages WHERE fts_messages MATCH 'segment';",
        )
        .await;
        assert_eq!(
            live, REPORTER_SEGMENT_MATCHES,
            "fsqlite MATCH 'segment' on the reporter corpus"
        );
        let uniq_probe = format!("uniq{}x7", REPORTER_TXNS * REPORTER_ROWS_PER_TXN);
        let live_uniq = franken_count(
            &conn,
            &format!("SELECT count(*) FROM fts_messages WHERE fts_messages MATCH '{uniq_probe}';"),
        )
        .await;
        assert_eq!(live_uniq, 1, "the last row's unique term is live in fsqlite");
        conn.execute("INSERT INTO fts_messages(fts_messages) VALUES('integrity-check');")
            .await
            .expect("fsqlite fts5 integrity-check");
        conn.execute("PRAGMA wal_checkpoint(TRUNCATE);")
            .await
            .expect("truncate checkpoint");
        conn.close().await.expect("close franken");

        let stock = rusqlite::Connection::open(&db_path).expect("stock open");
        let integrity: String = stock
            .query_row("PRAGMA integrity_check;", [], |r| r.get(0))
            .expect("stock integrity_check");
        assert_eq!(integrity, "ok", "stock PRAGMA integrity_check on the reporter corpus");
        assert_eq!(
            stock_count(&stock, "SELECT count(*) FROM fts_messages_docsize"),
            REPORTER_LIVE_ROWS,
            "docsize rows = written - deleted"
        );
        assert_eq!(
            stock_count(
                &stock,
                "SELECT count(*) FROM fts_messages WHERE fts_messages MATCH 'segment'"
            ),
            REPORTER_SEGMENT_MATCHES,
            "stock MATCH 'segment' parity (was silently 0 before the fix)"
        );
        assert_eq!(
            stock_count(
                &stock,
                &format!(
                    "SELECT count(*) FROM fts_messages WHERE fts_messages MATCH '{uniq_probe}'"
                )
            ),
            1,
            "stock finds the last row's unique term"
        );
        stock
            .execute_batch("INSERT INTO fts_messages(fts_messages) VALUES('integrity-check');")
            .expect("stock fts5 integrity-check command");
        stock
            .execute_batch(
                "CREATE VIRTUAL TABLE temp.v USING fts5vocab(main, 'fts_messages', 'row');",
            )
            .expect("create fts5vocab");
        let vocab_terms = stock_count(&stock, "SELECT count(*) FROM temp.v");
        assert!(
            vocab_terms > REPORTER_VOCAB.len() as i64,
            "fts5vocab enumerates the vocabulary plus uniq terms, got {vocab_terms}"
        );
        drop(stock);

        let reopened = Connection::open(&db_str).await.expect("reopen franken");
        let again = franken_count(
            &reopened,
            "SELECT count(*) FROM fts_messages WHERE fts_messages MATCH 'segment';",
        )
        .await;
        assert_eq!(again, REPORTER_SEGMENT_MATCHES, "fsqlite reopen MATCH parity");
        reopened.close().await.expect("close reopened");
    });
}

/// 'delete-all' must clear `%_idx` along with the segments: the next insert
/// restarts segids at 1, so a stale pre-delete-all `%_idx` row for segid 1
/// would alias the new segment's seek space (wrong pages for both engines).
#[test]
fn gh404_fts5_delete_all_clears_idx_and_rebuild_stays_stock_searchable() {
    asupersync::test_utils::run_test(|| async {
        let dir = tempfile::tempdir().expect("temp dir");
        let db_path = dir.path().join("gh404_fts5_delete_all.db");
        let db_str = db_path.to_string_lossy().into_owned();

        let conn = Connection::open(&db_str).await.expect("open franken");
        let _ = build_corpus(&conn, 600, 50).await;

        conn.execute("INSERT INTO t(t) VALUES('delete-all');")
            .await
            .expect("delete-all");
        let empty = franken_count(&conn, "SELECT count(*) FROM t WHERE t MATCH 'shared';").await;
        assert_eq!(empty, 0, "delete-all empties the index");
        conn.execute("PRAGMA wal_checkpoint(TRUNCATE);")
            .await
            .expect("truncate checkpoint after delete-all");
        conn.close().await.expect("close franken after delete-all");

        // Direct proof that delete-all cleared the seek shadow: any surviving
        // row would belong to a dropped segid and alias the restarted segid
        // space of the next insert generation.
        {
            let stock = rusqlite::Connection::open(&db_path).expect("stock open post-delete-all");
            let idx_left = stock_count(&stock, "SELECT count(*) FROM t_idx");
            assert_eq!(idx_left, 0, "delete-all must clear every %_idx row");
            let integrity: String = stock
                .query_row("PRAGMA integrity_check;", [], |r| r.get(0))
                .expect("stock integrity_check post-delete-all");
            assert_eq!(integrity, "ok", "stock integrity_check after delete-all");
        }

        // Rebuild a small corpus after the reset (fresh segids from 1).
        let conn = Connection::open(&db_str).await.expect("reopen franken");
        conn.execute("BEGIN").await.expect("begin rebuild");
        for rowid in 1..=8_usize {
            conn.execute(&format!(
                "INSERT INTO t(rowid, content, title, created_at) \
                 VALUES ({rowid}, 'fresh {unique}', 'title', '2026-09-01');",
                unique = tok(rowid),
            ))
            .await
            .expect("rebuild insert");
        }
        conn.execute("COMMIT").await.expect("commit rebuild");
        let fresh = franken_count(&conn, "SELECT count(*) FROM t WHERE t MATCH 'fresh';").await;
        assert_eq!(fresh, 8, "rebuilt corpus is searchable live");
        conn.execute("PRAGMA wal_checkpoint(TRUNCATE);")
            .await
            .expect("truncate checkpoint");
        conn.close().await.expect("close franken");

        let stock = rusqlite::Connection::open(&db_path).expect("stock open");
        let integrity: String = stock
            .query_row("PRAGMA integrity_check;", [], |r| r.get(0))
            .expect("stock integrity_check");
        assert_eq!(
            integrity, "ok",
            "stock integrity_check after delete-all + rebuild"
        );
        let matched = stock_count(&stock, "SELECT count(*) FROM t WHERE t MATCH 'fresh'");
        assert_eq!(matched, 8, "stock searches the rebuilt corpus");
    });
}
