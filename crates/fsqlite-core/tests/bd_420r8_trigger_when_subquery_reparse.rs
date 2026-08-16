//! bd-420r8 / GH#345: an ordinary ingestion loop must not re-parse the whole
//! schema on every write.
//!
//! A trigger `WHEN` clause containing a subquery, evaluated during nested-write
//! ingestion, runs a nested `execute_statement` that reaches
//! `refresh_memdb_from_active_txn_if_dirty`. Pre-fix, because a prior write left
//! the memdb dirty, that refresh does a FULL `reload_memdb_from_txn_with_mode`
//! which re-parses the ENTIRE schema text (the leaf is the identifier interner
//! re-lexing every `CREATE`) before re-hydrating rows — so the cost is
//! `O(rows_written × (schema_text_size + total_rows))`, a pure-userspace spin
//! that the GH#345 reporter observed as an unbounded HANG (~570:1 utime:stime,
//! zero I/O, pegged core) on a 150-table migrated store.
//!
//! The fix memoizes the pure `create_sql` → AST parse per schema generation, so
//! a rows-only reload no longer re-lexes the schema. This keeper asserts the
//! ALGORITHMIC invariant directly and machine-independently: the number of
//! actual stored-schema statement parses performed during an ingestion burst
//! stays `O(schema objects)`, NOT `O(rows × schema)`. Wall time is kept only as
//! a loose secondary backstop. The env var `BD_420R8_MEASURE=1` prints both the
//! elapsed time and the parse count.

use fsqlite_core::connection::Connection;
use fsqlite_types::value::SqliteValue;
use std::time::Instant;

const BLOAT_TABLES: usize = 80;
const INGEST_ROWS: usize = 300;

#[test]
fn ingestion_through_subquery_when_trigger_does_not_reparse_schema_per_write() {
    asupersync::test_utils::run_test(|| async {
        let conn = Connection::open(":memory:").await.unwrap();

        // Broad schema: many wide tables + CHECK constraints so a full schema
        // re-parse has real cost, mirroring the migrated store's 150-table shape.
        let mut ddl = String::new();
        for t in 0..BLOAT_TABLES {
            ddl.push_str(&format!(
                "CREATE TABLE bloat_{t}(\
                 id INTEGER PRIMARY KEY, \
                 a TEXT, b TEXT, c TEXT, d TEXT, e TEXT, f TEXT, g TEXT, h TEXT, \
                 CHECK(length(coalesce(a,'')) >= 0 AND length(coalesce(h,'')) >= 0)\
                );"
            ));
        }
        // Ingestion target + a sink the trigger body writes, plus a BEFORE-INSERT
        // trigger whose WHEN contains a subquery — the exact shape that reaches
        // the per-write memdb reload during nested-write ingestion.
        ddl.push_str("CREATE TABLE main(id INTEGER PRIMARY KEY, v TEXT);");
        ddl.push_str("CREATE TABLE sink(id INTEGER PRIMARY KEY, v TEXT);");
        ddl.push_str(
            "CREATE TRIGGER main_guard BEFORE INSERT ON main \
             WHEN (SELECT count(*) FROM sink) >= 0 \
             BEGIN INSERT INTO sink(v) VALUES (NEW.v); END;",
        );
        conn.execute_batch(&ddl).await.unwrap();

        // Schema is now fixed. Measure actual stored-schema statement parses
        // across ONLY the ingestion phase: the regression re-parses the whole
        // schema on every write (O(rows × schema)); the fix parses it once per
        // schema generation (O(schema)).
        let parses_before = conn.schema_reload_parse_count();
        let started = Instant::now();
        let mut batch = String::from("BEGIN;");
        for i in 0..INGEST_ROWS {
            batch.push_str(&format!("INSERT INTO main(v) VALUES ('row-{i}');"));
        }
        batch.push_str("COMMIT;");
        conn.execute_batch(&batch).await.unwrap();
        let elapsed = started.elapsed();
        let parse_delta = conn.schema_reload_parse_count() - parses_before;

        if std::env::var("BD_420R8_MEASURE").is_ok() {
            eprintln!(
                "BD_420R8: {INGEST_ROWS} inserts through a subquery-WHEN trigger over a \
                 {BLOAT_TABLES}-table schema took {elapsed:?}; stored-schema parses during \
                 ingestion = {parse_delta}"
            );
        }

        // Primary guard (machine-independent): stored-schema parses during the
        // ingestion burst must stay O(schema objects), NOT O(rows × schema).
        // Pre-fix this was ~INGEST_ROWS × object_count (tens of thousands); the
        // fix parses each stored statement once per schema generation. The bound
        // is a small constant multiple of the object count, so the per-write
        // re-parse regression can never satisfy it.
        let object_estimate = (BLOAT_TABLES + 3) as u64; // bloat tables + main + sink + trigger
        assert!(
            parse_delta <= object_estimate * 3,
            "GH#345 / bd-420r8: {parse_delta} stored-schema parses across {INGEST_ROWS} inserts \
             (~{object_estimate} schema objects) — per-write schema re-parse regression \
             (expected O(schema), got O(rows × schema))"
        );

        // Secondary backstop: ingestion must also stay bounded in wall time.
        // Generous so it is not machine-flaky; the parse-count guard above is the
        // real signal.
        assert!(
            elapsed.as_secs() < 60,
            "GH#345 / bd-420r8: ingestion took {elapsed:?} — unbounded spin"
        );

        // Correctness: the trigger fired once per ingested row.
        let rows = conn.query("SELECT count(*) FROM sink;").await.unwrap();
        assert_eq!(
            rows[0].values()[0],
            SqliteValue::Integer(INGEST_ROWS as i64)
        );
        let main_rows = conn.query("SELECT count(*) FROM main;").await.unwrap();
        assert_eq!(
            main_rows[0].values()[0],
            SqliteValue::Integer(INGEST_ROWS as i64)
        );
    });
}
