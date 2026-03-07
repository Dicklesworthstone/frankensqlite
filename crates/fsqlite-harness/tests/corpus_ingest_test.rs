//! Unit + integration tests for corpus ingestion and normalization (bd-1dp9.2.1).
//!
//! Validates:
//! - Family classification heuristic correctness
//! - Seed derivation determinism
//! - Corpus builder API
//! - Coverage report accuracy
//! - Seed corpus completeness (all families represented)
//! - Conformance fixture ingestion

use std::collections::BTreeSet;
use std::fs;
use std::path::Path;

use fsqlite_harness::corpus_ingest::{
    classify_family, derive_entry_seed, generate_seed_corpus,
    ingest_conformance_fixtures_with_report, ingest_slt_files_with_report,
    intake_user_repro_fixture, render_user_repro_fixture_json, write_user_repro_fixture,
    CorpusBuilder, CorpusSource, Family, UserReproIntakeRequest, CORPUS_SEED_BASE,
};
use fsqlite_harness::differential_v2::{StatementDivergence, StmtOutcome};
use fsqlite_harness::fixture_root_contract::{
    load_fixture_root_contract, DEFAULT_FIXTURE_ROOT_MANIFEST_PATH,
};
use fsqlite_harness::mismatch_minimizer::MinimizerConfig;
use proptest::prelude::*;
use tempfile::tempdir;

// ─── Family Classification Tests ─────────────────────────────────────────

#[test]
fn classify_select_as_sql() {
    let stmts = vec!["SELECT 1".to_owned()];
    let (family, _) = classify_family(&stmts);
    assert_eq!(family, Family::SQL);
}

#[test]
fn classify_create_table_as_sql() {
    let stmts = vec!["CREATE TABLE t(id INTEGER PRIMARY KEY, name TEXT)".to_owned()];
    let (family, _) = classify_family(&stmts);
    assert_eq!(family, Family::SQL);
}

#[test]
fn classify_begin_commit_as_txn() {
    let stmts = vec!["BEGIN".to_owned(), "COMMIT".to_owned()];
    let (family, _) = classify_family(&stmts);
    assert_eq!(family, Family::TXN);
}

#[test]
fn classify_savepoint_as_txn() {
    let stmts = vec!["SAVEPOINT sp1".to_owned(), "RELEASE sp1".to_owned()];
    let (family, _) = classify_family(&stmts);
    assert_eq!(family, Family::TXN);
}

#[test]
fn classify_aggregate_functions_as_fun() {
    let stmts = vec!["SELECT COUNT(*), SUM(x), AVG(x) FROM t".to_owned()];
    let (family, _) = classify_family(&stmts);
    assert_eq!(family, Family::FUN);
}

#[test]
fn classify_string_functions_as_fun() {
    let stmts = vec!["SELECT UPPER('hello'), LOWER('WORLD'), LENGTH('test')".to_owned()];
    let (family, _) = classify_family(&stmts);
    assert_eq!(family, Family::FUN);
}

#[test]
fn classify_explain_as_vdb() {
    let stmts = vec!["EXPLAIN SELECT * FROM t".to_owned()];
    let (family, _) = classify_family(&stmts);
    assert_eq!(family, Family::VDB);
}

#[test]
fn classify_pragma_as_pgm() {
    let stmts = vec!["PRAGMA page_size".to_owned()];
    let (family, _) = classify_family(&stmts);
    assert_eq!(family, Family::PGM);
}

#[test]
fn classify_json_as_ext() {
    let stmts = vec!["SELECT JSON('{\"a\":1}')".to_owned()];
    let (family, _) = classify_family(&stmts);
    assert_eq!(family, Family::EXT);
}

#[test]
fn classify_fts_as_ext() {
    let stmts = vec!["CREATE VIRTUAL TABLE docs USING FTS5(title, body)".to_owned()];
    let (family, _) = classify_family(&stmts);
    assert_eq!(family, Family::EXT);
}

#[test]
fn classify_join_as_pln() {
    let stmts = vec!["SELECT a.x, b.y FROM t1 a JOIN t2 b ON a.id = b.a_id".to_owned()];
    let (family, _) = classify_family(&stmts);
    assert_eq!(family, Family::PLN);
}

#[test]
fn classify_cte_as_pln() {
    let stmts = vec!["WITH cte AS (SELECT 1) SELECT * FROM cte".to_owned()];
    let (family, _) = classify_family(&stmts);
    assert_eq!(family, Family::PLN);
}

#[test]
fn classify_detects_secondary_families() {
    // A query with both functions and JOINs should have secondary families.
    let stmts = vec![
        "SELECT COUNT(*), SUM(b.val) FROM t1 a JOIN t2 b ON a.id = b.a_id GROUP BY a.name"
            .to_owned(),
    ];
    let (primary, secondary) = classify_family(&stmts);
    // Primary should be one of FUN or PLN (both have strong signals).
    assert!(
        primary == Family::FUN || primary == Family::PLN,
        "expected FUN or PLN, got {primary}"
    );
    // Secondary should include the other.
    assert!(
        !secondary.is_empty(),
        "expected secondary families for cross-domain query"
    );
}

// ─── Seed Derivation Tests ───────────────────────────────────────────────

#[test]
fn seed_derivation_is_deterministic() {
    let s1 = derive_entry_seed(42, 0);
    let s2 = derive_entry_seed(42, 0);
    assert_eq!(s1, s2);
}

#[test]
fn seed_derivation_varies_by_index() {
    let s0 = derive_entry_seed(42, 0);
    let s1 = derive_entry_seed(42, 1);
    let s2 = derive_entry_seed(42, 2);
    assert_ne!(s0, s1);
    assert_ne!(s1, s2);
    assert_ne!(s0, s2);
}

#[test]
fn seed_derivation_varies_by_base() {
    let s1 = derive_entry_seed(42, 5);
    let s2 = derive_entry_seed(43, 5);
    assert_ne!(s1, s2);
}

#[test]
fn corpus_seed_base_is_franken_seed() {
    assert_eq!(CORPUS_SEED_BASE, 0x0046_5241_4E4B_454E);
}

// ─── Corpus Builder Tests ────────────────────────────────────────────────

#[test]
fn builder_creates_empty_corpus() {
    let builder = CorpusBuilder::new(42);
    let manifest = builder.build();
    assert_eq!(manifest.entries.len(), 0);
    assert_eq!(manifest.coverage.total_entries, 0);
}

#[test]
fn builder_adds_classified_entries() {
    let mut builder = CorpusBuilder::new(42);
    builder.add_statements(
        ["SELECT 1"],
        CorpusSource::Custom {
            author: "test".to_owned(),
        },
        "test entry",
    );
    let manifest = builder.build();
    assert_eq!(manifest.entries.len(), 1);
    assert_eq!(manifest.entries[0].family, Family::SQL);
    assert!(!manifest.entries[0].id.is_empty());
}

#[test]
fn builder_assigns_deterministic_seeds() {
    let mut builder = CorpusBuilder::new(42);
    builder.add_statements(
        ["SELECT 1"],
        CorpusSource::Custom {
            author: "test".to_owned(),
        },
        "first",
    );
    builder.add_statements(
        ["SELECT 2"],
        CorpusSource::Custom {
            author: "test".to_owned(),
        },
        "second",
    );
    let manifest = builder.build();
    assert_ne!(manifest.entries[0].seed, manifest.entries[1].seed);
    assert_ne!(manifest.entries[0].seed, 0);
    assert_ne!(manifest.entries[1].seed, 0);
}

#[test]
fn builder_skip_marks_entry() {
    let mut builder = CorpusBuilder::new(42);
    builder.add_statements(
        ["SELECT 1"],
        CorpusSource::Custom {
            author: "test".to_owned(),
        },
        "skipped entry",
    );
    builder.skip_last("not supported yet", Some("X-AMAL.1".to_owned()));

    let manifest = builder.build();
    assert!(manifest.entries[0].skip.is_some());
    assert_eq!(manifest.coverage.skipped_entries, 1);
    assert_eq!(manifest.coverage.active_entries, 0);
}

#[test]
fn builder_link_features_attaches_ids() {
    let mut builder = CorpusBuilder::new(42);
    builder.add_statements(
        ["SELECT COUNT(*) FROM t"],
        CorpusSource::Custom {
            author: "test".to_owned(),
        },
        "function test",
    );
    builder.link_features(["F-FUN.5", "F-SQL.2"]);

    let manifest = builder.build();
    assert_eq!(manifest.entries[0].taxonomy_features.len(), 2);
    assert!(manifest.entries[0]
        .taxonomy_features
        .contains(&"F-FUN.5".to_owned()));
}

// ─── Coverage Report Tests ───────────────────────────────────────────────

#[test]
fn coverage_reports_missing_families() {
    let mut builder = CorpusBuilder::new(42);
    // Only add SQL entries — all other families should be missing.
    builder.add_with_family(
        Family::SQL,
        ["SELECT 1"],
        CorpusSource::Custom {
            author: "test".to_owned(),
        },
        "sql only",
    );
    let manifest = builder.build();

    assert!(manifest.coverage.missing_families.len() >= 7);
    assert!(manifest
        .coverage
        .missing_families
        .contains(&"TXN".to_owned()));
    assert!(manifest
        .coverage
        .missing_families
        .contains(&"FUN".to_owned()));
    assert!(manifest
        .coverage
        .missing_families
        .contains(&"VDB".to_owned()));
    assert!(!manifest
        .coverage
        .missing_families
        .contains(&"SQL".to_owned()));
}

#[test]
fn coverage_fill_percentage_is_correct() {
    let mut builder = CorpusBuilder::new(42);
    // SQL family minimum is 30; add 15 entries → 50%.
    for i in 0..15 {
        builder.add_with_family(
            Family::SQL,
            [format!("SELECT {i}")],
            CorpusSource::Custom {
                author: "test".to_owned(),
            },
            format!("entry {i}"),
        );
    }
    let manifest = builder.build();

    let sql_coverage = manifest.coverage.by_family.get("SQL").expect("SQL family");
    assert_eq!(sql_coverage.entry_count, 15);
    assert!(
        (sql_coverage.fill_pct - 50.0).abs() < 0.1,
        "expected ~50% fill, got {}",
        sql_coverage.fill_pct
    );
}

// ─── Seed Corpus Tests ───────────────────────────────────────────────────

#[test]
fn seed_corpus_covers_all_families() {
    let mut builder = CorpusBuilder::new(CORPUS_SEED_BASE);
    generate_seed_corpus(&mut builder);
    let manifest = builder.build();

    let families_present: BTreeSet<String> = manifest
        .entries
        .iter()
        .map(|e| e.family.to_string())
        .collect();

    for fam in Family::ALL {
        assert!(
            families_present.contains(&fam.to_string()),
            "seed corpus missing family: {fam}"
        );
    }

    eprintln!(
        "bead_id=bd-1dp9.2.1 test=seed_corpus families={} entries={}",
        families_present.len(),
        manifest.entries.len()
    );
}

#[test]
fn seed_corpus_has_taxonomy_feature_links() {
    let mut builder = CorpusBuilder::new(CORPUS_SEED_BASE);
    generate_seed_corpus(&mut builder);
    let manifest = builder.build();

    let linked = manifest
        .entries
        .iter()
        .filter(|e| !e.taxonomy_features.is_empty())
        .count();

    assert!(linked > 0, "seed corpus should have taxonomy feature links");
    assert!(
        linked >= manifest.entries.len() / 2,
        "at least half of seed corpus entries should have feature links, got {}/{}",
        linked,
        manifest.entries.len()
    );

    eprintln!(
        "bead_id=bd-1dp9.2.1 test=feature_links linked={} total={}",
        linked,
        manifest.entries.len()
    );
}

#[test]
fn seed_corpus_entries_have_unique_ids() {
    let mut builder = CorpusBuilder::new(CORPUS_SEED_BASE);
    generate_seed_corpus(&mut builder);
    let manifest = builder.build();

    let mut seen = BTreeSet::new();
    for entry in &manifest.entries {
        assert!(
            seen.insert(&entry.id),
            "duplicate corpus entry ID: {}",
            entry.id
        );
    }
}

#[test]
fn seed_corpus_entries_have_unique_seeds() {
    let mut builder = CorpusBuilder::new(CORPUS_SEED_BASE);
    generate_seed_corpus(&mut builder);
    let manifest = builder.build();

    let mut seen = BTreeSet::new();
    for entry in &manifest.entries {
        assert!(
            seen.insert(entry.seed),
            "duplicate seed for entry {}: {}",
            entry.id,
            entry.seed
        );
    }
}

// ─── Conformance Fixture Ingestion Tests ─────────────────────────────────

#[test]
fn ingest_conformance_fixtures_from_directory() {
    let conformance_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("conformance");
    if !conformance_dir.exists() {
        eprintln!("bead_id=bd-1dp9.2.1 test=conformance_ingest skip=no_conformance_dir");
        return;
    }

    let mut builder = CorpusBuilder::new(CORPUS_SEED_BASE);
    let report = ingest_conformance_fixtures_with_report(&conformance_dir, &mut builder)
        .expect("ingest fixtures");

    assert!(
        report.fixture_json_files_seen >= 8,
        "expected at least 8 fixture JSON files, found {}",
        report.fixture_json_files_seen
    );
    assert!(
        report.fixture_entries_ingested >= 8,
        "expected at least 8 ingested fixtures, found {}",
        report.fixture_entries_ingested
    );
    assert!(
        report.sql_statements_ingested >= 40,
        "expected at least 40 SQL statements from fixtures, found {}",
        report.sql_statements_ingested
    );

    let manifest = builder.build();
    assert_eq!(manifest.entries.len(), report.fixture_entries_ingested);

    // All ingested fixtures should have a Fixture source.
    for entry in &manifest.entries {
        assert!(
            matches!(&entry.source, CorpusSource::Fixture { .. }),
            "ingested entry should have Fixture source: {}",
            entry.id
        );
    }

    eprintln!(
        "bead_id=bd-1dp9.2.1 test=conformance_ingest files_seen={} entries={} sql={} families={:?}",
        report.fixture_json_files_seen,
        report.fixture_entries_ingested,
        report.sql_statements_ingested,
        manifest
            .entries
            .iter()
            .map(|e| e.family.to_string())
            .collect::<BTreeSet<_>>()
    );
}

#[test]
fn ingest_conformance_fixtures_reports_skipped_underspecified_files() {
    let temp = tempdir().expect("create tempdir");
    let dir = temp.path();

    let empty_fixture = dir.join("empty.json");
    fs::write(
        &empty_fixture,
        r#"{"id":"empty","description":"no sql","ops":[]}"#,
    )
    .expect("write empty fixture");

    let valid_fixture = dir.join("valid.json");
    fs::write(
        &valid_fixture,
        r#"{"id":"valid","description":"single statement","ops":[{"sql":"SELECT 1;"}]}"#,
    )
    .expect("write valid fixture");

    let mut builder = CorpusBuilder::new(CORPUS_SEED_BASE);
    let report =
        ingest_conformance_fixtures_with_report(dir, &mut builder).expect("ingest fixtures");

    assert_eq!(report.fixture_json_files_seen, 2);
    assert_eq!(report.fixture_entries_ingested, 1);
    assert_eq!(report.sql_statements_ingested, 1);
    assert_eq!(report.skipped_files.len(), 1);
    assert_eq!(report.skipped_files[0].file, "empty.json");
    assert!(report.skipped_files[0]
        .reason
        .contains("no ops[].sql statements"));

    let manifest = builder.build();
    assert_eq!(manifest.entries.len(), 1);
}

#[test]
fn ingest_slt_files_from_directory() {
    let temp = tempdir().expect("create tempdir");
    let dir = temp.path();

    let slt = dir.join("basic.slt");
    fs::write(
        &slt,
        "\
statement ok
CREATE TABLE t1(a INTEGER)

statement ok
INSERT INTO t1 VALUES(1)

query I nosort
SELECT a FROM t1
----
1

halt
",
    )
    .expect("write slt");

    let mut builder = CorpusBuilder::new(CORPUS_SEED_BASE);
    let report = ingest_slt_files_with_report(dir, &mut builder).expect("ingest slt");

    assert_eq!(report.slt_files_seen, 1);
    assert_eq!(report.slt_entries_ingested, 3);
    assert_eq!(report.sql_statements_ingested, 3);
    assert!(report.skipped_files.is_empty());

    let manifest = builder.build();
    assert_eq!(manifest.entries.len(), 1);
    let entry = &manifest.entries[0];
    assert!(matches!(&entry.source, CorpusSource::Slt { .. }));
    assert_eq!(entry.statements.len(), 3);
    assert_eq!(entry.statements[0], "CREATE TABLE t1(a INTEGER)");
    assert_eq!(entry.statements[1], "INSERT INTO t1 VALUES(1)");
    assert_eq!(entry.statements[2], "SELECT a FROM t1");
}

#[test]
fn ingest_slt_files_reports_skipped_files() {
    let temp = tempdir().expect("create tempdir");
    let dir = temp.path();

    let skipped = dir.join("empty.test");
    fs::write(&skipped, "-- comment-only file\n# no slt directives").expect("write skipped slt");

    let valid = dir.join("valid.sqllogictest");
    fs::write(
        &valid,
        "\
statement ok
CREATE TABLE t2(v INTEGER)
",
    )
    .expect("write valid slt");

    let mut builder = CorpusBuilder::new(CORPUS_SEED_BASE);
    let report = ingest_slt_files_with_report(dir, &mut builder).expect("ingest slt");

    assert_eq!(report.slt_files_seen, 2);
    assert_eq!(report.slt_entries_ingested, 1);
    assert_eq!(report.sql_statements_ingested, 1);
    assert_eq!(report.skipped_files.len(), 1);
    assert_eq!(report.skipped_files[0].file, "empty.test");
    assert!(report.skipped_files[0]
        .reason
        .contains("no SLT entries parsed"));
}

#[test]
fn ingest_slt_files_is_deterministic() {
    let temp = tempdir().expect("create tempdir");
    let dir = temp.path();

    fs::write(
        dir.join("b.slt"),
        "\
statement ok
CREATE TABLE b(v INTEGER)
",
    )
    .expect("write b.slt");
    fs::write(
        dir.join("a.slt"),
        "\
statement ok
CREATE TABLE a(v INTEGER)
",
    )
    .expect("write a.slt");

    let mut builder_left = CorpusBuilder::new(CORPUS_SEED_BASE);
    let left_report = ingest_slt_files_with_report(dir, &mut builder_left).expect("left ingest");
    let left = builder_left.build();

    let mut builder_right = CorpusBuilder::new(CORPUS_SEED_BASE);
    let right_report = ingest_slt_files_with_report(dir, &mut builder_right).expect("right ingest");
    let right = builder_right.build();

    assert_eq!(left_report, right_report);
    assert_eq!(left.entries.len(), right.entries.len());
    for (left_entry, right_entry) in left.entries.iter().zip(right.entries.iter()) {
        assert_eq!(left_entry.id, right_entry.id);
        assert_eq!(left_entry.seed, right_entry.seed);
        assert_eq!(left_entry.family, right_entry.family);
        assert_eq!(left_entry.statements, right_entry.statements);
        assert_eq!(left_entry.content_hash(), right_entry.content_hash());
    }
}

#[test]
fn ingest_slt_files_discovers_nested_suite_files() {
    let temp = tempdir().expect("create tempdir");
    let dir = temp.path();
    let nested = dir.join("core").join("smoke");
    fs::create_dir_all(&nested).expect("create nested suite");

    fs::write(
        nested.join("a.slt"),
        "\
statement ok
CREATE TABLE nested_a(v INTEGER)
",
    )
    .expect("write nested a.slt");
    fs::write(
        dir.join("top_level.sqllogictest"),
        "\
statement ok
CREATE TABLE top_level(v INTEGER)
",
    )
    .expect("write top_level.sqllogictest");

    let mut builder = CorpusBuilder::new(CORPUS_SEED_BASE);
    let report = ingest_slt_files_with_report(dir, &mut builder).expect("ingest nested slt");

    assert_eq!(report.slt_files_seen, 2);
    assert_eq!(report.slt_entries_ingested, 2);
    assert_eq!(report.sql_statements_ingested, 2);
    assert!(report.skipped_files.is_empty());

    let manifest = builder.build();
    assert_eq!(manifest.entries.len(), 2);
    assert!(manifest.entries.iter().any(|entry| {
        matches!(
            &entry.source,
            CorpusSource::Slt { file } if file == "core/smoke/a.slt"
        )
    }));
    assert!(manifest.entries.iter().any(|entry| {
        matches!(
            &entry.source,
            CorpusSource::Slt { file } if file == "top_level.sqllogictest"
        )
    }));
}

#[test]
fn canonical_slt_corpus_root_satisfies_manifest_contract() {
    let workspace_root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    let contract = load_fixture_root_contract(
        &workspace_root,
        Path::new(DEFAULT_FIXTURE_ROOT_MANIFEST_PATH),
    )
    .expect("load fixture-root contract");

    assert!(
        contract.slt_dir.exists(),
        "canonical slt_dir is missing: {}",
        contract.slt_dir.display()
    );

    let mut builder = CorpusBuilder::new(CORPUS_SEED_BASE);
    let report = ingest_slt_files_with_report(&contract.slt_dir, &mut builder)
        .expect("ingest canonical slt");

    assert!(
        report.slt_files_seen >= contract.min_slt_files,
        "slt_files_seen={} < min_slt_files={}",
        report.slt_files_seen,
        contract.min_slt_files
    );
    assert!(
        report.slt_entries_ingested >= contract.min_slt_entries,
        "slt_entries_ingested={} < min_slt_entries={}",
        report.slt_entries_ingested,
        contract.min_slt_entries
    );
    assert!(
        report.sql_statements_ingested >= contract.min_slt_sql_statements,
        "slt_sql_statements_ingested={} < min_slt_sql_statements={}",
        report.sql_statements_ingested,
        contract.min_slt_sql_statements
    );
}

// ─── User Repro Intake Tests (bd-2yqp6.3.5) ─────────────────────────────

#[test]
fn user_repro_intake_produces_minimized_entry_and_source_coverage() {
    let mut builder = CorpusBuilder::new(CORPUS_SEED_BASE);
    let request = UserReproIntakeRequest {
        title: "user report: mismatch on bad()".to_owned(),
        details: "reported via issue tracker".to_owned(),
        trace_id: "trace-c5-001".to_owned(),
        run_id: "run-c5-001".to_owned(),
        scenario_id: "PARITY-C5-INTAKE".to_owned(),
        seed: 3520,
        reporter: Some("user@example.com".to_owned()),
        schema: vec!["CREATE TABLE t(v INTEGER);".to_owned()],
        workload: vec![
            "SELECT 1;".to_owned(),
            "SELECT bad();".to_owned(),
            "SELECT 2;".to_owned(),
        ],
        taxonomy_features: vec!["F-SQL.10".to_owned()],
    };

    let minimizer = MinimizerConfig {
        max_iterations: 128,
        one_minimal: true,
        max_workload_size: 128,
    };
    let repro_test =
        |_schema: &[String], workload: &[String]| -> Option<Vec<StatementDivergence>> {
            if workload.iter().any(|sql| sql.contains("SELECT bad()")) {
                Some(vec![StatementDivergence {
                    index: 0,
                    sql: "SELECT bad();".to_owned(),
                    fsqlite_outcome: StmtOutcome::Error("no such function: bad".to_owned()),
                    csqlite_outcome: StmtOutcome::Error("no such function: bad".to_owned()),
                }])
            } else {
                None
            }
        };

    let report = intake_user_repro_fixture(&mut builder, &request, &minimizer, &repro_test)
        .expect("user repro intake should succeed");

    assert_eq!(report.artifact.trace_id, "trace-c5-001");
    assert_eq!(report.artifact.run_id, "run-c5-001");
    assert_eq!(report.artifact.scenario_id, "PARITY-C5-INTAKE");
    assert_eq!(report.artifact.seed, 3520);
    assert_eq!(report.artifact.original_statement_count, 3);
    assert!(
        report.artifact.minimized_statement_count <= report.artifact.original_statement_count,
        "minimizer should not expand workload"
    );
    assert!(
        report
            .artifact
            .minimized_replay_sql
            .iter()
            .any(|sql| sql.contains("SELECT bad()")),
        "minimized replay must preserve the failing statement"
    );

    let manifest = builder.build();
    assert_eq!(manifest.entries.len(), 1);
    assert_eq!(manifest.coverage.user_repro_entries, 1);
    assert_eq!(manifest.coverage.by_source.get("user_repro"), Some(&1));

    let entry = &manifest.entries[0];
    assert!(
        matches!(
            &entry.source,
            CorpusSource::UserRepro { fixture_id, .. } if fixture_id == &report.artifact.fixture_id
        ),
        "entry source should preserve user repro fixture metadata"
    );
}

#[test]
fn user_repro_fixture_json_roundtrip_and_ingestability() {
    let mut builder = CorpusBuilder::new(CORPUS_SEED_BASE);
    let request = UserReproIntakeRequest {
        title: "user report: deterministic minimization".to_owned(),
        details: String::new(),
        trace_id: "trace-c5-002".to_owned(),
        run_id: "run-c5-002".to_owned(),
        scenario_id: "PARITY-C5-ROUNDTRIP".to_owned(),
        seed: 777,
        reporter: None,
        schema: vec!["CREATE TABLE t(v INTEGER);".to_owned()],
        workload: vec!["SELECT bad();".to_owned()],
        taxonomy_features: vec![],
    };
    let minimizer = MinimizerConfig::default();
    let repro_test =
        |_schema: &[String], workload: &[String]| -> Option<Vec<StatementDivergence>> {
            if workload.iter().any(|sql| sql.contains("bad()")) {
                Some(vec![StatementDivergence {
                    index: 0,
                    sql: "SELECT bad();".to_owned(),
                    fsqlite_outcome: StmtOutcome::Error("boom".to_owned()),
                    csqlite_outcome: StmtOutcome::Error("boom".to_owned()),
                }])
            } else {
                None
            }
        };
    let report = intake_user_repro_fixture(&mut builder, &request, &minimizer, &repro_test)
        .expect("user repro intake should succeed");

    let fixture_json =
        render_user_repro_fixture_json(&report.artifact).expect("fixture json should serialize");
    let fixture_value: serde_json::Value =
        serde_json::from_str(&fixture_json).expect("fixture json should parse");
    assert_eq!(
        fixture_value["id"].as_str(),
        Some(report.artifact.fixture_id.as_str())
    );
    assert_eq!(
        fixture_value["metadata"]["trace_id"].as_str(),
        Some(report.artifact.trace_id.as_str())
    );
    assert_eq!(
        fixture_value["metadata"]["run_id"].as_str(),
        Some(report.artifact.run_id.as_str())
    );
    assert_eq!(
        fixture_value["metadata"]["scenario_id"].as_str(),
        Some(report.artifact.scenario_id.as_str())
    );
    assert_eq!(
        fixture_value["metadata"]["seed"].as_u64(),
        Some(report.artifact.seed)
    );

    let tmp = tempdir().expect("create temp dir");
    let fixture_path =
        write_user_repro_fixture(tmp.path(), &report.artifact).expect("fixture file write");
    assert!(fixture_path.exists(), "fixture file should exist");

    let mut ingest_builder = CorpusBuilder::new(CORPUS_SEED_BASE);
    let ingest_report = ingest_conformance_fixtures_with_report(tmp.path(), &mut ingest_builder)
        .expect("fixture should be ingestable by conformance ingestion");
    assert_eq!(ingest_report.fixture_json_files_seen, 1);
    assert_eq!(ingest_report.fixture_entries_ingested, 1);
    assert_eq!(ingest_report.skipped_files.len(), 0);
}

#[test]
fn user_repro_intake_rejects_missing_structured_metadata() {
    let mut builder = CorpusBuilder::new(CORPUS_SEED_BASE);
    let request = UserReproIntakeRequest {
        title: "bad intake".to_owned(),
        details: String::new(),
        trace_id: String::new(),
        run_id: "run-c5-003".to_owned(),
        scenario_id: "PARITY-C5-INVALID".to_owned(),
        seed: 1,
        reporter: None,
        schema: vec![],
        workload: vec!["SELECT 1;".to_owned()],
        taxonomy_features: vec![],
    };
    let minimizer = MinimizerConfig::default();
    let repro_test =
        |_schema: &[String], _workload: &[String]| -> Option<Vec<StatementDivergence>> {
            Some(vec![StatementDivergence {
                index: 0,
                sql: "SELECT 1;".to_owned(),
                fsqlite_outcome: StmtOutcome::Rows(Vec::new()),
                csqlite_outcome: StmtOutcome::Rows(Vec::new()),
            }])
        };

    let error = intake_user_repro_fixture(&mut builder, &request, &minimizer, &repro_test)
        .expect_err("missing trace_id should be rejected");
    assert!(
        error.contains("trace_id"),
        "error should mention missing trace_id: {error}"
    );
}

proptest! {
    #[test]
    fn user_repro_intake_is_deterministic_for_identical_inputs(
        trace_suffix in "[a-z0-9]{1,8}",
        run_suffix in "[a-z0-9]{1,8}",
        scenario_suffix in "[A-Z0-9_]{1,8}",
        seed in any::<u64>(),
        noise_len in 0usize..6,
    ) {
        let mut workload: Vec<String> = (0..noise_len)
            .map(|i| format!("SELECT {};", i))
            .collect();
        workload.insert(noise_len / 2, "SELECT bad();".to_owned());

        let request = UserReproIntakeRequest {
            title: "property repro".to_owned(),
            details: "determinism check".to_owned(),
            trace_id: format!("trace-{trace_suffix}"),
            run_id: format!("run-{run_suffix}"),
            scenario_id: format!("SCENARIO_{scenario_suffix}"),
            seed,
            reporter: None,
            schema: vec!["CREATE TABLE t(v INTEGER);".to_owned()],
            workload,
            taxonomy_features: vec![],
        };
        let minimizer = MinimizerConfig::default();
        let repro_test =
            |_schema: &[String], workload: &[String]| -> Option<Vec<StatementDivergence>> {
                if workload.iter().any(|sql| sql.contains("bad()")) {
                    Some(vec![StatementDivergence {
                        index: 0,
                        sql: "SELECT bad();".to_owned(),
                        fsqlite_outcome: StmtOutcome::Error("boom".to_owned()),
                        csqlite_outcome: StmtOutcome::Error("boom".to_owned()),
                    }])
                } else {
                    None
                }
            };

        let mut left_builder = CorpusBuilder::new(CORPUS_SEED_BASE);
        let mut right_builder = CorpusBuilder::new(CORPUS_SEED_BASE);
        let left =
            intake_user_repro_fixture(&mut left_builder, &request, &minimizer, &repro_test)
                .expect("left intake should succeed");
        let right =
            intake_user_repro_fixture(&mut right_builder, &request, &minimizer, &repro_test)
                .expect("right intake should succeed");

        prop_assert_eq!(left.artifact.fixture_id, right.artifact.fixture_id);
        prop_assert_eq!(left.artifact.signature_hash, right.artifact.signature_hash);
        prop_assert_eq!(
            left.artifact.minimized_replay_sql,
            right.artifact.minimized_replay_sql
        );
        prop_assert_eq!(
            left.artifact.minimized_statement_count,
            right.artifact.minimized_statement_count
        );
        prop_assert!(
            left.artifact.minimized_statement_count <= left.artifact.original_statement_count
        );
    }
}

// ─── Manifest Serialization Tests ────────────────────────────────────────

#[test]
fn manifest_serializes_to_json() {
    let mut builder = CorpusBuilder::new(42);
    generate_seed_corpus(&mut builder);
    let manifest = builder.build();

    let json = serde_json::to_string_pretty(&manifest).expect("serialize manifest");
    assert!(json.contains("\"bead_id\""));
    assert!(json.contains("\"coverage\""));
    assert!(json.contains("\"entries\""));

    // Verify round-trip.
    let m2: fsqlite_harness::corpus_ingest::CorpusManifest =
        serde_json::from_str(&json).expect("deserialize manifest");
    assert_eq!(m2.entries.len(), manifest.entries.len());
    assert_eq!(m2.coverage.total_entries, manifest.coverage.total_entries);
}

#[test]
fn coverage_report_json_has_all_families() {
    let mut builder = CorpusBuilder::new(CORPUS_SEED_BASE);
    generate_seed_corpus(&mut builder);
    let manifest = builder.build();

    for fam in Family::ALL {
        assert!(
            manifest.coverage.by_family.contains_key(&fam.to_string()),
            "coverage report missing family: {fam}"
        );
    }
}

// ─── Family Display/Parsing Tests ────────────────────────────────────────

#[test]
fn family_display_roundtrip() {
    for fam in Family::ALL {
        let s = fam.to_string();
        let parsed = Family::from_str_opt(&s).expect("parse family");
        assert_eq!(parsed, fam);
    }
}

#[test]
fn family_from_str_case_insensitive() {
    assert_eq!(Family::from_str_opt("sql"), Some(Family::SQL));
    assert_eq!(Family::from_str_opt("Txn"), Some(Family::TXN));
    assert_eq!(Family::from_str_opt("FUN"), Some(Family::FUN));
    assert_eq!(Family::from_str_opt("unknown"), None);
}
