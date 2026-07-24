//! Deterministic chain-length control end-to-end checks for `bd-2y306.2`.

use std::{env, fs, path::PathBuf};

use fsqlite_mvcc::{BeginKind, GLOBAL_EBR_METRICS, MvccError, TransactionManager};
use fsqlite_types::{PageData, PageNumber, PageSize};
use serde_json::json;

const BEAD_ID: &str = "bd-2y306.2";
const LOG_STANDARD_REF: &str = "AGENTS.md#cross-cutting-quality-contract";
const DEFAULT_SEED: u64 = 2_306_020_021;
const REPLAY_COMMAND: &str = "cargo test -p fsqlite-e2e --test bd_2y306_2_chain_length_controls -- --nocapture --test-threads=1";

#[derive(Debug, Clone, Copy)]
struct ChainBoundOutcome {
    chain_len: usize,
    max_chain: usize,
    gc_freed_delta: u64,
}

#[derive(Debug, Clone, Copy)]
struct BackpressureOutcome {
    saw_busy: bool,
    gc_blocked_delta: u64,
}

fn page_size() -> PageSize {
    PageSize::new(4096).expect("fixed page size must be valid")
}

fn test_data(byte: u8) -> PageData {
    let mut data = PageData::zeroed(page_size());
    data.as_bytes_mut()[0] = byte;
    data
}

fn run_chain_bound_scenario(
    updates: u32,
    max_chain_length: usize,
    warning_threshold: usize,
    pgno: PageNumber,
) -> ChainBoundOutcome {
    let mut mgr = TransactionManager::new(page_size());
    mgr.set_max_chain_length(max_chain_length);
    mgr.set_chain_length_warning(warning_threshold);

    let before = GLOBAL_EBR_METRICS.snapshot();
    for step in 0..updates {
        let mut txn = mgr.begin(BeginKind::Concurrent).expect("begin concurrent");
        let byte = u8::try_from(step % 251).expect("modulo bounds u8");
        mgr.write_page(&mut txn, pgno, test_data(byte))
            .expect("write page");
        mgr.commit(&mut txn).expect("commit txn");
    }

    let after = GLOBAL_EBR_METRICS.snapshot();
    ChainBoundOutcome {
        chain_len: mgr.version_store().chain_length(pgno),
        max_chain: mgr.max_chain_length(),
        gc_freed_delta: after.gc_freed_count.saturating_sub(before.gc_freed_count),
    }
}

fn run_backpressure_scenario(
    attempts: u32,
    max_chain_length: usize,
    warning_threshold: usize,
    busy_timeout_ms: u64,
    pgno: PageNumber,
) -> BackpressureOutcome {
    let mut mgr = TransactionManager::new(page_size());
    mgr.set_busy_timeout_ms(busy_timeout_ms);
    mgr.set_max_chain_length(max_chain_length);
    mgr.set_chain_length_warning(warning_threshold);

    let mut seed = mgr.begin(BeginKind::Concurrent).expect("seed begin");
    mgr.write_page(&mut seed, pgno, test_data(0x11))
        .expect("seed write");
    mgr.commit(&mut seed).expect("seed commit");

    let mut pinned_reader = mgr.begin(BeginKind::Concurrent).expect("reader begin");
    let _ = mgr.read_page(&mut pinned_reader, pgno).unwrap();

    let before = GLOBAL_EBR_METRICS.snapshot();
    let mut saw_busy = false;

    for step in 0..attempts {
        let mut writer = mgr.begin(BeginKind::Concurrent).expect("writer begin");
        let byte = u8::try_from((step + 2) % 251).expect("modulo bounds u8");
        mgr.write_page(&mut writer, pgno, test_data(byte))
            .expect("writer write");
        match mgr.commit(&mut writer) {
            Ok(_) => {}
            Err(MvccError::Busy) => {
                saw_busy = true;
                break;
            }
            Err(other) => panic!("unexpected commit error: {other:?}"),
        }
    }

    mgr.abort(&mut pinned_reader);
    let after = GLOBAL_EBR_METRICS.snapshot();

    BackpressureOutcome {
        saw_busy,
        gc_blocked_delta: after
            .gc_blocked_count
            .saturating_sub(before.gc_blocked_count),
    }
}

#[test]
fn bd_2y306_2_chain_length_bounded_hot_page_updates() {
    let run_id = "bd-2y306.2-hot-page-bound";
    let trace_id = 2_306_020_221_u64;
    let scenario_id = "CHAIN-LENGTH-HOT-PAGE-BOUND";

    let outcome =
        run_chain_bound_scenario(10_000, 64, 32, PageNumber::new(73_062).expect("valid page"));

    assert!(
        outcome.chain_len <= outcome.max_chain,
        "bead_id={BEAD_ID} case=chain_bound_violation run_id={run_id} trace_id={trace_id} scenario_id={scenario_id} chain_len={} max_chain={}",
        outcome.chain_len,
        outcome.max_chain
    );
    assert!(
        outcome.gc_freed_delta > 0,
        "bead_id={BEAD_ID} case=expected_gc_freed run_id={run_id} trace_id={trace_id} scenario_id={scenario_id} gc_freed_delta={}",
        outcome.gc_freed_delta
    );

    eprintln!(
        "INFO bead_id={BEAD_ID} run_id={run_id} trace_id={trace_id} scenario_id={scenario_id} seed={DEFAULT_SEED} chain_len={} max_chain={} gc_freed_delta={} log_standard_ref={LOG_STANDARD_REF}",
        outcome.chain_len, outcome.max_chain, outcome.gc_freed_delta
    );
}

#[test]
fn bd_2y306_2_chain_backpressure_reports_busy_when_reader_pins_horizon() {
    let run_id = "bd-2y306.2-backpressure";
    let trace_id = 2_306_020_222_u64;
    let scenario_id = "CHAIN-LENGTH-BACKPRESSURE";

    let outcome =
        run_backpressure_scenario(64, 4, 2, 3, PageNumber::new(73_063).expect("valid page"));

    assert!(
        outcome.saw_busy,
        "bead_id={BEAD_ID} case=expected_busy run_id={run_id} trace_id={trace_id} scenario_id={scenario_id}"
    );
    assert!(
        outcome.gc_blocked_delta > 0,
        "bead_id={BEAD_ID} case=expected_gc_blocked run_id={run_id} trace_id={trace_id} scenario_id={scenario_id} gc_blocked_delta={}",
        outcome.gc_blocked_delta
    );

    eprintln!(
        "INFO bead_id={BEAD_ID} run_id={run_id} trace_id={trace_id} scenario_id={scenario_id} seed={DEFAULT_SEED} saw_busy={} gc_blocked_delta={} log_standard_ref={LOG_STANDARD_REF}",
        outcome.saw_busy, outcome.gc_blocked_delta
    );
}

#[test]
fn bd_2y306_2_chain_length_e2e_replay_emits_artifact() {
    let seed = env::var("SEED")
        .ok()
        .and_then(|raw| raw.parse::<u64>().ok())
        .unwrap_or(DEFAULT_SEED);
    let trace_id = env::var("TRACE_ID").unwrap_or_else(|_| seed.to_string());
    let run_id = env::var("RUN_ID").unwrap_or_else(|_| format!("{BEAD_ID}-seed-{seed}"));
    let scenario_id =
        env::var("SCENARIO_ID").unwrap_or_else(|_| "CHAIN-LENGTH-CONTROLS-E2E-REPLAY".to_owned());

    let chain = run_chain_bound_scenario(4_096, 64, 32, PageNumber::new(73_064).unwrap());
    let backpressure = run_backpressure_scenario(64, 4, 2, 3, PageNumber::new(73_065).unwrap());

    assert!(
        chain.chain_len <= chain.max_chain,
        "bead_id={BEAD_ID} case=e2e_chain_bound_violation run_id={run_id} trace_id={trace_id} scenario_id={scenario_id} chain_len={} max_chain={}",
        chain.chain_len,
        chain.max_chain
    );
    assert!(
        chain.gc_freed_delta > 0,
        "bead_id={BEAD_ID} case=e2e_missing_gc_freed run_id={run_id} trace_id={trace_id} scenario_id={scenario_id} gc_freed_delta={}",
        chain.gc_freed_delta
    );
    assert!(
        backpressure.saw_busy,
        "bead_id={BEAD_ID} case=e2e_missing_backpressure run_id={run_id} trace_id={trace_id} scenario_id={scenario_id}"
    );
    assert!(
        backpressure.gc_blocked_delta > 0,
        "bead_id={BEAD_ID} case=e2e_missing_gc_blocked run_id={run_id} trace_id={trace_id} scenario_id={scenario_id} gc_blocked_delta={}",
        backpressure.gc_blocked_delta
    );

    if let Ok(path) = env::var("FSQLITE_CHAIN_LENGTH_E2E_ARTIFACT") {
        let artifact_path = PathBuf::from(path);
        if let Some(parent) = artifact_path.parent() {
            fs::create_dir_all(parent).expect("create artifact dir");
        }

        let artifact = json!({
            "bead_id": BEAD_ID,
            "run_id": run_id,
            "trace_id": trace_id,
            "scenario_id": scenario_id,
            "seed": seed,
            "log_standard_ref": LOG_STANDARD_REF,
            "overall_status": "pass",
            "replay_command": REPLAY_COMMAND,
            "checks": {
                "chain_bound": {
                    "chain_len": chain.chain_len,
                    "max_chain": chain.max_chain,
                    "gc_freed_delta": chain.gc_freed_delta
                },
                "backpressure": {
                    "saw_busy": backpressure.saw_busy,
                    "gc_blocked_delta": backpressure.gc_blocked_delta
                }
            }
        });

        let payload = serde_json::to_vec_pretty(&artifact).expect("serialize artifact");
        fs::write(&artifact_path, payload).expect("write artifact");
        eprintln!(
            "DEBUG bead_id={BEAD_ID} run_id={run_id} trace_id={trace_id} scenario_id={scenario_id} seed={seed} artifact_path={} replay_command={REPLAY_COMMAND}",
            artifact_path.display()
        );
    }

    eprintln!(
        "INFO bead_id={BEAD_ID} run_id={run_id} trace_id={trace_id} scenario_id={scenario_id} seed={seed} chain_len={} max_chain={} gc_freed_delta={} saw_busy={} gc_blocked_delta={} log_standard_ref={LOG_STANDARD_REF}",
        chain.chain_len,
        chain.max_chain,
        chain.gc_freed_delta,
        backpressure.saw_busy,
        backpressure.gc_blocked_delta
    );
}
