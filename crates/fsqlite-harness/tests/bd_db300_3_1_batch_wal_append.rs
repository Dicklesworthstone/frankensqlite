//! Harness integration verification for bd-db300.3.1.
//!
//! This suite verifies the transaction-wide batch WAL append path at two levels:
//! - direct `WalFile` single-frame vs contiguous-batch equivalence
//! - real pager commit through `WalBackendAdapter` into a traced `WalFile`
//!
//! The emitted report is intended for the bead-specific verification script and
//! records structured evidence for checksum equivalence, write-call reduction,
//! and cross-subsystem pager/WAL commit behavior.

use std::fs;
use std::path::{Path, PathBuf};
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use fsqlite_core::wal_adapter::WalBackendAdapter;
use fsqlite_pager::{JournalMode, MvccPager, SimplePager, TransactionMode};
use fsqlite_types::cx::Cx;
use fsqlite_types::flags::VfsOpenFlags;
use fsqlite_types::{PageNumber, PageSize};
use fsqlite_vfs::metrics::MetricsSnapshot;
use fsqlite_vfs::traits::Vfs;
use fsqlite_vfs::{GLOBAL_VFS_METRICS, MemoryVfs, TracingFile};
use fsqlite_wal::WalFile;
use fsqlite_wal::checksum::WalSalts;
use fsqlite_wal::wal::WalAppendFrameRef;
use serde::Serialize;

const BEAD_ID: &str = "bd-db300.3.1";
const PAGE_SIZE_U32: u32 = 4096;
const LOW_LEVEL_FRAME_MATRIX: [usize; 4] = [1, 4, 16, 64];
const PAGER_DIRTY_PAGE_MATRIX: [usize; 3] = [2, 4, 8];

#[derive(Debug, Clone)]
struct VerificationContext {
    run_id: String,
    trace_id: String,
    scenario_id: String,
    seed: u64,
    artifact_dir: PathBuf,
}

#[derive(Debug, Clone, Copy, Serialize)]
struct IoDelta {
    write_ops: u64,
    sync_ops: u64,
    write_bytes_total: u64,
}

#[derive(Debug, Serialize)]
struct LowLevelScenarioReport {
    scenario_id: String,
    frame_count: usize,
    single_elapsed_ns: u64,
    batch_elapsed_ns: u64,
    single_io: IoDelta,
    batch_io: IoDelta,
    checksum_equivalent: bool,
    frame_content_equivalent: bool,
    write_op_reduction: u64,
}

#[derive(Debug, Serialize)]
struct PagerScenarioReport {
    scenario_id: String,
    dirty_pages: usize,
    commit_elapsed_ns: u64,
    wal_io: IoDelta,
    wal_frame_count: usize,
    commit_frame_count: usize,
    commit_marker_last: bool,
    commit_db_size: u32,
}

#[derive(Debug, Serialize)]
struct BatchWalAppendReport {
    schema_version: &'static str,
    bead_id: &'static str,
    run_id: String,
    trace_id: String,
    scenario_id: String,
    seed: u64,
    low_level_matrix: Vec<LowLevelScenarioReport>,
    pager_matrix: Vec<PagerScenarioReport>,
    summary: BatchWalAppendSummary,
}

#[derive(Debug, Serialize)]
struct BatchWalAppendSummary {
    low_level_scenarios: usize,
    pager_scenarios: usize,
    min_write_op_reduction: u64,
    total_single_write_ops: u64,
    total_batch_write_ops: u64,
    all_checksum_equivalent: bool,
    all_pager_commits_single_write: bool,
}

fn workspace_root() -> Result<PathBuf, String> {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .canonicalize()
        .map_err(|error| format!("bead_id={BEAD_ID} case=workspace_root error={error}"))
}

fn verification_context() -> Result<VerificationContext, String> {
    let default_seed = 20_260_310_u64;
    let run_id = std::env::var("BD_DB300_3_1_RUN_ID").unwrap_or_else(|_| {
        let stamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_or(0_u128, |duration| duration.as_nanos());
        format!("{BEAD_ID}-local-{stamp}")
    });
    let trace_id =
        std::env::var("BD_DB300_3_1_TRACE_ID").unwrap_or_else(|_| format!("trace-{run_id}"));
    let scenario_id =
        std::env::var("BD_DB300_3_1_SCENARIO_ID").unwrap_or_else(|_| "WAL-BATCH-MATRIX".to_owned());
    let seed = std::env::var("BD_DB300_3_1_SEED")
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(default_seed);
    let artifact_dir = match std::env::var("BD_DB300_3_1_ARTIFACT_DIR") {
        Ok(path) => PathBuf::from(path),
        Err(_) => workspace_root()?
            .join("target")
            .join("bd_db300_3_1")
            .join(&run_id),
    };

    fs::create_dir_all(&artifact_dir).map_err(|error| {
        format!(
            "bead_id={BEAD_ID} case=artifact_dir_create path={} error={error}",
            artifact_dir.display()
        )
    })?;

    Ok(VerificationContext {
        run_id,
        trace_id,
        scenario_id,
        seed,
        artifact_dir,
    })
}

fn wal_salts() -> WalSalts {
    WalSalts {
        salt1: 0xD0C0_A110,
        salt2: 0x5EED_2026,
    }
}

fn sample_page(seed: u8) -> Vec<u8> {
    let page_size = PageSize::DEFAULT.as_usize();
    let mut page = vec![0_u8; page_size];
    for (index, byte) in page.iter_mut().enumerate() {
        let reduced = u8::try_from(index % 251).expect("modulo fits in u8");
        *byte = reduced ^ seed;
    }
    page
}

fn open_memory_wal_file(
    vfs: &MemoryVfs,
    cx: &Cx,
    path: &Path,
    create: bool,
) -> Result<<MemoryVfs as Vfs>::File, String> {
    let mut flags = VfsOpenFlags::READWRITE | VfsOpenFlags::WAL;
    if create {
        flags |= VfsOpenFlags::CREATE;
    }

    let (file, _) = vfs.open(cx, Some(path), flags).map_err(|error| {
        format!(
            "bead_id={BEAD_ID} case=open_memory_wal_file path={} create={} error={error}",
            path.display(),
            create
        )
    })?;
    Ok(file)
}

fn tracing_wal_file(
    vfs: &MemoryVfs,
    cx: &Cx,
    path: &Path,
    create: bool,
) -> Result<TracingFile<<MemoryVfs as Vfs>::File>, String> {
    let file = open_memory_wal_file(vfs, cx, path, create)?;
    Ok(TracingFile::new(file, path.display().to_string()))
}

fn io_delta(before: MetricsSnapshot, after: MetricsSnapshot) -> IoDelta {
    IoDelta {
        write_ops: after.write_ops.saturating_sub(before.write_ops),
        sync_ops: after.sync_ops.saturating_sub(before.sync_ops),
        write_bytes_total: after
            .write_bytes_total
            .saturating_sub(before.write_bytes_total),
    }
}

fn elapsed_ns(start: Instant) -> u64 {
    u64::try_from(start.elapsed().as_nanos()).unwrap_or(u64::MAX)
}

fn low_level_scenario(frame_count: usize) -> Result<LowLevelScenarioReport, String> {
    let cx = Cx::default();
    let vfs_single = MemoryVfs::new();
    let vfs_batch = MemoryVfs::new();
    let single_path = PathBuf::from(format!("/bd_db300_3_1_single_{frame_count}.wal"));
    let batch_path = PathBuf::from(format!("/bd_db300_3_1_batch_{frame_count}.wal"));

    let pages: Vec<Vec<u8>> = (0..frame_count)
        .map(|index| sample_page(u8::try_from(index % 251).expect("seed fits")))
        .collect();
    let commit_sizes: Vec<u32> = (0..frame_count)
        .map(|index| {
            if index + 1 == frame_count {
                u32::try_from(frame_count).expect("frame count fits u32")
            } else {
                0
            }
        })
        .collect();

    let single_file = tracing_wal_file(&vfs_single, &cx, &single_path, true)?;
    let mut wal_single = WalFile::create(&cx, single_file, PAGE_SIZE_U32, 0, wal_salts())
        .map_err(|error| format!("bead_id={BEAD_ID} case=single_create error={error}"))?;
    let single_before = GLOBAL_VFS_METRICS.snapshot();
    let single_start = Instant::now();
    for (index, page) in pages.iter().enumerate() {
        wal_single
            .append_frame(
                &cx,
                u32::try_from(index + 1).expect("page number fits u32"),
                page,
                commit_sizes[index],
            )
            .map_err(|error| format!("bead_id={BEAD_ID} case=single_append error={error}"))?;
    }
    let single_elapsed_ns = elapsed_ns(single_start);
    let single_after = GLOBAL_VFS_METRICS.snapshot();

    let batch_file = tracing_wal_file(&vfs_batch, &cx, &batch_path, true)?;
    let mut wal_batch = WalFile::create(&cx, batch_file, PAGE_SIZE_U32, 0, wal_salts())
        .map_err(|error| format!("bead_id={BEAD_ID} case=batch_create error={error}"))?;
    let batch_frames: Vec<_> = pages
        .iter()
        .enumerate()
        .map(|(index, page)| WalAppendFrameRef {
            page_number: u32::try_from(index + 1).expect("page number fits u32"),
            page_data: page,
            db_size_if_commit: commit_sizes[index],
        })
        .collect();
    let batch_before = GLOBAL_VFS_METRICS.snapshot();
    let batch_start = Instant::now();
    wal_batch
        .append_frames(&cx, &batch_frames)
        .map_err(|error| format!("bead_id={BEAD_ID} case=batch_append error={error}"))?;
    let batch_elapsed_ns = elapsed_ns(batch_start);
    let batch_after = GLOBAL_VFS_METRICS.snapshot();

    let checksum_equivalent = wal_single.running_checksum() == wal_batch.running_checksum();
    let mut frame_content_equivalent = true;
    for frame_index in 0..frame_count {
        let (single_header, single_data) = wal_single
            .read_frame(&cx, frame_index)
            .map_err(|error| format!("bead_id={BEAD_ID} case=single_read error={error}"))?;
        let (batch_header, batch_data) = wal_batch
            .read_frame(&cx, frame_index)
            .map_err(|error| format!("bead_id={BEAD_ID} case=batch_read error={error}"))?;
        if single_header != batch_header || single_data != batch_data {
            frame_content_equivalent = false;
            break;
        }
    }

    wal_single
        .close(&cx)
        .map_err(|error| format!("bead_id={BEAD_ID} case=single_close error={error}"))?;
    wal_batch
        .close(&cx)
        .map_err(|error| format!("bead_id={BEAD_ID} case=batch_close error={error}"))?;

    Ok(LowLevelScenarioReport {
        scenario_id: format!("wal-file-{frame_count}-frames"),
        frame_count,
        single_elapsed_ns,
        batch_elapsed_ns,
        single_io: io_delta(single_before, single_after),
        batch_io: io_delta(batch_before, batch_after),
        checksum_equivalent,
        frame_content_equivalent,
        write_op_reduction: io_delta(single_before, single_after)
            .write_ops
            .saturating_sub(io_delta(batch_before, batch_after).write_ops),
    })
}

fn pager_scenario(dirty_pages: usize) -> Result<PagerScenarioReport, String> {
    let cx = Cx::default();
    let vfs = MemoryVfs::new();
    let db_path = PathBuf::from(format!("/bd_db300_3_1_pager_{dirty_pages}.db"));
    let wal_path = PathBuf::from(format!("/bd_db300_3_1_pager_{dirty_pages}.db-wal"));

    let pager = SimplePager::open(vfs.clone(), &db_path, PageSize::DEFAULT).map_err(|error| {
        format!(
            "bead_id={BEAD_ID} case=pager_open path={} error={error}",
            db_path.display()
        )
    })?;

    let wal_file = tracing_wal_file(&vfs, &cx, &wal_path, true)?;
    let wal = WalFile::create(&cx, wal_file, PAGE_SIZE_U32, 0, wal_salts())
        .map_err(|error| format!("bead_id={BEAD_ID} case=pager_wal_create error={error}"))?;
    pager
        .set_wal_backend(Box::new(WalBackendAdapter::new(wal)))
        .map_err(|error| format!("bead_id={BEAD_ID} case=set_wal_backend error={error}"))?;
    pager
        .set_journal_mode(&cx, JournalMode::Wal)
        .map_err(|error| format!("bead_id={BEAD_ID} case=set_journal_mode error={error}"))?;

    let page_size = PageSize::DEFAULT.as_usize();
    let mut txn = pager
        .begin(&cx, TransactionMode::Immediate)
        .map_err(|error| format!("bead_id={BEAD_ID} case=begin_txn error={error}"))?;
    txn.write_page(&cx, PageNumber::ONE, &vec![0x11; page_size])
        .map_err(|error| format!("bead_id={BEAD_ID} case=write_page1 error={error}"))?;
    for index in 1..dirty_pages {
        let page_number = txn
            .allocate_page(&cx)
            .map_err(|error| format!("bead_id={BEAD_ID} case=allocate_page error={error}"))?;
        let seed = u8::try_from(index % 251).expect("seed fits in u8");
        txn.write_page(&cx, page_number, &sample_page(seed))
            .map_err(|error| format!("bead_id={BEAD_ID} case=write_allocated error={error}"))?;
    }

    let before_commit = GLOBAL_VFS_METRICS.snapshot();
    let commit_start = Instant::now();
    txn.commit(&cx)
        .map_err(|error| format!("bead_id={BEAD_ID} case=commit error={error}"))?;
    let commit_elapsed_ns = elapsed_ns(commit_start);
    let after_commit = GLOBAL_VFS_METRICS.snapshot();

    let reader_file = open_memory_wal_file(&vfs, &cx, &wal_path, false)?;
    let mut wal_reader = WalFile::open(&cx, reader_file)
        .map_err(|error| format!("bead_id={BEAD_ID} case=wal_reopen error={error}"))?;
    let wal_frame_count = wal_reader.frame_count();
    let mut commit_frame_count = 0_usize;
    let mut commit_marker_last = false;
    let mut commit_db_size = 0_u32;
    for frame_index in 0..wal_frame_count {
        let (header, _) = wal_reader
            .read_frame(&cx, frame_index)
            .map_err(|error| format!("bead_id={BEAD_ID} case=wal_reopen_read error={error}"))?;
        if header.db_size > 0 {
            commit_frame_count += 1;
            commit_db_size = header.db_size;
            commit_marker_last = frame_index + 1 == wal_frame_count;
        }
    }
    wal_reader
        .close(&cx)
        .map_err(|error| format!("bead_id={BEAD_ID} case=wal_reopen_close error={error}"))?;

    Ok(PagerScenarioReport {
        scenario_id: format!("pager-commit-{dirty_pages}-dirty-pages"),
        dirty_pages,
        commit_elapsed_ns,
        wal_io: io_delta(before_commit, after_commit),
        wal_frame_count,
        commit_frame_count,
        commit_marker_last,
        commit_db_size,
    })
}

fn write_report(path: &Path, report: &BatchWalAppendReport) -> Result<(), String> {
    let payload = serde_json::to_string_pretty(report)
        .map_err(|error| format!("bead_id={BEAD_ID} case=serialize_report error={error}"))?;
    fs::write(path, payload).map_err(|error| {
        format!(
            "bead_id={BEAD_ID} case=write_report path={} error={error}",
            path.display()
        )
    })
}

#[test]
fn test_e2e_bd_db300_3_1_batch_wal_append_verification() -> Result<(), String> {
    let context = verification_context()?;
    let report_path = context.artifact_dir.join("batch_wal_append_report.json");

    eprintln!(
        "INFO bead_id={BEAD_ID} phase=metadata run_id={} trace_id={} scenario_id={} seed={} artifact_dir={}",
        context.run_id,
        context.trace_id,
        context.scenario_id,
        context.seed,
        context.artifact_dir.display()
    );

    let mut low_level_matrix = Vec::new();
    for frame_count in LOW_LEVEL_FRAME_MATRIX {
        let scenario = low_level_scenario(frame_count)?;
        eprintln!(
            "DEBUG bead_id={BEAD_ID} phase=low_level scenario_id={} frame_count={} \
             single_write_ops={} batch_write_ops={} single_elapsed_ns={} batch_elapsed_ns={}",
            scenario.scenario_id,
            scenario.frame_count,
            scenario.single_io.write_ops,
            scenario.batch_io.write_ops,
            scenario.single_elapsed_ns,
            scenario.batch_elapsed_ns
        );
        if scenario.batch_elapsed_ns > scenario.single_elapsed_ns {
            eprintln!(
                "WARN bead_id={BEAD_ID} phase=low_level scenario_id={} \
                 batch_elapsed_ns={} single_elapsed_ns={} note=batch_slower_in_memory_run",
                scenario.scenario_id, scenario.batch_elapsed_ns, scenario.single_elapsed_ns
            );
        }
        assert!(
            scenario.checksum_equivalent,
            "bead_id={BEAD_ID} case=checksum_equivalent scenario={}",
            scenario.scenario_id
        );
        assert!(
            scenario.frame_content_equivalent,
            "bead_id={BEAD_ID} case=frame_content_equivalent scenario={}",
            scenario.scenario_id
        );
        assert_eq!(
            scenario.single_io.write_ops,
            u64::try_from(scenario.frame_count).expect("frame count fits u64"),
            "bead_id={BEAD_ID} case=single_write_ops scenario={}",
            scenario.scenario_id
        );
        assert_eq!(
            scenario.batch_io.write_ops, 1,
            "bead_id={BEAD_ID} case=batch_write_ops scenario={}",
            scenario.scenario_id
        );
        low_level_matrix.push(scenario);
    }

    let mut pager_matrix = Vec::new();
    for dirty_pages in PAGER_DIRTY_PAGE_MATRIX {
        let scenario = pager_scenario(dirty_pages)?;
        eprintln!(
            "INFO bead_id={BEAD_ID} phase=pager_commit scenario_id={} dirty_pages={} \
             wal_write_ops={} wal_sync_ops={} wal_frame_count={} commit_elapsed_ns={}",
            scenario.scenario_id,
            scenario.dirty_pages,
            scenario.wal_io.write_ops,
            scenario.wal_io.sync_ops,
            scenario.wal_frame_count,
            scenario.commit_elapsed_ns
        );
        assert_eq!(
            scenario.wal_io.write_ops, 1,
            "bead_id={BEAD_ID} case=pager_single_wal_write scenario={}",
            scenario.scenario_id
        );
        assert_eq!(
            scenario.wal_frame_count, scenario.dirty_pages,
            "bead_id={BEAD_ID} case=pager_frame_count scenario={}",
            scenario.scenario_id
        );
        assert_eq!(
            scenario.commit_frame_count, 1,
            "bead_id={BEAD_ID} case=pager_commit_frame_count scenario={}",
            scenario.scenario_id
        );
        assert!(
            scenario.commit_marker_last,
            "bead_id={BEAD_ID} case=pager_commit_marker_last scenario={}",
            scenario.scenario_id
        );
        assert_eq!(
            scenario.commit_db_size,
            u32::try_from(scenario.dirty_pages).expect("dirty_pages fits u32"),
            "bead_id={BEAD_ID} case=pager_commit_db_size scenario={}",
            scenario.scenario_id
        );
        pager_matrix.push(scenario);
    }

    let total_single_write_ops = low_level_matrix
        .iter()
        .map(|scenario| scenario.single_io.write_ops)
        .sum();
    let total_batch_write_ops = low_level_matrix
        .iter()
        .map(|scenario| scenario.batch_io.write_ops)
        .sum();
    let min_write_op_reduction = low_level_matrix
        .iter()
        .map(|scenario| scenario.write_op_reduction)
        .min()
        .unwrap_or(0);
    let all_checksum_equivalent = low_level_matrix
        .iter()
        .all(|scenario| scenario.checksum_equivalent && scenario.frame_content_equivalent);
    let all_pager_commits_single_write = pager_matrix
        .iter()
        .all(|scenario| scenario.wal_io.write_ops == 1 && scenario.commit_marker_last);

    let report = BatchWalAppendReport {
        schema_version: "fsqlite.batch-wal-append.v1",
        bead_id: BEAD_ID,
        run_id: context.run_id.clone(),
        trace_id: context.trace_id.clone(),
        scenario_id: context.scenario_id.clone(),
        seed: context.seed,
        low_level_matrix,
        pager_matrix,
        summary: BatchWalAppendSummary {
            low_level_scenarios: LOW_LEVEL_FRAME_MATRIX.len(),
            pager_scenarios: PAGER_DIRTY_PAGE_MATRIX.len(),
            min_write_op_reduction,
            total_single_write_ops,
            total_batch_write_ops,
            all_checksum_equivalent,
            all_pager_commits_single_write,
        },
    };

    write_report(&report_path, &report)?;

    eprintln!(
        "INFO bead_id={BEAD_ID} phase=artifact_written run_id={} trace_id={} scenario_id={} artifact_path={}",
        context.run_id,
        context.trace_id,
        context.scenario_id,
        report_path.display()
    );

    assert!(
        report.summary.all_checksum_equivalent,
        "bead_id={BEAD_ID} case=summary_checksum_equivalent"
    );
    assert!(
        report.summary.all_pager_commits_single_write,
        "bead_id={BEAD_ID} case=summary_pager_single_write"
    );
    assert!(
        report.summary.total_single_write_ops > report.summary.total_batch_write_ops,
        "bead_id={BEAD_ID} case=summary_write_ops_reduced"
    );

    Ok(())
}
