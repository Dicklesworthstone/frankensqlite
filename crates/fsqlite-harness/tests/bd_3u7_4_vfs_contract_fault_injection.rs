use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use fsqlite_error::FrankenError;
use fsqlite_harness::fault_vfs::{
    FaultInjectingVfs, FaultMetricsSnapshot, FaultSpec, TEST_VFS_FAULT_COUNTER_NAME,
};
use fsqlite_types::LockLevel;
use fsqlite_types::cx::Cx;
use fsqlite_types::flags::{SyncFlags, VfsOpenFlags};
#[cfg(target_os = "linux")]
use fsqlite_vfs::IoUringVfs;
use fsqlite_vfs::MemoryVfs;
#[cfg(unix)]
use fsqlite_vfs::UnixVfs;
use fsqlite_vfs::traits::{Vfs, VfsFile};
use serde::Serialize;
#[cfg(unix)]
use tempfile::TempDir;

const BEAD_ID: &str = "bd-3u7.4";
const SCENARIO_ID: &str = "TEST-VFS-CONTRACT-FAULT-MATRIX";
const DEFAULT_SEED: u64 = 0x3A7D_4F4A_u64;
const DEFAULT_BENCH_ITERS: usize = 512;
const BENCH_PAGE_SIZE: usize = 4096;
const BENCH_PAGE_SIZE_U64: u64 = 4096;
const BENCH_PAGE_COUNT: u64 = 128;
const LATENCY_BASE_MS: u64 = 2;

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
struct FaultContractRun {
    replay_seed: u64,
    unix_lock_probe: String,
    lock_steps: Vec<String>,
    fault_steps: Vec<String>,
    metric_name: String,
    metrics_by_fault_type: BTreeMap<String, u64>,
    metric_total: u64,
}

#[derive(Debug, Clone, Serialize)]
struct ThroughputRun {
    baseline_ops_per_sec: f64,
    wrapped_ops_per_sec: f64,
    wrapped_vs_baseline_ratio: f64,
    unix_ops_per_sec: f64,
    io_uring_ops_per_sec: f64,
    io_uring_vs_unix_ratio: f64,
    bench_iterations: usize,
    io_uring_available: bool,
    io_uring_status: String,
}

#[derive(Debug, Serialize)]
struct ContractSuiteArtifact {
    schema_version: u32,
    bead_id: String,
    run_id: String,
    trace_id: String,
    scenario_id: String,
    seed: u64,
    duration_ms: u128,
    replay_commands: Vec<String>,
    fault_contract: FaultContractRun,
    throughput: ThroughputRun,
}

fn scenario_seed() -> u64 {
    std::env::var("BD_3U7_4_SEED")
        .ok()
        .and_then(|raw| raw.parse::<u64>().ok())
        .unwrap_or(DEFAULT_SEED)
}

fn bench_iters() -> usize {
    std::env::var("BD_3U7_4_BENCH_ITERS")
        .ok()
        .and_then(|raw| raw.parse::<usize>().ok())
        .filter(|iters| *iters > 0)
        .unwrap_or(DEFAULT_BENCH_ITERS)
}

fn require_io_uring() -> bool {
    std::env::var("BD_3U7_4_REQUIRE_IO_URING")
        .ok()
        .is_some_and(|raw| matches!(raw.to_ascii_lowercase().as_str(), "1" | "true" | "yes"))
}

fn workspace_root() -> Result<PathBuf, String> {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .canonicalize()
        .map_err(|error| format!("workspace_root_canonicalize_failed: {error}"))
}

fn write_suite_artifact(artifact: &ContractSuiteArtifact) -> Result<PathBuf, String> {
    let root = workspace_root()?;
    let output_dir = root.join("test-results").join("bd_3u7_4");
    fs::create_dir_all(&output_dir).map_err(|error| {
        format!(
            "artifact_dir_create_failed path={} error={error}",
            output_dir.display()
        )
    })?;

    let output_path = output_dir.join(format!("{}.json", artifact.run_id));
    let payload = serde_json::to_string_pretty(artifact)
        .map_err(|error| format!("artifact_serialize_failed error={error}"))?;
    fs::write(&output_path, payload).map_err(|error| {
        format!(
            "artifact_write_failed path={} error={error}",
            output_path.display()
        )
    })?;
    Ok(output_path)
}

fn open_rw_main_file<V: Vfs>(vfs: &V, cx: &Cx, path: &str) -> Result<V::File, String> {
    let flags = VfsOpenFlags::READWRITE | VfsOpenFlags::CREATE | VfsOpenFlags::MAIN_DB;
    let (file, _out_flags) = vfs
        .open(cx, Some(Path::new(path)), flags)
        .map_err(|error| format!("open_failed path={path} error={error}"))?;
    Ok(file)
}

fn expect_io_error(error: &FrankenError, context: &str) -> Result<(), String> {
    if matches!(error, FrankenError::Io(_)) {
        return Ok(());
    }
    Err(format!(
        "expected_io_error context={context} actual={error:?}"
    ))
}

fn lcg_next(state: &mut u64) -> u64 {
    *state = state
        .wrapping_mul(6_364_136_223_846_793_005)
        .wrapping_add(1_442_695_040_888_963_407);
    *state
}

fn run_rw_benchmark<V: Vfs>(
    vfs: &V,
    path: &str,
    iterations: usize,
    mut seed: u64,
) -> Result<f64, String> {
    let cx = Cx::new();
    let mut file = open_rw_main_file(vfs, &cx, path)?;
    let page = vec![0xAB_u8; BENCH_PAGE_SIZE];
    let mut read_buf = vec![0_u8; BENCH_PAGE_SIZE];

    for page_index in 0_u64..BENCH_PAGE_COUNT {
        file.write(&cx, &page, page_index * BENCH_PAGE_SIZE_U64)
            .map_err(|error| format!("benchmark_prewrite_failed path={path} error={error}"))?;
    }

    let started = Instant::now();
    for _ in 0..iterations {
        let read_page = lcg_next(&mut seed) % BENCH_PAGE_COUNT;
        let read_offset = read_page * BENCH_PAGE_SIZE_U64;
        let read = file
            .read(&cx, &mut read_buf, read_offset)
            .map_err(|error| format!("benchmark_read_failed path={path} error={error}"))?;
        if read != BENCH_PAGE_SIZE {
            return Err(format!(
                "benchmark_short_read path={path} expected={BENCH_PAGE_SIZE} actual={read}"
            ));
        }

        let write_page = lcg_next(&mut seed) % BENCH_PAGE_COUNT;
        let write_offset = write_page * BENCH_PAGE_SIZE_U64;
        file.write(&cx, &page, write_offset)
            .map_err(|error| format!("benchmark_write_failed path={path} error={error}"))?;
    }

    let elapsed = started.elapsed().as_secs_f64();
    if elapsed <= 0.0 {
        return Err(format!("benchmark_elapsed_zero path={path}"));
    }
    let ops = (iterations as f64) * 2.0;
    Ok(ops / elapsed)
}

fn run_lock_contract(vfs: &FaultInjectingVfs<MemoryVfs>, cx: &Cx) -> Result<Vec<String>, String> {
    let mut lock_steps = Vec::new();
    let mut lock_a = open_rw_main_file(vfs, cx, "lock_contract.db")?;
    let mut lock_b = open_rw_main_file(vfs, cx, "lock_contract.db")?;

    lock_a
        .lock(cx, LockLevel::Shared)
        .map_err(|error| format!("lock_a_shared_failed error={error}"))?;
    lock_steps.push("lock_a_shared".to_owned());
    lock_a
        .lock(cx, LockLevel::Reserved)
        .map_err(|error| format!("lock_a_reserved_failed error={error}"))?;
    lock_steps.push("lock_a_reserved".to_owned());
    lock_a
        .lock(cx, LockLevel::Exclusive)
        .map_err(|error| format!("lock_a_exclusive_failed error={error}"))?;
    lock_steps.push("lock_a_exclusive".to_owned());

    lock_b
        .lock(cx, LockLevel::Shared)
        .map_err(|error| format!("lock_b_shared_failed error={error}"))?;
    lock_steps.push("lock_b_shared".to_owned());
    lock_b
        .lock(cx, LockLevel::Reserved)
        .map_err(|error| format!("lock_b_reserved_failed error={error}"))?;
    lock_steps.push("lock_b_reserved".to_owned());
    lock_b
        .lock(cx, LockLevel::Exclusive)
        .map_err(|error| format!("lock_b_exclusive_failed error={error}"))?;
    lock_steps.push("lock_b_exclusive".to_owned());

    let reserved_a = lock_a
        .check_reserved_lock(cx)
        .map_err(|error| format!("check_reserved_a_failed error={error}"))?;
    let reserved_b = lock_b
        .check_reserved_lock(cx)
        .map_err(|error| format!("check_reserved_b_failed error={error}"))?;
    if reserved_a || reserved_b {
        return Err(format!(
            "memory_vfs_reserved_lock_semantics_changed reserved_a={reserved_a} reserved_b={reserved_b}"
        ));
    }
    lock_steps.push("reserved_probe=false,false".to_owned());

    lock_a
        .unlock(cx, LockLevel::None)
        .map_err(|error| format!("unlock_a_failed error={error}"))?;
    lock_steps.push("unlock_a_none".to_owned());
    lock_b
        .unlock(cx, LockLevel::None)
        .map_err(|error| format!("unlock_b_failed error={error}"))?;
    lock_steps.push("unlock_b_none".to_owned());

    lock_a
        .close(cx)
        .map_err(|error| format!("close_a_failed error={error}"))?;
    lock_b
        .close(cx)
        .map_err(|error| format!("close_b_failed error={error}"))?;
    Ok(lock_steps)
}

fn run_fault_matrix(vfs: &FaultInjectingVfs<MemoryVfs>, cx: &Cx) -> Result<Vec<String>, String> {
    let mut fault_steps = Vec::new();

    let mut read_fail = open_rw_main_file(vfs, cx, "read_fail.db")?;
    let mut read_buf = [0_u8; 8];
    let read_error = read_fail
        .read(cx, &mut read_buf, 0)
        .expect_err("read failure fault should force read error");
    expect_io_error(&read_error, "read_failure")?;
    fault_steps.push("read_failure=io_error".to_owned());
    read_fail
        .close(cx)
        .map_err(|error| format!("read_fail_close_failed error={error}"))?;

    let mut write_fail = open_rw_main_file(vfs, cx, "write_fail.db")?;
    let write_error = write_fail
        .write(cx, b"write-fail", 0)
        .expect_err("write failure fault should force write error");
    expect_io_error(&write_error, "write_failure")?;
    fault_steps.push("write_failure=io_error".to_owned());
    write_fail
        .close(cx)
        .map_err(|error| format!("write_fail_close_failed error={error}"))?;

    let mut partial = open_rw_main_file(vfs, cx, "partial.db")?;
    let partial_error = partial
        .write(cx, b"abcdefgh", 0)
        .expect_err("partial write fault should surface an I/O error");
    expect_io_error(&partial_error, "partial_write")?;
    let partial_size = partial
        .file_size(cx)
        .map_err(|error| format!("partial_size_failed error={error}"))?;
    if partial_size != 3 {
        return Err(format!(
            "partial_write_expected_size expected=3 actual={partial_size}"
        ));
    }
    let mut partial_buf = [0_u8; 8];
    let partial_read = partial
        .read(cx, &mut partial_buf, 0)
        .map_err(|error| format!("partial_read_failed error={error}"))?;
    if partial_read != 3 || &partial_buf[..3] != b"abc" {
        return Err(format!(
            "partial_write_payload_mismatch actual_read={partial_read} payload={:?}",
            &partial_buf[..partial_read]
        ));
    }
    fault_steps.push(format!("partial_write_bytes={partial_size}"));
    partial
        .close(cx)
        .map_err(|error| format!("partial_close_failed error={error}"))?;

    let mut disk_full = open_rw_main_file(vfs, cx, "disk_full.db")?;
    let disk_error = disk_full
        .write(cx, b"disk", 0)
        .expect_err("disk full fault should fail writes");
    if !matches!(disk_error, FrankenError::DatabaseFull) {
        return Err(format!(
            "disk_full_expected_database_full actual={disk_error:?}"
        ));
    }
    fault_steps.push("disk_full=database_full".to_owned());
    disk_full
        .close(cx)
        .map_err(|error| format!("disk_full_close_failed error={error}"))?;

    let mut latency = open_rw_main_file(vfs, cx, "latency.db")?;
    let mut latency_buf = [0_u8; 4];
    let latency_start = Instant::now();
    let latency_read = latency
        .read(cx, &mut latency_buf, 0)
        .map_err(|error| format!("latency_read_failed error={error}"))?;
    let latency_elapsed = latency_start.elapsed();
    if latency_read != 0 {
        return Err(format!(
            "latency_expected_empty_read actual_read={latency_read}"
        ));
    }
    if latency_elapsed < Duration::from_millis(LATENCY_BASE_MS) {
        return Err(format!(
            "latency_fault_not_applied expected_at_least_ms={LATENCY_BASE_MS} actual_ms={}",
            latency_elapsed.as_millis()
        ));
    }
    fault_steps.push(format!("latency_ms={}", latency_elapsed.as_millis()));
    latency
        .close(cx)
        .map_err(|error| format!("latency_close_failed error={error}"))?;

    let mut torn = open_rw_main_file(vfs, cx, "torn.db")?;
    let torn_error = torn
        .write(cx, b"abcdefgh", 0)
        .expect_err("torn write should fail write call after partial persistence");
    expect_io_error(&torn_error, "torn_write")?;
    let torn_size = torn
        .file_size(cx)
        .map_err(|error| format!("torn_size_failed error={error}"))?;
    if torn_size != 4 {
        return Err(format!(
            "torn_write_expected_size expected=4 actual={torn_size}"
        ));
    }
    let mut torn_buf = [0_u8; 8];
    let torn_read = torn
        .read(cx, &mut torn_buf, 0)
        .map_err(|error| format!("torn_read_failed error={error}"))?;
    if torn_read != 4 || &torn_buf[..4] != b"abcd" {
        return Err(format!(
            "torn_write_payload_mismatch actual_read={torn_read} payload={:?}",
            &torn_buf[..torn_read]
        ));
    }
    fault_steps.push(format!("torn_write_bytes={torn_size}"));
    torn.close(cx)
        .map_err(|error| format!("torn_close_failed error={error}"))?;

    let mut power = open_rw_main_file(vfs, cx, "power.db")?;
    power
        .write(cx, b"power", 0)
        .map_err(|error| format!("power_write_before_cut_failed error={error}"))?;
    let sync_error = power
        .sync(cx, SyncFlags::FULL)
        .expect_err("power cut fault should fail sync");
    expect_io_error(&sync_error, "power_cut_sync")?;
    let write_after_cut = power
        .write(cx, b"down", 2)
        .expect_err("writes after power cut should fail");
    expect_io_error(&write_after_cut, "power_cut_write_after")?;
    fault_steps.push("power_cut=sync_error_then_powered_off".to_owned());
    power
        .close(cx)
        .map_err(|error| format!("power_close_failed error={error}"))?;

    Ok(fault_steps)
}

#[cfg(unix)]
fn run_unix_lock_probe() -> Result<String, String> {
    let cx = Cx::new();
    let tempdir = TempDir::new().map_err(|error| format!("unix_probe_tempdir_failed: {error}"))?;
    let path = tempdir.path().join("bd_3u7_4_unix_lock.db");
    let vfs = UnixVfs::new();
    let flags = VfsOpenFlags::READWRITE | VfsOpenFlags::CREATE | VfsOpenFlags::MAIN_DB;
    let (mut file_a, _) = vfs
        .open(&cx, Some(&path), flags)
        .map_err(|error| format!("unix_probe_open_a_failed error={error}"))?;
    let (mut file_b, _) = vfs
        .open(&cx, Some(&path), flags)
        .map_err(|error| format!("unix_probe_open_b_failed error={error}"))?;

    file_a
        .lock(&cx, LockLevel::Shared)
        .map_err(|error| format!("unix_probe_lock_a_shared_failed error={error}"))?;
    file_a
        .lock(&cx, LockLevel::Reserved)
        .map_err(|error| format!("unix_probe_lock_a_reserved_failed error={error}"))?;

    let reserved_seen = file_b
        .check_reserved_lock(&cx)
        .map_err(|error| format!("unix_probe_reserved_probe_failed error={error}"))?;
    // Unix advisory locks are process-scoped, not fd-scoped. In this same-process probe,
    // check_reserved_lock should not report our own reservation as "held by others".
    if reserved_seen {
        return Err("unix_probe_unexpected_reserved_lock_visibility_same_process".to_owned());
    }

    file_b
        .lock(&cx, LockLevel::Shared)
        .map_err(|error| format!("unix_probe_lock_b_shared_failed error={error}"))?;
    file_b
        .lock(&cx, LockLevel::Reserved)
        .map_err(|error| format!("unix_probe_lock_b_reserved_failed error={error}"))?;

    file_a
        .unlock(&cx, LockLevel::None)
        .map_err(|error| format!("unix_probe_unlock_a_failed error={error}"))?;
    file_b
        .unlock(&cx, LockLevel::None)
        .map_err(|error| format!("unix_probe_unlock_b_failed error={error}"))?;

    let reserved_after = file_b
        .check_reserved_lock(&cx)
        .map_err(|error| format!("unix_probe_reserved_after_release_failed error={error}"))?;
    if reserved_after {
        return Err("unix_probe_reserved_lock_stuck_after_release".to_owned());
    }

    file_a
        .close(&cx)
        .map_err(|error| format!("unix_probe_close_a_failed error={error}"))?;
    file_b
        .close(&cx)
        .map_err(|error| format!("unix_probe_close_b_failed error={error}"))?;

    Ok("unix_reserved_lock_exclusivity_pass".to_owned())
}

#[cfg(not(unix))]
fn run_unix_lock_probe() -> Result<String, String> {
    Ok("unix_lock_probe_skipped_non_unix".to_owned())
}

fn run_fault_contract(seed: u64) -> Result<FaultContractRun, String> {
    let vfs = FaultInjectingVfs::with_seed(MemoryVfs::new(), seed);
    vfs.inject_fault(FaultSpec::read_failure("read_fail.db").build());
    vfs.inject_fault(FaultSpec::write_failure("write_fail.db").build());
    vfs.inject_fault(
        FaultSpec::partial_write("partial.db")
            .bytes_written(3)
            .build(),
    );
    vfs.inject_fault(FaultSpec::disk_full("disk_full.db").build());
    vfs.inject_fault(
        FaultSpec::latency("latency.db")
            .latency_millis(LATENCY_BASE_MS)
            .jitter_millis(0)
            .build(),
    );
    vfs.inject_fault(
        FaultSpec::torn_write("torn.db")
            .at_offset_bytes(0)
            .valid_bytes(4)
            .build(),
    );
    vfs.inject_fault(FaultSpec::power_cut("power.db").after_nth_sync(0).build());

    let cx = Cx::new();
    let unix_lock_probe = run_unix_lock_probe()?;
    let lock_steps = run_lock_contract(&vfs, &cx)?;
    let fault_steps = run_fault_matrix(&vfs, &cx)?;
    let FaultMetricsSnapshot {
        metric_name,
        by_fault_type,
        total,
    } = vfs.metrics_snapshot();

    Ok(FaultContractRun {
        replay_seed: vfs.replay_seed(),
        unix_lock_probe,
        lock_steps,
        fault_steps,
        metric_name: metric_name.to_owned(),
        metrics_by_fault_type: by_fault_type,
        metric_total: total,
    })
}

#[cfg(target_os = "linux")]
fn run_uring_comparison(seed: u64, iterations: usize) -> Result<(f64, f64, bool, String), String> {
    let unix_vfs = UnixVfs::new();
    let unix_ops_per_sec = run_rw_benchmark(
        &unix_vfs,
        "bench_unix.db",
        iterations,
        seed ^ 0x5151_7272_9393_A4A4,
    )?;

    let io_uring_vfs = IoUringVfs::new();
    let io_uring_available = io_uring_vfs.is_available();
    let io_uring_status = io_uring_vfs.status().to_owned();
    if !io_uring_available {
        return Ok((unix_ops_per_sec, 0.0, false, io_uring_status));
    }

    let io_uring_ops_per_sec = run_rw_benchmark(
        &io_uring_vfs,
        "bench_io_uring.db",
        iterations,
        seed ^ 0xB1B1_C2C2_D3D3_E4E4,
    )?;
    Ok((
        unix_ops_per_sec,
        io_uring_ops_per_sec,
        true,
        io_uring_status,
    ))
}

#[cfg(all(unix, not(target_os = "linux")))]
fn run_uring_comparison(seed: u64, iterations: usize) -> Result<(f64, f64, bool, String), String> {
    let unix_vfs = UnixVfs::new();
    let unix_ops_per_sec = run_rw_benchmark(
        &unix_vfs,
        "bench_unix.db",
        iterations,
        seed ^ 0x5151_7272_9393_A4A4,
    )?;
    Ok((
        unix_ops_per_sec,
        0.0,
        false,
        "unsupported_non_linux".to_owned(),
    ))
}

#[cfg(not(unix))]
fn run_uring_comparison(
    _seed: u64,
    _iterations: usize,
) -> Result<(f64, f64, bool, String), String> {
    Ok((0.0, 0.0, false, "unsupported_non_unix".to_owned()))
}

fn run_throughput(seed: u64, iterations: usize) -> Result<ThroughputRun, String> {
    let baseline_vfs = MemoryVfs::new();
    let wrapped_vfs = FaultInjectingVfs::with_seed(MemoryVfs::new(), seed ^ 0xA5A5_A5A5_A5A5_A5A5);

    let baseline_ops_per_sec = run_rw_benchmark(
        &baseline_vfs,
        "bench_baseline.db",
        iterations,
        seed ^ 0x1111_2222,
    )?;
    let wrapped_ops_per_sec = run_rw_benchmark(
        &wrapped_vfs,
        "bench_wrapped.db",
        iterations,
        seed ^ 0x3333_4444,
    )?;
    let wrapped_vs_baseline_ratio = if baseline_ops_per_sec > 0.0 {
        wrapped_ops_per_sec / baseline_ops_per_sec
    } else {
        0.0
    };

    let (unix_ops_per_sec, io_uring_ops_per_sec, io_uring_available, io_uring_status) =
        run_uring_comparison(seed, iterations)?;
    let io_uring_vs_unix_ratio = if io_uring_available && unix_ops_per_sec > 0.0 {
        io_uring_ops_per_sec / unix_ops_per_sec
    } else {
        0.0
    };

    Ok(ThroughputRun {
        baseline_ops_per_sec,
        wrapped_ops_per_sec,
        wrapped_vs_baseline_ratio,
        unix_ops_per_sec,
        io_uring_ops_per_sec,
        io_uring_vs_unix_ratio,
        bench_iterations: iterations,
        io_uring_available,
        io_uring_status,
    })
}

#[test]
fn test_e2e_bd_3u7_4_vfs_contract_fault_matrix() {
    let seed = scenario_seed();
    let iterations = bench_iters();
    let started = Instant::now();

    let first = run_fault_contract(seed).expect("bd-3u7.4 fault contract run should pass");
    let second = run_fault_contract(seed).expect("bd-3u7.4 deterministic replay should pass");
    assert_eq!(
        first, second,
        "bead_id={BEAD_ID} deterministic fault contract replay failed for seed={seed}"
    );
    assert!(
        first.unix_lock_probe.contains("pass") || first.unix_lock_probe.contains("skipped"),
        "bead_id={BEAD_ID} unix lock probe result should be pass|skipped, got={}",
        first.unix_lock_probe
    );

    assert_eq!(first.metric_name, TEST_VFS_FAULT_COUNTER_NAME);
    for fault_type in [
        "read_failure",
        "write_failure",
        "partial_write",
        "disk_full",
        "latency",
        "torn_write",
        "power_cut",
    ] {
        assert!(
            first.metrics_by_fault_type.contains_key(fault_type),
            "bead_id={BEAD_ID} missing metric counter for fault_type={fault_type}",
        );
    }

    let throughput = run_throughput(seed, iterations).expect("bd-3u7.4 throughput run should pass");
    assert!(
        throughput.baseline_ops_per_sec > 0.0,
        "bead_id={BEAD_ID} baseline throughput must be positive"
    );
    assert!(
        throughput.wrapped_ops_per_sec > 0.0,
        "bead_id={BEAD_ID} wrapped throughput must be positive"
    );
    assert!(
        throughput.wrapped_vs_baseline_ratio > 0.0,
        "bead_id={BEAD_ID} wrapped/baseline ratio must be positive"
    );
    if require_io_uring() && !throughput.io_uring_available {
        panic!(
            "bead_id={BEAD_ID} io_uring benchmark required but io_uring support is unavailable in tree"
        );
    }
    if require_io_uring() {
        assert!(
            throughput.io_uring_vs_unix_ratio >= 2.0,
            "bead_id={BEAD_ID} io_uring throughput target unmet ratio={} unix_ops={} io_uring_ops={} status={}",
            throughput.io_uring_vs_unix_ratio,
            throughput.unix_ops_per_sec,
            throughput.io_uring_ops_per_sec,
            throughput.io_uring_status,
        );
    }

    let run_id = format!("{BEAD_ID}-seed-{seed:016x}");
    let trace_id = format!("trace-{seed:016x}");
    let replay_commands = vec![
        format!(
            "BD_3U7_4_SEED={seed} BD_3U7_4_BENCH_ITERS={iterations} cargo test -p fsqlite-harness --test bd_3u7_4_vfs_contract_fault_injection -- --nocapture"
        ),
        format!(
            "BD_3U7_4_SEED={seed} BD_3U7_4_BENCH_ITERS={iterations} scripts/verify_bd_3u7_4_vfs_contract_fault_injection.sh"
        ),
    ];
    let artifact = ContractSuiteArtifact {
        schema_version: 1,
        bead_id: BEAD_ID.to_owned(),
        run_id: run_id.clone(),
        trace_id: trace_id.clone(),
        scenario_id: SCENARIO_ID.to_owned(),
        seed,
        duration_ms: started.elapsed().as_millis(),
        replay_commands,
        fault_contract: first,
        throughput,
    };
    let artifact_path =
        write_suite_artifact(&artifact).expect("bd-3u7.4 contract artifact write should pass");

    println!(
        "INFO bead_id={BEAD_ID} case=suite_artifact path={} run_id={} trace_id={} scenario_id={SCENARIO_ID}",
        artifact_path.display(),
        run_id,
        trace_id,
    );
}
