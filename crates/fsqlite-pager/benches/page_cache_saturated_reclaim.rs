//! GH #326 saturated page-cache reclaim-cycle benchmark.
//!
//! The measured region contains only one or more `reclaim + exact-page
//! reinsertion` cycles using either the exact pre-GH-326 snapshot-and-sort
//! algorithm or the candidate cursor algorithm. Fixture construction,
//! invariant checks, provenance, hashing, serialization, stdout, and
//! filesystem writes stay outside the duration returned to Criterion.

use std::env;
use std::fs::{self, File, OpenOptions};
use std::hint::black_box;
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::Instant;

use criterion::{BenchmarkId, Criterion};
use fsqlite_pager::page_cache::ShardedPageCache;
use fsqlite_types::{PageNumber, PageSize};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};

const RESIDENT_POINTS: [usize; 3] = [64, 1_024, 8_192];
const SHARD_COUNT: usize = 4;
const BENCH_ARM: &str = match option_env!("FSQLITE_GH326_ARM") {
    Some(value) => value,
    None => "unconfigured",
};
const ENGINE_REVISION: &str = match option_env!("FSQLITE_GH326_ENGINE_REVISION") {
    Some(value) => value,
    None => "unconfigured",
};
const HARNESS_SHA256: &str = match option_env!("FSQLITE_GH326_HARNESS_SHA256") {
    Some(value) => value,
    None => "unconfigured",
};
const BUILD_NONCE: &str = match option_env!("FSQLITE_BENCH_BUILD_NONCE") {
    Some(value) => value,
    None => "unconfigured",
};
const PROFILE_NAME: &str = match option_env!("FSQLITE_BENCH_PROFILE_NAME") {
    Some(value) => value,
    None => "unconfigured",
};
const HARNESS_SOURCE: &[u8] = include_bytes!("page_cache_saturated_reclaim.rs");

fn required_env(name: &str) -> String {
    env::var(name).unwrap_or_else(|_| panic!("{name} must be set for a provenance-complete run"))
}

fn assert_lower_hex(value: &str, expected_len: usize, name: &str) {
    assert_eq!(
        value.len(),
        expected_len,
        "{name} must be {expected_len} hex digits"
    );
    assert!(
        value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte)),
        "{name} must contain lowercase hexadecimal digits only"
    );
}

fn command_output(program: &str, args: &[&str]) -> String {
    Command::new(program)
        .args(args)
        .output()
        .ok()
        .filter(|output| output.status.success())
        .map_or_else(
            || "unavailable".to_owned(),
            |output| String::from_utf8_lossy(&output.stdout).trim().to_owned(),
        )
}

fn cpu_model() -> String {
    fs::read_to_string("/proc/cpuinfo")
        .ok()
        .and_then(|contents| {
            contents.lines().find_map(|line| {
                let (key, value) = line.split_once(':')?;
                (key.trim() == "model name").then(|| value.trim().to_owned())
            })
        })
        .unwrap_or_else(|| "unavailable".to_owned())
}

fn sha256_file(path: &Path) -> (String, u64) {
    let mut file = File::open(path).expect("open benchmark executable for identity hashing");
    let byte_len = file.metadata().expect("stat benchmark executable").len();
    let mut hasher = Sha256::new();
    let mut buffer = [0_u8; 64 * 1_024];
    loop {
        let read = file
            .read(&mut buffer)
            .expect("read benchmark executable for identity hashing");
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
    }
    (format!("{:x}", hasher.finalize()), byte_len)
}

fn sha256_bytes(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}

fn write_new_json(path: &Path, record: &Value) {
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(path)
        .unwrap_or_else(|error| panic!("create fresh {}: {error}", path.display()));
    serde_json::to_writer_pretty(&mut file, record)
        .unwrap_or_else(|error| panic!("write {}: {error}", path.display()));
    writeln!(file).unwrap_or_else(|error| panic!("finish {}: {error}", path.display()));
}

fn saturated_clean_flat_fixture(resident_count: usize) -> ShardedPageCache {
    let cache = ShardedPageCache::with_max_buffers_and_shards(
        PageSize::DEFAULT,
        resident_count,
        SHARD_COUNT,
    );
    for raw_page_no in 1..=resident_count {
        let raw_page_no = u32::try_from(raw_page_no).expect("resident page number must fit in u32");
        let page_no = PageNumber::new(raw_page_no).expect("resident page number must be nonzero");
        let buffer = cache
            .pool()
            .acquire()
            .expect("fixture must fit exactly within the configured pool");
        cache.insert_buffer(page_no, buffer);
    }
    assert_eq!(cache.len(), resident_count);
    assert_eq!(cache.pool().available(), 0);
    assert_eq!(cache.overflow_resident_count_for_bench(), 0);
    cache
}

fn bench_saturated_reclaim(criterion: &mut Criterion) {
    let mut group = criterion.benchmark_group("page_cache_saturated_flat_reclaim");
    for resident_count in RESIDENT_POINTS {
        group.bench_with_input(
            BenchmarkId::new("resident_pages", resident_count),
            &resident_count,
            move |bencher, &resident_count| {
                bencher.iter_custom(|iters| {
                    let cache = saturated_clean_flat_fixture(resident_count);

                    let run_started = Instant::now();
                    let mut receipt = 0_u64;
                    for _ in 0..iters {
                        let reclaimed = if BENCH_ARM == "baseline" {
                            cache.take_clean_buffer_entry_legacy_for_bench()
                        } else {
                            cache.take_clean_buffer_entry_for_bench()
                        };
                        let (page_no, buffer) = reclaimed
                            .expect("saturated clean cache must expose a reclaimable buffer");
                        receipt ^= u64::from(page_no.get());
                        cache.insert_buffer(page_no, buffer);
                    }
                    let run_wall = run_started.elapsed();

                    assert_eq!(cache.len(), resident_count);
                    assert_eq!(cache.pool().available(), 0);
                    assert_eq!(cache.overflow_resident_count_for_bench(), 0);
                    let _ = black_box(receipt);
                    run_wall
                });
            },
        );
    }
    group.finish();
}

fn emit_official_samples(output_dir: &Path, receipt_path: &Path) {
    let mut pending = vec![output_dir.to_path_buf()];
    let mut sample_paths = Vec::new();
    while let Some(path) = pending.pop() {
        let entries = fs::read_dir(&path)
            .unwrap_or_else(|error| panic!("read Criterion output {}: {error}", path.display()));
        for entry in entries {
            let entry = entry
                .unwrap_or_else(|error| panic!("read entry below {}: {error}", path.display()));
            let entry_path = entry.path();
            if entry_path.is_dir() {
                pending.push(entry_path);
                continue;
            }
            let is_new_sample = entry_path.file_name().and_then(|name| name.to_str())
                == Some("sample.json")
                && entry_path
                    .parent()
                    .and_then(Path::file_name)
                    .and_then(|name| name.to_str())
                    == Some("new");
            if !is_new_sample {
                continue;
            }
            sample_paths.push(entry_path);
        }
    }
    sample_paths.sort();
    assert_eq!(
        sample_paths.len(),
        RESIDENT_POINTS.len(),
        "fresh Criterion output must contain one new/sample.json for each resident point"
    );

    let mut records = Vec::with_capacity(sample_paths.len());
    for entry_path in sample_paths {
        let contents = fs::read_to_string(&entry_path)
            .unwrap_or_else(|error| panic!("read {}: {error}", entry_path.display()));
        let sample: Value = serde_json::from_str(&contents)
            .unwrap_or_else(|error| panic!("parse {}: {error}", entry_path.display()));
        let iters = sample
            .get("iters")
            .and_then(Value::as_array)
            .unwrap_or_else(|| panic!("{} must contain an iters array", entry_path.display()));
        let times = sample
            .get("times")
            .and_then(Value::as_array)
            .unwrap_or_else(|| panic!("{} must contain a times array", entry_path.display()));
        assert!(
            !iters.is_empty(),
            "{} must contain measured samples",
            entry_path.display()
        );
        assert_eq!(
            iters.len(),
            times.len(),
            "{} iteration and time arrays must align",
            entry_path.display()
        );
        records.push(json!({
            "schema_version": 1,
            "record_type": "criterion_official_sample",
            "path": entry_path,
            "sample": sample
        }));
    }

    let mut receipt_file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(receipt_path)
        .unwrap_or_else(|error| panic!("create {}: {error}", receipt_path.display()));
    for record in records {
        let encoded = serde_json::to_string(&record).expect("serialize official sample record");
        println!("{encoded}");
        writeln!(receipt_file, "{encoded}").expect("append official sample record");
    }
    receipt_file.flush().expect("flush official sample records");
}

fn main() {
    let executable = env::current_exe().expect("resolve benchmark executable path");
    let (executable_sha256, executable_bytes) = sha256_file(&executable);
    let binary_identity = json!({
        "schema_version": 1,
        "record_type": "benchmark_binary_identity",
        "executable_path": executable,
        "executable_sha256": executable_sha256,
        "executable_bytes": executable_bytes
    });
    println!(
        "{}",
        serde_json::to_string(&binary_identity).expect("serialize binary identity")
    );

    assert!(
        BENCH_ARM == "baseline" || BENCH_ARM == "candidate",
        "FSQLITE_GH326_ARM must be baseline or candidate"
    );
    let implementation = if BENCH_ARM == "baseline" {
        "pre_gh326_snapshot_sort_reconstructed_at_0696131c"
    } else {
        "candidate_persistent_cursor_reclaim"
    };
    let run_label = required_env("FSQLITE_GH326_RUN_LABEL");
    assert_lower_hex(ENGINE_REVISION, 40, "FSQLITE_GH326_ENGINE_REVISION");
    assert_lower_hex(HARNESS_SHA256, 64, "FSQLITE_GH326_HARNESS_SHA256");
    let computed_harness_sha256 = sha256_bytes(HARNESS_SOURCE);
    assert_eq!(
        computed_harness_sha256, HARNESS_SHA256,
        "FSQLITE_GH326_HARNESS_SHA256 must identify the compiled benchmark source"
    );
    assert_lower_hex(BUILD_NONCE, 64, "FSQLITE_BENCH_BUILD_NONCE");
    assert_eq!(
        PROFILE_NAME, "release-perf",
        "GH #326 evidence must use release-perf"
    );
    let capture_dir = PathBuf::from(required_env("FSQLITE_GH326_CAPTURE_DIR"));
    if let Some(parent) = capture_dir.parent() {
        fs::create_dir_all(parent)
            .unwrap_or_else(|error| panic!("create capture parent {}: {error}", parent.display()));
    }
    fs::create_dir(&capture_dir).unwrap_or_else(|error| {
        panic!(
            "capture directory {} must be fresh: {error}",
            capture_dir.display()
        )
    });

    let criterion_output = capture_dir.join("criterion");
    let official_sample_path = capture_dir.join("criterion_samples.jsonl");

    let provenance = json!({
        "schema_version": 1,
        "record_type": "benchmark_provenance",
        "benchmark": "page_cache_saturated_flat_reclaim",
        "measurement_scope": "flat_tier_saturated_clean_reclaim_plus_exact_page_reinsert",
        "arm": BENCH_ARM,
        "implementation": implementation,
        "run_label": run_label,
        "engine_revision": ENGINE_REVISION,
        "harness_sha256": HARNESS_SHA256,
        "computed_embedded_harness_sha256": computed_harness_sha256,
        "build_nonce": BUILD_NONCE,
        "profile": PROFILE_NAME,
        "hostname": command_output("hostname", &[]),
        "kernel": command_output("uname", &["-srmo"]),
        "cpu_model": cpu_model(),
        "logical_cpus": std::thread::available_parallelism().map_or(0, std::num::NonZero::get),
        "argv": env::args().collect::<Vec<_>>(),
        "rustflags": env::var("RUSTFLAGS").unwrap_or_default(),
        "page_size_bytes": PageSize::DEFAULT.get(),
        "shard_count": SHARD_COUNT,
        "resident_points": RESIDENT_POINTS,
        "criterion_output_directory": criterion_output,
        "capture_directory": capture_dir,
        "source_binding_note": "The embedded harness hash is verified in-process; the outer RCH clean-overlay receipt must bind engine_revision to the exact source tree and overlay blobs",
        "binary": binary_identity
    });
    println!(
        "{}",
        serde_json::to_string(&provenance).expect("serialize benchmark provenance")
    );
    write_new_json(&capture_dir.join("provenance.json"), &provenance);

    let mut criterion = Criterion::default()
        .output_directory(&criterion_output)
        .configure_from_args();
    bench_saturated_reclaim(&mut criterion);
    criterion.final_summary();
    emit_official_samples(&criterion_output, &official_sample_path);
}
