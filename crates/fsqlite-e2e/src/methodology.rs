//! Benchmark methodology: fairness, repeatability, and statistical rigor.
//!
//! This module defines the canonical benchmark methodology that governs all
//! performance comparisons between FrankenSQLite and C SQLite.  Every
//! benchmark report embeds a [`MethodologyMeta`] record so that readers can
//! verify exactly how the numbers were produced.
//!
//! ## Principles
//!
//! 1. **Warmup before measurement.**  The first `WARMUP_ITERATIONS` iterations
//!    of every benchmark are discarded.  This eliminates cold-cache, JIT, and
//!    first-allocation effects that would skew results.
//!
//! 2. **Fixed iteration count.**  After warmup, every benchmark executes
//!    exactly `MIN_MEASUREMENT_ITERATIONS` timed iterations (more if Criterion
//!    requests additional samples for statistical confidence).  Using a fixed
//!    floor prevents wall-clock-based runs from producing fewer samples on
//!    slower hardware.
//!
//! 3. **Median and p95 as primary statistics.**  The median is the fairest
//!    central-tendency measure for benchmarks because it is robust to outliers
//!    caused by OS scheduling jitter, GC pauses in the test harness, or
//!    background I/O.  The 95th-percentile captures tail latency.
//!
//! 4. **Environment capture.**  Every report records CPU model, core count, OS,
//!    architecture, available RAM, disk type (if detectable), and the exact
//!    `rustc` version.  Without this context, numbers are not reproducible.
//!
//! 5. **Identical PRAGMA configuration.**  Both engines run with identical
//!    PRAGMA settings.  See [`crate::HarnessSettings`] and the separate
//!    fairness module (bd-3qeq) for the canonical PRAGMA list.
//!
//! 6. **Fresh database per iteration.**  Each benchmark iteration starts from
//!    a clean copy of the golden database (or a freshly-created in-memory DB).
//!    No state leaks between iterations.

use serde::{Deserialize, Serialize};

/// Number of warmup iterations discarded before measurement begins.
pub const WARMUP_ITERATIONS: u32 = 3;

/// Minimum number of timed measurement iterations per benchmark.
///
/// Criterion may run more samples if it needs additional data points for
/// confidence intervals, but it will never run fewer than this.
pub const MIN_MEASUREMENT_ITERATIONS: u32 = 20;

/// Default measurement time target in seconds.
///
/// Criterion uses this as a floor: it keeps sampling until at least this much
/// wall-clock time has elapsed *and* `MIN_MEASUREMENT_ITERATIONS` samples
/// have been collected.
pub const MEASUREMENT_TIME_SECS: u64 = 10;

/// Methodology metadata embedded in every benchmark report.
///
/// This record is serialized into the report JSON so that consumers can
/// verify exactly how the numbers were produced without reading source code.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MethodologyMeta {
    /// Human-readable methodology version for schema evolution.
    pub version: String,
    /// Number of warmup iterations discarded before measurement.
    pub warmup_iterations: u32,
    /// Minimum measurement iterations per benchmark.
    pub min_measurement_iterations: u32,
    /// Measurement time floor in seconds.
    pub measurement_time_secs: u64,
    /// Primary statistic reported for central tendency.
    pub primary_statistic: String,
    /// Tail-latency statistic reported.
    pub tail_statistic: String,
    /// Whether each iteration starts from a fresh database copy.
    pub fresh_db_per_iteration: bool,
    /// Whether identical PRAGMAs are enforced on both engines.
    pub identical_pragmas_enforced: bool,
}

impl Default for MethodologyMeta {
    fn default() -> Self {
        Self::current()
    }
}

impl MethodologyMeta {
    /// Returns the current canonical methodology metadata.
    #[must_use]
    pub fn current() -> Self {
        Self {
            version: "fsqlite-e2e.methodology.v1".to_owned(),
            warmup_iterations: WARMUP_ITERATIONS,
            min_measurement_iterations: MIN_MEASUREMENT_ITERATIONS,
            measurement_time_secs: MEASUREMENT_TIME_SECS,
            primary_statistic: "median".to_owned(),
            tail_statistic: "p95".to_owned(),
            fresh_db_per_iteration: true,
            identical_pragmas_enforced: true,
        }
    }
}

/// Environment metadata captured at benchmark time for reproducibility.
///
/// This goes beyond [`crate::report::HostInfo`] with benchmark-specific
/// fields like the Rust toolchain version and disk type.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "snake_case")]
pub enum EnvironmentCaptureMode {
    #[default]
    Captured,
    Suppressed,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EnvironmentMeta {
    /// Whether the environment was fully probed or intentionally suppressed.
    ///
    /// Profile-only runs keep the record shape stable while avoiding host
    /// probing so profilers mostly observe engine work.
    #[serde(default)]
    pub capture_mode: EnvironmentCaptureMode,
    /// OS name and version (e.g. "Linux 6.17.0-12-generic").
    pub os: String,
    /// CPU architecture (e.g. "x86_64", "aarch64").
    pub arch: String,
    /// Number of logical CPU cores.
    pub cpu_count: usize,
    /// CPU model string if available (e.g. from `/proc/cpuinfo`).
    pub cpu_model: Option<String>,
    /// Total RAM in bytes, if detectable.
    pub ram_bytes: Option<u64>,
    /// `rustc --version` output.
    pub rustc_version: String,
    /// Cargo profile used (e.g. "release", "release-perf").
    pub cargo_profile: String,
}

impl EnvironmentMeta {
    /// Capture the current environment.
    ///
    /// Best-effort: fields that cannot be detected are left as `None` or
    /// populated with a placeholder string.
    #[must_use]
    pub fn capture(cargo_profile: &str) -> Self {
        Self {
            capture_mode: EnvironmentCaptureMode::Captured,
            os: detect_os(),
            arch: std::env::consts::ARCH.to_owned(),
            cpu_count: std::thread::available_parallelism().map_or(1, std::num::NonZero::get),
            cpu_model: detect_cpu_model(),
            ram_bytes: detect_ram_bytes(),
            rustc_version: detect_rustc_version(),
            cargo_profile: cargo_profile.to_owned(),
        }
    }

    /// Construct an explicit "suppressed" environment without probing the host.
    #[must_use]
    pub fn suppressed(cargo_profile: &str) -> Self {
        Self {
            capture_mode: EnvironmentCaptureMode::Suppressed,
            os: "suppressed".to_owned(),
            arch: "suppressed".to_owned(),
            cpu_count: 0,
            cpu_model: None,
            ram_bytes: None,
            rustc_version: "suppressed".to_owned(),
            cargo_profile: cargo_profile.to_owned(),
        }
    }
}

fn detect_os() -> String {
    let os = std::env::consts::OS;
    // Try to get kernel version on Linux.
    #[cfg(target_os = "linux")]
    {
        if let Ok(uname) = std::fs::read_to_string("/proc/version") {
            if let Some(first_line) = uname.lines().next() {
                // Extract "Linux X.Y.Z-..." from the proc version string.
                let parts: Vec<&str> = first_line.split_whitespace().collect();
                if parts.len() >= 3 {
                    return format!("{} {}", parts[0], parts[2]);
                }
            }
        }
    }
    os.to_owned()
}

fn detect_cpu_model() -> Option<String> {
    #[cfg(target_os = "linux")]
    {
        if let Ok(cpuinfo) = std::fs::read_to_string("/proc/cpuinfo") {
            for line in cpuinfo.lines() {
                if line.starts_with("model name") {
                    if let Some((_key, val)) = line.split_once(':') {
                        return Some(val.trim().to_owned());
                    }
                }
            }
        }
    }
    None
}

fn detect_ram_bytes() -> Option<u64> {
    #[cfg(target_os = "linux")]
    {
        if let Ok(meminfo) = std::fs::read_to_string("/proc/meminfo") {
            for line in meminfo.lines() {
                if line.starts_with("MemTotal:") {
                    // Format: "MemTotal:       32717852 kB"
                    let parts: Vec<&str> = line.split_whitespace().collect();
                    if parts.len() >= 2 {
                        if let Ok(kb) = parts[1].parse::<u64>() {
                            return Some(kb * 1024);
                        }
                    }
                }
            }
        }
    }
    None
}

fn detect_rustc_version() -> String {
    std::process::Command::new("rustc")
        .arg("--version")
        .output()
        .ok()
        .and_then(|out| String::from_utf8(out.stdout).ok())
        .map_or_else(|| "unknown".to_owned(), |s| s.trim().to_owned())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn methodology_meta_default_matches_current() {
        let default = MethodologyMeta::default();
        let current = MethodologyMeta::current();
        assert_eq!(default.version, current.version);
        assert_eq!(default.warmup_iterations, current.warmup_iterations);
        assert_eq!(
            default.min_measurement_iterations,
            current.min_measurement_iterations
        );
        assert_eq!(default.primary_statistic, "median");
        assert_eq!(default.tail_statistic, "p95");
    }

    #[test]
    fn methodology_meta_serialization_roundtrip() {
        let meta = MethodologyMeta::current();
        let json = serde_json::to_string(&meta).unwrap();
        let parsed: serde_json::Value = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed["version"], "fsqlite-e2e.methodology.v1");
        assert_eq!(parsed["warmup_iterations"], 3);
        assert_eq!(parsed["min_measurement_iterations"], 20);
        assert_eq!(parsed["measurement_time_secs"], 10);
        assert_eq!(parsed["primary_statistic"], "median");
        assert_eq!(parsed["tail_statistic"], "p95");
        assert_eq!(parsed["fresh_db_per_iteration"], true);
        assert_eq!(parsed["identical_pragmas_enforced"], true);
    }

    #[test]
    fn environment_meta_capture_produces_sane_values() {
        let env = EnvironmentMeta::capture("release");
        assert_eq!(env.capture_mode, EnvironmentCaptureMode::Captured);
        assert!(!env.os.is_empty());
        assert!(!env.arch.is_empty());
        assert!(env.cpu_count >= 1);
        assert!(!env.rustc_version.is_empty());
        assert_eq!(env.cargo_profile, "release");
    }

    #[test]
    fn environment_meta_serialization_roundtrip() {
        let env = EnvironmentMeta::capture("release-perf");
        let json = serde_json::to_string(&env).unwrap();
        let parsed: EnvironmentMeta = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed.capture_mode, EnvironmentCaptureMode::Captured);
        assert_eq!(parsed.arch, env.arch);
        assert_eq!(parsed.cpu_count, env.cpu_count);
        assert_eq!(parsed.cargo_profile, "release-perf");
    }

    #[test]
    fn environment_meta_suppressed_roundtrip_marks_mode_and_placeholders() {
        let env = EnvironmentMeta::suppressed("release");
        let json = serde_json::to_string(&env).unwrap();
        let parsed: EnvironmentMeta = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed.capture_mode, EnvironmentCaptureMode::Suppressed);
        assert_eq!(parsed.os, "suppressed");
        assert_eq!(parsed.arch, "suppressed");
        assert_eq!(parsed.cpu_count, 0);
        assert_eq!(parsed.rustc_version, "suppressed");
        assert_eq!(parsed.cargo_profile, "release");
    }

    #[test]
    fn environment_meta_legacy_json_defaults_to_captured_mode() {
        let parsed: EnvironmentMeta = serde_json::from_str(
            r#"{
                "os": "Linux",
                "arch": "x86_64",
                "cpu_count": 8,
                "cpu_model": null,
                "ram_bytes": null,
                "rustc_version": "rustc 1.91.0-nightly",
                "cargo_profile": "release"
            }"#,
        )
        .unwrap();
        assert_eq!(parsed.capture_mode, EnvironmentCaptureMode::Captured);
        assert_eq!(parsed.arch, "x86_64");
        assert_eq!(parsed.cpu_count, 8);
    }

    #[test]
    fn constants_are_reasonable() {
        const { assert!(WARMUP_ITERATIONS >= 1, "at least 1 warmup iteration") };
        const {
            assert!(
                MIN_MEASUREMENT_ITERATIONS >= 10,
                "need enough samples for statistics"
            );
        };
        const {
            assert!(
                MEASUREMENT_TIME_SECS >= 5,
                "need enough time for stable results"
            );
        };
    }
}
