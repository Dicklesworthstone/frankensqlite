//! Rolling per-workspace JSONL logs and their rotation policy.
//!
//! One JSON line is appended per finding and one per probe tick to
//! `<state_dir>/<workspace>.jsonl`. The active file is rotated when it exceeds
//! [`MAX_LOG_BYTES`] **or** its last-modified time is older than [`MAX_LOG_AGE`].
//!
//! ## Clock split (honest note)
//!
//! `asupersync::time::wall_now()` is a process-monotonic clock (nanoseconds
//! since the process epoch), used for every log-line / notification
//! `timestamp` per the asupersync-native convention. It is deliberately **not**
//! a calendar clock, so it cannot decide whether a file on disk is "older than
//! 30 days" across process restarts. Age-based rotation therefore needs a
//! calendar instant, which asupersync does not vend; [`Clock::wall_clock`]
//! supplies it (the production impl uses `SystemTime::now`, quarantined to this
//! one filesystem-age comparison). Both clock reads are injected behind the
//! [`Clock`] trait, and the rotation *decision* is a pure function
//! ([`rotation_reason`]) that touches no clock at all — so rotation is fully
//! unit-testable with synthetic times.

use std::fs::{self, OpenOptions};
use std::io::Write as _;
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime};

use asupersync::Time;

/// Rotate the active log once it grows beyond 50 MiB.
pub const MAX_LOG_BYTES: u64 = 50 * 1024 * 1024;

/// Rotate the active log once it is older than 30 days.
// The value is exactly 720 hours; `from_secs` keeps the "30 days" intent legible.
#[allow(clippy::duration_suboptimal_units)]
pub const MAX_LOG_AGE: Duration = Duration::from_secs(30 * 24 * 60 * 60);

/// Source of "now" for timestamps and for file-age rotation.
///
/// Injecting this makes every time-dependent path testable and keeps the one
/// unavoidable `SystemTime::now` call isolated to the production impl.
pub trait Clock: Send + Sync {
    /// Monotonic timestamp for log lines and notification payloads
    /// (`asupersync::time::wall_now()`).
    fn now(&self) -> Time;
    /// Calendar instant used only for file-age rotation comparisons.
    fn wall_clock(&self) -> SystemTime;
}

/// Production clock: `wall_now()` for timestamps, `SystemTime::now()` for
/// file-age rotation.
#[derive(Debug, Default, Clone, Copy)]
pub struct SystemClock;

impl Clock for SystemClock {
    fn now(&self) -> Time {
        asupersync::time::wall_now()
    }

    fn wall_clock(&self) -> SystemTime {
        // Sole `SystemTime::now` in the crate: age-based log rotation compares a
        // file's mtime against a calendar instant, which the monotonic
        // `wall_now()` cannot express. See the module docs.
        SystemTime::now()
    }
}

/// A fully-injected clock for tests and consumers that want deterministic time.
#[derive(Debug, Clone, Copy)]
pub struct ManualClock {
    monotonic: Time,
    wall: SystemTime,
}

impl ManualClock {
    /// Creates a clock returning fixed values.
    #[must_use]
    pub const fn new(monotonic: Time, wall: SystemTime) -> Self {
        Self { monotonic, wall }
    }
}

impl Clock for ManualClock {
    fn now(&self) -> Time {
        self.monotonic
    }

    fn wall_clock(&self) -> SystemTime {
        self.wall
    }
}

/// Why a log file was rotated.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RotationReason {
    /// The file exceeded [`MAX_LOG_BYTES`].
    Size,
    /// The file was older than [`MAX_LOG_AGE`].
    Age,
}

/// Pure rotation decision: size takes precedence over age when both trip.
///
/// Returns `None` when the file should be kept as-is.
#[must_use]
pub fn rotation_reason(
    size_bytes: u64,
    max_size: u64,
    age: Duration,
    max_age: Duration,
) -> Option<RotationReason> {
    if size_bytes > max_size {
        Some(RotationReason::Size)
    } else if age > max_age {
        Some(RotationReason::Age)
    } else {
        None
    }
}

/// The active JSONL log path for a workspace under `state_dir`.
#[must_use]
pub fn log_path(state_dir: &Path, workspace: &str) -> PathBuf {
    state_dir.join(format!("{workspace}.jsonl"))
}

/// Rotates the active log for `path` if the size/age policy trips.
///
/// The rotated file is renamed to `<path>.<monotonic-nanos>` so the sidecar
/// name is unique and sortable. Returns the reason when a rotation happened.
///
/// # Errors
///
/// Propagates filesystem errors other than "file does not exist" (a missing
/// active file simply means nothing to rotate).
pub fn maybe_rotate(
    path: &Path,
    clock: &dyn Clock,
    max_size: u64,
    max_age: Duration,
) -> std::io::Result<Option<RotationReason>> {
    let metadata = match fs::metadata(path) {
        Ok(metadata) => metadata,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(err) => return Err(err),
    };

    let size = metadata.len();
    let age = metadata
        .modified()
        .ok()
        .and_then(|mtime| clock.wall_clock().duration_since(mtime).ok())
        .unwrap_or_default();

    let Some(reason) = rotation_reason(size, max_size, age, max_age) else {
        return Ok(None);
    };

    let stamp = clock.now().as_nanos();
    let mut rotated = path.as_os_str().to_os_string();
    rotated.push(format!(".{stamp}"));
    fs::rename(path, PathBuf::from(rotated))?;
    Ok(Some(reason))
}

/// Appends a single JSONL line for `workspace`, rotating first if needed.
///
/// Creates `state_dir` and the active file as required. `line` must already be
/// a serialized JSON object with no trailing newline.
///
/// # Errors
///
/// Propagates any filesystem error from directory creation, rotation, or the
/// append itself.
pub fn append_line(
    state_dir: &Path,
    workspace: &str,
    line: &str,
    clock: &dyn Clock,
    max_size: u64,
    max_age: Duration,
) -> std::io::Result<()> {
    fs::create_dir_all(state_dir)?;
    let path = log_path(state_dir, workspace);
    maybe_rotate(&path, clock, max_size, max_age)?;
    let mut file = OpenOptions::new().create(true).append(true).open(&path)?;
    file.write_all(line.as_bytes())?;
    file.write_all(b"\n")?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rotation_reason_prefers_size_then_age() {
        assert_eq!(
            rotation_reason(100, 50, Duration::ZERO, Duration::from_secs(10)),
            Some(RotationReason::Size)
        );
        assert_eq!(
            rotation_reason(10, 50, Duration::from_secs(20), Duration::from_secs(10)),
            Some(RotationReason::Age)
        );
        assert_eq!(
            rotation_reason(10, 50, Duration::from_secs(5), Duration::from_secs(10)),
            None
        );
        // Exactly at the limit does not rotate (strictly greater).
        assert_eq!(
            rotation_reason(50, 50, Duration::from_secs(10), Duration::from_secs(10)),
            None
        );
    }

    #[test]
    fn size_based_rotation_in_tempdir() {
        // Acceptance 5: write >50 MB, rotate, assert the active file is small.
        let dir = tempfile::tempdir().expect("tempdir");
        let state = dir.path();
        let ws = "big";
        let path = log_path(state, ws);

        // Write ~51 MiB in one line so the file exceeds MAX_LOG_BYTES.
        {
            let mut f = OpenOptions::new()
                .create(true)
                .append(true)
                .open(&path)
                .expect("create");
            let chunk = vec![b'x'; 1024 * 1024];
            for _ in 0..51 {
                f.write_all(&chunk).expect("write");
            }
        }
        assert!(fs::metadata(&path).expect("meta").len() > MAX_LOG_BYTES);

        // Clock whose wall time equals the file's mtime, so age never trips.
        let mtime = fs::metadata(&path).expect("meta").modified().expect("mtime");
        let clock = ManualClock::new(Time::from_nanos(1), mtime);

        let reason = maybe_rotate(&path, &clock, MAX_LOG_BYTES, MAX_LOG_AGE).expect("rotate");
        assert_eq!(reason, Some(RotationReason::Size));
        // The active path was renamed away; the sidecar holds the bulk.
        assert!(fs::metadata(&path).is_err());

        // A fresh append recreates a small active file.
        append_line(state, ws, "{\"kind\":\"tick\"}", &clock, MAX_LOG_BYTES, MAX_LOG_AGE)
            .expect("append");
        assert!(fs::metadata(&path).expect("meta").len() <= MAX_LOG_BYTES);
    }

    #[test]
    fn age_based_rotation_with_injected_clock() {
        // Acceptance 5: age rotation driven by an injected clock in a tempdir.
        let dir = tempfile::tempdir().expect("tempdir");
        let state = dir.path();
        let ws = "old";
        let path = log_path(state, ws);
        fs::write(&path, b"{\"kind\":\"tick\"}\n").expect("seed");

        let mtime = fs::metadata(&path).expect("meta").modified().expect("mtime");

        // Clock 31 days ahead of the file: age exceeds MAX_LOG_AGE -> rotate.
        #[allow(clippy::duration_suboptimal_units)]
        let thirty_one_days = Duration::from_secs(31 * 86_400);
        let far = ManualClock::new(Time::from_nanos(9), mtime + thirty_one_days);
        let reason = maybe_rotate(&path, &far, MAX_LOG_BYTES, MAX_LOG_AGE).expect("rotate");
        assert_eq!(reason, Some(RotationReason::Age));
        assert!(fs::metadata(&path).is_err());

        // Recreate, then a near clock does not rotate.
        fs::write(&path, b"{\"kind\":\"tick\"}\n").expect("reseed");
        let mtime2 = fs::metadata(&path).expect("meta").modified().expect("mtime");
        let near = ManualClock::new(Time::from_nanos(10), mtime2 + Duration::from_secs(1));
        let reason = maybe_rotate(&path, &near, MAX_LOG_BYTES, MAX_LOG_AGE).expect("no rotate");
        assert_eq!(reason, None);
        assert!(fs::metadata(&path).is_ok());
    }

    #[test]
    fn append_creates_dir_and_writes_lines() {
        let dir = tempfile::tempdir().expect("tempdir");
        let state = dir.path().join("nested").join("state");
        let clock = ManualClock::new(Time::from_nanos(1), SystemTime::UNIX_EPOCH);
        append_line(&state, "w", "{\"a\":1}", &clock, MAX_LOG_BYTES, MAX_LOG_AGE).expect("1");
        append_line(&state, "w", "{\"a\":2}", &clock, MAX_LOG_BYTES, MAX_LOG_AGE).expect("2");
        let content = fs::read_to_string(log_path(&state, "w")).expect("read");
        let lines: Vec<&str> = content.lines().collect();
        assert_eq!(lines.len(), 2);
        assert_eq!(lines[0], "{\"a\":1}");
        assert_eq!(lines[1], "{\"a\":2}");
    }
}
