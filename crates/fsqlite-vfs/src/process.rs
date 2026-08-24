//! Cross-platform process-liveness probe for crash cleanup (bd-4dr7g).
//!
//! `fsqlite-vfs` is the designated FFI boundary (it already performs `libc` /
//! Win32 calls and does not `forbid(unsafe_code)`), so the OS-specific liveness
//! probes live here. `fsqlite-mvcc` stays `unsafe`-free by consuming these
//! through its existing function-pointer injection into
//! `check_serialized_writer_exclusion`.
//!
//! A liveness answer is `Alive`, `Dead`, or `Unknown`. Callers treat `Unknown`
//! (an ambiguous OS error such as `EACCES`/`ERROR_ACCESS_DENIED`) as alive:
//! never reclaim a possibly-live writer's lease on the strength of a probe we
//! could not complete. That matches the pre-existing "return true" stubs this
//! replaces on macOS and Windows.
//!
//! ## PID-reuse safety
//!
//! A raw PID can be recycled by the OS. Each probe pairs the PID with a
//! *birth token* — a reuse-safe snapshot of the process's start time — so a
//! recycled PID (same number, different start time) reads as `Dead`. The token
//! is platform-tagged in its top bits, so a token minted on one platform is
//! never mis-decoded on another (and, on Linux, keeps bit 63 for on-disk
//! compatibility with lock tables written before this change).

/// Liveness verdict for a `(pid, birth)` pair.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProcessLiveness {
    /// The process exists and its start time matches the birth token.
    Alive,
    /// The process does not exist, or the PID was recycled (start-time mismatch).
    Dead,
    /// The probe could not complete (ambiguous OS error); treat as alive.
    Unknown,
}

/// Top-bit tag for a Linux procfs start-ticks birth token (unchanged for
/// on-disk compatibility with pre-bd-4dr7g lock tables).
pub const PID_BIRTH_PROCFS_TAG: u64 = 1_u64 << 63;
/// Top-bit tag for a macOS process start-time (microseconds) birth token, read
/// via `proc_pidinfo(PROC_PIDTBSDINFO)`.
pub const PID_BIRTH_SYSCTL_TAG: u64 = 1_u64 << 62;
/// Top-bit tag for a Windows process-creation `FILETIME` (100 ns) birth token.
pub const PID_BIRTH_FILETIME_TAG: u64 = 1_u64 << 61;

/// Mask isolating the payload bits (all three platform tags cleared).
///
/// Only the macOS/Windows probes and the unit tests read this; a Linux
/// non-test build (whose procfs probe lives in `fsqlite-mvcc`) never does, so
/// gate it to avoid a `dead_code` warning under `-D warnings`.
#[cfg(any(target_os = "macos", target_os = "windows", test))]
const PAYLOAD_MASK: u64 = !(PID_BIRTH_PROCFS_TAG | PID_BIRTH_SYSCTL_TAG | PID_BIRTH_FILETIME_TAG);

/// Classification shared by the native probes after an OS call fails.
///
/// Only an error code that unambiguously means "no such process" may release
/// shared ownership. Every other error remains ambiguous and therefore maps to
/// [`ProcessLiveness::Unknown`] at the platform boundary.
#[cfg(any(target_os = "macos", target_os = "windows", test))]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ProbeFailure {
    Absent,
    Ambiguous,
}

#[cfg(any(target_os = "macos", target_os = "windows", test))]
const fn classify_probe_failure(
    error_code: Option<i64>,
    definitive_absence_code: i64,
) -> ProbeFailure {
    if matches!(error_code, Some(code) if code == definitive_absence_code) {
        ProbeFailure::Absent
    } else {
        ProbeFailure::Ambiguous
    }
}

/// The platform-tagged birth token for the current process, or `None` when the
/// OS start time is unavailable (the caller then falls back to a time token).
#[must_use]
pub fn current_process_birth_token() -> Option<u64> {
    // ubs:ignore - the "birth token" is a process start time used only to
    // distinguish a recycled PID; it is not a security token / secret / nonce.
    let pid = std::process::id();
    #[cfg(target_os = "macos")]
    {
        macos::birth_token(pid)
    }
    #[cfg(windows)]
    {
        windows_impl::birth_token(pid)
    }
    #[cfg(not(any(target_os = "macos", windows)))]
    {
        let _ = pid;
        None
    }
}

/// Probe whether `pid` (with the reuse-safe `pid_birth` token) is still the same
/// live process.
///
/// Only macOS and Windows are implemented here; every other target
/// (including Linux, whose procfs probe stays in `fsqlite-mvcc`) returns
/// `Unknown` so the caller keeps its own logic.
#[must_use]
pub fn process_alive(pid: u32, pid_birth: u64) -> ProcessLiveness {
    if pid == 0 {
        return ProcessLiveness::Dead;
    }
    #[cfg(target_os = "macos")]
    {
        macos::alive(pid, pid_birth)
    }
    #[cfg(windows)]
    {
        windows_impl::alive(pid, pid_birth)
    }
    #[cfg(not(any(target_os = "macos", windows)))]
    {
        let _ = pid_birth;
        ProcessLiveness::Unknown
    }
}

#[cfg(target_os = "macos")]
mod macos {
    use super::{
        PAYLOAD_MASK, PID_BIRTH_SYSCTL_TAG, ProbeFailure, ProcessLiveness, classify_probe_failure,
    };

    /// Outcome of reading a process's start time via `sysctl`.
    enum StartTime {
        /// Process exists; start time in microseconds since the epoch.
        Present(u64),
        /// `sysctl` reported no such process (zero-length result).
        Absent,
        /// The probe failed ambiguously.
        Error,
    }

    /// Read a process's start time (microseconds) via
    /// `proc_pidinfo(pid, PROC_PIDTBSDINFO)` -> `proc_bsdinfo.pbi_start_tv*`.
    /// (`libc` does not expose `kinfo_proc` on Darwin, so this uses libproc.)
    fn read_start_time_usec(pid: u32) -> StartTime {
        let mut info = std::mem::MaybeUninit::<libc::proc_bsdinfo>::uninit();
        let size = std::mem::size_of::<libc::proc_bsdinfo>() as libc::c_int;
        // `errno` is sticky. Clear this thread's Darwin errno slot so a zero
        // return cannot inherit an unrelated earlier `ESRCH` and falsely
        // authorize lease takeover.
        // SAFETY: `__error` returns the current thread's writable errno slot.
        unsafe { *libc::__error() = 0 };
        // SAFETY: `proc_pidinfo` writes at most `size` bytes into the uninitialized,
        // correctly sized output buffer; it returns the byte count, or <= 0 on
        // error / no such process.
        let written = unsafe {
            libc::proc_pidinfo(
                pid as libc::c_int,
                libc::PROC_PIDTBSDINFO,
                0,
                info.as_mut_ptr().cast(),
                size,
            )
        };
        // Apple libproc normalizes the underlying __proc_info -1 failure to a
        // zero return, so errno must be captured immediately even when
        // `written == 0`. A zero result alone does not prove that the PID is
        // absent.
        let error_code = (written <= 0)
            .then(|| std::io::Error::last_os_error().raw_os_error())
            .flatten()
            .map(i64::from);
        if written <= 0 {
            return match classify_probe_failure(error_code, i64::from(libc::ESRCH)) {
                ProbeFailure::Absent => StartTime::Absent,
                ProbeFailure::Ambiguous => StartTime::Error,
            };
        }
        if written < size {
            return StartTime::Error;
        }
        // SAFETY: a successful full-size `proc_pidinfo` result initialized the
        // complete `proc_bsdinfo` output object.
        let info = unsafe { info.assume_init() };
        let usec = info
            .pbi_start_tvsec
            .wrapping_mul(1_000_000)
            .wrapping_add(info.pbi_start_tvusec);
        StartTime::Present(usec)
    }

    pub(super) fn birth_token(pid: u32) -> Option<u64> {
        match read_start_time_usec(pid) {
            StartTime::Present(usec) => Some(PID_BIRTH_SYSCTL_TAG | (usec & PAYLOAD_MASK)),
            StartTime::Absent | StartTime::Error => None,
        }
    }

    pub(super) fn alive(pid: u32, pid_birth: u64) -> ProcessLiveness {
        match read_start_time_usec(pid) {
            StartTime::Absent => ProcessLiveness::Dead,
            StartTime::Error => ProcessLiveness::Unknown,
            StartTime::Present(usec) => {
                if pid_birth & PID_BIRTH_SYSCTL_TAG == 0 {
                    // Untagged/legacy or foreign-platform token: cannot compare
                    // start time, so stay conservative.
                    return ProcessLiveness::Alive;
                }
                if (usec & PAYLOAD_MASK) == (pid_birth & PAYLOAD_MASK) {
                    ProcessLiveness::Alive
                } else {
                    ProcessLiveness::Dead
                }
            }
        }
    }
}

#[cfg(windows)]
mod windows_impl {
    use super::{
        PAYLOAD_MASK, PID_BIRTH_FILETIME_TAG, ProbeFailure, ProcessLiveness, classify_probe_failure,
    };
    use windows_sys::Win32::Foundation::{
        CloseHandle, ERROR_INVALID_PARAMETER, FILETIME, GetLastError,
    };
    use windows_sys::Win32::System::Threading::{
        GetProcessTimes, OpenProcess, PROCESS_QUERY_LIMITED_INFORMATION,
    };

    /// Outcome of reading a process's creation `FILETIME`.
    enum Creation {
        /// Process exists; creation time in 100 ns ticks since 1601.
        Present(u64),
        /// No such process (`OpenProcess` reported an invalid PID).
        Absent,
        /// The probe failed ambiguously (`OpenProcess` or `GetProcessTimes`).
        Error,
    }

    fn read_creation_100ns(pid: u32) -> Creation {
        // SAFETY: `OpenProcess` takes a query-limited access mask, a non-inherit
        // flag, and a PID; it returns a handle or null. No memory is aliased.
        let handle = unsafe { OpenProcess(PROCESS_QUERY_LIMITED_INFORMATION, 0, pid) };
        if handle.is_null() {
            // SAFETY: reads the calling thread's last-error, no arguments.
            let last = unsafe { GetLastError() };
            return match classify_probe_failure(
                Some(i64::from(last)),
                i64::from(ERROR_INVALID_PARAMETER),
            ) {
                ProbeFailure::Absent => Creation::Absent,
                ProbeFailure::Ambiguous => Creation::Error,
            };
        }
        let mut creation = FILETIME {
            dwLowDateTime: 0,
            dwHighDateTime: 0,
        };
        let mut exit = creation;
        let mut kernel = creation;
        let mut user = creation;
        // SAFETY: `handle` is a live process handle; the four `FILETIME` out
        // pointers are valid, initialized locals.
        let ok = unsafe {
            GetProcessTimes(
                handle,
                &raw mut creation,
                &raw mut exit,
                &raw mut kernel,
                &raw mut user,
            )
        };
        // SAFETY: `handle` came from `OpenProcess` above and is not used again.
        unsafe {
            CloseHandle(handle);
        }
        if ok == 0 {
            return Creation::Error;
        }
        let ticks = (u64::from(creation.dwHighDateTime) << 32) | u64::from(creation.dwLowDateTime);
        Creation::Present(ticks)
    }

    pub(super) fn birth_token(pid: u32) -> Option<u64> {
        match read_creation_100ns(pid) {
            Creation::Present(ticks) => Some(PID_BIRTH_FILETIME_TAG | (ticks & PAYLOAD_MASK)),
            Creation::Absent | Creation::Error => None,
        }
    }

    pub(super) fn alive(pid: u32, pid_birth: u64) -> ProcessLiveness {
        match read_creation_100ns(pid) {
            Creation::Absent => ProcessLiveness::Dead,
            Creation::Error => ProcessLiveness::Unknown,
            Creation::Present(ticks) => {
                if pid_birth & PID_BIRTH_FILETIME_TAG == 0 {
                    return ProcessLiveness::Alive;
                }
                if (ticks & PAYLOAD_MASK) == (pid_birth & PAYLOAD_MASK) {
                    ProcessLiveness::Alive
                } else {
                    ProcessLiveness::Dead
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn platform_birth_tags_are_distinct_and_high() {
        // No two tags overlap, and the payload mask clears all three.
        assert_ne!(PID_BIRTH_PROCFS_TAG, PID_BIRTH_SYSCTL_TAG);
        assert_ne!(PID_BIRTH_SYSCTL_TAG, PID_BIRTH_FILETIME_TAG);
        assert_eq!(PID_BIRTH_PROCFS_TAG & PAYLOAD_MASK, 0);
        assert_eq!(PID_BIRTH_SYSCTL_TAG & PAYLOAD_MASK, 0);
        assert_eq!(PID_BIRTH_FILETIME_TAG & PAYLOAD_MASK, 0);
    }

    #[test]
    fn pid_zero_is_dead() {
        assert_eq!(process_alive(0, 0), ProcessLiveness::Dead);
    }

    #[test]
    fn macos_zero_probe_result_requires_esrch_to_prove_absence() {
        assert_eq!(
            classify_probe_failure(Some(i64::from(libc::ESRCH)), i64::from(libc::ESRCH)),
            ProbeFailure::Absent
        );
        assert_eq!(
            classify_probe_failure(Some(i64::from(libc::EACCES)), i64::from(libc::ESRCH)),
            ProbeFailure::Ambiguous
        );
        assert_eq!(
            classify_probe_failure(None, i64::from(libc::ESRCH)),
            ProbeFailure::Ambiguous
        );
    }

    #[test]
    fn windows_open_process_resource_failure_is_ambiguous() {
        const ERROR_INVALID_PARAMETER_CODE: i64 = 87;
        const ERROR_NOT_ENOUGH_MEMORY_CODE: i64 = 8;

        assert_eq!(
            classify_probe_failure(
                Some(ERROR_INVALID_PARAMETER_CODE),
                ERROR_INVALID_PARAMETER_CODE
            ),
            ProbeFailure::Absent
        );
        assert_eq!(
            classify_probe_failure(
                Some(ERROR_NOT_ENOUGH_MEMORY_CODE),
                ERROR_INVALID_PARAMETER_CODE
            ),
            ProbeFailure::Ambiguous
        );
    }

    // The live probes below run only on the platforms they are implemented for;
    // on Linux `process_alive` intentionally returns `Unknown` (mvcc keeps its
    // procfs probe), which is asserted here.
    #[cfg(not(any(target_os = "macos", windows)))]
    #[test]
    fn non_macos_non_windows_returns_unknown() {
        assert_eq!(
            process_alive(std::process::id(), 0),
            ProcessLiveness::Unknown
        );
        assert!(current_process_birth_token().is_none());
    }

    #[cfg(any(target_os = "macos", windows))]
    #[test]
    fn current_process_is_alive_with_its_own_birth_token() {
        let birth = current_process_birth_token().expect("own birth token available");
        assert_eq!(
            process_alive(std::process::id(), birth),
            ProcessLiveness::Alive,
            "the current process must read as alive with its own birth token"
        );
    }

    #[cfg(any(target_os = "macos", windows))]
    #[test]
    fn recycled_pid_birth_mismatch_reads_dead() {
        // Same PID, deliberately wrong birth payload -> reused-PID -> Dead.
        let birth = current_process_birth_token().expect("own birth token");
        let tag = birth & !PAYLOAD_MASK;
        let mismatched = tag | ((birth & PAYLOAD_MASK) ^ 0x5A5A);
        assert_eq!(
            process_alive(std::process::id(), mismatched),
            ProcessLiveness::Dead,
            "a start-time mismatch on a live PID must read as Dead (reuse-safe)"
        );
    }

    #[cfg(any(target_os = "macos", windows))]
    #[test]
    fn almost_certainly_dead_pid_reads_dead() {
        // A very high PID that is almost certainly not running. If it happens to
        // exist, the birth mismatch still yields Dead; either way, not Alive.
        let verdict = process_alive(
            0x7FFF_FFF0,
            PID_BIRTH_SYSCTL_TAG | PID_BIRTH_FILETIME_TAG | 0x1234,
        );
        assert_ne!(verdict, ProcessLiveness::Alive);
    }
}
