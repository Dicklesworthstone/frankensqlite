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
    use super::{PAYLOAD_MASK, PID_BIRTH_SYSCTL_TAG, ProcessLiveness};

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
        // ubs:ignore - proc_bsdinfo is a plain-old-data C struct; a zeroed output
        // buffer is the standard `proc_pidinfo` idiom, not an uninitialized read.
        // SAFETY: `mem::zeroed()` is valid for `proc_bsdinfo` (all integer fields).
        let mut info: libc::proc_bsdinfo = unsafe { std::mem::zeroed() };
        let size = std::mem::size_of::<libc::proc_bsdinfo>() as libc::c_int;
        // SAFETY: `proc_pidinfo` writes at most `size` bytes into `info`, a live
        // and correctly-sized `proc_bsdinfo`; it returns the byte count, or <= 0
        // on error / no such process.
        let written = unsafe {
            libc::proc_pidinfo(
                pid as libc::c_int,
                libc::PROC_PIDTBSDINFO,
                0,
                std::ptr::from_mut(&mut info).cast(),
                size,
            )
        };
        if written <= 0 {
            // `proc_pidinfo` returns 0 or -1 (errno `ESRCH`) for a process that
            // no longer exists; other errno values are ambiguous.
            return if written == 0
                || std::io::Error::last_os_error().raw_os_error() == Some(libc::ESRCH)
            {
                StartTime::Absent
            } else {
                StartTime::Error
            };
        }
        if written < size {
            return StartTime::Error;
        }
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
    use super::{PAYLOAD_MASK, PID_BIRTH_FILETIME_TAG, ProcessLiveness};
    use windows_sys::Win32::Foundation::{CloseHandle, ERROR_ACCESS_DENIED, FILETIME, GetLastError};
    use windows_sys::Win32::System::Threading::{
        GetProcessTimes, OpenProcess, PROCESS_QUERY_LIMITED_INFORMATION,
    };

    /// Outcome of reading a process's creation `FILETIME`.
    enum Creation {
        /// Process exists; creation time in 100 ns ticks since 1601.
        Present(u64),
        /// No such process (`OpenProcess` failed, not access-denied).
        Absent,
        /// The probe failed ambiguously (access denied, or `GetProcessTimes`).
        Error,
    }

    fn read_creation_100ns(pid: u32) -> Creation {
        // SAFETY: `OpenProcess` takes a query-limited access mask, a non-inherit
        // flag, and a PID; it returns a handle or null. No memory is aliased.
        let handle = unsafe { OpenProcess(PROCESS_QUERY_LIMITED_INFORMATION, 0, pid) };
        if handle.is_null() {
            // SAFETY: reads the calling thread's last-error, no arguments.
            let last = unsafe { GetLastError() };
            return if last == ERROR_ACCESS_DENIED {
                Creation::Error
            } else {
                Creation::Absent
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
            GetProcessTimes(handle, &mut creation, &mut exit, &mut kernel, &mut user)
        };
        // SAFETY: `handle` came from `OpenProcess` above and is not used again.
        unsafe {
            CloseHandle(handle);
        }
        if ok == 0 {
            return Creation::Error;
        }
        let ticks =
            (u64::from(creation.dwHighDateTime) << 32) | u64::from(creation.dwLowDateTime);
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

    // The live probes below run only on the platforms they are implemented for;
    // on Linux `process_alive` intentionally returns `Unknown` (mvcc keeps its
    // procfs probe), which is asserted here.
    #[cfg(not(any(target_os = "macos", windows)))]
    #[test]
    fn non_macos_non_windows_returns_unknown() {
        assert_eq!(process_alive(std::process::id(), 0), ProcessLiveness::Unknown);
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
        let verdict = process_alive(0x7FFF_FFF0, PID_BIRTH_SYSCTL_TAG | PID_BIRTH_FILETIME_TAG | 0x1234);
        assert_ne!(verdict, ProcessLiveness::Alive);
    }
}
