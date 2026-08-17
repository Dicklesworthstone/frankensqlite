//! beads-doctor binary: a thin runtime owner.
//!
//! All logic lives in the `beads_doctor` library; this entry point only
//! collects arguments, delegates to [`beads_doctor::run_main`] (which builds the
//! asupersync runtime and dispatches), and translates the returned exit code.

#![forbid(unsafe_code)]

use std::process::ExitCode;

fn main() -> ExitCode {
    let args: Vec<std::ffi::OsString> = std::env::args_os().collect();
    let code = beads_doctor::run_main(args);
    // Exit codes are small, non-negative process statuses; clamp defensively.
    ExitCode::from(u8::try_from(code).unwrap_or(1))
}
