//! GH#424: time the actual shell, including runtime startup and shutdown.
//!
//! Keep the missing semicolon: the reported Windows command executes the
//! statement at EOF, rather than using the command-line SQL shortcut.

use std::fs::{self, File};
use std::io::Write;
use std::process::{Child, Command, Stdio};
use std::thread;
use std::time::{Duration, Instant};

// This is a coarse stall detector, not a 10 ms performance benchmark. The
// budget leaves substantial debug-build/host-load headroom while rejecting
// the five-second native-Cx handoff timeout reported in GH#424.
const STARTUP_BUDGET: Duration = Duration::from_secs(2);
const POLL_INTERVAL: Duration = Duration::from_millis(10);

/// Reap the subprocess even if an assertion or pipe write fails.
struct ReapOnDrop(Child);

impl Drop for ReapOnDrop {
    fn drop(&mut self) {
        if !matches!(self.0.try_wait(), Ok(Some(_))) {
            let _ = self.0.kill();
            let _ = self.0.wait();
        }
    }
}

fn assert_prompt_piped_select(file_backed: bool) {
    let dir = tempfile::tempdir().expect("create isolated shell working directory");
    let stdout_path = dir.path().join("stdout.txt");
    let stderr_path = dir.path().join("stderr.txt");
    let stdout = File::create(&stdout_path).expect("create stdout capture");
    let stderr = File::create(&stderr_path).expect("create stderr capture");
    let mut command = Command::new(env!("CARGO_BIN_EXE_fsqlite"));
    command
        .current_dir(dir.path())
        .stdin(Stdio::piped())
        // Files cannot fill a pipe buffer and deadlock the child while the
        // parent polls its status. Keep stdout/stderr for timeout diagnostics.
        .stdout(Stdio::from(stdout))
        .stderr(Stdio::from(stderr));
    if file_backed {
        command.arg("startup.db");
    }
    // Exercise ordinary defaults, not the invoking developer's opt-in
    // instrumentation, fault injection, or metrics-listener configuration.
    // Alter only the child environment; preserve Windows SystemRoot/PATH.
    for (key, _) in std::env::vars_os() {
        if key
            .to_str()
            .is_some_and(|name| name.to_ascii_uppercase().starts_with("FSQLITE_"))
        {
            command.env_remove(key);
        }
    }
    command.env_remove("RUST_LOG");

    // Cargo builds the executable before the integration test starts. The
    // interval below measures process creation through exit, not compilation.
    let started = Instant::now();
    let mut child = ReapOnDrop(command.spawn().expect("spawn the real fsqlite binary"));
    {
        let mut stdin = child.0.stdin.take().expect("child stdin is piped");
        stdin.write_all(b"select 1 a\n").expect("pipe SELECT");
        // Dropping this sole writer delivers EOF on Windows as well as Unix.
    }

    let mut timed_out = false;
    let (status, elapsed) = loop {
        if let Some(status) = child.0.try_wait().expect("poll shell exit status") {
            break (status, started.elapsed());
        }
        let elapsed = started.elapsed();
        if elapsed >= STARTUP_BUDGET {
            timed_out = true;
            child.0.kill().expect("terminate a stalled shell");
            break (child.0.wait().expect("reap a stalled shell"), elapsed);
        }
        thread::sleep(POLL_INTERVAL.min(STARTUP_BUDGET.saturating_sub(elapsed)));
    };
    // Release the Command's inherited capture handles before reading/removing
    // the captures, including on Windows.
    drop(command);
    let stdout = fs::read_to_string(&stdout_path).expect("read shell stdout");
    let stderr = fs::read_to_string(&stderr_path).expect("read shell stderr");

    assert!(
        !timed_out && elapsed < STARTUP_BUDGET,
        "GH#424: piped SELECT exceeded {STARTUP_BUDGET:?}; \
         file_backed={file_backed}, elapsed={elapsed:?}, status={status}, \
         stdout={stdout:?}, stderr={stderr:?}"
    );
    assert!(status.success(), "shell failed: {status}; stderr={stderr:?}");
    assert_eq!(stdout.replace("\r\n", "\n"), "1\n");
    assert!(stderr.is_empty(), "unexpected shell stderr: {stderr:?}");
}

#[test]
fn gh424_memory_piped_select_exits_promptly() {
    assert_prompt_piped_select(false);
}

#[test]
fn gh424_file_piped_select_exits_promptly() {
    assert_prompt_piped_select(true);
}
