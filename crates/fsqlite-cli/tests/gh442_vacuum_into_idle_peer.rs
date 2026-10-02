//! GH#442: `VACUUM INTO` must succeed while another process has the WAL-mode
//! source open and idle, as stock SQLite and fsqlite 0.3.9 do.
//!
//! Since 0.4.4 every connection keeps a main-file SHARED claim for its whole
//! WAL attachment (as stock SQLite does). The source-image receipt behind
//! `VACUUM INTO` was captured under the whole-image EXCLUSIVE maintenance
//! fence, which that idle claim refuses, so the statement waited out
//! `busy_timeout` and failed with "database is busy".

#![cfg(unix)]

use std::fs::{self, File};
use std::io::Write;
use std::path::Path;
use std::process::{Child, ChildStdin, Command, Output, Stdio};
use std::thread;
use std::time::{Duration, Instant};

const PEER_READY_BUDGET: Duration = Duration::from_secs(30);
const POLL_INTERVAL: Duration = Duration::from_millis(10);

/// Reap the subprocess even if an assertion fails.
struct ReapOnDrop(Child);

impl Drop for ReapOnDrop {
    fn drop(&mut self) {
        if !matches!(self.0.try_wait(), Ok(Some(_))) {
            let _ = self.0.kill();
            let _ = self.0.wait();
        }
    }
}

fn shell(dir: &Path) -> Command {
    let mut command = Command::new(env!("CARGO_BIN_EXE_fsqlite"));
    command.current_dir(dir);
    for (key, _) in std::env::vars_os() {
        if key
            .to_str()
            .is_some_and(|name| name.to_ascii_uppercase().starts_with("FSQLITE_"))
        {
            command.env_remove(key);
        }
    }
    command.env_remove("RUST_LOG");
    command
}

fn run_sql(dir: &Path, db: &str, sql: &str) -> Output {
    shell(dir)
        .arg(db)
        .arg("-c")
        .arg(sql)
        .output()
        .expect("run the real fsqlite binary")
}

fn stdout_of(output: &Output) -> String {
    String::from_utf8_lossy(&output.stdout).replace("\r\n", "\n")
}

/// Start a shell on `db`, run `sql` in it, and return once it has printed
/// `ready_marker`, leaving the process open (and idle unless `sql` opened a
/// transaction) with its stdin held.
fn open_peer(dir: &Path, db: &str, sql: &str, ready_marker: &str) -> (ReapOnDrop, ChildStdin) {
    let stdout_path = dir.join("peer_stdout.txt");
    let stdout = File::create(&stdout_path).expect("create peer stdout capture");
    let mut child = ReapOnDrop(
        shell(dir)
            .arg(db)
            .stdin(Stdio::piped())
            .stdout(Stdio::from(stdout))
            .stderr(Stdio::null())
            .spawn()
            .expect("spawn the peer shell"),
    );
    let mut stdin = child.0.stdin.take().expect("peer stdin is piped");
    stdin.write_all(sql.as_bytes()).expect("send peer SQL");
    stdin.flush().expect("flush peer SQL");

    let started = Instant::now();
    loop {
        let printed = fs::read_to_string(&stdout_path).unwrap_or_default();
        if printed.contains(ready_marker) {
            break;
        }
        assert!(
            child.0.try_wait().expect("poll peer").is_none(),
            "peer shell exited early; stdout={printed:?}"
        );
        assert!(
            started.elapsed() < PEER_READY_BUDGET,
            "peer shell never printed {ready_marker:?}; stdout={printed:?}"
        );
        thread::sleep(POLL_INTERVAL);
    }
    (child, stdin)
}

fn assert_vacuum_into_beside_open_peer(peer_sql: &str) {
    let dir = tempfile::tempdir().expect("create isolated working directory");
    let seeded = run_sql(
        dir.path(),
        "live.db",
        "CREATE TABLE t(x); INSERT INTO t SELECT value FROM generate_series(1,500);",
    );
    assert!(seeded.status.success(), "seed failed: {seeded:?}");

    let (peer, peer_stdin) = open_peer(dir.path(), "live.db", peer_sql, "500");

    let started = Instant::now();
    let vacuumed = run_sql(
        dir.path(),
        "live.db",
        "PRAGMA busy_timeout=3000; VACUUM INTO 'copy.db';",
    );
    let elapsed = started.elapsed();
    assert!(
        vacuumed.status.success(),
        "GH#442: VACUUM INTO failed beside an open peer after {elapsed:?}: \
         stdout={:?} stderr={:?}",
        stdout_of(&vacuumed),
        String::from_utf8_lossy(&vacuumed.stderr)
    );

    let checked = run_sql(
        dir.path(),
        "copy.db",
        "PRAGMA integrity_check; SELECT count(*), sum(x) FROM t;",
    );
    assert!(checked.status.success(), "copy check failed: {checked:?}");
    assert_eq!(stdout_of(&checked), "ok\n500|125250\n");

    // The source is still usable by both processes afterwards.
    let wrote = run_sql(dir.path(), "live.db", "INSERT INTO t VALUES(0);");
    assert!(
        wrote.status.success(),
        "post-VACUUM write failed: {wrote:?}"
    );

    drop(peer_stdin);
    drop(peer);
}

#[test]
fn gh442_vacuum_into_succeeds_beside_idle_peer_process() {
    assert_vacuum_into_beside_open_peer("SELECT count(*) FROM t;\n");
}
