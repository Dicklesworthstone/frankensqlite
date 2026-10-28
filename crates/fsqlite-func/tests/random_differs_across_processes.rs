//! random() and randomblob() must not replay one fixed sequence in every
//! process. C SQLite seeds its PRNG from the VFS (OS entropy) once per
//! process, so two processes draw different values. With a fixed seed every
//! process returned the same values, and `hex(randomblob(16))` keys made in
//! two processes collided.
//!
//! The test re-runs its own binary as child processes and compares what each
//! one draws first.

use std::process::Command;

use fsqlite_func::{FunctionRegistry, register_builtins};
use fsqlite_types::SqliteValue;

const TEST_NAME: &str = "random_and_randomblob_differ_across_processes";
const CHILD_ENV: &str = "FSQLITE_RANDOM_KEEPER_CHILD";
const MARKER: &str = "RANDOM_DRAW ";

/// The first values one process draws: three random() integers and one
/// 16-byte randomblob(), as text.
fn first_draws() -> String {
    let mut registry = FunctionRegistry::default();
    register_builtins(&mut registry);
    let random = registry
        .find_scalar("random", 0)
        .expect("random() is registered");
    let randomblob = registry
        .find_scalar("randomblob", 1)
        .expect("randomblob() is registered");

    let mut out = Vec::new();
    for _ in 0..3 {
        match random.invoke(&[]).expect("random() succeeds") {
            SqliteValue::Integer(v) => out.push(v.to_string()),
            other => panic!("random() returned {other:?}"),
        }
    }
    match randomblob
        .invoke(&[SqliteValue::Integer(16)])
        .expect("randomblob(16) succeeds")
    {
        SqliteValue::Blob(bytes) => {
            assert_eq!(bytes.len(), 16);
            out.push(bytes.iter().map(|b| format!("{b:02X}")).collect());
        }
        other => panic!("randomblob(16) returned {other:?}"),
    }
    out.join(" ")
}

fn child_draws() -> String {
    let exe = std::env::current_exe().expect("test binary path");
    let output = Command::new(exe)
        .args([TEST_NAME, "--exact", "--nocapture", "--test-threads=1"])
        .env(CHILD_ENV, "1")
        .output()
        .expect("spawn child test process");
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        output.status.success(),
        "child process failed: {}\nstdout:\n{stdout}\nstderr:\n{}",
        output.status,
        String::from_utf8_lossy(&output.stderr)
    );
    // libtest prints `test <name> ... ` on the same line before the output.
    stdout
        .lines()
        .find_map(|line| line.split_once(MARKER).map(|(_, draws)| draws))
        .unwrap_or_else(|| panic!("child printed no draw line; stdout:\n{stdout}"))
        .to_owned()
}

#[test]
fn random_and_randomblob_differ_across_processes() {
    if std::env::var_os(CHILD_ENV).is_some() {
        println!("{MARKER}{}", first_draws());
        return;
    }

    let first = child_draws();
    let second = child_draws();
    let third = child_draws();
    // Each value is 64 or 128 random bits; a repeat across seeded processes
    // has negligible probability, while a fixed seed repeats every time.
    assert_ne!(
        first, second,
        "two processes drew the same random()/randomblob() values"
    );
    assert_ne!(
        first, third,
        "two processes drew the same random()/randomblob() values"
    );
    assert_ne!(
        second, third,
        "two processes drew the same random()/randomblob() values"
    );
}
