//! Depth-parity oracle for GH#288 / bd-k0o2q — the explicit trigger trampoline.
//!
//! Stock SQLite executes trigger bodies as VDBE `OP_Program` subprograms with
//! heap-allocated frames, so it admits `SQLITE_MAX_TRIGGER_DEPTH = 1000` nested
//! trigger levels. FrankenSQLite currently Rust-recurses through
//! `execute_statement` per level (~185 KiB of native stack each), so it caps far
//! lower (`MAX_TRIGGER_DEPTH`) to stay below a stack overflow.
//!
//! This oracle MEASURES the gap rather than asserting a fixed target: it pins
//! frank against stock at the current cap boundary and well beyond it, and
//! verifies frank's cap fires as a clean typed error (never a crash) with
//! stock-parity wording. As the heap trampoline lands, raise
//! [`FRANK_TRIGGER_DEPTH_CAP`] in lock-step; the suite stays green at every
//! stage and the frontier moves until it reaches [`STOCK_TRIGGER_DEPTH_CAP`].

use fsqlite_core::connection::Connection;
use fsqlite_types::value::SqliteValue;

/// The nesting depth FrankenSQLite admits today before the typed
/// `too many levels of trigger recursion` error — equal to the engine's
/// `MAX_TRIGGER_DEPTH`. RAISE THIS in the same commit that raises the engine's
/// real ceiling, so this oracle tracks trampoline progress exactly.
const FRANK_TRIGGER_DEPTH_CAP: usize = 8;

/// Stock SQLite's `SQLITE_MAX_TRIGGER_DEPTH`. The parity target.
const STOCK_TRIGGER_DEPTH_CAP: usize = 1000;

/// A depth comfortably inside stock's ceiling but far above frank's current cap,
/// used to demonstrate the gap without approaching stock's own limit.
const STOCK_DEEP_PROBE: usize = 200;

/// Frank Rust-recurses ~185 KiB/level; admitting `FRANK_TRIGGER_DEPTH_CAP`
/// levels needs a few MiB of native stack, so frank's workload runs on a pinned
/// large thread stack. (Once the trampoline makes depth heap-bounded, this
/// headroom becomes unnecessary — see the ignored 1 MiB probe below.)
const FRANK_STACK_BYTES: usize = 16 * 1024 * 1024;

const DEPTH_ERROR: &str = "too many levels of trigger recursion";

#[derive(Debug, Clone, PartialEq, Eq)]
enum Outcome {
    /// The driving `UPDATE` and every admitted trigger level ran; the final
    /// `(a.n, b.n)` values are the deterministic result of the ping-pong chain.
    Completed { a: i64, b: i64 },
    /// The engine refused a level with the typed trigger-recursion error.
    DepthError,
    /// Any other error (fails the oracle — the boundary must be the typed one).
    OtherError(String),
}

/// A two-table ping-pong: `UPDATE a` fires an AFTER trigger that updates `b`,
/// whose AFTER trigger updates `a`, and so on, incrementing `n` each hop. The
/// `WHEN NEW.n < depth` guard terminates the chain at exactly `depth` nested
/// trigger frames without relying on either engine's recursion cap.
/// The discrete setup statements. Kept as separate strings because a
/// `CREATE TRIGGER … BEGIN …; END;` body contains its own semicolons — it must
/// be handed to `execute` whole, never split on `;`.
fn setup_statements(depth: usize) -> Vec<String> {
    vec![
        "PRAGMA recursive_triggers = ON;".to_owned(),
        "CREATE TABLE a (n INTEGER);".to_owned(),
        "CREATE TABLE b (n INTEGER);".to_owned(),
        "INSERT INTO a VALUES (0);".to_owned(),
        "INSERT INTO b VALUES (0);".to_owned(),
        format!(
            "CREATE TRIGGER trg_a AFTER UPDATE ON a WHEN NEW.n < {depth} \
             BEGIN UPDATE b SET n = NEW.n + 1; END;"
        ),
        format!(
            "CREATE TRIGGER trg_b AFTER UPDATE ON b WHEN NEW.n < {depth} \
             BEGIN UPDATE a SET n = NEW.n + 1; END;"
        ),
    ]
}

/// The same setup as one batch, for stock's `execute_batch` (a real SQL parser
/// that handles the trigger-body semicolons correctly).
fn setup_sql(depth: usize) -> String {
    setup_statements(depth).join("\n")
}

fn classify_error(msg: &str) -> Outcome {
    if msg.contains(DEPTH_ERROR) {
        Outcome::DepthError
    } else {
        Outcome::OtherError(msg.to_owned())
    }
}

/// Run the depth-`depth` chain through FrankenSQLite on a pinned large stack.
fn frank_outcome(depth: usize) -> Outcome {
    let (tx, rx) = std::sync::mpsc::channel();
    std::thread::Builder::new()
        .name("frank-trigger-depth".to_owned())
        .stack_size(FRANK_STACK_BYTES)
        .spawn(move || {
            asupersync::test_utils::run_test(move || async move {
                let conn = Connection::open(":memory:").await.expect("frank open");
                for stmt in setup_statements(depth) {
                    conn.execute(&stmt).await.expect("frank setup statement");
                }
                let outcome = match conn.execute("UPDATE a SET n = 1;").await {
                    Ok(_) => Outcome::Completed {
                        a: frank_scalar(&conn, "SELECT n FROM a;").await,
                        b: frank_scalar(&conn, "SELECT n FROM b;").await,
                    },
                    Err(e) => classify_error(&e.to_string()),
                };
                tx.send(outcome).expect("send frank outcome");
            });
        })
        .expect("spawn frank depth thread")
        .join()
        .expect("frank depth thread panicked");
    rx.recv().expect("frank outcome not produced")
}

async fn frank_scalar(conn: &Connection, sql: &str) -> i64 {
    let rows = conn.query_with_params(sql, &[]).await.expect("frank query");
    match &rows[0].values()[0] {
        SqliteValue::Integer(n) => *n,
        other => panic!("unexpected frank scalar {other:?}"),
    }
}

/// Run the same chain through stock SQLite (rusqlite). Stock uses heap VDBE
/// frames, so no large stack is needed.
fn stock_outcome(depth: usize) -> Outcome {
    let conn = rusqlite::Connection::open_in_memory().expect("stock open");
    conn.execute_batch(&setup_sql(depth)).expect("stock setup");
    match conn.execute("UPDATE a SET n = 1;", []) {
        Ok(_) => Outcome::Completed {
            a: conn
                .query_row("SELECT n FROM a;", [], |r| r.get(0))
                .expect("stock a"),
            b: conn
                .query_row("SELECT n FROM b;", [], |r| r.get(0))
                .expect("stock b"),
        },
        Err(e) => classify_error(&e.to_string()),
    }
}

#[test]
fn frank_admits_its_cap_and_rejects_the_next_level_cleanly() {
    // The exact boundary is admitted...
    assert!(
        matches!(frank_outcome(FRANK_TRIGGER_DEPTH_CAP), Outcome::Completed { .. }),
        "frank must admit its configured cap of {FRANK_TRIGGER_DEPTH_CAP} nested trigger levels"
    );
    // ...and the first level beyond it fails with the typed error, never a crash
    // or a different error kind.
    assert_eq!(
        frank_outcome(FRANK_TRIGGER_DEPTH_CAP + 1),
        Outcome::DepthError,
        "one level past frank's cap must be the typed trigger-recursion error"
    );
}

#[test]
fn stock_admits_far_beyond_franks_current_cap() {
    // Stock happily runs the level frank rejects, and a much deeper chain, all
    // inside its own 1000-level ceiling — this is the parity gap the trampoline
    // exists to close.
    assert!(
        matches!(stock_outcome(FRANK_TRIGGER_DEPTH_CAP + 1), Outcome::Completed { .. }),
        "stock must admit the level frank currently rejects"
    );
    assert!(
        matches!(stock_outcome(STOCK_DEEP_PROBE), Outcome::Completed { .. }),
        "stock must admit a {STOCK_DEEP_PROBE}-level chain (well within its 1000 cap)"
    );
    assert!(
        FRANK_TRIGGER_DEPTH_CAP < STOCK_TRIGGER_DEPTH_CAP,
        "parity frontier: frank cap {FRANK_TRIGGER_DEPTH_CAP} vs stock {STOCK_TRIGGER_DEPTH_CAP}"
    );
}

#[test]
fn frank_matches_stock_at_every_depth_within_franks_cap() {
    // Below the cap the two engines must produce byte-identical results: the
    // trampoline may only change how deep frank can go, never what a given depth
    // computes.
    for depth in 1..=FRANK_TRIGGER_DEPTH_CAP {
        let frank = frank_outcome(depth);
        let stock = stock_outcome(depth);
        assert_eq!(
            frank, stock,
            "frank and stock must agree on the depth-{depth} ping-pong result"
        );
        assert!(
            matches!(frank, Outcome::Completed { .. }),
            "depth {depth} (<= cap) must complete in both engines"
        );
    }
}

#[test]
fn depth_error_wording_is_at_stock_parity() {
    // Frank's over-cap error and stock's over-1000 error must read identically —
    // the message is part of the conformance contract.
    let frank_msg = frank_over_cap_message();
    let stock_msg = stock_over_cap_message();
    assert!(
        frank_msg.contains(DEPTH_ERROR),
        "frank depth error must say {DEPTH_ERROR:?}, got {frank_msg:?}"
    );
    assert!(
        stock_msg.contains(DEPTH_ERROR),
        "stock depth error must say {DEPTH_ERROR:?}, got {stock_msg:?}"
    );
}

fn frank_over_cap_message() -> String {
    let (tx, rx) = std::sync::mpsc::channel();
    std::thread::Builder::new()
        .name("frank-trigger-depth-msg".to_owned())
        .stack_size(FRANK_STACK_BYTES)
        .spawn(move || {
            asupersync::test_utils::run_test(move || async move {
                let conn = Connection::open(":memory:").await.expect("frank open");
                for stmt in setup_statements(FRANK_TRIGGER_DEPTH_CAP + 1) {
                    conn.execute(&stmt).await.expect("frank setup statement");
                }
                let msg = conn
                    .execute("UPDATE a SET n = 1;")
                    .await
                    .expect_err("frank must reject past its cap")
                    .to_string();
                tx.send(msg).expect("send frank message");
            });
        })
        .expect("spawn frank message thread")
        .join()
        .expect("frank message thread panicked");
    rx.recv().expect("frank message not produced")
}

fn stock_over_cap_message() -> String {
    let conn = rusqlite::Connection::open_in_memory().expect("stock open");
    // A chain that never terminates via WHEN forces stock past its own 1000 cap.
    conn.execute_batch(&setup_sql(STOCK_TRIGGER_DEPTH_CAP + 5))
        .expect("stock setup");
    conn.execute("UPDATE a SET n = 1;", [])
        .expect_err("stock must reject past its cap")
        .to_string()
}

/// Parity acceptance for the finished trampoline: on a 1 MiB thread stack —
/// smaller than the current native-recursion budget — frank must reach stock's
/// ceiling (or its own configured cap) as a clean typed error, never a
/// `SIGABRT` stack overflow. This is RED until the heap trampoline lands
/// (native recursion aborts around depth 3–4 on 1 MiB, see
/// bd-trigger-depth-1mib-probe-2pyzd), so it is ignored today. UN-IGNORE it in
/// the trampoline stage that makes depth heap-bounded; run in a subprocess so a
/// pre-trampoline abort can never take down the test binary.
#[test]
#[ignore = "GH#288: un-ignore when the heap trampoline lands; native recursion SIGABRTs on a 1 MiB stack"]
fn frank_reaches_cap_on_a_1mib_stack_without_crashing() {
    if std::env::var("FRANK_1MIB_TRIGGER_DEPTH_CHILD").is_ok() {
        // Child body: on a real 1 MiB stack, admit the cap and reject the next
        // level with the typed error — no stack-overflow abort.
        std::thread::Builder::new()
            .name("frank-1mib-trigger-depth".to_owned())
            .stack_size(1024 * 1024)
            .spawn(|| {
                assert!(
                    matches!(frank_outcome(FRANK_TRIGGER_DEPTH_CAP), Outcome::Completed { .. }),
                    "cap must be admitted on a 1 MiB stack once depth is heap-bounded"
                );
                assert_eq!(
                    frank_outcome(FRANK_TRIGGER_DEPTH_CAP + 1),
                    Outcome::DepthError,
                    "over-cap must be a clean typed error on a 1 MiB stack"
                );
            })
            .expect("spawn 1 MiB child thread")
            .join()
            .expect("1 MiB child thread panicked");
        return;
    }
    // Parent: run the child body in a fresh process so a native abort is
    // contained as a non-zero exit rather than killing this binary.
    let status = std::process::Command::new(
        std::env::current_exe().expect("test binary path"),
    )
    .args([
        "frank_reaches_cap_on_a_1mib_stack_without_crashing",
        "--exact",
        "--ignored",
        "--nocapture",
    ])
    .env("FRANK_1MIB_TRIGGER_DEPTH_CHILD", "1")
    .status()
    .expect("spawn 1 MiB trigger-depth child process");
    assert!(
        status.success(),
        "frank must reach its cap cleanly on a 1 MiB stack (heap-bounded depth): {status}"
    );
}
