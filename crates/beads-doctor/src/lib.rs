//! beads-doctor: a cross-platform periodic probe that runs `br doctor --quick
//! --json` against each configured beads workspace, notifies on error-severity
//! findings, and keeps a rolling per-workspace log.
//!
//! The binary (`src/main.rs`) is a thin runtime owner; this library holds the
//! testable core. Every async fn takes `&Cx` first (asupersync-native), and the
//! per-workspace probes run under one structured [`asupersync::combinator::JoinSet`]
//! region so no task is ever orphaned.

#![forbid(unsafe_code)]

use std::ffi::OsString;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use asupersync::{Cx, Outcome};
use serde::Serialize;

pub mod config;
pub mod doctor;
pub mod error;
pub mod install;
pub mod log;
pub mod notify;

use config::{Config, Workspace};
use doctor::{BrDoctorRunner, DoctorRunner};
use error::DoctorError;
use log::{Clock, SystemClock, append_line};
use notify::{Notifier, NtfyNotifier, build_notification, is_notify_worthy};

// ---------------------------------------------------------------------------
// Rolling-log line shapes.
// ---------------------------------------------------------------------------

#[derive(Debug, Serialize)]
struct FindingLine<'a> {
    timestamp: u64,
    kind: &'a str,
    workspace: &'a str,
    code: &'a str,
    severity: &'a str,
    message: &'a str,
    notified: bool,
}

#[derive(Debug, Serialize)]
struct TickLine<'a> {
    timestamp: u64,
    kind: &'a str,
    workspace: &'a str,
    anomalies: usize,
    notified: usize,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<&'a str>,
}

/// What one workspace's probe accomplished in a single cycle.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkspaceCycleReport {
    /// The probed workspace's name.
    pub workspace: String,
    /// Number of anomalies the doctor reported.
    pub anomalies_seen: usize,
    /// Number of notifications successfully posted.
    pub notified: usize,
    /// Number of log lines successfully appended.
    pub logged: usize,
    /// Set when the probe failed (subprocess/parse/spawn error).
    pub error: Option<String>,
}

impl WorkspaceCycleReport {
    fn failed(workspace: String, error: String) -> Self {
        Self {
            workspace,
            anomalies_seen: 0,
            notified: 0,
            logged: 0,
            error: Some(error),
        }
    }
}

// ---------------------------------------------------------------------------
// The probe cycle.
// ---------------------------------------------------------------------------

/// The injected collaborators and policy for one probe cycle.
///
/// Constructed with real implementations in production ([`Cycle::production`])
/// and with in-memory stubs in tests.
pub struct Cycle {
    /// Runs `br doctor` (or a stub).
    pub runner: Arc<dyn DoctorRunner>,
    /// Posts notifications (or records them).
    pub notifier: Arc<dyn Notifier>,
    /// Source of timestamps and file-age instants.
    pub clock: Arc<dyn Clock>,
    /// Directory holding the rolling JSONL logs.
    pub state_dir: PathBuf,
    /// Log rotation size ceiling.
    pub max_size: u64,
    /// Log rotation age ceiling.
    pub max_age: Duration,
}

impl Cycle {
    /// Production wiring: real `br` runner, real `curl` notifier, real clock,
    /// and the default 50 MiB / 30-day rotation policy.
    #[must_use]
    pub fn production(state_dir: PathBuf) -> Self {
        Self {
            runner: Arc::new(BrDoctorRunner),
            notifier: Arc::new(NtfyNotifier),
            clock: Arc::new(SystemClock),
            state_dir,
            max_size: log::MAX_LOG_BYTES,
            max_age: log::MAX_LOG_AGE,
        }
    }
}

/// Borrowed view of the [`Cycle`] collaborators, kept small so
/// [`probe_workspace`] stays within the argument-count lint budget.
struct Probe<'a> {
    runner: &'a dyn DoctorRunner,
    notifier: &'a dyn Notifier,
    clock: &'a dyn Clock,
    state_dir: &'a Path,
    max_size: u64,
    max_age: Duration,
}

/// Probes a single workspace: runs the doctor, logs every anomaly, and posts a
/// notification for each notify-worthy finding. All errors are captured into
/// the returned report rather than propagated, so one bad workspace never
/// aborts the cycle.
fn probe_workspace(cx: &Cx, probe: &Probe<'_>, ws: &Workspace) -> WorkspaceCycleReport {
    let timestamp = probe.clock.now().as_nanos();

    let report = match probe.runner.run(cx, ws) {
        Ok(report) => report,
        Err(err) => {
            let message = err.to_string();
            log_tick(probe, ws, timestamp, 0, 0, Some(&message));
            return WorkspaceCycleReport::failed(ws.name.clone(), message);
        }
    };

    let mut notified = 0usize;
    let mut logged = 0usize;

    for anomaly in &report.anomalies {
        let worthy = is_notify_worthy(&anomaly.code, &anomaly.severity, &ws.severity_threshold);

        let line = FindingLine {
            timestamp,
            kind: "finding",
            workspace: &ws.name,
            code: &anomaly.code,
            severity: &anomaly.severity,
            message: &anomaly.message,
            notified: worthy,
        };
        if append_json(probe, &ws.name, &line) {
            logged += 1;
        }

        if worthy {
            let payload = build_notification(ws, anomaly, timestamp);
            if probe
                .notifier
                .notify(cx, &ws.ntfy_server, &ws.ntfy_topic, &payload)
                .is_ok()
            {
                notified += 1;
            }
        }
    }

    log_tick(probe, ws, timestamp, report.anomalies.len(), notified, None);

    WorkspaceCycleReport {
        workspace: ws.name.clone(),
        anomalies_seen: report.anomalies.len(),
        notified,
        logged,
        error: None,
    }
}

fn log_tick(
    probe: &Probe<'_>,
    ws: &Workspace,
    timestamp: u64,
    anomalies: usize,
    notified: usize,
    error: Option<&str>,
) {
    let line = TickLine {
        timestamp,
        kind: "tick",
        workspace: &ws.name,
        anomalies,
        notified,
        error,
    };
    let _ = append_json(probe, &ws.name, &line);
}

fn append_json<T: Serialize>(probe: &Probe<'_>, workspace: &str, value: &T) -> bool {
    let Ok(line) = serde_json::to_string(value) else {
        return false;
    };
    append_line(
        probe.state_dir,
        workspace,
        &line,
        probe.clock,
        probe.max_size,
        probe.max_age,
    )
    .is_ok()
}

fn outcome_to_report(outcome: Outcome<WorkspaceCycleReport, DoctorError>) -> WorkspaceCycleReport {
    match outcome {
        Outcome::Ok(report) => report,
        Outcome::Err(err) => WorkspaceCycleReport::failed("<unknown>".to_string(), err.to_string()),
        Outcome::Cancelled(reason) => {
            WorkspaceCycleReport::failed("<unknown>".to_string(), format!("cancelled: {reason:?}"))
        }
        Outcome::Panicked(payload) => WorkspaceCycleReport::failed(
            "<unknown>".to_string(),
            format!("panicked: {}", payload.message()),
        ),
    }
}

/// Runs one probe cycle over the configured workspaces.
///
/// Optionally restricted to a single named workspace. Every probe runs under
/// one structured [`JoinSet`] region and is joined before returning, so no task
/// is orphaned. Returns one [`WorkspaceCycleReport`] per probed workspace.
pub async fn run_cycle(
    cx: &Cx,
    config: &Config,
    only: Option<&str>,
    cycle: &Cycle,
) -> Vec<WorkspaceCycleReport> {
    let workspaces: Vec<Workspace> = match only {
        Some(name) => config
            .workspace
            .iter()
            .filter(|ws| ws.name == name)
            .cloned()
            .collect(),
        None => config.workspace.clone(),
    };

    let mut set = asupersync::combinator::JoinSet::in_cx(cx);
    let mut spawn_failures: Vec<WorkspaceCycleReport> = Vec::new();

    for ws in workspaces {
        let runner = Arc::clone(&cycle.runner);
        let notifier = Arc::clone(&cycle.notifier);
        let clock = Arc::clone(&cycle.clock);
        let state_dir = cycle.state_dir.clone();
        let max_size = cycle.max_size;
        let max_age = cycle.max_age;
        let ws_name = ws.name.clone();

        let spawned = set.spawn(cx, move |child| async move {
            let probe = Probe {
                runner: runner.as_ref(),
                notifier: notifier.as_ref(),
                clock: clock.as_ref(),
                state_dir: &state_dir,
                max_size,
                max_age,
            };
            Ok::<WorkspaceCycleReport, DoctorError>(probe_workspace(&child, &probe, &ws))
        });

        if spawned.is_err() {
            // A runtime-wired Cx always carries a spawn gateway; record honestly
            // rather than silently dropping the workspace if it ever does not.
            spawn_failures.push(WorkspaceCycleReport::failed(
                ws_name,
                "probe task could not be spawned".to_string(),
            ));
        }
    }

    let mut reports: Vec<WorkspaceCycleReport> = set
        .join_all(cx)
        .await
        .into_iter()
        .map(outcome_to_report)
        .collect();
    reports.append(&mut spawn_failures);
    reports
}

// ---------------------------------------------------------------------------
// CLI.
// ---------------------------------------------------------------------------

const HELP: &str = "\
beads-doctor — periodic `br doctor` probe with error-severity notifications

USAGE:
    beads-doctor run [--once] [--workspace <name>] [--config <path>]
    beads-doctor install [--workspace <name>] [--config <path>] [--exec <path>]
    beads-doctor uninstall [--workspace <name>] [--config <path>]
    beads-doctor --help | --version

COMMANDS:
    run          Probe every configured workspace once (what the timer calls).
    install      Write the OS scheduler unit(s); prints the activation command.
    uninstall    Remove the scheduler unit(s) this tool manages.

Config: TOML at ~/.config/beads-doctor/workspaces.toml (see README).";

#[derive(Debug, PartialEq, Eq)]
enum Command {
    Run {
        only: Option<String>,
        config: Option<PathBuf>,
    },
    Install {
        only: Option<String>,
        config: Option<PathBuf>,
        exec: Option<PathBuf>,
    },
    Uninstall {
        only: Option<String>,
        config: Option<PathBuf>,
    },
    Help,
    Version,
}

fn parse_args(args: &[OsString]) -> Result<Command, String> {
    let mut iter = args.iter().skip(1).peekable();
    let Some(first) = iter.next() else {
        return Ok(Command::Help);
    };
    let sub = first.to_string_lossy();

    let mut only: Option<String> = None;
    let mut config: Option<PathBuf> = None;
    let mut exec: Option<PathBuf> = None;

    while let Some(flag) = iter.next() {
        match flag.to_string_lossy().as_ref() {
            "--once" => {}
            "--workspace" | "-w" => {
                only = Some(next_value(&mut iter, "--workspace")?);
            }
            "--config" | "-c" => {
                config = Some(PathBuf::from(next_value(&mut iter, "--config")?));
            }
            "--exec" => {
                exec = Some(PathBuf::from(next_value(&mut iter, "--exec")?));
            }
            other => return Err(format!("unknown flag: {other}")),
        }
    }

    match sub.as_ref() {
        "run" => Ok(Command::Run { only, config }),
        "install" => Ok(Command::Install {
            only,
            config,
            exec,
        }),
        "uninstall" => Ok(Command::Uninstall { only, config }),
        "--help" | "-h" | "help" => Ok(Command::Help),
        "--version" | "-V" | "version" => Ok(Command::Version),
        other => Err(format!("unknown command: {other}")),
    }
}

fn next_value<'a, I>(iter: &mut std::iter::Peekable<I>, flag: &str) -> Result<String, String>
where
    I: Iterator<Item = &'a OsString>,
{
    iter.next()
        .map(|v| v.to_string_lossy().into_owned())
        .ok_or_else(|| format!("{flag} requires a value"))
}

fn load_config(path: &Path) -> Result<Config, DoctorError> {
    match std::fs::read_to_string(path) {
        Ok(source) => Config::parse_toml(&source)
            .map_err(|err| DoctorError::Config(format!("{}: {err}", path.display()))),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(Config::default()),
        Err(err) => Err(DoctorError::Config(format!("{}: {err}", path.display()))),
    }
}

/// Async entry point. Parses the CLI, dispatches, and returns a process exit
/// code. Takes `&Cx` first per the asupersync-native convention.
pub async fn async_entry(cx: &Cx, args: Vec<OsString>) -> i32 {
    let command = match parse_args(&args) {
        Ok(command) => command,
        Err(message) => {
            eprintln!("beads-doctor: {message}\n\n{HELP}");
            return 2;
        }
    };

    match command {
        Command::Help => {
            println!("{HELP}");
            0
        }
        Command::Version => {
            println!("beads-doctor {}", env!("CARGO_PKG_VERSION"));
            0
        }
        Command::Run { only, config } => run_command(cx, only, config).await,
        Command::Install {
            only,
            config,
            exec,
        } => install_command(only, config, exec),
        Command::Uninstall { only, config } => uninstall_command(only, config),
    }
}

async fn run_command(cx: &Cx, only: Option<String>, config_path: Option<PathBuf>) -> i32 {
    let path = config_path.unwrap_or_else(config::default_config_path);
    let config = match load_config(&path) {
        Ok(config) => config,
        Err(err) => {
            eprintln!("beads-doctor: {err}");
            return 1;
        }
    };

    if config.workspace.is_empty() {
        println!(
            "beads-doctor: no workspaces configured at {} — nothing to probe",
            path.display()
        );
        return 0;
    }

    let cycle = Cycle::production(config::state_dir());
    let reports = run_cycle(cx, &config, only.as_deref(), &cycle).await;

    let mut any_error = false;
    for report in &reports {
        if let Some(err) = &report.error {
            any_error = true;
            eprintln!("beads-doctor: [{}] probe error: {err}", report.workspace);
        } else {
            println!(
                "beads-doctor: [{}] {} anomalies, {} notified, {} logged",
                report.workspace, report.anomalies_seen, report.notified, report.logged
            );
        }
    }

    i32::from(any_error)
}

fn resolve_exec(exec: Option<PathBuf>) -> String {
    exec.or_else(|| std::env::current_exe().ok())
        .map_or_else(
            || "beads-doctor".to_string(),
            |p| p.to_string_lossy().into_owned(),
        )
}

fn install_command(only: Option<String>, config_path: Option<PathBuf>, exec: Option<PathBuf>) -> i32 {
    let path = config_path.unwrap_or_else(config::default_config_path);
    let config = match load_config(&path) {
        Ok(config) => config,
        Err(err) => {
            eprintln!("beads-doctor: {err}");
            return 1;
        }
    };

    let platform = install::Platform::current();
    let dir = install::default_dir(platform);
    let exec_path = resolve_exec(exec);

    match install::write_units(platform, &dir, &config, &exec_path, only.as_deref()) {
        Ok(written) => {
            for path in &written {
                println!("wrote {}", path.display());
            }
            println!("\nActivate with:");
            for cmd in install::activation_commands(platform, &config, only.as_deref()) {
                println!("    {cmd}");
            }
            0
        }
        Err(err) => {
            eprintln!("beads-doctor: install failed: {err}");
            1
        }
    }
}

fn uninstall_command(only: Option<String>, config_path: Option<PathBuf>) -> i32 {
    let path = config_path.unwrap_or_else(config::default_config_path);
    let config = load_config(&path).unwrap_or_default();
    let platform = install::Platform::current();
    let dir = install::default_dir(platform);

    match install::remove_units(platform, &dir, &config, only.as_deref()) {
        Ok(removed) => {
            if removed.is_empty() {
                println!("beads-doctor: nothing to remove");
            }
            for path in &removed {
                println!("removed {}", path.display());
            }
            0
        }
        Err(err) => {
            eprintln!("beads-doctor: uninstall failed: {err}");
            1
        }
    }
}

/// Builds the asupersync runtime, then runs [`async_entry`] as the root task.
///
/// The root task is spawned through `runtime.handle().spawn(...)` (not a bare
/// `block_on`) so its `Cx` carries a spawn gateway — required for the
/// structured [`JoinSet`] region used by [`run_cycle`].
#[must_use]
pub fn run_main(args: Vec<OsString>) -> i32 {
    let runtime = match asupersync::runtime::RuntimeBuilder::current_thread().build() {
        Ok(runtime) => runtime,
        Err(err) => {
            eprintln!("beads-doctor: failed to start async runtime: {err}");
            return 1;
        }
    };

    runtime.block_on(runtime.handle().spawn(async move {
        let cx = Cx::current().expect("spawned root task carries a Cx");
        async_entry(&cx, args).await
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::doctor::{Anomaly, DoctorReport};
    use crate::notify::Notification;
    use asupersync::runtime::RuntimeBuilder;
    use std::sync::Mutex;
    use std::time::SystemTime;

    // ---- in-memory stubs -------------------------------------------------

    struct StubRunner {
        reports: Mutex<std::collections::HashMap<String, DoctorReport>>,
        probed: Mutex<Vec<String>>,
    }

    impl StubRunner {
        fn new() -> Self {
            Self {
                reports: Mutex::new(std::collections::HashMap::new()),
                probed: Mutex::new(Vec::new()),
            }
        }

        fn with(self, name: &str, anomalies: Vec<Anomaly>) -> Self {
            self.reports.lock().unwrap().insert(
                name.to_string(),
                DoctorReport {
                    health: None,
                    anomalies,
                },
            );
            self
        }
    }

    impl DoctorRunner for StubRunner {
        fn run(&self, _cx: &Cx, ws: &Workspace) -> Result<DoctorReport, DoctorError> {
            self.probed.lock().unwrap().push(ws.name.clone());
            Ok(self
                .reports
                .lock()
                .unwrap()
                .get(&ws.name)
                .cloned()
                .unwrap_or(DoctorReport {
                    health: None,
                    anomalies: Vec::new(),
                }))
        }
    }

    #[derive(Default)]
    struct StubNotifier {
        sent: Mutex<Vec<Notification>>,
    }

    impl Notifier for StubNotifier {
        fn notify(
            &self,
            _cx: &Cx,
            _server: &str,
            _topic: &str,
            payload: &Notification,
        ) -> Result<(), DoctorError> {
            self.sent.lock().unwrap().push(payload.clone());
            Ok(())
        }
    }

    fn anomaly(code: &str, severity: &str) -> Anomaly {
        Anomaly {
            code: code.to_string(),
            severity: severity.to_string(),
            message: format!("{code} happened"),
            remediation: None,
        }
    }

    fn workspace(name: &str, threshold: &str) -> Workspace {
        Workspace {
            name: name.to_string(),
            path: format!("/tmp/{name}"),
            ntfy_topic: format!("topic-{name}"),
            ntfy_server: "https://ntfy.sh".to_string(),
            severity_threshold: threshold.to_string(),
        }
    }

    fn run_in_runtime<F, Fut, T>(body: F) -> T
    where
        F: FnOnce(Cx) -> Fut + Send + 'static,
        Fut: std::future::Future<Output = T> + Send + 'static,
        T: Send + 'static,
    {
        let runtime = RuntimeBuilder::current_thread().build().expect("runtime");
        runtime.block_on(runtime.handle().spawn(async move {
            let cx = Cx::current().expect("root task Cx");
            body(cx).await
        }))
    }

    fn test_cycle(runner: Arc<dyn DoctorRunner>, notifier: Arc<dyn Notifier>, dir: PathBuf) -> Cycle {
        Cycle {
            runner,
            notifier,
            clock: Arc::new(log::ManualClock::new(
                asupersync::Time::from_nanos(42),
                SystemTime::UNIX_EPOCH,
            )),
            state_dir: dir,
            max_size: log::MAX_LOG_BYTES,
            max_age: log::MAX_LOG_AGE,
        }
    }

    #[test]
    fn error_finding_notifies_warn_finding_does_not() {
        // Acceptance 3 + 4: database_corrupt/error -> notified; warn -> log-only.
        let tmp = tempfile::tempdir().expect("tmp");
        let runner = Arc::new(StubRunner::new().with(
            "fsqlite",
            vec![
                anomaly("database_corrupt", "error"),
                anomaly("orphaned_write_lock", "warn"),
            ],
        ));
        let notifier = Arc::new(StubNotifier::default());
        let config = Config {
            workspace: vec![workspace("fsqlite", "error")],
        };

        let notifier_probe = Arc::clone(&notifier);
        let dir = tmp.path().to_path_buf();
        let reports = run_in_runtime(move |cx| async move {
            let cycle = test_cycle(runner, notifier_probe, dir);
            run_cycle(&cx, &config, None, &cycle).await
        });

        assert_eq!(reports.len(), 1);
        let report = &reports[0];
        assert_eq!(report.workspace, "fsqlite");
        assert_eq!(report.anomalies_seen, 2);
        assert_eq!(report.notified, 1);
        // Both anomalies logged, plus a tick line.
        assert_eq!(report.logged, 2);

        let sent = notifier.sent.lock().unwrap();
        assert_eq!(sent.len(), 1, "only the error-severity finding notifies");
        assert_eq!(sent[0].code, "database_corrupt");
        assert_eq!(sent[0].workspace, "fsqlite");
        assert_eq!(sent[0].timestamp, 42);

        // The rolling log recorded 3 lines (2 findings + 1 tick).
        let log_file = log::log_path(tmp.path(), "fsqlite");
        let content = std::fs::read_to_string(log_file).expect("log");
        assert_eq!(content.lines().count(), 3);
        assert!(content.contains("\"notified\":true"));
        assert!(content.contains("\"notified\":false"));
        assert!(content.contains("\"kind\":\"tick\""));
    }

    #[test]
    fn config_driven_pickup_probes_new_workspace() {
        // Acceptance 6: re-running with an extra [[workspace]] probes it.
        let tmp = tempfile::tempdir().expect("tmp");
        let runner = Arc::new(
            StubRunner::new()
                .with("fsqlite", vec![])
                .with("newproj", vec![anomaly("database_corrupt", "error")]),
        );
        let notifier = Arc::new(StubNotifier::default());

        // First run: single workspace.
        let config_before = Config {
            workspace: vec![workspace("fsqlite", "error")],
        };
        let runner1 = Arc::clone(&runner) as Arc<dyn DoctorRunner>;
        let notifier1 = Arc::clone(&notifier) as Arc<dyn Notifier>;
        let dir1 = tmp.path().to_path_buf();
        let reports_before = run_in_runtime(move |cx| async move {
            let cycle = test_cycle(runner1, notifier1, dir1);
            run_cycle(&cx, &config_before, None, &cycle).await
        });
        assert_eq!(reports_before.len(), 1);

        // Second run: config now has a second workspace — picked up with no code
        // change.
        let config_after = Config {
            workspace: vec![workspace("fsqlite", "error"), workspace("newproj", "error")],
        };
        let runner2 = Arc::clone(&runner) as Arc<dyn DoctorRunner>;
        let notifier2 = Arc::clone(&notifier) as Arc<dyn Notifier>;
        let dir2 = tmp.path().to_path_buf();
        let reports_after = run_in_runtime(move |cx| async move {
            let cycle = test_cycle(runner2, notifier2, dir2);
            run_cycle(&cx, &config_after, None, &cycle).await
        });
        assert_eq!(reports_after.len(), 2);

        let probed = runner.probed.lock().unwrap();
        assert!(probed.contains(&"newproj".to_string()));
        // newproj's corruption produced a notification on the second run.
        let sent = notifier.sent.lock().unwrap();
        assert!(sent.iter().any(|n| n.workspace == "newproj"));
    }

    #[test]
    fn only_filter_probes_single_workspace() {
        let tmp = tempfile::tempdir().expect("tmp");
        let runner = Arc::new(
            StubRunner::new()
                .with("a", vec![anomaly("database_corrupt", "error")])
                .with("b", vec![anomaly("database_corrupt", "error")]),
        );
        let notifier = Arc::new(StubNotifier::default());
        let config = Config {
            workspace: vec![workspace("a", "error"), workspace("b", "error")],
        };
        let dir = tmp.path().to_path_buf();
        let reports = run_in_runtime(move |cx| async move {
            let cycle = test_cycle(runner, notifier, dir);
            run_cycle(&cx, &config, Some("b"), &cycle).await
        });
        assert_eq!(reports.len(), 1);
        assert_eq!(reports[0].workspace, "b");
    }

    #[test]
    fn runner_error_is_captured_not_propagated() {
        struct FailRunner;
        impl DoctorRunner for FailRunner {
            fn run(&self, _cx: &Cx, _ws: &Workspace) -> Result<DoctorReport, DoctorError> {
                Err(DoctorError::NonUtf8Output)
            }
        }
        let tmp = tempfile::tempdir().expect("tmp");
        let config = Config {
            workspace: vec![workspace("fsqlite", "error")],
        };
        let dir = tmp.path().to_path_buf();
        let reports = run_in_runtime(move |cx| async move {
            let cycle = test_cycle(
                Arc::new(FailRunner),
                Arc::new(StubNotifier::default()),
                dir,
            );
            run_cycle(&cx, &config, None, &cycle).await
        });
        assert_eq!(reports.len(), 1);
        assert!(reports[0].error.is_some());
    }

    #[test]
    fn parse_args_variants() {
        let os = |s: &str| OsString::from(s);
        assert_eq!(
            parse_args(&[os("beads-doctor"), os("run")]).unwrap(),
            Command::Run {
                only: None,
                config: None
            }
        );
        assert_eq!(
            parse_args(&[os("beads-doctor"), os("run"), os("--once"), os("-w"), os("fsqlite")])
                .unwrap(),
            Command::Run {
                only: Some("fsqlite".to_string()),
                config: None
            }
        );
        assert!(matches!(
            parse_args(&[os("beads-doctor")]).unwrap(),
            Command::Help
        ));
        assert!(parse_args(&[os("beads-doctor"), os("bogus")]).is_err());
        assert!(parse_args(&[os("beads-doctor"), os("run"), os("--workspace")]).is_err());
    }
}
