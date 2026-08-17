//! Platform scheduler installers.
//!
//! Every generator is a pure function returning file content, so they are
//! unit-tested on any host (the Linux sandbox exercises systemd, launchd, and
//! Task Scheduler generators alike). The [`install`]/[`uninstall`] entry points
//! write/remove those files for the current OS; actually *activating* the timer
//! (`systemctl --user enable`, `launchctl load`, `schtasks /create`) is a
//! platform step the tool prints but does not perform in-process.

use std::fs;
use std::path::{Path, PathBuf};

use crate::config::{Config, Workspace, config_dir};
use crate::error::DoctorError;

/// The 30-minute cadence, expressed for each scheduler.
const SYSTEMD_INTERVAL: &str = "30min";
const LAUNCHD_INTERVAL_SECS: u32 = 1800;
const WINDOWS_INTERVAL: &str = "PT30M";

// ---------------------------------------------------------------------------
// systemd (Linux) — templated `@` units instantiated per workspace.
// ---------------------------------------------------------------------------

/// `beads-doctor@.service` template content (instance `%i` is the workspace).
#[must_use]
pub fn systemd_service_template(exec_path: &str) -> String {
    format!(
        "[Unit]\n\
         Description=beads-doctor probe for workspace %i\n\
         After=network-online.target\n\
         Wants=network-online.target\n\
         \n\
         [Service]\n\
         Type=oneshot\n\
         Nice=10\n\
         ExecStart={exec_path} run --once --workspace %i\n"
    )
}

/// `beads-doctor@.timer` template content (30-minute cadence).
#[must_use]
pub fn systemd_timer_template() -> String {
    format!(
        "[Unit]\n\
         Description=beads-doctor 30-minute schedule for workspace %i\n\
         \n\
         [Timer]\n\
         OnBootSec=5min\n\
         OnUnitActiveSec={SYSTEMD_INTERVAL}\n\
         OnCalendar=*:0/30\n\
         Persistent=true\n\
         \n\
         [Install]\n\
         WantedBy=timers.target\n"
    )
}

/// The instance timer unit name an operator enables for `workspace`.
#[must_use]
pub fn systemd_instance_unit(workspace: &str) -> String {
    format!("beads-doctor@{workspace}.timer")
}

/// The `systemctl --user` command that activates the workspace's timer.
#[must_use]
pub fn systemd_enable_command(workspace: &str) -> String {
    format!(
        "systemctl --user enable --now {}",
        systemd_instance_unit(workspace)
    )
}

// ---------------------------------------------------------------------------
// launchd (macOS) — one plist per workspace.
// ---------------------------------------------------------------------------

/// The launchd label for a workspace.
#[must_use]
pub fn launchd_label(workspace: &str) -> String {
    format!("com.beads.doctor.{workspace}")
}

/// The launchd plist filename for a workspace.
#[must_use]
pub fn launchd_plist_filename(workspace: &str) -> String {
    format!("{}.plist", launchd_label(workspace))
}

/// `com.beads.doctor.<workspace>.plist` content (StartInterval 1800s + a
/// 30-minute calendar schedule).
#[must_use]
pub fn launchd_plist(workspace: &str, exec_path: &str) -> String {
    let label = launchd_label(workspace);
    format!(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n\
         <!DOCTYPE plist PUBLIC \"-//Apple//DTD PLIST 1.0//EN\" \
         \"http://www.apple.com/DTDs/PropertyList-1.0.dtd\">\n\
         <plist version=\"1.0\">\n\
         <dict>\n\
         \x20 <key>Label</key>\n\
         \x20 <string>{label}</string>\n\
         \x20 <key>ProgramArguments</key>\n\
         \x20 <array>\n\
         \x20   <string>{exec_path}</string>\n\
         \x20   <string>run</string>\n\
         \x20   <string>--once</string>\n\
         \x20   <string>--workspace</string>\n\
         \x20   <string>{workspace}</string>\n\
         \x20 </array>\n\
         \x20 <key>StartInterval</key>\n\
         \x20 <integer>{LAUNCHD_INTERVAL_SECS}</integer>\n\
         \x20 <key>StartCalendarInterval</key>\n\
         \x20 <array>\n\
         \x20   <dict><key>Minute</key><integer>0</integer></dict>\n\
         \x20   <dict><key>Minute</key><integer>30</integer></dict>\n\
         \x20 </array>\n\
         \x20 <key>RunAtLoad</key>\n\
         \x20 <true/>\n\
         </dict>\n\
         </plist>\n"
    )
}

// ---------------------------------------------------------------------------
// Task Scheduler (Windows) — one task XML per workspace.
// ---------------------------------------------------------------------------

/// The Task Scheduler task name for a workspace.
#[must_use]
pub fn windows_task_name(workspace: &str) -> String {
    format!("beads-doctor-{workspace}")
}

/// The Task Scheduler XML filename for a workspace.
#[must_use]
pub fn windows_task_filename(workspace: &str) -> String {
    format!("{}.xml", windows_task_name(workspace))
}

/// Task Scheduler XML with a 30-minute (`PT30M`) repetition.
#[must_use]
pub fn windows_task_xml(workspace: &str, exec_path: &str) -> String {
    let task = windows_task_name(workspace);
    format!(
        "<?xml version=\"1.0\" encoding=\"UTF-16\"?>\n\
         <Task version=\"1.2\" \
         xmlns=\"http://schemas.microsoft.com/windows/2004/02/mit/task\">\n\
         \x20 <RegistrationInfo>\n\
         \x20   <URI>\\{task}</URI>\n\
         \x20   <Description>beads-doctor probe for workspace {workspace}</Description>\n\
         \x20 </RegistrationInfo>\n\
         \x20 <Triggers>\n\
         \x20   <TimeTrigger>\n\
         \x20     <StartBoundary>2024-01-01T00:00:00</StartBoundary>\n\
         \x20     <Enabled>true</Enabled>\n\
         \x20     <Repetition>\n\
         \x20       <Interval>{WINDOWS_INTERVAL}</Interval>\n\
         \x20       <StopAtDurationEnd>false</StopAtDurationEnd>\n\
         \x20     </Repetition>\n\
         \x20   </TimeTrigger>\n\
         \x20 </Triggers>\n\
         \x20 <Actions>\n\
         \x20   <Exec>\n\
         \x20     <Command>{exec_path}</Command>\n\
         \x20     <Arguments>run --once --workspace {workspace}</Arguments>\n\
         \x20   </Exec>\n\
         \x20 </Actions>\n\
         </Task>\n"
    )
}

// ---------------------------------------------------------------------------
// Target directories.
// ---------------------------------------------------------------------------

/// `~/.config/systemd/user` — where user timer/service units live.
#[must_use]
pub fn systemd_unit_dir() -> PathBuf {
    config_dir().join("systemd").join("user")
}

/// `~/Library/LaunchAgents` — where per-user launchd agents live.
#[must_use]
pub fn launchd_dir() -> PathBuf {
    std::env::var_os("HOME").map_or_else(
        || PathBuf::from("."),
        |home| PathBuf::from(home).join("Library").join("LaunchAgents"),
    )
}

/// Directory the generated Task Scheduler XMLs are staged in before `schtasks`
/// imports them.
#[must_use]
pub fn windows_task_dir() -> PathBuf {
    config_dir().join("beads-doctor").join("tasks")
}

/// Which platform [`install`] targets.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Platform {
    /// Linux systemd user units.
    Systemd,
    /// macOS launchd agents.
    Launchd,
    /// Windows Task Scheduler.
    Windows,
}

impl Platform {
    /// The platform this build runs on.
    #[must_use]
    pub const fn current() -> Self {
        #[cfg(target_os = "macos")]
        {
            Self::Launchd
        }
        #[cfg(target_os = "windows")]
        {
            Self::Windows
        }
        #[cfg(not(any(target_os = "macos", target_os = "windows")))]
        {
            Self::Systemd
        }
    }
}

fn selected<'a>(config: &'a Config, only: Option<&str>) -> Vec<&'a Workspace> {
    match only {
        Some(name) => config.workspace.iter().filter(|w| w.name == name).collect(),
        None => config.workspace.iter().collect(),
    }
}

/// Writes scheduler files for `platform` into `base_dir`, returning the paths
/// written. Kept dependency-injectable on `base_dir` so it is testable in a
/// tempdir without touching the real home directory.
///
/// # Errors
///
/// Propagates filesystem errors from directory creation or file writes.
pub fn write_units(
    platform: Platform,
    base_dir: &Path,
    config: &Config,
    exec_path: &str,
    only: Option<&str>,
) -> Result<Vec<PathBuf>, DoctorError> {
    fs::create_dir_all(base_dir)?;
    let mut written = Vec::new();
    match platform {
        Platform::Systemd => {
            let service = base_dir.join("beads-doctor@.service");
            fs::write(&service, systemd_service_template(exec_path))?;
            written.push(service);
            let timer = base_dir.join("beads-doctor@.timer");
            fs::write(&timer, systemd_timer_template())?;
            written.push(timer);
        }
        Platform::Launchd => {
            for ws in selected(config, only) {
                let path = base_dir.join(launchd_plist_filename(&ws.name));
                fs::write(&path, launchd_plist(&ws.name, exec_path))?;
                written.push(path);
            }
        }
        Platform::Windows => {
            for ws in selected(config, only) {
                let path = base_dir.join(windows_task_filename(&ws.name));
                fs::write(&path, windows_task_xml(&ws.name, exec_path))?;
                written.push(path);
            }
        }
    }
    Ok(written)
}

/// The manual activation commands to run after [`write_units`].
#[must_use]
pub fn activation_commands(platform: Platform, config: &Config, only: Option<&str>) -> Vec<String> {
    match platform {
        Platform::Systemd => selected(config, only)
            .iter()
            .map(|ws| systemd_enable_command(&ws.name))
            .collect(),
        Platform::Launchd => selected(config, only)
            .iter()
            .map(|ws| {
                format!(
                    "launchctl load ~/Library/LaunchAgents/{}",
                    launchd_plist_filename(&ws.name)
                )
            })
            .collect(),
        Platform::Windows => selected(config, only)
            .iter()
            .map(|ws| {
                format!(
                    "schtasks /create /tn {} /xml {}",
                    windows_task_name(&ws.name),
                    windows_task_filename(&ws.name)
                )
            })
            .collect(),
    }
}

/// Removes the scheduler files [`write_units`] would have written under
/// `base_dir`, returning the paths actually removed.
///
/// # Errors
///
/// Propagates filesystem errors other than "not found".
pub fn remove_units(
    platform: Platform,
    base_dir: &Path,
    config: &Config,
    only: Option<&str>,
) -> Result<Vec<PathBuf>, DoctorError> {
    let mut candidates = Vec::new();
    match platform {
        Platform::Systemd => {
            candidates.push(base_dir.join("beads-doctor@.service"));
            candidates.push(base_dir.join("beads-doctor@.timer"));
        }
        Platform::Launchd => {
            for ws in selected(config, only) {
                candidates.push(base_dir.join(launchd_plist_filename(&ws.name)));
            }
        }
        Platform::Windows => {
            for ws in selected(config, only) {
                candidates.push(base_dir.join(windows_task_filename(&ws.name)));
            }
        }
    }

    let mut removed = Vec::new();
    for path in candidates {
        match fs::remove_file(&path) {
            Ok(()) => removed.push(path),
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
            Err(err) => return Err(err.into()),
        }
    }
    Ok(removed)
}

/// The default install directory for a platform.
#[must_use]
pub fn default_dir(platform: Platform) -> PathBuf {
    match platform {
        Platform::Systemd => systemd_unit_dir(),
        Platform::Launchd => launchd_dir(),
        Platform::Windows => windows_task_dir(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_config() -> Config {
        Config {
            workspace: vec![Workspace {
                name: "fsqlite".to_string(),
                path: "/data/projects/frankensqlite".to_string(),
                ntfy_topic: "beads-fsqlite".to_string(),
                ntfy_server: "https://ntfy.sh".to_string(),
                severity_threshold: "error".to_string(),
            }],
        }
    }

    #[test]
    fn systemd_units_carry_interval_and_workspace_instance() {
        // Acceptance 6 (installer filegen): 30-minute cadence + workspace name.
        let service = systemd_service_template("/usr/local/bin/beads-doctor");
        let timer = systemd_timer_template();
        assert!(timer.contains("OnUnitActiveSec=30min"));
        assert!(timer.contains("OnCalendar=*:0/30"));
        assert!(service.contains("run --once --workspace %i"));
        // The workspace name lives in the enabled instance unit.
        assert!(systemd_instance_unit("fsqlite").contains("fsqlite"));
        assert!(systemd_enable_command("fsqlite").contains("beads-doctor@fsqlite.timer"));
    }

    #[test]
    fn launchd_plist_carries_interval_and_workspace_name() {
        let plist = launchd_plist("fsqlite", "/usr/local/bin/beads-doctor");
        assert!(plist.contains("fsqlite"));
        assert!(plist.contains("1800"));
        assert!(plist.contains("com.beads.doctor.fsqlite"));
    }

    #[test]
    fn windows_task_carries_interval_and_workspace_name() {
        let xml = windows_task_xml("fsqlite", "C:/tools/beads-doctor.exe");
        assert!(xml.contains("fsqlite"));
        assert!(xml.contains("PT30M"));
        assert!(xml.contains("beads-doctor-fsqlite"));
    }

    #[test]
    fn write_and_remove_systemd_units_roundtrip() {
        let dir = tempfile::tempdir().expect("tempdir");
        let config = sample_config();
        let written = write_units(
            Platform::Systemd,
            dir.path(),
            &config,
            "/usr/local/bin/beads-doctor",
            None,
        )
        .expect("write");
        assert_eq!(written.len(), 2);
        for path in &written {
            assert!(path.exists());
        }
        let removed =
            remove_units(Platform::Systemd, dir.path(), &config, None).expect("remove");
        assert_eq!(removed.len(), 2);
        for path in &written {
            assert!(!path.exists());
        }
    }

    #[test]
    fn write_launchd_and_windows_per_workspace() {
        let dir = tempfile::tempdir().expect("tempdir");
        let config = sample_config();
        let plists =
            write_units(Platform::Launchd, dir.path(), &config, "/bin/bd", None).expect("plist");
        assert_eq!(plists.len(), 1);
        assert!(plists[0].to_string_lossy().contains("com.beads.doctor.fsqlite.plist"));

        let tasks =
            write_units(Platform::Windows, dir.path(), &config, "bd.exe", None).expect("task");
        assert_eq!(tasks.len(), 1);
        assert!(tasks[0].to_string_lossy().contains("beads-doctor-fsqlite.xml"));
    }

    #[test]
    fn activation_commands_name_the_workspace() {
        let config = sample_config();
        let cmds = activation_commands(Platform::Systemd, &config, None);
        assert_eq!(cmds.len(), 1);
        assert!(cmds[0].contains("fsqlite"));
    }
}
