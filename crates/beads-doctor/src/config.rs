//! Configuration model for beads-doctor.
//!
//! The config lives at `~/.config/beads-doctor/workspaces.toml` (respecting
//! `XDG_CONFIG_HOME`). It is plain TOML — the workspace already vends `serde`,
//! `serde_json`, and `toml`, but no YAML crate, so TOML is the native choice.
//!
//! Config is picked up on every `run`: adding a new `[[workspace]]` block and
//! re-running `run` probes the new workspace with no code change.

use std::path::PathBuf;

use serde::{Deserialize, Serialize};

/// A single monitored beads workspace.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct Workspace {
    /// Stable identifier used for log filenames, notification payloads, and
    /// per-workspace scheduler units.
    pub name: String,
    /// Filesystem path of the workspace `br doctor` runs against.
    pub path: String,
    /// ntfy topic that error-severity findings are posted to.
    pub ntfy_topic: String,
    /// ntfy server base URL. Defaults to the public `https://ntfy.sh`.
    #[serde(default = "default_ntfy_server")]
    pub ntfy_server: String,
    /// Minimum severity that triggers a notification. Findings below this rank
    /// (and not on the always-notify code list) are logged only. Defaults to
    /// `"error"`, so `degraded`/`warn` findings are log-only.
    #[serde(default = "default_severity_threshold")]
    pub severity_threshold: String,
}

fn default_ntfy_server() -> String {
    "https://ntfy.sh".to_string()
}

fn default_severity_threshold() -> String {
    "error".to_string()
}

/// The parsed `workspaces.toml` file.
#[derive(Debug, Clone, Default, PartialEq, Eq, Deserialize, Serialize)]
pub struct Config {
    /// One entry per `[[workspace]]` table in the TOML file.
    #[serde(default)]
    pub workspace: Vec<Workspace>,
}

impl Config {
    /// Parses a TOML document into a [`Config`].
    ///
    /// # Errors
    ///
    /// Returns the underlying [`toml::de::Error`] when the document is not valid
    /// TOML or does not match the schema.
    pub fn parse_toml(source: &str) -> Result<Self, toml::de::Error> {
        toml::from_str(source)
    }

    /// Returns the workspace with the given name, if configured.
    #[must_use]
    pub fn workspace(&self, name: &str) -> Option<&Workspace> {
        self.workspace.iter().find(|ws| ws.name == name)
    }
}

/// Resolves the default config path, honoring `XDG_CONFIG_HOME` then `HOME`.
///
/// Falls back to a relative path when neither environment variable is set,
/// which keeps the function total (no panics, no ambient failure).
#[must_use]
pub fn default_config_path() -> PathBuf {
    config_dir().join("beads-doctor").join("workspaces.toml")
}

/// Resolves the base config directory (`$XDG_CONFIG_HOME` or `$HOME/.config`).
#[must_use]
pub fn config_dir() -> PathBuf {
    if let Some(xdg) = std::env::var_os("XDG_CONFIG_HOME").filter(|v| !v.is_empty()) {
        return PathBuf::from(xdg);
    }
    home_dir().join(".config")
}

/// Resolves the base state directory (`$XDG_STATE_HOME` or `$HOME/.local/state`).
///
/// Rolling per-workspace JSONL logs live under `beads-doctor/` inside this
/// directory.
#[must_use]
pub fn state_dir() -> PathBuf {
    if let Some(xdg) = std::env::var_os("XDG_STATE_HOME").filter(|v| !v.is_empty()) {
        return PathBuf::from(xdg).join("beads-doctor");
    }
    home_dir().join(".local").join("state").join("beads-doctor")
}

fn home_dir() -> PathBuf {
    std::env::var_os("HOME").map_or_else(|| PathBuf::from("."), PathBuf::from)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_workspaces_with_defaults() {
        let src = r#"
[[workspace]]
name = "fsqlite"
path = "/data/projects/frankensqlite"
ntfy_topic = "beads-fsqlite"

[[workspace]]
name = "other"
path = "/data/projects/other"
ntfy_topic = "beads-other"
severity_threshold = "warn"
ntfy_server = "https://ntfy.example.com"
"#;
        let config = Config::parse_toml(src).expect("valid TOML");
        assert_eq!(config.workspace.len(), 2);

        let fsqlite = &config.workspace[0];
        assert_eq!(fsqlite.name, "fsqlite");
        assert_eq!(fsqlite.path, "/data/projects/frankensqlite");
        assert_eq!(fsqlite.ntfy_topic, "beads-fsqlite");
        // Defaults applied.
        assert_eq!(fsqlite.severity_threshold, "error");
        assert_eq!(fsqlite.ntfy_server, "https://ntfy.sh");

        let other = &config.workspace[1];
        assert_eq!(other.severity_threshold, "warn");
        assert_eq!(other.ntfy_server, "https://ntfy.example.com");
    }

    #[test]
    fn config_driven_pickup_sees_new_workspace() {
        // Acceptance 6: adding a `[[workspace]]` and re-parsing surfaces it.
        let before = r#"
[[workspace]]
name = "fsqlite"
path = "/data/projects/frankensqlite"
ntfy_topic = "beads-fsqlite"
"#;
        let config_before = Config::parse_toml(before).expect("valid TOML");
        assert_eq!(config_before.workspace.len(), 1);
        assert!(config_before.workspace("newproj").is_none());

        let after = format!(
            "{before}\n[[workspace]]\nname = \"newproj\"\npath = \"/data/projects/newproj\"\nntfy_topic = \"beads-newproj\"\n"
        );
        let config_after = Config::parse_toml(&after).expect("valid TOML");
        assert_eq!(config_after.workspace.len(), 2);
        assert!(config_after.workspace("newproj").is_some());
    }

    #[test]
    fn empty_config_is_valid() {
        let config = Config::parse_toml("").expect("empty is valid");
        assert!(config.workspace.is_empty());
    }

    #[test]
    fn config_and_state_dirs_respect_xdg() {
        // These use process-global env; assert only shape, not exact host paths.
        let cfg = default_config_path();
        assert!(cfg.ends_with("beads-doctor/workspaces.toml"));
        let state = state_dir();
        assert!(state.ends_with("beads-doctor"));
    }
}
