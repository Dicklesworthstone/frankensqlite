//! Error type shared across the probe pipeline.

use thiserror::Error;

/// Errors surfaced while probing a workspace, notifying, or handling config.
///
/// Every variant is `Send + 'static`, so a [`DoctorError`] can be the error arm
/// of a structured-concurrency task result ([`asupersync::Outcome`]).
#[derive(Debug, Error)]
pub enum DoctorError {
    /// The doctor subprocess could not be spawned or waited on.
    #[error("failed to run `{command}` in {dir}: {source}")]
    Spawn {
        /// The command that failed to launch.
        command: String,
        /// The working directory it was launched in.
        dir: String,
        /// The underlying OS error.
        #[source]
        source: std::io::Error,
    },

    /// The doctor subprocess produced output that was not valid UTF-8.
    #[error("`br doctor` produced non-UTF-8 output")]
    NonUtf8Output,

    /// The doctor JSON could not be parsed.
    #[error("failed to parse `br doctor --json` output: {0}")]
    Json(#[from] serde_json::Error),

    /// The notifier subprocess failed.
    #[error("notification via `{command}` failed: {reason}")]
    Notify {
        /// The command used to post the notification.
        command: String,
        /// A human-readable failure reason.
        reason: String,
    },

    /// A filesystem operation (log append/rotate) failed.
    #[error("i/o error: {0}")]
    Io(#[from] std::io::Error),

    /// Configuration could not be read or parsed.
    #[error("configuration error: {0}")]
    Config(String),
}
