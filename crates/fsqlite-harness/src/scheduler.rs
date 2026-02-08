//! Scheduler priority lanes for FrankenSQLite verification harness (§4.20, bd-3go.13).
//!
//! Wraps asupersync's [`Scheduler`] with FrankenSQLite-specific lane assignment
//! logic: Cancel (highest), Timed (EDF), and Ready (background) lanes.
//!
//! Implementation is tracked by bead `bd-3go.13`.
