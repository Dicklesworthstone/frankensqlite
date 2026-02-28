//! rusqlite-compatible adapter layer for FrankenSQLite.
//!
//! Provides familiar macros, traits, and wrappers so that migrating from
//! `rusqlite` to `fsqlite` is mostly mechanical import swaps.

mod batch;
mod connection;
mod flags;
mod optional;
mod params;
mod row;
mod transaction;

pub use batch::*;
pub use connection::*;
pub use flags::*;
pub use optional::*;
pub use params::*;
pub use row::*;
pub use transaction::*;
