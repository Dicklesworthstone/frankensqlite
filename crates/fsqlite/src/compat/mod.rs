//! rusqlite-compatible adapter layer for FrankenSQLite.
//!
//! Provides familiar macros, traits, and wrappers so that migrating from
//! `rusqlite` to `fsqlite` is mostly mechanical import swaps.

mod batch;
#[cfg(feature = "session")]
pub mod changeset;
#[cfg(all(feature = "session", feature = "native", not(target_arch = "wasm32")))]
pub mod changeset_stream;
mod connection;
mod flags;
mod optional;
mod params;
#[cfg(all(feature = "native", not(target_arch = "wasm32"), unix))]
pub mod recovery;
mod row;
mod transaction;

pub use batch::*;
pub use connection::*;
pub use flags::*;
pub use optional::*;
pub use params::*;
pub use row::*;
pub use transaction::*;
