//! Dedicated Cargo benchmark entrypoint for the MIN/MAX prefix contract.
//!
//! The implementation remains shared with the ignored integration-test
//! profiler, while this distinct target path prevents Cargo from registering
//! one source file as both an integration test and a benchmark.

#[path = "../tests/minmax_prefix_profile.rs"]
mod contract;

fn main() {
    contract::run_entrypoint();
}
