# Dependency Upgrade Log

**Date:** 2026-08-11  |  **Project:** frankensqlite  |  **Language:** Rust (nightly, edition 2024)

## Summary

- **Updated:** asupersync 0.3.10 → 0.4.3 (+ lockstep franken-decision / franken-evidence / franken-kernel)
- **Compatible-range sweep:** see below
- **Failed:** none

## Updates

### asupersync: 0.3.10 → 0.4.3

- **Breaking:** none for this codebase. v0.4.0's only breaking items
  (`TrackedSender::try_reserve` takes `&Cx`; `TrackedPermit::try_send` returns
  `CommittedProof<SendPermit>`) already shipped in v0.3.10, which this
  workspace compiled against; v0.4.0 is the semver re-anchor of that API.
  v0.4.1–v0.4.3 are additive/internal (owned safe blocking kernel, panic
  containment at owned poll boundaries, io_uring capability control plane,
  scheduler/cancellation fixes).
- **Verification:** `cargo update -p asupersync` (locks 4 packages);
  `cargo check --workspace` clean; `cargo test -p fsqlite --lib` 658 passed /
  0 failed; `cargo test -p fsqlite-core --lib -- runtime region async`
  64 passed / 0 failed; `cargo clippy --workspace --all-targets -- -D warnings`
  clean.
- **Follow-up:** a usage audit against the 0.4.x surface (parked `block_on`
  kernel vs the bd-zavyn block_on instrument drift; io_uring capability plane
  vs the currently-inactive uring data path) runs separately; findings become
  beads.

## Notes

- Version rules honored: nightly toolchain pin untouched; path deps and
  intra-workspace 0.2.1 pins untouched.
