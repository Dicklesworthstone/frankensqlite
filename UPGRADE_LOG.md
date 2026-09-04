# Dependency Upgrade Log

**Date:** 2026-09-03 · **Project:** frankensqlite · **Language:** Rust (nightly, edition 2024)
**Method:** `cargo update` (semver-compatible lockfile refresh) verified on a quiet host (trj),
then landed. No manifest version constraints were changed — this is a lockfile-only refresh.

## Summary
- **Transitive/lockfile bumps applied:** 45 (all semver-compatible, `Cargo.lock` only)
- **Pinned back (breaks build):** 1 — `tinyvec` 1.13.0 → held at 1.12.0
- **Direct-dep minor updates available but deferred:** 2 (need manifest edits + per-dep testing)

## Applied (Cargo.lock refresh, verified)
Notable bumps:
- `asupersync` 0.4.8 → 0.4.10  (the async runtime — validated against the concurrency canon)
- `franken-decision`/`franken-evidence`/`franken-kernel` 0.4.8 → 0.4.9
- `blake3` 1.8.6 → 1.8.7, `aes-gcm` 0.11.0 → 0.11.1, `aes` 0.9.2 → 0.9.3, `chacha20` 0.10.1 → 0.10.2
- `smallvec` 1.15.2 → 1.16.0, `flate2` 1.1.9 → 1.1.10 (pulls `zlib-rs` 0.6.7), `miniz_oxide` 0.8.9 → 0.9.1
- `icu_*` 2.2.x → 2.3.x, `log` 0.4.33 → 0.4.34, `mio` 1.2.2 → 1.2.3, `rand` 0.8.7 → 0.8.8, and ~30 more
- Churn: −`arrayref`, +`itertools`, +`zlib-rs` (transitive)

**Verification (trj, refreshed lock):**
- `cargo check --workspace --all-targets`: 0
- `cargo clippy --workspace --all-targets -- -D warnings`: 0
- `mvcc_concurrent_writers`: 15 passed / 0 failed
- `bd_1r0ha_3_concurrent_writer_e2e`: 4 passed / 0 failed / 1 ignored
- `fsqlite-ext-fts5 --lib`: 332 passed / 0 failed
- `bd_fts5_lazy_ranked_parity` (lazy + in-memory ranked keepers): 2 passed / 0 failed

## Pinned back / skipped

### tinyvec: 1.13.0 → held at 1.12.0
- **Reason:** 1.13.0 fails to compile in this workspace: `error: cannot find macro 'vec' in this scope`
  (`could not compile 'tinyvec' (lib)`). A blanket `cargo update` that pulled 1.13.0 broke
  `cargo check` for the whole workspace.
- **Action:** `cargo update -p tinyvec --precise 1.12.0` after the refresh; the rest of the update is
  retained. Revisit 1.13.0 when the macro/`alloc`-feature issue is resolved upstream (or a later
  1.13.x lands).

## Deferred (direct-dep minor bumps — need manifest edits + per-dep test, not shipped in this release)

### smallvec: manifest still `1.15`-era constraint (lock now 1.16.0)
- Lockfile already at 1.16.0 via the refresh; the manifest constraint can be tightened in a
  follow-up if desired. No action needed for correctness.

### jsonschema (dev-dep, fsqlite-e2e): 0.48.5 → 0.52.1
- **Reason deferred:** a 4-minor jump on a dev-only conformance dep; warrants its own
  breaking-change review + test pass per the one-at-a-time policy. Not on the shipped path
  (dev-dependency), so excluded from this release's refresh.

## Notes
- This refresh is `Cargo.lock`-only; no `Cargo.toml` version constraints changed, so the published
  crates' declared dependency ranges are unchanged.
- The async-runtime bump (`asupersync` 0.4.10) is the highest-risk item and was gated on the
  concurrency canon above before landing.
