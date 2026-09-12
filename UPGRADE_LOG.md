# Dependency Upgrade Log

## September 12, 2026 — release dependency review in progress

The owner requested latest stable dependency updates before the next DSR,
crates.io and Homebrew release. Updates will be researched and tested one at
a time through RCH. Existing path/git dependencies and prereleases retain their
declared policy. The nightly toolchain and concurrent-writer defaults remain
unchanged. Completed upgrades and their individual checks are recorded below.

The live direct-dependency inventory is retained at
`/tmp/frankensqlite-dependency-research-live-20260912.json`. Patch candidates
include bitflags, smallvec, toml, crossbeam, io-uring, trybuild and asupersync;
Argon2, ftui, jsonschema and syn require breaking-change review. The earlier
tinyvec build failure below must be checked before accepting a newer version.

Release qualification is still incomplete: the all-features run exhausted its
four-hour RCH timeout during compilation, and the historical performance
comparison remains red. Earlier focused checks are not final updated-lockfile
acceptance. Homebrew's existing `fsqlite` formula is at 0.3.9 and needs an update
using verified hashes from the eventual new release assets.

Interim RustSec audit (bitflags/tinyvec upgraded, smallvec under test): 419
dependencies, zero reported vulnerabilities, no advisory warnings. Database
commit `b50980aad8b8f14f77e25a97b32dd94bf008b0af` contains 1,243 advisories and
was fetched for this run. The first RCH worker lacked `cargo-audit`; a verified
copy of the already installed Linux audit binary ran through RCH successfully.
This does not replace the final audit after all upgrades.

### bitflags 2.13.1 → 2.13.2 — passed

- The published patch moves const declarations outside nested const blocks;
  MSRV remains 1.56.0. No public API migration is declared.
- Research: [exact packaged changelog](https://github.com/bitflags/bitflags/blob/80ce9b545acb0bd42150695fc351889cac1d8eb4/CHANGELOG.md).
- Only the lockfile version and published registry checksum changed; the
  existing `2.13` requirement and serde feature are unchanged.
- Native macOS `fsqlite-types --lib`: 562 passed, zero failed/ignored,
  strict RCH job `30017370403635261`. All 1,616 post-run source hashes
  match manifest `91d724fdf5d58fb1a4bee62734b6be4641328b87215345c3383d837c741e12d4`.
- Test log SHA256: `838846882d5ee47c55876bf8b8a157fd0e21553ce12546cf02136de59f77951b`.
- Native macOS workspace/all-targets compilation passed in strict RCH
  `30017370403635263`; all 1,616 post-run source hashes match the same manifest.
  Check log SHA256: `e7ec0cc7cef30e15c4da3fd3dd2d7f3e8542547ff1678fba478f323ab2a3584f`.
- Final updated-dependency workspace tests, all-features checks and security
  audit remain separate release requirements.

### tinyvec 1.12.0 → 1.13.2 — passed; existing WASM warnings retained

- The previously rejected 1.13.0 is not retried. Upstream 1.13.1 fixes
  allocation without `std`, and 1.13.2 fixes a further `no_std` macro expansion
  bug. [Exact changelog](https://github.com/Lokathor/tinyvec/blob/5ae3e523dd46392d45f929591889430d1438ae5e/changelog.md).
- Only the lockfile version/checksum changes. This dependency is reached
  through `asupersync` → `unicode-normalization` → `tinyvec`.
- Native types tests passed 562/562, zero ignored, in strict RCH
  `30017370403635265`, with all 1,616 source hashes matching `07135f5f`.
  Test log SHA256: `5e0acdafdb34330a66a2cd74f9c4c2f2e0bb7b24c16b0f2168eb7baae566e417`.
- WebAssembly consumer compilation passed in RCH `30016197441356027`, with
  all 1,616 source hashes unchanged. Log SHA256:
  `43f46704a834166fb58160312b8c25e94bfcca9d9b89f5ad7511d4600f802668`.
- It emitted 45 project warnings in pager/core. Repeating the identical
  command with tinyvec 1.12.0 also passed and emitted exactly the same 47
  diagnostic headings (45 warnings plus two summaries). These warnings
  predate the upgrade; no warning-free or browser-runtime claim is made.

### smallvec 1.16.0 → 1.16.1 — passed

- Upstream changes `push` internals for performance and fixes documentation/
  Cargo warnings; no API migration is declared. [Release notes](https://github.com/servo/rust-smallvec/releases/tag/v1.16.1).
- The existing version requirement is preserved; only the lockfile version
  and registry checksum change.
- Native RCH `30017370403635267` passed 562 type, 613 parser and 489 B-tree
  tests (1,664 total); 12 B-tree tests were ignored. All 1,616 post-run source
  hashes match `58045a79`. Log SHA256:
  `822f88fc88c91c1fcdcb8f8d4eb9fccb2a4405dc3c5003c8788f623bc021a572`.
- No project performance improvement is inferred from the upstream optimization.

### toml 1.1.5 → 1.1.6 — passed

- Upstream reduces parser allocation; existing dependency requirements and
  feature selection remain compatible. [Exact changelog](https://github.com/toml-rs/toml/blob/572c005d80cca5f7bd163805c2f33ba0a5207b6d/crates/toml/CHANGELOG.md).
- Only lockfile version/checksum change. Native RCH `30017537169162241`
  passed all 29 beads-doctor tests; `30017537169162243` passed all 74 selected
  tests across the six harness TOML-consuming modules, zero ignored. Both
  post-run manifests match all 1,616 source inputs `7e712883`.
- Logs SHA256: doctor `8da582a0c60997c16bed061f9db7e0eeff7bff46cf23754d2fe644477f1452e9`;
  harness `bd9fb43658435b361c1e9fe513e91fb1235f5aa78f4c0b9122d7ef834ffb6bfc`.

### trybuild 1.0.120 → 1.0.121 — passed

- Replaces its sole `target-triple` dependency with `target-tuple` 1.0.2.
  Published helper build scripts are identical; no target-selection behavior
  change was identified. Other dependency requirements are unchanged.
  [Exact upstream comparison](https://github.com/dtolnay/trybuild/compare/2adc26560dba1d8eaeb596c5625f854e5d6c68b2...4b511198467970a3ec448df3e3837f53e0677940).
- Native RCH `30017537169162244` passed the real sealed/open-trait test:
  three compile-fail fixtures and one compile-pass fixture. Existing `.stderr`
  expectations are unchanged; `TRYBUILD=overwrite` was not used. All 1,619
  post-run source hashes match manifest `81b63a1b`. Log SHA256:
  `58bee4b96da4b618c6a52f1581f5f9c93c156c528e046d9be009fd1e7f7eac38`.
- On the same lockfile, Linux RCH `30016197441356035` passed all 30 MVCC EBR
  tests with the old Crossbeam versions, zero ignored. All 1,619 post-run
  inputs match. This is the baseline for the next Crossbeam update, not proof
  of an updated runtime. Log SHA256:
  `5f528f5613aa4d7c6c869069129bd3f556a9c409a437eccc8c59cbaebc6439a7`.

### crossbeam-utils 0.8.22 → 0.8.23 — passed

- Fixes a Stacked Borrows violation involving a leaked `ShardedLockWriteGuard`
  and improves ThreadSanitizer compatibility. Dependency requirements, features
  and MSRV 1.60 are unchanged. Only lockfile version/checksum change.
- Prior-version Linux EBR baseline passed 30/30. Candidate RCH
  `30016197441356043` passed the same 30 tests; full MVCC library RCH
  `30016197441356045` passed 1,568 tests, 15 ignored, zero failures.
  Both post-run manifests match all 1,619 source inputs `e42e3e2c`.
- Log SHA256: EBR `c7ebc584159f9b97dca370539f52e515e56576d2d5eeb1794d0071892d651844`;
  full MVCC `3ee46e9e1442242a4b036a6e38d3e9a4282cce6bfb7aa50e2353f4e43c754397`.
  No sanitizer execution or ignored performance-gate acceptance is inferred.

### crossbeam-epoch 0.9.20 → 0.9.21 — passed

- Upstream improves ThreadSanitizer compatibility and makes `Shared::null`
  const. Existing dependency requirements/features are preserved; only the
  lockfile version/checksum change. The project consumer is MVCC reclamation.
- Baseline full MVCC suite passed with epoch 0.9.20 and utilities 0.8.23.
  Candidate full MVCC RCH `30016197441356046` also passed 1,568 tests,
  15 ignored, zero failures, including EBR property tests. All 1,619 post-run
  source inputs match manifest `bfa6497b`. Log SHA256:
  `8e8fb1820ae13afd00b7a5b560d7929871fc275ad2dc2ea568370242203b5a9a`.
- This does not claim sanitizer execution or ignored performance acceptance.

### crossbeam-deque 0.8.7 → 0.8.8 — passed

- Upstream improves ThreadSanitizer compatibility and uses 64-bit indexes on
  32-bit platforms with 64-bit atomics. Existing requirements/features stay
  unchanged. Project consumer: VDBE vectorized dispatch and work stealing.
- Native old-version dispatcher baseline `30017537169162265` passed all 14
  tests, with all 1,619 `bfa6497b` inputs unchanged afterward. Candidate Linux
  `30016197441356048` and native macOS `30017537169162268` each passed the same
  14 tests, zero ignored; both post-run manifests match all 1,619 `6575a43d`
  inputs. These 64-bit runs cannot prove the changed 32-bit index path.
- Log SHA256: baseline `04be2e2f48cebd025b8a80a39699a9e17251239e69ea8297e0d02b4366ac71c1`;
  Linux `54c15bdc2e854e03a7afb0d4fe8ca41c29e920949a5ee921b098e174ad01b920`;
  macOS `eee8cf98993970f4bf95fc769b294a6ad1eb27ca0ead271445c4374c2b8881c5`.

### crossbeam-queue 0.3.13 → 0.3.14 — passed

- Uses 64-bit indexes on 32-bit platforms with 64-bit atomics; requirements,
  features and MSRV 1.60 remain unchanged. Only lockfile version/checksum change.
- Published asupersync source uses `SegQueue` for scheduler global queues,
  blocking tasks, epoch work and cleanup entries. The mpsc channel's mentions
  of ArrayQueue are explanatory comments, not its backing implementation.
- Native dispatcher RCH `30017537169162272` passed 14/14. Linux RCH
  `30016197441356050` passed all three real-kernel driver-failure ownership
  guards; `30016197441356051` passed tracked-write observer-drop coverage.
  All 1,619 post-run source hashes match `ff39544a` on both workers.
- Log SHA256: native `761873fd052c561c5049417d6a7656929e6ecc37a35e26c533fd02e250eaee96`;
  ownership `3c5d1571f4a891296f5bc54445d8b0f33cf1fc41a218484624477b7c6deb7d00`;
  observer `b137cb37687215907a3cd9ebd284ff11ce1a838eecb447918787833230a05432`.
- No 32-bit runtime proof or existing cancellation-latency release-gate
  acceptance is inferred from these focused checks.

### console 0.16.4 → 0.16.6 — passed

- Upstream fixes Unicode truncation panics, measures truncation tails in visible
  columns and strips OSC/DCS sequences. [0.16.5 notes](https://github.com/console-rs/console/releases/tag/0.16.5),
  [0.16.6 notes](https://github.com/console-rs/console/releases/tag/0.16.6).
- Only lockfile version/checksum change. The project consumes console through
  Insta. Native RCH `30017537169162284` passed all 11 tests across the four
  actual planner/bytecode snapshot targets, with `env INSTA_UPDATE=no` explicit
  in the remote command. All 1,643 source hashes match `cbbeb53c`, including
  all 24 expected snapshots. Log SHA256:
  `c9e19ada3107c1e5377e9410688a95ce3efbe462214cb2fe06ec08d31799dd3c`.
- The first run passed but its controller-only environment variable was not
  explicitly forwarded. A bare assignment retry failed with shell exit 127
  because RCH quoted the assignment; the final `env` command above succeeded.
  Neither preliminary attempt is used to prove the no-update setting.

### indexmap 2.14.1 → 2.14.2 — passed

- Fixes map/set macro item hygiene and permits const initialization of empty
  default-hasher maps/sets. Requirements/features and MSRV 1.85 stay unchanged.
- Only lockfile version/checksum change. Native RCH `30017537169162285`
  passed 210 full JSON extension tests, one ignored, including ordered object
  removal and interleaved duplicate-key controls. All 1,643 post-run hashes
  match `7bf5626a`. `preserve_order` stays local to that crate; the GH356
  workspace-wide feature leak is not reintroduced. Log SHA256:
  `0ed6fb75e096e8f87f1ec0929193b890dd170c5a1b2999332ef94a23a55d428a`.

### cc 1.4.4 → 1.4.5 with required find-msvc-tools 0.1.12 — passed

- Fixes flag probing outside Cargo build scripts when `OUT_DIR` is absent.
  The published manifest requires find-msvc-tools >=0.1.12, so the helper's
  0.1.11 → 0.1.12 update is part of this dependency closure. MSRV remains 1.65.
- Only lockfile versions/checksums change. Native RCH `30017537169162287`
  passed all 562 type/hashing tests. Linux RCH `30016197441356053` rebuilt
  bundled SQLite and passed 1,167 VDBE tests, including live SQLite oracle
  comparisons; one manual performance test was ignored. Both post-run
  manifests match all 1,643 source inputs `98707534`.
- Log SHA256: native `621f47a1bd194cfe77f6e7cd21fa88fcd43b2837e8928c62a27c257993e8c45b`;
  Linux `9ee9b5002fb137f2512d4572a3b7dd0d1af36088738c3b09e4919173989cd056`.
- Native Windows compiler discovery is not proven by these macOS/Linux checks;
  exact release target checks remain due.

### zerocopy and zerocopy-derive 0.8.56 → 0.8.57 — passed

- Exact published source `6dc429c4` → `0c90b11a` preserves original source bytes
  when `try_transmute!` validation fails and qualifies generated `KnownLayout`
  metadata extraction through the intended trait. It also fixes generated
  helper lint allowances. The derive dependency requires the exact same version.
- Inspected locked ahash, half and ppv-lite86 consumers use infallible transmute
  and generated traits; none calls `try_transmute!`. Project tests do not prove
  that upstream failure branch. Features and other requirements are unchanged.
- Native RCH `30017537169162291` passed all 562 type/property tests. Linux
  RCH `30016197441356054` passed workspace/all-target compilation with optional
  E2E TUI targets, no reported warnings/errors. Both post-run manifests match
  all 1,643 `08154e72` inputs.
- Native RCH `30017537169162296` passed the actual fixture-selection schema
  consumer test. Its required schema/manifest files were verified before and
  after; all 1,645 expanded-manifest inputs `1271261a` match. The preliminary
  selector named an uncompiled source module and ran zero tests; it is excluded
  from validation evidence. Only pair versions/checksums change.
- Log SHA256: types `0eee37f78527e48698aaafbdd535f907e1e4e40507c6c156f4f2fd42e3bf88b1`;
  workspace `618009d7a2a75885ed9158338a0f703a6565a6a021c922e174180f474b66a953`;
  schema `d8712f94d7d26a6950c2e6ed49ea4654a72c41ef1d980d8364e9ecce228bb778`.

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
