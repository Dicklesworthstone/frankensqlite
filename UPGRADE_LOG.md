# Dependency Upgrade Log

## September 12 — remote integration correction; final qualification pending

A rejected push revealed that the local checkout was behind remote main by
21 commits. Remote `a8b76fb81` already contains Asupersync 0.5.0, engine 0.4.0,
pager/core/facade 0.4.1, and read-only WAL/reserved-freelist fixes. The fsqlite
0.4.0 and 0.4.1 crates were published September 12 at 13:52 and 17:28 UTC;
GitHub's latest binary release remains v0.3.18. The earlier v0.3.19 release plan
is therefore stale. Remote changes were read and merged without dropping either
side's code or tests; the ordinary index and protected peer bytes are preserved.

The merged tree adds our WASM/parser repair and individually qualified FTUI 0.7
family, retaining remote Asupersync requirements and crate versions. Its 1,645-input
source manifest is `e727b7e8c216c655834000498dfbaa908a01bd2915221e03fe5a6d4e412f40bb`.
Merged-source RCH 56204 passed all six parser and eight read-only WAL regression
tests, with none ignored or filtered. RCH 56205 passed workspace/all-targets/TUI
checking. Both workers matched all 1,645 source hashes after completion. Formatting
also passed. Warnings-denied Clippy 56215, the restricted native-context consumer
56213 (one passed, 744 filtered), and real Chrome all-feature run 56206 (41 passed,
none ignored/filtered) passed. All source hashes matched afterward; all seven
browser-tool hashes also matched. Final TUI 56220 passed all seventeen tests
(dashboard one, viewer sixteen), with none ignored/filtered and all source hashes
matching afterward. Replacement default/memory browser variants passed as recorded below.
Default-browser run 56219 reported nineteen passes, but its post-run source
check found four files matching the older `a8b76fb81` instead of the frozen
manifest: both root Cargo files and the two parser-repair files. That run is
excluded from current-source acceptance. Clean-overlay recovery refused the
repository's Git submodule. The replacement uses an isolated worktree at
`dbca5d37c` with RCH's worker-verified source-content receipt; its first admission
outside the configured project root refused, then the worktree was moved under
the configured root. No local fallback or fleet repair was used.
Replacement default 56234 passed all nineteen tests and memory-options without
diagnostics 56242 passed all twenty-one, with none ignored or filtered. Both
completed remotely on hz3; their source-content receipts matched all 1,645 inputs
and their terminal tool checks matched all seven browser-tool hashes. Receipt
roots are `19f831a53cf2a1a4519885e4e901577e6c17b74b0201115b766c014f803775ad`
and `07e7dd40b17d21c5b49f37c5c42e46b1396120cdd16ac31a707da59c4929a641`.
The first TUI admission was refused for ovh-a disk pressure;
no local fallback or host cleanup occurred, and the retry uses ovh-b. Earlier
source-bound receipts below do not certify a new release. Job IDs use prefix
`300161974413`.

While qualification ran, remote main advanced to `5ced8e118`, incorporating the
same parser and dependency source. Its production files match the tested tree;
the histories are reconciled with the current validation notes retained.

Independent acceptance inspection retained closure of bd-7rg1a.3 after checking
the original contract, exact source and actual test receipts. This was independent
inspection, not an independent re-execution. Configured formatting excludes
fsqlite-core, so it does not mechanically certify the two parser-repair files;
the narrow diff was visually reviewed. No benchmark or whole-project acceptance
is implied by this closure.

Independent review against remote main found exactly eleven WASM and eleven FTUI
package-version changes, no other changed lock records, and no forbidden runtime.
The remote Asupersync 0.5.0 requirements and all engine package versions remain
intact. FTUI keeps `default-features=false` and the existing opt-in TUI feature.
Its separate pre-merge qualification on source `697f677d` passed seventeen binary
tests (56159), workspace checking (56160), Clippy (56171), formatting (56168), and
strict real-PTY navigation, resize and terminal-restoration checks (56173).
The first PTY run had an insufficient resize assertion; the tightened rerun is
the accepted receipt. These are UI checks, not database workload benchmarks.

Separate pre-merge Asupersync 0.5 consumer results on source `bb29a034`:
98 context tests (RCH 56195), 32 io_uring tests including actual kernel I/O (56198),
1,568 MVCC tests with 15 ignored (56200), 35 commit-repair tests (56202), and
41 real Chrome all-feature tests (56199) passed. All 1,645 source hashes matched
after each run. The original commit-repair selector 56197 ran **zero tests**;
it is excluded from proof and was replaced by the actual `commit_repair::` module
selector. These results do not establish the broader historical performance gate.
All job numbers in this paragraph have prefix `300161974413`.

### JSONschema 0.56 — focused qualification complete

The current 0.48.5 schema-consumer baseline 56227 passed both compiled
`matches_json_schema` tests (699 filtered), with all six fixture files present
and all 1,645 post-run source hashes matching. Isolated resolver 56229 selected
exactly the four JSONschema-family packages at 0.56.0, Fraction 0.17.0 and
fancy-regex 0.19.1; other package records are unchanged and no forbidden runtime
appears. The new IDNA feature must be explicitly enabled to retain old behavior;
HTTP and async resolution remain disabled. `serde_json/float_roundtrip` was
already enabled by 0.48.5. Review of fancy-regex 0.19.1 covered its optional
capture, Unicode matching, bounded seek expansion and delegated-engine cache
changes. The six-package candidate is now applied to main, preserving current
engine versions. The candidate's 1,649 source/fixture inputs have manifest hash
`d3c940f2cd16e408873ff7c56a6896cbec6faeac7d81fc9ee3d7c022fd779856`.

RCH 56238 passed nine actual tests across seven targets: fixture selection,
comprehensive report, realdb report, manifest, and SSI/busy/crash matrices.
The matrices exercised six, three and six scenario outcomes respectively.
All six tracked fixtures were present. There were no failures or ignored tests;
809 tests were filtered. The bridge selector in that TUI-only invocation was
not compiled, so it contributes no test to that count. Separate bridge-experiment
run 56248 passed its actual positive/negative schema test (one passed, fifty
filtered). Both source-content receipts match all 1,649 expected inputs, with
terminal remote exit zero and artifact retrieval complete. Receipt roots:
`a4b0eba0eee7e07830c08693e944bf932a171e225db91cc3c3b25d526c096a44`
and `27914ef5bc0a095e41f722a7197a710590b7f7176e6fd721ac9d22fd784d76fd`.
An independent agent inspected the bridge log and receipt; it did not rerun it.

Workspace/all-targets/TUI check 56249 and warnings-denied Clippy 56252 passed,
each with all 1,649 inputs matched and terminal remote exit zero. Configured
formatting 56251 passed as an RCH non-compilation job with matching post-worker
hashes; the existing fsqlite-core exclusion remains. The first check attempt
was refused for ovh-b disk pressure, and the first formatter invocation was
rejected because source-content receipts and job mode cannot be combined.
Neither refusal is validation. Tests and compiler checks establish this focused
dependency update, not whole-project release readiness or performance parity.

## September 12 — WASM family and recoverable parser diagnostics integrated

Updated wasm-bindgen and its matching packages from 0.2.127 to 0.2.128.
The eleven-package closure includes upstream's required minicov 0.3.8 pin
([profiling-runtime incompatibility](https://github.com/wasm-bindgen/wasm-bindgen/pull/5283));
unstable coverage generation was not exercised. Manifest feature policy is unchanged.

The old-lock browser run exposed bd-7rg1a.3: unexpected SQL tokens were
reported as nonrecoverable function errors. The connection now returns the
existing `SyntaxError` variant, preserving stock text, primary/extended code 1
and nontransient status. The added native test checks both execute and query,
then executes corrected SQL on the same connection. Existing assertions remain intact.

Before the remote merge, main's 1,645 source inputs matched qualified manifest
`84c57d313790585258e9b9226165e0d658914c6cc0f941c9436c4865fdb7ae2b`.
Strict RCH receipts on that source: full-feature Chrome 56145 passed 41 tests;
default Chrome 56150 passed 19; memory-options without diagnostics 56153 passed
21; native parser 56148 passed six. All had zero ignored tests. Workspace/all-targets/TUI
check 56147, warnings-denied Clippy 56149 and formatting 56146 passed, with
all source hashes checked afterward. Job numbers in this section have prefix
`300161974413`. Browser tooling was matching bindgen 128 plus Chrome/chromedriver
153.0.8010.36; its hashes are retained in
`/tmp/frankensqlite-wasm-browser-gpu-disabled-toolchain-20260912.sha256`.

The old full browser run 56141 remains a failure: 32 passed and nine failed
(the metadata assertion followed by poisoned-lock failures). An isolated old
generated-JS control passed six selected cases; it does not replace that failure.
Before integrating the dependency closure, the syntax-only main source also passed
all six native tests (56190), formatting (56196), and workspace/all-targets/TUI
check (56191). Its distinct source manifest is `8cff11f1`; those checks do not
stand in for the final dependency-source receipts above. Independent source review
found no material issue. UBS findings in the test file were test assertions/panics,
fixed SQL construction, and a false secret detection on the parser's `token` field;
no suppression or weakened gate was added. Concurrent-writer defaults are unchanged.

This was the nineteenth qualified dependency step. The newer remote versions and
their current integration status are recorded above; final updated-dependency and
release qualification remain incomplete.

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

### hybrid-array 0.4.14 → 0.4.15 — passed

- Adds the `ArraySize` implementation for `U513`; published source
  `09310c55` retains existing requirements/features. Only lockfile
  version/checksum change. The active consumer path includes crypto primitives.
- Native RCH `30017537169162299` passed all 32 pager encryption tests,
  including authenticated-context swaps, wrong keys, corrupted ciphertext
  and DEK wrapping/rekeying. All 1,645 post-run source hashes match `693b9117`.
  Log SHA256: `4fa9dd2b97e04ef68bb23c7a20e63163a00defbff86bd074a5227ae6953676ec`.

### argon2 0.5.3 → 0.6.0 — passed

- Reviewed published source `b1e0ad6fe229b1ba74e4696c7359ab45d7e931f0`.
  The raw Argon2id v19 API used by `KeyManager::derive_kek` is unchanged.
  Required closure: Blake2 0.11.0, password-hash 0.6.1 and PHC 0.6.1;
  the existing cpufeatures 0.3.1 and digest 0.11.3 satisfy their requirements.
  RCH `30016197441356062` resolved this closure without unrelated upgrades.
- Added two independent Libsodium known-answer vectors to the existing pager
  encryption tests, including an empty password and a password with a significant
  trailing space. Both pin all 32 derived key bytes; production KDF code is unchanged.
  Old Argon2 baseline RCH `30017537169162304` passed all 33 encryption tests.
  All 1,645 baseline source hashes match `8eb9ada0`.
- Candidate native RCH `30017537169162314` passed all 33 encryption tests.
  Linux workspace/all-targets check with optional TUI `30016197441356063`
  passed without warnings or errors. WASM target compilation `30016197441356065`
  passed with the same 45 existing warnings as the retained baseline; this is
  compilation evidence, not browser execution. Formatting `30016197441356067`
  passed. All four post-run manifests match all 1,645 inputs in source
  `2969036dea32316cb116da9405dcac3da82669d7e04a7f824b417902f4a2b233`.
- Workspace/all-targets warnings-denied Clippy with optional TUI
  `30016197441356066` passed; all 1,645 post-run source hashes match `2969036d`.
- Transcript SHA256 values: native encryption
  `4b5789723bfddeadddffd3d0c79eb8f6a2c0b68312c1761e6794741748b43ee7`;
  workspace check `65995b92a0f6a34d62337e40c7c1262c24318450ca07e74ec11b6b0d47bf5f49`;
  WASM `45a6eeb22c18a20fe7eb1dbd0f0bfccfc0344a034bb2bc73cae5eaa86ea93b5e`;
  Clippy `aceb65df8cb23d4a6712a648b20eb7fde5d43e22c7031f5b9f10401cb4d9e61c`;
  formatting `4d1813cff34bb44bf941153493eb563cc05638906a76d2683a81ac93037f44c0`.

### hashlink 0.12.1 → 0.12.2 — passed after metric-test repair

- Published source `c7aaa3c2504c08baf8c97da11f3536258464da8f` adds
  `LinkedHashMap::insert_front`; requirements/features remain unchanged.
  Only its lockfile version/checksum change. The consumer is bundled SQLite
  reference testing through rusqlite.
- Initial parallel VDBE run `30016197441356069` passed 1,165 tests, failed two
  process-global metric assertions, and ignored one manual performance test.
  All 1,645 source hashes match `47936999`; this failed run is retained.
  Old Hashlink control `30016197441356072` passed 1,167 tests, one ignored;
  a single passing control does not resolve an intermittent measurement race.
- Bug `bd-7rg1a.1`: isolate eight exact decode-counter tests with existing
  thread-local metrics, and the root-initialization test with three test-only
  page-motion counters at the actual normalization branches. Preserve numeric,
  row, cached-value and record-profile assertions; production metrics and
  writer locking are unchanged.
- A real borrowed page copy on another thread deterministically makes the old
  global assertion fail (`30016197441356074`, source `528c32e3`, 0 passed/1 failed).
  The final repaired parallel Linux suite `30016197441356077` passed 1,167 tests,
  one ignored, including that challenge and the live SQLite PRAGMA oracle.
  Native macOS full suite `30017537169162335` passed 1,167 tests, one ignored,
  before adding the challenge; final native challenge `30017537169162340`
  passed 1/1. Formatting `30016197441356076` passed. All final post-run manifests
  match 1,645 inputs in source
  `eef23d2f07dcdeb0ee7c9974095ca8c88e38b5a888cc881b308910003020cdcf`.
- Workspace/all-targets check `30016197441356078` and warnings-denied Clippy
  `30016197441356079`, both with optional TUI, passed without warnings/errors.
  All 1,645 post-run source hashes match `eef23d2f`.
- Transcript SHA256 values: initial failure
  `50985792b333ffad9fbc906255308c3e7baa7dc02d5b256eebd22f2ce85e9ac3`;
  deterministic negative `3b4be4b78863b13f8a06c96b09d7e6fc9e20f44a9b930d1082ac3daf8e6b6408`;
  final parallel suite `9e601346c32fd533518d64c20eb900584f0fd07600ec5d1ff831d7c129ef4730`;
  final native challenge `b1af447ba27e1c840ec6e7147469a955e9113b33a2aea56e7f544c91b1aee59a`;
  workspace check `28b6067d463e5d6c5c65cde6600f83157cc2451508e410cd6c7f97667baede87`;
  Clippy `84ffd7228afd1630c27d68dc8bf284124adf7d5c7cc15b030aba09d57eae6669`.

### io-uring 0.7.14 → 0.7.15 — passed scoped kernel checks

- Published source `a51717806263c0b11dcd0d4ea9f8c7de0357bd26` handles
  pending deferred task work during submission, adds completion-eventfd control
  and write-stream fields whose default remains zero. Requirements/features
  are unchanged; only lockfile version/checksum change. The VFS does not enable
  deferred-task-work setup, so this is not a claimed repair of its timing gate.
- On the same Linux worker, old-version RCH `30016197441356080` and candidate
  `30016197441356085` each passed all 32 `uring::tests`, no ignored tests,
  including actual kernel ownership, completion-after-observer-drop and both
  unchanged five-millisecond cancellation assertions. Both embedded fresh-process
  checks also passed. All 1,645 post-run inputs match the respective manifests:
  baseline `eef23d2f`, candidate
  `cc4cdb2444abb1f9101060cbea4c1951205c90e0591cf5a00f5aa537b5270533`.
- These bounded runs do not supersede earlier failed cancellation timing batches
  or close `bd-6hdwo.35`; final release qualification remains open.
- Transcript SHA256: baseline
  `b4b6e7e6ba9b12b990b149c095d2eb2d180d42d5c59b025a4677b532623212df`;
  candidate `c3566ee1d1ad0e3bf39009dad6d6da4e2d40f7ec2f6ccddb14f8a83dd4e23394`.

### asupersync 0.4.10 → 0.4.11 — passed scoped runtime checks

- Retained the existing workspace requirement and disabled default features.
  Updated only asupersync and its required franken-decision/evidence/kernel
  closure to 0.4.11. Unrelated resolver deduplication was not applied.
- Reviewed the published runtime changes to oneshot permit settlement,
  current-thread task driving, timer wakeups and runtime-owner teardown.
  No application runtime API or concurrency default changed.
- Old-version controls passed: native pager RCH `30017537169162353`
  (998 passed, 13 ignored), Linux MVCC `30016197441356088`
  (1,568 passed, 15 ignored), and the preceding io-uring kernel suite
  `30016197441356085` (32 passed, none ignored).
- Candidate source manifest
  `/tmp/frankensqlite-0319-asupersync-source-20260912.sha256`, SHA-256
  `144f11d6691f22cc4e61b1103aafc53a7f3755ee3753b8155bc7e31428dd0b2d`:
  native pager `30017537169162360` passed 998 tests (13 ignored);
  Linux kernel `30016197441356091` passed 32 (none ignored);
  full parallel MVCC `30016197441356095` passed 1,568 (15 ignored);
  workspace/all-targets/TUI check `30016197441356092` and warnings-denied
  Clippy `30016197441356096` passed. Native diagnostic public contract
  `30017537169162371` passed all 17 tests, including real mid-scan and
  cached prepared-read cancellation. All 1,645 inputs matched each worker
  after its run. Embedded subprocess test summaries are not extra cases.
- Candidate transcript SHA-256: pager
  `92aaeb27dcebb49e68b833fde481c52d6498b1fe0bad98bb1fa0472d82ad91f9`;
  kernel `ed3ef5ae7230ad4e4ba74cb68f154fc23349f7b4f1e9608968ef2d168fb79f06`;
  MVCC `174962db43f6c6da58df36d4b7617b0f89426431a6cfa45fbe005ef3e2edd118`;
  check `ca47f0f14b4ec4f12952d75dff895e28d6627002405088a7aa8abf9f11e7b9ee`;
  Clippy `b89a86329accb4700a6731f621ec05ff3d2736db8de0fc1f90b34aeaa75c13e7`;
  public contract `e63e8205076d3bf3a234dadbb186ae47e6c19a9116aed0092a5b960c0fe0afda`.
- Review found an existing coverage error, tracked as `bd-7rg1a.2`: the
  ordinary writer drop test used plain BEGIN, which promotes to concurrent
  mode. Both ordinary writer and sibling now explicitly use BEGIN IMMEDIATE,
  assert concurrent defaults remain enabled, and retain the original durable
  row, abandoned row and sibling progress assertions. The concurrent case
  remains unchanged. Native `30017537169162377` passed both cases, none
  ignored, on source manifest SHA-256
  `4f83a8c30a03f1b926620051cb572450650467bf5dd8d0767fc83e4edd86e9ed`;
  all 1,645 inputs matched. Transcript SHA-256
  `a458035e122fff2a446e5fcdb429f27bde597f46a4efc0a8586ce59bb7c75c75`.
- The first extra async-facade drop invocation `30017537169162378` failed
  before compilation because it omitted required feature `async-api`.
  It provides no test evidence. Corrected native `30017537169162379` passed
  the one actual test, none ignored: no background family-size mutation and
  unchanged main/WAL sizes through read-only opens. All 1,645 final test-source
  inputs matched afterward. Transcript SHA-256
  `1e12e7d420b55e25166183a28b48fa4c03b19662867d1c0499feed4f40611398`.
  Formatting `30016197441356099` also passed with matching final-source inputs;
  transcript `b38b4545df0ed71fb55f028fe51df0ad74e5cb057d4100b5143b20130618a332`.
- Canonical writer targets `30016197441356098` passed: 4 active cases in
  `bd_1r0ha_3_concurrent_writer_e2e` (one manual profile ignored), and all
  15 in `mvcc_concurrent_writers`. The latter includes SQLite scaling controls
  and sequential FrankenSQLite baselines; the former exercises actual parallel
  FrankenSQLite worker progress. All 1,645 source144f11d6 inputs matched.
  Transcript SHA-256
  `d9256a3dac6692a48df1650c0473c739e1af407c150efded36a2bf10a644ccf2`.
- Final test-source workspace/all-targets/TUI check `30016197441356100`
  passed with all 1,645 inputs matching; transcript SHA-256
  `598070d2327a143a1ab5432e718e6eb111984de052a0d06ef6213cddd57eab13`.
  Final warnings-denied Clippy `30016197441356107` passed with all 1,645
  inputs matching; transcript SHA-256
  `744455762ecc413fb8ab580227aed6a99bc1cc0bf988d44a6958e286fa03b86c`.
  Independent read-only review found no blocking issue in the lock delta,
  ordinary-drop correction or proof claims. These scoped results do not
  close the historical five-millisecond cancellation gate, ignored performance
  gates, private core-lib settlement coverage, or release qualification.

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
