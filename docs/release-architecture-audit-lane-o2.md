# Lane O2 — Release Architecture Audit (read-only)

**Bead:** `bd-0m1bp` · **Agent:** `FrostyFortress` (Agent Mail id 290) · **Date:** 2026-08-03
**Audited commit:** `ca5242aa` (`main`) · **Scope:** read-only. No version was changed, no tag
created, nothing published or uploaded, no GitHub Actions run.

This document states what the release machinery *is* today, the shortest safe path to a release,
and the points past which a mistake cannot be undone. It is precursor evidence for
[`bd-1dp9.9.4`](#) (final verification gate and release handoff package), which is currently
**unclaimable** — `br` refuses the claim because 16 issues block it. That refusal is the headline
finding: **the correctness gate, not the release plumbing, is what is holding this release.**

---

## 0. Verdict

| Axis | State |
|---|---|
| Version consistency | **Clean.** 27/27 crates and 27/27 internal pins at `0.1.19`; crates.io agrees; tag `v0.1.19` exists. |
| Publish DAG | **Correct**, self-validating, with one managed dev-edge that needs a one-line widen before a `0.2.x` cut. |
| crates.io | **Complete** at `0.1.19` — 25/25 publishable crates, zero yanks. |
| Binary artifacts | **Three versions behind.** Tags `v0.1.15`, `v0.1.18`, `v0.1.19` have no GitHub Release at all. |
| Installers | **Currently broken on the documented default path**, on every platform. See §6. |
| Signing | Key custody verified locally; epoch policy already cut over to the next release line. |
| CHANGELOG | `0.2.0` fully drafted, marked `Unreleased`, needs only a date. |
| Automation | **All release CI is off.** Tagging today publishes nothing. |
| Correctness gate | **RED.** 7 P0 release blockers; only one is even in progress. |

**Go/no-go: NO-GO on correctness, GO on plumbing.** The release machinery is in better shape than
the code gate. Nothing in §1–§8 is a reason to delay; §9's blocker list is.

---

## 1. Tags and releases

15 tags: 13 version tags `v0.1.7 … v0.1.19`, plus two foreign snapshot tags
(`jsm-v0.3.13-fsqlite-snapshot`, `jsm-v0.3.16-fsqlite-snapshot`) that belong to an unrelated tool
and should not be mistaken for release anchors.

Only **three** GitHub Releases exist:

| Tag | crates.io | GitHub Release | Binary artifacts |
|---|---|---|---|
| `v0.1.14` (2026-07-05) | yes | yes | 4 tarballs + `SHA256SUMS.txt`, **no signatures** |
| `v0.1.15` (2026-07-06) | yes | **none** | none |
| `v0.1.16` (2026-07-14) | yes | yes | 5 archives + `.minisig` + `.intoto.jsonl` + SPDX SBOM |
| `v0.1.17` (2026-07-18) | yes | yes (**Latest**) | as above, plus alias duplicates — see §5 |
| `v0.1.18` (2026-07-18) | yes | **none** | none |
| `v0.1.19` (2026-07-26) | yes | **none** | none |

So the project's *library* users are current at `0.1.19` while its *binary* users are pinned to
`v0.1.17`. Supply-chain rigor also improved sharply at `v0.1.16` (signatures, in-toto attestations,
SBOM appear); `v0.1.14` predates it.

`main` is **382 commits** ahead of `v0.1.19`.

---

## 2. Version consistency — clean

- All 27 workspace members declare `version = "0.1.19"` explicitly (no `version.workspace = true`;
  a bump touches 27 files).
- All 27 entries in `[workspace.dependencies]` pin `0.1.19`.
- Every internal path dependency also carries an explicit `version =`, so the crates.io
  requirement is already satisfied repo-wide.
- crates.io reports `max_version = 0.1.19` for all 25 publishable crates, none yanked.

The tree is internally consistent *at 0.1.19*. `0.2.0` exists only in prose.

## 3. Publish DAG

`.github/workflows/release.yml` hardcodes a 25-crate sequence and then **validates itself** against
`cargo metadata`: it diffs the configured list against the computed publishable set and aborts on
any mismatch, then checks every crate's version equals the tag. That guard is well-built and worth
preserving — it exists because an earlier tag "reported success while public packages remained on
an older version" (visible in the crates.io record: `fsqlite-cli`, `fsqlite-c-api`, and
`fsqlite-wasm` have no `0.1.14` or `0.1.15`).

I recomputed the dependency graph independently from the 27 `Cargo.toml` files:

- **Normal-dependency topological order: 0 violations.** The sequence is correct.
- `publish = false` on exactly `fsqlite-e2e` and `fsqlite-harness` — correct, both are internal.

### One managed back-edge — needs a one-line change before any `0.2.x` publish

`crates/fsqlite-pager/Cargo.toml` has a *dev*-dependency on `fsqlite-mvcc`, which is the reverse of
the normal edge (`mvcc → pager`). The authors handled this deliberately with a permissive range:

```toml
fsqlite-mvcc = { path = "../fsqlite-mvcc", version = ">=0.1.2, <0.2.0", default-features = false }
```

**That range excludes `0.2.0`.** On a `0.2.0` cut, `fsqlite-pager 0.2.0` would publish carrying a
dev-edge that resolves to `fsqlite-mvcc 0.1.19` from the registry — a stale cross-line dev
dependency. It will not block the publish, but it ships a wrong edge permanently. Widening it
(e.g. `>=0.1.2, <0.3.0`) is a reversible pre-publish edit and belongs in the freeze commit.

## 4. crates.io

25/25 publishable crates at `0.1.19`, published 2026-07-26, zero yanks. Nothing to reconcile.

Note for planning: `cargo publish` is irreversible (§10). The sequence is 25 *separate* irreversible
steps, and the historical gaps above prove a mid-sequence failure is a real failure mode here, not a
hypothetical.

## 5. Installer artifact matrix

DSR config (`~/.config/dsr/repos.d/fsqlite.yaml`) defines five targets, and the naming contract
matches both installers exactly (`fsqlite-<version>-<os>_<arch>.<ext>`):

| DSR label | Rust triple | Build host | Archive | Consumed by |
|---|---|---|---|---|
| `linux/amd64` | `x86_64-unknown-linux-musl` | trj | `.tar.gz` | `install.sh` |
| `linux/arm64` | `aarch64-unknown-linux-musl` | trj | `.tar.gz` | `install.sh` |
| `darwin/amd64` | `x86_64-apple-darwin` | mmini | `.tar.gz` | `install.sh` |
| `darwin/arm64` | `aarch64-apple-darwin` | mmini | `.tar.gz` | `install.sh` |
| `windows/amd64` | `x86_64-pc-windows-msvc` | wlap | `.zip` | `install.ps1` |

The matrix is coherent: `install.sh` maps `uname` to exactly the four Unix names, refuses
MSYS/Cygwin with a pointer to `install.ps1`, and `install.ps1` covers the fifth. Both Linux legs
build `musl`, which is what makes the README's "fully static, works on glibc and musl distros"
claim true.

Two things to know before touching it:

- **`include_files` is intentionally empty.** `install.sh` rejects any tarball that is not exactly
  one regular file named `fsqlite`; `install.ps1` rejects any zip whose single root entry is not
  `fsqlite.exe`. Adding a LICENSE or README to the archive breaks both installers.
- **`v0.1.17` shipped alias duplicates** (`darwin_aarch64`, `darwin_x86_64`, `linux_aarch64`,
  `linux_x86_64`, `windows_x86_64`) that no installer ever requests. Those aliases carry `.sha256`
  sidecars but **no `.minisig`** — an asymmetric authenticity surface. It also shipped both
  `SHA256SUMS` and `SHA256SUMS.txt`; only the `.txt` form is consumed. Dead weight, and a trap for
  anyone verifying by hand.
- No `windows/arm64` anywhere. `install.ps1` says so plainly: "Only windows/amd64 artifacts are
  currently published."

## 6. Signing, checksums, and the currently broken install path

Both installers hardcode **two minisign trust epochs**:

| Epoch | Key | Valid for |
|---|---|---|
| 1 | `RWTQoKUb0Ue4NsqTpPWnABCrIU0+m25zsMlbv6UcRClQ7jmRP3A7NmTB` | `v0.1.16`, `v0.1.17` only, **and only with an explicit version flag** |
| 2 | `RWTQGPeLsnm9G7VFdFWkkcRi3wJK/PqsYxWC+oLNN74W9IjBxRU1Xu70` | any version where `major != 0` **or** `minor ∉ {0, 1}` |

**Key custody is confirmed.** `~/.config/dsr/minisign.pub` is byte-identical to epoch 2, and the
secret key is present at `~/.config/dsr/secrets/minisign.key` (mode `0600`). `dsr doctor` reports
`minisign key: configured`. Signing capability for the next release exists on this machine.

Verification behavior is sound: SHA-256 against `SHA256SUMS.txt` is always mandatory; the `.minisig`
check is enforced when `minisign` is installed and *skipped with a warning* otherwise. That is
fail-closed on integrity, fail-open on authenticity — exactly what the README describes. Offline
installs require `--checksum` unless `--no-verify` is passed explicitly.

### The defect

The trust policy has already been cut over to the *next* release line, and that has broken the
current one. Tracing the default path (`install.sh:479` → `install.sh:481`):

1. User runs the README one-liner — no `--version`, so `VERSION_EXPLICIT=0`.
2. `resolve_version` queries GitHub's "latest release" → **`v0.1.17`**.
3. `select_minisign_public_key` matches the `v0.1.16|v0.1.17` case, sees `VERSION_EXPLICIT=0`, and
   **dies**: *"automatic version resolution returned legacy release v0.1.17; pass --version v0.1.17
   explicitly."*

`install.ps1` has the identical guard in `Get-MinisignPublicKeyForVersion`. **The documented default
install command currently fails on Linux, macOS, and Windows alike.**

Two consequences that shape the release plan:

- `v0.1.18` and `v0.1.19` are *unreachable* by the installers by construction (`major=0, minor=1`,
  not in the allowlist → hard die). Consistent with them having no release assets, but it means
  there is no "just publish artifacts for 0.1.19" escape hatch.
- **A `v0.1.20` would not fix this** — `minor=1` still dies. The next GitHub Release must have
  `minor ∉ {0,1}`, i.e. **`v0.2.0` or later**. The moment a `v0.2.0` release becomes "latest", the
  default path resolves to epoch 2 and self-heals.

This is a strong, independent argument for the `0.2.0` version choice that the CHANGELOG already
made on Cargo-semantics grounds. The two rationales agree.

## 7. CHANGELOG

1,888 lines, Keep-a-Changelog form, complete sections for every version back through `0.1.6`. The
`0.2.0` section is fully drafted — Breaking changes, Known limitations, Added, Performance, Fixed —
and correctly explains the `0.1.19 → 0.2.0` jump: the storage stack became `async` end to end, and
under Cargo's 0.x rules the minor version is the compatibility axis, so a caller pinning
`fsqlite = "0.1"` stays on `0.1.19` rather than being silently upgraded.

It also pre-documents the UTF-8-only limitation as a *shipped* `v0.2.0` constraint, with a migration
note for FTS5 `porter` index rebuilds.

Only gap: the heading still reads `-- Unreleased` and needs a date at cut time.

## 8. The manual, no-GitHub-Actions workflow

**Verified live on 2026-08-03 via `gh workflow list --all`:**

| Workflow | State |
|---|---|
| Release | **disabled_manually** |
| Verification Gates | disabled_manually |
| Unit Test Shard Matrix | disabled_manually |
| Concurrent Tests — Platform Matrix | disabled_manually |
| fsqlite-wasm CI | disabled_manually |
| Windows VFS Interop | disabled_manually |
| Workspace Lint Gate | active |
| Perf Regression Analyzer Contract | active |

Two things follow, and the first is load-bearing for safety:

1. **Pushing a `v*` tag today publishes nothing.** `release.yml` is the only automated crates.io
   path and it is off. Tag creation is currently *decoupled* from publication. This is the single
   biggest reason the plan below can order tagging before publishing without risk.
2. **`release.yml` never built binaries or created GitHub Releases in the first place** — it is
   crates-only. Every artifact on `v0.1.14/16/17` came from the manual/DSR path. There is no
   automation to restore here; the manual path *is* the path.

`bd-xekq8` tracks the administrative decision about re-enabling CI and explicitly marks it
owner-level ("do not act unilaterally"). It also records that `verification-gates` was failing every
night for four nights before being disabled — so re-enabling would produce instant red. Untouched.

### DSR readiness — verified read-only today

`dsr doctor`: **all checks passed.** `gh` authenticated as `Dicklesworthstone`; docker 29.1.3
running; `act` 0.2.89; `minisign` available with key configured; `syft` 1.44.0 for SBOM; 201 GB free
locally. `fsqlite` is registered against `Dicklesworthstone/frankensqlite` with all five targets.

`dsr health`: trj **healthy**, mmini **healthy**, **wlap: warnings — disk usage > 90%.**

That last line is a live hazard, not a nit: it is the textbook "remote build host out of disk space
mid-build" failure. The Windows leg is the one most likely to fail, and it fails *after* the Linux
and macOS legs have already succeeded. Clear space on wlap before starting a build, not during one.

The build command correctly sets `RCH_DISABLED=1`, which is required — otherwise RCH intercepts the
release build and the binaries land on a remote worker instead of in the artifact directory.

> **Open decision, not resolved here.** This lane's mandate is to route every Cargo command through
> `rch exec -- cargo`. DSR's `build_cmd` invokes `cargo zigbuild` directly on its own build hosts and
> deliberately disables RCH to do so. These are different contracts — the mandate governs *my*
> invocations, DSR's governs *its* hosts — but the operator should confirm which applies to release
> builds before the first one runs. I did not run any Cargo command in either mode.

## 9. What actually blocks the release

`bd-1dp9.9.4` cannot be claimed; `br` lists 16 blocking issues. The P0 subset:

| Bead | Status | Subject |
|---|---|---|
| `bd-6xjma` | **in_progress** (SunnyTiger) | retain pager ownership across dropped finalization futures |
| `bd-wymdl` (+`.1`,`.2`,`.3`) | open, unassigned | async cancellation / Drop / blocking-channel defects |
| `bd-yuj70` | open, unassigned | WITHOUT ROWID `UPDATE OR REPLACE` victim + secondary-index semantics |
| `bd-bld9w` | open, unassigned | end-to-end UTF-16 database and raw TEXT byte lifecycle |
| `bd-67tdh` | open | SQLite expression-depth 1000/1001 boundary |
| `bd-uh1fv` | open (RusticBasin) | make comprehensive / 128-writer perf claims citable |
| `bd-gh-windows-shm-stock-interop-kwe42` | open | Windows shm stock interop (#139) |

One observation worth surfacing to whoever scopes the cut: **`bd-bld9w` (UTF-16) may be
scope-reducible rather than blocking.** Both README and CHANGELOG already document UTF-8-only as a
*shipped* `v0.2.0` limitation with a documented conversion path, and the runtime fails closed on
encodings 2/3 rather than corrupting them. That is a coherent shipping posture, not a hidden gap.
Whether to hold `0.2.0` for full UTF-16 is a product call, not a correctness one.

Similarly, `bd-dqdoe` (PERF-P0, ~1.9× regression, cause unattributed) is serious but not obviously
release-blocking *as a documentation matter*: the README already states plainly that "no numeric
performance result is claimed for current `main`" and routes the reader to the negative-results
ledger. The project is not currently making a perf claim it cannot back.

---

## 10. Irreversible hold points

Ordered by how hard they are to undo. Everything above the line is recoverable; nothing below it is.

| # | Action | Why it cannot be undone |
|---|---|---|
| **H1** | `git tag -a vX.Y.Z && git push --tags` | Recoverable in principle, but tag protection commonly blocks the force-push needed to move it. Treat as one-way; if the tag is wrong, cut the next patch rather than fighting it. |
| **H2** | `cargo publish -p <crate>` | **Permanent.** The version number is consumed on crates.io forever. Yanking hides a version from resolution; it does not delete it and does not free the number. This is **25 separate irreversible steps**, and a mid-sequence failure leaves a partially-published version line — which has already happened to this project (`fsqlite-cli`/`-c-api`/`-wasm` have no `0.1.14` or `0.1.15`). |
| **H3** | `gh release create` | This is the **user-visible switch**. GitHub's "latest release" is what `install.sh`/`install.ps1` resolve to with no `--version`. Creating it immediately changes what every `curl \| bash` in the world downloads — and, per §6, is the specific act that repairs the currently broken default install path. |
| **H4** | Uploading `SHA256SUMS.txt.minisig` | Publishing a signature commits publicly to epoch 2. Rotating later requires an epoch 3 *plus* an installer edit *plus* a legacy allowlist clause — which is exactly the debt the epoch-1 `v0.1.16|v0.1.17` special case represents today. |
| **H5** | `release.yml` → `revoke_compromised_token` dispatch | One-way credential destruction. Only for incident response. |

**Ordering rule:** every reversible gate must be green before H1. Within the irreversible zone the
order is fixed — H1 → H2 → H3/H4 — because the tag is the provenance anchor that `release.yml`'s own
validator keys crate versions against, and keeping that invariant means a future CI re-enable stays
consistent. An unused tag is cheap; an unanchored publish is not.

---

## 11. Shortest safe critical path

Phases 1–4 are fully reversible. Phase 5 is not.

**Phase 1 — clear the gate (the long pole).**
Resolve or explicitly de-scope the P0s in §9. This is the entire schedule; everything below is
roughly a day's work once the gate is green. Decide `bd-bld9w` scope (ship UTF-8-only as documented,
or hold) before anything else, since it changes the release's contents.

**Phase 2 — freeze commit** *(reversible; one commit, revertable)*
1. Bump 27 crate versions `0.1.19 → 0.2.0` and all 27 `[workspace.dependencies]` pins.
2. Widen `fsqlite-pager`'s dev-dep on `fsqlite-mvcc` from `<0.2.0` to `<0.3.0` (§3) — otherwise
   `pager 0.2.0` ships a permanently-wrong dev edge.
3. Date the CHANGELOG `## [0.2.0]` heading.
4. Refresh `Cargo.lock` via `rch exec -- cargo check --workspace`.
5. Free disk on wlap (§8) — before, not during.

**Phase 3 — verification** *(reversible)*
`rch exec -- cargo test --workspace`, `rch exec -- cargo clippy --workspace --all-targets -- -D
warnings`, `rch exec -- cargo fmt --check`. Run clippy *before* tagging; nightly drift catching a
lint after the tag is the most common cause of retag churn. CI cannot substitute here — it is off.

**Phase 4 — build and rehearse** *(reversible; nothing leaves this machine)*
1. `dsr build fsqlite --version 0.2.0` → 5 artifacts across trj/mmini/wlap.
2. Sign, generate `SHA256SUMS.txt` + `.minisig`, SBOM via syft.
3. **Rehearse the installer against local artifacts** using `--offline <archive> --checksum <sha>`.
   This exercises archive-shape validation, the single-file constraint, and post-install smoke tests
   without touching GitHub.
4. `rch exec -- cargo publish -p <crate> --locked --dry-run` across the DAG. A dry run does not
   upload; it is the last cheap chance to catch a metadata error.
5. Decide whether to ship the `v0.1.17`-style alias duplicates. Recommendation: **do not** — no
   installer requests them and their missing `.minisig` files weaken the authenticity story (§5).

**Phase 5 — the irreversible zone.** Execute in this order, and stop on any failure:
1. **H1** tag `v0.2.0`.
2. **H2** publish 25 crates in the `release.yml` DAG order, leaf → root, ~30 s apart. If any crate
   fails mid-sequence, **stop and reassess** — do not improvise a partial line.
3. **H3/H4** create the GitHub Release; upload the 5 archives, `SHA256SUMS.txt`,
   `SHA256SUMS.txt.minisig`, and the SBOM.
4. Verify from a **clean host** that the bare README one-liner now succeeds — this is the
   confirmation that §6 is repaired.

---

## Appendix — provenance

Every claim above was read from the working tree at `ca5242aa` or queried live on 2026-08-03:
`git for-each-ref` / `git ls-remote --tags`; `gh release list` / `gh release view --json assets`;
`gh workflow list --all`; the crates.io v1 API for all 25 publishable crates; the 27 workspace
`Cargo.toml` files (dependency graph recomputed independently of `release.yml`'s hardcoded list);
`install.sh`, `install.ps1`, `.github/workflows/release.yml`, `.github/workflows/semver-check.yml`,
`CHANGELOG.md`, `README.md`, `AGENTS.md`; `~/.config/dsr/{config,hosts,minisign.pub,repos.d/fsqlite}`;
and `dsr doctor` / `dsr health` (both read-only).

No Cargo command was run in any mode. No file was deleted or modified. `lint_probe` was not touched.
