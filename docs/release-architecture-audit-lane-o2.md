# Release architecture and safe release path

This is a maintained architecture note, not a read-only execution receipt. It
records the release boundaries that are stable enough to guide a future cut;
live service state, host health, credentials, and issue counts must be checked
again at the release decision.

## Scope and evidence boundary

The original evidence-only audit was committed in `e1a73d88`. The 2026-09-05
revision reconciles this note with repository manifests, installers, workflows,
the changelog, and encoding admission source. A read-only GitHub API check also
confirmed repository Actions permission `enabled: false`. No Cargo execution,
workflow dispatch, publication, tag, upload, or release-source change was made
for this revision. External DSR configuration and release asset inventories
were not re-audited. Operational host names, filesystem key locations and key
material remain outside this document.

Local-tree facts checked for this revision:

- The [workspace manifests](../Cargo.toml) have 28 members, all locally declared
  at `0.3.16`; two members (`fsqlite-e2e`, `fsqlite-harness`) are marked
  `publish = false`, leaving 26
  publishable crates, including `beads-doctor`. Publishable is a manifest
  property, not evidence that a version is present in crates.io.
- [CHANGELOG.md](../CHANGELOG.md) records `v0.3.16` on 2026-09-03 and an
  Unreleased section for development after that tag. The old audit's
  `v0.1.19` semantic predecessor /
  `v0.1.18` released-ancestor analysis concerned preparation of `v0.3.0`; it
  does not identify the predecessor or ancestry of a future release. Check
  the selected tag range, ancestry and registry state at each cut.
- **DSR is the sole release path, for both registries. GitHub Actions does not
  publish.** Two checked-in workflows previously did:
  - `release.yml` would publish the 25 publishable crates to crates.io on any
    `v*` tag push. Its tag trigger is removed and its publish job is disabled
    fail-closed, so tagging a version publishes nothing. Its 25-crate sequence
    is retained in-file as a reviewable reference for the topological order
    DSR must follow; it is documentation now, not automation, and it was never
    a topology proof.
  - `fsqlite-wasm-ci.yml` ran `npm publish` with an `NPM_TOKEN` on GitHub
    Release publication, and exposed a manual `publish` dispatch input. The
    `release` trigger is removed, the input is inert, and its publish job is
    disabled fail-closed. Its build/test CI on `pull_request` and `push` is
    retained in source, but repository-level Actions disablement means those
    jobs do not currently execute either.

  **The npm package is part of the release surface** and is DSR's responsibility
  alongside the crates. Checked-in job definitions do not prove an executed
  gate, a registry publication, or a configured DSR packaging stage.
- In their normal online path, the Unix and PowerShell installers resolve an
  unspecified version from the latest GitHub Release and require a SHA-256
  manifest; when `minisign` is available they also verify its signature.

The Actions observation above is dated service state, not a permanent property.
Registry contents, GitHub Release assets, workflow enablement, build machines
and Beads blockers must be checked again at release time. DSR-only publication
is the owner policy recorded in `AGENTS.md` (`bd-0p0sp`) and in both disabled
publish workflows.

## Release architecture

### Version line

The project uses a lockstep workspace version. A release cut therefore changes
the workspace crates, internal dependency requirements, the changelog heading,
and the release tag as one coherent line. The tag version is the provenance
anchor against which DSR must validate each crate version; the checked-in
Actions publish jobs are disabled and have no tag-push trigger.

For every lockstep minor cut, review each internal dependency requirement
rather than assuming its old semver upper bound admits the new line. In
particular, dev-dependencies can be invisible to normal publish ordering while
still becoming permanent registry metadata after publication.

### Publish DAG

The disabled release workflow lists the original 25 publishable crates. The
original audit recorded a manifest review of that list's normal-dependency
topology; it does not prove the order of today's expanded package set.
The current workspace has 28 members: two are intentionally non-publishable,
and `beads-doctor` brings the live publishable set to 26. The workflow list
survives only as a reference sequence for DSR; because the workflow no longer
runs, the current Cargo metadata is authoritative and the crate-set and
version checks it used to perform are now DSR's responsibility, not an
executing Actions gate.

DSR must therefore itself confirm, at cut time, that the crate set matches
Cargo metadata and that every package version equals the release tag. Neither
check has ever proven that the configured order is topological, so the
leaf-to-root order still needs its independent review, and publishing must
stop on the first failure. Metadata, lockfile, credentials, and registry state
are all live state to be checked at cut time.

### Installer and artifact contract

The supported prebuilt matrix is four Unix artifacts plus one Windows artifact:

| Family | Architectures | Archive | Installer |
|---|---|---|---|
| Linux | x86_64, aarch64 | `.tar.gz` | `install.sh` |
| macOS | x86_64, aarch64 | `.tar.gz` | `install.sh` |
| Windows | x86_64 | `.zip` | `install.ps1` |

The archive layout is part of the security and compatibility contract: Unix
archives contain exactly the `fsqlite` binary, and the Windows archive contains
exactly `fsqlite.exe`. Adding convenience files changes installer behavior and
must be accompanied by an installer change and tests.

Both [install.sh](../install.sh) and [install.ps1](../install.ps1) consume a
versioned release artifact and the bare
`SHA256SUMS` manifest. Their normal online path behaves as follows:

- With `minisign` installed, download `SHA256SUMS.minisig` and require valid
  signature verification before accepting the archive checksum. A missing or
  invalid signature fails installation.
- Without `minisign`, skip signature authentication and warn unless quiet mode
  suppresses the warning; SHA-256 verification remains required. This is
  checksum verification, not authenticated release provenance.
- Explicit `--no-verify` / `-NoVerify` bypasses checksum and signature checks.
  The offline path requires an explicit version and caller-supplied SHA-256
  unless that bypass is selected; it does not fetch a signed manifest.

A release rehearsal must exercise the default latest-release path as well as
exact-version and offline paths, including installed/missing verifier and
invalid-signature cases. A valid archive alone does not prove installation.

### Signing and provenance

The installers contain a version-to-trust-policy selection. Treat a signing-key
rollover, manifest format change, or new version line as an installer
compatibility change, not merely a build detail. The release record needs:

- archives for every supported matrix entry;
- one SHA-256 manifest covering exactly those archives;
- the manifest signature under the policy selected by both installers;
- build and source provenance sufficient to connect each artifact to the tag;
- an SBOM or equivalent dependency inventory if the release policy requires it.

Credential presence, secret-key locations, key values, and individual build
host capacity are operational controls. Keep them in restricted release
runbooks, not in a repository architecture document.

### Signed checksum manifest stage

The old `.txt` producer/consumer mismatch is obsolete: `install.sh` and
`install.ps1` request `SHA256SUMS` and, when authentication is available,
`SHA256SUMS.minisig`. Per-asset `.sha256` files do not substitute for that
manifest. A crates.io publish still produces no CLI archives or release
checksum manifest.

Every published checksum manifest must be signed under release policy. This
producer obligation is stronger than the current installers' behavior when
`minisign` is absent. If a release also retains the historical
`SHA256SUMS.txt` alias, it must carry its own matching signature; the current
installers do not consume that alias.

Generate, sign and verify the manifest as an explicit release stage before
upload. The `AGENTS.md` DSR procedure records a separate manifest-signing step;
neither successful building nor these installer sources prove that external
DSR configuration automatically generates or signs it.

### Artifact-only release path

Building and publishing remain separate release stages. An artifact-only cut
must bind archives to the tested source and explicitly enumerate every upload,
whether the build and publication happen on the same host or different hosts.
Confirm the installed DSR version and effective contract before relying on
its artifact-directory, aliasing, signing or generation behavior.

The original audit recorded the following **historical v0.3.0 configuration**.
These details explain that release plan; they are not a claim about current
external DSR configuration:

- A pre-populated, flat artifact directory allowed publication without local
  compilation on the release host. Its build manifest named each archive with
  digest, target and format.
- The non-strict path scanned one directory level. Its pattern-based sidecar
  selection could include unintended scratch files, superseded manifests or
  editor backups. The release plan therefore required an isolated directory
  containing only intended assets.
- Aliasing published one local file under original, versioned and compatibility
  names, including `amd64`/`x86_64` and `arm64`/`aarch64` variants. Per-name
  `.sha256` files meant staged-file and published-asset counts could differ.

The historical v0.3.0 plan's closed surface was 43 assets, using the shape
recorded for v0.1.17:
five primary archives, five architecture aliases, ten per-name `.sha256`
sidecars, five primary-archive signatures, five primary-archive provenance
files and their five signatures, `SHA256SUMS` and `SHA256SUMS.txt` plus both
signatures, one SPDX SBOM plus its signature, and one build manifest plus its
signature. This preserves the original inventory example, not a universal
43-asset requirement for later releases. Every new cut needs its own explicit
staging list and published-name list; each must match its respective receipt.

In that plan, per-archive signatures and provenance covered primary names.
Alias names had to be byte-identical and independently named in the signed
checksum manifests; only under those conditions did the primary signatures
and manifests cover the aliases without separate archive signatures.

The historical strict contract replaced directory scanning with a closed asset
plan and an exact-count gate. Every intended sidecar had to be enumerated;
the operator could not rely on discovery from disk.

For that 43-name plan, five primary archives implied five primary `.sha256`
sidecars, and `exact_additional_assets` enumerated the remaining 33 names.
The recorded preflight could create the five primary sidecars without
clobbering an existing file, but did not discover, alias, sign or generate the
additional assets. The operator had to materialize those 33 files before
preflight hashed and froze all 43 local paths. Remote verification then
required exactly that asset set. For a current cut, independently verify the
effective contract and its behavior; additions outside the frozen plan need a
revised plan and verification rather than an unrecorded supplemental upload.

The original audit also found standalone SBOM generation, truncated archive
basenames and no license field in its generated documents. Do not infer that
the current generator has those same properties. Check the actual inventory
format and required license provenance as part of the present release plan.

### Historical v0.3.0 artifact decisions

The original v0.3.0 plan preserved the preceding line's per-archive provenance,
per-archive signatures and SPDX dependency inventory. It recorded these
decisions, which remain useful inputs to a future explicit release plan:

- **Provenance was a standalone stage.** The plan required one verified
  source/tag-bound provenance file for each of the five primary archives.
- **SPDX output had to be rehearsed.** The expected name was
  `frankensqlite-0.3.0.sbom.spdx.json`; a new version needs its own checked name.
- **Non-archive signing was explicit.** The audited automatic signer matched
  the tool-name prefix, leaving checksum manifests, the build manifest,
  dependency inventory and provenance files to separate signing steps.
- **The prefix split was deliberate.** Archives,
  aliases, provenance, and per-name checksums used `fsqlite`; the build manifest
  and SPDX inventory used `frankensqlite`, matching v0.1.17 rather than silently
  changing consumer-visible names.

Current release policy still requires signed manifests and sufficient source,
artifact and dependency provenance. Check the selected DSR contract and
generation/signing receipts for the chosen version; historical configuration
and inactive Actions definitions cannot supply that proof.

## Current release gate

The final release-handoff Bead is the tracker authority for outstanding
correctness and acceptance work. Its dependency set, ownership, and status are
expected to change while peers work; do not copy a blocker count or ownership
snapshot into release documentation. At the release decision, query the tracker
and attach an immutable snapshot to the handoff package.

The README makes two useful, durable constraints explicit:

- no current numeric performance claim is release evidence until it has a
  same-source, provenance-bound benchmark artifact;
- the current source admits UTF-8, UTF-16le and UTF-16be databases for reads and
  writes. ATTACH rejects a populated database with a different encoding; a
  new/empty database adopts the main database's encoding. Admission follows
  `TextEncoding::{is_read_supported,is_write_supported}` and Connection's
  encoding-aware dispatch. The existing UTF-16 stock-oracle keepers cover this
  surface; their presence is not a fresh executed release gate.

Whether a documented limitation is acceptable for a particular release is a
product decision. It must be made explicitly in the release handoff, together
with the associated test and compatibility evidence.

## Irreversible hold points

| Hold point | Why it is one-way | Required proof before crossing |
|---|---|---|
| Create and push a version tag | A published tag becomes release provenance; correcting it normally requires a new version. Since the tag trigger is disabled, tagging no longer publishes, but it is still one-way as provenance. | Freeze commit, exact version agreement, and recorded source SHA. |
| Publish each crate | A crate version cannot be reused; a failure can leave a partial registry line. | Full dependency order, dry-run evidence, credentials, and a stop-on-failure plan. |
| Create the public release and upload artifacts | This changes the default installer target and exposes artifacts to users. | Matrix artifacts, manifest/signature verification, and installer rehearsal. |
| Rotate signing policy | Existing installers must still select the intended trust policy. | Explicit compatibility review of both installers and an exact-version verification. |

Do not rely on a presumed workflow state to make tagging safe. The `v*` tag
trigger is disabled and the publish job is fail-closed, so no checked-in
automation should react to a tag; verify that this is still true in the tree
you are cutting from rather than trusting this sentence.

## Shortest safe critical path

1. **Decide scope and clear the acceptance gate.** Capture the current tracker
   snapshot; resolve or formally de-scope every stop-ship item. Preserve the
   concurrent-writer default and the supported async fallback while doing so.
2. **Prepare one reversible freeze commit.** Update all lockstep versions and
   internal requirements, date the changelog, and refresh generated dependency
   state through the approved remote Cargo route. Review dev-dependency bounds
   as part of that commit.
3. **Run the release verification matrix.** Use the approved remote execution
   policy for Cargo checks, tests, lints, formatting, and release-profile
   evidence. Capture source SHA, toolchain, command lines, and complete exits.
4. **Build and rehearse artifacts without public release mutation.** Produce
   the five supported artifacts, generate and verify the manifest/signature,
   then test exact-version, offline, and default-installer behavior in clean
   environments.
5. **Cross the hold points deliberately.** Create the tag only after the
   reversible gates are green; publish in DAG order and stop on any failure;
   finally create the public release and verify the newly public install path.

## Release-time evidence checklist

The final handoff should contain immutable or reproducible evidence for:

- source SHA, tag, clean-scope/peer-dirt note, and lockstep version inventory;
- current tracker dependency snapshot and disposition of every acceptance gate;
- remote Cargo commands, toolchain, workers, exits, and first failures where
  applicable;
- registry preflight and publish receipts in dependency order;
- all artifact hashes, manifest-signature verification, and provenance/SBOM;
- clean-environment installer receipts for Linux, macOS, and Windows coverage;
- the live automation policy at tag time and the operator who authorized any
  irreversible action.

This checklist is intentionally stronger than a green local build: a release is
a public, irreversible composition of code, registry metadata, artifacts,
signing policy, and installer behavior.
