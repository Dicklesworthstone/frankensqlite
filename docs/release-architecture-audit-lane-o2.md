# Release architecture and safe release path

This is a maintained architecture note, not a read-only execution receipt. It
records the release boundaries that are stable enough to guide a future cut;
live service state, host health, credentials, and issue counts must be checked
again at the release decision.

## Scope and evidence boundary

The original evidence-only audit was committed in `e1a73d88`. This revision
reconciles the document with the current local tree without running Cargo,
GitHub Actions, publishing, tagging, uploading, or changing release source.
It deliberately does not reproduce operational host names, filesystem key
locations, public-key material, or momentary service status.

Local-tree facts checked for this revision:

- The workspace has 27 members, all locally declared at `0.2.0`; two members
  are marked `publish = false`.
- `v0.1.19` is the preceding release tag. A commit count relative to that tag
  is intentionally omitted because it changes as release preparation lands and
  is not a release-readiness metric.
- `CHANGELOG.md` contains an unreleased `0.2.0` section describing the next
  lockstep release.
- The checked-in release workflow contains an explicit 25-crate publish order,
  checks that its crate set matches the publishable workspace set, and checks
  every crate version against the tag before publishing. Its dependency order
  requires an independent review; the workflow does not validate topology.
- The Unix and PowerShell installers both resolve an unspecified version from
  the latest GitHub Release and require a SHA-256 manifest; when `minisign` is
  available they also verify its signature.

Claims about crates.io, GitHub Releases, workflow enablement, build machines,
or Beads blockers are intentionally absent here because they are live state.
They are release-time checks, not properties of this document.

## Release architecture

### Version line

The project uses a lockstep workspace version. A release cut therefore changes
the workspace crates, internal dependency requirements, the changelog heading,
and the release tag as one coherent line. The tag version is the provenance
anchor used by the publish workflow's validation.

For every lockstep minor cut, review each internal dependency requirement
rather than assuming its old semver upper bound admits the new line. In
particular, dev-dependencies can be invisible to normal publish ordering while
still becoming permanent registry metadata after publication.

### Publish DAG

The checked-in release workflow lists 25 publishable crates in an order that an
independent manifest review found consistent with normal-dependency topology.
The workflow itself checks that the crate set exactly matches Cargo metadata,
then requires every package version to equal the release tag; it does not prove
that the configured order is topological. Two workspace members are
intentionally non-publishable.

That validation is useful, but it is not evidence that a release is ready:
metadata, lockfile, credentials, registry state, and the actual workflow policy
must all be checked at cut time. A manual release should preserve the same
leaf-to-root dependency order and stop on the first publish failure.

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

Both installers consume a versioned release artifact, a `SHA256SUMS.txt`
manifest, and—when the verifier is present—a matching minisign signature. The
offline path requires an explicit version and checksum unless verification is
explicitly disabled. A release rehearsal must exercise the default latest-
release path as well as these exact-version and offline paths; a valid archive
alone does not prove that users can install it.

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

## Current release gate

The final release-handoff Bead is the tracker authority for outstanding
correctness and acceptance work. Its dependency set, ownership, and status are
expected to change while peers work; do not copy a blocker count or ownership
snapshot into release documentation. At the release decision, query the tracker
and attach an immutable snapshot to the handoff package.

The README makes two useful, durable constraints explicit:

- no current numeric performance claim is release evidence until it has a
  same-source, provenance-bound benchmark artifact;
- the currently supported SQLite text-encoding surface is UTF-8, and valid
  UTF-16 databases must fail closed unless the release scope changes.

Whether a documented limitation is acceptable for a particular release is a
product decision. It must be made explicitly in the release handoff, together
with the associated test and compatibility evidence.

## Irreversible hold points

| Hold point | Why it is one-way | Required proof before crossing |
|---|---|---|
| Create and push a version tag | A published tag becomes release provenance; correcting it normally requires a new version. | Freeze commit, exact version agreement, and recorded source SHA. |
| Publish each crate | A crate version cannot be reused; a failure can leave a partial registry line. | Full dependency order, dry-run evidence, credentials, and a stop-on-failure plan. |
| Create the public release and upload artifacts | This changes the default installer target and exposes artifacts to users. | Matrix artifacts, manifest/signature verification, and installer rehearsal. |
| Rotate signing policy | Existing installers must still select the intended trust policy. | Explicit compatibility review of both installers and an exact-version verification. |

Do not rely on a presumed workflow state to make tagging safe. Before creating a
tag, establish whether any enabled automation reacts to it and whether that
behavior is intended for this cut.

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
