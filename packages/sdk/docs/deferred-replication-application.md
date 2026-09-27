# Apply dependent changes as one foreign-key-clean transaction

`applyChangeset`, `applyPatchset`, and `ChangesetBootstrapReceiver` accept the
explicit option `foreignKeys: "defer"`. It composes the existing
`withDeferredForeignKeys` transaction policy into their actual application path.
Defaults are unchanged. Unknown policy values reject before SQL admission.

This allows child-first rows and cyclic references to be installed without an
application manually manipulating a connection-wide pragma. Every referenced
row must exist by the end of the owned application scope. An unresolved FK is
an error, not a conflict omission or permission to issue a successful receipt.

```ts
// Enable enforcement when provisioning/opening the owned connection.
await destination.execute('PRAGMA foreign_keys=ON');

await applyChangeset(destination, bytes, {
  tables: ['child', 'parent'],
  deliveryId: 'source:transaction-42',
  foreignKeys: 'defer',
  // Add generatedColumns: 'recompute' for compatible generated-column schemas.
});
```

For streamed bootstrap, the boundary is the COMPLETE installation, not a chunk:

```ts
const receiver = new ChangesetBootstrapReceiver(destination, {
  receiverId: 'replica',
  tables: ['child', 'parent'],
  foreignKeys: 'defer',
  confirmCommit: () => destination.checkpoint(),
});
for (let i = 0; i < manifest.chunks; i++) {
  await receiver.stage(manifest, i, await readRetainedChunk(i));
}
const installed = await receiver.install(manifest);
```

Staging, status, and discard still use the original owner without enabling
FK deferral or scanning application rows for FK validity. Installation wraps
one outer transaction, encompassing all chunks, optional order-prefix receipts,
body reclamation and the installed marker. The per-chunk apply calls do NOT
create separate deferred scopes. Cross-chunk references and cycles can therefore
resolve before the final check. No new BEGIN/COMMIT is introduced by the wrapper.
The existing HTTP bootstrap handler uses this configured receiver unchanged.

## Enforcement, cleanup and recovery

Enforcement must already be ON. This option never executes `foreign_keys=OFF`,
never silently turns enforcement on, and never disables cascades or triggers.
The shared helper checks a foreign-key-clean entry and exit in main, TEMP and
all attached schemas. That whole-connection check is necessary because deferral
is connection-wide: switching it off must not erase an unrelated deferred
violation. Entry violations reject before changing the pragma. Exit violations
roll back rows, hook output and receipts; bootstrap staging remains recoverable.

The previous deferral flag is restored even after callback failure or cancellation.
Admitted SQL must settle before restoration and rollback. A restoration error
reports `ERR_FSQLITE_FOREIGN_KEY_STATE` with cleanup evidence; reconcile or discard
the connection after its owner drains rollback. No automatic retry occurs.

A standalone apply scope must finish clean. A nested result remains provisional
until the outer owner commits; it cannot absorb the outer owner's pre-existing
violations. To resolve dependencies spanning MULTIPLE application calls, wrap the
whole real owner once with `withDeferredForeignKeys` rather than independently
deferring each incomplete fragment. Do not add `foreignKeys: 'defer'` to those
inner fragments: their separate clean-entry/exit checks would reject them.

Cascading actions still run, and CHECK/NOT NULL/UNIQUE constraints retain their
ordinary behavior. Deferral is not a general deferred-constraint implementation
or a solution for executing source business triggers twice on a replica. Sources
and receivers must still provision compatible trusted schemas and expressions.
It does not transfer DDL, change wire protocols or choose a policy from remote data.
The ordinary ChangesetReceiver convenience class is unchanged; direct application
and atomic bootstrap expose this option, while custom ordered callbacks can pass
it to applyChangeset.

Successful apply replay checks the configured FK boundary without rerunning its
rebase hook. Installed bootstrap replay also rechecks the boundary and verifies
its retained evidence before repeating storage confirmation. A failure after
COMMIT remains an uncertain response: reconcile the SAME identity and payload.
Snapshot-backed databases still require genuine same-target storage confirmation.
This option does not itself checkpoint a standalone apply result.

## Cost and verification

The shared helper checks every attached schema twice. Returned violations are
bounded, but the database's scan work and memory are not. This explicit policy
is a correctness tradeoff, not a claim of constant cost for a small changeset.
It introduces no process-wide writer mutex, SQL queue or native concurrency change.

```sh
node --experimental-transform-types \
  --experimental-loader=./packages/sdk/tests/helpers/production-source-loader.mjs \
  --test packages/sdk/tests/changeset-deferred-application.test.mjs \
  packages/sdk/tests/changeset-rebase-hook-lifetime.test.mjs
```

On Node 22.16.0 / reference SQLite 3.49.1, 67 tests pass with no failures/skips:
47 deferral integration cases plus 20 rebase-hook lifetime regressions. The same
47-case suite fails 43 cases against the original applier/bootstrap without the
policy wiring. Actual application, bootstrap, codec, foreign-key helper, order,
store and fanout modules execute; no replication backend is substituted. Native
Session changesets and patchsets provide independent input and native application
is checked across UTF-8/UTF-16LE/UTF-16BE. Cases include generated FKs, omissions,
pre-existing attached/TEMP violations, nested scopes, cleanup failure, cancellation,
whole-seed rollback, ordered N+1, real HTTP and independent-reader isolation.
Eight child processes are actually SIGKILLed in WAL/DELETE journals at first row,
before/after COMMIT and during confirmation. Reopened files recover all-or-none
installation and retained staging. A child must report its exact cut before its
signal exit; timeout-killed children cannot count as a successful crash test.
Strict TypeScript checking traverses the actual current source dependencies.

This is SDK-component evidence over a Node SQLite transaction-owner adapter,
not FrankenSQLite Rust/WASM/MVCC execution, full SDK/worker packaging, browser
storage, production TLS/authentication or physical power-loss qualification.
