# Installed bootstrap recovery integrity

An installation receipt is usable for source reclamation only after the receiver
revalidates its retained installation evidence and completes storage confirmation.
That rule applies to unordered receivers as well as receivers with an order ledger.

Installed `ChangesetBootstrapReceiver.install()` replays now check the complete
chunk population, every indexed digest/length/change count, the reclaimed-body
state, and the manifest's ordered hash chain and totals. Missing, extra or changed
metadata rejects before confirmation. The transfer coordinator therefore retains
the source seed after a lost install response followed by damaged receiver history.
The installed flag alone is not sufficient. No missing history is recreated.

Verification reads one metadata entry at a time. It does not reload reclaimed
payloads, rescan application rows or reapply the baseline. Later legitimate row
updates remain untouched. Ordered replay additionally verifies the existing order
prefix and preserves later incremental progress. Replay is bounded-memory but
linear in retained chunks; cancellation can interrupt it before acknowledgement.

Digest projections require bounded, NUL-free TEXT and accommodate SQLite UTF-8,
UTF-16LE and UTF-16BE storage. SQL `length(TEXT)` alone is not a byte bound: a NUL
can hide a large tail. The binary-length guards run before values cross the SQL
adapter boundary. This does not change public UTF-8 identity limits or the wire
protocol. Local SQL metadata and adapters remain trusted; hashes are not sender
authentication or protection against an authorized writer rewriting all evidence.

## Executed production-module integration

Run from the repository root with Node 22.16.0 or a compatible Node runtime:

```sh
node --experimental-loader=./packages/sdk/tests/helpers/bootstrap-integration-loader.mjs \
  --test packages/sdk/tests/changeset-encoding-integration.test.mjs \
  packages/sdk/tests/changeset-bootstrap-replay-integration.test.mjs \
  packages/sdk/tests/changeset-bootstrap-public-integration.test.mjs
```

On Node 22.16.0 / reference SQLite 3.49.1, all 100 tests pass without skips.
The source loader transforms TypeScript but substitutes no production modules.
Actual public capture/outbox creation, manifest recovery, transfer, receiver,
application, order, fanout and storage modules run together. Tests compare capture
with native Session output, resume bounded uploads, lose install/source responses,
retain slow-replica bytes, verify all three encodings and preserve sequence N+1.
The replay/public subset fails 29 of 61 tests on the original receiver and passes
all 61 with the repair; the separate encoding suite originally failed 26 of 39.

Two child processes are actually killed after receiver COMMIT but before its
confirmation/response, under WAL and DELETE journals. Reopened files recover the
same seed without repeating receiver inserts. These tests supply reference SQLite
SQL transaction ownership, not FrankenSQLite Rust/WASM/MVCC execution. The new
public-API tests use direct in-process transport, not the HTTP client/handler.
Full SDK/worker packaging, browser storage and physical power loss are not tested.
Strict TypeScript checking passes against the actual transitive production sources,
without declaration fixtures; that is still narrower than a full SDK build.
