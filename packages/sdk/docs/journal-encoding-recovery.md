# Rebase journal recovery across SQLite text encodings

The retained local-operation and remote-decision paths now work in UTF-8,
UTF-16LE and UTF-16BE databases. Public identities remain bounded by UTF-8
bytes; changing database encoding does not change a bookmark or record digest.

Previously every local operation in either UTF-16 encoding failed while reading
its just-written record: a 64-character hexadecimal digest was required to
occupy exactly 64 database-encoded bytes. It occupies 128 in UTF-16. Maximum
ASCII delivery IDs also failed the remote journal's 512-byte database projection.

The SQL projections now admit bounded database representations, then retain the
existing UTF-8 identity and lowercase hexadecimal validation. Digest projections
require TEXT, at most 128 database bytes, exactly 64 characters and no embedded
NUL. The delivery-ID projection permits at most 1024 database bytes and rejects
NUL before crossing the SQL adapter. The public 512-byte UTF-8 limit is unchanged.
Malformed stored BLOBs, oversized tails and NUL-hidden text are not interpreted
as valid retained evidence. No schema migration or protocol change is needed.

Local capture still commits application work, original bytes and the verified
remote-history basis together. Reopening uses readLocal/rebaseLocal, not a new
capture. Exact retries retain the original basis, callback result policy and
conflict decisions. Missing or corrupted evidence fails rather than being
recreated. No receipt is expired, no replay is automated, and no checkpoint is
performed by these SQL-only helpers.

## Executed verification

```sh
node --experimental-transform-types \
  --experimental-loader=./packages/sdk/tests/helpers/production-source-loader.mjs \
  --test packages/sdk/tests/changeset-journal-encoding.test.mjs
```

The suite executes production journal, capture, apply, codec and rebase modules
with reference SQLite SQL ownership. It covers all three encodings, maximum
ASCII/Unicode/control/BOM identities, exact native Session capture, file reopen,
remote conflict decisions, original local replay, rebased native application,
empty decisions, lost local commit responses, cross-encoding bookmark equality,
and SQL projection checks for corrupted local/remote/receipt hashes.

This is not a full SDK/worker build or FrankenSQLite Rust/WASM/MVCC execution.
No browser persistence, transport, physical power-loss or performance guarantee
is established. Native concurrent-writer defaults and dependencies are unchanged.
