# SQLite session changesets in the JavaScript SDK

`decodeChangeset`, `encodeChangeset` and `invertChangeset` read and write the
SQLite session extension's binary changeset format. This is row-level change
interchange, not a whole database image, SQL dump, patchset, or new wire format.
It is independent of the native Rust session facade and does not pretend the
WASM handle has automatic session capture methods that it does not expose.

```ts
import { decodeChangeset, encodeChangeset, invertChangeset } from '@frankensqlite/sdk';
const tables = decodeChangeset(receivedBytes);
const wire = encodeChangeset(tables);
const undo = invertChangeset(wire);
```

The table descriptor carries its unqualified name, primary-key bytes in column
order, and changes in their original order. Integers decode as `bigint` (signed
64-bit), reals as `number`, text as `string`, blobs as owned `Uint8Array`, and SQL
NULL as `null`. `undefined` means an omitted UPDATE field, NOT SQL NULL. Author
integers using bigint, including `1n`; a number such as `1` intentionally encodes
a real. The indirect flag and nonzero primary-key ordinal bytes are preserved.
INSERT and DELETE carry complete rows; UPDATE keeps keys in its old record and
pairs old/new values only for modified non-key columns. Key changes must be a
DELETE and INSERT, as in SQLite's session output. Inversion keeps table/change
order and original UPDATE keys; it is not array reversal.

The decoder freezes structural arrays/objects. Blob bytes are independent,
exact-sized copies and remain mutable for normal JavaScript interoperability.
Changing a decoded blob changes a subsequent encoding, never the source bytes.
Table names are limited to 1024 UTF-8 bytes. Invalid UTF-8, unpaired UTF-16 on
encoding, NaN reals, NULL/missing keys, incomplete rows, malformed UPDATE pairs,
repeated table headers, noncanonical length varints, and truncated input reject.
Empty input is a valid empty changeset. Fixed non-shared ArrayBuffer-backed
inputs are required; shared, resizable and detached buffers are not accepted.

Optional limits cap bytes (default/hard maximum 64 MiB), tables (256), changes
(100,000), and total old/new slots (1,000,000), including omitted UPDATE slots.
Columns default to 2,000, configurable to 32,768 for custom SQLite builds.
Limits are checked before row allocation, not just after decoding. Encoding
uses the same semantic validator. These are protocol/allocation bounds, not
bounds on SQL execution memory or process RSS. Both operations materialize the
bounded changeset; neither is advertised as streaming.

Compact SQLite patchsets deliberately reject: they omit before-images and
cannot provide the same conflict detection or inversion guarantees. The codec
also does not implement changegroup coalescing, rebasing, automatic capture,
or authentication. Validate the source of received changes before applying
anything to application tables; format validation is not authorization.

## Verification

```sh
node --experimental-loader=./packages/sdk/tests/helpers/source-loader.mjs \
  --test packages/sdk/tests/changeset-codec.test.mjs
```

Tests use Node's actual SQLite session extension to generate binary inputs,
compare exact re-encoding, and apply SDK-authored/inverted changesets. They
include composite WITHOUT ROWID keys, mixed operations, all storage classes,
integer extremes, UTF-8/NUL/BOM handling, malformed input and allocation limits,
and 30 deterministic native mutation workloads. This is SQLite wire-format
interoperability evidence, not FrankenSQLite engine/WASM/browser certification.
