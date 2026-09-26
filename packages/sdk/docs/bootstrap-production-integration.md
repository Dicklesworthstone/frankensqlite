# Production SDK bootstrap integration on reference SQLite

The bootstrap transfer coordinator and HTTP endpoint must be exercised together
with the storage components they call. Contract fixtures remain useful for
malformed peer responses, but cannot validate the real receiver, fanout cursors,
source outbox, or ordered handoff. This suite imports the actual source modules:

- Capture and chunked source outbox, including their transactional publication.
- Manifest recovery, bootstrap staging/install, row application, and codec.
- Transfer coordinator, HTTP client and handler, order ledger, and fanout storage.

`production-source-loader.mjs` resolves local extensionless imports only; it does
not redirect any module to a fake implementation. `SqliteTarget` implements only
the SQL/transaction owner interface using Node's reference SQLite engine. It
contains no replica or receipt decisions. The HTTP host is a loopback socket to
Fetch Request/Response adapter. Authentication fixtures represent application
policy; no production TLS or deployment certification is implied.

## Executable recovery boundaries

The suite covers bounded twelve-chunk resume; no application visibility during
staging; independent file-reader isolation during installation; an actually
lost HTTP install response; renewed confirmation and exact source acknowledgement;
ordered sequence N+1 after seed installation; and retained replay after later
application mutations. Two required replicas use the production fanout cursor
and minimum-reclamation algorithm, preserving source bytes for the slow receiver.

A damaged unordered installed replay is exercised through both the actual
transfer coordinator and HTTP endpoint. Status may report its stored installed
flag, but only installation revalidation can confirm an ACK. A missing or
altered retained chunk therefore returns an error and leaves source bytes
pending. This reconciled increment preserves the existing upstream replay fix
and its bounded digest projections; it does not replace receiver source code.

The process-death matrix crosses WAL and DELETE journal modes, ordered and
unordered enrollment, and six actual SIGKILL points: after a staging commit,
after the first application INSERT, immediately before installation COMMIT,
after installation COMMIT, before source ACK COMMIT, and after source ACK COMMIT.
Fresh connections reopen both files and complete the same retained baseline.
These 24 process cuts use the direct production transport interface, while the
separate network tests use real HTTP sockets. Process death is not power loss.

Multi-table HTTP cases transfer more than one MiB of BLOBs in <=64 KiB chunks,
with int64 keys, REAL values, embedded NUL/BOM/Unicode text, composite descending
keys, foreign keys and an empty selected table. Native SQLite Session application
provides an independent oracle for the source-generated chunks. The transfer
must not read application source rows again. Additional checks cover UTF-16LE
and UTF-16BE source/receiver databases with maximum-size identities, preserving
the upstream order/fanout encoding fixes rather than replacing them.

## Run

```sh
node --experimental-transform-types \
  --experimental-loader=./packages/sdk/tests/helpers/production-source-loader.mjs \
  --test packages/sdk/tests/changeset-bootstrap-production.test.mjs
```

Validation in this increment: 37/37 tests, zero failures or skips, using Node
22.16.0 / reference SQLite 3.49.1. Strict TypeScript 5.8.3 checking traverses the
actual ten source modules, without dependency declaration substitutes. The
receiver source is byte-identical to upstream blob
`ac616561bf38abf6e124f0b605b9cd6fe67fdab4`. The earlier saved repair is superseded
by upstream commit `4758cb8`; its tests and peer changes remain untouched.

This evidence is SDK-module integration over a reference SQL owner, not a full
FrankenDB/worker package build, FrankenSQLite Rust/WASM/MVCC execution, browser
snapshot durability, real deployment authentication, or physical power-loss proof.
The source's and receiver's top-level transaction ownership and genuine durable
confirmation callbacks remain application requirements. No work is automatically
retried, no global writer lock is introduced, and native concurrency defaults
are unchanged. File-backed tests retain their temporary test directories for
inspection rather than removing files.
