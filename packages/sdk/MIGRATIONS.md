# Atomic, verified application schema migrations

`FrankenMigrationPlan` fills the worker-backed SDK's schema-upgrade surface.
It is separate from the native Rust `MigrationRunner`: it neither reads nor
modifies that runner's `_schema_migrations` table, and never changes
`PRAGMA user_version`.

```ts
import { FrankenDB, FrankenMigrationPlan } from "@frankensqlite/sdk";

const schema = new FrankenMigrationPlan([
  { version: 1, name: "create_notes", statements: [
    "CREATE TABLE notes(id INTEGER PRIMARY KEY, body TEXT NOT NULL)",
  ] },
  { version: 2, name: "tag_notes", statements: [
    "ALTER TABLE notes ADD COLUMN tag TEXT",
    "CREATE INDEX notes_tag ON notes(tag)",
  ] },
]);
const db = await FrankenDB.open();
const before = await schema.inspect(db); // No ledger/schema writes.
const upgraded = await schema.apply(db, { timeoutMs: 5000 });
console.log(before.pending, upgraded.applied, upgraded.currentVersion);
```

A plan captures and freezes all definitions synchronously. Changing the caller's
objects, arrays or strings later cannot change what is executed. Versions must
be strictly increasing positive JavaScript safe integers; numeric gaps are
allowed, duplicates and sorting guesses are not. Each name is 1..256 characters.
Plans are bounded to 256 migrations, 4096 total statements and 4 Mi UTF-16 code
units of SQL. Empty plans are valid, but cannot be used to ignore an existing
nonempty migration history.

Supply **one complete statement per array entry**, not a semicolon-split script.
A complete CREATE TRIGGER, including its internal semicolons, is one entry.
The shared worker SQL preflight handles quotes/comments and refuses scripts,
manual transaction boundaries, NUL and unbound parameters before a plan can
execute. Supported prefixes are ordinary CREATE/ALTER/DROP and data backfill
INSERT/UPDATE/DELETE/REPLACE/WITH. PRAGMA, ATTACH/DETACH, VACUUM, explicit TEMP
and virtual-table setup are not admitted. The core still parses all SQL.

## History verification and atomicity

The reserved main-schema table `_fsqlite_sdk_migrations_v1` stores version,
name and SHA-256. The digest covers the exact definition encoded as UTF-8 JSON:
`["frankensqlite-sdk-migration",1,version,name,statements]`. Whitespace, comments,
Unicode, order and statement boundaries count. A secure-context Web Crypto
implementation is required. A digest records definition identity; it is not
an authentication mechanism, a live schema fingerprint or a database checksum.

Every inspection/application verifies the **entire recorded history** as an
exact prefix of the supplied plan, not just MAX(version). A renamed or edited
applied version is `ERR_FSQLITE_MIGRATION_DRIFT`. Missing/interleaved/unknown
versions, older application plans, malformed rows, unsupported table shapes
and ledger triggers are `ERR_FSQLITE_MIGRATION_HISTORY`. Refusals run no pending
migration SQL and do not repair, delete or adopt history. Keep original applied
definitions and append a new version instead. Deliberately erasing the entire
ledger or its final suffix cannot be independently detected without an external
trusted record; this is not tamper-proof history.

`apply()` owns one **outer managed transaction** for every pending version,
including history creation and inserts. A failure in a later version rolls back
earlier pending DDL, data backfills and transactional trigger effects together.
Previously committed migrations remain intact. History is checked again before
and after each new history insertion, so SQL which erases/replaces the ledger
or silently suppresses an insertion cannot be acknowledged as a valid upgrade.
All generated ledger SQL is main-qualified; a same-named TEMP table cannot
redirect it. Attached databases are refused. A migration must leave the TEMP
schema as it found it, so temporary-only schema cannot be falsely recorded as
a persistent upgrade. Scratch tables created and removed within one migration
are permitted. Existing temporary data is not fingerprinted.

SQL is **trusted application code**, not an authorization sandbox. User-defined
functions can have external side effects which SQL rollback cannot undo. Do
not use migration SQL to edit the reserved ledger or mutate other connections,
external services or connection settings. Configure connection PRAGMAs, including
foreign-key enforcement, before entering the migration transaction. No downward
migration, repair-by-truncation, schema-inference or silent legacy import exists.

`inspect()` reads a consistent managed snapshot and does not create the ledger.
It returns immutable `{ currentVersion, applied, pending }` identities. Applying
a verified up-to-date plan returns an empty `applied` list and performs no schema
or data writes. `MigrationResult` is immutable and contains `previousVersion`,
`currentVersion` and the identities applied by that transaction only.

## Lifecycle and persistence

The connection is claimed before asynchronous hashing or SQL. Foreign operations
cannot interleave while the plan is running. `signal` and `timeoutMs` have the
normal managed-transaction contracts: active work and cleanup are joined before
rollback/rejection, rather than racing an abandoned operation. An acknowledged
COMMIT wins over a cancellation received after commit dispatch. SQL errors and
rollback/transport failures preserve the existing SDK error contracts.

Ordinary `apply(db)` is **single-attempt**, and its result acknowledges SQL
COMMIT, not browser persistence. For `indexeddb-snapshot`, explicitly await
`db.checkpoint()` to save the resulting image. Closing or terminating a worker
does not automatically save a migration. Never infer that a failed/lost COMMIT
or checkpoint acknowledgement proves that the migration did not take effect;
reopen authoritative state and verify history before deciding what to do next.

## Verification

```sh
node --test packages/sdk/tests/migrations.test.mjs
```

Requires Node 22.16+ with `node:sqlite` and the workspace TypeScript dependency.
`FSQLITE_TYPESCRIPT_MODULE` may select an installed TypeScript module. Tests
execute production migration, SDK transaction and worker SQL-preflight code
against an explicitly injected Node-SQLite transport. They exercise real DDL,
constraints, trigger effects, rollback, reopening, prefix/drift checks and
lifetime failures. They do not certify native FrankenSQLite SQL parity, worker
IPC, WASM or actual browser storage. Transpilation is not a workspace typecheck.

## Contention, FIFO queues and checkpoint-on-commit

The plan also exposes:

- `applyWithRetry(db, TransactionRetryOptions)` for opt-in whole-transaction
  conflict recovery. Ordinary `apply()` still runs once.
- `applyQueued(queue, QueuedTransactionOptions)` to apply at one FIFO job
  boundary, preserving wait/active deadlines, cancellation and close draining.
- `applyQueuedWithRetry(queue, QueuedTransactionRetryOptions)` to retain one
  bounded queue slot across every attempt, rollback, backoff and publication.

```ts
import { FrankenDBQueue, FrankenMigrationPlan } from "@frankensqlite/sdk";

const schema = new FrankenMigrationPlan([
  { version: 1, name: "notes", statements: [
    "CREATE TABLE notes(id INTEGER PRIMARY KEY, body TEXT NOT NULL)",
  ] },
]);
const queue = await FrankenDBQueue.open(
  { dbName: "offline-notes", persistence: "indexeddb-snapshot" },
  { checkpointOnCommit: true },
);
const upgrade = schema.applyQueuedWithRetry(queue, {
  maxAttempts: 4, timeoutMs: 5000, waitTimeoutMs: 1000,
});
// This job cannot overtake migration hashing, SQL, rollback or publication.
const write = queue.transaction(tx => tx.execute("INSERT INTO notes(body) VALUES (?)", ["hello"]));
await upgrade;
await write;
await queue.close();
```

A retry rereads history in its new transaction; no previously computed pending
list is reused. If another initializer won the race, its matching history makes
the new attempt a no-op rather than replaying seed rows or data backfills. A
winner with different migration definitions produces a drift refusal. Only the
existing SDK's typed BUSY-family errors and confirmed rollback authorize replay;
constraint/drift/history errors and uncertain commit outcomes do not. SQL which
calls external side-effecting functions must be safe to replay before opting in.

On a `checkpointOnCommit` queue the migration result is returned only after
snapshot publication completes. The entire upgrade publishes once, not once per
version or retry. Publication is outside SQL retries: a failure after SQL COMMIT
is a `FrankenCheckpointCommitError` whose `value` is the `MigrationResult` and
whose `sqlCommitted` is true. Later jobs are fenced before any SQL. Export and
explicit checkpoint recovery retain the existing queue contract; never rerun
the committed migration to repair a failed publication. Ordinary memory queues
and queues without this option acknowledge SQL commit only.

Separate browser workers still have independent in-memory databases, not native
cross-tab MVCC. If they loaded the same stored revision, a losing snapshot CAS
is a committed-state publication failure, not permission to merge schemas or
replay SQL. Reopen/reconcile authoritative state. These APIs do not change the
storage conflict contract or make browser retention unconditional.

A queued migration uses the queue's normal change journal. Data backfills notify
only for the successful local SQL commit, not for rolled-back attempts or an
idempotent history-only rerun. Existing subscriptions protect their schema:
unsubscribe and await `done` before upgrading a watched table. Do not subscribe
to the reserved history table. The runner does not silently remove application
subscriptions. Direct `inspect(db)` remains a database operation, not a queued
checkpoint-producing dry run; no inspection method is exposed on a child
transaction or on the queue's private connection.

The migration tests additionally reproduce real SQLite WAL snapshot conflicts
between two initializer connections and rollback-journal COMMIT contention.
Queue, notification, export/reopen and checkpoint failure tests execute their
production implementations with the repository's explicit IndexedDB transaction
model. They are not browser IndexedDB, worker IPC or native engine certification.
