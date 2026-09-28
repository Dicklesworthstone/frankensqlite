import type {
  ApplyChangesetOptions,
  ApplyChangesetResult,
  ChangesetExecutor,
  ChangesetTarget,
} from "./changeset-apply";
import { applyChangeset, CHANGESET_RECEIPTS_TABLE } from "./changeset-apply";
import type { ChangesetLimits, ChangesetValue } from "./changeset-codec";
import { decodeChangeset, decodeRebaseInfo, resolveChangesetLimits } from "./changeset-codec";
import { ChangesetRebaser } from "./changeset-rebase";
import type { CaptureChangesetOptions } from "./changeset-capture";
import { prepareChangesetCapture } from "./changeset-capture";
import type { ChangesetOutboxOptions, OutboxDelivery } from "./changeset-outbox-store";
import { TABLE as OUTBOX, ensure as ensureOutbox, find as findOutbox,
  load as loadOutbox, store as storeOutbox } from "./changeset-outbox-store";
import { captureFanoutGuard } from "./changeset-fanout";

export const REBASE_JOURNAL_HEADS_TABLE = "__fsqlite_rebase_journal_heads";
export const REBASE_JOURNAL_ENTRIES_TABLE = "__fsqlite_rebase_journal_entries";
export const REBASE_JOURNAL_LOCALS_TABLE = "__fsqlite_rebase_journal_locals";
export interface ChangesetRebaseJournalOptions {
  /** Stable identity for ONE local history, not the sending peer's identity. */
  journalId: string;
  /** All retained applications, including empty decisions. Default 10,000. */
  maxEntries?: number;
  /** Retained rebase wire bytes, not database/heap/RSS. Default 64 MiB, max 1 GiB. */
  maxBytes?: number;
  /** Retained original local operations, including net-zero work. Default 10,000. */
  maxLocalEntries?: number;
  /** Retained original local wire bytes. Default 64 MiB, maximum 1 GiB. */
  maxLocalBytes?: number;
  /** Per-message and in-memory combined rebaser limits. */
  limits?: ChangesetLimits;
}
export interface RebaseJournalOperationOptions {
  signal?: AbortSignal;
  timeoutMs?: number;
}
export type RebaseJournalApplyOptions = Omit<ApplyChangesetOptions, "onRebase" | "limits" | "deliveryId"> & {
  deliveryId: string;
};
export interface RebaseJournalHead {
  readonly journalId: string;
  readonly position: number;
  readonly byteLength: number;
}
export interface RebaseJournalEntry {
  readonly journalId: string;
  readonly position: number;
  readonly deliveryId: string;
  readonly messageSha256: string;
  readonly messageBytes: number;
  readonly sha256: string;
  /** Owned native apply_v2 decision bytes, not a database or changeset image. */
  readonly rebaseInfo: Uint8Array;
}
export interface RebaseJournalApplyResult extends ApplyChangesetResult {
  readonly entry: RebaseJournalEntry;
}
/** Content identity of an entire ordered history prefix, not authentication. */
export interface RebaseJournalBookmark {
  readonly format: "fsqlite-rebase-bookmark-v1";
  readonly journalId: string;
  readonly position: number;
  readonly sha256: string;
}
/** Immutable metadata and fresh owned ORIGINAL bytes, never previously rebased output. */
export interface RebaseJournalLocalRecord {
  readonly journalId: string;
  readonly operationId: string;
  readonly basis: RebaseJournalBookmark;
  readonly sha256: string;
  /** Binds the payload digest, basis, capture scope and counters; not authentication. */
  readonly recordSha256: string;
  readonly byteLength: number;
  readonly changes: number;
  readonly touchedRows: number;
  readonly changeset: Uint8Array;
}
export type RebaseJournalCaptureResult<T> =
  | { readonly replayed: false; readonly value: T; readonly record: RebaseJournalLocalRecord }
  | { readonly replayed: true; readonly record: RebaseJournalLocalRecord };
export interface RebaseJournalRangeOptions extends RebaseJournalOperationOptions {
  /** Exclusive basis. A bookmark also verifies the excluded prefix. Default zero. */
  after?: number | RebaseJournalBookmark;
  /** Inclusive bound; a bookmark pins its history. Defaults to this snapshot's tip. */
  through?: number | RebaseJournalBookmark;
}
export interface RebaseJournalResult {
  readonly journalId: string;
  readonly after: number;
  readonly through: number;
  readonly changeset: Uint8Array;
  /** Both identities were verified in the same SQL snapshot as the rebasing. */
  readonly afterBookmark: RebaseJournalBookmark;
  readonly throughBookmark: RebaseJournalBookmark;
}
export class RebaseJournalError extends Error {
  constructor(
    readonly code:
      | "ERR_FSQLITE_REBASE_JOURNAL_INPUT"
      | "ERR_FSQLITE_REBASE_JOURNAL_SCHEMA"
      | "ERR_FSQLITE_REBASE_JOURNAL_CORRUPT"
      | "ERR_FSQLITE_REBASE_JOURNAL_LIMIT"
      | "ERR_FSQLITE_REBASE_JOURNAL_MISSING"
      | "ERR_FSQLITE_REBASE_JOURNAL_HISTORY"
      | "ERR_FSQLITE_REBASE_JOURNAL_EXPIRED"
      | "ERR_FSQLITE_REBASE_JOURNAL_CANCELLED"
      | "ERR_FSQLITE_REBASE_JOURNAL_TIMEOUT",
    message: string,
  ) {
    super(message);
    this.name = "RebaseJournalError";
  }
}
const HEADS = `main."${REBASE_JOURNAL_HEADS_TABLE}"`;
const ENTRIES = `main."${REBASE_JOURNAL_ENTRIES_TABLE}"`;
const LOCALS = `main."${REBASE_JOURNAL_LOCALS_TABLE}"`;
const MAX_WIRE = 64 * 1024 * 1024;
const MAX_POSITION = Number.MAX_SAFE_INTEGER;
const RETENTION_NAME = "__fsqlite_rebase_journal_retention";
const RETENTION = `main."${RETENTION_NAME}"`;
const LOCAL_PUBLICATION_FORMAT = "fsqlite-rebase-outbox-v1";
const encodeJson = (value: unknown): Uint8Array => new TextEncoder().encode(JSON.stringify(value));
async function localDeliveryId(journalId: string, operationId: string): Promise<string> {
  return `${LOCAL_PUBLICATION_FORMAT}:${await hash(encodeJson([journalId, operationId]))}`;
}
function fail(kind: "INPUT" | "SCHEMA" | "CORRUPT" | "LIMIT" | "MISSING" | "HISTORY" | "EXPIRED" | "CANCELLED" | "TIMEOUT", message: string): never {
  throw new RebaseJournalError(`ERR_FSQLITE_REBASE_JOURNAL_${kind}`, message);
}
function identity(value: unknown): string {
  if (typeof value !== "string" || !value.length || value.length > 512 || value.includes("\0"))
    fail("INPUT", "Journal and delivery identities require 1..512 UTF-8 bytes without NUL");
  const bytes = new TextEncoder().encode(value);
  if (bytes.length > 512 || new TextDecoder("utf-8", { ignoreBOM: true }).decode(bytes) !== value)
    fail("INPUT", "Invalid journal or delivery identity UTF-8");
  return value;
}
function integer(value: unknown): number {
  if (typeof value === "bigint" && value >= 0n && value <= BigInt(Number.MAX_SAFE_INTEGER)) return Number(value);
  if (typeof value === "number" && Number.isSafeInteger(value) && value >= 0) return value;
  return fail("CORRUPT", "Expected a nonnegative safe SQL integer");
}
function bound(value: unknown, fallback: number, maximum: number): number {
  const n = value === undefined ? fallback : value;
  if (typeof n !== "number" || !Number.isSafeInteger(n) || n < 1 || n > maximum)
    fail("INPUT", `Journal limit must be in 1..${maximum}`);
  return n;
}
function position(value: unknown): number {
  if (typeof value !== "number" || !Number.isSafeInteger(value) || value < 0)
    fail("INPUT", "Journal positions must be nonnegative safe integers");
  return value;
}
interface HistoryBoundary {
  readonly position: number;
  readonly sha256: string | null;
}
const BOOKMARK_FORMAT = "fsqlite-rebase-bookmark-v1";
/** Capture before admission; accessors cannot substitute a different bookmark. */
function boundary(value: unknown, journalId: string): HistoryBoundary {
  if (typeof value === "number") return { position: position(value), sha256: null };
  if (typeof value !== "object" || value === null || Array.isArray(value))
    fail("INPUT", "Expected a journal position or history bookmark");
  const field = (key: string): unknown => {
    const property = Object.getOwnPropertyDescriptor(value, key);
    if (!property || !Object.hasOwn(property, "value"))
      fail("INPUT", "Bookmark fields must be own data properties");
    return property.value;
  };
  const format = field("format"), id = identity(field("journalId"));
  const at = position(field("position")), sha256 = field("sha256");
  if (format !== BOOKMARK_FORMAT || typeof sha256 !== "string" || !/^[0-9a-f]{64}$/.test(sha256))
    fail("INPUT", "Invalid journal bookmark format or digest");
  if (id !== journalId) fail("HISTORY", "Bookmark belongs to a different journal");
  return { position: at, sha256 };
}
function verifyBoundary(expected: HistoryBoundary, actual: RebaseJournalBookmark): void {
  if (expected.sha256 !== null && expected.sha256 !== actual.sha256)
    fail("HISTORY", "Journal history differs from its saved bookmark; reconcile the restored or replaced history");
}
function digest(value: unknown): string {
  if (typeof value !== "string" || !/^[0-9a-f]{64}$/.test(value)) fail("CORRUPT", "Invalid journal digest");
  return value;
}
/** Internal column names only. Bound SQL text before the adapter decodes it. */
function storedDigest(column: string): string {
  // A 64-character ASCII digest occupies 128 bytes in either UTF-16 encoding.
  // length(TEXT) alone cannot bound a NUL-hidden tail. Public digest validation
  // still requires exactly 64 lowercase hexadecimal characters after retrieval.
  return `CASE WHEN typeof(${column})='text' AND length(CAST(${column} AS BLOB))<=128 AND length(${column})=64 AND instr(${column},char(0))=0 THEN ${column} END`;
}
function owned(value: Uint8Array, maximum: number): Uint8Array {
  if (!(value instanceof Uint8Array)) fail("INPUT", "Expected Uint8Array bytes");
  const proto = Object.getPrototypeOf(Uint8Array.prototype) as object;
  const get = (name: string): unknown => Object.getOwnPropertyDescriptor(proto, name)!.get!.call(value);
  const buffer = get("buffer"), offset = get("byteOffset") as number, length = get("byteLength") as number;
  if (!(buffer instanceof ArrayBuffer) || Object.getOwnPropertyDescriptor(ArrayBuffer.prototype, "resizable")?.get?.call(buffer))
    fail("INPUT", "Journal input requires a fixed, non-shared buffer");
  if (length > maximum) fail("LIMIT", "Journal input exceeds the byte limit");
  return new Uint8Array(new Uint8Array(buffer, offset, length));
}
async function hash(bytes: Uint8Array): Promise<string> {
  if (!globalThis.crypto?.subtle) fail("INPUT", "Rebase journals require Web Crypto SHA-256");
  const sum = new Uint8Array(await crypto.subtle.digest("SHA-256", bytes));
  return Array.from(sum, (b) => b.toString(16).padStart(2, "0")).join("");
}
function operation(options: RebaseJournalOperationOptions = {}) {
  const signal = options.signal, timeoutMs = options.timeoutMs;
  if (signal !== undefined) {
    try { Object.getOwnPropertyDescriptor(AbortSignal.prototype, "aborted")!.get!.call(signal); }
    catch { fail("INPUT", "signal must be an AbortSignal"); }
  }
  if (timeoutMs !== undefined) bound(timeoutMs, 1, 2_147_483_647);
  const deadline = timeoutMs === undefined ? undefined : performance.now() + timeoutMs;
  const transactionOptions: RebaseJournalOperationOptions = {};
  if (signal !== undefined) transactionOptions.signal = signal;
  if (timeoutMs !== undefined) transactionOptions.timeoutMs = timeoutMs;
  const checkpoint = (): void => {
    if (signal?.aborted) fail("CANCELLED", "Journal operation cancelled");
    if (deadline !== undefined && performance.now() >= deadline) fail("TIMEOUT", "Journal deadline expired");
  };
  checkpoint();
  return { checkpoint, transactionOptions };
}
type Operation = ReturnType<typeof operation>;
/** Preserve the caller's original deadline through shared storage helpers. */
function storageExecutor(owner: ChangesetExecutor, op: Operation): ChangesetExecutor {
  return {
    execute: async (sql, params) => {
      op.checkpoint(); const n = await owner.execute(sql, params); op.checkpoint(); return n;
    },
    query: async (sql, params) => {
      op.checkpoint(); const rows = await owner.query(sql, params); op.checkpoint(); return rows;
    },
  };
}
async function query(tx: ChangesetExecutor, op: Operation, sql: string, params: readonly ChangesetValue[] = []) {
  op.checkpoint();
  const result = await tx.query(sql, params);
  op.checkpoint();
  if (!Array.isArray(result.rowArrays) || result.rowArrays.some((r) => !Array.isArray(r)))
    fail("CORRUPT", "Invalid journal SQL result");
  return result.rowArrays;
}
async function write(tx: ChangesetExecutor, op: Operation, sql: string, params: readonly ChangesetValue[] = []) {
  op.checkpoint();
  const n = await tx.execute(sql, params);
  op.checkpoint();
  if (n !== 1) fail("CORRUPT", "Journal write did not affect exactly one row");
}
const layouts = [
  { name: REBASE_JOURNAL_HEADS_TABLE, sql: HEADS, columns: ["journal_id", "position", "byte_length"], types: ["TEXT", "INTEGER", "INTEGER"], keys: [["journal_id"]] },
  { name: REBASE_JOURNAL_ENTRIES_TABLE, sql: ENTRIES, columns: ["journal_id", "position", "delivery_id", "message_sha256", "message_bytes", "sha256", "byte_length", "rebase_info"], types: ["TEXT", "INTEGER", "TEXT", "TEXT", "INTEGER", "TEXT", "INTEGER", "BLOB"], keys: [["journal_id", "position"], ["journal_id", "delivery_id"]] },
] as const;
/** Validate both tables together; never repair a missing half of existing history. */
async function ensure(tx: ChangesetExecutor, op: Operation, create: boolean): Promise<boolean> {
  const found = await query(tx, op, "SELECT name FROM main.sqlite_schema WHERE name COLLATE NOCASE IN (?, ?)", [REBASE_JOURNAL_HEADS_TABLE, REBASE_JOURNAL_ENTRIES_TABLE]);
  if (found.length === 0) {
    if (await ensureRetention(tx, op, false))
      fail("CORRUPT", "Retained history checkpoint lost its journal tables; do not reinitialize");
    if (!create) return false;
    op.checkpoint();
    await tx.execute(`CREATE TABLE ${HEADS} (journal_id TEXT NOT NULL COLLATE BINARY PRIMARY KEY, position INTEGER NOT NULL, byte_length INTEGER NOT NULL) WITHOUT ROWID`);
    op.checkpoint();
    await tx.execute(`CREATE TABLE ${ENTRIES} (journal_id TEXT NOT NULL COLLATE BINARY, position INTEGER NOT NULL, delivery_id TEXT NOT NULL COLLATE BINARY, message_sha256 TEXT NOT NULL, message_bytes INTEGER NOT NULL, sha256 TEXT NOT NULL, byte_length INTEGER NOT NULL, rebase_info BLOB NOT NULL, PRIMARY KEY(journal_id,position), UNIQUE(journal_id,delivery_id)) WITHOUT ROWID`);
  } else if (found.length !== 2) fail("SCHEMA", "Incomplete rebase journal schema");
  await validateLayouts(tx, op, layouts);
  return true;
}
interface JournalLayout {
  readonly name: string;
  readonly columns: readonly string[];
  readonly types: readonly string[];
  readonly keys: readonly (readonly string[])[];
}
async function validateLayouts(tx: ChangesetExecutor, op: Operation, selected: readonly JournalLayout[]): Promise<void> {
  for (const layout of selected) {
    const listed = (await query(tx, op, `PRAGMA main.table_list('${layout.name}')`)).filter((r) => r[0] === "main" && r[1] === layout.name);
    if (listed.length !== 1 || listed[0]![2] !== "table" || integer(listed[0]![3]) !== layout.columns.length || integer(listed[0]![4]) !== 1)
      fail("SCHEMA", "Journal storage must be the expected ordinary WITHOUT ROWID table");
    const columns = await query(tx, op, `PRAGMA main.table_xinfo('${layout.name}')`);
    if (columns.length !== layout.columns.length) fail("SCHEMA", "Invalid journal column count");
    for (let i = 0; i < columns.length; i++) {
      const r = columns[i]!, pk = (layout.keys[0] as readonly string[]).indexOf(layout.columns[i]!) + 1;
      if (integer(r[0]) !== i || r[1] !== layout.columns[i] || r[2] !== layout.types[i] || integer(r[3]) !== 1 || r[4] !== null || integer(r[5]) !== pk || integer(r[6]) !== 0)
        fail("SCHEMA", "Invalid journal column definition");
    }
    const indexes = await query(tx, op, `PRAGMA main.index_list('${layout.name}')`);
    if (indexes.length !== layout.keys.length) fail("SCHEMA", "Unexpected journal indexes");
    for (let k = 0; k < layout.keys.length; k++) {
      const matches = indexes.filter((r) => r[3] === (k === 0 ? "pk" : "u"));
      if (matches.length !== 1 || integer(matches[0]![2]) !== 1 || integer(matches[0]![4]) !== 0 || typeof matches[0]![1] !== "string") fail("SCHEMA", "Invalid journal key");
      const index = identity(matches[0]![1]);
      const keys = (await query(tx, op, `PRAGMA main.index_xinfo('${index.replaceAll("'", "''")}')`)).filter((r) => integer(r[5]) === 1);
      if (keys.length !== layout.keys[k]!.length || keys.some((r, i) => r[2] !== layout.keys[k]![i] || r[4] !== "BINARY" || integer(r[3]) !== 0))
        fail("SCHEMA", "Journal identities and positions require exact BINARY keys");
    }
    if ((await query(tx, op, `PRAGMA main.foreign_key_list('${layout.name}')`)).length) fail("SCHEMA", "Journal foreign keys are unsupported");
    for (const ns of ["main", "temp"])
      if ((await query(tx, op, `SELECT 1 FROM ${ns}.sqlite_schema WHERE type='trigger' AND tbl_name=? COLLATE NOCASE LIMIT 1`, [layout.name])).length)
        fail("SCHEMA", "Journal triggers are unsupported");
  }
}
const localLayout: JournalLayout = {
  name: REBASE_JOURNAL_LOCALS_TABLE,
  columns: ["journal_id", "operation_id", "scope_sha256", "basis_position", "basis_sha256", "sha256", "record_sha256", "byte_length", "change_count", "touched_rows", "changeset"],
  types: ["TEXT", "TEXT", "TEXT", "INTEGER", "TEXT", "TEXT", "TEXT", "INTEGER", "INTEGER", "INTEGER", "BLOB"],
  keys: [["journal_id", "operation_id"]],
};
async function ensureLocals(tx: ChangesetExecutor, op: Operation, create: boolean): Promise<boolean> {
  const found = await query(tx, op, "SELECT 1 FROM main.sqlite_schema WHERE name=? COLLATE NOCASE LIMIT 2", [REBASE_JOURNAL_LOCALS_TABLE]);
  if (!found.length) {
    if (!create) return false;
    op.checkpoint();
    await tx.execute(`CREATE TABLE ${LOCALS} (journal_id TEXT NOT NULL COLLATE BINARY, operation_id TEXT NOT NULL COLLATE BINARY, scope_sha256 TEXT NOT NULL, basis_position INTEGER NOT NULL, basis_sha256 TEXT NOT NULL, sha256 TEXT NOT NULL, record_sha256 TEXT NOT NULL, byte_length INTEGER NOT NULL, change_count INTEGER NOT NULL, touched_rows INTEGER NOT NULL, changeset BLOB NOT NULL, PRIMARY KEY(journal_id,operation_id)) WITHOUT ROWID`);
  } else if (found.length !== 1) fail("SCHEMA", "Ambiguous local changeset storage");
  await validateLayouts(tx, op, [localLayout]);
  return true;
}
async function localRecordDigest(record: Omit<RebaseJournalLocalRecord, "changeset" | "recordSha256">, scope: string): Promise<string> {
  return hash(new TextEncoder().encode(JSON.stringify([
    "fsqlite-local-changeset-v1", record.journalId, record.operationId, scope,
    record.basis.position, record.basis.sha256, record.sha256, record.byteLength,
    record.changes, record.touchedRows,
  ])));
}

const retentionLayout: JournalLayout = {
  name: RETENTION_NAME,
  columns: ["journal_id", "position", "sha256", "record_sha256"],
  types: ["TEXT", "INTEGER", "TEXT", "TEXT"],
  keys: [["journal_id"]],
};
async function ensureRetention(tx: ChangesetExecutor, op: Operation, create: boolean): Promise<boolean> {
  const objects = await query(tx, op,
    "SELECT 1 FROM main.sqlite_schema WHERE name=? COLLATE NOCASE LIMIT 2", [RETENTION_NAME]);
  if (!objects.length) {
    if (!create) return false;
    await tx.execute(`CREATE TABLE ${RETENTION} (journal_id TEXT NOT NULL COLLATE BINARY PRIMARY KEY, position INTEGER NOT NULL, sha256 TEXT NOT NULL, record_sha256 TEXT NOT NULL) WITHOUT ROWID`);
  } else if (objects.length !== 1) fail("SCHEMA", "Ambiguous journal retention storage");
  await validateLayouts(tx, op, [retentionLayout]);
  return true;
}
function floorSeal(floor: RebaseJournalBookmark): Promise<string> {
  return hash(encodeJson(["fsqlite-rebase-retention-v1", floor.journalId, floor.position, floor.sha256]));
}
async function historyFloor(tx: ChangesetExecutor, op: Operation, journalId: string): Promise<RebaseJournalBookmark> {
  if (await ensureRetention(tx, op, false)) {
    const rows = await query(tx, op, `SELECT CASE WHEN typeof(position)='integer' THEN position END, ${storedDigest("sha256")}, ${storedDigest("record_sha256")} FROM ${RETENTION} WHERE journal_id=? LIMIT 2`, [journalId]);
    if (rows.length) {
      if (rows.length !== 1 || rows[0]!.length !== 3) fail("CORRUPT", "Invalid history checkpoint row");
      const row = rows[0]!;
      const floor = Object.freeze({ format: BOOKMARK_FORMAT, journalId,
        position: integer(row[0]), sha256: digest(row[1]) });
      if (floor.position === 0 || await floorSeal(floor) !== digest(row[2]))
        fail("CORRUPT", "History checkpoint identity or checksum mismatch");
      op.checkpoint();
      return floor;
    }
  }
  const sha256 = await hash(encodeJson([BOOKMARK_FORMAT, journalId]));
  op.checkpoint();
  return Object.freeze({ format: BOOKMARK_FORMAT, journalId, position: 0, sha256 });
}

/**
 * Ordered persistent conflict decisions, atomically coupled to applyChangeset's
 * rows and delivery receipt. A replay MUST find its original journal entry.
 * Bounded retention refuses new applications rather than dropping needed history.
 * enqueueLocal explicitly retains rebased output in the same database outbox.
 * retireLocal releases acknowledged originals; retireThrough explicitly ends
 * older remote-decision retention without removing duplicate-delivery receipts.
 * No automatic pruning, native replication, or retry is implied.
 *
 * The target must own transactions. A nested target remains provisional until
 * its outer commit; browser snapshots still require explicit checkpointing.
 * Local SQL/schema is trusted: checksums detect corruption, not a malicious writer.
 */
export class ChangesetRebaseJournal {
  readonly #target: ChangesetTarget;
  readonly #id: string;
  readonly #maxEntries: number;
  readonly #maxBytes: number;
  readonly #maxLocalEntries: number;
  readonly #maxLocalBytes: number;
  readonly #policy: ReturnType<typeof resolveChangesetLimits>;
  constructor(target: ChangesetTarget, options: ChangesetRebaseJournalOptions) {
    this.#target = target;
    this.#id = identity(options?.journalId);
    this.#maxEntries = bound(options.maxEntries, 10_000, 100_000);
    this.#maxBytes = bound(options.maxBytes, MAX_WIRE, 1024 ** 3);
    this.#maxLocalEntries = bound(options.maxLocalEntries, 10_000, 100_000);
    this.#maxLocalBytes = bound(options.maxLocalBytes, MAX_WIRE, 1024 ** 3);
    this.#policy = Object.freeze(resolveChangesetLimits(options.limits));
  }
  get journalId(): string { return this.#id; }

  /**
   * Save original local changes AND their verified remote-history basis with
   * the application writes. The ID must permanently identify the same work.
   * Replay does not run work, recapture current rows, or move the saved basis.
   * Callback results are not persisted; external effects cannot be rolled back.
   */
  async captureLocal<T>(
    operationId: string,
    work: (tx: ChangesetExecutor) => T | Promise<T>,
    options: CaptureChangesetOptions,
  ): Promise<RebaseJournalCaptureResult<T>> {
    const id = identity(operationId);
    const capture = prepareChangesetCapture(work, options);
    // Reuse the captured signal/deadline; never read caller options twice or
    // restart the deadline after queueing, hashing or transaction admission.
    const op: Operation = capture;
    const tables = capture.tables.map((name) => name.replace(/[A-Z]/g, (c) => c.toLowerCase())).sort();
    const scope = await hash(new TextEncoder().encode(JSON.stringify(["fsqlite-local-scope-v1", tables, capture.indirect])));
    const publishedId = await localDeliveryId(this.#id, id);
    op.checkpoint();
    return this.#target.transaction(async (tx) => {
      await ensureLocals(tx, op, true);
      const prior = await this.#localEntry(tx, op, id);
      if (prior !== null) {
        if (prior.scope !== scope) fail("HISTORY", "Local operation ID already belongs to a different capture scope");
        return Object.freeze({ replayed: true, record: prior.record });
      }
      // A surviving publication is evidence of prior work even after its original
      // is explicitly retired (or lost). Never rerun that business callback.
      const unpublished = async () => {
        const storage = storageExecutor(tx, op);
        if (await ensureOutbox(storage, false) && await findOutbox(storage, publishedId) !== null)
          fail("HISTORY", "A published local operation cannot be recaptured after its original was retired or lost");
      };
      await unpublished();
      const usage = await this.#localUsage(tx, op);
      if (usage.entries >= this.#maxLocalEntries) fail("LIMIT", "Local changeset retention is full; work was not started");
      const basis = await this.#currentBookmark(tx, op);
      const captured = await capture.run(tx);
      await unpublished();
      await ensureLocals(tx, op, false);
      const after = await this.#currentBookmark(tx, op);
      if (after.position !== basis.position || after.sha256 !== basis.sha256)
        fail("HISTORY", "Remote history changed inside local capture; roll back and separate those operations");
      const current = await this.#localUsage(tx, op);
      if (current.entries !== usage.entries || current.bytes !== usage.bytes)
        fail("CORRUPT", "Local retention changed inside the capture callback");
      const bytes = owned(captured.changeset, this.#policy.maxBytes);
      const changes = decodeChangeset(bytes, this.#policy).reduce((n, t) => n + t.changes.length, 0);
      if (bytes.length > this.#maxLocalBytes - usage.bytes) fail("LIMIT", "Local changeset bytes exceed retention; work must roll back");
      const metadata = {
        journalId: this.#id, operationId: id, basis, sha256: await hash(bytes),
        byteLength: bytes.length, changes, touchedRows: captured.touchedRows,
      };
      const recordSha256 = await localRecordDigest(metadata, scope);
      op.checkpoint();
      const params: ChangesetValue[] = [this.#id, id, scope, BigInt(basis.position), basis.sha256, metadata.sha256, recordSha256, BigInt(bytes.length), BigInt(changes), BigInt(captured.touchedRows)];
      if (bytes.length) params.push(bytes);
      await write(tx, op, `INSERT OR ABORT INTO ${LOCALS} VALUES (?,?,?,?,?,?,?,?,?,?,${bytes.length ? "?" : "zeroblob(0)"})`, params);
      const saved = await this.#localEntry(tx, op, id);
      if (saved === null || saved.scope !== scope || saved.record.recordSha256 !== recordSha256)
        fail("CORRUPT", "Original local changeset was not retained exactly");
      // No post-commit checkpoint: a durable success is not a rollback.
      return Object.freeze({ replayed: false, value: captured.value, record: saved.record });
    }, op.transactionOptions);
  }

  /**
   * Freeze one ORIGINAL local operation's rebased output in this database's
   * ordinary outbox. The derived delivery ID is stable for (journalId, operationId):
   * retries recover the same choice, never rebase it against newer remote history.
   * Use a globally source-qualified journalId and enqueue operations in application
   * order. This is SQL publication, not transport, acknowledgement or a checkpoint.
   */
  async enqueueLocal(
    operationId: string,
    options: Omit<RebaseJournalRangeOptions, "after"> & ChangesetOutboxOptions & {
      /** Same table scope used for the original capture, including empty tables. */
      tables: readonly string[];
    },
  ): Promise<{
    readonly replayed: boolean;
    readonly delivery: OutboxDelivery;
    readonly recordSha256: string;
    readonly afterBookmark: RebaseJournalBookmark;
    readonly throughBookmark: RebaseJournalBookmark;
  }> {
    const id = identity(operationId);
    if (typeof options !== "object" || options === null ||
        (options as RebaseJournalRangeOptions).after !== undefined)
      fail("INPUT", "Publication requires the original operation's saved basis");
    const input = options.tables;
    if (!Array.isArray(input) || !input.length || input.length > 64)
      fail("INPUT", "Publication requires the original 1..64 table scope");
    const tables = Array.from({ length: input.length }, (_, i) => {
      const value: unknown = input[i];
      if (typeof value !== "string" || !value.length || value.length > 1024 || value.includes("\0"))
        fail("INPUT", "Invalid publication table name");
      const bytes = new TextEncoder().encode(value);
      if (bytes.length > 1024 || new TextDecoder("utf-8", { ignoreBOM: true }).decode(bytes) !== value)
        fail("INPUT", "Invalid publication table UTF-8");
      return value.replace(/[A-Z]/g, c => c.toLowerCase());
    }).sort();
    if (tables.some((value, i) => value.startsWith("sqlite_") || value.startsWith("__fsqlite_") ||
        (i > 0 && tables[i - 1] === value))) fail("INPUT", "Publication requires distinct application tables");
    const end = options.through;
    const through = end === undefined ? undefined : boundary(end, this.#id);
    const maxEntries = bound(options.maxEntries, 10_000, 100_000);
    const maxBytes = bound(options.maxPayloadBytes, MAX_WIRE, 1024 ** 3);
    const op = operation(options), format = LOCAL_PUBLICATION_FORMAT;
    const encode = encodeJson;
    const deliveryId = await localDeliveryId(this.#id, id);
    op.checkpoint();
    return this.#target.transaction(async owner => {
      // Storage helpers use the same owner and deadline, without nested BEGINs
      // or escaping SQL. Nothing invokes business callbacks or touches user rows.
      const tx = storageExecutor(owner, op);
      const fanout = await captureFanoutGuard(tx);
      const present = await ensureOutbox(tx, false);
      const prior = present ? await findOutbox(tx, deliveryId) : null;
      const usage = async () => {
        const valid = `typeof(acknowledged)='integer' AND acknowledged IN (0,1) AND ` +
          `typeof(byte_length)='integer' AND byte_length BETWEEN 0 AND ${MAX_WIRE} AND ` +
          `typeof(payload)='blob' AND length(payload)=CASE acknowledged WHEN 1 THEN 0 ELSE byte_length END`;
        const rows = await query(tx, op, `SELECT count(*),coalesce(sum(length(payload)),0),` +
          `count(CASE WHEN ${valid} THEN 1 END) FROM ${OUTBOX}`);
        if (rows.length !== 1 || rows[0]!.length !== 3 || integer(rows[0]![0]) !== integer(rows[0]![2]))
          fail("CORRUPT", "Invalid outbox publication accounting");
        return { entries: integer(rows[0]![0]), bytes: integer(rows[0]![1]) };
      };
      const before = present && prior === null ? await usage() : { entries: 0, bytes: 0 };
      if (prior === null && (before.entries >= maxEntries || before.bytes > maxBytes))
        fail("LIMIT", "Outbox retention is full; no rebasing was started");
      if (!await ensureLocals(tx, op, false)) fail("MISSING", "Original local changeset is not retained");
      const saved = await this.#localEntry(tx, op, id);
      if (saved === null) fail("MISSING", "Original local changeset is not retained");
      // The stored capture scope also covers net-zero originals, whose wire
      // contains no table header or indirect flag from which to infer policy.
      let indirect = false;
      if (await hash(encode(["fsqlite-local-scope-v1", tables, false])) !== saved.scope) {
        indirect = true;
        if (await hash(encode(["fsqlite-local-scope-v1", tables, true])) !== saved.scope)
          fail("HISTORY", "Publication table scope differs from the original capture");
      }
      const original = saved.record;
      const scopeFor = async (
        tip: RebaseJournalBookmark,
        payload: { sha256: string; byteLength: number; changes: number },
      ): Promise<string> => {
        const choice = { format, journalId: this.#id, operationId: id,
          recordSha256: original.recordSha256, afterBookmark: original.basis, throughBookmark: tip };
        const seal = await hash(encode([choice, tables, indirect, deliveryId,
          payload.sha256, payload.byteLength, payload.changes]));
        op.checkpoint();
        return JSON.stringify({ tables, indirect, rebasedLocal: { ...choice, seal } });
      };
      const result = (replayed: boolean, delivery: OutboxDelivery, tip: RebaseJournalBookmark) =>
        Object.freeze({ replayed, delivery, recordSha256: original.recordSha256,
          afterBookmark: original.basis, throughBookmark: tip });
      if (prior !== null) {
        const record = JSON.parse(prior.scope) as { rebasedLocal?: { throughBookmark?: unknown } };
        const value = record.rebasedLocal?.throughBookmark;
        if (value === null || typeof value !== "object") fail("HISTORY", "Delivery ID belongs to other source work");
        const at = boundary(value, this.#id);
        if (at.sha256 === null || at.position < original.basis.position || prior.stream !== null)
          fail("CORRUPT", "Invalid retained publication history boundary");
        const tip: RebaseJournalBookmark = Object.freeze({ format: BOOKMARK_FORMAT,
          journalId: this.#id, position: at.position, sha256: at.sha256 });
        if (through !== undefined && (through.position !== tip.position ||
            (through.sha256 !== null && through.sha256 !== tip.sha256)))
          fail("HISTORY", "This operation already published a different history boundary");
        if (await scopeFor(tip, prior.delivery) !== prior.scope)
          fail("CORRUPT", "Retained publication identity, scope or decision was changed");
        if (prior.delivery.byteLength > this.#policy.maxBytes)
          fail("LIMIT", "Retained publication exceeds the configured message limit");
        await loadOutbox(tx, prior);
        // Do not consult newer (or lost) remote history when recovering already
        // retained output. The original and sealed publication are the evidence.
        return result(true, prior.delivery, tip);
      }
      const rebased = await this.#rebaseAt(tx, op, original.changeset,
        boundary(original.basis, this.#id), through);
      const bytes = rebased.changeset;
      if (bytes.length > maxBytes - before.bytes) fail("LIMIT", "Rebased output exceeds outbox capacity");
      const changes = decodeChangeset(bytes, this.#policy).reduce((n, table) => n + table.changes.length, 0);
      const scope = await scopeFor(rebased.throughBookmark,
        { sha256: await hash(bytes), byteLength: bytes.length, changes });
      await ensureOutbox(tx, true);
      const delivery = await storeOutbox(tx, deliveryId, scope, { changeset: bytes, changes }, op.checkpoint);
      const retained = await findOutbox(tx, deliveryId);
      if (retained === null) fail("CORRUPT", "Published changeset disappeared");
      await loadOutbox(tx, retained);
      const after = await usage();
      if (after.entries !== before.entries + 1 || after.bytes !== before.bytes + bytes.length ||
          await captureFanoutGuard(tx) !== fanout)
        fail("CORRUPT", "Publication changed unrelated outbox or replica state");
      op.checkpoint();
      return result(false, delivery, rebased.throughBookmark);
    }, op.transactionOptions);
  }

  /**
   * Explicitly release one original AFTER its exact sealed outbox publication
   * is acknowledged under the configured single/all-replica policy. Keep that
   * publication identity, source numbering, remote history and application rows.
   * Supply the original recordSha256, not the outgoing (possibly rebased) hash.
   * This is SQL cleanup, not proof of remote durability or a storage checkpoint.
   * Never recycle operation IDs, especially after separately forgetting outbox
   * identities. A missing original returns removed:false, not proof of why it
   * disappeared; matching acknowledged publication evidence is still required.
   */
  async retireLocal(
    operationId: string,
    recordSha256: string,
    options: RebaseJournalOperationOptions = {},
  ): Promise<{
    readonly removed: boolean;
    readonly byteLength: number;
    readonly recordSha256: string;
    readonly delivery: OutboxDelivery;
  }> {
    const id = identity(operationId);
    if (typeof recordSha256 !== "string" || !/^[0-9a-f]{64}$/.test(recordSha256))
      fail("INPUT", "Retirement requires the exact original record SHA-256");
    const op = operation(options), deliveryId = await localDeliveryId(this.#id, id);
    op.checkpoint();
    return this.#target.transaction(async owner => {
      const tx = storageExecutor(owner, op);
      const fanout = await captureFanoutGuard(tx);
      if (!await ensureOutbox(tx, false)) fail("MISSING", "The original has no retained outbox publication");
      const published = await findOutbox(tx, deliveryId);
      if (published === null) fail("MISSING", "The original has no retained outbox publication");
      if (published.delivery.deliveryId !== deliveryId || !published.delivery.acknowledged || published.stream !== null)
        fail("HISTORY", "Every required receiver must acknowledge the exact local publication before retirement");
      // findOutbox has already bounded/validated the canonical table list and
      // reclaimed BLOB shape. Bind the entire publication, not just its ACK bit.
      const raw = JSON.parse(published.scope) as {
        tables: string[]; indirect: boolean;
        rebasedLocal?: { format?: unknown; journalId?: unknown; operationId?: unknown;
          recordSha256?: unknown; afterBookmark?: unknown; throughBookmark?: unknown };
      };
      const retained = raw.rebasedLocal;
      if (retained === null || typeof retained !== "object" || Array.isArray(retained) ||
          retained.format !== LOCAL_PUBLICATION_FORMAT || retained.journalId !== this.#id ||
          retained.operationId !== id || retained.recordSha256 !== recordSha256)
        fail("HISTORY", "Retirement does not identify the retained original publication");
      const after = boundary(retained.afterBookmark, this.#id), through = boundary(retained.throughBookmark, this.#id);
      if (after.sha256 === null || through.sha256 === null || after.position > through.position)
        fail("CORRUPT", "Invalid retained local publication bookmarks");
      const bookmark = (b: HistoryBoundary) => Object.freeze({ format: BOOKMARK_FORMAT,
        journalId: this.#id, position: b.position, sha256: b.sha256! });
      const choice = { format: LOCAL_PUBLICATION_FORMAT, journalId: this.#id, operationId: id,
        recordSha256, afterBookmark: bookmark(after), throughBookmark: bookmark(through) };
      const d = published.delivery;
      const seal = await hash(encodeJson([choice, raw.tables, raw.indirect, deliveryId,
        d.sha256, d.byteLength, d.changes]));
      if (JSON.stringify({ tables: raw.tables, indirect: raw.indirect, rebasedLocal: { ...choice, seal } }) !== published.scope)
        fail("CORRUPT", "Retained local publication seal or canonical scope changed");
      await loadOutbox(tx, published);
      if (!await ensureLocals(tx, op, false)) fail("MISSING", "Original local storage is missing; do not manufacture cleanup evidence");
      const saved = await this.#localEntry(tx, op, id);
      const result = (removed: boolean, byteLength: number) => Object.freeze({
        removed, byteLength, recordSha256, delivery: d,
      });
      if (saved === null) return result(false, 0);
      if (saved.record.recordSha256 !== recordSha256 ||
          JSON.stringify(saved.record.basis) !== JSON.stringify(choice.afterBookmark) ||
          saved.scope !== await hash(encodeJson(["fsqlite-local-scope-v1", raw.tables, raw.indirect])))
        fail("HISTORY", "Retained original disagrees with its acknowledged publication");
      // Cleanup must remain usable when aggregate retention exceeds a newly
      // lowered cap. Still verify the one original under its codec byte budget.
      const before = await this.#localUsage(tx, op, false);
      await write(tx, op, `DELETE FROM ${LOCALS} WHERE journal_id=? AND operation_id=? AND record_sha256=? COLLATE BINARY`,
        [this.#id, id, recordSha256]);
      const remaining = await this.#localUsage(tx, op, false);
      const latest = await findOutbox(tx, deliveryId);
      if ((await query(tx, op, `SELECT 1 FROM ${LOCALS} WHERE journal_id=? AND operation_id=? LIMIT 1`, [this.#id, id])).length ||
          remaining.entries !== before.entries - 1 || remaining.bytes !== before.bytes - saved.record.byteLength ||
          latest === null || latest.scope !== published.scope || latest.delivery.sequence !== d.sequence ||
          latest.delivery.sha256 !== d.sha256 || latest.delivery.byteLength !== d.byteLength ||
          latest.delivery.changes !== d.changes || !latest.delivery.acknowledged ||
          await captureFanoutGuard(tx) !== fanout)
        fail("CORRUPT", "Local retirement changed unexpected source or replica state");
      op.checkpoint();
      return result(true, saved.record.byteLength);
    }, op.transactionOptions);
  }

  /** Read-only recovery; remains usable even if the saved remote basis is missing. */
  async readLocal(operationId: string, options?: RebaseJournalOperationOptions): Promise<RebaseJournalLocalRecord | null> {
    const id = identity(operationId), op = operation(options);
    return this.#target.transaction(async (tx) => {
      if (!await ensureLocals(tx, op, false)) return null;
      return (await this.#localEntry(tx, op, id))?.record ?? null;
    }, op.transactionOptions);
  }

  async #localUsage(tx: ChangesetExecutor, op: Operation, enforceLimits = true): Promise<{ entries: number; bytes: number }> {
    const valid = `typeof(byte_length)='integer' AND byte_length BETWEEN 0 AND ${MAX_WIRE} AND typeof(changeset)='blob' AND length(changeset)=byte_length`;
    const rows = await query(tx, op, `SELECT count(*), coalesce(sum(CASE WHEN ${valid} THEN byte_length ELSE 0 END),0), count(CASE WHEN ${valid} THEN 1 END) FROM ${LOCALS} WHERE journal_id=?`, [this.#id]);
    if (rows.length !== 1 || rows[0]!.length !== 3) fail("CORRUPT", "Invalid local retention accounting");
    const entries = integer(rows[0]![0]), bytes = integer(rows[0]![1]);
    if (integer(rows[0]![2]) !== entries) fail("CORRUPT", "Invalid retained local payload shape");
    if (enforceLimits && (entries > this.#maxLocalEntries || bytes > this.#maxLocalBytes)) fail("LIMIT", "Local retention exceeds configured limits");
    return { entries, bytes };
  }

  async #localEntry(tx: ChangesetExecutor, op: Operation, id: string): Promise<{ scope: string; record: RebaseJournalLocalRecord } | null> {
    const hashes = ["scope_sha256", "basis_sha256", "sha256", "record_sha256"].map(storedDigest);
    const counters = ["basis_position", "byte_length", "change_count", "touched_rows"].map((c) => `CASE WHEN typeof(${c})='integer' THEN ${c} END`);
    const rows = await query(tx, op, `SELECT ${[...hashes, ...counters].join(",")}, CASE WHEN typeof(byte_length)='integer' AND byte_length BETWEEN 0 AND ${this.#policy.maxBytes} AND typeof(changeset)='blob' AND length(changeset)=byte_length THEN changeset END FROM ${LOCALS} WHERE journal_id=? AND operation_id=? LIMIT 2`, [this.#id, id]);
    if (!rows.length) return null;
    if (rows.length !== 1 || rows[0]!.length !== 9) fail("CORRUPT", "Invalid original local record");
    const r = rows[0]!, scope = digest(r[0]), basisSha = digest(r[1]), sha256 = digest(r[2]), recordSha256 = digest(r[3]);
    const at = integer(r[4]), byteLength = integer(r[5]), changes = integer(r[6]), touchedRows = integer(r[7]);
    if (byteLength > this.#policy.maxBytes || changes > this.#policy.maxChanges || touchedRows > 100_000 || !(r[8] instanceof Uint8Array))
      fail("CORRUPT", "Invalid or over-limit original local metadata");
    const changeset = owned(r[8], this.#policy.maxBytes);
    const basis: RebaseJournalBookmark = Object.freeze({ format: BOOKMARK_FORMAT, journalId: this.#id, position: at, sha256: basisSha });
    const record = Object.freeze({ journalId: this.#id, operationId: id, basis, sha256, recordSha256, byteLength, changes, touchedRows, changeset });
    if (await hash(changeset) !== sha256 || await localRecordDigest(record, scope) !== recordSha256)
      fail("CORRUPT", "Original local payload or basis checksum mismatch");
    op.checkpoint();
    if (decodeChangeset(changeset, this.#policy).reduce((n, t) => n + t.changes.length, 0) !== changes)
      fail("CORRUPT", "Original local change count does not match its payload");
    op.checkpoint();
    return { scope, record };
  }

  async #currentBookmark(tx: ChangesetExecutor, op: Operation): Promise<RebaseJournalBookmark> {
    const present = await ensure(tx, op, false);
    const head = present ? await this.#head(tx, op, false) : { position: 0 };
    const floor = await historyFloor(tx, op, this.#id);
    return (await this.#history(tx, op, floor, { position: head.position, sha256: null })).throughBookmark;
  }

  async #head(tx: ChangesetExecutor, op: Operation, create: boolean, enforceLimits = true): Promise<RebaseJournalHead> {
    const rows = await query(tx, op, `SELECT CASE WHEN typeof(position)='integer' THEN position END, CASE WHEN typeof(byte_length)='integer' THEN byte_length END FROM ${HEADS} WHERE journal_id=?`, [this.#id]);
    if (rows.length > 1) fail("CORRUPT", "Duplicate journal head");
    const position = rows.length ? integer(rows[0]![0]) : 0, byteLength = rows.length ? integer(rows[0]![1]) : 0;
    const floor = await historyFloor(tx, op, this.#id), retained = position - floor.position;
    if (retained < 0 || retained > 100_000 || (!rows.length && floor.position !== 0))
      fail("CORRUPT", "Journal head disagrees with its history checkpoint");
    if (enforceLimits && (retained > this.#maxEntries || byteLength > this.#maxBytes)) fail("LIMIT", "Retained journal exceeds configured limits");
    const totals = await query(tx, op, `SELECT count(*), coalesce(min(position),0), coalesce(max(position),0), coalesce(sum(CASE WHEN typeof(byte_length)='integer' AND byte_length BETWEEN 0 AND ${MAX_WIRE} THEN byte_length ELSE 0 END),0), count(CASE WHEN typeof(position)='integer' AND position>0 AND typeof(byte_length)='integer' AND byte_length BETWEEN 0 AND ${MAX_WIRE} AND typeof(rebase_info)='blob' AND length(rebase_info)=byte_length THEN 1 END) FROM ${ENTRIES} WHERE journal_id=?`, [this.#id]);
    const r = totals[0];
    if (totals.length !== 1 || r?.length !== 5 || integer(r[0]) !== retained || integer(r[1]) !== (retained ? floor.position + 1 : 0) || integer(r[2]) !== (retained ? position : 0) || integer(r[3]) !== byteLength || integer(r[4]) !== retained)
      fail("CORRUPT", "Journal history has a gap, missing head, or invalid byte accounting");
    if (!rows.length && create) await write(tx, op, `INSERT OR ABORT INTO ${HEADS} VALUES (?,0,0)`, [this.#id]);
    return Object.freeze({ journalId: this.#id, position, byteLength });
  }
  async #entry(tx: ChangesetExecutor, op: Operation, column: "position" | "delivery_id", value: number | string): Promise<RebaseJournalEntry | null> {
    const rows = await query(tx, op, `SELECT position, CASE WHEN typeof(delivery_id)='text' AND length(CAST(delivery_id AS BLOB))<=1024 AND instr(delivery_id,char(0))=0 THEN delivery_id END, ${storedDigest("message_sha256")}, CASE WHEN typeof(message_bytes)='integer' THEN message_bytes END, ${storedDigest("sha256")}, CASE WHEN typeof(byte_length)='integer' AND byte_length BETWEEN 0 AND ${this.#policy.maxBytes} AND typeof(rebase_info)='blob' AND length(rebase_info)=byte_length THEN rebase_info END FROM ${ENTRIES} WHERE journal_id=? AND ${column}=? LIMIT 2`, [this.#id, typeof value === "number" ? BigInt(value) : value]);
    if (!rows.length) return null;
    if (rows.length !== 1 || rows[0]!.length !== 6) fail("CORRUPT", "Invalid journal entry");
    const r = rows[0]!, position = integer(r[0]), deliveryId = identity(r[1]), messageSha256 = digest(r[2]), messageBytes = integer(r[3]), sha256 = digest(r[4]);
    if (!position || messageBytes > MAX_WIRE || !(r[5] instanceof Uint8Array)) fail("CORRUPT", "Invalid or over-limit journal record");
    const rebaseInfo = owned(r[5], this.#policy.maxBytes);
    if (await hash(rebaseInfo) !== sha256) fail("CORRUPT", "Journal decision checksum mismatch");
    op.checkpoint();
    decodeRebaseInfo(rebaseInfo, this.#policy);
    const receipt = await query(tx, op, `SELECT ${storedDigest("sha256")}, CASE WHEN typeof(byte_length)='integer' THEN byte_length END FROM main."${CHANGESET_RECEIPTS_TABLE}" WHERE delivery_id=? COLLATE BINARY LIMIT 2`, [deliveryId]);
    if (receipt.length !== 1 || receipt[0]![0] !== messageSha256 || integer(receipt[0]![1]) !== messageBytes) fail("CORRUPT", "Journal entry does not match its application receipt");
    return Object.freeze({ journalId: this.#id, position, deliveryId, messageSha256, messageBytes, sha256, rebaseInfo });
  }

  async apply(bytes: Uint8Array, options: RebaseJournalApplyOptions): Promise<RebaseJournalApplyResult> {
    // applyChangeset captures these options before its first await. Never pass
    // the caller's mutable bytes through to a later transaction or hash.
    const deliveryId = identity(options?.deliveryId);
    if ((options as ApplyChangesetOptions).onRebase !== undefined || (options as ApplyChangesetOptions).limits !== undefined)
      fail("INPUT", "The journal owns onRebase and its configured codec limits");
    const source = options.tables;
    if (!Array.isArray(source) || source.length > 256) fail("INPUT", "Use an explicit application table allowlist");
    const tables = Array.from({ length: source.length }, (_, i) => identity(source[i]));
    if (tables.some((s) => s.toLowerCase().startsWith("__fsqlite_"))) fail("INPUT", "Journal metadata cannot be a direct changeset target");
    const onConflict = options.onConflict;
    const generatedColumns = options.generatedColumns, foreignKeys = options.foreignKeys;
    const op = operation(options);
    const wire = owned(bytes, this.#policy.maxBytes);
    let saved: RebaseJournalEntry | null = null;
    let before: RebaseJournalHead | undefined;
    let beforeFloor: RebaseJournalBookmark | undefined;
    let messageSha256 = "";
    const target: ChangesetTarget = {
      transaction: (work, transactionOptions) => this.#target.transaction(async (tx) => {
        await ensure(tx, op, true);
        before = await this.#head(tx, op, true);
        beforeFloor = await historyFloor(tx, op, this.#id);
        messageSha256 = await hash(wire);
        op.checkpoint();
        const result = await work(tx);
        await ensure(tx, op, false);
        await this.#head(tx, op, false);
        if (JSON.stringify(await historyFloor(tx, op, this.#id)) !== JSON.stringify(beforeFloor))
          fail("CORRUPT", "Application changed the retained history checkpoint");
        saved = await this.#entry(tx, op, "delivery_id", deliveryId);
        if (saved === null) fail("MISSING", "The application receipt has no original journal decision; do not fabricate or replay it");
        if (saved.messageSha256 !== messageSha256 || saved.messageBytes !== wire.length) fail("CORRUPT", "Journal entry identifies different input bytes");
        return result;
      }, transactionOptions),
    };
    const applyOptions: ApplyChangesetOptions = {
      tables, deliveryId, limits: this.#policy, ...op.transactionOptions,
      ...(generatedColumns === undefined ? {} : { generatedColumns }),
      ...(foreignKeys === undefined ? {} : { foreignKeys }),
      onRebase: async (tx, info) => {
        await ensure(tx, op, false);
        const head = await this.#head(tx, op, false);
        const floor = await historyFloor(tx, op, this.#id);
        if (!before || head.position !== before.position || head.byteLength !== before.byteLength ||
            JSON.stringify(floor) !== JSON.stringify(beforeFloor)) fail("CORRUPT", "Journal history changed during application");
        if (head.position === MAX_POSITION || head.position - floor.position >= this.#maxEntries || info.length > this.#maxBytes - head.byteLength) fail("LIMIT", "Rebase journal is full; application must roll back");
        const sha256 = await hash(info);
        op.checkpoint();
        const next = head.position + 1;
        // An empty decision is still an ordered application. Spell its BLOB
        // explicitly: some SQLite adapters bind a zero-length view as NULL.
        const params: ChangesetValue[] = [this.#id, BigInt(next), deliveryId, messageSha256, BigInt(wire.length), sha256, BigInt(info.length)];
        if (info.length !== 0) params.push(info);
        await write(tx, op, `INSERT OR ABORT INTO ${ENTRIES} VALUES (?,?,?,?,?,?,?,${info.length === 0 ? "zeroblob(0)" : "?"})`, params);
        await write(tx, op, `UPDATE OR ABORT ${HEADS} SET position=?, byte_length=? WHERE journal_id=? AND position=? AND byte_length=?`, [BigInt(next), BigInt(head.byteLength + info.length), this.#id, BigInt(head.position), BigInt(head.byteLength)]);
      },
    };
    if (onConflict !== undefined) applyOptions.onConflict = onConflict;
    const result = await applyChangeset(target, wire, applyOptions);
    // No post-commit cancellation check: success must not be relabeled rollback.
    if (saved === null) fail("MISSING", "Journal transaction returned without a saved decision");
    return Object.freeze({ ...result, entry: saved });
  }

  /** Current resumable floor and retained decision capacity; never prunes. */
  async retention(options?: RebaseJournalOperationOptions): Promise<{
    readonly floor: RebaseJournalBookmark;
    readonly retainedEntries: number;
    readonly byteLength: number;
  }> {
    const op = operation(options);
    return this.#target.transaction(async tx => {
      const present = await ensure(tx, op, false);
      const head = present ? await this.#head(tx, op, false, false) : { position: 0, byteLength: 0 };
      const floor = await historyFloor(tx, op, this.#id);
      return Object.freeze({ floor, retainedEntries: head.position - floor.position, byteLength: head.byteLength });
    }, op.transactionOptions);
  }

  /**
   * Explicitly end remote-decision retention through an EXACT saved bookmark.
   * Retained originals pin their basis, including already-published originals:
   * retire those with retireLocal after delivery before crossing their basis.
   * Call only when external replay/rebase consumers no longer need the prefix.
   * Inbox receipts remain: old deliveries cannot reapply, but journaled replay
   * can no longer return a removed decision. No ACK or checkpoint is implied.
   */
  async retireThrough(through: RebaseJournalBookmark, options?: RebaseJournalOperationOptions): Promise<{
    readonly removed: number;
    readonly byteLength: number;
    readonly retainedEntries: number;
    readonly floor: RebaseJournalBookmark;
  }> {
    const selected = boundary(through, this.#id);
    if (selected.sha256 === null) fail("INPUT", "History retirement requires a complete bookmark, not a position");
    const op = operation(options);
    return this.#target.transaction(async tx => {
      const present = await ensure(tx, op, false);
      // Like original retirement, cleanup is permitted under lowered aggregate
      // caps. Individual messages and every stored record still validate.
      const before = present ? await this.#head(tx, op, false, false) : { position: 0, byteLength: 0 };
      const previous = await historyFloor(tx, op, this.#id);
      if (selected.position > before.position) fail("MISSING", "Retirement exceeds committed remote history");
      // Verify the selected prefix AND the live suffix before erasing evidence.
      // Starting from an earlier retained floor preserves original hash domains.
      const verified = await this.#history(tx, op, selected, { position: before.position, sha256: null });
      const floor = verified.afterBookmark, removed = floor.position - previous.position;
      const result = (bytes: number) => Object.freeze({ removed, byteLength: bytes,
        retainedEntries: before.position - floor.position, floor });
      if (removed === 0) return result(0);
      await this.#checkRetentionPins(tx, op, floor, before.position);
      const totals = await query(tx, op, `SELECT count(*),coalesce(sum(byte_length),0) FROM ${ENTRIES} WHERE journal_id=? AND position>? AND position<=?`,
        [this.#id, BigInt(previous.position), BigInt(floor.position)]);
      if (totals.length !== 1 || totals[0]!.length !== 2 || integer(totals[0]![0]) !== removed)
        fail("CORRUPT", "Retiring history population changed");
      const bytes = integer(totals[0]![1]);
      if (bytes > before.byteLength) fail("CORRUPT", "Retiring history exceeds retained byte accounting");
      const seal = await floorSeal(floor);
      op.checkpoint();
      await ensureRetention(tx, op, true);
      if (previous.position === 0) {
        await write(tx, op, `INSERT OR ABORT INTO ${RETENTION} VALUES (?,?,?,?)`,
          [this.#id, BigInt(floor.position), floor.sha256, seal]);
      } else {
        await write(tx, op, `UPDATE OR ABORT ${RETENTION} SET position=?,sha256=?,record_sha256=? WHERE journal_id=? AND position=? AND sha256=? COLLATE BINARY AND record_sha256=? COLLATE BINARY`,
          [BigInt(floor.position), floor.sha256, seal, this.#id, BigInt(previous.position), previous.sha256, await floorSeal(previous)]);
      }
      op.checkpoint();
      const deleted = await tx.execute(`DELETE FROM ${ENTRIES} WHERE journal_id=? AND position>? AND position<=?`,
        [this.#id, BigInt(previous.position), BigInt(floor.position)]);
      op.checkpoint();
      if (deleted !== removed) fail("CORRUPT", "History retirement removed an unexpected number of decisions");
      await write(tx, op, `UPDATE OR ABORT ${HEADS} SET byte_length=? WHERE journal_id=? AND position=? AND byte_length=?`,
        [BigInt(before.byteLength - bytes), this.#id, BigInt(before.position), BigInt(before.byteLength)]);
      const after = await this.#head(tx, op, false, false);
      const retained = await historyFloor(tx, op, this.#id);
      if (after.position !== before.position || after.byteLength !== before.byteLength - bytes ||
          JSON.stringify(retained) !== JSON.stringify(floor))
        fail("CORRUPT", "History retirement changed its committed frontier");
      // The same full-prefix identity must remain recoverable after compaction.
      await this.#history(tx, op, floor, verified.throughBookmark);
      op.checkpoint();
      return result(bytes);
    }, op.transactionOptions);
  }

  /** Verify one original at a time; a forged basis counter cannot hide a pin. */
  async #checkRetentionPins(tx: ChangesetExecutor, op: Operation, floor: RebaseJournalBookmark, tip: number): Promise<void> {
    if (!await ensureLocals(tx, op, false)) return;
    const usage = await this.#localUsage(tx, op, false);
    if (usage.entries > 100_000) fail("LIMIT", "Too many originals to validate for retirement");
    let cursor: string | null = null, checked = 0;
    while (checked < usage.entries) {
      const rows = await query(tx, op, `SELECT CASE WHEN typeof(operation_id)='text' AND length(CAST(operation_id AS BLOB))<=1024 AND instr(operation_id,char(0))=0 THEN operation_id END FROM ${LOCALS} WHERE journal_id=?${cursor === null ? "" : " AND operation_id>? COLLATE BINARY"} ORDER BY operation_id LIMIT 32`,
        cursor === null ? [this.#id] : [this.#id, cursor]);
      if (!rows.length || rows.length > 32) fail("CORRUPT", "Missing original retention page");
      for (const row of rows) {
        if (row.length !== 1) fail("CORRUPT", "Invalid original retention row");
        const id = identity(row[0]);
        if (id === cursor || ++checked > usage.entries) fail("CORRUPT", "Repeated original retention key");
        const saved = await this.#localEntry(tx, op, id);
        if (saved === null) fail("CORRUPT", "Retained original disappeared during retirement");
        const basis = saved.record.basis;
        if (basis.position < floor.position || basis.position > tip ||
            (basis.position === floor.position && basis.sha256 !== floor.sha256))
          fail("HISTORY", "A retained original still requires the retiring history, or has a different basis");
        cursor = id;
      }
    }
    op.checkpoint();
  }

  async head(options?: RebaseJournalOperationOptions): Promise<RebaseJournalHead> {
    const op = operation(options);
    return this.#target.transaction(async (tx) => {
      if (!await ensure(tx, op, false)) return Object.freeze({ journalId: this.#id, position: 0, byteLength: 0 });
      return this.#head(tx, op, false);
    }, op.transactionOptions);
  }
  async read(deliveryId: string, options?: RebaseJournalOperationOptions): Promise<RebaseJournalEntry | null> {
    const id = identity(deliveryId), op = operation(options);
    return this.#target.transaction(async (tx) => {
      if (!await ensure(tx, op, false)) return null;
      await this.#head(tx, op, false);
      return this.#entry(tx, op, "delivery_id", id);
    }, op.transactionOptions);
  }
  /** Read-only identity of the current prefix, including empty decisions. */
  async bookmark(options?: RebaseJournalOperationOptions): Promise<RebaseJournalBookmark> {
    const op = operation(options);
    return this.#target.transaction(async (tx) => {
      const present = await ensure(tx, op, false);
      const head = present ? await this.#head(tx, op, false) : { position: 0 };
      const floor = await historyFloor(tx, op, this.#id);
      const result = await this.#history(tx, op, floor, { position: head.position, sha256: null });
      return result.throughBookmark;
    }, op.transactionOptions);
  }

  /** One verified entry at a time: no copy of the whole retained wire history. */
  async #history(tx: ChangesetExecutor, op: Operation, after: HistoryBoundary, through: HistoryBoundary, rebaser?: ChangesetRebaser) {
    const encoder = new TextEncoder();
    op.checkpoint();
    // JSON arrays make bounded UTF-8 identities unambiguous. The versioned
    // domain binds the journal; each next digest binds the COMPLETE prefix.
    let current = await historyFloor(tx, op, this.#id);
    if (after.position < current.position || through.position < current.position)
      fail("EXPIRED", "This rebase history prefix was explicitly retired; do not substitute a newer basis");
    op.checkpoint();
    let afterBookmark = current;
    if (after.position === current.position) verifyBoundary(after, current);
    for (let i = current.position + 1; i <= through.position; i++) {
      const entry = await this.#entry(tx, op, "position", i);
      if (entry === null) fail("MISSING", "A rebase decision is missing from the requested history");
      if (entry.position !== i) fail("CORRUPT", "Journal returned the wrong history position");
      const sha256 = await hash(encoder.encode(JSON.stringify([
        BOOKMARK_FORMAT, current.sha256, i, entry.deliveryId, entry.messageSha256,
        entry.messageBytes, entry.sha256, entry.rebaseInfo.length,
      ])));
      op.checkpoint();
      current = Object.freeze({ format: BOOKMARK_FORMAT, journalId: this.#id, position: i, sha256 });
      if (i === after.position) {
        verifyBoundary(after, current);
        afterBookmark = current;
      }
      if (i > after.position) rebaser?.configure(entry.rebaseInfo);
    }
    verifyBoundary(through, current);
    op.checkpoint();
    return { afterBookmark, throughBookmark: current };
  }

  /** Rebase ORIGINAL local bytes over a complete range in one SQL snapshot. */
  async rebase(local: Uint8Array, options: RebaseJournalRangeOptions = {}): Promise<RebaseJournalResult> {
    const start = options.after, end = options.through;
    const after = boundary(start === undefined ? 0 : start, this.#id);
    const through = end === undefined ? undefined : boundary(end, this.#id), op = operation(options);
    const wire = owned(local, this.#policy.maxBytes);
    decodeChangeset(wire, this.#policy);
    return this.#target.transaction((tx) => this.#rebaseAt(tx, op, wire, after, through), op.transactionOptions);
  }

  /**
   * Recover an ORIGINAL by ID and verify its saved basis and selected history
   * in ONE SQL snapshot. Never accepts replacement bytes or an alternate basis.
   * Does not mutate the original, apply SQL, send a payload, or acknowledge it.
   */
  async rebaseLocal(
    operationId: string,
    options: Omit<RebaseJournalRangeOptions, "after"> = {},
  ): Promise<RebaseJournalResult> {
    const id = identity(operationId);
    if ((options as RebaseJournalRangeOptions).after !== undefined)
      fail("INPUT", "rebaseLocal always uses the original operation's saved basis");
    const end = options.through;
    const through = end === undefined ? undefined : boundary(end, this.#id);
    const op = operation(options);
    return this.#target.transaction(async (tx) => {
      if (!await ensureLocals(tx, op, false)) fail("MISSING", "Original local changeset is not retained");
      const saved = await this.#localEntry(tx, op, id);
      if (saved === null) fail("MISSING", "Original local changeset is not retained");
      return this.#rebaseAt(tx, op, saved.record.changeset, boundary(saved.record.basis, this.#id), through);
    }, op.transactionOptions);
  }

  async #rebaseAt(
    tx: ChangesetExecutor,
    op: Operation,
    wire: Uint8Array,
    after: HistoryBoundary,
    through: HistoryBoundary | undefined,
  ): Promise<RebaseJournalResult> {
    const present = await ensure(tx, op, false);
    const head = present ? await this.#head(tx, op, false) : { position: 0 };
    const tip = through ?? { position: head.position, sha256: null };
    if (after.position > tip.position || tip.position > head.position) fail("MISSING", "Requested journal range is not available");
    const rebaser = new ChangesetRebaser(this.#policy);
    const bookmarks = await this.#history(tx, op, after, tip, rebaser);
    op.checkpoint();
    const changeset = rebaser.rebase(wire);
    op.checkpoint();
    return Object.freeze({ journalId: this.#id, after: after.position, through: tip.position, changeset, ...bookmarks });
  }
}
