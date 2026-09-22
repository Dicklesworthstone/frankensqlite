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
function fail(kind: "INPUT" | "SCHEMA" | "CORRUPT" | "LIMIT" | "MISSING" | "HISTORY" | "CANCELLED" | "TIMEOUT", message: string): never {
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
  if (typeof value !== "number" || !Number.isSafeInteger(value) || value < 0 || value > 100_000)
    fail("INPUT", "Journal positions must be integers in 0..100000");
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

/**
 * Ordered persistent conflict decisions, atomically coupled to applyChangeset's
 * rows and delivery receipt. A replay MUST find its original journal entry.
 * Bounded retention refuses new applications rather than dropping needed history.
 * No pruning, outbox mutation, native replication, or automatic retry is implied.
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
    op.checkpoint();
    return this.#target.transaction(async (tx) => {
      await ensureLocals(tx, op, true);
      const prior = await this.#localEntry(tx, op, id);
      if (prior !== null) {
        if (prior.scope !== scope) fail("HISTORY", "Local operation ID already belongs to a different capture scope");
        return Object.freeze({ replayed: true, record: prior.record });
      }
      const usage = await this.#localUsage(tx, op);
      if (usage.entries >= this.#maxLocalEntries) fail("LIMIT", "Local changeset retention is full; work was not started");
      const basis = await this.#currentBookmark(tx, op);
      const captured = await capture.run(tx);
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

  /** Read-only recovery; remains usable even if the saved remote basis is missing. */
  async readLocal(operationId: string, options?: RebaseJournalOperationOptions): Promise<RebaseJournalLocalRecord | null> {
    const id = identity(operationId), op = operation(options);
    return this.#target.transaction(async (tx) => {
      if (!await ensureLocals(tx, op, false)) return null;
      return (await this.#localEntry(tx, op, id))?.record ?? null;
    }, op.transactionOptions);
  }

  async #localUsage(tx: ChangesetExecutor, op: Operation): Promise<{ entries: number; bytes: number }> {
    const valid = `typeof(byte_length)='integer' AND byte_length BETWEEN 0 AND ${MAX_WIRE} AND typeof(changeset)='blob' AND length(changeset)=byte_length`;
    const rows = await query(tx, op, `SELECT count(*), coalesce(sum(CASE WHEN ${valid} THEN byte_length ELSE 0 END),0), count(CASE WHEN ${valid} THEN 1 END) FROM ${LOCALS} WHERE journal_id=?`, [this.#id]);
    if (rows.length !== 1 || rows[0]!.length !== 3) fail("CORRUPT", "Invalid local retention accounting");
    const entries = integer(rows[0]![0]), bytes = integer(rows[0]![1]);
    if (integer(rows[0]![2]) !== entries) fail("CORRUPT", "Invalid retained local payload shape");
    if (entries > this.#maxLocalEntries || bytes > this.#maxLocalBytes) fail("LIMIT", "Local retention exceeds configured limits");
    return { entries, bytes };
  }

  async #localEntry(tx: ChangesetExecutor, op: Operation, id: string): Promise<{ scope: string; record: RebaseJournalLocalRecord } | null> {
    const hashes = ["scope_sha256", "basis_sha256", "sha256", "record_sha256"].map((c) => `CASE WHEN typeof(${c})='text' AND length(CAST(${c} AS BLOB))=64 THEN ${c} END`);
    const counters = ["basis_position", "byte_length", "change_count", "touched_rows"].map((c) => `CASE WHEN typeof(${c})='integer' THEN ${c} END`);
    const rows = await query(tx, op, `SELECT ${[...hashes, ...counters].join(",")}, CASE WHEN typeof(byte_length)='integer' AND byte_length BETWEEN 0 AND ${this.#policy.maxBytes} AND typeof(changeset)='blob' AND length(changeset)=byte_length THEN changeset END FROM ${LOCALS} WHERE journal_id=? AND operation_id=? LIMIT 2`, [this.#id, id]);
    if (!rows.length) return null;
    if (rows.length !== 1 || rows[0]!.length !== 9) fail("CORRUPT", "Invalid original local record");
    const r = rows[0]!, scope = digest(r[0]), basisSha = digest(r[1]), sha256 = digest(r[2]), recordSha256 = digest(r[3]);
    const at = integer(r[4]), byteLength = integer(r[5]), changes = integer(r[6]), touchedRows = integer(r[7]);
    if (at > 100_000 || byteLength > this.#policy.maxBytes || changes > this.#policy.maxChanges || touchedRows > 100_000 || !(r[8] instanceof Uint8Array))
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
    return (await this.#history(tx, op, { position: 0, sha256: null }, { position: head.position, sha256: null })).throughBookmark;
  }

  async #head(tx: ChangesetExecutor, op: Operation, create: boolean): Promise<RebaseJournalHead> {
    const rows = await query(tx, op, `SELECT CASE WHEN typeof(position)='integer' THEN position END, CASE WHEN typeof(byte_length)='integer' THEN byte_length END FROM ${HEADS} WHERE journal_id=?`, [this.#id]);
    if (rows.length > 1) fail("CORRUPT", "Duplicate journal head");
    const position = rows.length ? integer(rows[0]![0]) : 0, byteLength = rows.length ? integer(rows[0]![1]) : 0;
    if (position > this.#maxEntries || byteLength > this.#maxBytes) fail("LIMIT", "Retained journal exceeds configured limits");
    const totals = await query(tx, op, `SELECT count(*), coalesce(min(position),0), coalesce(max(position),0), coalesce(sum(CASE WHEN typeof(byte_length)='integer' AND byte_length BETWEEN 0 AND ${MAX_WIRE} THEN byte_length ELSE 0 END),0), count(CASE WHEN typeof(position)='integer' AND position>0 AND typeof(byte_length)='integer' AND byte_length BETWEEN 0 AND ${MAX_WIRE} AND typeof(rebase_info)='blob' AND length(rebase_info)=byte_length THEN 1 END) FROM ${ENTRIES} WHERE journal_id=?`, [this.#id]);
    const r = totals[0];
    if (totals.length !== 1 || r?.length !== 5 || integer(r[0]) !== position || integer(r[1]) !== (position ? 1 : 0) || integer(r[2]) !== position || integer(r[3]) !== byteLength || integer(r[4]) !== position)
      fail("CORRUPT", "Journal history has a gap, missing head, or invalid byte accounting");
    if (!rows.length && create) await write(tx, op, `INSERT OR ABORT INTO ${HEADS} VALUES (?,0,0)`, [this.#id]);
    return Object.freeze({ journalId: this.#id, position, byteLength });
  }
  async #entry(tx: ChangesetExecutor, op: Operation, column: "position" | "delivery_id", value: number | string): Promise<RebaseJournalEntry | null> {
    const rows = await query(tx, op, `SELECT position, CASE WHEN typeof(delivery_id)='text' AND length(CAST(delivery_id AS BLOB))<=512 THEN delivery_id END, CASE WHEN typeof(message_sha256)='text' AND length(message_sha256)=64 THEN message_sha256 END, CASE WHEN typeof(message_bytes)='integer' THEN message_bytes END, CASE WHEN typeof(sha256)='text' AND length(sha256)=64 THEN sha256 END, CASE WHEN typeof(byte_length)='integer' AND byte_length BETWEEN 0 AND ${this.#policy.maxBytes} AND typeof(rebase_info)='blob' AND length(rebase_info)=byte_length THEN rebase_info END FROM ${ENTRIES} WHERE journal_id=? AND ${column}=? LIMIT 2`, [this.#id, typeof value === "number" ? BigInt(value) : value]);
    if (!rows.length) return null;
    if (rows.length !== 1 || rows[0]!.length !== 6) fail("CORRUPT", "Invalid journal entry");
    const r = rows[0]!, position = integer(r[0]), deliveryId = identity(r[1]), messageSha256 = digest(r[2]), messageBytes = integer(r[3]), sha256 = digest(r[4]);
    if (!position || position > this.#maxEntries || messageBytes > MAX_WIRE || !(r[5] instanceof Uint8Array)) fail("CORRUPT", "Invalid or over-limit journal record");
    const rebaseInfo = owned(r[5], this.#policy.maxBytes);
    if (await hash(rebaseInfo) !== sha256) fail("CORRUPT", "Journal decision checksum mismatch");
    op.checkpoint();
    decodeRebaseInfo(rebaseInfo, this.#policy);
    const receipt = await query(tx, op, `SELECT CASE WHEN typeof(sha256)='text' AND length(sha256)=64 THEN sha256 END, CASE WHEN typeof(byte_length)='integer' THEN byte_length END FROM main."${CHANGESET_RECEIPTS_TABLE}" WHERE delivery_id=? COLLATE BINARY LIMIT 2`, [deliveryId]);
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
    const op = operation(options);
    const wire = owned(bytes, this.#policy.maxBytes);
    let saved: RebaseJournalEntry | null = null;
    let before: RebaseJournalHead | undefined;
    let messageSha256 = "";
    const target: ChangesetTarget = {
      transaction: (work, transactionOptions) => this.#target.transaction(async (tx) => {
        await ensure(tx, op, true);
        before = await this.#head(tx, op, true);
        messageSha256 = await hash(wire);
        op.checkpoint();
        const result = await work(tx);
        await ensure(tx, op, false);
        await this.#head(tx, op, false);
        saved = await this.#entry(tx, op, "delivery_id", deliveryId);
        if (saved === null) fail("MISSING", "The application receipt has no original journal decision; do not fabricate or replay it");
        if (saved.messageSha256 !== messageSha256 || saved.messageBytes !== wire.length) fail("CORRUPT", "Journal entry identifies different input bytes");
        return result;
      }, transactionOptions),
    };
    const applyOptions: ApplyChangesetOptions = {
      tables, deliveryId, limits: this.#policy, ...op.transactionOptions,
      onRebase: async (tx, info) => {
        await ensure(tx, op, false);
        const head = await this.#head(tx, op, false);
        if (!before || head.position !== before.position || head.byteLength !== before.byteLength) fail("CORRUPT", "Journal history changed during application");
        if (head.position >= this.#maxEntries || info.length > this.#maxBytes - head.byteLength) fail("LIMIT", "Rebase journal is full; application must roll back");
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
      const result = await this.#history(tx, op, { position: 0, sha256: null }, { position: head.position, sha256: null });
      return result.throughBookmark;
    }, op.transactionOptions);
  }

  /** One verified entry at a time: no copy of the whole retained wire history. */
  async #history(tx: ChangesetExecutor, op: Operation, after: HistoryBoundary, through: HistoryBoundary, rebaser?: ChangesetRebaser) {
    const encoder = new TextEncoder();
    op.checkpoint();
    // JSON arrays make bounded UTF-8 identities unambiguous. The versioned
    // domain binds the journal; each next digest binds the COMPLETE prefix.
    let current: RebaseJournalBookmark = Object.freeze({
      format: BOOKMARK_FORMAT, journalId: this.#id, position: 0,
      sha256: await hash(encoder.encode(JSON.stringify([BOOKMARK_FORMAT, this.#id]))),
    });
    op.checkpoint();
    let afterBookmark = current;
    if (after.position === 0) verifyBoundary(after, current);
    for (let i = 1; i <= through.position; i++) {
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
    return this.#target.transaction(async (tx) => {
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
    }, op.transactionOptions);
  }
}
