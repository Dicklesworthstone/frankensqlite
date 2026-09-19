import { prepareChangesetCapture, prepareChangesetSnapshot } from "./changeset-capture";
import type { CaptureChangesetOptions, ChangesetSnapshot, SnapshotChangesetOptions } from "./changeset-capture";
import type { ChangesetExecutor, ChangesetTarget } from "./changeset-apply";
import { decodeChangeset } from "./changeset-codec";
import type { ChangesetValue } from "./changeset-codec";

export const CHANGESET_OUTBOX_TABLE = "__fsqlite_changeset_outbox";
export interface ChangesetOutboxOptions {
  /** All retained identities, including acknowledged ones. Default 10,000. */
  maxEntries?: number;
  /** Retained pending payload bytes, not SQL-file or heap size. Default 64 MiB. */
  maxPayloadBytes?: number;
}
export interface OutboxDelivery {
  readonly sequence: bigint;
  readonly deliveryId: string;
  readonly sha256: string;
  readonly byteLength: number;
  readonly changes: number;
  readonly acknowledged: boolean;
}
export interface OutboxRecordOptions extends CaptureChangesetOptions {
  /** Stable, globally source-qualified operation ID. Never reuse for other work. */
  deliveryId: string;
}
export interface OutboxBootstrapOptions extends SnapshotChangesetOptions {
  /** Stable source-qualified seed identity, distinct from incremental operations. */
  deliveryId: string;
}
export interface OutboxBootstrapResult {
  readonly replayed: boolean;
  readonly delivery: OutboxDelivery;
}
export type OutboxRecordResult<T> =
  | { readonly replayed: false; readonly value: T; readonly delivery: OutboxDelivery }
  | { readonly replayed: true; readonly delivery: OutboxDelivery };
export interface OutboxReadResult {
  readonly delivery: OutboxDelivery;
  /** Acknowledged payloads are reclaimed, but their operation IDs remain. */
  readonly changeset: Uint8Array | null;
}
export interface OutboxPageOptions { after?: bigint; limit?: number }
export class ChangesetOutboxError extends Error {
  constructor(readonly code: "ERR_FSQLITE_OUTBOX_INPUT" | "ERR_FSQLITE_OUTBOX_SCHEMA" |
    "ERR_FSQLITE_OUTBOX_CORRUPT" | "ERR_FSQLITE_OUTBOX_FULL" | "ERR_FSQLITE_OUTBOX_REUSE" |
    "ERR_FSQLITE_OUTBOX_ACK" | "ERR_FSQLITE_OUTBOX_STATE", message: string) {
    super(message); this.name = "ChangesetOutboxError";
  }
}
const TABLE = `main."${CHANGESET_OUTBOX_TABLE}"`;
const COLUMNS = "seq INTEGER PRIMARY KEY AUTOINCREMENT, delivery_id TEXT NOT NULL UNIQUE COLLATE BINARY, " +
  "sha256 TEXT NOT NULL, byte_length INTEGER NOT NULL, change_count INTEGER NOT NULL, " +
  "scope TEXT NOT NULL, acknowledged INTEGER NOT NULL, payload BLOB NOT NULL";
const COLUMN_NAMES = ["seq", "delivery_id", "sha256", "byte_length", "change_count", "scope", "acknowledged", "payload"];
const COLUMN_TYPES = ["INTEGER", "TEXT", "TEXT", "INTEGER", "INTEGER", "TEXT", "INTEGER", "BLOB"];
const META = "CAST(seq AS TEXT) AS seq, " +
  "CASE WHEN length(CAST(delivery_id AS BLOB)) <= 1024 AND instr(delivery_id,char(0))=0 THEN delivery_id END AS delivery_id, " +
  "CASE WHEN length(sha256) = 64 AND instr(sha256,char(0))=0 THEN sha256 END AS sha256, byte_length, change_count, " +
  "CASE WHEN length(CAST(scope AS BLOB)) <= 262144 AND instr(scope,char(0))=0 THEN scope END AS scope, acknowledged, " +
  "typeof(payload) AS payload_type, length(payload) AS payload_length";
const fold = (s: string): string => s.replace(/[A-Z]/g, c => c.toLowerCase());
function fail(kind: "INPUT" | "SCHEMA" | "CORRUPT" | "FULL" | "REUSE" | "ACK" | "STATE", message: string): never {
  throw new ChangesetOutboxError(`ERR_FSQLITE_OUTBOX_${kind}`, message);
}
function identity(value: unknown): string {
  if (typeof value !== "string" || !value.length || value.length > 512 || value.includes("\0")) fail("INPUT", "deliveryId must be valid UTF-8 of 1..512 bytes without NUL");
  const bytes = new TextEncoder().encode(value);
  if (bytes.length > 512 || new TextDecoder("utf-8", { ignoreBOM: true }).decode(bytes) !== value) fail("INPUT", "Invalid delivery identity UTF-8");
  return value;
}
function digest(value: unknown): string {
  if (typeof value !== "string" || !/^[0-9a-f]{64}$/.test(value)) fail("CORRUPT", "Invalid outbox SHA-256");
  return value;
}
function integer(value: unknown): number {
  if (typeof value === "bigint" && value >= 0n && value <= BigInt(Number.MAX_SAFE_INTEGER)) return Number(value);
  if (typeof value === "number" && Number.isSafeInteger(value) && value >= 0) return value;
  return fail("CORRUPT", "Invalid outbox integer");
}
function bound(value: unknown, fallback: number, maximum: number): number {
  const n = value ?? fallback;
  if (typeof n !== "number" || !Number.isSafeInteger(n) || n < 1 || n > maximum) fail("INPUT", `Outbox limit must be an integer in 1..${maximum}`);
  return n;
}
async function hash(bytes: Uint8Array): Promise<string> {
  if (globalThis.crypto?.subtle === undefined) fail("INPUT", "The outbox requires Web Crypto SHA-256");
  const sum = new Uint8Array(await globalThis.crypto.subtle.digest("SHA-256", new Uint8Array(bytes)));
  return Array.from(sum, b => b.toString(16).padStart(2, "0")).join("");
}
async function query(tx: ChangesetExecutor, sql: string, params: readonly ChangesetValue[] = []) {
  const result = await tx.query(sql, params);
  if (!Array.isArray(result.rowArrays) || result.rowArrays.some(row => !Array.isArray(row))) fail("CORRUPT", "Invalid outbox SQL result");
  return result.rowArrays;
}
/** Ignore quoted identifiers/strings and comments when checking the rowid rule. */
function hasAutoincrement(sql: string): boolean {
  for (let i = 0; i < sql.length;) {
    const c = sql[i]!;
    if (c === "'" || c === '"' || c === "`" || c === "[") {
      const end = c === "[" ? "]" : c; i++;
      while (i < sql.length) {
        if (sql[i++] === end) { if (c !== "[" && sql[i] === end) { i++; continue; } break; }
      }
    } else if (sql.slice(i, i + 2) === "--") {
      const end = sql.indexOf("\n", i + 2); i = end < 0 ? sql.length : end + 1;
    } else if (sql.slice(i, i + 2) === "/*") {
      const end = sql.indexOf("*/", i + 2); i = end < 0 ? sql.length : end + 2;
    } else if (/[A-Za-z_]/.test(c)) {
      const start = i++;
      while (i < sql.length && /[A-Za-z0-9_]/.test(sql[i]!)) i++;
      if (sql.slice(start, i).toUpperCase() === "AUTOINCREMENT") return true;
    } else i++;
  }
  return false;
}
async function ensure(tx: ChangesetExecutor, create: boolean): Promise<boolean> {
  let rows = await query(tx, "SELECT type, sql FROM main.sqlite_schema WHERE name = ? COLLATE NOCASE", [CHANGESET_OUTBOX_TABLE]);
  if (!rows.length && create) {
    await tx.execute(`CREATE TABLE ${TABLE} (${COLUMNS})`);
    rows = await query(tx, "SELECT type, sql FROM main.sqlite_schema WHERE name = ? COLLATE NOCASE", [CHANGESET_OUTBOX_TABLE]);
  }
  if (!rows.length) return false;
  // Validate semantics, not SQL formatting: the engine may normalize CREATE.
  if (rows.length !== 1 || rows[0]![0] !== "table" || typeof rows[0]![1] !== "string" || !hasAutoincrement(rows[0]![1])) fail("SCHEMA", "The outbox requires monotonic AUTOINCREMENT sequences");
  const listed = await query(tx, `PRAGMA main.table_list('${CHANGESET_OUTBOX_TABLE}')`);
  const main = listed.filter(row => row[0] === "main" && row[1] === CHANGESET_OUTBOX_TABLE);
  if (main.length !== 1 || main[0]![2] !== "table" || integer(main[0]![3]) !== 8 || integer(main[0]![4]) !== 0) fail("SCHEMA", "Invalid outbox table layout");
  const columns = await query(tx, `PRAGMA main.table_xinfo('${CHANGESET_OUTBOX_TABLE}')`);
  if (columns.length !== 8) fail("SCHEMA", "Invalid outbox columns");
  for (let i = 0; i < columns.length; i++) {
    const row = columns[i]!;
    if (integer(row[0]) !== i || row[1] !== COLUMN_NAMES[i] || typeof row[2] !== "string" || row[2].toUpperCase() !== COLUMN_TYPES[i] ||
        (i > 0 && integer(row[3]) !== 1) || row[4] !== null || integer(row[5]) !== (i === 0 ? 1 : 0) || integer(row[6]) !== 0) fail("SCHEMA", "Invalid outbox column definition");
  }
  const indexes = await query(tx, `PRAGMA main.index_list('${CHANGESET_OUTBOX_TABLE}')`);
  if (indexes.length !== 1 || integer(indexes[0]![2]) !== 1 || indexes[0]![3] !== "u" || integer(indexes[0]![4]) !== 0) fail("SCHEMA", "Unexpected changeset outbox indexes");
  const indexName = indexes[0]![1];
  if (typeof indexName !== "string" || indexName.length > 1024 || indexName.includes("\0")) fail("SCHEMA", "Invalid outbox index name");
  const keys = (await query(tx, `PRAGMA main.index_xinfo('${indexName.replaceAll("'", "''")}')`)).filter(row => integer(row[5]) === 1);
  if (keys.length !== 1 || integer(keys[0]![1]) !== 1 || keys[0]![2] !== "delivery_id" || keys[0]![4] !== "BINARY") fail("SCHEMA", "Outbox delivery identities require a BINARY unique key");
  if ((await query(tx, `PRAGMA main.foreign_key_list('${CHANGESET_OUTBOX_TABLE}')`)).length) fail("SCHEMA", "Foreign keys on the outbox are not supported");
  for (const ns of ["main", "temp"]) {
    if ((await query(tx, `SELECT name FROM ${ns}.sqlite_schema WHERE type='trigger' AND tbl_name=? COLLATE NOCASE LIMIT 1`, [CHANGESET_OUTBOX_TABLE])).length) fail("SCHEMA", "Triggers on the changeset outbox are not supported");
  }
  return true;
}
interface Stored { delivery: OutboxDelivery; scope: string }
function metadata(row: readonly unknown[]): Stored {
  if (row.length !== 9 || typeof row[0] !== "string" || !/^[1-9][0-9]{0,18}$/.test(row[0])) fail("CORRUPT", "Invalid outbox sequence");
  const sequence = BigInt(row[0]);
  if (sequence > (1n << 63n) - 1n) fail("CORRUPT", "Outbox sequence exceeds int64");
  let deliveryId: string;
  try { deliveryId = identity(row[1]); } catch { return fail("CORRUPT", "Invalid stored delivery identity"); }
  const sha256 = digest(row[2]), byteLength = integer(row[3]), changes = integer(row[4]), ack = integer(row[6]);
  if (byteLength > 64 * 1024 * 1024 || changes > 100_000 || ack > 1 || row[7] !== "blob" || integer(row[8]) !== (ack ? 0 : byteLength)) fail("CORRUPT", "Invalid outbox payload metadata");
  if (typeof row[5] !== "string" || row[5].length > 131072) fail("CORRUPT", "Invalid outbox capture scope");
  let scope: unknown;
  try { scope = JSON.parse(row[5]); } catch { return fail("CORRUPT", "Invalid outbox capture scope JSON"); }
  if (typeof scope !== "object" || scope === null) fail("CORRUPT", "Invalid capture scope shape");
  const s = scope as { tables?: unknown; indirect?: unknown; snapshot?: unknown };
  if (s.snapshot !== undefined && s.snapshot !== true) fail("CORRUPT", "Invalid outbox snapshot scope");
  const tables = s.tables;
  if (!Array.isArray(tables) || !tables.length || tables.length > 64 || typeof s.indirect !== "boolean" ||
      tables.some(t => typeof t !== "string" || !t.length || t.length > 1024 || t.includes("\0") || t.startsWith("sqlite_") || t.startsWith("__fsqlite_")) ||
      tables.some((t, i) => fold(t) !== t || (i > 0 && tables[i - 1] >= t))) fail("CORRUPT", "Invalid capture scope tables");
  return { scope: row[5], delivery: Object.freeze({ sequence, deliveryId, sha256, byteLength, changes, acknowledged: ack === 1 }) };
}
async function find(tx: ChangesetExecutor, id: string): Promise<Stored | null> {
  const rows = await query(tx, `SELECT ${META} FROM ${TABLE} WHERE delivery_id=?`, [id]);
  if (rows.length > 1) fail("CORRUPT", "Multiple records for one delivery identity");
  return rows.length ? metadata(rows[0]!) : null;
}
async function load(tx: ChangesetExecutor, record: Stored): Promise<Uint8Array | null> {
  if (record.delivery.acknowledged) return null;
  const rows = await query(tx, `SELECT payload FROM ${TABLE} WHERE delivery_id=? AND typeof(payload)='blob' AND length(payload)=?`, [record.delivery.deliveryId, BigInt(record.delivery.byteLength)]);
  if (rows.length !== 1 || rows[0]!.length !== 1 || !(rows[0]![0] instanceof Uint8Array)) fail("CORRUPT", "Invalid outbox payload");
  const bytes = new Uint8Array(rows[0]![0]);
  if (bytes.byteLength !== record.delivery.byteLength || await hash(bytes) !== record.delivery.sha256) fail("CORRUPT", "Outbox payload does not match its retained digest");
  const tables = decodeChangeset(bytes);
  if (tables.reduce((n, t) => n + t.changes.length, 0) !== record.delivery.changes) fail("CORRUPT", "Outbox change count disagrees with payload");
  const scope = JSON.parse(record.scope) as { tables: string[]; indirect: boolean; snapshot?: true };
  if (tables.some(t => !scope.tables.includes(fold(t.name)) ||
      t.changes.some(c => c.indirect !== scope.indirect || (scope.snapshot === true && c.operation !== "insert")))) {
    fail("CORRUPT", "Outbox payload disagrees with its capture scope");
  }
  return bytes;
}

/** Shared atomic publication for incremental captures and initial row snapshots. */
async function store(tx: ChangesetExecutor, id: string, scope: string,
  result: ChangesetSnapshot, checkpoint: () => void): Promise<OutboxDelivery> {
  const sha256 = await hash(result.changeset); checkpoint();
  // Some SQL adapters bind a zero-length typed array as NULL, not BLOB.
  const payload = result.changeset.byteLength === 0 ? "X''" : "?";
  const params: ChangesetValue[] = [id, sha256, BigInt(result.changeset.byteLength), BigInt(result.changes), scope];
  if (result.changeset.byteLength) params.push(result.changeset);
  const changed = await tx.execute(`INSERT OR ABORT INTO ${TABLE} (delivery_id,sha256,byte_length,change_count,scope,acknowledged,payload) VALUES (?,?,?,?,?,0,${payload})`, params);
  checkpoint();
  if (changed !== 1) fail("CORRUPT", "Outbox insertion did not affect exactly one row");
  const saved = await find(tx, id); checkpoint();
  if (saved === null || saved.delivery.sha256 !== sha256 || saved.delivery.acknowledged ||
      saved.scope !== scope || saved.delivery.changes !== result.changes || saved.delivery.byteLength !== result.changeset.byteLength) {
    fail("CORRUPT", "Outbox insertion was not confirmed");
  }
  return saved.delivery;
}

/** Persistent source-side delivery state; transport and remote ACK policy are caller-owned. */
export class ChangesetOutbox {
  readonly #target: ChangesetTarget;
  readonly #maxEntries: number;
  readonly #maxPayloadBytes: number;
  constructor(target: ChangesetTarget, options: ChangesetOutboxOptions = {}) {
    this.#target = target;
    this.#maxEntries = bound(options.maxEntries, 10_000, 100_000);
    this.#maxPayloadBytes = bound(options.maxPayloadBytes, 64 * 1024 * 1024, 1024 * 1024 * 1024);
  }

  /** Source DML, binary payload and delivery identity commit in one SQL transaction. */
  async record<T>(work: (tx: ChangesetExecutor) => T | Promise<T>, options: OutboxRecordOptions): Promise<OutboxRecordResult<T>> {
    const id = identity(options?.deliveryId);
    const capture = prepareChangesetCapture(work, options);
    const scope = JSON.stringify({ tables: capture.tables.map(fold).sort(), indirect: capture.indirect });
    if (globalThis.crypto?.subtle === undefined) fail("INPUT", "The outbox requires Web Crypto SHA-256");
    return this.#target.transaction(async tx => {
      capture.checkpoint(); await ensure(tx, true); capture.checkpoint();
      const existing = await find(tx, id); capture.checkpoint();
      if (existing !== null) {
        if (existing.scope !== scope) fail("REUSE", "Delivery identity was already used with another capture scope");
        await load(tx, existing); capture.checkpoint();
        return { replayed: true, delivery: existing.delivery };
      }
      const size = await query(tx, `SELECT count(*), coalesce(sum(length(payload)),0) FROM ${TABLE}`);
      if (size.length !== 1 || size[0]!.length !== 2) fail("CORRUPT", "Invalid outbox capacity result");
      const entries = integer(size[0]![0]), bytes = integer(size[0]![1]);
      if (entries >= this.#maxEntries || bytes > this.#maxPayloadBytes) fail("FULL", "Outbox retention limit reached; acknowledge or explicitly forget old deliveries");
      const result = await capture.run(tx); capture.checkpoint();
      if (result.changeset.byteLength > this.#maxPayloadBytes - bytes) fail("FULL", "Outbox pending payload budget exceeded");
      const delivery = await store(tx, id, scope, result, capture.checkpoint);
      return { replayed: false, value: result.value, delivery };
    }, capture.transactionOptions);
  }

  /**
   * Persist a consistent existing-row seed as this outbox's FIRST operation.
   * Later record() calls follow it in sequence. A retained seed ID is recovered,
   * never regenerated from newer rows. No source DML or receiver schema edits.
   */
  async bootstrap(options: OutboxBootstrapOptions): Promise<OutboxBootstrapResult> {
    const id = identity(options?.deliveryId), snapshot = prepareChangesetSnapshot(options);
    const scope = JSON.stringify({ tables: snapshot.tables.map(fold).sort(), indirect: snapshot.indirect, snapshot: true });
    if (globalThis.crypto?.subtle === undefined) fail("INPUT", "The outbox requires Web Crypto SHA-256");
    return this.#target.transaction(async tx => {
      snapshot.checkpoint(); await ensure(tx, true); snapshot.checkpoint();
      const existing = await find(tx, id); snapshot.checkpoint();
      if (existing !== null) {
        if (existing.scope !== scope) fail("REUSE", "Delivery identity was already used for another operation or snapshot scope");
        await load(tx, existing); snapshot.checkpoint();
        return Object.freeze({ replayed: true, delivery: existing.delivery });
      }
      // An empty pending list is not a pristine history. Even explicitly
      // forgotten acknowledgements leave sqlite_sequence advanced. Never
      // append a fresh baseline after incremental data or silently reseed.
      if ((await query(tx, `SELECT 1 FROM ${TABLE} LIMIT 1`)).length ||
          (await query(tx, "SELECT 1 FROM main.sqlite_sequence WHERE name=? COLLATE BINARY LIMIT 1", [CHANGESET_OUTBOX_TABLE])).length) {
        fail("STATE", "Bootstrap requires an unused outbox; recover the original seed ID instead of reseeding");
      }
      snapshot.checkpoint();
      const result = await snapshot.run(tx); snapshot.checkpoint();
      if (result.changeset.byteLength > this.#maxPayloadBytes) fail("FULL", "Outbox cannot retain the complete bootstrap payload");
      const delivery = await store(tx, id, scope, result, snapshot.checkpoint);
      if (delivery.sequence !== 1n) fail("STATE", "Bootstrap did not become the first outbox operation");
      return Object.freeze({ replayed: false, delivery });
    }, snapshot.transactionOptions);
  }

  /** Bounded metadata page. Sequence cursors are monotonic, not SQL OFFSETs. */
  async pending(options: OutboxPageOptions = {}): Promise<readonly OutboxDelivery[]> {
    const limit = bound(options.limit, 100, 256), after = options.after ?? 0n;
    if (typeof after !== "bigint" || after < 0n || after > (1n << 63n) - 1n) fail("INPUT", "after must be a nonnegative int64 bigint sequence");
    return this.#target.transaction(async tx => {
      if (!await ensure(tx, false)) return [];
      const rows = await query(tx, `SELECT ${META} FROM ${TABLE} WHERE acknowledged=0 AND seq>? ORDER BY seq LIMIT ?`, [after, BigInt(limit)]);
      if (rows.length > limit) fail("CORRUPT", "Outbox page exceeded its bound");
      let previous = after;
      return Object.freeze(rows.map(row => {
        const item = metadata(row).delivery;
        if (item.acknowledged || item.sequence <= previous) fail("CORRUPT", "Invalid outbox page order");
        previous = item.sequence; return item;
      }));
    });
  }

  /** Hash and validate one pending payload before returning owned bytes. */
  async read(deliveryId: string): Promise<OutboxReadResult | null> {
    const id = identity(deliveryId);
    return this.#target.transaction(async tx => {
      if (!await ensure(tx, false)) return null;
      const record = await find(tx, id);
      return record === null ? null : { delivery: record.delivery, changeset: await load(tx, record) };
    });
  }

  /** Call only after the receiver durably acknowledges this exact ID and digest. */
  async acknowledge(deliveryId: string, sha256: string): Promise<boolean> {
    const id = identity(deliveryId), expected = digest(sha256);
    return this.#target.transaction(async tx => {
      if (!await ensure(tx, false)) fail("ACK", "Unknown outbox delivery");
      const record = await find(tx, id);
      if (record === null || record.delivery.sha256 !== expected) fail("ACK", "Acknowledgement does not match a retained delivery");
      if (record.delivery.acknowledged) return false;
      await load(tx, record);
      const changed = await tx.execute(`UPDATE OR ABORT ${TABLE} SET acknowledged=1,payload=X'' WHERE delivery_id=? AND sha256=? AND acknowledged=0`, [id, expected]);
      if (changed !== 1) fail("CORRUPT", "Outbox acknowledgement did not affect exactly one row");
      return true;
    });
  }

  /** Explicitly ends deduplication for an acknowledged identity. Never auto-expires. */
  async forgetAcknowledged(deliveryId: string, sha256: string): Promise<boolean> {
    const id = identity(deliveryId), expected = digest(sha256);
    return this.#target.transaction(async tx => {
      if (!await ensure(tx, false)) return false;
      const record = await find(tx, id);
      if (record === null) return false;
      if (!record.delivery.acknowledged || record.delivery.sha256 !== expected) fail("STATE", "Only the exact acknowledged delivery may be forgotten");
      const changed = await tx.execute(`DELETE FROM ${TABLE} WHERE delivery_id=? AND sha256=? AND acknowledged=1`, [id, expected]);
      if (changed !== 1) fail("CORRUPT", "Outbox forgetting did not affect exactly one row");
      return true;
    });
  }
}
