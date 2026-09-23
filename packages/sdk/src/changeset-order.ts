import type { ApplyChangesetResult, ChangesetExecutor, ChangesetTarget } from "./changeset-apply";

export const CHANGESET_ORDER_TABLE = "__fsqlite_changeset_order";
export const CHANGESET_ORDER_HEAD_TABLE = "__fsqlite_changeset_order_head";
const TABLE = `main."${CHANGESET_ORDER_TABLE}"`;
const HEAD = `main."${CHANGESET_ORDER_HEAD_TABLE}"`;
const NAMES = ["seq", "receiver_id", "source_id", "delivery_id", "sha256", "byte_length", "applied", "omitted"];
const TYPES = ["INTEGER", "TEXT", "TEXT", "TEXT", "TEXT", "INTEGER", "INTEGER", "INTEGER"];
const ZERO_HASH = "0".repeat(64);
const MAX_SEQUENCE = (1n << 63n) - 1n;
const DDL = `CREATE TABLE ${TABLE} (seq INTEGER PRIMARY KEY, receiver_id TEXT NOT NULL, source_id TEXT NOT NULL, delivery_id TEXT NOT NULL UNIQUE COLLATE BINARY, sha256 TEXT NOT NULL, byte_length INTEGER NOT NULL, applied INTEGER NOT NULL, omitted INTEGER NOT NULL)`;
// Admit retained metadata in SQL BEFORE it can cross a worker boundary.
const META = "CAST(seq AS TEXT), " +
  "CASE WHEN length(CAST(receiver_id AS BLOB))<=256 THEN receiver_id END, " +
  "CASE WHEN length(CAST(source_id AS BLOB))<=256 THEN source_id END, " +
  "CASE WHEN length(CAST(delivery_id AS BLOB))<=512 THEN delivery_id END, " +
  "CASE WHEN length(CAST(sha256 AS BLOB))=64 THEN sha256 END, byte_length, applied, omitted";

export interface ChangesetOrderOptions {
  /** Stable routing identities, NOT authentication. One source per ledger. */
  receiverId: string;
  sourceId: string;
  /** Retained delivery identities, excluding the binding row. Default 100,000. */
  maxEntries?: number;
  /** Before copying a payload. Default 8 MiB; maximum 64 MiB. */
  maxMessageBytes?: number;
}
export interface ChangesetOrderOperationOptions {
  signal?: AbortSignal;
  timeoutMs?: number;
}
export interface OrderedChangeset {
  /** Contiguous source-outbox sequence, starting at one; never renumber retries. */
  readonly sequence: bigint;
  readonly deliveryId: string;
  readonly sha256: string;
  readonly changeset: Uint8Array;
}
export interface ChangesetOrderHead {
  readonly receiverId: string;
  readonly sourceId: string;
  readonly sequence: bigint;
  readonly deliveryId: string;
  readonly sha256: string;
  readonly byteLength: number;
}
/** SQL commit only. The delivery layer MUST still await its durability barrier. */
export interface ChangesetOrderResult extends ChangesetOrderHead, ApplyChangesetResult {}
export type OrderedChangesetApply = (
  target: ChangesetTarget,
  bytes: Uint8Array,
) => Promise<ApplyChangesetResult>;
export class ChangesetOrderError extends Error {
  constructor(
    readonly code: "ERR_FSQLITE_ORDER_INPUT" | "ERR_FSQLITE_ORDER_SCHEMA" |
      "ERR_FSQLITE_ORDER_CORRUPT" | "ERR_FSQLITE_ORDER_UNINITIALIZED" |
      "ERR_FSQLITE_ORDER_BINDING" | "ERR_FSQLITE_ORDER_GAP" |
      "ERR_FSQLITE_ORDER_REUSE" | "ERR_FSQLITE_ORDER_FULL" |
      "ERR_FSQLITE_ORDER_BUSY" | "ERR_FSQLITE_ORDER_CANCELLED" |
      "ERR_FSQLITE_ORDER_TIMEOUT",
    message: string,
  ) {
    super(message);
    this.name = "ChangesetOrderError";
  }
}
function fail(kind: ChangesetOrderError["code"], message: string): never {
  throw new ChangesetOrderError(kind, message);
}
function field(value: unknown, key: string): unknown {
  if (typeof value !== "object" || value === null || Array.isArray(value))
    fail("ERR_FSQLITE_ORDER_INPUT", "An ordered delivery record is required");
  const descriptor = Object.getOwnPropertyDescriptor(value, key);
  if (descriptor === undefined || !Object.hasOwn(descriptor, "value"))
    fail("ERR_FSQLITE_ORDER_INPUT", `${key} must be an own data property`);
  return descriptor.value;
}
function identity(value: unknown, maximum: number): string {
  if (typeof value !== "string" || !value.length || value.length > maximum || value.includes("\0"))
    fail("ERR_FSQLITE_ORDER_INPUT", "Invalid ordered-delivery identity");
  const bytes = new TextEncoder().encode(value);
  if (bytes.length > maximum || new TextDecoder("utf-8", { ignoreBOM: true }).decode(bytes) !== value)
    fail("ERR_FSQLITE_ORDER_INPUT", "Ordered-delivery identities must be bounded UTF-8");
  return value;
}
function bound(value: unknown, fallback: number, maximum: number): number {
  const n = value === undefined ? fallback : value;
  if (typeof n !== "number" || !Number.isSafeInteger(n) || n < 1 || n > maximum)
    fail("ERR_FSQLITE_ORDER_INPUT", `Expected an integer in 1..${maximum}`);
  return n;
}
function count(value: unknown): number {
  if (typeof value === "bigint" && value >= 0n && value <= BigInt(Number.MAX_SAFE_INTEGER)) return Number(value);
  if (typeof value === "number" && Number.isSafeInteger(value) && value >= 0) return value;
  return fail("ERR_FSQLITE_ORDER_CORRUPT", "Invalid ordered-delivery integer");
}
function hash(value: unknown): string {
  if (typeof value !== "string" || !/^[0-9a-f]{64}$/.test(value))
    fail("ERR_FSQLITE_ORDER_INPUT", "A lowercase SHA-256 digest is required");
  return value;
}
function ownBytes(value: unknown, maximum: number): Uint8Array {
  if (!(value instanceof Uint8Array)) fail("ERR_FSQLITE_ORDER_INPUT", "A changeset Uint8Array is required");
  try {
    const prototype = Object.getPrototypeOf(Uint8Array.prototype) as object;
    const get = (key: string): unknown => Object.getOwnPropertyDescriptor(prototype, key)!.get!.call(value);
    const buffer = get("buffer"), offset = get("byteOffset") as number, length = get("byteLength") as number;
    if (!(buffer instanceof ArrayBuffer) ||
      Object.getOwnPropertyDescriptor(ArrayBuffer.prototype, "resizable")?.get?.call(buffer) || length > maximum)
      fail("ERR_FSQLITE_ORDER_INPUT", "Payload exceeds its limit or uses a shared/resizable buffer");
    return new Uint8Array(new Uint8Array(buffer, offset, length));
  } catch (error) {
    if (error instanceof ChangesetOrderError) throw error;
    return fail("ERR_FSQLITE_ORDER_INPUT", "An attached, fixed changeset buffer is required");
  }
}
async function digest(bytes: Uint8Array): Promise<string> {
  const sum = new Uint8Array(await globalThis.crypto.subtle.digest("SHA-256", new Uint8Array(bytes)));
  return Array.from(sum, (byte) => byte.toString(16).padStart(2, "0")).join("");
}
function budget(options: ChangesetOrderOperationOptions) {
  const signal = options.signal, duration = options.timeoutMs;
  if (signal !== undefined) {
    try { Object.getOwnPropertyDescriptor(AbortSignal.prototype, "aborted")!.get!.call(signal); }
    catch { fail("ERR_FSQLITE_ORDER_INPUT", "signal must be an AbortSignal"); }
  }
  if (duration !== undefined) bound(duration, 1, 2_147_483_647);
  const deadline = duration === undefined ? undefined : performance.now() + duration;
  const checkpoint = (): void => {
    if (signal?.aborted) fail("ERR_FSQLITE_ORDER_CANCELLED", "Ordered delivery cancelled");
    if (deadline !== undefined && performance.now() >= deadline)
      fail("ERR_FSQLITE_ORDER_TIMEOUT", "Ordered delivery deadline expired");
  };
  return {
    checkpoint,
    transactionOptions(): ChangesetOrderOperationOptions {
      checkpoint();
      const result: ChangesetOrderOperationOptions = {};
      if (signal !== undefined) result.signal = signal;
      if (deadline !== undefined) result.timeoutMs = Math.max(1, Math.ceil(deadline - performance.now()));
      return result;
    },
  };
}
async function query(tx: ChangesetExecutor, sql: string, params: readonly (string | bigint)[] = []) {
  const result = await tx.query(sql, params);
  if (!Array.isArray(result.rowArrays) || result.rowArrays.some(row => !Array.isArray(row)))
    fail("ERR_FSQLITE_ORDER_CORRUPT", "Invalid ordered-delivery SQL result");
  return result.rowArrays;
}
async function ensure(tx: ChangesetExecutor): Promise<boolean> {
  const objects = await query(tx, "SELECT type FROM main.sqlite_schema WHERE name=? COLLATE NOCASE", [CHANGESET_ORDER_TABLE]);
  if (!objects.length) return false;
  if (objects.length !== 1 || objects[0]![0] !== "table")
    fail("ERR_FSQLITE_ORDER_SCHEMA", "The ordered-delivery ledger must be a table");
  const listed = (await query(tx, `PRAGMA main.table_list('${CHANGESET_ORDER_TABLE}')`))
    .filter(row => row[0] === "main" && row[1] === CHANGESET_ORDER_TABLE);
  if (listed.length !== 1 || listed[0]![2] !== "table" || count(listed[0]![3]) !== 8 || count(listed[0]![4]) !== 0)
    fail("ERR_FSQLITE_ORDER_SCHEMA", "Invalid ordered-delivery table layout");
  const columns = await query(tx, `PRAGMA main.table_xinfo('${CHANGESET_ORDER_TABLE}')`);
  if (columns.length !== 8) fail("ERR_FSQLITE_ORDER_SCHEMA", "Invalid ledger columns");
  for (let i = 0; i < columns.length; i++) {
    const row = columns[i]!;
    if (count(row[0]) !== i || row[1] !== NAMES[i] || typeof row[2] !== "string" ||
      row[2].toUpperCase() !== TYPES[i] || (i > 0 && count(row[3]) !== 1) || row[4] !== null ||
      count(row[5]) !== (i === 0 ? 1 : 0) || count(row[6]) !== 0)
      fail("ERR_FSQLITE_ORDER_SCHEMA", "Invalid ledger column definition");
  }
  const indexes = await query(tx, `PRAGMA main.index_list('${CHANGESET_ORDER_TABLE}')`);
  if (indexes.length !== 1 || count(indexes[0]![2]) !== 1 || indexes[0]![3] !== "u" || count(indexes[0]![4]) !== 0)
    fail("ERR_FSQLITE_ORDER_SCHEMA", "The ledger requires one delivery-identity unique index");
  const indexName = identity(indexes[0]![1], 1024).replaceAll("'", "''");
  const keys = (await query(tx, `PRAGMA main.index_xinfo('${indexName}')`)).filter(row => count(row[5]) === 1);
  if (keys.length !== 1 || count(keys[0]![1]) !== 3 || keys[0]![2] !== "delivery_id" ||
    count(keys[0]![3]) !== 0 || keys[0]![4] !== "BINARY")
    fail("ERR_FSQLITE_ORDER_SCHEMA", "Delivery identities require an ascending BINARY unique key");
  if ((await query(tx, `PRAGMA main.foreign_key_list('${CHANGESET_ORDER_TABLE}')`)).length)
    fail("ERR_FSQLITE_ORDER_SCHEMA", "Foreign keys on the order ledger are not supported");
  for (const ns of ["main", "temp"]) {
    if ((await query(tx, `SELECT 1 FROM ${ns}.sqlite_schema WHERE type='trigger' AND tbl_name=? COLLATE NOCASE LIMIT 1`, [CHANGESET_ORDER_TABLE])).length)
      fail("ERR_FSQLITE_ORDER_SCHEMA", "Triggers on the order ledger are not supported");
  }
  return true;
}
async function ensureHead(tx: ChangesetExecutor): Promise<boolean> {
  const objects = await query(tx, "SELECT type FROM main.sqlite_schema WHERE name=? COLLATE NOCASE", [CHANGESET_ORDER_HEAD_TABLE]);
  if (!objects.length) return false;
  if (objects.length !== 1 || objects[0]![0] !== "table")
    fail("ERR_FSQLITE_ORDER_SCHEMA", "The order head must be an ordinary table");
  const listed = (await query(tx, `PRAGMA main.table_list('${CHANGESET_ORDER_HEAD_TABLE}')`))
    .filter(row => row[0] === "main" && row[1] === CHANGESET_ORDER_HEAD_TABLE);
  if (listed.length !== 1 || listed[0]![2] !== "table" || count(listed[0]![3]) !== 2 || count(listed[0]![4]) !== 0)
    fail("ERR_FSQLITE_ORDER_SCHEMA", "Invalid order-head layout");
  const columns = await query(tx, `PRAGMA main.table_xinfo('${CHANGESET_ORDER_HEAD_TABLE}')`);
  if (columns.length !== 2) fail("ERR_FSQLITE_ORDER_SCHEMA", "Invalid order-head columns");
  for (let i = 0; i < columns.length; i++) {
    const row = columns[i]!;
    if (count(row[0]) !== i || row[1] !== ["slot", "receipt"][i] ||
      typeof row[2] !== "string" || row[2].toUpperCase() !== ["INTEGER", "TEXT"][i] ||
      (i === 1 && count(row[3]) !== 1) || row[4] !== null || count(row[5]) !== (i === 0 ? 1 : 0) || count(row[6]) !== 0)
      fail("ERR_FSQLITE_ORDER_SCHEMA", "Invalid order-head column definition");
  }
  if ((await query(tx, `PRAGMA main.index_list('${CHANGESET_ORDER_HEAD_TABLE}')`)).length ||
    (await query(tx, `PRAGMA main.foreign_key_list('${CHANGESET_ORDER_HEAD_TABLE}')`)).length)
    fail("ERR_FSQLITE_ORDER_SCHEMA", "Indexes and foreign keys on the order head are not supported");
  for (const ns of ["main", "temp"]) {
    if ((await query(tx, `SELECT 1 FROM ${ns}.sqlite_schema WHERE type='trigger' AND tbl_name=? COLLATE NOCASE LIMIT 1`, [CHANGESET_ORDER_HEAD_TABLE])).length)
      fail("ERR_FSQLITE_ORDER_SCHEMA", "Triggers on the order head are not supported");
  }
  return true;
}
type Stored = ChangesetOrderHead & { readonly applied: number; readonly omitted: number };
function metadata(row: readonly unknown[]): Stored {
  try {
    if (row.length !== 8 || typeof row[0] !== "string" || !/^(0|[1-9][0-9]{0,18})$/.test(row[0]))
      fail("ERR_FSQLITE_ORDER_CORRUPT", "Invalid retained sequence");
    const sequence = BigInt(row[0]), receiverId = identity(row[1], 256), sourceId = identity(row[2], 256);
    const deliveryId = sequence === 0n && row[3] === "" ? "" : identity(row[3], 512);
    const sha256 = hash(row[4]), byteLength = count(row[5]), applied = count(row[6]), omitted = count(row[7]);
    if (sequence > MAX_SEQUENCE || byteLength > 64 * 1024 * 1024 || applied + omitted > 100_000 ||
      (sequence === 0n && (deliveryId !== "" || sha256 !== ZERO_HASH || byteLength !== 0 || applied !== 0 || omitted !== 0)))
      fail("ERR_FSQLITE_ORDER_CORRUPT", "Invalid retained ordered-delivery metadata");
    return Object.freeze({ sequence, receiverId, sourceId, deliveryId, sha256, byteLength, applied, omitted });
  } catch (error) {
    if (error instanceof ChangesetOrderError && error.code === "ERR_FSQLITE_ORDER_CORRUPT") throw error;
    return fail("ERR_FSQLITE_ORDER_CORRUPT", "Invalid retained order-ledger metadata");
  }
}
function fingerprint(row: Stored): string {
  return JSON.stringify({ ...row, sequence: row.sequence.toString() });
}

/**
 * @internal Publish/verify the prefix installed by ChangesetBootstrapReceiver.
 * The caller owns ONE transaction containing the validated baseline, this ledger
 * and the installed marker. Metadata must come from that same verified staging
 * snapshot, never from an untrusted remote receipt. This is not an enrollment API.
 *
 * Keep one metadata record at a time and advance the head once, avoiding a full
 * ledger population scan for every seed chunk. On installed replay, never create
 * missing storage or move the head backwards over later incremental deliveries.
 */
export async function bootstrapOrderPrefix(
  tx: ChangesetExecutor,
  options: {
    readonly receiverId: string;
    readonly sourceId: string;
    readonly deliveryId: string;
    readonly chunks: number;
    readonly replayed: boolean;
  },
  readChunk: (index: number) => Promise<{
    readonly sha256: string;
    readonly byteLength: number;
    readonly changes: number;
  }>,
  checkpoint: () => void,
): Promise<ChangesetOrderHead> {
  const receiverId = identity(options.receiverId, 256);
  const sourceId = identity(options.sourceId, 256);
  const root = identity(options.deliveryId, 480);
  const chunks = bound(options.chunks, 1, 100_000);
  const replayed = options.replayed;
  if (typeof replayed !== "boolean" || typeof readChunk !== "function" || typeof checkpoint !== "function")
    fail("ERR_FSQLITE_ORDER_INPUT", "Invalid verified-bootstrap prefix operation");
  const inside: ChangesetTarget = { transaction: async work => work(tx) };
  const order = new ChangesetOrder(inside, { receiverId, sourceId });
  checkpoint();
  // In particular, replay MUST NOT call initialize(): both missing tables can
  // mean lost history, not a new stream. Fresh installation permits only genesis.
  const before = replayed ? await order.head() : await order.initialize();
  checkpoint();
  if (replayed ? before.sequence < BigInt(chunks) : before.sequence !== 0n)
    fail("ERR_FSQLITE_ORDER_REUSE", "Bootstrap does not match the retained order frontier");
  let last: Stored | null = null;
  for (let index = 0; index < chunks; index++) {
    checkpoint();
    const meta = await readChunk(index);
    checkpoint();
    const sequence = BigInt(index + 1);
    const next = metadata([
      sequence.toString(), receiverId, sourceId,
      index === 0 ? root : `${root}/chunk/${index}`,
      meta.sha256, meta.byteLength, meta.changes, 0,
    ]);
    if (!replayed) {
      const changed = await tx.execute(
        `INSERT OR ABORT INTO ${TABLE} VALUES (?,?,?,?,?,?,?,?)`,
        [sequence, receiverId, sourceId, next.deliveryId, next.sha256,
          BigInt(next.byteLength), BigInt(next.applied), 0n],
      );
      checkpoint();
      if (changed !== 1)
        fail("ERR_FSQLITE_ORDER_CORRUPT", "Bootstrap order entry was not stored exactly once");
    }
    const rows = await query(tx, `SELECT ${META} FROM ${TABLE} WHERE seq=?`, [sequence]);
    checkpoint();
    if (rows.length !== 1 || fingerprint(metadata(rows[0]!)) !== fingerprint(next))
      fail("ERR_FSQLITE_ORDER_CORRUPT", "Bootstrap order prefix disagrees with verified staging");
    last = next;
  }
  if (last === null) fail("ERR_FSQLITE_ORDER_CORRUPT", "Missing bootstrap order prefix");
  if (!replayed) {
    const initial = metadata(["0", receiverId, sourceId, "", ZERO_HASH, 0, 0, 0]);
    const changed = await tx.execute(
      `UPDATE OR ABORT ${HEAD} SET receipt=? WHERE slot=1 AND receipt=? COLLATE BINARY`,
      [fingerprint(last), fingerprint(initial)],
    );
    checkpoint();
    if (changed !== 1)
      fail("ERR_FSQLITE_ORDER_CORRUPT", "Bootstrap order head did not advance atomically");
  }
  const after = await order.head();
  checkpoint();
  // head() returns the same Stored object, including its original decisions.
  const expected = replayed ? before : last;
  if (JSON.stringify(after, (_, value) => typeof value === "bigint" ? value.toString() : value) !==
      JSON.stringify(expected, (_, value) => typeof value === "bigint" ? value.toString() : value))
    fail("ERR_FSQLITE_ORDER_CORRUPT", "Bootstrap changed an unexpected order frontier");
  return last;
}

/** A staged ordered baseline owns its prefix until the whole install commits. */
async function requireInstalledBootstrap(tx: ChangesetExecutor, head: Stored): Promise<string | null> {
  // These names are the bootstrap storage protocol. Avoid a runtime import cycle:
  // bootstrap's publisher already imports this module, and both paths are covered
  // together by changeset-bootstrap-order.test.mjs.
  const stateName = "__fsqlite_bootstrap_state";
  const chunksName = "__fsqlite_bootstrap_chunks";
  const objects = await query(tx,
    "SELECT name, type FROM main.sqlite_schema WHERE name COLLATE NOCASE IN (?,?)",
    [stateName, chunksName]);
  if (objects.length === 0) return null;
  if (objects.length !== 2 || objects.some(row => row[1] !== "table"))
    fail("ERR_FSQLITE_ORDER_CORRUPT", "Partial or invalid bootstrap storage cannot authorize ordered writes");
  const rows = await query(tx,
    `SELECT id, CASE WHEN length(CAST(manifest AS BLOB))<=131072 THEN manifest END, ` +
    `installed, received, bytes, changes, CASE WHEN length(chain)=64 THEN chain END ` +
    `FROM main."${stateName}" LIMIT 2`);
  if (rows.length !== 1 || rows[0]!.length !== 7 || count(rows[0]![0]) !== 1 || typeof rows[0]![1] !== "string")
    fail("ERR_FSQLITE_ORDER_CORRUPT", "Missing or invalid bootstrap authority");
  const row = rows[0]!;
  let value: unknown;
  try { value = JSON.parse(row[1] as string); }
  catch { fail("ERR_FSQLITE_ORDER_CORRUPT", "Invalid bootstrap authority JSON"); }
  if (typeof value !== "object" || value === null || Array.isArray(value))
    fail("ERR_FSQLITE_ORDER_CORRUPT", "Invalid bootstrap authority record");
  if (!Object.hasOwn(value, "orderedSourceId")) return null; // Explicit legacy, unordered policy.
  const m = value as Record<string, unknown>;
  if (m.protocol !== "fsqlite-bootstrap-v1" || typeof m.receiverId !== "string" ||
      typeof m.orderedSourceId !== "string" || typeof m.deliveryId !== "string" ||
      !m.deliveryId.length || m.deliveryId.length > 480 || m.deliveryId.includes("\0") ||
      typeof m.chunks !== "number" || !Number.isSafeInteger(m.chunks) || m.chunks < 1 || m.chunks > 100_000 ||
      typeof m.byteLength !== "number" || !Number.isSafeInteger(m.byteLength) || m.byteLength < 0 || m.byteLength > 1024 * 1024 * 1024 ||
      typeof m.changes !== "number" || !Number.isSafeInteger(m.changes) || m.changes < 0 || m.changes > 10_000_000 ||
      typeof m.sha256 !== "string" || !/^[0-9a-f]{64}$/.test(m.sha256))
    fail("ERR_FSQLITE_ORDER_CORRUPT", "Invalid ordered bootstrap authority fields");
  if (m.receiverId !== head.receiverId || m.orderedSourceId !== head.sourceId)
    fail("ERR_FSQLITE_ORDER_BINDING", "Ordered writes do not own this bootstrap's source/receiver binding");
  const installed = count(row[2]);
  if (installed === 0)
    fail("ERR_FSQLITE_ORDER_GAP", "Finish atomic bootstrap installation before delivering its ordered stream");
  if (installed !== 1 || count(row[3]) !== m.chunks || count(row[4]) !== m.byteLength ||
      count(row[5]) !== m.changes || row[6] !== m.sha256 || head.sequence < BigInt(m.chunks))
    fail("ERR_FSQLITE_ORDER_CORRUPT", "Installed bootstrap and ordered history disagree; do not reinitialize");
  const prefix = await query(tx, `SELECT ${META} FROM ${TABLE} WHERE seq=?`, [BigInt(m.chunks)]);
  const chunk = await query(tx,
    `SELECT CASE WHEN length(sha256)=64 THEN sha256 END, byte_length, change_count, typeof(payload), length(payload) ` +
    `FROM main."${chunksName}" WHERE idx=?`, [BigInt(m.chunks - 1)]);
  if (prefix.length !== 1 || chunk.length !== 1 || chunk[0]!.length !== 5)
    fail("ERR_FSQLITE_ORDER_CORRUPT", "Installed bootstrap lost its terminal order receipt");
  const saved = metadata(prefix[0]!), end = chunk[0]!;
  if (saved.receiverId !== head.receiverId || saved.sourceId !== head.sourceId ||
      saved.deliveryId !== (m.chunks === 1 ? m.deliveryId : `${m.deliveryId}/chunk/${m.chunks - 1}`) ||
      saved.sha256 !== end[0] || saved.byteLength !== count(end[1]) || saved.applied !== count(end[2]) ||
      saved.omitted !== 0 || end[3] !== "blob" || count(end[4]) !== 0)
    fail("ERR_FSQLITE_ORDER_CORRUPT", "Installed bootstrap terminal receipt was changed or not reclaimed");
  return row[1] as string;
}

/** Drain admitted child scopes/SQL even when the application forgets to await. */
async function applyScoped(
  tx: ChangesetExecutor,
  apply: OrderedChangesetApply,
  bytes: Uint8Array,
  checkpoint: () => void,
): Promise<ApplyChangesetResult> {
  let accepting = true;
  const children = new Set<Promise<unknown>>(), errors: unknown[] = [];
  function track<T>(pending: Set<Promise<unknown>>, failures: unknown[], operation: Promise<T>): Promise<T> {
    pending.add(operation);
    void operation.then(() => pending.delete(operation), error => {
      failures.push(error);
      pending.delete(operation);
    });
    return operation;
  }
  const scoped: ChangesetTarget = {
    transaction: <T>(work: (executor: ChangesetExecutor) => Promise<T>, options: ChangesetOrderOperationOptions = {}): Promise<T> => {
      if (!accepting) return Promise.reject(new ChangesetOrderError("ERR_FSQLITE_ORDER_INPUT", "Ordered application scope has ended"));
      return track(children, errors, (async () => {
        const child = budget(options);
        let open = true;
        const pending = new Set<Promise<unknown>>(), failures: unknown[] = [];
        const submit = <U>(operation: () => Promise<U>): Promise<U> => {
          if (!open) return Promise.reject(new ChangesetOrderError("ERR_FSQLITE_ORDER_INPUT", "Ordered SQL scope has ended"));
          return track(pending, failures, (async () => {
            checkpoint(); child.checkpoint();
            const result = await operation();
            checkpoint(); child.checkpoint();
            return result;
          })());
        };
        const executor: ChangesetExecutor = {
          execute: (sql, params) => submit(() => tx.execute(sql, params)),
          query: (sql, params) => submit(() => tx.query(sql, params)),
        };
        let value: T;
        try {
          checkpoint(); child.checkpoint();
          value = await work(executor);
        } finally {
          open = false;
          await Promise.allSettled(pending);
        }
        if (failures.length) throw failures[0];
        checkpoint(); child.checkpoint();
        return value;
      })());
    },
  };
  let result: ApplyChangesetResult;
  try { result = await apply(scoped, bytes); }
  finally {
    accepting = false;
    await Promise.allSettled(children);
  }
  if (errors.length) throw errors[0];
  checkpoint();
  return result;
}

/**
 * Persisted, contiguous admission for ONE source on ONE target. Fresh application,
 * its receipt/rebase journal and the sequence marker share one SQL transaction.
 * Replays return retained decisions WITHOUT invoking application/conflict callbacks.
 * Not a SQL sandbox, transport, consensus algorithm or durability acknowledgement.
 * Callbacks must only apply the supplied bytes through the supplied scoped target;
 * they must not manipulate this ledger or publish external effects.
 */
export class ChangesetOrder {
  readonly #target: ChangesetTarget;
  readonly #receiver: string;
  readonly #source: string;
  readonly #entries: number;
  readonly #bytes: number;
  #active = false;
  constructor(target: ChangesetTarget, options: ChangesetOrderOptions) {
    this.#target = target;
    this.#receiver = identity(options?.receiverId, 256);
    this.#source = identity(options?.sourceId, 256);
    this.#entries = bound(options.maxEntries, 100_000, 100_000);
    this.#bytes = bound(options.maxMessageBytes, 8 * 1024 * 1024, 64 * 1024 * 1024);
  }
  async #head(tx: ChangesetExecutor): Promise<Stored> {
    if (!(await ensure(tx))) fail("ERR_FSQLITE_ORDER_UNINITIALIZED", "Explicitly initialize a new ordered receiver before delivery");
    if (!(await ensureHead(tx))) fail("ERR_FSQLITE_ORDER_CORRUPT", "The order ledger lost its independently retained head");
    const extent = await query(tx, `SELECT count(*), CAST(min(seq) AS TEXT), CAST(max(seq) AS TEXT) FROM ${TABLE}`);
    if (extent.length !== 1 || extent[0]!.length !== 3 || extent[0]![1] !== "0" ||
      typeof extent[0]![2] !== "string" || !/^(0|[1-9][0-9]{0,18})$/.test(extent[0]![2] as string))
      fail("ERR_FSQLITE_ORDER_CORRUPT", "Missing order-ledger binding or invalid sequence extent");
    const last = BigInt(extent[0]![2] as string);
    if (last > MAX_SEQUENCE || BigInt(count(extent[0]![0])) !== last + 1n)
      fail("ERR_FSQLITE_ORDER_CORRUPT", "The retained order ledger has gaps; do not reset or reseed it");
    const rows = await query(tx, `SELECT ${META} FROM ${TABLE} WHERE seq=0 OR seq=? ORDER BY seq`, [last]);
    if (rows.length !== (last === 0n ? 1 : 2)) fail("ERR_FSQLITE_ORDER_CORRUPT", "Missing ledger endpoints");
    for (const row of rows) {
      const entry = metadata(row);
      if (entry.receiverId !== this.#receiver || entry.sourceId !== this.#source)
        fail("ERR_FSQLITE_ORDER_BINDING", "This ledger belongs to another source or receiver");
    }
    const lastEntry = metadata(rows[rows.length - 1]!);
    const heads = await query(tx, `SELECT slot, CASE WHEN length(CAST(receipt AS BLOB))<=4096 THEN receipt END FROM ${HEAD} LIMIT 2`);
    if (heads.length !== 1 || count(heads[0]![0]) !== 1 || heads[0]![1] !== fingerprint(lastEntry))
      fail("ERR_FSQLITE_ORDER_CORRUPT", "Retained head disagrees with the ledger; a committed tail may be missing");
    return lastEntry;
  }
  /**
   * Explicit NEW-stream enrollment; never performed by apply(). Reopening the
   * same binding is idempotent. A damaged existing ledger is never recreated.
   * The caller owns baseline/schema admission. This does not install a snapshot.
   */
  async initialize(options: ChangesetOrderOperationOptions = {}): Promise<ChangesetOrderHead> {
    const b = budget(options);
    return this.#target.transaction(async tx => {
      b.checkpoint();
      const ledger = await ensure(tx), headExists = await ensureHead(tx);
      if (ledger !== headExists) fail("ERR_FSQLITE_ORDER_CORRUPT", "Partial ordered-delivery state must not be reinitialized");
      if (!ledger) {
        await tx.execute(DDL);
        await tx.execute(`INSERT INTO ${TABLE} VALUES (0,?,?,?, ?,0,0,0)`, [this.#receiver, this.#source, "", ZERO_HASH]);
        await tx.execute(`CREATE TABLE ${HEAD} (slot INTEGER PRIMARY KEY, receipt TEXT NOT NULL)`);
        const initial = metadata(["0", this.#receiver, this.#source, "", ZERO_HASH, 0, 0, 0]);
        await tx.execute(`INSERT INTO ${HEAD} VALUES (1,?)`, [fingerprint(initial)]);
      }
      const head = await this.#head(tx);
      b.checkpoint();
      return head;
    }, b.transactionOptions());
  }
  async head(options: ChangesetOrderOperationOptions = {}): Promise<ChangesetOrderHead> {
    const b = budget(options);
    return this.#target.transaction(async tx => {
      const head = await this.#head(tx);
      b.checkpoint();
      return head;
    }, b.transactionOptions());
  }
  async apply(
    message: OrderedChangeset,
    apply: OrderedChangesetApply,
    options: ChangesetOrderOperationOptions = {},
  ): Promise<ChangesetOrderResult> {
    if (this.#active) fail("ERR_FSQLITE_ORDER_BUSY", "This ordered receiver is active; nothing was queued");
    this.#active = true;
    try {
      const b = budget(options), sequence = field(message, "sequence");
      const deliveryId = identity(field(message, "deliveryId"), 512), sha256 = hash(field(message, "sha256"));
      if (typeof sequence !== "bigint" || sequence < 1n || sequence > MAX_SEQUENCE || typeof apply !== "function")
        fail("ERR_FSQLITE_ORDER_INPUT", "Use an int64 positive sequence and an application callback");
      b.checkpoint();
      const bytes = ownBytes(field(message, "changeset"), this.#bytes), byteLength = bytes.byteLength;
      if ((await digest(bytes)) !== sha256) fail("ERR_FSQLITE_ORDER_INPUT", "Ordered payload does not match its digest");
      b.checkpoint();
      return await this.#target.transaction(async tx => {
        b.checkpoint();
        const before = await this.#head(tx);
        const bootstrap = await requireInstalledBootstrap(tx, before);
        b.checkpoint();
        if (sequence > before.sequence + 1n) fail("ERR_FSQLITE_ORDER_GAP", "A source predecessor is missing; no rows were applied");
        if (sequence <= before.sequence) {
          const rows = await query(tx, `SELECT ${META} FROM ${TABLE} WHERE seq=?`, [sequence]);
          if (rows.length !== 1) fail("ERR_FSQLITE_ORDER_CORRUPT", "Missing ordered replay receipt");
          const saved = metadata(rows[0]!);
          if (saved.receiverId !== this.#receiver || saved.sourceId !== this.#source ||
            saved.deliveryId !== deliveryId || saved.sha256 !== sha256 || saved.byteLength !== byteLength)
            fail("ERR_FSQLITE_ORDER_REUSE", "A sequence was reused for different content or identity");
          b.checkpoint();
          return Object.freeze({ ...saved, replayed: true });
        }
        if (before.sequence >= BigInt(this.#entries)) fail("ERR_FSQLITE_ORDER_FULL", "Order-ledger identity retention limit reached");
        if ((await query(tx, `SELECT 1 FROM ${TABLE} WHERE delivery_id=? LIMIT 1`, [deliveryId])).length)
          fail("ERR_FSQLITE_ORDER_REUSE", "A delivery identity was reused at a new sequence");
        b.checkpoint();
        // No nested BEGIN/COMMIT and no second target: applyChangeset and its
        // rebase journal must use this SAME outer transaction.
        const result = await applyScoped(tx, apply, bytes, b.checkpoint);
        b.checkpoint();
        if ((await digest(bytes)) !== sha256)
          fail("ERR_FSQLITE_ORDER_CORRUPT", "Application changed the admitted payload bytes");
        const applied = count(field(result, "applied")), omitted = count(field(result, "omitted"));
        if (field(result, "replayed") !== false || applied + omitted > 100_000)
          fail("ERR_FSQLITE_ORDER_REUSE", "Fresh ordered work must not reuse an unordered receipt");
        if (fingerprint(await this.#head(tx)) !== fingerprint(before))
          fail("ERR_FSQLITE_ORDER_CORRUPT", "Application work changed the order ledger");
        if (await requireInstalledBootstrap(tx, before) !== bootstrap)
          fail("ERR_FSQLITE_ORDER_CORRUPT", "Application work changed its bootstrap authority");
        b.checkpoint();
        const changed = await tx.execute(`INSERT OR ABORT INTO ${TABLE} VALUES (?,?,?,?,?,?,?,?)`,
          [sequence, this.#receiver, this.#source, deliveryId, sha256, BigInt(byteLength), BigInt(applied), BigInt(omitted)]);
        if (changed !== 1) fail("ERR_FSQLITE_ORDER_CORRUPT", "Sequence marker was not written exactly once");
        const next = metadata([sequence.toString(), this.#receiver, this.#source, deliveryId, sha256, byteLength, applied, omitted]);
        if ((await tx.execute(`UPDATE OR ABORT ${HEAD} SET receipt=? WHERE slot=1 AND receipt=? COLLATE BINARY`,
          [fingerprint(next), fingerprint(before)])) !== 1)
          fail("ERR_FSQLITE_ORDER_CORRUPT", "Ordered-delivery head did not advance atomically");
        const after = await this.#head(tx);
        if (after.sequence !== sequence || after.deliveryId !== deliveryId || after.sha256 !== sha256 ||
          after.byteLength !== byteLength || after.applied !== applied || after.omitted !== omitted)
          fail("ERR_FSQLITE_ORDER_CORRUPT", "Sequence publication was not confirmed inside its transaction");
        b.checkpoint();
        return Object.freeze({ ...after, replayed: false });
      }, b.transactionOptions());
    } finally { this.#active = false; }
  }
}
