import { decodeChangeset } from "./changeset-codec";
import type { ChangesetChange, ChangesetField, ChangesetLimits, ChangesetTable, ChangesetValue } from "./changeset-codec";

/** The ordinary SQL surface used inside one owned transaction/savepoint. */
export interface ChangesetExecutor {
  execute(sql: string, params?: readonly ChangesetValue[]): Promise<number>;
  query(sql: string, params?: readonly ChangesetValue[]): Promise<{
    rowArrays: readonly (readonly unknown[])[];
  }>;
}

/** Implemented by FrankenDB and FrankenTransaction; no WASM session hooks needed. */
export interface ChangesetTarget {
  transaction<T>(work: (tx: ChangesetExecutor) => Promise<T>, options?: {
    signal?: AbortSignal;
    timeoutMs?: number;
  }): Promise<T>;
}

export type ChangesetConflictKind = "data" | "not-found" | "conflict";
export interface ChangesetConflict {
  readonly kind: ChangesetConflictKind;
  readonly table: string;
  /** Zero-based position in the complete changeset, not just this table. */
  readonly changeIndex: number;
  readonly columns: readonly string[];
  /** Owned before/after images; mutating a blob cannot change pending SQL. */
  readonly change: ChangesetChange;
}

export interface ApplyChangesetOptions {
  /** Explicit allowlist of direct target tables in main; NOT a SQL sandbox. */
  tables: readonly string[];
  /** Stable source-qualified delivery identity; atomically recorded with the rows. */
  deliveryId?: string;
  /** Default: abort. SQL/constraint errors always abort, never become omissions. */
  onConflict?: (conflict: ChangesetConflict) => "abort" | "omit" | Promise<"abort" | "omit">;
  limits?: ChangesetLimits;
  signal?: AbortSignal;
  timeoutMs?: number;
}

export interface ApplyChangesetResult {
  /** Direct row changes, excluding trigger and foreign-key side effects. */
  readonly applied: number;
  readonly omitted: number;
  /** A retained receipt was returned; counts describe its original application. */
  readonly replayed: boolean;
}

/** Reserved local inbox; removing receipts removes duplicate-delivery protection. */
export const CHANGESET_RECEIPTS_TABLE = "__fsqlite_changeset_receipts";

export class ChangesetApplyError extends Error {
  constructor(readonly code: "ERR_FSQLITE_CHANGESET_INPUT" | "ERR_FSQLITE_CHANGESET_SCHEMA" |
    "ERR_FSQLITE_CHANGESET_CONFLICT" | "ERR_FSQLITE_CHANGESET_RESULT" |
    "ERR_FSQLITE_CHANGESET_CANCELLED" | "ERR_FSQLITE_CHANGESET_TIMEOUT" |
    "ERR_FSQLITE_CHANGESET_DELIVERY_REUSE" | "ERR_FSQLITE_CHANGESET_RECEIPT",
    message: string, readonly conflict?: ChangesetConflict, options?: ErrorOptions) {
    super(message, options); this.name = "ChangesetApplyError";
  }
}

const quote = (name: string): string => `"${name.replaceAll('"', '""')}"`;
const literal = (name: string): string => `'${name.replaceAll("'", "''")}'`;
const fold = (name: string): string => name.replace(/[A-Z]/g, c => c.toLowerCase());
function invalid(message: string): never {
  throw new ChangesetApplyError("ERR_FSQLITE_CHANGESET_INPUT", message);
}
function badResult(): never {
  throw new ChangesetApplyError("ERR_FSQLITE_CHANGESET_RESULT", "Invalid changeset SQL result; application was stopped");
}
function schema(message: string): never {
  throw new ChangesetApplyError("ERR_FSQLITE_CHANGESET_SCHEMA", message);
}
function identifier(value: unknown): string {
  if (typeof value !== "string" || !value.length || value.length > 1024 || value.includes("\0")) {
    return invalid("Use nonempty identifiers of at most 1024 characters without NUL");
  }
  return value;
}
function integer(value: unknown): number {
  if (typeof value === "bigint" && value >= 0n && value <= BigInt(Number.MAX_SAFE_INTEGER)) return Number(value);
  if (typeof value === "number" && Number.isSafeInteger(value) && value >= 0) return value;
  return badResult();
}

function capture(options: ApplyChangesetOptions) {
  const source = options?.tables, onConflict = options?.onConflict, limits = options?.limits;
  const signal = options?.signal, timeoutMs = options?.timeoutMs;
  const deliveryId = options?.deliveryId;
  if (!Array.isArray(source) || source.length > 256) invalid("tables must be an explicit allowlist of at most 256 names");
  const names = new Set<string>();
  for (let i = 0, n = source.length; i < n; i++) {
    const name = fold(identifier(source[i]));
    if (name.startsWith("sqlite_") || name === CHANGESET_RECEIPTS_TABLE || names.has(name)) {
      invalid("Allowlist names must be distinct application tables, not the reserved changeset inbox");
    }
    names.add(name);
  }
  if (onConflict !== undefined && typeof onConflict !== "function") invalid("onConflict must be a function");
  if (deliveryId !== undefined) {
    if (typeof deliveryId !== "string" || deliveryId.length === 0 || deliveryId.length > 512 || deliveryId.includes("\0")) {
      invalid("deliveryId must be nonempty UTF-8 text of at most 512 bytes without NUL");
    }
    const encoded = new TextEncoder().encode(deliveryId);
    if (encoded.length > 512 || new TextDecoder("utf-8", { ignoreBOM: true }).decode(encoded) !== deliveryId) {
      invalid("deliveryId must be nonempty UTF-8 text of at most 512 bytes without NUL");
    }
  }
  if (timeoutMs !== undefined && (!Number.isSafeInteger(timeoutMs) || timeoutMs < 1 || timeoutMs > 2_147_483_647)) {
    invalid("timeoutMs must be an integer in 1..2147483647");
  }
  if (signal !== undefined) {
    try { Object.getOwnPropertyDescriptor(AbortSignal.prototype, "aborted")!.get!.call(signal); }
    catch { invalid("signal must be an AbortSignal"); }
  }
  const deadline = timeoutMs === undefined ? undefined : performance.now() + timeoutMs;
  const transactionOptions: { signal?: AbortSignal; timeoutMs?: number } = {};
  if (signal !== undefined) transactionOptions.signal = signal;
  if (timeoutMs !== undefined) transactionOptions.timeoutMs = timeoutMs;
  function checkpoint(): void {
    if (signal?.aborted) {
      throw new ChangesetApplyError("ERR_FSQLITE_CHANGESET_CANCELLED", "Changeset application was cancelled", undefined,
        { cause: signal.reason });
    }
    if (deadline !== undefined && performance.now() >= deadline) {
      throw new ChangesetApplyError("ERR_FSQLITE_CHANGESET_TIMEOUT", "Changeset application deadline expired");
    }
  }
  return { names, onConflict, limits, deliveryId, transactionOptions, checkpoint };
}

type Settings = ReturnType<typeof capture>;
interface TablePlan { wire: ChangesetTable; name: string; columns: readonly string[]; sqlName: string }

async function read(tx: ChangesetExecutor, settings: Settings, sql: string, params: readonly ChangesetValue[] = []) {
  settings.checkpoint();
  const result = await tx.query(sql, params);
  settings.checkpoint();
  if (!Array.isArray(result.rowArrays) || result.rowArrays.some(row => !Array.isArray(row))) return badResult();
  return result.rowArrays;
}

const RECEIPTS = `main.${quote(CHANGESET_RECEIPTS_TABLE)}`;
const RECEIPT_COLUMNS = ["delivery_id", "sha256", "byte_length", "applied", "omitted"] as const;
interface Delivery { id: string; sha256: string; byteLength: number; changes: number }
function receiptError(message: string): never {
  throw new ChangesetApplyError("ERR_FSQLITE_CHANGESET_RECEIPT", message);
}

async function fingerprint(bytes: Uint8Array, id: string, changes: number): Promise<Delivery> {
  if (globalThis.crypto?.subtle === undefined) invalid("Delivery receipts require Web Crypto SHA-256");
  // The codec has rejected shared/resizable/detached input. Copy synchronously
  // before hashing yields: caller mutation cannot bind a receipt to other bytes.
  const owned = new Uint8Array(bytes);
  const digest = new Uint8Array(await globalThis.crypto.subtle.digest("SHA-256", owned));
  const sha256 = Array.from(digest, value => value.toString(16).padStart(2, "0")).join("");
  return { id, sha256, byteLength: owned.byteLength, changes };
}

/** Validate local inbox authority instead of trusting CREATE IF NOT EXISTS. */
async function prepareReceipts(tx: ChangesetExecutor, settings: Settings, create: boolean): Promise<void> {
  let listed = await read(tx, settings, `PRAGMA main.table_list(${literal(CHANGESET_RECEIPTS_TABLE)})`);
  const matching = (rows: readonly (readonly unknown[])[]) => rows.filter(row => row[0] === "main" &&
    typeof row[1] === "string" && fold(row[1]) === CHANGESET_RECEIPTS_TABLE);
  if (matching(listed).length === 0 && create) {
    settings.checkpoint();
    await tx.execute(`CREATE TABLE ${RECEIPTS} (` +
      "delivery_id TEXT NOT NULL PRIMARY KEY COLLATE BINARY, sha256 TEXT NOT NULL, " +
      "byte_length INTEGER NOT NULL, applied INTEGER NOT NULL, omitted INTEGER NOT NULL)");
    settings.checkpoint();
    listed = await read(tx, settings, `PRAGMA main.table_list(${literal(CHANGESET_RECEIPTS_TABLE)})`);
  }
  const matches = matching(listed);
  if (matches.length !== 1 || matches[0]![2] !== "table" || integer(matches[0]![3]) !== RECEIPT_COLUMNS.length) {
    receiptError("The changeset inbox must be an ordinary table with the expected layout");
  }
  const info = await read(tx, settings, `PRAGMA main.table_xinfo(${literal(CHANGESET_RECEIPTS_TABLE)})`);
  if (info.length !== RECEIPT_COLUMNS.length) receiptError("Invalid changeset inbox columns");
  for (let i = 0; i < info.length; i++) {
    const row = info[i]!;
    if (integer(row[0]) !== i || row[1] !== RECEIPT_COLUMNS[i] || row[2] !== (i < 2 ? "TEXT" : "INTEGER") ||
        integer(row[3]) !== 1 || row[4] !== null || integer(row[5]) !== (i === 0 ? 1 : 0) || integer(row[6]) !== 0) {
      receiptError("Invalid changeset inbox column definitions");
    }
  }
  const indexes = await read(tx, settings, `PRAGMA main.index_list(${literal(CHANGESET_RECEIPTS_TABLE)})`);
  const primary = indexes.filter(row => row[3] === "pk");
  if (primary.length !== 1 || integer(primary[0]![2]) !== 1 || integer(primary[0]![4]) !== 0) {
    receiptError("The changeset inbox requires a unique complete delivery key");
  }
  const index = await read(tx, settings, `PRAGMA main.index_xinfo(${literal(identifier(primary[0]![1]))})`);
  const keys = index.filter(row => integer(row[5]) === 1);
  if (keys.length !== 1 || integer(keys[0]![1]) !== 0 || keys[0]![2] !== "delivery_id" || keys[0]![4] !== "BINARY") {
    receiptError("Changeset delivery identities require a BINARY primary key");
  }
  for (const namespace of ["main", "temp"]) {
    if ((await read(tx, settings, `SELECT name FROM ${namespace}.sqlite_schema ` +
        "WHERE type = 'trigger' AND tbl_name = ? COLLATE NOCASE LIMIT 1", [CHANGESET_RECEIPTS_TABLE])).length !== 0) {
      receiptError("Triggers on the changeset inbox are not supported");
    }
  }
  if ((await read(tx, settings, `PRAGMA main.foreign_key_list(${literal(CHANGESET_RECEIPTS_TABLE)})`)).length !== 0) {
    receiptError("Foreign keys on the changeset inbox are not supported");
  }
}

async function readReceipt(tx: ChangesetExecutor, settings: Settings, delivery: Delivery): Promise<ApplyChangesetResult | null> {
  const rows = await read(tx, settings, `SELECT sha256, byte_length, applied, omitted FROM ${RECEIPTS} ` +
    "WHERE delivery_id = ? COLLATE BINARY LIMIT 2", [delivery.id]);
  if (rows.length === 0) return null;
  if (rows.length !== 1 || rows[0]!.length !== 4) receiptError("Invalid changeset delivery receipt");
  const row = rows[0]!, hash = row[0], byteLength = integer(row[1]), applied = integer(row[2]), omitted = integer(row[3]);
  if (typeof hash !== "string" || !/^[0-9a-f]{64}$/.test(hash) || byteLength > 64 * 1024 * 1024 ||
      applied > 100_000 || omitted > 100_000 || applied + omitted > 100_000) {
    receiptError("Malformed changeset delivery receipt");
  }
  if (hash !== delivery.sha256 || byteLength !== delivery.byteLength) {
    throw new ChangesetApplyError("ERR_FSQLITE_CHANGESET_DELIVERY_REUSE", "The deliveryId already identifies different changeset bytes");
  }
  if (applied + omitted !== delivery.changes) receiptError("Changeset receipt counts do not match the retained payload");
  return Object.freeze({ applied, omitted, replayed: true });
}

async function planTable(tx: ChangesetExecutor, settings: Settings, wire: ChangesetTable): Promise<TablePlan> {
  const listed = await read(tx, settings, `PRAGMA main.table_list(${literal(wire.name)})`);
  const matches = listed.filter(row => row[0] === "main" && typeof row[1] === "string" && fold(row[1]) === fold(wire.name));
  if (matches.length !== 1 || matches[0]![2] !== "table") {
    schema(`Changesets require an ordinary main table: ${wire.name}`);
  }
  const metadata = matches[0]!, name = identifier(metadata[1]), count = integer(metadata[3]);
  if (count < wire.primaryKey.length || count > 32768) schema(`Incompatible column count: ${name}`);
  const info = await read(tx, settings, `PRAGMA main.table_xinfo(${literal(name)})`);
  if (info.length !== count) badResult();
  const columns: string[] = [], seen = new Set<string>();
  for (let i = 0; i < count; i++) {
    const row = info[i]!, column = identifier(row[1]);
    if (integer(row[0]) !== i || seen.has(fold(column))) badResult();
    if (integer(row[6]) !== 0) schema(`Generated and hidden columns are not supported: ${name}`);
    // SQLite changesets identify key positions; nonzero ordinal bytes are not
    // required to equal PRAGMA's ordinals (older producers use boolean bytes).
    if ((integer(row[5]) !== 0) !== (i < wire.primaryKey.length && wire.primaryKey[i] !== 0)) {
      schema(`Incompatible primary key: ${name}`);
    }
    seen.add(fold(column));
    if (i < wire.primaryKey.length) columns.push(column);
  }
  return { wire, name, columns: Object.freeze(columns), sqlName: `main.${quote(name)}` };
}

/** Balanced expressions avoid a linear-depth AND chain for wide DELETE images. */
function conjunction(parts: readonly string[], start = 0, end = parts.length): string {
  if (end === start) return "1";
  if (end - start === 1) return parts[start]!;
  const middle = start + Math.floor((end - start) / 2);
  return `(${conjunction(parts, start, middle)} AND ${conjunction(parts, middle, end)})`;
}

/** Preserve an integral REAL even if a JS/worker adapter binds numbers as integers. */
function parameter(value: ChangesetValue, params: ChangesetValue[]): string {
  params.push(value);
  // Unary + removes CAST's expression affinity without changing its REAL
  // value. Otherwise comparisons against TEXT columns would coerce the column
  // numerically, unlike a genuinely bound SQLite REAL parameter.
  return typeof value === "number" ? "+CAST(? AS REAL)" : "?";
}

function predicates(plan: TablePlan, change: ChangesetChange) {
  const record = change.operation === "insert" ? change.new : change.old;
  const keys: string[] = [], before: string[] = [], keyParams: ChangesetValue[] = [], beforeParams: ChangesetValue[] = [];
  for (let i = 0; i < plan.columns.length; i++) {
    const column = quote(plan.columns[i]!), value = record[i];
    if (plan.wire.primaryKey[i] !== 0) {
      keys.push(`${column} = ${parameter(value as ChangesetValue, keyParams)}`);
    } else if (change.operation !== "insert" && value !== undefined) {
      // Native session application uses the target column's affinity and
      // collation. IS preserves that behavior while also matching SQL NULL.
      before.push(`${column} IS ${parameter(value, beforeParams)}`);
    }
  }
  return { key: conjunction(keys), before: conjunction(before), keyParams, beforeParams };
}

function copyChange(change: ChangesetChange): ChangesetChange {
  const copy = <T extends ChangesetField>(row: readonly T[]): readonly T[] =>
    Object.freeze(row.map(value => value instanceof Uint8Array ? new Uint8Array(value) as T : value));
  if (change.operation === "insert") return Object.freeze({ ...change, new: copy(change.new) });
  if (change.operation === "delete") return Object.freeze({ ...change, old: copy(change.old) });
  return Object.freeze({ ...change, old: copy(change.old), new: copy(change.new) });
}

async function applyRow(tx: ChangesetExecutor, settings: Settings, plan: TablePlan,
  change: ChangesetChange, changeIndex: number): Promise<boolean> {
  const where = predicates(plan, change);
  const probe = await read(tx, settings,
    `SELECT CASE WHEN ${where.before} THEN 1 ELSE 0 END FROM ${plan.sqlName} WHERE ${where.key} LIMIT 2`,
    [...where.beforeParams, ...where.keyParams]);
  if (probe.length > 1 || (probe.length === 1 && (probe[0]!.length !== 1 || integer(probe[0]![0]) > 1))) badResult();
  const kind: ChangesetConflictKind | undefined = change.operation === "insert"
    ? probe.length === 1 ? "conflict" : undefined
    : probe.length === 0 ? "not-found" : integer(probe[0]![0]) === 0 ? "data" : undefined;
  if (kind !== undefined) {
    const conflict = Object.freeze({ kind, table: plan.name, changeIndex,
      columns: plan.columns, change: copyChange(change) });
    const action = settings.onConflict === undefined ? "abort" : await settings.onConflict(conflict);
    settings.checkpoint();
    if (action === "omit") return false;
    if (action !== "abort") invalid("onConflict must return abort or omit");
    throw new ChangesetApplyError("ERR_FSQLITE_CHANGESET_CONFLICT",
      `Changeset ${kind} conflict in ${plan.name} at change ${changeIndex}`, conflict);
  }
  const params: ChangesetValue[] = [];
  let sql: string;
  if (change.operation === "insert") {
    sql = `INSERT OR ABORT INTO ${plan.sqlName} (${plan.columns.map(quote).join(", ")}) VALUES (` +
      change.new.map(value => parameter(value, params)).join(", ") + ")";
  } else if (change.operation === "delete") {
    sql = `DELETE FROM ${plan.sqlName} WHERE ${where.key} AND ${where.before}`;
    params.push(...where.keyParams, ...where.beforeParams);
  } else {
    const assignments: string[] = [];
    for (let i = 0; i < change.new.length; i++) {
      const value = change.new[i];
      if (value !== undefined) assignments.push(`${quote(plan.columns[i]!)} = ${parameter(value, params)}`);
    }
    sql = `UPDATE OR ABORT ${plan.sqlName} SET ${assignments.join(", ")} WHERE ${where.key} AND ${where.before}`;
    params.push(...where.keyParams, ...where.beforeParams);
  }
  settings.checkpoint();
  const changed = await tx.execute(sql, params);
  settings.checkpoint();
  // A trigger using RAISE(IGNORE), stale predicate, or broken adapter must not
  // silently report a changeset entry applied. OR ABORT overrides schema-level
  // IGNORE/REPLACE policies for INSERT and UPDATE; SQL errors escape unchanged.
  if (changed !== 1) badResult();
  return true;
}

/**
 * Apply bounded SQLite session wire changes through real owned SQL. Schema and
 * explicit before-image conflicts fail closed by default. This is NOT the full
 * native sqlite3changeset_apply API: triggers/constraints retain ordinary SQL
 * behavior, and constraint omission, REPLACE, rebasing and FK deferral are not
 * synthesized. No manual BEGIN or global writer serialization is introduced.
 */
export async function applyChangeset(target: ChangesetTarget, bytes: Uint8Array,
  options: ApplyChangesetOptions): Promise<ApplyChangesetResult> {
  const settings = capture(options);
  settings.checkpoint();
  // Decode before the first await: caller mutation cannot change admitted work.
  const tables = decodeChangeset(bytes, settings.limits);
  for (const table of tables) {
    if (!settings.names.has(fold(table.name))) invalid(`Table is not authorized: ${table.name}`);
  }
  const delivery = settings.deliveryId === undefined ? null : await fingerprint(bytes, settings.deliveryId,
    tables.reduce((count, table) => count + table.changes.length, 0));
  settings.checkpoint();
  return target.transaction(async tx => {
    if (delivery !== null) {
      await prepareReceipts(tx, settings, true);
      const prior = await readReceipt(tx, settings, delivery);
      if (prior !== null) return prior;
    }
    const plans: TablePlan[] = [];
    // Validate ALL table layouts before executing the first application write.
    for (const table of tables) plans.push(await planTable(tx, settings, table));
    let applied = 0, omitted = 0, index = 0;
    for (const plan of plans) {
      for (const change of plan.wire.changes) {
        if (await applyRow(tx, settings, plan, change, index++)) applied++;
        else omitted++;
      }
    }
    if (delivery !== null) {
      // Application callbacks/triggers may have changed schema; verify again
      // before recording the decision. The insert is part of this SAME scope.
      await prepareReceipts(tx, settings, false);
      settings.checkpoint();
      const changed = await tx.execute(`INSERT OR ABORT INTO ${RECEIPTS} ` +
        "(delivery_id, sha256, byte_length, applied, omitted) VALUES (?, ?, ?, ?, ?)",
        [delivery.id, delivery.sha256, BigInt(delivery.byteLength), BigInt(applied), BigInt(omitted)]);
      settings.checkpoint();
      if (changed !== 1) receiptError("The changeset delivery receipt was not inserted");
      const saved = await readReceipt(tx, settings, delivery);
      if (saved === null || saved.applied !== applied || saved.omitted !== omitted) {
        receiptError("The changeset delivery receipt did not retain its decision");
      }
    }
    settings.checkpoint();
    return Object.freeze({ applied, omitted, replayed: false });
  }, settings.transactionOptions);
}
