import { withDeferredForeignKeys } from "./changeset-foreign-keys";
import { runChangesetScope } from "./changeset-scope";
import type { ChangesetScopeErrors } from "./changeset-scope";
import { FrankenSQLiteError } from "./errors";
import type {
  ChangesetChange,
  ChangesetLimits,
  ChangesetReader,
  ChangesetRebaseTable,
  ChangesetValue,
  PatchsetChange,
  PatchsetTable,
} from "./changeset-codec";
import {
  ChangesetError,
  prepareChangesetReader,
  preparePatchsetReader,
  encodeRebaseInfo,
  resolveChangesetLimits,
} from "./changeset-codec";

/** The ordinary SQL surface used inside one owned transaction/savepoint. */
export interface ChangesetExecutor {
  execute(sql: string, params?: readonly ChangesetValue[]): Promise<number>;
  query(
    sql: string,
    params?: readonly ChangesetValue[],
  ): Promise<{
    rowArrays: readonly (readonly unknown[])[];
  }>;
  /**
   * Optional owned child savepoint. Reject with the original callback error
   * only AFTER successful rollback; cleanup failures must remain distinguishable.
   * FrankenTransaction supplies this capability without manual boundary SQL.
   */
  transaction?<T>(work: (tx: ChangesetExecutor) => Promise<T>): Promise<T>;
}

/** Implemented by FrankenDB and FrankenTransaction; no WASM session hooks needed. */
export interface ChangesetTarget {
  transaction<T>(
    work: (tx: ChangesetExecutor) => Promise<T>,
    options?: {
      signal?: AbortSignal;
      timeoutMs?: number;
    },
  ): Promise<T>;
}

export type ChangesetConflictKind = "data" | "not-found" | "conflict" | "constraint";
export interface ChangesetConflict<Change extends PatchsetChange = ChangesetChange> {
  readonly kind: ChangesetConflictKind;
  readonly table: string;
  /** Zero-based position in the complete changeset, not just this table. */
  readonly changeIndex: number;
  readonly columns: readonly string[];
  /**
   * Receiver values for `columns` in this application's SQL snapshot, captured
   * before row-conflict policy runs. INTEGERs are bigint, REALs number, BLOBs
   * owned copies. The array is frozen; changing a BLOB cannot change pending
   * SQL or rebase evidence. Generated/trailing receiver-only fields are not
   * included. Null means NOTFOUND or CONSTRAINT: a SQL constraint can involve
   * several rows, so it has no single authoritative receiver image.
   */
  readonly current: readonly ChangesetValue[] | null;
  /** Owned before/after images; mutating a blob cannot change pending SQL. */
  readonly change: Change;
  /** Original SQL failure, present only for a rolled-back constraint conflict. */
  readonly error?: FrankenSQLiteError;
}

/** Patchset callbacks never receive DATA conflicts: there is no non-key old data. */
export type PatchsetConflict = ChangesetConflict<PatchsetChange>;

/** Save native rebase decisions using the SAME transaction as the applied rows. */
export type ChangesetRebaseHook = (
  tx: ChangesetExecutor,
  rebaseInfo: Uint8Array,
) => void | Promise<void>;

export interface ApplyChangesetOptions<Change extends PatchsetChange = ChangesetChange> {
  /** Opt in to one FK-clean deferred application scope; requires foreign_keys=ON. */
  foreignKeys?: "defer";
  /** Opt in: apply writable Session fields and let the trusted target schema recompute generated values. */
  generatedColumns?: "recompute";
  /**
   * Opt in to one owned child savepoint per row. A positively identified SQL
   * constraint failure reaches onConflict only after that child rolls back.
   * Requires the executor's transaction capability. Only abort/omit are valid;
   * cleanup, callback, transport and COMMIT failures never become omissions.
   * The default retains whole-scope abort and has no per-row savepoint cost.
   */
  constraintConflicts?: "isolate";
  /** Explicit allowlist of direct target tables in main; NOT a SQL sandbox. */
  tables: readonly string[];
  /** Stable source-qualified delivery identity; atomically recorded with the rows. */
  deliveryId?: string;
  /**
   * Default: abort. replace is valid only for data/primary-key conflicts, never
   * missing rows or constraints. SQL constraints abort unless explicitly
   * isolated above; all other SQL errors still abort the entire owned scope.
   */
  onConflict?: (
    conflict: ChangesetConflict<Change>,
  ) => "abort" | "omit" | "replace" | Promise<"abort" | "omit" | "replace">;
  /**
   * Fresh full changesets only, after row application but BEFORE commit. Save
   * these owned bytes with tx, never publish them externally from this hook.
   * Replay skips the hook: recover prior decisions from the journal you saved.
   * Admitted hook SQL drains before the receipt or rollback; saved executors
   * expire when the hook exits. Owned child savepoints are preserved when the
   * target supplies them: a handled, confirmed child rollback permits continued
   * hook work. Direct SQL failures abort even if caught; uncertain child cleanup
   * stops further hook SQL and retains its cleanup evidence across descendants.
   * Throws/cancellation/commit failure roll back journal, rows and receipt.
   */
  onRebase?: ChangesetRebaseHook;
  /**
   * Also bounds each conflicting receiver image before fetching it: 8 bytes
   * per scalar, BLOB length, twice encoded TEXT length. These are returned-data
   * bounds, not engine scratch/RSS or images retained by application callbacks.
   */
  limits?: ChangesetLimits;
  signal?: AbortSignal;
  timeoutMs?: number;
}

/** Explicitly choose the format without before-image conflict detection. */
export type ApplyPatchsetOptions = Omit<ApplyChangesetOptions<PatchsetChange>, "onRebase">;

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
  constructor(
    readonly code:
      | "ERR_FSQLITE_CHANGESET_INPUT"
      | "ERR_FSQLITE_CHANGESET_BUSY"
      | "ERR_FSQLITE_CHANGESET_STATE"
      | "ERR_FSQLITE_CHANGESET_SCHEMA"
      | "ERR_FSQLITE_CHANGESET_CONFLICT"
      | "ERR_FSQLITE_CHANGESET_RESULT"
      | "ERR_FSQLITE_CHANGESET_CANCELLED"
      | "ERR_FSQLITE_CHANGESET_TIMEOUT"
      | "ERR_FSQLITE_CHANGESET_DELIVERY_REUSE"
      | "ERR_FSQLITE_CHANGESET_RECEIPT",
    message: string,
    readonly conflict?: ChangesetConflict<PatchsetChange>,
    options?: ErrorOptions,
    /** Uncertain hook settlement requires reconciliation, not an inferred abort. */
    readonly cleanupErrors: readonly unknown[] = [],
  ) {
    super(message, options);
    this.name = "ChangesetApplyError";
  }
}

const quote = (name: string): string => `"${name.replaceAll('"', '""')}"`;
const literal = (name: string): string => `'${name.replaceAll("'", "''")}'`;
const fold = (name: string): string => name.replace(/[A-Z]/g, (c) => c.toLowerCase());
function invalid(message: string): never {
  throw new ChangesetApplyError("ERR_FSQLITE_CHANGESET_INPUT", message);
}
function badResult(): never {
  throw new ChangesetApplyError(
    "ERR_FSQLITE_CHANGESET_RESULT",
    "Invalid changeset SQL result; application was stopped",
  );
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
  if (typeof value === "bigint" && value >= 0n && value <= BigInt(Number.MAX_SAFE_INTEGER))
    return Number(value);
  if (typeof value === "number" && Number.isSafeInteger(value) && value >= 0) return value;
  return badResult();
}

function capture<Change extends PatchsetChange>(options: ApplyChangesetOptions<Change>) {
  const source = options?.tables,
    onConflict = options?.onConflict,
    limits = Object.freeze(resolveChangesetLimits(options?.limits));
  const onRebase = options?.onRebase;
  const signal = options?.signal,
    timeoutMs = options?.timeoutMs;
  const deliveryId = options?.deliveryId;
  const foreignKeys = options?.foreignKeys;
  if (foreignKeys !== undefined && foreignKeys !== "defer")
    invalid("foreignKeys must be defer when supplied");
  const generatedColumns = options?.generatedColumns;
  if (generatedColumns !== undefined && generatedColumns !== "recompute")
    invalid("generatedColumns must be recompute when supplied");
  const constraintConflicts = options?.constraintConflicts;
  if (constraintConflicts !== undefined && constraintConflicts !== "isolate")
    invalid("constraintConflicts must be isolate when supplied");
  if (!Array.isArray(source) || source.length > 256)
    invalid("tables must be an explicit allowlist of at most 256 names");
  const names = new Set<string>();
  for (let i = 0, n = source.length; i < n; i++) {
    const name = fold(identifier(source[i]));
    if (name.startsWith("sqlite_") || name === CHANGESET_RECEIPTS_TABLE || names.has(name)) {
      invalid(
        "Allowlist names must be distinct application tables, not the reserved changeset inbox",
      );
    }
    names.add(name);
  }
  if (onConflict !== undefined && typeof onConflict !== "function")
    invalid("onConflict must be a function");
  if (onRebase !== undefined && typeof onRebase !== "function")
    invalid("onRebase must be a function");
  if (deliveryId !== undefined) {
    if (
      typeof deliveryId !== "string" ||
      deliveryId.length === 0 ||
      deliveryId.length > 512 ||
      deliveryId.includes("\0")
    ) {
      invalid("deliveryId must be nonempty UTF-8 text of at most 512 bytes without NUL");
    }
    const encoded = new TextEncoder().encode(deliveryId);
    if (
      encoded.length > 512 ||
      new TextDecoder("utf-8", { ignoreBOM: true }).decode(encoded) !== deliveryId
    ) {
      invalid("deliveryId must be nonempty UTF-8 text of at most 512 bytes without NUL");
    }
  }
  if (
    timeoutMs !== undefined &&
    (!Number.isSafeInteger(timeoutMs) || timeoutMs < 1 || timeoutMs > 2_147_483_647)
  ) {
    invalid("timeoutMs must be an integer in 1..2147483647");
  }
  if (signal !== undefined) {
    try {
      Object.getOwnPropertyDescriptor(AbortSignal.prototype, "aborted")!.get!.call(signal);
    } catch {
      invalid("signal must be an AbortSignal");
    }
  }
  const deadline = timeoutMs === undefined ? undefined : performance.now() + timeoutMs;
  const transactionOptions: { signal?: AbortSignal; timeoutMs?: number } = {};
  if (signal !== undefined) transactionOptions.signal = signal;
  if (timeoutMs !== undefined) transactionOptions.timeoutMs = timeoutMs;
  function checkpoint(): void {
    if (signal?.aborted) {
      throw new ChangesetApplyError(
        "ERR_FSQLITE_CHANGESET_CANCELLED",
        "Changeset application was cancelled",
        undefined,
        { cause: signal.reason },
      );
    }
    if (deadline !== undefined && performance.now() >= deadline) {
      throw new ChangesetApplyError(
        "ERR_FSQLITE_CHANGESET_TIMEOUT",
        "Changeset application deadline expired",
      );
    }
  }
  const rebase = onRebase === undefined ? null : new Map<string, {
    name: string;
    primaryKey: readonly number[];
    changes: ChangesetRebaseTable["changes"][number][];
  }>();
  return { names, onConflict, onRebase, rebase, limits, deliveryId, generatedColumns, constraintConflicts, foreignKeys, transactionOptions, checkpoint };
}

type Settings<Change extends PatchsetChange> = ReturnType<typeof capture<Change>>;
type Checkpoints = Pick<Settings<PatchsetChange>, "checkpoint">;
interface TablePlan {
  wire: Omit<PatchsetTable, "changes">;
  name: string;
  columns: readonly string[];
  sqlName: string;
}

async function read(
  tx: ChangesetExecutor,
  settings: Checkpoints,
  sql: string,
  params: readonly ChangesetValue[] = [],
) {
  settings.checkpoint();
  const result = await tx.query(sql, params);
  settings.checkpoint();
  if (!Array.isArray(result.rowArrays) || result.rowArrays.some((row) => !Array.isArray(row)))
    return badResult();
  return result.rowArrays;
}

const RECEIPTS = `main.${quote(CHANGESET_RECEIPTS_TABLE)}`;
const RECEIPT_COLUMNS = ["delivery_id", "sha256", "byte_length", "applied", "omitted"] as const;
interface Delivery {
  id: string;
  sha256: string;
  byteLength: number;
  changes: number;
}
function receiptError(message: string): never {
  throw new ChangesetApplyError("ERR_FSQLITE_CHANGESET_RECEIPT", message);
}

async function fingerprint(bytes: Uint8Array, id: string, changes: number): Promise<Delivery> {
  if (globalThis.crypto?.subtle === undefined)
    invalid("Delivery receipts require Web Crypto SHA-256");
  // The codec has rejected shared/resizable/detached input. Copy synchronously
  // before hashing yields: caller mutation cannot bind a receipt to other bytes.
  const owned = new Uint8Array(bytes);
  const digest = new Uint8Array(await globalThis.crypto.subtle.digest("SHA-256", owned));
  const sha256 = Array.from(digest, (value) => value.toString(16).padStart(2, "0")).join("");
  return { id, sha256, byteLength: owned.byteLength, changes };
}

/** Validate local inbox authority instead of trusting CREATE IF NOT EXISTS. */
async function prepareReceipts(
  tx: ChangesetExecutor,
  settings: Checkpoints,
  create: boolean,
): Promise<void> {
  let listed = await read(
    tx,
    settings,
    `PRAGMA main.table_list(${literal(CHANGESET_RECEIPTS_TABLE)})`,
  );
  const matching = (rows: readonly (readonly unknown[])[]) =>
    rows.filter(
      (row) =>
        row[0] === "main" &&
        typeof row[1] === "string" &&
        fold(row[1]) === CHANGESET_RECEIPTS_TABLE,
    );
  if (matching(listed).length === 0 && create) {
    settings.checkpoint();
    await tx.execute(
      `CREATE TABLE ${RECEIPTS} (` +
        "delivery_id TEXT NOT NULL PRIMARY KEY COLLATE BINARY, sha256 TEXT NOT NULL, " +
        "byte_length INTEGER NOT NULL, applied INTEGER NOT NULL, omitted INTEGER NOT NULL)",
    );
    settings.checkpoint();
    listed = await read(
      tx,
      settings,
      `PRAGMA main.table_list(${literal(CHANGESET_RECEIPTS_TABLE)})`,
    );
  }
  const matches = matching(listed);
  if (
    matches.length !== 1 ||
    matches[0]![2] !== "table" ||
    integer(matches[0]![3]) !== RECEIPT_COLUMNS.length
  ) {
    receiptError("The changeset inbox must be an ordinary table with the expected layout");
  }
  const info = await read(
    tx,
    settings,
    `PRAGMA main.table_xinfo(${literal(CHANGESET_RECEIPTS_TABLE)})`,
  );
  if (info.length !== RECEIPT_COLUMNS.length) receiptError("Invalid changeset inbox columns");
  for (let i = 0; i < info.length; i++) {
    const row = info[i]!;
    if (
      integer(row[0]) !== i ||
      row[1] !== RECEIPT_COLUMNS[i] ||
      row[2] !== (i < 2 ? "TEXT" : "INTEGER") ||
      integer(row[3]) !== 1 ||
      row[4] !== null ||
      integer(row[5]) !== (i === 0 ? 1 : 0) ||
      integer(row[6]) !== 0
    ) {
      receiptError("Invalid changeset inbox column definitions");
    }
  }
  const indexes = await read(
    tx,
    settings,
    `PRAGMA main.index_list(${literal(CHANGESET_RECEIPTS_TABLE)})`,
  );
  const primary = indexes.filter((row) => row[3] === "pk");
  if (primary.length !== 1 || integer(primary[0]![2]) !== 1 || integer(primary[0]![4]) !== 0) {
    receiptError("The changeset inbox requires a unique complete delivery key");
  }
  const index = await read(
    tx,
    settings,
    `PRAGMA main.index_xinfo(${literal(identifier(primary[0]![1]))})`,
  );
  const keys = index.filter((row) => integer(row[5]) === 1);
  if (
    keys.length !== 1 ||
    integer(keys[0]![1]) !== 0 ||
    keys[0]![2] !== "delivery_id" ||
    keys[0]![4] !== "BINARY"
  ) {
    receiptError("Changeset delivery identities require a BINARY primary key");
  }
  for (const namespace of ["main", "temp"]) {
    if (
      (
        await read(
          tx,
          settings,
          `SELECT name FROM ${namespace}.sqlite_schema ` +
            "WHERE type = 'trigger' AND tbl_name = ? COLLATE NOCASE LIMIT 1",
          [CHANGESET_RECEIPTS_TABLE],
        )
      ).length !== 0
    ) {
      receiptError("Triggers on the changeset inbox are not supported");
    }
  }
  if (
    (await read(tx, settings, `PRAGMA main.foreign_key_list(${literal(CHANGESET_RECEIPTS_TABLE)})`))
      .length !== 0
  ) {
    receiptError("Foreign keys on the changeset inbox are not supported");
  }
}

async function readReceipt(
  tx: ChangesetExecutor,
  settings: Checkpoints,
  delivery: Delivery,
): Promise<ApplyChangesetResult | null> {
  const rows = await read(
    tx,
    settings,
    `SELECT sha256, byte_length, applied, omitted FROM ${RECEIPTS} ` +
      "WHERE delivery_id = ? COLLATE BINARY LIMIT 2",
    [delivery.id],
  );
  if (rows.length === 0) return null;
  if (rows.length !== 1 || rows[0]!.length !== 4)
    receiptError("Invalid changeset delivery receipt");
  const row = rows[0]!,
    hash = row[0],
    byteLength = integer(row[1]),
    applied = integer(row[2]),
    omitted = integer(row[3]);
  if (
    typeof hash !== "string" ||
    !/^[0-9a-f]{64}$/.test(hash) ||
    byteLength > 64 * 1024 * 1024 ||
    applied > 100_000 ||
    omitted > 100_000 ||
    applied + omitted > 100_000
  ) {
    receiptError("Malformed changeset delivery receipt");
  }
  if (hash !== delivery.sha256 || byteLength !== delivery.byteLength) {
    throw new ChangesetApplyError(
      "ERR_FSQLITE_CHANGESET_DELIVERY_REUSE",
      "The deliveryId already identifies different changeset bytes",
    );
  }
  if (applied + omitted !== delivery.changes)
    receiptError("Changeset receipt counts do not match the retained payload");
  return Object.freeze({ applied, omitted, replayed: true });
}

async function planTable<Change extends PatchsetChange>(
  tx: ChangesetExecutor,
  settings: Checkpoints & Pick<Settings<Change>, "generatedColumns">,
  wire: TablePlan["wire"],
): Promise<TablePlan> {
  const listed = await read(tx, settings, `PRAGMA main.table_list(${literal(wire.name)})`);
  const matches = listed.filter(
    (row) => row[0] === "main" && typeof row[1] === "string" && fold(row[1]) === fold(wire.name),
  );
  if (matches.length !== 1 || matches[0]![2] !== "table") {
    schema(`Changesets require an ordinary main table: ${wire.name}`);
  }
  const metadata = matches[0]!,
    name = identifier(metadata[1]),
    count = integer(metadata[3]);
  if (count < wire.primaryKey.length || count > 32768) schema(`Incompatible column count: ${name}`);
  const info = await read(tx, settings, `PRAGMA main.table_xinfo(${literal(name)})`);
  if (info.length !== count) badResult();
  const columns: string[] = [],
    seen = new Set<string>();
  let writable = 0;
  for (let i = 0; i < count; i++) {
    const row = info[i]!,
      column = identifier(row[1]);
    if (integer(row[0]) !== i || seen.has(fold(column))) badResult();
    seen.add(fold(column));
    const hidden = integer(row[6]);
    if (hidden === 2 || hidden === 3) {
      if (settings.generatedColumns !== "recompute") schema(`Generated and hidden columns require generatedColumns: recompute: ${name}`);
      if (integer(row[5]) !== 0) schema(`Generated primary keys are not supported: ${name}`);
      continue;
    }
    if (hidden !== 0) schema(`Hidden columns are not supported: ${name}`);
    // SQLite changesets identify key positions; nonzero ordinal bytes are not
    // required to equal PRAGMA's ordinals (older producers use boolean bytes).
    if ((integer(row[5]) !== 0) !== (writable < wire.primaryKey.length && wire.primaryKey[writable] !== 0)) {
      schema(`Incompatible primary key: ${name}`);
    }
    if (writable < wire.primaryKey.length) columns.push(column);
    writable++;
  }
  if (writable < wire.primaryKey.length) schema(`Incompatible writable column count: ${name}`);
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

function predicates(plan: TablePlan, change: PatchsetChange) {
  const record = change.operation === "insert" ? change.new : change.old;
  const keys: string[] = [],
    before: string[] = [],
    keyParams: ChangesetValue[] = [],
    beforeParams: ChangesetValue[] = [];
  for (let i = 0; i < plan.columns.length; i++) {
    const column = quote(plan.columns[i]!),
      value = record[i];
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

function copyChange<Change extends PatchsetChange>(change: Change): Change {
  // Only decoder-owned plain records reach this function. Clone preserves the
  // exact format's field types while isolating policy callbacks from its blobs.
  const copy = structuredClone(change);
  if (copy.operation !== "insert") Object.freeze(copy.old);
  if (copy.operation !== "delete") Object.freeze(copy.new);
  return Object.freeze(copy);
}

function conflictLimit(): never {
  throw new ChangesetError(
    "ERR_FSQLITE_CHANGESET_LIMIT",
    "Conflicting receiver row exceeds the changeset image budget",
  );
}

function conflictCost(columns: readonly string[], start = 0, end = columns.length): string {
  if (end - start === 1) {
    const column = quote(columns[start]!);
    // Match capture accounting, including UTF-8 expansion from UTF-16 text.
    return `(CASE typeof(${column}) WHEN 'text' THEN 2 * length(CAST(${column} AS BLOB)) ` +
      `WHEN 'blob' THEN length(${column}) ELSE 8 END)`;
  }
  const middle = start + Math.floor((end - start) / 2);
  return `(${conflictCost(columns, start, middle)} + ${conflictCost(columns, middle, end)})`;
}

/** SQL adapters must not substitute shadowed metadata or racing buffers. */
function conflictBytes(value: unknown): Uint8Array {
  if (!(value instanceof Uint8Array)) return badResult();
  const prototype = Object.getPrototypeOf(Uint8Array.prototype) as object;
  try {
    const get = (name: string): unknown =>
      Object.getOwnPropertyDescriptor(prototype, name)!.get!.call(value);
    const buffer = get("buffer"), offset = get("byteOffset") as number,
      size = get("byteLength") as number;
    if (!(buffer instanceof ArrayBuffer) ||
        Object.getOwnPropertyDescriptor(ArrayBuffer.prototype, "resizable")?.get?.call(buffer))
      return badResult();
    return new Uint8Array(buffer, offset, size);
  } catch { return badResult(); }
}

/**
 * Only conflicting rows pay for capture. No external connection/read, new
 * transaction, or application callback separates the cost and value reads.
 * The target must retain the same owned SQL snapshot throughout this operation.
 */
async function readConflictRow(
  tx: ChangesetExecutor,
  settings: Checkpoints & Pick<Settings<PatchsetChange>, "limits">,
  plan: TablePlan,
  where: ReturnType<typeof predicates>,
): Promise<readonly ChangesetValue[]> {
  if (plan.columns.length === 0) return badResult();
  if (plan.columns.length > settings.limits.maxCells) return conflictLimit();
  const suffix = ` FROM ${plan.sqlName} WHERE ${where.key} LIMIT 2`;
  const costs = await read(tx, settings,
    `SELECT ${conflictCost(plan.columns)} AS __fsqlite_conflict_cost${suffix}`, where.keyParams);
  if (costs.length !== 1 || costs[0]!.length !== 1) return badResult();
  const expectedCost = integer(costs[0]![0]);
  if (expectedCost > settings.limits.maxBytes) return conflictLimit();

  const encoding = await read(tx, settings, "PRAGMA main.encoding");
  const label = encoding[0]?.[0];
  if (encoding.length !== 1 || encoding[0]!.length !== 1 ||
      (label !== "UTF-8" && label !== "UTF-16le" && label !== "UTF-16be")) return badResult();
  const text = new TextDecoder(label, { fatal: true, ignoreBOM: true });
  const current: ChangesetValue[] = [];
  let cost = 0;
  // Two result fields per column. Chunking keeps wide tables below SQLite's
  // result-column limit without dropping fields or weakening their validation.
  for (let start = 0; start < plan.columns.length; start += 128) {
    const columns = plan.columns.slice(start, start + 128);
    const projection = columns.flatMap((name, index) => {
      const column = quote(name);
      return [
        `typeof(${column}) AS t${index}`,
        `CASE typeof(${column}) WHEN 'integer' THEN CAST(${column} AS TEXT) ` +
          `WHEN 'text' THEN CAST(${column} AS BLOB) ELSE ${column} END AS x${index}`,
      ];
    }).join(", ");
    const rows = await read(tx, settings, `SELECT ${projection}${suffix}`, where.keyParams);
    if (rows.length !== 1 || rows[0]!.length !== columns.length * 2) return badResult();
    const row = rows[0]!;
    for (let index = 0; index < columns.length; index++) {
      const tag = row[index * 2], value = row[index * 2 + 1];
      let captured: ChangesetValue;
      if (tag === "null" && value === null) { captured = null; cost += 8; }
      else if (tag === "integer" && typeof value === "string" &&
          /^(?:0|-?[1-9][0-9]{0,18})$/.test(value)) {
        const exact = BigInt(value);
        if (exact < -(1n << 63n) || exact >= (1n << 63n)) return badResult();
        captured = exact; cost += 8;
      } else if (tag === "real" && typeof value === "number" && !Number.isNaN(value)) {
        captured = value; cost += 8;
      } else if (tag === "text" || tag === "blob") {
        const bytes = conflictBytes(value);
        cost += bytes.length * (tag === "text" ? 2 : 1);
        // Check before cloning/decoding, including broken adapters returning
        // more bytes than preflight admitted. No partial image escapes.
        if (cost > expectedCost) return badResult();
        if (tag === "blob") captured = new Uint8Array(bytes);
        else {
          try { captured = text.decode(bytes); }
          catch { return badResult(); }
        }
      } else return badResult();
      if (cost > expectedCost) return badResult();
      current.push(captured);
    }
  }
  if (cost !== expectedCost) return badResult();
  return Object.freeze(current);
}

// No message matching, arbitrary cause traversal or "transient" heuristics.
// These ordinary SQL constraint families may be omitted only when the exact
// execute failure is returned unchanged by a successfully rolled-back child.
const ROW_CONSTRAINT_CODES: Readonly<Record<string, number>> = Object.freeze({
  SQLITE_CONSTRAINT: 19,
  SQLITE_CONSTRAINT_CHECK: 275,
  SQLITE_CONSTRAINT_FOREIGNKEY: 787,
  SQLITE_CONSTRAINT_NOTNULL: 1299,
  SQLITE_CONSTRAINT_PRIMARYKEY: 1555,
  SQLITE_CONSTRAINT_TRIGGER: 1811,
  SQLITE_CONSTRAINT_UNIQUE: 2067,
  SQLITE_CONSTRAINT_ROWID: 2579,
  SQLITE_CONSTRAINT_DATATYPE: 3091,
});

function isRowConstraint(error: unknown): error is FrankenSQLiteError {
  try {
    if (!(error instanceof FrankenSQLiteError) || error.cleanupErrors.length !== 0 ||
        error.userRecoverable === false || error.cause !== undefined ||
        !Object.hasOwn(ROW_CONSTRAINT_CODES, error.code)) return false;
    const code = ROW_CONSTRAINT_CODES[error.code]!;
    if (error.sqliteCode !== undefined && error.sqliteCode !== 19) return false;
    return error.extendedCode === undefined || (code === 19
      ? Object.values(ROW_CONSTRAINT_CODES).includes(error.extendedCode)
      : error.extendedCode === code);
  } catch { return false; } // Caller-created Error accessors cannot authorize omission.
}

type RowScope = (work: (tx: ChangesetExecutor) => Promise<boolean>) => Promise<boolean>;

function rowScope(tx: ChangesetExecutor): RowScope {
  const nested = tx.transaction;
  if (typeof nested !== "function")
    invalid("constraintConflicts: isolate requires owned child transactions");
  return work => nested.call(tx, work) as Promise<boolean>;
}

async function applyIsolatedRow<Change extends PatchsetChange>(
  isolate: RowScope,
  settings: Settings<Change>,
  plan: TablePlan,
  change: Change,
  changeIndex: number,
): Promise<boolean> {
  let sqlFailure: FrankenSQLiteError | undefined;
  try {
    return await isolate(child => applyRow({
      query: (sql, params) => child.query(sql, params),
      execute: async (sql, params) => {
        try { return await child.execute(sql, params); }
        catch (error: unknown) {
          if (isRowConstraint(error)) sqlFailure = error;
          throw error;
        }
      },
    }, settings, plan, change, changeIndex));
  } catch (error: unknown) {
    // Policy callbacks and child BEGIN/RELEASE errors did not come from our
    // execute. A cleanup aggregate/wrapper is not the original SQL failure.
    // Do not suppress it, even if it contains that failure as a cause.
    if (sqlFailure === undefined || error !== sqlFailure || !isRowConstraint(error)) throw error;
    settings.checkpoint();
    const conflict: ChangesetConflict<Change> = Object.freeze({
      kind: "constraint", table: plan.name, changeIndex, columns: plan.columns,
      change: copyChange(change), current: null, error: sqlFailure,
    });
    const action = settings.onConflict === undefined ? "abort" : await settings.onConflict(conflict);
    settings.checkpoint();
    if (action === "omit") {
      // Rebase describes policy decisions, not just surviving SQL. Like native
      // apply_v2, retain any preceding DATA/PK replacement decision, then OMIT.
      recordRebase(settings, plan, change, false);
      return false;
    }
    if (action !== "abort") invalid("constraint conflicts allow only abort or omit");
    throw new ChangesetApplyError(
      "ERR_FSQLITE_CHANGESET_CONFLICT",
      `Changeset constraint conflict in ${plan.name} at change ${changeIndex}`,
      conflict, { cause: sqlFailure },
    );
  }
}

async function applyRow<Change extends PatchsetChange>(
  tx: ChangesetExecutor,
  settings: Settings<Change>,
  plan: TablePlan,
  change: Change,
  changeIndex: number,
): Promise<boolean> {
  const where = predicates(plan, change);
  const probe = await read(
    tx,
    settings,
    `SELECT CASE WHEN ${where.before} THEN 1 ELSE 0 END FROM ${plan.sqlName} WHERE ${where.key} LIMIT 2`,
    [...where.beforeParams, ...where.keyParams],
  );
  if (
    probe.length > 1 ||
    (probe.length === 1 &&
      (probe[0]!.length !== 1 || integer(probe[0]![0]) > 1 ||
        (where.before === "1" && integer(probe[0]![0]) !== 1)))
  )
    badResult();
  const kind: ChangesetConflictKind | undefined =
    change.operation === "insert"
      ? probe.length === 1
        ? "conflict"
        : undefined
      : probe.length === 0
        ? "not-found"
        : integer(probe[0]![0]) === 0
          ? "data"
          : undefined;
  let replace = false;
  if (kind !== undefined) {
    const conflict = Object.freeze({
      kind,
      table: plan.name,
      changeIndex,
      columns: plan.columns,
      current: kind === "not-found" ? null : await readConflictRow(tx, settings, plan, where),
      change: copyChange(change),
    });
    const action =
      settings.onConflict === undefined ? "abort" : await settings.onConflict(conflict);
    settings.checkpoint();
    if (action === "omit") {
      recordRebase(settings, plan, change, false);
      return false;
    }
    if (action === "abort") {
      throw new ChangesetApplyError(
        "ERR_FSQLITE_CHANGESET_CONFLICT",
        `Changeset ${kind} conflict in ${plan.name} at change ${changeIndex}`,
        conflict,
      );
    }
    if (action !== "replace") invalid("onConflict must return abort, omit or replace");
    if (kind === "not-found") invalid("replace requires an existing conflicting row");
    recordRebase(settings, plan, change, true);
    replace = true;
  }
  if (replace && change.operation === "insert") {
    // Session REPLACE removes ONLY the row with the conflicting primary key,
    // then retries the ordinary INSERT. INSERT OR REPLACE could also delete
    // unrelated rows that collide on another UNIQUE index. Never do that.
    // This deletion, its trigger/FK effects, the insert and the inbox receipt
    // share the enclosing owned transaction: every later error rolls them back.
    settings.checkpoint();
    const removed = await tx.execute(
      `DELETE FROM ${plan.sqlName} WHERE ${where.key}`,
      where.keyParams,
    );
    settings.checkpoint();
    if (removed !== 1) badResult();
  }
  // DATA replacement overrides only the expected before-image, not the key,
  // unchanged UPDATE columns, constraint enforcement, or affected-row checks.
  const condition = replace ? where.key : `${where.key} AND ${where.before}`;
  const conditionParams = replace
    ? where.keyParams
    : [...where.keyParams, ...where.beforeParams];
  const params: ChangesetValue[] = [];
  let sql: string;
  if (change.operation === "insert") {
    sql =
      `INSERT OR ABORT INTO ${plan.sqlName} (${plan.columns.map(quote).join(", ")}) VALUES (` +
      change.new.map((value) => parameter(value, params)).join(", ") +
      ")";
  } else if (change.operation === "delete") {
    sql = `DELETE FROM ${plan.sqlName} WHERE ${condition}`;
    params.push(...conditionParams);
  } else {
    const assignments: string[] = [];
    for (let i = 0; i < change.new.length; i++) {
      const value = change.new[i];
      if (value !== undefined)
        assignments.push(`${quote(plan.columns[i]!)} = ${parameter(value, params)}`);
    }
    sql = `UPDATE OR ABORT ${plan.sqlName} SET ${assignments.join(", ")} WHERE ${condition}`;
    params.push(...conditionParams);
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

function recordRebase<Change extends PatchsetChange>(
  settings: Settings<Change>,
  plan: TablePlan,
  change: Change,
  replace: boolean,
): void {
  if (settings.rebase === null) return;
  const wire = plan.wire;
  let table = settings.rebase.get(wire.name);
  if (table === undefined) {
    table = { name: wire.name, primaryKey: wire.primaryKey, changes: [] };
    settings.rebase.set(wire.name, table);
  }
  // Decoder-owned values are isolated from both caller input and onConflict's
  // copies. UPDATE becomes a partial INSERT-shaped native rebase record.
  table.changes.push({
    operation: change.operation === "delete" ? "delete" : "insert",
    replace,
    values: change.operation === "insert" ? change.new
      : change.operation === "delete" ? change.old
      : change.new.map((value, i) => wire.primaryKey[i] !== 0 ? change.old[i] : value),
  });
}

const rebaseScopeErrors: ChangesetScopeErrors = Object.freeze({
  closed: () => new ChangesetApplyError(
    "ERR_FSQLITE_CHANGESET_INPUT", "Changeset rebase hook SQL scope has ended"),
  invalidChild: () => new ChangesetApplyError(
    "ERR_FSQLITE_CHANGESET_INPUT", "A child transaction callback is required"),
  busy: () => new ChangesetApplyError(
    "ERR_FSQLITE_CHANGESET_BUSY", "Settle parent work first; a child transaction owns this SQL scope"),
  uncertain: (cause: unknown, cleanupErrors: readonly unknown[]) => new ChangesetApplyError(
    "ERR_FSQLITE_CHANGESET_STATE", "Rebase hook child cleanup is uncertain; reconcile the transaction owner",
    undefined, { cause }, cleanupErrors),
});

/** Share capture/FK ownership through the hook, before receipt publication. */
async function runRebaseHook(
  tx: ChangesetExecutor,
  hook: ChangesetRebaseHook,
  bytes: Uint8Array,
  checkpoint: () => void,
): Promise<void> {
  await runChangesetScope(tx, scoped => hook(scoped, bytes), checkpoint, rebaseScopeErrors);
}

/**
 * Apply bounded SQLite session wire changes through real owned SQL. Schema and
 * explicit before-image conflicts fail closed by default. This is NOT the full
 * native sqlite3changeset_apply API: triggers/constraints retain ordinary SQL
 * behavior. Opt-in constraint isolation rolls back a complete row operation
 * before policy can omit it; it never retries or hides uncertain cleanup.
 * Explicit foreignKeys: defer
 * composes the existing FK-clean scope around rows, hook and receipt. onRebase
 * records actual full-changeset decisions inside the owned transaction.
 * Explicit data/primary-key replacement stays inside the owned
 * scope. No manual BEGIN or global writer serialization is introduced.
 */
export function applyChangeset(
  target: ChangesetTarget,
  bytes: Uint8Array,
  options: ApplyChangesetOptions,
): Promise<ApplyChangesetResult> {
  return applySession(target, bytes, options, prepareChangesetReader);
}

/**
 * Apply a compact SQLite patchset through the same owned SQL/receipt path.
 * UPDATE/DELETE match keys without before-images. Missing rows and duplicate
 * keys still invoke policy; constraint isolation is explicitly opt-in. No DATA conflict can be
 * inferred, and a patchset cannot be inverted into an undo changeset.
 */
export function applyPatchset(
  target: ChangesetTarget,
  bytes: Uint8Array,
  options: ApplyPatchsetOptions,
): Promise<ApplyChangesetResult> {
  return applySession(target, bytes, options, preparePatchsetReader);
}

async function applySession<Change extends PatchsetChange>(
  target: ChangesetTarget,
  bytes: Uint8Array,
  options: ApplyChangesetOptions<Change>,
  prepare: (bytes: Uint8Array, limits?: ChangesetLimits) => ChangesetReader<Change>,
): Promise<ApplyChangesetResult> {
  const settings = capture(options);
  if (settings.onRebase !== undefined && prepare !== prepareChangesetReader)
    invalid("Patchsets do not provide changeset rebase evidence");
  settings.checkpoint();
  // Validate the complete input before SQL, but do not retain every decoded
  // row for the transaction's lifetime. The reader owns a private wire copy;
  // later callbacks cannot change rows that have not been decoded for SQL yet.
  const reader = prepare(bytes, settings.limits);
  for (const table of reader.tables) {
    if (!settings.names.has(fold(table.name))) invalid(`Table is not authorized: ${table.name}`);
  }
  const delivery =
    settings.deliveryId === undefined
      ? null
      : await fingerprint(
          bytes,
          settings.deliveryId,
          reader.changeCount,
        );
  settings.checkpoint();
  const owner = settings.foreignKeys === "defer" ? withDeferredForeignKeys(target) : target;
  return owner.transaction(async (tx) => {
    // Resolve capability before inbox/schema/application SQL. Default callers
    // do not read this optional property or pay for child savepoints.
    const isolate = settings.constraintConflicts === "isolate" ? rowScope(tx) : undefined;
    if (delivery !== null) {
      await prepareReceipts(tx, settings, true);
      const prior = await readReceipt(tx, settings, delivery);
      if (prior !== null) return prior;
    }
    const plans: TablePlan[] = [];
    // Validate ALL table layouts before executing the first application write.
    for (const table of reader.tables) plans.push(await planTable(tx, settings, table));
    let applied = 0,
      omitted = 0;
    // Replay receipts return above without opening a second decoding cursor.
    // Abrupt completion closes this cursor while the owned target rolls back.
    // Requested rebase evidence still retains its selected conflict records.
    for (const { tableIndex, changeIndex, change } of reader) {
      const plan = plans[tableIndex]!;
      const changed = isolate === undefined
        ? await applyRow(tx, settings, plan, change, changeIndex)
        : await applyIsolatedRow(isolate, settings, plan, change, changeIndex);
      if (changed) applied++;
      else omitted++;
    }
    if (settings.onRebase !== undefined && settings.rebase !== null) {
      settings.checkpoint();
      const info = encodeRebaseInfo([...settings.rebase.values()], settings.limits);
      settings.checkpoint();
      await runRebaseHook(tx, settings.onRebase, info, settings.checkpoint);
      settings.checkpoint();
    }
    if (delivery !== null) {
      // Application callbacks/triggers may have changed schema; verify again
      // before recording the decision. The insert is part of this SAME scope.
      await prepareReceipts(tx, settings, false);
      settings.checkpoint();
      const changed = await tx.execute(
        `INSERT OR ABORT INTO ${RECEIPTS} ` +
          "(delivery_id, sha256, byte_length, applied, omitted) VALUES (?, ?, ?, ?, ?)",
        [
          delivery.id,
          delivery.sha256,
          BigInt(delivery.byteLength),
          BigInt(applied),
          BigInt(omitted),
        ],
      );
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