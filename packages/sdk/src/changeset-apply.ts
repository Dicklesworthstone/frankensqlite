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
}

export class ChangesetApplyError extends Error {
  constructor(readonly code: "ERR_FSQLITE_CHANGESET_INPUT" | "ERR_FSQLITE_CHANGESET_SCHEMA" |
    "ERR_FSQLITE_CHANGESET_CONFLICT" | "ERR_FSQLITE_CHANGESET_RESULT" |
    "ERR_FSQLITE_CHANGESET_CANCELLED" | "ERR_FSQLITE_CHANGESET_TIMEOUT",
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
  if (!Array.isArray(source) || source.length > 256) invalid("tables must be an explicit allowlist of at most 256 names");
  const names = new Set<string>();
  for (let i = 0, n = source.length; i < n; i++) {
    const name = fold(identifier(source[i]));
    if (name.startsWith("sqlite_") || names.has(name)) invalid("Allowlist names must be distinct application tables");
    names.add(name);
  }
  if (onConflict !== undefined && typeof onConflict !== "function") invalid("onConflict must be a function");
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
  return { names, onConflict, limits, transactionOptions, checkpoint };
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
  settings.checkpoint();
  return target.transaction(async tx => {
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
    settings.checkpoint();
    return Object.freeze({ applied, omitted });
  }, settings.transactionOptions);
}
