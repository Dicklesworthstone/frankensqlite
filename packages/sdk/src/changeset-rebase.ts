import type {
  ChangesetChange,
  ChangesetField,
  ChangesetLimits,
  ChangesetRebaseTable,
  ChangesetTable,
  ChangesetValue,
} from "./changeset-codec";
import {
  ChangesetError,
  decodeChangeset,
  decodeRebaseInfo,
  encodeChangeset,
  encodeRebaseInfo,
  resolveChangesetLimits,
} from "./changeset-codec";

export interface ChangesetRebaseResolution {
  /** Global zero-based index in the remote changeset, not the conflict count. */
  readonly changeIndex: number;
  readonly resolution: "omit" | "replace";
}
export class ChangesetRebaseError extends Error {
  constructor(
    readonly code: "ERR_FSQLITE_REBASE_INPUT" | "ERR_FSQLITE_REBASE_SCHEMA",
    message: string,
  ) {
    super(message);
    this.name = "ChangesetRebaseError";
  }
}
export interface ChangesetRebaseStats {
  readonly tables: number;
  /** Distinct conflicting keys, not the number of configure() calls. */
  readonly changes: number;
  /** Retained value and replacement-mask slots. */
  readonly cells: number;
  /** Accounted wire values, schema, masks and UTF-16 key bytes; not heap/RSS. */
  readonly byteLength: number;
}

/**
 * Capture the actual decisions made by a successful remote application as
 * SQLite apply_v2-compatible rebase information. No SQL is executed here.
 * Only conflicts belong in resolutions. Never infer decisions from two inputs,
 * include an aborted application, or treat an uncommitted child as durable.
 * Native sqlite3changeset_apply_v2 output can be configured directly instead.
 */
export function createChangesetRebaseInfo(
  remote: Uint8Array,
  resolutions: readonly ChangesetRebaseResolution[],
  options?: ChangesetLimits,
): Uint8Array {
  const policy = resolveChangesetLimits(options);
  const tables = decodeChangeset(remote, policy);
  if (!Array.isArray(resolutions) || resolutions.length > policy.maxChanges)
    throw new ChangesetRebaseError("ERR_FSQLITE_REBASE_INPUT", "Invalid rebase decisions");
  const decisions = new Map<number, boolean>();
  for (let i = 0, n = resolutions.length; i < n; i++) {
    const decision = resolutions[i];
    const index = decision?.changeIndex;
    const action = decision?.resolution;
    if (
      !Number.isSafeInteger(index) || index! < 0 || decisions.has(index!) ||
      (action !== "omit" && action !== "replace")
    ) throw new ChangesetRebaseError("ERR_FSQLITE_REBASE_INPUT", "Invalid or repeated rebase decision");
    decisions.set(index!, action === "replace");
  }
  let index = 0;
  const result: ChangesetRebaseTable[] = [];
  for (const table of tables) {
    const changes: ChangesetRebaseTable["changes"][number][] = [];
    for (const change of table.changes) {
      const replace = decisions.get(index);
      decisions.delete(index++);
      if (replace === undefined) continue;
      changes.push({
        operation: change.operation === "delete" ? "delete" : "insert",
        replace,
        values: change.operation === "insert" ? change.new
          : change.operation === "delete" ? change.old
          : change.new.map((value, i) => table.primaryKey[i] !== 0 ? change.old[i] : value),
      });
    }
    if (changes.length) result.push({ name: table.name, primaryKey: table.primaryKey, changes });
  }
  if (decisions.size)
    throw new ChangesetRebaseError("ERR_FSQLITE_REBASE_INPUT", "Rebase decision index is outside the changeset");
  return encodeRebaseInfo(result, policy);
}

interface RebaseRow {
  readonly operation: "insert" | "delete";
  readonly replace: boolean;
  readonly values: readonly ChangesetField[];
  readonly replaced: readonly boolean[];
}
interface RebaseTable {
  readonly name: string;
  readonly primaryKey: readonly number[];
  readonly rows: Map<string, RebaseRow>;
}
const fold = (name: string): string => name.replace(/[A-Z]/g, (c) => c.toLowerCase());
const emptyStats = (): ChangesetRebaseStats =>
  Object.freeze({ tables: 0, changes: 0, cells: 0, byteLength: 0 });

// Session keys match storage classes and exact bytes, not SQL affinity or a
// target collation. Length delimiters prevent composite/string/blob ambiguity.
function key(pk: readonly number[], values: readonly ChangesetField[]): string {
  const parts: string[] = [];
  for (let i = 0; i < pk.length; i++) {
    if (pk[i] === 0) continue;
    const value = values[i];
    if (typeof value === "bigint") parts.push(`i${value};`);
    else if (typeof value === "number") {
      const bits = new DataView(new ArrayBuffer(8));
      bits.setFloat64(0, value);
      parts.push(`r${bits.getBigUint64(0).toString(16).padStart(16, "0")}`);
    } else if (typeof value === "string") parts.push(`t${value.length}:${value}`);
    else if (value instanceof Uint8Array) {
      parts.push(`b${value.length}:`);
      for (let start = 0; start < value.length; start += 4096)
        parts.push(String.fromCharCode(...value.subarray(start, start + 4096)));
    } else throw new ChangesetError("ERR_FSQLITE_CHANGESET_FORMAT", "Missing rebase primary key");
  }
  return parts.join("");
}
function assertSchema(pk: readonly number[], other: readonly number[], name: string): void {
  if (pk.length !== other.length || pk.some((value, i) => value !== other[i]))
    throw new ChangesetRebaseError("ERR_FSQLITE_REBASE_SCHEMA", `Rebase schema mismatch for ${name}`);
}
function varintBytes(n: number): number {
  let bytes = 1;
  for (; n >= 128; n = Math.floor(n / 128)) bytes++;
  return bytes;
}
const utf8 = new TextEncoder();
function rowBytes(row: RebaseRow, rowKey: string): number {
  let bytes = 2 + row.replaced.length + rowKey.length * 2;
  for (const value of row.values) {
    bytes++;
    if (typeof value === "bigint" || typeof value === "number") bytes += 8;
    else if (typeof value === "string" || value instanceof Uint8Array) {
      const n = typeof value === "string" ? utf8.encode(value).length : value.length;
      bytes += varintBytes(n) + n;
    }
  }
  return bytes;
}
function merge(
  pk: readonly number[],
  previous: RebaseRow | undefined,
  incoming: ChangesetRebaseTable["changes"][number],
): RebaseRow {
  const values: ChangesetField[] = [];
  const replaced: boolean[] = [];
  for (let i = 0; i < pk.length; i++) {
    const next = incoming.values[i];
    const overwrite = pk[i] === 0 &&
      (previous?.replaced[i] === true || (incoming.replace && next !== undefined));
    replaced.push(overwrite);
    // REPLACE is a persistent per-field tombstone. A later OMIT must not
    // restore either its before-image or a now-overridden local modification.
    values.push(overwrite ? undefined : next === undefined ? previous?.values[i] : next);
  }
  return {
    operation: incoming.operation,
    replace: incoming.replace || previous?.replace === true,
    values,
    replaced,
  };
}
function rebaseRow(
  pk: readonly number[],
  local: ChangesetChange,
  remote: RebaseRow,
): ChangesetChange | null {
  if (local.operation === "insert") {
    if (remote.operation === "delete") return local;
    if (remote.replace) return null;
    // Both sides already contain this key, and there is no non-key value to
    // reconcile. Do not emit an UPDATE with an empty assignment list.
    if (pk.every((column) => column !== 0)) return null;
    // Normalize SQLite's redundant new.* PK values to the strict session
    // UPDATE representation used by this SDK. Do not invent missing evidence.
    return {
      operation: "update", indirect: local.indirect,
      old: remote.values,
      new: local.new.map((value, i) => pk[i] === 0 ? value : undefined),
    };
  }
  if (local.operation === "delete") {
    if (remote.operation === "delete") return null;
    return {
      ...local,
      old: local.old.map((value, i) => remote.values[i] === undefined ? value : remote.values[i]!),
    };
  }
  if (remote.operation === "delete") {
    if (remote.replace) return null;
    return {
      operation: "insert", indirect: local.indirect,
      new: local.new.map((value, i) => value === undefined ? remote.values[i] : value) as ChangesetValue[],
    };
  }
  const before: ChangesetField[] = [], after: ChangesetField[] = [];
  let changed = false;
  for (let i = 0; i < pk.length; i++) {
    if (pk[i] !== 0) {
      before.push(local.old[i]); after.push(undefined);
    } else if (local.new[i] === undefined || remote.replaced[i]) {
      before.push(undefined); after.push(undefined);
    } else {
      before.push(remote.values[i] === undefined ? local.old[i] : remote.values[i]);
      after.push(local.new[i]); changed = true;
    }
  }
  return changed ? { operation: "update", indirect: local.indirect, old: before, new: after } : null;
}

/**
 * Native SQLite session rebasing, independent of SQL execution and durability.
 * Configure ONLY committed remote decisions, in their actual application order.
 * Rebase the original local changes against the combined history; do not feed
 * previously rebased output back through the same history. Patchsets reject.
 *
 * configure() owns decoded values and publishes atomically. Input and retained
 * history are bounded; scratch memory includes decoded input and staged maps.
 * Budgets are not an engine/heap/RSS limit. No database lock or retry is added.
 */
export class ChangesetRebaser {
  readonly #policy: ReturnType<typeof resolveChangesetLimits>;
  #tables = new Map<string, RebaseTable>();
  #stats = emptyStats();

  constructor(options?: ChangesetLimits) {
    this.#policy = Object.freeze(resolveChangesetLimits(options));
  }
  stats(): ChangesetRebaseStats { return this.#stats; }

  configure(bytes: Uint8Array): ChangesetRebaseStats {
    const incoming = decodeRebaseInfo(bytes, this.#policy);
    const staged = new Map(this.#tables);
    let { tables, changes, cells, byteLength } = this.#stats;
    for (const source of incoming) {
      const name = fold(source.name);
      const previous = staged.get(name);
      if (previous !== undefined) assertSchema(previous.primaryKey, source.primaryKey, source.name);
      const target: RebaseTable = {
        name: previous?.name ?? source.name,
        primaryKey: source.primaryKey,
        rows: new Map(previous?.rows),
      };
      if (previous === undefined) {
        tables++;
        byteLength += 2 + varintBytes(source.primaryKey.length) + source.primaryKey.length +
          utf8.encode(source.name).length;
      }
      for (const change of source.changes) {
        const rowKey = key(source.primaryKey, change.values);
        const old = target.rows.get(rowKey);
        const row = merge(source.primaryKey, old, change);
        if (old === undefined) { changes++; cells += source.primaryKey.length * 2; }
        else byteLength -= rowBytes(old, rowKey);
        byteLength += rowBytes(row, rowKey);
        if (tables > this.#policy.maxTables || changes > this.#policy.maxChanges ||
            cells > this.#policy.maxCells || byteLength > this.#policy.maxBytes)
          throw new ChangesetError("ERR_FSQLITE_CHANGESET_LIMIT", "Rebase history exceeds its retained-state budget");
        target.rows.set(rowKey, row);
      }
      staged.set(name, target);
    }
    this.#tables = staged;
    this.#stats = Object.freeze({ tables, changes, cells, byteLength });
    return this.#stats;
  }

  rebase(bytes: Uint8Array): Uint8Array {
    const local = decodeChangeset(bytes, this.#policy);
    const output: ChangesetTable[] = [];
    for (const table of local) {
      const remote = this.#tables.get(fold(table.name));
      if (remote === undefined) { output.push(table); continue; }
      assertSchema(table.primaryKey, remote.primaryKey, table.name);
      const changes: ChangesetChange[] = [];
      for (const change of table.changes) {
        const values = change.operation === "insert" ? change.new : change.old;
        const row = remote.rows.get(key(table.primaryKey, values));
        const result = row === undefined ? change : rebaseRow(table.primaryKey, change, row);
        if (result !== null) changes.push(result);
      }
      if (changes.length) output.push({ ...table, changes });
    }
    // Shared validation refuses missing before-images or incomplete synthesized
    // INSERTs instead of publishing a malformed result. State stays reusable.
    return encodeChangeset(output, this.#policy);
  }

  clear(): void { this.#tables = new Map(); this.#stats = emptyStats(); }
}

/** One-shot native rebase buffers in committed application order. */
export function rebaseChangeset(
  local: Uint8Array,
  rebaseInfo: readonly Uint8Array[],
  options?: ChangesetLimits,
): Uint8Array {
  if (!Array.isArray(rebaseInfo) || rebaseInfo.length > 100_000)
    throw new ChangesetRebaseError("ERR_FSQLITE_REBASE_INPUT", "Rebase history must be an array of at most 100000 buffers");
  const policy = Object.freeze(resolveChangesetLimits(options));
  const rebaser = new ChangesetRebaser(policy);
  // Own local data before any history array getter can change the input.
  const captured = encodeChangeset(decodeChangeset(local, policy), policy);
  for (let i = 0, n = rebaseInfo.length; i < n; i++) rebaser.configure(rebaseInfo[i]!);
  return rebaser.rebase(captured);
}
