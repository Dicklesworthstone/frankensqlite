import type {
  ChangesetChange,
  ChangesetField,
  ChangesetLimits,
  ChangesetTable,
} from "./changeset-codec";
import {
  ChangesetError,
  decodeChangeset,
  encodeChangeset,
  resolveChangesetLimits,
} from "./changeset-codec";

/** Incompatible table layouts cannot be combined without a schema-aware rebase. */
export class ChangesetGroupError extends Error {
  readonly code = "ERR_FSQLITE_CHANGESET_SCHEMA";

  constructor(readonly table: string) {
    super(`Changesets disagree on the column count or primary key of table ${table}`);
    this.name = "ChangesetGroupError";
  }
}

interface GroupTable {
  readonly name: string;
  readonly primaryKey: readonly number[];
  readonly changes: Map<string, ChangesetChange>;
}

function captureLimits(options?: ChangesetLimits): Readonly<ChangesetLimits> {
  const policy: ChangesetLimits = {};
  for (const name of ["maxBytes", "maxTables", "maxColumns", "maxChanges", "maxCells"] as const) {
    const value = options?.[name];
    if (value !== undefined) policy[name] = value;
  }
  return Object.freeze(policy);
}

/** Session identity is storage-class/byte exact, NOT SQL affinity or collation. */
function equal(left: ChangesetField, right: ChangesetField): boolean {
  if (left instanceof Uint8Array && right instanceof Uint8Array) {
    return left.length === right.length && left.every((value, i) => value === right[i]);
  }
  // Distinguish integer from real, and preserve the IEEE representation of -0.
  return Object.is(left, right);
}

/** Length-delimited typed keys avoid coercion, concatenation ambiguity and blob identity. */
function rowKey(primaryKey: readonly number[], change: ChangesetChange): string {
  const values = change.operation === "insert" ? change.new : change.old;
  const parts: string[] = [];
  for (let i = 0; i < primaryKey.length; i++) {
    if (primaryKey[i] === 0) continue;
    const value = values[i];
    if (typeof value === "bigint") parts.push(`i${value};`);
    else if (typeof value === "number") {
      const bits = new DataView(new ArrayBuffer(8));
      bits.setFloat64(0, value);
      parts.push(`r${bits.getBigUint64(0).toString(16).padStart(16, "0")}`);
    } else if (typeof value === "string") parts.push(`t${value.length}:${value}`);
    else if (value instanceof Uint8Array) {
      parts.push(`b${value.length}:`);
      // Do not allocate one temporary object per byte or spread an unbounded
      // blob onto the call stack. Binary strings retain exact byte equality.
      for (let start = 0; start < value.length; start += 4096) {
        parts.push(String.fromCharCode(...value.subarray(start, start + 4096)));
      }
    } else {
      throw new ChangesetError("ERR_FSQLITE_CHANGESET_FORMAT", "Missing changeset primary key");
    }
  }
  return parts.join("");
}

function update(
  primaryKey: readonly number[],
  old: readonly ChangesetField[],
  next: readonly ChangesetField[],
  indirect: boolean,
): ChangesetChange | null {
  const before: ChangesetField[] = [];
  const after: ChangesetField[] = [];
  let modified = false;
  for (let i = 0; i < primaryKey.length; i++) {
    if (primaryKey[i] !== 0) {
      before.push(old[i]);
      after.push(undefined);
    } else if (equal(old[i], next[i])) {
      before.push(undefined);
      after.push(undefined);
    } else {
      before.push(old[i]);
      after.push(next[i]);
      modified = true;
    }
  }
  return modified ? { operation: "update", indirect, old: before, new: after } : null;
}

/** The nine sqlite3changegroup_add transitions; this is not conflict resolution. */
function combine(
  primaryKey: readonly number[],
  first: ChangesetChange,
  second: ChangesetChange,
): ChangesetChange | null {
  // SQLite ignores these inconsistent-history transitions, including their
  // indirect flag. Never turn them into a replacement or infer missing data.
  const indirect = first.indirect && second.indirect;
  if (first.operation === "insert") {
    if (second.operation === "insert") return first;
    if (second.operation === "delete") return null;
    const patch = second.new;
    return {
      operation: "insert",
      indirect,
      new: first.new.map((value, i) => (patch[i] === undefined ? value : patch[i]!)),
    };
  }
  if (first.operation === "delete") {
    return second.operation === "insert"
      ? update(primaryKey, first.old, second.new, indirect)
      : first;
  }
  if (second.operation === "insert") return first;
  if (second.operation === "delete") {
    return {
      operation: "delete",
      indirect,
      // A delete after an update must match the ORIGINAL before-image when
      // applied elsewhere, not the intermediate values that were just deleted.
      old: second.old.map((value, i) => (first.old[i] === undefined ? value : first.old[i]!)),
    };
  }
  return update(
    primaryKey,
    first.old.map((value, i) => (value === undefined ? second.old[i] : value)),
    first.new.map((value, i) => (second.new[i] === undefined ? value : second.new[i])),
    indirect,
  );
}

function addTables(group: Map<string, GroupTable>, tables: readonly ChangesetTable[]): void {
  for (const table of tables) {
    const name = table.name.replace(/[A-Z]/g, (c) => c.toLowerCase());
    let target = group.get(name);
    if (target === undefined) {
      target = { name: table.name, primaryKey: table.primaryKey, changes: new Map() };
      group.set(name, target);
    } else if (
      target.primaryKey.length !== table.primaryKey.length ||
      target.primaryKey.some((value, i) => value !== table.primaryKey[i])
    ) {
      throw new ChangesetGroupError(table.name);
    }
    for (const change of table.changes) {
      const key = rowKey(target.primaryKey, change);
      const previous = target.changes.get(key);
      const result = previous === undefined ? change : combine(target.primaryKey, previous, change);
      if (result === null) target.changes.delete(key);
      else target.changes.set(key, result);
    }
  }
}

function outputTables(group: ReadonlyMap<string, GroupTable>): ChangesetTable[] {
  const tables: ChangesetTable[] = [];
  for (const table of group.values()) {
    if (table.changes.size !== 0)
      tables.push({
        name: table.name,
        primaryKey: table.primaryKey,
        changes: [...table.changes.values()],
      });
  }
  return tables;
}

/** Counters describe encoded payload/schema and field slots, not process memory. */
export interface ChangesetGroupStats {
  /** Retained table layouts, including tables whose changes have cancelled out. */
  readonly tables: number;
  readonly changes: number;
  readonly cells: number;
  /** Exact byte length of output(), excluding empty tables. */
  readonly byteLength: number;
  /** Encoded header bytes for ALL retained table layouts. */
  readonly schemaBytes: number;
}

interface BudgetedTable extends GroupTable {
  readonly headerBytes: number;
}

function varintBytes(value: number): number {
  let bytes = 1;
  for (; value >= 128; value = Math.floor(value / 128)) bytes++;
  return bytes;
}

/** Values have already passed the codec's strict UTF-8 validation. */
function textBytes(value: string): number {
  let bytes = 0;
  for (let i = 0; i < value.length; i++) {
    const unit = value.charCodeAt(i);
    if (unit < 128) bytes++;
    else if (unit < 2048) bytes += 2;
    else if (unit >= 0xd800 && unit <= 0xdbff) {
      bytes += 4;
      i++;
    } else bytes += 3;
  }
  return bytes;
}

function rowCost(change: ChangesetChange): { bytes: number; cells: number } {
  const records =
    change.operation === "insert"
      ? [change.new]
      : change.operation === "delete"
        ? [change.old]
        : [change.old, change.new];
  let bytes = 2;
  let cells = 0;
  for (const record of records) {
    cells += record.length;
    for (const value of record) {
      bytes++; // Every field, including undefined and NULL, has a type tag.
      if (typeof value === "bigint" || typeof value === "number") bytes += 8;
      else if (typeof value === "string" || value instanceof Uint8Array) {
        const length = typeof value === "string" ? textBytes(value) : value.length;
        bytes += varintBytes(length) + length;
      }
    }
  }
  return { bytes, cells };
}

const emptyStats = (): ChangesetGroupStats =>
  Object.freeze({ tables: 0, changes: 0, cells: 0, byteLength: 0, schemaBytes: 0 });

/**
 * Incremental in-memory composition. add() parses only the incoming chunk and
 * stages only touched rows; it does not reparse/re-encode the accumulated batch.
 * Format, schema and limit failures leave the entire previous group unchanged.
 *
 * Limits apply to each input and retained state. Retained schema headers count
 * against maxBytes/maxTables even after their rows cancel, preventing unbounded
 * schema churn. clear() releases the whole history and starts a new batch; it
 * is neither a durability acknowledgement nor an automatic outbox compaction.
 */
export class ChangesetGroup {
  readonly #policy: ReturnType<typeof resolveChangesetLimits>;
  readonly #tables = new Map<string, BudgetedTable>();
  #stats = emptyStats();
  #rowBytes = 0;

  constructor(options?: ChangesetLimits) {
    this.#policy = Object.freeze(resolveChangesetLimits(options));
  }

  stats(): ChangesetGroupStats {
    return this.#stats;
  }

  add(bytes: Uint8Array): ChangesetGroupStats {
    const incoming = decodeChangeset(bytes, this.#policy);
    let rows = this.#stats.changes;
    let cells = this.#stats.cells;
    let rowBytes = this.#rowBytes;
    let schemaBytes = this.#stats.schemaBytes;
    let headerBytes = this.#stats.byteLength - this.#rowBytes;
    let tables = this.#tables.size;
    const plans: {
      name: string;
      table: BudgetedTable;
      operations: [string, ChangesetChange | null][];
    }[] = [];

    for (const source of incoming) {
      const name = source.name.replace(/[A-Z]/g, (c) => c.toLowerCase());
      let target = this.#tables.get(name);
      if (target === undefined) {
        const columns = source.primaryKey.length;
        target = {
          name: source.name,
          primaryKey: source.primaryKey,
          changes: new Map(),
          headerBytes: 2 + varintBytes(columns) + columns + textBytes(source.name),
        };
        schemaBytes += target.headerBytes;
        tables++;
      } else if (
        target.primaryKey.length !== source.primaryKey.length ||
        target.primaryKey.some((value, i) => value !== source.primaryKey[i])
      ) {
        throw new ChangesetGroupError(source.name);
      }

      const staged = new Map<string, ChangesetChange | null>();
      const operations: [string, ChangesetChange | null][] = [];
      let count = target.changes.size;
      for (const change of source.changes) {
        const key = rowKey(target.primaryKey, change);
        // has() distinguishes a staged deletion from a key not yet touched.
        const previous = (staged.has(key) ? staged.get(key) : target.changes.get(key)) ?? undefined;
        const result = previous === undefined ? change : combine(target.primaryKey, previous, change);
        if (previous !== undefined) {
          const cost = rowCost(previous);
          rowBytes -= cost.bytes;
          cells -= cost.cells;
          rows--;
          count--;
        }
        if (result !== null) {
          const cost = rowCost(result);
          rowBytes += cost.bytes;
          cells += cost.cells;
          rows++;
          count++;
        }
        staged.set(key, result);
        // Retain operation order, including delete/reinsert of the same key.
        // One small record per input change; no clone of the existing index.
        operations.push([key, result]);
      }
      if (target.changes.size === 0 && count !== 0) headerBytes += target.headerBytes;
      if (target.changes.size !== 0 && count === 0) headerBytes -= target.headerBytes;
      plans.push({ name, table: target, operations });
    }

    if (
      tables > this.#policy.maxTables ||
      rows > this.#policy.maxChanges ||
      cells > this.#policy.maxCells ||
      rowBytes + schemaBytes > this.#policy.maxBytes
    ) {
      throw new ChangesetError(
        "ERR_FSQLITE_CHANGESET_LIMIT",
        "Adding this changeset would exceed the group's retained-state budget",
      );
    }

    // No application callbacks or input access remain past this publication
    // point. Every fallible validation above operated only on the staged plan.
    for (const plan of plans) {
      this.#tables.set(plan.name, plan.table);
      for (const [key, result] of plan.operations) {
        if (result === null) plan.table.changes.delete(key);
        else plan.table.changes.set(key, result);
      }
    }
    this.#rowBytes = rowBytes;
    this.#stats = Object.freeze({
      tables,
      changes: rows,
      cells,
      byteLength: rowBytes + headerBytes,
      schemaBytes,
    });
    return this.#stats;
  }

  /** A fresh owned buffer on every call; reading never consumes the group. */
  output(): Uint8Array {
    return encodeChangeset(outputTables(this.#tables), this.#policy);
  }

  clear(): void {
    this.#tables.clear();
    this.#rowBytes = 0;
    this.#stats = emptyStats();
  }
}

/**
 * Compose consecutive SQLite session changesets: first, then second. The result
 * retains original conflict before-images and can be applied or inverted using
 * the existing APIs. Insert/delete pairs and fully reverted updates disappear.
 *
 * This does not rebase divergent replicas, preserve intermediate trigger events,
 * or acknowledge durable delivery. Do not replace an already-identified outbox
 * payload with composed bytes. Patchsets remain unsupported. Each input and the
 * final output independently obey ChangesetLimits; scratch memory also includes
 * both decoded inputs and their primary-key index, not a hard process-RSS cap.
 * Table order is first appearance; row order is not SQLite's internal hash order.
 */
export function concatChangesets(
  first: Uint8Array,
  second: Uint8Array,
  options?: ChangesetLimits,
): Uint8Array {
  const policy = captureLimits(options);
  // Parse both completely before composition. Blob values are owned copies and
  // neither caller input is mutated, including on a malformed trailing record.
  const a = decodeChangeset(first, policy);
  const b = decodeChangeset(second, policy);
  const group = new Map<string, GroupTable>();
  addTables(group, a);
  addTables(group, b);
  return encodeChangeset(outputTables(group), policy);
}
