import type {
  ChangesetChange,
  ChangesetField,
  ChangesetLimits,
  ChangesetTable,
} from "./changeset-codec";
import { ChangesetError, decodeChangeset, encodeChangeset } from "./changeset-codec";

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
