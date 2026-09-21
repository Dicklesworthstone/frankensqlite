/** SQLite session changesets, not database pages or the compact patchset format. */
export type ChangesetValue = null | bigint | number | string | Uint8Array;
export type ChangesetField = ChangesetValue | undefined;
export type ChangesetChange =
  | {
      readonly operation: "insert";
      readonly indirect: boolean;
      readonly new: readonly ChangesetValue[];
    }
  | {
      readonly operation: "delete";
      readonly indirect: boolean;
      readonly old: readonly ChangesetValue[];
    }
  | {
      readonly operation: "update";
      readonly indirect: boolean;
      readonly old: readonly ChangesetField[];
      readonly new: readonly ChangesetField[];
    };
export interface ChangesetTable {
  readonly name: string;
  /** Zero for non-key columns; preserve SQLite's nonzero key-position bytes. */
  readonly primaryKey: readonly number[];
  readonly changes: readonly ChangesetChange[];
}
export interface ChangesetLimits {
  maxBytes?: number;
  maxTables?: number;
  maxColumns?: number;
  maxChanges?: number;
  /** Total old/new field slots, including undefined UPDATE fields. */
  maxCells?: number;
}
export class ChangesetError extends Error {
  constructor(
    readonly code:
      | "ERR_FSQLITE_CHANGESET_FORMAT"
      | "ERR_FSQLITE_CHANGESET_LIMIT"
      | "ERR_FSQLITE_CHANGESET_INPUT",
    message: string,
    readonly offset?: number,
  ) {
    super(message);
    this.name = "ChangesetError";
  }
}
const DEFAULTS = {
  maxBytes: 64 * 1024 * 1024,
  maxTables: 256,
  maxColumns: 2000,
  maxChanges: 100_000,
  maxCells: 1_000_000,
} as const;
type Limits = { readonly [K in keyof typeof DEFAULTS]: number };
const utf8 = new TextDecoder("utf-8", { fatal: true, ignoreBOM: true });
const encoder = new TextEncoder();
/** @internal Capture and validate the common codec/group policy once. */
export function resolveChangesetLimits(input: ChangesetLimits = {}): Limits {
  const result = { ...DEFAULTS } as { -readonly [K in keyof Limits]: number };
  for (const key of Object.keys(DEFAULTS) as (keyof Limits)[]) {
    const value = input[key] ?? DEFAULTS[key];
    const ceiling = key === "maxColumns" ? 32768 : DEFAULTS[key];
    if (!Number.isSafeInteger(value) || value < 1 || value > ceiling) {
      throw new ChangesetError(
        "ERR_FSQLITE_CHANGESET_INPUT",
        `${key} must be an integer in 1..${ceiling}`,
      );
    }
    result[key] = value;
  }
  return result;
}
function format(message: string, offset?: number): never {
  throw new ChangesetError("ERR_FSQLITE_CHANGESET_FORMAT", message, offset);
}
function limit(message: string): never {
  throw new ChangesetError("ERR_FSQLITE_CHANGESET_LIMIT", message);
}
function input(message: string): never {
  throw new ChangesetError("ERR_FSQLITE_CHANGESET_INPUT", message);
}
function fixedInput(bytes: Uint8Array, maximum: number): Uint8Array {
  if (!(bytes instanceof Uint8Array)) input("Changeset must be a Uint8Array");
  // Read internal typed-array slots, not shadowable properties or subclass
  // methods. A caller getter must not change the budget, offsets or group state
  // while parsing. The ordinary fixed view below has no application callbacks.
  const prototype = Object.getPrototypeOf(Uint8Array.prototype) as object;
  try {
    const get = (name: string): unknown =>
      Object.getOwnPropertyDescriptor(prototype, name)!.get!.call(bytes);
    const buffer = get("buffer");
    const offset = get("byteOffset") as number;
    const size = get("byteLength") as number;
    if (size > maximum) limit("Changeset exceeds maxBytes");
    if (
      !(buffer instanceof ArrayBuffer) ||
      Object.getOwnPropertyDescriptor(ArrayBuffer.prototype, "resizable")?.get?.call(buffer)
    )
      input("Changeset requires a fixed, non-shared ArrayBuffer");
    // Constructing this view also rejects detached buffers, even empty ones.
    return new Uint8Array(buffer, offset, size);
  } catch (error: unknown) {
    if (error instanceof ChangesetError) throw error;
    return input("Changeset requires an attached Uint8Array");
  }
}
function nameBytes(name: string): Uint8Array {
  if (typeof name !== "string" || name.length === 0 || name.length > 1024 || name.includes("\0"))
    input("Invalid changeset table name");
  const bytes = textBytes(name, 1024);
  return bytes;
}
function textBytes(text: string, maximum: number): Uint8Array {
  if (text.length > maximum) limit("Changeset text exceeds byte budget");
  const bytes = encoder.encode(text);
  if (bytes.length > maximum) limit("Changeset text exceeds byte budget");
  if (utf8.decode(bytes) !== text)
    input("Changeset text must not contain unpaired UTF-16 surrogates");
  return bytes;
}
function validateChange(pk: readonly number[], change: ChangesetChange): void {
  const before = change.operation === "insert" ? undefined : change.old;
  const after = change.operation === "delete" ? undefined : change.new;
  if (
    typeof change.indirect !== "boolean" ||
    (before !== undefined && before.length !== pk.length) ||
    (after !== undefined && after.length !== pk.length)
  )
    format("Invalid changeset row shape");
  let modified = false;
  for (let i = 0; i < pk.length; i++) {
    const old = before?.[i],
      next = after?.[i];
    if (change.operation !== "update") {
      const value = change.operation === "insert" ? next : old;
      if (value === undefined || (pk[i] !== 0 && value === null))
        format("Incomplete row or NULL primary key");
    } else if (pk[i] !== 0) {
      if (old === undefined || old === null || next !== undefined)
        format("UPDATE must retain its original primary key");
    } else {
      if ((old === undefined) !== (next === undefined))
        format("UPDATE requires paired old/new values");
      if (next !== undefined) modified = true;
    }
  }
  if (change.operation === "update" && !modified) format("UPDATE changes no columns");
}

/** Owns blobs; integers are bigint, reals are number, undefined is NOT SQL NULL. */
export function decodeChangeset(
  bytes: Uint8Array,
  options?: ChangesetLimits,
): readonly ChangesetTable[] {
  const policy = resolveChangesetLimits(options);
  bytes = fixedInput(bytes, policy.maxBytes);
  const view = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength);
  let pos = 0,
    rows = 0,
    cells = 0;
  const need = (n: number): void => {
    if (n > bytes.length - pos) format("Truncated changeset", pos);
  };
  const byte = (): number => {
    need(1);
    return bytes[pos++]!;
  };
  const length = (maximum: number): number => {
    let value = 0;
    for (let n = 0; n < 9; n++) {
      const b = byte();
      if (n === 0 && b === 128) format("Noncanonical changeset varint", pos - 1);
      value = value * (n === 8 ? 256 : 128) + (n === 8 ? b : b & 127);
      if (!Number.isSafeInteger(value) || value > maximum) limit("Changeset length exceeds budget");
      if (n === 8 || b < 128) return value;
    }
    return format("Invalid changeset varint", pos);
  };
  const field = (): ChangesetField => {
    const tag = byte();
    if (tag === 0) return undefined;
    if (tag === 5) return null;
    if (tag === 1 || tag === 2) {
      need(8);
      const start = pos;
      pos += 8;
      if (tag === 1) return view.getBigInt64(start);
      const value = view.getFloat64(start);
      if (Number.isNaN(value)) format("NaN is not a SQLite stored real", start);
      return value;
    }
    if (tag !== 3 && tag !== 4) return format("Unknown changeset value tag", pos - 1);
    const size = length(policy.maxBytes);
    need(size);
    const data = bytes.subarray(pos, pos + size);
    pos += size;
    if (tag === 4) return new Uint8Array(data);
    try {
      return utf8.decode(data);
    } catch {
      return format("Invalid changeset UTF-8", pos - size);
    }
  };
  const result: ChangesetTable[] = [];
  const names = new Set<string>();
  while (pos < bytes.length) {
    const header = byte();
    if (header === 80) format("Patchsets are not reversible changesets", pos - 1);
    if (header !== 84) format("Expected changeset table header", pos - 1);
    if (result.length >= policy.maxTables) limit("Changeset exceeds maxTables");
    const count = length(policy.maxColumns);
    if (count === 0) format("Changeset table has no columns", pos);
    need(count);
    const pk = Array.from(bytes.subarray(pos, pos + count));
    pos += count;
    if (!pk.some((value) => value !== 0)) format("Changeset table has no primary key", pos);
    const start = pos;
    while (byte() !== 0) {
      if (pos - start > 1024) limit("Changeset table name exceeds 1024 bytes");
    }
    let name: string;
    try {
      name = utf8.decode(bytes.subarray(start, pos - 1));
    } catch {
      return format("Invalid table name UTF-8", start);
    }
    if (name.length === 0) format("Empty changeset table name", start);
    const folded = name.replace(/[A-Z]/g, (c) => c.toLowerCase());
    if (names.has(folded)) format("Repeated changeset table header", start);
    names.add(folded);
    const changes: ChangesetChange[] = [];
    while (pos < bytes.length && bytes[pos] !== 84 && bytes[pos] !== 80) {
      const op = byte(),
        flag = byte();
      if (![18, 9, 23].includes(op) || flag > 1)
        format("Invalid change operation or indirect flag", pos - 2);
      if (++rows > policy.maxChanges) limit("Changeset exceeds maxChanges");
      cells += count * (op === 23 ? 2 : 1);
      if (cells > policy.maxCells) limit("Changeset exceeds maxCells");
      // Every slot needs at least a tag; validate before allocating row arrays.
      need(count * (op === 23 ? 2 : 1));
      const record = (): readonly ChangesetField[] =>
        Object.freeze(Array.from({ length: count }, field));
      let change: ChangesetChange;
      if (op === 18)
        change = {
          operation: "insert",
          indirect: flag === 1,
          new: record() as readonly ChangesetValue[],
        };
      else if (op === 9)
        change = {
          operation: "delete",
          indirect: flag === 1,
          old: record() as readonly ChangesetValue[],
        };
      else change = { operation: "update", indirect: flag === 1, old: record(), new: record() };
      validateChange(pk, change);
      changes.push(Object.freeze(change));
    }
    if (changes.length === 0) format("Table header has no changes", pos);
    result.push(
      Object.freeze({ name, primaryKey: Object.freeze(pk), changes: Object.freeze(changes) }),
    );
  }
  return Object.freeze(result);
}

/** Encode explicit changes, without silently coercing real values to integers. */
export function encodeChangeset(
  tables: readonly ChangesetTable[],
  options?: ChangesetLimits,
): Uint8Array {
  const policy = resolveChangesetLimits(options);
  if (!Array.isArray(tables)) input("Changeset tables must be an array");
  if (tables.length > policy.maxTables) limit("Changeset exceeds maxTables");
  let buffer = new Uint8Array(Math.min(1024, policy.maxBytes)),
    pos = 0,
    rows = 0,
    cells = 0;
  const room = (n: number): void => {
    if (n > policy.maxBytes - pos) limit("Changeset exceeds maxBytes");
    if (pos + n > buffer.length) {
      const next = new Uint8Array(Math.min(policy.maxBytes, Math.max(pos + n, buffer.length * 2)));
      next.set(buffer);
      buffer = next;
    }
  };
  const put = (b: number): void => {
    room(1);
    buffer[pos++] = b;
  };
  const data = (b: Uint8Array): void => {
    room(b.length);
    buffer.set(b, pos);
    pos += b.length;
  };
  const length = (n: number): void => {
    const parts = [n & 127];
    while ((n = Math.floor(n / 128)) > 0) parts.unshift((n & 127) | 128);
    for (const b of parts) put(b);
  };
  const field = (value: ChangesetField): void => {
    if (value === undefined) {
      put(0);
      return;
    }
    if (value === null) {
      put(5);
      return;
    }
    if (typeof value === "bigint" || typeof value === "number") {
      if (typeof value === "bigint" && (value < -(1n << 63n) || value >= 1n << 63n))
        input("Integer must fit signed 64-bit");
      if (typeof value === "number" && Number.isNaN(value))
        input("NaN is not a SQLite stored real");
      put(typeof value === "bigint" ? 1 : 2);
      room(8);
      const view = new DataView(buffer.buffer);
      if (typeof value === "bigint") view.setBigInt64(pos, value);
      else view.setFloat64(pos, value);
      pos += 8;
      return;
    }
    if (typeof value === "string") {
      const b = textBytes(value, policy.maxBytes - pos);
      put(3);
      length(b.length);
      data(b);
      return;
    }
    if (value instanceof Uint8Array) {
      put(4);
      length(value.length);
      data(value);
      return;
    }
    input("Unsupported changeset value");
  };
  for (let t = 0, n = tables.length; t < n; t++) {
    const table = tables[t]!;
    const name = nameBytes(table.name),
      sourceKey = table.primaryKey,
      changes = table.changes;
    if (!Array.isArray(sourceKey)) input("Invalid changeset primary key");
    const count = sourceKey.length;
    if (count === 0 || count > policy.maxColumns) input("Invalid changeset column count");
    const pk = Array.from({ length: count }, (_, i) => sourceKey[i]!);
    if (!Array.isArray(pk) || pk.length === 0 || pk.length > policy.maxColumns)
      input("Invalid changeset column count");
    if (!Array.isArray(changes) || changes.length === 0) input("Each table needs changes");
    put(84);
    length(pk.length);
    for (let i = 0; i < pk.length; i++) {
      const key = pk[i]!;
      if (!Number.isInteger(key) || key < 0 || key > 255) input("Invalid primary-key byte");
      put(key);
    }
    data(name);
    put(0);
    for (let i = 0, total = changes.length; i < total; i++) {
      if (++rows > policy.maxChanges) limit("Changeset exceeds maxChanges");
      const change = changes[i]!;
      const op = change.operation;
      if (op !== "insert" && op !== "delete" && op !== "update")
        input("Invalid changeset operation");
      const records =
        op === "insert" ? [change.new] : op === "delete" ? [change.old] : [change.old, change.new];
      cells += pk.length * records.length;
      if (cells > policy.maxCells) limit("Changeset exceeds maxCells");
      const indirect = change.indirect;
      if (typeof indirect !== "boolean") input("indirect must be a boolean");
      put(op === "insert" ? 18 : op === "delete" ? 9 : 23);
      put(Number(indirect));
      for (const record of records) {
        if (!Array.isArray(record) || record.length !== pk.length)
          input("Invalid changeset row shape");
        for (let col = 0; col < pk.length; col++) field(record[col]);
      }
    }
  }
  const output = buffer.slice(0, pos);
  // One shared semantic validator, including caller-created row/key combinations.
  decodeChangeset(output, policy);
  return output;
}

/** SQLite inversion preserves change order; primary keys stay in old UPDATE slots. */
export function invertChangeset(bytes: Uint8Array, options?: ChangesetLimits): Uint8Array {
  const policy = resolveChangesetLimits(options);
  const tables = decodeChangeset(bytes, policy).map((table) => ({
    ...table,
    changes: table.changes.map((change): ChangesetChange => {
      if (change.operation === "insert")
        return { operation: "delete", indirect: change.indirect, old: change.new };
      if (change.operation === "delete")
        return { operation: "insert", indirect: change.indirect, new: change.old };
      return {
        operation: "update",
        indirect: change.indirect,
        old: change.new.map((value, i) => (table.primaryKey[i] !== 0 ? change.old[i] : value)),
        new: change.old.map((value, i) => (table.primaryKey[i] !== 0 ? undefined : value)),
      };
    }),
  }));
  return encodeChangeset(tables, policy);
}
