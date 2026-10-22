/** Limits on a completed SQL result BEFORE encoding/structured-clone delivery.
 * These are not limits on SQL execution, engine allocation, or process RSS.
 */
export interface ResultLimits {
  /** Positional result rows, including rows with no columns. */
  maxRows: number;
  /** Positional SQL cells, not object-property count. */
  maxCells: number;
  /** Conservative logical clone charge, including whole BLOB backing buffers. */
  maxBytes: number;
}

export const DEFAULT_RESULT_LIMITS: Readonly<ResultLimits> = Object.freeze({
  maxRows: 100_000,
  maxCells: 1_000_000,
  maxBytes: 64 * 1024 * 1024,
});

export class ResultBudgetError extends Error {
  readonly transient = false;
  constructor(
    readonly code: "ERR_FSQLITE_RESULT_LIMIT" | "ERR_FSQLITE_RESULT_INPUT" | "ERR_FSQLITE_RESULT_SHAPE" | "ERR_FSQLITE_RESULT_POLICY",
    message: string,
  ) {
    super(message);
    this.name = "ResultBudgetError";
  }
}

/** Capture application getters once, before acquiring any SQL authority. */
export function resolveResultLimits(input: Partial<ResultLimits> = {}): Readonly<ResultLimits> {
  if (typeof input !== "object" || input === null || Array.isArray(input)) {
    throw new ResultBudgetError("ERR_FSQLITE_RESULT_INPUT", "resultLimits must be an object");
  }
  const result = { ...DEFAULT_RESULT_LIMITS };
  for (const key of ["maxRows", "maxCells", "maxBytes"] as const) {
    const supplied = input[key];
    const value = supplied === undefined ? DEFAULT_RESULT_LIMITS[key] : supplied;
    if (!Number.isSafeInteger(value) || value < 1 || value > DEFAULT_RESULT_LIMITS[key]) {
      throw new ResultBudgetError("ERR_FSQLITE_RESULT_INPUT", `${key} must be an integer in 1..${DEFAULT_RESULT_LIMITS[key]}`);
    }
    result[key] = value;
  }
  return Object.freeze(result);
}

const AFTER_SQL = " SQL has already executed; writes may have committed. Do not replay SQL because result delivery failed.";
function invalid(): never {
  throw new ResultBudgetError("ERR_FSQLITE_RESULT_SHAPE", "Cannot safely account for this SQL result." + AFTER_SQL);
}
function exceeded(): never {
  throw new ResultBudgetError("ERR_FSQLITE_RESULT_LIMIT", "SQL result exceeds its configured delivery budget." + AFTER_SQL);
}
function field(object: object, name: string): unknown {
  const descriptor = Object.getOwnPropertyDescriptor(object, name);
  if (descriptor === undefined || !Object.hasOwn(descriptor, "value")) invalid();
  return descriptor.value;
}
function object(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

/**
 * Charge the complete structured-clone graph, not just a binary frame or the
 * visible slice of a BLOB. Object and positional rows both travel in clone
 * mode. Aliased buffers/objects are charged once, as in structured clone.
 * Additional plain metadata and cycles are preserved and accounted for too.
 *
 * 16 bytes per value/container/property slot and per array index (including
 * holes), plus UTF-16 string/property bytes and ArrayBuffer storage is a deterministic policy metric, NOT an exact browser
 * heap/wire-size estimate. Fail before binary encoding or either delivery path.
 * Accessors and unsupported extension objects are refused without calling them.
 */
export function enforceResultLimits(value: unknown, limits: Readonly<ResultLimits>): void {
  if (!object(value)) invalid();
  const rows = field(value, "rows"), arrays = field(value, "rowArrays");
  const columns = field(value, "columns"), count = field(value, "columnCount");
  if (!Array.isArray(rows) || !Array.isArray(arrays) || !Array.isArray(columns) ||
      !Number.isSafeInteger(count) || (count as number) < 0 || count !== columns.length ||
      rows.length !== arrays.length) invalid();
  if (arrays.length > limits.maxRows || arrays.length * columns.length > limits.maxCells) exceeded();
  // Result rows must be dense and consistent with column metadata. Validate
  // without fetching a getter supplied by a custom core adapter.
  for (let i = 0; i < arrays.length; i++) {
    const row = field(arrays, String(i)), named = field(rows, String(i));
    if (!Array.isArray(row) || row.length !== columns.length || !object(named)) invalid();
  }

  let bytes = 0;
  const charge = (size: number): void => {
    if (!Number.isSafeInteger(size) || size < 0 || size > limits.maxBytes - bytes) exceeded();
    bytes += size;
  };
  const seen = new Set<object>();
  const typed = Object.getPrototypeOf(Uint8Array.prototype) as object;
  const arrayBufferLength = Object.getOwnPropertyDescriptor(ArrayBuffer.prototype, "byteLength")!.get!;
  const resizable = Object.getOwnPropertyDescriptor(ArrayBuffer.prototype, "resizable")?.get;
  const typedBuffer = Object.getOwnPropertyDescriptor(typed, "buffer")!.get!;
  const typedTag = Object.getOwnPropertyDescriptor(typed, Symbol.toStringTag)!.get!;
  function bufferSize(buffer: object): number {
    try {
      // Reject SharedArrayBuffer, resizable and detached buffers. Their bytes
      // cannot be bounded as one stable ordinary structured-clone allocation.
      const length = arrayBufferLength.call(buffer) as number;
      if (resizable?.call(buffer)) invalid();
      new Uint8Array(buffer as ArrayBuffer, 0, 0);
      return length;
    } catch (error: unknown) {
      if (error instanceof ResultBudgetError) throw error;
      return invalid();
    }
  }
  function* properties(record: object): Generator<unknown> {
    for (const name in record) {
      if (!Object.hasOwn(record, name)) continue;
      charge(16 + name.length * 2);
      yield field(record, name);
    }
  }
  const stack: Iterator<unknown>[] = [[value][Symbol.iterator]()];
  while (stack.length !== 0) {
    const next = stack[stack.length - 1]!.next();
    if (next.done) { stack.pop(); continue; }
    const item: unknown = next.value;
    charge(16);
    if (item === null || item === undefined || typeof item === "boolean" || typeof item === "number") continue;
    if (typeof item === "string") { charge(item.length * 2); continue; }
    if (typeof item === "bigint") {
      // SQL INTEGER is signed 64-bit. Reject extension-sized integers rather
      // than allocate an unbounded decimal string just to measure them.
      if (item < -(1n << 63n) || item > (1n << 63n) - 1n) invalid();
      continue;
    }
    if (typeof item !== "object") invalid();
    if (seen.has(item)) continue;
    seen.add(item);
    if (ArrayBuffer.isView(item)) {
      let buffer: object;
      try {
        if (typedTag.call(item) !== "Uint8Array") invalid();
        buffer = typedBuffer.call(item) as object;
      } catch (error: unknown) {
        if (error instanceof ResultBudgetError) throw error;
        invalid();
      }
      // A 1-byte Uint8Array view can cause its entire 64-MiB backing buffer to
      // be cloned. The transport budget must account for that hidden cost.
      const length = bufferSize(buffer);
      if (!seen.has(buffer)) { seen.add(buffer); charge(length); }
      continue;
    }
    let isBuffer = false;
    try { arrayBufferLength.call(item); isBuffer = true; } catch { /* not an ArrayBuffer */ }
    if (isBuffer) { charge(bufferSize(item)); continue; }
    if (Array.isArray(item)) {
      // Structured clone preserves array length, including holes. A huge sparse
      // metadata array must not evade charging because it has no own indices.
      charge(item.length * 16);
    } else {
      const prototype = Object.getPrototypeOf(item);
      // Ordinary records (including cross-realm records) or null-prototype
      // dictionaries only. Map/Set/Date/class instances are not SQL scalars.
      if (prototype !== null && Object.getPrototypeOf(prototype) !== null) invalid();
    }
    if (stack.length >= 64) invalid();
    stack.push(properties(item));
  }
}

/** An explicit request requires a complete acknowledgement, not local defaults. */
export function captureResultLimitsAcknowledgement(
  value: unknown,
  requested?: Readonly<ResultLimits>,
): Readonly<ResultLimits> | null {
  if (value === undefined && requested === undefined) return null;
  const reject = (): never => {
    throw new ResultBudgetError("ERR_FSQLITE_RESULT_POLICY",
      "The worker did not acknowledge resultLimits within the requested policy; use a matching worker before submitting SQL");
  };
  if (!object(value)) return reject();
  const captured: Partial<ResultLimits> = {};
  for (const key of ["maxRows", "maxCells", "maxBytes"] as const) {
    const descriptor = Object.getOwnPropertyDescriptor(value, key);
    if (descriptor === undefined || !Object.hasOwn(descriptor, "value") ||
        typeof descriptor.value !== "number" || !Number.isSafeInteger(descriptor.value) ||
        descriptor.value < 1 || descriptor.value > DEFAULT_RESULT_LIMITS[key] ||
        (requested !== undefined && descriptor.value > requested[key])) return reject();
    captured[key] = descriptor.value;
  }
  return resolveResultLimits(captured);
}
