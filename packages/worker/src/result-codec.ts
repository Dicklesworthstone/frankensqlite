import type { QueryResult, SqlScalar } from "./protocol";

/** Binary transport is opt-in until measured on the application's browser. */
export type ResultEncoding = "structured-clone" | "binary" | "auto";
export const BINARY_RESULT_THRESHOLD = 64 * 1024;
export const MAX_BINARY_RESULT_BYTES = 64 * 1024 * 1024;
const MAX_SLOTS = 1_000_000;
const MAX_COLUMNS = 32_768;
const HEADER_BYTES = 32;
const MAGIC = 0x31525146; // FQR1, little-endian.
const MIN_I64 = -(1n << 63n);
const MAX_I64 = (1n << 63n) - 1n;

export class ResultCodecError extends Error {
  readonly code = "ERR_FSQLITE_RESULT_DECODE";
  readonly transient = false;
  constructor(message: string) {
    super(message);
    this.name = "ResultCodecError";
  }
}

export function resolveResultEncoding(value: unknown = "structured-clone"): ResultEncoding {
  if (value === "structured-clone" || value === "binary" || value === "auto") return value;
  throw Object.assign(new TypeError("resultEncoding must be structured-clone, binary or auto"), {
    code: "ERR_FSQLITE_RESULT_ENCODING", transient: false,
  });
}

function invalid(): never {
  throw new ResultCodecError("Invalid or unsupported FrankenSQLite binary result frame");
}

function checkShape(rows: number, columns: number, types: number): void {
  if (![rows, columns, types].every(n => Number.isSafeInteger(n) && n >= 0) ||
      columns > MAX_COLUMNS || types > MAX_COLUMNS || rows * (columns + 1) > MAX_SLOTS) invalid();
}

function sameValue(left: unknown, right: SqlScalar): boolean {
  if (Object.is(left, right)) return true;
  if (!(left instanceof Uint8Array) || !(right instanceof Uint8Array) || left.length !== right.length) return false;
  for (let i = 0; i < left.length; i++) if (left[i] !== right[i]) return false;
  return true;
}

/**
 * FQR1 encodes each positional value once. Object rows must be the exact
 * last-column-wins projection of rowArrays; otherwise return null and keep the
 * original structured-clone result. No source buffer is transferred or detached.
 * This does not bound core query execution, result construction, or process RSS.
 */
export function tryEncodeQueryResult(result: QueryResult, minimumBytes = 0): ArrayBuffer | null {
  try {
    if (!Number.isSafeInteger(minimumBytes) || minimumBytes < 0) return null;
    const { columns, columnCount, columnTypes, rows, rowArrays, changes } = result;
    if (!Array.isArray(columns) || !Array.isArray(columnTypes) || !Array.isArray(rows) ||
        !Array.isArray(rowArrays) || columnCount !== columns.length || rows.length !== rowArrays.length ||
        !Number.isSafeInteger(changes) || changes < 0) return null;
    checkShape(rows.length, columnCount, columnTypes.length);
    // Do not drop an extension's additional top-level metadata.
    const allowed = new Set(["columns", "columnCount", "columnTypes", "rows", "rowArrays", "changes"]);
    if (Object.keys(result).some(key => !allowed.has(key))) return null;
    let size = HEADER_BYTES;
    const charge = (bytes: number): void => {
      size += bytes;
      if (!Number.isSafeInteger(size) || size > MAX_BINARY_RESULT_BYTES) invalid();
    };
    const measureText = (text: string): void => {
      if (typeof text !== "string") invalid();
      charge(4 + text.length * 2);
    };
    const projection = new Map<string, number>();
    for (let column = 0; column < columnCount; column++) {
      measureText(columns[column]!);
      projection.set(columns[column]!, column);
    }
    for (const type of columnTypes) measureText(type);
    for (let index = 0; index < rowArrays.length; index++) {
      const values = rowArrays[index]!, row = rows[index];
      if (!Array.isArray(values) || values.length !== columnCount || typeof row !== "object" || row === null ||
          Object.keys(row).length !== projection.size) return null;
      for (const [name, column] of projection) {
        if (!Object.prototype.hasOwnProperty.call(row, name) || !sameValue(row[name], values[column]!)) return null;
      }
      for (const value of values) {
        charge(1); // type tag
        if (value === null || typeof value === "boolean") continue;
        if (typeof value === "number") charge(8);
        else if (typeof value === "bigint") {
          if (value < MIN_I64 || value > MAX_I64) return null;
          charge(8);
        } else if (typeof value === "string") measureText(value);
        else if (value instanceof Uint8Array) charge(4 + value.byteLength);
        else return null;
      }
    }
    if (size < minimumBytes) return null;
    const buffer = new ArrayBuffer(size), view = new DataView(buffer), bytes = new Uint8Array(buffer);
    view.setUint32(0, MAGIC, true);
    view.setUint16(4, 1, true); // version; bytes 6..7 reserved, zero.
    view.setUint32(8, size, true);
    view.setUint32(12, rows.length, true);
    view.setUint32(16, columnCount, true);
    view.setUint32(20, columnTypes.length, true);
    view.setFloat64(24, changes, true);
    let offset = HEADER_BYTES;
    const writeText = (text: string): void => {
      view.setUint32(offset, text.length, true); offset += 4;
      for (let i = 0; i < text.length; i++, offset += 2) view.setUint16(offset, text.charCodeAt(i), true);
    };
    for (const name of columns) writeText(name);
    for (const type of columnTypes) writeText(type);
    for (const values of rowArrays) for (const value of values) {
      if (value === null) bytes[offset++] = 0;
      else if (value === false) bytes[offset++] = 1;
      else if (value === true) bytes[offset++] = 2;
      else if (typeof value === "number") {
        bytes[offset++] = 3; view.setFloat64(offset, value, true); offset += 8;
      } else if (typeof value === "bigint") {
        bytes[offset++] = 4; view.setBigInt64(offset, value, true); offset += 8;
      } else if (typeof value === "string") {
        bytes[offset++] = 5; writeText(value);
      } else {
        bytes[offset++] = 6; view.setUint32(offset, value.byteLength, true); offset += 4;
        bytes.set(value, offset); offset += value.byteLength;
      }
    }
    if (offset !== size) return null;
    return buffer;
  } catch {
    // Encoding is an optimization AFTER SQL. Unsupported/over-cap results must
    // not turn a successful INSERT ... RETURNING into a retryable SQL failure.
    return null;
  }
}

/** Strict, bounded frame decoder. Text is UTF-16LE, including lone surrogates. */
export function decodeQueryResult(buffer: ArrayBuffer): QueryResult {
  try {
    if (!(buffer instanceof ArrayBuffer) || buffer.byteLength < HEADER_BYTES ||
        buffer.byteLength > MAX_BINARY_RESULT_BYTES) invalid();
    const view = new DataView(buffer);
    if (view.getUint32(0, true) !== MAGIC || view.getUint16(4, true) !== 1 ||
        view.getUint16(6, true) !== 0 || view.getUint32(8, true) !== buffer.byteLength) invalid();
    const rowCount = view.getUint32(12, true), columnCount = view.getUint32(16, true);
    const typeCount = view.getUint32(20, true), changes = view.getFloat64(24, true);
    checkShape(rowCount, columnCount, typeCount);
    if (!Number.isSafeInteger(changes) || changes < 0) invalid();
    let offset = HEADER_BYTES;
    const need = (length: number): void => {
      if (length > buffer.byteLength - offset) invalid();
    };
    // Check minimum encoded cost before allocating metadata or row containers.
    need((columnCount + typeCount) * 4 + rowCount * columnCount);
    const readLength = (): number => {
      need(4); const n = view.getUint32(offset, true); offset += 4; return n;
    };
    const readText = (): string => {
      const length = readLength(); need(length * 2);
      let text = "";
      // Avoid spread-argument limits and Unicode replacement by TextDecoder.
      for (let i = 0; i < length;) {
        const count = Math.min(length - i, 4096), codes = new Array<number>(count);
        for (let j = 0; j < count; j++, offset += 2) codes[j] = view.getUint16(offset, true);
        text += String.fromCharCode(...codes); i += count;
      }
      return text;
    };
    const columns: string[] = [], columnTypes: string[] = [];
    for (let i = 0; i < columnCount; i++) columns.push(readText());
    for (let i = 0; i < typeCount; i++) columnTypes.push(readText());
    const rows: Record<string, unknown>[] = [], rowArrays: SqlScalar[][] = [];
    for (let i = 0; i < rowCount; i++) {
      const row: Record<string, unknown> = {}, values: SqlScalar[] = [];
      for (let j = 0; j < columnCount; j++) {
        need(1);
        const tag = view.getUint8(offset++);
        let value: SqlScalar;
        switch (tag) {
          case 0: value = null; break;
          case 1: value = false; break;
          case 2: value = true; break;
          case 3: need(8); value = view.getFloat64(offset, true); offset += 8; break;
          case 4: need(8); value = view.getBigInt64(offset, true); offset += 8; break;
          case 5: value = readText(); break;
          case 6: {
            const length = readLength(); need(length);
            // A retained tiny blob must not retain the entire result frame.
            value = new Uint8Array(buffer.slice(offset, offset + length)); offset += length; break;
          }
          default: invalid();
        }
        values.push(value);
        const name = columns[j]!;
        if (name === "__proto__") Object.defineProperty(row, name,
          { value, writable: true, enumerable: true, configurable: true });
        else row[name] = value;
      }
      rows.push(row); rowArrays.push(values);
    }
    if (offset !== buffer.byteLength) invalid();
    return { columns, columnCount, columnTypes, rows, rowArrays, changes };
  } catch (error: unknown) {
    if (error instanceof ResultCodecError) throw error;
    // Detached/malformed buffers fail the pending request, never leave it hung.
    throw new ResultCodecError("Could not decode FrankenSQLite binary result frame");
  }
}
