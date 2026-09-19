import { MAX_EXECUTE_MANY_ROWS } from "./protocol";
import type { InitConfig, SqlBindings, SqlScalar, WorkerRequest } from "./protocol";
import { BindingError, isNamedBindings } from "./bindings";
import { validateTransactionId } from "./transactions";
import { resolveResultEncoding } from "./result-codec";
import { resolvePreparedStatementLimits } from "./statement-budget";
import { resolveSnapshotOwnership } from "./snapshot-ownership";

export interface RequestLimits {
  /** Active plus queued ordinary requests. Close/cancel use a separate lane. */
  maxPendingRequests: number;
  /** Accounted request payload, not result, database, heap or RSS memory. */
  maxPendingBytes: number;
}

export interface RequestQueueStats extends Readonly<RequestLimits> {
  readonly pendingRequests: number;
  readonly pendingBytes: number;
  readonly rejectedRequests: number;
}

export const DEFAULT_REQUEST_LIMITS: Readonly<RequestLimits> = Object.freeze({
  maxPendingRequests: 128,
  maxPendingBytes: 128 * 1024 * 1024,
});

export function resolveRequestLimits(options: Partial<RequestLimits> = {}): Readonly<RequestLimits> {
  const maxPendingRequests = options.maxPendingRequests ?? DEFAULT_REQUEST_LIMITS.maxPendingRequests;
  const maxPendingBytes = options.maxPendingBytes ?? DEFAULT_REQUEST_LIMITS.maxPendingBytes;
  if (!Number.isSafeInteger(maxPendingRequests) || maxPendingRequests < 1 || maxPendingRequests > 4096) {
    throw new RangeError("maxPendingRequests must be an integer in 1..4096");
  }
  if (!Number.isSafeInteger(maxPendingBytes) || maxPendingBytes < 256 || maxPendingBytes > 1024 ** 3) {
    throw new RangeError("maxPendingBytes must be an integer in 256..1073741824");
  }
  return Object.freeze({ maxPendingRequests, maxPendingBytes });
}

export class RequestAdmissionError extends Error {
  readonly userRecoverable = true;
  readonly transient: boolean;
  readonly suggestion: string;
  readonly batchIndex?: number;

  constructor(readonly code: "ERR_FSQLITE_QUEUE_FULL" | "ERR_FSQLITE_REQUEST_TOO_LARGE" |
    "ERR_FSQLITE_REQUEST_INPUT" | "ERR_FSQLITE_BULK_INPUT", message: string, batchIndex?: number) {
    super(message);
    this.name = "RequestAdmissionError";
    if (batchIndex !== undefined) this.batchIndex = batchIndex;
    this.transient = code === "ERR_FSQLITE_QUEUE_FULL";
    this.suggestion = this.transient
      ? "Await admitted work before submitting more. No SQL from this request ran."
      : "Reduce the request size, use executeStream, or correct the request fields.";
  }
}

function invalid(message: string): never {
  throw new RequestAdmissionError("ERR_FSQLITE_REQUEST_INPUT", message);
}

export function validateRequestId(id: number): void {
  if (!Number.isSafeInteger(id)) invalid("Request ids must be safe integers");
}

/**
 * Build a plain, bounded wire message before postMessage can clone it. Only
 * known protocol fields and captured scalar values survive: extra properties,
 * user array iterators and repeated getters cannot hide unaccounted payloads.
 * Blob aliases are counted once per request, but their ENTIRE backing buffers
 * are counted (structured clone does not copy only a subarray's visible bytes).
 */
function captureRequest(input: WorkerRequest, maximum: number): { request: WorkerRequest; bytes: number } {
  let bytes = 128;
  const buffers = new Set<ArrayBufferLike>();
  function charge(amount: number): void {
    if (!Number.isSafeInteger(amount) || amount < 0 || amount > maximum - bytes) {
      throw new RequestAdmissionError("ERR_FSQLITE_REQUEST_TOO_LARGE", "Request exceeds maxPendingBytes");
    }
    bytes += amount;
  }
  function text(value: string): string {
    if (typeof value !== "string") invalid("SQL, names and statement ids must be strings");
    charge(16 + value.length * 2);
    return value;
  }
  function blob(value: Uint8Array): Uint8Array {
    if (!(value instanceof Uint8Array)) invalid("Binary parameters must be Uint8Array values");
    const buffer = value.buffer;
    if (!buffers.has(buffer)) {
      // A resizable/growable buffer can reserve more than its current length.
      const maximumLength = (buffer as ArrayBuffer & { maxByteLength?: number }).maxByteLength;
      charge(maximumLength ?? buffer.byteLength);
      buffers.add(buffer);
    }
    return new Uint8Array(buffer, value.byteOffset, value.byteLength);
  }
  function params(values: readonly SqlScalar[], batchIndex?: number): SqlScalar[] {
    if (!Array.isArray(values)) {
      if (batchIndex !== undefined) throw new RequestAdmissionError("ERR_FSQLITE_BULK_INPUT", "Each parameter set must be an array", batchIndex);
      invalid("Parameters must be a positional array");
    }
    const length = values.length;
    charge(16 + length * 16);
    const captured: SqlScalar[] = [];
    for (let i = 0; i < length; i += 1) {
      const value = values[i];
      if (typeof value === "string") { charge(value.length * 2); captured.push(value); }
      else if (value instanceof Uint8Array) captured.push(blob(value));
      else if (typeof value === "bigint") {
        if (value < -(1n << 63n) || value >= (1n << 63n)) invalid("Integer parameters must fit signed 64-bit SQLite values");
        captured.push(value);
      } else if (value === null || typeof value === "boolean" || typeof value === "number") captured.push(value);
      else invalid("Parameters must be SQL scalars");
    }
    return captured;
  }
  function sets(values: readonly (readonly SqlScalar[])[]): SqlScalar[][] {
    if (!Array.isArray(values) || values.length > MAX_EXECUTE_MANY_ROWS) {
      throw new RequestAdmissionError("ERR_FSQLITE_BULK_INPUT", `Bulk execution accepts at most ${MAX_EXECUTE_MANY_ROWS} parameter sets`);
    }
    const length = values.length;
    charge(16 + length * 16);
    const captured: SqlScalar[][] = [];
    for (let i = 0; i < length; i += 1) captured.push(params(values[i]!, i));
    return captured;
  }
  function bindings(values: SqlBindings): SqlBindings {
    if (Array.isArray(values)) return params(values);
    if (!isNamedBindings(values)) {
      throw new BindingError("ERR_FSQLITE_BINDING_INPUT", "Bindings must be an array or a plain named object");
    }
    charge(16);
    const captured: Record<string, SqlScalar> = Object.create(null);
    // Charge incrementally before cloning. Do not invoke a custom iterator or
    // copy unrelated/inherited properties into the worker message.
    for (const key in values) {
      if (!Object.hasOwn(values, key)) continue;
      const name = text(key);
      const value = params([values[key]!])[0]!;
      captured[name] = value;
    }
    return captured;
  }
  const requestId = input.requestId;
  validateRequestId(requestId);
  const kind = input.kind;
  const transactionId = input.transactionId;
  if (transactionId !== undefined) { validateTransactionId(transactionId); text(transactionId); }
  let request: WorkerRequest;
  switch (kind) {
    case "transaction": {
      if (transactionId === undefined) invalid("Managed boundaries require a transaction id");
      const action = input.action, parentId = input.parentId;
      if (action !== "begin" && action !== "commit" && action !== "rollback") invalid("Invalid transaction action");
      request = { kind, requestId, transactionId, action };
      if (parentId !== undefined) {
        if (action !== "begin") invalid("Only begin accepts a parent transaction id");
        validateTransactionId(parentId);
        request.parentId = text(parentId);
      }
      break;
    }
    case "init": {
      const source = input.config;
      const config: InitConfig = {};
      const dbName = source.dbName, persistence = source.persistence, wasmUrl = source.wasmUrl, snapshot = source.snapshot;
      if (dbName !== undefined) config.dbName = text(dbName);
      if (persistence !== undefined) { text(persistence); config.persistence = persistence; }
      const ownership = resolveSnapshotOwnership(source.snapshotOwnership);
      if (ownership !== undefined) { text(ownership); config.snapshotOwnership = ownership; }
      if (wasmUrl !== undefined) config.wasmUrl = text(wasmUrl);
      if (snapshot !== undefined) config.snapshot = blob(snapshot);
      const resultEncoding = source.resultEncoding;
      if (resultEncoding !== undefined) {
        config.resultEncoding = resolveResultEncoding(resultEncoding);
        text(config.resultEncoding);
      }
      const statementLimits = source.preparedStatementLimits;
      if (statementLimits !== undefined) {
        charge(48);
        config.preparedStatementLimits = resolvePreparedStatementLimits(statementLimits);
      }
      request = { kind, requestId, config };
      break;
    }
    case "execute": case "query": {
      const sql = text(input.sql), values = input.params;
      request = { kind, requestId, sql, params: bindings(values ?? []) };
      break;
    }
    case "execute-batch": case "prepare":
      request = { kind, requestId, sql: text(input.sql) };
      break;
    case "statement-query": case "statement-execute": {
      const statementId = text(input.statementId), values = input.params;
      request = { kind, requestId, statementId, params: bindings(values ?? []) };
      break;
    }
    case "execute-many": case "statement-execute-many": {
      const identity = kind === "execute-many" ? text(input.sql) : text(input.statementId);
      const parameterSets = sets(input.parameterSets);
      const cancellable = input.cancellable;
      if (cancellable !== undefined && typeof cancellable !== "boolean") invalid("cancellable must be a boolean");
      request = kind === "execute-many" ? { kind, requestId, sql: identity, parameterSets } :
        { kind, requestId, statementId: identity, parameterSets };
      if (cancellable !== undefined) request.cancellable = cancellable;
      break;
    }
    case "statement-finalize":
      request = { kind, requestId, statementId: text(input.statementId) };
      break;
    case "checkpoint": {
      request = { kind, requestId };
      const publicationId = input.publicationId;
      if (publicationId !== undefined) request.publicationId = revision(publicationId);
      break;
    }
    case "checkpoint-recover": {
      const publicationId = revision(input.publicationId), parentRevision = input.parentRevision;
      request = { kind, requestId, publicationId,
        parentRevision: parentRevision === null ? null : revision(parentRevision) };
      if (publicationId === parentRevision) invalid("Recovery revision must differ from its parent");
      break;
    }
    case "export":
      request = { kind, requestId };
      break;
    default:
      invalid("Unsupported ordinary request kind");
  }
  if (transactionId !== undefined) request.transactionId = transactionId;
  return { request, bytes };

  function revision(value: string): string {
    const captured = text(value);
    if (!/^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/.test(captured)) {
      invalid("Checkpoint identities must be UUID-v4 revisions");
    }
    return captured;
  }
}

/** Leases are held until settlement, not merely until execution starts. */
export class RequestBudget {
  readonly #limits: Readonly<RequestLimits>;
  readonly #ids = new Set<number>();
  #bytes = 0;
  #rejected = 0;

  constructor(options: Partial<RequestLimits> = {}) {
    this.#limits = resolveRequestLimits(options);
  }

  get stats(): RequestQueueStats {
    return Object.freeze({ ...this.#limits, pendingRequests: this.#ids.size,
      pendingBytes: this.#bytes, rejectedRequests: this.#rejected });
  }

  admit(input: WorkerRequest): { request: WorkerRequest; release: () => void } {
    try {
      const checkCapacity = (bytes: number): void => {
        if (this.#ids.size >= this.#limits.maxPendingRequests || bytes > this.#limits.maxPendingBytes - this.#bytes) {
          throw new RequestAdmissionError("ERR_FSQLITE_QUEUE_FULL", "FrankenSQLite request queue is full");
        }
      };
      checkCapacity(0);
      const { request, bytes } = captureRequest(input, this.#limits.maxPendingBytes);
      // Capturing application-provided getters can re-enter admission. Check
      // AGAIN after capture, then reserve without invoking application code.
      checkCapacity(bytes);
      const id = request.requestId;
      if (this.#ids.has(id)) invalid("Duplicate active request id");
      this.#ids.add(id);
      this.#bytes += bytes;
      let released = false;
      return { request, release: () => {
        if (released) return;
        released = true;
        this.#ids.delete(id);
        this.#bytes -= bytes;
      } };
    } catch (error: unknown) {
      this.#rejected = Math.min(this.#rejected + 1, Number.MAX_SAFE_INTEGER);
      throw error;
    }
  }
}
