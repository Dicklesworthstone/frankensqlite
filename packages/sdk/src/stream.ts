import { MAX_EXECUTE_MANY_ROWS, validateBulkSql } from "@frankensqlite/worker";
import { FrankenSQLiteError } from "./errors";
import type { ExecuteStreamOptions, ExecuteStreamResult, SqlRowSource, SqlScalar } from "./types";
import type { FrankenWorkerClient } from "./worker-client";

export type StreamFailurePhase =
  | "input"
  | "source"
  | "prepare"
  | "execute"
  | "progress"
  | "cleanup"
  | "cancelled";

/** The original SQL/producer error remains available as cause. */
export class FrankenStreamError extends Error {
  readonly code: "ERR_FSQLITE_STREAM" | "ERR_FSQLITE_STREAM_CANCELLED";

  constructor(
    readonly phase: StreamFailurePhase,
    readonly rowIndex: number | undefined,
    cause: unknown,
    readonly cleanupErrors: readonly unknown[] = [],
  ) {
    super(
      `FrankenSQLite stream ${phase} failed${rowIndex === undefined ? "" : ` at input row ${rowIndex}`}`,
      { cause },
    );
    this.name = "FrankenStreamError";
    this.code = phase === "cancelled" ? "ERR_FSQLITE_STREAM_CANCELLED" : "ERR_FSQLITE_STREAM";
  }
}

export interface RowStreamConfig {
  readonly batchSize: number;
  readonly maxBatchBytes: number;
  readonly signal: AbortSignal | undefined;
  readonly onProgress: ExecuteStreamOptions["onProgress"];
}

export function checkStreamCancellation(config: RowStreamConfig, rowIndex?: number): void {
  if (config.signal?.aborted)
    throw new FrankenStreamError("cancelled", rowIndex, config.signal.reason);
}

/** Validate before BEGIN or touching a producer; capture options exactly once. */
export function streamOptions(sql: string, options: ExecuteStreamOptions = {}): RowStreamConfig {
  try {
    validateBulkSql(sql);
    const batchSize = options.batchSize ?? 256;
    const maxBatchBytes = options.maxBatchBytes ?? 1024 * 1024;
    const signal = options.signal;
    const onProgress = options.onProgress;
    if (!Number.isInteger(batchSize) || batchSize < 1 || batchSize > MAX_EXECUTE_MANY_ROWS) {
      throw new RangeError(`batchSize must be an integer in 1..${MAX_EXECUTE_MANY_ROWS}`);
    }
    if (
      !Number.isSafeInteger(maxBatchBytes) ||
      maxBatchBytes < 1 ||
      maxBatchBytes > 64 * 1024 * 1024
    ) {
      throw new RangeError("maxBatchBytes must be an integer in 1..67108864");
    }
    if (
      signal !== undefined &&
      (signal === null ||
        typeof signal.aborted !== "boolean" ||
        typeof signal.addEventListener !== "function" ||
        typeof signal.removeEventListener !== "function")
    ) {
      throw new TypeError("signal must be an AbortSignal");
    }
    if (onProgress !== undefined && typeof onProgress !== "function")
      throw new TypeError("onProgress must be a function");
    const config = { batchSize, maxBatchBytes, signal, onProgress };
    checkStreamCancellation(config, 0);
    return config;
  } catch (error: unknown) {
    if (error instanceof FrankenStreamError) throw error;
    throw new FrankenStreamError("input", undefined, error);
  }
}

interface RowIterator {
  next(): IteratorResult<readonly SqlScalar[]> | PromiseLike<IteratorResult<readonly SqlScalar[]>>;
  return?():
    | IteratorResult<readonly SqlScalar[]>
    | PromiseLike<IteratorResult<readonly SqlScalar[]>>;
}
type StreamClient = Pick<
  FrankenWorkerClient,
  "prepare" | "executePreparedMany" | "finalizePrepared"
>;

function rowIterator(source: SqlRowSource): RowIterator {
  const asyncFactory = (source as AsyncIterable<readonly SqlScalar[]>)[Symbol.asyncIterator];
  if (asyncFactory !== undefined && typeof asyncFactory !== "function") {
    throw new TypeError("Row source Symbol.asyncIterator must be a function");
  }
  const factory = asyncFactory ?? (source as Iterable<readonly SqlScalar[]>)[Symbol.iterator];
  if (typeof factory !== "function") throw new TypeError("Rows must be iterable or async iterable");
  const iterator = factory.call(source);
  if (iterator === null || typeof iterator !== "object" || typeof iterator.next !== "function") {
    throw new TypeError("Row source returned an invalid iterator");
  }
  return iterator;
}

function copyRow(
  row: readonly SqlScalar[],
  maxBytes: number,
): { params: SqlScalar[]; bytes: number } {
  if (!Array.isArray(row))
    throw new TypeError("Each input row must be a positional parameter array");
  // Account before cloning large blobs. The allowances deliberately make empty
  // arrays, NULLs and other zero-payload values consume the budget as well.
  let bytes = 16;
  if (row.length > Math.floor((maxBytes - bytes) / 16)) {
    throw new RangeError("One input row exceeds maxBatchBytes");
  }
  const params: SqlScalar[] = [];
  for (const value of row) {
    bytes += 16;
    if (value instanceof Uint8Array) bytes += value.byteLength;
    else if (typeof value === "string") bytes += value.length * 2;
    else if (typeof value === "bigint" && (value < -(1n << 63n) || value >= 1n << 63n)) {
      throw new RangeError("Integer parameters must fit SQLite's signed 64-bit range");
    } else if (value !== null && !["number", "bigint", "boolean"].includes(typeof value)) {
      throw new TypeError(
        "Parameters must be SQL scalars (null, string, number, bigint, boolean or Uint8Array)",
      );
    }
    if (bytes > maxBytes) throw new RangeError("One input row exceeds maxBatchBytes");
    // Producers often reuse a scratch row/blob on every yield. Detach both
    // before the next next() call, not merely when posting the eventual batch.
    params.push(value instanceof Uint8Array ? new Uint8Array(value) : value);
  }
  if (bytes > maxBytes) throw new RangeError("One input row exceeds maxBatchBytes");
  return { params, bytes };
}

/** Must run inside the owning SDK transaction/child savepoint. */
export async function executeRowStream(
  client: StreamClient,
  sql: string,
  source: SqlRowSource,
  config: RowStreamConfig,
): Promise<ExecuteStreamResult> {
  let statementId: string | undefined;
  let iterator: RowIterator | undefined;
  let exhausted = false;
  let consumed = 0;
  let phase: StreamFailurePhase = "prepare";
  let rowIndex: number | undefined;
  let failure: FrankenStreamError | undefined;
  let rows: SqlScalar[][] = [];
  let bytes = 0;
  const totals: ExecuteStreamResult = { executions: 0, changes: 0, batches: 0 };

  const flush = async (): Promise<void> => {
    if (rows.length === 0) return;
    checkStreamCancellation(config, totals.executions);
    phase = "execute";
    rowIndex = undefined;
    const batch = rows;
    rows = [];
    bytes = 0;
    let result;
    try {
      result = await client.executePreparedMany(
        statementId!,
        batch,
        config.signal === undefined ? undefined : { signal: config.signal },
      );
    } catch (error: unknown) {
      if (
        error instanceof FrankenSQLiteError &&
        error.batchIndex !== undefined &&
        Number.isSafeInteger(error.batchIndex) &&
        error.batchIndex >= 0 &&
        error.batchIndex < batch.length
      ) {
        rowIndex = totals.executions + error.batchIndex;
      }
      if (error instanceof FrankenSQLiteError && error.code === "ERR_FSQLITE_BULK_CANCELLED") {
        throw new FrankenStreamError("cancelled", rowIndex, error);
      }
      throw error;
    }
    if (
      result.executions !== batch.length ||
      !Number.isSafeInteger(result.changes) ||
      result.changes < 0 ||
      !Number.isSafeInteger(totals.executions + result.executions) ||
      !Number.isSafeInteger(totals.changes + result.changes) ||
      !Number.isSafeInteger(totals.batches + 1)
    ) {
      throw new RangeError("Stream result counts are invalid or exceed the safe integer range");
    }
    totals.executions += result.executions;
    totals.changes += result.changes;
    totals.batches += 1;
    checkStreamCancellation(config, totals.executions);
    if (config.onProgress !== undefined) {
      phase = "progress";
      rowIndex = undefined;
      // Never lend mutable counters to application code and never present an
      // executed chunk as durable/committed. Rejections roll back the stream.
      await config.onProgress(Object.freeze({ ...totals, committed: false }));
      checkStreamCancellation(config, totals.executions);
    }
  };

  try {
    checkStreamCancellation(config, 0);
    const metadata = await client.prepare(sql);
    statementId = metadata.statementId;
    checkStreamCancellation(config, 0);
    if (metadata.columnCount !== 0)
      throw new TypeError("Stream execution does not accept result rows");
    phase = "source";
    rowIndex = 0;
    iterator = rowIterator(source);
    while (true) {
      phase = "source";
      rowIndex = consumed;
      checkStreamCancellation(config, consumed);
      const next = await iterator.next();
      if (next === null || typeof next !== "object")
        throw new TypeError("Iterator next() must return an object");
      if (next.done) {
        exhausted = true;
        checkStreamCancellation(config, consumed);
        break;
      }
      checkStreamCancellation(config, consumed);
      phase = "input";
      const row = copyRow(next.value, config.maxBatchBytes);
      if (rows.length > 0 && bytes + row.bytes > config.maxBatchBytes) await flush();
      rows.push(row.params);
      bytes += row.bytes;
      consumed += 1;
      if (!Number.isSafeInteger(consumed))
        throw new RangeError("Stream input count exceeds the safe integer range");
      // No concurrent next() and no prefetch while a worker batch is outstanding.
      if (rows.length === config.batchSize || bytes === config.maxBatchBytes) await flush();
    }
    await flush();
  } catch (error: unknown) {
    failure =
      error instanceof FrankenStreamError ? error : new FrankenStreamError(phase, rowIndex, error);
  }

  const cleanupErrors: unknown[] = [];
  if (iterator !== undefined && !exhausted) {
    try {
      const finish = iterator.return;
      if (finish !== undefined && finish !== null) {
        if (typeof finish !== "function") throw new TypeError("Iterator return must be a function");
        const returned = await finish.call(iterator);
        if (returned === null || typeof returned !== "object")
          throw new TypeError("Iterator return() must return an object");
      }
    } catch (error: unknown) {
      cleanupErrors.push(error);
    }
  }
  if (statementId !== undefined) {
    try {
      await client.finalizePrepared(statementId);
    } catch (error: unknown) {
      cleanupErrors.push(error);
    }
  }
  if (failure !== undefined) {
    throw new FrankenStreamError(failure.phase, failure.rowIndex, failure.cause, cleanupErrors);
  }
  if (cleanupErrors.length > 0) {
    throw new FrankenStreamError("cleanup", undefined, cleanupErrors[0], cleanupErrors.slice(1));
  }
  checkStreamCancellation(config, consumed);
  return totals;
}
