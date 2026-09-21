import type { QueryRequest } from "@frankensqlite/worker";
import { RequestBudget, validateManagedSql } from "@frankensqlite/worker";
import { FrankenSQLiteError } from "./errors";
import type { FrankenDBQueue, QueuedJobOptions } from "./queue";
import type { TableSubscription } from "./subscriptions";
import type { QueryResult, SqlBindings } from "./types";

export interface LiveQueryOptions extends QueuedJobOptions {
  /** Explicit ordinary main-table dependencies, including tables behind views. */
  tables: readonly string[];
  params?: SqlBindings;
  /** Captured SQL/parameter accounting, 256 bytes..64 MiB; default 1 MiB. */
  maxInputBytes?: number;
}

export interface LiveQueryResult<Row extends Record<string, unknown> = Record<string, unknown>>
  extends QueryResult<Row> {
  /** Local watched-commit high-water at this read, not a native MVCC/durability id. */
  readonly throughSequence: bigint;
}

export interface LiveQuery<Row extends Record<string, unknown> = Record<string, unknown>>
  extends AsyncIterableIterator<LiveQueryResult<Row>> {
  readonly tables: readonly string[];
  readonly closed: boolean;
  /** Joins an admitted read and its rollback/cleanup after stop or failure. */
  readonly done: Promise<void>;
}

function invalid(message: string): FrankenSQLiteError {
  return new FrankenSQLiteError({
    code: "ERR_FSQLITE_LIVE_QUERY_INPUT",
    message,
    transient: false,
  });
}

function selectOnly(sql: string): void {
  validateManagedSql(sql);
  let offset = 0;
  while (offset < sql.length) {
    if (/[\t\n\v\f\r \uFEFF;]/.test(sql[offset]!)) {
      offset++;
      continue;
    }
    if (sql.startsWith("--", offset)) {
      const end = sql.indexOf("\n", offset + 2);
      offset = end < 0 ? sql.length : end + 1;
    } else if (sql.startsWith("/*", offset)) {
      const end = sql.indexOf("*/", offset + 2);
      offset = end < 0 ? sql.length : end + 2;
    } else break;
  }
  if (!/^SELECT(?:$|[^A-Za-z0-9_$\u0080-\uFFFF])/i.test(sql.slice(offset))) {
    throw invalid(
      "Live queries require one SELECT; scripts, WITH, PRAGMA and writes are not replayed",
    );
  }
}

function cancellationOnly(error: unknown): boolean {
  if (error instanceof AggregateError)
    return error.errors.length > 0 && error.errors.every(cancellationOnly);
  return (
    error instanceof FrankenSQLiteError &&
    (error.code === "ERR_FSQLITE_JOB_CANCELLED" ||
      error.code === "ERR_FSQLITE_TRANSACTION_CANCELLED")
  );
}

/**
 * Demand-driven requery after committed local table invalidations. Registration
 * precedes the first read; no polling, query replay of writes, or result backlog.
 * SELECT functions must be side-effect free. This is not an SQL sandbox.
 */
export async function watchQuery<Row extends Record<string, unknown> = Record<string, unknown>>(
  queue: FrankenDBQueue,
  sql: string,
  options: LiveQueryOptions,
): Promise<LiveQuery<Row>> {
  const tables = options.tables;
  const params = options.params ?? [];
  const maxInputBytes = options.maxInputBytes ?? 1024 * 1024;
  const callerSignal = options.signal;
  const waitTimeoutMs = options.waitTimeoutMs;
  if (!Number.isInteger(maxInputBytes) || maxInputBytes < 256 || maxInputBytes > 64 * 1024 * 1024) {
    throw invalid("maxInputBytes must be an integer in 256..67108864");
  }
  if (callerSignal !== undefined) {
    Object.getOwnPropertyDescriptor(AbortSignal.prototype, "aborted")!.get!.call(callerSignal);
  }
  const stop = new AbortController();
  const signal = AbortSignal.any(
    callerSignal === undefined ? [stop.signal] : [stop.signal, callerSignal],
  );
  const settings: QueuedJobOptions = {
    signal,
    ...(waitTimeoutMs === undefined ? {} : { waitTimeoutMs }),
  };
  const admission = new RequestBudget({ maxPendingBytes: maxInputBytes }).admit({
    kind: "query",
    requestId: 0,
    sql,
    params,
  });
  let request: QueryRequest | null;
  try {
    const captured = admission.request as QueryRequest;
    selectOnly(captured.sql);
    const values = Array.isArray(captured.params)
      ? captured.params
      : Object.values(captured.params ?? {});
    for (const value of values) {
      if (value instanceof Uint8Array && !(value.buffer instanceof ArrayBuffer)) {
        throw invalid("Live-query bindings cannot retain shared backing buffers");
      }
    }
    request = structuredClone(captured);
  } finally {
    admission.release();
  }

  let subscription: TableSubscription | undefined;
  let closed = false;
  let failure: { cause: unknown } | null = null;
  let initial = true;
  let requestedSequence = -1n;
  let readSequence = -1n;
  let active: Promise<void> | null = null;
  let waiting: {
    resolve: (result: IteratorResult<LiveQueryResult<Row>>) => void;
    reject: (cause: unknown) => void;
  } | null = null;
  let resolveDone!: () => void;
  let rejectDone!: (cause: unknown) => void;
  const done = new Promise<void>((resolve, reject) => {
    resolveDone = resolve;
    rejectDone = reject;
  });
  void done.catch(() => {});

  function settleClosed(): void {
    if (!closed || active !== null) return;
    request = null;
    const pending = waiting;
    waiting = null;
    if (failure !== null) {
      pending?.reject(failure.cause);
      rejectDone(failure.cause);
    } else {
      pending?.resolve({ done: true, value: undefined });
      resolveDone();
    }
  }
  function finish(error: { cause: unknown } | null = null): void {
    if (error !== null && failure !== null && error.cause !== failure.cause) {
      failure = {
        cause: new AggregateError(
          [failure.cause, error.cause],
          "Live-query consumer and database both failed",
          { cause: failure.cause },
        ),
      };
    } else failure ??= error;
    closed = true;
    subscription?.unsubscribe();
    stop.abort();
    settleClosed();
  }
  function pump(): void {
    if (signal.aborted) finish();
    if (
      closed ||
      active !== null ||
      waiting === null ||
      (!initial && requestedSequence <= readSequence)
    )
      return;
    const captured = request!;
    const operation = async (): Promise<void> => {
      try {
        const value = await queue.transaction(async (tx) => {
          const result = await tx.query<Row>(captured.sql, captured.params);
          return Object.freeze({ ...result, throughSequence: queue.changeSequence });
        }, settings);
        if (closed) return;
        initial = false;
        readSequence = value.throughSequence;
        const pending = waiting;
        waiting = null;
        pending?.resolve({ done: false, value });
      } catch (cause: unknown) {
        // Explicit stop cancels our read but never hides an unrelated SQL or
        // rollback failure. Its owning queue remains responsible for cleanup.
        if (!closed || !cancellationOnly(cause)) finish({ cause });
      } finally {
        active = null;
        settleClosed();
      }
    };
    active = operation();
  }

  try {
    subscription = await queue.subscribe(
      tables,
      (change) => {
        if (change.lastSequence > requestedSequence) requestedSequence = change.lastSequence;
        pump();
      },
      settings,
    );
  } catch (cause: unknown) {
    finish({ cause });
    throw cause;
  }
  void subscription.done.then(
    () => finish(),
    (cause) => finish({ cause }),
  );
  if (subscription.state !== "active") {
    finish(subscription.state === "failed" ? { cause: subscription.failure } : null);
  }
  const iterator: LiveQuery<Row> = Object.freeze({
    tables: subscription.tables,
    get closed() {
      return closed;
    },
    done,
    next(): Promise<IteratorResult<LiveQueryResult<Row>>> {
      if (signal.aborted) finish();
      if (closed) return done.then(() => ({ done: true, value: undefined }));
      if (waiting !== null)
        return Promise.reject(
          new FrankenSQLiteError({
            code: "ERR_FSQLITE_LIVE_QUERY_NEXT_PENDING",
            transient: false,
            message: "Await the outstanding live-query next() before requesting another result",
          }),
        );
      const result = new Promise<IteratorResult<LiveQueryResult<Row>>>((resolve, reject) => {
        waiting = { resolve, reject };
      });
      pump();
      return result;
    },
    async return(): Promise<IteratorResult<LiveQueryResult<Row>>> {
      finish();
      await done;
      return { done: true, value: undefined };
    },
    async throw(cause?: unknown): Promise<IteratorResult<LiveQueryResult<Row>>> {
      finish({ cause });
      await done;
      throw cause;
    },
    [Symbol.asyncIterator]() {
      return iterator;
    },
  });
  return iterator;
}
