import type {
  ExecuteManyResult,
  InitConfig,
  PersistenceMode,
  QueryResult as WorkerQueryResult,
  SerializedFrankenError,
  SqlScalar,
  SnapshotMetadata,
  RequestLimits,
  RequestQueueStats,
} from "@frankensqlite/worker";

import type { WorkerLike } from "./worker-client";

export type { ExecuteManyResult, PersistenceMode, SerializedFrankenError, SqlScalar, SnapshotMetadata };
export type { RequestLimits, RequestQueueStats };

export type QueryResult<Row extends Record<string, unknown> = Record<string, unknown>> =
  WorkerQueryResult<Row>;

export interface ExecuteManyOptions {
  /** Cooperative cancellation; the promise settles only after rollback/commit. */
  signal?: AbortSignal;
}

export interface TransactionOptions {
  /** Cancel the whole scope; settles after callback, SQL and rollback drain. */
  signal?: AbortSignal;
}

/** A pull-based source. Each yielded value is one positional parameter set. */
export type SqlRowSource =
  | Iterable<readonly SqlScalar[]>
  | AsyncIterable<readonly SqlScalar[]>;

export interface ExecuteStreamOptions {
  /** Maximum rows per worker request, 1..10,000. Defaults to 256. */
  batchSize?: number;
  /**
   * Accounted parameter bytes per batch, 1..64 MiB. Defaults to 1 MiB.
   * Counts UTF-16 text, blob bytes and fixed row/value allowances, not heap/RSS.
   * A row larger than this budget is rejected; one lookahead row may be held.
   */
  maxBatchBytes?: number;
  /** Cooperatively cancel input consumption and undo the entire stream. */
  signal?: AbortSignal;
  /** Awaited between chunks; all counts are provisional until the outer commit. */
  onProgress?: (progress: Readonly<ExecuteStreamProgress>) => void | Promise<void>;
}

/** Aggregate counts only: no result array proportional to the input length. */
export interface ExecuteStreamResult {
  executions: number;
  changes: number;
  batches: number;
}

export interface ExecuteStreamProgress extends ExecuteStreamResult {
  /** A delivered chunk is not a committed import. */
  committed: false;
}

export interface FrankenDbOpenOptions
  extends Omit<InitConfig, "snapshot"> {
  snapshot?: Uint8Array;
  worker?: WorkerLike | (() => WorkerLike);
  /** Bound active + queued request count and accounted payload before IPC. */
  requestLimits?: Partial<RequestLimits>;
}
