import type {
  ExecuteManyResult,
  InitConfig,
  PersistenceMode,
  QueryResult as WorkerQueryResult,
  SerializedFrankenError,
  SqlScalar,
  SnapshotMetadata,
} from "@frankensqlite/worker";

import type { WorkerLike } from "./worker-client";

export type { ExecuteManyResult, PersistenceMode, SerializedFrankenError, SqlScalar, SnapshotMetadata };

export type QueryResult<Row extends Record<string, unknown> = Record<string, unknown>> =
  WorkerQueryResult<Row>;

export interface ExecuteManyOptions {
  /** Cooperative cancellation; the promise settles only after rollback/commit. */
  signal?: AbortSignal;
}

export interface FrankenDbOpenOptions
  extends Omit<InitConfig, "snapshot"> {
  snapshot?: Uint8Array;
  worker?: WorkerLike | (() => WorkerLike);
}
