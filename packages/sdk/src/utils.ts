import { createFrankenSqliteWorker, isSnapshotPersistenceMode } from "@frankensqlite/worker";
import { FrankenSQLiteError } from "./errors";

import type { CheckpointRecoveryIdentity, FrankenDbOpenOptions } from "./types";
import type { WorkerLike } from "./worker-client";

export function normalizeOpenOptions(
  options?: FrankenDbOpenOptions | string,
): FrankenDbOpenOptions {
  if (typeof options === "string") {
    return { dbName: options };
  }
  return {
    persistence: "memory",
    ...options,
  };
}

export function resolveWorker(
  worker: FrankenDbOpenOptions["worker"],
): WorkerLike {
  if (typeof worker === "function") {
    return worker();
  }
  if (worker) {
    return worker;
  }
  return createFrankenSqliteWorker();
}

/** Capture the reopen precondition before allocating a worker or moving bytes. */
export function captureRequiredCheckpoint(options: FrankenDbOpenOptions): Readonly<CheckpointRecoveryIdentity> | null {
  const source = options.requireCheckpoint;
  if (source === undefined) return null;
  const invalid = (): never => {
    throw new FrankenSQLiteError({ code: "ERR_FSQLITE_SNAPSHOT_INPUT", transient: false,
      message: "requireCheckpoint must identify a publication in the requested snapshot database, without an initialization image" });
  };
  if (typeof source !== "object" || source === null || Array.isArray(source)) return invalid();
  const { path, persistence, publicationId, parentRevision } = source;
  const revision = (value: unknown): value is string => typeof value === "string" &&
    /^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/.test(value);
  if (typeof path !== "string" || path.trim().length === 0 || path.length > 256 || path.includes("\0") || path === ":memory:" ||
      !isSnapshotPersistenceMode(persistence) || persistence !== options.persistence || path !== options.dbName ||
      !revision(publicationId) || (parentRevision !== null && !revision(parentRevision)) ||
      publicationId === parentRevision || options.snapshot !== undefined) return invalid();
  return Object.freeze({ path, persistence, publicationId, parentRevision });
}
