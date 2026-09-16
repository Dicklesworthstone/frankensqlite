export {
  defaultCoreModuleLoader,
  serializeFrankenError,
  WorkerConnectionHost,
} from "./connection";
export * from "./protocol";
export { DEFAULT_REQUEST_LIMITS, RequestAdmissionError, RequestBudget, resolveRequestLimits } from "./admission";
export type { RequestLimits, RequestQueueStats } from "./admission";
export { validateBulkSql } from "./bulk";
export { IndexedDbSnapshotStore, MAX_SNAPSHOT_BYTES, SnapshotStoreError } from "./snapshot-store";
export type { SnapshotMetadata, StoredSnapshot } from "./snapshot-store";
export {
  assertSupportedPersistenceMode,
  createReadyResult,
  resolveDatabasePath,
  resolvePersistenceMode,
  UnsupportedPersistenceModeError,
} from "./vfs-init";

export interface FrankenSqliteWorkerOptions {
  name?: string;
  workerUrl?: URL;
}

export function createFrankenSqliteWorker(
  options: FrankenSqliteWorkerOptions = {},
): Worker {
  return new Worker(
    options.workerUrl ?? new URL("./worker.js", import.meta.url),
    {
      name: options.name ?? "frankensqlite-worker",
      type: "module",
    },
  );
}
