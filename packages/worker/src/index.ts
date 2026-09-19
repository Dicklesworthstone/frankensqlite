export {
  defaultCoreModuleLoader,
  serializeFrankenError,
  WorkerConnectionHost,
} from "./connection";
export * from "./protocol";
export { BindingError, MAX_BIND_PARAMETERS, parameterLayout, resolveBindings } from "./bindings";
export type { ParameterLayout } from "./bindings";
export { BINARY_RESULT_THRESHOLD, MAX_BINARY_RESULT_BYTES, ResultCodecError,
  decodeQueryResult, resolveResultEncoding, responseTransferList } from "./result-codec";
export type { ResultEncoding } from "./result-codec";
export { DEFAULT_REQUEST_LIMITS, RequestAdmissionError, RequestBudget, resolveRequestLimits } from "./admission";
export type { RequestLimits, RequestQueueStats } from "./admission";
export { DEFAULT_PREPARED_STATEMENT_LIMITS, PreparedStatementError, resolvePreparedStatementLimits } from "./statement-budget";
export type { PreparedStatementLimits, PreparedStatementStats } from "./statement-budget";
export { validateBulkSql } from "./bulk";
export { IndexedDbSnapshotStore, MAX_SNAPSHOT_BYTES, SnapshotStoreError, validateSnapshotBytes } from "./snapshot-store";
export { OpfsSnapshotStore } from "./opfs-snapshot-store";
export { validateManagedSql } from "./transactions";
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
