export type { RequestLimits, RequestQueueStats } from "./admission";
export {
  DEFAULT_REQUEST_LIMITS,
  RequestAdmissionError,
  RequestBudget,
  resolveRequestLimits,
} from "./admission";
export type { ParameterLayout } from "./bindings";
export { BindingError, MAX_BIND_PARAMETERS, parameterLayout, resolveBindings } from "./bindings";
export { validateBulkSql } from "./bulk";
export {
  defaultCoreModuleLoader,
  serializeFrankenError,
  WorkerConnectionHost,
} from "./connection";
export { OpfsSnapshotStore } from "./opfs-snapshot-store";
export * from "./protocol";
export type { ResultEncoding } from "./result-codec";
export {
  BINARY_RESULT_THRESHOLD,
  decodeQueryResult,
  MAX_BINARY_RESULT_BYTES,
  ResultCodecError,
  resolveResultEncoding,
  responseTransferList,
} from "./result-codec";
export type { SnapshotOwnership } from "./snapshot-ownership";
export { resolveSnapshotOwnership, SnapshotOwnershipError } from "./snapshot-ownership";
export type { SnapshotMetadata, StoredSnapshot } from "./snapshot-store";
export {
  IndexedDbSnapshotStore,
  MAX_SNAPSHOT_BYTES,
  SnapshotStoreError,
  validateSnapshotBytes,
} from "./snapshot-store";
export type { PreparedStatementLimits, PreparedStatementStats } from "./statement-budget";
export {
  DEFAULT_PREPARED_STATEMENT_LIMITS,
  PreparedStatementError,
  resolvePreparedStatementLimits,
} from "./statement-budget";
export { validateManagedSql } from "./transactions";
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

export function createFrankenSqliteWorker(options: FrankenSqliteWorkerOptions = {}): Worker {
  return new Worker(options.workerUrl ?? new URL("./worker.js", import.meta.url), {
    name: options.name ?? "frankensqlite-worker",
    type: "module",
  });
}
