import type { SnapshotMetadata } from "./snapshot-store";
import type { ResultEncoding } from "./result-codec";
import type { PreparedStatementLimits } from "./statement-budget";

export type PersistenceMode = "memory" | "opfs" | "indexeddb" | "indexeddb-snapshot" | "opfs-snapshot";

/** Whole-image checkpoints; neither mode is a page-level storage VFS. */
export function isSnapshotPersistenceMode(value: unknown): value is "indexeddb-snapshot" | "opfs-snapshot" {
  return value === "indexeddb-snapshot" || value === "opfs-snapshot";
}

export type SqlScalar =
  | null
  | string
  | number
  | bigint
  | boolean
  | Uint8Array;

export type SqlParams = readonly SqlScalar[];

/** Complete named bindings use exact SQL prefixes or unambiguous bare names. */
export type SqlBindings = SqlParams | Readonly<Record<string, SqlScalar>>;

/** A batch is never silently split: all executions share one savepoint. */
export const MAX_EXECUTE_MANY_ROWS = 10_000;

export interface ExecuteManyResult {
  executions: number;
  changes: number;
  changesPerExecution: number[];
}

export interface QueryResult<Row extends Record<string, unknown> = Record<string, unknown>> {
  columns: string[];
  columnCount: number;
  columnTypes: string[];
  rows: Row[];
  rowArrays: SqlScalar[][];
  changes: number;
}

export interface InitConfig {
  dbName?: string;
  persistence?: PersistenceMode;
  wasmUrl?: string;
  snapshot?: Uint8Array;
  /** Opt in to FQR1 transfer; unsupported result shapes still use structured clone. */
  resultEncoding?: ResultEncoding;
  /** Request retained-handle limits. The worker may enforce tighter ceilings. */
  preparedStatementLimits?: Partial<PreparedStatementLimits>;
}

export interface InitResult {
  path: string;
  persistence: PersistenceMode;
  /** Version 1 supports client publication identities and read-only recovery. */
  checkpointRecovery?: 1;
  /** Last explicit checkpoint; absent for non-snapshot modes. */
  snapshot?: SnapshotMetadata | null;
  /** Absent on older workers, which only return structured-clone query results. */
  resultEncoding?: ResultEncoding;
  /** Absent on older workers: no prepared-resource policy was acknowledged. */
  preparedStatementLimits?: Readonly<PreparedStatementLimits>;
}

export interface PreparedStatementMetadata {
  statementId: string;
  sql: string;
  columnCount: number;
  columnNames: string[];
  parameterCount?: number;
  parameterNames?: readonly (string | null)[];
}

export interface SerializedFrankenError {
  code: string;
  message: string;
  sqliteCode?: number;
  extendedCode?: number;
  transient?: boolean;
  userRecoverable?: boolean;
  suggestion?: string;
  stack?: string;
  /** Zero-based input row that failed; absent for boundary/cleanup failures. */
  batchIndex?: number;
  cause?: SerializedFrankenError;
  cleanupErrors?: SerializedFrankenError[];
}

interface WorkerRequestBase {
  requestId: number;
  /** Explicit worker-side ownership; absent for manually managed operations. */
  transactionId?: string;
}

export interface TransactionRequest extends WorkerRequestBase {
  kind: "transaction";
  transactionId: string;
  action: "begin" | "commit" | "rollback";
  /** Required for a nested begin, absent for a top-level begin. */
  parentId?: string;
}

export interface TransactionResponse extends WorkerResponseBase {
  kind: "transaction-result";
}

interface WorkerResponseBase {
  requestId: number;
}

export interface InitRequest extends WorkerRequestBase {
  kind: "init";
  config: InitConfig;
}

export interface ExecuteRequest extends WorkerRequestBase {
  kind: "execute";
  sql: string;
  params?: SqlBindings;
}

export interface ExecuteBatchRequest extends WorkerRequestBase {
  kind: "execute-batch";
  sql: string;
}

export interface ExecuteManyRequest extends WorkerRequestBase {
  kind: "execute-many";
  sql: string;
  parameterSets: readonly SqlParams[];
  cancellable?: boolean;
}

export interface StatementExecuteManyRequest extends WorkerRequestBase {
  kind: "statement-execute-many";
  statementId: string;
  parameterSets: readonly SqlParams[];
  cancellable?: boolean;
}

/** Out-of-band control: requests cancellation, never performs database work. */
export interface CancelBulkRequest extends WorkerRequestBase {
  kind: "cancel-bulk";
  targetRequestId: number;
}

/** Fence an active managed scope and its descendants, without running SQL. */
export interface CancelTransactionRequest extends WorkerRequestBase {
  kind: "cancel-transaction";
  targetTransactionId: string;
}

export interface QueryRequest extends WorkerRequestBase {
  kind: "query";
  sql: string;
  params?: SqlBindings;
}

export interface PrepareRequest extends WorkerRequestBase {
  kind: "prepare";
  sql: string;
}

export interface StatementExecuteRequest extends WorkerRequestBase {
  kind: "statement-execute";
  statementId: string;
  params?: SqlBindings;
}

export interface StatementQueryRequest extends WorkerRequestBase {
  kind: "statement-query";
  statementId: string;
  params?: SqlBindings;
}

export interface StatementFinalizeRequest extends WorkerRequestBase {
  kind: "statement-finalize";
  statementId: string;
}

export interface ExportRequest extends WorkerRequestBase {
  kind: "export";
}

export interface CloseRequest extends WorkerRequestBase {
  kind: "close";
}

export interface CheckpointRequest extends WorkerRequestBase {
  kind: "checkpoint";
  /** UUID-v4 selected before dispatch, retained across a lost acknowledgement. */
  publicationId?: string;
}

export interface CheckpointRecoverRequest extends WorkerRequestBase {
  kind: "checkpoint-recover";
  publicationId: string;
  parentRevision: string | null;
}

export type WorkerRequest =
  | TransactionRequest
  | InitRequest
  | ExecuteRequest
  | ExecuteBatchRequest
  | ExecuteManyRequest
  | CancelBulkRequest
  | CancelTransactionRequest
  | QueryRequest
  | PrepareRequest
  | StatementExecuteRequest
  | StatementExecuteManyRequest
  | StatementQueryRequest
  | StatementFinalizeRequest
  | ExportRequest
  | CheckpointRequest
  | CheckpointRecoverRequest
  | CloseRequest;

export interface ReadyResponse extends WorkerResponseBase {
  kind: "ready";
  data: InitResult;
}

export interface ExecuteResponse extends WorkerResponseBase {
  kind: "execute-result";
  changes: number;
}

export interface ExecuteBatchResponse extends WorkerResponseBase {
  kind: "execute-batch-result";
}

export interface ExecuteManyResponse extends WorkerResponseBase {
  kind: "execute-many-result";
  data: ExecuteManyResult;
}

export interface CancelBulkResponse extends WorkerResponseBase {
  kind: "cancel-bulk-result";
  /** Accepted before commit dispatch; NOT proof that rollback has completed. */
  accepted: boolean;
}

export interface CancelTransactionResponse extends WorkerResponseBase {
  kind: "cancel-transaction-result";
  /** The scope was fenced before COMMIT dispatch; rollback is still pending. */
  accepted: boolean;
}

export interface QueryResponse extends WorkerResponseBase {
  kind: "query-result";
  data: QueryResult;
}

export interface BinaryQueryResponse extends WorkerResponseBase {
  kind: "query-binary-result";
  encoding: "fqr1";
  /** Fresh owned bytes, transferred once; never a core/WASM backing allocation. */
  data: ArrayBuffer;
}

export interface PrepareResponse extends WorkerResponseBase {
  kind: "prepare-result";
  data: PreparedStatementMetadata;
}

export interface StatementFinalizeResponse extends WorkerResponseBase {
  kind: "statement-finalize-result";
}

export interface ExportResponse extends WorkerResponseBase {
  kind: "export-result";
  data: Uint8Array;
}

export interface CloseResponse extends WorkerResponseBase {
  kind: "close-result";
}

export interface CheckpointResponse extends WorkerResponseBase {
  kind: "checkpoint-result";
  data: SnapshotMetadata;
}

export interface ErrorResponse extends WorkerResponseBase {
  kind: "error";
  error: SerializedFrankenError;
}

/** A connection-wide transport failure cannot be attributed to a request id. */
export interface WorkerFatalMessage {
  kind: "worker-fatal";
  error: SerializedFrankenError;
}

export type WorkerMessage = WorkerResponse | WorkerFatalMessage;

export type WorkerResponse =
  | TransactionResponse
  | ReadyResponse
  | ExecuteResponse
  | ExecuteBatchResponse
  | ExecuteManyResponse
  | CancelBulkResponse
  | CancelTransactionResponse
  | QueryResponse
  | BinaryQueryResponse
  | PrepareResponse
  | StatementFinalizeResponse
  | ExportResponse
  | CheckpointResponse
  | CloseResponse
  | ErrorResponse;
