import type {
  ExecuteBatchResponse,
  ExecuteResponse,
  ExportResponse,
  InitConfig,
  PrepareResponse,
  QueryResponse,
  ReadyResponse,
  SerializedFrankenError,
  StatementFinalizeResponse,
  WorkerRequest,
  WorkerResponse,
} from "./protocol";
import {
  assertSupportedPersistenceMode,
  createReadyResult,
  resolveDatabasePath,
  resolvePersistenceMode,
  UnsupportedPersistenceModeError,
} from "./vfs-init";

export interface CorePreparedStatementHandle {
  readonly sql: string;
  readonly columnCount: number;
  columnNames(): string[];
  execute(): number;
  executeWithParams(params: unknown[]): number;
  query(): QueryResponse["data"];
  queryWithParams(params: unknown[]): QueryResponse["data"];
}

export interface CoreDatabaseHandle {
  readonly path: string;
  close(): void;
  execute(sql: string): number;
  executeBatch(sql: string): void;
  executeWithParams(sql: string, params: unknown[]): number;
  query(sql: string): QueryResponse["data"];
  queryWithParams(sql: string, params: unknown[]): QueryResponse["data"];
  prepare(sql: string): CorePreparedStatementHandle;
  export(): Uint8Array;
}

export interface CoreDatabaseConstructor {
  new (path?: string): CoreDatabaseHandle;
  import(data: Uint8Array): CoreDatabaseHandle;
}

export interface CoreModule {
  FrankenDB: CoreDatabaseConstructor;
}

export interface CoreModuleLoader {
  load(wasmUrl?: string): Promise<CoreModule>;
}

export const defaultCoreModuleLoader: CoreModuleLoader = {
  async load(wasmUrl?: string): Promise<CoreModule> {
    const core = await import("@frankensqlite/core");
    await core.default(wasmUrl);
    return {
      FrankenDB: core.FrankenDB as unknown as CoreDatabaseConstructor,
    };
  },
};

export class WorkerConnectionHost {
  readonly #loader: CoreModuleLoader;
  #db: CoreDatabaseHandle | null = null;
  #nextStatementId = 1;
  readonly #statements = new Map<string, CorePreparedStatementHandle>();

  constructor(loader: CoreModuleLoader = defaultCoreModuleLoader) {
    this.#loader = loader;
  }

  async handle(request: WorkerRequest): Promise<WorkerResponse> {
    try {
      switch (request.kind) {
        case "init":
          return await this.#initialize(request.requestId, request.config);
        case "execute":
          return this.#execute(
            request.requestId,
            request.sql,
            request.params ?? [],
          );
        case "execute-batch":
          return this.#executeBatch(request.requestId, request.sql);
        case "query":
          return this.#query(
            request.requestId,
            request.sql,
            request.params ?? [],
          );
        case "prepare":
          return this.#prepare(request.requestId, request.sql);
        case "statement-execute":
          return this.#statementExecute(
            request.requestId,
            request.statementId,
            request.params ?? [],
          );
        case "statement-query":
          return this.#statementQuery(
            request.requestId,
            request.statementId,
            request.params ?? [],
          );
        case "statement-finalize":
          return this.#statementFinalize(request.requestId, request.statementId);
        case "export":
          return this.#exportSnapshot(request.requestId);
        case "close":
          return this.#close(request.requestId);
      }
    } catch (error: unknown) {
      return {
        kind: "error",
        requestId: request.requestId,
        error: serializeFrankenError(error),
      };
    }
  }

  async #initialize(
    requestId: number,
    config: InitConfig,
  ): Promise<ReadyResponse> {
    const ready = createReadyResult(config);
    assertSupportedPersistenceMode(ready.persistence);

    const core = await this.#loader.load(config.wasmUrl);
    this.#disposeDatabase();
    this.#db = config.snapshot
      ? core.FrankenDB.import(config.snapshot)
      : new core.FrankenDB(resolveDatabasePath(config));

    return {
      kind: "ready",
      requestId,
      data: {
        path: this.#db.path || ready.path,
        persistence: resolvePersistenceMode(config.persistence),
      },
    };
  }

  #execute(
    requestId: number,
    sql: string,
    params: readonly unknown[],
  ): ExecuteResponse {
    const db = this.#requireDatabase();
    const changes =
      params.length === 0 ? db.execute(sql) : db.executeWithParams(sql, [...params]);
    return {
      kind: "execute-result",
      requestId,
      changes,
    };
  }

  #executeBatch(requestId: number, sql: string): ExecuteBatchResponse {
    this.#requireDatabase().executeBatch(sql);
    return {
      kind: "execute-batch-result",
      requestId,
    };
  }

  #query(
    requestId: number,
    sql: string,
    params: readonly unknown[],
  ): QueryResponse {
    const db = this.#requireDatabase();
    const data =
      params.length === 0 ? db.query(sql) : db.queryWithParams(sql, [...params]);
    return {
      kind: "query-result",
      requestId,
      data,
    };
  }

  #prepare(requestId: number, sql: string): PrepareResponse {
    const stmt = this.#requireDatabase().prepare(sql);
    const statementId = String(this.#nextStatementId++);
    this.#statements.set(statementId, stmt);
    return {
      kind: "prepare-result",
      requestId,
      data: {
        statementId,
        sql: stmt.sql,
        columnCount: stmt.columnCount,
        columnNames: stmt.columnNames(),
      },
    };
  }

  #statementExecute(
    requestId: number,
    statementId: string,
    params: readonly unknown[],
  ): ExecuteResponse {
    const stmt = this.#requireStatement(statementId);
    const changes =
      params.length === 0 ? stmt.execute() : stmt.executeWithParams([...params]);
    return {
      kind: "execute-result",
      requestId,
      changes,
    };
  }

  #statementQuery(
    requestId: number,
    statementId: string,
    params: readonly unknown[],
  ): QueryResponse {
    const stmt = this.#requireStatement(statementId);
    const data =
      params.length === 0 ? stmt.query() : stmt.queryWithParams([...params]);
    return {
      kind: "query-result",
      requestId,
      data,
    };
  }

  #statementFinalize(
    requestId: number,
    statementId: string,
  ): StatementFinalizeResponse {
    this.#requireStatement(statementId);
    this.#statements.delete(statementId);
    return {
      kind: "statement-finalize-result",
      requestId,
    };
  }

  #exportSnapshot(requestId: number): ExportResponse {
    return {
      kind: "export-result",
      requestId,
      data: this.#requireDatabase().export(),
    };
  }

  #close(requestId: number): WorkerResponse {
    this.#disposeDatabase();
    return {
      kind: "close-result",
      requestId,
    };
  }

  #disposeDatabase(): void {
    this.#statements.clear();
    this.#db?.close();
    this.#db = null;
  }

  #requireDatabase(): CoreDatabaseHandle {
    if (this.#db === null) {
      throw new Error("FrankenSQLite worker is not initialized");
    }
    return this.#db;
  }

  #requireStatement(statementId: string): CorePreparedStatementHandle {
    const stmt = this.#statements.get(statementId);
    if (stmt === undefined) {
      throw new Error(`Unknown prepared statement id \`${statementId}\``);
    }
    return stmt;
  }
}

export function serializeFrankenError(
  error: unknown,
): SerializedFrankenError {
  const code =
    error instanceof UnsupportedPersistenceModeError
      ? error.code
      : extractStringProperty(error, "code") ?? "ERR_FSQLITE_WORKER";

  const serialized: SerializedFrankenError = {
    code,
    message:
      error instanceof Error
        ? error.message
        : typeof error === "string"
          ? error
          : "Unknown FrankenSQLite worker error",
  };

  const sqliteCode = extractNumberProperty(error, "sqliteCode");
  if (sqliteCode !== undefined) {
    serialized.sqliteCode = sqliteCode;
  }
  const extendedCode = extractNumberProperty(error, "extendedCode");
  if (extendedCode !== undefined) {
    serialized.extendedCode = extendedCode;
  }
  const transient = extractBooleanProperty(error, "transient");
  if (transient !== undefined) {
    serialized.transient = transient;
  }
  const userRecoverable = extractBooleanProperty(error, "userRecoverable");
  if (userRecoverable !== undefined) {
    serialized.userRecoverable = userRecoverable;
  }
  const suggestion = extractStringProperty(error, "suggestion");
  if (suggestion !== undefined) {
    serialized.suggestion = suggestion;
  }
  if (error instanceof Error && error.stack !== undefined) {
    serialized.stack = error.stack;
  }

  return serialized;
}

function extractStringProperty(
  value: unknown,
  key: string,
): string | undefined {
  if (typeof value !== "object" || value === null) {
    return undefined;
  }
  const property = Reflect.get(value, key);
  return typeof property === "string" ? property : undefined;
}

function extractNumberProperty(
  value: unknown,
  key: string,
): number | undefined {
  if (typeof value !== "object" || value === null) {
    return undefined;
  }
  const property = Reflect.get(value, key);
  return typeof property === "number" ? property : undefined;
}

function extractBooleanProperty(
  value: unknown,
  key: string,
): boolean | undefined {
  if (typeof value !== "object" || value === null) {
    return undefined;
  }
  const property = Reflect.get(value, key);
  return typeof property === "boolean" ? property : undefined;
}
