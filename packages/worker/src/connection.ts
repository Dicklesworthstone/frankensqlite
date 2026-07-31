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
  free(): void;
  columnNames(): string[];
  execute(): Promise<number>;
  executeWithParams(params: unknown[]): Promise<number>;
  query(): Promise<QueryResponse["data"]>;
  queryWithParams(params: unknown[]): Promise<QueryResponse["data"]>;
}

export interface CoreDatabaseHandle {
  readonly path: string;
  free(): void;
  close(): void;
  execute(sql: string): Promise<number>;
  executeBatch(sql: string): Promise<void>;
  executeWithParams(sql: string, params: unknown[]): Promise<number>;
  query(sql: string): Promise<QueryResponse["data"]>;
  queryWithParams(
    sql: string,
    params: unknown[],
  ): Promise<QueryResponse["data"]>;
  prepare(sql: string): Promise<CorePreparedStatementHandle>;
  export(): Promise<Uint8Array>;
}

export interface CoreDatabaseConstructor {
  create(path?: string): Promise<CoreDatabaseHandle>;
  import(data: Uint8Array): Promise<CoreDatabaseHandle>;
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
      FrankenDB: core.FrankenDB,
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
          return await this.#execute(
            request.requestId,
            request.sql,
            request.params ?? [],
          );
        case "execute-batch":
          return await this.#executeBatch(request.requestId, request.sql);
        case "query":
          return await this.#query(
            request.requestId,
            request.sql,
            request.params ?? [],
          );
        case "prepare":
          return await this.#prepare(request.requestId, request.sql);
        case "statement-execute":
          return await this.#statementExecute(
            request.requestId,
            request.statementId,
            request.params ?? [],
          );
        case "statement-query":
          return await this.#statementQuery(
            request.requestId,
            request.statementId,
            request.params ?? [],
          );
        case "statement-finalize":
          return this.#statementFinalize(request.requestId, request.statementId);
        case "export":
          return await this.#exportSnapshot(request.requestId);
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
      ? await core.FrankenDB.import(config.snapshot)
      : await core.FrankenDB.create(resolveDatabasePath(config));

    return {
      kind: "ready",
      requestId,
      data: {
        path: this.#db.path || ready.path,
        persistence: resolvePersistenceMode(config.persistence),
      },
    };
  }

  async #execute(
    requestId: number,
    sql: string,
    params: readonly unknown[],
  ): Promise<ExecuteResponse> {
    const db = this.#requireDatabase();
    const changes =
      params.length === 0
        ? await db.execute(sql)
        : await db.executeWithParams(sql, [...params]);
    return {
      kind: "execute-result",
      requestId,
      changes,
    };
  }

  async #executeBatch(
    requestId: number,
    sql: string,
  ): Promise<ExecuteBatchResponse> {
    await this.#requireDatabase().executeBatch(sql);
    return {
      kind: "execute-batch-result",
      requestId,
    };
  }

  async #query(
    requestId: number,
    sql: string,
    params: readonly unknown[],
  ): Promise<QueryResponse> {
    const db = this.#requireDatabase();
    const data =
      params.length === 0
        ? await db.query(sql)
        : await db.queryWithParams(sql, [...params]);
    return {
      kind: "query-result",
      requestId,
      data,
    };
  }

  async #prepare(requestId: number, sql: string): Promise<PrepareResponse> {
    const stmt = await this.#requireDatabase().prepare(sql);
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

  async #statementExecute(
    requestId: number,
    statementId: string,
    params: readonly unknown[],
  ): Promise<ExecuteResponse> {
    const stmt = this.#requireStatement(statementId);
    const changes =
      params.length === 0
        ? await stmt.execute()
        : await stmt.executeWithParams([...params]);
    return {
      kind: "execute-result",
      requestId,
      changes,
    };
  }

  async #statementQuery(
    requestId: number,
    statementId: string,
    params: readonly unknown[],
  ): Promise<QueryResponse> {
    const stmt = this.#requireStatement(statementId);
    const data =
      params.length === 0
        ? await stmt.query()
        : await stmt.queryWithParams([...params]);
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
    const stmt = this.#requireStatement(statementId);
    this.#statements.delete(statementId);
    stmt.free();
    return {
      kind: "statement-finalize-result",
      requestId,
    };
  }

  async #exportSnapshot(requestId: number): Promise<ExportResponse> {
    return {
      kind: "export-result",
      requestId,
      data: await this.#requireDatabase().export(),
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
    const statements = [...this.#statements.values()];
    this.#statements.clear();
    const db = this.#db;
    this.#db = null;

    let firstError: unknown;
    for (const stmt of statements) {
      try {
        stmt.free();
      } catch (error: unknown) {
        firstError ??= error;
      }
    }
    if (db !== null) {
      try {
        db.close();
      } catch (error: unknown) {
        firstError ??= error;
      }
      try {
        db.free();
      } catch (error: unknown) {
        firstError ??= error;
      }
    }
    if (firstError !== undefined) {
      throw firstError;
    }
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
