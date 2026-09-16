import { FrankenPreparedStatement } from "./statement";
import { FrankenTransaction } from "./transaction";
import type { ExecuteManyOptions, ExecuteManyResult, FrankenDbOpenOptions, PersistenceMode, QueryResult, SqlScalar, SnapshotMetadata } from "./types";
import { normalizeOpenOptions, resolveWorker } from "./utils";
import { FrankenWorkerClient } from "./worker-client";
import { FrankenSQLiteError } from "./errors";

interface TransactionScope {
  accepting: boolean;
  pending: Set<Promise<unknown>>;
  statements: Set<string>;
  errors: unknown[];
  children: Set<Promise<unknown>>;
}

export class FrankenDB {
  readonly #client: FrankenWorkerClient;
  readonly #path: string;
  readonly #persistence: PersistenceMode;
  #snapshotRevision: string | null;
  #transactionScope: TransactionScope | null = null;
  #transactionFailure: Error | null = null;
  #nextSavepointId = 1n;

  private constructor(client: FrankenWorkerClient, path: string, persistence: PersistenceMode, snapshotRevision: string | null) {
    this.#client = client;
    this.#path = path;
    this.#persistence = persistence;
    this.#snapshotRevision = snapshotRevision;
  }

  static async open(options?: FrankenDbOpenOptions | string): Promise<FrankenDB> {
    const normalized = normalizeOpenOptions(options);
    const client = new FrankenWorkerClient(resolveWorker(normalized.worker));
    const config: FrankenDbOpenOptions = {};
    if (normalized.dbName !== undefined) {
      config.dbName = normalized.dbName;
    }
    if (normalized.persistence !== undefined) {
      config.persistence = normalized.persistence;
    }
    if (normalized.wasmUrl !== undefined) {
      config.wasmUrl = normalized.wasmUrl;
    }
    if (normalized.snapshot !== undefined) {
      config.snapshot = normalized.snapshot;
    }
    try {
      const ready = await client.init(config);
      return new FrankenDB(client, ready.path, ready.persistence, ready.snapshot?.revision ?? null);
    } catch (error: unknown) {
      try {
        client.dispose();
      } catch (cleanupError: unknown) {
        throw new AggregateError([error, cleanupError],
          "FrankenSQLite initialization and worker cleanup both failed", { cause: error });
      }
      throw error;
    }
  }

  static import(
    snapshot: Uint8Array,
    options?: Omit<FrankenDbOpenOptions, "snapshot">,
  ): Promise<FrankenDB> {
    return FrankenDB.open({
      ...options,
      snapshot,
    });
  }

  get path(): string {
    return this.#path;
  }

  get persistence(): PersistenceMode {
    return this.#persistence;
  }

  /** Last loaded/published checkpoint, not the state of unsaved memory writes. */
  get snapshotRevision(): string | null {
    return this.#snapshotRevision;
  }

  execute(sql: string, params: readonly SqlScalar[] = []): Promise<number> {
    return this.#run(null, () => this.#client.execute(sql, params));
  }

  executeBatch(sql: string): Promise<void> {
    return this.#run(null, () => this.#client.executeBatch(sql));
  }

  /** Execute one prepared DML statement for every parameter set, atomically. */
  executeMany(
    sql: string,
    parameterSets: readonly (readonly SqlScalar[])[],
    options?: ExecuteManyOptions,
  ): Promise<ExecuteManyResult> {
    return this.#run(null, () => this.#client.executeMany(sql, parameterSets, options));
  }

  query<Row extends Record<string, unknown> = Record<string, unknown>>(
    sql: string,
    params: readonly SqlScalar[] = [],
  ): Promise<QueryResult<Row>> {
    return this.#run(null, () => this.#client.query<Row>(sql, params));
  }

  prepare<Row extends Record<string, unknown> = Record<string, unknown>>(
    sql: string,
  ): Promise<FrankenPreparedStatement<Row>> {
    return this.#run(null, () => this.#prepare<Row>(sql, null));
  }

  async #prepare<Row extends Record<string, unknown>>(
    sql: string,
    scope: TransactionScope | null,
  ): Promise<FrankenPreparedStatement<Row>> {
    const metadata = await this.#client.prepare(sql);
    scope?.statements.add(metadata.statementId);
    return new FrankenPreparedStatement<Row>(
      this.#client,
      metadata.statementId,
      metadata.sql,
      metadata.columnCount,
      metadata.columnNames,
      (operation) => this.#run(scope, operation),
      () => { scope?.statements.delete(metadata.statementId); },
    );
  }

  export(): Promise<Uint8Array> {
    return this.#run(null, () => this.#client.export());
  }

  /** Publish an explicit whole-image checkpoint after all SQL transactions end. */
  checkpoint(): Promise<SnapshotMetadata> {
    return this.#run(null, async () => {
      const saved = await this.#client.checkpoint();
      this.#snapshotRevision = saved.revision;
      return saved;
    });
  }

  transaction<T>(
    work: (tx: FrankenTransaction) => T | Promise<T>,
  ): Promise<T> {
    return this.#transaction(null, work);
  }

  async #transaction<T>(
    parent: TransactionScope | null,
    work: (tx: FrankenTransaction) => T | Promise<T>,
  ): Promise<T> {
    this.#assertOwner(parent);
    const scope: TransactionScope = {
      accepting: true, pending: new Set(), statements: new Set(), errors: [],
      children: new Set(),
    };
    const savepoint = parent === null ? null : `fsqlite_sdk_${this.#nextSavepointId++}`;
    // Claim before the first await so foreign operations cannot enter between
    // BEGIN and the callback, or while the callback awaits application work.
    this.#transactionScope = scope;
    let began = false;
    try {
      await this.#client.executeBatch(savepoint === null ? "BEGIN" : `SAVEPOINT ${savepoint}`);
      began = true;
      const result = await this.#finishScope(scope, work);
      await this.#client.executeBatch(savepoint === null ? "COMMIT" : `RELEASE SAVEPOINT ${savepoint}`);
      return result;
    } catch (error: unknown) {
      // A failed BEGIN does not authorize rolling back an existing transaction.
      if (began && this.#transactionFailure === null) {
        try {
          if (savepoint === null) {
            await this.#client.executeBatch("ROLLBACK");
          } else {
            // ROLLBACK TO keeps the savepoint on the stack. RELEASE it only
            // after rollback succeeds, never after a failed rollback-to.
            await this.#client.executeBatch(`ROLLBACK TO SAVEPOINT ${savepoint}`);
            await this.#client.executeBatch(`RELEASE SAVEPOINT ${savepoint}`);
          }
        } catch (rollbackError: unknown) {
          const failure = new AggregateError([error, rollbackError],
            "FrankenSQLite transaction and rollback both failed", { cause: error });
          this.#transactionFailure = failure;
          // The connection's transactional state is now unknown. Never allow
          // the next caller to accidentally commit the failed callback's work.
          try {
            this.#client.dispose(failure);
          } catch (cleanupError: unknown) {
            const cleanupFailure = new AggregateError([error, rollbackError, cleanupError],
              "FrankenSQLite transaction, rollback and cleanup failed", { cause: error });
            this.#transactionFailure = cleanupFailure;
            throw cleanupFailure;
          }
          throw failure;
        }
      }
      throw error;
    } finally {
      scope.accepting = false;
      this.#transactionScope = parent;
    }
  }

  #nestedTransaction<T>(
    parent: TransactionScope,
    work: (tx: FrankenTransaction) => T | Promise<T>,
  ): Promise<T> {
    try {
      this.#assertOwner(parent);
    } catch (error: unknown) {
      return Promise.reject(error);
    }
    const promise = this.#transaction(parent, work);
    parent.children.add(promise);
    // A rolled-back child is recoverable by its parent. Unlike a direct SQL
    // failure, a caught child failure must not automatically poison the parent.
    void promise.then(
      () => { parent.children.delete(promise); },
      () => { parent.children.delete(promise); },
    );
    return promise;
  }

  close(): Promise<void> {
    return this.#run(null, () => this.#client.close());
  }

  #assertOwner(scope: TransactionScope | null): void {
    if (scope !== null && !scope.accepting) {
      throw new FrankenSQLiteError({ code: "ERR_FSQLITE_TRANSACTION_CLOSED",
        message: "This FrankenSQLite transaction callback has finished" });
    }
    if (this.#transactionScope !== scope) {
      throw new FrankenSQLiteError({ code: "ERR_FSQLITE_TRANSACTION_OWNERSHIP",
        message: "A transaction owns this connection; use its transaction handle or wait until it finishes" });
    }
  }

  #run<T>(scope: TransactionScope | null, operation: () => Promise<T>): Promise<T> {
    try {
      this.#assertOwner(scope);
    } catch (error: unknown) {
      return Promise.reject(error);
    }
    let promise: Promise<T>;
    try {
      promise = operation();
    } catch (error: unknown) {
      promise = Promise.reject(error);
    }
    if (scope !== null) {
      scope.pending.add(promise);
      void promise.then(
        () => { scope.pending.delete(promise); },
        (error: unknown) => {
          scope.pending.delete(promise);
          if (!scope.errors.includes(error)) scope.errors.push(error);
        },
      );
    }
    return promise;
  }

  async #finishScope<T>(
    scope: TransactionScope,
    work: (tx: FrankenTransaction) => T | Promise<T>,
  ): Promise<T> {
    const tx = new FrankenTransaction({
      execute: (sql, params) => this.#run(scope, () => this.#client.execute(sql, params)),
      executeMany: (sql, parameterSets, options) => this.#run(scope, () => this.#client.executeMany(sql, parameterSets, options)),
      query: <Row extends Record<string, unknown>>(sql: string, params: readonly SqlScalar[] = []) =>
        this.#run(scope, () => this.#client.query<Row>(sql, params)),
      prepare: <Row extends Record<string, unknown>>(sql: string) =>
        this.#run(scope, () => this.#prepare<Row>(sql, scope)),
    }, (nestedWork) => this.#nestedTransaction(scope, nestedWork));
    let result!: T;
    let callbackErrors: unknown[] = [];
    try {
      result = await work(tx);
    } catch (error: unknown) {
      callbackErrors = [error];
    } finally {
      // Reject escaped handles before draining work admitted during the callback.
      scope.accepting = false;
    }
    const children = [...scope.children];
    if (children.length > 0) {
      scope.errors.push(new FrankenSQLiteError({ code: "ERR_FSQLITE_TRANSACTION_UNAWAITED",
        message: "Await nested transactions before returning from the parent callback" }));
    }
    await Promise.allSettled([...scope.pending]);
    for (const child of await Promise.allSettled(children)) {
      if (child.status === "rejected") scope.errors.push(child.reason);
    }
    for (const statementId of scope.statements) {
      try {
        await this.#client.finalizePrepared(statementId);
      } catch (error: unknown) {
        scope.errors.push(error);
      }
    }
    scope.statements.clear();
    if (this.#transactionFailure !== null) scope.errors.push(this.#transactionFailure);
    const errors = [...new Set([...callbackErrors, ...scope.errors])];
    if (errors.length === 1) throw errors[0];
    if (errors.length > 1) {
      throw new AggregateError(errors, "FrankenSQLite transaction operations failed", { cause: errors[0] });
    }
    return result;
  }
}
