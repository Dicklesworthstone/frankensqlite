import { parameterLayout, validateManagedSql } from "@frankensqlite/worker";
import { FrankenDB } from "./database";
import type { QueuedTransactionOptions, QueuedTransactionRetryOptions } from "./queue";
import { FrankenDBQueue } from "./queue";
import type { FrankenTransaction } from "./transaction";
import type { TransactionRetryOptions } from "./transaction-retry";
import type { TransactionOptions } from "./types";

/** Distinct from the native Rust runner's legacy _schema_migrations format. */
export const MIGRATION_HISTORY_TABLE = "_fsqlite_sdk_migrations_v1";
const TABLE = `main."${MIGRATION_HISTORY_TABLE}"`;
const MAX_MIGRATIONS = 256;
const MAX_STATEMENTS = 4096;
const MAX_SQL_UNITS = 4 * 1024 * 1024;
const CREATE_HISTORY = `CREATE TABLE ${TABLE} (version INTEGER PRIMARY KEY, name TEXT NOT NULL, sha256 TEXT NOT NULL)`;

export interface SchemaMigration {
  /** Strictly increasing positive safe integer. Numeric gaps are allowed. */
  readonly version: number;
  readonly name: string;
  /** One complete, unbound SQL statement per entry; a trigger is one statement. */
  readonly statements: readonly string[];
}

export interface MigrationIdentity {
  readonly version: number;
  readonly name: string;
  /** SHA-256 of the versioned, exact-text migration definition, not schema bytes. */
  readonly sha256: string;
}

export interface MigrationStatus {
  readonly currentVersion: number;
  readonly applied: readonly MigrationIdentity[];
  readonly pending: readonly MigrationIdentity[];
}

export interface MigrationResult {
  readonly previousVersion: number;
  readonly currentVersion: number;
  /** Only migrations applied by this transaction. Empty on a verified rerun. */
  readonly applied: readonly MigrationIdentity[];
}

export class FrankenMigrationError extends Error {
  readonly transient = false;
  constructor(
    readonly code:
      | "ERR_FSQLITE_MIGRATION_INPUT"
      | "ERR_FSQLITE_MIGRATION_SQL"
      | "ERR_FSQLITE_MIGRATION_HISTORY"
      | "ERR_FSQLITE_MIGRATION_DRIFT",
    message: string,
    readonly version?: number,
    options?: ErrorOptions,
  ) {
    super(message, options);
    this.name = "FrankenMigrationError";
  }
}

function invalid(message: string, version?: number): never {
  throw new FrankenMigrationError("ERR_FSQLITE_MIGRATION_INPUT", message, version);
}
function historyError(message: string, version?: number): never {
  throw new FrankenMigrationError("ERR_FSQLITE_MIGRATION_HISTORY", message, version);
}

/** Read only the command prefix, leaving complete statement parsing to the core. */
function commandWords(sql: string): string[] {
  const words: string[] = [];
  for (let i = 0; i < sql.length && words.length < 3; ) {
    if (/[\t\n\v\f\r \uFEFF]/.test(sql[i]!) || (words.length === 0 && sql[i] === ";")) {
      i++;
      continue;
    }
    if (sql.startsWith("--", i)) {
      const end = sql.indexOf("\n", i + 2);
      i = end < 0 ? sql.length : end + 1;
      continue;
    }
    if (sql.startsWith("/*", i)) {
      const end = sql.indexOf("*/", i + 2);
      i = end < 0 ? sql.length : end + 2;
      continue;
    }
    const word = /^[A-Za-z_][A-Za-z0-9_$]*/.exec(sql.slice(i))?.[0];
    if (word === undefined) break;
    words.push(word.toUpperCase());
    i += word.length;
  }
  return words;
}

function validateStatement(sql: string, version: number): void {
  try {
    // Do not split on ';': quoted data and CREATE TRIGGER bodies contain it.
    // The existing worker preflight also excludes every manual txn boundary.
    validateManagedSql(sql);
    if (parameterLayout(sql).count !== 0)
      throw new Error("Migration SQL must not contain bind parameters");
    const [verb, object, third] = commandWords(sql);
    const allowed =
      verb === "CREATE"
        ? ["TABLE", "INDEX", "VIEW", "TRIGGER"].includes(object ?? "") ||
          (object === "UNIQUE" && third === "INDEX")
        : verb === "ALTER"
          ? object === "TABLE"
          : verb === "DROP"
            ? ["TABLE", "INDEX", "VIEW", "TRIGGER"].includes(object ?? "")
            : ["INSERT", "UPDATE", "DELETE", "REPLACE", "WITH"].includes(verb ?? "");
    if (!allowed)
      throw new Error(
        "Use transactional DDL/DML, not PRAGMA, ATTACH, VACUUM, TEMP or virtual-table setup",
      );
  } catch (cause: unknown) {
    throw new FrankenMigrationError(
      "ERR_FSQLITE_MIGRATION_SQL",
      `Migration ${version} must contain one supported SQL statement per entry`,
      version,
      { cause },
    );
  }
}

function capturePlan(input: readonly SchemaMigration[]): readonly SchemaMigration[] {
  if (!Array.isArray(input) || input.length > MAX_MIGRATIONS)
    invalid(`A migration plan accepts at most ${MAX_MIGRATIONS} migrations`);
  const length = input.length;
  const result: SchemaMigration[] = [];
  let previous = 0,
    statements = 0,
    units = 0;
  for (let i = 0; i < length; i++) {
    if (!Object.hasOwn(input, i)) invalid("Migration arrays must not contain holes");
    const entry = input[i];
    if (typeof entry !== "object" || entry === null) invalid("A migration definition is required");
    // Capture each caller field once; retain no mutable input arrays or objects.
    const version = entry.version,
      name = entry.name,
      source = entry.statements;
    if (!Number.isSafeInteger(version) || version <= previous)
      invalid("Versions must be positive safe integers in strictly increasing order", version);
    if (
      typeof name !== "string" ||
      name.trim().length === 0 ||
      name.length > 256 ||
      name.includes("\0")
    )
      invalid("Migration names must contain 1..256 characters without NUL", version);
    if (
      !Array.isArray(source) ||
      source.length === 0 ||
      source.length > MAX_STATEMENTS - statements
    )
      invalid(`A plan accepts 1..${MAX_STATEMENTS} total SQL statements`, version);
    const count = source.length,
      captured: string[] = [];
    statements += count;
    for (let j = 0; j < count; j++) {
      if (!Object.hasOwn(source, j)) invalid("Statement arrays must not contain holes", version);
      const sql: unknown = source[j];
      if (typeof sql !== "string" || sql.length === 0)
        invalid("Migration statements must be nonempty strings", version);
      units += sql.length;
      if (units > MAX_SQL_UNITS)
        invalid("Migration SQL exceeds the 4 Mi UTF-16-code-unit plan limit", version);
      validateStatement(sql, version);
      captured.push(sql);
    }
    result.push(Object.freeze({ version, name, statements: Object.freeze(captured) }));
    previous = version;
  }
  return Object.freeze(result);
}

interface History {
  readonly schema: string | null;
  readonly entries: readonly MigrationIdentity[];
}

async function temporarySchema(tx: FrankenTransaction): Promise<string> {
  const result = await tx.query(
    "SELECT type, name, tbl_name, sql FROM temp.sqlite_master ORDER BY type COLLATE BINARY, name COLLATE BINARY LIMIT 4097",
  );
  if (result.rowArrays.length > 4096)
    historyError("Too many temporary schema objects to verify a migration");
  return JSON.stringify(result.rowArrays);
}

function integer(value: unknown): number | undefined {
  if (typeof value === "number" && Number.isSafeInteger(value)) return value;
  if (
    typeof value === "bigint" &&
    value >= BigInt(Number.MIN_SAFE_INTEGER) &&
    value <= BigInt(Number.MAX_SAFE_INTEGER)
  )
    return Number(value);
  return undefined;
}

async function readHistory(tx: FrankenTransaction): Promise<History> {
  const catalog = await tx.query(
    "SELECT name, type, sql FROM main.sqlite_master WHERE name = ? COLLATE NOCASE",
    [MIGRATION_HISTORY_TABLE],
  );
  if (catalog.rowArrays.length === 0) return { schema: null, entries: Object.freeze([]) };
  const object = catalog.rowArrays[0]!;
  if (
    catalog.rowArrays.length !== 1 ||
    object[0] !== MIGRATION_HISTORY_TABLE ||
    object[1] !== "table" ||
    typeof object[2] !== "string"
  ) {
    historyError("Migration history must be an ordinary main-schema table");
  }
  const columns = await tx.query(`PRAGMA main.table_xinfo("${MIGRATION_HISTORY_TABLE}")`);
  const names = ["version", "name", "sha256"],
    types = ["INTEGER", "TEXT", "TEXT"];
  if (columns.rowArrays.length !== 3) historyError("Unsupported migration history table shape");
  for (let i = 0; i < 3; i++) {
    const row = columns.rowArrays[i]!;
    if (
      row.length !== 7 ||
      integer(row[0]) !== i ||
      row[1] !== names[i] ||
      typeof row[2] !== "string" ||
      row[2].toUpperCase() !== types[i] ||
      integer(row[3]) !== (i === 0 ? 0 : 1) ||
      row[4] !== null ||
      integer(row[5]) !== (i === 0 ? 1 : 0) ||
      integer(row[6]) !== 0
    ) {
      historyError("Unsupported migration history columns, defaults or constraints");
    }
  }
  // A trigger could silently rewrite the ledger or give metadata insertion
  // application side effects. Neither permanent nor TEMP ledger triggers qualify.
  for (const schema of ["main", "temp"]) {
    const triggers = await tx.query(
      `SELECT name FROM ${schema}.sqlite_master WHERE type = 'trigger' AND tbl_name = ? COLLATE NOCASE LIMIT 1`,
      [MIGRATION_HISTORY_TABLE],
    );
    if (triggers.rowArrays.length !== 0) historyError("Migration history must not have triggers");
  }
  const rows = await tx.query(
    `SELECT version, name, sha256 FROM ${TABLE} ORDER BY version LIMIT ${MAX_MIGRATIONS + 1}`,
  );
  if (rows.rowArrays.length > MAX_MIGRATIONS)
    historyError("Migration history exceeds the supported bound");
  const entries: MigrationIdentity[] = [];
  let previous = 0;
  for (const row of rows.rowArrays) {
    const version = integer(row[0]),
      name = row[1],
      sha256 = row[2];
    if (
      row.length !== 3 ||
      version === undefined ||
      version <= previous ||
      typeof name !== "string" ||
      name.length === 0 ||
      name.length > 256 ||
      typeof sha256 !== "string" ||
      !/^[0-9a-f]{64}$/.test(sha256)
    ) {
      historyError("Malformed migration history row", version);
    }
    entries.push(Object.freeze({ version, name, sha256 }));
    previous = version;
  }
  return { schema: object[2], entries: Object.freeze(entries) };
}

function verifyPrefix(history: History, plan: readonly MigrationIdentity[]): void {
  for (let i = 0; i < history.entries.length; i++) {
    const actual = history.entries[i]!,
      expected = plan[i];
    if (actual.version !== expected?.version)
      historyError(
        "Applied history must be an exact prefix of this plan; missing, reordered or newer versions cannot be skipped",
        actual.version,
      );
    if (actual.name !== expected.name || actual.sha256 !== expected.sha256) {
      throw new FrankenMigrationError(
        "ERR_FSQLITE_MIGRATION_DRIFT",
        `Applied migration ${actual.version} differs from this plan; restore the original definition and add a new version`,
        actual.version,
      );
    }
  }
}

/**
 * Immutable application schema upgrades. All pending versions and their history
 * rows share one transaction; SQL errors/cancellation cannot commit a prefix.
 * SQL is trusted application code, not an authorization sandbox. No down
 * migrations, automatic repair, PRAGMA user_version changes or native-ledger adoption.
 */
export class FrankenMigrationPlan {
  readonly #migrations: readonly SchemaMigration[];
  #identities: readonly MigrationIdentity[] | null = null;

  constructor(migrations: readonly SchemaMigration[]) {
    this.#migrations = capturePlan(migrations);
  }

  /** Verify the complete history without creating or changing any SQL objects. */
  inspect(db: FrankenDB, options?: TransactionOptions): Promise<MigrationStatus> {
    if (!(db instanceof FrankenDB))
      return Promise.reject(new TypeError("inspect requires a FrankenDB connection"));
    return db.transaction(async (tx) => {
      const identities = await this.#manifest();
      const history = await readHistory(tx);
      verifyPrefix(history, identities);
      return Object.freeze({
        currentVersion: history.entries.at(-1)?.version ?? 0,
        applied: history.entries,
        pending: Object.freeze(identities.slice(history.entries.length)),
      });
    }, options);
  }

  /** Apply every pending migration atomically. Existing transaction() policy applies. */
  apply(db: FrankenDB, options?: TransactionOptions): Promise<MigrationResult> {
    if (!(db instanceof FrankenDB))
      return Promise.reject(new TypeError("apply requires a FrankenDB connection"));
    // Claim connection authority synchronously. Hashing stays inside the owned
    // scope and its deadline; no detached preflight can be overtaken by SQL.
    return db.transaction((tx) => this.#apply(tx), options);
  }

  /** Opt-in restart after confirmed conflict rollback; history is reread each time. */
  applyWithRetry(db: FrankenDB, options?: TransactionRetryOptions): Promise<MigrationResult> {
    if (!(db instanceof FrankenDB))
      return Promise.reject(new TypeError("applyWithRetry requires a FrankenDB connection"));
    return db.transactionWithRetry((tx) => this.#apply(tx), options);
  }

  /** One ordered queue job, including the queue's checkpoint-on-commit barrier. */
  applyQueued(queue: FrankenDBQueue, options?: QueuedTransactionOptions): Promise<MigrationResult> {
    if (!(queue instanceof FrankenDBQueue))
      return Promise.reject(new TypeError("applyQueued requires a FrankenDBQueue"));
    return queue.transaction((tx) => this.#apply(tx), options);
  }

  /** All conflict retries retain the same FIFO job; publication is never replayed. */
  applyQueuedWithRetry(
    queue: FrankenDBQueue,
    options?: QueuedTransactionRetryOptions,
  ): Promise<MigrationResult> {
    if (!(queue instanceof FrankenDBQueue))
      return Promise.reject(new TypeError("applyQueuedWithRetry requires a FrankenDBQueue"));
    return queue.transactionWithRetry((tx) => this.#apply(tx), options);
  }

  async #manifest(): Promise<readonly MigrationIdentity[]> {
    if (this.#identities !== null) return this.#identities;
    if (!globalThis.crypto?.subtle)
      invalid("Secure-context Web Crypto is required for migration checksums");
    const identities: MigrationIdentity[] = [];
    for (const migration of this.#migrations) {
      // Domain/version separated JSON encodes exact statement boundaries, names,
      // whitespace and Unicode escapes; concatenation would be ambiguous.
      const bytes = new TextEncoder().encode(
        JSON.stringify([
          "frankensqlite-sdk-migration",
          1,
          migration.version,
          migration.name,
          migration.statements,
        ]),
      );
      const digest = new Uint8Array(await crypto.subtle.digest("SHA-256", bytes));
      identities.push(
        Object.freeze({
          version: migration.version,
          name: migration.name,
          sha256: [...digest].map((value) => value.toString(16).padStart(2, "0")).join(""),
        }),
      );
    }
    this.#identities = Object.freeze(identities);
    return this.#identities;
  }

  async #apply(tx: FrankenTransaction): Promise<MigrationResult> {
    const identities = await this.#manifest();
    const databases = await tx.query("PRAGMA database_list");
    if (
      databases.rowArrays.some((row: readonly unknown[]) => row[1] !== "main" && row[1] !== "temp")
    ) {
      historyError("Detach auxiliary databases before applying main-schema migrations");
    }
    let history = await readHistory(tx);
    verifyPrefix(history, identities);
    const initialCount = history.entries.length;
    const previousVersion = history.entries.at(-1)?.version ?? 0;
    if (initialCount < identities.length && history.schema === null) {
      await tx.execute(CREATE_HISTORY);
      history = await readHistory(tx);
      if (history.schema === null || history.entries.length !== 0)
        historyError("Could not initialize an empty migration history");
    }
    const schema = history.schema;
    const temporary = initialCount < identities.length ? await temporarySchema(tx) : null;
    for (let i = initialCount; i < identities.length; i++) {
      const identity = identities[i]!;
      for (const sql of this.#migrations[i]!.statements) await tx.executeBatch(sql);
      // A qualified CREATE TABLE temp.x must not acquire a durable 'applied'
      // record for a table which disappears on reopen. Temporary staging is
      // allowed only if the migration restores the pre-existing TEMP schema.
      if ((await temporarySchema(tx)) !== temporary)
        historyError(
          "A migration must not leave temporary schema changes behind",
          identity.version,
        );
      // Migration SQL must not erase/rewrite the previously recorded history.
      // Validate before recording, then read back to catch ignored/altered inserts.
      history = await readHistory(tx);
      verifyPrefix(history, identities);
      if (history.schema !== schema || history.entries.length !== i)
        historyError("Migration SQL changed the reserved history table", identity.version);
      await tx.execute(`INSERT INTO ${TABLE} (version, name, sha256) VALUES (?, ?, ?)`, [
        identity.version,
        identity.name,
        identity.sha256,
      ]);
      history = await readHistory(tx);
      verifyPrefix(history, identities);
      if (history.schema !== schema || history.entries.length !== i + 1)
        historyError(
          "Migration history insertion did not record exactly one version",
          identity.version,
        );
    }
    return Object.freeze({
      previousVersion,
      currentVersion: identities.at(-1)?.version ?? 0,
      applied: Object.freeze(identities.slice(initialCount)),
    });
  }
}
