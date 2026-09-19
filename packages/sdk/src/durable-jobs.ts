/** Persistent job state lives in ordinary SQL, never in the callback scheduler. */
export const DURABLE_JOBS_TABLE = "__fsqlite_durable_jobs_v1";
const TABLE = DURABLE_JOBS_TABLE;
const MAX_TEXT_BYTES = 1024 * 1024;
const MAX_LEASE_MS = 86_400_000;
type Parameter = string | number | null;
type SqlRow = Record<string, unknown>;

/** Minimal transaction surface; FrankenTransaction implements this directly. */
export interface DurableJobTransaction {
  execute(sql: string, params?: readonly Parameter[]): Promise<number>;
  query(sql: string, params?: readonly Parameter[]): Promise<{ readonly rows: readonly SqlRow[] }>;
}

/**
 * FrankenDB and FrankenDBQueue implement this contract. The callback MUST run
 * in one atomic transaction and settle only after commit/rollback completes.
 * Do not supply an adapter that retries callbacks or swallows commit failures.
 */
export interface DurableJobDatabase {
  transaction<T>(work: (tx: DurableJobTransaction) => Promise<T>): Promise<T>;
}

export interface DurableJobQueueOptions {
  /** Persisted leases use wall-clock milliseconds. Workers must agree on time. */
  clock?: () => number;
}

export interface EnqueueJob {
  /** Stable idempotency key, scoped to this queue. Retain it across retries. */
  id: string;
  /** Application-encoded text (for example JSON), at most 1 MiB of UTF-8. */
  payload: string;
  priority?: number;
  availableAt?: number;
  /** Includes the first claim; defaults to 3, range 1..1,000,000. */
  maxAttempts?: number;
}

export type DurableJobState = "ready" | "leased" | "completed" | "dead" | "cancelled";
export interface DurableJob {
  readonly id: string;
  readonly queue: string;
  readonly payload: string;
  readonly state: DurableJobState;
  readonly priority: number;
  readonly availableAt: number;
  readonly attempts: number;
  readonly maxAttempts: number;
  readonly owner: string | null;
  readonly leaseExpiresAt: number | null;
  readonly createdAt: number;
  readonly updatedAt: number;
  readonly result: string | null;
  readonly lastError: string | null;
}

/** An ownership receipt, not an authorization boundary against direct SQL. */
export interface DurableJobLease {
  readonly queue: string;
  readonly id: string;
  readonly payload: string;
  readonly owner: string;
  readonly token: string;
  readonly attempt: number;
  /** Informational; mutations always check the current persisted deadline. */
  readonly expiresAt: number;
}

export class DurableJobError extends Error {
  constructor(readonly code: string, message: string) {
    super(message);
    this.name = "DurableJobError";
  }
}

/**
 * At-least-once, fenced job delivery over a transaction-owning database.
 * Reopening the SAME durable database preserves jobs and claims. Independently
 * imported browser snapshots are NOT shared live queues; a snapshot checkpoint
 * conflict is not permission to replay jobs. Persistence is the host's policy.
 */
export class DurableJobQueue {
  readonly #db: DurableJobDatabase;
  readonly #clock: () => number;
  readonly name: string;

  private constructor(db: DurableJobDatabase, name: string, clock: () => number) {
    this.#db = db;
    this.name = name;
    this.#clock = clock;
  }

  static async open(db: DurableJobDatabase, name: string, options?: DurableJobQueueOptions): Promise<DurableJobQueue> {
    identifier(name, "queue name");
    if (db === null || typeof db !== "object" || typeof db.transaction !== "function") {
      throw new TypeError("A transaction-owning database is required");
    }
    const clock = options?.clock ?? Date.now;
    if (typeof clock !== "function") throw new TypeError("clock must be a function");
    const queue = new DurableJobQueue(db, name, clock);
    await db.transaction(async tx => {
      await tx.execute(`CREATE TABLE IF NOT EXISTS ${TABLE} (
        queue_name TEXT NOT NULL, job_id TEXT NOT NULL, payload TEXT NOT NULL,
        state TEXT NOT NULL CHECK (state IN ('ready','leased','completed','dead','cancelled')),
        priority INTEGER NOT NULL, scheduled_at INTEGER NOT NULL, available_at INTEGER NOT NULL,
        attempts INTEGER NOT NULL CHECK (attempts >= 0),
        max_attempts INTEGER NOT NULL CHECK (max_attempts > 0),
        lease_owner TEXT, lease_token TEXT, lease_expires_at INTEGER,
        created_at INTEGER NOT NULL, updated_at INTEGER NOT NULL, result TEXT, last_error TEXT,
        PRIMARY KEY (queue_name, job_id),
        CHECK ((state = 'leased' AND lease_owner IS NOT NULL AND lease_token IS NOT NULL AND lease_expires_at IS NOT NULL)
          OR (state <> 'leased' AND lease_owner IS NULL AND lease_token IS NULL AND lease_expires_at IS NULL))
      )`);
      await tx.execute(`CREATE INDEX IF NOT EXISTS __fsqlite_jobs_ready_v1 ON ${TABLE}
        (queue_name, state, available_at, priority)`);
      await tx.execute(`CREATE INDEX IF NOT EXISTS __fsqlite_jobs_expiry_v1 ON ${TABLE}
        (queue_name, state, lease_expires_at)`);
    });
    return queue;
  }

  /** A duplicate id returns its original job; conflicting input is rejected. */
  async enqueue(input: EnqueueJob): Promise<{ readonly inserted: boolean; readonly job: DurableJob }> {
    // Capture all caller-owned properties before yielding to queue admission.
    const { id, payload, priority = 0, availableAt, maxAttempts = 3 } = input;
    identifier(id, "job id"); text(payload, "payload");
    integer(priority, "priority", -2_147_483_648, 2_147_483_647);
    integer(maxAttempts, "maxAttempts", 1, 1_000_000);
    if (availableAt !== undefined) integer(availableAt, "availableAt");
    return this.#db.transaction(async tx => {
      const now = this.#now();
      const scheduled = availableAt ?? now;
      const inserted = await tx.execute(`INSERT INTO ${TABLE}
        (queue_name,job_id,payload,state,priority,scheduled_at,available_at,attempts,max_attempts,created_at,updated_at)
        VALUES (?,?,?,'ready',?,?,?,0,?,?,?) ON CONFLICT(queue_name,job_id) DO NOTHING`,
        [this.name, id, payload, priority, scheduled, scheduled, maxAttempts, now, now]);
      const row = await this.#row(tx, id);
      if (row === null) throw corrupt("Enqueued job is missing");
      const job = decodeJob(row);
      if (job.payload !== payload || job.priority !== priority || job.maxAttempts !== maxAttempts ||
          (availableAt !== undefined && number(row, "scheduled_at") !== availableAt)) {
        throw new DurableJobError("ERR_FSQLITE_JOB_ID_CONFLICT", "Job id already identifies different input");
      }
      return Object.freeze({ inserted: inserted === 1, job });
    });
  }

  async get(id: string): Promise<DurableJob | null> {
    identifier(id, "job id");
    return this.#db.transaction(async tx => {
      const row = await this.#row(tx, id);
      return row === null ? null : decodeJob(row);
    });
  }

  /**
   * Atomically claim ready work or reclaim an expired lease. Priority descends;
   * ties use availability, creation time and id. No user work runs in this txn.
   * Database contention propagates; there is no blind callback replay.
   */
  async claim(owner: string, leaseMs = 30_000): Promise<DurableJobLease | null> {
    identifier(owner, "worker owner"); integer(leaseMs, "leaseMs", 1, MAX_LEASE_MS);
    return this.#db.transaction(async tx => {
      const now = this.#now();
      const expires = addTime(now, leaseMs);
      const rows = (await tx.query(`SELECT job_id FROM ${TABLE} WHERE queue_name = ?
        AND attempts < max_attempts AND ((state = 'ready' AND available_at <= ?)
          OR (state = 'leased' AND lease_expires_at <= ?))
        ORDER BY priority DESC, available_at, created_at, job_id LIMIT 1`, [this.name, now, now])).rows;
      if (rows.length === 0) return null;
      const id = string(rows[0]!, "job_id");
      const token = crypto.randomUUID();
      const changed = await tx.execute(`UPDATE ${TABLE} SET state = 'leased', attempts = attempts + 1,
        lease_owner = ?, lease_token = ?, lease_expires_at = ?, updated_at = ?
        WHERE queue_name = ? AND job_id = ? AND attempts < max_attempts
          AND ((state = 'ready' AND available_at <= ?) OR (state = 'leased' AND lease_expires_at <= ?))`,
        [owner, token, expires, now, this.name, id, now, now]);
      if (changed !== 1) throw new DurableJobError("ERR_FSQLITE_JOB_CONFLICT", "Job changed during claim; no lease was granted");
      return this.#receipt(tx, id);
    });
  }

  /** A heartbeat cannot resurrect an expired lease or shorten its deadline. */
  async renew(lease: DurableJobLease, leaseMs = 30_000): Promise<DurableJobLease> {
    const keys = this.#keys(lease);
    integer(leaseMs, "leaseMs", 1, MAX_LEASE_MS);
    return this.#db.transaction(async tx => {
      const now = this.#now();
      const expires = addTime(now, leaseMs);
      this.#changed(await tx.execute(`UPDATE ${TABLE}
        SET lease_expires_at = CASE WHEN lease_expires_at > ? THEN lease_expires_at ELSE ? END, updated_at = ?
        WHERE ${fence()}`, [expires, expires, now, ...keys, now]));
      return this.#receipt(tx, keys[1]! as string);
    });
  }

  async complete(lease: DurableJobLease, result: string | null = null): Promise<void> {
    const keys = this.#keys(lease);
    if (result !== null) text(result, "result");
    await this.#db.transaction(async tx => {
      const now = this.#now();
      this.#changed(await tx.execute(`UPDATE ${TABLE} SET state = 'completed', result = ?, updated_at = ?,
        lease_owner = NULL, lease_token = NULL, lease_expires_at = NULL WHERE ${fence()}`,
        [result, now, ...keys, now]));
    });
  }

  /** Release to a delayed retry, or dead-letter the final attempt. */
  async fail(lease: DurableJobLease, error: string, retryDelayMs = 0): Promise<void> {
    const keys = this.#keys(lease);
    text(error, "error"); integer(retryDelayMs, "retryDelayMs");
    await this.#db.transaction(async tx => {
      const now = this.#now();
      const available = addTime(now, retryDelayMs);
      this.#changed(await tx.execute(`UPDATE ${TABLE}
        SET state = CASE WHEN attempts >= max_attempts THEN 'dead' ELSE 'ready' END,
          available_at = ?, updated_at = ?, last_error = ?,
          lease_owner = NULL, lease_token = NULL, lease_expires_at = NULL WHERE ${fence()}`,
        [available, now, error, ...keys, now]));
    });
  }

  /** Cancel queued or leased work. A running handler must observe lease loss. */
  async cancel(id: string): Promise<boolean> {
    identifier(id, "job id");
    return this.#db.transaction(async tx => (await tx.execute(`UPDATE ${TABLE}
      SET state = 'cancelled', updated_at = ?, lease_owner = NULL, lease_token = NULL, lease_expires_at = NULL
      WHERE queue_name = ? AND job_id = ? AND state IN ('ready','leased')`, [this.#now(), this.name, id])) === 1);
  }

  /** Bounded crash recovery, including workers that died on their final attempt. */
  async reapExpired(limit = 100): Promise<number> {
    integer(limit, "limit", 1, 1000);
    return this.#db.transaction(async tx => {
      const now = this.#now();
      return tx.execute(`UPDATE ${TABLE}
        SET state = CASE WHEN attempts >= max_attempts THEN 'dead' ELSE 'ready' END,
          updated_at = ?, lease_owner = NULL, lease_token = NULL, lease_expires_at = NULL
        WHERE queue_name = ? AND state = 'leased' AND lease_expires_at <= ? AND job_id IN (
          SELECT job_id FROM ${TABLE} WHERE queue_name = ? AND state = 'leased' AND lease_expires_at <= ?
          ORDER BY lease_expires_at, job_id LIMIT ?)`, [now, this.name, now, this.name, now, limit]);
    });
  }

  #now(): number { const now = this.#clock(); integer(now, "clock result"); return now; }

  #keys(lease: DurableJobLease): readonly Parameter[] {
    const { queue, id, owner, token, attempt } = lease;
    if (queue !== this.name) throw new TypeError("Lease belongs to another queue");
    identifier(id, "job id"); identifier(owner, "worker owner"); identifier(token, "lease token");
    integer(attempt, "attempt", 1, 1_000_000);
    return [queue, id, owner, token, attempt];
  }

  #changed(changed: number): void {
    if (changed !== 1) throw new DurableJobError("ERR_FSQLITE_JOB_LEASE_LOST",
      "Job lease expired, was cancelled, or belongs to another claim; no mutation was committed");
  }

  async #row(tx: DurableJobTransaction, id: string): Promise<SqlRow | null> {
    const rows = (await tx.query(`SELECT * FROM ${TABLE} WHERE queue_name = ? AND job_id = ?`, [this.name, id])).rows;
    return rows[0] ?? null;
  }

  async #receipt(tx: DurableJobTransaction, id: string): Promise<DurableJobLease> {
    const row = await this.#row(tx, id);
    if (row === null || row.state !== "leased") throw corrupt("Claimed job is missing");
    const job = decodeJob(row);
    return Object.freeze({ queue: job.queue, id: job.id, payload: job.payload,
      owner: string(row, "lease_owner"), token: string(row, "lease_token"),
      attempt: job.attempts, expiresAt: number(row, "lease_expires_at") });
  }
}

function fence(): string {
  return "queue_name = ? AND job_id = ? AND lease_owner = ? AND lease_token = ? AND attempts = ? AND state = 'leased' AND lease_expires_at > ?";
}

function identifier(value: string, label: string): void {
  if (typeof value !== "string" || value.length === 0 || value.length > 256 || value.includes("\0")) {
    throw new TypeError(`${label} must be a nonempty string of at most 256 characters without NUL`);
  }
}

function text(value: string, label: string): void {
  if (typeof value !== "string") throw new TypeError(`${label} must be a string`);
  if (value.length > MAX_TEXT_BYTES || new TextEncoder().encode(value).byteLength > MAX_TEXT_BYTES) {
    throw new RangeError(`${label} exceeds ${MAX_TEXT_BYTES} UTF-8 bytes`);
  }
}

function integer(value: number, label: string, min = 0, max = Number.MAX_SAFE_INTEGER): void {
  if (!Number.isSafeInteger(value) || value < min || value > max) throw new RangeError(`${label} must be an integer in ${min}..${max}`);
}

function addTime(now: number, duration: number): number {
  const value = now + duration;
  integer(value, "deadline");
  return value;
}

function corrupt(message: string): DurableJobError { return new DurableJobError("ERR_FSQLITE_JOB_CORRUPT", message); }
function string(row: SqlRow, key: string): string {
  const value = row[key];
  if (typeof value !== "string") throw corrupt(`Invalid job field: ${key}`);
  return value;
}
function number(row: SqlRow, key: string): number {
  const value = typeof row[key] === "bigint" ? Number(row[key]) : row[key];
  if (typeof value !== "number" || !Number.isSafeInteger(value)) throw corrupt(`Invalid job field: ${key}`);
  return value;
}
function nullableString(row: SqlRow, key: string): string | null { return row[key] === null ? null : string(row, key); }
function decodeJob(row: SqlRow): DurableJob {
  const state = string(row, "state");
  if (!["ready", "leased", "completed", "dead", "cancelled"].includes(state)) throw corrupt("Invalid job state");
  return Object.freeze({ id: string(row, "job_id"), queue: string(row, "queue_name"), payload: string(row, "payload"),
    state: state as DurableJobState, priority: number(row, "priority"), availableAt: number(row, "available_at"),
    attempts: number(row, "attempts"), maxAttempts: number(row, "max_attempts"), owner: nullableString(row, "lease_owner"),
    leaseExpiresAt: row.lease_expires_at === null ? null : number(row, "lease_expires_at"),
    createdAt: number(row, "created_at"), updatedAt: number(row, "updated_at"),
    result: nullableString(row, "result"), lastError: nullableString(row, "last_error") });
}
