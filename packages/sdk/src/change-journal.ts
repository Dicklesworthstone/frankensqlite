import type { FrankenDB } from "./database";
import { FrankenSQLiteError } from "./errors";
import type { FrankenTransaction } from "./transaction";
import type { TransactionOptions } from "./types";

const MAX_TABLES = 64;
const EVENTS = ["INSERT", "UPDATE", "DELETE"] as const;
const quote = (name: string): string => `"${name.replaceAll('"', '""')}"`;
const fold = (name: string): string => name.replace(/[A-Z]/g, c => c.toLowerCase());

function invalid(message: string): FrankenSQLiteError {
  return new FrankenSQLiteError({ code: "ERR_FSQLITE_SUBSCRIPTION_INPUT", message, transient: false });
}
function changed(): FrankenSQLiteError {
  return new FrankenSQLiteError({ code: "ERR_FSQLITE_SUBSCRIPTION_SCHEMA",
    message: "A watched table or its notification triggers changed; unsubscribe before changing that schema",
    transient: false });
}

/** Capture identifiers, not SQL fragments; never invoke a caller's array iterator. */
export function captureTables(input: readonly string[]): readonly string[] {
  if (!Array.isArray(input) || input.length === 0 || input.length > MAX_TABLES) {
    throw invalid(`Subscribe to 1..${MAX_TABLES} ordinary main-schema tables`);
  }
  const names = new Map<string, string>();
  const length = input.length;
  for (let i = 0; i < length; i++) {
    const name: unknown = input[i];
    if (typeof name !== "string" || name.length === 0 || name.length > 1024 || name.includes("\0") ||
      fold(name).startsWith("sqlite_") || fold(name).startsWith("__fsqlite_watch_")) {
      throw invalid("Use nonempty, non-reserved table names of at most 1024 characters, without NUL");
    }
    names.set(fold(name), name);
  }
  return Object.freeze([...names.values()]);
}

interface Watch {
  readonly id: number;
  readonly name: string;
  readonly sql: string;
  readonly triggers: ReadonlyMap<string, string>;
}

/**
 * Transaction-local dirty bits, one row and three TEMP triggers per watched
 * ordinary main table. This is not a native update hook or a cross-connection
 * feed. Callers must serialize configuration and run() on one private database.
 */
export class TableChangeJournal {
  readonly #name = `__fsqlite_watch_${crypto.randomUUID().replaceAll("-", "")}`;
  #watches = new Map<string, Watch>();
  #initialized = false;
  #nextId = 1;

  get tables(): readonly string[] { return Object.freeze([...this.#watches.keys()]); }

  matches(tables: readonly string[]): boolean {
    return tables.length === this.#watches.size && tables.every(name => this.#watches.has(name));
  }

  /** Install/remove atomically. JS configuration changes only after COMMIT. */
  async configure(db: FrankenDB, requested: readonly string[], options?: TransactionOptions): Promise<readonly string[]> {
    const names = requested.length === 0 ? [] : captureTables(requested);
    const next = new Map<string, Watch>();
    let nextId = this.#nextId;
    await db.transaction(async tx => {
      const definitions = new Map<string, { name: string; sql: string }>();
      for (const name of names) {
        const result = await tx.query(
          "SELECT name, sql FROM main.sqlite_master WHERE type = 'table' AND name = ? COLLATE NOCASE", [name]);
        const row = result.rowArrays[0];
        if (result.rowArrays.length !== 1 || typeof row?.[0] !== "string" || typeof row[1] !== "string" ||
          /^\s*CREATE\s+VIRTUAL\b/i.test(row[1])) {
          throw invalid(`Not an ordinary main-schema table: ${name}`);
        }
        definitions.set(row[0], { name: row[0], sql: row[1] });
      }
      if (!this.#initialized) {
        await tx.execute(`CREATE TEMP TABLE ${quote(this.#name)} (id INTEGER PRIMARY KEY, dirty INTEGER NOT NULL) WITHOUT ROWID`);
      }
      for (const watch of this.#watches.values()) {
        if (definitions.has(watch.name)) continue;
        for (const trigger of watch.triggers.keys()) await tx.execute(`DROP TRIGGER IF EXISTS temp.${quote(trigger)}`);
        await tx.execute(`DELETE FROM temp.${quote(this.#name)} WHERE id = ?`, [watch.id]);
      }
      for (const definition of definitions.values()) {
        const existing = this.#watches.get(definition.name);
        if (existing !== undefined) {
          if (existing.sql !== definition.sql) throw changed();
          next.set(existing.name, existing);
          continue;
        }
        const id = nextId++;
        await tx.execute(`INSERT INTO temp.${quote(this.#name)} (id, dirty) VALUES (?, 0)`, [id]);
        const triggers = new Map<string, string>();
        for (const event of EVENTS) {
          const trigger = `${this.#name}_${id}_${event.toLowerCase()}`;
          // UPDATE a preallocated bit: an outer OR FAIL/REPLACE policy cannot
          // override an inner INSERT OR IGNORE and create duplicate-key errors.
          await tx.execute(`CREATE TEMP TRIGGER ${quote(trigger)} AFTER ${event} ON main.${quote(definition.name)} ` +
            `BEGIN UPDATE ${quote(this.#name)} SET dirty = 1 WHERE id = ${id}; END`);
          const metadata = await tx.query("SELECT sql FROM temp.sqlite_master WHERE type = 'trigger' AND name = ?", [trigger]);
          const sql = metadata.rowArrays[0]?.[0];
          if (metadata.rowArrays.length !== 1 || typeof sql !== "string") throw changed();
          triggers.set(trigger, sql);
        }
        next.set(definition.name, { ...definition, id, triggers });
      }
      await this.#verify(tx, next);
    }, options);
    this.#initialized = true;
    this.#nextId = nextId;
    this.#watches = next;
    return Object.freeze(names.map(name => [...next.keys()].find(key => fold(key) === fold(name))!));
  }

  /** Return dirty tables only after the enclosing SQL COMMIT has succeeded. */
  async run<T>(db: FrankenDB, work: (tx: FrankenTransaction) => T | Promise<T>, options?: TransactionOptions):
    Promise<{ value: T; tables: readonly string[] }> {
    if (this.#watches.size === 0) return { value: await db.transaction(work, options), tables: [] };
    return db.transaction(async outer => {
      await this.#verify(outer, this.#watches);
      // The user's handle must expire when their callback ends, not after our
      // asynchronous journal read. A private parent owns that postlude.
      const value = await outer.transaction(work);
      await this.#verify(outer, this.#watches);
      const result = await outer.query(`SELECT id, dirty FROM temp.${quote(this.#name)} ORDER BY id`);
      const byId = new Map([...this.#watches.values()].map(watch => [watch.id, watch.name]));
      if (result.rowArrays.length !== byId.size) throw changed();
      const tables: string[] = [];
      for (const row of result.rowArrays) {
        const id = row[0];
        const name = typeof id === "number" ? byId.get(id) : typeof id === "bigint" && id <= BigInt(Number.MAX_SAFE_INTEGER) ? byId.get(Number(id)) : undefined;
        if (name === undefined) throw changed();
        if (row[1] === 1 || row[1] === 1n) tables.push(name);
        else if (row[1] !== 0 && row[1] !== 0n) throw changed();
      }
      if (tables.length !== 0) await outer.execute(`UPDATE temp.${quote(this.#name)} SET dirty = 0 WHERE dirty = 1`);
      return { value, tables: Object.freeze(tables) };
    }, options);
  }

  async #verify(tx: FrankenTransaction, watches: ReadonlyMap<string, Watch>): Promise<void> {
    if (watches.size === 0) return;
    const names = [...watches.keys()];
    const tables = await tx.query(`SELECT name, sql FROM main.sqlite_master WHERE type = 'table' AND name IN (${names.map(() => "?").join(",")})`, names);
    if (tables.rowArrays.length !== watches.size) throw changed();
    for (const row of tables.rowArrays) {
      if (typeof row[0] !== "string" || watches.get(row[0])?.sql !== row[1]) throw changed();
    }
    const expected = new Map([...watches.values()].flatMap(watch => [...watch.triggers]));
    // An earlier application TEMP trigger can use RAISE(IGNORE) to prevent our
    // AFTER trigger from firing after a real write. Refuse competing TEMP
    // triggers instead of depending on an undocumented firing order.
    const triggers = await tx.query(`SELECT name, sql FROM temp.sqlite_master WHERE type = 'trigger' AND tbl_name IN (${names.map(() => "?").join(",")})`, names);
    if (triggers.rowArrays.length !== expected.size) throw changed();
    for (const row of triggers.rowArrays) {
      if (typeof row[0] !== "string" || expected.get(row[0]) !== row[1]) throw changed();
    }
  }
}
