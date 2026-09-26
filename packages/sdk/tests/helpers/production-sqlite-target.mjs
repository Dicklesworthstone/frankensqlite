import { DatabaseSync } from 'node:sqlite';

// SQL ownership adapter only. No replica decisions or metadata are simulated.
export class SqliteTarget {
  constructor(path = ':memory:', schema = '') {
    this.db = new DatabaseSync(path);
    this.db.exec('PRAGMA foreign_keys=ON; PRAGMA recursive_triggers=ON; PRAGMA busy_timeout=0;');
    if (schema) this.db.exec(schema);
    this.active = false;
    this.statements = [];
    this.before = null;
    this.after = null;
    this.beforeCommit = null;
    this.afterCommit = null;
  }
  async execute(sql, params = []) {
    this.statements.push(sql);
    await this.before?.('execute', sql, params);
    const result = Number(this.db.prepare(sql).run(...params).changes);
    await this.after?.('execute', sql, params, result);
    return result;
  }
  async query(sql, params = []) {
    this.statements.push(sql);
    await this.before?.('query', sql, params);
    const statement = this.db.prepare(sql);
    statement.setReadBigInts(true);
    statement.setReturnArrays(true);
    const result = { rowArrays: statement.all(...params) };
    await this.after?.('query', sql, params, result);
    return result;
  }
  async transaction(work, options = {}) {
    if (this.active) throw new Error('Test target requires exclusive top-level transaction ownership');
    options.signal?.throwIfAborted();
    this.active = true;
    let begun = false, committed = false;
    try {
      this.db.exec('BEGIN'); begun = true;
      const value = await work({
        execute: (sql, params) => this.execute(sql, params),
        query: (sql, params) => this.query(sql, params),
      });
      await this.beforeCommit?.();
      options.signal?.throwIfAborted();
      this.db.exec('COMMIT');
      committed = true;
      await this.afterCommit?.();
      return value;
    } catch (error) {
      if (begun && !committed) this.db.exec('ROLLBACK');
      throw error;
    } finally { this.active = false; }
  }
  rows(sql = 'SELECT * FROM t ORDER BY id', params = []) {
    const statement = this.db.prepare(sql);
    statement.setReadBigInts(true); statement.setReturnArrays(true);
    return statement.all(...params);
  }
  close() { this.db.close(); }
}

// Native SQLite Session is an independent changeset producer.
export function nativeSeed(chunks = 3, schema = 'CREATE TABLE t(id INTEGER PRIMARY KEY, value TEXT)') {
  const db = new DatabaseSync(':memory:'); db.exec(schema);
  try {
    const result = [];
    for (let index = 0; index < chunks; index++) {
      const session = db.createSession();
      db.prepare('INSERT INTO t VALUES (?,?)').run(BigInt(index + 1), `seed-${index}`);
      result.push(new Uint8Array(session.changeset()));
      session.close();
    }
    return result;
  } finally { db.close(); }
}
