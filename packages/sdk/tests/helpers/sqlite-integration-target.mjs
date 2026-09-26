import assert from 'node:assert/strict';
import { DatabaseSync } from 'node:sqlite';

/** Reference SQL ownership only. All replication modules run unmodified. */
export class SQLiteTarget {
  constructor(path = ':memory:', encoding = 'UTF-8', journal = 'WAL') {
    assert.ok(['UTF-8', 'UTF-16le', 'UTF-16be'].includes(encoding));
    assert.ok(['WAL', 'DELETE'].includes(journal));
    this.db = new DatabaseSync(path);
    this.db.exec(`PRAGMA encoding='${encoding}'; PRAGMA journal_mode=${journal}; PRAGMA foreign_keys=ON;`);
    this.depth = 0;
    this.serial = 0;
    this.onExecute = null;
    this.onQuery = null;
    this.onCommit = null;
  }
  async execute(sql, params = []) {
    const changes = Number(this.db.prepare(sql).run(...params).changes);
    await this.onExecute?.(sql, params, changes);
    return changes;
  }
  async query(sql, params = []) {
    const statement = this.db.prepare(sql);
    statement.setReadBigInts(true);
    statement.setReturnArrays(true);
    const rowArrays = statement.all(...params);
    await this.onQuery?.(sql, params, rowArrays);
    return { rowArrays };
  }
  async transaction(work) {
    const nested = this.depth > 0, name = `integration_${++this.serial}`;
    this.db.exec(nested ? `SAVEPOINT ${name}` : 'BEGIN');
    this.depth++;
    let committed = false;
    try {
      const result = await work(this);
      this.db.exec(nested ? `RELEASE ${name}` : 'COMMIT');
      committed = true;
      if (!nested) await this.onCommit?.();
      return result;
    } catch (error) {
      if (!committed) this.db.exec(nested ? `ROLLBACK TO ${name}; RELEASE ${name}` : 'ROLLBACK');
      throw error;
    } finally { this.depth--; }
  }
  close() { this.db.close(); }
}

/** The oracle generates SQLite Session bytes; it does not encode a stand-in wire. */
export function nativeChanges(work, schema = 'CREATE TABLE notes(id INTEGER PRIMARY KEY, value TEXT)', initial = '') {
  const db = new DatabaseSync(':memory:');
  db.exec(schema + ';' + initial);
  const session = db.createSession();
  try { work(db); return new Uint8Array(session.changeset()); }
  finally { session.close(); db.close(); }
}
