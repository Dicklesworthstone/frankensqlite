import { DatabaseSync } from 'node:sqlite';

// Reference SQL ownership only. The production queue is imported unchanged.
// Hooks delay real statements/COMMIT; they never interpret or simulate SQL.
export class JobSqliteTarget {
  constructor(path = ':memory:', ddl = '') {
    this.db = new DatabaseSync(path);
    this.db.exec('PRAGMA foreign_keys=ON;' + ddl);
    this.depth = 0;
    this.serial = 0;
    this.statements = [];
    this.beforeSql = null;
    this.afterSql = null;
    this.beforeCommit = null;
    this.afterCommit = null;
  }
  async execute(sql, params = []) {
    this.statements.push(sql);
    await this.beforeSql?.(sql, params, 'execute');
    const n = Number(this.db.prepare(sql).run(...params).changes);
    await this.afterSql?.(sql, params, 'execute');
    return n;
  }
  async query(sql, params = []) {
    this.statements.push(sql);
    await this.beforeSql?.(sql, params, 'query');
    const rows = this.db.prepare(sql).all(...params);
    await this.afterSql?.(sql, params, 'query');
    return { rows };
  }
  async transaction(work) {
    const nested = this.depth++ > 0, sp = `job_test_${++this.serial}`;
    let active = false;
    try {
      this.db.exec(nested ? `SAVEPOINT ${sp}` : 'BEGIN');
      active = true;
      const value = await work(this);
      await this.beforeCommit?.(nested);
      this.db.exec(nested ? `RELEASE ${sp}` : 'COMMIT');
      active = false;
      await this.afterCommit?.(nested);
      return value;
    } catch (cause) {
      if (active) {
        try { this.db.exec(nested ? `ROLLBACK TO ${sp}; RELEASE ${sp}` : 'ROLLBACK'); }
        catch (cleanup) { throw new AggregateError([cause, cleanup], 'Work and rollback failed'); }
      }
      throw cause;
    } finally { this.depth--; }
  }
  rows(sql = 'SELECT * FROM effects ORDER BY id', params = []) {
    return this.db.prepare(sql).all(...params).map(row => ({ ...row }));
  }
  close() { this.db.close(); }
}
export const gate = () => {
  let resolve;
  const promise = new Promise(r => { resolve = r; });
  return { promise, resolve };
};
export const tick = () => new Promise(resolve => setImmediate(resolve));
