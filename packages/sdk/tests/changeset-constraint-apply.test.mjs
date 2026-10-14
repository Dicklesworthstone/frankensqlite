// Production session application with real SQLite statements and savepoints.
// This checks SDK policy/SQL ownership, not the Rust engine or browser/WASM.
import assert from "node:assert/strict";
import { DatabaseSync } from "node:sqlite";
import { test } from "node:test";
import { applyChangeset, applyPatchset } from "../src/changeset-apply.ts";
import { encodeChangeset, encodePatchset, decodeRebaseInfo } from "../src/changeset-codec.ts";
import { FrankenSQLiteError } from "../src/errors.ts";
import { withDeferredForeignKeys } from "../src/changeset-foreign-keys.ts";

const constraint = (extendedCode = 2067, extra = {}) => new FrankenSQLiteError({
  code: "SQLITE_CONSTRAINT", sqliteCode: 19, extendedCode,
  message: "constraint fixture", ...extra,
});
const inputError = { code: "ERR_FSQLITE_CHANGESET_INPUT" };
const conflictError = { code: "ERR_FSQLITE_CHANGESET_CONFLICT" };
const insert = (...values) => ({ operation: "insert", indirect: false, new: values });
const change = (old, next) => ({ operation: "update", indirect: false, old, new: next });
const wire = (rows, compact = false, name = "t", pk = [1, 0]) =>
  (compact ? encodePatchset : encodeChangeset)([{ name, primaryKey: pk, changes: rows }]);
const policy = (extra = {}) => ({ tables: ["t"], constraintConflicts: "isolate", ...extra });
const columns = (db, sql = "SELECT * FROM t ORDER BY id") => {
  const s = db.prepare(sql); s.setReadBigInts(true); s.setReturnArrays(true); return s.all();
};
const deferred = () => {
  let resolve; const promise = new Promise(r => { resolve = r; }); return { promise, resolve };
};

class Target {
  constructor(t, schema = "CREATE TABLE t(id INTEGER PRIMARY KEY,v TEXT UNIQUE)", seed = "") {
    this.db = new DatabaseSync(":memory:");
    this.db.exec("PRAGMA foreign_keys=ON;" + schema + ";" + seed);
    this.depth = 0; this.serial = 0; this.log = []; this.errors = [];
    this.beforeExecute = null; this.beforeRelease = null; this.beforeRollback = null;
    this.rollbackFailure = null; this.capable = true;
    t.after(() => this.db.close());
  }
  async transaction(work) {
    const depth = this.depth++, name = `constraint_scope_${++this.serial}`;
    this.log.push(["begin", depth]);
    this.db.exec(depth ? `SAVEPOINT ${name}` : "BEGIN");
    let active = true;
    const check = () => { assert.ok(active, "expired SQL scope"); };
    const tx = {
      execute: async (sql, params = []) => {
        check(); this.log.push(["execute", depth, sql]);
        await this.beforeExecute?.(sql, params, depth);
        try { return Number(this.db.prepare(sql).run(...params).changes); }
        catch (error) {
          // The native reference's SQLite result code crosses the same typed
          // SDK error boundary as worker errors. Do not classify by message.
          if (error.code === "ERR_SQLITE_ERROR" && (error.errcode & 255) === 19) {
            const typed = constraint(error.errcode); this.errors.push(typed); throw typed;
          }
          throw error;
        }
      },
      query: async (sql, params = []) => {
        check(); this.log.push(["query", depth, sql]);
        const s = this.db.prepare(sql); s.setReadBigInts(true); s.setReturnArrays(true);
        return { rowArrays: s.all(...params) };
      },
    };
    if (this.capable) tx.transaction = nested => { check(); return this.transaction(nested); };
    try {
      const result = await work(tx);
      active = false;
      await this.beforeRelease?.(depth);
      this.db.exec(depth ? `RELEASE ${name}` : "COMMIT");
      this.log.push(["release", depth]);
      return result;
    } catch (error) {
      active = false;
      try {
        await this.beforeRollback?.(depth, error);
        this.db.exec(depth ? `ROLLBACK TO ${name}; RELEASE ${name}` : "ROLLBACK");
        this.log.push(["rollback", depth]);
        if (depth && this.rollbackFailure !== null) throw this.rollbackFailure;
      } catch (cleanup) {
        throw new AggregateError([error, cleanup], "SQL and scope cleanup failed", { cause: error });
      }
      throw error;
    } finally { active = false; this.depth--; }
  }
}

for (const compact of [false, true]) {
  const apply = compact ? applyPatchset : applyChangeset;
  const label = compact ? "patchset" : "changeset";
  test(`${label}: constraint omission preserves other rows and matches native Session apply`, async t => {
    const schema = "CREATE TABLE t(id INTEGER PRIMARY KEY,v TEXT NOT NULL UNIQUE CHECK(v!='bad'))";
    const target = new Target(t, schema, "INSERT INTO t VALUES(9,'taken')");
    const rows = [insert(1n,"good"), insert(2n,"taken"), insert(3n,null), insert(4n,"bad"), insert(5n,"last")];
    const bytes = wire(rows, compact), conflicts = [];
    const result = await apply(target, bytes, policy({ onConflict: c => {
      assert.equal(c.kind,"constraint"); assert.ok(c.error instanceof FrankenSQLiteError);
      assert.equal(c.error, target.errors.at(-1));
      assert.deepEqual(target.log.at(-1),["rollback",1],"policy runs AFTER rollback");
      assert.ok(Object.isFrozen(c) && Object.isFrozen(c.change));
      conflicts.push([c.changeIndex,c.error.extendedCode]); return "omit";
    }}));
    assert.deepEqual(result,{applied:2,omitted:3,replayed:false});
    assert.deepEqual(conflicts,[[1,2067],[2,1299],[3,275]]);
    assert.equal(target.log.filter(([op,d])=>op==="begin"&&d===1).length,5);
    const oracle = new DatabaseSync(":memory:");
    try {
      oracle.exec(schema+";INSERT INTO t VALUES(9,'taken')"); const kinds=[];
      assert.equal(oracle.applyChangeset(bytes,{onConflict:kind=>{kinds.push(kind);return 0;}}),true);
      assert.deepEqual(kinds,[4,4,4]); assert.deepEqual(columns(target.db),columns(oracle));
      t.diagnostic(`Native Session oracle SQLite ${oracle.prepare("SELECT sqlite_version() v").get().v}`);
    } finally { oracle.close(); }
  });

  test(`${label}: rejected UPDATE restores its before-image and later work still applies`, async t => {
    const target = new Target(t,undefined,"INSERT INTO t VALUES(1,'first'),(2,'taken')");
    const before = compact ? [1n,undefined] : [1n,"first"];
    const bytes=wire([change(before,[undefined,"taken"]),insert(3n,"after")],compact);
    let called=0;
    assert.deepEqual(await apply(target,bytes,policy({onConflict:c=>{
      called++; assert.equal(c.kind,"constraint");
      assert.deepEqual(columns(target.db),[[1n,"first"],[2n,"taken"]]); return "omit";
    }})),{applied:1,omitted:1,replayed:false});
    assert.equal(called,1); assert.deepEqual(columns(target.db),[[1n,"first"],[2n,"taken"],[3n,"after"]]);
  });

  test(`${label}: failed PK replacement restores deleted row and trigger/FK effects`, async t => {
    const target = new Target(t, `CREATE TABLE t(id INTEGER PRIMARY KEY,v TEXT UNIQUE);
      CREATE TABLE child(id PRIMARY KEY REFERENCES t(id) ON DELETE CASCADE);
      CREATE TABLE audit(v); CREATE TRIGGER removed AFTER DELETE ON t BEGIN INSERT INTO audit VALUES(old.v); END`,
      "INSERT INTO t VALUES(1,'original'),(2,'taken');INSERT INTO child VALUES(1)");
    const seen=[];
    const bytes=wire([insert(1n,"taken"),insert(3n,"after")],compact);
    const result=await apply(target,bytes,policy({onConflict:c=>{
      seen.push(c.kind);
      if(c.kind==="conflict") return "replace";
      assert.equal(c.kind,"constraint");
      assert.deepEqual(columns(target.db),[[1n,"original"],[2n,"taken"]]);
      assert.deepEqual(columns(target.db,"SELECT * FROM child"),[[1n]]);
      assert.deepEqual(columns(target.db,"SELECT * FROM audit"),[]); return "omit";
    }}));
    assert.deepEqual(seen,["conflict","constraint"]);
    assert.deepEqual(result,{applied:1,omitted:1,replayed:false});
    assert.deepEqual(columns(target.db,"SELECT * FROM audit"),[]);
    assert.deepEqual(columns(target.db,"PRAGMA foreign_key_check"),[]);
  });
}

test("default application remains atomic with no child savepoints; opt-in is validated before SQL",async t=>{
  const target=new Target(t,undefined,"INSERT INTO t VALUES(9,'taken')");let calls=0;
  await assert.rejects(applyChangeset(target,wire([insert(1n,"good"),insert(2n,"taken")]),{
    tables:["t"],onConflict:()=>{calls++;return "omit";},
  }),e=>e instanceof FrankenSQLiteError);
  assert.equal(calls,0);assert.deepEqual(columns(target.db),[[9n,"taken"]]);
  assert.equal(target.log.filter(([op,d])=>op==="begin"&&d>0).length,0);
  target.log=[];target.capable=false;
  await assert.rejects(applyChangeset(target,wire([insert(1n,"good")]),policy({deliveryId:"new"})),inputError);
  assert.equal(target.log.some(([op])=>op==="execute"||op==="query"),false);
  target.log=[];
  await assert.rejects(applyChangeset(target,wire([insert(1n,"good")]),policy({constraintConflicts:"ignore"})),inputError);
  assert.equal(target.log.length,0);
});

test("RAISE(FAIL) partial trigger effects are rolled back before omission",async t=>{
  const target=new Target(t,`CREATE TABLE t(id INTEGER PRIMARY KEY,v);CREATE TABLE audit(v);
    CREATE TRIGGER reject BEFORE INSERT ON t WHEN new.id=2 BEGIN
      INSERT INTO audit VALUES('must not survive');SELECT RAISE(FAIL,'stop');END`);
  const result=await applyChangeset(target,wire([insert(1n,"before"),insert(2n,"no"),insert(3n,"after")]),policy({
    onConflict:c=>{assert.equal(c.error.extendedCode,1811);assert.deepEqual(columns(target.db,"SELECT * FROM audit"),[]);return "omit";},
  }));
  assert.deepEqual(result,{applied:2,omitted:1,replayed:false});
  assert.deepEqual(columns(target.db),[[1n,"before"],[3n,"after"]]);
});

test("rollback failures and transaction-wide RAISE(ROLLBACK) are never omittable",async t=>{
  for(const mode of ["cleanup","rollback"]){
    const schema=mode==="rollback"?`CREATE TABLE t(id INTEGER PRIMARY KEY,v);
      CREATE TRIGGER reject BEFORE INSERT ON t WHEN new.id=2 BEGIN SELECT RAISE(ROLLBACK,'stop');END`:
      "CREATE TABLE t(id INTEGER PRIMARY KEY,v UNIQUE)";
    const target=new Target(t,schema,"INSERT INTO t VALUES(9,'taken')");let called=0;
    if(mode==="cleanup")target.rollbackFailure=new Error("injected cleanup failure");
    await assert.rejects(applyChangeset(target,wire([insert(1n,"first"),insert(2n,"taken")]),policy({
      onConflict:()=>{called++;return "omit";},
    })),AggregateError);
    assert.equal(called,0);assert.deepEqual(columns(target.db),[[9n,"taken"]]);
  }
});

test("only the exact typed execute error may become a constraint decision",async t=>{
  const errors=[new Error("SQLITE_CONSTRAINT"),constraint(2067,{code:"SQLITE_IOERR"}),
    constraint(2067,{sqliteCode:5}),constraint(999),constraint(2067,{code:"SQLITE_CONSTRAINT_NOTNULL"}),
    constraint(2067,{userRecoverable:false}),constraint(2067,{cause:{code:"SQLITE_IOERR",message:"cause"}}),
    constraint(2067,{cleanupErrors:[{code:"SQLITE_IOERR",message:"cleanup"}]}),
    new AggregateError([constraint()],"not a single authoritative error")];
  for(const error of errors){
    const target=new Target(t);let called=0;
    target.beforeExecute=(sql)=>{if(sql.startsWith("INSERT OR ABORT INTO main.\"t\""))throw error;};
    await assert.rejects(applyChangeset(target,wire([insert(1n,"x")]),policy({onConflict:()=>{called++;return "omit";}})),e=>e===error);
    assert.equal(called,0);assert.deepEqual(columns(target.db),[]);
  }
  const accessor=constraint();Object.defineProperty(accessor,"code",{get(){throw new Error("do not trust getters");}});
  const target=new Target(t);target.beforeExecute=()=>{throw accessor;};
  await assert.rejects(applyChangeset(target,wire([insert(1n,"x")]),policy({onConflict:()=>"omit"})),e=>e===accessor);
});

test("policy-thrown constraints and child RELEASE failures are not execute failures",async t=>{
  for(const mode of ["policy","release"]){
    const target=new Target(t,undefined,"INSERT INTO t VALUES(1,'old')");const error=constraint();let called=0;
    if(mode==="release")target.beforeRelease=d=>{if(d===1)throw error;};
    await assert.rejects(applyChangeset(target,wire([insert(1n,"new")]),policy({onConflict:c=>{
      called++;assert.equal(c.kind,"conflict");if(mode==="policy")throw error;return "replace";
    }})),e=>e===error);
    assert.equal(called,1);assert.deepEqual(columns(target.db),[[1n,"old"]]);
  }
});

test("abort, invalid replacement and throwing constraint policies roll back the whole application",async t=>{
  for(const action of [undefined,"abort","replace","unexpected","throw"]){
    const target=new Target(t,undefined,"INSERT INTO t VALUES(9,'taken')");const thrown=new Error("policy failure");
    const options=policy({deliveryId:"not-committed"});
    if(action!==undefined)options.onConflict=()=>{if(action==="throw")throw thrown;return action;};
    await assert.rejects(applyChangeset(target,wire([insert(1n,"first"),insert(2n,"taken")]),options),
      action==="throw"?e=>e===thrown:action==="replace"||action==="unexpected"?inputError:conflictError);
    assert.deepEqual(columns(target.db),[[9n,"taken"]]);
    assert.deepEqual(columns(target.db,"SELECT name FROM sqlite_schema WHERE name='__fsqlite_changeset_receipts'"),[]);
  }
});

test("rows, native-shaped rebase decisions and delivery counts commit together and replay exactly once",async t=>{
  const target=new Target(t,"CREATE TABLE t(id INTEGER PRIMARY KEY,v TEXT UNIQUE);CREATE TABLE decisions(info BLOB)",
    "INSERT INTO t VALUES(1,'original'),(9,'taken')");let calls=0,hooks=0;
  const bytes=wire([insert(1n,"taken"),insert(2n,"taken"),insert(3n,"okay")]);
  const options=policy({deliveryId:"sender/7",onConflict:c=>{calls++;return c.kind==="conflict"?"replace":"omit";},
    onRebase:async(tx,info)=>{hooks++;await tx.execute("INSERT INTO decisions VALUES(?)",[info]);}});
  const result=await applyChangeset(target,bytes,options);
  assert.deepEqual(result,{applied:1,omitted:2,replayed:false});assert.equal(calls,3);assert.equal(hooks,1);
  const info=columns(target.db,"SELECT info FROM decisions")[0][0];
  const decisions=decodeRebaseInfo(info)[0].changes;
  assert.deepEqual(decisions.map(c=>[c.values[0],c.replace]),[[1n,true],[1n,false],[2n,false]]);
  assert.deepEqual(columns(target.db,"SELECT applied,omitted FROM __fsqlite_changeset_receipts"),[[1n,2n]]);
  const childCount=target.log.filter(([op,d])=>op==="begin"&&d===1).length;
  assert.deepEqual(await applyChangeset(target,bytes,options),{...result,replayed:true});
  assert.equal(target.log.filter(([op,d])=>op==="begin"&&d===1).length,childCount);
  assert.equal(calls,3);assert.equal(hooks,1);
});

test("cancellation cannot abandon child rollback or turn it into an omission",async t=>{
  const target=new Target(t,undefined,"INSERT INTO t VALUES(9,'taken')"), controller=new AbortController();
  const entered=deferred(),settle=deferred();let called=0,finished=false;
  target.beforeRollback=async d=>{if(d===1){entered.resolve();await settle.promise;}};
  const operation=applyChangeset(target,wire([insert(1n,"taken")]),policy({signal:controller.signal,onConflict:()=>{called++;return "omit";}}));
  void operation.then(()=>{finished=true;},()=>{finished=true;});
  await Promise.race([entered.promise,operation.then(()=>assert.fail("rollback was not entered"))]);
  controller.abort(new Error("stop"));await new Promise(r=>setImmediate(r));
  assert.equal(finished,false);assert.equal(target.depth,2);settle.resolve();
  await assert.rejects(operation,{code:"ERR_FSQLITE_CHANGESET_CANCELLED"});
  assert.equal(called,0);assert.equal(target.depth,0);assert.deepEqual(columns(target.db),[[9n,"taken"]]);
});

test("cleanup metadata added to the original SQL error during rollback still prevents omission",async t=>{
  const target=new Target(t,undefined,"INSERT INTO t VALUES(9,'taken')");let calls=0;
  target.beforeRollback=(depth,error)=>{if(depth===1)error.cleanupErrors.push(new FrankenSQLiteError({
    code:"SQLITE_IOERR",message:"cleanup obligation retained",
  }));};
  await assert.rejects(applyChangeset(target,wire([insert(1n,"taken")]),policy({onConflict:()=>{calls++;return "omit";} })),
    e=>e===target.errors[0]);
  assert.equal(calls,0);assert.deepEqual(columns(target.db),[[9n,"taken"]]);
});

test("nested application omits only rejected rows, retaining the outer caller's writes",async t=>{
  const target=new Target(t,undefined,"INSERT INTO t VALUES(9,'taken')");
  await target.transaction(async outer=>{
    await outer.execute("INSERT INTO t VALUES(10,'caller')");
    assert.deepEqual(await applyChangeset(outer,wire([insert(1n,"taken"),insert(2n,"valid")]),policy({onConflict:()=>"omit"})),
      {applied:1,omitted:1,replayed:false});
    await outer.execute("INSERT INTO t VALUES(11,'after')");
  });
  assert.deepEqual(columns(target.db),[[2n,"valid"],[9n,"taken"],[10n,"caller"],[11n,"after"]]);
});

for (const compact of [false, true]) {
  test(`deferred ${compact ? "patchset" : "changeset"}: isolated omission and later parents share one commit`, async t => {
    const target = new Target(t, `CREATE TABLE p(id INTEGER PRIMARY KEY);
      CREATE TABLE t(id INTEGER PRIMARY KEY,pid REFERENCES p(id),v TEXT UNIQUE);
      CREATE TABLE decisions(info BLOB)`);
    const tables = [
      { name: "t", primaryKey: [1,0,0], changes: [insert(1n,7n,"first"),insert(2n,7n,"first"),insert(3n,7n,"last")] },
      { name: "p", primaryKey: [1], changes: [insert(7n)] },
    ];
    const payload = (compact ? encodePatchset : encodeChangeset)(tables);
    let calls = 0;
    const options = policy({ tables:["t","p"], foreignKeys:"defer", deliveryId:`deferred:${compact}`,
      onConflict: c => {
        calls++; assert.equal(c.kind,"constraint"); assert.equal(target.depth,1);
        assert.deepEqual(columns(target.db),[[1n,7n,"first"]]);
        assert.equal(target.db.prepare("PRAGMA defer_foreign_keys").get().defer_foreign_keys,1);
        return "omit";
      },
      ...(!compact ? { onRebase:async(tx,info)=>tx.execute("INSERT INTO decisions VALUES(?)",[info]) } : {}),
    });
    const apply = compact ? applyPatchset : applyChangeset;
    const result = await apply(target,payload,options);
    assert.deepEqual(result,{applied:3,omitted:1,replayed:false});
    assert.equal(calls,1);assert.deepEqual(columns(target.db),[[1n,7n,"first"],[3n,7n,"last"]]);
    assert.deepEqual(columns(target.db,"SELECT * FROM p"),[[7n]]);
    assert.deepEqual(columns(target.db,"PRAGMA foreign_key_check"),[]);
    assert.equal(target.db.prepare("PRAGMA defer_foreign_keys").get().defer_foreign_keys,0);
    assert.deepEqual(columns(target.db,"SELECT applied,omitted FROM __fsqlite_changeset_receipts"),[[3n,1n]]);
    if (!compact) assert.equal(decodeRebaseInfo(columns(target.db,"SELECT info FROM decisions")[0][0])[0].changes[0].replace,false);
    const children=target.log.filter(([op,d])=>op==="begin"&&d===1).length;
    assert.deepEqual(await apply(target,payload,options),{...result,replayed:true});
    assert.equal(calls,1);assert.equal(target.log.filter(([op,d])=>op==="begin"&&d===1).length,children);
  });
}

test("deferred isolated application still rejects unresolved foreign keys and rolls back receipt plus rows",async t=>{
  const target=new Target(t,`CREATE TABLE p(id INTEGER PRIMARY KEY);
    CREATE TABLE t(id INTEGER PRIMARY KEY,v REFERENCES p(id))`);let calls=0;
  await assert.rejects(applyChangeset(target,wire([insert(1n,99n)]),policy({
    foreignKeys:"defer",deliveryId:"must-not-survive",onConflict:()=>{calls++;return "omit";},
  })),{code:"ERR_FSQLITE_FOREIGN_KEY_VIOLATION"});
  assert.equal(calls,0);assert.deepEqual(columns(target.db),[]);
  assert.deepEqual(columns(target.db,"SELECT name FROM sqlite_schema WHERE name='__fsqlite_changeset_receipts'"),[]);
  assert.equal(target.db.prepare("PRAGMA defer_foreign_keys").get().defer_foreign_keys,0);
});

test("a handled child rollback preserves parent work, while a caught direct SQL error still aborts",async t=>{
  const target=new Target(t);const wrapped=withDeferredForeignKeys(target);
  const expected=new Error("abort only the child");let parent,child;
  await wrapped.transaction(async tx=>{
    parent=tx;await tx.execute("INSERT INTO t VALUES(1,'parent')");
    await assert.rejects(tx.transaction(async nested=>{
      child=nested;await nested.execute("INSERT INTO t VALUES(2,'child')");throw expected;
    }),e=>e===expected);
    await tx.execute("INSERT INTO t VALUES(3,'after')");
  });
  assert.deepEqual(columns(target.db),[[1n,"parent"],[3n,"after"]]);
  const count=target.log.length;
  await assert.rejects(parent.query("SELECT 1"),{code:"ERR_FSQLITE_FOREIGN_KEY_STATE"});
  await assert.rejects(child.query("SELECT 1"),{code:"ERR_FSQLITE_FOREIGN_KEY_STATE"});
  await assert.rejects(child.transaction(async()=>assert.fail("expired child")),{code:"ERR_FSQLITE_FOREIGN_KEY_STATE"});
  assert.equal(target.log.length,count);
  await assert.rejects(wrapped.transaction(async tx=>{
    await tx.execute("INSERT INTO t VALUES(4,'before')");
    await assert.rejects(tx.execute("INSERT INTO t VALUES(5,'parent')"),FrankenSQLiteError);
    await tx.execute("INSERT INTO t VALUES(6,'after')");
  }),FrankenSQLiteError);
  assert.deepEqual(columns(target.db),[[1n,"parent"],[3n,"after"]]);
});

test("uncertain child rollback or RELEASE remains fatal even when its rejection is caught",async t=>{
  for(const mode of ["rollback","release","metadata"]){
    const target=new Target(t),wrapped=withDeferredForeignKeys(target);
    if(mode==="rollback") target.rollbackFailure=new Error("unconfirmed rollback");
    if(mode==="release") target.beforeRelease=d=>{if(d===1)throw new Error("unconfirmed release");};
    if(mode==="metadata") target.beforeRollback=(d,e)=>{if(d===1)e.cleanupErrors.push(new Error("retained obligation"));};
    await assert.rejects(wrapped.transaction(async tx=>{
      await tx.execute("INSERT INTO t VALUES(1,'outer')");
      await assert.rejects(tx.transaction(async child=>{
        await child.execute("INSERT INTO t VALUES(2,'child')");
        if(mode!=="release")throw constraint();
      }));
      await tx.execute("INSERT INTO t VALUES(3,'after-caught')");
    }));
    assert.deepEqual(columns(target.db),[],mode);assert.equal(target.depth,0);
  }
});

test("deferred work joins an unawaited child through its final release before restoring FK state",async t=>{
  const target=new Target(t),wrapped=withDeferredForeignKeys(target),entered=deferred(),settle=deferred();
  target.beforeRelease=async d=>{if(d===1){entered.resolve();await settle.promise;}};
  let completed=false;
  const operation=wrapped.transaction(async tx=>{void tx.transaction(async child=>{
    await child.execute("INSERT INTO t VALUES(1,'child')");
  });});
  void operation.then(()=>{completed=true;},()=>{completed=true;});
  await Promise.race([entered.promise,operation.then(()=>assert.fail("child did not reach release"))]);
  await new Promise(r=>setImmediate(r));assert.equal(completed,false);assert.equal(target.depth,2);
  assert.equal(target.db.prepare("PRAGMA defer_foreign_keys").get().defer_foreign_keys,1);
  settle.resolve();await operation;
  assert.deepEqual(columns(target.db),[[1n,"child"]]);assert.equal(target.depth,0);
});

test("cancellation joins child cleanup and cannot commit a deferred prefix",async t=>{
  const target=new Target(t),wrapped=withDeferredForeignKeys(target),entered=deferred(),settle=deferred();
  const controller=new AbortController();let completed=false;
  target.beforeRollback=async d=>{if(d===1){entered.resolve();await settle.promise;}};
  const operation=wrapped.transaction(async tx=>{
    await tx.execute("INSERT INTO t VALUES(1,'parent')");
    try { await tx.transaction(async child=>{await child.execute("INSERT INTO t VALUES(2,'child')");throw new Error("rollback");}); }
    catch { /* This local failure is handled, but outer cancellation still wins. */ }
  },{signal:controller.signal});
  void operation.then(()=>{completed=true;},()=>{completed=true;});
  await Promise.race([entered.promise,operation.then(()=>assert.fail("child did not reach rollback"))]);
  controller.abort("stop");await new Promise(r=>setImmediate(r));
  assert.equal(completed,false);assert.equal(target.depth,2);settle.resolve();
  await assert.rejects(operation,{code:"ERR_FSQLITE_FOREIGN_KEY_CANCELLED"});
  assert.deepEqual(columns(target.db),[]);assert.equal(target.depth,0);
});

test("parent and sibling work cannot interleave inside an active deferred child",async t=>{
  const target=new Target(t),wrapped=withDeferredForeignKeys(target),entered=deferred(),settle=deferred();
  await wrapped.transaction(async tx=>{
    const child=tx.transaction(async inner=>{
      await inner.execute("INSERT INTO t VALUES(1,'child')");entered.resolve();await settle.promise;
    });
    await entered.promise;const count=target.log.length;
    await assert.rejects(tx.execute("INSERT INTO t VALUES(2,'must not enter child')"),{code:"ERR_FSQLITE_FOREIGN_KEY_BUSY"});
    await assert.rejects(tx.transaction(async()=>assert.fail("sibling admitted")),{code:"ERR_FSQLITE_FOREIGN_KEY_BUSY"});
    assert.equal(target.log.length,count);settle.resolve();await child;
    await tx.execute("INSERT INTO t VALUES(3,'parent')");
  });
  assert.deepEqual(columns(target.db),[[1n,"child"],[3n,"parent"]]);
});

test("pending parent work blocks child admission and unsupported targets get no invented capability",async t=>{
  const target=new Target(t),wrapped=withDeferredForeignKeys(target),entered=deferred(),settle=deferred();
  target.beforeExecute=async(sql,params,d)=>{if(d===0&&sql.startsWith("INSERT")){entered.resolve();await settle.promise;}};
  await wrapped.transaction(async tx=>{
    const pending=tx.execute("INSERT INTO t VALUES(1,'parent')");await entered.promise;
    try {
      await assert.rejects(tx.transaction(async()=>assert.fail("child admitted early")),{code:"ERR_FSQLITE_FOREIGN_KEY_BUSY"});
    } finally { settle.resolve();await pending; }
    await assert.rejects(tx.transaction(null),{code:"ERR_FSQLITE_FOREIGN_KEY_INPUT"});
  });
  target.capable=false;
  await wrapped.transaction(async tx=>{assert.equal(tx.transaction,undefined);});
  assert.deepEqual(columns(target.db),[[1n,"parent"]]);
});

test("grandchildren preserve deferred dependencies until the outer clean check",async t=>{
  const target=new Target(t,`CREATE TABLE p(id INTEGER PRIMARY KEY);
    CREATE TABLE t(id INTEGER PRIMARY KEY,v REFERENCES p(id))`);
  await withDeferredForeignKeys(target).transaction(async tx=>{
    await tx.transaction(async child=>{
      await child.transaction(async grandchild=>{await grandchild.execute("INSERT INTO t VALUES(1,7)");});
      await child.execute("INSERT INTO p VALUES(7)");
    });
  });
  assert.deepEqual(columns(target.db),[[1n,7n]]);
  assert.deepEqual(columns(target.db,"PRAGMA foreign_key_check"),[]);
  assert.equal(target.log.filter(([op,d])=>op==="begin"&&d===2).length,1);
});

test("a thrown parent failure retains separately uncertain child cleanup",async t=>{
  const target=new Target(t),expected=new Error("parent failure"),childDone=deferred();
  target.rollbackFailure=new Error("child cleanup");
  await assert.rejects(withDeferredForeignKeys(target).transaction(async tx=>{
    const child=tx.transaction(async inner=>{await inner.execute("INSERT INTO t VALUES(1,'child')");throw constraint();});
    void child.catch(()=>childDone.resolve());await childDone.promise;throw expected;
  }),e=>e.code==="ERR_FSQLITE_FOREIGN_KEY_STATE"&&e.cause===expected&&e.cleanupErrors.length===1);
  assert.deepEqual(columns(target.db),[]);
});

test("grandchild cleanup uncertainty remains sticky through two caught rejections",async t=>{
  const target=new Target(t);
  target.beforeRollback=(depth,error)=>{if(depth===2)error.cleanupErrors.push(new Error("grandchild cleanup"));};
  await assert.rejects(withDeferredForeignKeys(target).transaction(async tx=>{
    await tx.execute("INSERT INTO t VALUES(1,'parent')");
    await assert.rejects(tx.transaction(async child=>{
      await assert.rejects(child.transaction(async grandchild=>{
        await grandchild.execute("INSERT INTO t VALUES(2,'grandchild')");throw constraint();
      }));
      await child.execute("INSERT INTO t VALUES(3,'child after caught')");
    }),{code:"ERR_FSQLITE_FOREIGN_KEY_STATE"});
    await tx.execute("INSERT INTO t VALUES(4,'parent after caught')");
  }),e=>e.code==="ERR_FSQLITE_FOREIGN_KEY_STATE"&&e.cleanupErrors.length===1);
  assert.deepEqual(columns(target.db),[]);assert.equal(target.depth,0);
});

test("caught uncertain child cleanup immediately fences parent reads, writes and new savepoints",async t=>{
  for(const mode of ["rollback","release","metadata"]){
    const target=new Target(t);let original,checked=0,admitted;
    if(mode==="rollback")target.rollbackFailure=new Error("cleanup unresolved");
    if(mode==="release")target.beforeRelease=d=>{if(d===1)throw new Error("release unresolved");};
    if(mode==="metadata")target.beforeRollback=(d,e)=>{if(d===1)e.cleanupErrors.push(new Error("late cleanup"));};
    await assert.rejects(withDeferredForeignKeys(target).transaction(async tx=>{
      try { await tx.transaction(async child=>{
        await child.execute("INSERT INTO t VALUES(1,'tentative')");
        if(mode!=="release")throw constraint();
      });assert.fail("uncertain child must reject"); }
      catch(error){original=error;}
      const count=target.log.length;
      for(const operation of [
        ()=>tx.execute("INSERT INTO t VALUES(2,'must not be admitted')"),
        ()=>tx.query("SELECT * FROM t"),
        ()=>tx.transaction(async()=>assert.fail("new child admitted after uncertain cleanup")),
      ]) {
        await assert.rejects(operation(),e=>e.code==="ERR_FSQLITE_FOREIGN_KEY_STATE"&&e.cause===original&&e.cleanupErrors[0]===original);
        checked++;
      }
      admitted=target.log.length-count;
    }),{code:"ERR_FSQLITE_FOREIGN_KEY_STATE"});
    // Assertions inside uncertain work can themselves be wrapped by the
    // expected outer STATE error. Prove all checks completed outside it.
    assert.equal(checked,3,mode);assert.equal(admitted,0,mode);
    assert.deepEqual(columns(target.db),[]);assert.equal(target.depth,0);
  }
});

test("grandchild cleanup fences each ancestor while confirmed child rollback still permits progress",async t=>{
  const target=new Target(t);let checkedChild=false,checkedParent=false,childWrites,parentWrites;
  target.beforeRollback=(d,e)=>{if(d===2)e.cleanupErrors.push(new Error("grandchild unresolved"));};
  await assert.rejects(withDeferredForeignKeys(target).transaction(async tx=>{
    await assert.rejects(tx.transaction(async child=>{
      await assert.rejects(child.transaction(async inner=>{throw constraint();}));
      const count=target.log.length;
      await assert.rejects(child.query("SELECT 1"),{code:"ERR_FSQLITE_FOREIGN_KEY_STATE"});
      checkedChild=true;childWrites=target.log.length-count;
    }),{code:"ERR_FSQLITE_FOREIGN_KEY_STATE"});
    const count=target.log.length;
    await assert.rejects(tx.execute("INSERT INTO t VALUES(1,'must not enter')"),{code:"ERR_FSQLITE_FOREIGN_KEY_STATE"});
    checkedParent=true;parentWrites=target.log.length-count;
  }),{code:"ERR_FSQLITE_FOREIGN_KEY_STATE"});
  assert.equal(checkedChild,true);assert.equal(checkedParent,true);
  assert.equal(childWrites,0);assert.equal(parentWrites,0);
  target.beforeRollback=null;
  await withDeferredForeignKeys(target).transaction(async tx=>{
    const error=new Error("ordinary child rollback");
    await assert.rejects(tx.transaction(async()=>{throw error;}),e=>e===error);
    await tx.execute("INSERT INTO t VALUES(2,'fresh confirmed scope')");
  });
  assert.deepEqual(columns(target.db),[[2n,"fresh confirmed scope"]]);
});
