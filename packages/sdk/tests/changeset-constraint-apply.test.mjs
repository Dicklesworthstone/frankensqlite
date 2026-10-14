// Production session application with real SQLite statements and savepoints.
// This checks SDK policy/SQL ownership, not the Rust engine or browser/WASM.
import assert from "node:assert/strict";
import { DatabaseSync } from "node:sqlite";
import { test } from "node:test";
import { applyChangeset, applyPatchset } from "../src/changeset-apply.ts";
import { encodeChangeset, encodePatchset, decodeRebaseInfo } from "../src/changeset-codec.ts";
import { FrankenSQLiteError } from "../src/errors.ts";

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
  await assert.rejects(applyChangeset(target,wire([insert(1n,"taken")]),policy({onConflict:()=>{calls++;return "omit";}})),
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
