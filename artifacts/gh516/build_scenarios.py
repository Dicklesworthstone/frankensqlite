#!/usr/bin/env python3
"""Build the GH516 corpus and check its success/failure expectations on stock SQLite.

The native Rust tests use rusqlite's bundled SQLite as the live differential
oracle; this Python check only proves the corpus itself executes as intended.
"""
import sys
import json, pathlib, sqlite3
out=[]
def e(sql,ok=True): return {'sql':sql,'query':False,'ok':ok}
def q(sql): return {'sql':sql,'query':True,'ok':True}
def add(name,setup,steps): out.append({'name':name,'setup':setup,'steps':steps})
def audit(timing,when='1',body=None):
    body=body or "INSERT INTO audit VALUES ('%s',NEW.id,NEW.v,(SELECT count(*) FROM t));"%timing
    return f"CREATE TRIGGER t_{timing} {timing} INSERT ON t WHEN {when} BEGIN {body} END"
base=["CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT NOT NULL UNIQUE CHECK(length(v)>0))", "CREATE TABLE audit(phase TEXT,id INTEGER,v TEXT,n INTEGER)"]
for timing in ['BEFORE','AFTER']:
 for pred,name in [('0','false'),('1','true'),('NEW.id%2=0','mixed'),('NULL','null')]:
  add(f'{timing.lower()}_when_{name}',base+[audit(timing,pred)], [e("INSERT INTO t VALUES(1,'a'),(2,'b'),(3,'c'),(4,'d')"),q('SELECT changes(),total_changes(),last_insert_rowid()'),q('SELECT * FROM audit ORDER BY rowid'),q('SELECT * FROM t ORDER BY id')])
add('before_after_row_interleaving',base+[audit('BEFORE'),audit('AFTER')],[e("INSERT INTO t VALUES(1,'a'),(2,'b'),(3,'c')"),q('SELECT * FROM audit ORDER BY rowid')])
add('when_observes_prior_row_side_effect',base+["CREATE TABLE flag(n INTEGER)","INSERT INTO flag VALUES(0)",audit('BEFORE','(SELECT n FROM flag)=NEW.id-1'), audit('AFTER','1','UPDATE flag SET n=NEW.id;')],[e("INSERT INTO t VALUES(1,'a'),(2,'b'),(3,'c')"),q('SELECT * FROM audit ORDER BY rowid'),q('SELECT * FROM flag')])
for constraint,values in [('unique',"(1,'a'),(2,'a'),(3,'c')"),('not_null',"(1,'a'),(2,NULL),(3,'c')"),('check',"(1,'a'),(2,''),(3,'c')"),('primary_key',"(1,'a'),(1,'b'),(3,'c')")]:
 for action in ['', 'OR ABORT','OR IGNORE','OR FAIL','OR ROLLBACK']:
  ok=action=='OR IGNORE'
  add(f"{constraint}_{action or 'default'}",base+[audit('BEFORE'),audit('AFTER')], [e('BEGIN'),e("INSERT INTO t VALUES(99,'prior')"),e(f'INSERT {action} INTO t VALUES{values}',ok),q('SELECT changes(),total_changes(),last_insert_rowid()'),q('SELECT * FROM t ORDER BY id'),q('SELECT * FROM audit ORDER BY rowid'),e('ROLLBACK',action!='OR ROLLBACK'),q('SELECT * FROM t ORDER BY id')])
for timing in ['BEFORE','AFTER']:
 for action in ['IGNORE','ABORT','FAIL','ROLLBACK']:
  raise_expr='RAISE(IGNORE)' if action=='IGNORE' else f"RAISE({action},'row two')"
  body=f"INSERT INTO audit VALUES('{timing}',NEW.id,NEW.v,(SELECT count(*) FROM t)); SELECT {raise_expr} WHERE NEW.id=2;"
  add(f'raise_{timing.lower()}_{action.lower()}',base+[audit(timing,body=body)], [e('BEGIN'),e("INSERT INTO t VALUES(99,'prior')"),e("INSERT INTO t VALUES(1,'a'),(2,'b'),(3,'c')",action=='IGNORE'),q('SELECT changes(),total_changes(),last_insert_rowid()'),q('SELECT * FROM t ORDER BY id'),q('SELECT * FROM audit ORDER BY rowid'),e('ROLLBACK',action!='ROLLBACK'),q('SELECT * FROM t ORDER BY id')])
add('autocommit_abort_is_atomic',base+[audit('AFTER')],[e("INSERT INTO t VALUES(1,'a'),(2,'a'),(3,'c')",False),q('SELECT * FROM t'),q('SELECT * FROM audit'),e("INSERT INTO t VALUES(10,'x'),(11,'y')"),q('SELECT * FROM t ORDER BY id')])
add('user_savepoint_survives_abort',base+[audit('AFTER')],[e('BEGIN'),e("INSERT INTO t VALUES(99,'prior')"),e('SAVEPOINT caller'),e("INSERT INTO t VALUES(1,'a'),(2,'a')",False),e("INSERT INTO t VALUES(3,'c'),(4,'d')"),e('ROLLBACK TO caller'),q('SELECT * FROM t ORDER BY id'),q('SELECT * FROM audit ORDER BY rowid'),e('RELEASE caller'),e('COMMIT')])
add('replace_preserves_inbound_fk_actions', ['PRAGMA foreign_keys=ON','PRAGMA recursive_triggers=ON']+base+["CREATE TABLE child(id INTEGER PRIMARY KEY,p INTEGER REFERENCES t(id) ON DELETE CASCADE)","INSERT INTO t VALUES(1,'a')","INSERT INTO child VALUES(7,1)",audit('BEFORE'),audit('AFTER'),"CREATE TRIGGER child_ad AFTER DELETE ON child BEGIN INSERT INTO audit VALUES('DELETE',OLD.id,'child',OLD.p); END"], [e("INSERT OR REPLACE INTO t VALUES(2,'a'),(3,'c')"),q('SELECT * FROM t ORDER BY id'),q('SELECT * FROM child'),q('SELECT * FROM audit ORDER BY rowid')])
fkbase=['PRAGMA foreign_keys=ON','CREATE TABLE p(id INTEGER PRIMARY KEY)','CREATE TABLE t(id INTEGER PRIMARY KEY,pid INTEGER REFERENCES p(id))','CREATE TABLE audit(id INTEGER,pid INTEGER)']
add('fk_immediate_error_undoes_prefix',fkbase+['INSERT INTO p VALUES(1)', 'CREATE TRIGGER ai AFTER INSERT ON t BEGIN INSERT INTO audit VALUES(NEW.id,NEW.pid); END'],[e('BEGIN'),e('INSERT INTO t VALUES(1,1),(2,999),(3,1)',False),q('SELECT * FROM t'),q('SELECT * FROM audit'),e('COMMIT')])
for timing in ['BEFORE','AFTER']:
 add(f'fk_parent_created_by_{timing.lower()}',fkbase+[f'CREATE TRIGGER parent {timing} INSERT ON t BEGIN INSERT OR IGNORE INTO p VALUES(NEW.pid); END','CREATE TRIGGER log AFTER INSERT ON t BEGIN INSERT INTO audit VALUES(NEW.id,NEW.pid); END'],[e('INSERT INTO t VALUES(1,10),(2,20),(3,10)'),q('SELECT * FROM p ORDER BY id'),q('SELECT * FROM t ORDER BY id'),q('SELECT * FROM audit ORDER BY rowid')])
add('fk_forward_reference_final_image',['PRAGMA foreign_keys=ON','CREATE TABLE t(id INTEGER PRIMARY KEY,pid INTEGER REFERENCES t(id))','CREATE TABLE audit(id INTEGER)','CREATE TRIGGER ai AFTER INSERT ON t BEGIN INSERT INTO audit VALUES(NEW.id); END'],[e('INSERT INTO t VALUES(1,2),(2,NULL)'),q('SELECT * FROM t ORDER BY id'),q('SELECT * FROM audit ORDER BY rowid')])
add('deferred_fk_failed_commit_keeps_transaction',['PRAGMA foreign_keys=ON','CREATE TABLE p(id INTEGER PRIMARY KEY)','CREATE TABLE t(id INTEGER PRIMARY KEY,pid INTEGER REFERENCES p(id) DEFERRABLE INITIALLY DEFERRED)','CREATE TABLE audit(id INTEGER)','CREATE TRIGGER ai AFTER INSERT ON t BEGIN INSERT INTO audit VALUES(NEW.id); END'],[e('BEGIN'),e('INSERT INTO t VALUES(1,10),(2,20)'),e('COMMIT',False),q('SELECT * FROM t ORDER BY id'),e('INSERT INTO p VALUES(10),(20)'),e('COMMIT'),q('SELECT * FROM audit ORDER BY rowid')])
add('nested_update_old_new',base+['CREATE TABLE totals(id INTEGER PRIMARY KEY,n INTEGER)','INSERT INTO totals VALUES(1,0)','CREATE TABLE history(old INTEGER,new INTEGER)',audit('AFTER',body='UPDATE totals SET n=n+NEW.id WHERE id=1;'),'CREATE TRIGGER totals_au AFTER UPDATE ON totals BEGIN INSERT INTO history VALUES(OLD.n,NEW.n); END'],[e("INSERT INTO t VALUES(1,'a'),(2,'b'),(3,'c')"),q('SELECT * FROM history ORDER BY rowid'),q('SELECT * FROM totals')])
add('recursive_insert',base+['PRAGMA recursive_triggers=ON',audit('AFTER','NEW.id<3',"INSERT INTO t VALUES(NEW.id+10, NEW.v||'+');")],[e("INSERT INTO t VALUES(1,'a'),(2,'b'),(3,'c')"),q('SELECT * FROM t ORDER BY id')])
add('default_affinity_generated_autoincrement',['CREATE TABLE t(id INTEGER PRIMARY KEY AUTOINCREMENT,v INTEGER DEFAULT 7,g INTEGER GENERATED ALWAYS AS(v*2) STORED)','CREATE TABLE audit(phase TEXT,id INTEGER,v INTEGER,g INTEGER)','CREATE TRIGGER bi BEFORE INSERT ON t BEGIN INSERT INTO audit VALUES(\'B\',NEW.id,NEW.v,NEW.g); END','CREATE TRIGGER ai AFTER INSERT ON t BEGIN INSERT INTO audit VALUES(\'A\',NEW.id,NEW.v,NEW.g); END'],[e("INSERT INTO t(v) VALUES('4'),('5'),('6')"),q('SELECT * FROM t ORDER BY id'),q('SELECT * FROM audit ORDER BY rowid'),e('INSERT INTO t(id) VALUES(NULL),(NULL)'),q('SELECT * FROM t ORDER BY id'),q('SELECT * FROM sqlite_sequence')])
add('explicit_and_implicit_rowid',['CREATE TABLE t(v TEXT)','CREATE TABLE audit(phase TEXT,r INTEGER,v TEXT)','CREATE TRIGGER bi BEFORE INSERT ON t BEGIN INSERT INTO audit VALUES(\'B\',NEW.rowid,NEW.v); END','CREATE TRIGGER ai AFTER INSERT ON t BEGIN INSERT INTO audit VALUES(\'A\',NEW.rowid,NEW.v); END'],[e("INSERT INTO t(rowid,v) VALUES(40,'x'),(50,'y')"),e("INSERT INTO t VALUES('a'),('b')"),q('SELECT rowid,* FROM t ORDER BY rowid'),q('SELECT * FROM audit ORDER BY rowid')])
add('returning_conservative_path',base+[audit('AFTER')],[q("INSERT INTO t VALUES(1,'a'),(2,'b') RETURNING id,v"),q('SELECT * FROM audit ORDER BY rowid')])
add('upsert_update_conservative_path',base+['CREATE TABLE history(old TEXT,new TEXT)',audit('BEFORE'),audit('AFTER'),'CREATE TRIGGER au AFTER UPDATE ON t BEGIN INSERT INTO history VALUES(OLD.v,NEW.v); END',"INSERT INTO t VALUES(1,'old')"],[e("INSERT INTO t VALUES(1,'new'),(2,'two') ON CONFLICT(id) DO UPDATE SET v=excluded.v"),q('SELECT * FROM t ORDER BY id'),q('SELECT * FROM audit ORDER BY rowid'),q('SELECT * FROM history')])
add('insert_select_conservative_path',base+[audit('BEFORE'),audit('AFTER'),'CREATE TABLE source(id INTEGER,v TEXT)',"INSERT INTO source VALUES(1,'a'),(2,'b')"],[e('INSERT INTO t SELECT * FROM source ORDER BY id'),q('SELECT * FROM audit ORDER BY rowid')])
# Check the corpus is valid stock SQLite and that expected failures are intentional.
checks=0
for scenario in out:
 c=sqlite3.connect(':memory:',isolation_level=None)
 for sql in scenario['setup']: c.execute(sql)
 for step in scenario['steps']:
  try:
   cur=c.execute(step['sql'])
   rows=cur.fetchall() if step['query'] else None
   ok=True
  except sqlite3.Error:
   ok=False
  assert ok == step['ok'], (scenario['name'],step)
  checks+=1
 c.close()
p=pathlib.Path(sys.argv[1])
if p.exists(): raise ValueError(f'refusing to overwrite {p}')
p.write_text(json.dumps(out,indent=2)+'\n')
print('Stock SQLite',sqlite3.sqlite_version,':',len(out),'scenarios,',checks,'steps validated')
