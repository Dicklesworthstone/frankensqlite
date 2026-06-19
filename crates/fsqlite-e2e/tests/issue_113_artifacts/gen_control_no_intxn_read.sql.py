#!/usr/bin/env python3
# CONTROL: identical churn but NO interleaved table-scan read inside the txn.
ROWS=8000; CYCLES=12; BATCH=5600
def row(id):
    return f"INSERT INTO t(id,j,k,payload) VALUES ({id},{id},{id%8},'p%07d');" % id
L=["PRAGMA foreign_keys=off;","PRAGMA fsqlite.concurrent_mode = OFF;",
   "CREATE TABLE t(id INTEGER PRIMARY KEY, j INTEGER, k INTEGER, payload TEXT);",
   "CREATE INDEX idx_j ON t(j);","CREATE INDEX idx_k ON t(k);","CREATE INDEX idx_kj ON t(k,j);",
   "BEGIN IMMEDIATE;"]
for i in range(1,ROWS+1): L.append(row(i))
L.append("COMMIT;")
nid=ROWS+1; lo=1
for c in range(CYCLES):
    L.append("BEGIN IMMEDIATE;")
    hi=lo+BATCH-1
    L.append(f"DELETE FROM t WHERE id BETWEEN {lo} AND {hi};"); lo=hi+1
    for _ in range(BATCH):
        L.append(row(nid)); nid+=1
    # NO interleaved read here
    L.append("COMMIT;")
    L.append("PRAGMA integrity_check;")  # check AFTER commit, outside txn
L.append("PRAGMA integrity_check;")
open("/tmp/control_noread.sql","w").write("\n".join(L)+"\n")
print("wrote control")
