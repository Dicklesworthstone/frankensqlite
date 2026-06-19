#!/usr/bin/env python3
# Deterministic churn generator for GitHub #113 repro.
ROWS = 8000
CYCLES = 12
BATCH = 5600  # ~70% churn per cycle

def row(id):
    j = id            # high-cardinality
    k = id % 8        # low-cardinality
    payload = "p%07d" % id
    return f"INSERT INTO t(id,j,k,payload) VALUES ({id},{j},{k},'{payload}');"

lines = []
lines.append("PRAGMA foreign_keys=off;")
lines.append("PRAGMA fsqlite.concurrent_mode = OFF;")
lines.append("CREATE TABLE t(id INTEGER PRIMARY KEY, j INTEGER, k INTEGER, payload TEXT);")
lines.append("CREATE INDEX idx_j ON t(j);")       # high-card
lines.append("CREATE INDEX idx_k ON t(k);")       # low-card
lines.append("CREATE INDEX idx_kj ON t(k,j);")    # composite

# bulk load
lines.append("BEGIN IMMEDIATE;")
for id in range(1, ROWS+1):
    lines.append(row(id))
lines.append("COMMIT;")

next_id = ROWS + 1
lo = 1
for c in range(CYCLES):
    lines.append("BEGIN IMMEDIATE;")
    hi = lo + BATCH - 1
    lines.append(f"DELETE FROM t WHERE id BETWEEN {lo} AND {hi};")
    lo = hi + 1
    for _ in range(BATCH):
        lines.append(row(next_id))
        next_id += 1
    # interleaved table-scan reads in same connection inside the txn
    lines.append("SELECT count(*) FROM t WHERE +id >= 0;")
    lines.append("SELECT max(payload) FROM t;")
    lines.append("PRAGMA integrity_check;")
    lines.append("COMMIT;")

# final
lines.append("PRAGMA integrity_check;")
lines.append("SELECT count(*) FROM t;")

with open("/tmp/churn.sql","w") as f:
    f.write("\n".join(lines) + "\n")
print("wrote /tmp/churn.sql lines:", len(lines))
