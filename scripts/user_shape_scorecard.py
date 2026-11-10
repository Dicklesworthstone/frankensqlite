#!/usr/bin/env python3
"""User-shape scorecard: FrankenSQLite CLI builds vs stock sqlite3 (bd-w4pr1.4).

Times the workloads a user of an embedded database actually runs, on
file-backed databases at realistic size, through each engine's command-line
shell: bulk load of 1M rows, scans, GROUP BY, point lookups, joins, sorting,
windows, index builds, UPDATE/DELETE, autocommit and small-transaction writes,
an in-memory script, and 8 concurrent writer processes.

Every arm runs the identical SQL; each workload's final answer is compared
with the first arm (stock sqlite3), so a fast wrong answer is flagged. Each
run records wall time and peak RSS (wait4), and each arm records the binary's
SHA-256, its version output, and host load before and after.

Run it in its own memory-capped cgroup. A runaway build (an old release grew to
16 GB here) must not take the calling session with it:

    systemd-run --user --scope --quiet -p MemoryMax=12G -p MemorySwapMax=0 \\
        python3 -I scripts/user_shape_scorecard.py --work /dev/shm/scorecard \\
        --out scorecard.json --md scorecard.md \\
        sqlite3=/usr/bin/sqlite3 head=target/release-perf/fsqlite \\
        v0.4.13=/path/to/fsqlite-0.4.13 sqlite3_end=/usr/bin/sqlite3

The first arm is the baseline for ratios (time_arm / time_baseline; >1 means
slower than the baseline). Repeat the stock arm last to see host drift (an A/A
check). The run is resumable: arms already present in --out are skipped.
This is a diagnostic, not release evidence: see bd-uh1fv for the citable
gauntlet requirements.
"""
import argparse
import hashlib
import json
import os
import platform
import re
import statistics
import subprocess
import threading
import time

PRE = "PRAGMA journal_mode=WAL;\nPRAGMA synchronous=NORMAL;\n"
N = 1_000_000

BASE_SETUP = PRE + f"""
CREATE TABLE t(id INTEGER PRIMARY KEY, a INTEGER, b TEXT, c REAL);
BEGIN;
INSERT INTO t SELECT value, (value*7919)%1000, 'row-'||value, value*1.5 FROM generate_series(1,{N});
COMMIT;
SELECT count(*), sum(a) FROM t;
"""
BASE_EXTRA = PRE + """
CREATE TABLE u(a INTEGER PRIMARY KEY, name TEXT);
INSERT INTO u SELECT value, 'name-'||value FROM generate_series(0,999);
CREATE TABLE p(k INTEGER);
INSERT INTO p SELECT (value*7919)%1000000+1 FROM generate_series(1,200000);
CREATE TABLE v1(k INTEGER, x INTEGER);
CREATE TABLE v2(k INTEGER, y INTEGER);
INSERT INTO v1 SELECT (value*31)%5000, value FROM generate_series(1,5000);
INSERT INTO v2 SELECT (value*17)%5000, value FROM generate_series(1,5000);
SELECT count(*) FROM p;
"""
READS = {
    "scan_filter_agg": "SELECT count(*), sum(a) FROM t WHERE a > 500;",
    "group_by_1000_groups": "SELECT count(*), sum(n) FROM (SELECT a, count(*) n FROM t GROUP BY a);",
    "rowid_lookups_200k": "SELECT count(*), sum(t.a) FROM p CROSS JOIN t WHERE t.id = p.k;",
    "join_1M_to_pk_1000": "SELECT count(*), sum(length(u.name)) FROM t JOIN u ON u.a = t.a;",
    "order_by_text_limit10": "SELECT count(*), sum(id) FROM (SELECT id FROM t ORDER BY b DESC LIMIT 10);",
    "window_row_number": "SELECT sum(rn) FROM (SELECT row_number() OVER (PARTITION BY a ORDER BY c) rn FROM t);",
    "join_no_index_5k_x_5k": "SELECT count(*) FROM v1 JOIN v2 ON v1.k = v2.k;",
    "count_star": "SELECT count(*) FROM t;",
}
MUT_SETUP = PRE + """
CREATE TABLE m(id INTEGER PRIMARY KEY, a INTEGER, b TEXT, c REAL);
BEGIN;
INSERT INTO m SELECT value, (value*7919)%1000, 'row-'||value, value*1.5 FROM generate_series(1,300000);
COMMIT;
SELECT count(*) FROM m;
"""
MUTS = {
    "create_index_int_300k": "CREATE INDEX m_a ON m(a);\nSELECT count(*) FROM m;",
    "update_10pct_300k": "UPDATE m SET a = a + 1 WHERE id % 10 = 0;\nSELECT sum(a) FROM m;",
    "delete_14pct_300k": "DELETE FROM m WHERE id % 7 = 0;\nSELECT count(*) FROM m;",
}
MEM_SCRIPT = """
CREATE TABLE t(id INTEGER PRIMARY KEY, a INTEGER, b TEXT, c REAL);
INSERT INTO t SELECT value, (value*7919)%1000, 'row-'||value, value*1.5 FROM generate_series(1,200000);
CREATE TABLE u(a INTEGER PRIMARY KEY, name TEXT);
INSERT INTO u SELECT value, 'name-'||value FROM generate_series(0,999);
SELECT count(*), sum(a) FROM t WHERE a > 500;
SELECT count(*), sum(n) FROM (SELECT a, count(*) n FROM t GROUP BY a);
SELECT count(*), sum(length(u.name)) FROM t JOIN u ON u.a = t.a;
UPDATE t SET a = a + 1 WHERE id % 10 = 0;
DELETE FROM t WHERE id % 7 = 0;
SELECT count(*), sum(a) FROM t;
"""


def autocommit_script(n):
    lines = [PRE, "CREATE TABLE e(id INTEGER PRIMARY KEY, w INTEGER, s TEXT);"]
    lines += [f"INSERT INTO e(w,s) VALUES({i},'payload-{i}');" for i in range(n)]
    lines.append("SELECT count(*) FROM e;")
    return "\n".join(lines) + "\n"


def txn_script():
    lines = [PRE, "CREATE TABLE e(id INTEGER PRIMARY KEY, w INTEGER, s TEXT);"]
    for i in range(1000):
        lines.append("BEGIN;")
        lines += [f"INSERT INTO e(w,s) VALUES({i},'p-{i}-{j}');" for j in range(10)]
        lines.append("COMMIT;")
    lines.append("SELECT count(*) FROM e;")
    return "\n".join(lines) + "\n"


def oom_victim_first():
    # The engine, never this driver, should be the OOM killer's first choice.
    with open("/proc/self/oom_score_adj", "w", encoding="ascii") as fh:
        fh.write("1000")


def spawn(binp, db, stdin_bytes):
    return subprocess.Popen([binp, db], stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                            stderr=subprocess.PIPE, preexec_fn=oom_victim_first)


def reap(p, deadline):
    """Wait for p with a deadline; return (rc, timed_out, peak_rss_kib)."""
    fired = threading.Event()

    def kill():
        fired.set()
        p.kill()

    timer = threading.Timer(max(0.0, deadline - time.perf_counter()), kill)
    timer.start()
    _, status, usage = os.wait4(p.pid, 0)
    timer.cancel()
    p.returncode = os.waitstatus_to_exitcode(status)
    return p.returncode, fired.is_set(), usage.ru_maxrss


def run(binp, db, script, timeout):
    t0 = time.perf_counter()
    p = spawn(binp, db, script)
    out, err = [], []
    readers = [threading.Thread(target=lambda s=p.stdout: out.append(s.read())),
               threading.Thread(target=lambda s=p.stderr: err.append(s.read()))]
    for r in readers:
        r.start()
    try:
        p.stdin.write(script.encode())
        p.stdin.close()
    except BrokenPipeError:
        pass
    rc, timed_out, rss = reap(p, t0 + timeout)
    wall = time.perf_counter() - t0
    for r in readers:
        r.join()
    return {"s": wall, "rc": rc, "timeout": timed_out, "rss_kib": rss,
            "out": b"".join(out).decode(errors="replace").strip(),
            "err": b"".join(err).decode(errors="replace").strip()[:300]}


def last_line(s):
    lines = [x for x in s.strip().split("\n") if x.strip()]
    return re.sub(r"[\s']", "", lines[-1]) if lines else ""


def summarize(runs):
    ok = [r for r in runs if not r["timeout"] and r["rc"] == 0 and "rror" not in r["err"]]
    return {"median_s": round(statistics.median(r["s"] for r in ok), 4) if ok else None,
            "all_s": [round(r["s"], 3) for r in runs],
            "peak_rss_mib": max((r["rss_kib"] for r in runs), default=0) // 1024,
            "timeouts": sum(r["timeout"] for r in runs),
            "errors": sum(1 for r in runs if r not in ok and not r["timeout"]),
            "answer": last_line(runs[-1]["out"]),
            "err": runs[-1]["err"][:160]}


def multiproc(binp, work, tag, timeout, nproc=8, nrows=500):
    db = os.path.join(work, f"{tag}_mp_{time.time_ns()}.db")
    r0 = run(binp, db, PRE + "CREATE TABLE e0(id INTEGER PRIMARY KEY, w INTEGER, s TEXT);\n", timeout)
    if r0["rc"] != 0:
        return dict(r0, err="setup: " + r0["err"])
    t0 = time.perf_counter()
    procs = []
    for k in range(nproc):
        body = ["PRAGMA busy_timeout=10000;"]
        body += [f"INSERT INTO e0(w,s) VALUES({k},'w{k}-{i}');" for i in range(nrows)]
        p = spawn(binp, db, None)
        p.stdin.write(("\n".join(body) + "\n").encode())
        p.stdin.close()
        procs.append(p)
    rss, timed_out, errs = 0, False, 0
    for p in procs:
        _, to, r = reap(p, t0 + timeout)
        rss, timed_out = max(rss, r), timed_out or to
        errs += p.stderr.read().decode(errors="replace").count("rror")
    wall = time.perf_counter() - t0
    count = run(binp, db, "SELECT count(*) FROM e0;\n", timeout)
    return {"s": wall, "rc": 0 if errs == 0 else 1, "timeout": timed_out, "rss_kib": rss,
            "out": f"{last_line(count['out'])}of{nproc * nrows}", "err": f"{errs} error lines"}


def provenance(binp):
    with open(binp, "rb") as fh:
        data = fh.read()
    try:
        version = subprocess.run([binp, "--version"], capture_output=True, text=True,
                                 timeout=10).stdout.strip()[:80]
    except (OSError, subprocess.SubprocessError):
        version = "unknown"
    return {"path": binp, "sha256": hashlib.sha256(data).hexdigest(), "version": version,
            "loadavg_start": os.getloadavg()[0]}


def measure(label, binp, work, reps, timeout):
    tag = re.sub(r"[^A-Za-z0-9]", "_", label)
    res = {"_provenance": provenance(binp)}
    base, runs = None, []
    for _ in range(reps):
        db = os.path.join(work, f"{tag}_bulk_{time.time_ns()}.db")
        r = run(binp, db, BASE_SETUP, timeout)
        runs.append(r)
        if base is None and r["rc"] == 0 and not r["timeout"]:
            base = db
    res["bulk_insert_1M_one_txn"] = summarize(runs)
    if base:
        extra = run(binp, base, BASE_EXTRA, timeout)
        if extra["rc"] != 0 or extra["timeout"]:
            res["_setup_error"] = extra["err"] or "timeout"
        for name, q in READS.items():
            res[name] = summarize([run(binp, base, q + "\n", timeout) for _ in range(reps)])
    for name, q in MUTS.items():
        runs = []
        for _ in range(reps):
            db = os.path.join(work, f"{tag}_{name}_{time.time_ns()}.db")
            s0 = run(binp, db, MUT_SETUP, timeout)
            runs.append(run(binp, db, PRE + q + "\n", timeout) if s0["rc"] == 0 and not s0["timeout"]
                        else dict(s0, rc=1, err="setup: " + s0["err"]))
        res[name] = summarize(runs)
    res["autocommit_5k_inserts"] = summarize(
        [run(binp, os.path.join(work, f"{tag}_auto_{time.time_ns()}.db"), autocommit_script(5000), timeout)
         for _ in range(reps)])
    res["txn_1000x10_rows"] = summarize(
        [run(binp, os.path.join(work, f"{tag}_txn_{time.time_ns()}.db"), txn_script(), timeout)
         for _ in range(reps)])
    res["memory_script_200k"] = summarize([run(binp, ":memory:", MEM_SCRIPT, timeout) for _ in range(reps)])
    res["multiproc_8x500"] = summarize([multiproc(binp, work, tag, timeout) for _ in range(reps)])
    res["_provenance"]["loadavg_end"] = os.getloadavg()[0]
    return res


def markdown(results):
    labels = list(results)
    base = labels[0]
    workloads = [k for k in results[base] if not k.startswith("_")]
    head = "| workload | " + " | ".join(f"{l} s (x{base})" for l in labels) + " |"
    lines = [head, "|" + "---|" * (len(labels) + 1)]
    for w in workloads:
        b = results[base].get(w, {})
        cells = []
        for l in labels:
            r = results[l].get(w)
            if not r or r.get("median_s") is None:
                cells.append("failed" if r else "-")
                continue
            ratio = (f" (x{r['median_s'] / b['median_s']:.2f})"
                     if b.get("median_s") else "")
            wrong = " WRONG ANSWER" if b.get("answer") and r["answer"] != b["answer"] else ""
            cells.append(f"{r['median_s']:.3f}{ratio}{wrong}")
        lines.append(f"| {w} | " + " | ".join(cells) + " |")
    return "\n".join(lines) + "\n"


def main():
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--work", required=True, help="scratch dir for databases (tmpfs recommended)")
    ap.add_argument("--out", required=True, help="results JSON (resumable)")
    ap.add_argument("--md", help="also write a markdown table here")
    ap.add_argument("--reps", type=int, default=2)
    ap.add_argument("--timeout", type=float, default=150.0, help="seconds per run")
    ap.add_argument("arms", nargs="+", help="label=path/to/shell; first arm is the baseline")
    args = ap.parse_args()
    os.makedirs(args.work, exist_ok=True)
    results = {}
    if os.path.exists(args.out):
        with open(args.out, encoding="utf-8") as fh:
            results = json.load(fh)
    results.setdefault("_host", {"node": platform.node(), "cpus": os.cpu_count(),
                                 "kernel": platform.release(), "started": time.time()})
    host = results.pop("_host")
    for arm in args.arms:
        label, binp = arm.split("=", 1)
        if label in results:
            continue
        results[label] = measure(label, binp, args.work, args.reps, args.timeout)
        with open(args.out, "w", encoding="utf-8") as fh:
            json.dump(dict(results, _host=host), fh, indent=1)
        print(label, "done", flush=True)
    if args.md:
        with open(args.md, "w", encoding="utf-8") as fh:
            fh.write(markdown(results))


if __name__ == "__main__":
    main()
