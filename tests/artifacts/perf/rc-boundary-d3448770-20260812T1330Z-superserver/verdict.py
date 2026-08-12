#!/usr/bin/env python3
"""RC boundary verdict: per-cell F_rc/F_ctrl with C-drift null (bd-dqdoe)."""
import json, glob, statistics, os

B = os.path.dirname(os.path.abspath(__file__))

def engine_key(row):
    for k in ("frankensqlite", "fsqlite"):
        if k in row:
            return k
    raise KeyError(f"no F engine key in {list(row)}")

def load_arm(family, side):
    cells = {}  # scenario_id -> {'C': [medians], 'F': [medians]}
    files = sorted(glob.glob(f"{B}/runs/{family}/*.{side}.json"))
    for path in files:
        d = json.load(open(path))
        for sec in d["sections"]:
            for row in sec["rows"]:
                sid = row["scenario_id"]
                c_obj = row.get("csqlite")
                f_obj = row.get(engine_key(row)) if any(
                    k in row for k in ("frankensqlite", "fsqlite")) else None
                if not isinstance(c_obj, dict) or not isinstance(f_obj, dict):
                    continue  # single-engine baseline rows
                cell = cells.setdefault(sid, {"C": [], "F": [], "FC": []})
                cell["C"].append(c_obj["median_ms"])
                cell["F"].append(f_obj["median_ms"])
                if c_obj["median_ms"]:
                    cell["FC"].append(f_obj["median_ms"] / c_obj["median_ms"])
    return len(files), cells

for family in ("read", "conc"):
    n_ctrl, ctrl = load_arm(family, "ctrl")
    n_rc, rc = load_arm(family, "rc")
    shared = sorted(set(ctrl) & set(rc))
    only_ctrl = sorted(set(ctrl) - set(rc))
    only_rc = sorted(set(rc) - set(ctrl))
    print(f"\n=== {family}: {n_ctrl} ctrl + {n_rc} rc invocations, "
          f"{len(shared)} shared cells ===")
    if only_ctrl: print("  ctrl-only cells:", only_ctrl)
    if only_rc: print("  rc-only cells:", only_rc)
    print(f"{'cell':58s} {'n':>5s} {'F/C@ctrl':>9s} {'F/C@rc':>7s} "
          f"{'DiD':>7s} {'C_drift':>8s}")
    for sid in shared:
        fc = statistics.median(ctrl[sid]["F"])
        fr = statistics.median(rc[sid]["F"])
        cc = statistics.median(ctrl[sid]["C"])
        cr = statistics.median(rc[sid]["C"])
        n = f"{len(ctrl[sid]['FC'])}/{len(rc[sid]['FC'])}"
        fc_ctrl = statistics.median(ctrl[sid]["FC"])
        fc_rc = statistics.median(rc[sid]["FC"])
        did = fc_rc / fc_ctrl if fc_ctrl else float("nan")
        cdrift = cr / cc if cc else float("nan")
        unit = " unit-chg" if (cdrift > 1.5 or cdrift < 0.67) else ""
        flag = ""
        if did > 1.05: flag = " REGRESS"
        elif did < 0.95: flag = " improve"
        short = sid.split("__")[-1][:56]
        print(f"{short:58s} {n:>5s} {fc_ctrl:9.3f} {fc_rc:7.3f} "
              f"{did:7.3f} {cdrift:8.3f}{unit}{flag}")
