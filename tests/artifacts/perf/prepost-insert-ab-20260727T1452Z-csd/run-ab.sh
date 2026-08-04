#!/bin/bash
# bd-dqdoe diagnostic A/B: pre-fix (648460d3) vs post-fix (a20b3144) comprehensive-bench,
# --quick --filter insert, interleaved ABBA-AB, pinned. Diagnostic-grade (loaded host),
# NOT citable; each binary's own C SQLite arm is the internal control.
set -euo pipefail
S=/data/tmp/claude-1000/-data-projects-frankensqlite/17278c52-0acb-4726-8860-bcc0100e6531/scratchpad
R=$S/ab-results
mkdir -p $R
PRE=$S/pre-648460d3-comprehensive-bench
POST=$S/post-a20b3144-comprehensive-bench
order=(pre post post pre pre post)
for i in "${!order[@]}"; do
  arm=${order[$i]}
  bin=$PRE; [ "$arm" = post ] && bin=$POST
  echo "=== run $i arm=$arm $(date -u +%H:%M:%S) loadavg=$(cut -d' ' -f1-3 /proc/loadavg)"
  taskset -c 48-63 "$bin" --quick --filter insert --no-html > "$R/run$i-$arm.log" 2>&1 || echo "run $i FAILED rc=$?"
  grep -E "average time ratio|FrankenSQLite faster|C SQLite faster|Comparable|Total scenarios" "$R/run$i-$arm.log" | tail -4
done
echo "=== SUMMARY (average time ratio per run) ==="
for f in $R/run*-pre.log;  do printf "pre  %s : " "$(basename $f)"; grep -oE "average time ratio [0-9.]+x" "$f" | tail -1; done
for f in $R/run*-post.log; do printf "post %s : " "$(basename $f)"; grep -oE "average time ratio [0-9.]+x" "$f" | tail -1; done
