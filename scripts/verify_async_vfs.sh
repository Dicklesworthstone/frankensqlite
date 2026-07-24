#!/usr/bin/env bash
set -euo pipefail

repo_root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
trace_log=$(mktemp /tmp/frankensqlite-async-vfs-trace.XXXXXX.jsonl)

cd "$repo_root"
if [[ ${FSQLITE_ASYNC_VFS_USE_RCH:-0} == 1 ]]; then
  RCH_REQUIRE_REMOTE=1 RCH_NO_SELF_HEALING=1 \
    /home/ubuntu/.local/bin/rch --no-self-healing exec -- \
    env FSQLITE_ASYNC_VFS_TRACE=1 cargo test -p fsqlite-vfs \
      uring::tests::test_shared_ring_multiplexes_one_hundred_concurrent_reads \
      -j 4 \
      -- --exact --nocapture \
    2>&1 | tee "$trace_log"
else
  FSQLITE_ASYNC_VFS_TRACE=1 cargo test -p fsqlite-vfs \
    uring::tests::test_shared_ring_multiplexes_one_hundred_concurrent_reads \
    -j 4 \
    -- --exact --nocapture \
    2>&1 | tee "$trace_log"
fi

python3 - "$trace_log" <<'PY'
import json
import sys

path = sys.argv[1]
events = []
with open(path, encoding="utf-8") as stream:
    for line_number, line in enumerate(stream, 1):
        try:
            record = json.loads(line)
        except json.JSONDecodeError:
            continue
        fields = record.get("fields", {})
        event = fields.get("event")
        request_id = fields.get("request_id")
        if event in {"read_at_start", "read_at_complete"} and request_id is not None:
            events.append((event, int(request_id), line_number))

starts = {request_id: line for event, request_id, line in events if event == "read_at_start"}
completes = {
    request_id: line for event, request_id, line in events if event == "read_at_complete"
}
if len(starts) != 100:
    raise SystemExit(f"expected 100 read_at_start events, found {len(starts)}")
if set(starts) != set(completes):
    missing = sorted(set(starts) - set(completes))
    extra = sorted(set(completes) - set(starts))
    raise SystemExit(f"start/complete request IDs differ: missing={missing} extra={extra}")
if any(starts[request_id] >= completes[request_id] for request_id in starts):
    raise SystemExit("a read_at_complete event appeared before its read_at_start event")
if max(starts.values()) >= min(completes.values()):
    raise SystemExit("not all 100 reads were enqueued before the first completion")

print(f"verified 100 multiplexed reads; JSON trace retained at {path}")
PY
