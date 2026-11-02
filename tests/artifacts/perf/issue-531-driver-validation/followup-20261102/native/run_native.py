import datetime
import hashlib
import json
import os
from pathlib import Path
import resource
import subprocess
import sys
import time

label, workdir, *command = sys.argv[1:]
out = Path(__file__).parent
started = datetime.datetime.now(datetime.timezone.utc).isoformat()
begin = time.monotonic()
lock = Path(workdir) / 'Cargo.lock'
env_keys = ['CARGO_HOME', 'CARGO_TARGET_DIR', 'CARGO_BUILD_JOBS',
            'CARGO_INCREMENTAL', 'CARGO_PROFILE_DEV_DEBUG',
            'CARGO_PROFILE_TEST_DEBUG', 'RUSTFLAGS', 'RUST_MIN_STACK']
metadata = {
    'command': command, 'cwd': workdir, 'started_utc': started,
    'environment': {key: os.environ.get(key) for key in env_keys},
    'lock_sha256': hashlib.sha256(lock.read_bytes()).hexdigest() if lock.exists() else None,
    'cgroup_memory_limit': Path('/sys/fs/cgroup/memory.max').read_text().strip(),
}
with (out / (label + '.log')).open('w') as log:
    proc = subprocess.Popen(command, cwd=workdir, stdout=log, stderr=subprocess.STDOUT)
    print(json.dumps({'label': label, 'pid': proc.pid, 'started_utc': started}), flush=True)
    metadata['exit_code'] = proc.wait()
metadata['elapsed_seconds'] = time.monotonic() - begin
metadata['child_max_rss_kib'] = resource.getrusage(resource.RUSAGE_CHILDREN).ru_maxrss
metadata['cgroup_memory_events'] = Path('/sys/fs/cgroup/memory.events').read_text()
(out / (label + '.json')).write_text(json.dumps(metadata, indent=2) + '\n')
print(json.dumps(metadata), flush=True)
sys.exit(metadata['exit_code'])
