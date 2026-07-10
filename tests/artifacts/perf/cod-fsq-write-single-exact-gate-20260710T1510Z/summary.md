# Exact prepared-DELETE profile matrix substrate rejection

- Worker: `vmi1152480`; target CPU 2; perf collector CPUs `0-1,3-9`.
- Source SHA-256: `814c2458007cca3ecf32c2c2f9ece283484580012fc242dbbc473a1853c42b23`.
- Binary SHA-256: `7c0050ae105b751e500112a82cb832b29fc8ec6ea208a561f6a5e6cd564314af`.
- Attempted first arm: FSQLite, 1280 source rows, 64 sparse prepared
  DELETEs, 40,000 measured transactions.
- Exact boundary: acknowledged perf enable around only
  `BEGIN -> prepared DELETE x64 -> COMMIT`; fixture population, three warmups,
  and restore INSERTs were disabled.
- Result: timeout status 124 after 900 seconds on a contended worker. The
  target exited, but perf then spent more than 17 minutes in its build-ID cache
  pass spawning `addr2line`. The collector was stopped; the 348,653,656-byte
  raw file therefore retained `data size = 0` and is intentionally invalid for
  attribution.
- Raw data remains remote. Its SHA-256 is
  `f1870d97a9d4886e3d586f2a9e5a3e64c548e253bdc2d4e80128f957395fe5ae`.
- Retry with `--no-buildid --no-buildid-cache`, a kill-after guard, balanced
  FIFO descriptors, and counts `5k/10k`, `1.25k/2.5k`, `320/640` for
  FSQLite/C SQLite at 64/256/1024 deletes.
