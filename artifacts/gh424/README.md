# GH#424 startup regression: reproducer and unapplied candidate

**Status: tests and a candidate patch only. The production fix is NOT applied.**
Neither Rust compilation nor native timing tests ran in the editing session.
The CLI and core startup tests are expected to expose the reported defect; no
red/green result is claimed. These are ordinary Cargo integration targets, not
verified GitHub Actions gates.

The candidate removes `RuntimeContext::io_native_cx`'s synchronous
`recv_timeout(Duration::from_secs(5))` after spawning a context-minting task.
On Asupersync 0.5.0's caller-driven current-thread runtime, that wait prevents
the same executor from polling the task. The proposed replacement obtains a
request context directly from the retained owning runtime handle. It leaves
the context cache, ownership, attachment guards, blocking pool and awaited
connection shutdown in place; it does not substitute an ambient task context
or merely shorten the timeout.

Source trail: the bootstrap was introduced in FrankenSQLite commit
`dc56e76f1c8727c382f8859acc602f1bab6f255b`; the direct handle API was added in
Asupersync commit `04a4914afff4b131bba82760e17d1fbd4dcc53c3` and was checked in
its v0.5.0 tag. FrankenSQLite v0.3.18 pins Asupersync 0.4.10, while the inspected
main lockfile pins 0.5.0. This is a source-derived explanation, not a bisect or
an independently measured Windows result.

The core file at inspected main `a914a346e9e02c852f6e548fe08db7ab3a30c97f` has
blob `f3db10f5c4219cae76a280d2b5974eb42336bc90`. The connector returned empty
contents for range reads and rejected the full blob as too large. Container
network downloads also failed. Therefore the bootstrap hunk is recovered from
history and checked against a function fixture, NOT applied or checked against
the complete current source. Its line offsets are fixture-local.

On a full checkout, run the unchanged tests before applying the candidate,
review the exact current helper, then check and apply the hunk:

```sh
cargo test -p fsqlite-cli --test gh424_startup -- --test-threads=1
cargo test -p fsqlite-core --test gh424_startup -- --test-threads=1
git apply --check artifacts/gh424/native-context-bootstrap.patch
git apply artifacts/gh424/native-context-bootstrap.patch
```

Then repeat both tests on Windows and a native Unix host, run
`test_shell_runtime_generates_wal_fec_before_exit` and `bd_2lt76_1_bridge`, and
run the repository's formatting, check and Clippy gates through its required
RCH execution path. Refresh the old MINT-AND-EXIT documentation only after the
current source has been inspected and the implementation is changed. The
process tests measure spawn through exit with a two-second coarse stall budget;
they do not assert a machine-independent 10 ms startup guarantee.

In this session `rch exec -- cargo test ...`, `cargo fmt --check`,
`cargo check --workspace --all-targets`, and
`cargo clippy --workspace --all-targets -- -D warnings` all failed to start
(command not found, exit 127). Do not treat these attempts as passing gates.
