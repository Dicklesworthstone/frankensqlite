//! Network exposition for the metrics registry — the socket-I/O follow-on that
//! [`crate::metrics`] deliberately excludes (bd-zywqc.11 AC#2: Prometheus HTTP
//! `/metrics`).
//!
//! [`crate::metrics::render_prometheus`] produces the exposition *string*; this
//! module serves it over HTTP/1.1 from a dedicated daemon thread using only
//! `std::net` (the workspace bans `tokio`/`hyper`/`axum`). Opt-in: nothing binds
//! a socket unless the caller invokes [`start_metrics_http`] or sets
//! `FRANKENSQLITE_METRICS_BIND` (honored by [`autostart_from_env`]). When the
//! subsystem is disabled via `FRANKENSQLITE_METRICS_DISABLE=1`,
//! [`autostart_from_env`] is a no-op.
//!
//! Privacy (AC#3) is a property of the exposition *content*, which
//! `render_prometheus` already guarantees (fixed structural labels only); this
//! module adds no labels of its own and never echoes request content.

use std::io::{Read as _, Write as _};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::sync::atomic::{AtomicBool, Ordering};

use crate::metrics::{metrics_disabled, render_prometheus};

/// The default bind address used by `PRAGMA enable_metrics_http=1` (and any
/// value-less enable) when the environment does not override it — bd-zywqc.11.1
/// AC#2: default bind `localhost:9009`.
pub const DEFAULT_METRICS_BIND: &str = "127.0.0.1:9009";

/// Set once the process-wide metrics endpoint has bound a socket, so repeated
/// autostart / `PRAGMA enable_metrics_http` triggers never bind a second
/// listener. A single endpoint per process, whichever trigger fires first.
static ENDPOINT_STARTED: AtomicBool = AtomicBool::new(false);

/// Whether the process-wide metrics HTTP endpoint is currently bound. Used by
/// the `PRAGMA enable_metrics_http` query (no-value) readback.
#[must_use]
pub fn metrics_http_bound() -> bool {
    ENDPOINT_STARTED.load(Ordering::Acquire)
}

/// Resolve the bind address for a value-less enable: `FRANKENSQLITE_METRICS_BIND`
/// when set and non-empty, else [`DEFAULT_METRICS_BIND`].
#[must_use]
pub fn default_bind() -> String {
    match std::env::var("FRANKENSQLITE_METRICS_BIND") {
        Ok(v) if !v.is_empty() => v,
        _ => DEFAULT_METRICS_BIND.to_owned(),
    }
}

/// Start the metrics HTTP endpoint at most once per process, on `bind`.
///
/// Returns `Ok(Some(addr))` if this call bound the endpoint, `Ok(None)` if it was
/// already started (or metrics are disabled), and `Err` only when this call
/// attempted the bind and it failed — in which case the started-flag is cleared
/// so a later call with a working address can retry. Idempotent and race-safe:
/// concurrent callers race the CAS, and exactly one performs the bind.
///
/// # Errors
/// Propagates the `bind` / `local_addr` / thread-spawn I/O error from
/// [`start_metrics_http`] when this call is the one that attempts the bind.
pub fn ensure_metrics_http(bind: &str) -> std::io::Result<Option<SocketAddr>> {
    if metrics_disabled() {
        return Ok(None);
    }
    // Only the first caller to flip false -> true performs the bind; everyone
    // else observes the endpoint is already up and returns `Ok(None)`.
    if ENDPOINT_STARTED
        .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
        .is_err()
    {
        return Ok(None);
    }
    match start_metrics_http(bind) {
        Ok(addr) => Ok(Some(addr)),
        Err(e) => {
            // Release the claim so a later call (e.g. a PRAGMA supplying a valid
            // address after an env bind failed) can retry the bind.
            ENDPOINT_STARTED.store(false, Ordering::Release);
            Err(e)
        }
    }
}

/// Serve `GET /metrics` on `bind` from a named daemon thread.
///
/// Renders the Prometheus exposition (`text/plain; version=0.0.4`) and returns
/// the actually-bound local address (useful with an ephemeral `:0` port). The
/// thread runs until the process exits — there is no shutdown handle, matching a
/// standard Prometheus scrape endpoint.
///
/// # Errors
/// Propagates the `bind` / `local_addr` / thread-spawn I/O error.
pub fn start_metrics_http(bind: &str) -> std::io::Result<SocketAddr> {
    let listener = TcpListener::bind(bind)?;
    let addr = listener.local_addr()?;
    std::thread::Builder::new()
        .name("fsqlite-metrics-http".to_owned())
        .spawn(move || {
            for stream in listener.incoming().flatten() {
                // A single scrape failing must never stop the endpoint.
                let _ = serve_one(&stream);
            }
        })?;
    Ok(addr)
}

/// Start the HTTP endpoint from `FRANKENSQLITE_METRICS_BIND` when it is set.
///
/// No-op when the variable is unset or the subsystem is disabled. Idempotent —
/// safe to call on every connection open; only the first call binds. A bind
/// failure is swallowed (a metrics endpoint must never take down the engine).
pub fn autostart_from_env() {
    if let Ok(bind) = std::env::var("FRANKENSQLITE_METRICS_BIND")
        && !bind.is_empty()
    {
        // Idempotent via `ensure_metrics_http`'s process-wide guard; a bind
        // failure is swallowed (a metrics endpoint must never take down the
        // engine).
        let _ = ensure_metrics_http(&bind);
    }
}

/// Serve exactly one request: parse the request line, answer `GET /metrics` with
/// the Prometheus exposition, `404` any other path, `405` any other method.
fn serve_one(mut stream: &TcpStream) -> std::io::Result<()> {
    // Bound the read so a slow/stuck client can never wedge the (serial) accept
    // loop, and cap total request size to avoid unbounded buffering.
    let _ = stream.set_read_timeout(Some(std::time::Duration::from_secs(5)));
    // bd-7lhxi: the accept loop is serial, so a client that sends a request but
    // never reads the response (zero receive window) would otherwise block
    // write_all forever and wedge the endpoint for the process lifetime.
    let _ = stream.set_write_timeout(Some(std::time::Duration::from_secs(5)));
    let mut head: Vec<u8> = Vec::with_capacity(1024);
    let mut buf = [0_u8; 1024];
    // Read the whole request head (through the blank line) before answering:
    // a `write!`-style client emits its request in several segments, so a single
    // read can see a routable request line while the rest is still in flight.
    // Closing then sends RST and trips the client's own write/read — so drain
    // first. Bounded to 8 KiB; EOF or timeout also ends the loop.
    loop {
        let n = stream.read(&mut buf)?;
        if n == 0 {
            break;
        }
        head.extend_from_slice(&buf[..n]);
        if head.len() >= 8192 || head.windows(4).any(|w| w == b"\r\n\r\n") {
            break;
        }
    }
    let request = String::from_utf8_lossy(&head);
    let request_line = request.lines().next().unwrap_or_default();
    let (status, body) = route(request_line);
    let response = format!(
        "HTTP/1.1 {status}\r\n\
         Content-Type: text/plain; version=0.0.4; charset=utf-8\r\n\
         Content-Length: {len}\r\n\
         Connection: close\r\n\r\n{body}",
        len = body.len(),
    );
    stream.write_all(response.as_bytes())?;
    stream.flush()
}

/// Map a request line (`"GET /metrics HTTP/1.1"`) to `(status, body)`.
fn route(request_line: &str) -> (&'static str, String) {
    let mut parts = request_line.split_whitespace();
    let method = parts.next().unwrap_or_default();
    let path = parts.next().unwrap_or_default();
    match (method, path) {
        ("GET", "/metrics") => ("200 OK", render_prometheus()),
        ("GET", _) => ("404 Not Found", "not found\n".to_owned()),
        _ => ("405 Method Not Allowed", "method not allowed\n".to_owned()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn http_get(addr: SocketAddr, path: &str) -> String {
        let mut stream = TcpStream::connect(addr).expect("connect to metrics endpoint");
        // Write the request in one shot, then close the write half so the server
        // observes a complete request with no in-flight bytes at its close.
        let request = format!("GET {path} HTTP/1.1\r\nHost: localhost\r\n\r\n");
        stream.write_all(request.as_bytes()).expect("write request");
        stream
            .shutdown(std::net::Shutdown::Write)
            .expect("shutdown write half");
        let mut response = String::new();
        stream.read_to_string(&mut response).expect("read response");
        response
    }

    #[test]
    fn http_metrics_endpoint_serves_prometheus_exposition() {
        // A non-zero sample so the body is meaningful (render still emits HELP/
        // TYPE lines at zero, so the metric name is present regardless).
        crate::metrics::global().commits_total.inc();
        let addr = start_metrics_http("127.0.0.1:0").expect("bind ephemeral port");

        let ok = http_get(addr, "/metrics");
        assert!(ok.starts_with("HTTP/1.1 200 OK"), "status line: {ok:?}");
        assert!(
            ok.contains("Content-Type: text/plain; version=0.0.4"),
            "prometheus content-type missing: {ok:?}"
        );
        assert!(
            ok.contains("fsqlite_commits_total"),
            "exposition body missing metrics: {ok:?}"
        );

        // Only /metrics is served; other GET paths 404 (no request content echoed).
        let not_found = http_get(addr, "/../etc/passwd");
        assert!(
            not_found.starts_with("HTTP/1.1 404"),
            "expected 404: {not_found:?}"
        );
    }

    #[test]
    fn route_rejects_non_get_methods() {
        assert_eq!(route("POST /metrics HTTP/1.1").0, "405 Method Not Allowed");
        assert_eq!(route("GET /metrics HTTP/1.1").0, "200 OK");
        assert_eq!(route("GET /other HTTP/1.1").0, "404 Not Found");
    }

    #[test]
    fn ensure_metrics_http_binds_at_most_once() {
        // First call binds an ephemeral endpoint; the second (regardless of the
        // address it would use) observes the process-wide endpoint is already up
        // and does not bind a second listener. This is the guarantee that lets
        // both env-autostart and `PRAGMA enable_metrics_http` be called freely.
        let first = ensure_metrics_http("127.0.0.1:0").expect("first ensure ok");
        assert!(first.is_some(), "first ensure should bind the endpoint");

        let addr = first.unwrap();
        let second = ensure_metrics_http("127.0.0.1:0").expect("second ensure ok");
        assert!(
            second.is_none(),
            "second ensure must be a no-op once the endpoint is up"
        );

        // And the endpoint bound by the first call actually serves.
        let ok = http_get(addr, "/metrics");
        assert!(ok.starts_with("HTTP/1.1 200 OK"), "status line: {ok:?}");
    }

    #[test]
    fn default_bind_falls_back_to_localhost_9009() {
        // With the env override unset, the value-less enable uses the documented
        // default. (The test avoids mutating process env; it only asserts the
        // constant wiring, which is what the PRAGMA relies on.)
        assert_eq!(DEFAULT_METRICS_BIND, "127.0.0.1:9009");
    }
}
