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
use std::sync::Once;

use crate::metrics::{metrics_disabled, render_prometheus};

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
    static ONCE: Once = Once::new();
    ONCE.call_once(|| {
        if metrics_disabled() {
            return;
        }
        if let Ok(bind) = std::env::var("FRANKENSQLITE_METRICS_BIND")
            && !bind.is_empty()
        {
            let _ = start_metrics_http(&bind);
        }
    });
}

/// Serve exactly one request: parse the request line, answer `GET /metrics` with
/// the Prometheus exposition, `404` any other path, `405` any other method.
fn serve_one(mut stream: &TcpStream) -> std::io::Result<()> {
    // Bound the read so a slow/stuck client can never wedge the (serial) accept
    // loop, and cap total request size to avoid unbounded buffering.
    let _ = stream.set_read_timeout(Some(std::time::Duration::from_secs(5)));
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
}
