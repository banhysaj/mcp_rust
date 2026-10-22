# mcp-async

[![CI](https://github.com/banhysaj/mcp_rust/actions/workflows/ci.yml/badge.svg)](https://github.com/banhysaj/mcp_rust/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)
![Rust 1.74+](https://img.shields.io/badge/rust-1.74%2B-orange.svg)
![Dependencies: serde_json](https://img.shields.io/badge/dependencies-serde__json-success.svg)

*A dependency-light, multithreaded MCP server library for Rust — stdio and Streamable HTTP, no async runtime.*

> [!NOTE]
> ✨ No async runtime and a single dependency (`serde_json`) — just `std::net` and a worker thread pool.

An MCP server library for Rust. Tool calls run on a worker thread pool, a running
request can be cancelled, and the same server can be served over stdio or over
Streamable HTTP with server-sent events.

- 🧵 **Worker pool dispatch.** `set_max_threads` sizes the pool; the default is one
  worker per hardware thread.
- 🛑 **Cooperative cancellation.** Handlers poll `ctx.cancelled()`, driven by
  `notifications/cancelled` on stdio and by a client hang-up on HTTP.
- 🤝 **Inline handshake.** `initialize` always completes before later requests are
  dispatched, and a cancellation is never delayed behind a busy worker.
- 🔒 **Serialised output.** Replies and notifications from different workers cannot
  interleave.
- 🗂️ **Both revision families.** Stateless `2026-07-28` and stateful `2025-11-25`,
  `2025-06-18`, `2025-03-26`, `2024-11-05`, with `resultType`, `_meta.serverInfo`
  and cache hints on stateless replies.
- 💬 **Elicitation.** Form and url modes, capability checks, and a defined second run
  that reads the client's answers.
- 🌊 **Streamable HTTP.** One POST endpoint, `Origin` checks, pluggable authorization,
  header/body agreement, and chunked SSE progress for `tools/call`.
- 🛡️ **Nesting guard.** Messages nested deeper than 128 levels are refused before they
  reach the parser, on both transports.

The only dependency is [`serde_json`](https://crates.io/crates/serde_json). The HTTP
server is built on `std::net`, so there is nothing vendored.

## Installation

```toml
[dependencies]
mcp-async = "1.0"
```

The library is imported as `mcp_async`.

## Quick start: stdio

```rust
use std::sync::Arc;
use mcp_async::{args, PropertyType, Server, Tool, ToolResult};

fn main() -> std::io::Result<()> {
    let mut server = Server::new("my-server", "1.0.0");
    server.set_max_threads(4); // 0 (the default) = one worker per hardware thread

    let crunch = Tool::new("crunch", "A long-running job")
        .add_parameter_simple("n", PropertyType::Integer, "iterations");

    // add_tool_ctx gives the handler a RequestContext (progress, log, cancel).
    server.add_tool_ctx(crunch, |arguments, ctx| {
        let n = args::integer(arguments, "n", 0);
        for i in 0..n {
            if ctx.cancelled() {
                // the client sent notifications/cancelled for this request
                return ToolResult::error("cancelled");
            }
            ctx.progress(i as f64, Some(n as f64), None);
        }
        ToolResult::text("done")
    });

    let server = Arc::new(server); // run() needs the server to outlive its workers
    std::process::exit(server.run_stdio()?)
}
```

A handler registered with `add_tool` gets the arguments only, so it cannot report
progress or observe cancellation.

## Quick start: Streamable HTTP

```rust
use std::sync::Arc;
use mcp_async::{AuthDecision, HttpOptions, HttpRequest, HttpServer, Server};

// ... build the server and register tools ...

let server = Arc::new(server);

let opts = HttpOptions {
    host: "0.0.0.0".to_string(),
    port: 8080,
    authorize: Some(Arc::new(|req: &HttpRequest| {
        if req.header("Authorization") == "Bearer secret" {
            return AuthDecision::allow_as("user-1");
        }
        AuthDecision::deny(
            "Bearer resource_metadata=\"https://example.com/.well-known/oauth-protected-resource\"",
        )
    })),
    ..Default::default()
};

let http = HttpServer::new(server, opts);
std::process::exit(if http.listen() { 0 } else { 1 });
```

### What the HTTP transport does

- **One endpoint, POST only.** `path` defaults to `/mcp`. `GET` and `DELETE` get
  `405` with `Allow: POST`; any other method or path gets `404`.
- **`tools/call` streams.** When the client sends `Accept: text/event-stream`,
  progress and log lines from the context are sent as SSE events before the result.
  Every other reply is plain JSON.
- **A closed stream cancels.** If the client hangs up mid-call, `ctx.cancelled()`
  becomes true.
- **Headers must match the body.** For `2026-07-28` requests, `MCP-Protocol-Version`,
  `Mcp-Method` and, for methods that name something, `Mcp-Name` must agree with the
  request body, or the reply is `400` with `HeaderMismatch` (-32020). `Mcp-Name`
  accepts the `=?base64?` form.
- **Origin is checked.** A request whose `Origin` is not in `allowed_origins` gets
  `403`. An empty list allows localhost origins only. A request with no `Origin` at
  all (server to server) is allowed.
- **Authorization runs first.** The callback sees the request before the body is
  read, and the principal it returns is available to tools as `ctx.principal()`.
- **HTTP status follows the JSON-RPC code** for errors that are the caller's fault:
  -32601 becomes 404, and -32700, -32600, -32020, -32021 and -32022 become 400.

### HTTP limits

- No TLS. Put a reverse proxy in front for HTTPS.
- No server-wide notifications. `send_notification` and `notify_tools_list_changed`
  reach stdio clients only.
- Older clients are served without sessions, but they share one `Server`, so the
  initialized flag and the log level are shared.
- `set_max_threads` does not apply to HTTP; `HttpOptions::threads` sizes the HTTP
  pool instead.

## Implemented methods

`initialize`, `server/discover`, `ping`, `tools/list`, `tools/call`,
`logging/setLevel`, `notifications/initialized` and `notifications/cancelled`.

Anything else with an id is answered with `-32601`; a notification with an unknown
method is ignored.

## Examples

```sh
cargo run --example stdio    # stdio server with a cancellable job, an echo tool and elicitation
cargo run --example http     # HTTP server with bearer auth and SSE progress
```

## Notes

- **Handlers must be thread-safe.** With more than one worker they run concurrently,
  so shared mutable state needs its own synchronisation. The compiler enforces that
  a handler is `Send + Sync`.
- **Cancellation is cooperative.** `ctx.cancelled()` only reports a cancellation if
  the handler polls it; the server never interrupts a running handler.
- **Cancellation needs `add_tool_ctx`.** Plain handlers have no context to poll.
- **`run` owns its threads.** Call it once, from one thread, on an `Arc<Server>`.
  When it returns, every queued request has finished and its reply has been written.
- **A panicking handler is contained.** It becomes an ordinary tool error whose text
  starts with `Tool error:`, rather than losing a worker.
- **Elicitation does not tie up a worker.** `ToolResult::elicit` returns immediately;
  the client's follow-up call can land on any worker. Anything the tool needs on the
  second run belongs in `with_request_state()`, not in a field.
- **Request state is client input.** The client sends `requestState` back and can
  change it, so treat it like any other argument and re-check what matters.

## Development

```sh
cargo test                                    # 105 tests
cargo clippy --all-targets -- -D warnings
cargo fmt --all -- --check
```

The test suite drives the server over an in-memory transport, asserting on the exact
JSON that comes back, and exercises the HTTP transport over real sockets, including
streamed calls and cancellation on hang-up.

## Minimum supported Rust version

1.74.

## License

MIT. See [LICENSE](LICENSE).
