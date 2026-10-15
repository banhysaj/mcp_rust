//! A Streamable HTTP server with bearer-token authorization and a tool that streams
//! progress over server-sent events.
//!
//! ```text
//! cargo run --example http
//! curl -s http://127.0.0.1:8080/mcp \
//!   -H 'Content-Type: application/json' \
//!   -H 'Authorization: Bearer secret' \
//!   -d '{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-11-25"}}'
//! ```
//!
//! Stateless (2026-07-28) requests must also repeat their revision and method in
//! headers:
//!
//! ```text
//! curl -sN http://127.0.0.1:8080/mcp \
//!   -H 'Content-Type: application/json' \
//!   -H 'Accept: text/event-stream' \
//!   -H 'MCP-Protocol-Version: 2026-07-28' \
//!   -H 'Mcp-Method: tools/call' \
//!   -H 'Mcp-Name: crunch' \
//!   -d '{"jsonrpc":"2.0","id":2,"method":"tools/call","params":{"name":"crunch","arguments":{"n":5},"_meta":{"io.modelcontextprotocol/protocolVersion":"2026-07-28","progressToken":"t1"}}}'
//! ```

use std::sync::Arc;

use mcp_async::{
    args, AuthDecision, HttpOptions, HttpRequest, HttpServer, PropertyType, Server, Tool,
    ToolResult,
};

fn main() -> std::io::Result<()> {
    let mut server = Server::new("my-server", "1.0.0");
    server.set_instructions("Tool calls can stream progress over SSE.");

    let crunch = Tool::new("crunch", "A long-running job").add_parameter_simple(
        "n",
        PropertyType::Integer,
        "iterations",
    );
    server.add_tool_ctx(crunch, |arguments, ctx| {
        let n = args::integer(arguments, "n", 0);
        for i in 0..n {
            if ctx.cancelled() {
                // The client hung up mid-call.
                return ToolResult::error("cancelled");
            }
            ctx.progress(i as f64, Some(n as f64), Some("working"));
            std::thread::sleep(std::time::Duration::from_millis(200));
        }
        ToolResult::text(format!("finished {n} iterations for {}", ctx.principal()))
    });

    let server = Arc::new(server);

    let opts = HttpOptions {
        host: "0.0.0.0".to_string(),
        port: 8080,
        allowed_origins: vec!["https://example.com".to_string()],
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

    let (host, port, path) = (opts.host.clone(), opts.port, opts.path.clone());
    let http = HttpServer::new(server, opts);
    eprintln!("listening on http://{host}:{port}{path}");

    if http.listen() {
        Ok(())
    } else {
        eprintln!("could not bind {host}:{port}");
        std::process::exit(1);
    }
}
