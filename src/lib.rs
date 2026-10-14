//! An MCP server library with a worker-pool request model.
//!
//! Incoming JSON-RPC requests are dispatched to a pool of worker threads, so
//! several tool calls can run at the same time instead of blocking each other,
//! and a running request can be cancelled. The handshake and cancellation
//! notifications are handled on the read thread, so `initialize` always completes
//! before later requests are dispatched and a cancellation takes effect
//! immediately rather than waiting behind a busy worker.
//!
//! Two transports are included: newline-delimited JSON over stdin/stdout, and
//! Streamable HTTP with server-sent events for streamed tool calls.
//!
//! # Quick start (stdio)
//!
//! ```
//! use std::sync::Arc;
//! use mcp_async::args;
//! use mcp_async::{PropertyType, Server, Tool, ToolResult};
//!
//! # fn main() -> std::io::Result<()> {
//! let mut server = Server::new("my-server", "1.0.0");
//! server.set_max_threads(4); // 0 (default) = one worker per hardware thread
//!
//! let crunch = Tool::new("crunch", "A long-running job")
//!     .add_parameter_simple("n", PropertyType::Integer, "iterations");
//!
//! server.add_tool_ctx(crunch, |arguments, ctx| {
//!     let n = args::integer(arguments, "n", 0);
//!     for i in 0..n {
//!         if ctx.cancelled() {
//!             return ToolResult::error("cancelled");
//!         }
//!         ctx.progress(i as f64, Some(n as f64), None);
//!     }
//!     ToolResult::text("done")
//! });
//!
//! let server = Arc::new(server);
//! // serve over stdin/stdout:
//! // server.run_stdio()?;
//! # let _ = server;
//! # Ok(())
//! # }
//! ```
//!
//! # Quick start (Streamable HTTP)
//!
//! ```no_run
//! use std::sync::Arc;
//! use mcp_async::{AuthDecision, HttpOptions, HttpRequest, HttpServer, Server};
//!
//! # fn main() -> std::io::Result<()> {
//! let mut server = Server::new("my-server", "1.0.0");
//! // ... add tools ...
//! let server = Arc::new(server);
//!
//! let mut opts = HttpOptions::default();
//! opts.host = "0.0.0.0".into();
//! opts.port = 8080;
//! opts.authorize = Some(Arc::new(|req: &HttpRequest| {
//!     if req.header("Authorization") == "Bearer secret" {
//!         return AuthDecision::allow_as("user-1");
//!     }
//!     AuthDecision::deny(
//!         "Bearer resource_metadata=\"https://example.com/.well-known/oauth-protected-resource\"",
//!     )
//! }));
//!
//! let http = HttpServer::new(server, opts);
//! std::process::exit(if http.listen() { 0 } else { 1 });
//! # }
//! ```
//!
//! # Modules
//!
//! | module | contents |
//! |--------|----------|
//! | [`constants`] | protocol strings and revision negotiation |
//! | [`json`] | JSON readers and writers, nesting guard |
//! | [`types`] | tools, content, results and elicitation |
//! | [`transport`] | transport traits, stdio transport, in-memory transport |
//! | [`server`] | request dispatch, cancellation, notifications |
//! | [`http`] | HTTP/1.1 transport with server-sent events |

#![forbid(unsafe_code)]
#![warn(missing_docs)]

pub mod constants;
pub mod http;
pub mod json;
mod pool;
pub mod server;
pub mod transport;
pub mod types;

pub use constants::*;
pub use json::{args, arr, obj, parse_into, serialize, within_depth, MAX_DEPTH};
pub use server::{RequestContext, RequestScope, Server, ToolHandler, ToolHandlerCtx};
pub use transport::{LineWrite, MemoryTransport, StdioTransport, Transport};
pub use types::{
    random_elicitation_id, Content, Elicitation, ErrorCode, PropertyType, Resource,
    ResourceContent, Tool, ToolParameter, ToolResult,
};

pub use http::{AuthDecision, Authorizer, HttpOptions, HttpRequest, HttpServer};
