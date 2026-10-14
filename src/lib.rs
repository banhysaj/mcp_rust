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
