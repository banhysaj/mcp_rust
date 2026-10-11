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
pub mod json;
pub mod types;
