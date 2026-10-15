//! A stdio server with a cancellable long-running tool, a plain echo tool and a
//! tool that asks the client for input.
//!
//! Speak JSON-RPC to it over stdin/stdout, one message per line:
//!
//! ```text
//! cargo run --example stdio
//! {"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-11-25"}}
//! {"jsonrpc":"2.0","method":"notifications/initialized"}
//! {"jsonrpc":"2.0","id":2,"method":"tools/list"}
//! {"jsonrpc":"2.0","id":3,"method":"tools/call","params":{"name":"crunch","arguments":{"n":3}}}
//! ```

use std::sync::Arc;

use mcp_async::{args, Elicitation, PropertyType, Server, Tool, ToolResult};

fn main() -> std::io::Result<()> {
    let mut server = Server::new("my-server", "1.0.0");
    server.set_max_threads(4); // 0 (the default) = one worker per hardware thread
    server
        .set_instructions("Call `crunch` for long jobs; it reports progress and can be cancelled.");

    // A long-running tool that reports progress and polls for cancellation.
    let crunch = Tool::new("crunch", "A long-running job").add_parameter_simple(
        "n",
        PropertyType::Integer,
        "iterations",
    );

    server.add_tool_ctx(crunch, |arguments, ctx| {
        let n = args::integer(arguments, "n", 0);
        for i in 0..n {
            if ctx.cancelled() {
                // The client sent notifications/cancelled for this request.
                return ToolResult::error("cancelled");
            }
            ctx.progress(i as f64, Some(n as f64), None);
            ctx.log("debug", &format!("iteration {i}"));
        }
        ToolResult::text(format!("finished {n} iterations"))
    });

    // A plain handler: fast, and it cannot be cancelled because it has no context.
    server.add_tool(
        Tool::new("echo", "Echo text").add_parameter_simple("text", PropertyType::String, "text"),
        |a| ToolResult::text(args::str(a, "text", "")),
    );

    // Elicitation: ask the user something, then run again with the answer.
    server.add_tool_ctx(
        Tool::new("ask", "Ask how many iterations to run"),
        |_a, ctx| {
            if !ctx.has_answer("how_many") {
                return ToolResult::elicit(
                    Elicitation::make_form("how_many", "How many iterations?").add_field(
                        "n",
                        PropertyType::Integer,
                        "iterations",
                    ),
                )
                .with_request_state("from-ask");
            }
            if !ctx.accepted("how_many") {
                return ToolResult::error(format!("declined ({})", ctx.answer_action("how_many")));
            }
            let n = ctx
                .answer_content("how_many")
                .and_then(|content| content.get("n"))
                .and_then(|value| value.as_i64())
                .unwrap_or(0);
            ToolResult::text(format!("n={n} (state={})", ctx.request_state()))
        },
    );

    let server = Arc::new(server);
    std::process::exit(server.run_stdio()?);
}
