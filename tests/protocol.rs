//! End-to-end protocol tests over an in-memory transport.
//!
//! These drive [`mcp_async::Server`] the way a stdio client would and assert on the
//! exact bytes that come back, which pins the wire format as well as the behaviour.

use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use mcp_async::{
    args, Elicitation, MemoryTransport, PropertyType, Server, Tool, ToolParameter, ToolResult,
};
use serde_json::{json, Value};

const V: &str = "2026-07-28";
const META_VERSION: &str = "io.modelcontextprotocol/protocolVersion";

// ---------------------------------------------------------------------------
// Harness
// ---------------------------------------------------------------------------

/// Run every line through a single-worker server and return the output lines.
///
/// One worker makes the output order deterministic, which is what lets these tests
/// assert on exact strings.
fn serve(server: Server, input: &[String]) -> Vec<String> {
    let server = Arc::new(server);
    let transport = MemoryTransport::new(input.iter().cloned());
    let captured = transport.clone();
    server.run(transport).expect("run failed");
    captured.output()
}

fn base_server() -> Server {
    let mut server = Server::new("test-server", "9.9.9");
    server.set_max_threads(1);
    server.set_instructions("be nice");
    server.add_tool(
        Tool::new("echo", "Echo text").add_parameter_simple("text", PropertyType::String, "what"),
        |a| ToolResult::text(args::str(a, "text", "")),
    );
    server
}

fn init_line() -> String {
    json!({
        "jsonrpc": "2.0", "id": 1, "method": "initialize",
        "params": {"protocolVersion": "2025-11-25", "capabilities": {}, "clientInfo": {"name":"c","version":"1"}}
    })
    .to_string()
}

fn initialized_line() -> String {
    json!({"jsonrpc": "2.0", "method": "notifications/initialized"}).to_string()
}

/// A stateless call wrapper: every request carries its revision in `_meta`.
fn stateless(id: i64, method: &str, mut params: Value) -> String {
    let obj = params.as_object_mut().expect("params must be an object");
    let meta = obj
        .entry("_meta")
        .or_insert_with(|| json!({}))
        .as_object_mut()
        .expect("_meta must be an object");
    meta.insert(META_VERSION.to_string(), json!(V));
    json!({"jsonrpc": "2.0", "id": id, "method": method, "params": params}).to_string()
}

fn parse(line: &str) -> Value {
    serde_json::from_str(line).unwrap_or_else(|e| panic!("not JSON ({e}): {line}"))
}

fn error_code(line: &str) -> i64 {
    let v = parse(line);
    v["error"]["code"]
        .as_i64()
        .unwrap_or_else(|| panic!("no error.code in {line}"))
}

// ---------------------------------------------------------------------------
// Handshake and discovery
// ---------------------------------------------------------------------------

#[test]
fn initialize_echoes_a_supported_version_and_reports_capabilities() {
    let out = serve(base_server(), &[init_line()]);
    assert_eq!(out.len(), 1);
    assert_eq!(
        out[0],
        r#"{"jsonrpc":"2.0","id":1,"result":{"protocolVersion":"2025-11-25","capabilities":{"tools":{"listChanged":true}},"serverInfo":{"name":"test-server","version":"9.9.9"},"instructions":"be nice"}}"#
    );
}

#[test]
fn initialize_falls_back_to_the_default_stateful_version() {
    let line = json!({
        "jsonrpc": "2.0", "id": 7, "method": "initialize",
        "params": {"protocolVersion": "1999-01-01"}
    })
    .to_string();
    let out = serve(base_server(), &[line]);
    let v = parse(&out[0]);
    assert_eq!(v["result"]["protocolVersion"], "2025-11-25");
}

#[test]
fn stateful_requests_are_refused_before_the_handshake() {
    let out = serve(
        base_server(),
        &[json!({"jsonrpc":"2.0","id":3,"method":"tools/list"}).to_string()],
    );
    assert_eq!(out.len(), 1);
    assert_eq!(
        out[0],
        r#"{"jsonrpc":"2.0","id":3,"error":{"code":-32600,"message":"Server not initialized"}}"#
    );
}

#[test]
fn a_notification_before_the_handshake_is_ignored() {
    let out = serve(
        base_server(),
        &[json!({"jsonrpc":"2.0","method":"tools/list"}).to_string()],
    );
    assert!(out.is_empty(), "{out:?}");
}

#[test]
fn ping_is_an_empty_result() {
    let out = serve(
        base_server(),
        &[
            init_line(),
            initialized_line(),
            json!({"jsonrpc":"2.0","id":2,"method":"ping"}).to_string(),
        ],
    );
    assert_eq!(out.len(), 2);
    assert_eq!(out[1], r#"{"jsonrpc":"2.0","id":2,"result":{}}"#);
}

#[test]
fn server_discover_is_always_stateless() {
    let out = serve(
        base_server(),
        &[json!({"jsonrpc":"2.0","id":1,"method":"server/discover"}).to_string()],
    );
    assert_eq!(
        out[0],
        r#"{"jsonrpc":"2.0","id":1,"result":{"supportedVersions":["2026-07-28","2025-11-25","2025-06-18","2025-03-26","2024-11-05"],"capabilities":{"tools":{"listChanged":true}},"resultType":"complete","_meta":{"io.modelcontextprotocol/serverInfo":{"name":"test-server","version":"9.9.9"}},"ttlMs":0,"cacheScope":"private"}}"#
    );
}

#[test]
fn cache_hints_appear_only_on_cacheable_stateless_replies() {
    let mut server = base_server();
    server.set_cache_hint(30_000, "public");
    let out = serve(
        server,
        &[
            stateless(1, "tools/list", json!({})),
            stateless(
                2,
                "tools/call",
                json!({"name":"echo","arguments":{"text":"hi"}}),
            ),
        ],
    );
    let list = parse(&out[0]);
    assert_eq!(list["result"]["ttlMs"], 30_000);
    assert_eq!(list["result"]["cacheScope"], "public");
    let call = parse(&out[1]);
    assert!(call["result"].get("ttlMs").is_none(), "{}", out[1]);
    assert!(call["result"].get("cacheScope").is_none(), "{}", out[1]);
}

#[test]
fn stateless_requests_need_no_handshake() {
    let out = serve(base_server(), &[stateless(1, "tools/list", json!({}))]);
    let v = parse(&out[0]);
    assert_eq!(v["result"]["resultType"], "complete");
    assert_eq!(
        v["result"]["_meta"]["io.modelcontextprotocol/serverInfo"]["name"],
        "test-server"
    );
    assert_eq!(v["result"]["tools"][0]["name"], "echo");
}

#[test]
fn an_unknown_per_call_version_is_rejected() {
    let line = json!({
        "jsonrpc": "2.0", "id": 4, "method": "tools/list",
        "params": {"_meta": {META_VERSION: "2020-01-01"}}
    })
    .to_string();
    let out = serve(base_server(), &[line]);
    assert_eq!(error_code(&out[0]), -32022);
    let v = parse(&out[0]);
    assert_eq!(v["error"]["message"], "Unsupported protocol version");
    assert_eq!(v["error"]["data"]["requested"], "2020-01-01");
    assert_eq!(v["error"]["data"]["supported"][0], V);
    assert_eq!(v["error"]["data"]["supported"].as_array().unwrap().len(), 5);
}

// ---------------------------------------------------------------------------
// tools/list and tools/call
// ---------------------------------------------------------------------------

#[test]
fn tools_list_reports_generated_schemas() {
    let mut server = base_server();
    server.add_tool(
        Tool::new("typed", "Many args")
            .add_parameter_simple("n", PropertyType::Integer, "iters")
            .add_parameter(
                ToolParameter::new("tags", PropertyType::Array, "").set_item_type("string"),
            )
            .add_enum_param("mode", ["fast", "slow"], "pick one")
            .add_parameter(ToolParameter::new("opt", PropertyType::Boolean, "flag").optional())
            .set_output_schema(r#"{"type":"object"}"#),
        |_| ToolResult::text("ok"),
    );
    let out = serve(server, &[stateless(1, "tools/list", json!({}))]);
    let tools = parse(&out[0])["result"]["tools"].clone();
    let typed = tools
        .as_array()
        .unwrap()
        .iter()
        .find(|t| t["name"] == "typed")
        .unwrap()
        .clone();
    assert_eq!(typed["description"], "Many args");
    let schema = &typed["inputSchema"];
    assert_eq!(schema["type"], "object");
    assert_eq!(schema["properties"]["n"]["type"], "integer");
    assert_eq!(schema["properties"]["tags"]["items"]["type"], "string");
    assert_eq!(
        schema["properties"]["mode"]["enum"],
        json!(["fast", "slow"])
    );
    assert_eq!(schema["required"], json!(["n", "tags", "mode"]));
    assert_eq!(typed["outputSchema"], json!({"type": "object"}));
}

#[test]
fn custom_input_schema_overrides_the_generated_one() {
    let mut server = base_server();
    server.add_tool(
        Tool::new("custom", "d")
            .add_parameter_simple("ignored", PropertyType::String, "x")
            .set_input_schema(r#"{"type":"object","properties":{"only":{"type":"string"}}}"#),
        |_| ToolResult::text("ok"),
    );
    let out = serve(server, &[stateless(1, "tools/list", json!({}))]);
    let tools = parse(&out[0])["result"]["tools"].clone();
    let custom = tools
        .as_array()
        .unwrap()
        .iter()
        .find(|t| t["name"] == "custom")
        .unwrap()
        .clone();
    assert!(custom["inputSchema"]["properties"].get("ignored").is_none());
    assert_eq!(
        custom["inputSchema"]["properties"]["only"]["type"],
        "string"
    );
}

#[test]
fn tools_call_returns_content() {
    let out = serve(
        base_server(),
        &[stateless(
            2,
            "tools/call",
            json!({"name":"echo","arguments":{"text":"hi"}}),
        )],
    );
    assert_eq!(
        out[0],
        r#"{"jsonrpc":"2.0","id":2,"result":{"content":[{"type":"text","text":"hi"}],"isError":false,"resultType":"complete","_meta":{"io.modelcontextprotocol/serverInfo":{"name":"test-server","version":"9.9.9"}}}}"#
    );
}

#[test]
fn structured_content_is_emitted_when_it_parses() {
    let mut server = base_server();
    server.add_tool(Tool::new("structured", "d"), |_| {
        ToolResult::text("see below").with_structured(r#"{"a":1}"#)
    });
    server.add_tool(Tool::new("badjson", "d"), |_| {
        ToolResult::text("see below").with_structured("{oops")
    });
    let out = serve(
        server,
        &[
            stateless(1, "tools/call", json!({"name":"structured"})),
            stateless(2, "tools/call", json!({"name":"badjson"})),
        ],
    );
    assert_eq!(
        parse(&out[0])["result"]["structuredContent"],
        json!({"a": 1})
    );
    assert!(parse(&out[1])["result"].get("structuredContent").is_none());
}

#[test]
fn tool_error_results_are_not_jsonrpc_errors() {
    let mut server = base_server();
    server.add_tool(Tool::new("bad", "d"), |_| ToolResult::error("nope"));
    let out = serve(server, &[stateless(1, "tools/call", json!({"name":"bad"}))]);
    let v = parse(&out[0]);
    assert!(v.get("error").is_none());
    assert_eq!(v["result"]["isError"], true);
    assert_eq!(v["result"]["content"][0]["text"], "nope");
}

#[test]
fn unknown_tools_and_bad_arguments_are_invalid_params() {
    let mut server = base_server();
    server.add_tool(
        Tool::new("needs", "d").add_parameter_simple("n", PropertyType::Integer, "iters"),
        |_| ToolResult::text("ok"),
    );
    let out = serve(
        server,
        &[
            stateless(1, "tools/call", json!({"name":"ghost"})),
            stateless(2, "tools/call", json!({"name":""})),
            stateless(3, "tools/call", json!({"name":"needs"})),
            stateless(
                4,
                "tools/call",
                json!({"name":"needs","arguments":{"n":"no"}}),
            ),
        ],
    );
    assert_eq!(error_code(&out[0]), -32602);
    assert_eq!(parse(&out[0])["error"]["message"], "Unknown tool: ghost");
    assert_eq!(parse(&out[1])["error"]["message"], "Missing tool name");
    assert_eq!(
        parse(&out[2])["error"]["message"],
        "Missing required argument: n"
    );
    assert_eq!(
        parse(&out[3])["error"]["message"],
        "Argument 'n' should be of type integer"
    );
}

#[test]
fn a_panicking_handler_becomes_a_tool_error() {
    let mut server = base_server();
    server.add_tool(Tool::new("boom", "d"), |_| panic!("kaboom"));
    // Keep the panic hook quiet for this one test.
    let previous = std::panic::take_hook();
    std::panic::set_hook(Box::new(|_| {}));
    let out = serve(
        server,
        &[stateless(1, "tools/call", json!({"name":"boom"}))],
    );
    std::panic::set_hook(previous);

    let v = parse(&out[0]);
    assert_eq!(v["result"]["isError"], true);
    assert_eq!(v["result"]["content"][0]["text"], "Tool error: kaboom");
}

#[test]
fn boxed_registration_works() {
    let mut server = base_server();
    server.add_tool_boxed(
        Tool::new("empty", "d"),
        Box::new(|_| ToolResult::text("never")),
    );
    // Both registration paths must produce a working tool.
    let out = serve(
        server,
        &[stateless(1, "tools/call", json!({"name":"empty"}))],
    );
    assert_eq!(parse(&out[0])["result"]["content"][0]["text"], "never");
}

// ---------------------------------------------------------------------------
// Envelope handling
// ---------------------------------------------------------------------------

#[test]
fn parse_errors_carry_a_null_id() {
    let out = serve(base_server(), &["{not json".to_string()]);
    assert_eq!(
        out[0],
        r#"{"jsonrpc":"2.0","id":null,"error":{"code":-32700,"message":"Parse error"}}"#
    );
}

#[test]
fn deeply_nested_messages_are_refused() {
    let deep = format!("{}{}", "[".repeat(200), "]".repeat(200));
    let out = serve(base_server(), &[deep]);
    assert_eq!(error_code(&out[0]), -32600);
    assert_eq!(
        parse(&out[0])["error"]["message"],
        "Message is nested too deeply"
    );
}

#[test]
fn non_objects_are_invalid_requests() {
    let out = serve(base_server(), &["5".to_string(), "null".to_string()]);
    assert_eq!(out.len(), 2);
    for line in &out {
        assert_eq!(
            line,
            r#"{"jsonrpc":"2.0","id":null,"error":{"code":-32600,"message":"Invalid Request"}}"#
        );
    }
}

#[test]
fn method_not_found_is_reported_for_requests_only() {
    let out = serve(
        base_server(),
        &[
            init_line(),
            initialized_line(),
            json!({"jsonrpc":"2.0","id":9,"method":"resources/list"}).to_string(),
            json!({"jsonrpc":"2.0","method":"resources/list"}).to_string(),
        ],
    );
    assert_eq!(out.len(), 2, "{out:?}");
    assert_eq!(error_code(&out[1]), -32601);
    assert_eq!(
        parse(&out[1])["error"]["message"],
        "Method not found: resources/list"
    );
}

#[test]
fn batches_return_an_array_of_replies_in_order() {
    let batch = json!([
        {"jsonrpc":"2.0","id":1,"method":"tools/list","params":{"_meta":{META_VERSION:V}}},
        {"jsonrpc":"2.0","method":"notifications/initialized"},
        {"jsonrpc":"2.0","id":2,"method":"ping"}
    ])
    .to_string();
    let out = serve(base_server(), &[batch]);
    assert_eq!(out.len(), 1);
    let v = parse(&out[0]);
    let arr = v.as_array().expect("batch reply must be an array");
    assert_eq!(arr.len(), 2);
    assert_eq!(arr[0]["id"], 1);
    assert_eq!(arr[1]["id"], 2);
    assert_eq!(arr[1]["result"], json!({}));
}

#[test]
fn a_batch_with_no_replies_produces_no_output() {
    let batch = json!([{"jsonrpc":"2.0","method":"notifications/initialized"}]).to_string();
    let out = serve(base_server(), &[batch]);
    assert!(out.is_empty(), "{out:?}");
}

#[test]
fn a_bom_and_surrounding_whitespace_are_tolerated() {
    let line = format!("\u{feff}  {}  ", stateless(1, "tools/list", json!({})));
    let out = serve(base_server(), &[line]);
    assert_eq!(parse(&out[0])["id"], 1);
}

#[test]
fn a_blank_line_produces_nothing() {
    let out = serve(base_server(), &["   ".to_string(), "".to_string()]);
    assert!(out.is_empty(), "{out:?}");
}

// ---------------------------------------------------------------------------
// Progress and logging
// ---------------------------------------------------------------------------

fn crunch_server(observed_cancel: Option<Arc<AtomicBool>>) -> Server {
    let mut server = Server::new("test-server", "9.9.9");
    server.set_max_threads(1);
    server.add_tool_ctx(
        Tool::new("crunch", "A long-running job").add_parameter_simple(
            "n",
            PropertyType::Integer,
            "iterations",
        ),
        move |a, ctx| {
            let n = args::integer(a, "n", 0);
            for i in 0..n {
                if ctx.cancelled() {
                    if let Some(flag) = &observed_cancel {
                        flag.store(true, Ordering::SeqCst);
                    }
                    return ToolResult::error("cancelled");
                }
                ctx.progress(i as f64, Some(n as f64), None);
            }
            ToolResult::text("done")
        },
    );
    server
}

#[test]
fn progress_events_precede_the_result() {
    let out = serve(
        crunch_server(None),
        &[stateless(
            2,
            "tools/call",
            json!({
                "name": "crunch",
                "arguments": {"n": 3},
                "_meta": {META_VERSION: V, "progressToken": "tok-1"}
            }),
        )],
    );
    assert_eq!(out.len(), 4, "{out:?}");
    assert_eq!(
        out[0],
        r#"{"jsonrpc":"2.0","method":"notifications/progress","params":{"progressToken":"tok-1","progress":0.0,"total":3.0}}"#
    );
    assert_eq!(
        out[1],
        r#"{"jsonrpc":"2.0","method":"notifications/progress","params":{"progressToken":"tok-1","progress":1.0,"total":3.0}}"#
    );
    assert_eq!(
        out[2],
        r#"{"jsonrpc":"2.0","method":"notifications/progress","params":{"progressToken":"tok-1","progress":2.0,"total":3.0}}"#
    );
    assert!(out[3].contains(r#""text":"done""#), "{}", out[3]);
}

#[test]
fn progress_without_a_token_is_silent() {
    let out = serve(
        crunch_server(None),
        &[stateless(
            2,
            "tools/call",
            json!({"name":"crunch","arguments":{"n":3}}),
        )],
    );
    assert_eq!(out.len(), 1, "{out:?}");
    assert!(out[0].contains("done"));
}

#[test]
fn progress_message_and_absent_total_are_optional() {
    let mut server = Server::new("s", "1");
    server.set_max_threads(1);
    server.add_tool_ctx(Tool::new("p", "d"), |_a, ctx| {
        ctx.progress(1.5, None, Some("half way"));
        ToolResult::text("ok")
    });
    let out = serve(
        server,
        &[stateless(
            1,
            "tools/call",
            json!({"name":"p","_meta":{"progressToken":7}}),
        )],
    );
    assert_eq!(
        out[0],
        r#"{"jsonrpc":"2.0","method":"notifications/progress","params":{"progressToken":7,"progress":1.5,"message":"half way"}}"#
    );
}

#[test]
fn logging_level_filters_context_logs() {
    let mut server = Server::new("s", "1");
    server.set_max_threads(1);
    server.add_tool_ctx(Tool::new("logger", "d"), |_a, ctx| {
        ctx.log("debug", "dbg");
        ctx.log("error", "err");
        ToolResult::text("ok")
    });

    // No level set: everything passes.
    let out = serve(
        server,
        &[stateless(1, "tools/call", json!({"name":"logger"}))],
    );
    let msgs: Vec<&String> = out
        .iter()
        .filter(|l| l.contains("notifications/message"))
        .collect();
    assert_eq!(msgs.len(), 2, "{out:?}");
    assert_eq!(
        msgs[0],
        r#"{"jsonrpc":"2.0","method":"notifications/message","params":{"level":"debug","data":"dbg"}}"#
    );

    // Now raise the floor to "warning": only "error" survives.
    let mut server = Server::new("s", "1");
    server.set_max_threads(1);
    server.add_tool_ctx(Tool::new("logger", "d"), |_a, ctx| {
        ctx.log("debug", "dbg");
        ctx.log("error", "err");
        ToolResult::text("ok")
    });
    let out = serve(
        server,
        &[
            stateless(1, "logging/setLevel", json!({"level":"warning"})),
            stateless(2, "tools/call", json!({"name":"logger"})),
        ],
    );
    let msgs: Vec<&String> = out
        .iter()
        .filter(|l| l.contains("notifications/message"))
        .collect();
    assert_eq!(msgs.len(), 1, "{out:?}");
    assert!(msgs[0].contains("\"level\":\"error\""), "{}", msgs[0]);
}

#[test]
fn server_wide_notifications_go_out_through_the_transport() {
    let mut server = base_server();
    server.add_tool_ctx(Tool::new("notifier", "d"), |_a, ctx| {
        ctx.notify("notifications/custom", r#"{"x":1}"#);
        ToolResult::text("ok")
    });
    let out = serve(
        server,
        &[stateless(1, "tools/call", json!({"name":"notifier"}))],
    );
    assert_eq!(
        out[0],
        r#"{"jsonrpc":"2.0","method":"notifications/custom","params":{"x":1}}"#
    );
}

// ---------------------------------------------------------------------------
// Cancellation
// ---------------------------------------------------------------------------

#[test]
fn a_cancellation_notification_stops_a_running_tool() {
    let observed = Arc::new(AtomicBool::new(false));
    let out = serve(
        crunch_server(Some(Arc::clone(&observed))),
        &[
            stateless(
                2,
                "tools/call",
                json!({"name":"crunch","arguments":{"n":1_000_000}}),
            ),
            json!({"jsonrpc":"2.0","method":"notifications/cancelled","params":{"requestId":2}})
                .to_string(),
        ],
    );

    assert!(
        observed.load(Ordering::SeqCst),
        "the handler never saw the cancellation"
    );
    let replies: Vec<&String> = out.iter().filter(|l| l.contains("\"id\":2")).collect();
    assert_eq!(replies.len(), 1, "{out:?}");
    assert!(
        replies[0].contains(r#""text":"cancelled""#),
        "{}",
        replies[0]
    );
    assert!(replies[0].contains(r#""isError":true"#));
}

#[test]
fn cancelling_an_unknown_request_is_harmless() {
    let out = serve(
        base_server(),
        &[
            json!({"jsonrpc":"2.0","method":"notifications/cancelled","params":{"requestId":99}})
                .to_string(),
        ],
    );
    assert!(out.is_empty(), "{out:?}");
}

#[test]
fn a_plain_tool_cannot_observe_cancellation_but_still_runs() {
    let out = serve(
        base_server(),
        &[
            stateless(
                5,
                "tools/call",
                json!({"name":"echo","arguments":{"text":"still here"}}),
            ),
            json!({"jsonrpc":"2.0","method":"notifications/cancelled","params":{"requestId":5}})
                .to_string(),
        ],
    );
    assert_eq!(out.len(), 1);
    assert!(out[0].contains("still here"), "{}", out[0]);
}

// ---------------------------------------------------------------------------
// Elicitation
// ---------------------------------------------------------------------------

fn ask_server() -> Server {
    let mut server = Server::new("test-server", "9.9.9");
    server.set_max_threads(1);
    server.add_tool_ctx(Tool::new("ask", "Ask the user"), |_a, ctx| {
        if !ctx.has_answer("how_many") {
            return ToolResult::elicit(Elicitation::make_form("how_many", "How many?").add_field(
                "n",
                PropertyType::Integer,
                "iterations",
            ))
            .with_request_state("state-1");
        }
        if !ctx.accepted("how_many") {
            return ToolResult::error(format!("declined: {}", ctx.answer_action("how_many")));
        }
        let n = ctx
            .answer_content("how_many")
            .and_then(|c| c.get("n"))
            .and_then(|v| v.as_i64())
            .unwrap_or(-1);
        ToolResult::text(format!("n={n} state={}", ctx.request_state()))
    });
    server
}

/// `tools/call` params for a client that advertises form-mode elicitation.
fn eliciting_params(name: &str, extra: Value) -> Value {
    let mut params = json!({
        "name": name,
        "_meta": {
            META_VERSION: V,
            "io.modelcontextprotocol/clientCapabilities": {"elicitation": {"form": {}}}
        }
    });
    if let (Some(p), Value::Object(e)) = (params.as_object_mut(), extra) {
        for (k, v) in e {
            p.insert(k, v);
        }
    }
    params
}

#[test]
fn a_tool_can_ask_for_input_and_run_again_with_the_answer() {
    let first = stateless(2, "tools/call", eliciting_params("ask", json!({})));
    let second = stateless(
        3,
        "tools/call",
        eliciting_params(
            "ask",
            json!({
                "requestState": "state-1",
                "inputResponses": {"how_many": {"action": "accept", "content": {"n": 5}}}
            }),
        ),
    );
    let out = serve(ask_server(), &[first, second]);

    let v = parse(&out[0]);
    assert_eq!(v["result"]["resultType"], "input_required");
    assert_eq!(v["result"]["requestState"], "state-1");
    assert_eq!(
        v["result"]["inputRequests"]["how_many"],
        json!({
            "method": "elicitation/create",
            "params": {
                "mode": "form",
                "message": "How many?",
                "requestedSchema": {
                    "type": "object",
                    "properties": {"n": {"type": "integer", "description": "iterations"}},
                    "required": ["n"]
                }
            }
        })
    );

    let v2 = parse(&out[1]);
    assert_eq!(v2["result"]["content"][0]["text"], "n=5 state=state-1");
}

#[test]
fn a_declined_elicitation_is_reported_to_the_tool() {
    let out = serve(
        ask_server(),
        &[stateless(
            2,
            "tools/call",
            eliciting_params(
                "ask",
                json!({"inputResponses": {"how_many": {"action": "decline"}}}),
            ),
        )],
    );
    let v = parse(&out[0]);
    assert_eq!(v["result"]["isError"], true);
    assert_eq!(v["result"]["content"][0]["text"], "declined: decline");
}

#[test]
fn elicitation_requires_the_latest_protocol_version() {
    let mut server = ask_server();
    server.set_max_threads(1);
    let out = serve(
        server,
        &[
            init_line(),
            initialized_line(),
            json!({
                "jsonrpc": "2.0", "id": 2, "method": "tools/call",
                "params": {"name": "ask"}
            })
            .to_string(),
        ],
    );
    assert_eq!(out.len(), 2, "{out:?}");
    assert_eq!(error_code(&out[1]), -32021);
    assert!(parse(&out[1])["error"]["message"]
        .as_str()
        .unwrap()
        .contains(V));
}

#[test]
fn elicitation_requires_the_client_capability() {
    let out = serve(
        ask_server(),
        &[stateless(2, "tools/call", json!({"name":"ask"}))],
    );
    assert_eq!(error_code(&out[0]), -32021);
    assert_eq!(
        parse(&out[0])["error"]["message"],
        "Client does not support form mode elicitation"
    );
}

#[test]
fn url_mode_elicitation_needs_url_capability() {
    let mut server = Server::new("s", "1");
    server.set_max_threads(1);
    server.add_tool_ctx(Tool::new("go", "d"), |_a, _ctx| {
        ToolResult::elicit(Elicitation::make_url(
            "go",
            "Open this",
            "https://example.com/x",
        ))
    });
    let params = eliciting_params("go", json!({}));
    let out = serve(server, &[stateless(1, "tools/call", params)]);
    assert_eq!(
        parse(&out[0])["error"]["message"],
        "Client does not support url mode elicitation"
    );
}

#[test]
fn an_empty_elicitation_capability_means_form_mode() {
    let params = json!({
        "name": "ask",
        "_meta": {
            META_VERSION: V,
            "io.modelcontextprotocol/clientCapabilities": {"elicitation": {}}
        }
    });
    let out = serve(ask_server(), &[stateless(1, "tools/call", params)]);
    assert_eq!(parse(&out[0])["result"]["resultType"], "input_required");
}

#[test]
fn a_plain_tool_that_elicits_reports_a_configuration_error() {
    let mut server = Server::new("s", "1");
    server.set_max_threads(1);
    server.add_tool(Tool::new("plain", "d"), |_a| {
        ToolResult::elicit(Elicitation::make_form("k", "m"))
    });
    let out = serve(
        server,
        &[stateless(
            1,
            "tools/call",
            eliciting_params("plain", json!({})),
        )],
    );
    assert_eq!(error_code(&out[0]), -32603);
    assert_eq!(
        parse(&out[0])["error"]["message"],
        "Tool plain asked for input but was added with add_tool, not add_tool_ctx, so it can't read the answers"
    );
}

#[test]
fn an_elicitation_with_a_broken_schema_is_an_internal_error() {
    let mut server = Server::new("s", "1");
    server.set_max_threads(1);
    server.add_tool_ctx(Tool::new("bad", "d"), |_a, _ctx| {
        ToolResult::elicit(Elicitation::make_form("k", "m").set_schema("{not json"))
    });
    let out = serve(
        server,
        &[stateless(
            1,
            "tools/call",
            eliciting_params("bad", json!({})),
        )],
    );
    assert_eq!(error_code(&out[0]), -32603);
    assert_eq!(
        parse(&out[0])["error"]["message"],
        "Elicitation 'k' has a schema that is not valid JSON"
    );
}

#[test]
fn an_elicitation_field_must_be_a_primitive() {
    let mut server = Server::new("s", "1");
    server.set_max_threads(1);
    server.add_tool_ctx(Tool::new("bad", "d"), |_a, _ctx| {
        ToolResult::elicit(Elicitation::make_form("k", "m").add_field(
            "o",
            PropertyType::Object,
            "nope",
        ))
    });
    let out = serve(
        server,
        &[stateless(
            1,
            "tools/call",
            eliciting_params("bad", json!({})),
        )],
    );
    assert_eq!(
        parse(&out[0])["error"]["message"],
        "Elicitation 'k' field 'o' must be a string, number, integer or boolean"
    );
}

// ---------------------------------------------------------------------------
// Concurrency
// ---------------------------------------------------------------------------

#[test]
fn many_calls_can_be_in_flight_at_once() {
    const REQUESTS: i64 = 40;
    let mut server = Server::new("test-server", "9.9.9");
    server.set_max_threads(4);
    let live = Arc::new(AtomicUsize::new(0));
    let peak = Arc::new(AtomicUsize::new(0));
    let live_tool = Arc::clone(&live);
    let peak_tool = Arc::clone(&peak);
    server.add_tool_ctx(
        Tool::new("work", "d").add_parameter_simple("i", PropertyType::Integer, "index"),
        move |a, _ctx| {
            let now = live_tool.fetch_add(1, Ordering::SeqCst) + 1;
            peak_tool.fetch_max(now, Ordering::SeqCst);
            std::thread::sleep(Duration::from_millis(20));
            live_tool.fetch_sub(1, Ordering::SeqCst);
            ToolResult::text(format!("i={}", args::integer(a, "i", -1)))
        },
    );

    let mut input = Vec::new();
    for i in 0..REQUESTS {
        input.push(stateless(
            i,
            "tools/call",
            json!({"name":"work","arguments":{"i":i}}),
        ));
    }

    let started = Instant::now();
    let out = serve(server, &input);
    let elapsed = started.elapsed();

    assert_eq!(out.len() as i64, REQUESTS, "{out:?}");
    let mut ids: Vec<i64> = out
        .iter()
        .map(|l| parse(l)["id"].as_i64().unwrap())
        .collect();
    ids.sort_unstable();
    assert_eq!(ids, (0..REQUESTS).collect::<Vec<_>>());
    for line in &out {
        let v = parse(line);
        let id = v["id"].as_i64().unwrap();
        assert_eq!(v["result"]["content"][0]["text"], format!("i={id}"));
    }
    assert!(
        peak.load(Ordering::SeqCst) > 1,
        "calls never actually overlapped"
    );
    assert!(
        elapsed < Duration::from_millis(20 * REQUESTS as u64),
        "calls did not run in parallel: {elapsed:?}"
    );
}
