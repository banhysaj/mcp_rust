//! End-to-end tests for the Streamable HTTP transport.
//!
//! These start a real [`HttpServer`] on an ephemeral port and speak HTTP/1.1 over a
//! real socket, so the request parsing, routing, status mapping and SSE framing are
//! all exercised.

use std::io::{Read, Write};
use std::net::TcpStream;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use mcp_async::{
    args, AuthDecision, HttpOptions, HttpRequest, HttpServer, PropertyType, Server, Tool,
    ToolResult,
};
use serde_json::{json, Value};

const V: &str = "2026-07-28";
const META_VERSION: &str = "io.modelcontextprotocol/protocolVersion";

// ---------------------------------------------------------------------------
// Fixtures
// ---------------------------------------------------------------------------

fn base_server() -> Server {
    let mut server = Server::new("http-test", "1.0.0");
    server.set_max_threads(2);
    server.add_tool(
        Tool::new("echo", "Echo").add_parameter_simple("text", PropertyType::String, "what"),
        |a| ToolResult::text(args::str(a, "text", "")),
    );
    server.add_tool_ctx(Tool::new("who", "Report the principal"), |_a, ctx| {
        ToolResult::text(ctx.principal())
    });
    server
}

fn start(server: Server, mut opts: HttpOptions) -> (u16, Arc<HttpServer>) {
    // Every test gets its own ephemeral port so they can run in parallel.
    opts.port = 0;
    let http = Arc::new(HttpServer::new(Arc::new(server), opts));
    assert!(http.listen_in_background(), "could not bind");
    let port = http.port();
    assert!(port > 0, "no port was bound");
    (port, http)
}

/// A plain JSON content type.
const JSON_CT: (&str, &str) = ("Content-Type", "application/json");
/// For `2026-07-28` requests the revision header must agree with `_meta`.
const HDR_VERSION: (&str, &str) = ("MCP-Protocol-Version", V);

fn init_body() -> String {
    json!({
        "jsonrpc": "2.0", "id": 1, "method": "initialize",
        "params": {"protocolVersion": "2025-11-25", "capabilities": {}, "clientInfo": {"name":"c","version":"1"}}
    })
    .to_string()
}

/// A stateless `tools/call` body.
///
/// Extra `params` are merged in; a `_meta` key in `extra` is merged *into* the
/// per-call `_meta` rather than replacing it, so a `progressToken` cannot
/// accidentally drop the protocol revision.
fn call_body(id: i64, name: &str, extra: Value) -> String {
    let mut meta = json!({ META_VERSION: V });
    let mut extra = extra;
    if let Value::Object(e) = &mut extra {
        if let Some(Value::Object(m)) = e.remove("_meta") {
            let slot = meta.as_object_mut().expect("meta is an object");
            for (k, v) in m {
                slot.insert(k, v);
            }
        }
    }

    let mut params = json!({ "name": name });
    if let Value::Object(e) = extra {
        let slot = params.as_object_mut().expect("params is an object");
        for (k, v) in e {
            slot.insert(k, v);
        }
    }
    params
        .as_object_mut()
        .expect("params is an object")
        .insert("_meta".to_string(), meta);

    json!({"jsonrpc":"2.0","id":id,"method":"tools/call","params":params}).to_string()
}

fn list_body() -> String {
    json!({"jsonrpc":"2.0","id":1,"method":"tools/list","params":{"_meta":{META_VERSION:V}}})
        .to_string()
}

// ---------------------------------------------------------------------------
// Raw HTTP client
// ---------------------------------------------------------------------------

fn request_bytes(method: &str, path: &str, headers: &[(&str, &str)], body: &str) -> String {
    let mut req = format!("{method} {path} HTTP/1.1\r\nHost: 127.0.0.1\r\n");
    for (k, v) in headers {
        req.push_str(k);
        req.push_str(": ");
        req.push_str(v);
        req.push_str("\r\n");
    }
    req.push_str(&format!("Content-Length: {}\r\n", body.len()));
    req.push_str("Connection: close\r\n\r\n");
    req.push_str(body);
    req
}

fn connect(port: u16) -> TcpStream {
    let s = TcpStream::connect(("127.0.0.1", port)).expect("connect");
    s.set_read_timeout(Some(Duration::from_secs(15))).unwrap();
    s
}

/// Send one request and read until the server closes.
fn exchange(port: u16, spec: &str) -> Vec<u8> {
    let mut s = connect(port);
    s.write_all(spec.as_bytes()).unwrap();
    s.flush().unwrap();
    let mut out = Vec::new();
    let _ = s.read_to_end(&mut out);
    out
}

struct Resp {
    status: u16,
    headers: Vec<(String, String)>,
    body: String,
    raw: String,
}

impl Resp {
    fn header(&self, name: &str) -> String {
        self.headers
            .iter()
            .find(|(k, _)| k.eq_ignore_ascii_case(name))
            .map(|(_, v)| v.clone())
            .unwrap_or_default()
    }

    fn json(&self) -> Value {
        serde_json::from_str(&self.body).unwrap_or_else(|e| panic!("not JSON ({e}): {}", self.body))
    }
}

fn parse_response(bytes: &[u8]) -> Resp {
    let raw = String::from_utf8_lossy(bytes).into_owned();
    let (head, body) = match raw.find("\r\n\r\n") {
        Some(i) => (&raw[..i], &raw[i + 4..]),
        None => (raw.as_str(), ""),
    };
    let mut lines = head.split("\r\n");
    let status_line = lines.next().unwrap_or("");
    let status = status_line
        .split_whitespace()
        .nth(1)
        .and_then(|s| s.parse().ok())
        .unwrap_or_else(|| panic!("bad status line: {status_line:?}"));
    let headers = lines
        .filter_map(|l| {
            l.split_once(':')
                .map(|(k, v)| (k.trim().to_ascii_lowercase(), v.trim().to_string()))
        })
        .collect();
    Resp {
        status,
        headers,
        body: body.to_string(),
        raw,
    }
}

fn post(port: u16, path: &str, headers: &[(&str, &str)], body: &str) -> Resp {
    parse_response(&exchange(port, &request_bytes("POST", path, headers, body)))
}

fn post_json(port: u16, body: &str) -> Resp {
    post(port, "/mcp", &[("Content-Type", "application/json")], body)
}

/// Read exactly one `Content-Length`-delimited response from a live connection.
fn read_one_response(s: &mut TcpStream) -> (u16, String) {
    let mut head = Vec::new();
    let mut byte = [0u8; 1];
    while !head.ends_with(b"\r\n\r\n") {
        match s.read(&mut byte) {
            Ok(0) => break,
            Ok(_) => head.push(byte[0]),
            Err(e) => panic!("read failed: {e}"),
        }
    }
    let head = String::from_utf8_lossy(&head).into_owned();
    let status = head
        .split_whitespace()
        .nth(1)
        .and_then(|x| x.parse().ok())
        .unwrap_or(0);
    let len = head
        .split("\r\n")
        .find_map(|l| {
            let (k, v) = l.split_once(':')?;
            if k.trim().eq_ignore_ascii_case("content-length") {
                v.trim().parse::<usize>().ok()
            } else {
                None
            }
        })
        .unwrap_or(0);
    let mut body = vec![0u8; len];
    if len > 0 {
        s.read_exact(&mut body).expect("short body");
    }
    (status, String::from_utf8_lossy(&body).into_owned())
}

/// Decode a `Transfer-Encoding: chunked` payload.
fn dechunk(raw: &[u8]) -> Vec<u8> {
    let mut out = Vec::new();
    let mut i = 0usize;
    loop {
        let start = i;
        while i < raw.len() && raw[i] != b'\n' {
            i += 1;
        }
        if i >= raw.len() {
            break;
        }
        let line = String::from_utf8_lossy(&raw[start..i]).trim().to_string();
        i += 1;
        let size = usize::from_str_radix(line.split(';').next().unwrap_or("").trim(), 16)
            .unwrap_or_else(|_| panic!("bad chunk size {line:?}"));
        if size == 0 {
            break;
        }
        out.extend_from_slice(&raw[i..i + size]);
        i += size + 2;
    }
    out
}

fn sse_body(resp: &Resp) -> String {
    let idx = resp.raw.find("\r\n\r\n").expect("no header terminator") + 4;
    let raw = resp.raw.as_bytes();
    String::from_utf8_lossy(&dechunk(&raw[idx..])).into_owned()
}

// ---------------------------------------------------------------------------
// Happy paths
// ---------------------------------------------------------------------------

#[test]
fn post_initialize_returns_json() {
    let (port, http) = start(base_server(), HttpOptions::default());
    let resp = post_json(port, &init_body());
    assert_eq!(resp.status, 200);
    assert_eq!(resp.header("content-type"), "application/json");
    let v = resp.json();
    assert_eq!(v["id"], 1);
    assert_eq!(v["result"]["protocolVersion"], "2025-11-25");
    assert_eq!(v["result"]["serverInfo"]["name"], "http-test");
    // `initialize` is always answered in the stateful shape.
    assert!(v["result"].get("resultType").is_none());
    http.stop();
}

#[test]
fn stateless_tools_list_carries_the_cache_hint() {
    let (port, http) = start(base_server(), HttpOptions::default());
    let resp = post(
        port,
        "/mcp",
        &[JSON_CT, HDR_VERSION, ("Mcp-Method", "tools/list")],
        &list_body(),
    );
    assert_eq!(resp.status, 200);
    let v = resp.json();
    assert_eq!(v["result"]["resultType"], "complete");
    assert_eq!(v["result"]["ttlMs"], 0);
    assert_eq!(v["result"]["cacheScope"], "private");
    assert_eq!(v["result"]["tools"][0]["name"], "echo");
    http.stop();
}

#[test]
fn a_notification_is_acknowledged_with_202_and_no_body() {
    let (port, http) = start(base_server(), HttpOptions::default());
    let body = json!({"jsonrpc":"2.0","method":"notifications/initialized"}).to_string();
    let resp = post_json(port, &body);
    assert_eq!(resp.status, 202);
    assert!(resp.body.is_empty(), "{:?}", resp.body);
    http.stop();
}

#[test]
fn keep_alive_serves_two_requests_on_one_connection() {
    let (port, http) = start(base_server(), HttpOptions::default());
    let mut s = connect(port);
    let one = format!(
        "POST /mcp HTTP/1.1\r\nHost: x\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{}",
        init_body().len(),
        init_body()
    );
    s.write_all(one.as_bytes()).unwrap();
    s.flush().unwrap();
    let (status1, body1) = read_one_response(&mut s);
    assert_eq!(status1, 200);
    assert_eq!(serde_json::from_str::<Value>(&body1).unwrap()["id"], 1);

    let two_body = list_body();
    let two = format!(
        "POST /mcp HTTP/1.1\r\nHost: x\r\nContent-Type: application/json\r\n\
         MCP-Protocol-Version: {V}\r\nMcp-Method: tools/list\r\nContent-Length: {}\r\n\r\n{}",
        two_body.len(),
        two_body
    );
    s.write_all(two.as_bytes()).unwrap();
    s.flush().unwrap();
    let (status2, body2) = read_one_response(&mut s);
    assert_eq!(status2, 200);
    assert_eq!(
        serde_json::from_str::<Value>(&body2).unwrap()["result"]["tools"][0]["name"],
        "echo"
    );
    drop(s);
    http.stop();
}

#[test]
fn a_chunked_request_body_is_accepted() {
    let (port, http) = start(base_server(), HttpOptions::default());
    let body = list_body();
    let spec = format!(
        "POST /mcp HTTP/1.1\r\nHost: x\r\nContent-Type: application/json\r\n\
         MCP-Protocol-Version: {V}\r\nMcp-Method: tools/list\r\n\
         Transfer-Encoding: chunked\r\nConnection: close\r\n\r\n{:x}\r\n{}\r\n0\r\n\r\n",
        body.len(),
        body
    );
    let resp = parse_response(&exchange(port, &spec));
    assert_eq!(resp.status, 200);
    assert_eq!(resp.json()["result"]["tools"][0]["name"], "echo");
    http.stop();
}

// ---------------------------------------------------------------------------
// Routing
// ---------------------------------------------------------------------------

#[test]
fn get_and_delete_get_405_with_allow_post() {
    let (port, http) = start(base_server(), HttpOptions::default());
    for method in ["GET", "DELETE"] {
        let resp = parse_response(&exchange(
            port,
            &format!("{method} /mcp HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n"),
        ));
        assert_eq!(resp.status, 405, "{method}");
        assert_eq!(resp.header("allow"), "POST", "{method}");
        assert!(resp.body.is_empty());
    }
    http.stop();
}

#[test]
fn other_methods_and_paths_get_404() {
    let (port, http) = start(base_server(), HttpOptions::default());
    let put = parse_response(&exchange(
        port,
        "PUT /mcp HTTP/1.1\r\nHost: x\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
    ));
    assert_eq!(put.status, 404);

    let wrong_path = post(port, "/nope", &[], &init_body());
    assert_eq!(wrong_path.status, 404);
    http.stop();
}

#[test]
fn a_bad_request_line_is_rejected() {
    let (port, http) = start(base_server(), HttpOptions::default());
    let resp = parse_response(&exchange(port, "GARBAGE\r\n\r\n"));
    assert_eq!(resp.status, 400);
    http.stop();
}

#[test]
fn an_oversized_body_is_rejected_with_413() {
    let opts = HttpOptions {
        max_body_bytes: 32,
        ..Default::default()
    };
    let (port, http) = start(base_server(), opts);
    let big = json!({"jsonrpc":"2.0","id":1,"method":"ping","params":{"pad":"x".repeat(200)}})
        .to_string();
    let resp = post_json(port, &big);
    assert_eq!(resp.status, 413);
    http.stop();
}

// ---------------------------------------------------------------------------
// Origin and auth
// ---------------------------------------------------------------------------

#[test]
fn a_foreign_origin_is_refused() {
    let (port, http) = start(base_server(), HttpOptions::default());
    let resp = post(
        port,
        "/mcp",
        &[
            ("Origin", "http://evil.example"),
            ("Content-Type", "application/json"),
        ],
        &init_body(),
    );
    assert_eq!(resp.status, 403);
    assert_eq!(resp.json()["error"]["code"], -32600);
    assert_eq!(resp.json()["error"]["message"], "Origin not allowed");
    http.stop();
}

#[test]
fn localhost_origins_and_missing_origins_pass() {
    let (port, http) = start(base_server(), HttpOptions::default());
    for origin in [
        "http://localhost:5173",
        "https://127.0.0.1",
        "http://[::1]:9000",
    ] {
        let resp = post(
            port,
            "/mcp",
            &[("Origin", origin), ("Content-Type", "application/json")],
            &init_body(),
        );
        assert_eq!(resp.status, 200, "origin {origin}");
    }
    // Server-to-server: no Origin at all.
    assert_eq!(post_json(port, &init_body()).status, 200);
    http.stop();
}

#[test]
fn an_explicit_origin_allow_list_replaces_the_localhost_default() {
    let opts = HttpOptions {
        allowed_origins: vec!["https://ok.example".to_string()],
        ..Default::default()
    };
    let (port, http) = start(base_server(), opts);

    let denied = post(
        port,
        "/mcp",
        &[
            ("Origin", "http://localhost"),
            ("Content-Type", "application/json"),
        ],
        &init_body(),
    );
    assert_eq!(denied.status, 403);

    let allowed = post(
        port,
        "/mcp",
        &[
            ("Origin", "https://ok.example"),
            ("Content-Type", "application/json"),
        ],
        &init_body(),
    );
    assert_eq!(allowed.status, 200);
    http.stop();
}

#[test]
fn a_denied_authorization_returns_401_with_the_challenge() {
    let opts = HttpOptions {
        authorize: Some(Arc::new(|_req: &HttpRequest| {
            AuthDecision::deny("Bearer resource_metadata=\"https://example.com/prm\"")
        })),
        ..Default::default()
    };
    let (port, http) = start(base_server(), opts);
    let resp = post_json(port, &init_body());
    assert_eq!(resp.status, 401);
    assert_eq!(
        resp.header("www-authenticate"),
        "Bearer resource_metadata=\"https://example.com/prm\""
    );
    http.stop();
}

#[test]
fn an_allowed_authorization_exposes_the_principal_to_tools() {
    let opts = HttpOptions {
        authorize: Some(Arc::new(|req: &HttpRequest| {
            if req.header("Authorization") == "Bearer secret" {
                AuthDecision::allow_as("user-1")
            } else {
                AuthDecision::deny("Bearer")
            }
        })),
        ..Default::default()
    };
    let (port, http) = start(base_server(), opts);

    let denied = post_json(port, &init_body());
    assert_eq!(denied.status, 401);
    assert_eq!(denied.header("www-authenticate"), "Bearer");

    let allowed = post(
        port,
        "/mcp",
        &[
            ("Authorization", "Bearer secret"),
            JSON_CT,
            HDR_VERSION,
            ("Mcp-Method", "tools/call"),
            ("Mcp-Name", "who"),
        ],
        &call_body(3, "who", json!({})),
    );
    assert_eq!(allowed.status, 200);
    assert_eq!(allowed.json()["result"]["content"][0]["text"], "user-1");
    http.stop();
}

// ---------------------------------------------------------------------------
// Body / header validation
// ---------------------------------------------------------------------------

#[test]
fn a_parse_error_is_a_400() {
    let (port, http) = start(base_server(), HttpOptions::default());
    let resp = post_json(port, "{oops");
    assert_eq!(resp.status, 400);
    assert_eq!(resp.json()["error"]["code"], -32700);
    assert_eq!(resp.json()["error"]["message"], "Parse error");
    http.stop();
}

#[test]
fn a_non_object_body_is_a_400() {
    let (port, http) = start(base_server(), HttpOptions::default());
    let resp = post_json(port, "[1,2,3]");
    assert_eq!(resp.status, 400);
    assert_eq!(resp.json()["error"]["code"], -32600);
    assert_eq!(
        resp.json()["error"]["message"],
        "Body must be a single JSON-RPC message"
    );
    http.stop();
}

#[test]
fn a_deeply_nested_body_is_a_400() {
    let (port, http) = start(base_server(), HttpOptions::default());
    let deep = format!("{}{}", "[".repeat(200), "]".repeat(200));
    let resp = post_json(port, &deep);
    assert_eq!(resp.status, 400);
    assert_eq!(
        resp.json()["error"]["message"],
        "Message is nested too deeply"
    );
    http.stop();
}

#[test]
fn missing_protocol_headers_are_a_header_mismatch() {
    let (port, http) = start(base_server(), HttpOptions::default());
    let resp = post_json(
        port,
        &call_body(2, "echo", json!({"arguments": {"text": "hi"}})),
    );
    assert_eq!(resp.status, 400);
    let v = resp.json();
    assert_eq!(v["error"]["code"], -32020);
    // The id sits on the envelope, not inside the error object.
    assert_eq!(v["id"], 2);
    assert!(
        v["error"]["message"]
            .as_str()
            .unwrap()
            .contains("missing MCP-Protocol-Version header"),
        "{v}"
    );
    http.stop();
}

#[test]
fn matching_protocol_headers_are_accepted() {
    let (port, http) = start(base_server(), HttpOptions::default());
    let resp = post(
        port,
        "/mcp",
        &[
            ("Content-Type", "application/json"),
            ("MCP-Protocol-Version", V),
            ("Mcp-Method", "tools/call"),
            ("Mcp-Name", "echo"),
        ],
        &call_body(2, "echo", json!({"arguments": {"text": "hi"}})),
    );
    assert_eq!(resp.status, 200);
    assert_eq!(resp.json()["result"]["content"][0]["text"], "hi");
    http.stop();
}

#[test]
fn a_base64_mcp_name_header_is_accepted() {
    let (port, http) = start(base_server(), HttpOptions::default());
    // "echo" in base64 is "ZWNobw==".
    let resp = post(
        port,
        "/mcp",
        &[
            ("Content-Type", "application/json"),
            ("MCP-Protocol-Version", V),
            ("Mcp-Method", "tools/call"),
            ("Mcp-Name", "=?base64?ZWNobw==?="),
        ],
        &call_body(2, "echo", json!({"arguments": {"text": "hi"}})),
    );
    assert_eq!(resp.status, 200);
    http.stop();
}

#[test]
fn a_mismatched_mcp_name_header_is_rejected() {
    let (port, http) = start(base_server(), HttpOptions::default());
    let resp = post(
        port,
        "/mcp",
        &[
            ("Content-Type", "application/json"),
            ("MCP-Protocol-Version", V),
            ("Mcp-Method", "tools/call"),
            ("Mcp-Name", "something-else"),
        ],
        &call_body(2, "echo", json!({"arguments": {"text": "hi"}})),
    );
    assert_eq!(resp.status, 400);
    assert_eq!(resp.json()["error"]["code"], -32020);
    assert!(resp.json()["error"]["message"]
        .as_str()
        .unwrap()
        .contains("does not match body value 'echo'"));
    http.stop();
}

#[test]
fn status_codes_follow_the_jsonrpc_error_code() {
    let (port, http) = start(base_server(), HttpOptions::default());

    // Unknown method -> -32601 -> 404
    let unknown =
        json!({"jsonrpc":"2.0","id":1,"method":"nope","params":{"_meta":{META_VERSION:V}}})
            .to_string();
    let resp = post(
        port,
        "/mcp",
        &[JSON_CT, HDR_VERSION, ("Mcp-Method", "nope")],
        &unknown,
    );
    assert_eq!(resp.status, 404);
    assert_eq!(resp.json()["error"]["code"], -32601);

    // Invalid params -> -32602 -> 200 (the call itself was well formed)
    let bad = post(
        port,
        "/mcp",
        &[
            JSON_CT,
            HDR_VERSION,
            ("Mcp-Method", "tools/call"),
            ("Mcp-Name", "ghost"),
        ],
        &call_body(2, "ghost", json!({})),
    );
    assert_eq!(bad.status, 200);
    assert_eq!(bad.json()["error"]["code"], -32602);

    // An unsupported per-call version -> -32022 -> 400
    let old =
        json!({"jsonrpc":"2.0","id":1,"method":"tools/list","params":{"_meta":{META_VERSION:"1999-01-01"}}})
            .to_string();
    let resp = post(
        port,
        "/mcp",
        &[
            JSON_CT,
            ("MCP-Protocol-Version", "1999-01-01"),
            ("Mcp-Method", "tools/list"),
        ],
        &old,
    );
    assert_eq!(resp.status, 400);
    assert_eq!(resp.json()["error"]["code"], -32022);

    http.stop();
}

// ---------------------------------------------------------------------------
// SSE
// ---------------------------------------------------------------------------

fn crunch_server() -> Server {
    let mut server = Server::new("sse-test", "1.0.0");
    server.add_tool_ctx(
        Tool::new("crunch", "Long job").add_parameter_simple("n", PropertyType::Integer, "iters"),
        |a, ctx| {
            let n = args::integer(a, "n", 0);
            for i in 0..n {
                if ctx.cancelled() {
                    return ToolResult::error("cancelled");
                }
                ctx.progress(i as f64, Some(n as f64), Some("working"));
                ctx.log("info", "step");
            }
            ToolResult::text("done")
        },
    );
    server
}

#[test]
fn tools_call_streams_sse_when_the_client_asks_for_it() {
    let (port, http) = start(crunch_server(), HttpOptions::default());
    let body = call_body(
        4,
        "crunch",
        json!({"arguments": {"n": 2}, "_meta": {"progressToken": "tok"}}),
    );
    let resp = post(
        port,
        "/mcp",
        &[
            JSON_CT,
            HDR_VERSION,
            ("Mcp-Method", "tools/call"),
            ("Mcp-Name", "crunch"),
            ("Accept", "text/event-stream"),
        ],
        &body,
    );
    assert_eq!(resp.status, 200);
    assert_eq!(resp.header("content-type"), "text/event-stream");
    assert_eq!(resp.header("cache-control"), "no-cache");
    assert_eq!(resp.header("x-accel-buffering"), "no");
    assert_eq!(resp.header("transfer-encoding"), "chunked");

    let stream = sse_body(&resp);
    let events: Vec<&str> = stream.split("\n\n").filter(|e| !e.is_empty()).collect();
    // 2 progress + 2 log + the result
    assert_eq!(events.len(), 5, "{stream:?}");

    let payload = |e: &str| -> Value {
        let data = e
            .strip_prefix("data: ")
            .unwrap_or_else(|| panic!("bad event {e:?}"));
        serde_json::from_str(data).unwrap()
    };

    assert_eq!(payload(events[0])["method"], "notifications/progress");
    assert_eq!(payload(events[0])["params"]["progress"], 0.0);
    assert_eq!(payload(events[0])["params"]["total"], 2.0);
    assert_eq!(payload(events[0])["params"]["progressToken"], "tok");
    assert_eq!(payload(events[0])["params"]["message"], "working");
    assert_eq!(payload(events[1])["method"], "notifications/message");
    assert_eq!(payload(events[1])["params"]["data"], "step");

    let last = payload(events[4]);
    assert_eq!(last["id"], 4);
    assert_eq!(last["result"]["content"][0]["text"], "done");
    assert_eq!(last["result"]["resultType"], "complete");

    http.stop();
}

#[test]
fn a_plain_json_reply_is_used_when_sse_is_not_accepted() {
    let (port, http) = start(crunch_server(), HttpOptions::default());
    let body = call_body(4, "crunch", json!({"arguments": {"n": 1}}));
    let resp = post(
        port,
        "/mcp",
        &[
            JSON_CT,
            HDR_VERSION,
            ("Mcp-Method", "tools/call"),
            ("Mcp-Name", "crunch"),
        ],
        &body,
    );
    assert_eq!(resp.status, 200);
    assert_eq!(resp.header("content-type"), "application/json");
    assert_eq!(resp.json()["result"]["content"][0]["text"], "done");
    http.stop();
}

#[test]
fn closing_an_sse_stream_cancels_the_tool() {
    let cancelled = Arc::new(AtomicBool::new(false));
    let flag = Arc::clone(&cancelled);
    let mut server = Server::new("s", "1.0.0");
    server.add_tool_ctx(Tool::new("long", "Runs until cancelled"), move |_a, ctx| {
        let deadline = Instant::now() + Duration::from_secs(8);
        while Instant::now() < deadline {
            if ctx.cancelled() {
                flag.store(true, Ordering::SeqCst);
                return ToolResult::error("cancelled");
            }
            std::thread::sleep(Duration::from_millis(20));
        }
        ToolResult::text("timed out")
    });

    let (port, http) = start(server, HttpOptions::default());
    let body = call_body(4, "long", json!({}));
    let mut s = connect(port);
    let spec = request_bytes(
        "POST",
        "/mcp",
        &[
            JSON_CT,
            HDR_VERSION,
            ("Mcp-Method", "tools/call"),
            ("Mcp-Name", "long"),
            ("Accept", "text/event-stream"),
        ],
        &body,
    );
    s.write_all(spec.as_bytes()).unwrap();
    s.flush().unwrap();

    // Wait for the response head, then hang up mid-call.
    let mut buf = [0u8; 256];
    let n = s.read(&mut buf).expect("no response head");
    assert!(n > 0);
    assert!(String::from_utf8_lossy(&buf[..n]).starts_with("HTTP/1.1 200"));
    drop(s);

    let deadline = Instant::now() + Duration::from_secs(7);
    while Instant::now() < deadline && !cancelled.load(Ordering::SeqCst) {
        std::thread::sleep(Duration::from_millis(25));
    }
    assert!(
        cancelled.load(Ordering::SeqCst),
        "the handler never observed the client hang-up"
    );
    http.stop();
}

#[test]
fn sse_keep_alive_comments_are_emitted_while_idle() {
    let mut server = Server::new("s", "1.0.0");
    server.add_tool_ctx(Tool::new("slow", "Sleeps"), |_a, _ctx| {
        std::thread::sleep(Duration::from_millis(2500));
        ToolResult::text("done")
    });
    let opts = HttpOptions {
        keep_alive_seconds: 1,
        ..Default::default()
    };
    let (port, http) = start(server, opts);

    let resp = post(
        port,
        "/mcp",
        &[
            JSON_CT,
            HDR_VERSION,
            ("Mcp-Method", "tools/call"),
            ("Mcp-Name", "slow"),
            ("Accept", "text/event-stream"),
        ],
        &call_body(4, "slow", json!({})),
    );
    let stream = sse_body(&resp);
    assert!(
        stream.contains(": keep-alive"),
        "no keep-alive comment in {stream:?}"
    );
    assert!(stream.contains("\"done\""), "{stream:?}");
    http.stop();
}
