//! Streamable HTTP transport.
//!
//! A small HTTP/1.1 server for the single MCP endpoint, including chunked
//! server-sent events for streamed tool calls. Each connection is served by a
//! worker thread, and a client that closes a stream mid-call cancels the tool
//! through [`crate::RequestContext::cancelled`].
//!
//! Requests are handled in this order: `Origin` check, authorization, nesting
//! guard, parse, header/body agreement, dispatch. For `2026-07-28` requests the
//! `MCP-Protocol-Version`, `Mcp-Method` and `Mcp-Name` headers have to agree with
//! the body, or the reply is `400` with `HeaderMismatch` (-32020).
//!
//! There is no TLS: put a reverse proxy in front when serving over the public
//! internet.

use std::collections::{HashMap, VecDeque};
use std::io::{self, BufRead, BufReader, Write};
use std::net::{Shutdown, SocketAddr, TcpListener, TcpStream};
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::atomic::{AtomicBool, AtomicU16, Ordering};
use std::sync::{Arc, Condvar, Mutex, MutexGuard};
use std::thread::{self, JoinHandle};
use std::time::Duration;

use serde_json::Value;

use crate::constants::*;
use crate::json::{self, args};
use crate::pool::ThreadPool;
use crate::server::{RequestScope, Server};
use crate::types::ErrorCode;

/// Most events queued for a slow SSE client before further events are dropped.
const MAX_QUEUED_EVENTS: usize = 1024;

/// Most headers accepted in one request.
const MAX_HEADERS: usize = 100;

/// Most requests served on one kept-alive connection.
const MAX_KEEP_ALIVE_REQUESTS: usize = 5;

/// Default connection-handler count: the larger of 8 and one less than the number
/// of hardware threads.
fn default_http_threads() -> usize {
    let hw = thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(1);
    std::cmp::max(8, hw.saturating_sub(1))
}

// ---------------------------------------------------------------------------
// Request and options
// ---------------------------------------------------------------------------

/// The parts of an HTTP request that an authorizer can inspect.
#[derive(Debug, Clone, Default)]
pub struct HttpRequest {
    /// The peer address, as `host:port`.
    pub remote_addr: String,
    /// Header names, lower-cased. The first occurrence of a repeated header wins.
    headers: HashMap<String, String>,
}

impl HttpRequest {
    /// The value of a header, looked up case-insensitively.
    ///
    /// Returns an empty string when the header is absent.
    pub fn header(&self, name: &str) -> String {
        self.headers
            .get(&name.to_ascii_lowercase())
            .cloned()
            .unwrap_or_default()
    }

    /// All headers, keyed by lower-cased name.
    pub fn headers(&self) -> &HashMap<String, String> {
        &self.headers
    }
}

/// The result of an [`HttpOptions::authorize`] callback.
#[derive(Debug, Clone)]
pub enum AuthDecision {
    /// Serve the request, exposing `principal` to tools as
    /// [`crate::RequestContext::principal`].
    Allow {
        /// The authenticated principal.
        principal: String,
    },
    /// Refuse with a `401`.
    Deny {
        /// Value for the `WWW-Authenticate` response header. Omitted when empty.
        challenge: String,
    },
}

impl Default for AuthDecision {
    fn default() -> Self {
        AuthDecision::Allow {
            principal: String::new(),
        }
    }
}

impl AuthDecision {
    /// Serve the request with no principal.
    pub fn allow() -> Self {
        AuthDecision::Allow {
            principal: String::new(),
        }
    }

    /// Serve the request as `principal`.
    pub fn allow_as(principal: impl Into<String>) -> Self {
        AuthDecision::Allow {
            principal: principal.into(),
        }
    }

    /// Refuse the request, optionally with a `WWW-Authenticate` challenge.
    pub fn deny(challenge: impl Into<String>) -> Self {
        AuthDecision::Deny {
            challenge: challenge.into(),
        }
    }
}

/// Decides whether a request is served, and as whom.
///
/// The callback runs before the request body is parsed. It can be called from
/// several connection handlers at once.
pub type Authorizer = Arc<dyn Fn(&HttpRequest) -> AuthDecision + Send + Sync>;

/// Configuration for [`HttpServer`].
pub struct HttpOptions {
    /// Address to bind. Keep this at `127.0.0.1` when there is no `authorize`.
    pub host: String,
    /// Port to bind. Zero asks the operating system for a free port.
    pub port: u16,
    /// Path of the MCP endpoint.
    pub path: String,
    /// Exact-match `Origin` allow-list. Empty means localhost origins only.
    pub allowed_origins: Vec<String>,
    /// Largest accepted request body, in bytes.
    pub max_body_bytes: usize,
    /// Number of connection handlers. Zero means the default.
    pub threads: usize,
    /// Seconds of silence after which an SSE stream emits a keep-alive comment.
    pub keep_alive_seconds: u64,
    /// Seconds to wait for the next request on a kept-alive connection.
    pub read_timeout_seconds: u64,
    /// Cap on the size of the request line and headers together.
    pub max_header_bytes: usize,
    /// Runs before the body is parsed. `None` serves every request.
    pub authorize: Option<Authorizer>,
}

impl Default for HttpOptions {
    fn default() -> Self {
        HttpOptions {
            host: "127.0.0.1".to_string(),
            port: 8080,
            path: "/mcp".to_string(),
            allowed_origins: Vec::new(),
            max_body_bytes: 4 * 1024 * 1024,
            threads: 0,
            keep_alive_seconds: 15,
            read_timeout_seconds: 5,
            max_header_bytes: 64 * 1024,
            authorize: None,
        }
    }
}

impl std::fmt::Debug for HttpOptions {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HttpOptions")
            .field("host", &self.host)
            .field("port", &self.port)
            .field("path", &self.path)
            .field("allowed_origins", &self.allowed_origins)
            .field("max_body_bytes", &self.max_body_bytes)
            .field("threads", &self.threads)
            .field("keep_alive_seconds", &self.keep_alive_seconds)
            .field("read_timeout_seconds", &self.read_timeout_seconds)
            .field("max_header_bytes", &self.max_header_bytes)
            .field("authorize", &self.authorize.is_some())
            .finish()
    }
}

// ---------------------------------------------------------------------------
// HttpServer
// ---------------------------------------------------------------------------

/// Serves one [`Server`] over HTTP.
///
/// Call [`HttpServer::listen`] to block, or [`HttpServer::listen_in_background`]
/// to serve from another thread. Both bind first, so [`HttpServer::port`] is
/// usable as soon as either returns.
pub struct HttpServer {
    server: Arc<Server>,
    opts: Arc<HttpOptions>,
    listener: Mutex<Option<TcpListener>>,
    bound_port: AtomicU16,
    stopped: Arc<AtomicBool>,
    accept_thread: Mutex<Option<JoinHandle<()>>>,
}

impl HttpServer {
    /// Create a server. Nothing is bound until [`HttpServer::bind`] or one of the
    /// listen methods is called.
    pub fn new(server: Arc<Server>, options: HttpOptions) -> Self {
        HttpServer {
            server,
            opts: Arc::new(options),
            listener: Mutex::new(None),
            bound_port: AtomicU16::new(0),
            stopped: Arc::new(AtomicBool::new(false)),
            accept_thread: Mutex::new(None),
        }
    }

    /// Bind the listen socket. Returns false when the address cannot be bound.
    pub fn bind(&self) -> bool {
        if self.bound_port.load(Ordering::SeqCst) > 0 {
            return true;
        }
        match TcpListener::bind((self.opts.host.as_str(), self.opts.port)) {
            Ok(l) => {
                let port = match l.local_addr() {
                    Ok(a) => a.port(),
                    Err(_) => return false,
                };
                if port == 0 {
                    return false;
                }
                *lock(&self.listener) = Some(l);
                self.bound_port.store(port, Ordering::SeqCst);
                true
            }
            Err(_) => false,
        }
    }

    /// The bound port, or 0 before a successful bind.
    pub fn port(&self) -> u16 {
        self.bound_port.load(Ordering::SeqCst)
    }

    /// Serve until [`HttpServer::stop`] is called. Blocks the calling thread.
    pub fn listen(&self) -> bool {
        if !self.bind() {
            return false;
        }
        let listener = match lock(&self.listener)
            .as_ref()
            .and_then(|l| l.try_clone().ok())
        {
            Some(l) => l,
            None => return false,
        };
        if listener.set_nonblocking(true).is_err() {
            return false;
        }

        self.stopped.store(false, Ordering::SeqCst);
        let threads = if self.opts.threads > 0 {
            self.opts.threads
        } else {
            default_http_threads()
        };
        let pool = ThreadPool::new(threads);

        while !self.stopped.load(Ordering::SeqCst) {
            match listener.accept() {
                Ok((stream, addr)) => {
                    let server = Arc::clone(&self.server);
                    let opts = Arc::clone(&self.opts);
                    pool.submit(move || {
                        let _ = handle_connection(stream, addr, server, opts);
                    });
                }
                Err(e) if e.kind() == io::ErrorKind::WouldBlock => {
                    thread::sleep(Duration::from_millis(10));
                }
                Err(e) if e.kind() == io::ErrorKind::Interrupted => {}
                Err(_) => thread::sleep(Duration::from_millis(10)),
            }
        }

        drop(pool);
        true
    }

    /// Bind and serve from a background thread.
    ///
    /// The socket is bound before this returns, so [`HttpServer::port`] is
    /// immediately usable.
    pub fn listen_in_background(self: &Arc<Self>) -> bool {
        if !self.bind() {
            return false;
        }
        let this = Arc::clone(self);
        let handle = thread::Builder::new()
            .name("mcp-http-accept".to_string())
            .spawn(move || {
                this.listen();
            })
            .ok();
        *lock(&self.accept_thread) = handle;
        true
    }

    /// Ask the accept loop to finish. Safe to call from any thread.
    pub fn stop(&self) {
        self.stopped.store(true, Ordering::SeqCst);
    }

    /// Wait for a background accept loop to finish.
    pub fn join(&self) {
        let handle = lock(&self.accept_thread).take();
        if let Some(h) = handle {
            let _ = h.join();
        }
    }
}

impl Drop for HttpServer {
    fn drop(&mut self) {
        self.stop();
        let handle = self
            .accept_thread
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .take();
        if let Some(h) = handle {
            let _ = h.join();
        }
    }
}

fn lock<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    m.lock().unwrap_or_else(|e| e.into_inner())
}

// ---------------------------------------------------------------------------
// Connection handling
// ---------------------------------------------------------------------------

struct Head {
    method: String,
    target: String,
    version: String,
    headers: HashMap<String, String>,
}

enum HeadOutcome {
    Request(Head),
    Eof,
    TooLarge,
    Malformed,
}

enum BodyError {
    TooLarge,
    Bad,
    Io(io::Error),
}

struct SimpleResponse {
    status: u16,
    headers: Vec<(String, String)>,
    body: Vec<u8>,
    json: bool,
}

impl SimpleResponse {
    fn empty(status: u16) -> Self {
        SimpleResponse {
            status,
            headers: Vec::new(),
            body: Vec::new(),
            json: false,
        }
    }

    fn json(status: u16, body: String) -> Self {
        SimpleResponse {
            status,
            headers: Vec::new(),
            body: body.into_bytes(),
            json: true,
        }
    }

    fn with_header(mut self, k: &str, v: &str) -> Self {
        self.headers.push((k.to_string(), v.to_string()));
        self
    }
}

/// What routing decided to do with a request.
enum Routed {
    Response(SimpleResponse),
    /// Stream the reply, then close the connection.
    Sse {
        body: String,
        scope: RequestScope,
    },
}

fn handle_connection(
    stream: TcpStream,
    addr: SocketAddr,
    server: Arc<Server>,
    opts: Arc<HttpOptions>,
) -> io::Result<()> {
    let _ = stream.set_nodelay(true);
    // A socket accepted from a non-blocking listener can inherit non-blocking mode
    // on Windows, which would make every read fail with WSAEWOULDBLOCK instead of
    // waiting for the read timeout. Put it back into blocking mode explicitly.
    let _ = stream.set_nonblocking(false);

    // One socket, borrowed twice: reads go through a buffered reader and writes
    // through the shared handle. `&TcpStream` implements both `Read` and `Write`,
    // which avoids duplicating the socket.
    let mut reader = BufReader::new(&stream);
    let mut writer = &stream;
    let remote = addr.to_string();
    let mut served: usize = 0;
    let idle = Duration::from_secs(opts.read_timeout_seconds.max(1));

    loop {
        let _ = stream.set_read_timeout(Some(idle));
        let head = match read_head(&mut reader, opts.max_header_bytes) {
            HeadOutcome::Request(h) => h,
            HeadOutcome::Eof => return Ok(()),
            HeadOutcome::TooLarge => {
                write_response(&mut writer, &SimpleResponse::empty(431), true)?;
                return Ok(());
            }
            HeadOutcome::Malformed => {
                write_response(&mut writer, &SimpleResponse::empty(400), true)?;
                return Ok(());
            }
        };

        let close_after = wants_close(&head);

        if head
            .headers
            .get("expect")
            .map(|v| v.eq_ignore_ascii_case("100-continue"))
            .unwrap_or(false)
        {
            let _ = writer.write_all(b"HTTP/1.1 100 Continue\r\n\r\n");
            let _ = writer.flush();
        }

        // The idle timeout above bounds the wait for a request to begin. A body can
        // legitimately arrive in bursts, so give it a longer inactivity window rather
        // than dropping a slow upload the moment it pauses.
        let _ = stream.set_read_timeout(Some(Duration::from_secs(
            opts.read_timeout_seconds.max(30),
        )));
        let body = match read_body(&mut reader, &head, opts.max_body_bytes) {
            Ok(b) => b,
            Err(BodyError::TooLarge) => {
                write_response(&mut writer, &SimpleResponse::empty(413), true)?;
                return Ok(());
            }
            Err(BodyError::Bad) => {
                write_response(&mut writer, &SimpleResponse::empty(400), true)?;
                return Ok(());
            }
            Err(BodyError::Io(e)) => return Err(e),
        };

        let path = head.target.split('?').next().unwrap_or("").to_string();
        let routed = if path != opts.path {
            Routed::Response(SimpleResponse::empty(404))
        } else if head.method == "POST" {
            handle_post(&server, &head, &body, &remote, &opts)
        } else if head.method == "GET" || head.method == "DELETE" {
            Routed::Response(SimpleResponse::empty(405).with_header("Allow", "POST"))
        } else {
            Routed::Response(SimpleResponse::empty(404))
        };

        match routed {
            Routed::Response(r) => {
                served += 1;
                // Close after the last request we will serve, and say so, rather than
                // dropping a kept-alive socket the client still thinks it can reuse.
                let close = close_after || served >= MAX_KEEP_ALIVE_REQUESTS;
                write_response(&mut writer, &r, close)?;
                if close {
                    return Ok(());
                }
            }
            Routed::Sse { body, scope } => {
                serve_sse(&stream, &server, body, scope, opts.keep_alive_seconds);
                return Ok(()); // an SSE reply always ends the connection
            }
        }
    }
}

fn wants_close(head: &Head) -> bool {
    let conn = head
        .headers
        .get("connection")
        .map(|v| v.to_ascii_lowercase())
        .unwrap_or_default();
    if conn.split(',').any(|t| t.trim() == "close") {
        return true;
    }
    if head.version == "HTTP/1.0" {
        return !conn.split(',').any(|t| t.trim() == "keep-alive");
    }
    false
}

fn write_response<W: Write>(w: &mut W, resp: &SimpleResponse, close: bool) -> io::Result<()> {
    let mut head = format!("HTTP/1.1 {} {}\r\n", resp.status, reason(resp.status));
    if resp.json {
        head.push_str("Content-Type: application/json\r\n");
    }
    for (k, v) in &resp.headers {
        head.push_str(k);
        head.push_str(": ");
        head.push_str(v);
        head.push_str("\r\n");
    }
    head.push_str(&format!("Content-Length: {}\r\n", resp.body.len()));
    if close {
        head.push_str("Connection: close\r\n");
    }
    head.push_str("\r\n");
    w.write_all(head.as_bytes())?;
    if !resp.body.is_empty() {
        w.write_all(&resp.body)?;
    }
    w.flush()
}

fn reason(status: u16) -> &'static str {
    match status {
        200 => "OK",
        202 => "Accepted",
        204 => "No Content",
        400 => "Bad Request",
        401 => "Unauthorized",
        403 => "Forbidden",
        404 => "Not Found",
        405 => "Method Not Allowed",
        413 => "Payload Too Large",
        431 => "Request Header Fields Too Large",
        500 => "Internal Server Error",
        _ => "OK",
    }
}

// ---------------------------------------------------------------------------
// Reading a request
// ---------------------------------------------------------------------------

const LINE_TOO_LONG: &str = "line too long";

fn limit_error() -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, LINE_TOO_LONG)
}

fn is_limit_error(e: &io::Error) -> bool {
    e.kind() == io::ErrorKind::InvalidData
}

/// Read one `\n`-terminated line, dropping a trailing `\r\n`.
///
/// Returns `Ok(None)` at a clean end of input.
fn read_line_limited<R: BufRead>(r: &mut R, limit: usize) -> io::Result<Option<String>> {
    let mut buf: Vec<u8> = Vec::new();
    loop {
        let available = match r.fill_buf() {
            Ok(b) => b,
            Err(ref e) if e.kind() == io::ErrorKind::Interrupted => continue,
            Err(e) => return Err(e),
        };
        if available.is_empty() {
            if buf.is_empty() {
                return Ok(None);
            }
            break; // end of input part way through a line
        }
        match available.iter().position(|&b| b == b'\n') {
            Some(i) => {
                if buf.len() + i + 1 > limit {
                    return Err(limit_error());
                }
                buf.extend_from_slice(&available[..i]);
                r.consume(i + 1);
                break;
            }
            None => {
                let n = available.len();
                if buf.len() + n > limit {
                    return Err(limit_error());
                }
                buf.extend_from_slice(available);
                r.consume(n);
            }
        }
    }
    if buf.last() == Some(&b'\r') {
        buf.pop();
    }
    Ok(Some(String::from_utf8_lossy(&buf).into_owned()))
}

fn read_line_strict<R: BufRead>(r: &mut R, limit: usize) -> io::Result<String> {
    match read_line_limited(r, limit)? {
        Some(l) => Ok(l),
        None => Err(io::Error::new(
            io::ErrorKind::UnexpectedEof,
            "unexpected eof",
        )),
    }
}

fn read_head<R: BufRead>(r: &mut R, max_header_bytes: usize) -> HeadOutcome {
    let mut total = 0usize;

    // Tolerate a stray blank line before the request line.
    let request_line = loop {
        match read_line_limited(r, max_header_bytes) {
            Ok(Some(l)) => {
                total += l.len() + 2;
                if total > max_header_bytes {
                    return HeadOutcome::TooLarge;
                }
                if l.is_empty() {
                    continue;
                }
                break l;
            }
            Ok(None) => return HeadOutcome::Eof,
            Err(ref e) if is_limit_error(e) => return HeadOutcome::TooLarge,
            Err(_) => return HeadOutcome::Eof,
        }
    };

    let mut parts = request_line.split_whitespace();
    let method = parts.next().unwrap_or("").to_ascii_uppercase();
    let target = parts.next().unwrap_or("").to_string();
    let version = parts.next().unwrap_or("HTTP/1.1").to_string();
    if method.is_empty() || target.is_empty() {
        return HeadOutcome::Malformed;
    }

    let mut headers: HashMap<String, String> = HashMap::new();
    let mut count = 0usize;
    loop {
        match read_line_limited(r, max_header_bytes) {
            Ok(Some(l)) => {
                total += l.len() + 2;
                if total > max_header_bytes {
                    return HeadOutcome::TooLarge;
                }
                if l.is_empty() {
                    break;
                }
                if let Some(idx) = l.find(':') {
                    let name = l[..idx].trim().to_ascii_lowercase();
                    let value = l[idx + 1..].trim().to_string();
                    if !name.is_empty() {
                        count += 1;
                        if count > MAX_HEADERS {
                            return HeadOutcome::TooLarge;
                        }
                        // Keep the first occurrence of a repeated header.
                        headers.entry(name).or_insert(value);
                    }
                }
            }
            Ok(None) => return HeadOutcome::Eof,
            Err(ref e) if is_limit_error(e) => return HeadOutcome::TooLarge,
            Err(_) => return HeadOutcome::Eof,
        }
    }

    HeadOutcome::Request(Head {
        method,
        target,
        version,
        headers,
    })
}

fn read_body<R: BufRead>(
    r: &mut R,
    head: &Head,
    max_body_bytes: usize,
) -> Result<Vec<u8>, BodyError> {
    let chunked = head
        .headers
        .get("transfer-encoding")
        .map(|v| v.to_ascii_lowercase().contains("chunked"))
        .unwrap_or(false);

    if chunked {
        return read_chunked(r, max_body_bytes);
    }

    let len = match head.headers.get("content-length") {
        Some(v) => match v.trim().parse::<usize>() {
            Ok(n) => n,
            Err(_) => return Err(BodyError::Bad),
        },
        None => return Ok(Vec::new()),
    };
    if len > max_body_bytes {
        return Err(BodyError::TooLarge);
    }
    let mut buf = vec![0u8; len];
    if len > 0 {
        r.read_exact(&mut buf).map_err(BodyError::Io)?;
    }
    Ok(buf)
}

fn read_chunked<R: BufRead>(r: &mut R, max_body_bytes: usize) -> Result<Vec<u8>, BodyError> {
    let mut out: Vec<u8> = Vec::new();
    loop {
        let line = read_line_strict(r, 1024).map_err(BodyError::Io)?;
        let size_text = line.split(';').next().unwrap_or("").trim().to_string();
        let size = match usize::from_str_radix(&size_text, 16) {
            Ok(n) => n,
            Err(_) => return Err(BodyError::Bad),
        };
        if size == 0 {
            // Consume any trailers up to the final blank line.
            loop {
                match read_line_limited(r, 8192) {
                    Ok(Some(l)) if !l.is_empty() => continue,
                    _ => break,
                }
            }
            break;
        }
        // `size` comes straight off the wire, so `out.len() + size` could overflow and
        // wrap past the limit; compare against the remaining budget instead.
        if size > max_body_bytes.saturating_sub(out.len()) {
            return Err(BodyError::TooLarge);
        }
        let mut buf = vec![0u8; size];
        r.read_exact(&mut buf).map_err(BodyError::Io)?;
        out.extend_from_slice(&buf);
        let _ = read_line_limited(r, 8).map_err(BodyError::Io)?;
    }
    Ok(out)
}

// ---------------------------------------------------------------------------
// Routing a POST
// ---------------------------------------------------------------------------

fn handle_post(
    server: &Arc<Server>,
    head: &Head,
    body: &[u8],
    remote_addr: &str,
    opts: &HttpOptions,
) -> Routed {
    let origin = head.headers.get("origin").cloned().unwrap_or_default();
    if !origin.is_empty() && !origin_allowed(opts, &origin) {
        return Routed::Response(SimpleResponse::json(
            403,
            error_body(ErrorCode::InvalidRequest, "Origin not allowed", None),
        ));
    }

    let info = HttpRequest {
        remote_addr: remote_addr.to_string(),
        headers: head.headers.clone(),
    };

    let mut scope = RequestScope::default();
    if let Some(authorize) = &opts.authorize {
        match authorize(&info) {
            AuthDecision::Allow { principal } => scope.principal = principal,
            AuthDecision::Deny { challenge } => {
                let mut r = SimpleResponse::empty(401);
                if !challenge.is_empty() {
                    r = r.with_header("WWW-Authenticate", &challenge);
                }
                return Routed::Response(r);
            }
        }
    }

    let body = String::from_utf8_lossy(body).into_owned();

    if !json::within_depth(&body, json::MAX_DEPTH) {
        return Routed::Response(SimpleResponse::json(
            400,
            error_body(
                ErrorCode::InvalidRequest,
                "Message is nested too deeply",
                None,
            ),
        ));
    }

    let msg: Value = match serde_json::from_str(&body) {
        Ok(v) => v,
        Err(_) => {
            return Routed::Response(SimpleResponse::json(
                400,
                error_body(ErrorCode::ParseError, "Parse error", None),
            ))
        }
    };
    if !msg.is_object() {
        return Routed::Response(SimpleResponse::json(
            400,
            error_body(
                ErrorCode::InvalidRequest,
                "Body must be a single JSON-RPC message",
                None,
            ),
        ));
    }

    let id = args::get(&msg, FLD_ID);
    if let Err(err) = check_headers(&msg, &info) {
        return Routed::Response(SimpleResponse::json(
            400,
            error_body(ErrorCode::HeaderMismatch, &err, id),
        ));
    }

    // A notification is acknowledged with 202; its reply, if any, is discarded.
    if id.is_none() {
        let _ = server.handle_message(&body, Some(&scope));
        return Routed::Response(SimpleResponse::empty(202));
    }

    let is_call = args::str(&msg, FLD_METHOD, "") == MTD_TOOLS_CALL;
    let accepts_sse = head
        .headers
        .get("accept")
        .map(|a| a.contains("text/event-stream"))
        .unwrap_or(false);

    if is_call && accepts_sse {
        return Routed::Sse { body, scope };
    }

    let out = server.handle_message(&body, Some(&scope));
    if out.is_empty() {
        return Routed::Response(SimpleResponse::empty(202));
    }
    Routed::Response(SimpleResponse::json(status_for(&out), out))
}

// ---------------------------------------------------------------------------
// Server-sent events
// ---------------------------------------------------------------------------

struct StreamState {
    events: VecDeque<String>,
    done: bool,
}

struct Stream {
    state: Mutex<StreamState>,
    cv: Condvar,
    cancel: Arc<AtomicBool>,
}

impl Stream {
    fn new() -> Self {
        Stream {
            state: Mutex::new(StreamState {
                events: VecDeque::new(),
                done: false,
            }),
            cv: Condvar::new(),
            cancel: Arc::new(AtomicBool::new(false)),
        }
    }

    fn lock(&self) -> MutexGuard<'_, StreamState> {
        lock(&self.state)
    }

    fn pop(&self) -> Option<String> {
        self.lock().events.pop_front()
    }

    /// Queue one event, or mark the stream finished.
    fn push(&self, line: &str, last: bool) {
        {
            let mut g = self.lock();
            if !last && g.events.len() >= MAX_QUEUED_EVENTS {
                return;
            }
            if !line.is_empty() {
                g.events.push_back(line.to_string());
            }
            if last {
                g.done = true;
            }
        }
        self.cv.notify_one();
    }
}

fn write_chunk<W: Write>(w: &mut W, data: &[u8]) -> io::Result<()> {
    w.write_all(format!("{:x}\r\n", data.len()).as_bytes())?;
    w.write_all(data)?;
    w.write_all(b"\r\n")
}

fn write_last_chunk<W: Write>(w: &mut W) -> io::Result<()> {
    w.write_all(b"0\r\n\r\n")
}

/// Whether the peer is still connected.
///
/// A zero-length read says nothing, so this peeks one byte with a very short read
/// timeout: `Ok(0)` means the peer closed, and a timeout means the connection is
/// idle but alive.
fn is_writable(stream: &TcpStream) -> bool {
    let prev = stream.read_timeout().ok().flatten();
    if stream
        .set_read_timeout(Some(Duration::from_millis(1)))
        .is_err()
    {
        return true;
    }
    let mut buf = [0u8; 1];
    let closed = match stream.peek(&mut buf) {
        Ok(0) => true,
        Err(ref e) if e.kind() == io::ErrorKind::ConnectionReset => true,
        _ => false,
    };
    let _ = stream.set_read_timeout(prev);
    !closed
}

/// Drain queued events to the socket, keeping the stream warm and noticing a
/// client that has gone away.
fn pump<W: Write>(
    st: &Stream,
    w: &mut W,
    sock: &TcpStream,
    keep_alive_seconds: u64,
) -> io::Result<()> {
    let mut idle: u64 = 0;
    loop {
        while let Some(ev) = st.pop() {
            let data = format!("data: {ev}\n\n");
            write_chunk(w, data.as_bytes())?;
            w.flush()?;
            idle = 0;
        }

        // Finish only once the worker is done AND the queue is drained. The final
        // reply is pushed together with `done`, so testing `done` alone could skip an
        // event enqueued in the gap between the drain above and this check.
        {
            let guard = st.lock();
            if guard.done {
                if guard.events.is_empty() {
                    drop(guard);
                    write_last_chunk(w)?;
                    w.flush()?;
                    return Ok(());
                }
                drop(guard);
                continue;
            }
        }

        {
            let guard = st.lock();
            let (guard, _) = st
                .cv
                .wait_timeout(guard, Duration::from_secs(1))
                .unwrap_or_else(|e| e.into_inner());
            let empty = guard.events.is_empty();
            let done = guard.done;
            drop(guard);
            if !empty || done {
                continue;
            }
        }

        if !is_writable(sock) {
            return Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "client closed the stream",
            ));
        }
        if keep_alive_seconds > 0 {
            idle += 1;
            if idle >= keep_alive_seconds {
                idle = 0;
                write_chunk(w, b": keep-alive\n\n")?;
                w.flush()?;
            }
        }
    }
}

/// Stream one reply, running the request on a separate thread.
///
/// If the client closes the connection mid-call, the stream's cancel flag is set,
/// which is what makes [`crate::RequestContext::cancelled`] report true inside the
/// running tool.
fn serve_sse(
    sock: &TcpStream,
    server: &Arc<Server>,
    body: String,
    mut scope: RequestScope,
    keep_alive_seconds: u64,
) {
    let st = Arc::new(Stream::new());
    scope.cancel = Some(Arc::clone(&st.cancel));
    {
        let st = Arc::clone(&st);
        scope.notify = Some(Arc::new(move |line: String| st.push(&line, false)));
    }

    let head = "HTTP/1.1 200 OK\r\n\
                Content-Type: text/event-stream\r\n\
                Cache-Control: no-cache\r\n\
                X-Accel-Buffering: no\r\n\
                Transfer-Encoding: chunked\r\n\
                Connection: close\r\n\r\n";
    let mut w = sock;
    if w.write_all(head.as_bytes()).is_err() || w.flush().is_err() {
        st.cancel.store(true, Ordering::SeqCst);
        return;
    }

    let worker = {
        let server = Arc::clone(server);
        let st = Arc::clone(&st);
        thread::spawn(move || {
            let out = catch_unwind(AssertUnwindSafe(|| {
                server.handle_message(&body, Some(&scope))
            }))
            .unwrap_or_else(|_| error_body(ErrorCode::InternalError, "Internal error", None));
            st.push(&out, true);
        })
    };

    let pumped = pump(&st, &mut w, sock, keep_alive_seconds);
    if pumped.is_err() {
        st.cancel.store(true, Ordering::SeqCst);
    }
    let _ = worker.join();
    let _ = w.flush();
    let _ = sock.shutdown(Shutdown::Both);
}

// ---------------------------------------------------------------------------
// Small helpers
// ---------------------------------------------------------------------------

fn origin_allowed(opts: &HttpOptions, origin: &str) -> bool {
    if opts.allowed_origins.is_empty() {
        return is_local_origin(origin);
    }
    opts.allowed_origins.iter().any(|o| o == origin)
}

/// Whether an `Origin` value names a local address.
///
/// Accepts `http` and `https` with the host `localhost`, `127.0.0.1` or `[::1]`,
/// with or without a port.
fn is_local_origin(origin: &str) -> bool {
    let rest = if let Some(r) = origin.strip_prefix("http://") {
        r
    } else if let Some(r) = origin.strip_prefix("https://") {
        r
    } else {
        return false;
    };
    if let Some(r) = rest.strip_prefix("[::1]") {
        return r.is_empty() || r.starts_with(':');
    }
    let host = rest.split(':').next().unwrap_or("");
    host == "localhost" || host == "127.0.0.1"
}

/// The HTTP status for a JSON-RPC reply, based on its error code.
///
/// Errors that are the caller's fault become 4xx; everything else is a 200.
fn status_for(response: &str) -> u16 {
    let d: Value = match serde_json::from_str(response) {
        Ok(v) => v,
        Err(_) => return 200,
    };
    if !d.is_object() {
        return 200;
    }
    let code = match args::get(&d, FLD_ERROR)
        .and_then(|e| args::get(e, FLD_CODE))
        .and_then(|c| c.as_i64())
    {
        Some(c) => c as i32,
        None => return 200,
    };
    if code == ErrorCode::MethodNotFound.code() {
        return 404;
    }
    if code == ErrorCode::ParseError.code()
        || code == ErrorCode::InvalidRequest.code()
        || code == ErrorCode::HeaderMismatch.code()
        || code == ErrorCode::MissingRequiredClientCapability.code()
        || code == ErrorCode::UnsupportedProtocolVersion.code()
    {
        return 400;
    }
    200
}

/// A JSON-RPC error envelope, with an optional envelope-level id.
fn error_body(code: ErrorCode, message: &str, id: Option<&Value>) -> String {
    let mut env = json::obj();
    json::add_str(&mut env, FLD_JSONRPC, K_JSON_RPC_VERSION);
    if let Some(id) = id {
        json::add_val(&mut env, FLD_ID, id.clone());
    }
    let mut err = json::obj();
    json::add_int(&mut err, FLD_CODE, code.code() as i64);
    json::add_str(&mut err, FLD_MESSAGE, message);
    json::add_val(&mut env, FLD_ERROR, err);
    json::serialize(&env)
}

/// Describe a header that is missing or disagrees with the body.
fn mismatch(header: &str, got: &str, want: &str) -> Option<String> {
    if got.is_empty() {
        return Some(format!("Header mismatch: missing {header} header"));
    }
    if got != want {
        return Some(format!(
            "Header mismatch: {header} header value '{got}' does not match body value '{want}'"
        ));
    }
    None
}

/// Check the per-call headers of a stateless request against its body.
fn check_headers(msg: &Value, req: &HttpRequest) -> Result<(), String> {
    let params = args::get(msg, FLD_PARAMS);
    let pv = params
        .and_then(|p| args::get(p, FLD_META))
        .and_then(|m| args::get(m, META_PROTOCOL_VERSION));
    let pv = match pv {
        Some(v) if v.is_string() => v.as_str().unwrap_or_default().to_string(),
        _ => return Ok(()),
    };

    let method = args::str(msg, FLD_METHOD, "");
    if let Some(err) = mismatch(
        "MCP-Protocol-Version",
        &req.header("mcp-protocol-version"),
        &pv,
    ) {
        return Err(err);
    }
    if let Some(err) = mismatch("Mcp-Method", &req.header("mcp-method"), &method) {
        return Err(err);
    }

    let name_field = if method == MTD_TOOLS_CALL || method == MTD_PROMPTS_GET {
        Some(FLD_NAME)
    } else if method == MTD_RESOURCES_READ {
        Some(FLD_URI)
    } else {
        None
    };
    let name_field = match name_field {
        Some(f) => f,
        None => return Ok(()),
    };

    let name = match header_text(&req.header("mcp-name")) {
        Some(n) => n,
        None => return Err("Header mismatch: Mcp-Name header is not valid base64".to_string()),
    };
    let want = params
        .map(|p| args::str(p, name_field, ""))
        .unwrap_or_default();
    match mismatch("Mcp-Name", &name, &want) {
        Some(err) => Err(err),
        None => Ok(()),
    }
}

/// Decode a header value wrapped in the `=?base64?` and `?=` markers.
fn header_text(raw: &str) -> Option<String> {
    const PRE: &str = "=?base64?";
    const SUF: &str = "?=";
    if raw.len() >= PRE.len() + SUF.len() && raw.starts_with(PRE) && raw.ends_with(SUF) {
        return decode_base64(&raw[PRE.len()..raw.len() - SUF.len()]);
    }
    Some(raw.to_string())
}

/// Decode standard base64. Returns `None` for characters outside the alphabet.
fn decode_base64(input: &str) -> Option<String> {
    const CHARS: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let bytes = input.as_bytes();
    let mut out: Vec<u8> = Vec::new();
    let mut val: i32 = 0;
    let mut bits: i32 = -8;
    let mut i = 0usize;
    while i < bytes.len() && bytes[i] != b'=' {
        let p = CHARS.iter().position(|&c| c == bytes[i])?;
        val = (val << 6) + p as i32;
        bits += 6;
        if bits >= 0 {
            out.push(((val >> bits) & 0xFF) as u8);
            bits -= 8;
        }
        i += 1;
    }
    while i < bytes.len() {
        if bytes[i] != b'=' {
            return None;
        }
        i += 1;
    }
    Some(String::from_utf8_lossy(&out).into_owned())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn local_origin_rules() {
        assert!(is_local_origin("http://localhost"));
        assert!(is_local_origin("http://localhost:3000"));
        assert!(is_local_origin("https://127.0.0.1:8080"));
        assert!(is_local_origin("http://[::1]"));
        assert!(is_local_origin("http://[::1]:9000"));
        assert!(!is_local_origin("http://[::1]x"));
        assert!(!is_local_origin("http://example.com"));
        assert!(!is_local_origin("ftp://localhost"));
        assert!(!is_local_origin("localhost"));
    }

    #[test]
    fn origin_allow_list_replaces_the_default() {
        let mut opts = HttpOptions::default();
        assert!(origin_allowed(&opts, "http://localhost"));
        assert!(!origin_allowed(&opts, "https://example.com"));
        opts.allowed_origins = vec!["https://example.com".to_string()];
        assert!(origin_allowed(&opts, "https://example.com"));
        assert!(!origin_allowed(&opts, "http://localhost"));
    }

    #[test]
    fn status_mapping() {
        let e = |code: ErrorCode| {
            json::serialize(
                &json!({"jsonrpc":"2.0","id":1,"error":{"code":code.code(),"message":"x"}}),
            )
        };
        assert_eq!(status_for(&e(ErrorCode::MethodNotFound)), 404);
        for c in [
            ErrorCode::ParseError,
            ErrorCode::InvalidRequest,
            ErrorCode::HeaderMismatch,
            ErrorCode::MissingRequiredClientCapability,
            ErrorCode::UnsupportedProtocolVersion,
        ] {
            assert_eq!(status_for(&e(c)), 400, "{c:?}");
        }
        assert_eq!(status_for(&e(ErrorCode::InternalError)), 200);
        assert_eq!(status_for(&e(ErrorCode::InvalidParams)), 200);
        assert_eq!(status_for("not json"), 200);
        assert_eq!(status_for(r#"{"result":{}}"#), 200);
    }

    #[test]
    fn base64_header_text() {
        assert_eq!(header_text("plain").as_deref(), Some("plain"));
        assert_eq!(header_text("=?base64?aGk=?=").as_deref(), Some("hi"));
        assert_eq!(header_text("=?base64?!!!=?="), None);
        // a value that only looks like the encoding is passed through
        assert_eq!(
            header_text("=?base64?aGk=?=x"),
            Some("=?base64?aGk=?=x".to_string())
        );
    }

    #[test]
    fn header_checks() {
        let mut req = HttpRequest::default();
        // No per-call revision in the body means there is nothing to check.
        assert!(check_headers(&json!({"method":"tools/call"}), &req).is_ok());

        let msg = json!({"method":"tools/call","params":{"name":"crunch","_meta":{"io.modelcontextprotocol/protocolVersion":"2026-07-28"}}});

        let err = check_headers(&msg, &req).unwrap_err();
        assert!(err.contains("missing MCP-Protocol-Version header"), "{err}");

        req.headers
            .insert("mcp-protocol-version".into(), "2026-07-28".into());
        let err = check_headers(&msg, &req).unwrap_err();
        assert!(err.contains("missing Mcp-Method header"), "{err}");

        req.headers.insert("mcp-method".into(), "tools/call".into());
        let err = check_headers(&msg, &req).unwrap_err();
        assert!(err.contains("missing Mcp-Name header"), "{err}");

        req.headers.insert("mcp-name".into(), "crunch".into());
        assert!(check_headers(&msg, &req).is_ok());

        req.headers.insert("mcp-name".into(), "other".into());
        let err = check_headers(&msg, &req).unwrap_err();
        assert!(err.contains("does not match body value 'crunch'"), "{err}");

        // the base64 form is accepted
        req.headers
            .insert("mcp-name".into(), "=?base64?Y3J1bmNo?=".into());
        assert!(check_headers(&msg, &req).is_ok());

        // tools/list has no name field, so Mcp-Name is not required
        req.headers.clear();
        req.headers
            .insert("mcp-protocol-version".into(), "2026-07-28".into());
        req.headers.insert("mcp-method".into(), "tools/list".into());
        assert!(check_headers(
            &json!({"method":"tools/list","params":{"_meta":{"io.modelcontextprotocol/protocolVersion":"2026-07-28"}}}),
            &req
        )
        .is_ok());
    }

    #[test]
    fn request_line_and_headers_parse() {
        let raw = "POST /mcp HTTP/1.1\r\nHost: x\r\nOrigin: http://localhost\r\n\r\n";
        let mut r = BufReader::new(std::io::Cursor::new(raw.as_bytes().to_vec()));
        match read_head(&mut r, 8192) {
            HeadOutcome::Request(h) => {
                assert_eq!(h.method, "POST");
                assert_eq!(h.target, "/mcp");
                assert_eq!(h.version, "HTTP/1.1");
                assert_eq!(h.headers.get("origin").unwrap(), "http://localhost");
                assert!(!wants_close(&h));
            }
            _ => panic!("expected a request"),
        }
    }

    #[test]
    fn keep_alive_defaults() {
        let h = |version: &str, conn: Option<&str>| {
            let mut headers = HashMap::new();
            if let Some(c) = conn {
                headers.insert("connection".to_string(), c.to_string());
            }
            Head {
                method: "POST".into(),
                target: "/mcp".into(),
                version: version.into(),
                headers,
            }
        };
        assert!(!wants_close(&h("HTTP/1.1", None)));
        assert!(wants_close(&h("HTTP/1.1", Some("close"))));
        assert!(!wants_close(&h("HTTP/1.1", Some("keep-alive"))));
        assert!(wants_close(&h("HTTP/1.0", None)));
        assert!(!wants_close(&h("HTTP/1.0", Some("keep-alive"))));
    }

    #[test]
    fn oversized_header_block_is_refused() {
        let mut raw = String::from("POST /mcp HTTP/1.1\r\n");
        for i in 0..200 {
            raw.push_str(&format!("X-Pad-{i}: {}\r\n", "p".repeat(64)));
        }
        raw.push_str("\r\n");
        let mut r = BufReader::new(std::io::Cursor::new(raw.into_bytes()));
        match read_head(&mut r, 64 * 1024) {
            HeadOutcome::TooLarge => {}
            HeadOutcome::Request(_) => panic!("expected TooLarge, got a request"),
            _ => panic!("expected TooLarge"),
        }
    }

    #[test]
    fn chunked_body_is_decoded() {
        let raw = "4\r\nWiki\r\n5\r\npedia\r\n0\r\n\r\n";
        let mut r = BufReader::new(std::io::Cursor::new(raw.as_bytes().to_vec()));
        let head = Head {
            method: "POST".into(),
            target: "/mcp".into(),
            version: "HTTP/1.1".into(),
            headers: HashMap::from([("transfer-encoding".to_string(), "chunked".to_string())]),
        };
        let body = read_body(&mut r, &head, 1024).ok().unwrap();
        assert_eq!(String::from_utf8_lossy(&body), "Wikipedia");

        let mut r = BufReader::new(std::io::Cursor::new(raw.as_bytes().to_vec()));
        match read_body(&mut r, &head, 4) {
            Err(BodyError::TooLarge) => {}
            _ => panic!("expected TooLarge"),
        }
    }

    #[test]
    fn chunked_size_cannot_overflow_the_limit() {
        // A first small chunk leaves `out` non-empty, then a chunk size near
        // usize::MAX. `out.len() + size` used to wrap past the limit and reach a huge
        // allocation; the size must be rejected before anything is allocated.
        let huge = format!("{:x}", usize::MAX - 1);
        let raw = format!("2\r\nAB\r\n{huge}\r\n");
        let head = Head {
            method: "POST".into(),
            target: "/mcp".into(),
            version: "HTTP/1.1".into(),
            headers: HashMap::from([("transfer-encoding".to_string(), "chunked".to_string())]),
        };
        let mut r = BufReader::new(std::io::Cursor::new(raw.into_bytes()));
        match read_body(&mut r, &head, 1024) {
            Err(BodyError::TooLarge) => {}
            _ => panic!("expected TooLarge"),
        }
    }

    #[test]
    fn content_length_body_is_read_and_capped() {
        let raw = "hello";
        let head = Head {
            method: "POST".into(),
            target: "/mcp".into(),
            version: "HTTP/1.1".into(),
            headers: HashMap::from([("content-length".to_string(), "5".to_string())]),
        };
        let mut r = BufReader::new(std::io::Cursor::new(raw.as_bytes().to_vec()));
        assert_eq!(read_body(&mut r, &head, 16).ok().unwrap(), b"hello");

        let mut r = BufReader::new(std::io::Cursor::new(raw.as_bytes().to_vec()));
        match read_body(&mut r, &head, 4) {
            Err(BodyError::TooLarge) => {}
            _ => panic!("expected TooLarge"),
        }

        let bad = Head {
            headers: HashMap::from([("content-length".to_string(), "nope".to_string())]),
            ..head
        };
        let mut r = BufReader::new(std::io::Cursor::new(Vec::new()));
        match read_body(&mut r, &bad, 16) {
            Err(BodyError::Bad) => {}
            _ => panic!("expected Bad"),
        }
    }
}
