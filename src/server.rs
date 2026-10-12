//! The server: request dispatch, cancellation, notifications and tool results.
//!
//! [`Server::run`] reads messages on the calling thread and dispatches requests to
//! a worker pool. The handshake and cancellation notifications are handled inline on
//! the read thread so that `initialize` always completes before later requests are
//! dispatched and a cancellation is not delayed by a busy worker. All outbound
//! writes go through one lock, so replies and notifications from different workers
//! never interleave.
//!
//! Handlers registered with [`Server::add_tool_ctx`] receive a [`RequestContext`],
//! which reports cancellation and can emit progress, log lines and notifications.

use std::any::Any;
use std::collections::HashMap;
use std::io;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};

use serde_json::Value;

use crate::constants::*;
use crate::json::{self, args};
use crate::pool::{default_worker_count, ThreadPool};
use crate::transport::{LineWrite, StdioTransport, Transport};
use crate::types::*;

/// Lock a mutex, recovering the data if another thread panicked while holding it.
///
/// A panicking handler must not make the server permanently unusable.
fn lock<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    m.lock().unwrap_or_else(|e| e.into_inner())
}

// ---------------------------------------------------------------------------
// RequestScope
// ---------------------------------------------------------------------------

/// Per-request hooks supplied by a transport.
///
/// The stdio path passes `None` and the server resolves the cancellation flag from
/// its own table. The HTTP path supplies both halves, which is what lets a client
/// hang-up cancel the request.
#[derive(Default)]
pub struct RequestScope {
    /// Where this request's notifications go. `None` means the transport itself.
    pub notify: Option<Arc<dyn Fn(String) + Send + Sync>>,
    /// Set to true when the client cancels this request.
    pub cancel: Option<Arc<AtomicBool>>,
    /// The authenticated principal, from [`crate::HttpOptions::authorize`].
    pub principal: String,
}

// ---------------------------------------------------------------------------
// RequestContext
// ---------------------------------------------------------------------------

/// The per-request context given to a handler registered with
/// [`Server::add_tool_ctx`].
pub struct RequestContext<'a> {
    /// The JSON-RPC method being handled.
    pub method: String,
    /// Whether the request is stateless (`2026-07-28`).
    pub stateless: bool,
    /// The protocol revision the call was made under.
    pub protocol_version: String,
    /// Whether the client attached a `progressToken`.
    pub has_progress_token: bool,

    server: &'a Server,
    sink: Option<Arc<dyn Fn(String) + Send + Sync>>,
    principal: String,
    progress_token: Option<Value>,
    input_responses: Option<Value>,
    request_state: String,
    elicit_form: bool,
    elicit_url: bool,
    cancel: Option<Arc<AtomicBool>>,
}

impl<'a> RequestContext<'a> {
    /// Report progress for this request.
    ///
    /// Does nothing unless the client attached a `progressToken`. A `total` below
    /// zero and an empty `message` are omitted from the notification.
    pub fn progress(&self, progress: f64, total: Option<f64>, message: Option<&str>) {
        if !self.has_progress_token {
            return;
        }
        let token = match &self.progress_token {
            Some(t) => t,
            None => return,
        };
        let mut d = json::obj();
        json::add_val(&mut d, FLD_PROGRESS_TOKEN, token.clone());
        json::add_val(&mut d, FLD_PROGRESS, number_value(progress));
        if let Some(t) = total {
            if t >= 0.0 {
                json::add_val(&mut d, FLD_TOTAL, number_value(t));
            }
        }
        if let Some(m) = message {
            if !m.is_empty() {
                json::add_str(&mut d, FLD_MESSAGE, m);
            }
        }
        self.send(NTF_PROGRESS, Some(&d));
    }

    /// Send a log line to the client, if its level passes the client's threshold.
    pub fn log(&self, level: &str, message: &str) {
        if !self.server.log_enabled(level) {
            return;
        }
        let mut d = json::obj();
        json::add_str(&mut d, FLD_LEVEL, level);
        json::add_str(&mut d, FLD_DATA, message);
        self.send(NTF_MESSAGE, Some(&d));
    }

    /// Send an arbitrary notification for this request.
    ///
    /// `raw_json_params` is parsed as JSON; an empty string means no `params`.
    pub fn notify(&self, method: &str, raw_json_params: &str) {
        let params = if raw_json_params.is_empty() {
            None
        } else {
            json::parse_into(raw_json_params)
        };
        self.send(method, params.as_ref());
    }

    /// Whether the client has cancelled this request.
    ///
    /// Cancellation is cooperative: a handler should poll this and return early.
    /// The server never interrupts a running handler.
    pub fn cancelled(&self) -> bool {
        match &self.cancel {
            Some(c) => c.load(Ordering::SeqCst),
            None => false,
        }
    }

    /// Whether the client supports form-mode elicitation.
    pub fn supports_elicit_form(&self) -> bool {
        self.elicit_form
    }

    /// Whether the client supports url-mode elicitation.
    pub fn supports_elicit_url(&self) -> bool {
        self.elicit_url
    }

    /// Whether the client answered the elicitation with this key.
    pub fn has_answer(&self, key: &str) -> bool {
        self.answer(key).is_some()
    }

    /// The client's action for this elicitation key.
    ///
    /// One of `"accept"`, `"decline"`, `"cancel"`, or an empty string when the key
    /// was not answered.
    pub fn answer_action(&self, key: &str) -> String {
        match self.answer(key) {
            Some(r) => args::str(r, FLD_ACTION, ""),
            None => String::new(),
        }
    }

    /// Whether the client accepted the elicitation with this key.
    pub fn accepted(&self, key: &str) -> bool {
        self.answer_action(key) == VAL_ACTION_ACCEPT
    }

    /// The form values the client submitted for this key, if any.
    pub fn answer_content(&self, key: &str) -> Option<&Value> {
        let c = self.answer(key).and_then(|r| args::get(r, FLD_CONTENT))?;
        if c.is_object() {
            Some(c)
        } else {
            None
        }
    }

    fn answer(&self, key: &str) -> Option<&Value> {
        let r = args::get(self.input_responses.as_ref()?, key)?;
        if r.is_object() {
            Some(r)
        } else {
            None
        }
    }

    /// The opaque state the client echoed back.
    ///
    /// This is client-supplied: the client can change it between the two runs, so
    /// anything that matters must be re-checked.
    pub fn request_state(&self) -> &str {
        &self.request_state
    }

    /// The authenticated principal for this request, or an empty string.
    pub fn principal(&self) -> &str {
        &self.principal
    }

    /// The server handling this request.
    pub fn server(&self) -> &'a Server {
        self.server
    }

    /// Route a notification through this request's sink if it has one, and through
    /// the transport otherwise.
    fn send(&self, notification: &str, params: Option<&Value>) {
        let line = Server::notification_line(notification, params);
        match &self.sink {
            Some(f) => f(line),
            None => self.server.send_line(&line),
        }
    }
}

/// Convert a float to a JSON number, or `null` when JSON cannot represent it.
fn number_value(d: f64) -> Value {
    match serde_json::Number::from_f64(d) {
        Some(n) => Value::Number(n),
        None => Value::Null,
    }
}

// ---------------------------------------------------------------------------
// Handlers
// ---------------------------------------------------------------------------

/// A plain tool handler. It cannot observe cancellation, because it has no
/// [`RequestContext`].
pub type ToolHandler = Box<dyn Fn(&Value) -> ToolResult + Send + Sync>;

/// A tool handler that receives a [`RequestContext`].
pub type ToolHandlerCtx =
    Box<dyn for<'a> Fn(&Value, &mut RequestContext<'a>) -> ToolResult + Send + Sync>;

struct ToolEntry {
    tool: Tool,
    handler: Option<ToolHandler>,
    handler_ctx: Option<ToolHandlerCtx>,
    ctx: bool,
}

// ---------------------------------------------------------------------------
// Server
// ---------------------------------------------------------------------------

/// An MCP server.
///
/// Register tools and configure the server before sharing it, then hand it to a
/// transport. [`Server::run`] and [`Server::run_stdio`] need an `Arc<Server>`,
/// because queued requests outlive the call.
pub struct Server {
    name: String,
    version: String,
    cache_ttl_ms: i64,
    cache_scope: String,
    initialized: AtomicBool,
    logging_level: Mutex<String>,
    instructions: String,
    writer: Mutex<Option<Box<dyn LineWrite>>>,
    tools: Vec<ToolEntry>,
    max_threads: usize,
    out_mutex: Mutex<()>,
    cancels: Mutex<HashMap<String, Arc<AtomicBool>>>,
    negotiated_version: Mutex<String>,
}

impl Server {
    /// Create a server with no tools.
    pub fn new(name: impl Into<String>, version: impl Into<String>) -> Self {
        Server {
            name: name.into(),
            version: version.into(),
            cache_ttl_ms: 0,
            cache_scope: "private".to_string(),
            initialized: AtomicBool::new(false),
            logging_level: Mutex::new(String::new()),
            instructions: String::new(),
            writer: Mutex::new(None),
            tools: Vec::new(),
            max_threads: 0,
            out_mutex: Mutex::new(()),
            cancels: Mutex::new(HashMap::new()),
            negotiated_version: Mutex::new(default_stateful_version().to_string()),
        }
    }

    /// The server name reported in `serverInfo`.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// The server version reported in `serverInfo`.
    pub fn version(&self) -> &str {
        &self.version
    }

    /// Whether the handshake has completed.
    pub fn initialized(&self) -> bool {
        self.initialized.load(Ordering::SeqCst)
    }

    /// Set the number of worker threads used by [`Server::run`].
    ///
    /// The default, 0, means one worker per hardware thread. Set it to 1 for serial
    /// execution.
    pub fn set_max_threads(&mut self, n: usize) {
        self.max_threads = n;
    }

    /// The configured worker count, where 0 means automatic.
    pub fn max_threads(&self) -> usize {
        self.max_threads
    }

    /// Set the text returned in the `initialize` result's `instructions` field.
    ///
    /// Clients typically surface this to the model as usage guidance for the
    /// session.
    pub fn set_instructions(&mut self, text: impl Into<String>) {
        self.instructions = text.into();
    }

    /// The configured instructions.
    pub fn instructions(&self) -> &str {
        &self.instructions
    }

    /// Set the cache hint emitted on stateless replies to cacheable methods.
    pub fn set_cache_hint(&mut self, ttl_ms: i64, scope: impl Into<String>) {
        self.cache_ttl_ms = ttl_ms;
        self.cache_scope = scope.into();
    }

    /// Register a tool with a plain handler.
    ///
    /// The handler does not receive a [`RequestContext`], so it cannot report
    /// progress or observe cancellation. Use [`Server::add_tool_ctx`] if it needs
    /// either.
    pub fn add_tool<F>(&mut self, tool: Tool, handler: F) -> &mut Self
    where
        F: Fn(&Value) -> ToolResult + Send + Sync + 'static,
    {
        self.tools.push(ToolEntry {
            tool,
            handler: Some(Box::new(handler)),
            handler_ctx: None,
            ctx: false,
        });
        self
    }

    /// Register a tool whose handler receives a [`RequestContext`].
    pub fn add_tool_ctx<F>(&mut self, tool: Tool, handler: F) -> &mut Self
    where
        F: for<'a> Fn(&Value, &mut RequestContext<'a>) -> ToolResult + Send + Sync + 'static,
    {
        self.tools.push(ToolEntry {
            tool,
            handler: None,
            handler_ctx: Some(Box::new(handler)),
            ctx: true,
        });
        self
    }

    /// Register a plain handler from an already boxed closure.
    pub fn add_tool_boxed(&mut self, tool: Tool, handler: ToolHandler) -> &mut Self {
        self.tools.push(ToolEntry {
            tool,
            handler: Some(handler),
            handler_ctx: None,
            ctx: false,
        });
        self
    }

    /// Register a context handler from an already boxed closure.
    pub fn add_tool_ctx_boxed(&mut self, tool: Tool, handler: ToolHandlerCtx) -> &mut Self {
        self.tools.push(ToolEntry {
            tool,
            handler: None,
            handler_ctx: Some(handler),
            ctx: true,
        });
        self
    }

    /// The registered tool with this name, if any.
    pub fn tool(&self, name: &str) -> Option<&Tool> {
        self.find_tool(name).map(|e| &e.tool)
    }

    /// The number of registered tools.
    pub fn tool_count(&self) -> usize {
        self.tools.len()
    }

    /// Send a server-wide notification.
    ///
    /// This reaches stdio clients only; there is no server-wide notification
    /// channel for HTTP clients.
    pub fn send_notification(&self, method: &str, raw_json_params: &str) {
        let params = if raw_json_params.is_empty() {
            None
        } else {
            json::parse_into(raw_json_params)
        };
        let line = Self::notification_line(method, params.as_ref());
        self.send_line(&line);
    }

    /// Send `notifications/tools/list_changed`.
    pub fn notify_tools_list_changed(&self) {
        self.send_notification(NTF_TOOLS_LIST_CHANGED, "{}");
    }

    /// Send a `notifications/message`.
    ///
    /// An empty `logger` omits the field. The line is dropped when the client's log
    /// level is higher than `level`.
    pub fn log_message(&self, level: &str, message: &str, logger: &str) {
        if !self.log_enabled(level) {
            return;
        }
        let mut d = json::obj();
        json::add_str(&mut d, FLD_LEVEL, level);
        if !logger.is_empty() {
            json::add_str(&mut d, FLD_LOGGER, logger);
        }
        json::add_str(&mut d, FLD_DATA, message);
        self.send_notification_doc(NTF_MESSAGE, &d);
    }

    /// Handle one raw line, returning the reply or an empty string.
    pub fn handle_line(&self, line: &str) -> String {
        self.handle(line, None)
    }

    /// Handle one raw line with a [`RequestScope`], returning the reply or an empty
    /// string.
    pub fn handle_message(&self, message: &str, scope: Option<&RequestScope>) -> String {
        self.handle(message, scope)
    }

    /// Serve over stdin and stdout until end of input.
    pub fn run_stdio(self: &Arc<Self>) -> io::Result<i32> {
        self.run(StdioTransport::new())
    }

    /// Read from `transport` on this thread and dispatch requests to workers.
    ///
    /// Returns once the transport reports end of input. Every queued request has
    /// finished and its reply has been written by the time this returns.
    pub fn run<T: Transport>(self: &Arc<Self>, mut transport: T) -> io::Result<i32> {
        *lock(&self.writer) = Some(Box::new(transport.clone()));
        let pool = ThreadPool::new(self.worker_count());

        let mut outcome = Ok(0);
        loop {
            match transport.read_line() {
                Ok(Some(line)) => {
                    let incoming = self.preview(&line);

                    // Cancellations and the handshake run inline, so the handshake
                    // completes before later requests are dispatched and a cancel
                    // never waits behind a busy worker.
                    if incoming.method == NTF_CANCELLED {
                        if !incoming.cancel_id.is_empty() {
                            self.mark_cancelled(&incoming.cancel_id);
                        }
                        continue;
                    }
                    if incoming.method == MTD_INITIALIZE || incoming.method == NTF_INITIALIZED {
                        self.handle_and_send(&line);
                        continue;
                    }

                    if !incoming.id.is_empty() {
                        self.register_cancel(&incoming.id);
                    }
                    let srv = Arc::clone(self);
                    let req_id = incoming.id;
                    pool.submit(move || srv.run_request(&line, &req_id));
                }
                Ok(None) => break,
                Err(e) if e.kind() == io::ErrorKind::Interrupted => continue,
                Err(e) => {
                    outcome = Err(e);
                    break;
                }
            }
        }

        // Join the workers before detaching the writer, so no reply is lost.
        drop(pool);
        *lock(&self.writer) = None;
        outcome
    }

    /// Write one line to the transport.
    ///
    /// Every outbound byte goes through this method, under a single lock, so replies
    /// and notifications from different workers cannot interleave.
    pub fn send_line(&self, line: &str) {
        let _guard = lock(&self.out_mutex);
        if let Some(w) = lock(&self.writer).as_mut() {
            let _ = w.write_line(line);
        }
    }

    // -- internals ----------------------------------------------------------

    fn worker_count(&self) -> usize {
        if self.max_threads > 0 {
            self.max_threads
        } else {
            default_worker_count()
        }
    }

    /// Read a line's method and id without dispatching it.
    fn preview(&self, line: &str) -> Incoming {
        let mut incoming = Incoming::default();
        let text = strip_bom_and_trim(line);
        if text.is_empty() || !json::within_depth(text, json::MAX_DEPTH) {
            return incoming;
        }
        let d: Value = match serde_json::from_str(text) {
            Ok(v) => v,
            Err(_) => return incoming,
        };
        if !d.is_object() {
            return incoming;
        }
        if let Some(m) = args::get(&d, FLD_METHOD) {
            if let Some(s) = m.as_str() {
                incoming.method = s.to_string();
            }
        }
        if let Some(id) = args::get(&d, FLD_ID) {
            if !id.is_null() {
                incoming.id = json::serialize(id);
            }
        }
        if incoming.method == NTF_CANCELLED {
            if let Some(rid) = args::get(&d, FLD_PARAMS).and_then(|p| args::get(p, FLD_REQUEST_ID))
            {
                incoming.cancel_id = json::serialize(rid);
            }
        }
        incoming
    }

    fn handle_and_send(&self, line: &str) {
        let resp = self.handle_line(line);
        if !resp.is_empty() {
            self.send_line(&resp);
        }
    }

    fn run_request(&self, line: &str, id: &str) {
        self.handle_and_send(line);
        if !id.is_empty() {
            self.clear_cancel(id);
        }
    }

    // -- cancellation table -------------------------------------------------

    fn register_cancel(&self, id: &str) {
        lock(&self.cancels).insert(id.to_string(), Arc::new(AtomicBool::new(false)));
    }

    fn mark_cancelled(&self, id: &str) {
        if let Some(flag) = lock(&self.cancels).get(id) {
            flag.store(true, Ordering::SeqCst);
        }
    }

    fn clear_cancel(&self, id: &str) {
        lock(&self.cancels).remove(id);
    }

    fn cancel_flag_for(&self, id: &str) -> Option<Arc<AtomicBool>> {
        lock(&self.cancels).get(id).cloned()
    }

    // -- schema building ----------------------------------------------------

    fn server_info(&self) -> Value {
        let mut out = json::obj();
        json::add_str(&mut out, FLD_NAME, &self.name);
        json::add_str(&mut out, FLD_VERSION, &self.version);
        out
    }

    fn add_capabilities(&self, result: &mut Value) {
        let mut caps = json::obj();
        if !self.tools.is_empty() {
            let mut tools_cap = json::obj();
            json::add_bool(&mut tools_cap, FLD_LIST_CHANGED, true);
            json::add_val(&mut caps, FLD_TOOLS, tools_cap);
        }
        json::add_val(result, FLD_CAPABILITIES, caps);
    }

    fn find_tool(&self, name: &str) -> Option<&ToolEntry> {
        self.tools.iter().find(|t| t.tool.name == name)
    }

    /// Whether a log line at `level` passes the client's threshold.
    fn log_enabled(&self, level: &str) -> bool {
        let current = lock(&self.logging_level);
        current.is_empty() || log_rank(level) >= log_rank(&current)
    }

    fn notification_line(method: &str, params: Option<&Value>) -> String {
        let mut env = json::obj();
        json::add_str(&mut env, FLD_JSONRPC, K_JSON_RPC_VERSION);
        json::add_str(&mut env, FLD_METHOD, method);
        if let Some(p) = params {
            json::add_val(&mut env, FLD_PARAMS, p.clone());
        }
        json::serialize(&env)
    }

    fn send_notification_doc(&self, method: &str, params: &Value) {
        let line = Self::notification_line(method, Some(params));
        self.send_line(&line);
    }

    /// Wrap a result in an envelope, adding the stateless fields when the request
    /// was made under a stateless revision.
    fn finalize(
        &self,
        id: &Value,
        method: &str,
        stateless: bool,
        mut result: Value,
        input_required: bool,
    ) -> String {
        let mut env = json::obj();
        json::add_str(&mut env, FLD_JSONRPC, K_JSON_RPC_VERSION);
        json::add_val(&mut env, FLD_ID, id.clone());

        if stateless {
            json::add_str(
                &mut result,
                FLD_RESULT_TYPE,
                if input_required {
                    K_RESULT_TYPE_INPUT_REQUIRED
                } else {
                    K_RESULT_TYPE_COMPLETE
                },
            );
            let mut meta = json::obj();
            let info = self.server_info();
            json::add_val(&mut meta, META_SERVER_INFO, info);
            json::add_val(&mut result, FLD_META, meta);

            if is_cacheable_method(method) {
                json::add_int(&mut result, FLD_TTL_MS, self.cache_ttl_ms);
                json::add_str(&mut result, FLD_CACHE_SCOPE, &self.cache_scope);
            }
        }

        json::add_val(&mut env, FLD_RESULT, result);
        json::serialize(&env)
    }

    /// Build an error reply.
    fn make_error(&self, id: &Value, code: ErrorCode, message: &str) -> String {
        let mut env = json::obj();
        json::add_str(&mut env, FLD_JSONRPC, K_JSON_RPC_VERSION);
        json::add_val(&mut env, FLD_ID, id.clone());
        let mut err = json::obj();
        json::add_int(&mut err, FLD_CODE, code.code() as i64);
        json::add_str(&mut err, FLD_MESSAGE, message);
        json::add_val(&mut env, FLD_ERROR, err);
        json::serialize(&env)
    }

    /// Build an `-32022` reply listing the supported revisions.
    fn unsupported_version(&self, id: &Value, requested: &str) -> String {
        let mut env = json::obj();
        json::add_str(&mut env, FLD_JSONRPC, K_JSON_RPC_VERSION);
        json::add_val(&mut env, FLD_ID, id.clone());

        let mut data = json::obj();
        let mut supported = json::arr();
        for v in supported_protocol_versions() {
            json::push_str(&mut supported, &v);
        }
        json::add_val(&mut data, FLD_SUPPORTED, supported);
        json::add_str(&mut data, FLD_REQUESTED, requested);

        let mut err = json::obj();
        json::add_int(
            &mut err,
            FLD_CODE,
            ErrorCode::UnsupportedProtocolVersion.code() as i64,
        );
        json::add_str(&mut err, FLD_MESSAGE, "Unsupported protocol version");
        json::add_val(&mut err, FLD_DATA, data);
        json::add_val(&mut env, FLD_ERROR, err);
        json::serialize(&env)
    }

    /// The revision for a request: the per-call `_meta` value when present, and the
    /// version agreed at `initialize` otherwise.
    fn request_protocol_version(&self, params: &Value) -> String {
        if let Some(v) =
            args::get(params, FLD_META).and_then(|m| args::get(m, META_PROTOCOL_VERSION))
        {
            if let Some(s) = v.as_str() {
                return s.to_string();
            }
        }
        lock(&self.negotiated_version).clone()
    }

    // -- dispatch -----------------------------------------------------------

    /// Dispatch a single message and return the reply, or an empty string when the
    /// message is a notification that needs no reply.
    fn dispatch_one(&self, msg: &Value, scope: Option<&RequestScope>) -> String {
        let null_id = Value::Null;

        let obj = match msg.as_object() {
            Some(o) => o,
            None => return self.make_error(&null_id, ErrorCode::InvalidRequest, "Invalid Request"),
        };

        let method_v = obj.get(FLD_METHOD);
        let id_v = obj.get(FLD_ID);
        let id: &Value = id_v.unwrap_or(&null_id);

        let method = match method_v.and_then(|m| m.as_str()) {
            Some(m) => m.to_string(),
            None => {
                // A notification without a usable method is ignored.
                if id_v.is_none() {
                    return String::new();
                }
                return self.make_error(id, ErrorCode::InvalidRequest, "Invalid Request");
            }
        };

        let empty_obj = json::obj();
        let params = obj.get(FLD_PARAMS).unwrap_or(&empty_obj);

        let pv = args::get(params, FLD_META).and_then(|m| args::get(m, META_PROTOCOL_VERSION));
        let stateless = matches!(pv, Some(p) if p.is_string());
        if stateless && id_v.is_some() {
            let requested = pv.and_then(|p| p.as_str()).unwrap_or_default();
            if !is_stateless_version(requested) {
                return self.unsupported_version(id, requested);
            }
        }

        // The handshake, the notifications and ping are allowed at any time.
        if method == MTD_INITIALIZE {
            return self.on_initialize(id, params);
        }
        if method == MTD_SERVER_DISCOVER {
            return self.on_discover(id);
        }
        if method == NTF_INITIALIZED {
            self.initialized.store(true, Ordering::SeqCst);
            return String::new();
        }
        if method == NTF_CANCELLED {
            return String::new();
        }
        if method == MTD_PING {
            return self.finalize(id, MTD_PING, stateless, json::obj(), false);
        }

        // Everything else needs a completed handshake, except stateless requests,
        // which carry their revision per call.
        if !stateless && !self.initialized() {
            if id_v.is_none() {
                return String::new();
            }
            return self.make_error(id, ErrorCode::InvalidRequest, "Server not initialized");
        }

        if method == MTD_TOOLS_LIST {
            return self.on_tools_list(id, stateless);
        }
        if method == MTD_TOOLS_CALL {
            return self.on_tools_call(id, stateless, params, scope);
        }
        if method == MTD_LOGGING_SET_LEVEL {
            *lock(&self.logging_level) = args::str(params, FLD_LEVEL, "");
            return self.finalize(id, MTD_LOGGING_SET_LEVEL, stateless, json::obj(), false);
        }

        if id_v.is_none() {
            return String::new();
        }
        self.make_error(
            id,
            ErrorCode::MethodNotFound,
            &format!("Method not found: {method}"),
        )
    }

    /// Handle one message: a single request object or a batch array.
    fn handle(&self, line: &str, scope: Option<&RequestScope>) -> String {
        let text = strip_bom_and_trim(line);
        if text.is_empty() {
            return String::new();
        }

        let null_id = Value::Null;
        if !json::within_depth(text, json::MAX_DEPTH) {
            return self.make_error(
                &null_id,
                ErrorCode::InvalidRequest,
                "Message is nested too deeply",
            );
        }

        let msg: Value = match serde_json::from_str(text) {
            Ok(v) => v,
            Err(_) => return self.make_error(&null_id, ErrorCode::ParseError, "Parse error"),
        };

        if let Value::Array(items) = &msg {
            let mut out = String::new();
            for item in items {
                let r = self.dispatch_one(item, scope);
                if r.is_empty() {
                    continue;
                }
                // Open the array on the first reply, then separate with commas.
                out.push_str(if out.is_empty() { "[" } else { "," });
                out.push_str(&r);
            }
            if !out.is_empty() {
                out.push(']');
            }
            return out;
        }

        self.dispatch_one(&msg, scope)
    }

    // -- handlers -----------------------------------------------------------

    /// Answer `initialize`, echoing the client's revision when it is supported.
    fn on_initialize(&self, id: &Value, params: &Value) -> String {
        let mut result = json::obj();

        let requested = args::str(params, FLD_PROTOCOL_VERSION, "");
        let supported = is_stateful_version(&requested) || is_stateless_version(&requested);
        let negotiated = if supported {
            requested.clone()
        } else {
            default_stateful_version().to_string()
        };
        json::add_str(&mut result, FLD_PROTOCOL_VERSION, &negotiated);

        self.add_capabilities(&mut result);

        let info = self.server_info();
        json::add_val(&mut result, FLD_SERVER_INFO, info);

        if !self.instructions.is_empty() {
            json::add_str(&mut result, FLD_INSTRUCTIONS, &self.instructions);
        }

        *lock(&self.negotiated_version) = negotiated;
        self.initialized.store(true, Ordering::SeqCst);
        self.finalize(id, MTD_INITIALIZE, false, result, false)
    }

    /// Answer `server/discover`.
    fn on_discover(&self, id: &Value) -> String {
        let mut result = json::obj();

        let mut versions = json::arr();
        for v in supported_protocol_versions() {
            json::push_str(&mut versions, &v);
        }
        json::add_val(&mut result, FLD_SUPPORTED_VERSIONS, versions);

        self.add_capabilities(&mut result);

        self.finalize(id, MTD_SERVER_DISCOVER, true, result, false)
    }

    /// Answer `tools/list`.
    fn on_tools_list(&self, id: &Value, stateless: bool) -> String {
        let mut result = json::obj();
        let mut arr = json::arr();
        for entry in &self.tools {
            json::push(&mut arr, tool_value(&entry.tool));
        }
        json::add_val(&mut result, FLD_TOOLS, arr);
        self.finalize(id, MTD_TOOLS_LIST, stateless, result, false)
    }

    /// Answer `tools/call` by running the tool's handler.
    fn on_tools_call(
        &self,
        id: &Value,
        stateless: bool,
        params: &Value,
        scope: Option<&RequestScope>,
    ) -> String {
        let name = args::str(params, FLD_NAME, "");
        if name.is_empty() {
            return self.make_error(id, ErrorCode::InvalidParams, "Missing tool name");
        }
        let entry = match self.find_tool(&name) {
            Some(e) => e,
            None => {
                return self.make_error(
                    id,
                    ErrorCode::InvalidParams,
                    &format!("Unknown tool: {name}"),
                )
            }
        };

        let empty_args = json::obj();
        let tool_args = args::get(params, FLD_ARGUMENTS).unwrap_or(&empty_args);

        if let Err(verr) = validate_tool_args(&entry.tool, tool_args) {
            return self.make_error(id, ErrorCode::InvalidParams, &verr);
        }

        let progress_token = args::get(params, FLD_META)
            .and_then(|m| args::get(m, FLD_PROGRESS_TOKEN))
            .cloned();
        let caps = client_elicit_caps(params);
        let input_responses = args::get(params, FLD_INPUT_RESPONSES)
            .filter(|v| v.is_object())
            .cloned();

        // A scope-supplied flag takes precedence; otherwise it comes from the table.
        let cancel = match scope {
            Some(s) => s.cancel.clone(),
            None => self.cancel_flag_for(&json::serialize(id)),
        };

        // A panicking handler becomes a tool error rather than losing a worker.
        let tr = if entry.ctx {
            let noop: Arc<dyn Fn(String) + Send + Sync> = Arc::new(|_| {});
            let sink = match scope {
                Some(s) => Some(s.notify.clone().unwrap_or(noop)),
                None => None,
            };
            let mut ctx = RequestContext {
                method: MTD_TOOLS_CALL.to_string(),
                stateless,
                protocol_version: self.request_protocol_version(params),
                has_progress_token: progress_token.is_some(),
                server: self,
                sink,
                principal: scope.map(|s| s.principal.clone()).unwrap_or_default(),
                progress_token,
                input_responses,
                request_state: args::str(params, FLD_REQUEST_STATE, ""),
                elicit_form: caps.form,
                elicit_url: caps.url,
                cancel,
            };
            let handler = entry.handler_ctx.as_ref();
            let caught = catch_unwind(AssertUnwindSafe(|| match handler {
                Some(h) => h(tool_args, &mut ctx),
                None => ToolResult::error("Tool has no handler"),
            }));
            match caught {
                Ok(r) => r,
                Err(p) => ToolResult::error(format!("Tool error: {}", panic_message(p))),
            }
        } else {
            let handler = entry.handler.as_ref();
            let caught = catch_unwind(AssertUnwindSafe(|| match handler {
                Some(h) => h(tool_args),
                None => ToolResult::error("Tool has no handler"),
            }));
            match caught {
                Ok(r) => r,
                Err(p) => ToolResult::error(format!("Tool error: {}", panic_message(p))),
            }
        };

        if !tr.elicitations.is_empty() {
            if !entry.ctx {
                return self.make_error(
                    id,
                    ErrorCode::InternalError,
                    &format!(
                        "Tool {name} asked for input but was added with add_tool, \
                         not add_tool_ctx, so it can't read the answers"
                    ),
                );
            }
            if !stateless {
                return self.make_error(
                    id,
                    ErrorCode::MissingRequiredClientCapability,
                    &format!("Elicitation requires protocol version {PROTOCOL_VERSION_LATEST}"),
                );
            }
            for e in &tr.elicitations {
                let supported = if e.is_url() { caps.url } else { caps.form };
                if !supported {
                    return self.make_error(
                        id,
                        ErrorCode::MissingRequiredClientCapability,
                        &format!(
                            "Client does not support {} mode elicitation",
                            if e.is_url() {
                                VAL_MODE_URL
                            } else {
                                VAL_MODE_FORM
                            }
                        ),
                    );
                }
            }

            let mut ir = json::obj();
            let mut reqs = json::obj();
            for e in &tr.elicitations {
                match elicit_request_value(e) {
                    Ok(r) => json::add_key_val(&mut reqs, &e.key, r),
                    Err(err) => return self.make_error(id, ErrorCode::InternalError, &err),
                }
            }
            json::add_val(&mut ir, FLD_INPUT_REQUESTS, reqs);
            if !tr.request_state.is_empty() {
                json::add_str(&mut ir, FLD_REQUEST_STATE, &tr.request_state);
            }
            return self.finalize(id, MTD_TOOLS_CALL, stateless, ir, true);
        }

        let mut result = json::obj();
        let mut content = json::arr();
        for c in &tr.content {
            json::push(&mut content, content_value(c));
        }
        json::add_val(&mut result, FLD_CONTENT, content);
        json::add_bool(&mut result, FLD_IS_ERROR, tr.is_error);
        if tr.has_structured_content && !tr.structured_content.is_empty() {
            if let Some(sc) = json::parse_into(&tr.structured_content) {
                json::add_val(&mut result, FLD_STRUCTURED_CONTENT, sc);
            }
        }
        self.finalize(id, MTD_TOOLS_CALL, stateless, result, false)
    }
}

/// The method, id and cancellation id read from a line before dispatch.
#[derive(Debug, Default, Clone)]
struct Incoming {
    method: String,
    id: String,
    cancel_id: String,
}

// ---------------------------------------------------------------------------
// Free helpers
// ---------------------------------------------------------------------------

/// Methods that may carry a cache hint on a stateless reply.
fn is_cacheable_method(m: &str) -> bool {
    m == MTD_TOOLS_LIST
        || m == MTD_RESOURCES_LIST
        || m == MTD_RESOURCES_TEMPLATES_LIST
        || m == MTD_RESOURCES_READ
        || m == MTD_PROMPTS_LIST
        || m == MTD_SERVER_DISCOVER
}

/// Map a log level onto its severity. Unknown levels rank as `info`.
fn log_rank(lvl: &str) -> i32 {
    match lvl {
        "debug" => 0,
        "info" => 1,
        "notice" => 2,
        "warning" => 3,
        "error" => 4,
        "critical" => 5,
        "alert" => 6,
        "emergency" => 7,
        _ => 1,
    }
}

/// The JSON Schema for one tool parameter.
fn property_schema(p: &ToolParameter) -> Value {
    let mut pj = json::obj();
    json::add_str(&mut pj, FLD_TYPE, p.param_type.schema_name());
    if !p.description.is_empty() {
        json::add_str(&mut pj, FLD_DESCRIPTION, &p.description);
    }
    if p.param_type == PropertyType::Array && !p.item_type.is_empty() {
        let mut items = json::obj();
        json::add_str(&mut items, FLD_TYPE, &p.item_type);
        json::add_val(&mut pj, FLD_ITEMS, items);
    }
    if !p.enum_values.is_empty() {
        let mut e = json::arr();
        for v in &p.enum_values {
            json::push_str(&mut e, v);
        }
        json::add_val(&mut pj, FLD_ENUM, e);
    }
    pj
}

/// The JSON Schema for an object with the given properties.
///
/// Used for a tool's `inputSchema` and for an elicitation form.
fn object_schema(params: &[ToolParameter]) -> Value {
    let mut schema = json::obj();
    json::add_str(&mut schema, FLD_TYPE, VAL_SCHEMA_OBJECT);
    let mut props = json::obj();
    let mut required = json::arr();
    for p in params {
        let pj = property_schema(p);
        json::add_key_val(&mut props, &p.name, pj);
        if p.required {
            json::push_str(&mut required, &p.name);
        }
    }
    json::add_val(&mut schema, FLD_PROPERTIES, props);
    if !required.as_array().map(|a| a.is_empty()).unwrap_or(true) {
        json::add_val(&mut schema, FLD_REQUIRED, required);
    }
    schema
}

/// The `tools/list` entry for one tool.
fn tool_value(t: &Tool) -> Value {
    let mut j = json::obj();
    json::add_str(&mut j, FLD_NAME, &t.name);
    if !t.title.is_empty() {
        json::add_str(&mut j, FLD_TITLE, &t.title);
    }
    if !t.description.is_empty() {
        json::add_str(&mut j, FLD_DESCRIPTION, &t.description);
    }

    let schema = if !t.custom_input_schema.is_empty() {
        match json::parse_into(&t.custom_input_schema) {
            Some(v) => v,
            None => object_schema(&t.properties),
        }
    } else {
        object_schema(&t.properties)
    };
    json::add_val(&mut j, FLD_INPUT_SCHEMA, schema);

    if !t.output_schema.is_empty() {
        if let Some(os) = json::parse_into(&t.output_schema) {
            json::add_val(&mut j, FLD_OUTPUT_SCHEMA, os);
        }
    }
    j
}

/// The `content` entry for one content block.
fn content_value(c: &Content) -> Value {
    let mut j = json::obj();
    match c {
        Content::Text { text } => {
            json::add_str(&mut j, FLD_TYPE, VAL_TYPE_TEXT);
            json::add_str(&mut j, FLD_TEXT, text);
        }
        Content::Image { data, mime_type } => {
            json::add_str(&mut j, FLD_TYPE, VAL_TYPE_IMAGE);
            json::add_str(&mut j, FLD_DATA, data);
            json::add_str(&mut j, FLD_MIME_TYPE, mime_type);
        }
        Content::Audio { data, mime_type } => {
            json::add_str(&mut j, FLD_TYPE, VAL_TYPE_AUDIO);
            json::add_str(&mut j, FLD_DATA, data);
            json::add_str(&mut j, FLD_MIME_TYPE, mime_type);
        }
        Content::ResourceLink {
            uri,
            name,
            mime_type,
            description,
        } => {
            json::add_str(&mut j, FLD_TYPE, VAL_TYPE_RESOURCE_LINK);
            json::add_str(&mut j, FLD_URI, uri);
            json::add_str(&mut j, FLD_NAME, name);
            if !mime_type.is_empty() {
                json::add_str(&mut j, FLD_MIME_TYPE, mime_type);
            }
            if !description.is_empty() {
                json::add_str(&mut j, FLD_DESCRIPTION, description);
            }
        }
        Content::EmbeddedResource {
            uri,
            mime_type,
            text,
            blob,
        } => {
            json::add_str(&mut j, FLD_TYPE, VAL_TYPE_RESOURCE);
            let mut r = json::obj();
            json::add_str(&mut r, FLD_URI, uri);
            if !mime_type.is_empty() {
                json::add_str(&mut r, FLD_MIME_TYPE, mime_type);
            }
            if !blob.is_empty() {
                json::add_str(&mut r, FLD_BLOB, blob);
            } else {
                json::add_str(&mut r, FLD_TEXT, text);
            }
            json::add_val(&mut j, FLD_RESOURCE, r);
        }
    }
    j
}

/// Check required arguments and their types before running a handler.
fn validate_tool_args(tool: &Tool, args: &Value) -> Result<(), String> {
    for p in &tool.properties {
        let v = match args::get(args, &p.name) {
            Some(v) => v,
            None => {
                if p.required {
                    return Err(format!("Missing required argument: {}", p.name));
                }
                continue;
            }
        };
        let ok = match p.param_type {
            PropertyType::String => v.is_string(),
            PropertyType::Number => v.is_number(),
            PropertyType::Integer => v.as_i64().is_some() || v.as_u64().is_some(),
            PropertyType::Boolean => v.is_boolean(),
            PropertyType::Array => v.is_array(),
            PropertyType::Object => v.is_object(),
        };
        if !ok {
            return Err(format!(
                "Argument '{}' should be of type {}",
                p.name,
                p.param_type.schema_name()
            ));
        }
    }
    Ok(())
}

/// Build the `elicitation/create` request that asks for one key.
fn elicit_request_value(e: &Elicitation) -> Result<Value, String> {
    let mut req = json::obj();
    json::add_str(&mut req, FLD_METHOD, MTD_ELICITATION_CREATE);

    let mut p = json::obj();
    json::add_str(
        &mut p,
        FLD_MODE,
        if e.is_url() {
            VAL_MODE_URL
        } else {
            VAL_MODE_FORM
        },
    );
    json::add_str(&mut p, FLD_MESSAGE, &e.message);

    if e.is_url() {
        if e.elicitation_id.is_empty() {
            return Err(format!("Elicitation '{}' has no elicitationId", e.key));
        }
        json::add_str(&mut p, FLD_ELICITATION_ID, &e.elicitation_id);
        json::add_str(&mut p, FLD_URL, &e.url);
    } else {
        let schema = if !e.schema.is_empty() {
            match json::parse_into(&e.schema) {
                Some(v) => v,
                None => {
                    return Err(format!(
                        "Elicitation '{}' has a schema that is not valid JSON",
                        e.key
                    ))
                }
            }
        } else {
            for f in &e.fields {
                if matches!(f.param_type, PropertyType::Object | PropertyType::Array) {
                    return Err(format!(
                        "Elicitation '{}' field '{}' must be a string, number, integer or boolean",
                        e.key, f.name
                    ));
                }
            }
            object_schema(&e.fields)
        };
        json::add_val(&mut p, FLD_REQUESTED_SCHEMA, schema);
    }

    json::add_val(&mut req, FLD_PARAMS, p);
    Ok(req)
}

/// The elicitation modes a client advertises.
#[derive(Debug, Default, Clone, Copy)]
struct ElicitCaps {
    form: bool,
    url: bool,
}

/// Read the client's elicitation capabilities out of a request's `_meta`.
fn client_elicit_caps(params: &Value) -> ElicitCaps {
    let mut c = ElicitCaps::default();
    let el = args::get(params, FLD_META)
        .and_then(|m| args::get(m, META_CLIENT_CAPABILITIES))
        .and_then(|caps| args::get(caps, FLD_ELICITATION));
    let el = match el {
        Some(e) if e.is_object() => e,
        _ => return c,
    };
    // An empty `elicitation` object means form mode and nothing else.
    if el.as_object().map(|o| o.is_empty()).unwrap_or(false) {
        c.form = true;
        return c;
    }
    c.form = args::has(el, FLD_FORM);
    c.url = args::has(el, FLD_URL);
    c
}

/// Strip a leading byte order mark and surrounding whitespace.
fn strip_bom_and_trim(line: &str) -> &str {
    let b = line.as_bytes();
    let mut start = 0usize;
    let mut end = b.len();
    if end - start >= 3 && b[0] == 0xEF && b[1] == 0xBB && b[2] == 0xBF {
        start += 3;
    }
    while start < end && matches!(b[start], b' ' | b'\t' | b'\r' | b'\n') {
        start += 1;
    }
    while end > start && matches!(b[end - 1], b' ' | b'\t' | b'\r' | b'\n') {
        end -= 1;
    }
    &line[start..end]
}

/// Describe a panic payload as the text of a tool error.
fn panic_message(payload: Box<dyn Any + Send>) -> String {
    if let Some(s) = payload.downcast_ref::<&'static str>() {
        return (*s).to_string();
    }
    if let Some(s) = payload.downcast_ref::<String>() {
        return s.clone();
    }
    "unknown exception".to_string()
}
