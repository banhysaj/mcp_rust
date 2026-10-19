# Changelog

All notable changes to this project are documented in this file.

The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and this
project follows [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [1.0.0] - 2026-10-15

### Added

- `Server` with a worker thread pool (`set_max_threads`), inline handling of the
  handshake and cancellation notifications, and serialised output.
- Cooperative cancellation through `RequestContext::cancelled`, driven by
  `notifications/cancelled` on stdio and by a client hang-up on HTTP.
- Tool registration with `add_tool`, `add_tool_ctx`, `add_tool_boxed` and
  `add_tool_ctx_boxed`; generated `inputSchema` from `ToolParameter`s, with
  `set_input_schema` and `set_output_schema` for hand-written schemas.
- `ToolResult` with text, image, audio, resource-link and embedded-resource content,
  plus `structuredContent` and tool-level errors.
- Elicitation in form and url modes, including capability checks, `requestState`
  echo and reading the client's answers on the second run.
- Progress and log notifications from a handler, and `logging/setLevel` filtering.
- Protocol revision negotiation across `2026-07-28` (stateless) and `2025-11-25`,
  `2025-06-18`, `2025-03-26`, `2024-11-05` (stateful), with `resultType`,
  `_meta.serverInfo` and cache hints on stateless replies.
- `StdioTransport` and `MemoryTransport`, over a `Transport` trait for other
  newline-delimited transports.
- `HttpServer` on `std::net`: single POST endpoint, `Origin` checks, pluggable
  authorization, header/body agreement for `2026-07-28` requests, chunked
  server-sent events for `tools/call`, keep-alive, chunked request bodies and
  request size limits.
- A nesting guard that refuses messages deeper than 128 levels before parsing, on
  both transports.
