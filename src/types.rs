//! Protocol value types: tools, tool parameters, content, results and elicitation.

use crate::constants::*;

/// JSON-RPC and MCP error codes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(i32)]
pub enum ErrorCode {
    /// Invalid JSON was received.
    ParseError = -32700,
    /// The message was not a valid request object.
    InvalidRequest = -32600,
    /// The method does not exist.
    MethodNotFound = -32601,
    /// Invalid method parameters.
    InvalidParams = -32602,
    /// Internal server error.
    InternalError = -32603,
    /// A transport header disagrees with the request body.
    HeaderMismatch = -32020,
    /// The client did not advertise a capability the request needs.
    MissingRequiredClientCapability = -32021,
    /// The requested protocol revision is not supported.
    UnsupportedProtocolVersion = -32022,
}

impl ErrorCode {
    /// The numeric code carried in `error.code`.
    pub const fn code(self) -> i32 {
        self as i32
    }
}

/// The type of a tool parameter, and the JSON Schema type name it maps to.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PropertyType {
    /// The JSON Schema type `"string"`.
    String,
    /// The JSON Schema type `"number"`.
    Number,
    /// The JSON Schema type `"integer"`.
    Integer,
    /// The JSON Schema type `"boolean"`.
    Boolean,
    /// The JSON Schema type `"object"`.
    Object,
    /// The JSON Schema type `"array"`.
    Array,
}

impl PropertyType {
    /// The JSON Schema type name for this parameter type.
    pub const fn schema_name(self) -> &'static str {
        match self {
            PropertyType::String => VAL_SCHEMA_STRING,
            PropertyType::Number => VAL_SCHEMA_NUMBER,
            PropertyType::Integer => VAL_SCHEMA_INTEGER,
            PropertyType::Boolean => VAL_SCHEMA_BOOLEAN,
            PropertyType::Object => VAL_SCHEMA_OBJECT,
            PropertyType::Array => VAL_SCHEMA_ARRAY,
        }
    }
}

// ---------------------------------------------------------------------------
// Content
// ---------------------------------------------------------------------------

/// One entry of a tool result's `content` array.
///
/// An embedded resource carries both `text` and `blob`; `blob` wins when it is
/// non-empty.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Content {
    /// A text block, sent as `{"type":"text","text":...}`.
    Text {
        /// The text.
        text: String,
    },
    /// An image, sent as `{"type":"image","data":...,"mimeType":...}`.
    Image {
        /// Base64 payload.
        data: String,
        /// Media type; always included, even when empty.
        mime_type: String,
    },
    /// Audio, sent as `{"type":"audio","data":...,"mimeType":...}`.
    Audio {
        /// Base64 payload.
        data: String,
        /// Media type; always included, even when empty.
        mime_type: String,
    },
    /// A link to a resource the client can fetch separately.
    ResourceLink {
        /// The resource URI.
        uri: String,
        /// Display name.
        name: String,
        /// Media type; omitted when empty.
        mime_type: String,
        /// Human description; omitted when empty.
        description: String,
    },
    /// A resource inlined into the result.
    EmbeddedResource {
        /// The resource URI.
        uri: String,
        /// Media type; omitted when empty.
        mime_type: String,
        /// Used when `blob` is empty.
        text: String,
        /// Base64 payload; takes precedence over `text` when non-empty.
        blob: String,
    },
}

impl Content {
    /// A text block.
    pub fn text(t: impl Into<String>) -> Self {
        Content::Text { text: t.into() }
    }

    /// An image from base64 data.
    pub fn image(base64_data: impl Into<String>, mime: impl Into<String>) -> Self {
        Content::Image {
            data: base64_data.into(),
            mime_type: mime.into(),
        }
    }

    /// Audio from base64 data.
    pub fn audio(base64_data: impl Into<String>, mime: impl Into<String>) -> Self {
        Content::Audio {
            data: base64_data.into(),
            mime_type: mime.into(),
        }
    }

    /// A link to a resource.
    pub fn resource_link(
        uri: impl Into<String>,
        name: impl Into<String>,
        mime: impl Into<String>,
        description: impl Into<String>,
    ) -> Self {
        Content::ResourceLink {
            uri: uri.into(),
            name: name.into(),
            mime_type: mime.into(),
            description: description.into(),
        }
    }

    /// An inlined resource carrying text.
    pub fn embedded_text(
        uri: impl Into<String>,
        text: impl Into<String>,
        mime: impl Into<String>,
    ) -> Self {
        Content::EmbeddedResource {
            uri: uri.into(),
            mime_type: mime.into(),
            text: text.into(),
            blob: String::new(),
        }
    }

    /// An inlined resource carrying base64 data.
    pub fn embedded_blob(
        uri: impl Into<String>,
        base64: impl Into<String>,
        mime: impl Into<String>,
    ) -> Self {
        Content::EmbeddedResource {
            uri: uri.into(),
            mime_type: mime.into(),
            text: String::new(),
            blob: base64.into(),
        }
    }
}

// ---------------------------------------------------------------------------
// Tool parameters
// ---------------------------------------------------------------------------

/// One parameter of a tool, and the JSON Schema property generated from it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ToolParameter {
    /// Argument name; also the key in `arguments`.
    pub name: String,
    /// The parameter type.
    pub param_type: PropertyType,
    /// Omitted from the schema when empty.
    pub description: String,
    /// Listed in the schema's `required` array when true.
    pub required: bool,
    /// Element type, used when `param_type` is [`PropertyType::Array`].
    pub item_type: String,
    /// Allowed values; emits an `enum` when non-empty.
    pub enum_values: Vec<String>,
}

impl ToolParameter {
    /// A required parameter.
    pub fn new(
        name: impl Into<String>,
        param_type: PropertyType,
        description: impl Into<String>,
    ) -> Self {
        ToolParameter {
            name: name.into(),
            param_type,
            description: description.into(),
            required: true,
            item_type: String::new(),
            enum_values: Vec::new(),
        }
    }

    /// Mark the parameter optional.
    pub fn optional(mut self) -> Self {
        self.required = false;
        self
    }

    /// Set whether the parameter is required.
    pub fn set_required(mut self, req: bool) -> Self {
        self.required = req;
        self
    }

    /// Restrict the parameter to a fixed set of values.
    pub fn set_enum<I, S>(mut self, values: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.enum_values = values.into_iter().map(Into::into).collect();
        self
    }

    /// Set the element type of an array parameter.
    pub fn set_item_type(mut self, t: impl Into<String>) -> Self {
        self.item_type = t.into();
        self
    }
}

// ---------------------------------------------------------------------------
// Elicitation
// ---------------------------------------------------------------------------

/// A request for input from the client.
///
/// Returned as part of a [`ToolResult`]; the tool then runs a second time with the
/// answers, keyed by [`Elicitation::key`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Elicitation {
    /// Names the answer, and the entry in `inputResponses` the tool reads back.
    pub key: String,
    /// Shown to the user.
    pub message: String,
    /// Set for url mode; empty means form mode.
    pub url: String,
    /// Required in url mode. Minted by [`Elicitation::make_url`].
    pub elicitation_id: String,
    /// Raw JSON schema for the form. Overrides `fields` when set.
    pub schema: String,
    /// Form fields, which may only be flat primitives.
    pub fields: Vec<ToolParameter>,
}

impl Elicitation {
    /// A form-mode elicitation built from `fields`.
    pub fn make_form(key: impl Into<String>, message: impl Into<String>) -> Self {
        Elicitation {
            key: key.into(),
            message: message.into(),
            url: String::new(),
            elicitation_id: String::new(),
            schema: String::new(),
            fields: Vec::new(),
        }
    }

    /// A url-mode elicitation, with an id from [`random_elicitation_id`].
    pub fn make_url(
        key: impl Into<String>,
        message: impl Into<String>,
        url: impl Into<String>,
    ) -> Self {
        Elicitation {
            key: key.into(),
            message: message.into(),
            url: url.into(),
            elicitation_id: random_elicitation_id(),
            schema: String::new(),
            fields: Vec::new(),
        }
    }

    /// A url-mode elicitation with a caller-supplied id.
    ///
    /// Use this when the id has to come from a cryptographically secure source.
    pub fn make_url_with_id(
        key: impl Into<String>,
        message: impl Into<String>,
        url: impl Into<String>,
        elicitation_id: impl Into<String>,
    ) -> Self {
        Elicitation {
            key: key.into(),
            message: message.into(),
            url: url.into(),
            elicitation_id: elicitation_id.into(),
            schema: String::new(),
            fields: Vec::new(),
        }
    }

    /// Whether this elicitation uses url mode.
    pub fn is_url(&self) -> bool {
        !self.url.is_empty()
    }

    /// Add a form field.
    pub fn add_field_param(mut self, param: ToolParameter) -> Self {
        self.fields.push(param);
        self
    }

    /// Add a form field by name and type.
    pub fn add_field(
        mut self,
        name: impl Into<String>,
        param_type: PropertyType,
        description: impl Into<String>,
    ) -> Self {
        self.fields
            .push(ToolParameter::new(name, param_type, description));
        self
    }

    /// Use a hand-written JSON schema for the form instead of `fields`.
    pub fn set_schema(mut self, raw_json: impl Into<String>) -> Self {
        self.schema = raw_json.into();
        self
    }
}

/// A random 32-character lowercase hex id for [`Elicitation::make_url`].
///
/// The value is derived from the system clock, a process-wide counter, the process
/// id and a stack address. It is unique enough to key an elicitation round trip,
/// but it is **not** cryptographically random. When an unpredictable id is needed,
/// generate one yourself and pass it to [`Elicitation::make_url_with_id`].
pub fn random_elicitation_id() -> String {
    use std::sync::atomic::{AtomicU64, Ordering};

    static COUNTER: AtomicU64 = AtomicU64::new(0);

    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0);
    let stack = &nanos as *const u64 as u64;
    let mut seed = nanos
        ^ COUNTER
            .fetch_add(1, Ordering::Relaxed)
            .wrapping_mul(0x9E37_79B9_7F4A_7C15)
        ^ stack.rotate_left(17)
        ^ (std::process::id() as u64).rotate_left(31);
    if seed == 0 {
        seed = 0x9E37_79B9_7F4A_7C15;
    }

    let mut out = String::with_capacity(32);
    for _ in 0..2 {
        // splitmix64
        seed = seed.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = seed;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^= z >> 31;
        out.push_str(&format!("{z:016x}"));
    }
    out
}

// ---------------------------------------------------------------------------
// ToolResult
// ---------------------------------------------------------------------------

/// What a tool handler returns.
///
/// The builder methods take `self` and return `Self`, so they chain:
/// `ToolResult::text("ok").add_text("more").mark_error(false)`.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ToolResult {
    /// The `content` array, in order.
    pub content: Vec<Content>,
    /// Sets `isError` on the reply.
    pub is_error: bool,
    /// Raw JSON for `structuredContent`.
    pub structured_content: String,
    /// Whether `structured_content` should be emitted at all.
    pub has_structured_content: bool,
    /// Input requests to put to the client before the tool runs again.
    pub elicitations: Vec<Elicitation>,
    /// Opaque state echoed back by the client on the second run.
    pub request_state: String,
}

impl ToolResult {
    /// An empty result.
    pub fn new() -> Self {
        Self::default()
    }

    /// A result holding a single text block.
    pub fn text(t: impl Into<String>) -> Self {
        ToolResult {
            content: vec![Content::text(t)],
            ..Default::default()
        }
    }

    /// An error result holding a single text block, with `isError` set.
    pub fn error(message: impl Into<String>) -> Self {
        ToolResult {
            content: vec![Content::text(message)],
            is_error: true,
            ..Default::default()
        }
    }

    /// A result that asks the client for input.
    pub fn elicit(e: Elicitation) -> Self {
        ToolResult {
            elicitations: vec![e],
            ..Default::default()
        }
    }

    /// Add another input request.
    pub fn add_elicitation(mut self, e: Elicitation) -> Self {
        self.elicitations.push(e);
        self
    }

    /// Attach state to hand back on the second run.
    pub fn with_request_state(mut self, state: impl Into<String>) -> Self {
        self.request_state = state.into();
        self
    }

    /// Append a content block.
    pub fn add_content(mut self, c: Content) -> Self {
        self.content.push(c);
        self
    }

    /// Append a text block.
    pub fn add_text(mut self, t: impl Into<String>) -> Self {
        self.content.push(Content::text(t));
        self
    }

    /// Set `structuredContent` from raw JSON.
    ///
    /// The JSON is parsed when the result is serialised; if it does not parse, the
    /// field is omitted and the text content stands alone.
    pub fn with_structured(mut self, raw_json: impl Into<String>) -> Self {
        self.structured_content = raw_json.into();
        self.has_structured_content = true;
        self
    }

    /// Set or clear `isError`.
    pub fn mark_error(mut self, e: bool) -> Self {
        self.is_error = e;
        self
    }
}

// ---------------------------------------------------------------------------
// Tool
// ---------------------------------------------------------------------------

/// A tool definition, as advertised by `tools/list`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Tool {
    /// Unique name, and what `tools/call` passes as `params.name`.
    pub name: String,
    /// Optional display title; omitted when empty.
    pub title: String,
    /// Description; omitted when empty.
    pub description: String,
    /// Parameters, from which `inputSchema` is generated.
    pub properties: Vec<ToolParameter>,
    /// Raw JSON that replaces the generated `inputSchema`.
    pub custom_input_schema: String,
    /// Optional raw JSON `outputSchema`.
    pub output_schema: String,
}

impl Tool {
    /// A tool with no parameters.
    pub fn new(name: impl Into<String>, description: impl Into<String>) -> Self {
        Tool {
            name: name.into(),
            title: String::new(),
            description: description.into(),
            properties: Vec::new(),
            custom_input_schema: String::new(),
            output_schema: String::new(),
        }
    }

    /// Set the display title.
    pub fn set_title(mut self, t: impl Into<String>) -> Self {
        self.title = t.into();
        self
    }

    /// Replace the description.
    pub fn set_description(mut self, d: impl Into<String>) -> Self {
        self.description = d.into();
        self
    }

    /// Supply a hand-written `inputSchema` instead of generating one.
    pub fn set_input_schema(mut self, raw_json: impl Into<String>) -> Self {
        self.custom_input_schema = raw_json.into();
        self
    }

    /// Set the `outputSchema`.
    pub fn set_output_schema(mut self, raw_json: impl Into<String>) -> Self {
        self.output_schema = raw_json.into();
        self
    }

    /// Add a parameter.
    pub fn add_parameter(mut self, p: ToolParameter) -> Self {
        self.properties.push(p);
        self
    }

    /// Add a required parameter by name and type.
    pub fn add_parameter_simple(
        mut self,
        name: impl Into<String>,
        param_type: PropertyType,
        description: impl Into<String>,
    ) -> Self {
        self.properties
            .push(ToolParameter::new(name, param_type, description));
        self
    }

    /// Add a string parameter restricted to a fixed set of values.
    pub fn add_enum_param<I, S>(
        mut self,
        name: impl Into<String>,
        values: I,
        description: impl Into<String>,
    ) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.properties
            .push(ToolParameter::new(name, PropertyType::String, description).set_enum(values));
        self
    }
}

// ---------------------------------------------------------------------------
// Resources
// ---------------------------------------------------------------------------

/// A resource description, as advertised by `resources/list`.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Resource {
    /// The resource URI.
    pub uri: String,
    /// Machine-readable name.
    pub name: String,
    /// Optional display title.
    pub title: String,
    /// Optional human description.
    pub description: String,
    /// Default media type.
    pub mime_type: String,
    /// Size in bytes, or 0 when unknown.
    pub size: i64,
}

impl Resource {
    /// A resource with a URI and a name.
    pub fn new(uri: impl Into<String>, name: impl Into<String>) -> Self {
        Resource {
            uri: uri.into(),
            name: name.into(),
            ..Default::default()
        }
    }
}

/// The contents of a resource read with `resources/read`.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ResourceContent {
    /// The resource URI.
    pub uri: String,
    /// Media type of the payload.
    pub mime_type: String,
    /// Sent as `text` when non-empty and `blob` is empty.
    pub text: String,
    /// Otherwise sent as a base64 `blob`.
    pub blob: String,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn error_codes_match_the_protocol() {
        assert_eq!(ErrorCode::ParseError.code(), -32700);
        assert_eq!(ErrorCode::InvalidRequest.code(), -32600);
        assert_eq!(ErrorCode::MethodNotFound.code(), -32601);
        assert_eq!(ErrorCode::InvalidParams.code(), -32602);
        assert_eq!(ErrorCode::InternalError.code(), -32603);
        assert_eq!(ErrorCode::HeaderMismatch.code(), -32020);
        assert_eq!(ErrorCode::MissingRequiredClientCapability.code(), -32021);
        assert_eq!(ErrorCode::UnsupportedProtocolVersion.code(), -32022);
    }

    #[test]
    fn result_builders_chain() {
        let r = ToolResult::text("a")
            .add_text("b")
            .with_structured("{}")
            .mark_error(true);
        assert_eq!(r.content.len(), 2);
        assert!(r.has_structured_content);
        assert!(r.is_error);
        assert_eq!(
            ToolResult::elicit(Elicitation::make_form("k", "m"))
                .elicitations
                .len(),
            1
        );
    }

    #[test]
    fn make_url_mints_a_32_char_hex_id() {
        let e = Elicitation::make_url("k", "m", "https://example.com");
        assert!(e.is_url());
        assert_eq!(e.elicitation_id.len(), 32);
        assert!(e.elicitation_id.chars().all(|c| c.is_ascii_hexdigit()));
        assert_ne!(
            e.elicitation_id,
            Elicitation::make_url("k", "m", "https://example.com").elicitation_id
        );
    }

    #[test]
    fn tool_builder_shape() {
        let t = Tool::new("t", "d")
            .add_parameter_simple("n", PropertyType::Integer, "iters")
            .add_enum_param("mode", ["a", "b"], "pick one")
            .set_output_schema("{}");
        assert_eq!(t.properties.len(), 2);
        assert_eq!(t.properties[1].enum_values, vec!["a", "b"]);
        assert!(t.properties[0].required);
        assert_eq!(
            ToolParameter::new("x", PropertyType::Array, "")
                .set_item_type("integer")
                .item_type,
            "integer"
        );
    }
}
