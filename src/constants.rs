//! Protocol strings and revision negotiation.
//!
//! Method names, notification names, JSON field names, fixed enum values and the
//! list of supported protocol revisions.

// Each constant is named after the protocol string it holds, so an individual doc
// comment would only repeat the value. The sections below group them instead.
#![allow(missing_docs)]

/// The newest protocol revision, and the only stateless one.
pub const PROTOCOL_VERSION_LATEST: &str = "2026-07-28";

// ---------------------------------------------------------------------------
// `_meta` keys
// ---------------------------------------------------------------------------

pub const META_PROTOCOL_VERSION: &str = "io.modelcontextprotocol/protocolVersion";
pub const META_CLIENT_INFO: &str = "io.modelcontextprotocol/clientInfo";
pub const META_CLIENT_CAPABILITIES: &str = "io.modelcontextprotocol/clientCapabilities";
pub const META_SERVER_INFO: &str = "io.modelcontextprotocol/serverInfo";

// ---------------------------------------------------------------------------
// Revision negotiation
// ---------------------------------------------------------------------------

/// Revisions that carry their protocol version on every call and need no
/// handshake. Newest first.
const STATELESS_VERSIONS: &[&str] = &[PROTOCOL_VERSION_LATEST];

/// Revisions that must open with `initialize`. Newest first.
const STATEFUL_VERSIONS: &[&str] = &["2025-11-25", "2025-06-18", "2025-03-26", "2024-11-05"];

/// Whether `v` is a revision that needs no handshake.
pub fn is_stateless_version(v: &str) -> bool {
    STATELESS_VERSIONS.contains(&v)
}

/// Whether `v` is a revision that must open with `initialize`.
pub fn is_stateful_version(v: &str) -> bool {
    STATEFUL_VERSIONS.contains(&v)
}

/// The revision assumed when the client asks for one that is not supported.
pub fn default_stateful_version() -> &'static str {
    STATEFUL_VERSIONS[0]
}

/// Every supported revision, stateless first.
pub fn supported_protocol_versions() -> Vec<String> {
    STATELESS_VERSIONS
        .iter()
        .chain(STATEFUL_VERSIONS.iter())
        .map(|s| (*s).to_string())
        .collect()
}

// ---------------------------------------------------------------------------
// Methods
// ---------------------------------------------------------------------------

pub const MTD_INITIALIZE: &str = "initialize";
pub const MTD_PING: &str = "ping";
pub const MTD_SERVER_DISCOVER: &str = "server/discover";

pub const MTD_TOOLS_LIST: &str = "tools/list";
pub const MTD_TOOLS_CALL: &str = "tools/call";

pub const MTD_RESOURCES_LIST: &str = "resources/list";
pub const MTD_RESOURCES_TEMPLATES_LIST: &str = "resources/templates/list";
pub const MTD_RESOURCES_READ: &str = "resources/read";
pub const MTD_RESOURCES_SUBSCRIBE: &str = "resources/subscribe";
pub const MTD_RESOURCES_UNSUBSCRIBE: &str = "resources/unsubscribe";

pub const MTD_PROMPTS_LIST: &str = "prompts/list";
pub const MTD_PROMPTS_GET: &str = "prompts/get";

pub const MTD_COMPLETION_COMPLETE: &str = "completion/complete";
pub const MTD_LOGGING_SET_LEVEL: &str = "logging/setLevel";

pub const MTD_ROOTS_LIST: &str = "roots/list";
pub const MTD_SAMPLING_CREATE_MESSAGE: &str = "sampling/createMessage";
pub const MTD_ELICITATION_CREATE: &str = "elicitation/create";

// ---------------------------------------------------------------------------
// Notifications
// ---------------------------------------------------------------------------

pub const NTF_INITIALIZED: &str = "notifications/initialized";
pub const NTF_CANCELLED: &str = "notifications/cancelled";
pub const NTF_PROGRESS: &str = "notifications/progress";
pub const NTF_MESSAGE: &str = "notifications/message";
pub const NTF_RESOURCES_UPDATED: &str = "notifications/resources/updated";
pub const NTF_RESOURCES_LIST_CHANGED: &str = "notifications/resources/list_changed";
pub const NTF_TOOLS_LIST_CHANGED: &str = "notifications/tools/list_changed";
pub const NTF_PROMPTS_LIST_CHANGED: &str = "notifications/prompts/list_changed";
pub const NTF_ROOTS_LIST_CHANGED: &str = "notifications/roots/list_changed";

// ---------------------------------------------------------------------------
// JSON-RPC envelope fields
// ---------------------------------------------------------------------------

pub const FLD_JSONRPC: &str = "jsonrpc";
pub const FLD_ID: &str = "id";
pub const FLD_METHOD: &str = "method";
pub const FLD_PARAMS: &str = "params";
pub const FLD_RESULT: &str = "result";
pub const FLD_ERROR: &str = "error";
pub const FLD_CODE: &str = "code";
pub const FLD_MESSAGE: &str = "message";
pub const FLD_DATA: &str = "data";
pub const FLD_META: &str = "_meta";

// ---------------------------------------------------------------------------
// Handshake and discovery fields
// ---------------------------------------------------------------------------

pub const FLD_PROTOCOL_VERSION: &str = "protocolVersion";
pub const FLD_CAPABILITIES: &str = "capabilities";
pub const FLD_SERVER_INFO: &str = "serverInfo";
pub const FLD_CLIENT_INFO: &str = "clientInfo";
pub const FLD_INSTRUCTIONS: &str = "instructions";
pub const FLD_SUPPORTED_VERSIONS: &str = "supportedVersions";
pub const FLD_REQUESTED: &str = "requested";
pub const FLD_SUPPORTED: &str = "supported";

pub const FLD_NAME: &str = "name";
pub const FLD_VERSION: &str = "version";
pub const FLD_TITLE: &str = "title";
pub const FLD_DESCRIPTION: &str = "description";

pub const FLD_LIST_CHANGED: &str = "listChanged";
pub const FLD_SUBSCRIBE: &str = "subscribe";

// ---------------------------------------------------------------------------
// Tools
// ---------------------------------------------------------------------------

pub const FLD_TOOLS: &str = "tools";
pub const FLD_INPUT_SCHEMA: &str = "inputSchema";
pub const FLD_OUTPUT_SCHEMA: &str = "outputSchema";
pub const FLD_ARGUMENTS: &str = "arguments";
pub const FLD_CONTENT: &str = "content";
pub const FLD_STRUCTURED_CONTENT: &str = "structuredContent";
pub const FLD_IS_ERROR: &str = "isError";

pub const FLD_TYPE: &str = "type";
pub const FLD_PROPERTIES: &str = "properties";
pub const FLD_REQUIRED: &str = "required";
pub const FLD_ITEMS: &str = "items";
pub const FLD_ENUM: &str = "enum";

pub const FLD_TEXT: &str = "text";
pub const FLD_BLOB: &str = "blob";
pub const FLD_MIME_TYPE: &str = "mimeType";
pub const FLD_URI: &str = "uri";
pub const FLD_RESOURCE: &str = "resource";

// ---------------------------------------------------------------------------
// Resources and prompts
// ---------------------------------------------------------------------------

pub const FLD_RESOURCES: &str = "resources";
pub const FLD_RESOURCE_TEMPLATES: &str = "resourceTemplates";
pub const FLD_URI_TEMPLATE: &str = "uriTemplate";
pub const FLD_CONTENTS: &str = "contents";
pub const FLD_SIZE: &str = "size";

pub const FLD_PROMPTS: &str = "prompts";
pub const FLD_MESSAGES: &str = "messages";
pub const FLD_ROLE: &str = "role";

pub const FLD_CURSOR: &str = "cursor";
pub const FLD_NEXT_CURSOR: &str = "nextCursor";

pub const FLD_COMPLETION: &str = "completion";
pub const FLD_REF: &str = "ref";
pub const FLD_ARGUMENT: &str = "argument";
pub const FLD_VALUE: &str = "value";
pub const FLD_VALUES: &str = "values";
pub const FLD_TOTAL: &str = "total";
pub const FLD_HAS_MORE: &str = "hasMore";

// ---------------------------------------------------------------------------
// Progress and logging
// ---------------------------------------------------------------------------

pub const FLD_PROGRESS_TOKEN: &str = "progressToken";
pub const FLD_PROGRESS: &str = "progress";
pub const FLD_REQUEST_ID: &str = "requestId";
pub const FLD_REASON: &str = "reason";
pub const FLD_LEVEL: &str = "level";
pub const FLD_LOGGER: &str = "logger";

pub const FLD_RESULT_TYPE: &str = "resultType";
pub const FLD_TTL_MS: &str = "ttlMs";
pub const FLD_CACHE_SCOPE: &str = "cacheScope";

// ---------------------------------------------------------------------------
// Elicitation
// ---------------------------------------------------------------------------

pub const FLD_INPUT_REQUESTS: &str = "inputRequests";
pub const FLD_INPUT_RESPONSES: &str = "inputResponses";
pub const FLD_REQUEST_STATE: &str = "requestState";
pub const FLD_ELICITATION: &str = "elicitation";
pub const FLD_FORM: &str = "form";
pub const FLD_MODE: &str = "mode";
pub const FLD_URL: &str = "url";
pub const FLD_REQUESTED_SCHEMA: &str = "requestedSchema";
pub const FLD_ACTION: &str = "action";
pub const FLD_ELICITATION_ID: &str = "elicitationId";
pub const FLD_REQUIRED_CAPABILITIES: &str = "requiredCapabilities";

// ---------------------------------------------------------------------------
// Fixed values
// ---------------------------------------------------------------------------

pub const K_JSON_RPC_VERSION: &str = "2.0";
pub const K_RESULT_TYPE_COMPLETE: &str = "complete";
pub const K_RESULT_TYPE_INPUT_REQUIRED: &str = "input_required";
pub const VAL_MODE_FORM: &str = "form";
pub const VAL_MODE_URL: &str = "url";
pub const VAL_ACTION_ACCEPT: &str = "accept";
pub const VAL_ACTION_DECLINE: &str = "decline";
pub const VAL_ACTION_CANCEL: &str = "cancel";
pub const VAL_TYPE_TEXT: &str = "text";
pub const VAL_TYPE_IMAGE: &str = "image";
pub const VAL_TYPE_AUDIO: &str = "audio";
pub const VAL_TYPE_RESOURCE_LINK: &str = "resource_link";
pub const VAL_TYPE_RESOURCE: &str = "resource";

pub const VAL_ROLE_USER: &str = "user";
pub const VAL_ROLE_ASSISTANT: &str = "assistant";

pub const VAL_SCHEMA_STRING: &str = "string";
pub const VAL_SCHEMA_NUMBER: &str = "number";
pub const VAL_SCHEMA_INTEGER: &str = "integer";
pub const VAL_SCHEMA_BOOLEAN: &str = "boolean";
pub const VAL_SCHEMA_OBJECT: &str = "object";
pub const VAL_SCHEMA_ARRAY: &str = "array";

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn version_classification() {
        assert!(is_stateless_version("2026-07-28"));
        assert!(!is_stateless_version("2025-11-25"));
        assert!(is_stateful_version("2025-11-25"));
        assert!(!is_stateful_version("2026-07-28"));
        assert!(!is_stateful_version("1999-01-01"));
        assert_eq!(default_stateful_version(), "2025-11-25");
    }

    #[test]
    fn versions_are_ordered_stateless_first() {
        assert_eq!(
            supported_protocol_versions(),
            vec![
                "2026-07-28",
                "2025-11-25",
                "2025-06-18",
                "2025-03-26",
                "2024-11-05"
            ]
        );
    }
}
