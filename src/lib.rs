//! Nano Gateway: a typed boundary that exposes JSON-RPC 2.0 and translates ledger calls to Nano node custom JSON RPC.

use std::{
    collections::{HashMap, VecDeque},
    convert::Infallible,
    path::Path,
    sync::{
        atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
        Arc,
    },
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use async_stream::stream;
use axum::{
    body::Bytes,
    // Aliased because `std::path::Path` is also in scope.
    extract::State,
    http::{header, HeaderMap, HeaderValue, Method, StatusCode},
    response::{
        sse::{Event, KeepAlive, Sse},
        IntoResponse, Response,
    },
    routing::{get, post},
    Json,
    Router,
};
use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine};
use ed25519_dalek::{Signature, Signer, SigningKey, Verifier, VerifyingKey};
use futures_util::{SinkExt, StreamExt};
use reqwest::Client;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use thiserror::Error;
use tokio::sync::{broadcast, Mutex};
use tokio_tungstenite::{
    connect_async,
    tungstenite::{
        client::IntoClientRequest,
        http::{
            header::USER_AGENT, HeaderValue as TungsteniteHeaderValue,
            Request as TungsteniteRequest,
        },
        Message,
    },
};
use tower::limit::ConcurrencyLimitLayer;
use tower_http::{
    cors::{AllowOrigin, CorsLayer},
    limit::RequestBodyLimitLayer,
    trace::TraceLayer,
};
use url::Url;

const DEFAULT_CONFIG: &str = r#"listen: "127.0.0.1:8090"
node_rpc_urls:
  - "http://127.0.0.1:7076"
node_ws_urls:
  - "ws://127.0.0.1:7078"
profile: "nano-node/V28.2"
require_common_auth: true
allow_work: false
allow_control: false
auth_public_key: null
enable_discovery: true
enable_browser_ui: false
log_rpc: false
cors_origins:
  - "*"
public_url: null
"#;
/// A Nano address is 65 characters, so a comma-joined list of n accounts takes
/// 66n - 1 bytes. These caps are sized to what an HTTP request line can actually
/// carry rather than to what the node would accept, because the node documents no
/// cap at all while nginx and Apache commonly limit a request line to about 8 KiB.
/// A larger cap would pass every test here and then fail behind the first reverse
/// proxy. 110 accounts is 7259 bytes, which leaves room for the path and the
/// other parameters inside 8 KiB.
const MAX_ACCOUNT_FILTER_BYTES: usize = 7680;
/// The POST variant carries the account set in a request body, so the request line is
/// not the constraint. This is bounded by the 1 MiB request body limit instead: at 66
/// bytes per account that allows roughly fifteen thousand, and this cap stops a
/// caller from asking for more than it could usefully be sent.
const MAX_BODY_ACCOUNT_ITEMS: usize = 15_000;
/// Per-subscriber buffer. A subscriber that falls this far behind is closed, because
/// the node offers no replay to catch up with.
const SSE_QUEUE_CAPACITY: usize = 256;
/// How often an idle stream emits a keepalive comment. Frequent enough that an
/// intermediary does not treat the connection as dead between events.
const STREAM_KEEPALIVE: Duration = Duration::from_secs(15);
const MAX_ACCOUNT_FILTER_ITEMS: usize = 110;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Config {
    #[serde(default = "default_listen")]
    pub listen: String,
    #[serde(default = "default_rpc_urls")]
    pub node_rpc_urls: Vec<String>,
    #[serde(default = "default_ws_urls")]
    pub node_ws_urls: Vec<String>,
    #[serde(default = "default_profile")]
    pub profile: String,
    /// Require a bearer PASETO for Common methods such as `process`.
    /// Set false only when the gateway is intentionally public.
    #[serde(default = "default_require_common_auth")]
    pub require_common_auth: bool,
    #[serde(default)]
    pub allow_work: bool,
    #[serde(default)]
    pub allow_control: bool,
    pub auth_public_key: Option<String>,
    #[serde(default = "default_discovery")]
    pub enable_discovery: bool,
    /// Serve the opt-in browser landing page and API reference.
    #[serde(default)]
    pub enable_browser_ui: bool,
    /// Emit safe JSON-RPC and SSE lifecycle diagnostics.
    #[serde(default)]
    pub log_rpc: bool,
    #[serde(default = "default_cors_origins")]
    pub cors_origins: Vec<String>,
    /// Full public JSON-RPC endpoint advertised in the OpenRPC and AsyncAPI documents.
    /// Set this when a proxy or tunnel fronts the gateway; omit it to advertise `listen`.
    pub public_url: Option<String>,
    pub tls_cert: Option<String>,
    pub tls_key: Option<String>,
}

fn default_listen() -> String {
    "127.0.0.1:8090".into()
}
fn default_rpc_urls() -> Vec<String> {
    vec!["http://127.0.0.1:7076".into()]
}
fn default_ws_urls() -> Vec<String> {
    vec!["ws://127.0.0.1:7078".into()]
}
fn default_profile() -> String {
    "nano-node/V28.2".into()
}
fn default_require_common_auth() -> bool {
    true
}
fn default_discovery() -> bool {
    true
}
fn default_cors_origins() -> Vec<String> {
    vec!["*".into()]
}

impl Default for Config {
    fn default() -> Self {
        serde_yaml::from_str(DEFAULT_CONFIG).expect("default config is valid")
    }
}

impl Config {
    pub fn load(path: &Path) -> Result<Self, GatewayError> {
        match std::fs::read_to_string(path) {
            Ok(contents) => {
                let dotenv_path = path
                    .parent()
                    .filter(|parent| !parent.as_os_str().is_empty())
                    .map(|parent| parent.join(".env"))
                    .unwrap_or_else(|| Path::new(".").join(".env"));
                let dotenv = load_dotenv(&dotenv_path)?;
                let contents = expand_config_variables(&contents, |name| {
                    std::env::var(name)
                        .ok()
                        .or_else(|| dotenv.get(name).cloned())
                })?;
                serde_yaml::from_str::<Self>(&contents)
                    .map_err(GatewayError::Config)
                    .and_then(|config| config.validate())
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                if let Some(parent) = path
                    .parent()
                    .filter(|parent| !parent.as_os_str().is_empty())
                {
                    std::fs::create_dir_all(parent).map_err(GatewayError::Io)?;
                }
                std::fs::write(path, DEFAULT_CONFIG).map_err(GatewayError::Io)?;
                Ok(Self::default())
            }
            Err(error) => Err(GatewayError::Io(error)),
        }
    }

    pub fn validate(self) -> Result<Self, GatewayError> {
        if self.node_rpc_urls.is_empty() || self.node_ws_urls.is_empty() {
            return Err(GatewayError::InvalidRequest(
                "node_rpc_urls and node_ws_urls must each be non-empty".into(),
            ));
        }
        for endpoint in &self.node_rpc_urls {
            validate_upstream_url(endpoint, &["http", "https"])?;
        }
        for endpoint in &self.node_ws_urls {
            validate_upstream_url(endpoint, &["ws", "wss"])?;
        }
        if self.tls_cert.is_some() != self.tls_key.is_some() {
            return Err(GatewayError::InvalidRequest(
                "tls_cert and tls_key must be configured together".into(),
            ));
        }
        for origin in &self.cors_origins {
            HeaderValue::from_str(origin).map_err(|error| {
                GatewayError::InvalidRequest(format!("invalid CORS origin: {error}"))
            })?;
        }
        if let Some(public_url) = &self.public_url {
            validate_upstream_url(public_url, &["http", "https"]).map_err(|error| {
                GatewayError::InvalidRequest(format!("invalid public_url: {error}"))
            })?;
        }
        Ok(self)
    }
}

fn load_dotenv(path: &Path) -> Result<HashMap<String, String>, GatewayError> {
    let contents = match std::fs::read_to_string(path) {
        Ok(contents) => contents,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(HashMap::new()),
        Err(error) => return Err(GatewayError::Io(error)),
    };
    let mut values = HashMap::new();
    for (line_number, line) in contents.lines().enumerate() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let line = line.strip_prefix("export ").unwrap_or(line).trim();
        let Some((name, value)) = line.split_once('=') else {
            return Err(GatewayError::ConfigValue(format!(
                "invalid .env entry at line {}",
                line_number + 1
            )));
        };
        if !is_environment_name(name) {
            return Err(GatewayError::ConfigValue(format!(
                "invalid .env variable name at line {}",
                line_number + 1
            )));
        }
        let value = value.trim();
        let value = match value.chars().next() {
            Some(quote @ ('\'' | '\"')) => value
                .strip_prefix(quote)
                .and_then(|value| value.strip_suffix(quote))
                .ok_or_else(|| {
                    GatewayError::ConfigValue(format!(
                        "unterminated .env value at line {}",
                        line_number + 1
                    ))
                })?,
            _ => value,
        };
        values.insert(name.into(), value.into());
    }
    Ok(values)
}

fn expand_config_variables<F>(contents: &str, mut lookup: F) -> Result<String, GatewayError>
where
    F: FnMut(&str) -> Option<String>,
{
    let mut expanded = String::with_capacity(contents.len());
    let mut remaining = contents;
    while let Some(start) = remaining.find("${") {
        expanded.push_str(&remaining[..start]);
        let variable = &remaining[start + 2..];
        let Some(end) = variable.find('}') else {
            return Err(GatewayError::ConfigValue(
                "unterminated configuration variable".into(),
            ));
        };
        let name = &variable[..end];
        if !is_environment_name(name) {
            return Err(GatewayError::ConfigValue(
                "invalid configuration variable name".into(),
            ));
        }
        let value = lookup(name).ok_or_else(|| {
            GatewayError::ConfigValue(format!("configuration variable {name} is not set"))
        })?;
        expanded.push_str(&value);
        remaining = &variable[end + 1..];
    }
    expanded.push_str(remaining);
    Ok(expanded)
}

fn is_environment_name(name: &str) -> bool {
    let mut characters = name.bytes();
    matches!(characters.next(), Some(b'A'..=b'Z' | b'a'..=b'z' | b'_'))
        && characters
            .all(|character| matches!(character, b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'_'))
}

#[derive(Debug, Error)]
pub enum GatewayError {
    #[error("configuration: {0}")]
    Config(serde_yaml::Error),
    #[error("configuration: {0}")]
    ConfigValue(String),
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
    #[error("upstream: {0}")]
    Upstream(String),
    #[error("upstream rejected request: {0}")]
    UpstreamRejected(String),
    /// The node answered with its own `{"error": ...}` object. The inner string is
    /// the node's message verbatim and becomes the JSON-RPC `message` unchanged.
    ///
    /// A rejected call says nothing about upstream health — a caller can submit an
    /// invalid block indefinitely without the node being any less able to serve — so
    /// this variant leaves routing untouched.
    #[error("node reported an error: {0}")]
    NodeRejected(String),
    /// The node reported that it is rate limiting this caller. The message is still
    /// the node's own string, but the upstream also leaves the rotation briefly
    /// because this is a capacity condition rather than a caller error.
    #[error("node rate limited the request: {0}")]
    NodeRateLimited(String),
    #[error("upstream unavailable: {0}")]
    UpstreamUnavailable(String),
    #[error("upstream transport: {0}")]
    UpstreamTransport(String),
    #[error("invalid request: {0}")]
    InvalidRequest(String),
    #[error("authentication: {0}")]
    Auth(String),
    /// An operator guard refused the call before it reached the node. Surfaced as a
    /// transport-level rejection, never as a JSON-RPC error body.
    #[error("rate limited: {0}")]
    RateLimited(String),
}

/// Translation-layer item 2: one fixed code for every error the node itself reported.
/// The inner string is the node's `error` value verbatim, so this code is reserved
/// exclusively for it and never carries a gateway-authored message.
pub const NODE_ERROR_CODE: i32 = -32000;

/// Translation-layer item 2: a failure that never reached the node, or that the
/// gateway detected on an upstream response, uses the standard JSON-RPC internal
/// error code rather than the node's reserved space. The message is the gateway's
/// own, never the node's error string.
pub const GATEWAY_ERROR_CODE: i32 = -32603;

/// A rejection that never reached the node, so it cannot be a JSON-RPC error body.
///
/// Translation-layer item 6 requires this separation: a refused call must not put a
/// gateway-authored condition into the node's error space.
pub struct TransportRejection {
    pub status: StatusCode,
    pub message: String,
    pub retry_after: Option<u64>,
}

impl TransportRejection {
    pub fn unauthorized() -> Self {
        Self {
            status: StatusCode::UNAUTHORIZED,
            message: "Unauthorized".into(),
            retry_after: None,
        }
    }

    pub fn forbidden(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::FORBIDDEN,
            message: message.into(),
            retry_after: None,
        }
    }

    pub fn rate_limited(message: impl Into<String>, retry_after: u64) -> Self {
        Self {
            status: StatusCode::TOO_MANY_REQUESTS,
            message: message.into(),
            retry_after: Some(retry_after),
        }
    }

    pub fn not_found(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::NOT_FOUND,
            message: message.into(),
            retry_after: None,
        }
    }

    pub fn gone(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::GONE,
            message: message.into(),
            retry_after: None,
        }
    }

    pub fn into_response(self) -> Response {
        let mut response = (self.status, Json(json!({"error": self.message}))).into_response();
        if let Some(retry_after) = self.retry_after {
            if let Ok(value) = HeaderValue::from_str(&retry_after.to_string()) {
                response.headers_mut().insert(header::RETRY_AFTER, value);
            }
        }
        response
    }
}

impl IntoResponse for TransportRejection {
    fn into_response(self) -> Response {
        self.into_response()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RpcRequest {
    pub jsonrpc: String,
    pub method: String,
    #[serde(default)]
    pub params: Option<Value>,
    #[serde(skip)]
    pub id_present: bool,
    pub id: Option<Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RpcResponse {
    pub jsonrpc: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub result: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<RpcError>,
    pub id: Value,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RpcError {
    pub code: i32,
    pub message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub data: Option<Value>,
}

impl RpcResponse {
    fn ok(id: Option<Value>, value: Value) -> Self {
        Self {
            jsonrpc: "2.0".into(),
            result: Some(value),
            error: None,
            id: id.unwrap_or(Value::Null),
        }
    }
    fn err(id: Option<Value>, code: i32, message: impl Into<String>) -> Self {
        Self::err_with_data(id, code, message, None)
    }
    fn err_with_data(
        id: Option<Value>,
        code: i32,
        message: impl Into<String>,
        data: Option<Value>,
    ) -> Self {
        Self {
            jsonrpc: "2.0".into(),
            result: None,
            error: Some(RpcError {
                code,
                message: message.into(),
                data,
            }),
            id: id.unwrap_or(Value::Null),
        }
    }
}

#[derive(Debug, Clone)]
pub struct MethodSpec {
    pub name: &'static str,
    pub scope: Scope,
    pub params: &'static str,
    pub result: &'static str,
    pub description: &'static str,
    pub schema_provenance: &'static str,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Scope {
    Base,
    Common,
    Work,
    Control,
}

pub fn registry() -> Vec<MethodSpec> {
    vec![
        MethodSpec {
            name: "version",
            scope: Scope::Base,
            params: "EmptyParams",
            result: "VersionResult",
            description: "Read the upstream node version.",
            schema_provenance: "profiles/nano-node-v28.2.yaml",
        },
        MethodSpec {
            name: "block_count",
            scope: Scope::Base,
            params: "EmptyParams",
            result: "BlockCountResult",
            description: "Read upstream ledger block counts.",
            schema_provenance: "profiles/nano-node-v28.2.yaml",
        },
        MethodSpec {
            name: "account_info",
            scope: Scope::Base,
            params: "AccountInfoParams",
            result: "AccountInfoResult",
            description: "Read current and confirmed frontier state for one account.",
            schema_provenance: "profiles/nano-node-v28.2.yaml",
        },
        MethodSpec {
            name: "receivable",
            scope: Scope::Base,
            params: "ReceivableParams",
            result: "ReceivableResult",
            description: "Read receivable blocks for an account.",
            schema_provenance: "profiles/nano-node-v28.2.yaml",
        },
        MethodSpec {
            name: "account_balance",
            scope: Scope::Base,
            params: "AccountBalanceParams",
            result: "AccountBalanceResult",
            description: "Read balance and receivable state for one account.",
            schema_provenance: "profiles/nano-node-v28.2.yaml",
        },
        MethodSpec {
            name: "accounts_balances",
            scope: Scope::Base,
            params: "AccountsParams",
            result: "AccountsBalancesResult",
            description: "Read balances and receivables for several accounts.",
            schema_provenance: "profiles/nano-node-v28.2.yaml",
        },
        MethodSpec {
            name: "account_history",
            scope: Scope::Base,
            params: "AccountHistoryParams",
            result: "AccountHistoryResult",
            description: "Read account history.",
            schema_provenance: "profiles/nano-node-v28.2.yaml",
        },
        MethodSpec {
            name: "block_info",
            scope: Scope::Base,
            params: "BlockParams",
            result: "BlockInfoResult",
            description: "Read block metadata and whether the block is cemented.",
            schema_provenance: "profiles/nano-node-v28.2.yaml",
        },
        MethodSpec {
            name: "blocks_info",
            scope: Scope::Base,
            params: "BlocksParams",
            result: "BlocksInfoResult",
            description: "Read metadata for several blocks.",
            schema_provenance: "profiles/nano-node-v28.2.yaml",
        },
        MethodSpec {
            name: "accounts_frontiers",
            scope: Scope::Base,
            params: "AccountsFrontiersParams",
            result: "AccountsFrontiersResult",
            description: "Read the head block hash for each account.",
            schema_provenance: "profiles/nano-node-v28.2.yaml",
        },
        MethodSpec {
            name: "accounts_receivable",
            scope: Scope::Base,
            params: "AccountsReceivableParams",
            result: "AccountsReceivableResult",
            description: "Read receivable blocks for several accounts.",
            schema_provenance: "profiles/nano-node-v28.2.yaml",
        },
        MethodSpec {
            name: "block_confirm",
            scope: Scope::Base,
            params: "BlockConfirmParams",
            result: "BlockConfirmResult",
            description: "Request confirmation for a block from online representatives.",
            schema_provenance: "profiles/nano-node-v28.2.yaml",
        },
        MethodSpec {
            name: "process",
            scope: Scope::Common,
            params: "ProcessParams",
            result: "ProcessResult",
            description: "Submit a precomputed block and return its hash.",
            schema_provenance: "profiles/nano-node-v28.2.yaml",
        },
        MethodSpec {
            name: "work_generate",
            scope: Scope::Work,
            params: "WorkGenerateParams",
            result: "WorkGenerateResult",
            description: "Generate proof of work for a block. Requires a Work-scope bearer token and allow_work.",
            schema_provenance: "https://docs.nano.org/commands/rpc-protocol/#work_generate",
        },
        MethodSpec {
            name: "work_cancel",
            scope: Scope::Control,
            params: "WorkCancelParams",
            result: "WorkCancelResult",
            description: "Stop generating work for a block. Requires a Control-scope bearer token and allow_control.",
            schema_provenance: "https://docs.nano.org/commands/rpc-protocol/#work_cancel",
        },
        MethodSpec {
            name: "work_validate",
            scope: Scope::Common,
            params: "WorkValidateParams",
            result: "WorkValidateResult",
            description: "Check whether work is valid for a block. Authentication follows the Common-method policy.",
            schema_provenance: "https://docs.nano.org/commands/rpc-protocol/#work_validate",
        },
    ]
}

pub fn openrpc_document(
    profile: &str,
    include_work: bool,
    include_discovery: bool,
    gateway_url: &str,
) -> Value {
    openrpc_document_with_auth(
        profile,
        include_work,
        false,
        include_discovery,
        gateway_url,
        true,
    )
}

/// Build the runtime discovery document, including whether Common methods are public.
pub fn openrpc_document_with_auth(
    profile: &str,
    include_work: bool,
    include_control: bool,
    include_discovery: bool,
    gateway_url: &str,
    require_common_auth: bool,
) -> Value {
    let mut document = build_openrpc_document(
        profile,
        include_work,
        include_control,
        include_discovery,
        gateway_url,
    );
    document["x-nano-common-authentication"] = json!({"required": require_common_auth});
    if !require_common_auth {
        if let Some(methods) = document["methods"].as_array_mut() {
            for method in methods
                .iter_mut()
                .filter(|method| method["name"] == "process" || method["name"] == "work_validate")
            {
                method["x-nano-capability"]["requires"] = json!([]);
            }
        }
    }
    let digest = openrpc_artifact_digest(profile, include_work, include_control, include_discovery);
    if let Some(object) = document.as_object_mut() {
        object.insert("x-nano-artifact-sha256".into(), Value::String(digest));
    }
    document
}

pub fn openrpc_artifact_digest(
    profile: &str,
    include_work: bool,
    include_control: bool,
    include_discovery: bool,
) -> String {
    let document = build_openrpc_document(
        profile,
        include_work,
        include_control,
        include_discovery,
        "https://gateway.invalid/rpc",
    );
    let bytes = serde_json::to_vec(&document).unwrap_or_default();
    format!("sha256:{:x}", Sha256::digest(bytes))
}

#[derive(Clone, Copy)]
struct EventSpec {
    name: &'static str,
    summary: &'static str,
    params_schema: &'static str,
}

fn event_registry() -> [EventSpec; 1] {
    [EventSpec {
        name: "nano.confirmation",
        summary: "A block from the configured Nano profile has been cemented.",
        params_schema: "ConfirmationParams",
    }]
}

/// Build the receive-only AsyncAPI contract for confirmation SSE consumers.
pub fn asyncapi_document(profile: &str, gateway_url: &str) -> Value {
    let parsed_gateway = Url::parse(gateway_url).ok();
    let protocol = parsed_gateway.as_ref().map_or("https", |url| url.scheme());
    let host = parsed_gateway.as_ref().map_or_else(
        || gateway_url.to_owned(),
        |url| {
            let port = url
                .port()
                .map_or_else(String::new, |port| format!(":{port}"));
            format!("{}{}", url.host_str().unwrap_or("gateway.invalid"), port)
        },
    );
    let messages = event_registry()
        .into_iter()
        .map(|event| {
            (
                event.name.to_owned(),
                json!({
                    "name": event.name,
                    "title": event.name,
                    "summary": event.summary,
                    "contentType": "application/json",
                    "payload": notification_schema(event.name, event.params_schema),
                    "examples": [{"payload": notification_example(event.name)}]
                }),
            )
        })
        .collect::<serde_json::Map<_, _>>();
    json!({
        "asyncapi": "3.1.0",
        "info": {
            "title": "Nano Gateway SSE Confirmation Events",
            "version": "0.1.0",
            "description": "Receive-only SSE notifications for a pinned Nano node profile. Subscribe before submitting a block, deduplicate notifications by hash, and reconcile through block_info if confirmation is not observed. Notifications identify account activity; call accounts_balances on the Nano node's custom JSON RPC interface for an event-triggered batch snapshot of balance and receivable state."
        },
        "defaultContentType": "application/json",
        "servers": {"gateway": {"host": host, "protocol": protocol, "pathname": "/events/confirmations"}},
        "channels": {
            "confirmations": {
                "address": "/events/confirmations",
                "title": "Confirmation event stream",
                "description": "Establish this stream before calling process. The accounts filter matches a confirmed block owned by a tracked account (including sends and receives), and a state send whose link_as_account is a tracked account. For a send, params.hash equals the process result hash. Deduplicate by hash. When the upstream WebSocket drops the stream closes, as a node closing a WebSocket would; there is no replay and no gap announcement, so reconcile with accounts_frontiers and account_history before applying later events. The filter cannot be changed on a live stream: the node changes one with an update message carrying accounts_add and accounts_del, and SSE carries no client-to-server message. A client that needs to change its tracked set opens a new stream and reconciles afterwards. A client with more accounts than fit in a request line splits them across several streams, or POSTs the same options as a JSON body.",

                "servers": [{"$ref": "#/servers/gateway"}],
                "messages": {
                    "nanoConfirmation": {"$ref": "#/components/messages/nano.confirmation"}
                },
                "bindings": {"http": {"bindingVersion": "0.3.0"}}
            }
        },
        "operations": {
            "receiveConfirmations": {
                "action": "receive",
                "summary": "Receive cemented Nano account confirmations.",
                "description": "GET carries the account filter in the request line, which is what EventSource speaks and needs no custom client code. It is bounded by what an intermediary accepts as a request line, commonly about 8 KiB, so roughly a hundred accounts.",
                "channel": {"$ref": "#/channels/confirmations"},
                "bindings": {"http": {
                    "method": "GET",
                    "query": {"type": "object", "properties": {
                        "accounts": {"type": "string", "description": "Comma-separated account filter."}
                    }},
                    "x-nano-filter-ceiling": {
                        "maxAccounts": MAX_ACCOUNT_FILTER_ITEMS,
                        "maxBytes": MAX_ACCOUNT_FILTER_BYTES,
                        "why": "The node documents no cap. This bound is what an HTTP request line can carry, because nginx and Apache commonly limit one to about 8 KiB. A Nano address is 65 characters, so 110 accounts is 7259 bytes. Use POST, or split the set across several streams, to go beyond it."
                    },
                    "bindingVersion": "0.3.0"
                }}
            },
            "receiveConfirmationsWithBody": {
                "action": "receive",
                "summary": "The same stream, with the subscribe options in a JSON body.",
                "description": "EventSource only issues GET, so a client whose filter outgrows the request line uses fetch and reads the response as a stream. The response is byte-for-byte the same SSE the GET form returns. Requires custom client code.",
                "channel": {"$ref": "#/channels/confirmations"},
                "bindings": {"http": {
                    "method": "POST",
                    "query": {"$ref": "#/components/schemas/ConfirmationSubscribeOptions"},
                    "bindingVersion": "0.3.0"
                }}
            }
        },
        "components": {
            "messages": messages,
            "schemas": {
                "ConfirmationParams": confirmation_params_schema(),
                "ConfirmationSubscribeOptions": confirmation_subscribe_options()
            }
        },
        "x-nano-profile": profile,
        "x-nano-confirmation-tracking": {
            "subscribe_before_process": true,
            "correlation": {
                "process_method": "process",
                "process_result_hash": "result.hash",
                "confirmation_hash": "params.hash",
                "idempotency_key": "params.hash"
            },
            "fallback": {
                "method": "block_info",
                "params": {"hash": "process result hash"},
                "confirmed_field": "result.confirmed",
                "suggested_delay_seconds": 5,
                "sequence": [
                    "Wait about 5 seconds after a block of interest.",
                    "Call block_info with the hash and read result.confirmed.",
                    "If it is false, call block_confirm with the hash.",
                    "Expect a confirmation notification, then call block_info again.",
                    "Escalate any remaining delay in your own systems."
                ],
                "escalation": {
                    "method": "block_confirm",
                    "params": {"hash": "process result hash"},
                    "result": {"started": "Always the string 1. It reports that no error occurred, never that the block confirmed. The call also re-fires confirmation notifications for a block that is already confirmed."},
                    "rate_limited": "An operator guard answers HTTP 429 with Retry-After and no JSON-RPC body."
                }
            }
        },
        "x-nano-account-tracking": {
            "accounts_filter": "Subscribe with the watched accounts before any related process call.",
            "state_refresh": {
                "method": "accounts_balances",
                "params": {"accounts": "the watched accounts", "include_only_confirmed": true},
                "result": {"balances": "per-account balance and receivable values"},
                "when": "Read balance and receivable totals after a matching confirmation. This cannot detect every change: it returns no hashes, and a representative-change block leaves the balance unchanged. Use the frontier delta sequence below when you need to detect changes rather than totals."
            },
            "frontier_delta": {
                "when": "The canonical refresh. Three batch calls plus one account_history per changed account, whatever the size of the account set.",
                "steps": [
                    "accounts_frontiers with accounts: compare each result against the stored frontier hash.",
                    "account_history with head set to the stored hash, reverse true and raw true, for each changed account. head is inclusive, so the first entry is the head block itself: dedupe by hash rather than dropping entry 0. raw is required because the summary form skips change and epoch blocks.",
                    "accounts_receivable with source, for incoming blocks that are not yet received.",
                    "blocks_info with the new frontier hashes, and read confirmed. If false, escalate with block_confirm."
                ]
            },
            "frontier_confirmation": {
                "method": "account_info",
                "current_frontier_field": "result.frontier",
                "cemented_frontier_field": "result.confirmed_frontier",
                "status_method": "block_info",
                "status_field": "result.confirmed",
                "when": "Use this flow when the known account frontier, rather than a submitted block hash, is the starting point. Query the current frontier and inspect its confirmation status."
            },
            "not_provided": ["balance aggregation", "derived balance events", "vote events", "telemetry events"]
        },
        "x-http-response": {
            "contentType": "text/event-stream",
            "headers": {}
        }
    })
}

fn notification_schema(method: &str, params_schema: &str) -> Value {
    json!({
        "type": "object", "additionalProperties": false,
        "required": ["jsonrpc", "method", "params"],
        "properties": {
            "jsonrpc": {"const": "2.0"}, "method": {"const": method},
            "params": {"$ref": format!("#/components/schemas/{params_schema}")}
        }
    })
}

fn confirmation_params_schema() -> Value {
    json!({
        "type": "object",
        "required": ["account", "amount", "hash", "confirmation_type"],
        "properties": {
            "time": {"type": "string", "description": "Event time in milliseconds since the epoch, from the upstream envelope."},
            "hash": {"type": "string", "description": "Cemented block hash. For a submitted block, this equals process result.hash and is the idempotency key."},
            "account": {"type": "string", "description": "Confirmed block account."},
            "amount": {"type": "string", "description": "Block amount in raw."},
            "confirmation_type": {"type": "string", "enum": ["active_quorum", "active_confirmation_height", "inactive"], "description": "How the block was confirmed."},
            "block": {"type": "object", "description": "Native confirmed block content. `subtype` and `link_as_account` live here; the node keeps them inside the block.", "additionalProperties": true},
            "election_info": {
                "type": "object",
                "description": "Present when the subscription requested include_election_info.",
                "properties": {
                    "duration": {"type": "string", "description": "Election duration in milliseconds."},
                    "time": {"type": "string", "description": "End of the election, in milliseconds since the epoch."},
                    "tally": {"type": "string", "description": "Weight tally in raw."},
                    "request_count": {"type": "string"},
                    "blocks": {"type": "string"},
                    "voters": {"type": "string"}
                },
                "additionalProperties": true
            }
        },
        "additionalProperties": true
    })
}

/// The subscribe options the node documents for the confirmation topic.
///
/// They map to connect-time parameters on the subscription, so a caller chooses
/// them when it subscribes rather than per event. The gateway's own upstream
/// subscription is opened unfiltered and each subscriber's options are applied to
/// the events it receives, which reproduces the node's selection without one
/// upstream socket per distinct option set.
fn confirmation_subscribe_options() -> Value {
    json!({
        "type": "object",
        "properties": {
            "confirmation_type": {"type": "string", "enum": ["all", "active", "inactive", "active_quorum", "active_confirmation_height"], "default": "all", "description": "Which confirmations to receive, filtered on the event's own confirmation_type. The upstream guide recommends leaving this unset so a notification is less likely to be missed."},
            "accounts": {"type": "array", "items": {"type": "string"}, "description": "Only receive confirmations that mention these accounts. A state send matches on its owning account or its block's link_as_account. Legacy blocks are never delivered once a filter is given."},
            "all_local_accounts": {"type": "boolean", "default": false, "description": "Accepted and ignored. It asks for accounts held in the node's own wallet, which is a custodial dev-ledger feature the gateway does not track and does not expose. A non-custodial integrator lists its own accounts in `accounts` instead, which is the equivalent behaviour. This is the one option the gateway does not pass upstream."},
            "include_block": {"type": "boolean", "default": true, "description": "Include the block content. Omit the block from the payload when false. Incompatible with an accounts filter, as it is upstream."},
            "include_election_info": {"type": "boolean", "default": false, "description": "Add election_info to each confirmation. The gateway always requests it upstream so it is available when asked for."},
            "include_sideband_info": {"type": "boolean", "default": false, "description": "Add height and local_timestamp to the block. The gateway always requests it upstream so it is available when asked for."}
        },
        "additionalProperties": false
    })
}

fn notification_example(method: &str) -> Value {
    // The registry holds exactly one event, so the example is that event.
    notification(
        method,
        json!({
            "time": "1564935350664",
            "hash": "ABC123",
            "account": "nano_sender...",
            "amount": "10000000000000000000000000000000000",
            "confirmation_type": "active_quorum",
            "block": {"type": "state", "subtype": "send", "link_as_account": "nano_new_account..."}
        }),
    )
}

fn notification(method: &str, params: Value) -> Value {
    json!({"jsonrpc": "2.0", "method": method, "params": params})
}

/// The component schemas for the pinned profile.
///
/// Each schema is built as its own literal and collected into a map. Keeping them
/// separate keeps any single macro invocation small, and makes each definition
/// reviewable on its own against the upstream documentation.
fn component_schemas() -> serde_json::Map<String, Value> {
    let mut schemas = serde_json::Map::new();
    let mut add = |name: &str, schema: Value| {
        schemas.insert(name.to_owned(), schema);
    };

    add(
        "AccountsParams",
        json!({"type":"object","required":["accounts"],"properties":{
            "accounts":{"type":"array","minItems":1,"items":{"type":"string","minLength":1}},
            "include_only_confirmed":{"type":"boolean","default":true,"description":"Only count blocks whose confirmation height is up to date."}}}),
    );
    add(
        "AccountInfoParams",
        json!({"type":"object","required":["account"],"properties":{
            "account":{"type":"string","minLength":1},
            "representative":{"type":"boolean","default":false,"description":"Add the representative account."},
            "weight":{"type":"boolean","default":false,"description":"Add the account's voting weight."},
            "receivable":{"type":"boolean","default":false,"description":"Add the receivable total, plus confirmed_receivable when include_confirmed is also set."},
            "include_confirmed":{"type":"boolean","default":false,"description":"Add the confirmed_ fields. Without it the response carries no confirmed_* fields."}}}),
    );
    add(
        "AccountBalanceParams",
        json!({"type":"object","required":["account"],"properties":{
            "account":{"type":"string","minLength":1},
            "include_only_confirmed":{"type":"boolean","default":true,"description":"Report only blocks whose confirmation height is up to date."}}}),
    );
    add(
        "ReceivableParams",
        json!({"type":"object","required":["account"],"properties":{
            "account":{"type":"string","minLength":1},
            "count":{"type":"integer","description":"Maximum number of blocks to return."},
            "threshold":{"type":"string","description":"Only return blocks whose amount is at least this value, in raw. Selects the hash-to-amount result shape."},
            "source":{"type":"boolean","default":false,"description":"Include the sending account with each block. Selects the hash-to-object result shape."},
            "include_active":{"type":"boolean","default":false,"description":"Include blocks without a finished confirmation."},
            "min_version":{"type":"integer","description":"Only return blocks that this version (epoch) or later can pocket."},
            "sorting":{"type":"boolean","default":false,"description":"Sort blocks by amount, descending."},
            "include_only_confirmed":{"type":"boolean","default":true,"description":"Only return blocks whose confirmation height is up to date."}}}),
    );
    add(
        "AccountsFrontiersParams",
        json!({"type":"object","required":["accounts"],"properties":{
            "accounts":{"type":"array","minItems":1,"items":{"type":"string","minLength":1}}}}),
    );
    add(
        "BlockConfirmParams",
        json!({"type":"object","required":["hash"],"properties":{
            "hash":{"type":"string","minLength":1,"description":"The block to ask online representatives about."}}}),
    );
    add(
        "AccountsReceivableParams",
        json!({"type":"object","required":["accounts"],"properties":{
            "accounts":{"type":"array","minItems":1,"items":{"type":"string","minLength":1}},
            "count":{"type":"integer","description":"Maximum number of blocks to return per account."},
            "threshold":{"type":"string","description":"Only return blocks whose amount is at least this value, in raw. Selects the hash-to-amount result shape."},
            "source":{"type":"boolean","default":false,"description":"Include the sending account with each block. Selects the hash-to-object result shape."},
            "include_active":{"type":"boolean","default":false,"description":"Include blocks without a finished confirmation."},
            "sorting":{"type":"boolean","default":false,"description":"Sort each account's blocks by amount, descending."},
            "include_only_confirmed":{"type":"boolean","default":true,"description":"Only return blocks whose confirmation height is up to date."}}}),
    );
    add(
        "AccountHistoryParams",
        json!({"type":"object","required":["account"],"properties":{
            "account":{"type":"string","minLength":1},
            "head":{"type":"string","minLength":64,"maxLength":64,"description":"Start from this block instead of the account's latest, and include it as the first entry. Use it to paginate."},
            "count":{"type":"integer","description":"Maximum number of entries. -1 returns the whole chain."},
            "offset":{"type":"integer","description":"Skip this many blocks, starting from head when given."},
            "reverse":{"type":"boolean","default":false,"description":"Walk forward from head toward the frontier instead of backward. The response reports `next` in place of `previous`."},
            "raw":{"type":"boolean","default":false,"description":"Return full block content instead of a send/receive summary. Required to see change and epoch blocks, which the summary skips."},
            "account_filter":{"type":"array","items":{"type":"string","minLength":1},"description":"Only return entries connected to these accounts."},
            "include_linked_account":{"type":"boolean","default":false,"description":"Add `linked_account` to each entry, or \"0\" when there is none or the block is pruned."}}}),
    );
    add(
        "BlockParams",
        json!({"type":"object","required":["hash"],"properties":{
            "hash":{"type":"string","minLength":1},
            "json_block":{"type":"boolean","default":false,"description":"Return `contents` as a JSON subtree instead of a JSON string."},
            "include_linked_account":{"type":"boolean","default":false,"description":"Add `linked_account`, or \"0\" when there is none or the block is pruned."}}}),
    );
    add(
        "BlocksParams",
        json!({"type":"object","required":["hashes"],"properties":{
            "hashes":{"type":"array","minItems":1,"items":{"type":"string","minLength":1}},
            "json_block":{"type":"boolean","default":false,"description":"Return `contents` as a JSON subtree instead of a JSON string."},
            "include_not_found":{"type":"boolean","default":false,"description":"Report unknown hashes in `blocks_not_found` instead of failing."},
            "receive_hash":{"type":"boolean","default":false,"description":"For send blocks, add the matching receive block hash."},
            "source":{"type":"boolean","default":false,"description":"Add `source_account` for receive and open blocks, and \"0\" for send and change blocks."},
            "include_linked_account":{"type":"boolean","default":false,"description":"Add `linked_account`, or \"0\" when there is none or the block is pruned."}}}),
    );
    add(
        "ProcessParams",
        json!({"type":"object","required":["block"],"properties":{
            "block":{"type":"object","additionalProperties":true},
            "subtype":{"type":"string","enum":["send","receive","open","change","epoch"],"description":"Extra check on the block's intent. Strongly recommended, and required in a future node version."},
            "force":{"type":"boolean","default":false,"description":"Force fork resolution when the block is not accepted as the fork."},
            "async":{"type":"boolean","default":false,"description":"Queue the block and return `started` immediately instead of waiting. The outcome arrives on the `new_unconfirmed_block` topic."},
            "json_block":{"type":"boolean","default":false,"description":"Send `block` as a JSON subtree instead of a JSON string."}}}),
    );
    add(
        "WorkGenerateParams",
        json!({"type":"object","required":["hash"],"properties":{
            "hash":{"type":"string","minLength":1},
            "use_peers":{"type":"boolean","default":false,"description":"Query configured work peers when generating work."},
            "difficulty":{"type":"string","description":"Optional 16-digit hexadecimal work difficulty."},
            "multiplier":{"type":"number","exclusiveMinimum":0,"description":"Optional positive multiplier; overrides difficulty."},
            "account":{"type":"string","description":"Nano account forwarded to work peers when use_peers is true."},
            "version":{"type":"string","enum":["work_1"]},
            "block":{"description":"Optional block for selecting difficulty; a JSON string or object.","oneOf":[{"type":"string"},{"type":"object"}]},
            "json_block":{"type":"boolean","default":false,"description":"Interpret block as a JSON object."}}}),
    );
    add(
        "WorkCancelParams",
        json!({"type":"object","required":["hash"],"properties":{
            "hash":{"type":"string","minLength":1,"description":"Block hash whose work generation should stop."}}}),
    );
    add(
        "WorkValidateParams",
        json!({"type":"object","required":["hash","work"],"properties":{
            "hash":{"type":"string","minLength":1},
            "work":{"type":"string","minLength":1},
            "difficulty":{"type":"string","description":"Optional 16-digit hexadecimal difficulty."},
            "multiplier":{"type":"number","exclusiveMinimum":0,"description":"Optional positive multiplier; overrides difficulty."},
            "version":{"type":"string","enum":["work_1"]}}}),
    );
    add(
        "EmptyParams",
        json!({"type":"object","properties":{},"additionalProperties":false}),
    );

    add(
        "VersionResult",
        json!({"type":"object","required":["rpc_version"],"properties":{
            "rpc_version":{"type":"string"},
            "store_version":{"type":"string"},
            "protocol_version":{"type":"string"},
            "node_vendor":{"type":"string"},
            "store_vendor":{"type":"string"},
            "network":{"type":"string"},
            "network_identifier":{"type":"string","description":"Hash of the genesis open block."},
            "build_info":{"type":"string"}}}),
    );
    add(
        "BlockCountResult",
        json!({"type":"object","required":["count","unchecked","cemented"],"properties":{
            "count":{"type":"string"},"unchecked":{"type":"string"},"cemented":{"type":"string"}}}),
    );
    add(
        "AccountInfoResult",
        json!({"type":"object","required":["frontier","open_block","representative_block","balance","modified_timestamp","block_count","account_version"],"description":"Fields beyond this set appear only when the matching request parameter is set.","properties":{
            "frontier":{"type":"string","description":"Current frontier hash. It can be ahead of cemented state."},
            "open_block":{"type":"string"},
            "representative_block":{"type":"string"},
            "balance":{"type":"string","description":"Balance at frontier, in raw. It can include unconfirmed state."},
            "modified_timestamp":{"type":"string"},
            "block_count":{"type":"string"},
            "account_version":{"type":"string"},
            "confirmation_height":{"type":"string","description":"Cemented blocks on the account chain."},
            "confirmation_height_frontier":{"type":"string","description":"The block hash at confirmation_height."},
            "representative":{"type":"string","description":"Present when `representative` was requested."},
            "weight":{"type":"string","description":"Present when `weight` was requested."},
            "receivable":{"type":"string","description":"Present when `receivable` was requested."},
            "confirmed_frontier":{"type":"string","description":"Present when `include_confirmed` was requested. Confirm a frontier with blocks_info before treating it as final."},
            "confirmed_balance":{"type":"string","description":"Present when `include_confirmed` was requested."},
            "confirmed_height":{"type":"string","description":"Present when `include_confirmed` was requested. Same value as confirmation_height."},
            "confirmed_representative":{"type":"string","description":"Present when both `representative` and `include_confirmed` were requested."},
            "confirmed_receivable":{"type":"string","description":"Present when both `receivable` and `include_confirmed` were requested."}}}),
    );
    add(
        "AccountBalanceResult",
        json!({"type":"object","required":["balance","receivable"],"properties":{
            "balance":{"type":"string","description":"Balance in raw. Confirmed only when include_only_confirmed is true."},
            "receivable":{"type":"string","description":"Incoming amount not yet received, in raw."}}}),
    );
    add(
        "AccountsBalancesResult",
        json!({"type":"object","description":"Both maps are optional; the node omits one when no entry applies to it. An account absent from the ledger reports a zero balance and zero receivable rather than an error.","properties":{
            "balances":{"type":"object","description":"One entry per requested account. This is not an aggregate.","additionalProperties":{"$ref":"#/components/schemas/AccountBalanceResult"}},
            "errors":{"type":"object","description":"Per-account error message.","additionalProperties":{"type":"string"}}}}),
    );
    add(
        "AccountsFrontiersResult",
        json!({"type":"object","description":"Both maps are optional. The node omits `frontiers` when every requested account failed and omits `errors` when none did. A head block can be unconfirmed, so confirm it with blocks_info and read `confirmed` before treating it as final.","properties":{
            "frontiers":{"type":"object","description":"Account to head block hash.","additionalProperties":{"type":"string"}},
            "errors":{"type":"object","description":"Account to the node's message for that account.","additionalProperties":{"type":"string"}}}}),
    );
    add(
        "BlockInfoResult",
        json!({"type":"object","required":["block_account","amount","balance","height","local_timestamp","confirmed"],"description":"`subtype` is present for state blocks only, and is the source of truth for a state block's kind.","properties":{
            "block_account":{"type":"string","description":"Account that owns the block."},
            "amount":{"type":"string","description":"Block amount in raw."},
            "balance":{"type":"string","description":"Account balance after this block, in raw."},
            "height":{"type":"string"},
            "local_timestamp":{"type":"string"},
            "confirmed":{"type":"string","description":"\"true\" only when the node reports the block cemented."},
            "successor":{"type":"string","description":"The block that superseded this one."},
            "subtype":{"type":"string","enum":["send","receive","change","epoch"]},
            "contents":{"description":"Block content. A JSON string unless `json_block` was requested."},
            "linked_account":{"type":"string","description":"Present when include_linked_account was requested; \"0\" when there is none."},
            "source_account":{"type":"string"},
            "receive_hash":{"type":"string"}}}),
    );
    add(
        "BlocksInfoResult",
        json!({"type":"object","required":["blocks"],"properties":{
            "blocks":{"type":"object","additionalProperties":{"$ref":"#/components/schemas/BlockInfoResult"}},
            "blocks_not_found":{"type":"array","items":{"type":"string"},"description":"Present when include_not_found was requested."}}}),
    );
    add(
        "BlockConfirmResult",
        json!({"type":"object","required":["started"],"properties":{
            "started":{"type":"string","const":"1"}}}),
    );
    add(
        "ProcessResult",
        json!({"oneOf":[
            {"type":"object","required":["hash"],"properties":{"hash":{"type":"string"}}},
            {"type":"object","required":["started"],"description":"Returned when `async` was requested. The block was queued, not processed; the outcome arrives on the `new_unconfirmed_block` topic.","properties":{"started":{"type":"string","const":"1"}}}]}),
    );
    add(
        "WorkGenerateResult",
        json!({"type":"object","required":["work","difficulty","multiplier","hash"],"properties":{
            "work":{"type":"string"},"difficulty":{"type":"string"},
            "multiplier":{"type":"string"},"hash":{"type":"string"}}}),
    );
    add(
        "WorkCancelResult",
        json!({"type":"object","required":["success"],"properties":{
            "success":{"type":"string","const":""}}}),
    );
    add(
        "WorkValidateResult",
        json!({"type":"object","required":["valid_all","valid_receive","difficulty","multiplier"],"properties":{
            "valid":{"type":"string","enum":["0","1"],"description":"Present when difficulty or multiplier was supplied."},
            "valid_all":{"type":"string","enum":["0","1"]},
            "valid_receive":{"type":"string","enum":["0","1"]},
            "difficulty":{"type":"string"},"multiplier":{"type":"string"}}}),
    );
    add(
        "HistoryEntry",
        json!({"type":"object","required":["type","account","amount","hash"],"description":"Without `raw`, `type` is `send` or `receive` and change and epoch blocks are skipped. With `raw`, it is the block type and every block field is present. For an open block in raw mode, `account` is the source block's account.","properties":{
            "type":{"type":"string"},
            "account":{"type":"string"},
            "amount":{"type":"string"},
            "local_timestamp":{"type":"string"},
            "height":{"type":"string"},
            "hash":{"type":"string"},
            "confirmed":{"type":"string","description":"\"true\" when the node reports the entry cemented."},
            "subtype":{"type":"string","description":"State blocks only. The source of truth for the block's kind."},
            "linked_account":{"type":"string","description":"Present when include_linked_account was requested."},
            "previous":{"type":"string","description":"Hash of the preceding block on the account chain."},
            "representative":{"type":"string"},
            "balance":{"type":"string"},
            "link":{"type":"string"},
            "link_as_account":{"type":"string"},
            "signature":{"type":"string"},
            "work":{"type":"string"}}}),
    );
    add(
        "AccountHistoryResult",
        json!({"type":"object","required":["account","history"],"properties":{
            "account":{"type":"string"},
            "history":{"type":"array","items":{"$ref":"#/components/schemas/HistoryEntry"}},
            "previous":{"type":"string","description":"Present only when `count` truncated the result: the hash to pass as the next `head`."},
            "next":{"type":"string","description":"Present in place of `previous` when `reverse` was requested."}}}),
    );
    add(
        "ReceivableSource",
        json!({"type":"object","required":["amount","source"],"properties":{
            "amount":{"type":"string"},"source":{"type":"string"}}}),
    );
    add(
        "ReceivableBlocks",
        json!({"description":"The node selects one of three shapes from the request parameters: an array of hashes by default, hash to amount with `threshold`, and hash to amount and source account with `source`.","oneOf":[
            {"type":"array","items":{"type":"string"}},
            {"type":"object","additionalProperties":{"type":"string"}},
            {"type":"object","additionalProperties":{"$ref":"#/components/schemas/ReceivableSource"}}]}),
    );
    add(
        "ReceivableResult",
        json!({"type":"object","required":["blocks"],"properties":{
            "blocks":{"$ref":"#/components/schemas/ReceivableBlocks"}}}),
    );
    add(
        "AccountsReceivableResult",
        json!({"type":"object","required":["blocks"],"properties":{
            "blocks":{"type":"object","description":"Account to that account's receivable blocks.","additionalProperties":{"$ref":"#/components/schemas/ReceivableBlocks"}}}}),
    );
    schemas
}

fn build_openrpc_document(
    profile: &str,
    include_work: bool,
    include_control: bool,
    include_discovery: bool,
    gateway_url: &str,
) -> Value {
    let mut methods = registry()
        .into_iter()
        .filter(|method| match method.scope {
            Scope::Work => include_work,
            Scope::Control => include_control,
            _ => true,
        })
        .map(openrpc_method)
        .collect::<Vec<_>>();
    if include_discovery {
        methods.push(json!({
            "name": "rpc.discover",
            "summary": "Return the OpenRPC document.",
            "params": [],
            "result": {"name": "result", "schema": {"type": "object", "properties": {}, "additionalProperties": true}}
        }));
    }
    // Errors live under components, where the OpenRPC meta-schema expects them and
    // where each method's `#/components/errors/...` reference resolves.
    let mut components = serde_json::Map::new();
    components.insert("schemas".into(), Value::Object(component_schemas()));
    components.insert(
        "errors".into(),
        json!({
            "InvalidRequest": {"code": -32602, "message": "Invalid method parameters"},
            "MethodNotFound": {"code": -32601, "message": "Method not found"},
            "NodeError": {"code": -32000, "message": "The node reported an error. This code is reserved for node-reported errors, and the message is the node's own string unchanged."},
            "InternalError": {"code": -32603, "message": "The gateway could not complete the call. The node may not have been reached, or its response did not match the pinned profile. The call may or may not have been processed."}
        }),
    );
    json!({
        "openrpc": "1.3.2", "info": {"title":"Nano Gateway JSON-RPC 2.0 API", "version":"0.1.0", "description":"This OpenRPC document describes the gateway's JSON-RPC 2.0 API. The gateway translates ledger methods into actions on the pinned Nano node profile's separate custom JSON RPC interface."},
        "servers": [{"name":"gateway", "url":gateway_url, "variables":{"gatewayUrl":{"default":gateway_url}}}],
        "methods": methods,
        "components": Value::Object(components), "x-nano-profile": profile
    })
}

fn openrpc_method(method: MethodSpec) -> Value {
    let mut document = json!({
        "name": method.name,
        "summary": method.description,
        "x-nano-schema-provenance": method.schema_provenance,
        // The parameter list below is derived from this schema, so a reader can check
        // the two against each other instead of taking the inline copy on trust.
        "x-nano-params-schema": {"$ref": format!("#/components/schemas/{}", method.params)},
        "paramStructure": "by-name",
        "params": openrpc_params(method.params),
        "result": {
            "name": "result",
            "schema": {"$ref": format!("#/components/schemas/{}", method.result)}
        },
        "errors": [
            {"$ref":"#/components/errors/InvalidRequest"},
            {"$ref":"#/components/errors/MethodNotFound"},
            {"$ref":"#/components/errors/NodeError"},
            {"$ref":"#/components/errors/InternalError"}
        ]
    });
    if method.name == "process" || method.name == "work_validate" {
        document["x-nano-capability"] = json!({
            "name": method.name,
            "browser_safe": method.name == "process",
            "requires": ["authenticated", "common"]
        });
    } else if method.scope == Scope::Work {
        document["x-nano-capability"] = json!({
            "name": "work",
            "browser_safe": false,
            "requires": ["authenticated", "work"],
            "enabled_by": "allow_work"
        });
    } else if method.scope == Scope::Control {
        document["x-nano-capability"] = json!({
            "name": "control",
            "browser_safe": false,
            "requires": ["authenticated", "control"],
            "enabled_by": "allow_control"
        });
    }
    document
}

/// Derive an OpenRPC parameter list from the method's component param schema.
///
/// Both spellings of the same contract live side by side: `components.schemas`
/// describes the parameters, and `methods[].params` points at them. Deriving one
/// from the other is what keeps them from drifting apart.
fn openrpc_params(params_schema: &str) -> Vec<Value> {
    let schemas = component_schemas();
    let Some(schema) = schemas.get(params_schema) else {
        return Vec::new();
    };
    let required = schema
        .get("required")
        .and_then(Value::as_array)
        .map(|names| {
            names
                .iter()
                .filter_map(Value::as_str)
                .map(ToOwned::to_owned)
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    let properties = match schema.get("properties").and_then(Value::as_object) {
        Some(properties) => properties,
        None => return Vec::new(),
    };
    properties
        .iter()
        .map(|(name, definition)| {
            json!({
                "name": name,
                "required": required.iter().any(|required| required == name),
                "schema": definition
            })
        })
        .collect()
}

#[derive(Clone)]
pub struct NativeClient {
    client: Client,
    endpoint: String,
    label: String,
    basic_auth: Option<(String, String)>,
}

impl NativeClient {
    pub fn new(endpoint: impl Into<String>) -> Result<Self, GatewayError> {
        Self::with_timeout(endpoint, Duration::from_secs(10))
    }

    pub fn with_timeout(
        endpoint: impl Into<String>,
        timeout: Duration,
    ) -> Result<Self, GatewayError> {
        let endpoint = endpoint.into();
        validate_upstream_url(&endpoint, &["http", "https"])?;
        let mut parsed = Url::parse(&endpoint).map_err(|error| {
            GatewayError::InvalidRequest(format!("invalid upstream URL: {error}"))
        })?;
        let basic_auth = if parsed.username().is_empty() {
            None
        } else {
            let username = parsed.username().to_owned();
            let password = parsed.password().unwrap_or_default().to_owned();
            parsed
                .set_username("")
                .map_err(|_| GatewayError::InvalidRequest("invalid upstream username".into()))?;
            parsed
                .set_password(None)
                .map_err(|_| GatewayError::InvalidRequest("invalid upstream password".into()))?;
            Some((username, password))
        };
        Ok(Self {
            client: Client::builder()
                .timeout(timeout)
                .redirect(reqwest::redirect::Policy::none())
                .build()
                .map_err(|e| GatewayError::Upstream(e.to_string()))?,
            endpoint: parsed.to_string(),
            label: upstream_label(&parsed),
            basic_auth,
        })
    }
    pub async fn call(&self, action: &str, params: &Value) -> Result<Value, GatewayError> {
        self.call_with_response_logging(action, params, false).await
    }

    async fn call_with_response_logging(
        &self,
        action: &str,
        params: &Value,
        log_response: bool,
    ) -> Result<Value, GatewayError> {
        let body = native_request_body(action, params);
        let redacted_endpoint = self.redacted_endpoint();
        let authorization = self.authorization_mode();
        let started = Instant::now();
        let mut request = self.client.post(&self.endpoint).json(&body);
        if let Some((username, password)) = &self.basic_auth {
            request = request.basic_auth(username, Some(password));
        }
        let response = match request.send().await {
            Ok(response) => response,
            Err(error) => {
                log_upstream_error("rpc", &self.endpoint, &error.to_string());
                return Err(if error.is_connect() {
                    GatewayError::UpstreamUnavailable(error.to_string())
                } else {
                    GatewayError::UpstreamTransport(error.to_string())
                });
            }
        };
        let status = response.status();
        if log_response {
            tracing::info!(
                target: "nano_rpc_gateway::rpc",
                event = "rpc_upstream_response",
                method = action,
                upstream = %self.label,
                upstream_url = %redacted_endpoint,
                status = status.as_u16(),
                duration_ms = started.elapsed().as_millis() as u64,
                "Nano node custom JSON RPC response >>>"
            );
        }
        if !status.is_success() {
            tracing::warn!(
                action,
                upstream = %self.label,
                upstream_url = %redacted_endpoint,
                authorization,
                %status,
                "native upstream returned an unsuccessful response"
            );
            if status.is_server_error() || status == reqwest::StatusCode::TOO_MANY_REQUESTS {
                return Err(GatewayError::UpstreamUnavailable(format!("HTTP {status}")));
            }
            return Err(GatewayError::Upstream(format!("HTTP {status}")));
        }
        let value: Value = match response.json().await {
            Ok(value) => value,
            Err(error) => {
                log_upstream_error("rpc", &self.endpoint, &error.to_string());
                return Err(GatewayError::UpstreamTransport(error.to_string()));
            }
        };
        if let Some(error) = value.get("error") {
            // Translation-layer item 2: the node reported this error, so its string
            // becomes the JSON-RPC `message` verbatim. Every node-reported error takes
            // the same path — including a rate limit, which is a node condition here
            // and an operator guard only when the gateway itself refuses the call.
            let reason = native_error_reason(error);
            if is_rate_limited_error(error) {
                tracing::warn!(
                    action,
                    upstream = %self.label,
                    upstream_url = %redacted_endpoint,
                    authorization,
                    reason = %reason,
                    "native upstream rate limited request"
                );
            } else {
                tracing::warn!(
                    action,
                    upstream = %self.label,
                    upstream_url = %redacted_endpoint,
                    authorization,
                    reason = %reason,
                    "native upstream rejected request"
                );
            }
            return Err(if is_rate_limited_error(error) {
                GatewayError::NodeRateLimited(reason)
            } else {
                GatewayError::NodeRejected(reason)
            });
        }
        Ok(value)
    }

    fn authorization_mode(&self) -> &'static str {
        if self.basic_auth.is_some() {
            "basic"
        } else {
            "none"
        }
    }

    fn redacted_endpoint(&self) -> String {
        redact_upstream_url(&self.endpoint)
    }
}

fn redact_upstream_url(value: &str) -> String {
    let Ok(mut endpoint) = Url::parse(value) else {
        return "(invalid upstream URL)".into();
    };
    let _ = endpoint.set_username("");
    let _ = endpoint.set_password(None);
    if endpoint.query().is_some() {
        let query = endpoint
            .query_pairs()
            .map(|(name, _)| format!("{name}=****"))
            .collect::<Vec<_>>()
            .join("&");
        endpoint.set_query(Some(&query));
    }
    endpoint.to_string()
}

fn log_upstream_selected(protocol: &str, endpoint: &str, selection: &str) {
    tracing::info!(
        target: "nano_rpc_gateway::upstream",
        event = "upstream_selected",
        protocol,
        upstream_url = %redact_upstream_url(endpoint),
        selection,
        "upstream selected"
    );
}

fn log_upstream_error(protocol: &str, endpoint: &str, error: &str) {
    let redacted_endpoint = redact_upstream_url(endpoint);
    let safe_error = error.replace(endpoint, &redacted_endpoint);
    tracing::warn!(
        target: "nano_rpc_gateway::upstream",
        event = "upstream_error",
        protocol,
        upstream_url = %redacted_endpoint,
        error = %safe_error,
        "upstream error"
    );
}

fn is_rate_limited_error(error: &Value) -> bool {
    match error {
        Value::Number(value) => value.as_u64() == Some(429),
        Value::String(message) => {
            let message = message.trim().to_ascii_lowercase();
            message == "429"
                || message.contains("too many requests")
                || message.contains("rate limit")
                || message.contains("rate-limit")
                || message.contains("ratelimit")
        }
        _ => false,
    }
}

fn native_error_reason(error: &Value) -> String {
    error
        .as_str()
        .map(ToOwned::to_owned)
        .unwrap_or_else(|| error.to_string())
}

fn upstream_label(url: &Url) -> String {
    let host = url.host_str().unwrap_or("(missing host)");
    match url.port() {
        Some(port) => format!("{}://{host}:{port}", url.scheme()),
        None => format!("{}://{host}", url.scheme()),
    }
}

/// Build the upstream request body.
///
/// Translation-layer item 1: `params` is the upstream request object without
/// `action`, and it is forwarded unchanged. The gateway never injects a
/// parameter, never rewrites one, and never substitutes an upstream spelling.
/// Upstream defaults therefore apply as the node documents them, which is what
/// keeps the response shape predictable for callers.
fn native_request_body(action: &str, params: &Value) -> serde_json::Map<String, Value> {
    let mut body = serde_json::Map::new();
    body.insert("action".into(), Value::String(action.to_owned()));
    if let Value::Object(values) = params {
        body.extend(values.clone());
    }
    body
}

const UPSTREAM_COOLDOWN: Duration = Duration::from_secs(15);
const WEBSOCKET_USER_AGENT: &str = "Mozilla/5.0 (compatible; nano-rpc-gateway/0.1)";
/// An acknowledged WebSocket that never forwards confirmations is unusable.
/// The upstream subscription is unfiltered, so a healthy public network must
/// produce a confirmation within this bound.
const CONFIRMATION_DELIVERY_TIMEOUT: Duration = Duration::from_secs(30);

#[derive(Clone)]
pub struct NativeRouter {
    clients: Arc<Vec<NativeClient>>,
    active: Arc<AtomicUsize>,
    unhealthy_until: Arc<Mutex<Vec<Instant>>>,
    log_responses: bool,
}

impl NativeRouter {
    pub fn new(rpc_urls: &[String]) -> Result<Self, GatewayError> {
        Self::with_response_logging(rpc_urls, false)
    }

    fn with_response_logging(
        rpc_urls: &[String],
        log_responses: bool,
    ) -> Result<Self, GatewayError> {
        let clients = rpc_urls
            .iter()
            .map(NativeClient::new)
            .collect::<Result<Vec<_>, _>>()?;
        if clients.is_empty() {
            return Err(GatewayError::InvalidRequest(
                "at least one upstream RPC URL is required".into(),
            ));
        }
        log_upstream_selected("rpc", &clients[0].endpoint, "initial");
        Ok(Self {
            clients: Arc::new(clients),
            active: Arc::new(AtomicUsize::new(0)),
            unhealthy_until: Arc::new(Mutex::new(vec![Instant::now(); rpc_urls.len()])),
            log_responses,
        })
    }

    pub fn len(&self) -> usize {
        self.clients.len()
    }

    pub fn is_empty(&self) -> bool {
        self.clients.is_empty()
    }

    pub fn active_index(&self) -> usize {
        self.active.load(Ordering::Acquire) % self.clients.len()
    }

    pub async fn call(&self, action: &str, params: &Value) -> Result<Value, GatewayError> {
        let start = self.active_index();
        let now = Instant::now();
        let unhealthy = self.unhealthy_until.lock().await.clone();
        let mut last_error = None;
        for offset in 0..self.clients.len() {
            let index = (start + offset) % self.clients.len();
            if unhealthy[index] > now {
                continue;
            }
            match self.clients[index]
                .call_with_response_logging(action, params, self.log_responses)
                .await
            {
                Ok(value) => {
                    let previous = self.active.swap(index, Ordering::Release);
                    if previous != index {
                        log_upstream_selected(
                            "rpc",
                            &self.clients[index].endpoint,
                            "active_changed",
                        );
                    }
                    return Ok(value);
                }
                Err(error) if self.cools_down(&error) => {
                    // A node-reported condition takes this upstream out of rotation for
                    // future calls, but it is not retried elsewhere: the caller gets the
                    // node's own answer rather than a different node's.
                    self.mark_unhealthy(index).await;
                    if self.can_failover(&error) {
                        last_error = Some(error);
                    } else {
                        return Err(error);
                    }
                }
                Err(error) => return Err(error),
            }
        }
        if let Some(error) = last_error {
            return Err(error);
        }
        let error = GatewayError::UpstreamUnavailable("all upstreams cooling down".into());
        let index = self.active_index();
        log_upstream_error("rpc", &self.clients[index].endpoint, &error.to_string());
        Err(error)
    }

    /// Whether this failure should take the upstream out of rotation for a while.
    /// Only capacity conditions qualify; a rejected call must not degrade service
    /// for every other caller.
    fn cools_down(&self, error: &GatewayError) -> bool {
        matches!(
            error,
            GatewayError::UpstreamUnavailable(_) | GatewayError::NodeRateLimited(_)
        )
    }

    /// Whether this failure is worth retrying against a different upstream.
    fn can_failover(&self, error: &GatewayError) -> bool {
        matches!(error, GatewayError::UpstreamUnavailable(_))
    }

    async fn mark_unhealthy(&self, index: usize) {
        self.unhealthy_until.lock().await[index] = Instant::now() + UPSTREAM_COOLDOWN;
        let next = (index + 1) % self.clients.len();
        let previous = self.active.swap(next, Ordering::Release);
        if previous != next {
            log_upstream_selected("rpc", &self.clients[next].endpoint, "active_changed");
        }
    }
}

fn validate_upstream_url(endpoint: &str, schemes: &[&str]) -> Result<(), GatewayError> {
    let parsed = Url::parse(endpoint)
        .map_err(|error| GatewayError::InvalidRequest(format!("invalid upstream URL: {error}")))?;
    if !schemes.contains(&parsed.scheme()) || parsed.host_str().is_none() {
        return Err(GatewayError::InvalidRequest(format!(
            "upstream URL must use one of {schemes:?} and include a host"
        )));
    }
    if parsed
        .password()
        .is_some_and(|password| !password.is_empty())
    {
        return Err(GatewayError::InvalidRequest(
            "upstream URL password must be empty; use the username for Nano.to API-key authentication".into(),
        ));
    }
    Ok(())
}

fn websocket_connect_request(endpoint: &str) -> Result<TungsteniteRequest<()>, GatewayError> {
    let mut request = endpoint
        .into_client_request()
        .map_err(|error| GatewayError::UpstreamUnavailable(error.to_string()))?;
    request.headers_mut().insert(
        USER_AGENT,
        TungsteniteHeaderValue::from_static(WEBSOCKET_USER_AGENT),
    );
    Ok(request)
}

#[derive(Clone)]
/// Fan-out for live confirmation events.
///
/// The node offers no replay: a WebSocket that drops is simply closed. So there is
/// no buffer to retain, no cursor to resume from and no gap to announce. A subscriber
/// that falls behind is closed rather than told about it, and it reconciles with
/// accounts_frontiers and account_history when it reconnects.
pub struct EventHub {
    tx: broadcast::Sender<NanoEvent>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NanoEvent {
    pub event: String,
    pub data: Value,
}

impl Default for EventHub {
    fn default() -> Self {
        Self::new()
    }
}

impl EventHub {
    pub fn new() -> Self {
        Self::with_capacity(SSE_QUEUE_CAPACITY)
    }
    /// A subscriber that falls this far behind is closed rather than told about it.
    pub fn with_capacity(capacity: usize) -> Self {
        let (tx, _) = broadcast::channel(capacity.max(1));
        Self { tx }
    }
    pub async fn publish(&self, event: impl Into<String>, data: Value) -> NanoEvent {
        let event = event.into();
        let item = NanoEvent { event, data };
        let _ = self.tx.send(item.clone());
        item
    }
    fn subscribe(&self) -> broadcast::Receiver<NanoEvent> {
        self.tx.subscribe()
    }
}

/// Operator guards for methods whose cost falls outside a normal read.
///
/// `block_confirm` starts or re-triggers an election and re-fires confirmation
/// notifications even for a block that is already confirmed, so it is bounded two
/// ways. Neither guard inspects block state: suppressing a call because the block
/// happens to be confirmed would change the method's behaviour, which the upstream
/// contract does not allow. A refused call returns a transport error and never a
/// synthesized `started`.
///
/// A per-client limit is deliberately absent. The gateway has no client identity it
/// can trust, so any per-client key today would either collapse every caller behind
/// one reverse proxy into a single client, or trust a header the caller controls.
/// Either gives false confidence and can lock out legitimate callers. A global bucket
/// on its own budget is the honest bound until identity exists.
#[derive(Debug)]
pub struct ConfirmGuards {
    /// Tokens per second for `block_confirm`, deliberately separate from the read
    /// budget so that using it up cannot starve account reads.
    rate_per_second: f64,
    burst: f64,
    tokens: Mutex<f64>,
    last_refill: Mutex<Instant>,
    /// Hashes the node accepted, with the time it was accepted. Bounded in both
    /// length and age, because an unbounded cooldown cache is itself an exhaustion
    /// path.
    cooldown: Mutex<VecDeque<(String, Instant)>>,
    cooldown_ttl: Duration,
    cooldown_capacity: usize,
}

impl ConfirmGuards {
    pub fn new() -> Self {
        Self {
            rate_per_second: 1.0,
            burst: 4.0,
            tokens: Mutex::new(4.0),
            last_refill: Mutex::new(Instant::now()),
            cooldown: Mutex::new(VecDeque::new()),
            cooldown_ttl: Duration::from_secs(60),
            cooldown_capacity: 1024,
        }
    }

    /// Reserve one call, or explain how long the caller should wait.
    pub async fn reserve(&self) -> Result<(), TransportRejection> {
        let now = Instant::now();
        let mut last = self.last_refill.lock().await;
        let mut tokens = self.tokens.lock().await;
        let elapsed = now.duration_since(*last).as_secs_f64();
        *tokens = (*tokens + elapsed * self.rate_per_second).min(self.burst);
        *last = now;
        if *tokens >= 1.0 {
            *tokens -= 1.0;
            return Ok(());
        }
        let deficit = 1.0 - *tokens;
        let retry_after = (deficit / self.rate_per_second).ceil().max(1.0);
        Err(TransportRejection::rate_limited(
            "block_confirm rate limit exceeded",
            retry_after as u64,
        ))
    }

    /// Reject a hash still inside its cooldown. The check runs before the call, so a
    /// refused request never reaches the node.
    pub async fn check_cooldown(&self, hash: &str) -> Result<(), TransportRejection> {
        let now = Instant::now();
        let mut cooldown = self.cooldown.lock().await;
        Self::evict(
            &mut cooldown,
            now,
            self.cooldown_ttl,
            self.cooldown_capacity,
        );
        if cooldown.iter().any(|(seen, _)| seen == hash) {
            return Err(TransportRejection::rate_limited(
                "block_confirm cooldown for this block hash is still active",
                self.cooldown_ttl.as_secs(),
            ));
        }
        Ok(())
    }

    /// Record a hash the node accepted, starting its cooldown.
    pub async fn record_accepted(&self, hash: &str) {
        let now = Instant::now();
        let mut cooldown = self.cooldown.lock().await;
        Self::evict(
            &mut cooldown,
            now,
            self.cooldown_ttl,
            self.cooldown_capacity,
        );
        if !cooldown.iter().any(|(seen, _)| seen == hash) {
            cooldown.push_back((hash.to_owned(), now));
        }
    }

    fn evict(
        entries: &mut VecDeque<(String, Instant)>,
        now: Instant,
        ttl: Duration,
        capacity: usize,
    ) {
        while let Some((_, seen)) = entries.front() {
            if now.duration_since(*seen) >= ttl {
                entries.pop_front();
            } else {
                break;
            }
        }
        while entries.len() > capacity {
            entries.pop_front();
        }
    }
}

impl Default for ConfirmGuards {
    fn default() -> Self {
        Self::new()
    }
}

/// A block hash is 64 hexadecimal characters. Checking the shape before it reaches
/// the cooldown cache keeps arbitrary caller-supplied strings out of it.
fn is_block_hash(value: &str) -> bool {
    value.len() == 64 && value.bytes().all(|byte| byte.is_ascii_hexdigit())
}

#[derive(Clone)]
pub struct AppState {
    pub config: Config,
    pub native: NativeRouter,
    pub events: EventHub,
    pub metrics: Metrics,
    pub verifying_key: Option<VerifyingKey>,
    pub upstream_ready: Arc<AtomicBool>,
    upstream_seen: Arc<AtomicBool>,
    ws_selected: Arc<Mutex<Option<String>>>,
    // RPC and WebSocket providers have independent availability and ordering.
    ws_active: Arc<AtomicUsize>,
    ws_unhealthy_until: Arc<Mutex<Vec<Instant>>>,
    pub confirm_guards: Arc<ConfirmGuards>,
}

#[derive(Clone, Default)]
pub struct Metrics {
    pub requests: Arc<AtomicU64>,
    pub errors: Arc<AtomicU64>,
    pub active_streams: Arc<AtomicU64>,
    pub overflow_resets: Arc<AtomicU64>,
    pub upstream_reconnects: Arc<AtomicU64>,
    pub request_duration_ms_sum: Arc<AtomicU64>,
    pub request_duration_ms_count: Arc<AtomicU64>,
}

struct ActiveStreamGuard {
    active_streams: Arc<AtomicU64>,
    log_rpc: bool,
    started: Instant,
    account_filter_count: usize,
}

struct RequestMetricsGuard {
    metrics: Metrics,
    started: Instant,
}

impl Drop for RequestMetricsGuard {
    fn drop(&mut self) {
        self.metrics
            .request_duration_ms_sum
            .fetch_add(self.started.elapsed().as_millis() as u64, Ordering::Relaxed);
        self.metrics
            .request_duration_ms_count
            .fetch_add(1, Ordering::Relaxed);
    }
}

impl Drop for ActiveStreamGuard {
    fn drop(&mut self) {
        self.active_streams.fetch_sub(1, Ordering::Relaxed);
        if self.log_rpc {
            tracing::info!(
                target: "nano_rpc_gateway::sse",
                event = "sse_session_closed",
                duration_ms = self.started.elapsed().as_millis() as u64,
                account_filter_count = self.account_filter_count,
                "SSE session closed"
            );
        }
    }
}

fn log_rpc_request(method: &str) {
    tracing::info!(
        target: "nano_rpc_gateway::rpc",
        event = "rpc_request",
        method,
        "JSON-RPC request <<<"
    );
}

fn sse_event(item: NanoEvent) -> Event {
    let data = serde_json::to_string(&notification(&item.event, item.data))
        .unwrap_or_else(|_| "null".into());
    Event::default().event(item.event).data(data)
}

impl AppState {
    async fn active_ws_url(&self) -> Result<(usize, String), GatewayError> {
        let ws_urls = &self.config.node_ws_urls;
        let start = self.ws_active.load(Ordering::Acquire);
        let now = Instant::now();
        let unhealthy = self.ws_unhealthy_until.lock().await.clone();
        for offset in 0..ws_urls.len() {
            let index = (start + offset) % ws_urls.len();
            if unhealthy[index] <= now {
                return Ok((index, ws_urls[index].clone()));
            }
        }
        Err(GatewayError::UpstreamUnavailable(
            "all upstreams cooling down".into(),
        ))
    }

    async fn mark_ws_unhealthy(&self, index: usize) {
        self.ws_unhealthy_until.lock().await[index] = Instant::now() + UPSTREAM_COOLDOWN;
        let next = (index + 1) % self.config.node_ws_urls.len();
        self.ws_active.store(next, Ordering::Release);
    }
    pub fn new(config: Config) -> Result<Self, GatewayError> {
        let config = config.validate()?;
        let verifying_key = config
            .auth_public_key
            .as_deref()
            .map(parse_public_key)
            .transpose()?;
        Ok(Self {
            native: NativeRouter::with_response_logging(&config.node_rpc_urls, config.log_rpc)?,
            events: EventHub::with_capacity(SSE_QUEUE_CAPACITY),
            metrics: Metrics::default(),
            ws_active: Arc::new(AtomicUsize::new(0)),
            ws_unhealthy_until: Arc::new(Mutex::new(vec![
                Instant::now();
                config.node_ws_urls.len()
            ])),
            confirm_guards: Arc::new(ConfirmGuards::new()),
            config,
            verifying_key,
            upstream_ready: Arc::new(AtomicBool::new(false)),
            upstream_seen: Arc::new(AtomicBool::new(false)),
            ws_selected: Arc::new(Mutex::new(None)),
        })
    }
    async fn dispatch(
        &self,
        request: RpcRequest,
        headers: &HeaderMap,
    ) -> Result<RpcResponse, TransportRejection> {
        if request.jsonrpc != "2.0" || request.method.is_empty() {
            return Ok(RpcResponse::err(request.id, -32600, "Invalid Request"));
        }
        if request.id_present
            && request
                .id
                .as_ref()
                .is_some_and(|id| !(id.is_string() || id.is_number() || id.is_null()))
        {
            return Ok(RpcResponse::err(
                request.id,
                -32600,
                "Invalid Request: id must be string, number, or null",
            ));
        }
        if !request.id_present {
            return Ok(RpcResponse::err(
                None,
                -32600,
                "Notifications are not supported",
            ));
        }
        if request.method == "rpc.discover" {
            return Ok(if self.config.enable_discovery {
                RpcResponse::ok(
                    request.id,
                    openrpc_document_with_auth(
                        &self.config.profile,
                        self.config.allow_work,
                        self.config.allow_control,
                        self.config.enable_discovery,
                        &self.gateway_url(),
                        self.config.require_common_auth,
                    ),
                )
            } else {
                RpcResponse::err(
                    request.id,
                    -32601,
                    "Discovery is disabled; fetch /openrpc.json",
                )
            });
        }
        let spec = registry()
            .into_iter()
            .find(|item| item.name == request.method);
        let Some(spec) = spec else {
            return Ok(RpcResponse::err(request.id, -32601, "Method not found"));
        };
        if spec.scope == Scope::Work && !self.config.allow_work {
            return Ok(RpcResponse::err(
                request.id,
                -32604,
                "Work generation is disabled",
            ));
        }
        if spec.scope == Scope::Control && !self.config.allow_control {
            return Ok(RpcResponse::err(
                request.id,
                -32604,
                "Control methods are disabled",
            ));
        }
        // An authentication failure never reaches the node, so it is answered at the
        // transport layer rather than as a node-reported condition.
        let common_auth_required = spec.scope != Scope::Common || self.config.require_common_auth;
        if spec.scope != Scope::Base
            && common_auth_required
            && !self.authorized(headers, spec.scope)
        {
            return Err(TransportRejection::unauthorized());
        }
        let params = request.params.unwrap_or_else(|| json!({}));
        if let Err(message) = validate_params(spec.name, &params) {
            return Ok(RpcResponse::err(request.id, -32602, message));
        }
        // Translation-layer item 6: an operator guard refuses the call at the
        // transport layer, so it can never appear as a JSON-RPC error body.
        self.guard(spec.name, &params).await?;
        match self.native.call(spec.name, &params).await {
            // Forward upstream results unchanged except for the guarded work_generate
            // alias normalization below, then validate against the pinned profile.
            Ok(mut result) => {
                // Only a hash the node accepted starts a cooldown. Recording a
                // rejected hash would let a caller lock out a block that never
                // reached the node in the first place.
                if spec.name == "block_confirm" {
                    if let Some(hash) = params.get("hash").and_then(Value::as_str) {
                        self.confirm_guards.record_accepted(hash).await;
                    }
                }
                if normalize_work_generate_result(spec.name, &params, &mut result) {
                    tracing::warn!(
                        event = "rpc_result_compatibility_remap",
                        method = spec.name,
                        upstream_field = "frontier",
                        contract_field = "hash",
                        "normalized work_generate result field to match Nano RPC"
                    );
                }
                if validate_result(spec.name, &result) {
                    Ok(RpcResponse::ok(request.id, result))
                } else {
                    // The node answered, but not in the shape the profile pins. That is
                    // a gateway-detected failure, not a node error string, so it uses
                    // the gateway code.
                    Ok(RpcResponse::err(
                        request.id,
                        GATEWAY_ERROR_CODE,
                        "Upstream result does not match the selected profile schema",
                    ))
                }
            }
            // Translation-layer item 2: the node reported this error, so its string is
            // the JSON-RPC message verbatim, under the code reserved for it.
            Err(GatewayError::NodeRejected(reason))
            | Err(GatewayError::NodeRateLimited(reason)) => {
                Ok(RpcResponse::err(request.id, NODE_ERROR_CODE, reason))
            }
            Err(GatewayError::Upstream(_)) => {
                // Native responses can echo request material (for example a signed
                // block). Keep that diagnostic inside the adapter and expose only the
                // stable public error contract.
                Ok(RpcResponse::err(
                    request.id,
                    GATEWAY_ERROR_CODE,
                    "Upstream request failed",
                ))
            }
            Err(GatewayError::UpstreamUnavailable(_)) | Err(GatewayError::UpstreamTransport(_)) => {
                Ok(RpcResponse::err(
                    request.id,
                    GATEWAY_ERROR_CODE,
                    "Upstream unavailable",
                ))
            }
            Err(GatewayError::InvalidRequest(message)) => {
                Ok(RpcResponse::err(request.id, -32602, message))
            }
            // Any other gateway-authored failure stays in the gateway's own code space
            // with a fixed message, so no internal detail reaches the caller.
            Err(_) => Ok(RpcResponse::err(
                request.id,
                GATEWAY_ERROR_CODE,
                "Internal error",
            )),
        }
    }
    /// Operator guards that refuse a call before it reaches the node. Returning an
    /// error here means the call never happened, so the response is not JSON-RPC.
    async fn guard(&self, method: &str, params: &Value) -> Result<(), TransportRejection> {
        if method != "block_confirm" {
            return Ok(());
        }
        let hash = params
            .get("hash")
            .and_then(Value::as_str)
            .ok_or_else(|| TransportRejection::forbidden("invalid parameter: hash"))?;
        if !is_block_hash(hash) {
            return Err(TransportRejection::forbidden(
                "invalid parameter: hash must be 64 hexadecimal characters",
            ));
        }
        self.confirm_guards.reserve().await?;
        self.confirm_guards.check_cooldown(hash).await
    }
    fn authorized(&self, headers: &HeaderMap, scope: Scope) -> bool {
        let Some(key) = self.verifying_key.as_ref() else {
            return false;
        };
        let Some(value) = headers
            .get(header::AUTHORIZATION)
            .and_then(|v| v.to_str().ok())
        else {
            return false;
        };
        let Some(token) = value.strip_prefix("Bearer ") else {
            return false;
        };
        verify_paseto(token, key, scope).is_ok()
    }
    fn gateway_url(&self) -> String {
        if let Some(public_url) = &self.config.public_url {
            return public_url.clone();
        }
        let scheme = if self.config.tls_cert.is_some() && self.config.tls_key.is_some() {
            "https"
        } else {
            "http"
        };
        format!("{scheme}://{}/rpc", self.config.listen)
    }
    fn schema_url(&self, path: &str) -> String {
        let mut url = Url::parse(&self.gateway_url()).expect("validated gateway URL");
        url.set_path(path);
        url.set_query(None);
        url.set_fragment(None);
        url.to_string()
    }
}

fn validate_params(method: &str, params: &Value) -> Result<(), String> {
    let object = params
        .as_object()
        .ok_or_else(|| "params must be an object".to_string())?;
    let required = match method {
        "account_info" | "receivable" | "account_balance" | "account_history" => "account",
        "accounts_balances" | "accounts_frontiers" | "accounts_receivable" => "accounts",
        "blocks_info" => "hashes",
        "block_info" | "block_confirm" | "work_generate" | "work_cancel" | "work_validate" => {
            "hash"
        }
        "process" => "block",
        _ => return Ok(()),
    };
    let value = object
        .get(required)
        .ok_or_else(|| format!("missing required parameter: {required}"))?;
    let valid = match required {
        "account" | "hash" => value.as_str().is_some_and(|value| !value.is_empty()),
        "accounts" | "hashes" => value.as_array().is_some_and(|values| {
            !values.is_empty()
                && values
                    .iter()
                    .all(|value| value.as_str().is_some_and(|value| !value.is_empty()))
        }),
        "block" => value.is_object(),
        _ => true,
    };
    if !valid {
        return Err(format!("invalid parameter: {required}"));
    }

    // `count` is documented as an integer. -1 means the whole chain, so the floor is
    // -1 rather than 1; anything below that cannot select a documented behaviour.
    if matches!(
        method,
        "account_history" | "receivable" | "accounts_receivable"
    ) {
        if let Some(count) = object.get("count") {
            if !count.as_i64().is_some_and(|value| value >= -1) {
                return Err("invalid parameter: count".into());
            }
        }
    }

    // The boolean options the node documents. `head` and `offset` are checked for
    // type only, and `head` for the 64-hex-character form the node documents.
    for name in [
        "include_only_confirmed",
        "source",
        "include_active",
        "sorting",
        "include_linked_account",
        "reverse",
        "raw",
        "json_block",
        "include_not_found",
        "receive_hash",
        "include_confirmed",
        "representative",
        "weight",
        "receivable",
        "use_peers",
        "force",
        "async",
    ] {
        if object
            .get(name)
            .is_some_and(|value| !value.is_boolean() && !value.is_string())
        {
            return Err(format!("invalid parameter: {name}"));
        }
    }
    for name in ["head", "threshold"] {
        if object
            .get(name)
            .is_some_and(|value| !value.as_str().is_some_and(|value| !value.is_empty()))
        {
            return Err(format!("invalid parameter: {name}"));
        }
    }
    for name in ["offset", "min_version"] {
        if object
            .get(name)
            .is_some_and(|value| !value.as_i64().is_some_and(|value| value >= 0))
        {
            return Err(format!("invalid parameter: {name}"));
        }
    }
    if object.get("account_filter").is_some_and(|value| {
        !value.as_array().is_some_and(|values| {
            !values.is_empty()
                && values
                    .iter()
                    .all(|value| value.as_str().is_some_and(|value| !value.is_empty()))
        })
    }) {
        return Err("invalid parameter: account_filter".into());
    }
    if object.get("subtype").is_some_and(|value| {
        !value.as_str().is_some_and(|subtype| {
            matches!(subtype, "send" | "receive" | "open" | "change" | "epoch")
        })
    }) {
        return Err("invalid parameter: subtype".into());
    }
    if method == "work_validate"
        && object
            .get("work")
            .and_then(Value::as_str)
            .is_none_or(str::is_empty)
    {
        return Err("missing required parameter: work".into());
    }
    for name in ["difficulty", "account", "version"] {
        if object.get(name).is_some_and(|value| !value.is_string()) {
            return Err(format!("invalid parameter: {name}"));
        }
    }
    if object
        .get("multiplier")
        .is_some_and(|value| !value.as_f64().is_some_and(|multiplier| multiplier > 0.0))
    {
        return Err("invalid parameter: multiplier".into());
    }
    if object.get("work").is_some_and(|value| !value.is_string()) {
        return Err("invalid parameter: work".into());
    }
    if object
        .get("block")
        .is_some_and(|value| !(value.is_string() || value.is_object()))
    {
        return Err("invalid parameter: block".into());
    }
    if object
        .get("version")
        .and_then(Value::as_str)
        .is_some_and(|version| version != "work_1")
    {
        return Err("invalid parameter: version".into());
    }
    Ok(())
}

/// Normalize Nano.to's `frontier` alias only when it identifies the exact
/// work root requested by the caller. A mismatched alias remains invalid.
fn normalize_work_generate_result(method: &str, params: &Value, result: &mut Value) -> bool {
    if method != "work_generate" {
        return false;
    }
    let Some(requested_hash) = params.get("hash").and_then(Value::as_str) else {
        return false;
    };
    let Some(object) = result.as_object_mut() else {
        return false;
    };
    if object.contains_key("hash") {
        return false;
    }
    let Some(frontier) = object.get("frontier").and_then(Value::as_str) else {
        return false;
    };
    if frontier != requested_hash {
        return false;
    }
    object.remove("frontier");
    object.insert("hash".into(), Value::String(requested_hash.to_owned()));
    true
}

fn has_string(object: &serde_json::Map<String, Value>, name: &str) -> bool {
    object
        .get(name)
        .and_then(Value::as_str)
        .is_some_and(|value| !value.is_empty())
}

/// Check that an upstream response still matches the shape the profile documents.
///
/// This guards against a provider variant drifting away from the pinned baseline. It
/// asserts only the fields the node documents as unconditional, and it accepts the
/// documented value forms: `confirmed` and `height` arrive as strings, and `contents`
/// is a JSON string unless `json_block` was requested.
/// Accept the three shapes the node documents for a receivable block set: an array
/// of hashes, a hash-to-amount map, and a hash-to-amount-and-source map. The node
/// picks between them from `threshold` and `source`, so every shape is legitimate.
fn validate_receivable_value(value: &Value) -> bool {
    match value {
        Value::Array(hashes) => hashes.iter().all(Value::is_string),
        Value::Object(entries) => entries.values().all(|entry| match entry {
            Value::String(_) => true,
            Value::Object(fields) => {
                fields.get("amount").is_some_and(Value::is_string)
                    && fields.get("source").is_some_and(Value::is_string)
            }
            _ => false,
        }),
        // Some public nodes encode an empty set as an empty string.
        Value::String(text) => text.is_empty(),
        _ => false,
    }
}

fn validate_receivable_result(object: &serde_json::Map<String, Value>) -> bool {
    object.get("blocks").is_some_and(validate_receivable_value)
}

fn validate_result(method: &str, result: &Value) -> bool {
    let Some(object) = result.as_object() else {
        return false;
    };
    match method {
        "version" => has_string(object, "rpc_version"),
        "block_count" => ["count", "unchecked", "cemented"]
            .iter()
            .all(|field| has_string(object, field)),
        // `confirmed_*` fields appear only when `include_confirmed` was requested,
        // so they cannot be required here.
        "account_info" => [
            "frontier",
            "open_block",
            "representative_block",
            "balance",
            "modified_timestamp",
            "block_count",
            "account_version",
        ]
        .iter()
        .all(|field| has_string(object, field)),
        // `pending` is a deprecated upstream alias and is not requested, so the
        // response carries `balance` and `receivable` only.
        "account_balance" => ["balance", "receivable"]
            .iter()
            .all(|field| has_string(object, field)),
        // Both maps are optional: the node omits `balances` when every requested
        // account failed and omits `errors` when none did.
        "accounts_balances" => {
            let balances = object.get("balances").is_some_and(|balances| {
                balances.as_object().is_some_and(|items| {
                    items
                        .values()
                        .all(|item| validate_result("account_balance", item))
                })
            });
            let errors = object.get("errors").is_some_and(|errors| {
                errors
                    .as_object()
                    .is_some_and(|items| items.values().all(Value::is_string))
            });
            (balances || errors)
                && object
                    .get("balances")
                    .is_none_or(|balances| balances.is_object())
                && object.get("errors").is_none_or(Value::is_object)
        }
        // `previous` is present only when `count` truncated the result; `next`
        // replaces it when `reverse` is set.
        "account_history" => {
            has_string(object, "account") && object.get("history").is_some_and(Value::is_array)
        }
        // No `hash`: the caller supplied it. `subtype` exists for state blocks only,
        // so it is not required.
        "block_info" => {
            [
                "block_account",
                "amount",
                "balance",
                "height",
                "local_timestamp",
            ]
            .iter()
            .all(|field| has_string(object, field))
                && object.contains_key("contents")
                && object
                    .get("confirmed")
                    .is_some_and(|confirmed| confirmed.is_boolean() || confirmed.is_string())
        }
        "blocks_info" => object.get("blocks").is_some_and(|blocks| {
            blocks.as_object().is_some_and(|items| {
                items
                    .values()
                    .all(|item| validate_result("block_info", item))
            })
        }),
        // Both maps are optional: the node omits `frontiers` when every account
        // failed and omits `errors` when none did.
        "accounts_frontiers" => {
            object.get("frontiers").is_none_or(|frontiers| {
                frontiers
                    .as_object()
                    .is_some_and(|items| items.values().all(|hash| hash.is_string()))
            }) && object.get("errors").is_none_or(|errors| {
                errors
                    .as_object()
                    .is_some_and(|items| items.values().all(Value::is_string))
            })
        }
        // `started` is the only field, and it is always "1": the call reports that no
        // error occurred, never that the block confirmed.
        "block_confirm" => object.get("started").and_then(Value::as_str) == Some("1"),
        // The node selects one of three `blocks` shapes from the request parameters,
        // so only the shape's outer form is asserted.
        "receivable" => validate_receivable_result(object),
        "accounts_receivable" => object.get("blocks").is_some_and(|blocks| {
            blocks
                .as_object()
                .is_some_and(|items| items.values().all(validate_receivable_value))
        }),
        "process" => has_string(object, "hash"),
        "work_generate" => ["hash", "work", "difficulty", "multiplier"]
            .iter()
            .all(|field| has_string(object, field)),
        "work_cancel" => object.get("success").and_then(Value::as_str) == Some(""),
        "work_validate" => {
            ["valid_all", "valid_receive", "difficulty", "multiplier"]
                .iter()
                .all(|field| has_string(object, field))
                && object.get("valid").is_none_or(|value| {
                    value
                        .as_str()
                        .is_some_and(|valid| matches!(valid, "0" | "1"))
                })
        }
        _ => false,
    }
}

pub fn app(state: AppState) -> Router {
    let origins = state
        .config
        .cors_origins
        .iter()
        .filter_map(|origin| HeaderValue::from_str(origin).ok())
        .collect::<Vec<_>>();
    // A wildcard must go through `AllowOrigin::any`; `AllowOrigin::list` panics on `*`.
    let allow_origin = if origins.iter().any(|origin| origin == "*") {
        AllowOrigin::any()
    } else {
        AllowOrigin::list(origins)
    };
    let cors = CorsLayer::new()
        .allow_origin(allow_origin)
        .allow_methods([Method::GET, Method::POST, Method::OPTIONS])
        .allow_headers([header::AUTHORIZATION, header::CONTENT_TYPE]);
    let mut router = Router::new()
        .route("/", get(browser_root))
        .route("/rpc", post(rpc_handler))
        .route("/openrpc.json", get(openrpc_handler))
        .route("/asyncapi.json", get(asyncapi_handler))
        // Both verbs reach the same stream: EventSource can only GET, and a fetch
        // client that outgrew the request line can POST the same options.
        .route("/events/confirmations", get(sse_handler).post(sse_handler))
        .route("/health", get(|| async { Json(json!({"status":"ok"})) }))
        .route("/readyz", get(ready_handler))
        .route("/metrics", get(metrics_handler))
        .layer(cors)
        .layer(RequestBodyLimitLayer::new(1024 * 1024))
        .layer(ConcurrencyLimitLayer::new(128))
        .layer(TraceLayer::new_for_http());
    if state.config.enable_browser_ui {
        router = router
            .route("/api-landing-page", get(landing_page))
            .route("/docs/", get(reference_page))
            .route("/docs", get(reference_redirect))
            .route("/docs/assets/:asset", get(reference_asset))
            .route("/docs/assets/fonts/:asset", get(reference_font))
            .route("/docs/openrpc/", get(openrpc_viewer_redirect))
            .route("/docs/asyncapi/", get(asyncapi_viewer_redirect));
    }
    router.with_state(state)
}

async fn browser_root(State(state): State<AppState>, headers: HeaderMap) -> Response {
    let mut response =
        if state.config.enable_browser_ui && accepts_html(headers.get(header::ACCEPT)) {
            axum::response::Redirect::temporary("/api-landing-page").into_response()
        } else {
            StatusCode::NOT_FOUND.into_response()
        };
    response
        .headers_mut()
        .insert(header::VARY, HeaderValue::from_static("Accept"));
    response
}

fn accepts_html(accept: Option<&axum::http::HeaderValue>) -> bool {
    let Some(accept) = accept.and_then(|value| value.to_str().ok()) else {
        return false;
    };
    accept.split(',').any(|item| {
        let mut parts = item.trim().split(';');
        if !parts
            .next()
            .unwrap_or_default()
            .trim()
            .eq_ignore_ascii_case("text/html")
        {
            return false;
        }
        let Some(quality) = parts.find_map(|parameter| {
            let (name, value) = parameter.trim().split_once('=')?;
            name.trim()
                .eq_ignore_ascii_case("q")
                .then_some(value.trim())
        }) else {
            return true;
        };
        quality
            .parse::<f32>()
            .is_ok_and(|q| q.is_finite() && q > 0.0 && q <= 1.0)
    })
}

async fn landing_page(State(state): State<AppState>) -> axum::response::Html<String> {
    let profile = escape_html_text(&state.config.profile);
    axum::response::Html(
        include_str!("../docs/browser-landing.html").replace("{{profile}}", &profile),
    )
}

fn escape_html_text(value: &str) -> String {
    value
        .replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&#39;")
}
async fn reference_page() -> axum::response::Html<&'static str> {
    axum::response::Html(include_str!("../docs/api-docs/index.html"))
}
async fn reference_redirect() -> axum::response::Redirect {
    axum::response::Redirect::permanent("/docs/")
}

async fn reference_asset(axum::extract::Path(asset): axum::extract::Path<String>) -> Response {
    match asset.as_str() {
        "reference.css" => (
            [(header::CONTENT_TYPE, "text/css; charset=utf-8")],
            include_bytes!("../docs/api-docs/assets/reference.css").as_slice(),
        )
            .into_response(),
        "reference.js" => (
            [(header::CONTENT_TYPE, "text/javascript; charset=utf-8")],
            include_bytes!("../docs/api-docs/assets/reference.js").as_slice(),
        )
            .into_response(),
        _ => StatusCode::NOT_FOUND.into_response(),
    }
}
async fn reference_font(axum::extract::Path(asset): axum::extract::Path<String>) -> Response {
    match asset.as_str() {
        "montserrat-latin-wght-normal.woff2" => (
            [(header::CONTENT_TYPE, "font/woff2")],
            include_bytes!("../docs/api-docs/assets/fonts/montserrat-latin-wght-normal.woff2")
                .as_slice(),
        )
            .into_response(),
        "open-sans-latin-wght-normal.woff2" => (
            [(header::CONTENT_TYPE, "font/woff2")],
            include_bytes!("../docs/api-docs/assets/fonts/open-sans-latin-wght-normal.woff2")
                .as_slice(),
        )
            .into_response(),
        _ => StatusCode::NOT_FOUND.into_response(),
    }
}

async fn openrpc_viewer_redirect(State(state): State<AppState>) -> Response {
    viewer_redirect(
        "https://playground.open-rpc.org/?schemaUrl=",
        &state.schema_url("/openrpc.json"),
    )
}
async fn asyncapi_viewer_redirect(State(state): State<AppState>) -> Response {
    viewer_redirect(
        "https://studio.asyncapi.com/?url=",
        &state.schema_url("/asyncapi.json"),
    )
}
fn viewer_redirect(prefix: &str, schema_url: &str) -> Response {
    let target = format!("{prefix}{}", urlencoding::encode(schema_url));
    axum::response::Redirect::temporary(&target).into_response()
}

async fn ready_handler(State(state): State<AppState>) -> (StatusCode, Json<Value>) {
    if state.upstream_ready.load(Ordering::Relaxed) {
        (StatusCode::OK, Json(json!({"status":"ready"})))
    } else {
        (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(
                json!({"status":"not_ready","reason":"native WebSocket subscription unavailable"}),
            ),
        )
    }
}

async fn metrics_handler(
    State(state): State<AppState>,
) -> ([(header::HeaderName, &'static str); 1], String) {
    let ready = u8::from(state.upstream_ready.load(Ordering::Relaxed));
    (
        [(header::CONTENT_TYPE, "text/plain; version=0.0.4")],
        format!(
            "nano_gateway_up 1\nnano_gateway_upstream_ready {ready}\n\
nano_gateway_requests_total {}\nnano_gateway_errors_total {}\n\
nano_gateway_active_streams {}\n\
nano_gateway_overflow_resets_total {}\nnano_gateway_upstream_reconnects_total {}\n\
nano_gateway_sse_queue_capacity {}\n\
nano_gateway_request_duration_ms_sum {}\nnano_gateway_request_duration_ms_count {}\n",
            state.metrics.requests.load(Ordering::Relaxed),
            state.metrics.errors.load(Ordering::Relaxed),
            state.metrics.active_streams.load(Ordering::Relaxed),
            state.metrics.overflow_resets.load(Ordering::Relaxed),
            state.metrics.upstream_reconnects.load(Ordering::Relaxed),
            SSE_QUEUE_CAPACITY,
            state
                .metrics
                .request_duration_ms_sum
                .load(Ordering::Relaxed),
            state
                .metrics
                .request_duration_ms_count
                .load(Ordering::Relaxed),
        ),
    )
}

async fn rpc_handler(State(state): State<AppState>, headers: HeaderMap, body: Bytes) -> Response {
    let started = Instant::now();
    let log_rpc = state.config.log_rpc;
    let _request_metrics = RequestMetricsGuard {
        metrics: state.metrics.clone(),
        started,
    };
    state.metrics.requests.fetch_add(1, Ordering::Relaxed);
    if body
        .iter()
        .find(|byte| !byte.is_ascii_whitespace())
        .is_some_and(|byte| *byte == b'[')
    {
        state.metrics.errors.fetch_add(1, Ordering::Relaxed);
        return Json(RpcResponse::err(
            None,
            -32600,
            "Batch requests are not supported",
        ))
        .into_response();
    }
    let value = match serde_json::from_slice::<Value>(&body) {
        Ok(value) => value,
        Err(error) => {
            state.metrics.errors.fetch_add(1, Ordering::Relaxed);
            return Json(RpcResponse::err(
                None,
                -32700,
                format!("Parse error: {error}"),
            ))
            .into_response();
        }
    };
    if !value.is_object() {
        state.metrics.errors.fetch_add(1, Ordering::Relaxed);
        return Json(RpcResponse::err(None, -32600, "Invalid Request")).into_response();
    }
    let id_present = value.get("id").is_some();
    let mut request = match serde_json::from_value::<RpcRequest>(value) {
        Ok(request) => request,
        Err(error) => {
            state.metrics.errors.fetch_add(1, Ordering::Relaxed);
            return Json(RpcResponse::err(
                None,
                -32600,
                format!("Invalid Request: {error}"),
            ))
            .into_response();
        }
    };
    request.id_present = id_present;
    let method = request.method.clone();
    if log_rpc {
        log_rpc_request(&method);
    }
    match state.dispatch(request, &headers).await {
        Ok(response) => {
            if response.error.is_some() {
                state.metrics.errors.fetch_add(1, Ordering::Relaxed);
            }
            Json(response).into_response()
        }
        Err(rejection) => {
            state.metrics.errors.fetch_add(1, Ordering::Relaxed);
            rejection.into_response()
        }
    }
}
async fn openrpc_handler(State(state): State<AppState>) -> Response {
    let document = openrpc_document_with_auth(
        &state.config.profile,
        state.config.allow_work,
        state.config.allow_control,
        state.config.enable_discovery,
        &state.gateway_url(),
        state.config.require_common_auth,
    );
    // The validator must identify this representation. The profile artifact digest
    // stays constant across public_url and cors changes, so it cannot be reused here.
    let bytes = serde_json::to_vec(&document).unwrap_or_default();
    let mut response = Json(document).into_response();
    if let Ok(value) = HeaderValue::from_str(&format!("\"sha256:{:x}\"", Sha256::digest(&bytes))) {
        response.headers_mut().insert(header::ETAG, value);
    }
    response
}

async fn asyncapi_handler(State(state): State<AppState>) -> Response {
    Json(asyncapi_document(
        &state.config.profile,
        &state.gateway_url(),
    ))
    .into_response()
}

/// How a subscriber selected which confirmations it wants.
///
/// These are the node's own connect-time subscribe options. The upstream guide asks
/// clients to leave `confirmation_type` unset so a notification is less likely to be
/// missed, and to omit `confirmation_type` entirely for that reason.
///
/// Runtime filter updates are not available. The node changes a live subscription
/// with an `update` message carrying `accounts_add` and `accounts_del`; SSE has no
/// channel for the client to send one. Changing the tracked set therefore means
/// opening a new stream, which the upstream guide already requires a client to
/// reconcile afterwards. A client with more accounts than fit in a request line can
/// split them across several streams.
#[derive(Debug, Clone)]
pub struct StreamOptions {
    pub confirmation_type: ConfirmationType,
    pub accounts: Option<Vec<String>>,
    pub include_block: bool,
    pub include_election_info: bool,
    pub include_sideband_info: bool,
}

/// The node classifies each confirmation and can filter on that classification.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ConfirmationType {
    #[default]
    All,
    Active,
    Inactive,
    ActiveQuorum,
    ActiveConfirmationHeight,
}

impl ConfirmationType {
    fn parse(value: Option<&str>) -> Result<Self, &'static str> {
        match value {
            None | Some("all") => Ok(Self::All),
            Some("active") => Ok(Self::Active),
            Some("inactive") => Ok(Self::Inactive),
            Some("active_quorum") => Ok(Self::ActiveQuorum),
            Some("active_confirmation_height") => Ok(Self::ActiveConfirmationHeight),
            Some(_) => Err("invalid value for confirmation_type"),
        }
    }

    /// Whether a confirmation with this classification passes the filter.
    ///
    /// `active` covers both active classifications, as the node documents. An
    /// unrecognised classification is kept rather than dropped: the node may add
    /// one, and silently losing events would be worse than delivering an extra.
    fn accepts(self, confirmation_type: Option<&str>) -> bool {
        let Some(confirmation_type) = confirmation_type else {
            return self == Self::All;
        };
        match self {
            Self::All => true,
            Self::Active => matches!(
                confirmation_type,
                "active_quorum" | "active_confirmation_height"
            ),
            Self::Inactive => confirmation_type == "inactive",
            Self::ActiveQuorum => confirmation_type == "active_quorum",
            Self::ActiveConfirmationHeight => confirmation_type == "active_confirmation_height",
        }
    }
}

impl StreamOptions {
    /// Read the options from a JSON object, as sent in a POST body.
    fn from_object(object: &serde_json::Map<String, Value>) -> Result<Self, &'static str> {
        for (name, value) in object {
            let known = matches!(
                name.as_str(),
                "confirmation_type"
                    | "accounts"
                    | "all_local_accounts"
                    | "include_block"
                    | "include_election_info"
                    | "include_sideband_info"
            );
            if !known {
                return Err("unknown subscription option");
            }
            let valid = if name == "accounts" {
                value.as_array().is_some_and(|accounts| {
                    !accounts.is_empty()
                        && accounts
                            .iter()
                            .all(|account| account.as_str().is_some_and(|a| !a.is_empty()))
                })
            } else {
                value.is_boolean() || value.is_string()
            };
            if !valid {
                return Err("invalid subscription option");
            }
        }
        let accounts = object
            .get("accounts")
            .and_then(Value::as_array)
            .map(|accounts| {
                accounts
                    .iter()
                    .filter_map(Value::as_str)
                    .map(ToOwned::to_owned)
                    .collect::<Vec<_>>()
            });
        if let Some(accounts) = &accounts {
            if accounts.len() > MAX_BODY_ACCOUNT_ITEMS {
                return Err("too many accounts in filter");
            }
        }
        let confirmation_type =
            ConfirmationType::parse(object.get("confirmation_type").and_then(Value::as_str))?;
        let include_block = object
            .get("include_block")
            .and_then(Value::as_bool)
            .unwrap_or(true);
        // The node documents that omitting block content is incompatible with account
        // filtering, because account filtering needs the block to work. Honour the
        // restriction rather than silently delivering one or the other.
        let accounts_are_set = accounts
            .as_ref()
            .is_some_and(|accounts| !accounts.is_empty());
        if !include_block && accounts_are_set {
            return Err("include_block false is incompatible with an accounts filter");
        }
        Ok(Self {
            confirmation_type,
            accounts,
            include_block,
            include_election_info: object
                .get("include_election_info")
                .and_then(Value::as_bool)
                .unwrap_or(false),
            include_sideband_info: object
                .get("include_sideband_info")
                .and_then(Value::as_bool)
                .unwrap_or(false),
        })
    }

    /// Read the options from the query string, as sent by EventSource.
    ///
    /// A comma-separated list is the form a URL can carry; a JSON array in the query
    /// is accepted too so that the same options object can be posted or queried.
    fn from_query(query: &EventsQuery) -> Result<Self, &'static str> {
        let Some(accounts) = query.accounts.as_deref() else {
            return Ok(Self::default());
        };
        let accounts = accounts
            .split(',')
            .map(str::trim)
            .filter(|account| !account.is_empty())
            .map(ToOwned::to_owned)
            .collect::<Vec<_>>();
        if accounts.is_empty() {
            return Ok(Self::default());
        }
        if accounts.len() > MAX_ACCOUNT_FILTER_ITEMS {
            return Err("too many accounts in filter");
        }
        if query.accounts.as_deref().map(str::len).unwrap_or(0) > MAX_ACCOUNT_FILTER_BYTES {
            return Err("account filter is too large");
        }
        Ok(Self {
            accounts: Some(accounts),
            ..Self::default()
        })
    }

    fn matches(&self, event: &NanoEvent) -> bool {
        event.event == "nano.confirmation"
            && self
                .confirmation_type
                .accepts(event.data.get("confirmation_type").and_then(Value::as_str))
            && event_matches_accounts(event, self.accounts.as_deref())
    }

    /// Apply the response options the node would have applied upstream.
    ///
    /// The gateway's own upstream subscription asks for election and sideband
    /// information unconditionally so a subscriber that wants them is served from
    /// the one socket. A subscriber that did not ask for them does not receive them,
    /// which is what the node would have done.
    fn project(&self, event: &NanoEvent) -> NanoEvent {
        let mut data = event.data.clone();
        if let Some(fields) = data.as_object_mut() {
            if !self.include_block {
                fields.remove("block");
            }
            if !self.include_election_info {
                fields.remove("election_info");
            }
            if !self.include_sideband_info {
                if let Some(block) = fields.get_mut("block").and_then(Value::as_object_mut) {
                    block.remove("height");
                    block.remove("local_timestamp");
                }
            }
        }
        NanoEvent {
            event: event.event.clone(),
            data,
        }
    }
}

impl Default for StreamOptions {
    fn default() -> Self {
        Self {
            confirmation_type: ConfirmationType::All,
            accounts: None,
            include_block: true,
            include_election_info: false,
            include_sideband_info: false,
        }
    }
}

#[derive(Debug, Deserialize)]
struct EventsQuery {
    accounts: Option<String>,
}

fn accounts_equal(left: &str, right: &str) -> bool {
    let left_account = left
        .strip_prefix("nano_")
        .or_else(|| left.strip_prefix("xrb_"));
    let right_account = right
        .strip_prefix("nano_")
        .or_else(|| right.strip_prefix("xrb_"));
    match (left_account, right_account) {
        (Some(left), Some(right)) => left == right,
        _ => left == right,
    }
}

fn is_legacy_confirmation(item: &NanoEvent) -> bool {
    item.data
        .get("block")
        .and_then(Value::as_object)
        .and_then(|block| block.get("type"))
        .and_then(Value::as_str)
        .is_some_and(|block_type| matches!(block_type, "open" | "send" | "receive" | "change"))
}

/// Apply the node's account filter.
///
/// The node never broadcasts a legacy block once a filter is given, even when the
/// block's account matches, so the same rule applies here. A state send matches on
/// either its owning account or its `link_as_account`, which is where the recipient
/// lives now that the payload has no derived `destination` field.
fn event_matches_accounts(item: &NanoEvent, accounts: Option<&[String]>) -> bool {
    let Some(values) = accounts.filter(|values| !values.is_empty()) else {
        return true;
    };
    if is_legacy_confirmation(item) {
        return false;
    }
    let matches = |account: Option<&str>| {
        account.is_some_and(|account| values.iter().any(|wanted| accounts_equal(wanted, account)))
    };
    matches(item.data.get("account").and_then(Value::as_str))
        || matches(
            item.data
                .get("block")
                .and_then(Value::as_object)
                .and_then(|block| block.get("link_as_account"))
                .and_then(Value::as_str),
        )
}

/// The confirmation stream, reachable two ways.
///
/// GET is what EventSource speaks, so a browser can subscribe with no custom code,
/// at the cost of carrying the account filter in the request line. POST takes the
/// same options as a JSON body and streams the identical response, which lifts the
/// request-line ceiling at the cost of a fetch-and-read client. Neither supports
/// changing the filter on a live stream: that needs a client-to-server message, which
/// SSE does not carry. A client that needs to change its tracked set opens a new
/// stream, and reconciles afterwards as the upstream guide already requires.
async fn sse_handler(
    State(state): State<AppState>,
    method: axum::http::Method,
    query: Option<axum::extract::Query<EventsQuery>>,
    body: Option<Json<Value>>,
) -> Response {
    let options = match method {
        axum::http::Method::GET => {
            let query = query.map(|axum::extract::Query(query)| query);
            StreamOptions::from_query(&query.unwrap_or(EventsQuery { accounts: None }))
        }
        _ => {
            let body = body.map(|Json(body)| body);
            let Some(Value::Object(object)) = body else {
                return bad_request(&state, "a POST body must be a JSON object");
            };
            StreamOptions::from_object(&object)
        }
    };
    let options = match options {
        Ok(options) => options,
        Err(message) => return bad_request(&state, message),
    };

    state.metrics.active_streams.fetch_add(1, Ordering::Relaxed);
    let account_filter_count = options.accounts.as_ref().map_or(0, Vec::len);
    let log_rpc = state.config.log_rpc;
    let started = Instant::now();
    if log_rpc {
        tracing::info!(
            target: "nano_rpc_gateway::sse",
            event = "sse_session_created",
            method = %method,
            account_filter_count,
            confirmation_type = ?options.confirmation_type,
            include_block = options.include_block,
            include_election_info = options.include_election_info,
            include_sideband_info = options.include_sideband_info,
            upstream_ws_circuit = if state.upstream_ready.load(Ordering::Acquire) {
                "existing"
            } else {
                "establishing"
            },
            "SSE session created"
        );
    }
    let mut receiver = state.events.subscribe();
    let active_streams = state.metrics.active_streams.clone();
    let output = stream! {
        let _guard = ActiveStreamGuard {
            active_streams,
            log_rpc,
            started,
            account_filter_count,
        };
        loop {
            match receiver.recv().await {
                Ok(item) if options.matches(&item) => {
                    yield Ok::<Event, Infallible>(sse_event(options.project(&item)));
                }
                Ok(_) => {}
                // A subscriber that cannot keep up is closed, not told about it.
                // The node offers no replay, so there is nothing to catch up with.
                Err(broadcast::error::RecvError::Lagged(_)) => break,
                Err(broadcast::error::RecvError::Closed) => break,
            }
        }
    };
    // The node answers a keepalive with an acknowledgement. SSE has no channel to
    // carry one back, so the gateway emits a comment on the same interval: it keeps
    // intermediaries from treating an idle stream as dead, which is the job the
    // node's ping does on the WebSocket.
    let mut response = Sse::new(output)
        .keep_alive(KeepAlive::new().interval(STREAM_KEEPALIVE))
        .into_response();
    response.headers_mut().insert(
        header::CACHE_CONTROL,
        header::HeaderValue::from_static("no-cache, no-transform"),
    );
    response.headers_mut().insert(
        header::HeaderName::from_static("x-accel-buffering"),
        header::HeaderValue::from_static("no"),
    );
    response
}

fn bad_request(state: &AppState, message: &str) -> Response {
    state.metrics.errors.fetch_add(1, Ordering::Relaxed);
    (StatusCode::BAD_REQUEST, Json(json!({"error": message}))).into_response()
}

pub async fn run_ws_bridge(state: AppState) -> Result<(), GatewayError> {
    run_ws_bridge_with_confirmation_liveness(state, CONFIRMATION_DELIVERY_TIMEOUT).await
}

async fn run_ws_bridge_with_confirmation_liveness(
    state: AppState,
    confirmation_liveness: Duration,
) -> Result<(), GatewayError> {
    state.upstream_ready.store(false, Ordering::Relaxed);
    let mut connected = false;
    let mut ws_index = None;
    let mut selected_ws_url = None;
    let result = async {
        let (index, ws_url) = match state
            .active_ws_url()
            .await
        {
            Ok(selection) => selection,
            Err(error) => {
                let index = state.ws_active.load(Ordering::Acquire);
                if let Some(ws_url) = state.config.node_ws_urls.get(index) {
                    log_upstream_error("websocket", ws_url, &error.to_string());
                }
                return Err(error);
            }
        };
        ws_index = Some(index);
        selected_ws_url = Some(ws_url.clone());
        {
            let mut selected = state.ws_selected.lock().await;
            if selected.as_deref() != Some(ws_url.as_str()) {
                log_upstream_selected(
                    "websocket",
                    &ws_url,
                    if selected.is_some() {
                        "active_changed"
                    } else {
                        "initial"
                    },
                );
                *selected = Some(ws_url.clone());
            }
        }
        if let Err(error) = validate_upstream_url(&ws_url, &["ws", "wss"]) {
            log_upstream_error("websocket", &ws_url, &error.to_string());
            return Err(error);
        }
        if state.config.log_rpc {
            tracing::info!(
                target: "nano_rpc_gateway::upstream",
                event = "upstream_ws_connecting",
                protocol = "websocket",
                circuit = "new",
                upstream_url = %redact_upstream_url(&ws_url),
                "establishing upstream WebSocket circuit"
            );
        }
        let request = websocket_connect_request(&ws_url)?;
        let (mut socket, _) = match tokio::time::timeout(Duration::from_secs(5), connect_async(request)).await {
            Ok(Ok(connection)) => connection,
            Ok(Err(error)) => {
                log_upstream_error("websocket", &ws_url, &error.to_string());
                return Err(GatewayError::UpstreamUnavailable(error.to_string()));
            }
            Err(_) => return Err(GatewayError::UpstreamUnavailable("websocket connection timed out".into())),
        };
        if let Err(error) = socket
            .send(Message::Text(
                json!({"action":"subscribe","topic":"confirmation","ack":true,"options":{
                    // Unfiltered, so no subscriber can miss a notification the way an
                    // upstream-side confirmation_type filter would let it.
                    "confirmation_type":"all",
                    // The election and sideband fields are requested unconditionally so
                    // a subscription asking for them is served from this one socket
                    // rather than needing an upstream connection of its own. A
                    // subscriber that did not ask for them does not receive them.
                    "include_election_info":true,
                    "include_sideband_info":true
                }}).to_string(),
            ))
            .await
        {
            log_upstream_error("websocket", &ws_url, &error.to_string());
            return Err(GatewayError::UpstreamUnavailable(error.to_string()));
        }
        tokio::time::timeout(Duration::from_secs(5), async {
            while let Some(message) = socket.next().await {
                let message = message.map_err(|error| GatewayError::UpstreamTransport(error.to_string()))?;
                if let Message::Text(text) = message {
                    if let Ok(value) = serde_json::from_str::<Value>(&text) {
                        if value.get("error").is_some() {
                            return Err(GatewayError::UpstreamUnavailable("confirmation subscription rejected".into()));
                        }
                        if value.get("ack").and_then(Value::as_str) == Some("subscribe") {
                            return Ok(());
                        }
                    }
                }
            }
            Err(GatewayError::UpstreamUnavailable("closed before subscription acknowledgement".into()))
        }).await.map_err(|_| GatewayError::UpstreamUnavailable("confirmation subscription acknowledgement timed out".into()))??;
        connected = true;
        let reconnecting = state.upstream_seen.swap(true, Ordering::Relaxed);
        if reconnecting {
            state
                .metrics
                .upstream_reconnects
                .fetch_add(1, Ordering::Relaxed);
        }
        if state.config.log_rpc {
            tracing::info!(
                target: "nano_rpc_gateway::upstream",
                event = "upstream_ws_connected",
                protocol = "websocket",
                circuit = "new",
                reconnecting,
                upstream_url = %redact_upstream_url(&ws_url),
                "upstream WebSocket circuit connected"
            );
        }
        state.upstream_ready.store(true, Ordering::Relaxed);

        let mut heartbeat = tokio::time::interval(Duration::from_secs(15));
        heartbeat.tick().await;
        let mut awaiting_pong = false;
        let confirmation_deadline = tokio::time::sleep(confirmation_liveness);
        tokio::pin!(confirmation_deadline);
        loop {
            let message = tokio::select! {
                message = socket.next() => match message { Some(message) => message, None => break },
                _ = &mut confirmation_deadline => {
                    return Err(GatewayError::UpstreamUnavailable("confirmation delivery timed out".into()));
                }
                _ = heartbeat.tick() => {
                    if awaiting_pong {
                        return Err(GatewayError::UpstreamUnavailable("websocket heartbeat timed out".into()));
                    }
                    socket.send(Message::Ping(vec![])).await.map_err(|error| GatewayError::UpstreamTransport(error.to_string()))?;
                    awaiting_pong = true;
                    continue;
                }
            };
            let message = match message {
                Ok(message) => message,
                Err(error) => {
                    log_upstream_error("websocket", &ws_url, &error.to_string());
                    return Err(GatewayError::UpstreamTransport(error.to_string()));
                }
            };
            let value = match message {
                Message::Pong(_) => { awaiting_pong = false; continue; }
                Message::Close(_) => break,
                Message::Text(text) => serde_json::from_str::<Value>(text.as_ref()),
                Message::Binary(bytes) => {
                    let Ok(text) = std::str::from_utf8(bytes.as_ref()) else {
                        continue;
                    };
                    serde_json::from_str::<Value>(text)
                }
                _ => continue,
            };
            if let Ok(value) = value {
                if let Some(event) = normalize_confirmation(&value) {
                    confirmation_deadline
                        .as_mut()
                        .reset(tokio::time::Instant::now() + confirmation_liveness);
                    state.events.publish("nano.confirmation", event).await;
                }
            }
        }
        Ok(())
    }
    .await;
    state.upstream_ready.store(false, Ordering::Relaxed);
    if let Some(index) = ws_index {
        state.mark_ws_unhealthy(index).await;
    }
    if let (Err(error), Some(url)) = (&result, &selected_ws_url) {
        log_upstream_error("websocket", url, &error.to_string());
    }
    if connected && result.is_ok() {
        if let Some(ws_url) = selected_ws_url.as_deref() {
            tracing::warn!(
                target: "nano_rpc_gateway::upstream",
                event = "upstream_closed",
                protocol = "websocket",
                upstream_url = %redact_upstream_url(ws_url),
                "websocket upstream closed"
            );
        }
    }
    result
}

/// Extract a confirmation message from an upstream WebSocket frame.
///
/// Translation-layer item 3: the payload carries every upstream field unchanged,
/// including `time`. The envelope's `topic` becomes the SSE event name, so it is
/// not duplicated into the payload. Nothing is added: `profile`, `destination`
/// and a top-level `subtype` are all derivable from the block the node already
/// sends, and the node has no such fields.
fn normalize_confirmation(value: &Value) -> Option<Value> {
    if value.get("ack").is_some()
        || value.get("topic").and_then(Value::as_str) != Some("confirmation")
    {
        return None;
    }
    let mut data = value
        .get("message")
        .cloned()
        .unwrap_or_else(|| value.clone());
    if let Value::Object(fields) = &mut data {
        if let Some(time) = value.get("time").cloned() {
            fields.insert("time".into(), time);
        }
    }
    Some(data)
}

fn parse_public_key(value: &str) -> Result<VerifyingKey, GatewayError> {
    let bytes = URL_SAFE_NO_PAD
        .decode(value)
        .map_err(|e| GatewayError::Auth(e.to_string()))?;
    let array: [u8; 32] = bytes
        .try_into()
        .map_err(|_| GatewayError::Auth("public key must be 32 bytes".into()))?;
    VerifyingKey::from_bytes(&array).map_err(|e| GatewayError::Auth(e.to_string()))
}

fn pae(parts: &[&[u8]]) -> Vec<u8> {
    fn le64(n: usize) -> [u8; 8] {
        (n as u64).to_le_bytes()
    }
    let mut out = Vec::new();
    out.extend(le64(parts.len()));
    for part in parts {
        out.extend(le64(part.len()));
        out.extend(*part);
    }
    out
}

pub fn sign_paseto(claims: &Value, key: &SigningKey) -> String {
    let payload = serde_json::to_vec(claims).expect("claims are serializable");
    let message = pae(&[b"v4.public", &payload, b"", b""]);
    let signature = key.sign(&message);
    let mut body = payload;
    body.extend(signature.to_bytes());
    format!("v4.public.{}", URL_SAFE_NO_PAD.encode(body))
}

fn verify_paseto(token: &str, key: &VerifyingKey, scope: Scope) -> Result<Value, GatewayError> {
    let encoded = token
        .strip_prefix("v4.public.")
        .ok_or_else(|| GatewayError::Auth("unsupported token version".into()))?;
    let bytes = URL_SAFE_NO_PAD
        .decode(encoded)
        .map_err(|e| GatewayError::Auth(e.to_string()))?;
    if bytes.len() < 64 {
        return Err(GatewayError::Auth("truncated token".into()));
    }
    let split = bytes.len() - 64;
    let payload = &bytes[..split];
    let signature =
        Signature::from_slice(&bytes[split..]).map_err(|e| GatewayError::Auth(e.to_string()))?;
    key.verify(&pae(&[b"v4.public", payload, b"", b""]), &signature)
        .map_err(|_| GatewayError::Auth("invalid signature".into()))?;
    let claims: Value =
        serde_json::from_slice(payload).map_err(|e| GatewayError::Auth(e.to_string()))?;
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    if claims
        .get("exp")
        .and_then(Value::as_u64)
        .is_none_or(|exp| exp < now)
    {
        return Err(GatewayError::Auth("expired token".into()));
    }
    let token_scope = claims
        .get("scope")
        .and_then(Value::as_str)
        .unwrap_or_default();
    let allowed = match scope {
        Scope::Base => true,
        Scope::Common => matches!(token_scope, "common" | "work" | "control"),
        Scope::Work => matches!(token_scope, "work" | "control"),
        Scope::Control => token_scope == "control",
    };
    if !allowed {
        return Err(GatewayError::Auth("scope denied".into()));
    }
    if claims.get("aud").and_then(Value::as_str) != Some("nano-rpc-gateway") {
        return Err(GatewayError::Auth("audience denied".into()));
    }
    Ok(claims)
}

pub fn generate_signing_key() -> SigningKey {
    SigningKey::generate(&mut rand_core::OsRng)
}

/// Builds a development Playground URL without embedding Playground in the gateway.
pub fn playground_url(gateway_url: &str, schema_url: Option<&str>, local: bool) -> String {
    let schema = schema_url.map(str::to_owned).unwrap_or_else(|| {
        let gateway = gateway_url.trim_end_matches('/');
        let root = gateway.rsplit_once('/').map_or(gateway, |(root, _)| root);
        format!("{root}/openrpc.json")
    });
    let host = if local {
        "http://127.0.0.1:8080/"
    } else {
        "https://playground.open-rpc.org/"
    };
    format!(
        "{host}?schemaUrl={}&uiSchema%5BappBar%5D%5Bui%3Aedit%5D=false",
        urlencoding::encode(&schema)
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio_tungstenite::accept_async;

    #[test]
    fn shipped_example_config_parses_and_validates() {
        let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("gateway.yaml.example");
        let contents = std::fs::read_to_string(&path).expect("gateway.yaml.example is readable");
        let config: Config = serde_yaml::from_str(&contents).expect("example config parses");
        config.clone().validate().expect("example config validates");
        assert_eq!(config.cors_origins, vec!["*".to_owned()]);
        assert!(config.public_url.is_none());
        assert!(config.require_common_auth);
        assert!(!config.allow_work);
        assert!(!config.allow_control);
    }

    #[test]
    fn registry_methods_have_unique_names() {
        let methods = registry();
        let mut names = methods.iter().map(|item| item.name).collect::<Vec<_>>();
        names.sort_unstable();
        names.dedup();
        assert_eq!(names.len(), methods.len());
    }
    #[test]
    fn openrpc_contains_only_enabled_methods() {
        let document =
            openrpc_document("nano-node/V28.2", false, true, "http://127.0.0.1:7076/rpc");
        // version, block_count, account_info, receivable, account_balance,
        // accounts_balances, account_history, block_info, blocks_info,
        // accounts_frontiers, accounts_receivable, block_confirm, process,
        // work_validate and rpc.discover. work_generate is filtered out while
        // work is disabled.
        assert_eq!(document["methods"].as_array().expect("methods").len(), 15);
    }

    #[test]
    fn asyncapi_inventory_matches_event_registry() {
        let document = asyncapi_document("nano-node/V28.2", "https://gateway.invalid/rpc");
        let advertised = document["components"]["messages"]
            .as_object()
            .expect("AsyncAPI messages");
        let expected = event_registry()
            .into_iter()
            .map(|event| event.name)
            .collect::<Vec<_>>();
        assert_eq!(
            advertised.keys().map(String::as_str).collect::<Vec<_>>(),
            expected
        );
    }

    #[test]
    fn asyncapi_advertises_only_the_confirmation_event() {
        // The node closes a WebSocket rather than announcing a gap, so there is no
        // reset event and no reason enum.
        let document = asyncapi_document("nano-node/V28.2", "https://gateway.invalid/rpc");
        let schemas = document["components"]["schemas"]
            .as_object()
            .expect("AsyncAPI schemas");
        assert!(schemas.contains_key("ConfirmationParams"));
        assert!(!schemas.contains_key("StreamResetParams"));
        assert!(!document["channels"]["confirmations"]["description"]
            .as_str()
            .expect("description")
            .contains("Last-Event-ID"));
    }

    #[test]
    fn notification_examples_match_their_sse_event_names() {
        let document = asyncapi_document("nano-node/V28.2", "https://gateway.invalid/rpc");
        for message in document["components"]["messages"]
            .as_object()
            .expect("AsyncAPI messages")
            .values()
        {
            let payload = &message["examples"][0]["payload"];
            assert_eq!(payload["jsonrpc"], "2.0");
            assert_eq!(payload["method"], message["name"]);
            assert!(payload.get("id").is_none());
            assert!(payload["params"].is_object());
        }
    }

    #[test]
    fn asyncapi_account_tracking_is_limited_to_native_batch_state_refresh() {
        let document = asyncapi_document("nano-node/V28.2", "https://gateway.invalid/rpc");
        let tracking = &document["x-nano-account-tracking"];
        assert_eq!(tracking["state_refresh"]["method"], "accounts_balances");
        assert_eq!(
            tracking["state_refresh"]["params"]["include_only_confirmed"],
            true
        );
        assert_eq!(tracking["frontier_confirmation"]["method"], "account_info");
        assert_eq!(
            tracking["frontier_confirmation"]["current_frontier_field"],
            "result.frontier"
        );
        assert_eq!(
            tracking["frontier_confirmation"]["status_method"],
            "block_info"
        );
        assert_eq!(
            tracking["not_provided"],
            json!([
                "balance aggregation",
                "derived balance events",
                "vote events",
                "telemetry events"
            ])
        );
    }

    #[test]
    fn public_account_tracking_surface_excludes_vote_and_telemetry_features() {
        let openrpc = openrpc_document(
            "nano-node/V28.2",
            false,
            true,
            "https://gateway.invalid/rpc",
        );
        let methods = openrpc["methods"]
            .as_array()
            .expect("OpenRPC methods")
            .iter()
            .filter_map(|method| method["name"].as_str())
            .collect::<Vec<_>>();
        assert!(methods.contains(&"accounts_balances"));
        for unsupported in ["account_weight", "confirmation_history", "telemetry"] {
            assert!(!methods.contains(&unsupported));
        }

        let asyncapi = asyncapi_document("nano-node/V28.2", "https://gateway.invalid/rpc");
        let messages = asyncapi["components"]["messages"]
            .as_object()
            .expect("AsyncAPI messages")
            .keys()
            .map(String::as_str)
            .collect::<Vec<_>>();
        assert_eq!(messages, vec!["nano.confirmation"]);
    }
    #[test]
    fn openrpc_methods_have_callable_shapes() {
        let document =
            openrpc_document("nano-node/V28.2", false, true, "http://127.0.0.1:7076/rpc");
        for method in document["methods"].as_array().expect("methods") {
            assert!(method["name"].is_string());
            assert!(method["params"].is_array());
            assert!(method["result"]["schema"].is_object());
            if method["name"] != "rpc.discover" {
                assert!(method["x-nano-schema-provenance"].is_string());
            }
        }
    }

    #[test]
    fn public_common_profile_advertises_process_without_authentication() {
        let document = openrpc_document_with_auth(
            "nano-node/V28.2",
            false,
            false,
            true,
            "http://127.0.0.1:8090/rpc",
            false,
        );
        assert_eq!(document["x-nano-common-authentication"]["required"], false);
        let process = document["methods"]
            .as_array()
            .expect("methods")
            .iter()
            .find(|method| method["name"] == "process")
            .expect("process method");
        assert_eq!(process["x-nano-capability"]["requires"], json!([]));
        let work_validate = document["methods"]
            .as_array()
            .expect("methods")
            .iter()
            .find(|method| method["name"] == "work_validate")
            .expect("work_validate method");
        assert_eq!(work_validate["x-nano-capability"]["requires"], json!([]));
    }

    #[test]
    fn public_profile_is_clean_and_digest_is_stable() {
        let first = openrpc_document("nano-node/V28.2", false, true, "http://127.0.0.1:8090/rpc");
        let second = openrpc_document(
            "nano-node/V28.2",
            false,
            true,
            "https://example.invalid/rpc",
        );
        assert_eq!(
            first["x-nano-artifact-sha256"],
            second["x-nano-artifact-sha256"]
        );
        assert!(!first.to_string().contains("pending"));
        assert!(!first["methods"]
            .as_array()
            .expect("methods")
            .iter()
            .any(|method| method["name"] == "active_difficulty"));
    }

    #[test]
    fn account_info_result_is_the_upstream_object_unchanged() {
        // Translation-layer item 1: no `opened` field, no null placeholders, and no
        // synthesis of `confirmation_height` from `confirmed_height`.
        let upstream = json!({
            "frontier": "FF84533A571D953A596EA401FD41743AC85D04F406E76FDE4408EAED50B473C5",
            "open_block": "991CF190094C00F0B68E2E5F75F6BEE95A2E0BD93CEAA4A6734DB9F19B728948",
            "representative_block": "991CF190094C00F0B68E2E5F75F6BEE95A2E0BD93CEAA4A6734DB9F19B728948",
            "balance": "10000",
            "modified_timestamp": "1501793775",
            "block_count": "33",
            "account_version": "1",
            "confirmation_height": "28"
        });
        assert!(validate_result("account_info", &upstream));
        assert!(upstream
            .as_object()
            .expect("object")
            .get("opened")
            .is_none());
        assert!(upstream
            .as_object()
            .expect("object")
            .get("confirmed_balance")
            .is_none());
    }

    #[test]
    fn accounts_balances_keeps_each_native_balance_verbatim() {
        let upstream = json!({"balances": {
            "nano_first": {"balance": "10", "receivable": "1"},
            "nano_second": {"balance": "20", "receivable": "2"}
        }});
        assert!(validate_result("accounts_balances", &upstream));
        // Both maps are optional, so an errors-only response is valid too.
        assert!(validate_result(
            "accounts_balances",
            &json!({"errors": {"nano_first": "Account not found"}})
        ));
    }

    #[test]
    fn registry_and_openrpc_method_inventories_match() {
        let document = openrpc_document_with_auth(
            "nano-node/V28.2",
            true,
            true,
            false,
            "http://127.0.0.1:7076/rpc",
            true,
        );
        let advertised = document["methods"]
            .as_array()
            .expect("methods")
            .iter()
            .filter_map(|method| method["name"].as_str())
            .collect::<Vec<_>>();
        for spec in registry() {
            assert!(
                spec.schema_provenance.starts_with("profiles/")
                    || spec
                        .schema_provenance
                        .starts_with("https://docs.nano.org/commands/rpc-protocol/#")
            );
            assert!(advertised.contains(&spec.name));
        }
        assert_eq!(advertised.len(), registry().len());
    }
    #[test]
    fn paseto_round_trip_preserves_claims() {
        let key = generate_signing_key();
        let token = sign_paseto(
            &json!({"aud":"nano-rpc-gateway","exp":4_000_000_000u64,"scope":"work"}),
            &key,
        );
        assert!(verify_paseto(&token, &key.verifying_key(), Scope::Work).is_ok());
    }
    #[test]
    fn paseto_rejects_expired_or_wrong_audience_claims() {
        let key = generate_signing_key();
        let expired = sign_paseto(
            &json!({"aud":"nano-rpc-gateway","exp":1u64,"scope":"work"}),
            &key,
        );
        assert!(verify_paseto(&expired, &key.verifying_key(), Scope::Work).is_err());
        let wrong_audience = sign_paseto(
            &json!({"aud":"other","exp":4_000_000_000u64,"scope":"work"}),
            &key,
        );
        assert!(verify_paseto(&wrong_audience, &key.verifying_key(), Scope::Work).is_err());
    }
    #[test]
    fn upstream_urls_require_expected_scheme_and_supported_credentials() {
        assert!(NativeClient::new("ftp://127.0.0.1:7076").is_err());
        assert!(NativeClient::new("http://user:pass@127.0.0.1:7076").is_err());
        let nano_to =
            NativeClient::new("https://nano_api_key:@rpc.nano.to/").expect("Nano.to endpoint");
        assert_eq!(nano_to.endpoint, "https://rpc.nano.to/");
        assert_eq!(nano_to.basic_auth, Some(("nano_api_key".into(), "".into())));
        assert_eq!(nano_to.authorization_mode(), "basic");
        assert_eq!(nano_to.redacted_endpoint(), "https://rpc.nano.to/");
        let nanswap = NativeClient::new("https://nodes.nanswap.com/XNO?api_key=nano_api_key")
            .expect("Nanswap endpoint");
        assert_eq!(
            nanswap.endpoint,
            "https://nodes.nanswap.com/XNO?api_key=nano_api_key"
        );
        assert_eq!(nanswap.basic_auth, None);
        assert_eq!(nanswap.authorization_mode(), "none");
        assert_eq!(
            nanswap.redacted_endpoint(),
            "https://nodes.nanswap.com/XNO?api_key=****"
        );
        assert!(NativeClient::new("http://127.0.0.1:7076").is_ok());
        assert!(validate_upstream_url("ws://127.0.0.1:7078", &["ws", "wss"]).is_ok());
        assert!(validate_upstream_url("http://127.0.0.1:7078", &["ws", "wss"]).is_err());
    }

    #[test]
    fn upstream_label_excludes_credentials_path_and_query() {
        let url = Url::parse("https://api-key:@nodes.example.test/XNO?api_key=secret")
            .expect("test upstream URL");
        assert_eq!(upstream_label(&url), "https://nodes.example.test");
    }

    #[test]
    fn upstream_url_redaction_preserves_endpoint_without_secrets() {
        assert_eq!(
            redact_upstream_url("https://api-key:@nodes.example.test/XNO?api_key=secret"),
            "https://nodes.example.test/XNO?api_key=****"
        );
    }

    #[test]
    fn rate_limit_error_detection_accepts_provider_variants() {
        assert!(is_rate_limited_error(&json!("429")));
        assert!(is_rate_limited_error(&json!(429)));
        assert!(is_rate_limited_error(&json!("Too Many Requests")));
        assert!(is_rate_limited_error(&json!("rate limit exceeded")));
        assert!(!is_rate_limited_error(&json!("invalid account")));
    }
    #[test]
    fn config_variables_expand_with_process_values_taking_precedence() {
        let dotenv: HashMap<String, String> = HashMap::from([
            ("NANO_TO_API_KEY".into(), "dotenv-nano-to".into()),
            ("NANSWAP_COM_API_KEY".into(), "dotenv-nanswap".into()),
        ]);
        let expanded = expand_config_variables(
            "https://${NANO_TO_API_KEY}:@rpc.nano.to/ ${NANSWAP_COM_API_KEY}",
            |name| match name {
                "NANO_TO_API_KEY" => Some("process-nano-to".into()),
                _ => dotenv.get(name).cloned(),
            },
        )
        .expect("variables expand");
        assert_eq!(
            expanded,
            "https://process-nano-to:@rpc.nano.to/ dotenv-nanswap"
        );
    }
    #[test]
    fn config_variables_reject_unset_or_malformed_placeholders() {
        let unset = expand_config_variables("url: ${MISSING_KEY}", |_| None)
            .expect_err("missing variables fail");
        assert_eq!(
            unset.to_string(),
            "configuration: configuration variable MISSING_KEY is not set"
        );
        let malformed = expand_config_variables("url: ${MISSING_KEY", |_| None)
            .expect_err("unterminated variables fail");
        assert_eq!(
            malformed.to_string(),
            "configuration: unterminated configuration variable"
        );
    }
    #[test]
    fn config_loads_provider_variables_from_adjacent_dotenv() {
        let nano_to_key = std::env::var("NANO_TO_API_KEY").unwrap_or_else(|_| "nano-to-key".into());
        let nanswap_key =
            std::env::var("NANSWAP_COM_API_KEY").unwrap_or_else(|_| "nanswap-key".into());
        let directory =
            std::env::temp_dir().join(format!("nano-rpc-gateway-config-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&directory);
        std::fs::create_dir_all(&directory).expect("create config directory");
        std::fs::write(
            directory.join(".env"),
            "NANO_TO_API_KEY=nano-to-key\nNANSWAP_COM_API_KEY=nanswap-key\n",
        )
        .expect("write dotenv");
        std::fs::write(
            directory.join("gateway.yaml"),
            r#"node_rpc_urls:
  - "https://${NANO_TO_API_KEY}:@rpc.nano.to/"
  - "https://nodes.nanswap.com/XNO?api_key=${NANSWAP_COM_API_KEY}"
node_ws_urls:
  - "wss://ws.nano.to"
  - "wss://nodes.nanswap.com/ws/?ticker=XNO&api_key=${NANSWAP_COM_API_KEY}"
"#,
        )
        .expect("write config");
        let config = Config::load(&directory.join("gateway.yaml")).expect("load config");
        assert_eq!(
            config.node_rpc_urls,
            [
                format!("https://{nano_to_key}:@rpc.nano.to/"),
                format!("https://nodes.nanswap.com/XNO?api_key={nanswap_key}")
            ]
        );
        assert_eq!(
            config.node_ws_urls[1],
            format!("wss://nodes.nanswap.com/ws/?ticker=XNO&api_key={nanswap_key}")
        );
        std::fs::remove_dir_all(directory).expect("remove config directory");
    }
    #[test]
    fn params_require_account() {
        assert!(validate_params("account_info", &json!({})).is_err());
    }
    #[test]
    fn params_reject_non_string_account() {
        assert!(validate_params("account_info", &json!({"account": 7})).is_err());
    }
    #[test]
    fn native_request_body_forwards_params_untouched() {
        // Translation-layer item 1: the gateway injects and rewrites nothing, so the
        // node's own documented defaults apply.
        let request = native_request_body("account_info", &json!({"account": "nano_opened"}));
        assert_eq!(request["action"], "account_info");
        assert_eq!(request.len(), 2);
        assert!(!request.contains_key("include_confirmed"));
        assert!(!request.contains_key("pending"));

        // `source` is opt-in and never forced.
        let request = native_request_body("receivable", &json!({"account": "nano_opened"}));
        assert_eq!(request["action"], "receivable");
        assert!(!request.contains_key("source"));
    }

    #[test]
    fn accounts_balances_requires_nonempty_accounts_and_a_boolean_confirmation_flag() {
        assert!(validate_params("accounts_balances", &json!({"accounts": []})).is_err());
        // The node documents these flags in both native boolean and string form.
        assert!(validate_params(
            "accounts_balances",
            &json!({"accounts": ["nano_test"], "include_only_confirmed": "true"})
        )
        .is_ok());
        assert!(validate_params(
            "accounts_balances",
            &json!({"accounts": ["nano_test"], "include_only_confirmed": true})
        )
        .is_ok());
        assert!(validate_params(
            "accounts_balances",
            &json!({"accounts": ["nano_test"], "include_only_confirmed": 7})
        )
        .is_err());
    }

    #[test]
    fn result_shapes_are_checked_against_the_profile() {
        assert!(validate_result("process", &json!({"hash": "A"})));
        assert!(!validate_result("process", &json!({"hash": 7})));
        assert!(!validate_result("account_info", &json!({"frontier": "A"})));
    }

    #[test]
    fn receivable_result_keeps_the_nodes_three_documented_shapes() {
        // Default: an array of hashes.
        assert!(validate_result(
            "receivable",
            &json!({"blocks": ["000D1BAEC8EC208142C99059B393051BAC8380F9B5A2E6B2489A277D81789F3F"]})
        ));
        // With `threshold`: hash to amount.
        assert!(validate_result(
            "receivable",
            &json!({"blocks": {"A": "6000000000000000000000000000000"}})
        ));
        // With `source`: hash to amount and source account.
        assert!(validate_result(
            "receivable",
            &json!({"blocks": {"A": {"amount": "6000000000000000000000000000000", "source": "nano_source"}}})
        ));
        // A provider that encodes an empty set as an empty string is passed through
        // rather than rewritten into an array.
        assert!(validate_result("receivable", &json!({"blocks": ""})));
    }

    #[test]
    fn account_info_confirmed_fields_appear_only_when_requested() {
        // No `include_confirmed`, so no `confirmed_*` fields.
        let without = json!({
            "frontier": "A",
            "open_block": "A",
            "representative_block": "A",
            "balance": "9",
            "modified_timestamp": "1",
            "block_count": "2",
            "account_version": "1"
        });
        assert!(validate_result("account_info", &without));
        let with = json!({
            "frontier": "A",
            "open_block": "A",
            "representative_block": "A",
            "balance": "9",
            "modified_timestamp": "1",
            "block_count": "2",
            "account_version": "1",
            "confirmed_frontier": "A",
            "confirmed_balance": "9",
            "confirmed_height": "2"
        });
        assert!(validate_result("account_info", &with));
        // `confirmation_height` is never synthesized from `confirmed_height`.
        assert!(with
            .as_object()
            .expect("object")
            .get("confirmation_height")
            .is_none());
    }

    #[test]
    fn block_info_keeps_contents_and_string_valued_confirmed() {
        // No `hash` (the caller supplied it) and `contents` keeps the node's name.
        // `json_block` is false by default upstream, so `contents` is a JSON string.
        let as_string = json!({
            "block_account": "nano_sender",
            "amount": "1",
            "balance": "9",
            "height": "2",
            "local_timestamp": "0",
            "confirmed": "true",
            "subtype": "send",
            "contents": "{\"type\":\"state\"}"
        });
        assert!(validate_result("block_info", &as_string));
        let fields = as_string.as_object().expect("object");
        assert!(fields.get("hash").is_none());
        assert!(fields.get("block").is_none());
        // `subtype` is absent for non-state blocks and must stay optional.
        let mut legacy = as_string.clone();
        legacy.as_object_mut().expect("object").remove("subtype");
        assert!(validate_result("block_info", &legacy));
        // With `json_block` the contents are a subtree.
        let as_object = json!({
            "block_account": "nano_sender",
            "amount": "1",
            "balance": "9",
            "height": "2",
            "local_timestamp": "0",
            "confirmed": "true",
            "subtype": "send",
            "contents": {"type": "state"}
        });
        assert!(validate_result("block_info", &as_object));
    }

    #[test]
    fn blocks_info_keeps_blocks_not_found_beside_the_block_map() {
        let upstream = json!({
            "blocks": {
                "A": {
                    "block_account": "nano_sender",
                    "amount": "1",
                    "balance": "9",
                    "height": "2",
                    "local_timestamp": "0",
                    "confirmed": "true",
                    "successor": "B",
                    "contents": {"type": "state"}
                }
            },
            "blocks_not_found": ["C"]
        });
        assert!(validate_result("blocks_info", &upstream));
        assert_eq!(upstream["blocks"]["A"]["successor"], "B");
        assert_eq!(upstream["blocks_not_found"][0], "C");
    }

    #[test]
    fn collection_parameters_follow_profile_limits() {
        // -1 selects the whole chain, so the floor is -1 rather than 1.
        for count in [-1, 0, 1, 10] {
            assert!(
                validate_params(
                    "account_history",
                    &json!({"account": "nano_test", "count": count})
                )
                .is_ok(),
                "count {count}"
            );
        }
        assert!(validate_params(
            "account_history",
            &json!({"account": "nano_test", "count": -2})
        )
        .is_err());
        assert!(validate_params(
            "account_history",
            &json!({"account": "nano_test", "count": "all"})
        )
        .is_err());
        assert!(validate_params("blocks_info", &json!({"hashes": []})).is_err());
        assert!(validate_params("accounts_frontiers", &json!({"accounts": []})).is_err());
        assert!(validate_params("accounts_receivable", &json!({"accounts": []})).is_err());
        assert!(validate_params("block_confirm", &json!({})).is_err());
    }

    #[test]
    fn account_history_accepts_the_pagination_and_raw_parameters() {
        for method in ["account_history", "receivable", "accounts_receivable"] {
            let base = if method == "accounts_receivable" {
                json!({"accounts": ["nano_test"]})
            } else {
                json!({"account": "nano_test"})
            };
            let mut params = base.clone();
            params["count"] = json!(5);
            params["sorting"] = json!(true);
            assert!(validate_params(method, &params).is_ok(), "{method}");
        }
        let history = json!({
            "account": "nano_test",
            "head": "A".repeat(64),
            "count": 2,
            "offset": 0,
            "reverse": true,
            "raw": true,
            "include_linked_account": true,
            "account_filter": ["nano_other"]
        });
        assert!(validate_params("account_history", &history).is_ok());
        // An empty filter and a negative offset cannot select a documented behaviour.
        assert!(validate_params(
            "account_history",
            &json!({"account": "nano_test", "account_filter": []})
        )
        .is_err());
        assert!(validate_params(
            "account_history",
            &json!({"account": "nano_test", "offset": -1})
        )
        .is_err());
    }
    #[test]
    fn playground_url_uses_gateway_root_schema_by_default() {
        let url = playground_url("http://127.0.0.1:8123/rpc", None, true);
        assert!(url.contains("schemaUrl=http%3A%2F%2F127.0.0.1%3A8123%2Fopenrpc.json"));
        assert!(url.ends_with("uiSchema%5BappBar%5D%5Bui%3Aedit%5D=false"));
    }
    #[test]
    fn playground_url_preserves_schema_override_and_host_mode() {
        let hosted = playground_url(
            "http://127.0.0.1:8123/rpc",
            Some("http://127.0.0.1:8123/schema%20snapshot.json"),
            false,
        );
        assert!(hosted.starts_with("https://playground.open-rpc.org/?schemaUrl="));
        assert!(
            hosted.contains("schemaUrl=http%3A%2F%2F127.0.0.1%3A8123%2Fschema%2520snapshot.json")
        );
        assert!(hosted.ends_with("uiSchema%5BappBar%5D%5Bui%3Aedit%5D=false"));

        let local = playground_url("http://127.0.0.1:8123/rpc", None, true);
        assert!(local.starts_with("http://127.0.0.1:8080/?schemaUrl="));
    }
    #[test]
    fn default_config_keeps_elevated_operations_disabled() {
        let config = Config::default();
        assert!(!config.allow_work && !config.allow_control);
    }
    #[test]
    fn request_logging_is_disabled_by_default_and_configurable() {
        assert!(!Config::default().log_rpc);
        let config: Config = serde_yaml::from_str("log_rpc: true").expect("config");
        assert!(config.log_rpc);
    }
    #[test]
    fn missing_config_is_created_with_safe_defaults() {
        let path =
            std::env::temp_dir().join(format!("nano-rpc-gateway-{}.yaml", std::process::id()));
        let _ = std::fs::remove_file(&path);
        let config = Config::load(&path).expect("missing config should use defaults");
        assert_eq!(config.listen, "127.0.0.1:8090");
        assert!(path.is_file());
        let _ = std::fs::remove_file(path);
    }
    #[test]
    fn malformed_config_is_rejected() {
        let path = std::env::temp_dir().join(format!(
            "nano-rpc-gateway-invalid-{}.yaml",
            std::process::id()
        ));
        std::fs::write(&path, "listen: [not-a-string]").expect("write test config");
        assert!(Config::load(&path).is_err());
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn partial_tls_configuration_fails_closed() {
        let config = Config {
            tls_cert: Some("cert.pem".into()),
            ..Config::default()
        };
        assert!(config.validate().is_err());
    }
    #[test]
    fn native_fixture_examples_are_valid_json() {
        let account: Value =
            serde_json::from_str(include_str!("../fixtures/native/account_info.success.json"))
                .expect("account fixture");
        let process: Value =
            serde_json::from_str(include_str!("../fixtures/native/process.success.json"))
                .expect("process fixture");
        let unavailable: Value =
            serde_json::from_str(include_str!("../fixtures/native/upstream_unavailable.json"))
                .expect("unavailable fixture");
        let duplicate: Value = serde_json::from_str(include_str!(
            "../fixtures/native/confirmation.duplicate.json"
        ))
        .expect("duplicate fixture");
        let disconnect: Value =
            serde_json::from_str(include_str!("../fixtures/native/disconnect.json"))
                .expect("disconnect fixture");
        assert_eq!(account["action"], "account_info");
        assert_eq!(process["action"], "process");
        assert_eq!(unavailable["condition"], "connection-refused");
        assert_eq!(duplicate["delivery"], "duplicate");
        assert_eq!(duplicate["expected_gateway_behavior"], "delivered");
        assert_eq!(
            disconnect["expected_gateway_behavior"],
            "stream-closes-without-event"
        );
    }
    #[test]
    fn live_v28_fixture_corpus_is_valid_and_profiled() {
        let manifest: Value =
            serde_yaml::from_str(include_str!("../fixtures/native/v28.2/manifest.yaml"))
                .expect("v28 manifest");
        assert_eq!(manifest["implementation"], "nano-node");
        assert_eq!(manifest["release"], "V28.2");
        assert_eq!(manifest["status"], "PASS");
        assert_eq!(manifest["confirmation_topic"], "confirmation");
        assert!(manifest["rpc_endpoint"].as_str().is_some());
        assert!(manifest["websocket_endpoint"].as_str().is_some());
        for fixture in [
            include_str!("../fixtures/native/v28.2/account_info.request.json"),
            include_str!("../fixtures/native/v28.2/account_info.response.json"),
            include_str!("../fixtures/native/v28.2/account_balance.request.json"),
            include_str!("../fixtures/native/v28.2/account_balance.response.json"),
            include_str!("../fixtures/native/v28.2/account_history.request.json"),
            include_str!("../fixtures/native/v28.2/account_history.response.json"),
            include_str!("../fixtures/native/v28.2/block_info.request.json"),
            include_str!("../fixtures/native/v28.2/block_info.response.json"),
            include_str!("../fixtures/native/v28.2/blocks_info.request.json"),
            include_str!("../fixtures/native/v28.2/blocks_info.response.json"),
            include_str!("../fixtures/native/v28.2/process.invalid.request.json"),
            include_str!("../fixtures/native/v28.2/process.invalid.response.json"),
            include_str!("../fixtures/native/v28.2/process.success.request.json"),
            include_str!("../fixtures/native/v28.2/process.success.response.json"),
            include_str!("../fixtures/native/v28.2/confirmation_subscription.ack.json"),
            include_str!("../fixtures/native/v28.2/confirmation.live.json"),
            include_str!("../fixtures/native/v28.2/confirmation.disconnect.live.json"),
        ] {
            serde_json::from_str::<Value>(fixture).expect("fixture JSON");
        }
        let process: Value = serde_json::from_str(include_str!(
            "../fixtures/native/v28.2/process.success.response.json"
        ))
        .expect("process response");
        assert_eq!(
            process["hash"],
            "174C8572181F2C2FB478593214B2B0281DC404406D83924B21ABFBF05B1C0B7E"
        );
        let confirmation: Value = serde_json::from_str(include_str!(
            "../fixtures/native/v28.2/confirmation.live.json"
        ))
        .expect("confirmation response");
        // The recorded frame is the node's own envelope, topic and time included.
        assert_eq!(confirmation["topic"], "confirmation");
        assert!(confirmation["time"].as_str().is_some());
        assert!(confirmation["message"]["hash"].as_str().is_some());
        assert!(confirmation["message"]["confirmation_type"]
            .as_str()
            .is_some());
        let event = normalize_confirmation(&confirmation).expect("confirmation event");
        assert_eq!(event["hash"], confirmation["message"]["hash"]);
        assert_eq!(event["time"], confirmation["time"]);
        let disconnect: Value = serde_json::from_str(include_str!(
            "../fixtures/native/v28.2/confirmation.disconnect.live.json"
        ))
        .expect("disconnect response");
        assert_eq!(disconnect["event"], "confirmation");
    }
    #[test]
    fn confirmation_normalization_filters_acks_and_carries_the_event_time() {
        assert!(normalize_confirmation(&json!({"ack":"subscribe"})).is_none());
        let event = normalize_confirmation(&json!({
            "topic":"confirmation",
            "time":"1564935350664",
            "message":{"hash":"A"}
        }))
        .expect("confirmation event");
        assert_eq!(event["hash"], "A");
        assert_eq!(event["time"], "1564935350664");
        // Nothing is added that the node did not send.
        assert!(event.get("profile").is_none());
        assert!(event.get("destination").is_none());
        assert!(event.get("subtype").is_none());
    }

    #[test]
    fn websocket_handshake_uses_a_browser_compatible_user_agent() {
        let request = websocket_connect_request("wss://gateway.test/websocket").expect("request");
        assert_eq!(
            request
                .headers()
                .get(USER_AGENT)
                .and_then(|value| value.to_str().ok()),
            Some(WEBSOCKET_USER_AGENT)
        );
    }
    #[test]
    fn confirmation_filter_matches_only_requested_accounts() {
        let item = NanoEvent {
            event: "nano.confirmation".into(),
            data: json!({"account":"nano_a"}),
        };
        assert!(event_matches_accounts(&item, Some(&["nano_a".into()])));
        assert!(!event_matches_accounts(&item, Some(&["nano_b".into()])));
        assert!(event_matches_accounts(&item, None));
    }

    #[test]
    fn confirmation_filter_treats_nano_and_xrb_prefixes_as_equivalent() {
        let item = NanoEvent {
            event: "nano.confirmation".into(),
            data: json!({"account":"nano_3example"}),
        };
        assert!(event_matches_accounts(
            &item,
            Some(&["xrb_3example".into()])
        ));
        assert!(!event_matches_accounts(
            &item,
            Some(&["xrb_3different".into()])
        ));
    }

    #[test]
    fn confirmation_filter_excludes_legacy_blocks_when_accounts_are_given() {
        let item = NanoEvent {
            event: "nano.confirmation".into(),
            data: json!({"account":"nano_a", "block":{"type":"open"}}),
        };
        assert!(!event_matches_accounts(&item, Some(&["nano_a".into()])));
        assert!(event_matches_accounts(&item, None));
    }

    #[test]
    fn confirmation_filter_matches_a_state_send_recipient() {
        // The recipient lives in the block's link_as_account. There is no derived
        // `destination` field: the node does not send one.
        let item = NanoEvent {
            event: "nano.confirmation".into(),
            data: json!({"account":"nano_sender", "block":{"type":"state","link_as_account":"nano_player"}}),
        };
        assert!(event_matches_accounts(&item, Some(&["nano_player".into()])));
        assert!(!event_matches_accounts(&item, Some(&["nano_other".into()])));
        // The owning account still matches too.
        assert!(event_matches_accounts(&item, Some(&["nano_sender".into()])));
    }

    #[test]
    fn confirmation_filter_is_not_applied_without_accounts() {
        // The node documents one filter for this topic. A block hash filter would be
        // gateway-only, so an unfiltered subscriber receives every confirmation.
        let item = NanoEvent {
            event: "nano.confirmation".into(),
            data: json!({"account":"nano_a","hash":"ABC"}),
        };
        assert!(event_matches_accounts(&item, None));
    }

    #[test]
    fn account_filter_ceiling_covers_a_hundred_accounts_and_not_one_more() {
        // Sized to what an HTTP request line can carry, not to what the node would
        // accept: nginx and Apache commonly limit a request line to about 8 KiB.
        let hundred = (0..100)
            .map(|index| format!("nano_{index:0>60}"))
            .collect::<Vec<_>>();
        let encoded = hundred.join(",");
        assert_eq!(encoded.len(), 66 * 100 - 1);
        assert!(encoded.len() <= MAX_ACCOUNT_FILTER_BYTES);
        assert!(hundred.len() <= MAX_ACCOUNT_FILTER_ITEMS);
        assert!(StreamOptions::from_query(&EventsQuery {
            accounts: Some(encoded)
        })
        .is_ok());

        let overflow = (0..=MAX_ACCOUNT_FILTER_ITEMS)
            .map(|index| format!("nano_{index:0>60}"))
            .collect::<Vec<_>>()
            .join(",");
        assert!(StreamOptions::from_query(&EventsQuery {
            accounts: Some(overflow)
        })
        .is_err());
    }

    #[tokio::test]
    async fn event_hub_fans_out_live_confirmations_without_retaining_them() {
        let hub = EventHub::new();
        let mut receiver = hub.subscribe();
        hub.publish("nano.confirmation", json!({"hash":"a"})).await;
        hub.publish("nano.confirmation", json!({"hash":"b"})).await;
        assert_eq!(receiver.recv().await.expect("first").data["hash"], "a");
        assert_eq!(receiver.recv().await.expect("second").data["hash"], "b");
    }

    #[tokio::test]
    async fn event_hub_does_not_deduplicate_confirmations() {
        // The node can send the same confirmation more than once, and the upstream
        // guide requires integrations to dedupe by hash outside the node. The gateway
        // does not do that for them: every event it receives is delivered.
        let hub = EventHub::new();
        let mut receiver = hub.subscribe();
        hub.publish("nano.confirmation", json!({"hash":"a"})).await;
        hub.publish("nano.confirmation", json!({"hash":"a"})).await;
        assert_eq!(receiver.recv().await.expect("first").data["hash"], "a");
        assert_eq!(receiver.recv().await.expect("second").data["hash"], "a");
    }

    #[tokio::test]
    async fn event_hub_reports_broadcast_lag_by_closing_the_stream() {
        // A subscriber that cannot keep up is closed rather than told about it. The
        // node offers no replay, so there is nothing to catch up with.
        let hub = EventHub::with_capacity(1);
        let mut receiver = hub.subscribe();
        hub.publish("nano.confirmation", json!({"hash": "a"})).await;
        hub.publish("nano.confirmation", json!({"hash": "b"})).await;
        assert!(matches!(
            receiver.recv().await,
            Err(broadcast::error::RecvError::Lagged(_))
        ));
    }

    #[test]
    fn rpc_and_websocket_pools_can_have_different_lengths() {
        let config = Config {
            node_ws_urls: vec!["ws://127.0.0.1:1".into(), "ws://127.0.0.1:2".into()],
            ..Config::default()
        };
        assert!(config.validate().is_ok());
    }

    #[tokio::test]
    async fn websocket_failure_does_not_change_rpc_selection_or_health() {
        let state = AppState::new(Config {
            node_rpc_urls: vec!["http://127.0.0.1:1".into(), "http://127.0.0.1:2".into()],
            node_ws_urls: vec!["ws://127.0.0.1:1".into(), "ws://127.0.0.1:2".into()],
            ..Config::default()
        })
        .expect("state");
        assert!(run_ws_bridge(state.clone()).await.is_err());
        assert_eq!(state.native.active_index(), 0);
        assert!(state
            .native
            .unhealthy_until
            .lock()
            .await
            .iter()
            .all(|until| *until <= Instant::now()));
    }

    #[tokio::test]
    async fn rpc_failure_does_not_change_websocket_selection_or_health() {
        let state = AppState::new(Config {
            node_rpc_urls: vec!["http://127.0.0.1:1".into(), "http://127.0.0.1:2".into()],
            node_ws_urls: vec!["ws://127.0.0.1:3".into()],
            ..Config::default()
        })
        .expect("state");
        assert!(state.native.call("version", &json!({})).await.is_err());
        assert_eq!(
            state.active_ws_url().await.expect("WS unaffected"),
            (0, "ws://127.0.0.1:3".into())
        );
        state.mark_ws_unhealthy(0).await;
        assert!(state.active_ws_url().await.is_err());
    }

    #[tokio::test]
    async fn websocket_missing_pong_clears_readiness_and_rotates_only_ws() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("listener");
        let address = listener.local_addr().expect("address");
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.expect("client");
            let mut socket = accept_async(stream).await.expect("handshake");
            socket.next().await.expect("subscribe").expect("frame");
            socket
                .send(Message::Text(json!({"ack":"subscribe"}).to_string()))
                .await
                .expect("ack");
            // Hold the TCP connection open without reading/responding to ping.
            tokio::time::sleep(Duration::from_secs(35)).await;
            drop(socket);
        });
        let state = AppState::new(Config {
            node_ws_urls: vec![format!("ws://{address}"), "ws://127.0.0.1:1".into()],
            ..Config::default()
        })
        .expect("state");
        let error = tokio::time::timeout(
            Duration::from_secs(34),
            run_ws_bridge_with_confirmation_liveness(state.clone(), Duration::from_secs(40)),
        )
        .await
        .expect("bounded heartbeat")
        .expect_err("missing pong");
        assert!(error.to_string().contains("heartbeat timed out"));
        assert!(!state.upstream_ready.load(Ordering::Relaxed));
        assert_eq!(state.active_ws_url().await.expect("standby").0, 1);
        assert_eq!(state.native.active_index(), 0);
        server.abort();
    }

    #[tokio::test]
    async fn websocket_silent_after_ack_rotates_only_ws() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("listener");
        let address = listener.local_addr().expect("listener address");
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.expect("client");
            let mut socket = accept_async(stream).await.expect("handshake");
            socket.next().await.expect("subscribe").expect("frame");
            socket
                .send(Message::Text(json!({"ack":"subscribe"}).to_string()))
                .await
                .expect("ack");
            tokio::time::sleep(Duration::from_secs(1)).await;
        });
        let state = AppState::new(Config {
            node_rpc_urls: vec!["http://127.0.0.1:1".into(), "http://127.0.0.1:2".into()],
            node_ws_urls: vec![format!("ws://{address}"), "ws://127.0.0.1:1".into()],
            ..Config::default()
        })
        .expect("state");

        let error =
            run_ws_bridge_with_confirmation_liveness(state.clone(), Duration::from_millis(50))
                .await
                .expect_err("silent upstream must fail delivery liveness");

        assert!(error
            .to_string()
            .contains("confirmation delivery timed out"));
        assert!(!state.upstream_ready.load(Ordering::Relaxed));
        assert_eq!(state.active_ws_url().await.expect("standby").0, 1);
        assert_eq!(state.native.active_index(), 0);
        server.abort();
    }

    #[tokio::test]
    async fn websocket_connection_failure_emits_no_events() {
        let config = Config {
            node_ws_urls: vec!["ws://127.0.0.1:1".into()],
            ..Config::default()
        };
        let state = AppState::new(config).expect("state");
        let mut receiver = state.events.subscribe();
        assert!(run_ws_bridge(state.clone()).await.is_err());
        assert!(receiver.try_recv().is_err());
    }

    #[tokio::test]
    async fn websocket_subscription_normalizes_binary_confirmation_and_close() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("websocket listener");
        let address = listener.local_addr().expect("listener address");
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.expect("websocket client");
            let mut socket = accept_async(stream).await.expect("websocket handshake");
            let request = socket
                .next()
                .await
                .expect("subscribe frame")
                .expect("frame");
            assert!(request
                .into_text()
                .expect("subscribe text")
                .contains("confirmation"));
            socket
                .send(Message::Text(
                    json!({"ack":"subscribe","topic":"confirmation"}).to_string(),
                ))
                .await
                .expect("ack");
            socket
                .send(Message::Ping(vec![1, 2, 3]))
                .await
                .expect("ping");
            let pong = tokio::time::timeout(Duration::from_secs(1), socket.next())
                .await
                .expect("pong timeout")
                .expect("pong frame")
                .expect("pong message");
            assert!(matches!(pong, Message::Pong(_)));
            socket
                .send(Message::Text("not-json".into()))
                .await
                .expect("malformed event");
            socket
                .send(Message::Binary(
                    json!({"topic":"confirmation","message":{"hash":"A","account":"nano_test"}})
                        .to_string()
                        .into(),
                ))
                .await
                .expect("confirmation");
            socket
                .send(Message::Text(
                    json!({"topic":"confirmation","message":{"hash":"A","account":"nano_test"}})
                        .to_string(),
                ))
                .await
                .expect("duplicate confirmation");
            socket.close(None).await.expect("close");
        });
        let config = Config {
            node_ws_urls: vec![format!("ws://{address}")],
            ..Config::default()
        };
        let state = AppState::new(config).expect("state");
        let mut receiver = state.events.subscribe();
        assert!(run_ws_bridge(state.clone()).await.is_ok());
        server.await.expect("server task");
        // The bridge publishes confirmations and nothing else: the node announces
        // neither a connection nor a disconnection.
        let first = receiver.recv().await.expect("confirmation");
        assert_eq!(first.event, "nano.confirmation");
        assert_eq!(first.data["hash"], "A");
        assert_eq!(first.data["account"], "nano_test");
    }

    #[tokio::test]
    async fn websocket_unacknowledged_subscription_never_becomes_ready() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("listener");
        let address = listener.local_addr().expect("address");
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.expect("client");
            let mut socket = accept_async(stream).await.expect("handshake");
            let request = socket
                .next()
                .await
                .expect("subscribe")
                .expect("frame")
                .into_text()
                .expect("text");
            assert_eq!(
                serde_json::from_str::<Value>(&request).expect("json")["ack"],
                true
            );
            tokio::time::sleep(Duration::from_secs(6)).await;
        });
        let state = AppState::new(Config {
            node_ws_urls: vec![format!("ws://{address}")],
            ..Config::default()
        })
        .expect("state");
        let bridge = tokio::spawn(run_ws_bridge(state.clone()));
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert!(!state.upstream_ready.load(Ordering::Relaxed));
        assert!(tokio::time::timeout(Duration::from_secs(6), bridge)
            .await
            .expect("bounded timeout")
            .expect("task")
            .is_err());
        assert!(!state.upstream_ready.load(Ordering::Relaxed));
        server.abort();
    }

    #[tokio::test]
    async fn websocket_subscription_rejection_keeps_readiness_false() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("listener");
        let address = listener.local_addr().expect("address");
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.expect("client");
            let mut socket = accept_async(stream).await.expect("handshake");
            socket.next().await.expect("subscribe").expect("frame");
            socket
                .send(Message::Text(
                    json!({"error":"subscription rejected"}).to_string(),
                ))
                .await
                .expect("reject");
        });
        let state = AppState::new(Config {
            node_ws_urls: vec![format!("ws://{address}")],
            ..Config::default()
        })
        .expect("state");
        assert!(run_ws_bridge(state.clone()).await.is_err());
        assert!(!state.upstream_ready.load(Ordering::Relaxed));
        server.await.expect("server");
    }

    #[tokio::test]
    async fn websocket_can_reconnect_and_resubscribe() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("websocket listener");
        let address = listener.local_addr().expect("listener address");
        let server = tokio::spawn(async move {
            for _ in 0..2 {
                let (stream, _) = listener.accept().await.expect("websocket client");
                let mut socket = accept_async(stream).await.expect("websocket handshake");
                let request = socket
                    .next()
                    .await
                    .expect("subscribe frame")
                    .expect("frame");
                assert!(request
                    .into_text()
                    .expect("subscribe text")
                    .contains("confirmation"));
                socket
                    .send(Message::Text(json!({"ack":"subscribe"}).to_string()))
                    .await
                    .expect("ack");
                socket.close(None).await.expect("close");
            }
        });
        let config = Config {
            node_ws_urls: vec![format!("ws://{address}"), format!("ws://{address}")],
            ..Config::default()
        };
        let state = AppState::new(config).expect("state");
        assert!(run_ws_bridge(state.clone()).await.is_ok());
        assert!(run_ws_bridge(state.clone()).await.is_ok());
        server.await.expect("server task");
        assert_eq!(state.metrics.upstream_reconnects.load(Ordering::Relaxed), 1);
    }
}
