//! Nano RPC Gateway: a deliberately small, typed boundary around native Nano RPC.

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
    extract::State,
    http::{header, HeaderMap, HeaderName, HeaderValue, Method, StatusCode},
    response::{
        sse::{Event, KeepAlive, Sse},
        Html, IntoResponse, Response,
    },
    routing::{get, post},
    Json, Router,
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
use tokio_tungstenite::{connect_async, tungstenite::Message};
use tower::limit::ConcurrencyLimitLayer;
use tower_http::{cors::CorsLayer, limit::RequestBodyLimitLayer, trace::TraceLayer};
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
enable_inspector: false
log_rpc: false
cors_origins:
  - "http://127.0.0.1:8080"
  - "http://localhost:8080"
  - "https://playground.open-rpc.org"
"#;
const EVENT_HISTORY_CAPACITY: usize = 256;
const MAX_ACCOUNT_FILTER_BYTES: usize = 4096;
const MAX_ACCOUNT_FILTER_ITEMS: usize = 64;

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
    #[serde(default)]
    pub enable_inspector: bool,
    /// Emit safe JSON-RPC and SSE lifecycle diagnostics.
    #[serde(default)]
    pub log_rpc: bool,
    #[serde(default = "default_cors_origins")]
    pub cors_origins: Vec<String>,
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
    vec![
        "http://127.0.0.1:8080".into(),
        "http://localhost:8080".into(),
        "https://playground.open-rpc.org".into(),
    ]
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
        if self.node_rpc_urls.is_empty() || self.node_rpc_urls.len() != self.node_ws_urls.len() {
            return Err(GatewayError::InvalidRequest(
                "node_rpc_urls and node_ws_urls must be non-empty parallel lists".into(),
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
    #[error("upstream unavailable: {0}")]
    UpstreamUnavailable(String),
    #[error("upstream transport: {0}")]
    UpstreamTransport(String),
    #[error("upstream outcome is indeterminate: {0}")]
    UpstreamIndeterminate(String),
    #[error("invalid request: {0}")]
    InvalidRequest(String),
    #[error("authentication: {0}")]
    Auth(String),
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
            description: "Read account frontier and representative state.",
            schema_provenance: "profiles/nano-node-v28.2.yaml",
        },
        MethodSpec {
            name: "receivable",
            scope: Scope::Base,
            params: "AccountParams",
            result: "ReceivableResult",
            description: "Read receivable blocks for an account.",
            schema_provenance: "profiles/nano-node-v28.2.yaml",
        },
        MethodSpec {
            name: "account_balance",
            scope: Scope::Base,
            params: "AccountParams",
            result: "AccountBalanceResult",
            description: "Read an account balance.",
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
            description: "Read block metadata.",
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
            name: "process",
            scope: Scope::Common,
            params: "ProcessParams",
            result: "ProcessResult",
            description: "Submit a precomputed block to the node.",
            schema_provenance: "profiles/nano-node-v28.2.yaml",
        },
        MethodSpec {
            name: "work_generate",
            scope: Scope::Work,
            params: "WorkGenerateParams",
            result: "WorkGenerateResult",
            description: "Delegate proof-of-work generation to the node.",
            schema_provenance: "profiles/nano-node-v28.2.yaml",
        },
    ]
}

pub fn openrpc_document(
    profile: &str,
    include_work: bool,
    include_discovery: bool,
    gateway_url: &str,
) -> Value {
    openrpc_document_with_auth(profile, include_work, include_discovery, gateway_url, true)
}

/// Build the runtime discovery document, including whether Common methods are public.
pub fn openrpc_document_with_auth(
    profile: &str,
    include_work: bool,
    include_discovery: bool,
    gateway_url: &str,
    require_common_auth: bool,
) -> Value {
    let mut document =
        build_openrpc_document(profile, include_work, include_discovery, gateway_url);
    document["x-nano-common-authentication"] = json!({"required": require_common_auth});
    if !require_common_auth {
        if let Some(method) = document["methods"].as_array_mut().and_then(|methods| {
            methods
                .iter_mut()
                .find(|method| method["name"] == "process")
        }) {
            method["x-nano-capability"]["requires"] = json!([]);
        }
    }
    let digest = openrpc_artifact_digest(profile, include_work, include_discovery);
    if let Some(object) = document.as_object_mut() {
        object.insert("x-nano-artifact-sha256".into(), Value::String(digest));
    }
    document
}

pub fn openrpc_artifact_digest(
    profile: &str,
    include_work: bool,
    include_discovery: bool,
) -> String {
    let document = build_openrpc_document(
        profile,
        include_work,
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

fn event_registry() -> [EventSpec; 2] {
    [
        EventSpec {
            name: "nano.confirmation",
            summary: "A block confirmation observed from the configured Nano profile.",
            params_schema: "ConfirmationParams",
        },
        EventSpec {
            name: "nano.stream_reset",
            summary: "Stream continuity was lost or changed; reconcile before continuing.",
            params_schema: "StreamResetParams",
        },
    ]
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
                    "examples": [{"payload": notification_example(event.name, profile)}]
                }),
            )
        })
        .collect::<serde_json::Map<_, _>>();
    json!({
        "asyncapi": "3.1.0",
        "info": {
            "title": "Nano Gateway Events",
            "version": "0.1.0",
            "description": "Receive-only SSE notifications for a pinned Nano node profile. A reset means consumers must reconcile authoritative state with Nano RPC before applying later confirmations."
        },
        "defaultContentType": "application/json",
        "servers": {"gateway": {"host": host, "protocol": protocol, "pathname": "/events/confirmations"}},
        "channels": {
            "confirmations": {
                "address": "/events/confirmations",
                "title": "Confirmation event stream",
                "description": "Bounded process-local replay. Send Last-Event-ID to resume; nano.stream_reset reports that replay or live continuity is unavailable.",
                "servers": [{"$ref": "#/servers/gateway"}],
                "messages": {
                    "nanoConfirmation": {"$ref": "#/components/messages/nano.confirmation"},
                    "nanoStreamReset": {"$ref": "#/components/messages/nano.stream_reset"}
                },
                "bindings": {"http": {"bindingVersion": "0.3.0"}}
            }
        },
        "operations": {
            "receiveConfirmations": {
                "action": "receive",
                "summary": "Receive Nano confirmation and stream reset notifications.",
                "channel": {"$ref": "#/channels/confirmations"},
                "bindings": {"http": {
                    "method": "GET",
                    "query": {"type": "object", "properties": {
                        "accounts": {"type": "string", "description": "Comma-separated account filter."},
                        "hashes": {"type": "string", "description": "Comma-separated block hash filter."}
                    }},
                    "bindingVersion": "0.3.0"
                }}
            }
        },
        "components": {
            "messages": messages,
            "schemas": {
                "ConfirmationParams": confirmation_params_schema(),
                "StreamResetParams": stream_reset_params_schema()
            }
        },
        "x-nano-profile": profile,
        "x-http-response": {
            "contentType": "text/event-stream",
            "headers": {"Last-Event-ID": {"type": "string", "description": "Resume after this SSE event cursor."}}
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
        "type": "object", "required": ["profile", "hash"],
        "properties": {
            "profile": {"type": "string"}, "hash": {"type": "string"},
            "account": {"type": "string"}, "destination": {"type": "string"},
            "amount": {"type": "string"}, "confirmation_type": {"type": "string"},
            "block": {"type": "object", "additionalProperties": true},
            "election_info": {"type": "object", "additionalProperties": true}
        }, "additionalProperties": true
    })
}

fn stream_reset_params_schema() -> Value {
    json!({
        "type": "object", "additionalProperties": false,
        "required": ["reason", "profile", "reconcile"],
        "properties": {
            "reason": {"type": "string", "enum": ["upstream_connected", "upstream_reconnected", "upstream_disconnected", "upstream_closed", "replay_unavailable", "subscriber_lagged"]},
            "profile": {"type": "string"}, "reconcile": {"type": "string"}
        }
    })
}

fn notification_example(method: &str, profile: &str) -> Value {
    match method {
        "nano.confirmation" => notification(
            method,
            json!({"profile": profile, "hash": "ABC123", "account": "nano_..."}),
        ),
        _ => notification(method, reset_params("replay_unavailable", profile)),
    }
}

fn notification(method: &str, params: Value) -> Value {
    json!({"jsonrpc": "2.0", "method": method, "params": params})
}

fn reset_params(reason: &str, profile: &str) -> Value {
    json!({"reason": reason, "profile": profile, "reconcile": "Query authoritative Nano RPC state before applying new confirmations"})
}

fn build_openrpc_document(
    profile: &str,
    include_work: bool,
    include_discovery: bool,
    gateway_url: &str,
) -> Value {
    let mut methods = registry()
        .into_iter()
        .filter(|method| include_work || method.scope != Scope::Work)
        .map(openrpc_method)
        .collect::<Vec<_>>();
    if include_discovery {
        methods.push(json!({
            "name": "rpc.discover",
            "summary": "Return this OpenRPC document.",
            "params": [],
            "result": {"name": "result", "schema": {"type": "object", "properties": {}, "additionalProperties": true}}
        }));
    }
    json!({
        "openrpc": "1.3.2", "info": {"title":"Nano Gateway", "version":"0.1.0", "description":"JSON-RPC 2.0 adapter for a pinned Nano node profile."},
        "servers": [{"name":"gateway", "url":gateway_url, "variables":{"gatewayUrl":{"default":gateway_url}}}],
        "methods": methods,
        "components": {"schemas": {
            "AccountParams":{"type":"object","required":["account"],"properties":{"account":{"type":"string","minLength":1}}},
            "AccountInfoParams":{"$ref":"#/components/schemas/AccountParams"},
            "EmptyParams":{"type":"object","properties":{},"additionalProperties":false},
            "VersionResult":{"type":"object","required":["rpc_version"],"properties":{"rpc_version":{"type":"string"}}},
            "BlockCountResult":{"type":"object","required":["count","unchecked","cemented"],"properties":{"count":{"type":"string"},"unchecked":{"type":"string"},"cemented":{"type":"string"}}},
            "AccountBalanceResult":{"type":"object","required":["balance","receivable"],"properties":{"balance":{"type":"string"},"receivable":{"type":"string"}}},
            "AccountHistoryResult":{"type":"object","required":["account","history"],"properties":{"account":{"type":"string"},"history":{"type":"array","items":true}}},
            "AccountInfoResult":{"type":"object","required":["opened","frontier","open_block","representative_block","balance","confirmed_frontier","confirmed_balance","confirmation_height","confirmation_height_frontier"],"properties":{"opened":{"type":"boolean"},"frontier":{"oneOf":[{"type":"string"},{"type":"null"}]},"open_block":{"oneOf":[{"type":"string"},{"type":"null"}]},"representative_block":{"oneOf":[{"type":"string"},{"type":"null"}]},"balance":{"type":"string"},"confirmed_frontier":{"oneOf":[{"type":"string"},{"type":"null"}]},"confirmed_balance":{"type":"string"},"confirmation_height":{"type":"string"},"confirmation_height_frontier":{"oneOf":[{"type":"string"},{"type":"null"}]}}},
            "ReceivableEntry":{"type":"object","required":["source","hash","amount"],"properties":{"source":{"type":"string"},"hash":{"type":"string"},"amount":{"type":"string"}}},
            "ReceivableResult":{"type":"array","items":{"$ref":"#/components/schemas/ReceivableEntry"}},
            "AccountHistoryParams":{"type":"object","required":["account"],"properties":{"account":{"type":"string","minLength":1},"count":{"type":"integer","minimum":1}}},
            "BlockParams":{"type":"object","required":["hash"],"properties":{"hash":{"type":"string","minLength":1}}},
            "BlockInfoResult":{"type":"object","required":["hash","block_account","amount","balance","height","subtype","confirmed","block"],"properties":{"hash":{"type":"string"},"block_account":{"type":"string"},"amount":{"type":"string"},"balance":{"type":"string"},"height":{"type":"string"},"subtype":{"type":"string"},"confirmed":{"type":"boolean"},"block":{"type":"object","additionalProperties":true}}},
            "BlocksParams":{"type":"object","required":["hashes"],"properties":{"hashes":{"type":"array","minItems":1,"items":{"type":"string","minLength":1}}}},
            "BlocksInfoResult":{"type":"object","required":["blocks"],"properties":{"blocks":{"type":"object","additionalProperties":{"$ref":"#/components/schemas/BlockInfoResult"}}}},
            "ProcessParams":{"type":"object","required":["block"],"properties":{"block":{"type":"object","additionalProperties":true}}}, "ProcessResult":{"type":"object","required":["hash"],"properties":{"hash":{"type":"string"}}},
            "WorkGenerateParams":{"type":"object","required":["hash","difficulty"],"properties":{"hash":{"type":"string","minLength":1},"difficulty":{"type":"string","minLength":1}}}, "WorkGenerateResult":{"type":"object","required":["hash","work","difficulty","multiplier"],"properties":{"hash":{"type":"string"},"work":{"type":"string"},"difficulty":{"type":"string"},"multiplier":{"type":"string"}}}
        }, "errors": {
            "InvalidRequest": {"code": -32602, "message": "Invalid method parameters"},
            "MethodNotFound": {"code": -32601, "message": "Method not found"},
            "Unauthorized": {"code": -32001, "message": "Unauthorized"},
            "UpstreamFailure": {"code": -32000, "message": "Upstream request failed"},
            "UpstreamIndeterminate": {"code": -32002, "message": "Upstream outcome is indeterminate"},
            "UpstreamRejection": {"code": -32010, "message": "Request rejected by upstream"}
        }}, "x-nano-profile": profile
    })
}

fn openrpc_method(method: MethodSpec) -> Value {
    let params = match method.name {
        "receivable" | "account_info" | "account_balance" => vec![json!({
            "name": "account",
            "required": true,
            "schema": {"type": "string"}
        })],
        "account_history" => vec![
            json!({"name": "account", "required": true, "schema": {"type": "string"}}),
            json!({"name": "count", "required": false, "schema": {"type": "integer"}}),
        ],
        "block_info" => vec![json!({
            "name": "hash",
            "required": true,
            "schema": {"type": "string"}
        })],
        "blocks_info" => vec![json!({
            "name": "hashes",
            "required": true,
            "schema": {"type": "array", "items": {"type": "string"}}
        })],
        "process" => vec![json!({
            "name": "block",
            "required": true,
            "schema": {"type": "object"}
        })],
        "work_generate" => vec![
            json!({"name": "hash", "required": true, "schema": {"type": "string"}}),
            json!({"name": "difficulty", "required": true, "schema": {"type": "string"}}),
        ],
        _ => Vec::new(),
    };
    let mut document = json!({
        "name": method.name,
        "summary": method.description,
        "x-nano-schema-provenance": method.schema_provenance,
        "paramStructure": "by-name",
        "params": params,
        "result": {
            "name": "result",
            "schema": {"$ref": format!("#/components/schemas/{}", method.result)}
        },
        "errors": [
            {"$ref":"#/components/errors/InvalidRequest"},
            {"$ref":"#/components/errors/MethodNotFound"},
            {"$ref":"#/components/errors/Unauthorized"},
            {"$ref":"#/components/errors/UpstreamFailure"},
            {"$ref":"#/components/errors/UpstreamIndeterminate"},
            {"$ref":"#/components/errors/UpstreamRejection"}
        ]
    });
    if method.name == "process" {
        document["x-nano-capability"] = json!({
            "name": "process",
            "browser_safe": true,
            "requires": ["authenticated"]
        });
    }
    document
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
                } else if error.is_timeout() {
                    GatewayError::UpstreamIndeterminate(error.to_string())
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
                "Nano RPC response >>>"
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
            if action == "account_info"
                && error
                    .as_str()
                    .is_some_and(|message| message.to_ascii_lowercase().contains("not found"))
            {
                return Ok(json!({
                    "frontier": null,
                    "open_block": null,
                    "representative_block": null,
                    "balance": "0",
                    "confirmed_frontier": null,
                    "confirmed_balance": "0",
                    "confirmation_height": "0",
                    "confirmation_height_frontier": null
                }));
            }
            if is_rate_limited_error(error) {
                tracing::warn!(
                    action,
                    upstream = %self.label,
                    upstream_url = %redacted_endpoint,
                    authorization,
                    reason = %error,
                    "native upstream rate limited request"
                );
                return Err(GatewayError::UpstreamUnavailable(
                    "upstream rate limit exceeded".into(),
                ));
            }
            if action == "process" {
                tracing::warn!(
                    action,
                    upstream = %self.label,
                    upstream_url = %redacted_endpoint,
                    authorization,
                    "native upstream rejected a process request"
                );
            } else {
                tracing::warn!(
                    action,
                    upstream = %self.label,
                    upstream_url = %redacted_endpoint,
                    authorization,
                    reason = %error,
                    "native upstream rejected request"
                );
            }
            return Err(GatewayError::UpstreamRejected(error.to_string()));
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

fn upstream_label(url: &Url) -> String {
    let host = url.host_str().unwrap_or("(missing host)");
    match url.port() {
        Some(port) => format!("{}://{host}:{port}", url.scheme()),
        None => format!("{}://{host}", url.scheme()),
    }
}

fn native_request_body(action: &str, params: &Value) -> serde_json::Map<String, Value> {
    let mut body = serde_json::Map::new();
    let upstream_action = match action {
        "receivable" => "pending",
        other => other,
    };
    body.insert("action".into(), Value::String(upstream_action.into()));
    if let Value::Object(values) = params {
        let mut native_params = values.clone();
        // `receivable` is provided by the gateway's separate normalized method;
        // it is not an account_info argument accepted by Nano node RPC.
        if action == "account_info" {
            native_params.remove("receivable");
            native_params.insert("include_confirmed".into(), Value::String("true".into()));
        }
        if action == "receivable" {
            native_params.insert("source".into(), Value::String("true".into()));
        }
        body.extend(native_params);
    }
    body
}

const UPSTREAM_COOLDOWN: Duration = Duration::from_secs(15);

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
                Err(error) if self.can_failover(&error) => {
                    self.mark_unhealthy(index).await;
                    last_error = Some(error);
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

    pub async fn active_ws_url(&self, ws_urls: &[String]) -> Result<(usize, String), GatewayError> {
        let start = self.active_index();
        let now = Instant::now();
        let unhealthy = self.unhealthy_until.lock().await.clone();
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

    pub async fn mark_ws_unhealthy(&self, index: usize) {
        self.unhealthy_until.lock().await[index] = Instant::now() + UPSTREAM_COOLDOWN;
        let next = (index + 1) % self.clients.len();
        self.active.store(next, Ordering::Release);
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

#[derive(Clone)]
pub struct EventHub {
    generation: String,
    next: Arc<Mutex<u64>>,
    history: Arc<Mutex<VecDeque<NanoEvent>>>,
    tx: broadcast::Sender<NanoEvent>,
    capacity: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NanoEvent {
    pub id: String,
    pub event: String,
    pub data: Value,
}

impl EventHub {
    pub fn new(capacity: usize) -> Self {
        static GENERATION_COUNTER: AtomicU64 = AtomicU64::new(0);
        let counter = GENERATION_COUNTER.fetch_add(1, Ordering::Relaxed) + 1;
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        let (tx, _) = broadcast::channel(capacity.max(1));
        Self {
            generation: format!("{timestamp:x}-{counter:x}"),
            next: Arc::new(Mutex::new(0)),
            history: Arc::new(Mutex::new(VecDeque::with_capacity(capacity))),
            tx,
            capacity: capacity.max(1),
        }
    }
    pub async fn publish(&self, event: impl Into<String>, data: Value) -> NanoEvent {
        let mut history = self.history.lock().await;
        let event = event.into();
        if let Some(existing) = history.iter().find(|item| {
            item.event == event
                && item.data.get("hash").and_then(Value::as_str)
                    == data.get("hash").and_then(Value::as_str)
                && item.data.get("account").and_then(Value::as_str)
                    == data.get("account").and_then(Value::as_str)
                && item.data.get("hash").is_some()
        }) {
            return existing.clone();
        }
        let mut next = self.next.lock().await;
        *next += 1;
        let item = NanoEvent {
            id: format!("{}:{next}", self.generation),
            event,
            data,
        };
        history.push_back(item.clone());
        while history.len() > self.capacity {
            history.pop_front();
        }
        let _ = self.tx.send(item.clone());
        item
    }
    async fn replay(&self, cursor: Option<&str>) -> (bool, Vec<NanoEvent>) {
        let history = self.history.lock().await;
        let parsed = cursor.and_then(parse_event_cursor);
        let same_generation = parsed
            .as_ref()
            .is_some_and(|(generation, _)| generation == &self.generation);
        let oldest = history
            .front()
            .and_then(|item| parse_event_cursor(&item.id));
        let reset = cursor.is_some()
            && (!same_generation
                || parsed.as_ref().is_none_or(|(_, requested)| {
                    oldest
                        .as_ref()
                        .is_some_and(|(_, oldest)| *requested < oldest.saturating_sub(1))
                }));
        let sequence = parsed
            .filter(|(generation, _)| generation == &self.generation)
            .map(|(_, sequence)| sequence);
        let events = history
            .iter()
            .filter(|item| {
                sequence.is_none_or(|cursor| {
                    parse_event_cursor(&item.id).is_some_and(|(_, sequence)| sequence > cursor)
                })
            })
            .cloned()
            .collect();
        (reset, events)
    }
    fn subscribe(&self) -> broadcast::Receiver<NanoEvent> {
        self.tx.subscribe()
    }
}

fn parse_event_cursor(value: &str) -> Option<(String, u64)> {
    let (generation, sequence) = value.split_once(':')?;
    Some((generation.to_owned(), sequence.parse().ok()?))
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
}

#[derive(Clone, Default)]
pub struct Metrics {
    pub requests: Arc<AtomicU64>,
    pub errors: Arc<AtomicU64>,
    pub active_streams: Arc<AtomicU64>,
    pub replay_resets: Arc<AtomicU64>,
    pub replay_hits: Arc<AtomicU64>,
    pub replay_misses: Arc<AtomicU64>,
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
    hash_filter_count: usize,
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
                event = "sse_subscription_closed",
                duration_ms = self.started.elapsed().as_millis() as u64,
                account_filter_count = self.account_filter_count,
                hash_filter_count = self.hash_filter_count,
                "SSE subscription closed"
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
    Event::default().id(item.id).event(item.event).data(data)
}

fn reset_sse_event(reason: &str, profile: &str) -> Event {
    let data = serde_json::to_string(&notification(
        "nano.stream_reset",
        reset_params(reason, profile),
    ))
    .unwrap_or_else(|_| "null".into());
    Event::default().event("nano.stream_reset").data(data)
}

impl AppState {
    pub fn new(config: Config) -> Result<Self, GatewayError> {
        let config = config.validate()?;
        let verifying_key = config
            .auth_public_key
            .as_deref()
            .map(parse_public_key)
            .transpose()?;
        Ok(Self {
            native: NativeRouter::with_response_logging(&config.node_rpc_urls, config.log_rpc)?,
            events: EventHub::new(EVENT_HISTORY_CAPACITY),
            metrics: Metrics::default(),
            config,
            verifying_key,
            upstream_ready: Arc::new(AtomicBool::new(false)),
            upstream_seen: Arc::new(AtomicBool::new(false)),
            ws_selected: Arc::new(Mutex::new(None)),
        })
    }
    async fn dispatch(&self, request: RpcRequest, headers: &HeaderMap) -> RpcResponse {
        if request.jsonrpc != "2.0" || request.method.is_empty() {
            return RpcResponse::err(request.id, -32600, "Invalid Request");
        }
        if request.id_present
            && request
                .id
                .as_ref()
                .is_some_and(|id| !(id.is_string() || id.is_number() || id.is_null()))
        {
            return RpcResponse::err(
                request.id,
                -32600,
                "Invalid Request: id must be string, number, or null",
            );
        }
        if !request.id_present {
            return RpcResponse::err(None, -32600, "Notifications are not supported");
        }
        if request.method == "rpc.discover" {
            return if self.config.enable_discovery {
                RpcResponse::ok(
                    request.id,
                    openrpc_document_with_auth(
                        &self.config.profile,
                        self.config.allow_work,
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
            };
        }
        let spec = registry()
            .into_iter()
            .find(|item| item.name == request.method);
        let Some(spec) = spec else {
            return RpcResponse::err(request.id, -32601, "Method not found");
        };
        if spec.scope == Scope::Work && !self.config.allow_work {
            return RpcResponse::err(request.id, -32604, "Work generation is disabled");
        }
        let common_auth_required = spec.scope != Scope::Common || self.config.require_common_auth;
        if spec.scope != Scope::Base
            && common_auth_required
            && !self.authorized(headers, spec.scope)
        {
            return RpcResponse::err(request.id, -32001, "Unauthorized");
        }
        let params = request.params.unwrap_or_else(|| json!({}));
        if let Err(message) = validate_params(spec.name, &params) {
            return RpcResponse::err(request.id, -32602, message);
        }
        match self.native.call(spec.name, &params).await {
            Ok(result) => {
                let normalized = normalize_result(spec.name, &result, &params);
                if validate_result(spec.name, &normalized) {
                    RpcResponse::ok(request.id, normalized)
                } else {
                    RpcResponse::err(
                        request.id,
                        -32000,
                        "Upstream result does not match the selected profile schema",
                    )
                }
            }
            Err(GatewayError::Upstream(_)) => {
                // Native responses can echo request material (for example a
                // signed block). Keep that diagnostic inside the adapter and
                // expose only the stable public error contract.
                RpcResponse::err(request.id, -32000, "Upstream request failed")
            }
            Err(GatewayError::UpstreamRejected(_)) => RpcResponse::err_with_data(
                request.id,
                -32010,
                "Request rejected by upstream",
                Some(json!({"kind":"upstream_rejection"})),
            ),
            Err(GatewayError::UpstreamIndeterminate(_)) => RpcResponse::err_with_data(
                request.id,
                -32002,
                "Upstream outcome is indeterminate",
                Some(json!({"kind":"indeterminate"})),
            ),
            Err(GatewayError::UpstreamUnavailable(_)) | Err(GatewayError::UpstreamTransport(_)) => {
                RpcResponse::err_with_data(
                    request.id,
                    -32000,
                    "Upstream unavailable",
                    Some(json!({"kind":"upstream_unavailable"})),
                )
            }
            Err(GatewayError::InvalidRequest(message)) => RpcResponse::err_with_data(
                request.id,
                -32602,
                message,
                Some(json!({"kind":"invalid_request"})),
            ),
            Err(GatewayError::Auth(_)) => RpcResponse::err_with_data(
                request.id,
                -32001,
                "Unauthorized",
                Some(json!({"kind":"unauthorized"})),
            ),
            Err(error) => RpcResponse::err(request.id, -32000, error.to_string()),
        }
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
        let scheme = if self.config.tls_cert.is_some() && self.config.tls_key.is_some() {
            "https"
        } else {
            "http"
        };
        format!("{scheme}://{}/rpc", self.config.listen)
    }
}

fn validate_params(method: &str, params: &Value) -> Result<(), String> {
    let object = params
        .as_object()
        .ok_or_else(|| "params must be an object".to_string())?;
    let required = match method {
        "account_info" | "receivable" | "account_balance" | "account_history" => "account",
        "block_info" => "hash",
        "blocks_info" => "hashes",
        "process" => "block",
        "work_generate" => "hash",
        _ => return Ok(()),
    };
    let value = object
        .get(required)
        .ok_or_else(|| format!("missing required parameter: {required}"))?;
    let valid = match required {
        "account" | "hash" => value.as_str().is_some_and(|value| !value.is_empty()),
        "hashes" => value.as_array().is_some_and(|values| {
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
    if method == "account_history"
        && object
            .get("count")
            .is_some_and(|count| !count.as_i64().is_some_and(|value| value >= 1))
    {
        return Err("invalid parameter: count".into());
    }
    if method == "work_generate"
        && object
            .get("difficulty")
            .and_then(Value::as_str)
            .is_none_or(str::is_empty)
    {
        return Err("missing required parameter: difficulty".into());
    }
    Ok(())
}

fn has_string(object: &serde_json::Map<String, Value>, name: &str) -> bool {
    object
        .get(name)
        .and_then(Value::as_str)
        .is_some_and(|value| !value.is_empty())
}

fn validate_result(method: &str, result: &Value) -> bool {
    if method == "receivable" {
        return result.as_array().is_some_and(|entries| {
            entries.iter().all(|entry| {
                entry.as_object().is_some_and(|item| {
                    ["source", "hash", "amount"]
                        .iter()
                        .all(|field| has_string(item, field))
                })
            })
        });
    }
    let Some(object) = result.as_object() else {
        return false;
    };
    match method {
        "version" => has_string(object, "rpc_version"),
        "block_count" => ["count", "unchecked", "cemented"]
            .iter()
            .all(|field| has_string(object, field)),
        "account_info" => {
            object.get("opened").is_some_and(Value::is_boolean)
                && ["balance", "confirmed_balance", "confirmation_height"]
                    .iter()
                    .all(|field| has_string(object, field))
        }
        "account_balance" => ["balance", "receivable"]
            .iter()
            .all(|field| has_string(object, field)),
        "account_history" => {
            has_string(object, "account") && object.get("history").is_some_and(Value::is_array)
        }
        "block_info" => {
            [
                "hash",
                "block_account",
                "amount",
                "balance",
                "height",
                "subtype",
            ]
            .iter()
            .all(|field| has_string(object, field))
                && object.get("confirmed").is_some_and(Value::is_boolean)
                && object.get("block").is_some_and(Value::is_object)
        }
        "blocks_info" => object.get("blocks").is_some_and(|blocks| {
            blocks.as_object().is_some_and(|items| {
                items
                    .values()
                    .all(|item| validate_result("block_info", item))
            })
        }),
        "process" => has_string(object, "hash"),
        "work_generate" => ["hash", "work", "difficulty", "multiplier"]
            .iter()
            .all(|field| has_string(object, field)),
        _ => false,
    }
}

fn normalize_result(method: &str, value: &Value, params: &Value) -> Value {
    let Some(object) = value.as_object() else {
        return value.clone();
    };
    match method {
        "account_info" => {
            let mut normalized = value.clone();
            let Some(fields) = normalized.as_object_mut() else {
                return normalized;
            };
            let opened = fields
                .get("frontier")
                .and_then(Value::as_str)
                .is_some_and(|frontier| !frontier.is_empty());
            fields.insert("opened".into(), Value::Bool(opened));
            for field in [
                "frontier",
                "open_block",
                "representative_block",
                "confirmation_height_frontier",
            ] {
                fields.entry(field).or_insert(Value::Null);
            }
            if !fields.contains_key("confirmation_height") {
                let height = fields
                    .get("confirmed_height")
                    .cloned()
                    .unwrap_or(Value::String("0".into()));
                fields.insert("confirmation_height".into(), height);
            }
            normalized
        }
        "account_balance" => {
            let mut normalized = value.clone();
            let Some(fields) = normalized.as_object_mut() else {
                return normalized;
            };
            let receivable = fields
                .remove("receivable")
                .or_else(|| fields.remove("pending"))
                .unwrap_or(Value::String("0".into()));
            fields.insert("receivable".into(), receivable);
            fields.remove("pending");
            normalized
        }
        "receivable" => {
            if let Some(entries) = value.as_array() {
                return Value::Array(entries.iter().map(normalize_receivable_entry).collect());
            }
            // Public Nano RPC nodes commonly encode an empty receivable set as
            // `blocks: ""` for unopened accounts. The profile contract uses an
            // empty array, so normalize that wire representation explicitly.
            if object
                .get("blocks")
                .and_then(Value::as_str)
                .is_some_and(str::is_empty)
            {
                return Value::Array(Vec::new());
            }
            if let Some(entries) = object.get("blocks").and_then(Value::as_array) {
                return Value::Array(entries.iter().map(normalize_receivable_entry).collect());
            }
            let entries = object
                .get("blocks")
                .and_then(Value::as_object)
                .unwrap_or(object);
            Value::Array(
                entries
                    .iter()
                    .map(|(hash, item)| {
                        let mut entry = normalize_receivable_entry(item);
                        if entry
                            .get("hash")
                            .and_then(Value::as_str)
                            .is_none_or(str::is_empty)
                        {
                            if let Some(entry) = entry.as_object_mut() {
                                entry.insert("hash".into(), Value::String(hash.clone()));
                            }
                        }
                        entry
                    })
                    .collect(),
            )
        }
        "block_info" => normalize_block_info(value, params.get("hash").and_then(Value::as_str)),
        "blocks_info" => {
            let blocks = object
                .get("blocks")
                .and_then(Value::as_object)
                .unwrap_or(object);
            Value::Object(serde_json::Map::from_iter([(
                "blocks".into(),
                Value::Object(
                    blocks
                        .iter()
                        .map(|(hash, item)| (hash.clone(), normalize_block_info(item, Some(hash))))
                        .collect(),
                ),
            )]))
        }
        _ => value.clone(),
    }
}

fn normalize_receivable_entry(value: &Value) -> Value {
    let Some(object) = value.as_object() else {
        return json!({});
    };
    json!({
        "source": object.get("source").or_else(|| object.get("sender")).cloned().unwrap_or(Value::String(String::new())),
        "hash": object.get("hash").cloned().unwrap_or(Value::String(String::new())),
        "amount": object.get("amount").cloned().unwrap_or(Value::String(String::new())),
    })
}

fn normalize_block_info(value: &Value, hash: Option<&str>) -> Value {
    let Some(object) = value.as_object() else {
        return json!({});
    };
    let block = object
        .get("block")
        .cloned()
        .or_else(|| {
            object.get("contents").and_then(|contents| match contents {
                Value::Object(_) => Some(contents.clone()),
                Value::String(text) => serde_json::from_str(text).ok(),
                _ => None,
            })
        })
        .unwrap_or_else(|| json!({}));
    let subtype = object
        .get("subtype")
        .and_then(Value::as_str)
        .or_else(|| block.get("subtype").and_then(Value::as_str))
        .or_else(|| block.get("type").and_then(Value::as_str))
        .unwrap_or_default();
    json!({
        "hash": object.get("hash").and_then(Value::as_str).or(hash).unwrap_or_default(),
        "block_account": object.get("block_account").cloned().unwrap_or(Value::String(String::new())),
        "amount": object.get("amount").cloned().unwrap_or(Value::String("0".into())),
        "balance": object.get("balance").cloned().unwrap_or(Value::String("0".into())),
        "height": object.get("height").cloned().unwrap_or(Value::String("0".into())),
        "subtype": subtype,
        "confirmed": object.get("confirmed").and_then(Value::as_bool).or_else(|| object.get("confirmed").and_then(Value::as_str).map(|value| value == "true")).unwrap_or(false),
        "block": block,
    })
}

pub fn app(state: AppState) -> Router {
    let origins = state
        .config
        .cors_origins
        .iter()
        .filter_map(|origin| HeaderValue::from_str(origin).ok())
        .collect::<Vec<_>>();
    let cors = CorsLayer::new()
        .allow_origin(origins)
        .allow_methods([Method::GET, Method::POST, Method::OPTIONS])
        .allow_headers([
            header::AUTHORIZATION,
            header::CONTENT_TYPE,
            HeaderName::from_static("last-event-id"),
        ]);
    Router::new()
        .route("/rpc", post(rpc_handler))
        .route("/openrpc.json", get(openrpc_handler))
        .route("/asyncapi.json", get(asyncapi_handler))
        .route("/inspector", get(inspector_handler))
        .route("/inspector/", get(inspector_handler))
        .route("/events/confirmations", get(sse_handler))
        .route("/health", get(|| async { Json(json!({"status":"ok"})) }))
        .route("/readyz", get(ready_handler))
        .route("/metrics", get(metrics_handler))
        .layer(cors)
        .layer(RequestBodyLimitLayer::new(1024 * 1024))
        .layer(ConcurrencyLimitLayer::new(128))
        .layer(TraceLayer::new_for_http())
        .with_state(state)
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
nano_gateway_active_streams {}\nnano_gateway_replay_resets_total {}\n\
nano_gateway_replay_hits_total {}\nnano_gateway_replay_misses_total {}\n\
nano_gateway_overflow_resets_total {}\nnano_gateway_upstream_reconnects_total {}\n\
nano_gateway_sse_queue_capacity {}\n\
nano_gateway_request_duration_ms_sum {}\nnano_gateway_request_duration_ms_count {}\n",
            state.metrics.requests.load(Ordering::Relaxed),
            state.metrics.errors.load(Ordering::Relaxed),
            state.metrics.active_streams.load(Ordering::Relaxed),
            state.metrics.replay_resets.load(Ordering::Relaxed),
            state.metrics.replay_hits.load(Ordering::Relaxed),
            state.metrics.replay_misses.load(Ordering::Relaxed),
            state.metrics.overflow_resets.load(Ordering::Relaxed),
            state.metrics.upstream_reconnects.load(Ordering::Relaxed),
            EVENT_HISTORY_CAPACITY,
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

async fn rpc_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    body: Bytes,
) -> Json<RpcResponse> {
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
        ));
    }
    let value = match serde_json::from_slice::<Value>(&body) {
        Ok(value) => value,
        Err(error) => {
            state.metrics.errors.fetch_add(1, Ordering::Relaxed);
            return Json(RpcResponse::err(
                None,
                -32700,
                format!("Parse error: {error}"),
            ));
        }
    };
    if !value.is_object() {
        state.metrics.errors.fetch_add(1, Ordering::Relaxed);
        return Json(RpcResponse::err(None, -32600, "Invalid Request"));
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
            ));
        }
    };
    request.id_present = id_present;
    let method = request.method.clone();
    if log_rpc {
        log_rpc_request(&method);
    }
    let response = state.dispatch(request, &headers).await;
    if response.error.is_some() {
        state.metrics.errors.fetch_add(1, Ordering::Relaxed);
    }
    Json(response)
}
async fn openrpc_handler(State(state): State<AppState>) -> Response {
    let document = openrpc_document_with_auth(
        &state.config.profile,
        state.config.allow_work,
        state.config.enable_discovery,
        &state.gateway_url(),
        state.config.require_common_auth,
    );
    let digest = document["x-nano-artifact-sha256"]
        .as_str()
        .map(str::to_owned);
    let mut response = Json(document).into_response();
    if let Some(digest) = digest {
        if let Ok(value) = HeaderValue::from_str(&format!("\"{digest}\"")) {
            response.headers_mut().insert(header::ETAG, value);
        }
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

async fn inspector_handler(State(state): State<AppState>) -> Response {
    if !state.config.enable_inspector {
        return StatusCode::NOT_FOUND.into_response();
    }
    Html(format!(
        r#"<!doctype html><html><head><meta charset="utf-8"><title>Nano RPC Inspector</title>
<style>body{{font:16px system-ui;margin:2rem;max-width:72rem}}textarea{{width:100%;height:10rem;font:14px monospace}}button{{padding:.5rem 1rem}}pre{{background:#f5f5f5;padding:1rem;overflow:auto}}</style>
</head><body><h1>Nano RPC Inspector</h1><p>Profile: <code>{}</code></p>
<label>Request JSON</label><textarea id="request">{{"jsonrpc":"2.0","method":"version","params":{{}},"id":1}}</textarea>
<p><button id="send">Send request</button></p><pre id="response"></pre>
<script>
const output=document.getElementById("response");
document.getElementById("send").onclick=async()=>{{try{{const body=JSON.parse(document.getElementById("request").value);const r=await fetch("/rpc",{{method:"POST",headers:{{"content-type":"application/json"}},body:JSON.stringify(body)}});output.textContent=JSON.stringify(await r.json(),null,2)}}catch(e){{output.textContent=String(e)}}}};
</script></body></html>"#,
        state.config.profile
    ))
    .into_response()
}

#[derive(Debug, Deserialize)]
struct EventsQuery {
    accounts: Option<String>,
    hashes: Option<String>,
}

impl EventsQuery {
    fn filter(
        value: Option<&str>,
        label: &'static str,
    ) -> Result<Option<Vec<String>>, &'static str> {
        let Some(value) = value else {
            return Ok(None);
        };
        if value.len() > MAX_ACCOUNT_FILTER_BYTES {
            return Err(match label {
                "account" => "account filter is too large",
                _ => "hash filter is too large",
            });
        }
        let values = value
            .split(',')
            .filter(|item| !item.is_empty())
            .map(str::to_owned)
            .collect::<Vec<_>>();
        if values.len() > MAX_ACCOUNT_FILTER_ITEMS {
            return Err(match label {
                "account" => "too many accounts in filter",
                _ => "too many hashes in filter",
            });
        }
        Ok(Some(values))
    }

    fn filters(&self) -> Result<EventFilters, &'static str> {
        Ok(EventFilters {
            accounts: Self::filter(self.accounts.as_deref(), "account")?,
            hashes: Self::filter(self.hashes.as_deref(), "hash")?,
        })
    }
}

struct EventFilters {
    accounts: Option<Vec<String>>,
    hashes: Option<Vec<String>>,
}

fn event_matches_filters(
    item: &NanoEvent,
    accounts: Option<&[String]>,
    hashes: Option<&[String]>,
) -> bool {
    if item.event != "nano.confirmation" {
        return true;
    }
    let account_match = accounts.is_none_or(|values| {
        let source_matches = item
            .data
            .get("account")
            .and_then(Value::as_str)
            .is_some_and(|account| values.iter().any(|wanted| wanted == account));
        let destination_matches = item
            .data
            .get("destination")
            .and_then(Value::as_str)
            .is_some_and(|destination| values.iter().any(|wanted| wanted == destination))
            || item
                .data
                .get("block")
                .and_then(Value::as_object)
                .and_then(|block| block.get("link_as_account"))
                .and_then(Value::as_str)
                .is_some_and(|destination| values.iter().any(|wanted| wanted == destination));
        source_matches || destination_matches
    });
    let hash_match = hashes.is_none_or(|values| {
        item.data
            .get("hash")
            .and_then(Value::as_str)
            .is_some_and(|hash| values.iter().any(|wanted| wanted == hash))
    });
    account_match && hash_match
}

#[cfg(test)]
fn event_matches_accounts(item: &NanoEvent, accounts: Option<&[String]>) -> bool {
    event_matches_filters(item, accounts, None)
}

async fn sse_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    axum::extract::Query(query): axum::extract::Query<EventsQuery>,
) -> Response {
    let filters = match query.filters() {
        Ok(filters) => filters,
        Err(message) => {
            state.metrics.errors.fetch_add(1, Ordering::Relaxed);
            return (StatusCode::BAD_REQUEST, Json(json!({"error": message}))).into_response();
        }
    };
    let cursor = headers
        .get("last-event-id")
        .and_then(|value| value.to_str().ok());
    let (reset, replay) = state.events.replay(cursor).await;
    if cursor.is_some() {
        if reset {
            state.metrics.replay_misses.fetch_add(1, Ordering::Relaxed);
        } else {
            state.metrics.replay_hits.fetch_add(1, Ordering::Relaxed);
        }
    }
    state.metrics.active_streams.fetch_add(1, Ordering::Relaxed);
    if reset {
        state.metrics.replay_resets.fetch_add(1, Ordering::Relaxed);
    }
    let account_filter_count = filters.accounts.as_ref().map_or(0, Vec::len);
    let hash_filter_count = filters.hashes.as_ref().map_or(0, Vec::len);
    let log_rpc = state.config.log_rpc;
    let started = Instant::now();
    if log_rpc {
        tracing::info!(
            target: "nano_rpc_gateway::sse",
            event = "sse_subscription_opened",
            account_filter_count,
            hash_filter_count,
            last_event_id_present = cursor.is_some(),
            replay_reset = reset,
            replay_event_count = replay.len(),
            "SSE subscription opened"
        );
    }
    let mut receiver = state.events.subscribe();
    let active_streams = state.metrics.active_streams.clone();
    let replay_resets = state.metrics.replay_resets.clone();
    let output = stream! {
        let _guard = ActiveStreamGuard {
            active_streams,
            log_rpc,
            started,
            account_filter_count,
            hash_filter_count,
        };
        if reset {
            yield Ok::<Event, Infallible>(reset_sse_event("replay_unavailable", &state.config.profile));
        }
        for item in replay {
            if event_matches_filters(&item, filters.accounts.as_deref(), filters.hashes.as_deref()) {
                yield Ok::<Event, Infallible>(sse_event(item));
            }
        }
        loop {
            match receiver.recv().await {
                Ok(item) if event_matches_filters(&item, filters.accounts.as_deref(), filters.hashes.as_deref()) => {
                    yield Ok::<Event, Infallible>(sse_event(item));
                }
                Ok(_) => {}
                Err(broadcast::error::RecvError::Lagged(_)) => {
                    replay_resets.fetch_add(1, Ordering::Relaxed);
                    state.metrics.overflow_resets.fetch_add(1, Ordering::Relaxed);
                    yield Ok::<Event, Infallible>(reset_sse_event("subscriber_lagged", &state.config.profile));
                }
                Err(broadcast::error::RecvError::Closed) => break,
            }
        }
    };
    let mut response = Sse::new(output)
        .keep_alive(KeepAlive::default())
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

pub async fn run_ws_bridge(state: AppState) -> Result<(), GatewayError> {
    state.upstream_ready.store(false, Ordering::Relaxed);
    let mut connected = false;
    let mut ws_index = None;
    let mut selected_ws_url = None;
    let result = async {
        let (index, ws_url) = match state
            .native
            .active_ws_url(&state.config.node_ws_urls)
            .await
        {
            Ok(selection) => selection,
            Err(error) => {
                let index = state.native.active_index();
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
        let (mut socket, _) = match connect_async(&ws_url).await {
            Ok(connection) => connection,
            Err(error) => {
                log_upstream_error("websocket", &ws_url, &error.to_string());
                return Err(GatewayError::UpstreamUnavailable(error.to_string()));
            }
        };
        if let Err(error) = socket
            .send(Message::Text(
                json!({"action":"subscribe","topic":"confirmation"}).to_string(),
            ))
            .await
        {
            log_upstream_error("websocket", &ws_url, &error.to_string());
            return Err(GatewayError::UpstreamUnavailable(error.to_string()));
        }
        connected = true;
        let reconnecting = state.upstream_seen.swap(true, Ordering::Relaxed);
        if reconnecting {
            state
                .metrics
                .upstream_reconnects
                .fetch_add(1, Ordering::Relaxed);
        }
        state.upstream_ready.store(true, Ordering::Relaxed);
        state
            .events
            .publish(
                "nano.stream_reset",
                json!({
                    "reason": if reconnecting { "upstream_reconnected" } else { "upstream_connected" },
                    "profile": state.config.profile,
                    "reconcile": "Query account_info for affected accounts before applying new confirmations"
                }),
            )
            .await;
        while let Some(message) = socket.next().await {
            let message = match message {
                Ok(message) => message,
                Err(error) => {
                    log_upstream_error("websocket", &ws_url, &error.to_string());
                    return Err(GatewayError::UpstreamTransport(error.to_string()));
                }
            };
            let value = match message {
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
                if let Some(event) = normalize_confirmation(&value, &state.config.profile) {
                    state.events.publish("nano.confirmation", event).await;
                }
            }
        }
        Ok(())
    }
    .await;
    state.upstream_ready.store(false, Ordering::Relaxed);
    if let Some(index) = ws_index.filter(|_| result.is_err()) {
        state.native.mark_ws_unhealthy(index).await;
    }
    if connected {
        if result.is_ok() {
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
        state
            .events
            .publish(
                "nano.stream_reset",
                json!({
                    "reason": if result.is_err() { "upstream_disconnected" } else { "upstream_closed" },
                    "profile": state.config.profile,
                    "reconcile": "Query account_info for affected accounts before applying new confirmations"
                }),
            )
            .await;
    }
    result
}

fn normalize_confirmation(value: &Value, profile: &str) -> Option<Value> {
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
        if !fields.contains_key("destination") {
            let destination = fields
                .get("block")
                .and_then(Value::as_object)
                .and_then(|block| block.get("link_as_account"))
                .and_then(Value::as_str)
                .map(str::to_owned);
            if let Some(destination) = destination {
                fields.insert("destination".into(), Value::String(destination));
            }
        }
        if !fields.contains_key("subtype") {
            let subtype = fields
                .get("block")
                .and_then(Value::as_object)
                .and_then(|block| block.get("subtype"))
                .cloned();
            if let Some(subtype) = subtype {
                fields.insert("subtype".into(), subtype);
            }
        }
        fields.insert("profile".into(), Value::String(profile.into()));
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
        assert_eq!(document["methods"].as_array().expect("methods").len(), 10);
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
    fn asyncapi_reset_schema_lists_every_runtime_reason() {
        let document = asyncapi_document("nano-node/V28.2", "https://gateway.invalid/rpc");
        assert_eq!(
            document["components"]["schemas"]["StreamResetParams"]["properties"]["reason"]["enum"],
            json!([
                "upstream_connected",
                "upstream_reconnected",
                "upstream_disconnected",
                "upstream_closed",
                "replay_unavailable",
                "subscriber_lagged"
            ])
        );
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
    fn account_info_normalizes_unopened_accounts_and_process_is_browser_capable() {
        let account = normalize_result(
            "account_info",
            &json!({"frontier": null, "balance": "0"}),
            &json!({"account": "nano_unopened"}),
        );
        assert_eq!(account["opened"], false);
        assert!(account.get("confirmed_balance").is_none());
        assert!(account.get("confirmed_frontier").is_none());
        let document =
            openrpc_document("nano-node/V28.2", false, true, "http://127.0.0.1:8090/rpc");
        let process = document["methods"]
            .as_array()
            .expect("methods")
            .iter()
            .find(|method| method["name"] == "process")
            .expect("process method");
        assert_eq!(process["x-nano-capability"]["browser_safe"], true);
    }

    #[test]
    fn registry_and_openrpc_method_inventories_match() {
        let document =
            openrpc_document("nano-node/V28.2", true, false, "http://127.0.0.1:7076/rpc");
        let advertised = document["methods"]
            .as_array()
            .expect("methods")
            .iter()
            .filter_map(|method| method["name"].as_str())
            .collect::<Vec<_>>();
        for spec in registry() {
            assert!(spec.schema_provenance.starts_with("profiles/"));
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
    fn account_info_does_not_forward_normalized_receivable_parameter() {
        let request = native_request_body(
            "account_info",
            &json!({"account": "nano_opened", "include_confirmed": true, "receivable": true}),
        );
        assert_eq!(request["action"], "account_info");
        assert_eq!(request["include_confirmed"], "true");
        assert!(!request.contains_key("receivable"));
    }

    #[test]
    fn receivable_requests_native_source_metadata() {
        let request = native_request_body("receivable", &json!({"account": "nano_opened"}));
        assert_eq!(request["action"], "pending");
        assert_eq!(request["source"], "true");
    }

    #[test]
    fn result_shapes_are_checked_against_the_profile() {
        assert!(validate_result("process", &json!({"hash": "A"})));
        assert!(!validate_result("process", &json!({"hash": 7})));
        assert!(!validate_result("account_info", &json!({"frontier": "A"})));
    }

    #[test]
    fn receivable_empty_blocks_string_normalizes_to_empty_array() {
        let normalized = normalize_result(
            "receivable",
            &json!({"blocks": ""}),
            &json!({"account": "nano_unopened"}),
        );
        assert_eq!(normalized, json!([]));
        assert!(validate_result("receivable", &normalized));
    }

    #[test]
    fn account_info_confirmed_height_normalizes_to_profile_name() {
        let normalized = normalize_result(
            "account_info",
            &json!({
                "frontier": "A",
                "balance": "1",
                "confirmed_frontier": "A",
                "confirmed_balance": "1",
                "confirmed_height": "7"
            }),
            &json!({"account": "nano_opened"}),
        );
        assert_eq!(normalized["confirmation_height"], "7");
        assert!(validate_result("account_info", &normalized));
    }

    #[test]
    fn account_info_does_not_invent_confirmed_state() {
        let normalized = normalize_result(
            "account_info",
            &json!({"frontier": "unconfirmed", "balance": "9"}),
            &json!({"account": "nano_opened"}),
        );
        assert!(normalized.get("confirmed_balance").is_none());
        assert!(normalized.get("confirmed_frontier").is_none());
        assert!(!validate_result("account_info", &normalized));
    }

    #[test]
    fn receivable_blocks_array_normalizes_to_profile_entries() {
        let normalized = normalize_result(
            "receivable",
            &json!({
                "blocks": [{
                    "hash": "A",
                    "amount": "2",
                    "source": "nano_source"
                }]
            }),
            &json!({"account": "nano_opened"}),
        );
        assert_eq!(
            normalized,
            json!([{"hash": "A", "amount": "2", "source": "nano_source"}])
        );
        assert!(validate_result("receivable", &normalized));
    }

    #[test]
    fn blocks_info_preserves_object_contents_as_the_canonical_block() {
        let normalized = normalize_result(
            "blocks_info",
            &json!({
                "blocks": {
                    "A": {
                        "block_account": "nano_sender",
                        "amount": "1",
                        "balance": "9",
                        "height": "2",
                        "confirmed": "true",
                        "subtype": "send",
                        "contents": {
                            "type": "state",
                            "account": "nano_sender",
                            "link": "nano_destination"
                        }
                    }
                }
            }),
            &json!({"hashes": ["A"]}),
        );
        assert_eq!(normalized["blocks"]["A"]["hash"], "A");
        assert_eq!(
            normalized["blocks"]["A"]["block"]["link"],
            "nano_destination"
        );
        assert!(validate_result("blocks_info", &normalized));
    }

    #[test]
    fn receivable_blocks_map_supplies_key_as_missing_hash() {
        let normalized = normalize_result(
            "receivable",
            &json!({
                "blocks": {
                    "A": {
                        "amount": "2",
                        "source": "nano_source"
                    }
                }
            }),
            &json!({"account": "nano_opened"}),
        );
        assert_eq!(
            normalized,
            json!([{"hash": "A", "amount": "2", "source": "nano_source"}])
        );
        assert!(validate_result("receivable", &normalized));
    }

    #[test]
    fn collection_parameters_follow_profile_limits() {
        assert!(validate_params(
            "account_history",
            &json!({"account": "nano_test", "count": 0})
        )
        .is_err());
        assert!(validate_params("blocks_info", &json!({"hashes": []})).is_err());
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
        assert_eq!(disconnect["expected_gateway_event"], "nano.stream_reset");
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
        assert_eq!(confirmation["profile"], "nano-node/V28.2");
        assert!(confirmation["hash"].as_str().is_some());
        let disconnect: Value = serde_json::from_str(include_str!(
            "../fixtures/native/v28.2/confirmation.disconnect.live.json"
        ))
        .expect("disconnect response");
        assert_eq!(disconnect["data"]["reason"], "upstream_disconnect");
    }
    #[test]
    fn confirmation_normalization_filters_acks_and_adds_profile() {
        assert!(normalize_confirmation(&json!({"ack":"subscribe"}), "nano-node/V28.2").is_none());
        let event = normalize_confirmation(
            &json!({"topic":"confirmation","message":{"hash":"A"}}),
            "nano-node/V28.2",
        )
        .expect("confirmation event");
        assert_eq!(event["hash"], "A");
        assert_eq!(event["profile"], "nano-node/V28.2");
    }
    #[test]
    fn confirmation_filter_matches_only_requested_accounts() {
        let item = NanoEvent {
            id: "1".into(),
            event: "nano.confirmation".into(),
            data: json!({"account":"nano_a"}),
        };
        assert!(event_matches_accounts(&item, Some(&["nano_a".into()])));
        assert!(!event_matches_accounts(&item, Some(&["nano_b".into()])));
        assert!(event_matches_accounts(&item, None));
    }

    #[test]
    fn confirmation_filter_matches_destination_accounts() {
        let item = NanoEvent {
            id: "1".into(),
            event: "nano.confirmation".into(),
            data: json!({"account":"nano_sender", "destination":"nano_player"}),
        };
        assert!(event_matches_accounts(&item, Some(&["nano_player".into()])));
        assert!(!event_matches_accounts(&item, Some(&["nano_other".into()])));

        let native_item = NanoEvent {
            id: "2".into(),
            event: "nano.confirmation".into(),
            data: json!({"account":"nano_sender", "block":{"link_as_account":"nano_player"}}),
        };
        assert!(event_matches_accounts(
            &native_item,
            Some(&["nano_player".into()])
        ));
    }

    #[test]
    fn confirmation_filter_supports_hashes_and_keeps_control_events_visible() {
        let item = NanoEvent {
            id: "1".into(),
            event: "nano.confirmation".into(),
            data: json!({"account":"nano_a","hash":"ABC"}),
        };
        assert!(event_matches_filters(&item, None, Some(&["ABC".into()])));
        assert!(!event_matches_filters(&item, None, Some(&["DEF".into()])));
        let reset = NanoEvent {
            id: "2".into(),
            event: "nano.stream_reset".into(),
            data: json!({}),
        };
        assert!(event_matches_filters(&reset, None, Some(&["DEF".into()])));
    }
    #[tokio::test]
    async fn event_hub_reports_reset_after_bounded_history_is_lost() {
        let hub = EventHub::new(2);
        hub.publish("nano.confirmation", json!({"hash":"a"})).await;
        hub.publish("nano.confirmation", json!({"hash":"b"})).await;
        hub.publish("nano.confirmation", json!({"hash":"c"})).await;
        let (reset, events) = hub.replay(Some("old-generation:0")).await;
        assert!(reset);
        assert_eq!(events.len(), 2);
    }

    #[tokio::test]
    async fn event_hub_replays_after_same_generation_cursor() {
        let hub = EventHub::new(4);
        let first = hub.publish("nano.confirmation", json!({"hash":"a"})).await;
        hub.publish("nano.confirmation", json!({"hash":"b"})).await;
        let (reset, events) = hub.replay(Some(&first.id)).await;
        assert!(!reset);
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].data["hash"], "b");
    }

    #[tokio::test]
    async fn event_hub_deduplicates_recent_confirmation_hashes() {
        let hub = EventHub::new(4);
        let first = hub
            .publish(
                "nano.confirmation",
                json!({"hash":"A","account":"nano_test"}),
            )
            .await;
        let duplicate = hub
            .publish(
                "nano.confirmation",
                json!({"hash":"A","account":"nano_test"}),
            )
            .await;
        assert_eq!(first.id, duplicate.id);
        let (_, events) = hub.replay(None).await;
        assert_eq!(events.len(), 1);
        assert!(events[0].id.contains(':'));
    }

    #[tokio::test]
    async fn event_hub_exposes_broadcast_lag_and_new_generation() {
        let first = EventHub::new(1);
        let mut receiver = first.subscribe();
        let first_event = first
            .publish("nano.confirmation", json!({"hash": "a"}))
            .await;
        first
            .publish("nano.confirmation", json!({"hash": "b"}))
            .await;
        assert!(matches!(
            receiver.recv().await,
            Err(broadcast::error::RecvError::Lagged(_))
        ));

        let second = EventHub::new(1);
        let second_event = second
            .publish("nano.confirmation", json!({"hash": "a"}))
            .await;
        assert_ne!(
            first_event.id.split_once(':').expect("first cursor").0,
            second_event.id.split_once(':').expect("second cursor").0
        );
    }

    #[test]
    fn stream_control_events_bypass_account_filters() {
        let item = NanoEvent {
            id: "1".into(),
            event: "nano.stream_reset".into(),
            data: json!({"reason":"upstream_disconnect"}),
        };
        assert!(event_matches_accounts(&item, Some(&["nano_other".into()])));
    }

    #[tokio::test]
    async fn websocket_connection_failure_does_not_emit_spurious_reset() {
        let config = Config {
            node_ws_urls: vec!["ws://127.0.0.1:1".into()],
            ..Config::default()
        };
        let state = AppState::new(config).expect("state");
        assert!(run_ws_bridge(state.clone()).await.is_err());
        let (_, events) = state.events.replay(None).await;
        assert!(events.is_empty());
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
        assert!(run_ws_bridge(state.clone()).await.is_ok());
        server.await.expect("server task");
        let (_, events) = state.events.replay(None).await;
        assert_eq!(events.len(), 3);
        assert_eq!(events[0].event, "nano.stream_reset");
        assert_eq!(events[0].data["reason"], "upstream_connected");
        assert_eq!(events[1].event, "nano.confirmation");
        assert_eq!(events[1].data["profile"], "nano-node/V28.2");
        assert_eq!(events[2].event, "nano.stream_reset");
        assert_eq!(events[2].data["reason"], "upstream_closed");
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
            node_ws_urls: vec![format!("ws://{address}")],
            ..Config::default()
        };
        let state = AppState::new(config).expect("state");
        assert!(run_ws_bridge(state.clone()).await.is_ok());
        assert!(run_ws_bridge(state.clone()).await.is_ok());
        server.await.expect("server task");
        assert_eq!(state.metrics.upstream_reconnects.load(Ordering::Relaxed), 1);
        let (_, events) = state.events.replay(None).await;
        assert_eq!(events.len(), 4);
        assert_eq!(events[0].data["reason"], "upstream_connected");
        assert_eq!(events[1].data["reason"], "upstream_closed");
        assert_eq!(events[2].data["reason"], "upstream_reconnected");
        assert_eq!(events[3].data["reason"], "upstream_closed");
    }
}
