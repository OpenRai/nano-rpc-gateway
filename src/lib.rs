//! Nano RPC Gateway: a deliberately small, typed boundary around native Nano RPC.

use std::{
    collections::VecDeque,
    convert::Infallible,
    path::Path,
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Arc,
    },
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use async_stream::stream;
use axum::{
    body::Bytes,
    extract::State,
    http::{header, HeaderMap, StatusCode},
    response::{
        sse::{Event, KeepAlive, Sse},
        IntoResponse, Response,
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
use thiserror::Error;
use tokio::sync::{broadcast, Mutex};
use tokio_tungstenite::{connect_async, tungstenite::Message};
use tower::limit::ConcurrencyLimitLayer;
use tower_http::{cors::CorsLayer, limit::RequestBodyLimitLayer, trace::TraceLayer};
use url::Url;

const DEFAULT_CONFIG: &str = r#"listen: "127.0.0.1:8090"
node_rpc_url: "http://127.0.0.1:7076"
node_ws_url: "ws://127.0.0.1:7078"
profile: "nano-node/V28.2"
allow_work: false
allow_control: false
auth_public_key: null
enable_discovery: true
"#;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Config {
    #[serde(default = "default_listen")]
    pub listen: String,
    #[serde(default = "default_rpc")]
    pub node_rpc_url: String,
    #[serde(default = "default_ws")]
    pub node_ws_url: String,
    #[serde(default = "default_profile")]
    pub profile: String,
    #[serde(default)]
    pub allow_work: bool,
    #[serde(default)]
    pub allow_control: bool,
    pub auth_public_key: Option<String>,
    #[serde(default = "default_discovery")]
    pub enable_discovery: bool,
    pub tls_cert: Option<String>,
    pub tls_key: Option<String>,
}

fn default_listen() -> String {
    "127.0.0.1:8090".into()
}
fn default_rpc() -> String {
    "http://127.0.0.1:7076".into()
}
fn default_ws() -> String {
    "ws://127.0.0.1:7078".into()
}
fn default_profile() -> String {
    "nano-node/V28.2".into()
}
fn default_discovery() -> bool {
    true
}

impl Default for Config {
    fn default() -> Self {
        serde_yaml::from_str(DEFAULT_CONFIG).expect("default config is valid")
    }
}

impl Config {
    pub fn load(path: &Path) -> Result<Self, GatewayError> {
        match std::fs::read_to_string(path) {
            Ok(contents) => serde_yaml::from_str::<Self>(&contents)
                .map_err(GatewayError::Config)
                .and_then(|config| config.validate()),
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
        if self.tls_cert.is_some() != self.tls_key.is_some() {
            return Err(GatewayError::InvalidRequest(
                "tls_cert and tls_key must be configured together".into(),
            ));
        }
        Ok(self)
    }
}

#[derive(Debug, Error)]
pub enum GatewayError {
    #[error("configuration: {0}")]
    Config(serde_yaml::Error),
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
    #[error("upstream: {0}")]
    Upstream(String),
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
        Self {
            jsonrpc: "2.0".into(),
            result: None,
            error: Some(RpcError {
                code,
                message: message.into(),
                data: None,
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
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Scope {
    Base,
    Work,
    Control,
}

pub fn registry() -> Vec<MethodSpec> {
    vec![
        MethodSpec {
            name: "account_info",
            scope: Scope::Base,
            params: "AccountInfoParams",
            result: "AccountInfoResult",
            description: "Read account frontier and representative state.",
        },
        MethodSpec {
            name: "account_balance",
            scope: Scope::Base,
            params: "AccountParams",
            result: "AccountBalanceResult",
            description: "Read an account balance.",
        },
        MethodSpec {
            name: "account_history",
            scope: Scope::Base,
            params: "AccountHistoryParams",
            result: "AccountHistoryResult",
            description: "Read account history.",
        },
        MethodSpec {
            name: "block_info",
            scope: Scope::Base,
            params: "BlockParams",
            result: "BlockInfoResult",
            description: "Read block metadata.",
        },
        MethodSpec {
            name: "blocks_info",
            scope: Scope::Base,
            params: "BlocksParams",
            result: "BlocksInfoResult",
            description: "Read metadata for several blocks.",
        },
        MethodSpec {
            name: "process",
            scope: Scope::Base,
            params: "ProcessParams",
            result: "ProcessResult",
            description: "Submit a precomputed block to the node.",
        },
        MethodSpec {
            name: "work_generate",
            scope: Scope::Work,
            params: "WorkGenerateParams",
            result: "WorkGenerateResult",
            description: "Delegate proof-of-work generation to the node.",
        },
    ]
}

pub fn openrpc_document(
    profile: &str,
    include_work: bool,
    include_discovery: bool,
    gateway_url: &str,
) -> Value {
    let mut methods = registry()
        .into_iter()
        .filter(|method| include_work || method.scope == Scope::Base)
        .map(openrpc_method)
        .collect::<Vec<_>>();
    if include_discovery {
        methods.push(json!({
            "name": "rpc.discover",
            "summary": "Return this OpenRPC document.",
            "params": [],
            "result": {"name": "result", "schema": {"type": "object"}}
        }));
    }
    json!({
        "openrpc": "1.3.2", "info": {"title":"Nano Gateway", "version":"0.1.0", "description":"JSON-RPC 2.0 adapter for a pinned Nano node profile."},
        "servers": [{"name":"gateway", "url":gateway_url, "variables":{"gatewayUrl":{"default":gateway_url}}}],
        "methods": methods,
        "components": {"schemas": {
            "AccountParams":{"type":"object","required":["account"],"properties":{"account":{"type":"string","minLength":1}}},
            "AccountInfoParams":{"$ref":"#/components/schemas/AccountParams"},
            "AccountBalanceResult":{"type":"object","required":["balance","pending"],"properties":{"balance":{"type":"string"},"pending":{"type":"string"}}},
            "AccountHistoryResult":{"type":"object","required":["account","history"],"properties":{"account":{"type":"string"},"history":{"type":"array"}}},
            "AccountInfoResult":{"type":"object","required":["frontier","open_block","representative_block","balance","modified_timestamp","block_count","account_version","confirmation_height","confirmation_height_frontier"],"properties":{"frontier":{"type":"string"},"open_block":{"type":"string"},"representative_block":{"type":"string"},"balance":{"type":"string"},"modified_timestamp":{"type":"string"},"block_count":{"type":"string"},"account_version":{"type":"string"},"confirmation_height":{"type":"string"},"confirmation_height_frontier":{"type":"string"}}},
            "AccountHistoryParams":{"type":"object","required":["account"],"properties":{"account":{"type":"string","minLength":1},"count":{"type":"integer","minimum":1}}},
            "BlockParams":{"type":"object","required":["hash"],"properties":{"hash":{"type":"string","minLength":1}}},
            "BlockInfoResult":{"type":"object","required":["block_account","amount","balance","height","contents"],"properties":{"block_account":{"type":"string"},"amount":{"type":"string"},"balance":{"type":"string"},"height":{"type":"string"},"contents":{"type":"string"}}},
            "BlocksParams":{"type":"object","required":["hashes"],"properties":{"hashes":{"type":"array","minItems":1,"items":{"type":"string","minLength":1}}}},
            "BlocksInfoResult":{"type":"object","required":["blocks"],"properties":{"blocks":{"type":"object"}}},
            "ProcessParams":{"type":"object","required":["block"],"properties":{"block":{"type":"object"}}}, "ProcessResult":{"type":"object","required":["hash"],"properties":{"hash":{"type":"string"}}},
            "WorkGenerateParams":{"type":"object","required":["hash"],"properties":{"hash":{"type":"string","minLength":1}}}, "WorkGenerateResult":{"type":"object","required":["hash","work","difficulty","multiplier"],"properties":{"hash":{"type":"string"},"work":{"type":"string"},"difficulty":{"type":"string"},"multiplier":{"type":"string"}}}
        }, "errors": {
            "InvalidRequest": {"code": -32602, "message": "Invalid method parameters"},
            "MethodNotFound": {"code": -32601, "message": "Method not found"},
            "Unauthorized": {"code": -32001, "message": "Unauthorized"},
            "UpstreamFailure": {"code": -32000, "message": "Upstream request failed"}
        }}, "x-nano-profile": profile
    })
}

fn openrpc_method(method: MethodSpec) -> Value {
    let params = match method.name {
        "account_info" | "account_balance" => vec![json!({
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
        "work_generate" => vec![json!({
            "name": "hash",
            "required": true,
            "schema": {"type": "string"}
        })],
        _ => Vec::new(),
    };
    json!({
        "name": method.name,
        "summary": method.description,
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
            {"$ref":"#/components/errors/UpstreamFailure"}
        ]
    })
}

#[derive(Clone)]
pub struct NativeClient {
    client: Client,
    endpoint: String,
}

impl NativeClient {
    pub fn new(endpoint: impl Into<String>) -> Result<Self, GatewayError> {
        let endpoint = endpoint.into();
        validate_upstream_url(&endpoint, &["http", "https"])?;
        Ok(Self {
            client: Client::builder()
                .timeout(Duration::from_secs(10))
                .redirect(reqwest::redirect::Policy::none())
                .build()
                .map_err(|e| GatewayError::Upstream(e.to_string()))?,
            endpoint,
        })
    }
    pub async fn call(&self, action: &str, params: &Value) -> Result<Value, GatewayError> {
        let mut body = serde_json::Map::new();
        body.insert("action".into(), Value::String(action.into()));
        if let Value::Object(values) = params {
            body.extend(values.clone());
        }
        let response = self
            .client
            .post(&self.endpoint)
            .json(&body)
            .send()
            .await
            .map_err(|e| GatewayError::Upstream(e.to_string()))?;
        let status = response.status();
        let value: Value = response
            .json()
            .await
            .map_err(|e| GatewayError::Upstream(e.to_string()))?;
        if !status.is_success() {
            return Err(GatewayError::Upstream(format!("HTTP {status}")));
        }
        if value.get("error").is_some() {
            return Err(GatewayError::Upstream(value.to_string()));
        }
        Ok(value)
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
    if !parsed.username().is_empty() || parsed.password().is_some() {
        return Err(GatewayError::InvalidRequest(
            "upstream URL must not contain credentials".into(),
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
    pub native: NativeClient,
    pub events: EventHub,
    pub metrics: Metrics,
    pub verifying_key: Option<VerifyingKey>,
    pub upstream_ready: Arc<AtomicBool>,
}

#[derive(Clone, Default)]
pub struct Metrics {
    pub requests: Arc<AtomicU64>,
    pub errors: Arc<AtomicU64>,
    pub active_streams: Arc<AtomicU64>,
    pub replay_resets: Arc<AtomicU64>,
}

struct ActiveStreamGuard(Arc<AtomicU64>);

impl Drop for ActiveStreamGuard {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::Relaxed);
    }
}

fn sse_event(item: NanoEvent) -> Event {
    let data = serde_json::to_string(&item.data).unwrap_or_else(|_| "null".into());
    Event::default().id(item.id).event(item.event).data(data)
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
            native: NativeClient::new(&config.node_rpc_url)?,
            events: EventHub::new(256),
            metrics: Metrics::default(),
            config,
            verifying_key,
            upstream_ready: Arc::new(AtomicBool::new(false)),
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
                    openrpc_document(
                        &self.config.profile,
                        self.config.allow_work,
                        self.config.enable_discovery,
                        &self.gateway_url(),
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
        if spec.scope != Scope::Base && !self.authorized(headers, spec.scope) {
            return RpcResponse::err(request.id, -32001, "Unauthorized");
        }
        let params = request.params.unwrap_or_else(|| json!({}));
        if let Err(message) = validate_params(spec.name, &params) {
            return RpcResponse::err(request.id, -32602, message);
        }
        match self.native.call(spec.name, &params).await {
            Ok(result) if validate_result(spec.name, &result) => {
                RpcResponse::ok(request.id, result)
            }
            Ok(_) => RpcResponse::err(
                request.id,
                -32000,
                "Upstream result does not match the selected profile schema",
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
        "account_info" | "account_balance" | "account_history" => "account",
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
    Ok(())
}

fn has_string(object: &serde_json::Map<String, Value>, name: &str) -> bool {
    object
        .get(name)
        .and_then(Value::as_str)
        .is_some_and(|value| !value.is_empty())
}

fn validate_result(method: &str, result: &Value) -> bool {
    let Some(object) = result.as_object() else {
        return false;
    };
    match method {
        "account_info" => [
            "frontier",
            "open_block",
            "representative_block",
            "balance",
            "modified_timestamp",
            "block_count",
            "account_version",
            "confirmation_height",
            "confirmation_height_frontier",
        ]
        .iter()
        .all(|field| has_string(object, field)),
        "account_balance" => ["balance", "pending"]
            .iter()
            .all(|field| has_string(object, field)),
        "account_history" => {
            has_string(object, "account") && object.get("history").is_some_and(Value::is_array)
        }
        "block_info" => ["block_account", "amount", "balance", "height", "contents"]
            .iter()
            .all(|field| has_string(object, field)),
        "blocks_info" => object.get("blocks").is_some_and(Value::is_object),
        "process" => has_string(object, "hash"),
        "work_generate" => ["hash", "work", "difficulty", "multiplier"]
            .iter()
            .all(|field| has_string(object, field)),
        _ => false,
    }
}

pub fn app(state: AppState) -> Router {
    Router::new()
        .route("/rpc", post(rpc_handler))
        .route("/openrpc.json", get(openrpc_handler))
        .route("/events/confirmations", get(sse_handler))
        .route("/health", get(|| async { Json(json!({"status":"ok"})) }))
        .route("/readyz", get(ready_handler))
        .route("/metrics", get(metrics_handler))
        .layer(CorsLayer::permissive())
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
nano_gateway_active_streams {}\nnano_gateway_replay_resets_total {}\n",
            state.metrics.requests.load(Ordering::Relaxed),
            state.metrics.errors.load(Ordering::Relaxed),
            state.metrics.active_streams.load(Ordering::Relaxed),
            state.metrics.replay_resets.load(Ordering::Relaxed),
        ),
    )
}

async fn rpc_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    body: Bytes,
) -> Json<RpcResponse> {
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
    let response = state.dispatch(request, &headers).await;
    if response.error.is_some() {
        state.metrics.errors.fetch_add(1, Ordering::Relaxed);
    }
    Json(response)
}
async fn openrpc_handler(State(state): State<AppState>) -> Json<Value> {
    Json(openrpc_document(
        &state.config.profile,
        state.config.allow_work,
        state.config.enable_discovery,
        &state.gateway_url(),
    ))
}

#[derive(Debug, Deserialize)]
struct EventsQuery {
    accounts: Option<String>,
}

impl EventsQuery {
    fn account_filter(&self) -> Option<Vec<String>> {
        self.accounts.as_deref().map(|accounts| {
            accounts
                .split(',')
                .filter(|account| !account.is_empty())
                .map(str::to_owned)
                .collect()
        })
    }
}

fn event_matches_accounts(item: &NanoEvent, accounts: Option<&[String]>) -> bool {
    if item.event != "nano.confirmation" {
        return true;
    }
    let Some(accounts) = accounts else {
        return true;
    };
    item.data
        .get("account")
        .and_then(Value::as_str)
        .is_some_and(|account| accounts.iter().any(|wanted| wanted == account))
}

async fn sse_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    axum::extract::Query(query): axum::extract::Query<EventsQuery>,
) -> Response {
    let accounts = query.account_filter();
    let cursor = headers
        .get("last-event-id")
        .and_then(|value| value.to_str().ok());
    let (reset, replay) = state.events.replay(cursor).await;
    state.metrics.active_streams.fetch_add(1, Ordering::Relaxed);
    if reset {
        state.metrics.replay_resets.fetch_add(1, Ordering::Relaxed);
    }
    let mut receiver = state.events.subscribe();
    let active_streams = state.metrics.active_streams.clone();
    let replay_resets = state.metrics.replay_resets.clone();
    let output = stream! {
        let _guard = ActiveStreamGuard(active_streams);
        if reset {
            yield Ok::<Event, Infallible>(Event::default()
                .event("nano.stream_reset")
                .data("reconcile with JSON-RPC before applying new events"));
        }
        for item in replay {
            if event_matches_accounts(&item, accounts.as_deref()) {
                yield Ok::<Event, Infallible>(sse_event(item));
            }
        }
        loop {
            match receiver.recv().await {
                Ok(item) if event_matches_accounts(&item, accounts.as_deref()) => {
                    yield Ok::<Event, Infallible>(sse_event(item));
                }
                Ok(_) => {}
                Err(broadcast::error::RecvError::Lagged(_)) => {
                    replay_resets.fetch_add(1, Ordering::Relaxed);
                    yield Ok::<Event, Infallible>(Event::default()
                        .event("nano.stream_reset")
                        .data("reconnect and reconcile with JSON-RPC"));
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
    let result = async {
        validate_upstream_url(&state.config.node_ws_url, &["ws", "wss"])?;
        let (mut socket, _) = connect_async(&state.config.node_ws_url)
            .await
            .map_err(|e| GatewayError::Upstream(e.to_string()))?;
        socket
            .send(Message::Text(
                json!({"action":"subscribe","topic":"confirmation"}).to_string(),
            ))
            .await
            .map_err(|e| GatewayError::Upstream(e.to_string()))?;
        connected = true;
        state.upstream_ready.store(true, Ordering::Relaxed);
        while let Some(message) = socket.next().await {
            if let Message::Text(text) =
                message.map_err(|e| GatewayError::Upstream(e.to_string()))?
            {
                if let Ok(value) = serde_json::from_str::<Value>(&text) {
                    if let Some(event) = normalize_confirmation(&value, &state.config.profile) {
                        state.events.publish("nano.confirmation", event).await;
                    }
                }
            }
        }
        Ok(())
    }
    .await;
    state.upstream_ready.store(false, Ordering::Relaxed);
    if connected {
        state
            .events
            .publish(
                "nano.stream_reset",
                json!({
                    "reason": if result.is_err() { "upstream_disconnect" } else { "upstream_closed" },
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
    let wanted = match scope {
        Scope::Work => "work",
        Scope::Control => "control",
        Scope::Base => "base",
    };
    if !claims
        .get("scope")
        .and_then(Value::as_str)
        .is_some_and(|value| value == wanted)
    {
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
        assert_eq!(document["methods"].as_array().expect("methods").len(), 7);
    }
    #[test]
    fn openrpc_methods_have_callable_shapes() {
        let document =
            openrpc_document("nano-node/V28.2", false, true, "http://127.0.0.1:7076/rpc");
        for method in document["methods"].as_array().expect("methods") {
            assert!(method["name"].is_string());
            assert!(method["params"].is_array());
            assert!(method["result"]["schema"].is_object());
        }
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
    fn upstream_urls_require_expected_scheme_and_no_credentials() {
        assert!(NativeClient::new("ftp://127.0.0.1:7076").is_err());
        assert!(NativeClient::new("http://user:pass@127.0.0.1:7076").is_err());
        assert!(NativeClient::new("http://127.0.0.1:7076").is_ok());
        assert!(validate_upstream_url("ws://127.0.0.1:7078", &["ws", "wss"]).is_ok());
        assert!(validate_upstream_url("http://127.0.0.1:7078", &["ws", "wss"]).is_err());
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
    fn result_shapes_are_checked_against_the_profile() {
        assert!(validate_result("process", &json!({"hash": "A"})));
        assert!(!validate_result("process", &json!({"hash": 7})));
        assert!(!validate_result("account_info", &json!({"frontier": "A"})));
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
    fn default_config_keeps_elevated_operations_disabled() {
        let config = Config::default();
        assert!(!config.allow_work && !config.allow_control);
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
            node_ws_url: "ws://127.0.0.1:1".into(),
            ..Config::default()
        };
        let state = AppState::new(config).expect("state");
        assert!(run_ws_bridge(state.clone()).await.is_err());
        let (_, events) = state.events.replay(None).await;
        assert!(events.is_empty());
    }

    #[tokio::test]
    async fn websocket_subscription_normalizes_confirmation_and_close() {
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
                .send(Message::Text(
                    json!({"topic":"confirmation","message":{"hash":"A","account":"nano_test"}})
                        .to_string(),
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
            node_ws_url: format!("ws://{address}"),
            ..Config::default()
        };
        let state = AppState::new(config).expect("state");
        assert!(run_ws_bridge(state.clone()).await.is_ok());
        server.await.expect("server task");
        let (_, events) = state.events.replay(None).await;
        assert_eq!(events.len(), 2);
        assert_eq!(events[0].event, "nano.confirmation");
        assert_eq!(events[0].data["profile"], "nano-node/V28.2");
        assert_eq!(events[1].event, "nano.stream_reset");
        assert_eq!(events[1].data["reason"], "upstream_closed");
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
            node_ws_url: format!("ws://{address}"),
            ..Config::default()
        };
        let state = AppState::new(config).expect("state");
        assert!(run_ws_bridge(state.clone()).await.is_ok());
        assert!(run_ws_bridge(state.clone()).await.is_ok());
        server.await.expect("server task");
        let (_, events) = state.events.replay(None).await;
        assert_eq!(events.len(), 2);
        assert!(events
            .iter()
            .all(|event| event.event == "nano.stream_reset"));
    }
}
