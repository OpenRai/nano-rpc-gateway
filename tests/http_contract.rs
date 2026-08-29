use std::sync::atomic::Ordering;

use axum::{routing::post, Json, Router};
use base64::Engine;
use http_body_util::BodyExt;
use nano_rpc_gateway::{app, generate_signing_key, sign_paseto, AppState, Config};
use serde_json::{json, Value};
use tokio::net::TcpListener;
use tower::ServiceExt;

async fn start_native_stub() -> String {
    let router = Router::new().route(
        "/",
        post(|| async { Json(json!({"frontier":"A", "balance":"0"})) }),
    );
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind stub");
    let address = listener.local_addr().expect("stub address");
    tokio::spawn(async move { axum::serve(listener, router).await.expect("stub server") });
    format!("http://{address}")
}

fn test_config(node_rpc_url: String) -> Config {
    Config {
        listen: "127.0.0.1:0".into(),
        node_rpc_url,
        node_ws_url: "ws://127.0.0.1:1".into(),
        profile: "nano-node/test".into(),
        allow_work: false,
        allow_control: false,
        auth_public_key: None,
        enable_discovery: true,
        tls_cert: None,
        tls_key: None,
    }
}

async fn rpc(state: AppState, body: &str) -> Value {
    let request = axum::http::Request::builder()
        .method("POST")
        .uri("/rpc")
        .header("content-type", "application/json")
        .body(axum::body::Body::from(body.to_owned()))
        .expect("request");
    let response = app(state).oneshot(request).await.expect("gateway response");
    let bytes = response
        .into_body()
        .collect()
        .await
        .expect("response body")
        .to_bytes();
    serde_json::from_slice(&bytes).expect("JSON-RPC response")
}

async fn rpc_with_auth(state: AppState, body: &str, token: &str) -> Value {
    let request = axum::http::Request::builder()
        .method("POST")
        .uri("/rpc")
        .header("content-type", "application/json")
        .header("authorization", format!("Bearer {token}"))
        .body(axum::body::Body::from(body.to_owned()))
        .expect("request");
    let response = app(state).oneshot(request).await.expect("gateway response");
    let bytes = response
        .into_body()
        .collect()
        .await
        .expect("response body")
        .to_bytes();
    serde_json::from_slice(&bytes).expect("JSON-RPC response")
}

#[tokio::test]
async fn account_info_translates_native_response() {
    let state = AppState::new(test_config(start_native_stub().await)).expect("state");
    let response = rpc(
        state,
        r#"{"jsonrpc":"2.0","method":"account_info","params":{"account":"nano_test"},"id":1}"#,
    )
    .await;
    assert_eq!(response["result"]["frontier"], "A");
}

#[tokio::test]
async fn malformed_json_returns_jsonrpc_parse_error() {
    let state = AppState::new(test_config(start_native_stub().await)).expect("state");
    let response = rpc(state, "not-json").await;
    assert_eq!(response["error"]["code"], -32700);
}

#[tokio::test]
async fn batch_requests_are_rejected_as_unsupported() {
    let state = AppState::new(test_config(start_native_stub().await)).expect("state");
    let response = rpc(
        state,
        "  \n[{\"jsonrpc\":\"2.0\",\"method\":\"account_info\",\"id\":1}]",
    )
    .await;
    assert_eq!(response["error"]["code"], -32600);
}

#[tokio::test]
async fn disabled_work_is_rejected_before_upstream_call() {
    let state = AppState::new(test_config(start_native_stub().await)).expect("state");
    let response = rpc(
        state,
        r#"{"jsonrpc":"2.0","method":"work_generate","params":{"hash":"A"},"id":1}"#,
    )
    .await;
    assert_eq!(response["error"]["code"], -32604);
}

#[tokio::test]
async fn discovery_returns_the_profile_document() {
    let state = AppState::new(test_config(start_native_stub().await)).expect("state");
    let response = rpc(state, r#"{"jsonrpc":"2.0","method":"rpc.discover","id":1}"#).await;
    assert_eq!(response["result"]["openrpc"], "1.3.2");
}

#[tokio::test]
async fn confirmation_stream_uses_event_stream_content_type() {
    let state = AppState::new(test_config(start_native_stub().await)).expect("state");
    let request = axum::http::Request::builder()
        .method("GET")
        .uri("/events/confirmations")
        .body(axum::body::Body::empty())
        .expect("request");
    let response = app(state).oneshot(request).await.expect("gateway response");
    assert_eq!(response.headers()["content-type"], "text/event-stream");
    assert_eq!(
        response.headers()["cache-control"],
        "no-cache, no-transform"
    );
    assert_eq!(response.headers()["x-accel-buffering"], "no");
}

#[tokio::test]
async fn readiness_tracks_native_subscription_state() {
    let state = AppState::new(test_config(start_native_stub().await)).expect("state");
    let request = axum::http::Request::builder()
        .method("GET")
        .uri("/readyz")
        .body(axum::body::Body::empty())
        .expect("request");
    let response = app(state.clone())
        .oneshot(request)
        .await
        .expect("gateway response");
    assert_eq!(
        response.status(),
        axum::http::StatusCode::SERVICE_UNAVAILABLE
    );

    state.upstream_ready.store(true, Ordering::Relaxed);
    let request = axum::http::Request::builder()
        .method("GET")
        .uri("/readyz")
        .body(axum::body::Body::empty())
        .expect("request");
    let response = app(state).oneshot(request).await.expect("gateway response");
    assert_eq!(response.status(), axum::http::StatusCode::OK);
}

#[tokio::test]
async fn valid_work_token_reaches_node_delegation() {
    let key = generate_signing_key();
    let mut config = test_config(start_native_stub().await);
    config.allow_work = true;
    config.auth_public_key = Some(
        base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(key.verifying_key().to_bytes()),
    );
    let state = AppState::new(config).expect("state");
    let token = sign_paseto(
        &json!({"aud":"nano-rpc-gateway","sub":"test","scope":"work","exp":4_000_000_000u64}),
        &key,
    );
    let response = rpc_with_auth(
        state,
        r#"{"jsonrpc":"2.0","method":"work_generate","params":{"hash":"A"},"id":1}"#,
        &token,
    )
    .await;
    assert_eq!(response["result"]["frontier"], "A");
}

#[tokio::test]
async fn base_scoped_token_cannot_call_work_generation() {
    let key = generate_signing_key();
    let mut config = test_config(start_native_stub().await);
    config.allow_work = true;
    config.auth_public_key = Some(
        base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(key.verifying_key().to_bytes()),
    );
    let state = AppState::new(config).expect("state");
    let token = sign_paseto(
        &json!({"aud":"nano-rpc-gateway","sub":"test","scope":"base","exp":4_000_000_000u64}),
        &key,
    );
    let response = rpc_with_auth(
        state,
        r#"{"jsonrpc":"2.0","method":"work_generate","params":{"hash":"A"},"id":1}"#,
        &token,
    )
    .await;
    assert_eq!(response["error"]["code"], -32001);
}
