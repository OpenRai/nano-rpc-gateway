use std::sync::atomic::Ordering;
use std::time::Instant;

use axum::{routing::post, Json, Router};
use base64::Engine;
use futures_util::{SinkExt, StreamExt};
use futures_util::future::join_all;
use http_body_util::BodyExt;
use nano_rpc_gateway::{app, generate_signing_key, sign_paseto, AppState, Config};
use serde_json::{json, Value};
use tokio::net::TcpListener;
use tower::ServiceExt;

async fn start_native_stub() -> String {
    let router = Router::new().route(
        "/",
        post(|Json(body): Json<Value>| async move {
            let response = match body["action"].as_str() {
                Some("account_info") if body["account"] == "secret" => {
                    json!({"error":"private request material"})
                }
                Some("account_info") => json!({
                    "frontier":"A", "open_block":"B", "representative_block":"C",
                    "balance":"0", "modified_timestamp":"0", "block_count":"1",
                    "account_version":"0", "confirmation_height":"1",
                    "confirmation_height_frontier":"A"
                }),
                Some("account_balance") => json!({"balance":"0", "pending":"0"}),
                Some("account_history") => json!({"account":"nano_test", "history": []}),
                Some("block_info") => json!({"block_account":"nano_test", "amount":"0", "balance":"0", "height":"1", "contents":"{}"}),
                Some("blocks_info") => json!({"blocks": {}}),
                Some("process") => json!({"hash":"A"}),
                Some("work_generate") => json!({"hash":"A", "work":"B", "difficulty":"C", "multiplier":"1"}),
                _ => json!({"error":"unknown action"}),
            };
            Json(response)
        }),
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
    let response = app(state.clone())
        .oneshot(request)
        .await
        .expect("gateway response");
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
    let response = app(state.clone())
        .oneshot(request)
        .await
        .expect("gateway response");
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
    assert_eq!(response["id"], 1);
}

#[tokio::test]
async fn base_profile_matrix_translates_all_six_methods() {
    let state = AppState::new(test_config(start_native_stub().await)).expect("state");
    let cases = [
        ("account_info", json!({"account":"nano_test"}), "frontier"),
        ("account_balance", json!({"account":"nano_test"}), "balance"),
        (
            "account_history",
            json!({"account":"nano_test", "count": 1}),
            "history",
        ),
        ("block_info", json!({"hash":"A"}), "block_account"),
        ("blocks_info", json!({"hashes":["A"]}), "blocks"),
        ("process", json!({"block":{"type":"state"}}), "hash"),
    ];
    for (index, (method, params, result_field)) in cases.into_iter().enumerate() {
        let request = format!(
            r#"{{"jsonrpc":"2.0","method":"{method}","params":{params},"id":{}}}"#,
            index + 1
        );
        let response = rpc(state.clone(), &request).await;
        assert!(response["error"].is_null(), "{method}: {response}");
        assert!(
            response["result"].get(result_field).is_some(),
            "{method}: {response}"
        );
        assert_eq!(response["id"], index + 1);
    }

    let invalid_history = rpc(
        state.clone(),
        r#"{"jsonrpc":"2.0","method":"account_history","params":{"account":"nano_test","count":0},"id":7}"#,
    )
    .await;
    assert_eq!(invalid_history["error"]["code"], -32602);
    let invalid_blocks = rpc(
        state,
        r#"{"jsonrpc":"2.0","method":"blocks_info","params":{"hashes":[]},"id":8}"#,
    )
    .await;
    assert_eq!(invalid_blocks["error"]["code"], -32602);
}

#[tokio::test]
async fn native_client_flattens_action_and_classifies_native_errors() {
    let endpoint = start_native_stub().await;
    let client = nano_rpc_gateway::NativeClient::new(endpoint).expect("native client");
    let result = client
        .call("account_info", &json!({"account":"nano_test"}))
        .await
        .expect("account result");
    assert_eq!(result["frontier"], "A");
    let error = client
        .call("unknown_action", &json!({}))
        .await
        .expect_err("native error");
    assert!(error.to_string().contains("unknown action"));
}

#[tokio::test]
async fn native_client_preserves_string_and_numeric_named_parameters() {
    let router = Router::new().route(
        "/",
        post(|Json(body): Json<Value>| async move {
            Json(json!({
                "account": body["account"],
                "count": body["count"],
                "action": body["action"]
            }))
        }),
    );
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind stub");
    let address = listener.local_addr().expect("stub address");
    tokio::spawn(async move { axum::serve(listener, router).await.expect("stub server") });

    let client =
        nano_rpc_gateway::NativeClient::new(format!("http://{address}")).expect("native client");
    let result = client
        .call(
            "account_history",
            &json!({"account":"nano_test", "count": 7}),
        )
        .await
        .expect("echo result");
    assert_eq!(result["action"], "account_history");
    assert_eq!(result["account"], "nano_test");
    assert_eq!(result["count"], 7);
}

#[tokio::test]
async fn native_client_classifies_malformed_response_and_timeout() {
    let malformed_router = Router::new().route(
        "/",
        post(|| async { (axum::http::StatusCode::OK, "not-json") }),
    );
    let malformed_listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind malformed stub");
    let malformed_address = malformed_listener.local_addr().expect("malformed address");
    tokio::spawn(async move {
        axum::serve(malformed_listener, malformed_router)
            .await
            .expect("malformed server")
    });
    let malformed_client =
        nano_rpc_gateway::NativeClient::new(format!("http://{malformed_address}"))
            .expect("malformed client");
    assert!(malformed_client
        .call("account_info", &json!({}))
        .await
        .is_err());

    let hanging_listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind hanging stub");
    let hanging_address = hanging_listener.local_addr().expect("hanging address");
    tokio::spawn(async move {
        let (_stream, _) = hanging_listener.accept().await.expect("hanging client");
        tokio::time::sleep(std::time::Duration::from_secs(1)).await;
    });
    let timeout_client = nano_rpc_gateway::NativeClient::with_timeout(
        format!("http://{hanging_address}"),
        std::time::Duration::from_millis(20),
    )
    .expect("timeout client");
    assert!(timeout_client
        .call("account_info", &json!({"account": "nano_test"}))
        .await
        .is_err());
}

#[tokio::test]
async fn native_client_reports_connection_refusal() {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("reserve port");
    let address = listener.local_addr().expect("reserved address");
    drop(listener);
    let client = nano_rpc_gateway::NativeClient::with_timeout(
        format!("http://{address}"),
        std::time::Duration::from_millis(100),
    )
    .expect("refusal client");
    assert!(client
        .call("account_info", &json!({"account": "nano_test"}))
        .await
        .is_err());
}

#[tokio::test]
async fn upstream_error_response_is_redacted_at_public_rpc_boundary() {
    let state = AppState::new(test_config(start_native_stub().await)).expect("state");
    let response = rpc(
        state,
        r#"{"jsonrpc":"2.0","method":"account_info","params":{"account":"secret"},"id":1}"#,
    )
    .await;
    assert_eq!(response["error"]["code"], -32000);
    assert!(!response.to_string().contains("private request material"));
}

#[tokio::test]
async fn explicit_null_id_is_preserved_as_jsonrpc_null() {
    let state = AppState::new(test_config(start_native_stub().await)).expect("state");
    let response = rpc(
        state,
        r#"{"jsonrpc":"2.0","method":"account_info","params":{"account":"nano_test"},"id":null}"#,
    )
    .await;
    assert!(response["error"].is_null());
    assert!(response["result"].is_object());
    assert!(response["id"].is_null());
}

#[tokio::test]
async fn malformed_json_returns_jsonrpc_parse_error() {
    let state = AppState::new(test_config(start_native_stub().await)).expect("state");
    let response = rpc(state, "not-json").await;
    assert_eq!(response["error"]["code"], -32700);
}

#[tokio::test]
async fn dispatcher_returns_stable_errors_for_invalid_params_unknown_method_and_notifications() {
    let state = AppState::new(test_config(start_native_stub().await)).expect("state");

    let invalid_params = rpc(
        state.clone(),
        r#"{"jsonrpc":"2.0","method":"account_info","params":{"account":7},"id":"a"}"#,
    )
    .await;
    assert_eq!(invalid_params["error"]["code"], -32602);
    assert_eq!(invalid_params["id"], "a");

    let unknown_method = rpc(
        state.clone(),
        r#"{"jsonrpc":"2.0","method":"does_not_exist","id":2}"#,
    )
    .await;
    assert_eq!(unknown_method["error"]["code"], -32601);
    assert_eq!(unknown_method["id"], 2);

    let notification = rpc(
        state,
        r#"{"jsonrpc":"2.0","method":"account_info","params":{"account":"nano_test"}}"#,
    )
    .await;
    assert_eq!(notification["error"]["code"], -32600);
    assert!(notification["id"].is_null());

    let invalid_id = rpc(
        AppState::new(test_config(start_native_stub().await)).expect("state"),
        r#"{"jsonrpc":"2.0","method":"account_info","params":{"account":"nano_test"},"id":{}}"#,
    )
    .await;
    assert_eq!(invalid_id["error"]["code"], -32600);

    let invalid_version = rpc(
        AppState::new(test_config(start_native_stub().await)).expect("state"),
        r#"{"jsonrpc":"1.0","method":"account_info","params":{"account":"nano_test"},"id":3}"#,
    )
    .await;
    assert_eq!(invalid_version["error"]["code"], -32600);
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
async fn oversized_rpc_body_is_rejected_by_gateway_limit() {
    let state = AppState::new(test_config(start_native_stub().await)).expect("state");
    let body = vec![b'x'; 1024 * 1024 + 1];
    let request = axum::http::Request::builder()
        .method("POST")
        .uri("/rpc")
        .header("content-type", "application/json")
        .body(axum::body::Body::from(body))
        .expect("request");
    let response = app(state.clone())
        .oneshot(request)
        .await
        .expect("gateway response");
    assert_eq!(response.status(), axum::http::StatusCode::PAYLOAD_TOO_LARGE);
}

#[tokio::test]
async fn metrics_expose_request_and_error_counters() {
    let state = AppState::new(test_config(start_native_stub().await)).expect("state");
    let _ = rpc(
        state.clone(),
        r#"{"jsonrpc":"2.0","method":"account_info","params":{"account":"nano_test"},"id":1}"#,
    )
    .await;
    let request = axum::http::Request::builder()
        .method("GET")
        .uri("/metrics")
        .body(axum::body::Body::empty())
        .expect("request");
    let response = app(state.clone())
        .oneshot(request)
        .await
        .expect("gateway response");
    let bytes = response
        .into_body()
        .collect()
        .await
        .expect("metrics body")
        .to_bytes();
    let metrics = String::from_utf8(bytes.to_vec()).expect("metrics text");
    assert!(metrics.contains("nano_gateway_requests_total 1"));
    assert!(metrics.contains("nano_gateway_errors_total 0"));
    assert!(metrics.contains("nano_gateway_request_duration_ms_count 1"));
    assert!(metrics.contains("nano_gateway_sse_queue_capacity 256"));
    assert!(!metrics.contains("nano_test"));
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
async fn discovery_can_be_disabled_without_removing_static_schema() {
    let mut config = test_config(start_native_stub().await);
    config.enable_discovery = false;
    let state = AppState::new(config).expect("state");
    let response = rpc(
        state.clone(),
        r#"{"jsonrpc":"2.0","method":"rpc.discover","id":1}"#,
    )
    .await;
    assert_eq!(response["error"]["code"], -32601);
    assert!(response["error"]["message"]
        .as_str()
        .expect("error message")
        .contains("/openrpc.json"));

    let request = axum::http::Request::builder()
        .method("GET")
        .uri("/openrpc.json")
        .body(axum::body::Body::empty())
        .expect("request");
    let response = app(state).oneshot(request).await.expect("gateway response");
    let document = response
        .into_body()
        .collect()
        .await
        .expect("schema body")
        .to_bytes();
    let document: Value = serde_json::from_slice(&document).expect("schema JSON");
    assert_eq!(document["openrpc"], "1.3.2");
    assert!(!document["methods"]
        .as_array()
        .expect("methods")
        .iter()
        .any(|method| method["name"] == "rpc.discover"));
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
async fn confirmation_stream_rejects_unbounded_account_filters() {
    let state = AppState::new(test_config(start_native_stub().await)).expect("state");
    let oversized = "a".repeat(4097);
    let request = axum::http::Request::builder()
        .method("GET")
        .uri(format!("/events/confirmations?accounts={oversized}"))
        .body(axum::body::Body::empty())
        .expect("request");
    let response = app(state).oneshot(request).await.expect("gateway response");
    assert_eq!(response.status(), axum::http::StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn cors_allows_playground_origins_but_not_arbitrary_origins() {
    let state = AppState::new(test_config(start_native_stub().await)).expect("state");
    let allowed = axum::http::Request::builder()
        .method("GET")
        .uri("/health")
        .header("origin", "http://127.0.0.1:8080")
        .body(axum::body::Body::empty())
        .expect("allowed request");
    let response = app(state.clone())
        .oneshot(allowed)
        .await
        .expect("allowed response");
    assert_eq!(
        response.headers()["access-control-allow-origin"],
        "http://127.0.0.1:8080"
    );

    let denied = axum::http::Request::builder()
        .method("GET")
        .uri("/health")
        .header("origin", "https://unexpected.example")
        .body(axum::body::Body::empty())
        .expect("denied request");
    let response = app(state).oneshot(denied).await.expect("denied response");
    assert!(response
        .headers()
        .get("access-control-allow-origin")
        .is_none());
}

#[tokio::test]
async fn confirmation_stream_emits_reset_for_unknown_generation() {
    let state = AppState::new(test_config(start_native_stub().await)).expect("state");
    state
        .events
        .publish(
            "nano.confirmation",
            json!({"account":"nano_test","hash":"A"}),
        )
        .await;
    let request = axum::http::Request::builder()
        .method("GET")
        .uri("/events/confirmations?accounts=nano_test")
        .header("last-event-id", "old-generation:0")
        .body(axum::body::Body::empty())
        .expect("request");
    let response = app(state.clone())
        .oneshot(request)
        .await
        .expect("gateway response");
    let mut body = response.into_body();
    let frame = body
        .frame()
        .await
        .expect("reset frame")
        .expect("reset data")
        .into_data()
        .expect("reset bytes");
    let text = String::from_utf8(frame.to_vec()).expect("reset text");
    assert!(text.contains("event: nano.stream_reset"));
    assert!(text.contains("reconcile with JSON-RPC"));
    assert_eq!(state.metrics.replay_misses.load(Ordering::Relaxed), 1);
}

#[tokio::test]
async fn confirmation_stream_replays_events_after_last_event_id() {
    let state = AppState::new(test_config(start_native_stub().await)).expect("state");
    let first = state
        .events
        .publish("nano.confirmation", json!({"hash": "first"}))
        .await;
    let second = state
        .events
        .publish("nano.confirmation", json!({"hash": "second"}))
        .await;
    let request = axum::http::Request::builder()
        .method("GET")
        .uri("/events/confirmations")
        .header("last-event-id", first.id)
        .body(axum::body::Body::empty())
        .expect("request");
    let response = app(state.clone())
        .oneshot(request)
        .await
        .expect("gateway response");
    let frame = response
        .into_body()
        .frame()
        .await
        .expect("replay frame")
        .expect("replay data")
        .into_data()
        .expect("replay bytes");
    let text = String::from_utf8(frame.to_vec()).expect("replay text");
    assert!(text.contains("id: "));
    assert!(text.contains(&second.id));
    assert!(text.contains("\"hash\":\"second\""));
    assert_eq!(state.metrics.replay_hits.load(Ordering::Relaxed), 1);
}

#[tokio::test]
async fn confirmation_stream_emits_reset_after_bounded_receiver_lag() {
    let state = AppState::new(test_config(start_native_stub().await)).expect("state");
    let request = axum::http::Request::builder()
        .method("GET")
        .uri("/events/confirmations")
        .body(axum::body::Body::empty())
        .expect("request");
    let response = app(state.clone())
        .oneshot(request)
        .await
        .expect("gateway response");
    let mut body = response.into_body();

    for index in 0..(256 + 8) {
        state
            .events
            .publish(
                "nano.confirmation",
                json!({"hash": format!("hash-{index}")}),
            )
            .await;
    }

    let mut saw_reset = false;
    for _ in 0..(256 + 16) {
        let frame = tokio::time::timeout(std::time::Duration::from_secs(1), body.frame())
            .await
            .expect("SSE frame timeout")
            .expect("SSE frame")
            .expect("SSE data")
            .into_data()
            .expect("SSE bytes");
        if String::from_utf8_lossy(&frame).contains("event: nano.stream_reset") {
            saw_reset = true;
            break;
        }
    }
    assert!(saw_reset, "bounded receiver lag must produce a reset frame");
    assert_eq!(state.metrics.overflow_resets.load(Ordering::Relaxed), 1);
}

#[tokio::test]
async fn confirmation_stream_releases_active_stream_metric_when_cancelled() {
    let state = AppState::new(test_config(start_native_stub().await)).expect("state");
    state
        .events
        .publish("nano.confirmation", json!({"hash": "cancel-test"}))
        .await;
    let request = axum::http::Request::builder()
        .method("GET")
        .uri("/events/confirmations")
        .body(axum::body::Body::empty())
        .expect("request");
    let response = app(state.clone())
        .oneshot(request)
        .await
        .expect("gateway response");
    let mut body = response.into_body();
    let _ = body.frame().await.expect("SSE frame").expect("SSE data");
    assert_eq!(state.metrics.active_streams.load(Ordering::Relaxed), 1);
    drop(body);
    for _ in 0..20 {
        if state.metrics.active_streams.load(Ordering::Relaxed) == 0 {
            return;
        }
        tokio::task::yield_now().await;
    }
    assert_eq!(state.metrics.active_streams.load(Ordering::Relaxed), 0);
}

#[tokio::test]
async fn deterministic_public_flow_covers_discovery_process_confirmation_and_reconcile() {
    let native_router = Router::new().route(
        "/",
        post(|Json(body): Json<Value>| async move {
            let response = match body["action"].as_str() {
                Some("account_info") => json!({
                    "frontier":"A", "open_block":"B", "representative_block":"C",
                    "balance":"0", "modified_timestamp":"0", "block_count":"1",
                    "account_version":"0", "confirmation_height":"1",
                    "confirmation_height_frontier":"A"
                }),
                Some("process") => json!({"hash":"FLOW-HASH"}),
                _ => json!({"error":"unsupported"}),
            };
            Json(response)
        }),
    );
    let native_listener = TcpListener::bind("127.0.0.1:0").await.expect("native bind");
    let native_address = native_listener.local_addr().expect("native address");
    tokio::spawn(async move {
        axum::serve(native_listener, native_router)
            .await
            .expect("native server")
    });

    let ws_listener = TcpListener::bind("127.0.0.1:0").await.expect("ws bind");
    let ws_address = ws_listener.local_addr().expect("ws address");
    let ws_task = tokio::spawn(async move {
        let (stream, _) = ws_listener.accept().await.expect("ws client");
        let mut socket = tokio_tungstenite::accept_async(stream)
            .await
            .expect("ws handshake");
        let subscribe = socket
            .next()
            .await
            .expect("subscribe frame")
            .expect("subscribe message")
            .into_text()
            .expect("subscribe text");
        assert!(subscribe.contains("\"topic\":\"confirmation\""));
        socket
            .send(tokio_tungstenite::tungstenite::Message::Text(
                json!({"ack":"subscribe","topic":"confirmation"}).to_string(),
            ))
            .await
            .expect("subscribe ack");
        socket
            .send(tokio_tungstenite::tungstenite::Message::Text(
                json!({
                    "topic":"confirmation",
                    "message":{"account":"nano_flow","hash":"FLOW-HASH"}
                })
                .to_string(),
            ))
            .await
            .expect("confirmation");
        socket.close(None).await.expect("upstream close");
    });

    let gateway_listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("gateway bind");
    let gateway_address = gateway_listener.local_addr().expect("gateway address");
    let config = Config {
        listen: format!("{gateway_address}"),
        node_rpc_url: format!("http://{native_address}"),
        node_ws_url: format!("ws://{ws_address}"),
        profile: "nano-node/test".into(),
        allow_work: false,
        allow_control: false,
        auth_public_key: None,
        enable_discovery: true,
        tls_cert: None,
        tls_key: None,
    };
    let state = AppState::new(config).expect("state");
    let gateway_state = state.clone();
    let gateway_task = tokio::spawn(async move {
        axum::serve(gateway_listener, app(gateway_state))
            .await
            .expect("gateway server")
    });

    let client = reqwest::Client::new();
    let base = format!("http://{gateway_address}");
    let mut sse_response = client
        .get(format!("{base}/events/confirmations"))
        .send()
        .await
        .expect("SSE connect");
    assert_eq!(sse_response.status(), reqwest::StatusCode::OK);
    let bridge_state = state.clone();
    let bridge = tokio::spawn(async move { nano_rpc_gateway::run_ws_bridge(bridge_state).await });

    let discover: Value = client
        .post(format!("{base}/rpc"))
        .json(&json!({"jsonrpc":"2.0","method":"rpc.discover","id":1}))
        .send()
        .await
        .expect("discover request")
        .json()
        .await
        .expect("discover JSON");
    assert_eq!(discover["result"]["openrpc"], "1.3.2");

    let account_info: Value = client
        .post(format!("{base}/rpc"))
        .json(&json!({
            "jsonrpc":"2.0", "method":"account_info",
            "params":{"account":"nano_flow"}, "id":2
        }))
        .send()
        .await
        .expect("account request")
        .json()
        .await
        .expect("account JSON");
    assert_eq!(account_info["result"]["frontier"], "A");

    let process: Value = client
        .post(format!("{base}/rpc"))
        .json(&json!({
            "jsonrpc":"2.0", "method":"process",
            "params":{"block":{"type":"state","hash":"FLOW-HASH"}}, "id":3
        }))
        .send()
        .await
        .expect("process request")
        .json()
        .await
        .expect("process JSON");
    assert_eq!(process["result"]["hash"], "FLOW-HASH");

    let mut transcript = String::new();
    for _ in 0..8 {
        let chunk = tokio::time::timeout(std::time::Duration::from_secs(2), sse_response.chunk())
            .await
            .expect("SSE timeout")
            .expect("SSE chunk result")
            .expect("SSE chunk");
        transcript.push_str(std::str::from_utf8(&chunk).expect("SSE UTF-8"));
        if transcript.contains("event: nano.confirmation")
            && transcript.contains("event: nano.stream_reset")
        {
            break;
        }
    }
    assert!(transcript.contains("event: nano.confirmation"));
    assert!(transcript.contains("FLOW-HASH"));
    assert!(transcript.contains("event: nano.stream_reset"));
    let confirmation_id = transcript
        .split("\n\n")
        .find(|frame| frame.contains("event: nano.confirmation"))
        .and_then(|frame| frame.lines().find(|line| line.starts_with("id: ")))
        .and_then(|line| line.strip_prefix("id: "))
        .expect("confirmation cursor");

    let mut replay = client
        .get(format!("{base}/events/confirmations"))
        .header("last-event-id", confirmation_id)
        .send()
        .await
        .expect("replay connect");
    let replay_chunk = tokio::time::timeout(std::time::Duration::from_secs(2), replay.chunk())
        .await
        .expect("replay timeout")
        .expect("replay chunk result")
        .expect("replay chunk");
    assert!(std::str::from_utf8(&replay_chunk)
        .expect("replay UTF-8")
        .contains("event: nano.stream_reset"));

    assert!(bridge.await.expect("bridge task").is_ok());
    ws_task.await.expect("ws task");
    gateway_task.abort();
}

#[tokio::test]
async fn sse_fanout_benchmark_delivers_one_event_to_all_clients() {
    let clients = std::env::var("SSE_FANOUT_CLIENTS")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .filter(|value| (1..=64).contains(value))
        .unwrap_or(8);

    let ws_listener = TcpListener::bind("127.0.0.1:0").await.expect("ws bind");
    let ws_address = ws_listener.local_addr().expect("ws address");
    let ws_task = tokio::spawn(async move {
        let (stream, _) = ws_listener.accept().await.expect("ws client");
        let mut socket = tokio_tungstenite::accept_async(stream)
            .await
            .expect("ws handshake");
        socket.next().await.expect("subscribe frame").expect("subscribe");
        socket
            .send(tokio_tungstenite::tungstenite::Message::Text(
                json!({"ack":"subscribe","topic":"confirmation"}).to_string(),
            ))
            .await
            .expect("subscribe ack");
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        socket
            .send(tokio_tungstenite::tungstenite::Message::Text(
                json!({
                    "topic":"confirmation",
                    "message":{"account":"nano_fanout","hash":"FANOUT-HASH"}
                })
                .to_string(),
            ))
            .await
            .expect("confirmation");
        socket.close(None).await.expect("ws close");
    });

    let native_url = start_native_stub().await;
    let gateway_listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("gateway bind");
    let gateway_address = gateway_listener.local_addr().expect("gateway address");
    let mut config = test_config(native_url);
    config.listen = format!("{gateway_address}");
    config.node_ws_url = format!("ws://{ws_address}");
    let state = AppState::new(config).expect("state");
    let gateway_state = state.clone();
    let gateway_task = tokio::spawn(async move {
        axum::serve(gateway_listener, app(gateway_state))
            .await
            .expect("gateway server")
    });

    let client = reqwest::Client::new();
    let base = format!("http://{gateway_address}");
    let mut responses = Vec::with_capacity(clients);
    for _ in 0..clients {
        let response = client
            .get(format!("{base}/events/confirmations"))
            .send()
            .await
            .expect("SSE connect");
        assert_eq!(response.status(), reqwest::StatusCode::OK);
        responses.push(response);
    }

    let bridge_state = state.clone();
    let bridge = tokio::spawn(async move { nano_rpc_gateway::run_ws_bridge(bridge_state).await });
    let started = Instant::now();
    let readers = responses.into_iter().map(|mut response| async move {
        let mut transcript = String::new();
        loop {
            let chunk = tokio::time::timeout(std::time::Duration::from_secs(3), response.chunk())
                .await
                .expect("SSE timeout")
                .expect("SSE chunk result")
                .expect("SSE chunk");
            transcript.push_str(std::str::from_utf8(&chunk).expect("SSE UTF-8"));
            if transcript.contains("event: nano.confirmation")
                && transcript.contains("FANOUT-HASH")
            {
                return;
            }
        }
    });
    join_all(readers).await;
    let elapsed_ms = started.elapsed().as_secs_f64() * 1000.0;
    println!("sse_fanout clients={clients} delivered={clients} elapsed_ms={elapsed_ms:.3}");

    assert_eq!(state.metrics.active_streams.load(Ordering::Relaxed), clients as u64);
    bridge.await.expect("bridge task").expect("bridge result");
    ws_task.await.expect("ws task");
    gateway_task.abort();
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
    assert_eq!(response["result"]["hash"], "A");
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

#[tokio::test]
async fn work_generation_requires_valid_configured_token() {
    let key = generate_signing_key();
    let mut config = test_config(start_native_stub().await);
    config.allow_work = true;
    config.auth_public_key = Some(
        base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(key.verifying_key().to_bytes()),
    );
    let state = AppState::new(config).expect("state");

    let missing = rpc(
        state.clone(),
        r#"{"jsonrpc":"2.0","method":"work_generate","params":{"hash":"A"},"id":1}"#,
    )
    .await;
    assert_eq!(missing["error"]["code"], -32001);

    let expired = sign_paseto(
        &json!({"aud":"nano-rpc-gateway","sub":"test","scope":"work","exp":1}),
        &key,
    );
    let expired_response = rpc_with_auth(
        state.clone(),
        r#"{"jsonrpc":"2.0","method":"work_generate","params":{"hash":"A"},"id":2}"#,
        &expired,
    )
    .await;
    assert_eq!(expired_response["error"]["code"], -32001);

    let wrong_audience = sign_paseto(
        &json!({"aud":"other","sub":"test","scope":"work","exp":4_000_000_000u64}),
        &key,
    );
    let wrong_response = rpc_with_auth(
        state,
        r#"{"jsonrpc":"2.0","method":"work_generate","params":{"hash":"A"},"id":3}"#,
        &wrong_audience,
    )
    .await;
    assert_eq!(wrong_response["error"]["code"], -32001);
}
