//! Private, disposable relay for deployed SSE continuity tests. Never publish its ports.
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};

use axum::{
    extract::State,
    routing::{get, post},
    Json, Router,
};
use clap::Parser;
use futures_util::{SinkExt, StreamExt};
use serde_json::json;
use tokio::{
    net::{TcpListener, TcpStream},
    sync::watch,
};
use tokio_tungstenite::{
    accept_hdr_async, connect_async,
    tungstenite::{
        client::IntoClientRequest,
        handshake::server::{Request, Response},
        http::StatusCode,
    },
};
use tokio_util::sync::CancellationToken;

#[derive(Parser)]
#[command(name = "ws-fault-relay", version = nano_rpc_gateway::build_info::version())]
struct Args {
    /// Print the shared build identity as JSON and exit.
    #[arg(long)]
    build_info: bool,
    /// Real upstreams, in the same order as /0, /1, ... relay paths.
    #[arg(long, required_unless_present = "build_info")]
    upstream: Vec<String>,
}

struct Relay {
    upstreams: Vec<String>,
    paused: AtomicBool,
    circuit: watch::Sender<CancellationToken>,
}

async fn pause(State(state): State<Arc<Relay>>) -> Json<serde_json::Value> {
    state.paused.store(true, Ordering::Release);
    state.circuit.borrow().cancel();
    Json(json!({"paused":true}))
}

async fn resume(State(state): State<Arc<Relay>>) -> Json<serde_json::Value> {
    state.circuit.send_replace(CancellationToken::new());
    state.paused.store(false, Ordering::Release);
    Json(json!({"paused":false}))
}

#[expect(
    clippy::result_large_err,
    reason = "tungstenite's handshake callback requires an HTTP response error"
)]
async fn relay(stream: TcpStream, state: Arc<Relay>) -> anyhow::Result<()> {
    let cancelled = state.circuit.borrow().clone();
    let mut index = 0;
    let mut client = accept_hdr_async(stream, |request: &Request, response: Response| {
        index = request
            .uri()
            .path()
            .trim_start_matches('/')
            .parse()
            .unwrap_or(usize::MAX);
        if state.paused.load(Ordering::Acquire) || index >= state.upstreams.len() {
            let mut error = tokio_tungstenite::tungstenite::http::Response::new(Some(
                "relay unavailable".into(),
            ));
            *error.status_mut() = StatusCode::SERVICE_UNAVAILABLE;
            Err(error)
        } else {
            Ok(response)
        }
    })
    .await?;
    let mut request = state.upstreams[index].as_str().into_client_request()?;
    request.headers_mut().insert(
        "user-agent",
        nano_rpc_gateway::build_info::USER_AGENT.parse()?,
    );
    let (mut upstream, _) = tokio::select! {
        biased;
        _ = cancelled.cancelled() => return Ok(()),
        connected = connect_async(request) => connected?,
    };
    loop {
        tokio::select! {
            biased;
            _ = cancelled.cancelled() => break,
            message = client.next() => match message {
                Some(Ok(message)) => upstream.send(message).await?,
                Some(Err(error)) => return Err(error.into()),
                None => break,
            },
            message = upstream.next() => match message {
                Some(Ok(message)) => client.send(message).await?,
                Some(Err(error)) => return Err(error.into()),
                None => break,
            },
        }
    }
    Ok(())
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
    let args = Args::parse();
    if args.build_info {
        println!(
            "{}",
            serde_json::to_string_pretty(&nano_rpc_gateway::build_info::json())?
        );
        return Ok(());
    }
    let (circuit, _) = watch::channel(CancellationToken::new());
    let state = Arc::new(Relay {
        upstreams: args.upstream,
        paused: AtomicBool::new(false),
        circuit,
    });
    let control = Router::new()
        .route("/pause", post(pause))
        .route("/resume", post(resume))
        .route(
            "/status",
            get(|State(state): State<Arc<Relay>>| async move {
                Json(json!({"paused":state.paused.load(Ordering::Acquire)}))
            }),
        )
        .with_state(state.clone());
    let listener = TcpListener::bind("0.0.0.0:8080").await?;
    let control_listener = TcpListener::bind("0.0.0.0:8081").await?;
    let proxy = async move {
        loop {
            let (stream, _) = match listener.accept().await {
                Ok(connection) => connection,
                Err(error) => break Err::<(), anyhow::Error>(error.into()),
            };
            let state = state.clone();
            tokio::spawn(async move {
                if let Err(error) = relay(stream, state).await {
                    eprintln!("relay circuit ended: {error}");
                }
            });
        }
    };
    tokio::select! {
        result = proxy => result?,
        result = axum::serve(control_listener, control) => result?,
    }
    Ok(())
}
