use std::{path::PathBuf, process::Command};

use base64::Engine;
use clap::{Parser, Subcommand};
use nano_rpc_gateway::{app, generate_signing_key, playground_url, sign_paseto, AppState, Config};
use serde_json::json;

#[derive(Debug, Parser)]
#[command(name = "nano-rpc-gateway", version)]
struct Cli {
    #[command(subcommand)]
    command: CommandKind,
}

#[derive(Debug, Subcommand)]
enum CommandKind {
    Serve {
        #[arg(long, default_value = "gateway.yaml")]
        config: PathBuf,
    },
    Playground {
        #[arg(long, default_value = "http://127.0.0.1:8090/rpc")]
        gateway_url: String,
        #[arg(long)]
        schema_url: Option<String>,
        #[arg(long)]
        launch: bool,
        #[arg(long)]
        serve: bool,
    },
    Keygen,
    Issue {
        #[arg(long)]
        secret: String,
        #[arg(long, default_value = "work")]
        scope: String,
        #[arg(long, default_value_t = 3600)]
        ttl: u64,
        #[arg(long, default_value = "developer")]
        subject: String,
    },
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // axum-server selects the aws-lc-rs Rustls backend while reqwest also
    // enables ring. Install the server provider explicitly so TLS startup is
    // deterministic when both backends are present in the dependency graph.
    let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
    tracing_subscriber::fmt()
        .with_env_filter("info")
        .json()
        .init();
    match Cli::parse().command {
        CommandKind::Serve { config } => {
            let config = Config::load(&config)?;
            let state = AppState::new(config.clone())?;
            let bridge_state = state.clone();
            tokio::spawn(async move {
                loop {
                    if let Err(error) = nano_rpc_gateway::run_ws_bridge(bridge_state.clone()).await
                    {
                        tracing::warn!(%error, "native WebSocket bridge disconnected; retrying");
                    }
                    tokio::time::sleep(std::time::Duration::from_secs(2)).await;
                }
            });
            tracing::info!(address = %config.listen, "gateway listening");
            let router = app(state);
            match (config.tls_cert, config.tls_key) {
                (Some(cert), Some(key)) => {
                    let tls =
                        axum_server::tls_rustls::RustlsConfig::from_pem_file(cert, key).await?;
                    axum_server::bind_rustls(config.listen.parse()?, tls)
                        .serve(router.into_make_service())
                        .await?;
                }
                _ => {
                    let listener = tokio::net::TcpListener::bind(&config.listen).await?;
                    axum::serve(listener, router).await?;
                }
            }
        }
        CommandKind::Playground {
            gateway_url,
            schema_url,
            launch,
            serve,
        } => {
            let url = if serve {
                let script = PathBuf::from("scripts/serve-openrpc-playground.sh");
                if !script.is_file() {
                    anyhow::bail!(
                        "--serve is development-only and requires {} from the repository checkout",
                        script.display()
                    );
                }
                let _child = Command::new(&script).spawn()?;
                playground_url(&gateway_url, schema_url.as_deref(), true)
            } else {
                playground_url(&gateway_url, schema_url.as_deref(), false)
            };
            println!("{url}");
            if launch {
                let _ = Command::new("open").arg(&url).status();
            }
        }
        CommandKind::Keygen => {
            let key = generate_signing_key();
            println!(
                "public_key={}",
                base64::engine::general_purpose::URL_SAFE_NO_PAD
                    .encode(key.verifying_key().to_bytes())
            );
            println!(
                "secret_key={}",
                base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(key.to_bytes())
            );
        }
        CommandKind::Issue {
            secret,
            scope,
            ttl,
            subject,
        } => {
            let bytes: [u8; 32] = base64::engine::general_purpose::URL_SAFE_NO_PAD
                .decode(secret)?
                .try_into()
                .map_err(|_| anyhow::anyhow!("secret key must be 32 bytes"))?;
            let key = ed25519_dalek::SigningKey::from_bytes(&bytes);
            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)?
                .as_secs();
            println!(
                "{}",
                sign_paseto(
                    &json!({"aud":"nano-rpc-gateway","sub":subject,"scope":scope,"iat":now,"exp":now+ttl}),
                    &key
                )
            );
        }
    }
    Ok(())
}
