use std::{
    io::{self, IsTerminal},
    path::{Path, PathBuf},
    process::Command,
};

use base64::Engine;
use clap::{Parser, Subcommand, ValueEnum};
use nano_rpc_gateway::{
    app, asyncapi_document, generate_signing_key, openrpc_document_with_auth, playground_url,
    sign_paseto, AppState, Config,
};
use serde_json::json;
use tokio_util::{sync::CancellationToken, task::TaskTracker};
use tracing_subscriber::EnvFilter;
use url::Url;

const STARTUP_MARK: &str = include_str!("../assets/startup-banner.txt");

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
enum LogFormat {
    Pretty,
    Json,
}

#[derive(Debug, Parser)]
#[command(name = "nano-rpc-gateway", version)]
struct Cli {
    /// Terminal logs are human-readable by default; use JSON for log collectors.
    #[arg(long, global = true, value_enum, default_value_t = LogFormat::Pretty, env = "NANO_GATEWAY_LOG_FORMAT")]
    log_format: LogFormat,
    /// Log safe JSON-RPC receipt, upstream response, and SSE subscription lifecycle events.
    #[arg(long, global = true, env = "NANO_GATEWAY_LOG_RPC")]
    log_rpc: bool,
    #[command(subcommand)]
    command: CommandKind,
}

fn init_logging(format: LogFormat) {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    let subscriber = tracing_subscriber::fmt().with_env_filter(filter);
    match format {
        LogFormat::Pretty => subscriber
            .compact()
            .with_target(false)
            .with_ansi(io::stderr().is_terminal())
            .init(),
        LogFormat::Json => subscriber.json().init(),
    }
}

fn endpoint_label(value: &str) -> String {
    let Ok(url) = Url::parse(value) else {
        return "(invalid endpoint)".into();
    };
    let host = url.host_str().unwrap_or("(missing host)");
    let port = url
        .port()
        .map(|port| format!(":{port}"))
        .unwrap_or_default();
    format!("{}://{host}{port}{}", url.scheme(), url.path())
}

fn render_startup_template(config_path: &Path, config: &Config) -> String {
    STARTUP_MARK
        .replace("{{config_path}}", &config_path.display().to_string())
        .replace("{{listener}}", &config.listen)
        .replace("{{profile}}", &config.profile)
}

fn serve_banner(config_path: &Path, config: &Config, transport: &str) -> String {
    format!(
        "\n{}\n\
         transport={} common={} work={} rpc={} websocket={}",
        render_startup_template(config_path, config),
        transport,
        if config.require_common_auth {
            "authenticated"
        } else {
            "public"
        },
        if config.allow_work {
            "enabled"
        } else {
            "disabled"
        },
        config
            .node_rpc_urls
            .iter()
            .map(|url| endpoint_label(url))
            .collect::<Vec<_>>()
            .join(", "),
        config
            .node_ws_urls
            .iter()
            .map(|url| endpoint_label(url))
            .collect::<Vec<_>>()
            .join(", "),
    )
}

fn print_serve_banner(config_path: &Path, config: &Config, transport: &str) {
    eprintln!("{}", serve_banner(config_path, config, transport));
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
    /// Export the versioned machine-readable contracts.
    Contracts {
        #[arg(long, default_value = "generated")]
        output_dir: PathBuf,
    },
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
    let cli = Cli::parse();
    init_logging(cli.log_format);
    match cli.command {
        CommandKind::Serve {
            config: config_path,
        } => {
            let mut config = Config::load(&config_path)?;
            config.log_rpc |= cli.log_rpc;
            let state = AppState::new(config.clone())?;
            let bridge_state = state.clone();
            let shutdown = CancellationToken::new();
            let tracker = TaskTracker::new();
            let bridge_shutdown = shutdown.clone();
            tracker.spawn(async move {
                loop {
                    tokio::select! {
                        _ = bridge_shutdown.cancelled() => break,
                        _ = nano_rpc_gateway::run_ws_bridge(bridge_state.clone()) => {
                            tokio::select! {
                                _ = bridge_shutdown.cancelled() => break,
                                _ = tokio::time::sleep(std::time::Duration::from_secs(2)) => {}
                            }
                        }
                    }
                }
            });
            let router = app(state);
            let server = async {
                match (&config.tls_cert, &config.tls_key) {
                    (Some(cert), Some(key)) => {
                        let tls =
                            axum_server::tls_rustls::RustlsConfig::from_pem_file(cert, key).await?;
                        let listener = std::net::TcpListener::bind(&config.listen)?;
                        if cli.log_format == LogFormat::Pretty {
                            print_serve_banner(&config_path, &config, "https");
                        } else {
                            tracing::info!(address = %config.listen, transport = "https", "gateway started");
                        }
                        axum_server::from_tcp_rustls(listener, tls)
                            .serve(router.into_make_service())
                            .await?;
                    }
                    _ => {
                        let listener = tokio::net::TcpListener::bind(&config.listen).await?;
                        if cli.log_format == LogFormat::Pretty {
                            print_serve_banner(&config_path, &config, "http");
                        } else {
                            tracing::info!(address = %config.listen, transport = "http", "gateway started");
                        }
                        axum::serve(listener, router).await?;
                    }
                }
                Ok::<(), anyhow::Error>(())
            };
            tokio::select! {
                result = server => result?,
                _ = tokio::signal::ctrl_c() => tracing::info!("shutdown requested"),
            }
            shutdown.cancel();
            tracker.close();
            tracker.wait().await;
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
        CommandKind::Contracts { output_dir } => {
            std::fs::create_dir_all(&output_dir)?;
            let openrpc = openrpc_document_with_auth(
                "nano-node/V28.2",
                false,
                true,
                "https://gateway.invalid/rpc",
                true,
            );
            let asyncapi = asyncapi_document("nano-node/V28.2", "https://gateway.invalid/rpc");
            for (name, document) in [("openrpc.json", openrpc), ("asyncapi.json", asyncapi)] {
                let mut bytes = serde_json::to_vec_pretty(&document)?;
                bytes.push(b'\n');
                std::fs::write(output_dir.join(name), bytes)?;
            }
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

#[cfg(test)]
mod tests {
    use std::path::Path;

    use nano_rpc_gateway::Config;

    use super::{endpoint_label, serve_banner};

    #[test]
    fn endpoint_labels_exclude_credentials_and_query_values() {
        assert_eq!(
            endpoint_label("https://api-key:@rpc.nano.to/?secret=do-not-log"),
            "https://rpc.nano.to/"
        );
    }

    #[test]
    fn serve_banner_reports_running_configuration_without_secrets() {
        let config = Config {
            node_rpc_urls: vec!["https://nodes.nanswap.com/XNO?api_key=do-not-log".into()],
            node_ws_urls: vec!["wss://nodes.nanswap.com/ws/?api_key=do-not-log".into()],
            require_common_auth: false,
            ..Config::default()
        };

        let banner = serve_banner(Path::new("gateway.yaml"), &config, "http");

        assert!(banner.contains("config:    gateway.yaml"));
        assert!(banner.contains("listener:  127.0.0.1:8090"));
        assert!(banner.contains("profile:   nano-node/V28.2"));
        assert!(banner.contains("transport=http common=public work=disabled"));
        assert!(!banner.contains("{{"));
        assert!(!banner.contains("Listener started on"));
        assert!(banner.contains("https://nodes.nanswap.com/XNO"));
        assert!(banner.contains("wss://nodes.nanswap.com/ws/"));
        assert!(!banner.contains("do-not-log"));
    }
}
