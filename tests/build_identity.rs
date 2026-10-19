use std::{fs, path::Path, process::Command};

use axum::{body::Body, http::Request};
use http_body_util::BodyExt;
use nano_rpc_gateway::{app, build_info, AppState, Config};
use serde_json::Value;
use tower::ServiceExt;

#[test]
fn executable_and_contracts_share_the_release_identity() {
    let binary = env!("CARGO_BIN_EXE_nano-rpc-gateway");
    let output = Command::new(binary)
        .arg("build-info")
        .env(
            "NANO_GATEWAY_GIT_DESCRIBE",
            "runtime-env-must-not-change-the-stamp",
        )
        .output()
        .expect("build-info");
    assert!(output.status.success());
    let identity: Value = serde_json::from_slice(&output.stdout).expect("JSON identity");
    assert_eq!(identity, build_info::json());
    let output = Command::new(binary)
        .arg("--version")
        .output()
        .expect("version");
    assert_eq!(
        String::from_utf8_lossy(&output.stdout).trim(),
        format!("nano-rpc-gateway {}", build_info::version())
    );
    let openrpc = nano_rpc_gateway::openrpc_document(
        "nano-node/V28.2",
        false,
        true,
        "https://gateway.test/rpc",
    );
    let asyncapi =
        nano_rpc_gateway::asyncapi_document("nano-node/V28.2", "https://gateway.test/rpc");
    assert_eq!(openrpc["info"]["version"], build_info::RELEASE_VERSION);
    assert_eq!(asyncapi["info"]["version"], build_info::RELEASE_VERSION);
    assert!(openrpc.get("git_revision").is_none());
}

#[tokio::test]
async fn landing_contains_the_embedded_stamp_without_a_client_request() {
    let config = Config {
        enable_browser_ui: true,
        ..Config::default()
    };
    let response = app(AppState::new(config).expect("state"))
        .oneshot(
            Request::builder()
                .uri("/api-landing-page")
                .body(Body::empty())
                .expect("request"),
        )
        .await
        .expect("response");
    assert_eq!(response.status(), axum::http::StatusCode::OK);
    let bytes = response
        .into_body()
        .collect()
        .await
        .expect("HTML")
        .to_bytes();
    let html = String::from_utf8(bytes.to_vec()).expect("UTF-8");
    assert!(html.contains(&format!(
        "<p class=\"version\">{}</p>",
        build_info::version()
    )));
    assert!(!html.contains("{{version}}"));
    assert!(!html.contains("<script"));
}

fn run(command: &mut Command) -> String {
    let output = command.output().expect("run fixture command");
    assert!(
        output.status.success(),
        "{command:?}: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8(output.stdout)
        .expect("UTF-8")
        .trim()
        .to_owned()
}

fn git(root: &Path, args: &[&str]) -> String {
    run(Command::new("git").arg("-C").arg(root).args(args))
}

fn cargo(root: &Path, overrides: &[(&str, &str)]) -> std::process::Output {
    let mut command = Command::new(env!("CARGO"));
    command
        .env("CARGO_TARGET_DIR", root.join("target"))
        .current_dir(root)
        .args(["run", "--quiet", "--offline"]);
    for name in [
        "NANO_GATEWAY_VERSION",
        "NANO_GATEWAY_GIT_DESCRIBE",
        "NANO_GATEWAY_GIT_REVISION",
        "NANO_GATEWAY_GIT_DIRTY",
    ] {
        command.env_remove(name);
    }
    command
        .envs(overrides.iter().copied())
        .output()
        .expect("cargo fixture")
}

fn identity(root: &Path) -> String {
    let output = cargo(root, &[]);
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8(output.stdout)
        .expect("identity")
        .trim()
        .to_owned()
}

#[test]
fn incremental_builds_refresh_git_identity_and_validate_oci_overrides() {
    // A dependency-free disposable crate exercises the actual build script and
    // Cargo invalidation without touching this checkout or compiling the gateway again.
    let temp = std::env::temp_dir().join(format!("gateway-build-identity-{}", std::process::id()));
    fs::create_dir(&temp).expect("isolated fixture");
    struct Cleanup(std::path::PathBuf);
    impl Drop for Cleanup {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }
    let _cleanup = Cleanup(temp.clone());
    let root = temp.join("repo");
    fs::create_dir_all(root.join("src")).expect("src");
    fs::write(
        root.join("Cargo.toml"),
        "[package]\nname=\"identity-fixture\"\nversion=\"9.8.7\"\nedition=\"2024\"\n",
    )
    .expect("manifest");
    fs::write(root.join("build.rs"), include_str!("../build.rs")).expect("build script");
    fs::write(root.join("src/main.rs"), r#"fn main() { println!("{}|{}|{}", env!("NANO_GATEWAY_GIT_DESCRIBE"), env!("NANO_GATEWAY_GIT_REVISION"), env!("NANO_GATEWAY_GIT_DIRTY")); }"#).expect("main");
    fs::write(root.join(".gitignore"), "/target/\n").expect("ignore");
    fs::write(root.join("tracked"), "one\n").expect("tracked file");
    assert_eq!(identity(&root), "unknown|unknown|unknown");
    git(&root, &["init", "-q"]);
    git(&root, &["config", "user.name", "Build Identity Test"]);
    git(
        &root,
        &["config", "user.email", "build-identity@example.invalid"],
    );
    git(&root, &["config", "core.hooksPath", "/dev/null"]);
    git(&root, &["config", "tag.gpgSign", "false"]);
    git(&root, &["add", "."]);
    git(&root, &["commit", "-q", "--no-gpg-sign", "-m", "initial"]);
    let revision = git(&root, &["rev-parse", "HEAD"]);
    assert_eq!(
        identity(&root),
        format!(
            "{}|{revision}|false",
            git(&root, &["describe", "--tags", "--always", "--dirty"])
        )
    );
    git(&root, &["tag", "v9.8.7"]);
    assert_eq!(identity(&root), format!("v9.8.7|{revision}|false"));
    fs::write(root.join("tracked"), "two\n").expect("edit tracked file");
    assert_eq!(identity(&root), format!("v9.8.7-dirty|{revision}|true"));
    git(&root, &["add", "tracked"]);
    assert_eq!(identity(&root), format!("v9.8.7-dirty|{revision}|true"));
    git(&root, &["commit", "-q", "--no-gpg-sign", "-m", "second"]);
    let description = git(&root, &["describe", "--tags", "--always", "--dirty"]);
    let revision = git(&root, &["rev-parse", "HEAD"]);
    assert_eq!(identity(&root), format!("{description}|{revision}|false"));
    git(&root, &["tag", "-f", "v9.8.7"]);
    assert_eq!(identity(&root), format!("v9.8.7|{revision}|false"));
    let worktree = temp.join("worktree");
    git(
        &root,
        &[
            "worktree",
            "add",
            "--detach",
            worktree.to_str().expect("path"),
            "HEAD",
        ],
    );
    assert_eq!(identity(&worktree), format!("v9.8.7|{revision}|false"));
    let overrides = [
        ("NANO_GATEWAY_VERSION", "9.8.7"),
        ("NANO_GATEWAY_GIT_DESCRIBE", "provided-dirty"),
        ("NANO_GATEWAY_GIT_REVISION", revision.as_str()),
        ("NANO_GATEWAY_GIT_DIRTY", "true"),
    ];
    let output = cargo(&root, &overrides);
    assert!(output.status.success());
    assert_eq!(
        String::from_utf8_lossy(&output.stdout).trim(),
        format!("provided-dirty|{revision}|true")
    );
    assert_eq!(identity(&root), format!("v9.8.7|{revision}|false"));
    assert!(!cargo(&root, &[("NANO_GATEWAY_VERSION", "wrong")])
        .status
        .success());
    assert!(!cargo(&root, &[("NANO_GATEWAY_GIT_DESCRIBE", "partial")])
        .status
        .success());
    let mut inconsistent = overrides;
    inconsistent[3].1 = "false";
    assert!(!cargo(&root, &inconsistent).status.success());
}
