use std::{env, error::Error, path::Path, process::Command};

fn git(args: &[&str]) -> Option<String> {
    let output = Command::new("git").args(args).output().ok()?;
    output.status.success().then(|| {
        String::from_utf8_lossy(&output.stdout)
            .trim_end()
            .to_owned()
    })
}

fn watch_git_inputs() {
    // Resolve Git paths instead of assuming .git is a directory: linked worktrees
    // keep HEAD/index locally and branch/tag references in the common Git dir.
    for name in ["HEAD", "index", "packed-refs", "refs/tags"] {
        if let Some(path) = git(&["rev-parse", "--git-path", name]) {
            println!("cargo:rerun-if-changed={path}");
        }
    }
    if let Some(reference) = git(&["symbolic-ref", "-q", "HEAD"])
        && let Some(path) = git(&["rev-parse", "--git-path", &reference])
    {
        println!("cargo:rerun-if-changed={path}");
    }
    if let Some(files) = git(&["ls-files", "-z"]) {
        for file in files.split('\0').filter(|file| !file.is_empty()) {
            println!("cargo:rerun-if-changed={file}");
        }
    }
}

fn main() -> Result<(), Box<dyn Error>> {
    let names = [
        "NANO_GATEWAY_GIT_DESCRIBE",
        "NANO_GATEWAY_GIT_REVISION",
        "NANO_GATEWAY_GIT_DIRTY",
    ];
    for name in names.into_iter().chain(["NANO_GATEWAY_VERSION"]) {
        println!("cargo:rerun-if-env-changed={name}");
    }
    println!("cargo:rerun-if-changed=build.rs");
    println!("cargo:rerun-if-changed=Cargo.toml");
    if let Ok(expected) = env::var("NANO_GATEWAY_VERSION")
        && expected != env::var("CARGO_PKG_VERSION")?
    {
        return Err("NANO_GATEWAY_VERSION must match package.version in Cargo.toml".into());
    }
    let supplied = names.map(|name| env::var(name).ok());
    let values = if supplied.iter().any(Option::is_some) {
        let [Some(description), Some(revision), Some(dirty)] = supplied else {
            return Err("supply all three NANO_GATEWAY_GIT_* metadata fields together".into());
        };
        if !matches!(revision.len(), 40 | 64)
            || !revision.bytes().all(|byte| byte.is_ascii_hexdigit())
            || !matches!(dirty.as_str(), "true" | "false")
            || description.is_empty()
            || description.chars().any(char::is_control)
            || description.ends_with("-dirty") != (dirty == "true")
        {
            return Err("invalid or inconsistent NANO_GATEWAY_GIT_* build metadata".into());
        }
        [description, revision, dirty]
    } else {
        // Do not accidentally identify a source archive using a parent repo.
        let root = git(&["rev-parse", "--show-toplevel"]);
        let in_checkout = root
            .as_deref()
            .and_then(|root| Path::new(root).canonicalize().ok())
            == Some(Path::new(&env::var("CARGO_MANIFEST_DIR")?).canonicalize()?);
        if in_checkout {
            watch_git_inputs();
        } else {
            println!("cargo:rerun-if-changed=.git");
        }
        match (
            in_checkout
                .then(|| git(&["describe", "--tags", "--always", "--dirty"]))
                .flatten(),
            in_checkout.then(|| git(&["rev-parse", "HEAD"])).flatten(),
        ) {
            (Some(description), Some(revision)) => {
                let dirty = description.ends_with("-dirty").to_string();
                [description, revision, dirty]
            }
            _ => ["unknown".into(), "unknown".into(), "unknown".into()],
        }
    };
    for (name, value) in names.into_iter().zip(values) {
        println!("cargo:rustc-env={name}={value}");
    }
    Ok(())
}
