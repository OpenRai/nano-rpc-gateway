//! One release identity shared by the executables, contracts, and browser UI.

use std::{fmt, sync::LazyLock};

use serde::Serialize;

/// The only maintained release number comes from the Cargo package manifest.
pub const RELEASE_VERSION: &str = env!("CARGO_PKG_VERSION");
/// Browser-compatible upstream user agent, derived from the same release.
pub const USER_AGENT: &str = concat!(
    "Mozilla/5.0 (compatible; nano-rpc-gateway/",
    env!("CARGO_PKG_VERSION"),
    ")"
);

/// Source identity captured when compiling, never read from runtime environment.
#[derive(Debug, Serialize)]
pub struct BuildInfo {
    pub version: &'static str,
    pub git_describe: &'static str,
    pub git_revision: &'static str,
    /// None means source provenance was unavailable (for example, an archive).
    pub dirty: Option<bool>,
}

/// Build identity of this executable, including development builds.
pub const BUILD_INFO: BuildInfo = BuildInfo {
    version: RELEASE_VERSION,
    git_describe: env!("NANO_GATEWAY_GIT_DESCRIBE"),
    git_revision: env!("NANO_GATEWAY_GIT_REVISION"),
    dirty: match env!("NANO_GATEWAY_GIT_DIRTY").as_bytes() {
        b"true" => Some(true),
        b"false" => Some(false),
        _ => None,
    },
};

impl fmt::Display for BuildInfo {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.version)?;
        if self
            .git_describe
            .strip_prefix('v')
            .unwrap_or(self.git_describe)
            != self.version
        {
            write!(f, " ({})", self.git_describe)?;
        }
        Ok(())
    }
}

/// Canonical display stamp for Clap, startup logs, and the landing page.
pub fn version() -> &'static str {
    static VERSION: LazyLock<String> = LazyLock::new(|| BUILD_INFO.to_string());
    &VERSION
}

/// Machine-readable identity with the same formatted stamp shown to users.
pub fn json() -> serde_json::Value {
    let mut document = serde_json::json!(BUILD_INFO);
    document["display_version"] = serde_json::json!(version());
    document
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn display_preserves_provenance_without_repeating_a_matching_release_tag() {
        for (description, expected) in [
            ("v1.2.3", "1.2.3"),
            ("1.2.3", "1.2.3"),
            ("abc1234-dirty", "1.2.3 (abc1234-dirty)"),
            ("v1.2.2-3-gabc1234", "1.2.3 (v1.2.2-3-gabc1234)"),
            ("unknown", "1.2.3 (unknown)"),
        ] {
            let build = BuildInfo {
                version: "1.2.3",
                git_describe: description,
                git_revision: "unknown",
                dirty: None,
            };
            assert_eq!(build.to_string(), expected);
        }
    }
}
