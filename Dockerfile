# syntax=docker/dockerfile:1

# Multi-arch image for the Nano Gateway JSON-RPC API.
#
# Nothing pins the builder platform, so the file builds identically on an amd64 host and
# on an arm64 one targeting amd64. In the latter case BuildKit runs the build through its
# registered binfmt handlers, which is slower than compiling natively but produces the
# same image and needs no cross toolchain. A cross-compiled variant was tried first:
# rustls pulls in aws-lc-sys, which compiles C, and every translation unit needs the
# *target's* libc headers plus a target-named CC. Getting that right also means teaching
# apt about the foreign architecture, since the base image's sources list only advertises
# its own. The emulation route removes all of that and is a plain multi-stage build.
#
# If the emulation cost becomes a problem, the fast path is a native amd64 builder
# (`docker buildx create --driver docker-container --platform linux/amd64 --use`) rather
# than cross-compiling.

FROM rust:1.89-bookworm AS builder

WORKDIR /src

# Manifests are copied in their own layer so a source-only change does not invalidate
# dependency compilation. --locked keeps Cargo.lock authoritative, so a build cannot
# silently resolve versions other than the ones under test.
COPY Cargo.toml Cargo.lock ./
COPY src ./src
# src/main.rs embeds this file with include_str!, so the crate does not compile without it.
# Copying only src/ fails at the very end of the build with "couldn't read
# src/../assets/startup-banner.txt", after the entire dependency tree has compiled.
COPY assets ./assets
# The gateway embeds the generated browser reference and landing page at compile time.
COPY docs/api-docs ./docs/api-docs
COPY docs/browser-landing.html ./docs/browser-landing.html

# Registry and target directories are cache mounts rather than layers: they are large,
# they change on nearly every build, and committing them to a layer would carry them in
# every intermediate step. sharing=locked serialises concurrent builds of this image,
# which Cargo's own file lock cannot do across processes.
RUN --mount=type=cache,target=/usr/local/cargo/registry,sharing=locked \
    --mount=type=cache,target=/usr/local/cargo/git,sharing=locked \
    --mount=type=cache,target=/src/target,sharing=locked \
    cargo build --release --locked \
 && cp target/release/nano-rpc-gateway /usr/local/bin/nano-rpc-gateway


FROM debian:bookworm-slim AS runtime

# ca-certificates is required, not optional: every node_rpc_url and node_ws_url in a
# deployment points at a public https/wss provider, and a missing trust store fails at
# first request rather than at startup. curl is only here for the health check.
RUN apt-get update \
 && apt-get install -y --no-install-recommends ca-certificates curl \
 && rm -rf /var/lib/apt/lists/* \
 && groupadd --system --gid 10001 gateway \
 && useradd --system --uid 10001 --gid 10001 --no-create-home --shell /usr/sbin/nologin gateway

COPY --from=builder /usr/local/bin/nano-rpc-gateway /usr/local/bin/nano-rpc-gateway

USER gateway
EXPOSE 8090

# /health is a static liveness answer. The start period is generous because the first
# upstream probe happens after the listener binds, and a cold DNS/TLS path to a public
# node can take a moment.
HEALTHCHECK --interval=30s --timeout=5s --start-period=30s --retries=3 \
  CMD curl -fsS http://127.0.0.1:8090/health || exit 1

ENTRYPOINT ["/usr/local/bin/nano-rpc-gateway", "serve", "--config", "/etc/nano-rpc-gateway/gateway.yaml"]
