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
COPY Cargo.toml Cargo.lock build.rs ./
COPY src ./src
# src/main.rs embeds this file with include_str!, so the crate does not compile without it.
# Copying only src/ fails at the very end of the build with "couldn't read
# src/../assets/startup-banner.txt", after the entire dependency tree has compiled.
COPY assets ./assets
# The gateway embeds the generated browser reference and landing page at compile time.
COPY docs/api-docs ./docs/api-docs
COPY docs/browser-landing.html ./docs/browser-landing.html

# The supported wrappers resolve these before .git is excluded from the context.
# ARGs participate in the Cargo RUN cache key, so a new source stamp invalidates it.
ARG NANO_GATEWAY_VERSION
ARG NANO_GATEWAY_GIT_DESCRIBE
ARG NANO_GATEWAY_GIT_REVISION
ARG NANO_GATEWAY_GIT_DIRTY
ENV NANO_GATEWAY_VERSION=$NANO_GATEWAY_VERSION \
    NANO_GATEWAY_GIT_DESCRIBE=$NANO_GATEWAY_GIT_DESCRIBE \
    NANO_GATEWAY_GIT_REVISION=$NANO_GATEWAY_GIT_REVISION \
    NANO_GATEWAY_GIT_DIRTY=$NANO_GATEWAY_GIT_DIRTY
RUN test -n "$NANO_GATEWAY_VERSION" && test -n "$NANO_GATEWAY_GIT_REVISION" \
    || { echo 'Build metadata required: use scripts/build-image.sh or scripts/with-build-metadata.sh' >&2; exit 1; }

# Registry and target directories are cache mounts rather than layers: they are large,
# they change on nearly every build, and committing them to a layer would carry them in
# every intermediate step. sharing=locked serialises concurrent builds of this image,
# which Cargo's own file lock cannot do across processes.
# Discard this package's outputs before compiling: a cached artifact from another
# context can be newer than COPY's source mtimes. Keep dependency artifacts cached.
RUN --mount=type=cache,target=/usr/local/cargo/registry,sharing=locked \
    --mount=type=cache,target=/usr/local/cargo/git,sharing=locked \
    --mount=type=cache,target=/src/target,sharing=locked \
    cargo clean --release --package nano-rpc-gateway \
 && cargo build --release --locked \
 && cp target/release/nano-rpc-gateway /usr/local/bin/nano-rpc-gateway


# Optional test target; the default runtime below never contains the fault relay.
FROM builder AS fault-relay-builder
COPY examples ./examples
RUN --mount=type=cache,target=/usr/local/cargo/registry,sharing=locked \
    --mount=type=cache,target=/usr/local/cargo/git,sharing=locked \
    --mount=type=cache,target=/src/target,sharing=locked \
    cargo clean --release --package nano-rpc-gateway \
 && cargo build --release --locked --example ws_fault_relay \
 && cp target/release/examples/ws_fault_relay /usr/local/bin/ws_fault_relay

# Shared package metadata for both deliverables. No release literal lives here.
FROM debian:trixie-slim AS runtime-base
# Public upstream https/wss needs a trust store; curl supports the health check.
RUN apt-get update \
 && apt-get upgrade -y --no-install-recommends \
 && apt-get install -y --no-install-recommends ca-certificates curl \
 && rm -rf /var/lib/apt/lists/* \
 && groupadd --system --gid 10001 gateway \
 && useradd --system --uid 10001 --gid 10001 --no-create-home --shell /usr/sbin/nologin gateway
# Apply source metadata after the OS layer so a new commit reuses installed packages.
ARG NANO_GATEWAY_GIT_DESCRIBE
ARG NANO_GATEWAY_VERSION
ARG NANO_GATEWAY_GIT_REVISION
ARG NANO_GATEWAY_GIT_DIRTY
ARG NANO_GATEWAY_SOURCE
ARG NANO_GATEWAY_LICENSE
RUN test -n "$NANO_GATEWAY_SOURCE" && test -n "$NANO_GATEWAY_LICENSE" \
    || { echo 'Image source/license metadata required: use scripts/build-image.sh' >&2; exit 1; }
LABEL org.opencontainers.image.version=$NANO_GATEWAY_VERSION \
      org.opencontainers.image.revision=$NANO_GATEWAY_GIT_REVISION \
      org.opencontainers.image.source=$NANO_GATEWAY_SOURCE \
      org.opencontainers.image.licenses=$NANO_GATEWAY_LICENSE \
      com.github.openrai.nano-rpc-gateway.source.dirty=$NANO_GATEWAY_GIT_DIRTY \
      com.github.openrai.nano-rpc-gateway.source.description=$NANO_GATEWAY_GIT_DESCRIBE

USER gateway

FROM runtime-base AS ws-fault-relay
COPY --from=fault-relay-builder /usr/local/bin/ws_fault_relay /usr/local/bin/ws_fault_relay
ENTRYPOINT ["/usr/local/bin/ws_fault_relay"]

FROM runtime-base AS runtime

COPY --from=builder /usr/local/bin/nano-rpc-gateway /usr/local/bin/nano-rpc-gateway

USER gateway
EXPOSE 8090

# /health is a static liveness answer. The start period is generous because the first
# upstream probe happens after the listener binds, and a cold DNS/TLS path to a public
# node can take a moment.
HEALTHCHECK --interval=30s --timeout=5s --start-period=30s --retries=3 \
  CMD curl -fsS http://127.0.0.1:8090/health || exit 1

ENTRYPOINT ["/usr/local/bin/nano-rpc-gateway", "serve", "--config", "/etc/nano-rpc-gateway/gateway.yaml"]
