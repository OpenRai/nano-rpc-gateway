FROM rust:1.82-bookworm AS build
WORKDIR /src
COPY Cargo.toml Cargo.lock ./
COPY src ./src
RUN cargo build --release --locked
FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates && rm -rf /var/lib/apt/lists/*
RUN useradd --system --uid 10001 --no-create-home --shell /usr/sbin/nologin gateway
COPY --from=build /src/target/release/nano-rpc-gateway /usr/local/bin/nano-rpc-gateway
USER gateway
EXPOSE 8090
ENTRYPOINT ["/usr/local/bin/nano-rpc-gateway", "serve", "--config", "/etc/nano-rpc-gateway/gateway.yaml"]
