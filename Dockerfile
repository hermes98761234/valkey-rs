# syntax=docker/dockerfile:1
# Stage 1: Build the valkey-server binary
FROM rust:1.86-slim AS builder

WORKDIR /app

# Copy workspace manifests first for better layer caching
COPY Cargo.toml Cargo.lock ./
COPY crates/ ./crates/

# Build the server binary in release mode
RUN cargo build --release -p valkey-server

# Stage 2: Runtime image
FROM debian:bookworm-slim AS runtime

ARG VALKEY_PORT=6379
ENV VALKEY_PORT=${VALKEY_PORT}

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /data

COPY --from=builder /app/target/release/valkey-server /usr/local/bin/valkey-server

EXPOSE ${VALKEY_PORT}

ENTRYPOINT ["valkey-server"]
