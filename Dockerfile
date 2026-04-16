# ── Build stage ──────────────────────────────────────────
FROM rust:1.82-bookworm AS builder

WORKDIR /build
COPY . .

RUN cargo build --release --bin vaultfs

# ── Runtime stage ────────────────────────────────────────
FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

RUN useradd -r -s /bin/false vaultfs

COPY --from=builder /build/target/release/vaultfs /usr/local/bin/vaultfs

RUN mkdir -p /data && chown vaultfs:vaultfs /data

USER vaultfs

ENV VAULTFS_CONFIG=""

EXPOSE 8000

VOLUME ["/data"]

ENTRYPOINT ["vaultfs"]
