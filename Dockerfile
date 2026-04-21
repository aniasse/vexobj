# ── Build stage ──────────────────────────────────────────
FROM rust:1.88-bookworm AS builder

WORKDIR /build
COPY . .

RUN cargo build --release --bin vexobj --bin vexobjctl

# ── Runtime stage ────────────────────────────────────────
FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

RUN useradd -r -s /bin/false vexobj

COPY --from=builder /build/target/release/vexobj /usr/local/bin/vexobj
COPY --from=builder /build/target/release/vexobjctl /usr/local/bin/vexobjctl

RUN mkdir -p /data && chown vexobj:vexobj /data

USER vexobj

ENV VEXOBJ_CONFIG=""

EXPOSE 8000

VOLUME ["/data"]

ENTRYPOINT ["vexobj"]
