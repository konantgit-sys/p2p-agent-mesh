# syntax=docker/dockerfile:1.4
# P2P Agent Mesh — Docker
# Multi-stage build, <80MB final image

# === Stage 1: builder ===
FROM python:3.11-slim AS builder

WORKDIR /build
COPY requirements.txt ./
RUN pip install --user --no-cache-dir -r requirements.txt

# === Stage 2: runtime ===
FROM python:3.11-slim AS runtime

WORKDIR /app

# Runtime libs
RUN apt-get update && apt-get install -y --no-install-recommends \
    libssl3 ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Copy pip packages from builder
COPY --from=builder /root/.local /root/.local
ENV PATH=/root/.local/bin:$PATH

# Copy application — все модули
COPY phase0/ phase0/
COPY sdk/ sdk/
COPY relay/ relay/
COPY adapters/ adapters/
COPY coordination/ coordination/
COPY depin/ depin/
COPY pilot/ pilot/
COPY cli.py pyproject.toml ./

# Non-root user
RUN useradd -m -u 1000 mesh && chown -R mesh:mesh /app
USER mesh

# Healthcheck
HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
  CMD python -c "from phase0.transport import P2PTransport; print('ok')" || exit 1

# Default: show help
CMD ["python", "cli.py", "--help"]
