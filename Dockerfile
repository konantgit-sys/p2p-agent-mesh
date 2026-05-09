# P2P Agent Mesh Dockerfile
FROM python:3.11-slim

# Install IPFS kubo
RUN apt-get update && apt-get install -y wget curl && \
    wget -q https://dist.ipfs.tech/kubo/v0.29.0/kubo_v0.29.0_linux-amd64.tar.gz && \
    tar -xzf kubo_v0.29.0_linux-amd64.tar.gz && \
    cd kubo && bash install.sh && \
    cd .. && rm -rf kubo kubo_v0.29.0_linux-amd64.tar.gz && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

# Python dependencies
COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

# Code
WORKDIR /app
COPY . .

ENV PYTHONPATH=/app

# Entrypoint for multi-container Docker
COPY docker/entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh
ENTRYPOINT ["/entrypoint.sh"]
