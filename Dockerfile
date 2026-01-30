# Multi-stage build for SQL Server to PostgreSQL migration tool
# Includes BCP tools for data export

# =============================================================================
# Stage 1: Build Rust application
# =============================================================================
FROM rust:1.86-slim-bookworm AS builder

WORKDIR /build

# Copy source code
COPY Cargo.toml ./
COPY src ./src

# Build release binary without GUI feature (no X11 dependencies needed)
ENV CARGO_NET_GIT_FETCH_WITH_CLI=true
RUN cargo build --release --no-default-features

# =============================================================================
# Stage 2: Get Microsoft SQL Tools (BCP)
# =============================================================================
FROM mcr.microsoft.com/mssql-tools:latest AS mssql-tools

# =============================================================================
# Stage 3: Runtime image with matching glibc
# =============================================================================
FROM rust:1.86-slim-bookworm AS runtime

# Install runtime dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    unixodbc \
    libkrb5-3 \
    && rm -rf /var/lib/apt/lists/*

# Copy BCP tools from mssql-tools image
COPY --from=mssql-tools /opt/mssql-tools /opt/mssql-tools
ENV PATH="/opt/mssql-tools/bin:${PATH}"

# Copy migration binary
COPY --from=builder /build/target/release/sql_migration_engine /usr/local/bin/

# Set working directory
WORKDIR /app

# Default to CLI mode
ENTRYPOINT ["sql_migration_engine", "--cli"]
