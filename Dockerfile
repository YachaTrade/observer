# Builder stage
FROM rust:latest as builder

WORKDIR /app/observer

# Copy only files needed for dependency resolution first
COPY Cargo.toml Cargo.lock ./

# Create a dummy main.rs to build dependencies
RUN mkdir -p src && \
    echo "fn main() {}" > src/main.rs && \
    echo "pub fn add(a: i32, b: i32) -> i32 { a + b }" > src/lib.rs && \
    cargo build --release && \
    rm -rf src

# Now copy the real source code and .sqlx directory
COPY . .
COPY .sqlx .sqlx

# Install sqlx-cli for database preparation
RUN cargo install sqlx-cli --no-default-features --features native-tls,postgres

# Build with offline mode
ENV SQLX_OFFLINE=true
RUN cargo clean && cargo build --release

# Runtime stage
FROM debian:bookworm-slim

WORKDIR /app/observer

# Install runtime dependencies
RUN apt-get update && apt-get install -y \
    libpq5 \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Create directories
RUN mkdir -p /run/observer

# Copy the binary and necessary files
COPY --from=builder /app/observer/target/release/observer /run/observer/
COPY --from=builder /app/observer/migrations /app/migrations

# Set environment variables
ENV RUST_LOG=info

CMD ["/run/observer/observer"]