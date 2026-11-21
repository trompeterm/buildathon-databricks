# Shopping Sessions: native Rust pipeline.
# Build stage compiles a static-ish release binary tuned for the scorer's x86-64 Xeons (AVX2+).
FROM rust:1.88-bookworm AS build
WORKDIR /src
ENV CARGO_NET_RETRY=5 \
    RUSTFLAGS="-C target-cpu=x86-64-v3"
COPY Cargo.toml Cargo.lock ./
# Pre-fetch and build dependencies in their own layer.
RUN mkdir src && echo 'fn main() {}' > src/main.rs && cargo build --release --locked && rm -rf src
COPY src ./src
RUN touch src/main.rs && cargo build --release --locked && strip target/release/pipeline

FROM debian:bookworm-slim
RUN useradd --uid 1000 --no-create-home --home-dir /scratch app
COPY --from=build /src/target/release/pipeline /app/pipeline
ENV DATA_DIR=/data OUTPUT_DIR=/output SCRATCH_DIR=/scratch HOME=/scratch
WORKDIR /app
USER 1000
CMD ["/app/pipeline"]
