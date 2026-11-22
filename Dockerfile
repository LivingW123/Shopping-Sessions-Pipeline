# Shopping Sessions: native Rust pipeline (no engine).
# Build stage: compile a release binary for x86-64-v3 (AVX2/BMI2; the scorers
# are Cascade Lake or newer). Runtime stage: slim Debian with just the binary.
FROM rust:1.85-slim-bookworm AS build
WORKDIR /src
COPY Cargo.toml Cargo.lock* ./
COPY src/ src/
ENV RUSTFLAGS="-C target-cpu=x86-64-v3"
RUN cargo build --release --locked || cargo build --release

FROM debian:bookworm-slim
RUN useradd --uid 1000 --no-create-home --home-dir /scratch app
COPY --from=build /src/target/release/pipeline /app/pipeline
WORKDIR /app
CMD ["/app/pipeline"]
