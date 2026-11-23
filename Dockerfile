FROM rust:1.94-bookworm AS builder
RUN apt-get update && apt-get install -y --no-install-recommends clang libclang-dev cmake && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY Cargo.toml Cargo.lock ./
COPY src ./src
RUN cargo build --release --locked

FROM debian:bookworm-slim
LABEL org.opencontainers.image.source="https://github.com/karnotxyz/committed-data-proxy-service"
RUN useradd --uid 10001 --create-home proxy && mkdir /data && chown proxy:proxy /data
COPY --from=builder /app/target/release/committed-data-proxy-service /usr/local/bin/committed-data-proxy-service
USER 10001:10001
EXPOSE 9945 8080
CMD ["/usr/local/bin/committed-data-proxy-service"]
