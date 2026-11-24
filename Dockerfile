FROM rust:1.94-bookworm AS builder
WORKDIR /app
COPY Cargo.toml Cargo.lock ./
COPY src ./src
RUN cargo build --release --locked

FROM debian:bookworm-slim
LABEL org.opencontainers.image.source="https://github.com/karnotxyz/committed-data-proxy-service"
COPY --from=builder /app/target/release/committed-data-proxy-service /usr/local/bin/committed-data-proxy-service
USER 10001:10001
EXPOSE 9945 8080
CMD ["/usr/local/bin/committed-data-proxy-service"]
