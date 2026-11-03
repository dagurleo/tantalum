# Tantalum as a server: a small image with just the binary. (The Dockerfile at
# the root is the benchmark image, with Redis, Valkey and the rest.)
#
#   docker build -f deploy/Dockerfile -t tantalum .
#   docker run -p 6379:6379 -e TANTALUM_REQUIREPASS=... tantalum
#
# On Railway, set RAILWAY_DOCKERFILE_PATH=deploy/Dockerfile. Options such as
# --maxmemory go in the start command.

FROM rust:1-bookworm AS build
WORKDIR /src
COPY Cargo.toml Cargo.lock ./
COPY src src
RUN cargo build --release --bin tantalum

FROM debian:bookworm-slim
COPY --from=build /src/target/release/tantalum /usr/local/bin/tantalum
EXPOSE 6379
# `::` accepts IPv4 and IPv6, which Railway's private network uses.
CMD ["tantalum", "--bind", "::", "--port", "6379"]
