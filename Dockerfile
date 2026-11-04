# Benchmark image: tantalum, Redis 8, Valkey, memcached, Dragonfly and
# memtier_benchmark together. See bench/railway/run.sh for the roles.

FROM rust:1-bookworm AS tantalum
WORKDIR /src
COPY Cargo.toml Cargo.lock build.rs ./
COPY vendor vendor
COPY src src
RUN cargo build --release --bin tantalum

# Built on the final base image so its shared libraries match.
FROM redis:8 AS memtier
RUN apt-get update \
 && apt-get install -y --no-install-recommends \
      build-essential autoconf automake pkg-config libevent-dev zlib1g-dev git ca-certificates \
 && git clone --depth 1 https://github.com/RedisLabs/memtier_benchmark /memtier \
 && cd /memtier \
 && autoreconf -ivf \
 && ./configure --disable-tls \
 && make -j"$(nproc)"

FROM valkey/valkey:latest AS valkey

# Dragonfly's release binary is built to run on most distributions.
FROM debian:bookworm-slim AS dragonfly
ARG TARGETARCH
RUN apt-get update \
 && apt-get install -y --no-install-recommends curl ca-certificates \
 && mkdir /df \
 && case "$TARGETARCH" in arm64) arch=aarch64 ;; *) arch=x86_64 ;; esac \
 && curl -fsSL "https://github.com/dragonflydb/dragonfly/releases/latest/download/dragonfly-$arch.tar.gz" \
    | tar xz -C /df \
 && find /df -type f -name 'dragonfly*' -exec mv {} /usr/local/bin/dragonfly ';' \
 && test -x /usr/local/bin/dragonfly

FROM redis:8
RUN apt-get update \
 && apt-get install -y --no-install-recommends libevent-dev procps memcached \
 && rm -rf /var/lib/apt/lists/*
COPY --from=valkey /usr/local/bin/valkey-server /usr/local/bin/
COPY --from=dragonfly /usr/local/bin/dragonfly /usr/local/bin/
# Fail the build now, not mid-benchmark, if a copied binary doesn't run here.
RUN valkey-server --version && redis-server --version && memcached -V && dragonfly --version
COPY --from=memtier /memtier/memtier_benchmark /usr/local/bin/
COPY --from=tantalum /src/target/release/tantalum /usr/local/bin/
COPY bench/railway/run.sh bench/railway/suite.sh /usr/local/bin/
ENTRYPOINT ["/usr/local/bin/run.sh"]
CMD []
