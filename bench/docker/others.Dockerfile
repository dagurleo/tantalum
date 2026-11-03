# The bench image (../../Dockerfile) plus Microsoft Garnet and KeyDB, for
# saturate.sh's SERVERS=garnet and keydb. x86_64 only (KeyDB's last release
# image). Build the bench image first, then from the repo root:
#
#   curl -fsSL https://github.com/microsoft/garnet/releases/download/v2.2.5/linux-x64-based.tar.xz \
#     | (mkdir -p bench/docker/garnet && tar xJ -C bench/docker/garnet)
#   docker build -f bench/docker/others.Dockerfile -t tantalum-bench-others bench/docker

FROM eqalpha/keydb:latest AS keydb
FROM mcr.microsoft.com/dotnet/runtime:10.0 AS dotnet

FROM tantalum-bench
# KeyDB 6.3.4 is built for Ubuntu 20.04: it runs with that system's loader
# and libraries, exec'd so the server keeps the wrapper's pid.
COPY --from=keydb /lib/x86_64-linux-gnu /opt/keydb/lib
COPY --from=keydb /usr/local/bin/keydb-server /opt/keydb/
RUN printf '#!/bin/sh\nexec /opt/keydb/lib/ld-linux-x86-64.so.2 --library-path /opt/keydb/lib /opt/keydb/keydb-server "$@"\n' \
      > /usr/local/bin/keydb-server \
 && chmod +x /usr/local/bin/keydb-server
COPY --from=dotnet /usr/share/dotnet /usr/share/dotnet
COPY garnet/net10.0 /opt/garnet
ENV DOTNET_ROOT=/usr/share/dotnet DOTNET_SYSTEM_GLOBALIZATION_INVARIANT=1
RUN keydb-server --version && /opt/garnet/GarnetServer --help >/dev/null
