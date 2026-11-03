#!/usr/bin/env bash
# Multi-core scaling on a dedicated Linux machine: bench/docker/saturate.sh at
# each thread count, servers pinned to the first cores and memtier to the
# rest (the same cores and connections at every thread count).
#
#   docker run -d --name bench --privileged -v ~/tantalum:/tantalum -e ROLE=idle tantalum-bench
#   docker exec -i bench bash /tantalum/bench/aws/scale.sh
#
# Defaults suit 32 cores: servers on up to 8, memtier on cores 8-31
# (24 threads x 10 connections).
set -uo pipefail

CLIENT_CPUS=${CLIENT_CPUS:-8-31}
for t in ${THREAD_COUNTS:-1 2 4 8}; do
  echo "### $t server thread(s) on CPU 0-$((t - 1)); memtier on CPU $CLIENT_CPUS"
  THREADS=$t SERVER_CPUS=0-$((t - 1)) CLIENT_CPUS=$CLIENT_CPUS \
    CLIENT_THREADS=${CLIENT_THREADS:-24} CLIENTS=${CLIENTS:-10} \
    bash "$(dirname "$0")/../docker/saturate.sh"
done
