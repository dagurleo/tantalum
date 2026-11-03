#!/usr/bin/env bash
# Runs identical redis-benchmark scenarios against two servers and prints CSV.
#
#   bench/compare.sh [tantalum-port] [redis-port]
#
# Start both servers first, with persistence off for Redis:
#   target/release/tantalum --port 6380 --threads 4
#   redis-server --port 6381 --save '' --appendonly no
#
# Results on a single machine are only indicative: the benchmark client
# competes with the server for cores. Use separate Linux hosts for real numbers.
set -euo pipefail

FERRO_PORT=${1:-6380}
REDIS_PORT=${2:-6381}
CLIENT_THREADS=${CLIENT_THREADS:-4}
KEYSPACE=${KEYSPACE:-100000}
VALUE_SIZE=${VALUE_SIZE:-32}

# name, connections, pipeline depth, requests
SCENARIOS=(
  "c50-p1   50  1  1000000"
  "c50-p16  50  16 5000000"
  "c500-p1  500 1  1000000"
)

echo "scenario,server,test,rps,avg_ms,p50_ms,p99_ms,max_ms"
for s in "${SCENARIOS[@]}"; do
  read -r name conns pipe requests <<<"$s"
  for target in "tantalum:$FERRO_PORT" "redis:$REDIS_PORT"; do
    server=${target%%:*}
    port=${target##*:}
    redis-benchmark -p "$port" -t set,get -n "$requests" -r "$KEYSPACE" -d "$VALUE_SIZE" \
      -c "$conns" -P "$pipe" --threads "$CLIENT_THREADS" --csv 2>/dev/null |
      tail -n +2 | tr -d '"' |
      awk -F, -v n="$name" -v s="$server" '{ printf "%s,%s,%s,%s,%s,%s,%s,%s\n", n, s, $1, $2, $3, $5, $7, $8 }'
  done
done
