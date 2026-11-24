#!/usr/bin/env bash
# Starts a built server and checks it answers PING, SET and GET, for the release
# workflow: tools/smoke-test.sh path/to/tantalum [port]. No redis-cli needed.
set -euo pipefail
bin=$1
port=${2:-6399}

"$bin" --help >/dev/null
"$bin" --port "$port" &
pid=$!
trap 'kill $pid 2>/dev/null || true' EXIT

for _ in $(seq 100); do
  if (exec 3<>"/dev/tcp/127.0.0.1/$port") 2>/dev/null; then break; fi
  sleep 0.1
done

exec 3<>"/dev/tcp/127.0.0.1/$port"
printf 'PING\r\nSET smoke yes\r\nGET smoke\r\n' >&3
expect() {
  local line
  IFS= read -r -t 5 line <&3
  if [[ "$line" != "$1"$'\r' ]]; then
    echo "expected $1, got: $line" >&2
    exit 1
  fi
}
expect '+PONG'
expect '+OK'
expect '$3'
expect 'yes'
echo "smoke test ok: $("$bin" --help | head -1)"
