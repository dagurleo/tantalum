# Sourced in the container by each suite's script.

# A copy of the suite's clone, so that runs side by side don't share a tree.
work_on() {
  mkdir -p /work
  cp -r "/fw/$1" "/work/$1"
  cd "/work/$1" || exit 1
}

# A fresh server on the port the suites expect, and nothing kept between runs:
# start_server [port, default 6379] [more arguments for Redis, which Tantalum may not have].
start_server() {
  local port=${1:-6379}
  [ $# -gt 0 ] && shift
  case $SERVER in
    tantalum)
      # shellcheck disable=SC2086
      tantalum --port "$port" $TANTALUM_ARGS >/out/server.log 2>&1 &
      ;;
    redis) redis-server --port "$port" --save '' --appendonly no "$@" >/out/server.log 2>&1 & ;;
    *) echo "no such server: $SERVER"; exit 2 ;;
  esac
  for _ in $(seq 1 100); do
    (exec 3<>/dev/tcp/127.0.0.1/"$port") 2>/dev/null && { echo "server $SERVER is up"; return; }
    sleep 0.1
  done
  echo "server $SERVER did not start:"; cat /out/server.log; exit 1
}
