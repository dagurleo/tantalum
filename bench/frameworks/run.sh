#!/usr/bin/env bash
# Runs a framework's own test suite against one server, in a container of its
# own, so the server is on localhost:6379 as the suite expects and two runs can
# go side by side. Results land in $OUT/SUITE-SERVER; compare two with compare.py.
#
#   run.sh setup SUITE            once: builds the image, installs what it needs
#   run.sh SUITE SERVER           SERVER is tantalum or redis
#
# SUITE: sidekiq, rails, celery, kombu, laravel, bullmq, or a client library: redis-py,
# ioredis, node-redis, redis-rb, predis, phpredis. Needs git clones of each under
# $FW (fetch.sh makes them), and a Tantalum and a redis-server binary.
set -uo pipefail

here=$(cd "$(dirname "$0")" && pwd)
FW=${FW:-/root/fw}
OUT=${OUT:-/root/fw-out}
TANTALUM=${TANTALUM:-/root/tantalum-fw}
REDIS=${REDIS:-/root/redis-724}

image_of() {
  case $1 in
    sidekiq | rails | redis-rb) echo fw-ruby ;;
    celery | kombu | redis-py) echo fw-python ;;
    laravel | predis | phpredis) echo fw-php ;;
    bullmq | ioredis | node-redis) echo fw-node ;;
    *) echo "no such suite: $1" >&2; exit 2 ;;
  esac
}

# --privileged so that io_uring is as available as on a real machine.
in_container() {
  local suite=$1 name=$2 out=$3 step=$4 server=$5
  docker run --rm --name "$name" --privileged \
    -v "$FW:/fw" -v "$out:/out" -v "$here:/scripts:ro" \
    -v "$TANTALUM:/usr/local/bin/tantalum:ro" -v "$REDIS:/usr/local/bin/redis-server:ro" \
    -v "$(image_of "$suite")-cache:/cache" \
    -e "SERVER=$server" -e "SUITE=$suite" -e "TANTALUM_ARGS=${TANTALUM_ARGS:-}" -e "ONLY=${ONLY:-}" -e "DATABASE=${DATABASE:-}" \
    "$(image_of "$suite")" bash -c ". /scripts/common.sh; . /scripts/$suite.sh; $step"
}

if [ "$1" = setup ]; then
  suite=$2
  image=$(image_of "$suite")
  docker image inspect "$image" >/dev/null 2>&1 ||
    docker build -q -t "$image" -f "$here/$image.Dockerfile" "$here" || exit 1
  out=$OUT/setup-$suite
  mkdir -p "$out"
  in_container "$suite" "fw-setup-$suite" "$out" setup none
  exit $?
fi

suite=$1 server=$2
out=$OUT/$suite-$server${LABEL:+-$LABEL}
rm -rf "$out"
mkdir -p "$out"
started=$(date +%s)
in_container "$suite" "fw-$suite-$server${LABEL:+-$LABEL}" "$out" tests "$server" >"$out/log.txt" 2>&1
echo "$suite on $server: exit $?, $(($(date +%s) - started)) s, in $out"
