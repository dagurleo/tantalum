# redis-rb's own tests, of one server and of Redis::Distributed over it, at its port 6381.
export BUNDLE_PATH=/cache/redis-rb BUNDLE_JOBS=4 BUNDLE_SILENCE_ROOT_WARNING=1

setup() {
  work_on redis-rb
  bundle install --quiet
  cp Gemfile.lock /cache/redis-rb.lock 2>/dev/null
}

tests() {
  work_on redis-rb
  cp /cache/redis-rb.lock Gemfile.lock 2>/dev/null
  mkdir -p tmp
  start_server 6381 --unixsocket "$PWD/tmp/redis.sock"
  # It won't start without a Unix socket, which Tantalum doesn't have: one in front of its port.
  if [ "$SERVER" = tantalum ]; then
    socat "UNIX-LISTEN:$PWD/tmp/redis.sock,fork" TCP:127.0.0.1:6381 &
    sleep 0.5
  fi
  timeout 900 bundle exec rake test:redis TESTOPTS=-v >/out/redis.txt 2>&1
  echo "redis: exit $?"
  tail -1 /out/redis.txt | cut -c1-200
  timeout 900 bundle exec rake test:distributed TESTOPTS=-v >/out/distributed.txt 2>&1
  echo "distributed: exit $?"
  tail -1 /out/distributed.txt | cut -c1-200
}
