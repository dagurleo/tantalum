# phpredis's own tests, of its Redis class, at the version the image's extension is.
setup() {
  local version
  version=$(php -r 'echo phpversion("redis");')
  [ -d /fw/phpredis ] || git -c advice.detachedHead=false clone -q --depth 1 --branch "$version" \
    https://github.com/phpredis/phpredis /fw/phpredis
  echo "phpredis $version"
}

tests() {
  work_on phpredis
  start_server
  # One at a time: a test that gets something it can't read ends the whole run with a fatal error.
  grep -oE 'function test[A-Za-z0-9_]+' tests/RedisTest.php | cut -d' ' -f2 | while read -r name; do
    timeout 120 php tests/TestRedis.php --class Redis --host 127.0.0.1 --port 6379 --nocolors --test "$name" \
      >>/out/phpredis.log 2>&1
  done
  # Its lines, as compare.py reads minitest's.
  sed -nE 's/^[[:space:]]*(test[A-Za-z0-9_]+)[^[]*\[(PASSED|FAILED|SKIPPED)\].*/phpredis#\1 \2/p' /out/phpredis.log |
    sed -E 's/ PASSED$/ = 0.00 s = ./; s/ FAILED$/ = 0.00 s = F/; s/ SKIPPED$/ = 0.00 s = S/' >/out/phpredis.txt
}
