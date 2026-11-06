# ioredis's own unit and functional tests, not its cluster's.
setup() {
  cd /fw/ioredis || exit 1
  npm ci --silent 2>&1 | tail -2
}

tests() {
  work_on ioredis
  start_server
  export TS_NODE_TRANSPILE_ONLY=true NODE_ENV=test
  timeout 1800 npx mocha --no-experimental-strip-types "test/helpers/*.ts" "test/unit/**/*.ts" \
    "test/functional/**/*.ts" --exit --reporter xunit --reporter-option output=/out/ioredis.xml \
    >/out/ioredis.txt 2>&1
  echo "mocha: exit $?"
  tail -2 /out/ioredis.txt | cut -c1-200
}
