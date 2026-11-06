# node-redis's client tests: every command has its own. They start each server in
# Docker; node-redis-shim.py starts SERVER itself instead, as they ask, at Redis 7.2.
setup() {
  cd /fw/node-redis || exit 1
  npm ci --silent 2>&1 | tail -2
}

tests() {
  work_on node-redis
  python3 /scripts/node-redis-shim.py packages/test-utils/lib/dockers.ts
  npm run build --silent 2>&1 | tail -2
  cd packages/client || exit 1
  timeout 2400 npx mocha -r tsx --reporter xunit --reporter-option output=/out/node-redis.xml \
    './lib/**/*.spec.ts' --redis-version=7.2 --exit >/out/node-redis.txt 2>&1
  echo "mocha: exit $?"
  tail -2 /out/node-redis.txt | cut -c1-200
}
