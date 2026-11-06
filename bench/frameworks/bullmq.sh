# BullMQ's own tests (vitest), with the server at its default localhost:6379.
setup() {
  cd /fw/bullmq || exit 1
  corepack enable >/dev/null 2>&1
  export COREPACK_ENABLE_DOWNLOAD_PROMPT=0
  yarn install --frozen-lockfile --silent 2>&1 | tail -3
  npm run --silent pretest 2>&1 | tail -3
  # Its child-process tests run the compiled code.
  npx tsc 2>&1 | tail -3
  npx tsc -p tsconfig-cjs.json 2>&1 | tail -3
}

tests() {
  work_on bullmq
  start_server
  timeout 2700 npx vitest run --no-file-parallelism --reporter=junit --outputFile=/out/bullmq.xml \
    >/out/bullmq.txt 2>&1
  echo "vitest: exit $?"
  tail -3 /out/bullmq.txt | cut -c1-200
}
