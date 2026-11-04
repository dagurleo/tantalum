// BullMQ's two hot paths against one server, for bench/bullmq/run.sh:
//
//   node jobs.mjs add  PORT JOBS      add JOBS jobs with addBulk, 500 at a time
//   node jobs.mjs work PORT CONCURRENCY   process jobs (doing nothing) until stopped
//
// Jobs are removed on completion, as busy queues usually run, so the server's
// work per job is the scripts that add, fetch and finish it and the events
// they log.
import { Queue, Worker } from 'bullmq';

const [mode, port, n] = process.argv.slice(2);
const connection = { host: '127.0.0.1', port: Number(port) };
const opts = { removeOnComplete: true, removeOnFail: true };

if (mode === 'add') {
  const queue = new Queue('bench', { connection });
  const jobs = Number(n);
  const start = process.hrtime.bigint();
  for (let i = 0; i < jobs; i += 500) {
    const batch = [];
    for (let j = i; j < Math.min(i + 500, jobs); j++) {
      batch.push({ name: 'job', data: { i: j, payload: 'x'.repeat(100) }, opts });
    }
    await queue.addBulk(batch);
  }
  const seconds = Number(process.hrtime.bigint() - start) / 1e9;
  console.log(JSON.stringify({ added: jobs, seconds }));
  await queue.close();
} else if (mode === 'work') {
  const worker = new Worker('bench', async () => {}, {
    connection,
    concurrency: Number(n),
    // Fetching stays on the worker's own connections; no stalled-job sweeps
    // or metrics during a short run.
    stalledInterval: 300000,
  });
  process.on('SIGTERM', async () => {
    await worker.close(true);
    process.exit(0);
  });
} else {
  console.error('usage: node jobs.mjs add|work PORT N');
  process.exit(2);
}
