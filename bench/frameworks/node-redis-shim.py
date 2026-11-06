#!/usr/bin/env python3
"""Starts the server under test as a process, where node-redis's tests start a container."""
import re
import sys

path = sys.argv[1]
s = open(path).read()
s, n = re.subn(r"export async function spawnRedisServerDocker\(", "export async function spawnInDocker(", s, count=1)
assert n == 1
s, n = re.subn(
    r"async function dockerRemove\(dockerId: string\): Promise<void> \{.*?\n\}\n",
    """async function dockerRemove(dockerId: string): Promise<void> {
  try { process.kill(Number(dockerId), 'SIGKILL'); } catch {}
}
""",
    s,
    count=1,
    flags=re.S,
)
assert n == 1
s += """
export async function spawnRedisServerDocker(
  options: RedisServerDockerOptions, serverArguments: Array<string>): Promise<RedisServerDocker> {
  const { spawn } = await import('node:child_process');
  const { createConnection } = await import('node:net');
  const port = (await portIterator.next()).value;
  // Tantalum gets the options it has: others are for what it doesn't do yet.
  const tantalum = process.env.SERVER === 'tantalum';
  const args: Array<string> = ['--port', String(port)];
  if (tantalum) {
    const at = serverArguments.indexOf('--requirepass');
    if (at >= 0) args.push('--requirepass', serverArguments[at + 1]);
  } else {
    args.push('--save', '', '--appendonly', 'no', ...serverArguments);
  }
  const child = spawn(tantalum ? 'tantalum' : 'redis-server', args, { stdio: 'ignore' });
  for (let i = 0; i < 100; i++) {
    const up = await new Promise<boolean>(resolve => {
      const socket = createConnection(port, '127.0.0.1');
      socket.once('connect', () => { socket.destroy(); resolve(true); });
      socket.once('error', () => resolve(false));
    });
    if (up) break;
    await setTimeout(50);
  }
  return { port, dockerId: String(child.pid) };
}
"""
open(path, "w").write(s)
