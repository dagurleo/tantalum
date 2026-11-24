# Security

## Reporting a vulnerability

Please report security problems privately, through GitHub: the repository's **Security**
tab, then **Report a vulnerability**. Don't open a public issue for them.

Say what's affected (version, platform, options), how to reproduce it, and what an attacker
could do with it. You'll get a reply within a week. Once there's a fix, it's released and the
advisory published, crediting you unless you'd rather not be named.

## Supported versions

Tantalum is in alpha: fixes go into the latest release only.

## Running it safely

- It listens on `127.0.0.1` unless `--bind` says otherwise. Anything reachable from other
  machines should have a password (`--requirepass` or an ACL file) and, off a private
  network, TLS (`--tls-port` with a certificate).
- Like Redis, a client that has the password can run Lua scripts and administrative
  commands (`CONFIG`, `SHUTDOWN`, `FLUSHALL`, `REPLICAOF`). Use ACL users to limit what each
  client may do.
- Replication, Sentinel and the cluster bus trust their peers as Redis does; keep their
  ports on a private network.
