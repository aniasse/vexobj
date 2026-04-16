# Primary failure runbook

VaultFS replication is single-writer and async (see
[docs/replication.md](replication.md)). When the primary dies, an
operator must promote a replica; there is no automatic failover.
This document walks through the promotion, recovery, and return-to-
service steps.

## When to use this runbook

- The primary is unreachable and won't come back quickly (disk failure,
  VM gone, data-centre outage).
- The primary returns errors but is still running — **do not** promote
  a replica while the primary might still be accepting writes. Split
  brain destroys data silently. Stop the primary first (firewall,
  systemd stop, revoke its admin key).

## Before you start

You should have:

- SSH / shell access to the chosen replica.
- The replica's admin API key (the one `vaultfsctl replicate` was
  using as `--local-key`).
- Ability to update DNS or the load balancer that points clients at
  the current primary.
- If SSE is on, the same master key is already configured on the
  replica (otherwise its ciphertext is unreadable).

## Step 1 — confirm the replica is healthy and caught up

```bash
# On the replica:
vaultfsctl --url http://localhost:8000 --key "$REPLICA_KEY" health
vaultfsctl --url http://localhost:8000 --key "$REPLICA_KEY" stats
```

Optional: if the primary is reachable enough for one last poll, run
`vaultfsctl replicate --interval 0` one more time to pull any
pending events before promotion. Skip this if the primary is fully
gone.

## Step 2 — promote

```bash
vaultfsctl \
  --url http://localhost:8000 \
  --key "$REPLICA_KEY" \
  promote \
  --cursor-file /var/lib/vaultfs/replica.cursor
```

This:

1. Hits `/health` and `/v1/stats` on the local server to catch
   "replica is itself broken" cases before any external state changes.
2. Prints the last-applied event id so you know where the replica
   was in the log when the primary died.
3. Deletes the cursor file (pass `--keep-cursor` to skip). Without
   deletion, a future `replicate` call that pointed back at the dead
   primary could happily start from id 0 and re-apply everything.

The command does NOT modify server state. Promotion is a coordination
change, not a data change — the replica already accepts writes on
every endpoint; what's missing is clients actually being pointed at
it.

## Step 3 — redirect clients

Do this in roughly this order to minimise split-brain risk:

1. **Revoke the old primary's admin key.** If the primary comes back
   online while you're still cutting over, you don't want
   `vaultfsctl replicate` on remaining replicas to keep pulling from
   it.
2. **Update DNS / load balancer** to point `vaultfs.example.com` at
   the new primary.
3. **Update SDK / client config** for any caller that doesn't go
   through the load balancer (CI jobs, cron, etc.).

## Step 4 — restart replication on the remaining replicas

Each surviving replica now needs to point at the new primary with a
fresh cursor:

```bash
# On each remaining replica:
rm -f /var/lib/vaultfs/replica.cursor
vaultfsctl \
  --url http://localhost:8000 \
  --key "$REPLICA_KEY" \
  replicate \
  --primary https://new-primary.example.com \
  --primary-key "$NEW_PRIMARY_ADMIN_KEY" \
  --cursor-file /var/lib/vaultfs/replica.cursor \
  --interval 5 &
```

Replicas will do a full catch-up from event 0 on the new primary —
most events are no-ops because the content is already present via
content-addressed blobs, so catch-up is cheap after the first pull.

## Step 5 — post-mortem

Don't reuse the old primary node until you understand why it failed.
If the disk is intact, pull a backup:

```bash
# From the dead primary (or its disk mounted elsewhere)
tar czf vaultfs-primary-corpse.tgz /var/lib/vaultfs/
```

Then wipe the node and re-provision it as a new replica pointing at
the promoted primary.

## What this design trades away

- **No zero-RPO failover.** A write that reached the primary but
  hadn't been pulled by any replica is lost. If that's unacceptable,
  run `vaultfsctl replicate --interval 1` on at least one replica and
  monitor its cursor.
- **No automatic election.** If the operator isn't around, clients
  just hit a dead primary. Adding Raft is not on the 0.1.x roadmap —
  the tradeoff is keeping the server small enough to operate
  yourself.
- **Post-promotion renames are manual.** VaultFS doesn't care which
  node is "the primary"; clients do. The load balancer / DNS layer
  is the source of truth for that.
