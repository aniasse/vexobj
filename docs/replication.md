# Replication

VaultFS ships an **async primary-replica** replication model. One node
holds the writable copy; any number of replicas poll it and pull new
data. This document explains the design, what it guarantees, and what
it deliberately leaves out.

## Goals

- Read scaling and geographic redundancy with minimal ceremony.
- Zero dependencies on external coordinators (Consul, etcd, Raft).
- Operable with a single CLI command on the replica side.
- Non-disruptive: a replica that falls offline catches up when it
  comes back, without manual intervention.

## Non-goals (for 0.1.x)

- **Automatic failover / leader election.** If the primary dies, an
  operator has to promote a replica by flipping its config from
  `replica` to `primary`. Adding Raft/Paxos is a lot of code for
  questionable value on a single-writer object store.
- **Synchronous writes.** A PUT succeeds as soon as the primary has
  persisted it locally. Replicas catch up after the fact. Lossy if the
  primary's disk dies before the event is pulled.
- **Cross-region consistency for reads.** Clients that read a replica
  right after writing to the primary may not see their write yet.

## Architecture

```
┌──────────────┐   GET /v1/replication/events?since=<id>
│   Replica    │ ◄──────────────────────────────────────┐
│  vaultfsctl  │   GET /v1/replication/blob/<sha256>    │
│  replicate   │ ◄──────────────────────────────────────┤
└──────┬───────┘                                        │
       │ applies locally                     ┌──────────▼──────────┐
       │ (puts blob, writes                  │       Primary       │
       │  metadata, advances                 │  appends to         │
       │  cursor)                            │  replication_events │
       ▼                                     │  on every write     │
┌──────────────┐                             └─────────────────────┘
│ local vaultfs│
└──────────────┘
```

### Write path on the primary

Every state-changing operation (`put_object`, `delete_object`,
`save_version`, `save_delete_marker`) appends one row to the
`replication_events` table inside the same SQLite transaction.
Bundling the append with the metadata write means the event log and
the object store can never disagree.

### Event schema

| Column | Purpose |
|---|---|
| `id`         | Monotonic event cursor (replicas hold the last applied `id`) |
| `op`         | `put` \| `delete` \| `version_put` \| `delete_marker` |
| `bucket`     | Target bucket name |
| `key`        | Target object key |
| `sha256`     | Content hash of the blob (empty for delete events) |
| `version_id` | Filled only for `version_put` / `delete_marker` |
| `timestamp`  | RFC-3339 wall clock for observability |

### Pulling events

```
GET /v1/replication/events?since=<id>&limit=<n>
Authorization: Bearer <admin-key>
```

Returns events with `id > since` in ascending order, up to `limit`.
Replicas call this in a tight loop with `since = last_applied_id`.

### Pulling blobs

```
GET /v1/replication/blob/<sha256>
Authorization: Bearer <admin-key>
```

Returns the raw blob (ciphertext if SSE is on — the replica must share
the same master key). Replicas only need to call this when the event
is a `put` or `version_put` whose `sha256` they don't already have on
disk.

### Applying events on the replica

For every event, in order:

- **`put`** / **`version_put`**: pull the blob if absent, write it to
  the content-addressed path, insert the metadata row.
- **`delete`**: remove the object row (and the blob if no dedup + no
  other references).
- **`delete_marker`**: insert a delete-marker row, no blob fetch.

The replica persists the cursor after each batch so a crash resumes
cleanly.

## Current status (0.1.0)

- ✅ Primary-side event log with atomic append
- ✅ `/v1/replication/events` + `/v1/replication/blob/:sha256`
- ⏳ `vaultfsctl replicate` CLI — coming in a follow-up commit
- ⏳ Replica-side cursor persistence + apply loop
- ⏳ End-to-end replication e2e test (two servers, primary → replica)

The primary side is usable today: you can point any HTTP client at the
endpoints and reconstruct a mirror by hand. The turnkey CLI is the
next step.

## Security

Both replication endpoints require an API key with the `admin`
permission. In practice, operators create a dedicated
`replication:read` admin key per replica so it can be revoked
independently of human admin keys. Endpoints support the same
Bearer / SigV4 auth as the rest of the API.
