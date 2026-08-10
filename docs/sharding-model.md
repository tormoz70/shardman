# Sharding model

Nesting: `shard_key → bucket_id → physical shards (standby | active | draining | sealed | cleaning)`.

`bucket_id` is the routing slot. How it is derived depends on mode/axis.

## Range mode

### Time axis

Time-bucket from key → `bucket_id` (e.g. `2026-08`).

Numeric `shard_key` may be RFC3339 / `YYYY-MM-DD` / `YYYY-MM` strings, or Unix timestamp. Values above `32503680000` (~year 3000 in seconds) are treated as **milliseconds**; otherwise seconds.

### Numeric axis

`floor(key / width)` → `bucket_id` (e.g. `n42`).

### Volume subshards

Within one `bucket_id`, shards grow by volume:

```
standby → active → draining → sealed → (next standby promoted to active)
```

One **active** subshard per bucket at a time. Seal path uses **draining** so agents can revoke writes before metadata marks the shard sealed.

### Time retention ring

Parameters (immutable at bootstrap):

- `bucket_spec.unit` — hour | day | month
- `retention_depth` — e.g. 3 = keep last 3 time buckets relative to wall-clock
- `max_future_buckets` — 0 = no normal writes to future; N = allow up to N buckets ahead
- `shard_max_bytes` — volume seal threshold

**Minimum physical shards:**

```
retention_depth + 1 + max_future_buckets + 1(error)
```

When a time bucket falls outside retention:

1. Bucket must have no `active`/`draining` shards (retention skips otherwise).
2. **Sealed** data subshards → `cleaning` → agent truncates → `standby` (recycled).

Retention is anchored to **wall-clock**, not the newest written bucket.

### Error shard

Exactly one `role=error` shard. Receives writes outside the time model:

- future beyond `max_future_buckets`
- `max_future_buckets=0` and any future bucket
- evicted past buckets

Never retention-cleaned.

### Resolve matrix (time)

| Condition | Route |
|-----------|--------|
| bucket in retention window | data bucket active |
| `current < bucket <= current + F` | future data bucket |
| else | error shard |

### Numeric axis (MVP)

Volume subshards only; no retention ring, no error shard requirement.

## Hash mode

Bootstrap: `mode=hash`, `bucket_axis=hash`, `bucket_spec={bucket_count, hash_algo}`.

- `bucket_id = h{hash(key) % bucket_count}` (algo: `xxhash64`)
- String `shard_key` values are **lowercased** before hashing (deterministic routing across clients)
- `bucket_count` **immutable** — adding physical capacity = standbys + seal-rotate, never remapping keys
- Same volume FSM as range (incl. draining)
- No retention ring / error shard required for MVP

## Hash mode analytics

> **Warning:** Hash destroys key locality. Point reads/writes by shard key work; range/analytics queries require **scatter-gather** in the application (or OLAP elsewhere).

1. **Fan-out tax:** one analytics query = N shard connections. Do not run scatter queries inside transactions.
2. **Straggler problem:** query latency = slowest shard. Always use `context.WithTimeout`.
3. **No unbounded `SELECT *`:** without `LIMIT`, scatter-gather can OOM the client.
4. **JOIN:** only when both tables use the same shard key (colocation). Otherwise join in app memory or use CQRS → ClickHouse/BigQuery/etc.

**Best practice:** OLTP on shardman-routed Postgres; async replicate to OLAP for analytics. Use `pkg/client.ScatterQuery` only as a connectivity/fan-out helper — not a SQL proxy.

## Client integration

1. `Topology.Watch` or poll `Get` — cache `topology_version` and shard endpoints.
2. **Local resolve** (`pkg/client`): classify `shard_key` → `bucket_id`, pick active/sealed from cache — no gRPC on hit.
3. On miss (no active, auto-promote, stale failover): `Resolve.Write` / `Resolve.Read` over gRPC.
4. Connect to Postgres `endpoint` directly.
5. Invalidate cache on `topology_version` bump, gRPC `Unavailable`, or sealed-shard write error.

### Heartbeat / stale active

Agents report `last_seen_at` via `Internal.ReportStats`. If a data **active** shard misses heartbeats for longer than `HEARTBEAT_TIMEOUT` (default 60s):

- it is **soft-excluded** from resolve (still `state=active` in metadata)
- `internal/health` supervisor auto `SealRotate`s to the next standby
- error shard is never auto-rotated

Sealed shards remain readable even when the agent is down (data on disk).
