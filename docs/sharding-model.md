# Sharding model

Nesting: `shard_key → bucket_id → physical shards (standby | active | sealed)`.

`bucket_id` is the routing slot. How it is derived depends on mode/axis.

## Range mode

### Time axis

Time-bucket from key → `bucket_id` (e.g. `2026-08`).

### Numeric axis

`floor(key / width)` → `bucket_id` (e.g. `n42`).

### Volume subshards

Within one `bucket_id`, shards grow by volume:

```
standby → active → sealed → (next standby promoted to active)
```

One **active** subshard per bucket.

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

When a time bucket falls outside retention, **all** its data subshards → `cleaning` → agent truncates → `standby` (recycled).

Retention is anchored to **wall-clock**, not the newest written bucket.

### Error shard

Exactly one `role=error` shard. Receives writes outside the time model:

- future beyond `max_future_buckets`
- `max_future_buckets=0` and any future bucket
- evicted past buckets

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
- `bucket_count` **immutable** — adding physical capacity = standbys + seal-rotate, never remapping keys
- Same volume FSM as range
- No retention ring / error shard required for MVP
- Hash generations (increase `bucket_count` without move) — **out of MVP**
