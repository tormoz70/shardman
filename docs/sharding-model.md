# Sharding model

## Range mode

Shard key → `period_id` → active subshard (write) or sealed∪active (read).

### Volume subshards

Within one `period_id`, shards grow by volume:

```
standby → active → sealed → (next standby promoted to active)
```

One **active** subshard per period.

### Time retention ring

Parameters (immutable at bootstrap):

- `period_spec.unit` — hour | day | month
- `retention_depth` — e.g. 3 = keep last 3 period buckets relative to wall-clock
- `max_future_periods` — 0 = no normal writes to future; N = allow up to N periods ahead
- `shard_max_bytes` — volume seal threshold

**Minimum physical shards:**

```
retention_depth + 1 + max_future_periods + 1(error)
```

When a period falls outside retention, **all** its data subshards → `cleaning` → agent truncates → `standby` (recycled).

Retention is anchored to **wall-clock**, not the newest written period.

### Error shard

Exactly one `role=error` shard. Receives writes outside the time model:

- future beyond `max_future_periods`
- `max_future_periods=0` and any future period
- evicted past periods

### Resolve matrix (time)

| Condition | Route |
|-----------|--------|
| period in retention window | data period active |
| `current < period <= current + F` | future data period |
| else | error shard |

### Numeric axis (MVP)

Volume subshards only; no retention ring, no error shard requirement.
