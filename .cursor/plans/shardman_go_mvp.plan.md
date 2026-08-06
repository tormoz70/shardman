---
name: Shardman Go MVP
overview: "Go control plane: volume subshards; time retention ring; max_future_periods (0=no future); dedicated error shard for out-of-model writes; Prometheus."
todos:
  - id: workspace-core
    content: "Go module + period/fsm (data/error roles, cleaning, retention)"
    status: pending
  - id: metadata-api
    content: "Bootstrap retention_depth + max_future_periods + error shard; resolve routing"
    status: pending
  - id: seal-loop
    content: "Volume seal (data) + retention clean; error shard never cleaned by retention"
    status: pending
  - id: agent-cli
    content: "Agent stats+clean; CLI bootstrap with future/error sizing"
    status: pending
  - id: observability-deploy
    content: "Metrics/tests: F=0/1 routing, eviction→error, retention spares error shard"
    status: pending
  - id: docs
    content: "Sync master-spec/rules; sharding-model with pool formula and error shard"
    status: pending
isProject: true
---

# Shardman Go MVP

Full text: [docs/mvp-plan.md](../../docs/mvp-plan.md)

## Pool (time)

`min_shards = retention_depth + 1 + max_future_periods + 1(error)`

- `max_future_periods = 0` → future timestamps route to **error shard** (not normal periods)
- `max_future_periods = N` → up to N future period slots for normal writes
- Beyond N / evicted / outside model → **error shard**

## Error shard

One `role=error`, always writable quarantine; not in retention ring. MVP: no volume rotation.
