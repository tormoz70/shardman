# Shardman — Product Contract (MVP)

**Version:** 0.2.0  
**Date:** 2026-08-07  
**Agent SoT:** [`.ai/master-spec.yaml`](../.ai/master-spec.yaml)

## Goals

- Go **control plane** for PostgreSQL range sharding (no SQL proxy).
- Volume **subshards** per `period_id`.
- **Time axis:** retention ring + `max_future_periods` + dedicated **error shard**.

## Invariants

1. Bootstrap fields immutable (`mode`, `period_axis`, `period_spec`, `shard_max_bytes`, `retention_depth`, `max_future_periods`).
2. One **active** data subshard per `period_id`.
3. FSM: `standby → active → sealed → cleaning → standby` (data); error shard always writable.
4. Time pool minimum: `retention_depth + 1 + max_future_periods + 1`.
5. `max_future_periods = 0` → future writes route to error shard.
6. Metadata in separate PostgreSQL.

## API

**Public:** `/v1/resolve/write`, `/v1/resolve/read`, `/v1/periods/{id}/shards`, `/healthz`, `/metrics`

**Admin/internal (`X-Cluster-Key`):** bootstrap, shards, seal-rotate, state patch, retention-tick, internal/stats, internal/cleaned

## Acceptance

- [x] Unit: period_id, ClassifyWrite (future/evicted), FSM
- [x] Bootstrap + shards + resolve + seal + retention supervisors
- [x] Agent stats + clean on `cleaning`
- [x] Prometheus metrics + docker-compose
- [ ] Full integration test on live PG (requires `METADATA_PG_DSN`)

## Docs

- [sharding-model.md](sharding-model.md)
- [architecture.md](architecture.md)
- [mvp-plan.md](mvp-plan.md)
