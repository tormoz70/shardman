# Shardman — Product Contract (MVP)

**Version:** 0.3.0  
**Date:** 2026-08-10  
**Agent SoT:** [`.ai/master-spec.yaml`](../.ai/master-spec.yaml)

## Goals

- Go **control plane** for PostgreSQL sharding (no SQL proxy).
- Routing slot: **`bucket_id`** (`shard_key → bucket_id → volume subshards`).
- Modes: **`range`** (time | numeric) and **`hash`** (fixed `bucket_count`, xxhash64).
- **Time axis:** retention ring + `max_future_buckets` + dedicated **error shard**.

## Invariants

1. Bootstrap fields immutable (`mode`, `bucket_axis`, `bucket_spec`, `shard_max_bytes`, `retention_depth`, `max_future_buckets`).
2. One **active** data subshard per `bucket_id`.
3. FSM: `standby → active → sealed → cleaning → standby` (data); error shard always writable.
4. Time pool minimum: `retention_depth + 1 + max_future_buckets + 1`.
5. `max_future_buckets = 0` → future writes route to error shard.
6. Hash: `bucket_count` / `hash_algo` never change live (no rebalance).
7. Metadata in separate PostgreSQL.

## API

**Public:** `/v1/resolve/write`, `/v1/resolve/read`, `/v1/buckets/{id}/shards`, `/healthz`, `/metrics`

**Admin/internal (`X-Cluster-Key`):** bootstrap, shards, seal-rotate, state patch, retention-tick, internal/stats, internal/cleaned

Resolve JSON uses `bucket_id` and `routing: "bucket"|"error"`.

## Acceptance

- [x] Unit: bucket_id (time/numeric/hash), ClassifyWrite (future/evicted), FSM
- [x] Bootstrap + shards + resolve + seal + retention supervisors
- [x] Agent stats + clean on `cleaning`
- [x] Prometheus metrics + docker-compose
- [x] Full integration test on live PG (`make test-stand` or `METADATA_PG_DSN=... make test-integration`)

## Docs

- [sharding-model.md](sharding-model.md)
- [architecture.md](architecture.md)
- [mvp-plan.md](mvp-plan.md)
