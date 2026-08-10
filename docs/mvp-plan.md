# Shardman Go MVP

> Overview: control plane на Go для range/hash-шардирования Postgres: metadata-БД, HTTP resolve, `bucket_id`, volume-subshards, time retention-кольцо, `max_future_buckets`, error-шард, Prometheus.

**Стек:** Go 1.22+. SQL proxy — out of MVP.

**Live contract:** [master-spec.md](master-spec.md) / [`.ai/master-spec.yaml`](../.ai/master-spec.yaml). Этот файл — краткий обзор MVP.

## Status

- [x] `internal/bucket` + `internal/fsm` (time/numeric/hash, retention, error)
- [x] Migrations + chi bootstrap/resolve/`CLUSTER_KEY`
- [x] Volume seal + retention clean + error routing
- [x] `cmd/agent` + `cmd/shardman`
- [x] Prometheus + compose + unit tests
- [x] Integration suite `internal/integration` (time, numeric, hash, API)

## Scope

- Nesting: `shard_key → bucket_id → volume subshards`
- Modes: `range` (time|numeric) | `hash` (fixed `bucket_count`, xxhash64)
- One active per `bucket_id`; seal-rotate без ребаланса
- Time: retention + `max_future_buckets` + one error shard
- Hash generations — out of MVP

## Out of MVP

- tenant-per-shard, SQL proxy
- error-shard volume rotation / multi-error
- retention для numeric/hash
- in-place `bucket_count` change / consistent-hash rebalance
- HA, gRPC

## Git / GitHub

SSH only; github-ssh preflight; no HTTPS fallback.
