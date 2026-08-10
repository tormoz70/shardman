# Shardman Go MVP

> Control plane на Go для range/hash-шардирования Postgres: metadata (PgBouncer), **gRPC**, `bucket_id`, volume-subshards, drain FSM, time retention, error-шард, Prometheus/OTel.

**Стек:** Go 1.22+, gRPC/protobuf. SQL proxy — out of MVP.

**Live contract:** [master-spec.md](master-spec.md) / [`.ai/master-spec.yaml`](../.ai/master-spec.yaml).

## Status (v0.4.0)

- [x] `internal/bucket` + `internal/fsm` (time/numeric/hash, draining)
- [x] gRPC: Resolve, Topology (Get/Watch), Admin, Internal
- [x] Volume seal + **drain** + retention (sealed→cleaning only)
- [x] `cmd/agent` (`SIZE_SOURCE`), `cmd/shardman`, `pkg/client`
- [x] Prometheus metrics + optional OTel + alert runbook
- [x] Tests: unit, integration (`METADATA_PG_DSN`), e2e (testcontainers)

## Scope

- Nesting: `shard_key → bucket_id → volume subshards`
- Modes: `range` (time|numeric) | `hash` (fixed `bucket_count`, xxhash64)
- One active per `bucket_id`; seal-rotate без ребаланса
- Time: retention + `max_future_buckets` + one error shard
- Topology `Get` + `Watch` для client-side route cache

## Out of MVP

- tenant-per-shard, SQL proxy
- error-shard volume rotation / multi-error
- retention для numeric/hash
- in-place `bucket_count` change / consistent-hash rebalance
- mTLS / JWT (auth = `CLUSTER_KEY` в gRPC metadata)

## Post-MVP backlog

- filesystem-based shard sizing (`Statfs` на PVC агента)

## Quick commands

```bash
make docker-up          # gRPC :9091, metrics :8080
make test-integration
make test-e2e           # Docker + testcontainers
```

## Git / GitHub

SSH only; github-ssh preflight; no HTTPS fallback.
