# Shardman Go MVP

> Control plane на Go для range/hash-шардирования Postgres: metadata (PgBouncer), **gRPC**, `bucket_id`, volume-subshards, drain FSM, time retention, error-шард, Prometheus/OTel.

**Стек:** Go 1.22+, gRPC/protobuf. SQL proxy — out of MVP.

**Live contract:** [master-spec.md](master-spec.md) / [`.ai/master-spec.yaml`](../.ai/master-spec.yaml).

## Status (v0.5.0)

- [x] `internal/bucket` + `internal/fsm` (time/numeric/hash, draining)
- [x] gRPC: Resolve, Topology (Get/Watch), Admin, Internal
- [x] Volume seal + **drain** + retention (sealed→cleaning only)
- [x] `cmd/agent` (`SIZE_SOURCE`), `cmd/shardman`, `pkg/client` with **local resolve**
- [x] Prometheus metrics + optional OTel + alert runbook
- [x] Tests: unit, integration (`METADATA_PG_DSN`), e2e (testcontainers)
- [x] Resolve hot path: in-memory `ClusterConfig` cache + cache hit/miss metrics
- [x] Hash string keys lowercased before `xxhash64`; ms/sec timestamp threshold fix
- [x] `PatchShardState` + topology bumps transactional (same DB tx as shard updates)
- [x] **Heartbeat health:** soft-exclude stale data active; `internal/health` auto `SealRotate`
- [x] Metadata pool hardening: `statement_timeout=5s`, `METADATA_PG_MAX_CONNS`
- [x] Metrics: `topology_version`, `seal_duration_seconds`, `heartbeat_failover_total`

## Scope

- Nesting: `shard_key → bucket_id → volume subshards`
- Modes: `range` (time|numeric) | `hash` (fixed `bucket_count`, xxhash64; string keys lowercased)
- One active per `bucket_id`; seal-rotate без ребаланса
- Time: retention + `max_future_buckets` + one error shard
- Topology `Get` + `Watch` для client-side route cache; SDK resolves locally on cache hit

## Out of MVP

- tenant-per-shard, SQL proxy
- error-shard volume rotation / multi-error
- retention для numeric/hash
- in-place `bucket_count` change / consistent-hash rebalance
- mTLS / JWT (auth = `CLUSTER_KEY` в gRPC metadata)
- gRPC `UpdateConfig` (runtime config mutation)

## Post-MVP backlog

- filesystem-based shard sizing (`Statfs` на PVC агента)
- topology_version stuck alert / recording rule

## Quick commands

```bash
make docker-up          # gRPC :9091, metrics :8080
make test-integration
make test-e2e           # Docker + testcontainers
```

## Git / GitHub

SSH only; github-ssh preflight; no HTTPS fallback.
