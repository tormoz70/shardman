# Shardman — Product Contract

**Version:** 0.4.0  
**Date:** 2026-08-11  
**Agent SoT:** [`.ai/master-spec.yaml`](../.ai/master-spec.yaml)

## Goals

- Go **control plane** for PostgreSQL sharding (no SQL proxy).
- Routing slot: **`bucket_id`** (`shard_key → bucket_id → volume subshards`).
- Modes: **`range`** (time | numeric) and **`hash`** (fixed `bucket_count`, xxhash64).
- **Time axis:** retention ring + `max_future_buckets` + dedicated **error shard**.
- **gRPC API** for resolve, topology, admin, internal agent RPCs.

## Invariants

1. Bootstrap fields immutable (`mode`, `bucket_axis`, `bucket_spec`, `shard_max_bytes`, `retention_depth`, `max_future_buckets`).
2. One **active** data subshard per `bucket_id`.
3. FSM (data): `standby → active → draining → sealed → cleaning → standby`; error shard always writable.
4. Time pool minimum: `retention_depth + 1 + max_future_buckets + 1`.
5. `max_future_buckets = 0` → future writes route to error shard.
6. Hash: `bucket_count` / `hash_algo` never change live (no rebalance).
7. Metadata in separate PostgreSQL (PgBouncer DSN in production).

## API (gRPC)

Proto: `api/proto/shardman/v1/shardman.proto`

| Service | Auth | Methods |
|---------|------|---------|
| `ResolveService` | — | `Write`, `Read`, `ListBucketShards` |
| `TopologyService` | — | `Get`, `Watch` |
| `AdminService` | `x-cluster-key` | `Bootstrap`, `ListShards`, `RegisterShard`, `SealRotate`, `PatchShardState`, `RetentionTick` |
| `InternalService` | `x-cluster-key` | `ReportStats`, `ReportCleaned`, `ReportDrainComplete` |

**Ops HTTP only:** `GET /healthz`, `GET /metrics` (no REST resolve API).

### Topology

- `Topology.Get` — snapshot + `topology_version`
- `Topology.Watch` — server stream on version bumps (seal, promote, register, clean, retention)

Clients cache topology; resolve on cache miss. Invalidate on `topology_version` change, `Unavailable`, or sealed-shard write errors.

### gRPC status mapping

| Condition | Code |
|-----------|------|
| No active / standby exhausted | `Unavailable` |
| Second bootstrap | `AlreadyExists` |
| Bad/missing cluster key | `Unauthenticated` |
| Seal / state conflict | `Aborted` / `NotFound` |

## Write / read paths

- **Write:** `shard_key → bucket_id → active` (auto-promote standby on miss; `Unavailable` if pool empty).
- **Read:** `sealed ∪ active` for bucket; never `standby`.
- **Retention:** only **sealed** shards → `cleaning`; skip bucket if `active`/`draining` present.

## Client routing

Do **not** call resolve on every SQL. Use `pkg/client` + `Topology.Watch`. See [architecture.md](architecture.md).

## Hash analytics (warnings)

- Fan-out tax, straggler, no unbounded `SELECT *`, colocated JOIN only.
- Prefer **CQRS → OLAP** for analytics.

## Environment (summary)

| Component | Key vars |
|-----------|----------|
| Server | `METADATA_PG_DSN`, `GRPC_ADDR`, `HTTP_ADDR`, `CLUSTER_KEY`, `DRAIN_TIMEOUT`, `OTEL_EXPORTER_OTLP_ENDPOINT` |
| Agent | `PG_DSN`, `SHARD_UUID`, `COORDINATOR_ADDR`, `CLUSTER_KEY`, `SIZE_SOURCE`, `APP_DB_ROLE`, `DRAIN_MODE` |
| CLI | `SHARDMAN_ADDR`, `CLUSTER_KEY` |

Compose: gRPC **`:9091`** (Prometheus uses host `:9090`).

## Acceptance

- [x] Unit: bucket_id, ClassifyWrite, FSM (incl. draining)
- [x] gRPC bootstrap/resolve/topology + supervisors (seal drain, retention)
- [x] Agent stats/clean/drain; `pkg/client` SDK
- [x] Prometheus + optional OTel; alert runbook
- [x] Integration (`make test-integration`) + e2e testcontainers (`make test-e2e`)

## Docs

- [sharding-model.md](sharding-model.md)
- [architecture.md](architecture.md)
- [mvp-plan.md](mvp-plan.md)
- [runbook-alerts.md](runbook-alerts.md)
