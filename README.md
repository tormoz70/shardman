# shardman

External **control plane** for PostgreSQL range/hash sharding (Go). Apps resolve write/read targets via **gRPC**; data lives on Postgres shards.

## Quick start (docker-compose)

```bash
make docker-up    # metadata :5433, gRPC :9091, metrics :8080, Prometheus :9090
make build
```

Bootstrap and resolve (CLI talks gRPC to compose server):

```bash
export CLUSTER_KEY=dev-cluster-key
export SHARDMAN_ADDR=localhost:9091   # compose maps GRPC_ADDR :9091 (Prometheus uses :9090)
shardman bootstrap --axis time --unit month --retention 3 --future 1 --max-bytes 1073741824
shardman register --uuid $(uuidgen) --role error --dsn postgres://err/db
shardman register --uuid $(uuidgen) --dsn postgres://shard1/db
shardman resolve-write --key "2026-08-01T00:00:00Z"
shardman topology
curl http://localhost:8080/healthz
curl http://localhost:8080/metrics
```

Local dev without compose (default gRPC `:9090`):

```bash
export METADATA_PG_DSN=postgres://shardman:shardman@127.0.0.1:5433/shardman_meta?sslmode=disable
export CLUSTER_KEY=dev-cluster-key
shardman-server &
export SHARDMAN_ADDR=localhost:9090
shardman bootstrap --axis numeric --width 1000 --max-bytes 1073741824
```

## Binaries

| Binary | Role |
|--------|------|
| `shardman-server` | gRPC control plane + seal/retention/health loops + ops HTTP |
| `shardman-agent` | Reports shard size, drain revoke, clean on `cleaning` |
| `shardman` | CLI (gRPC client) |

## Ports (compose)

| Port | Service |
|------|---------|
| `9091` | gRPC API (`GRPC_ADDR`) |
| `8080` | Ops HTTP `/healthz`, `/metrics` |
| `5433` | Metadata Postgres |
| `9090` | Prometheus (not shardman gRPC) |
| `3000` | Grafana |

## Config (server)

| Env | Default | Description |
|-----|---------|-------------|
| `METADATA_PG_DSN` | — | Metadata Postgres (PgBouncer DSN in prod) |
| `METADATA_PG_MAX_CONNS` | `20` | pgxpool max connections to metadata DB |
| `GRPC_ADDR` | `:9090` | gRPC listen (`:9091` in compose) |
| `HTTP_ADDR` | `:8080` | Ops HTTP only |
| `CLUSTER_KEY` | — | Admin/internal auth (`x-cluster-key` metadata) |
| `SEAL_CHECK_INTERVAL` | `30s` | Seal + retention tick |
| `DRAIN_TIMEOUT` | `30s` | Max wait in `draining` before force seal |
| `HEARTBEAT_TIMEOUT` | `60s` | Stale agent threshold; stale data active excluded from resolve |
| `HEALTH_CHECK_INTERVAL` | `15s` | Health supervisor tick (auto seal-rotate on stale active) |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | — | Optional OTLP traces (see runbook) |

## Config (agent)

| Env | Default | Description |
|-----|---------|-------------|
| `PG_DSN` | — | Data shard Postgres |
| `SHARD_UUID` | — | Shard identity |
| `COORDINATOR_ADDR` | — | Server gRPC host:port |
| `CLUSTER_KEY` | — | `x-cluster-key` for Internal RPCs |
| `STATS_INTERVAL` | `15s` | Heartbeat interval |
| `SIZE_SOURCE` | `database` | `database` (`pg_database_size`) or `relations` |
| `APP_DB_ROLE` | — | Role to revoke on drain (if set) |
| `DRAIN_MODE` | `revoke` | `revoke` or `terminate` backends |

## Config (CLI)

| Env | Default | Description |
|-----|---------|-------------|
| `SHARDMAN_ADDR` | `localhost:9090` | gRPC server address |
| `CLUSTER_KEY` | — | For admin commands |

## Client SDK

```go
import "github.com/tormoz70/shardman/pkg/client"

c, _ := client.Dial(ctx, "localhost:9091", client.Options{ClusterKey: os.Getenv("CLUSTER_KEY")})
_ = c.WatchTopology(ctx) // push updates on topology_version bump
wr, _ := c.ResolveWrite(ctx, "2026-08-01T00:00:00Z") // local resolve from cached topology
```

`Dial` loads topology via `Get`; `WatchTopology` keeps the cache fresh. `ResolveWrite` / `ResolveRead` resolve **locally** from the cached snapshot when the route and shard are present; on miss (no active, auto-promote needed) they fall back to gRPC. Invalidate on `topology_version` change, `Unavailable`, or sealed-shard write errors.

Hash mode: string keys are case-insensitive (`"ABC"` and `"abc"` route to the same bucket).

## Proto / codegen

```bash
make proto   # requires protoc (see scripts/generate-proto.ps1)
```

## Time-axis pool sizing

```
min_shards = retention_depth + 1 + max_future_buckets + 1(error)
```

## Docs

- [docs/master-spec.md](docs/master-spec.md) — product contract
- [docs/architecture.md](docs/architecture.md) — components, API, supervisors
- [docs/sharding-model.md](docs/sharding-model.md) — buckets, FSM, retention, hash
- [docs/mvp-plan.md](docs/mvp-plan.md) — scope and status
- [docs/runbook-alerts.md](docs/runbook-alerts.md) — Prometheus alerts + OTel
- [.ai/master-spec.yaml](.ai/master-spec.yaml) — agent source of truth

## Tests

```bash
make test
make test-integration    # METADATA_PG_DSN → localhost:5433
make test-e2e            # testcontainers + Docker
make test-stand          # docker-up + integration
```

## Tracing (optional)

```bash
OTEL_EXPORTER_OTLP_ENDPOINT=tempo:4317 docker compose -f deploy/docker-compose.yml --profile tracing up
```

GitHub remotes: **SSH only** (`git@github.com:...`).
