# shardman

External **control plane** for PostgreSQL range/hash sharding (Go). Apps resolve write/read targets via HTTP; data lives on Postgres shards.

## Quick start

```bash
make docker-up
```

Bootstrap (from host with CLI built, or exec into container):

```bash
export CLUSTER_KEY=dev-cluster-key
# range + time
shardman bootstrap --axis time --unit month --retention 3 --future 1 --max-bytes 1073741824
# or hash (fixed buckets, no rebalance)
# shardman bootstrap --axis hash --buckets 256 --max-bytes 1073741824
shardman register --uuid $(uuidgen) --role error --dsn postgres://err/db
shardman register --uuid $(uuidgen) --dsn postgres://shard1/db
shardman resolve-write --key "2026-08-01T00:00:00Z"
curl http://localhost:8080/metrics
```

## Binaries

| Binary | Role |
|--------|------|
| `shardman-server` | Control plane API + seal/retention loops |
| `shardman-agent` | Reports `pg_database_size`, runs clean on `cleaning` |
| `shardman` | CLI |

## Config (server)

| Env | Default | Description |
|-----|---------|-------------|
| `METADATA_PG_DSN` | — | Metadata Postgres (required) |
| `HTTP_ADDR` | `:8080` | Listen address |
| `CLUSTER_KEY` | — | Admin/internal auth |
| `SEAL_CHECK_INTERVAL` | `30s` | Seal + retention tick |

## Time-axis pool sizing

```
min_shards = retention_depth + 1 + max_future_buckets + 1(error)
```

- `max_future_buckets=0` → future timestamps route to **error shard**
- `max_future_buckets=N` → up to N future time buckets accept normal writes

## Docs

- [docs/mvp-plan.md](docs/mvp-plan.md)
- [docs/sharding-model.md](docs/sharding-model.md)
- [docs/architecture.md](docs/architecture.md)
- [.ai/master-spec.yaml](.ai/master-spec.yaml)

## Integration tests

Requires metadata Postgres (e.g. `make docker-up` exposes `:5433`):

```bash
make test-stand          # docker-up + integration suite
make test-integration    # METADATA_PG_DSN defaults to localhost:5433
```

Covers time / numeric / hash modes via `internal/integration` (control plane only; fake shard DSNs). Not covered: real data Postgres SQL, agent `pg_database_size` e2e.

## Build

```bash
make build
make test
make test-integration
```

GitHub remotes: **SSH only** (`git@github.com:...`).
