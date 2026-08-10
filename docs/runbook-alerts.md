# Alert runbook

Prometheus metrics at `GET http://<HTTP_ADDR>/metrics` (default `:8080`). Example rules: `deploy/alerts.yaml`.

## Key metrics

| Metric | Type | Meaning |
|--------|------|---------|
| `shardman_resolve_duration_seconds` | histogram | Resolve RPC latency (`op=write\|read`) |
| `shardman_active_shards` | gauge | Active data shards |
| `shardman_standby_pool_size` | gauge | Free standbys |
| `shardman_standby_exhausted_total` | counter | Seal without standby |
| `shardman_seal_total` | counter | Seal events per bucket |
| `shardman_seal_duration_seconds` | histogram | Seal rotate / drain-complete duration |
| `shardman_topology_version` | gauge | Current topology version |
| `shardman_resolve_config_cache_hits_total` | counter | ClusterConfig cache hits |
| `shardman_resolve_config_cache_misses_total` | counter | ClusterConfig cache misses |
| `shardman_heartbeat_failover_total` | counter | Auto seal-rotate on stale active |
| `shardman_stale_active_shards` | gauge | Stale active shards pending failover |
| `shardman_agent_last_seen_seconds` | gauge | Agent heartbeat age |
| `shardman_error_route_total` | counter | Routes to error shard |
| `shardman_retention_clean_total` | counter | Retention evictions |

## Agent stale

**Signal:** `shardman_agent_last_seen_seconds` > 120 for a shard UUID.

**Meaning:** Agent stopped sending stats; seal/clean may stall. Stale **data active** shards are excluded from resolve; `health` supervisor auto `SealRotate` when standby available.

**Actions:**
1. Check `shardman-agent` process on the host.
2. Verify gRPC reachability to server `GRPC_ADDR`.
3. Confirm `CLUSTER_KEY` matches server.
4. Watch `shardman_heartbeat_failover_total` and `shardman_stale_active_shards`; if failover fails, register standbys.

## Heartbeat failover

**Signal:** `increase(shardman_heartbeat_failover_total[5m]) > 0` or `shardman_stale_active_shards > 0`.

**Meaning:** Control plane detected stale active and attempted seal-rotate to standby.

**Actions:**
1. Confirm new active shard is healthy (`shardman_agent_last_seen_seconds` fresh).
2. Investigate old sealed shard host; data remains on sealed shard for reads.
3. If `shardman_stale_active_shards` persists, check standby pool and metadata DB connectivity.

## Standby pool exhausted

**Signal:** `shardman_standby_pool_size == 0` or `shardman_standby_exhausted_total` increasing.

**Meaning:** Seal could not promote; writes may return gRPC `Unavailable`.

**Actions:**
1. Register new standby shards via `Admin.RegisterShard`.
2. Investigate stuck `cleaning` shards (agent not truncating).

## Error-shard growth

**Signal:** `shardman_error_shard_bytes` or `shardman_error_route_total` rising.

**Meaning:** Keys outside time model (future, evicted, invalid).

**Actions:**
1. Check application clocks and `max_future_buckets`.
2. Review retention depth vs wall clock.
3. Fix client shard keys.

## Seal rate anomaly

**Signal:** `rate(shardman_seal_total[5m])` spike.

**Meaning:** `shard_max_bytes` too low or hot write pattern.

**Actions:**
1. Raise `shard_max_bytes` at bootstrap (immutable — requires new cluster) or add capacity via standbys.
2. Review per-shard write volume.

## Resolve errors

**Signal:** gRPC `Unavailable` / `Internal` on `Resolve.Write`.

**Actions:**
1. Check standby pool.
2. Check metadata via PgBouncer.
3. Check topology: missing active for bucket.

## Topology version stuck

**Signal:** Clients report stale routes after seal.

**Actions:**
1. Verify `Topology.Watch` or poll `Topology.Get`.
2. Compare `topology_version` with server.

## OpenTelemetry traces (optional)

Enable when `OTEL_EXPORTER_OTLP_ENDPOINT` is set (empty = disabled).

**Local with compose:**

```bash
OTEL_EXPORTER_OTLP_ENDPOINT=tempo:4317 docker compose -f deploy/docker-compose.yml --profile tracing up
```

Then open Grafana (`http://localhost:3000`, admin/admin) → Explore → Tempo data source (`http://tempo:3200` if configured) or use Grafana Tempo plugin.

**Span attributes on resolve RPCs:** `rpc.method`, `grpc.status_code`, `bucket_id`, `shard_uuid`, `shardman.routing`.
