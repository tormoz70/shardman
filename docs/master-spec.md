# Shardman — Product Contract (MVP)

**Version:** 0.1.0  
**Date:** 2026-08-06  
**Agent SoT:** [`.ai/master-spec.yaml`](../.ai/master-spec.yaml)

## Goals

- Внешний **control plane** для шардирования PostgreSQL **без** патчей СУБД и **без** SQL proxy.
- Режим MVP: **`range`** с осью периода **`time` | `numeric`** (выбор при bootstrap, immutable).
- Рост без rebalance: заполненный шард → `sealed`, пишет следующий `standby` → `active`.

## Non-goals (MVP)

- tenant-per-shard mode  
- Postgres wire-protocol proxy  
- смена `period_spec` / rebalance / перенос данных  
- cross-period транзакции  
- HA control plane (один инстанс + metadata PG)  
- gRPC  

## Invariants

1. `mode`, `period_axis`, `period_spec`, `shard_max_bytes` задаются один раз при bootstrap; изменение → **409**.
2. На каждый `period_id` — **ровно один** `active` шард.
3. FSM: `standby → active → sealed` (только вперёд).
4. Hot-add registration: только `startup_state=standby`.
5. Control plane не хранит и не проксирует пользовательские данные.
6. Metadata — **отдельный** PostgreSQL.

## Functional requirements

| ID | Requirement |
|----|-------------|
| FR-1 | Bootstrap кластера с immutable config |
| FR-2 | Register / list shards (`CLUSTER_KEY`) |
| FR-3 | `POST /v1/resolve/write` → active endpoint по shard_key |
| FR-4 | `POST /v1/resolve/read` → sealed ∪ active |
| FR-5 | Manual `seal-rotate` по `period_id` |
| FR-6 | Auto-seal по `reported_bytes >= max` (agent heartbeat) |
| FR-7 | Auto-promote standby, если active отсутствует |
| FR-8 | Prometheus `/metrics` |

## API (summary)

**Public:** `/v1/resolve/write`, `/v1/resolve/read`, `/v1/periods/{period_id}/shards`, `/healthz`, `/metrics`  

**Admin/internal (`X-Cluster-Key`):** `/v1/admin/bootstrap`, `/v1/admin/shards`, `/v1/admin/seal-rotate`, `/v1/admin/shards/{id}/state`, `/v1/internal/stats`

## NFR

- Seal-rotate и смена state — CAS / одна DB-транзакция (нет dual-active).
- Structured tracing + Prometheus metrics (`shardman_*`).
- GitHub interaction: **SSH only** (`git@github.com:...`).

## Acceptance (MVP)

- [ ] Bootstrap + reject second bootstrap / config mutate (409)
- [ ] period_id: time day + numeric width unit tests
- [ ] Register two standby → promote → resolve write points to active
- [ ] Stats over max → seal → new active; resolve read returns both
- [ ] Concurrent seal-rotate does not create two actives
- [ ] `/metrics` exposes shard state and seal counters
- [ ] Agent updates `reported_bytes` / `last_seen_at`

## Related docs

- [mvp-plan.md](mvp-plan.md) — implementation plan  
- sharding-model.md / architecture.md — TBD during implementation  
