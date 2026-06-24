# Event Bus Migration Plan (v2)

## Status

| Phase | Description | Status |
|---|---|---|
| 0 | Elixir EventBus adapters (Prism + Beacon) | ✅ Complete |
| 1 | Cut over Prism → Beacon PubSub | ❌ Next |
| 2 | Iris Go EventBus adapter | ❌ |
| 3 | Bot Python EventBus adapter + CacheInvalidator | ❌ |
| 4 | Bot callback migration (pull from reply index) | ❌ |
| 5 | Polarizer CloudEvents (dual-path) | ❌ |
| 6 | Cut-overs and cleanup | ❌ |

---

## Goal

Replace direct Redis PubSub inter-service communication with a unified Redis Streams-based event bus using CloudEvents v1.0 envelopes, behind per-language adapter abstractions that enable future Kafka migration.

## Scope

| Service | Language | Role | Repo |
|---|---|---|---|
| **Prism** | Elixir | Producer (`broadcast.completed`) | `~/Developments/interchat-broadcast-worker` |
| **Beacon** | Elixir | Consumer (`broadcast.completed` → WebSocket) | `~/Developments/beacon` |
| **Iris** | Go | Producer (`permissions.invalidated`) | `~/Developments/iris` |
| **Bot** | Python | Producer + Consumer (cache inval, callbacks, polarizer) | `~/Developments/interchat.py` |
| **Polarizer** | Rust | Producer (`polarizer.result.ready`) | `~/Developments/polarizer` |

### In Scope
- 8 PubSub channels → CloudEvents on Redis Streams
- Per-language EventBus adapters: Elixir ✅, Python ❌, Go ❌, Rust ❌
- Dead-letter queue with retry (3 attempts, exponential backoff)
- OpenTelemetry context propagation (traceparent/tracestate)
- Dual-write migration phases with safe cut-overs

### Out of Scope
- `prism:wakeup` (internal Prism scheduling)
- `prism:stream:fast/slow/retries` (webhook work queues)
- `polarizer:jobs` / `mud:commands` / `mud:responses` (existing work queues)
- Prism's ZSET-based DelayedQueue (domain-specific webhook retry)
- Iris ↔ Bot HTTP/ConnectRPC (authorization RPCs)
- Kafka implementation (adapter interface enables future swap)

---

## Design Decisions

### 1. Event Envelope: CloudEvents v1.0 JSON

```json
{
  "specversion": "1.0",
  "type": "fun.interchat.broadcast.completed",
  "source": "/prism",
  "id": "evt_a1b2c3d4",
  "time": "2026-06-24T01:12:54Z",
  "datacontenttype": "application/json",
  "data": { ... }
}
```

Extension attributes injected by adapter:
- `traceparent` — W3C Trace Context propagation
- `tracestate` — W3C Trace State

`source` values: `/prism`, `/beacon`, `/iris`, `/bot`, `/polarizer`

### 2. Adapter Approach

Per-language idiomatic modules implementing the same conceptual contract. No shared code, only shared contract.

| Language | Adapter | Transport | Status |
|---|---|---|---|
| Elixir | `EventBus` module | Redis Streams via Redix | ✅ |
| Python | `EventBus` class | Redis Streams via `redis.asyncio` | ❌ |
| Go | `eventbus` package | Redis Streams via `go-redis` | ❌ |
| Rust | `eventbus` module | Redis Streams via `redis-rs` | ❌ |

### 3. Stream Topology

```
                        ┌─────────────────┐
                        │   events:bus     │  Shared event stream
                        │   (Redis Stream) │
                        └────────┬────────┘
                                 │
              ┌──────────────────┼──────────────────────┐
              │                  │                      │
     Consumer Group:    Consumer Group:    Consumer Groups:
     beacon-hub-fanout  bot-broadcast-     bot-cache-invalidator-{shard_id}
     (Elixir Beacon)    callbacks          (Python Bot, per shard)
                        (Python Bot)

┌─────────────────┐
│ events:polarizer │  Dedicated NSFW results stream
│ (Redis Stream)   │
└────────┬────────┘
         │
  Consumer Group:
  bot-polarizer-results
  (Python Bot)
```

### 4. Consumer Group Naming

| Consumer Group | Service | Stream |
|---|---|---|
| `beacon-hub-fanout` | Beacon | `events:bus` |
| `bot-broadcast-callbacks` | Bot | `events:bus` |
| `bot-cache-invalidator-{shard_id}` | Bot (per shard) | `events:bus` |
| `bot-polarizer-results` | Bot | `events:polarizer` |

**Per-shard rationale:** Each Bot shard maintains independent in-memory caches. Every shard must receive every cache invalidation event to keep caches consistent. Per-shard consumer groups ensure each shard independently processes every event.

### 5. Adapter API

```elixir
# Elixir
EventBus.publish(stream, type: type, data: data, opts)
EventBus.subscribe(stream: stream, consumer_group: group, handler: fn, opts)
```

```python
# Python
await event_bus.publish(stream, event_type, data, **opts)
await event_bus.subscribe(stream, consumer_group, handler, **opts)
```

```go
// Go (publish only for Iris)
eventbus.Publish(ctx, stream, eventType, data, opts...)
```

```rust
// Rust (publish only for Polarizer)
eventbus::publish(stream, event_type, data, opts).await
```

**`publish` behavior:**
1. Generate CloudEvent envelope (`id`, `time`, `source`, `type`)
2. Inject OTel trace context into extension attributes
3. Serialize to JSON
4. `XADD` to stream with `MAXLEN ~ 100000`

**`subscribe` behavior:**
1. Create consumer group if not exists (`XGROUP CREATE ... MKSTREAM`)
2. Background consume loop: `XREADGROUP ... BLOCK ... COUNT ...`
3. Parse CloudEvent, extract trace context, invoke handler
4. On success: `XACK`
5. On failure: retry (3 attempts, exponential backoff), then `XADD` to DLQ + `XACK`
6. Periodic `XAUTOCLAIM` for stale message recovery

### 6. Dead-Letter Queue

**Stream:** `events:bus:dlq` (shared DLQ)

DLQ envelope:
```json
{
  "original_event": { /* full CloudEvent */ },
  "error": "handler timed out after 5000ms",
  "failed_at": "2026-06-24T01:12:55Z",
  "attempts": 3,
  "consumer_group": "bot-cache-invalidator-0"
}
```

### 7. Retry Policy

- 3 attempts total (1 initial + 2 retries)
- Exponential backoff: 1s, 2s, 4s (base=1s, capped at 30s)
- Handles transport/processing failures (deserialization, handler crashes)
- Separate from Prism's domain-specific webhook retry in `DelayedQueue`

### 8. Telemetry

**OTel context propagation:**
- Publish: inject current span → `traceparent`/`tracestate`
- Consume: extract → create child span wrapping handler

**Spans:** `eventbus.publish`, `eventbus.subscribe`, `eventbus.retry`, `eventbus.dlq`

**Metrics:** `eventbus.published`, `eventbus.consumed`, `eventbus.retries`, `eventbus.dlq` (counters), `eventbus.processing_latency_ms` (histogram), `eventbus.dlq_depth` (gauge)

### 9. Naming Conventions

| Resource | Convention | Examples |
|---|---|---|
| Stream keys | `events:{name}` | `events:bus`, `events:polarizer`, `events:bus:dlq` |
| Consumer groups | `{service}-{purpose}[-{shard}]` | `beacon-hub-fanout`, `bot-cache-invalidator-0` |
| Event types | `fun.interchat.{domain}.{action}` | `fun.interchat.broadcast.completed` |
| Source identifiers | `/{service}` | `/prism`, `/iris`, `/bot`, `/beacon`, `/polarizer` |

---

## Event Type Catalog

### On `events:bus` (shared stream)

| # | Current PubSub | CloudEvent `type` | Source | Producers | Consumers |
|---|---|---|---|---|---|
| 1 | `beacon:hub:<id>:messages` | `fun.interchat.broadcast.completed` | `/prism` | Prism | Beacon (`beacon-hub-fanout`), Bot (`bot-broadcast-callbacks`) |
| 2 | `sync:hub_permissions` | `fun.interchat.authz.permissions.invalidated` | `/iris` | Iris | Bot (`bot-cache-invalidator-{shard_id}`) |
| 3 | `sync:hub_connections` | `fun.interchat.hub.connections.changed` | `/bot` | Bot (any shard) | Bot (all shards) |
| 4 | `sync:hub_context` | `fun.interchat.hub.context.changed` | `/bot` | Bot (any shard) | Bot (all shards) |
| 5 | `sync:access_restrictions` | `fun.interchat.moderation.access_restrictions.changed` | `/bot` | Bot (any shard) | Bot (all shards) |
| 6 | `sync:automod` | `fun.interchat.moderation.automod.changed` | `/bot` | Bot (any shard) | Bot (all shards) |
| 7 | `sync:blacklist` | `fun.interchat.moderation.blacklist.changed` | `/bot` | Bot (any shard) | Bot (all shards) |

### On `events:polarizer` (dedicated stream)

| # | Current PubSub | CloudEvent `type` | Source | Producers | Consumers |
|---|---|---|---|---|---|
| 8 | `polarizer:events:<url>` | `fun.interchat.polarizer.result.ready` | `/polarizer` | Polarizer | Bot (`bot-polarizer-results`) |

**Polarizer dual-path:** CloudEvents on `events:polarizer` for durable delivery + per-image PubSub (`polarizer:events:<url>`) for zero-latency cross-process wakeups. Both paths kept; Bot already uses dual-path for polarizer results.

### Event `data` schemas

**`fun.interchat.broadcast.completed`:** (unchanged)
```json
{
  "batch_id": "b_a1b2c3d4",
  "action": "execute",
  "ok_count": 15,
  "fail_count": 2,
  "parent_message_id": "1234567890",
  "hub_id": "hub_abc123",
  "timestamp": 1719202374000
}
```

**`fun.interchat.authz.permissions.invalidated`:**
```json
{ "hub_id": "hub_abc123", "user_id": "user_xyz789" }
```
`user_id` absent/null for hub-wide invalidation.

**`fun.interchat.hub.connections.changed`:**
```json
{ "hub_id": "hub_abc123", "server_id": null }
```

**`fun.interchat.hub.context.changed`:**
```json
{ "user_id": "user_xyz789", "hub_id": null }
```

**`fun.interchat.moderation.access_restrictions.changed`:**
```json
{ "entity": "user", "user_id": "user_xyz789", "server_id": null, "hub_id": null }
```

**`fun.interchat.moderation.automod.changed`:**
```json
{ "scope": "server", "scope_id": "srv_12345" }
```

**`fun.interchat.moderation.blacklist.changed`:**
```json
{ "entity_type": "user", "entity_id": "user_xyz789" }
```

**`fun.interchat.polarizer.result.ready`:**
```json
{
  "url": "https://cdn.discordapp.com/attachments/.../image.png",
  "safe": true,
  "labels": []
}
```

---

## Phase 1: Cut Over Prism → Beacon (Next)

**State:** Both dual-publish and dual-consume are running. Events flow through both PubSub and `events:bus`. The PubSub path is now redundant.

### Steps

1. Verify Beacon is consistently receiving `broadcast.completed` via `events:bus` consumer for ≥24h
2. **Remove dual-publish** in `Prism.FanoutBroadway.Batch.process_batch/10`:
   - Delete `Helpers.redix_command(["PUBLISH", "beacon:hub:#{root_hub_id}:messages", event])` (lines 369)
   - Keep `EventBus.publish("events:bus", ...)` (lines 371-382)
3. **Remove `Beacon.RedisListener`**:
   - Delete `lib/beacon/redis_listener.ex`
   - Remove `Beacon.RedisListener` from `application.ex` supervision tree
   - Remove `:beacon_redix_pubsub` Redix.PubSub connection from supervision tree
4. Clean up config:
   - Remove `beacon_hub_pubsub_prefix` from `runtime.exs` and `.env.example`
   - Remove `Prism.PubSub` from Prism supervision tree (was only used for PubSub publish)

**Validation:** Real broadcast → Beacon WebSocket clients receive events via `events:bus` only. No WebSocket delivery regression. No PubSub traffic on `beacon:hub:*`.

**Rollback:** Re-add dual-publish and RedisListener. No data loss.

---

## Phase 2: Iris Go EventBus Adapter (Publish-Only)

Iris publishes to exactly one PubSub channel (`sync:hub_permissions`) from two call sites. It needs a publish-only Go adapter.

### Steps

1. **Create Go `eventbus` package** in `~/Developments/iris/eventbus/`:
   - `publisher.go` — CloudEvent envelope builder + `XADD` to `events:bus`
   - `telemetry.go` — OTel spans (`eventbus.publish`), traceparent injection
   - `types.go` — CloudEvent struct
   - Add OTel dependencies to `go.mod` (`go.opentelemetry.io/otel`, `otel/trace`, `otel/sdk` if not already present)
   - Use existing `*redis.Client` from `main.go`
   - Source: `/iris`

2. **Dual-publish** in `service/authz.go`:
   - In `InvalidateUserPermissions` and `InvalidateHubPermissions`:
     - Keep existing `s.invalidator.Publish(ctx, "sync:hub_permissions", string(b))`
     - Add `eventbus.Publish(ctx, "events:bus", "fun.interchat.authz.permissions.invalidated", payload)`
   - Log but don't fail on EventBus publish errors (fire-and-forget during dual-write)

3. **Add config** for stream key, maxlen, source (defaults: `events:bus`, `100000`, `/iris`)

4. **Write unit tests** using the existing fake `RedisInvalidator` pattern

**Validation:** Trigger permission invalidation → verify event appears in both `sync:hub_permissions` PubSub (via `redis-cli PSUBSCRIBE`) and `events:bus` stream (via `redis-cli XREAD`).

---

## Phase 3: Bot Python EventBus Adapter + CacheInvalidator

The Bot needs a full Python adapter: publish AND subscribe.

### 3a. Build Python EventBus Adapter

**Location:** `~/Developments/interchat.py/apps/bot/services/event_bus/`

```python
services/event_bus/
  __init__.py          # EventBus class (public API)
  publisher.py         # CloudEvent envelope + XADD
  consumer.py          # Consumer group loop (asyncio.Task)
  retry.py             # Exponential backoff
  dlq.py               # DLQ XADD wrapper
  telemetry.py         # OTel spans + metrics
  types.py             # CloudEvent dataclass (msgspec)
```

Key patterns:
- Use existing `redis_client` singleton from `utils.constants`
- `opentelemetry.propagators.inject()` / `extract()` (OTel already instrumented in bot — see `utils/telemetry.py`)
- Consumer: `asyncio.Task` with `asyncio.Event` for graceful shutdown
- `msgspec.json` for serialization (already used throughout codebase)

### 3b. Add CacheInvalidator Consumer

Update `utils/cache/invalidation.py`:

1. Add `EventBus.subscribe("events:bus", f"bot-cache-invalidator-{shard_id}", router_handler)` alongside existing PubSub subscriptions
2. **Router handler** dispatches by CloudEvent `type`:

| CloudEvent `type` | → Existing handler |
|---|---|
| `fun.interchat.authz.permissions.invalidated` | `_handle_hub_permissions` |
| `fun.interchat.hub.connections.changed` | `_handle_hub_connections` |
| `fun.interchat.hub.context.changed` | `_handle_hub_context` |
| `fun.interchat.moderation.access_restrictions.changed` | `_handle_access_restrictions` |
| `fun.interchat.moderation.automod.changed` | `_handle_automod` |
| `fun.interchat.moderation.blacklist.changed` | `_handle_blacklist` |

3. Extract shard ID from bot instance (`bot.shard_id` or `bot.shard_count`)
4. Keep existing PubSub subscriptions active (dual-consume)

### 3c. Dual-Publish Cache Invalidation Events

Update all publishers to dual-publish alongside existing PubSub:

| Publisher file(s) | PubSub channel → | CloudEvent `type` |
|---|---|---|
| `connectionService.py` (6 sites), `hub/helpers/connection.py`, `blocklistService.py` (2) | `sync:hub_connections` → | `fun.interchat.hub.connections.changed` |
| `moderationService.py` (7 sites) | `sync:access_restrictions` → | `fun.interchat.moderation.access_restrictions.changed` |
| `utils/automod/filterService.py` | `sync:automod` → | `fun.interchat.moderation.automod.changed` |
| `moderation/blacklist_filter.py` | `sync:blacklist` → | `fun.interchat.moderation.blacklist.changed` |
| `permissionService.py` | `sync:hub_context` → | `fun.interchat.hub.context.changed` |

**Validation:** Trigger each invalidation type → verify received via both PubSub and `events:bus` consumer.

---

## Phase 4: Bot Callback Migration (Pull from Reply Index)

**Design decision:** Bot will consume `broadcast.completed` CloudEvents and pull per-target results from Prism's reply index, rather than pushing full callback payloads through the event bus. This is cleaner — the event bus carries lightweight signals, and detailed results are pulled on demand.

### Steps

1. **Add callback consumer** to `PrismCallbackListener`:
   - Subscribe to `events:bus` with consumer group `bot-broadcast-callbacks`
   - Route `fun.interchat.broadcast.completed` → new handler

2. **New handler: pull from reply index**
   - Extract `batch_id` and `hub_id` from CloudEvent
   - Query `reply_index` Redis keys (format: `prism:reply:{batch_id}:*`) — already stored by Prism via `FanoutBroadway.Batch.store_reply_index/2`
   - Process per-target message IDs, failures, etc. same as existing callback flow
   - Keep existing `prism:stream:callbacks` consumer active (dual-consume)

3. **Validation:** Real broadcast → callbacks processed from both `events:bus` (pulled) and `prism:stream:callbacks` (pushed). Verify identical callback behavior.

4. **Cut over:** Remove `prism:stream:callbacks` consumer. Optionally deprecate the callback stream (can keep publishing for backward compatibility).

---

## Phase 5: Polarizer CloudEvents (Dual-Path)

Polarizer publishes NSFW detection results. Current pattern: `XADD polarizer:results` + `PUBLISH polarizer:events:<url>` as an atomic pipeline.

### Steps

1. **Add Rust `eventbus` module** in `~/Developments/polarizer/src/eventbus/`:
   - `publisher.rs` — CloudEvent envelope builder + `XADD` to `events:polarizer`
   - `mod.rs` — module root with `publish()` function
   - Use existing `redis::aio::ConnectionManager` from `Pipeline`
   - Source: `/polarizer`
   - Serialize with `serde_json` (already in deps)

2. **Dual-publish** in `redis_stream.rs::publish_result()`:
   - Keep existing atomic pipeline: `XADD polarizer:results` + `PUBLISH polarizer:events:<url>`
   - Add `eventbus::publish("events:polarizer", "fun.interchat.polarizer.result.ready", data)` as a separate call (non-atomic — CloudEvent is best-effort supplement)
   - The existing PubSub per-image channel stays for low-latency wakeups

3. **Update Bot's `PolarizerCallbackListener`**:
   - Add consumer for `events:polarizer` with consumer group `bot-polarizer-results`
   - Route `fun.interchat.polarizer.result.ready` → existing result processing
   - Keep existing `polarizer:results` stream consumer + `polarizer:events:*` PubSub listener (dual-consume)

4. **Validation:** Process NSFW image → verify result received via all three paths: CloudEvent stream, existing results stream, PubSub.

5. **Cut over (future):** Once CloudEvent path is proven, the `polarizer:results` stream consumer could be deprecated in favor of `events:polarizer`. Per-image PubSub stays for zero-latency.

---

## Phase 6: Cut-Overs and Cleanup

After all dual-paths are validated:

### 6a. Iris cut-over
- Remove `s.invalidator.Publish()` from Iris `authz.go`
- Remove `sync:hub_permissions` from Bot's `CacheInvalidator` PubSub subscriptions

### 6b. Bot cache invalidation cut-over
- Remove all `redis_client.publish("sync:*", ...)` calls from publisher files
- Remove `sync:*` PubSub subscriptions from `CacheInvalidator._listen_loop()`
- Remove the PubSub listen loop entirely

### 6c. Bot callback cut-over
- Remove `prism:stream:callbacks` consumer
- Optionally deprecate callback stream publish in Prism

### 6d. Final cleanup
- Remove `Prism.PubSub` from Prism supervision tree
- Remove `beacon_hub_pubsub_prefix` config
- Remove all `sync:*` channel references from code and docs
- Update `CONTRACT.md` files in Prism and Polarizer with CloudEvents documentation

---

## Implementation Order

1. ✅ ~~Elixir EventBus adapter (Prism)~~
2. ✅ ~~Prism dual-publish (broadcast.completed → events:bus + PubSub)~~
3. ✅ ~~Beacon EventBus adapter + dual-consume~~
4. **Phase 1: Prism → Beacon cut-over** (remove PubSub dual-path)
5. **Phase 2: Iris Go EventBus adapter + dual-publish**
6. **Phase 3: Bot Python EventBus adapter**
7. **Phase 3b: Bot CacheInvalidator dual-consume** (`events:bus` + PubSub)
8. **Phase 3c: Bot cache invalidation dual-publish** (all `sync:*` → `events:bus`)
9. **Phase 4: Bot callback migration** (pull from reply index)
10. **Phase 5: Polarizer CloudEvents** (Rust adapter + dual-path)
11. **Phase 6a: Iris + Bot cache cut-over** (remove `sync:hub_permissions` PubSub)
12. **Phase 6b: Bot cache invalidation cut-over** (remove all `sync:*` PubSub)
13. **Phase 6c: Bot callback cut-over** (remove `prism:stream:callbacks` consumer)
14. **Phase 6d: Final cleanup** (PubSub infrastructure, docs)

---

## Known Issues in Completed Phase 0 Code

These are non-blocking and can be addressed anytime:

| Issue | Location | Severity | Fix |
|---|---|---|---|
| `eventbus.retry` OTel span not called | `Prism.EventBus.Consumer` line 262 | Low | Call `Telemetry.span_retry/4` in `invoke_with_retry` (span_retry exists but is dead code) |
| `eventbus.dlq_depth` gauge not implemented | Missing | Low | Add periodic `XLEN events:bus:dlq` gauge in metrics logger |
| GenServer blocks during retry backoff | `Prism.EventBus.Consumer` line 261 | Low | `Process.sleep` blocks the GenServer; acceptable for single-consumer but worth noting |
| `CONTRACT.md` not updated | Prism repo | Medium | Add CloudEvents/EventBus documentation to Prism's CONTRACT.md |
| Stream Trimmer doesn't cover `events:bus` / `events:bus:dlq` | `Prism.StreamTrimmer` | Low | `MAXLEN ~` on XADD is sufficient, but adding periodic trim is safer |
| Config getters duplicated | `Prism.Config` + `Prism.EventBus.Config`, same in Beacon | Low | Consolidate into single config module |

---

## Risks and Rollback

| Risk | Mitigation |
|---|---|
| Dual-publish overhead during migration | Acceptable — temporary, phased cut-over |
| Consumer group naming collisions | Use unique names with hostname/shard-ID suffix |
| Event schema drift between services | CloudEvents enforces envelope; data schema documented here |
| DLQ fills up unnoticed | DLQ depth metric + alert threshold |
| OTel tracing breaks | Verify trace context propagation in each phase validation |
| Redis Streams memory growth | `MAXLEN ~ 100000` on all streams |
| Per-shard consumer groups on Bot restart | Each shard creates `bot-cache-invalidator-{shard_id}` on start; XAUTOCLAIM recovers stale messages if a shard restarts with different consumer name |

**Rollback per phase:** Each phase uses dual-write or dual-consume. Rollback is removing the new path and falling back to the old path. No data loss risk.

---

## Unresolved Questions

- DLQ monitoring/replay tooling — deferred until post-migration
- Kafka adapter — out of scope; adapter interface must not assume Redis-specific primitives
- Polarizer dead-letter stream — Polarizer has a TODO for a DLQ on pipeline failures (separate from event bus DLQ)
