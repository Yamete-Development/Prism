# Event Bus Migration — Handoff Document

**Date:** 2026-06-24
**Status:** All cut-over and known-issue cleanup complete. No PubSub paths remain. 13 known issues resolved across 4 services.

---

## Project Map

| Service | Language | Repo Path | Role |
|---|---|---|---|
| **Prism** | Elixir | `~/Developments/interchat-broadcast-worker` | Producer: `broadcast.completed` → `events:bus` |
| **Beacon** | Elixir | `~/Developments/beacon` | Consumer: `events:bus` → Phoenix WebSocket |
| **Iris** | Go | `~/Developments/iris` | Producer: `permissions.invalidated` → `events:bus` |
| **Bot** | Python | `~/Developments/interchat.py` | Producer + Consumer: cache inval, callbacks, polarizer results |
| **Polarizer** | Rust | `~/Developments/polarizer` | Producer: `polarizer.result.ready` → `events:polarizer` |

### Stream Topology

```
events:bus          → Shared inter-service stream (MAXLEN ~ 100000)
events:bus:dlq      → Dead-letter queue
events:polarizer    → Dedicated NSFW results stream (MAXLEN ~ 100000)
```

### Consumer Groups

| Group | Service | Stream | Notes |
|---|---|---|---|
| `beacon-hub-fanout` | Beacon | `events:bus` | Single consumer |
| `bot-cache-invalidator-{shard_id}` | Bot | `events:bus` | Per-shard, every shard receives every event |
| `bot-broadcast-callbacks` | Bot | `events:bus` | Planned (Phase 4, not yet implemented) |
| `bot-polarizer-results` | Bot | `events:polarizer` | Planned (Phase 5b, not yet implemented) |

---

## CloudEvents v1.0 Envelope (Universal Across All Services)

```json
{
  "specversion": "1.0",
  "type": "fun.interchat.broadcast.completed",
  "source": "/prism",
  "id": "evt_a1b2c3d4e5f6",
  "time": "2026-06-24T01:12:54Z",
  "datacontenttype": "application/json",
  "data": { ... },
  "traceparent": "...",
  "tracestate": "..."
}
```

Extension attributes: `traceparent`, `tracestate` (injected by adapter on publish, extracted on consume).

Source values: `/prism`, `/beacon`, `/iris`, `/bot`, `/polarizer`.

---

## Event Type Catalog

### On `events:bus`

| # | CloudEvent `type` | Source | Producers | Consumers |
|---|---|---|---|---|
| 1 | `fun.interchat.broadcast.completed` | `/prism` | Prism | Beacon, Bot (planned) |
| 2 | `fun.interchat.authz.permissions.invalidated` | `/iris` | Iris | Bot |
| 3 | `fun.interchat.hub.connections.changed` | `/bot` | Bot | Bot |
| 4 | `fun.interchat.hub.context.changed` | `/bot` | Bot | Bot |
| 5 | `fun.interchat.moderation.access_restrictions.changed` | `/bot` | Bot | Bot |
| 6 | `fun.interchat.moderation.automod.changed` | `/bot` | Bot | Bot |
| 7 | `fun.interchat.moderation.blacklist.changed` | `/bot` | Bot | Bot |

### On `events:polarizer`

| # | CloudEvent `type` | Source | Producers | Consumers |
|---|---|---|---|---|
| 8 | `fun.interchat.polarizer.result.ready` | `/polarizer` | Polarizer | Bot (planned) |

---

## Per-Service Implementation Details

### Prism (`~/Developments/interchat-broadcast-worker`)

**Adapter:** `lib/prism/event_bus/` — 10 files, Elixir GenServer + Redis Streams, with pluggable transport behaviour.

| File | Purpose |
|---|---|
| `event_bus.ex` | Public API: `publish/2`, `publish_cloud_event/3`, `subscribe/1`. |
| `publisher.ex` | CloudEvent envelope builder, OTel span injection, delegates to `Transport` facade. |
| `consumer.ex` | GenServer with `XREADGROUP` loop, retry (3 attempts), DLQ, `XAUTOCLAIM`. OTel spans wired: `span_consume`, `span_retry`, `span_dlq`. |
| `transport.ex` | Facade — delegates to `Prism.EventBus.Config.transport_backend()` (no direct `Application.get_env` bypass). |
| `transport/behaviour.ex` | `@behaviour` with 5 `@callback`s + `system_name/0`. |
| `transport/redis.ex` | Redis Streams implementation (`XADD`, `XREADGROUP`, `XACK`, `XAUTOCLAIM`). |
| `message.ex` | `%EventBus.Message{id, stream, data}` — normalized message type. |
| `retry.ex` | Exponential backoff: `base × 2^(attempt-1)`, capped at 30s. |
| `dlq.ex` | Dead-letter queue publisher with failure metadata. |
| `telemetry.ex` | `:telemetry` events + OTel span creation (`span_consume`, `span_retry`, `span_dlq`). |
| `config.ex` | Canonical adapter config (11 getters). **No duplicates in `Prism.Config`.** |

**Transport bypass fix:** `Prism.EventBus.Transport.backend/0` now calls `Prism.EventBus.Config.transport_backend()` instead of `Application.get_env(:prism, :event_bus_transport_backend, Redis)`. Removed unused `alias Prism.EventBus.Transport.Redis`.

**Span wiring pattern (important for future work):** Uses `if/else` expression assignment to bind handler result, then separate `case` for `Enum.reduce_while` halting:
```elixir
result = if attempt > 1 do
  s_ctx = Telemetry.span_retry(...)
  invoke_result = invoke_handler(...)
  if s_ctx, do: end_span with status
  invoke_result
else
  invoke_handler(...)
end
case result do ... end
```
This avoids variable scoping issues with nested `case` inside `if/else` branches.

**Config dedup:** 11 event-bus getters removed from `Prism.Config`. `Prism.EventBus.Config` is the canonical module. Only external caller was `Prism.MetricsLogger` — updated to `Prism.EventBus.Config.events_dlq_stream()`.

**Stream Trimmer:** No entries added for `events:bus`/`events:bus:dlq`. Prism is producer-only for these streams (no consumer group). MINID trimming requires a consumer group for XPENDING; would be a permanent NOGROUP skip. MAXLEN ~ 100000 on XADD handles capping.

**Test fix:** `test/prism/event_bus_test.exs` — added `flush_test_mailbox()` in setup + destroy all consumer group variants (`-retry-test`, `-ack-test`, `-opts-test`) to fix inter-test contamination race.

**Tests:** 12 tests, 0 failures (was flaky, now stable across 3+ runs).

### Beacon (`~/Developments/beacon`)

**Adapter:** `lib/beacon/event_bus/` — 10 files, identical architecture to Prism's adapter.

**Integration:**
- `lib/beacon/application.ex` — `Beacon.EventBus.Consumer` in supervision tree, consumer group `beacon-hub-fanout`
- `lib/beacon/event_handler.ex` — `handle_broadcast_completed/2` handler broadcasts to `Beacon.PubSub` → WebSocket

**Span wiring:** `invoke_with_retry/3` uses same `if/else` expression pattern as Prism. `span_retry` wired for attempts > 1 with `set_status/2` on error.

**Config dedup:** 11 event-bus getters removed from `Beacon.Config`. Updated `application.ex` to use `Beacon.EventBus.Config.events_stream()`. Updated test references from `Beacon.Config.event_source()` → `Beacon.EventBus.Config.event_source()`.

**Deleted in Phase 1:**
- `lib/beacon/redis_listener.ex` — old PubSub subscriber
- `lib/beacon/config.ex` — `beacon_hub_pubsub_prefix` removed
- `.env.example` — PubSub section removed

**Tests:** 9 tests, 0 failures (stable across 3+ runs).

### Iris (`~/Developments/iris`)

**Adapter:** `eventbus/` — 4 Go files, publish-only.

**Key files:** `publisher.go`, `transport.go` (`Publisher` interface + `SystemName()`), `redis_publisher.go`, `nop_publisher.go` (test mock), `publisher_test.go`.

**All PubSub removed.** `s.invalidator.Publish()` calls removed from `InvalidateUserPermissions` and `InvalidateHubPermissions`.

### Bot (`~/Developments/interchat.py`)

**Adapter:** `apps/bot/services/event_bus/` — 7 Python files.

**Critical fixes applied (this session):**
- **`transport_redis.py`:** Fixed `read_batch()` indentation bug — `return messages` was unconditionally early-returning instead of inside `if not result:`. `system_name()` was absorbed into `read_batch()` body at wrong indent. Message-parsing loop now at correct indent (inside `read_batch`).
- **`invalidation.py`:** Removed `_get_shard_id()` (tried `from main import _bot` which doesn't exist). `CacheInvalidator.start()` now accepts `shard_id: str = "0"` parameter.
- **`lifecycle.py`:** Updated caller to `await invalidator.start(str(bot.shard_id))`.

**All 20 PubSub publisher sites removed.** Only CloudEvent event bus path remains.

### Polarizer (`~/Developments/polarizer`)

**Adapter:** `src/eventbus.rs` — 1 Rust file, publish-only.

**Fixes applied (this session):**
- `#[allow(dead_code)]` on `system_name()` trait method and `RedisEventBus` impl (reserved for future OTel `messaging.system`).
- Added 7 unit tests in `#[cfg(test)]` module:
  - `test_build_envelope_produces_valid_cloud_event` — specversion, source, ID format (`evt_` + 32 hex), RFC3339 time
  - `test_generate_id_format` — uniqueness, hex digits
  - `test_format_utc_rfc3339_format` — valid timestamp structure
  - `test_build_envelope_json_serializable` — round-trip through serde
  - `test_build_envelope_id_uniqueness` — 100 IDs all unique
  - `test_mock_event_bus_publish` — `MockEventBus` trait impl captures args
  - `test_civil_from_days_known_date` — epoch (1970-01-01) and known date (1990-01-01)

**Tests:** 7 tests, 0 failures. `cargo check` clean (no dead_code warnings).

---

## Design Decisions

1. **CloudEvents v1.0 JSON** — all events use this envelope. Extension attributes: `traceparent`, `tracestate` for OTel propagation.
2. **Per-shard consumer groups** for Bot CacheInvalidator — `bot-cache-invalidator-{shard_id}` ensures every shard independently processes every invalidation event.
3. **Pluggable transport abstraction** — every service has a compiler-enforced transport contract (Elixir `@behaviour`, Go `interface`, Python `Protocol`, Rust `trait`). Set `EVENT_BUS_TRANSPORT` to swap backends.
4. **Dynamic `messaging_system` OTel attribute** — all services derive `messaging.system` from the transport backend via `system_name()`.
5. **Span wiring pattern** — use `if/else` expression assignment to bind handler result, then separate `case` for branching. Avoid nested `case` inside `if/else` branches (Elixir variable scoping).
6. **`OpenTelemetry.Span.set_status/2`** — takes `(span_ctx, :error)`. The 3-arg form (`set_status/3`) does not exist.
7. **Stream Trimmer** — no entries for producer-only streams. MINID trimming requires consumer groups; MAXLEN on XADD is sufficient for capping.
8. **Test isolation** — flush process mailbox in setup + destroy all consumer group variants to prevent inter-test contamination in shared-Redis tests.

---

## Configuration Reference

### Common (all services)

| Env Var | Default | Description |
|---|---|---|
| `EVENTS_STREAM` | `events:bus` | Shared inter-service event stream |
| `EVENTS_DLQ_STREAM` | `events:bus:dlq` | Dead-letter queue |
| `EVENTS_STREAM_MAXLEN` | `100000` | Approximate stream length cap |
| `EVENT_SOURCE` | `/{service}` | CloudEvent source identifier |

### Prism-specific

`EVENT_BUS_MAX_RETRIES` (3), `EVENT_BUS_RETRY_BACKOFF_BASE_MS` (1000), `EVENT_BUS_RETRY_BACKOFF_MAX_MS` (30000), `EVENT_BUS_CONSUMER_BATCH_SIZE` (10), `EVENT_BUS_CONSUMER_BLOCK_MS` (3000), `EVENT_BUS_STALE_CLAIM_IDLE_MS` (30000), `EVENT_BUS_STALE_CLAIM_INTERVAL_MS` (60000).

### Beacon-specific

Same env vars as Prism.

### Polarizer-specific

`EVENTS_STREAM_KEY` (default: `events:polarizer`), `EVENTS_STREAM_MAXLEN` (default: `100000`).

---

## Verification Results (All Services)

| Check | Result |
|---|---|
| Prism `mix compile --no-deps-check` | ✅ Clean, 0 warnings |
| Prism `mix test test/prism/event_bus_test.exs` | ✅ 12 tests, 0 failures (stable across 3 runs) |
| Beacon `mix compile --no-deps-check` | ✅ Clean, 0 warnings |
| Beacon `mix test test/beacon/event_bus_test.exs` | ✅ 9 tests, 0 failures (stable across 3 runs) |
| Polarizer `cargo test` | ✅ 7 tests, 0 failures |
| Polarizer `cargo check` | ✅ Clean, 0 dead_code warnings |
| Bot `rg "redis_client\.publish\(" apps/bot/` | ✅ Zero results |
| Bot `transport_redis.py` indentation fix | ✅ Verified (read_batch conditional return, system_name as own method) |
| Bot `_get_shard_id` removed | ✅ Verified (start takes shard_id param) |
| Bot `lifecycle.py` caller updated | ✅ Verified (passes str(bot.shard_id)) |

---

## What's Left (Lower Priority)

- **Phase 4**: Bot callback migration — consume `broadcast.completed` from `events:bus`, pull per-target results from Prism reply index.
- **Phase 5b**: Bot Polarizer consumer — subscribe to `events:polarizer` with `bot-polarizer-results` group.
- **Kafka adapter**: Implement the transport contract for Kafka. Only the adapter module needs to be written.
- **DLQ monitoring**: Implement `eventbus.dlq_depth` gauge across remaining services (Bot, Iris).
