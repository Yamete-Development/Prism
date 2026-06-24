# Make EventBus Transport Truly Pluggable Across All Services

**Date:** 2026-06-24
**Status:** ✅ Implemented — see `.kilo/HANDOFF.md` for current state.

---

## Goal

Introduce a compiler-enforced transport abstraction (behaviour/interface/trait/protocol) in every service's EventBus adapter so the backend can be swapped from Redis Streams to Kafka (or any future broker) by changing a single configuration variable, without modifying publisher, consumer, or DLQ logic.

Current state: every service documents the transport as "pluggable" but none has an actual abstraction — all code is hard-coupled to Redis Stream commands (`XADD`, `XREADGROUP`, `XACK`, `XAUTOCLAIM`).

---

## Design Decisions (Resolved)

| Decision | Choice |
|---|---|
| Scope | All 5 services (Prism, Beacon, Iris, Bot, Polarizer) |
| Kafka implementation | Abstraction only — no Kafka adapter in this phase |
| Consumer-side abstraction | Full lifecycle: subscribe, read, ack, stale claim |
| Shared Elixir code | Copy-paste per repo (no shared Hex package) |
| Backend selection | `EVENT_BUS_TRANSPORT` env var, default `"redis"` |
| Backward compatibility | Existing `Transport` module name preserved as a facade/alias to the Redis adapter |

---

## Common Transport Contract

Every language must expose these 5 operations. The `claim_stale` operation may be a no-op for backends that handle stale message recovery differently (e.g., Kafka consumer-group rebalancing).

| # | Operation | Redis Equivalent | Kafka Equivalent (future) |
|---|---|---|---|
| 1 | `publish(stream, payload, maxlen) → :ok \| error` | `XADD ... MAXLEN ~` | `producer.send(topic, message)` |
| 2 | `create_consumer_group(stream, group) → :ok \| error` | `XGROUP CREATE ... MKSTREAM` | No-op (auto-created on subscribe) |
| 3 | `read_batch(stream, group, consumer, block_ms, batch_size) → [Message]` | `XREADGROUP ... BLOCK ... COUNT` | `consumer.poll(timeout)` |
| 4 | `ack(stream, group, message_ids) → :ok` | `XACK` | `consumer.commit(offsets)` |
| 5 | `claim_stale(stream, group, consumer, idle_ms, count) → [Message]` | `XAUTOCLAIM` | No-op or log warning |

### Normalized Message Type

Each language defines a transport-agnostic message struct so consumers don't parse Redis-specific wire formats:

- **Elixir**: `%EventBus.Message{id: String.t(), stream: String.t(), data: String.t()}`
- **Python**: `EventBusMessage(id: str, stream: str, data: bytes)`
- **Go**: N/A (publisher-only service)
- **Rust**: N/A (publisher-only service)

---

## Per-Service Implementation Plan

---

### 1. Prism (`~/Developments/interchat-broadcast-worker`)

**New files:**
- `lib/prism/event_bus/transport/behaviour.ex` — `@behaviour` with 5 `@callback` specs + `EventBus.Message` struct
- `lib/prism/event_bus/transport/redis.ex` — Redis implementation (extracted from current `transport.ex`)

**Modified files:**
- `lib/prism/event_bus/transport.ex` — becomes a facade that reads `EVENT_BUS_TRANSPORT` config at compile time and delegates to the correct backend module. Defaults to `Transport.Redis`.
- `lib/prism/event_bus/publisher.ex` — replace `alias Prism.EventBus.Transport` with dynamic resolution via `@transport` module attribute
- `lib/prism/event_bus/consumer.ex` — same dynamic resolution; parse raw transport results into `%EventBus.Message{}` structs before processing
- `lib/prism/event_bus/dlq.ex` — same dynamic resolution
- `lib/prism/event_bus.ex` — remove public `redis_command/1`; mark as `@doc false` or delegate to transport backend
- `lib/prism/event_bus/config.ex` — add `transport_backend/0` getter
- `lib/prism/config.ex` — remove duplicate EventBus getters (already in `EventBus.Config`), or add `transport_backend` to the duplicate set
- `config/runtime.exs` — add `event_bus_transport` mapping from `EVENT_BUS_TRANSPORT` env var

**Tests to update:**
- `test/prism/event_bus_test.exs` — update any tests that reach into `Transport` internals or use `redis_command/1` directly. Add tests verifying the behaviour dispatch works correctly.

**Order of work:**
1. Create `transport/behaviour.ex` (contract definition)
2. Create `transport/redis.ex` (extract existing code)
3. Update `transport.ex` to delegate to Redis adapter
4. Update `publisher.ex`, `consumer.ex`, `dlq.ex` to use dynamic resolution + `Message` struct
5. Update `event_bus.ex` public API
6. Update config + `runtime.exs`
7. Run `mix test` — all 110 existing tests must pass unchanged

---

### 2. Beacon (`~/Developments/beacon`)

Identical structure to Prism. Same files, same changes.

**Order of work:** Same as Prism, run `mix test` to verify 11 tests pass unchanged.

---

### 3. Iris (`~/Developments/iris`)

Iris is publisher-only — no consumer or DLQ concerns.

**New files:**
- `eventbus/transport.go` — `Publisher` interface + `TransportBackend` enum

```go
package eventbus

import "context"

type Publisher interface {
    Publish(ctx context.Context, stream string, eventType string, data interface{}, opts ...PublishOption) error
}

type TransportBackend string

const (
    TransportRedis TransportBackend = "redis"
    TransportKafka TransportBackend = "kafka"
)
```

- `eventbus/redis_publisher.go` — `RedisPublisher` struct (extracted from current `publisher.go`)

**Modified files:**
- `eventbus/publisher.go` — keep `CloudEvent` struct and `build_envelope`; remove standalone `Publish()` function (becomes method on `RedisPublisher`)
- `service/authz.go` — replace concrete `*redis.Client` field with `eventbus.Publisher` interface
- `main.go` — construct `RedisPublisher` and inject into `AuthZService`

**Order of work:**
1. Create `transport.go` (interface + enum)
2. Create `redis_publisher.go` (extract existing code)
3. Refactor `publisher.go` (remove standalone function)
4. Update `authz.go` (interface injection)
5. Update `main.go` (wire-up)
6. `go build ./...` must succeed; `go test ./eventbus/...` must pass

---

### 4. Bot (`~/Developments/interchat.py`)

Bot has both publisher and consumer. This is the most complex change.

**New files:**
- `apps/bot/services/event_bus/transport.py` — `EventBusTransport` Protocol + `EventBusMessage` dataclass + transport factory

```python
from typing import Protocol, runtime_checkable
from dataclasses import dataclass

@dataclass
class EventBusMessage:
    id: str
    stream: str
    data: bytes

@runtime_checkable
class EventBusTransport(Protocol):
    async def publish(self, stream: str, payload: bytes, *, maxlen: int = 100000) -> bool: ...
    async def create_consumer_group(self, stream: str, group: str) -> None: ...
    async def read_batch(self, stream: str, group: str, consumer: str, *, block_ms: int, batch_size: int) -> list[EventBusMessage]: ...
    async def ack(self, stream: str, group: str, message_ids: list[str]) -> None: ...
    async def claim_stale(self, stream: str, group: str, consumer: str, *, min_idle_ms: int, count: int) -> list[EventBusMessage]: ...

def get_transport() -> EventBusTransport:
    backend = os.getenv("EVENT_BUS_TRANSPORT", "redis")
    if backend == "redis":
        from utils.constants import redis_client
        return RedisStreamTransport(redis_client)
    raise ValueError(f"Unknown transport backend: {backend}")
```

- `apps/bot/services/event_bus/transport_redis.py` — `RedisStreamTransport` class implementing `EventBusTransport`

**Modified files:**
- `apps/bot/services/event_bus/publisher.py` — `publish_cloud_event()` accepts a `transport: EventBusTransport` parameter instead of calling `redis_client.xadd()` directly
- `apps/bot/services/event_bus/consumer.py` — `EventBusConsumer.__init__` accepts `transport: EventBusTransport`; `_consume_loop` calls `self.transport` methods instead of `redis_client.xreadgroup/xack/xautoclaim`
- `apps/bot/services/event_bus/dlq.py` — `send_to_dlq()` accepts `transport` parameter
- `apps/bot/services/event_bus/__init__.py` — update exports
- `utils/cache/invalidation.py` — `CacheInvalidator` constructs transport via `get_transport()` and passes it to `EventBusConsumer`
- `services/connectionService.py`, `services/permissionService.py`, and the 12 remaining dual-publish sites — pass transport to `publish_cloud_event()` (or use module-level transport singleton)

**Order of work:**
1. Create `transport.py` (Protocol + Message + factory)
2. Create `transport_redis.py` (extract existing Redis calls)
3. Update `publisher.py` to accept transport
4. Update `consumer.py` to accept transport
5. Update `dlq.py` to accept transport
6. Update `CacheInvalidator` wire-up
7. Update all dual-publish call sites
8. Run existing Bot tests

---

### 5. Polarizer (`~/Developments/polarizer`)

Polarizer is publisher-only.

**New files:**
- `src/eventbus/traits.rs` — `EventBus` async trait

```rust
#[async_trait]
pub trait EventBus: Send + Sync {
    async fn publish(&self, event_type: &str, data: serde_json::Value) -> anyhow::Result<()>;
}
```

- `src/eventbus/redis.rs` — `RedisEventBus` struct (extracted from current `eventbus.rs`)

**Modified files:**
- `src/eventbus.rs` — keep `CloudEvent` struct and `build_envelope`; remove standalone `publish()` function
- `src/config.rs` — add `transport` field (`TransportBackend` enum, parsed from `EVENT_BUS_TRANSPORT` env var)
- `src/redis_stream.rs` — call site: replace `crate::eventbus::publish(&mut conn, ...)` with `self.eventbus.publish(...)`
- `src/main.rs` — construct `RedisEventBus` and inject into pipeline

**Order of work:**
1. Create `src/eventbus/traits.rs` (trait definition)
2. Create `src/eventbus/redis.rs` (extract existing code)
3. Refactor `src/eventbus.rs` (remove standalone function)
4. Add `transport` to `AppConfig` in `src/config.rs`
5. Update `src/redis_stream.rs` call site
6. Wire up in `src/main.rs`
7. `cargo build` must succeed

---

## Configuration Changes (All Services)

Every service adds one new environment variable:

| Service | Env Var | Default | Values |
|---|---|---|---|
| Prism | `EVENT_BUS_TRANSPORT` | `redis` | `redis`, `kafka` (future) |
| Beacon | `EVENT_BUS_TRANSPORT` | `redis` | `redis`, `kafka` (future) |
| Iris | `EVENT_BUS_TRANSPORT` | `redis` | `redis`, `kafka` (future) |
| Bot | `EVENT_BUS_TRANSPORT` | `redis` | `redis`, `kafka` (future) |
| Polarizer | `EVENT_BUS_TRANSPORT` | `redis` | `redis`, `kafka` (future) |

Existing Redis-specific env vars (`EVENTS_STREAM`, `EVENTS_DLQ_STREAM`, `EVENTS_STREAM_MAXLEN`, etc.) remain unchanged as they are passed to the Redis adapter at construction time. A future Kafka adapter would use Kafka-specific env vars instead.

---

## Test Strategy

### Prism + Beacon (Elixir)
- Existing tests (`test/prism/event_bus_test.exs`, `test/beacon/event_bus_test.exs`) verify the full publish → consume → handler path. These must continue to pass without modification.
- Add a new test that verifies the backend selection logic: when `EVENT_BUS_TRANSPORT=redis`, the correct adapter is resolved.
- Add a contract test that verifies `Transport.Redis` implements all 5 `@callback` functions.

### Iris (Go)
- Existing `eventbus/publisher_test.go` verifies CloudEvent envelope format. Must continue to pass.
- Add test for `RedisPublisher` implementing `Publisher` interface.

### Bot (Python)
- Existing integration tests for `CacheInvalidator` must pass.
- Add a unit test verifying `RedisStreamTransport` satisfies `EventBusTransport` Protocol.

### Polarizer (Rust)
- No pre-existing tests. Add a basic test that `RedisEventBus` compiles as `dyn EventBus`.

---

## Migration & Rollback

### Deployment order
Since Prism and Beacon share the same Elixir code structure:
1. Deploy Prism first (producer-only, lowest risk)
2. Deploy Beacon second (consumer, verify WebSocket delivery still works)
3. Deploy Iris (producer-only)
4. Deploy Bot (producer + consumer, most complex)
5. Deploy Polarizer (producer-only)

### Backward compatibility
- The `Transport` module name is preserved as a facade. Any internal code referencing `Prism.EventBus.Transport` or `Beacon.EventBus.Transport` will continue to work.
- The `redis_command/1` helper is retained (marked `@doc false`) for backward compat, but external callers should not use it.

### Rollback
- Setting `EVENT_BUS_TRANSPORT=redis` (the default) preserves exact current behavior. No runtime behavior changes unless a new backend is selected.

---

## Risks

| Risk | Mitigation |
|---|---|
| Performance regression from dynamic dispatch | Elixir uses compile-time `Application.compile_env` in module attribute — zero runtime overhead. Python and Go use a factory called once at startup. Rust uses a trait object constructed once. |
| Message type mismatch introducing parsing bugs | The normalized `EventBus.Message` struct wraps raw transport data. Existing parsing logic in the consumer is moved behind the adapter and remains unchanged. |
| Prism and Beacon diverging | Both repos follow the identical file structure and task list. Any divergence would be caught by tests. |
| Bot dual-publish call sites (12 remaining) need updates | These sites call `publish_event()` which is updated to use the transport singleton. No per-site changes beyond what's already planned for dual-publish completion. |

---

## Open Questions (Out of Scope)

- Kafka adapter implementation (future phase)
- DLQ monitoring (`eventbus.dlq_depth` gauge — pre-existing known issue)
- Stream Trimmer coverage for `events:bus` / `events:bus:dlq`
- `CONTRACT.md` CloudEvents documentation update

---

## Task Summary (Ordered)

1. **Prism**: Create `Transport.Behaviour` + `Transport.Redis`, refactor facade, update Publisher/Consumer/DLQ, update config
2. **Beacon**: Identical changes to Prism (copy-paste)
3. **Iris**: Create `Publisher` interface + `RedisPublisher`, refactor `authz.go` + `main.go`
4. **Bot**: Create `EventBusTransport` Protocol + `RedisStreamTransport`, refactor Publisher/Consumer/DLQ, update `CacheInvalidator`
5. **Polarizer**: Create `EventBus` trait + `RedisEventBus`, update `config.rs` + `redis_stream.rs` + `main.rs`
