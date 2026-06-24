# Event Bus Full Migration — Implementation Plan

**Date:** 2026-06-24
**Status:** Planning complete. Ready for implementation.
**Base:** `.kilo/HANDOFF.md` (Phase 0–3.5 complete across all 5 services)

---

## Context

Migration from Redis PubSub to Redis Streams CloudEvents v1.0 across 5 services (Prism, Beacon, Iris, Bot, Polarizer). All adapter code and pluggable transport contracts are written and tested. Prism and Beacon are fully cut over. Remaining work:

1. **Bot dual-publish**: 12 sites still need `publish_event()` calls
2. **Cut-over**: Remove old PubSub paths from Iris and Bot
3. **Known issues**: CONTRACT.md, OTel messaging_system, DLQ depth gauge

---

## Phase A: Bot Dual-Publish Completion (12 sites across 5 files)

**Repo:** `~/Developments/interchat.py`
**Base path for all files:** `apps/bot/`

### Pattern to Follow (from `connectionService.py`)

```python
from services.event_bus.publisher import publish_event

await publish_event("events:bus", "fun.interchat.{domain}.{entity}.{action}",
                    {data_dict})
```

### A.1 — `services/moderation/moderationService.py` (7 sites)

**Add import at top of file (after existing imports):**
```python
from services.event_bus.publisher import publish_event
```

**Site 1 — `create_infraction()`, line ~180** (after `redis_client.publish('sync:access_restrictions', ...)`):
```python
                    await publish_event(
                        "events:bus",
                        "fun.interchat.moderation.access_restrictions.changed",
                        {"hub_id": hub_id, "user_id": user_id, "server_id": server_id},
                    )
```

**Site 2 — `revoke_infraction()`, line ~293** (after `redis_client.publish(...)`):
```python
                    await publish_event(
                        "events:bus",
                        "fun.interchat.moderation.access_restrictions.changed",
                        {"hub_id": infraction.hubId, "user_id": infraction.userId, "server_id": infraction.serverId},
                    )
```

**Site 3 — `delete_infraction()`, line ~317** (after `redis_client.publish(...)`):
```python
                    await publish_event(
                        "events:bus",
                        "fun.interchat.moderation.access_restrictions.changed",
                        {"hub_id": infraction.hubId, "user_id": infraction.userId, "server_id": infraction.serverId},
                    )
```

**Site 4 — `create_blacklist_entry()`, line ~361** (after `redis_client.publish(...)`):
```python
                await publish_event(
                    "events:bus",
                    "fun.interchat.moderation.access_restrictions.changed",
                    {"user_id": user_id},
                )
```

**Site 5 — `create_server_blacklist_entry()`, line ~396** (after `redis_client.publish(...)`):
```python
                await publish_event(
                    "events:bus",
                    "fun.interchat.moderation.access_restrictions.changed",
                    {"server_id": server_id},
                )
```

**Site 6 — `delete_blacklist_entry()`, line ~428** (after `redis_client.publish(...)`):
```python
                await publish_event(
                    "events:bus",
                    "fun.interchat.moderation.access_restrictions.changed",
                    {"user_id": user_id},
                )
```

**Site 7 — `delete_server_blacklist_entry()`, line ~448** (after `redis_client.publish(...)`):
```python
                await publish_event(
                    "events:bus",
                    "fun.interchat.moderation.access_restrictions.changed",
                    {"server_id": server_id},
                )
```

### A.2 — `utils/automod/filterService.py` (1 site)

**Add import at top of file:**
```python
from services.event_bus.publisher import publish_event
```

**Site 1 — `clear_cache()` classmethod, line ~285** (after `redis_client.publish('sync:automod', ...)`):
```python
            scope = "server" if is_server else "hub"
            await publish_event(
                "events:bus",
                "fun.interchat.moderation.automod.changed",
                {"scope": scope, "scope_id": target_id},
            )
```

### A.3 — `services/moderation/blacklist_filter.py` (1 site)

**Add import at top of file:**
```python
from services.event_bus.publisher import publish_event
```

**Site 1 — `BlacklistManager.broadcast_update()`, line ~84** (after `redis_client.publish('sync:blacklist', ...)`):
```python
        await publish_event(
            "events:bus",
            "fun.interchat.moderation.blacklist.changed",
            {"action": action, "target_id": str(target_id), "is_server": is_server},
        )
```

### A.4 — `services/moderation/blocklistService.py` (2 sites)

**Add import at top of file:**
```python
from services.event_bus.publisher import publish_event
```

**Site 1 — `remove_block()`, line ~65** (after `redis_client.publish('sync:hub_connections', ...)`):
```python
            await publish_event(
                "events:bus",
                "fun.interchat.hub.connections.changed",
                {"hub_id": None, "server_id": server_id},
            )
```

**Site 2 — `add_block()`, line ~85** (after `redis_client.publish('sync:hub_connections', ...)`):
```python
            await publish_event(
                "events:bus",
                "fun.interchat.hub.connections.changed",
                {"hub_id": None, "server_id": server_id},
            )
```

### A.5 — `services/hub/helpers/connection.py` (1 site)

**Add import at top of file:**
```python
from services.event_bus.publisher import publish_event
```

**Site 1 — `ConnectionCommandHelper.handle_connect()`, line ~380** (after `redis_client.publish('sync:hub_connections', ...)`):
```python
            await publish_event(
                "events:bus",
                "fun.interchat.hub.connections.changed",
                {"hub_id": result_hub.id, "server_id": None},
            )
```

---

## Phase B: Validation Period (Manual — No Code Changes)

1. Deploy all services with dual-publish active
2. Monitor `events:bus` stream: `redis-cli XLEN events:bus` and `redis-cli XREAD COUNT 10 BLOCK 5000 STREAMS events:bus >`
3. Verify Bot `CacheInvalidator` logs show EventBus consumer processing events for all 6 event types
4. Verify no regressions in cache invalidation behavior
5. Run for ≥24h to confirm stability
6. **Gate check:** All 6 event types appear in `events:bus` → proceed to Phase C

---

## Phase C: Full Cut-Over — Remove Old PubSub Paths

### C.1 — Iris Cut-Over

**Repo:** `~/Developments/iris`
**File:** `service/authz.go`

**Task 1:** Remove PubSub publish from `InvalidateUserPermissions()` (line ~257):
```go
// REMOVE these lines:
	payload := permInvalidationPayload{HubID: hubID, UserID: userID}
	if b, err := json.Marshal(payload); err == nil {
		s.invalidator.Publish(ctx, permPubSubChannel, string(b))
	}
```
Keep the CloudEvent dual-publish block (lines 260–269).

**Task 2:** Remove PubSub publish from `InvalidateHubPermissions()` (line ~327):
```go
// REMOVE these lines:
	payload := permInvalidationPayload{HubID: hubID}
	if b, err := json.Marshal(payload); err == nil {
		s.invalidator.Publish(ctx, permPubSubChannel, string(b))
	}
```
Keep the CloudEvent dual-publish block (lines 330–339).

**Task 3:** Cleanup unused declarations:
- Remove `permPubSubChannel` constant (line 25)
- Remove `Publish(ctx, channel, message) error` from `RedisInvalidator` interface (line 38)
- Remove `permInvalidationPayload` struct if no longer referenced (line 20)
- Verify `json` import is still needed; remove if not

### C.2 — Bot Publisher Files: Remove `redis_client.publish()` Calls

**Repo:** `~/Developments/interchat.py`
**Base path:** `apps/bot/`

#### C.2.1 — `services/connectionService.py` (6 sites)

For each method, remove: `import msgspec`, payload build line, and `redis_client.publish()` line. Keep `publish_event()` line.

| Method | Remove lines |
|---|---|
| `create_connection` | `import msgspec`, `payload = msgspec.json.encode(...)`, `await redis_client.publish(...)` |
| `update_connection` | Same pattern |
| `update_connection_by_id` | Same pattern |
| `delete_connection` | Same pattern |
| `delete_all_by_server` | Same pattern |
| `disconnect_inactive` | Same pattern |

Also remove `from utils.constants import redis_client` from methods where it's no longer used.

#### C.2.2 — `services/permissionService.py` (1 site)

Remove line 293 (`await redis_client.publish('sync:hub_context', ...)`). Also remove `import msgspec` on line 287 if not used elsewhere.

#### C.2.3 — `services/moderation/moderationService.py` (7 sites)

After dual-publish is complete (Phase A), remove ALL `await redis_client.publish('sync:access_restrictions', ...)` calls (7 sites). Each site wraps the publish in `create_background_task(...)` — remove the entire `create_background_task` call wrapping the publish. Keep `publish_event()` calls.

#### C.2.4 — `utils/automod/filterService.py` (1 site)

Remove `redis_client.publish('sync:automod', ...)` call. Keep `publish_event()`.

#### C.2.5 — `services/moderation/blacklist_filter.py` (1 site)

Remove `redis_client.publish('sync:blacklist', ...)` call. Keep `publish_event()`.

#### C.2.6 — `services/moderation/blocklistService.py` (2 sites)

Remove both `redis_client.publish('sync:hub_connections', ...)` calls. Keep `publish_event()`.

#### C.2.7 — `services/hub/helpers/connection.py` (1 site)

Remove `redis_client.publish('sync:hub_connections', ...)` call. Keep `publish_event()`.

### C.3 — Bot CacheInvalidator: Remove PubSub Listener

**Repo:** `~/Developments/interchat.py`
**File:** `apps/bot/utils/cache/invalidation.py`

**Task 1:** Remove `CHANNELS` class variable (lines 35–42):
```python
# REMOVE entirely:
    CHANNELS = [
        'sync:hub_connections',
        'sync:access_restrictions',
        'sync:automod',
        'sync:blacklist',
        'sync:hub_permissions',
        'sync:hub_context',
    ]
```

**Task 2:** Remove `_listen_loop()` method (lines 83–122):
```python
# REMOVE entirely:
    async def _listen_loop(self) -> None:
        while not self._stop_event.is_set():
            ...
```

**Task 3:** From `start()`, remove `_task` creation (lines 54–55):
```python
# REMOVE:
        self._stop_event.clear()
        self._task = asyncio.create_task(self._listen_loop(), ...)
```

**Task 4:** From `stop()`, remove task cancellation (lines 75–81):
```python
# REMOVE:
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
            self._task = None
```

**Task 5:** Update `__init__` to remove `self._task` and `self._stop_event` if no longer used.

**Preserve (do NOT remove):**
- `_eventbus_router()` — EventBus consumer handler
- `_handle_hub_connections()`, `_handle_access_restrictions()`, `_handle_automod()`, `_handle_blacklist()`, `_handle_hub_permissions()`, `_handle_hub_context()` — shared handlers
- `EventBusConsumer` start/stop in `start()` and `stop()`

---

## Phase D: Known Issues Resolution

### D.1 — StreamTrimmer: Out of Scope (Won't Fix)

**Rationale:** `events:bus` and `events:bus:dlq` are shared infrastructure streams — Prism is a producer, not the owner. Consumer groups live in Beacon and Bot. MAXLEN ~ 100,000 from publishers provides sufficient approximate trimming. No code changes needed.

### D.2 — CONTRACT.md: Add CloudEvents Documentation

**Repo:** `~/Developments/interchat-broadcast-worker`
**File:** `CONTRACT.md`

Add these sections after the existing "Configurable Stream Keys" table:

1. **Event Bus Output (CloudEvents v1.0)** — envelope format, stream keys, MAXLEN
2. **CloudEvent Envelope Schema** — `specversion`, `type`, `source`, `id`, `time`, `datacontenttype`, `data`, `traceparent`, `tracestate`
3. **Event Type Catalog** — all 8 event types with source, data schema, consumers
4. **Event Bus DLQ** — `events:bus:dlq` schema: `original_event`, `error`, `failed_at`, `attempts`, `consumer_group`
5. **Consumer Group Model** — per-service, per-shard pattern
6. **Transport Abstraction** — pluggable backend (Redis/Kafka) via `@behaviour` / `interface` / `Protocol` / `trait`

### D.3 — OTel `messaging_system`: Derive from Transport Backend

**Rationale:** All 10 instances hardcode `{:messaging_system, :redis}`. When a Kafka backend is added, this should reflect the active transport. The fix adds a way for the transport to declare its system name.

#### D.3.1 — Prism

**File:** `lib/prism/event_bus/transport/behaviour.ex`
Add callback:
```elixir
@callback system_name() :: String.t()
```

**File:** `lib/prism/event_bus/transport/redis.ex`
Implement:
```elixir
@impl true
def system_name, do: "redis"
```

**File:** `lib/prism/event_bus/publisher.ex` (lines 23, 52)
Replace `{:messaging_system, :redis}` with `{:messaging_system, String.to_atom(Transport.system_name())}`

**File:** `lib/prism/event_bus/telemetry.ex` (lines 55, 80, 103)
Replace `{:messaging_system, :redis}` with `{:messaging_system, String.to_atom(Transport.system_name())}`

#### D.3.2 — Beacon

Same changes in:
- `lib/beacon/event_bus/transport/behaviour.ex` — add `system_name/0` callback
- `lib/beacon/event_bus/transport/redis.ex` — implement returning `"redis"`
- `lib/beacon/event_bus/publisher.ex` (lines 23, 52) — replace hardcoded value
- `lib/beacon/event_bus/telemetry.ex` (lines 56, 81, 104) — replace hardcoded value

#### D.3.3 — Bot (Python)

**File:** `apps/bot/services/event_bus/transport.py`
Add method to `EventBusTransport` Protocol:
```python
def system_name(self) -> str: ...
```

**File:** `apps/bot/services/event_bus/transport_redis.py`
Implement:
```python
def system_name(self) -> str:
    return "redis"
```

#### D.3.4 — Iris (Go)

**File:** `eventbus/transport.go`
Add method to `Publisher` interface:
```go
SystemName() string
```

**File:** `eventbus/redis_publisher.go`
Implement:
```go
func (p *RedisPublisher) SystemName() string { return "redis" }
```

#### D.3.5 — Polarizer (Rust)

**File:** `src/eventbus.rs`
Add method to `EventBus` trait:
```rust
fn system_name(&self) -> &'static str;
```

Implement in `RedisEventBus`:
```rust
fn system_name(&self) -> &'static str { "redis" }
```

### D.4 — DLQ Depth Gauge

**Rationale:** `eventbus.dlq_depth` gauge metric is defined in telemetry but never emitted. Add periodic measurement.

#### D.4.1 — Prism

**File:** `lib/prism/metrics_logger.ex`

Add `events:bus:dlq` length to the periodic log output and emit telemetry:
```elixir
# Add to existing periodic log:
dlq_len = stream_length(Prism.Config.events_dlq_stream())

# Emit telemetry gauge:
:telemetry.execute([:prism, :event_bus, :dlq_depth], %{length: dlq_len}, %{})
```

#### D.4.2 — Beacon

**File:** `lib/beacon/metrics_logger.ex` (or equivalent periodic metrics module)

If Beacon has a metrics logger, add the same `XLEN events:bus:dlq` + `:telemetry.execute([:beacon, :event_bus, :dlq_depth], ...)` pattern. If Beacon has no periodic metrics logger, create a minimal one or skip (Prism's gauge is sufficient since it measures the same shared Redis key).

---

## Execution Order

```
A.1    →  moderationService.py dual-publish (7 sites)
A.2    →  filterService.py dual-publish (1 site)
A.3    →  blacklist_filter.py dual-publish (1 site)
A.4    →  blocklistService.py dual-publish (2 sites)
A.5    →  connection.py dual-publish (1 site)
--- MANUAL VALIDATION GATE ---
B      →  Deploy + monitor ≥24h
--- CUT-OVER GATE ---
C.1    →  Iris: remove PubSub publishes + cleanup
C.2.1  →  connectionService.py: remove redis_client.publish (6 sites)
C.2.2  →  permissionService.py: remove redis_client.publish (1 site)
C.2.3  →  moderationService.py: remove redis_client.publish (7 sites)
C.2.4  →  filterService.py: remove redis_client.publish (1 site)
C.2.5  →  blacklist_filter.py: remove redis_client.publish (1 site)
C.2.6  →  blocklistService.py: remove redis_client.publish (2 sites)
C.2.7  →  connection.py: remove redis_client.publish (1 site)
C.3    →  CacheInvalidator: remove PubSub listener
--- KNOWN ISSUES (can run in parallel) ---
D.2    →  CONTRACT.md: CloudEvents docs
D.3    →  OTel messaging_system (all 5 services)
D.4    →  DLQ depth gauge (Prism + Beacon)
```

---

## Post-Migration Verification

After all phases complete, verify the old PubSub paths are gone:

```bash
# Confirm no PubSub publishes in any service
rg "redis_client\.publish\(" ~/Developments/interchat.py/apps/bot/
rg "s\.invalidator\.Publish\(" ~/Developments/iris/
# Should return 0 results

# Confirm events:bus receives all events
redis-cli XINFO STREAM events:bus
redis-cli XLEN events:bus:dlq

# Confirm CacheInvalidator only has EventBus consumer (no PubSub listener)
# Check Bot logs for "Started centralized CacheInvalidator listener" and absence of PubSub subscription messages
```

---

## Risk Notes

- **Iris test constructor** (`NewAuthZServiceWithFakeRedis`) does not set `eventBusPublisher` — any test calling invalidation methods through it will nil-panic. Fix: set `eventBusPublisher: &nopPublisher{}` or skip event bus publish in tests.
- **Bot import cleanup**: When removing `redis_client.publish()` calls, verify `redis_client` is still needed elsewhere in the function. If not, remove `from utils.constants import redis_client` to avoid unused import warnings.
- **`_handle_automod` data shape**: PubSub sends `"server:target_id"` as a raw string. EventBus sends `{"scope": "server", "scope_id": "target_id"}`. The `_eventbus_router` reconstructs the string before passing to the handler. Both paths work; the handler is agnostic.
