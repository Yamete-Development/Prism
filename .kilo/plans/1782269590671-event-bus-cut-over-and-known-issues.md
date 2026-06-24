# Event Bus Cut-Over & Known Issues — Implementation Plan

**Date:** 2026-06-24
**Status:** Planning complete. Ready for implementation.
**Base:** `.kilo/plans/1782269198601-event-bus-full-migration.md` (Phases A, D.2, D.3.1, D.4.1, D.3.3 completed)

**Change from original plan:** Phase B (manual validation gate) is skipped. Staging system available for testing. Cut-over proceeds immediately.

---

## Context

Dual-publish is complete across all 5 services:
- Prism, Beacon: already fully cut over to EventBus (no PubSub dependency)
- Iris: dual-publish active (`InvalidateUserPermissions`, `InvalidateHubPermissions`)
- Bot: dual-publish active at all 19 publisher sites + `CacheInvalidator` dual-consume

Now remove the old PubSub paths everywhere and complete remaining known issues (D.3.2, D.3.4, D.3.5, Iris test fix).

---

## Phase C: Full Cut-Over — Remove Old PubSub Paths

### C.1 — Iris Cut-Over

**Repo:** `~/Developments/iris`
**File:** `service/authz.go`

#### C.1.1 — Remove PubSub publish from `InvalidateUserPermissions()`

Remove lines ~220–229 (the entire `s.invalidator.Publish(...)` block):

```go
// REMOVE:
	// Fan-out to all bot shards: they will clear L1 on receipt.
	payload := permInvalidationPayload{HubID: hubID, UserID: userID}
	if b, err := json.Marshal(payload); err == nil {
		s.invalidator.Publish(ctx, permPubSubChannel, string(b))
	}
```

Keep the CloudEvent publish block that follows.

> **Risk note:** The dual-publish block reuses the same `payload` variable. After removing the PubSub block, the CloudEvent block still references `payload`. Since the `payload` declaration is removed, the CloudEvent block will get its `payload` from the *existing* declaration above the PubSub block. Verify that the `payload` variable is still in scope. If the payload variable was declared inside the removed block, declare it before the CloudEvent publish block instead.

#### C.1.2 — Remove PubSub publish from `InvalidateHubPermissions()`

Remove lines ~285–294 (the entire `s.invalidator.Publish(...)` block):

```go
// REMOVE:
	// Fan-out hub-wide invalidation to all bot shards.
	payload := permInvalidationPayload{HubID: hubID}
	if b, err := json.Marshal(payload); err == nil {
		s.invalidator.Publish(ctx, permPubSubChannel, string(b))
	}
```

Keep the CloudEvent publish block that follows.

Same risk note as C.1.1 applies — verify `payload` is still in scope for the CloudEvent block.

#### C.1.3 — Cleanup unused declarations

1. **Remove `permPubSubChannel` constant** (line 25):
   ```go
   // REMOVE:
   const permPubSubChannel = "sync:hub_permissions"
   ```

2. **Remove `Publish` from `RedisInvalidator` interface** (line 38):
   ```go
   // CHANGE from:
   type RedisInvalidator interface {
       Scan(ctx context.Context, cursor uint64, match string, count int64) (keys []string, nextCursor uint64, err error)
       Del(ctx context.Context, keys ...string) error
       Publish(ctx context.Context, channel string, message interface{}) error
   }
   // TO:
   type RedisInvalidator interface {
       Scan(ctx context.Context, cursor uint64, match string, count int64) (keys []string, nextCursor uint64, err error)
       Del(ctx context.Context, keys ...string) error
   }
   ```

   > **Note:** `Scan` and `Del` are kept on the interface because they are used by tests. The `invalidator` field is still wired in the production constructor (`NewAuthZService`) for backward compatibility with test code.

3. **Remove `Publish` method from `redisClientAdapter`**:
   ```go
   // REMOVE:
   func (a *redisClientAdapter) Publish(ctx context.Context, channel string, message interface{}) error {
       return a.c.Publish(ctx, channel, message).Err()
   }
   ```

4. **Verify `json` import** — still needed for CloudEvent publish (used in `json.Marshal` calls). Keep it.

#### C.1.4 — Fix `NewAuthZServiceWithFakeRedis` test constructor

**File:** `service/authz.go`, line ~85

The constructor doesn't set `eventBusPublisher`. Any test calling `InvalidateUserPermissions` or `InvalidateHubPermissions` through it will nil-panic.

**Solution:** Add a NOOP publisher implementation and set it in the constructor.

In `eventbus/` package, add to an existing file or create `eventbus/nop_publisher.go`:

```go
package eventbus

import "context"

// NopPublisher is a no-op Publisher for testing.
type NopPublisher struct{}

func (NopPublisher) Publish(ctx context.Context, stream, eventType string, data interface{}, opts ...PublishOption) error {
    return nil
}
```

Then update `NewAuthZServiceWithFakeRedis`:

```go
// CHANGE FROM:
func NewAuthZServiceWithFakeRedis(q db.Querier, inv RedisInvalidator) *AuthZService {
    return &AuthZService{q: q, rdb: nil, invalidator: inv}
}
// TO:
func NewAuthZServiceWithFakeRedis(q db.Querier, inv RedisInvalidator) *AuthZService {
    return &AuthZService{
        q:                q,
        rdb:              nil,
        invalidator:      inv,
        eventBusPublisher: eventbus.NopPublisher{},
    }
}
```

---

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

Also remove `from utils.constants import redis_client` from function-level imports in methods where it's no longer used (check if any other usage remains).

> **Important:** In `delete_all_by_server`, the `import msgspec` at line ~162 is on its own line (not inside a function). Verify this module-level import is only used by the now-removed PubSub lines. If so, remove it.

#### C.2.2 — `services/permissionService.py` (1 site)

In the `invalidate_rules_cache` inner coroutine (line ~293), remove:

```python
await redis_client.publish('sync:hub_context', msgspec.json.encode({'user_id': str(user.id)}))
```

Also remove `import msgspec` from the inner function (line ~287) if it was only used for the PubSub publish (the EventBus path uses `publish_event` which handles its own serialization).

#### C.2.3 — `services/moderation/moderationService.py` (7 sites)

For each site, remove the `create_background_task(redis_client.publish(...), ...)` call wrapping the PubSub publish. Keep `await publish_event(...)`.

The 7 sites are in:
- `create_infraction()` — remove `create_background_task(redis_client.publish('sync:access_restrictions', payload), f'pub_access_{hub_id}')`
- `revoke_infraction()` — remove `create_background_task(redis_client.publish('sync:access_restrictions', payload), f'pub_access_{infraction.hubId}')`
- `delete_infraction()` — remove `create_background_task(redis_client.publish('sync:access_restrictions', payload), f'pub_access_{infraction.hubId}')`
- `create_blacklist_entry()` — remove `create_background_task(redis_client.publish('sync:access_restrictions', payload), f'pub_access_{user_id}')`
- `create_server_blacklist_entry()` — remove `create_background_task(redis_client.publish('sync:access_restrictions', payload), f'pub_access_{server_id}')`
- `delete_blacklist_entry()` — remove `create_background_task(redis_client.publish('sync:access_restrictions', payload), f'pub_access_{user_id}')`
- `delete_server_blacklist_entry()` — remove `create_background_task(redis_client.publish('sync:access_restrictions', payload), f'pub_access_{server_id}')`

> **Important:** For sites 1–3, do NOT remove the `import msgspec` line inside the `if` block — the `payload` variable from `msgspec.json.encode(...)` is still needed as it's only used for PubSub. After removing the PubSub publish, the `import msgspec` and `payload = ...` lines can also be removed since they serve no other purpose.

#### C.2.4 — `utils/automod/filterService.py` (1 site)

In `clear_cache()` classmethod, remove:

```python
await redis_client.publish(topic, payload)
```

Also remove the now-unused `topic` and `payload` lines:
```python
topic = 'sync:automod'
payload = f'{"server" if is_server else "hub"}:{target_id}'
await redis_client.publish(topic, payload)
```

Keep `await publish_event(...)`.

#### C.2.5 — `services/moderation/blacklist_filter.py` (1 site)

In `broadcast_update()`, remove `await redis_client.publish(...)` line. Keep `await publish_event(...)`.

#### C.2.6 — `services/moderation/blocklistService.py` (2 sites)

In `remove_block()` and `add_block()`, remove both `await redis_client.publish(...)` calls and their associated `import msgspec` + `payload = ...` lines. Keep `await publish_event(...)`.

#### C.2.7 — `services/hub/helpers/connection.py` (1 site)

In `handle_connect()`, remove `await redis_client.publish(...)` and the `payload = msgspec.json.encode(...)` line. Keep `await publish_event(...)`.

---

### C.3 — Bot CacheInvalidator: Remove PubSub Listener

**Repo:** `~/Developments/interchat.py`
**File:** `apps/bot/utils/cache/invalidation.py`

#### C.3.1 — Remove `CHANNELS` class variable

Remove lines 35–42:
```python
CHANNELS = [
    'sync:hub_connections',
    'sync:access_restrictions',
    'sync:automod',
    'sync:blacklist',
    'sync:hub_permissions',
    'sync:hub_context',
]
```

#### C.3.2 — Remove `_listen_loop()` method

Remove the entire method (lines 83–122).

#### C.3.3 — From `start()`, remove `_task` creation

Remove lines referencing `_stop_event.clear()`, `_task = asyncio.create_task(...)`.

#### C.3.4 — From `stop()`, remove task cancellation

Remove lines referencing `_task.cancel()`, `await self._task`, `self._task = None`.

#### C.3.5 — Update `__init__`

Remove `self._task` and `self._stop_event` fields. Remove all references to `asyncio.Task`, `asyncio.Event`.

#### C.3.6 — Update `RESTART_DELAY_SECONDS`

Remove this class variable if no longer referenced.

#### C.3.7 — Remove `import asyncio` if no longer needed

Check if `asyncio` is used elsewhere in the file (it's used for `asyncio.CancelledError`, `asyncio.sleep`, `asyncio.Event`, `asyncio.Task`). After removing `_listen_loop`, none of these should remain. Remove the import.

**Preserve (do NOT remove):**
- `_eventbus_router()` — EventBus consumer handler
- `_handle_hub_connections()`, `_handle_access_restrictions()`, `_handle_automod()`, `_handle_blacklist()`, `_handle_hub_permissions()`, `_handle_hub_context()` — shared handlers
- `EventBusConsumer` start/stop in `start()` and `stop()`
- `_eventbus_consumer` field in `__init__`

---

## Phase D: Known Issues Resolution (Remaining)

### D.3.2 — Beacon: Derive `messaging_system` from Transport Backend

**Repo:** `~/Developments/beacon`
**Pattern:** Identical to Prism D.3.1 (already completed in `interchat-broadcast-worker`)

#### D.3.2.1 — `lib/beacon/event_bus/transport/behaviour.ex`

Add callback after `claim_stale/5`:

```elixir
@doc """
Returns the system name of the transport backend (e.g. `"redis"`, `"kafka"`).
Used for OpenTelemetry `messaging.system` attribute.
"""
@callback system_name() :: String.t()
```

#### D.3.2.2 — `lib/beacon/event_bus/transport/redis.ex`

Add before `# ── Private ──` section:

```elixir
@impl true
def system_name, do: "redis"
```

#### D.3.2.3 — `lib/beacon/event_bus/transport.ex`

Add facade method (check if `transport.ex` exists in Beacon; if not, add `system_name/0` to whichever module serves as the transport facade):

```elixir
@doc """
Returns the system name of the configured transport backend.
"""
@spec system_name() :: String.t()
def system_name do
  backend().system_name()
end
```

#### D.3.2.4 — `lib/beacon/event_bus/publisher.ex`

Replace 2 instances of `{:messaging_system, :redis}` with `{:messaging_system, String.to_atom(Transport.system_name())}` (lines 23, 52).

#### D.3.2.5 — `lib/beacon/event_bus/telemetry.ex`

Replace 3 instances of `{:messaging_system, :redis}` with `{:messaging_system, String.to_atom(Beacon.EventBus.Transport.system_name())}` (lines 56, 81, 104).

---

### D.3.4 — Iris (Go): Add `SystemName()` to Publisher Interface

**Repo:** `~/Developments/iris`

#### D.3.4.1 — `eventbus/transport.go`

Add method to `Publisher` interface:

```go
type Publisher interface {
    Publish(ctx context.Context, stream, eventType string, data interface{}, opts ...PublishOption) error
    SystemName() string
}
```

#### D.3.4.2 — `eventbus/redis_publisher.go`

Implement on `RedisPublisher`:

```go
// SystemName returns the transport backend identifier for OpenTelemetry.
func (p *RedisPublisher) SystemName() string { return "redis" }
```

#### D.3.4.3 — `eventbus/nop_publisher.go`

Implement on `NopPublisher` (created in C.1.4):

```go
func (NopPublisher) SystemName() string { return "nop" }
```

---

### D.3.5 — Polarizer (Rust): Add `system_name()` to EventBus Trait

**Repo:** `~/Developments/polarizer`
**File:** `src/eventbus.rs`

#### D.3.5.1 — Add method to trait

In the `EventBus` trait, add:

```rust
    /// Returns the transport backend name for OpenTelemetry messaging.system attribute.
    fn system_name(&self) -> &'static str;
```

#### D.3.5.2 — Implement in `RedisEventBus`

```rust
    fn system_name(&self) -> &'static str { "redis" }
```

---

### D.4.2 — Beacon DLQ Depth Gauge

**Repo:** `~/Developments/beacon`

**Decision: SKIP.** Beacon has no periodic metrics logger module. Prism's `metrics_logger.ex` already emits `[:prism, :event_bus, :dlq_depth]` which measures the same shared Redis key `events:bus:dlq`. Duplicating this in Beacon adds no value.

---

## Execution Order

```
C.1.1  →  Iris: remove PubSub from InvalidateUserPermissions
C.1.2  →  Iris: remove PubSub from InvalidateHubPermissions
C.1.3  →  Iris: cleanup permPubSubChannel, interface, adapter
C.1.4  →  Iris: fix NopPublisher + NewAuthZServiceWithFakeRedis
C.2.1  →  connectionService.py: remove redis_client.publish (6 sites)
C.2.2  →  permissionService.py: remove redis_client.publish (1 site)
C.2.3  →  moderationService.py: remove redis_client.publish (7 sites)
C.2.4  →  filterService.py: remove redis_client.publish (1 site)
C.2.5  →  blacklist_filter.py: remove redis_client.publish (1 site)
C.2.6  →  blocklistService.py: remove redis_client.publish (2 sites)
C.2.7  →  connection.py: remove redis_client.publish (1 site)
C.3    →  CacheInvalidator: remove PubSub listener
--- KNOWN ISSUES ---
D.3.2  →  Beacon: messaging_system (5 files)
D.3.4  →  Iris: SystemName() on Publisher interface
D.3.5  →  Polarizer: system_name() on EventBus trait
D.4.2  →  Beacon DLQ: SKIP (Prism covers shared key)
```

Phase C subtasks within each repo can run in parallel. Phase D subtasks are independent and can also run in parallel.

---

## Post-Migration Verification

After all phases complete:

```bash
# Confirm no PubSub publishes in any service
rg "redis_client\.publish\(" ~/Developments/interchat.py/apps/bot/
# Should return 0 results

rg "s\.invalidator\.Publish\(" ~/Developments/iris/
# Should return 0 results

# Confirm CacheInvalidator has no PubSub listener
rg "CHANNELS\|_listen_loop\|_stop_event\|_task" ~/Developments/interchat.py/apps/bot/utils/cache/invalidation.py
# Should return 0 results for CHANNELS, _listen_loop, _stop_event, _task

# Confirm events:bus receives all events
redis-cli XINFO STREAM events:bus
redis-cli XLEN events:bus:dlq

# Compile each repo
cd ~/Developments/iris && go build ./...
cd ~/Developments/beacon && mix compile --no-deps-check
cd ~/Developments/polarizer && cargo check
```

---

## Risk Notes

- **Iris `payload` variable scope:** When removing the PubSub publish block from `InvalidateUserPermissions` and `InvalidateHubPermissions`, verify the `payload` variable used by the CloudEvent publish block is still declared. If the `payload` declaration is inside the removed PubSub block, either move the declaration before the CloudEvent block or the CloudEvent block will need its own declaration.

- **`moderationService.py` sites 1-3 conditional gating:** The PubSub publish is inside `if user_id or server_id:` (or `if infraction.userId or infraction.serverId:`). The `publish_event` was placed inside the same block. When removing PubSub, do NOT accidentally remove the closing `if` block — only remove the `create_background_task(redis_client.publish(...), ...)` line.

- **`connectionService.py` function-level imports:** Each method imports `redis_client`, `publish_event`, and `msgspec` locally. After removing PubSub, remove `import msgspec` and `from utils.constants import redis_client` from methods where they're no longer used.

- **Iris `NewAuthZServiceWithFakeRedis`**: After C.1.4, all existing tests using this constructor will safely no-op on `Publish` and `SystemName` calls. No test breakage expected.

- **Bot `CacheInvalidator.__init__`**: After removing `_task` and `_stop_event`, ensure the `start()` method doesn't reference them (it currently calls `self._stop_event.clear()` and `self._task = asyncio.create_task(...)`). These lines must also be removed.
