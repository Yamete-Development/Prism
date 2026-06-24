# Known Issues Cleanup Plan

**Date:** 2026-06-24
**Status:** Planning complete — ready for implementation
**Context:** Post event-bus migration cleanup. All PubSub paths removed, all services on CloudEvents. 13 known issues remain across 4 services.

---

## Task Groups (Independent — can be done in parallel)

### Group 1: Bot Critical Fixes (`~/Developments/interchat.py`)

#### 1.1 Fix `read_batch()` dead code in `transport_redis.py`

**File:** `apps/bot/services/event_bus/transport_redis.py`

**Problem:** `read_batch()` has an unconditional early `return messages` (line 67) that should be conditional inside `if not result:`. Additionally, `system_name()` (line 69) is inserted at wrong indent level — it ends `read_batch()` prematurely and absorbs the message-parsing loop as its body.

**Fix:**
- Add 4 spaces to `return messages` (line 67) so it's inside the `if not result:` block
- Move `def system_name(self) -> str:` and its body to after `read_batch()`'s closing (i.e., between `read_batch` and the next method)
- Fix indentation of the message-parsing `for` loop (lines 72-78) to be at 8-space (inside `read_batch`, not inside `system_name`)

**Target structure after fix:**
```python
    async def read_batch(self, stream, group, consumer, *, block_ms, batch_size):
        try:
            result = await self._redis.xreadgroup(...)
        except Exception as e:
            logger.error(...)
            return []

        messages: list[EventBusMessage] = []
        if not result:
            return messages    # ← now conditional

        for _stream_name, entries in result:
            for msg_id, fields in entries:
                payload = fields.get("payload", "")
                messages.append(
                    EventBusMessage(id=str(msg_id), stream=stream, data=payload)
                )

        return messages

    def system_name(self) -> str:
        return "redis"
```

#### 1.2 Fix shard ID in `invalidation.py`

**File:** `apps/bot/utils/cache/invalidation.py`

**Problem:** `_get_shard_id()` does `from main import _bot` but `main.py` has no module-level `_bot`. ImportError is silently caught → always returns `"0"`. All shards share consumer group `bot-cache-invalidator-0`.

**Fix:**
- Change `CacheInvalidator.start()` to accept `shard_id: str = "0"` parameter
- Remove `_get_shard_id()` function
- In `start()`, use the passed `shard_id` directly instead of calling `_get_shard_id()`

**File:** `apps/bot/lifecycle.py` (or wherever `invalidator.start()` is called)

**Fix:** Change caller from `await invalidator.start()` to `await invalidator.start(str(bot.shard_id))`

---

### Group 2: Prism Cleanup (`~/Developments/interchat-broadcast-worker`)

#### 2.1 Deduplicate config getters

**Files:** `lib/prism/config.ex`, `lib/prism/event_bus/config.ex`, `lib/prism/metrics_logger.ex`

**Problem:** 11 event-bus config getters duplicated between `Prism.Config` and `Prism.EventBus.Config` with different function names.

**Fix:**
- Keep `Prism.EventBus.Config` as canonical (used by all EventBus internals + has unique `transport_backend/0`)
- Remove the 11 event bus getters (lines 253-293) from `Prism.Config`
- Update `Prism.MetricsLogger` (the one external caller) to use `Prism.EventBus.Config.events_dlq_stream()` instead of `Prism.Config.events_dlq_stream()`

#### 2.2 Wire `eventbus.retry` OTel span

**Files:** `lib/prism/event_bus/consumer.ex`, `lib/prism/event_bus/telemetry.ex`

**Problem:** `Telemetry.span_retry/4` is defined but never called. Retry loop in `invoke_with_retry/3` only emits `:telemetry` event, no OTel span.

**Fix:** In `consumer.ex`'s `invoke_with_retry/3`, wrap each retry attempt (when `attempt > 1`) with `span_retry` start → handler call → `end_span`. Follow same pattern as `span_consume` in the main message path.

#### 2.3 Wire `eventbus.dlq` OTel span

**Files:** `lib/prism/event_bus/consumer.ex`, `lib/prism/event_bus/telemetry.ex`

**Problem:** `Telemetry.span_dlq/4` is defined but never called. DLQ publish path only emits `:telemetry` event.

**Fix:** In `consumer.ex`'s `process_message/3`, wrap the DLQ publish block with `span_dlq` start → DLQ.publish → `end_span`.

#### 2.4 Fix transport config bypass

**File:** `lib/prism/event_bus/transport.ex` (line ~80)

**Problem:** Transport facade reads `Application.get_env(:prism, :event_bus_transport_backend, Redis)` directly, bypassing both config modules and using inconsistent default (`Redis` vs `Prism.EventBus.Transport.Redis`).

**Fix:** Change to `EventBus.Config.transport_backend()` which already returns the correct default.

#### 2.5 Stream Trimmer: add event bus streams

**File:** `lib/prism/stream_trimmer.ex`

**Problem:** Trimmer covers fast/slow/callbacks streams but not `events:bus`, `events:bus:dlq`, or retry stream.

**Fix:** Add entries for `events:bus` and `events:bus:dlq`. Note: Prism is a producer-only for these streams (no consumer group), so XTRIM with MINID may not apply. Use MAXLEN-based trimming or skip if the existing `MAXLEN ~ 100000` on XADD is sufficient. Check the trimmer's `xtrim_strategy` logic and add only if compatible.

---

### Group 3: Beacon Cleanup (`~/Developments/beacon`)

#### 3.1 Deduplicate config getters

**Files:** `lib/beacon/config.ex`, `lib/beacon/event_bus/config.ex`

**Problem:** Same 11-getter duplication pattern as Prism. `Beacon.EventBus.Config` is the canonical module (used by `consumer.ex`); `Beacon.Config` has redundant copies.

**Fix:**
- Remove the 11 event bus getters from `Beacon.Config`
- Check for any external callers of those getters on `Beacon.Config` and update them to use `Beacon.EventBus.Config`

#### 3.2 Wire `eventbus.retry` OTel span

**Files:** `lib/beacon/event_bus/consumer.ex`, `lib/beacon/event_bus/telemetry.ex`

**Problem:** Identical to Prism — `span_retry/4` defined but never called; retry loop only emits telemetry event.

**Fix:** Same fix as Prism 2.2 — wrap retry attempts with `span_retry` start/end in `invoke_with_retry/3`.

---

### Group 4: Polarizer Cleanup (`~/Developments/polarizer`)

#### 4.1 Suppress `system_name` dead code warning

**File:** `src/eventbus.rs`

**Problem:** `system_name()` on `EventBus` trait and `RedisEventBus` impl has no callers. Produces `dead_code` warning on build.

**Fix:** Add `#[allow(dead_code)]` attribute on `RedisEventBus`'s `system_name()` implementation with a comment: `// Reserved for future OpenTelemetry messaging.system attribute`. The trait method declaration should remain (required by the impl).

#### 4.2 Add EventBus unit tests

**File:** `src/eventbus.rs` (add `#[cfg(test)]` module) or new `tests/eventbus_test.rs`

**Tests to add:**
- `build_envelope` produces valid CloudEvent with correct `specversion`, `source`, `id` format (`evt_` + 32 hex chars), RFC3339 `time`
- `generate_id()` produces unique IDs with correct format
- `format_utc_rfc3339()` produces valid ISO 8601 timestamp strings (spot-check known dates)
- `RedisEventBus.publish()` serializes correct JSON and calls Redis XADD with correct args (mock Redis connection)

---

## Verification

| Service | Command | Expected |
|---|---|---|
| Bot | `rg "redis_client\.publish\(" apps/bot/` (after fixes) | Zero results |
| Bot | Visual review: `system_name()` is callable from transport instance | No NameError |
| Bot | Check `_get_shard_id` removed, `start(shard_id)` signature | Param accepted |
| Prism | `mix test test/prism/event_bus_test.exs` | 110 tests, 0 failures |
| Prism | `mix compile --no-deps-check` | Clean, no warnings |
| Beacon | `mix test test/beacon/event_bus_test.exs` | 11 tests, 0 failures |
| Beacon | `mix compile --no-deps-check` | Clean, no warnings |
| Polarizer | `cargo test` | New eventbus tests pass |
| Polarizer | `cargo check` | No dead_code warning for system_name |

---

## Risks

- **Bot transport_redis fix:** Low risk — purely structural fix (indentation only, no logic change). Verify `system_name()` is reachable after fix.
- **Bot shard ID fix:** Low risk — changes function signature. All callers must be updated (check `lifecycle.py` and any test files).
- **Config dedup (Prism/Beacon):** Low risk — removing dead duplicates. Risk is missing an external caller. Grep for `Config.event_bus_` / `Config.events_` before deleting.
- **OTel span wiring:** Low risk — adding span instrumentation around existing logic. Risk is span leak if `end_span` is missed in error paths. Use try/after pattern.
- **Stream Trimmer:** Low risk — adding entries to an existing list. If trimmer uses MINID and no consumer group exists for events:bus, XTRIM may fail silently or be a no-op. Test in isolation.
- **Polarizer mock tests:** Medium risk — requires mock Redis or a test-only transport. Use the existing `NopPublisher` pattern or mock with `redis_test` crate.
