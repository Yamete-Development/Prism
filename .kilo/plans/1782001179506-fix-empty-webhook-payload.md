# Fix Empty Webhook Payload & Network Error Resilience

## Problem

1. **Empty/Null content sent to Discord webhooks** — `_format_relay_content` returns `None` when content is empty and no media exists, which propagates through Redis → Elixir → Discord API, causing 400 errors (50006: Cannot send an empty message).
2. **Mint.HTTP2 `:unprocessed` errors** — HTTP/2 connection pool exhaustion under load, already retried but causes latency.
3. **Sentry lock blocks asyncio event loop** — Sentry SDK's batcher lock inside logging handlers blocks Discord gateway heartbeats (Python bot).

---

## Root Cause (Empty Content)

**File:** `interchat.py/apps/bot/services/lobbies/relay.py`

Line 715: `return base_content[:2000] or None`
— When `base_content` is empty string `""`, Python's `or` evaluates to `None`.

This `None` survives the entire pipeline:
- `payload['content'] = None` (line 242)
- Minified to `{"x": null}` by `PrismClient._minify_dict`
- Expanded to `%{"content" => nil}` by Elixir
- `Jason.encode_to_iodata!(%{"content" => nil, ...})` → `{"content":null,...}`
- Discord API returns 400 (50006)

**Elixir side (`fanout_broadway.ex`):** No validation guards exist — if `payload` key is missing, defaults to `%{}`, and `DiscordWorker` will happily `Jason.encode_to_iodata!` an empty map for execute actions.

---

## Plan

### Task 1: Fix `_format_relay_content` — Never return `None`

**File:** `apps/bot/services/lobbies/relay.py` (line 688–722)

- Change line 715 (`return base_content[:2000] or None`) to return a safe placeholder instead of `None`.
- Add a constant safe placeholder like `"*(empty message)*"` or similar.
- Ensure the fallback on lines 709–712 (`*[Media attachment]*`) is always reached when there IS media but no text. (Currently works correctly, but keep it.)

### Task 2: Add guard in `_prepare_relay` to skip empty relays

**File:** `apps/bot/services/lobbies/relay.py` (after line 246)

- After constructing the `payload` dict (lines 241–246), check if `payload['content']` is `None` or empty whitespace.
- If so, return `None` to skip the relay entirely (no Redis push, no webhook dispatch).
- Log a debug message so empty skips are observable.

### Task 3: Add Elixir-side guard in `DiscordWorker.process_target/7`

**File:** `lib/prism/discord_worker.ex` (lines 44–53)

- After merging content and overrides for `execute`/`edit` actions, check if the resulting map has meaningful content fields (`content`, `embeds`, `components`).
- If all are nil/empty, return `{:error, :empty_payload}` without making an HTTP request.
- This is a defense-in-depth layer — the Python fix should prevent the issue, but this catches any future regressions or misconfigured callers.

### Task 4: Add Elixir-side guard in `FanoutBroadway.process_batch/11`

**File:** `lib/prism/fanout_broadway.ex` (around line 191)

- After extracting `discord_payload`, validate that it's not an empty map for `execute` actions.
- If empty, skip batch processing and log a warning. Do NOT push a callback (avoids noise).

### Task 5: Improve Mint.HTTP2 connection resilience

**File:** `lib/prism/discord_worker.ex` (line 702)

- Reduce `pool_timeout` from 30_000ms to 10_000ms (faster failure → faster retry).
- Add `max_idle_time: 60_000` to Finch pool configuration to rotate stale HTTP/2 connections.
- Consider switching affected requests to HTTP/1.1 on `:unprocessed` retry (if Mint.HTTP2 support makes this feasible).

### Task 6: Fix Sentry lock contention (Python bot)

**File:** `apps/bot/main.py` (or boot config)

- Add `sentry_sdk.integrations.logging.ignore_logger('InterChat')` for high-volume loggers that don't need Sentry capture.
- Or configure `Sentry.init(send_default_pii=False, traces_sample_rate=0.1)` to reduce telemetry volume.
- Alternative: Wrap Sentry's `_capture_log` calls in `loop.call_soon_threadsafe` or move batcher to a thread to avoid blocking the event loop.

---

## Validation

1. **Unit test for `_format_relay_content`**: Verify it never returns `None` for any combination of inputs (empty content, no media, no badge, etc.).
2. **Integration test**: Simulate a message with only server emojis (voted user) and confirm no webhook dispatch occurs.
3. **Elixir test**: Verify `DiscordWorker.process_target("execute", target, %{}, ...)` returns `{:error, :empty_payload}`.
4. **Load test**: Run a batch of 200+ concurrent webhooks and verify no `Finch.HTTPError{:unprocessed}` cascade (pool tuning).
5. **Monitor Sentry**: After reducing logging capture, confirm heartbeat blocks drop to zero.

---

## Files Changed

| File | Change |
|------|--------|
| `interchat.py/apps/bot/services/lobbies/relay.py` | Fix `_format_relay_content` return value + add skip guard |
| `interchat-broadcast-worker/lib/prism/discord_worker.ex` | Add empty payload guard for execute/edit |
| `interchat-broadcast-worker/lib/prism/fanout_broadway.ex` | Validate discord_payload for execute actions |
| `interchat.py/apps/bot/main.py` | Reduce Sentry logging capture to fix heartbeat blocks |
| Finch pool config (env or app config) | Reduce pool_timeout, add max_idle_time |
