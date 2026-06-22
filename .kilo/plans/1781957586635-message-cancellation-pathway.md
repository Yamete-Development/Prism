# Proactive Message Cancellation Pathway

## Goal

When a Discord message is deleted (by author, moderator, admin, or bulk delete), prevent Elixir Prism from wasting resources broadcasting the now-deleted content. Instead of "try hitting and getting 404," the Python bot signals Prism to skip all in-flight processing for that message.

## Approach

A shared Redis flag `prism:cancel:{message_id}` acting as a kill switch:

- **Python bot** sets the flag in `MessageDeletionService.execute_deletion()` (central choke point for all deletion paths)
- **Elixir Prism** checks the flag at batch-processing time in `FanoutBroadway.handle_message/3` and at retry time in `process_retry/3`

## Redis Key Design

| Field | Value |
|---|---|
| Key pattern | `prism:cancel:{message_id}` |
| Value | `"1"` |
| TTL | 300 seconds (configurable via `PRISM_CANCEL_TTL` env var) |
| Set by | Python bot (`MessageDeletionService.execute_deletion()`) |
| Checked by | Elixir Prism (`FanoutBroadway`, retry path) |

---

## Changes — Python Bot (`interchat.py`)

### 1. `apps/bot/utils/moderation/messageDelete.py` — Set cancellation flag

In `MessageDeletionService.execute_deletion()`, after the existing `_cancel_pending_edits()` call:

```python
# Set cancellation flag for Prism to skip in-flight batches
await redis_client.setex(
    f"prism:cancel:{message_id}",
    PRISM_CANCEL_TTL,  # default 300
    "1"
)
```

**PENDING+delete race fix:** When the message status is `PENDING`, the existing code sets `deletionQueuedAt` and relies on `_maybe_activate_message()` to process the deferred deletion. But if all shards are cancelled by this flag, activation never fires. Fix: check if the message is PENDING; if so, directly finalize deletion (set `status=DELETED`, remove `Broadcast` rows) instead of deferring.

### 2. `apps/bot/cogs/events/discord/onMessage.py` — Pre-flight check

In `_handle_broadcast()` or the async broadcast pipeline, before pushing to Prism:

```python
# Skip broadcast if source message was already deleted
if await redis_client.exists(f"prism:cancel:{message.id}"):
    logger.info(f"Skipping broadcast for deleted message {message.id}")
    return
```

### 3. `apps/bot/cogs/events/discord/onMessageEdit.py` — Unify cancellation check

The existing `prism:cancelled:{msg_id}` check (30s TTL, for edit debounce) should be unified with the new `prism:cancel:{msg_id}` check. At both checkpoints (after debounce and before Prism push):

```python
if await redis_client.exists(f"prism:cancel:{message_id}"):
    logger.info(f"Skipping edit for deleted message {message_id}")
    return
```

### 4. `apps/bot/services/broadcast/payloads.py` — Reply parent check

In `PayloadBuilder._resolve_original_message_id()` or the reply-building path, check if the parent message was cancelled:

```python
if parent_msg_id and await redis_client.exists(f"prism:cancel:{parent_msg_id}"):
    # Parent was deleted — skip reply broadcast or mark as "parent unavailable"
    return None  # or raise a sentinel
```

---

## Changes — Elixir Prism (`interchat-broadcast-worker`)

### 1. New module: `lib/prism/cancel_checker.ex`

```elixir
defmodule Prism.CancelChecker do
  @moduledoc """
  Checks whether a message has been cancelled (source deleted on Discord).
  Reads the Redis key `prism:cancel:{message_id}`.
  """

  @cancel_prefix "prism:cancel:"

  @doc """
  Returns `true` if the message has been cancelled.
  """
  @spec cancelled?(String.t()) :: boolean()
  def cancelled?(message_id) when is_binary(message_id) do
    case Redix.command(:my_redix, ["EXISTS", "#{@cancel_prefix}#{message_id}"]) do
      {:ok, 1} -> true
      {:ok, 0} -> false
      {:error, reason} ->
        require Logger
        Logger.warning("CancelChecker: Redis error checking #{message_id}: #{inspect(reason)}")
        false  # Fail open — don't block processing on Redis errors
    end
  end
end
```

### 2. `lib/prism/fanout_broadway.ex` — Batch-level check

In `handle_message/3`, after key expansion and before `Task.async_stream` fan-out:

```elixir
def handle_message(_processor, message, _context) do
  # ... existing backpressure gate, parse, expand_keys ...

  message_id = payload["message_id"] || payload["m"]  # handle both expanded and minified

  if message_id && Prism.CancelChecker.cancelled?(message_id) do
    Logger.info("FanoutBroadway: skipping cancelled batch batch_id=#{batch_id} message_id=#{message_id}")
    message  # ack and return without processing
  else
    # ... existing fan-out logic ...
  end
end
```

**Note on minified keys:** The check must happen AFTER `expand_keys/1` so `message_id` is available in long form. If the payload arrives minified, `payload["m"]` contains the message_id short key.

### 3. `lib/prism/discord_worker.ex` — Retry payload enrichment

In `spawn_retry/13`, ensure the retry payload map includes the source `message_id` (or `batch_id`) so the retry path can check cancellation:

```elixir
defp spawn_retry(webhook_id, webhook_token, message_id, action, payload, ...) do
  retry_payload = %{
    # ... existing fields ...
    "source_message_id" => source_message_id,  # NEW: for cancellation check
    "batch_id" => batch_id,                     # NEW: for cancellation check
  }
  Prism.DelayedQueue.enqueue(retry_payload, delay_ms)
end
```

### 4. `lib/prism/retry_broadway.ex` — Retry cancellation check

In `process_retry/3`, before reconstructing and sending the HTTP request:

```elixir
def process_retry(data, ack_data, context) do
  source_message_id = data["source_message_id"] || data["batch_id"]

  if source_message_id && Prism.CancelChecker.cancelled?(source_message_id) do
    Logger.info("RetryBroadway: skipping cancelled retry for #{source_message_id}")
    publish_partial(data, :cancelled, context)  # or silent skip
  else
    # ... existing retry logic ...
  end
end
```

**Decision on cancelled retries:** Silent skip (no callback) to match the batch-level behavior. The retry item is simply dropped.

### 5. `config/runtime.exs` — Configuration

```elixir
config :prism, :cancel_ttl,
  default: String.to_integer(System.get_env("PRISM_CANCEL_TTL", "300"))
```

---

## What We Do NOT Change

- **Delayed queue ZSET**: No active removal. Items migrate to retry stream naturally and are caught by the `process_retry` check.
- **Dead message cache**: Unchanged. Still handles 404s for broadcast copies deleted independently on target servers.
- **Callback stream**: No new callback types. Cancelled batches are silently skipped.
- **Per-target checks**: Not needed. Batch-level check in FanoutBroadway is sufficient. The sub-second window between batch check and target processing is acceptable.

---

## Edge Cases

| Scenario | Handling |
|---|---|
| **Cancellation arrives between batch check and target processing** | Window is <1s. Rare misses handled by existing 404 (10008) dead message cache. |
| **Bulk delete (multiple messages at once)** | Multiple `SETEX` calls in rapid succession. No key conflicts (each message has unique ID). |
| **Broadcast copy deleted on target server (not source)** | Bot doesn't detect this. Existing dead message cache + 404 path handles it. |
| **Redis unavailable during cancellation check** | `CancelChecker` fails open (returns `false`), allowing processing to proceed. Logged as warning. |
| **Message ID not present in payload** | Check is skipped (fails safe). Only applies if payload is malformed. |
| **PENDING message deleted before any shard completes** | Bot directly finalizes deletion (sets `status=DELETED`) instead of relying on deferred activation. |

---

## Implementation Order

1. **Prism: `CancelChecker` module** — standalone, no dependencies, testable in isolation
2. **Prism: `FanoutBroadway` check** — batch-level interception
3. **Prism: `discord_worker.ex` retry enrichment** — add `source_message_id`/`batch_id` to retry payload
4. **Prism: `RetryBroadway` check** — retry-level interception
5. **Prism: `runtime.exs` config** — `PRISM_CANCEL_TTL` env var
6. **Python: `messageDelete.py`** — set cancellation flag + PENDING race fix
7. **Python: `onMessage.py`** — pre-flight check
8. **Python: `onMessageEdit.py`** — unify cancellation checks
9. **Python: `payloads.py`** — reply parent check

## Validation

- **Unit test**: `CancelChecker.cancelled?/1` with Redis mock (key exists → true, key absent → false, Redis error → false)
- **Integration test**: Push a batch to fast stream, set `prism:cancel:{msg_id}`, verify batch is acked without fan-out
- **End-to-end**: Send message → delete it → verify no Prism HTTP calls made for that message
- **Retry path**: Enqueue a retry item, set cancellation flag, trigger scheduler, verify retry is skipped
