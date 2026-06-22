# Fix Broadway Queue Time Spikes (5-6s delays)

## Problem

Batch messages sit unclaimed in Redis streams for 1.5–6 seconds before Broadway processors pick them up. The queue time (`polled_at - enqueued_at`) spikes when multiple large (80-target) batches arrive simultaneously, blocking Broadway processors that would otherwise handle the steady stream of 1-target batches.

## Root Cause: Head-of-Line Blocking

`handle_message/3` in `FanoutBroadway` calls `process_batch/11` **synchronously**. An 80-target batch occupies a Broadway processor for 1–3.4 seconds (observed from logs). During this time:

1. The blocked processor emits zero demand upstream
2. Its internal FIFO queue backs up — any 1-target "mice" behind a large "elephant" batch wait the full duration
3. When 3–4 eighty-target batches hit simultaneously, queue times cascade to 5–6s

The load pattern is "mostly 1-target batches with occasional 80-target bursts," making head-of-line blocking the dominant failure mode.

## Solution: Async Batch Dispatch + Config Tuning

Three phases, ordered by speed of deployment and impact:

### Phase 1: Immediate Config Tuning (no code changes)

Increase concurrency to give headroom. This mitigates but doesn't fix the root cause — within each processor's FIFO queue, elephants still block mice.

**Changes:**
- Set `PRISM_BROADWAY_CONCURRENCY=200` (was 50)
- Set `PRISM_FINCH_POOL_COUNT=100` (was 50)

**Expected effect:** Queue time drops from 5-6s to 1-3s under burst. 1-target batch latency improves because more processors are available to handle them while a few are tied up with large batches.

**Validation:** Deploy, observe `Queue Time` in debug logs under normal + burst load. Verify no CPU exhaustion (monitor Erlang run queue via `MetricsLogger`).

**Rollback:** Revert env vars.

---

### Phase 2: Async Batch Dispatch (code change)

Decouple batch processing from Broadway message handling. `handle_message/3` spawns the batch as an async task and returns immediately (<1ms), freeing the processor to pull the next message. A concurrency cap prevents runaway task creation under extreme load.

**File changed:** `lib/prism/fanout_broadway.ex`

#### 2a. Add config key

In `config/runtime.exs`, add:

```elixir
max_async_batches: String.to_integer(System.get_env("PRISM_MAX_ASYNC_BATCHES") || "300")
```

#### 2b. Modify `handle_message/3` — the `else` branch (line ~160)

Replace the synchronous `process_batch(...)` call with async dispatch:

```elixir
# Current (lines ~160-233):
else
  polled_at = :os.system_time(:millisecond)
  ...
  case Jason.decode(payload_json) do
    {:ok, raw} ->
      payload = expand_keys(raw)
      ...
      process_batch(action, batch_id, discord_payload, targets,
        polled_at, enqueued_at, parent_message_id, metadata, hub_id, shard_index)
      message
    ...
  end
end

# New:
else
  polled_at = :os.system_time(:millisecond)
  ...
  case Jason.decode(payload_json) do
    {:ok, raw} ->
      payload = expand_keys(raw)
      ...
      max_async = Application.get_env(:prism, :max_async_batches, 300)
      active_ref = :persistent_term.get(:active_batches, nil)
      current = if active_ref, do: :atomics.get(active_ref, 1), else: 0

      if current < max_async do
        spawn_async_batch(action, batch_id, discord_payload, targets,
          polled_at, enqueued_at, parent_message_id, metadata, hub_id, shard_index)
        message
      else
        Logger.warning(
          "Async batch cap reached (#{current}/#{max_async}). " <>
          "Re-enqueueing batch #{batch_id} to delayed queue (200ms)."
        )
        Prism.DelayedQueue.enqueue(payload, 200)
        message
      end
    ...
  end
end
```

#### 2c. Add `spawn_async_batch/11` private function

```elixir
defp spawn_async_batch(action, batch_id, discord_payload, targets, polled_at,
                       enqueued_at, parent_message_id, metadata, hub_id, shard_index) do
  Task.Supervisor.start_child(Prism.TaskSup, fn ->
    try do
      process_batch(action, batch_id, discord_payload, targets, polled_at,
                    enqueued_at, parent_message_id, metadata, hub_id, shard_index)
    rescue
      e ->
        Logger.error(
          "Async batch #{batch_id} crashed: #{Exception.message(e)}\n" <>
          Exception.format_stacktrace(__STACKTRACE__)
        )
        # Re-enqueue for retry with a backoff
        payload = %{
          "action" => action,
          "batch_id" => batch_id,
          "payload" => discord_payload,
          "targets" => targets,
          "message_id" => parent_message_id,
          "metadata" => metadata,
          "hub_id" => hub_id,
          "shard_index" => shard_index
        }
        Prism.DelayedQueue.enqueue(payload, 5_000)
    end
  end)

  :ok
end
```

**Key design decisions:**
- Uses existing `Prism.TaskSup` (already supervised in Application)
- Uses existing `:active_batches` atomics counter for the cap check
- Soft cap: TOCTOU race between read and spawn means brief overshoot by at most `broadway_concurrency` tasks — acceptable for a safety limit
- On crash, re-enqueues to delayed queue with 5s backoff for retry
- `process_batch/11` is unchanged — the `active_batches` increment/decrement inside it still works, and all callback/SSE/reply-index logic runs in the async task

#### 2d. Remove the `nil` check on `active_batches` ref from `process_batch`

Currently line ~506 in `fanout_broadway.ex`:

```elixir
after
  if ref = :persistent_term.get(:active_batches, nil) do
    :atomics.sub(ref, 1, 1)
  end
end
```

This `nil` check is a safety guard. Since `spawn_async_batch` also checks for `nil`, both should be consistent. No change needed — the guard is harmless.

**Expected effect:** Broadway processors never block for >1ms. Queue time stays near zero regardless of batch size. The `max_async_batches` cap (300) prevents memory exhaustion: at 300 concurrent 80-target batches × ~80KB each = ~24MB, well within safe limits.

**Validation:**
1. Deploy to staging, verify `Queue Time` stays <100ms for 99% of batches
2. Flood with 80-target batches, verify cap triggers and delayed queue drains smoothly
3. Check `MetricsLogger` for TaskSup child count and Erlang process count
4. Verify callbacks still publish (check `discord:fanout:callbacks` stream)
5. Verify SSE still publishes for `shard_index == 0` batches

**Rollback:** Revert `fanout_broadway.ex` to previous version and redeploy.

---

### Phase 3: Enhanced Monitoring

Add stream-length tracking to detect Redis backlog buildup before it impacts latency.

#### 3a. Add stream length to `MetricsLogger`

In `lib/prism/metrics_logger.ex`, add to the `handle_info(:log_metrics, ...)` function:

```elixir
# After existing metrics collection, add:
fast_stream = Application.get_env(:prism, :redis_stream_fast, "discord:fanout:stream:fast")
slow_stream = Application.get_env(:prism, :redis_stream_slow, "discord:fanout:stream:slow")

fast_len = stream_length(fast_stream)
slow_len = stream_length(slow_stream)

Logger.info(
  "[Metrics] ... | Fast Stream Len: #{fast_len} | Slow Stream Len: #{slow_len}"
)
```

Add helper:

```elixir
defp stream_length(stream_key) do
  idx = :erlang.phash2(System.unique_integer(), 5)
  case Redix.command(:"my_redix_#{idx}", ["XLEN", stream_key]) do
    {:ok, len} -> len
    _ -> -1
  end
end
```

#### 3b. Add queue-time warning threshold

In `fanout_broadway.ex` `process_batch/11`, after the existing queue time log, add:

```elixir
if queue_time > 2_000 do
  Logger.warning(
    "High queue time for batch #{batch_id}: #{queue_time}ms (Targets: #{length(targets)})"
  )
end
```

## Risks

| Risk | Mitigation |
|------|-----------|
| Async task crash loses batch | `rescue` in `spawn_async_batch` re-enqueues to delayed queue |
| `max_async_batches` too low → throughput bottleneck | Start at 300, monitor, tune upward if needed |
| `max_async_batches` too high → memory exhaustion | 300 × ~80KB ≈ 24MB; Erlang process limit is default 262,144 |
| Lost Broadway backpressure | Cap prevents unbounded growth; delayed queue absorbs overflow |
| Finch pool saturation | HTTP/2 multiplexing + 100 connections handles 10k+ concurrent streams |

## Tasks (ordered)

1. **Set env vars:** `PRISM_BROADWAY_CONCURRENCY=200`, `PRISM_FINCH_POOL_COUNT=100` — deploy, verify improvement
2. **Add config:** `PRISM_MAX_ASYNC_BATCHES` to `config/runtime.exs`
3. **Add `spawn_async_batch/11`** to `lib/prism/fanout_broadway.ex`
4. **Modify `handle_message/3`** else-branch to call `spawn_async_batch` instead of `process_batch` directly, with cap check
5. **Add stream-length logging** to `lib/prism/metrics_logger.ex`
6. **Add queue-time warning** to `process_batch/11` in `fanout_broadway.ex`
7. **Deploy, observe metrics**, tune `max_async_batches` if needed
