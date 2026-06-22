# Preflight Redis Batching for Small-Target Fan-Out

## Problem

Every `process_target/7` call independently makes **up to 4 separate `redix_command` calls** to Redis:

| Call | Location | Purpose |
|------|----------|---------|
| `GET checkpoint:*` | `discord_worker.ex:63` | Check if target already processed |
| `EVAL Bucket.acquire` | `bucket.ex:90` | Rate-limit pre-flight check |
| `SETEX checkpoint:*` | `discord_worker.ex:196` | Write success checkpoint |
| `EVAL Bucket.update` | `bucket.ex:123` | Update rate-limit from response |

For a 1-target batch: 4 Redis round-trips. For 5 targets: 20 round-trips. At 10K small batches/sec, that's **40K–200K Redis ops/sec**, making Redis the bottleneck before HTTP.

The checkpoint `GET` is a **guaranteed cache miss** on the initial pass (no target processed yet), making those calls 100% wasted work. They only pay off on retries where a subset already succeeded.

## Solution

Batch all pre-flight Redis operations into pipelines **before** the `Task.async_stream` fan-out, and aggregate post-flight writes into a pipeline after all tasks complete. This reduces Redis round-trips from **4N to ~4 total** (2 pre-flight pipelines + 2 post-flight pipelines), regardless of batch size.

### Architecture Diagram

```
BEFORE (per-target Redis):
  process_batch
    ├── Task.async_stream(targets)
    │   ├── process_target(t1) → [GET ck][EVAL rl] → HTTP → [SETEX ck][EVAL update]
    │   ├── process_target(t2) → [GET ck][EVAL rl] → HTTP → [SETEX ck][EVAL update]
    │   └── process_target(tN) → [GET ck][EVAL rl] → HTTP → [SETEX ck][EVAL update]
    └── aggregate results

AFTER (batched pre-flight + post-flight):
  process_batch
    ├── PREFLIGHT: pipeline([GET ck1, GET ck2, ...]) → 1 round-trip
    ├── PREFLIGHT: pipeline([EVAL rl1, EVAL rl2, ...]) → 1 round-trip
    ├── FILTER: remove already-done, defer long-rate-limited
    ├── Task.async_stream(ready_targets)
    │   ├── process_target(t1) → HTTP (no Redis reads)
    │   └── process_target(tN) → HTTP (no Redis reads)
    ├── POSTFLIGHT: pipeline([SETEX ck1, SETEX ck2, ...]) → 1 round-trip
    └── aggregate results
```

## Files to Create/Modify

| File | Action | Purpose |
|------|--------|---------|
| `lib/prism/fanout_broadway/preflight.ex` | **NEW** | Preflight orchestration: checkpoint + rate-limit batched reads |
| `lib/prism/fanout_broadway/batch.ex` | **MODIFY** | `process_batch/10` orchestrates preflight → filter → fan-out → postflight |
| `lib/prism/discord_worker.ex` | **MODIFY** | `process_target/7` accepts optional `opts`, skips Redis when preflight data provided; `process_retry/3` accepts `opts` |
| `lib/prism/discord_worker/retry.ex` | **MODIFY** | Remove `source_message_id` duplicate field |
| `lib/prism/helpers.ex` | **MODIFY** | Add `checkpoint_key/3` helper (single source of truth for checkpoint key format) |
| `lib/prism/rate_limit/bucket.ex` | **MODIFY** | Expose `acquire_script/0`, `acquire_pipeline_commands/1`, `global_key/0` (all `@doc false`) |
| `lib/prism/config.ex` | **MODIFY** | Add `preflight_batching_enabled?/0` feature gate |
| `.env.example` | **MODIFY** | Add `PRISM_PREFLIGHT_BATCHING_ENABLED` env var |

## Implementation Steps

### Step 1: Add feature gate config

**File:** `lib/prism/config.ex`

Add after the Broadway tuning section:
```elixir
@doc "Enable/disable preflight Redis batching (pipelines checkpoint + rate-limit reads before fan-out)"
def preflight_batching_enabled?,
  do: Application.get_env(:prism, :preflight_batching_enabled, true)
```

Add to `.env.example`:
```bash
# Enable/disable preflight Redis batching for batch processing
# PRISM_PREFLIGHT_BATCHING_ENABLED=true
```

### Step 2: Add checkpoint_key helper to Helpers

**File:** `lib/prism/helpers.ex`

Add a single-source-of-truth function for checkpoint key construction. This replaces the two inline string interpolations currently at `discord_worker.ex:58` and `discord_worker.ex:610`, and will be used by the Preflight module:

```elixir
@doc """
Builds a Redis checkpoint key from batch metadata.
Format: `checkpoint:<action>:<batch_id>:<webhook_id>`
"""
@spec checkpoint_key(String.t(), String.t(), String.t()) :: String.t()
def checkpoint_key(action, batch_id, webhook_id),
  do: "checkpoint:#{action}:#{batch_id}:#{webhook_id}"
```

### Step 3: Expose Bucket internals for pipeline use

**File:** `lib/prism/rate_limit/bucket.ex`

- Make `bucket_key/2` public (already `@doc false`) — no change needed
- Make `global_key/0` public with `@doc false`
- Expose the `@acquire_script` as a public function `acquire_script/0` (returns the Lua source string)
- Expose a new function `acquire_pipeline_commands/1` that takes a list of `{webhook_id, method_str}` tuples and returns a list of `["EVAL", script, "2", key, g_key, now]` commands ready for `redix_pipeline`

```elixir
@doc false
def acquire_script, do: @acquire_script

@doc false
def acquire_pipeline_commands(targets) when is_list(targets) do
  g_key = global_key()
  now_ms = System.monotonic_time(:millisecond)
  script = @acquire_script

  Enum.map(targets, fn {webhook_id, method_str} ->
    key = bucket_key(webhook_id, method_str)
    ["EVAL", script, "2", key, g_key, to_string(now_ms)]
  end)
end
```

Also expose `update_script/0` and `update_pipeline_commands/1` for post-flight batching (step 5).

### Step 4: Create Preflight module

**File:** `lib/prism/fanout_broadway/preflight.ex` (NEW)

```elixir
defmodule Prism.FanoutBroadway.Preflight do
  @moduledoc """
  Batched pre-flight checks for all targets in a batch.
  Pipelines checkpoint GETs and rate-limit EVALs before fan-out to eliminate
  per-target Redis round-trips.

  Used by `Prism.FanoutBroadway.Batch.process_batch/10` when
  `Prism.Config.preflight_batching_enabled?/0` is true.
  """

  alias Prism.Helpers
  require Logger

  @type checkpoint_result :: :not_found | {:done} | {:ok, String.t()}
  @type rate_limit_result :: {:ok, integer()} | {:blocked, integer()}
  @type preflight_map :: %{
    target: map(),
    webhook_id: String.t(),
    preflight: %{checkpoint: checkpoint_result(), rate_limit: rate_limit_result()}
  }

  @doc """
  Runs batched pre-flight checks for all targets.

  Returns a list of `preflight_map` structs, one per target, or
  `{:error, reason}` if either pipeline fails (caller should fall back to
  per-target Redis calls).
  """
  @spec run([map()], String.t(), String.t() | nil) :: {:ok, [preflight_map()]} | {:error, term()}
  def run(targets, action, batch_id)

  def run(targets, action, batch_id) when is_list(targets) and is_binary(batch_id) do
    method_str = Helpers.action_to_method_string(action)

    target_infos = Enum.map(targets, fn target ->
      webhook_id = Map.get(target, "webhook_id")
      %{
        target: target,
        webhook_id: webhook_id,
        checkpoint_key: Helpers.checkpoint_key(action, batch_id, webhook_id),
        bucket_method: method_str
      }
    end)

    with {:ok, checkpoint_results} <- pipeline_checkpoints(target_infos),
         {:ok, rate_limit_results} <- pipeline_rate_limits(target_infos) do
      preflights =
        Enum.zip_with(
          [target_infos, checkpoint_results, rate_limit_results],
          fn [ti, ck_res, rl_res] ->
            %{
              target: ti.target,
              webhook_id: ti.webhook_id,
              preflight: %{
                checkpoint: parse_checkpoint_result(ck_res),
                rate_limit: parse_rate_limit_result(rl_res)
              }
            }
          end
        )

      {:ok, preflights}
    end
  end

  def run(_targets, _action, nil), do: {:ok, []}

  # ── Pipeline helpers ────────────────────────────────────────────────

  defp pipeline_checkpoints(target_infos) do
    commands = Enum.map(target_infos, fn ti -> ["GET", ti.checkpoint_key] end)

    case Helpers.redix_pipeline(commands) do
      {:ok, results} -> {:ok, results}
      {:error, reason} ->
        Logger.warning("Preflight checkpoint pipeline failed: #{inspect(reason)}")
        {:error, reason}
    end
  end

  defp pipeline_rate_limits(target_infos) do
    acquire_targets = Enum.map(target_infos, fn ti ->
      {ti.webhook_id, ti.bucket_method}
    end)

    commands = Prism.RateLimit.Bucket.acquire_pipeline_commands(acquire_targets)

    case Helpers.redix_pipeline(commands) do
      {:ok, results} -> {:ok, results}
      {:error, reason} ->
        Logger.warning("Preflight rate-limit pipeline failed: #{inspect(reason)}")
        {:error, reason}
    end
  end

  # ── Result parsers ──────────────────────────────────────────────────

  defp parse_checkpoint_result("done"), do: {:done}
  defp parse_checkpoint_result(msg_id) when is_binary(msg_id) and byte_size(msg_id) > 0,
    do: {:ok, msg_id}
  defp parse_checkpoint_result(_), do: :not_found

  defp parse_rate_limit_result([1, remaining, _]) when is_integer(remaining),
    do: {:ok, remaining}
  defp parse_rate_limit_result([0, _, ttl_ms]) when is_integer(ttl_ms),
    do: {:blocked, ttl_ms}
  defp parse_rate_limit_result(_), do: {:ok, -1}  # Error fallback: allow through

  # checkpoint_key/3 is defined in Prism.Helpers
end
```

### Step 5: Add 1-target fast path and preflight batching to process_batch

**File:** `lib/prism/fanout_broadway/batch.ex`

Modify `process_batch/10` to:

1. Check `Prism.Config.preflight_batching_enabled?/0`
2. If enabled, run preflight checks; split targets into `ready`, `deferred`, and `done`
3. Enqueue deferred targets directly to delayed queue
4. If only 1 ready target, call `process_target` directly (no Task.async_stream overhead)
5. If ≥2 ready targets, use `Task.async_stream` as before
6. After all tasks complete, pipeline checkpoint writes for successful targets
7. Merge results from `ready` + `done` targets for callback aggregation

**Detailed changes to `process_batch/10` (replace lines 67-170):**

Before the existing `Task.async_stream` block, insert preflight logic:

```elixir
# Pre-flight: batch Redis checkpoint + rate-limit reads (gated)
preflight_enabled = Prism.Config.preflight_batching_enabled?()
defer_threshold = Prism.Config.rate_limit_defer_threshold_ms()

{targets_to_process, preflight_done_targets, preflight_deferred_targets} =
  if preflight_enabled do
    case Prism.FanoutBroadway.Preflight.run(targets, action, batch_id) do
      {:ok, preflights} ->
        {ready, deferred, done} =
          Enum.reduce(preflights, {[], [], []}, fn pf, {r_acc, d_acc, dn_acc} ->
            case pf.preflight.checkpoint do
              {:done} ->
                {r_acc, d_acc, [{:done, pf} | dn_acc]}
              {:ok, msg_id} ->
                {r_acc, d_acc, [{:ok, msg_id, pf} | dn_acc]}
              :not_found ->
                case pf.preflight.rate_limit do
                  {:blocked, ttl} when ttl > defer_threshold ->
                    {r_acc, [%{pf | delay_ms: ttl} | d_acc], dn_acc}
                  _ ->
                    {[pf | r_acc], d_acc, dn_acc}
                end
            end
          end)

        # Enqueue deferred targets directly (long rate-limit blocks)
        for pf <- deferred do
          target = pf.target
          webhook_id = Map.get(target, "webhook_id")
          message_id = Map.get(target, "message_id")
          base_url = "#{Prism.Config.discord_base_url()}/api/webhooks/#{webhook_id}/#{Map.get(target, "webhook_token")}"
          thread_id = Map.get(target, "thread_id")

          case Prism.DiscordWorker.HTTP.build_request(action, base_url, message_id, thread_id) do
            {:ok, method, url} ->
              parent_msg_id = if action == "execute", do: parent_message_id, else: nil
              Prism.DiscordWorker.Retry.spawn_retry(
                action, target, method, url, [{"Content-Type", "application/json"}],
                if(action == "delete", do: nil, else: Jason.encode_to_iodata!(Map.merge(discord_payload, Map.get(target, "overrides", %{})))),
                webhook_id, message_id, batch_id, pf.delay_ms, 1, parent_msg_id, :rate_limited
              )
            {:error, _} -> :ok
          end
        end

        {ready, done, []}

      {:error, _reason} ->
        # Preflight failed: fall back to per-target Redis calls
        {targets, [], []}
    end
  else
    {targets, [], []}
  end

ctx = OpenTelemetry.Ctx.get_current()

results =
  case targets_to_process do
    [] ->
      []

    [single_pf] when is_map(single_pf) and preflight_enabled ->
      # Fast path: single target, skip Task.async_stream overhead
      OpenTelemetry.Ctx.attach(ctx)
      result = Prism.DiscordWorker.process_target(
        action, single_pf.target, discord_payload,
        batch_id, polled_at, enqueued_at, parent_message_id,
        preflight: single_pf.preflight, skip_checkpoint_write: true
      )
      [{:ok, result}]

    [single_target] when is_map(single_target) ->
      # Fast path without preflight: single target, direct call
      OpenTelemetry.Ctx.attach(ctx)
      result = Prism.DiscordWorker.process_target(
        action, single_target, discord_payload,
        batch_id, polled_at, enqueued_at, parent_message_id
      )
      [{:ok, result}]

    multiple_targets ->
      # Fan-out with bounded concurrency
      Task.async_stream(
        multiple_targets,
        fn item ->
          OpenTelemetry.Ctx.attach(ctx)
          if preflight_enabled do
            Prism.DiscordWorker.process_target(
              action, item.target, discord_payload,
              batch_id, polled_at, enqueued_at, parent_message_id,
              preflight: item.preflight, skip_checkpoint_write: true
            )
          else
            Prism.DiscordWorker.process_target(
              action, item, discord_payload,
              batch_id, polled_at, enqueued_at, parent_message_id
            )
          end
        end,
        max_concurrency: batch_max_concurrency,
        timeout: Prism.Config.task_timeout_ms()
      )
      |> Enum.to_list()
  end

# Merge preflight done-target results into results list
done_results = Enum.map(preflight_done_targets, fn
  {:done, pf} -> {:ok, {:ok, nil}}
  {:ok, msg_id, pf} -> {:ok, {:ok, msg_id}}
end)

all_results = results ++ done_results

# Post-flight: batch checkpoint writes for successful targets
if preflight_enabled do
  checkpoint_ttl = to_string(Prism.Config.checkpoint_ttl_seconds())
  checkpoint_commands =
    Enum.zip(targets_to_process, results)
    |> Enum.filter(fn {_target_pf, {:ok, {:ok, _msg_id}}} -> true
                      {_target_pf, {:ok, result}} when is_tuple(result) ->
                        elem(result, 0) == :ok
                      _ -> false end)
    |> Enum.map(fn {item, {:ok, worker_result}} ->
      ck_result = case worker_result do
        {:ok, msg_id} when is_binary(msg_id) -> msg_id
        {:ok, _} -> "done"
        _ -> "done"
      end
      webhook_id = if preflight_enabled, do: item.webhook_id, else: Map.get(item, "webhook_id")
      ck = Helpers.checkpoint_key(action, batch_id, webhook_id)
      ["SETEX", ck, checkpoint_ttl, ck_result]
    end)

  if checkpoint_commands != [] do
    Helpers.redix_pipeline(checkpoint_commands)
  end
end
```

Then continue with the existing success/failure aggregation (lines 88+), using `all_results` and `targets_to_process ++ preflight_done_targets_targets` for the zip.

### Step 6: Modify process_target to accept preflight opts

**File:** `lib/prism/discord_worker.ex`

Add a new 8-arity function clause that accepts `opts`:

```elixir
def process_target(
      action,
      %{"webhook_id" => webhook_id, "webhook_token" => webhook_token} = target,
      content,
      batch_id,
      polled_at,
      enqueued_at,
      parent_message_id,
      opts \\ []
    )
```

Inside this clause, add preflight-aware checkpoint and rate-limit checks:

```elixir
preflight = Keyword.get(opts, :preflight)
skip_checkpoint_write = Keyword.get(opts, :skip_checkpoint_write, false)

# ── Checkpoint check (preflight-aware) ────────────────────────────────
cached_result =
  cond do
    preflight && preflight.checkpoint == :not_found -> nil
    preflight && preflight.checkpoint == {:done} -> {:ok, nil}
    preflight && match?({:ok, _}, preflight.checkpoint) ->
      {:ok, elem(preflight.checkpoint, 1)}
    batch_id ->
      checkpoint_key = Helpers.checkpoint_key(action, batch_id, webhook_id)
      case Helpers.redix_command(["GET", checkpoint_key]) do
        {:ok, "done"} -> {:ok, nil}
        {:ok, msg_id} when is_binary(msg_id) -> {:ok, msg_id}
        _ -> nil
      end
    true -> nil
  end

# ── Rate-limit check (preflight-aware) ────────────────────────────────
# ... existing dead_message_cache check ...
# ... existing backpressure check ...

{should_defer, should_sleep, rate_limit_delay_ms} =
  cond do
    preflight ->
      case preflight.rate_limit do
        {:ok, _remaining} -> {false, false, 0}
        {:blocked, ttl} ->
          if ttl > Prism.Config.rate_limit_defer_threshold_ms(),
            do: {true, false, ttl},
            else: {false, true, ttl}
      end
    true ->
      case Prism.RateLimit.check(webhook_id, method_str) do
        {:ok, _remaining} -> {false, false, 0}
        {:blocked, ttl_ms} ->
          if ttl_ms > Prism.Config.rate_limit_defer_threshold_ms(),
            do: {true, false, ttl_ms},
            else: {false, true, ttl_ms}
      end
  end
```

Then, for the checkpoint write section (lines 191-213), wrap with skip condition:

```elixir
if batch_id and not skip_checkpoint_write do
  checkpoint_ttl = to_string(Prism.Config.checkpoint_ttl_seconds())
  checkpoint_key = Helpers.checkpoint_key(action, batch_id, webhook_id)
  # ... existing SETEX logic ...
end
```

### Step 7: Remove retry payload field duplication

**File:** `lib/prism/discord_worker/retry.ex`

Remove the redundant `"source_message_id"` field (line 56) since `process_retry/3` already falls back to `payload["parent_msg_id"]` (line 344):

```elixir
# BEFORE:
"parent_msg_id" => parent_msg_id,
"source_message_id" => parent_msg_id,  # DELETE THIS LINE

# AFTER:
"parent_msg_id" => parent_msg_id,
```

### Step 8: Update process_retry to accept skip_checkpoint_write

**File:** `lib/prism/discord_worker.ex` — `process_retry/3`

The `process_retry/3` function (line 342) writes checkpoints on success (lines 609-619). For now, retries are not batched, so this step adds the `skip_checkpoint_write` option for future use but doesn't change the RetryBroadway call site:

```elixir
def process_retry(payload, polled_at, enqueued_at, opts \\ []) do
  skip_checkpoint_write = Keyword.get(opts, :skip_checkpoint_write, false)
  # ... existing logic ...
  
  # Checkpoint write:
  if batch_id and not skip_checkpoint_write do
    # ... existing logic ...
  end
end
```

## Edge Cases Handled

| Scenario | Handling |
|----------|----------|
| Preflight pipeline fails (Redis error) | Fall back to per-target Redis calls via graceful `with` block |
| Target missing `webhook_id` | `Map.get(target, "webhook_id")` returns `nil`; checkpoint key becomes `checkpoint:*:*:nil`; rate-limit skips |
| Empty checkpoint pipeline (no targets) | `redix_pipeline([])` returns `{:ok, []}` — handled naturally |
| Mixed already-done + new targets | Checkpoint hits skip processing; results merged from checkpoint data |
| Long rate-limit TTL (> 10s default) | Deferred in preflight → enqueued directly to delayed queue → no Task overhead |
| Short rate-limit TTL (< 10s default) | Passed through to `process_target` which does `Process.sleep` |
| Single target in batch | Fast path: direct `process_target` call, no Task.async_stream |
| Feature gate disabled | Falls through to existing per-target Redis code — zero behavioral change |
| process_retry (individual retries) | Unchanged — retries are 1-at-a-time, no batching applied |
| Whole-batch retry via RetryBroadway→route_batch_to_fanout | Goes through FanoutBroadway → benefits from preflight batching |

## Metrics / Validation

After deployment, observe:

1. **Redis call count** per batch (via existing telemetry or Redis MONITOR):
   - Before: ~4N calls per batch
   - After (N=1): ~4 calls (preflight: 2 + postflight: 1) vs 4 before (minor improvement)
   - After (N=5): ~6 calls vs 20 before (3.3x reduction)
   - After (N=80): ~162 calls vs 320 before (2x reduction; rate-limit pipeline overhead grows)

2. **Batch latency** (via existing `queue_time` / `batch_time` logs):
   - Preflight adds ~2 pipeline round-trips (~0.5-1ms each)
   - Removes N × ~1ms per-target Redis round-trips
   - Net: should see lower p50/p95 batch time for N > 1

3. **Error rate**: Monitor `Preflight pipeline failed` log occurrences

4. **`process_retry` source_message_id fallback**: Verify that `process_retry/3` at line 344 correctly resolves `payload["source_message_id"] || payload["parent_msg_id"] || payload["batch_id"]` after removing `source_message_id` from retry payload

## Rollout Plan

1. Deploy with `PRISM_PREFLIGHT_BATCHING_ENABLED=false` (opt-in flag)
2. Enable on one node, monitor Redis metrics for 1 hour
3. Enable on 50% of nodes, compare callback latency percentiles
4. Enable on all nodes
5. After 1 week of stable operation, remove feature gate (always-on)

## Future Improvements (Out of Scope)

1. **Batched rate-limit Lua script**: Replace N individual EVAL calls with a single script that checks N buckets atomically — further reduces Redis protocol overhead for large batches
2. **Post-flight `Bucket.update` batching**: Pipeline the `handle_response` EVAL calls after all HTTP responses complete (requires `do_http_request` to defer the update)
3. **Retry batching**: Accumulate retry messages in RetryBroadway and process in batches with preflight batching
4. **Preflight caching**: Cache checkpoint results for recently-seen batches to avoid re-reading them on re-enqueue
