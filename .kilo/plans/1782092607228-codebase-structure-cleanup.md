# Codebase Structure Cleanup & Configurability

## Goal

Refactor the Prism codebase to:
1. Split large files following Elixir best practices
2. Extract duplicated code into shared helpers and a centralized config module
3. Make all previously hardcoded, opinionated values configurable via environment variables
4. Ensure the project is open-source friendly (no InterChat-specific defaults that can't be overridden)

---

## Task 1: Create `Prism.Config` — Centralized Configuration Module

**New file:** `lib/prism/config.ex`

Centralize all `Application.get_env(:prism, ...)` calls with defaults into a single module. This:
- Eliminates duplicated `Application.get_env` calls across files
- Provides a single source of truth for all configurable values
- Makes it easy to see every configurable knob in one place

```elixir
defmodule Prism.Config do
  # All getter functions with env var documentation and defaults

  # Redis
  def redis_opts, do: Application.get_env(:prism, :redis_opts, host: "localhost", port: 6379)
  def redis_host, do: ...
  def redis_port, do: ...
  def redix_pool_size, do: Application.get_env(:prism, :redix_pool_size, 5)

  # Stream keys
  def stream_fast, do: Application.get_env(:prism, :redis_stream_fast, "discord:fanout:stream:fast")
  def stream_slow, do: Application.get_env(:prism, :redis_stream_slow, "discord:fanout:stream:slow")
  def stream_retries, do: Application.get_env(:prism, :redis_retry_stream, "discord:fanout:stream:retries")
  def stream_callbacks, do: Application.get_env(:prism, :redis_callback_stream, "discord:fanout:callbacks")
  def redis_group, do: Application.get_env(:prism, :redis_group, "elixir_fanout_pool")

  # Delayed queue
  def delayed_zset_key, do: Application.get_env(:prism, :delayed_zset_key, "discord:fanout:delayed")
  def pubsub_channel, do: Application.get_env(:prism, :pubsub_channel, "prism:wakeup")

  # Discord / HTTP
  def discord_base_url, do: Application.get_env(:prism, :discord_base_url, "https://discord.com")
  def finch_pool_count, do: ...
  def finch_receive_timeout_ms, do: ...
  def finch_pool_timeout_ms, do: ...
  def finch_idle_timeout_ms, do: ...
  def finch_keepalive_ms, do: ...

  # Rate limiting
  def backpressure_enabled?, do: ...
  def backpressure_max_backoff_ms, do: ...
  def backpressure_min_cooldown_ms, do: ...
  def invalid_request_window_ms, do: ...
  def invalid_request_backpressure_threshold, do: ...
  def invalid_request_critical_threshold, do: ...
  def bucket_hash_ttl_seconds, do: ...

  # Retry parameters
  def server_error_base_delay_ms, do: ...
  def server_error_max_retries, do: ...
  def network_error_base_delay_ms, do: ...
  def network_error_max_retries, do: ...
  def message_not_found_max_retries, do: ...
  def rate_limit_defer_threshold_ms, do: ...
  def checkpoint_ttl_seconds, do: ...

  # Features (enable/disable)
  def dead_message_cache_enabled?, do: ...
  def key_expansion_enabled?, do: ...
  def cancel_checker_enabled?, do: ...
  def stream_trimmer_enabled?, do: ...

  # Dead message cache
  def dead_message_cache_prefix, do: ...
  def dead_message_cache_ttl_seconds, do: ...
  def cancel_prefix, do: ...

  # Callback consumer group (for stream trimmer)
  def callback_consumer_group, do: ...

  # Broadway tuning
  def slow_lane_threshold, do: ...
  def fast_receive_interval, do: ...
  def slow_receive_interval, do: ...
  def retry_receive_interval, do: ...
  def queue_time_warn_ms, do: ...
  def task_timeout_ms, do: ...

  # SSE
  def sse_enabled?, do: ...
  def sse_topic_prefix, do: ...

  # Reply index
  def reply_index_enabled?, do: ...

  # Cluster
  def cluster_topology, do: ...

  # Worker
  def worker_id, do: ...
end
```

**Replace all inline `Application.get_env` calls** in every module with `Prism.Config` calls.

---

## Task 2: Create `Prism.Helpers` — Shared Utility Module

**New file:** `lib/prism/helpers.ex`

Consolidate duplicated logic:

```elixir
defmodule Prism.Helpers do
  # Redis command wrapper (currently duplicated in discord_worker.ex, cancel_checker.ex)
  def redix_command(command)

  # Extract payload from Redis stream data (duplicated in fanout_broadway.ex, retry_broadway.ex)
  def get_payload_from_redis_data(data)

  # Empty payload check (duplicated in discord_worker.ex, fanout_broadway.ex)
  def empty_discord_payload?(payload)

  # Method string helpers
  def action_to_method_string(action)
  def safe_method_atom(method_str)
end
```

Update all callers to use `Prism.Helpers` instead of local private functions.

---

## Task 3: Split `discord_worker.ex` (932 lines → 5 files)

### 3a. `lib/prism/discord_worker.ex` (~400 lines)
Keep the core orchestration:
- `process_target/7` — main entry point for processing webhook targets
- `process_retry/3` — retry processing from retry pipeline
- Backpressure gate logic
- Dead message cache gate
- Pre-flight rate-limit check + defer/sleep logic
- Result handling (dispatching to retry spawner or callbacks)

### 3b. `lib/prism/discord_worker/http.ex` (~250 lines)
Request building and HTTP execution:
- `build_request/4` — URL/method construction for execute/edit/delete
- `do_http_request/7` — Finch HTTP call wrapper with telemetry
- `do_http_request_internal/7` — Core HTTP execution + response classification
- All HTTP status code handling (2xx, 429, 401/403, 400, 404, 5xx, network error)
- 404 sub-classification (10008 → message_not_found/dead cache, 10003/10015 → invalid_webhook)

### 3c. `lib/prism/discord_worker/retry.ex` (~60 lines)
Retry spawning:
- `spawn_retry/14` — encode retry payload and enqueue to delayed queue
- Uses `Prism.DelayedQueue.enqueue/2`

### 3d. `lib/prism/discord_worker/callbacks.ex` (~80 lines)
Partial callback publishing:
- `publish_partial/6` — publish individual retry result to callback stream
- Error reason to string/type mapping

### 3e. `lib/prism/discord_worker/dead_message.ex` (~60 lines)
Dead message cache:
- `dead_message_cached?/2` — check both `dead_msg:{webhook_id}:{message_id}` and `dead_msg:{message_id}`
- `cache_dead_message/3` — store dead message with configurable TTL
- Uses `Prism.Config` for prefix and TTL
- Gated by `Prism.Config.dead_message_cache_enabled?/0`

### 3f. Delete orphaned helpers from old file
After split, ensure no orphaned private functions remain. Move `empty_webhook_body?/1` to `Prism.Helpers.empty_discord_payload?/1`.

---

## Task 4: Split `fanout_broadway.ex` (669 lines → 4 files)

### 4a. `lib/prism/fanout_broadway.ex` (~280 lines)
Core Broadway pipeline:
- `start_link/1` — Broadway initialization with configurable stream keys
- `handle_message/3` — message handler (parse JSON, expand keys, gate checks, spawn async batch)
- Backpressure gate
- Cancel checker gate
- Async batch cap check

### 4b. `lib/prism/fanout_broadway/key_expansion.ex` (~100 lines)
Key expansion logic:
- `@key_map` — the 23-entry short→long key mapping (compile-time constant)
- `expand_keys/1` — recursive key expansion
- `do_expand_keys/1` — private expansion helper
- Gated by `Prism.Config.key_expansion_enabled?/0`
- Module docs explaining the key_map is InterChat-specific; users can disable key expansion

### 4c. `lib/prism/fanout_broadway/batch.ex` (~200 lines)
Batch processing:
- `process_batch/11` — Task.async_stream fan-out, success/failure aggregation
- `spawn_async_batch/10` — Task.Supervisor async spawning with crash recovery
- `store_reply_index/2` — Redis reply index storage
- Callback stream publishing (the main "batch done" callback)

### 4d. `lib/prism/fanout_broadway/sse.ex` (~100 lines)
SSE/Dashboard publishing:
- `publish_sse_event/...` — extract metadata, build SSE payload, publish to Redis PubSub
- InterChat-specific fields (authorId, guildId, badges, etc.) clearly documented as opinionated
- Gated by `Prism.Config.sse_enabled?/0`

---

## Task 5: Make Delayed Queue & Scheduler Keys Configurable

**Files:** `lib/prism/delayed_queue.ex`, `lib/prism/delayed_scheduler.ex`

### 5a. `delayed_queue.ex`
- Replace `@zset_key`, `@stream_key`, `@pubsub_channel` with `Prism.Config` calls
- `@zset_key` → `Prism.Config.delayed_zset_key/0`
- `@stream_key` → `Prism.Config.stream_retries/0`
- `@pubsub_channel` → `Prism.Config.pubsub_channel/0`

### 5b. `delayed_scheduler.ex`
- Replace `@pubsub_channel` with `Prism.Config.pubsub_channel/0`
- Remove duplicated constant

---

## Task 6: Make `retry_broadway.ex` Keys Configurable

**File:** `lib/prism/retry_broadway.ex`

- Replace hardcoded `"discord:fanout:stream:retries"` with `Prism.Config.stream_retries/0`
- Replace hardcoded `> 80` slow lane threshold with `Prism.Config.slow_lane_threshold/0`
- Replace hardcoded `receive_interval: 100` with `Prism.Config.retry_receive_interval/0`
- Replace `backpressure_enabled?` private function with `Prism.Config.backpressure_enabled?/0`
- Replace `get_payload_from_redis_data` with `Prism.Helpers.get_payload_from_redis_data/1`

---

## Task 7: Make `cancel_checker.ex` Configurable & Disableable

**File:** `lib/prism/cancel_checker.ex`

- Replace `@cancel_prefix` with `Prism.Config.cancel_prefix/0`
- Add `Prism.Config.cancel_checker_enabled?/0` gate — if disabled, `cancelled?/1` always returns `false`
- Replace local `redix_command/1` with `Prism.Helpers.redix_command/1`

---

## Task 8: Make `stream_trimmer.ex` Configurable

**File:** `lib/prism/stream_trimmer.ex`

- Replace hardcoded `"bot_team"` callback group with `Prism.Config.callback_consumer_group/0`
- Replace `@trim_interval_ms` with `Prism.Config` getter
- Add `Prism.Config.stream_trimmer_enabled?/0` — if disabled, GenServer does nothing

---

## Task 9: Make Rate Limit Thresholds Configurable

### 9a. `lib/prism/rate_limit/backpressure.ex`
- Replace `@max_backoff_ms` → `Prism.Config.backpressure_max_backoff_ms/0`
- Replace `@min_cooldown_ms` → `Prism.Config.backpressure_min_cooldown_ms/0`

### 9b. `lib/prism/rate_limit/invalid_request_tracker.ex`
- Replace `@window_ms` → `Prism.Config.invalid_request_window_ms/0`
- Replace `@backpressure_threshold` → `Prism.Config.invalid_request_backpressure_threshold/0`
- Replace `@critical_threshold` → `Prism.Config.invalid_request_critical_threshold/0`

### 9c. `lib/prism/rate_limit/bucket.ex`
- Replace `@hash_ttl_seconds` → `Prism.Config.bucket_hash_ttl_seconds/0`

---

## Task 10: Update `application.ex` — Cluster Name, Pool Size, Finch Timeouts

**File:** `lib/prism/application.ex`

### 10a. Cluster topology name
- Replace `:interchat` with `Prism.Config.cluster_topology/0` (default `:prism_cluster`)

### 10b. Redix pool size
- Replace hardcoded `0..4` loop with `0..(Prism.Config.redix_pool_size() - 1)`
- Update phash2 calls in all modules to use `Prism.Config.redix_pool_size()` instead of hardcoded `5`

### 10c. Finch timeouts
- Replace `conn_max_idle_time: 60_000` → `Prism.Config.finch_idle_timeout_ms/0`
- Replace `keepalive: 30_000` → `Prism.Config.finch_keepalive_ms/0`

### 10d. Replace inline `discord_base_url` with `Prism.Config.discord_base_url/0`

---

## Task 11: Update `config/runtime.exs` — Add All New Env Vars

**File:** `config/runtime.exs`

Add configuration for all new env vars with sensible defaults. Group by category:

```elixir
# Redis pool
config :prism, redix_pool_size: String.to_integer(System.get_env("PRISM_REDIX_POOL_SIZE") || "5")

# Retry stream key
config :prism, redis_retry_stream: System.get_env("REDIS_RETRY_STREAM") || "discord:fanout:stream:retries"

# Delayed queue
config :prism,
  delayed_zset_key: System.get_env("PRISM_DELAYED_ZSET_KEY") || "discord:fanout:delayed",
  pubsub_channel: System.get_env("PRISM_PUBSUB_CHANNEL") || "prism:wakeup"

# HTTP / Finch
config :prism,
  finch_receive_timeout_ms: String.to_integer(System.get_env("PRISM_FINCH_RECEIVE_TIMEOUT_MS") || "30000"),
  finch_pool_timeout_ms: String.to_integer(System.get_env("PRISM_FINCH_POOL_TIMEOUT_MS") || "10000"),
  finch_idle_timeout_ms: String.to_integer(System.get_env("PRISM_FINCH_IDLE_TIMEOUT_MS") || "60000"),
  finch_keepalive_ms: String.to_integer(System.get_env("PRISM_FINCH_KEEPALIVE_MS") || "30000")

# Rate limiting thresholds
config :prism,
  backpressure_max_backoff_ms: String.to_integer(System.get_env("PRISM_BACKPRESSURE_MAX_BACKOFF_MS") || "600000"),
  backpressure_min_cooldown_ms: String.to_integer(System.get_env("PRISM_BACKPRESSURE_MIN_COOLDOWN_MS") || "60000"),
  invalid_request_window_ms: String.to_integer(System.get_env("PRISM_INVALID_REQUEST_WINDOW_MS") || "600000"),
  invalid_request_backpressure_threshold: String.to_integer(System.get_env("PRISM_INVALID_REQUEST_BACKPRESSURE_THRESHOLD") || "9500"),
  invalid_request_critical_threshold: String.to_integer(System.get_env("PRISM_INVALID_REQUEST_CRITICAL_THRESHOLD") || "10000"),
  bucket_hash_ttl_seconds: String.to_integer(System.get_env("PRISM_BUCKET_HASH_TTL_SECONDS") || "3600")

# Retry parameters
config :prism,
  server_error_base_delay_ms: String.to_integer(System.get_env("PRISM_SERVER_ERROR_BASE_DELAY_MS") || "2000"),
  server_error_max_retries: String.to_integer(System.get_env("PRISM_SERVER_ERROR_MAX_RETRIES") || "3"),
  network_error_base_delay_ms: String.to_integer(System.get_env("PRISM_NETWORK_ERROR_BASE_DELAY_MS") || "1000"),
  network_error_max_retries: String.to_integer(System.get_env("PRISM_NETWORK_ERROR_MAX_RETRIES") || "5"),
  message_not_found_max_retries: String.to_integer(System.get_env("PRISM_MESSAGE_NOT_FOUND_MAX_RETRIES") || "5"),
  rate_limit_defer_threshold_ms: String.to_integer(System.get_env("PRISM_RATE_LIMIT_DEFER_THRESHOLD_MS") || "10000"),
  checkpoint_ttl_seconds: String.to_integer(System.get_env("PRISM_CHECKPOINT_TTL_SECONDS") || "86400")

# Feature gates
config :prism,
  dead_message_cache_enabled: parse_bool.(System.get_env("PRISM_DEAD_MESSAGE_CACHE_ENABLED") || "true"),
  key_expansion_enabled: parse_bool.(System.get_env("PRISM_KEY_EXPANSION_ENABLED") || "true"),
  cancel_checker_enabled: parse_bool.(System.get_env("PRISM_CANCEL_CHECKER_ENABLED") || "true"),
  stream_trimmer_enabled: parse_bool.(System.get_env("PRISM_STREAM_TRIMMER_ENABLED") || "true")

# Dead message cache
config :prism,
  dead_message_cache_prefix: System.get_env("PRISM_DEAD_MESSAGE_CACHE_PREFIX") || "dead_msg:",
  dead_message_cache_ttl: String.to_integer(System.get_env("PRISM_DEAD_MESSAGE_CACHE_TTL") || "1800")

# Cancel checker
config :prism,
  cancel_prefix: System.get_env("PRISM_CANCEL_PREFIX") || "prism:cancel:"

# Stream trimmer
config :prism,
  callback_consumer_group: System.get_env("PRISM_CALLBACK_CONSUMER_GROUP") || "bot_team",
  stream_trim_interval_ms: String.to_integer(System.get_env("PRISM_STREAM_TRIM_INTERVAL_MS") || "30000")

# Broadway tuning
config :prism,
  slow_lane_threshold: String.to_integer(System.get_env("PRISM_SLOW_LANE_THRESHOLD") || "80"),
  fast_receive_interval: String.to_integer(System.get_env("PRISM_FAST_RECEIVE_INTERVAL") || "5"),
  slow_receive_interval: String.to_integer(System.get_env("PRISM_SLOW_RECEIVE_INTERVAL") || "5"),
  retry_receive_interval: String.to_integer(System.get_env("PRISM_RETRY_RECEIVE_INTERVAL") || "100"),
  queue_time_warn_ms: String.to_integer(System.get_env("PRISM_QUEUE_TIME_WARN_MS") || "2000"),
  task_timeout_ms: String.to_integer(System.get_env("PRISM_TASK_TIMEOUT_MS") || "60000")

# Cluster
config :prism,
  cluster_topology: System.get_env("PRISM_CLUSTER_TOPOLOGY") || "prism_cluster"
```

---

## Task 12: Update `test.exs` — Add Test Overrides

**File:** `config/test.exs`

Add test-specific config for new keys where tests depend on them:
- `dead_message_cache_enabled: true`, `cancel_checker_enabled: true`, `stream_trimmer_enabled: true`
- Keep current overrides (`finch_pool_count: 10`, etc.)

---

## Task 13: Update Tests

### 13a. Update imports
- All modules that reference split-out functions must be updated
- `test/support/stress_helpers.ex` may need updates for changed module references

### 13b. Tests should still pass
- No behavioral changes, only structural — tests should pass without modification
- Run `mix test` to verify

---

## Task 14: Update Documentation

### 14a. `.env.example`
Update with all new environment variables, organized by category with clear defaults and descriptions.

### 14b. `CONTRACT.md`
- Rewrite to be less InterChat-specific
- Replace "Python publisher" references with generic "publisher"
- Note that the key expansion map is the default; users can disable it
- Document all configurable stream key names
- Document the error types generically

### 14c. `AGENTS.md`
Update to reflect new file structure and module organization.

### 14d. `README.md`
- Update Docker image name references from "interchat-broadcast-worker" to generic
- Document new configuration options
- Update architecture diagram description

---

## Task 15: Remove Redundant Code

After all splits and extractions:
1. Ensure no dead code remains (orphaned private functions)
2. Verify no circular dependencies between new modules
3. Run `mix compile --warnings-as-errors` to catch unused imports/aliases

---

## Implementation Order (Dependency Chain)

```
1. Prism.Config        (no deps, foundation)
2. Prism.Helpers       (depends on Prism.Config for redix_pool_size)
3. discord_worker/http.ex    (depends on Prism.Config, Prism.Helpers)
4. discord_worker/retry.ex   (depends on Prism.DelayedQueue)
5. discord_worker/callbacks.ex
6. discord_worker/dead_message.ex (depends on Prism.Config)
7. discord_worker.ex         (depends on 3-6 modules)
8. fanout_broadway/key_expansion.ex (depends on Prism.Config)
9. fanout_broadway/sse.ex    (depends on Prism.Config, Prism.Helpers)
10. fanout_broadway/batch.ex (depends on DiscordWorker, SSE)
11. fanout_broadway.ex       (depends on key_expansion, batch)
12. delayed_queue.ex         (depends on Prism.Config)
13. delayed_scheduler.ex     (depends on Prism.Config)
14. retry_broadway.ex        (depends on Prism.Config, Prism.Helpers)
15. cancel_checker.ex        (depends on Prism.Config, Prism.Helpers)
16. stream_trimmer.ex        (depends on Prism.Config)
17. rate_limit/*.ex          (depends on Prism.Config)
18. application.ex           (depends on Prism.Config)
19. runtime.exs / test.exs   (config updates)
20. Documentation            (.env.example, CONTRACT.md, AGENTS.md, README.md)
21. Tests                    (verify, fix if needed)
```

---

## Risks & Mitigations

| Risk | Mitigation |
|------|-----------|
| Breaking existing deployments with new env var names | All new env vars have defaults matching current hardcoded values — no breaking change |
| Circular dependency between Config and Helpers | Keep Config pure (no logic, no deps). Helpers depends on Config only for redix_pool_size. |
| Test breakage from file splits | No behavioral change; only module references move. Update test imports as needed. |
| Large diff makes review hard | Implement in dependency order; each file change is self-contained. |

---

## Validation Plan

1. `mix compile --warnings-as-errors` — must compile cleanly
2. `mix test` — all existing tests must pass
3. `mix format --check-formatted` — all files must be formatted
4. Manual verification: start app with `iex -S mix`, verify no startup errors
5. Verify env var overrides work: set `PRISM_DEAD_MESSAGE_CACHE_ENABLED=false` and confirm dead message cache is skipped
6. Verify disable switches work: set `PRISM_KEY_EXPANSION_ENABLED=false`, `PRISM_CANCEL_CHECKER_ENABLED=false`, `PRISM_STREAM_TRIMMER_ENABLED=false`
