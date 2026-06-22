# Unify Redis Keys & Deduplicate Logic

## Summary

Unify all 15+ Redis key patterns under a single `prism:` namespace with short, consistent segment names. Refactor 12 identified code duplications across the codebase. Migration is defaults-only (env var escape hatch for operators who need old keys).

## Redis Key Mapping

| # | Current Default | New Default | Config Key |
|---|---|---|---|
| 1 | `discord:fanout:stream:fast` | `prism:stream:fast` | `:redis_stream_fast` |
| 2 | `discord:fanout:stream:slow` | `prism:stream:slow` | `:redis_stream_slow` |
| 3 | `discord:fanout:stream:retries` | `prism:stream:retries` | `:redis_retry_stream` |
| 4 | `discord:fanout:callbacks` | `prism:stream:callbacks` | `:redis_callback_stream` |
| 5 | `discord:fanout:delayed` | `prism:delayed` | `:delayed_zset_key` |
| 6 | `prism:wakeup` | `prism:wakeup` (no change) | `:pubsub_channel` |
| 7 | `elixir_fanout_pool` | `prism:cg:fanout` | `:redis_group` |
| 8 | `elixir_fanout_pool_retries` | `prism:cg:retries` | *(appends `_retries` to `redis_group`)* |
| 9 | `bot_team` | `prism:cg:callbacks` | `:callback_consumer_group` |
| 10 | `rl:b:{worker_id}:{webhook}:{method}` | `prism:rl:{worker_id}:{webhook}:{method}` | *(built in `RateLimit.Bucket.key_prefix/0`)* |
| 11 | `checkpoint:{action}:{batch}:{webhook}` | `prism:ck:{action}:{batch}:{webhook}` | *(built in `Helpers.checkpoint_key/3`)* |
| 12 | `dead_msg:{webhook}:{msg_id}` / `dead_msg:{msg_id}` | `prism:dead:{webhook}:{msg_id}` / `prism:dead:{msg_id}` | `:dead_message_cache_prefix` |
| 13 | `prism:cancel:{msg_id}` | `prism:cancel:{msg_id}` (no change) | `:cancel_prefix` |
| 14 | `p:d:reply:{id}` / `p:d:copy:{id}` | `prism:reply:{id}` / `prism:copy:{id}` | `:reply_index_prefix` (now `prism`; `:reply:` and `:copy:` appended in code) |
| 15 | `dashboard:stream:hub:{id}` | `prism:sse:{id}` | `:redis_sse_topic_prefix` |

Consumer groups now live under `prism:cg:*` to distinguish them from stream keys. The retry consumer group is built as `"#{redis_group}_retries"` → becomes `prism:cg:retries` (caller appends `_retries` suffix).

## Task List

### Phase 1: Redis Key Unification (config changes)

- [ ] **1.1** Update `lib/prism/config.ex` — change all 15 default values to new `prism:` keys
- [ ] **1.2** Update `lib/prism/helpers.ex` — change `checkpoint_key/3` from `"checkpoint:#{action}:#{batch_id}:#{webhook_id}"` to `"prism:ck:#{action}:#{batch_id}:#{webhook_id}"`
- [ ] **1.3** Update `lib/prism/rate_limit/bucket.ex` — change `key_prefix/0` from `"rl:b:#{worker_id}"` to `"prism:rl:#{worker_id}"`
- [ ] **1.4** Update `config/runtime.exs` — change all env var defaults to new `prism:` keys
- [ ] **1.5** Update `.env.example` — document new defaults
- [ ] **1.6** Update `CONTRACT.md` — update all documented Redis key defaults (stream keys table, callback section, delayed queue section)
- [ ] **1.7** Update test files:
  - `test/prism/delayed_queue_test.exs` — change `@zset_key`, `@stream_key`, `@pubsub_channel`
  - `test/prism/delayed_scheduler_test.exs` — same
  - `test/prism/cancel_checker_test.exs` — change `"prism:cancel:#{message_id}"` to new prefix (no change needed if prefix stays `prism:cancel:`, but verify)
  - `test/prism/rate_limit_bucket_test.exs` — change `"rl:b:*"` glob to `"prism:rl:*"`
- [ ] **1.8** Verify no other files construct Redis keys with hardcoded strings (check `fanout_broadway/batch.ex` for reply index prefix usage — reads from config, no change needed in construction logic beyond config defaults)

### Phase 2: High-Severity Deduplication

- [ ] **2.1** Extract shared backpressure re-enqueue handler
  - **Files:** `fanout_broadway.ex` lines 48–81, `retry_broadway.ex` lines 48–71
  - **Action:** Create `Prism.FanoutBroadway.Backpressure` module (or add to existing module) with `re_enqueue_on_backpressure/1` that takes `data`, extracts payload, logs with a label param, and calls `DelayedQueue.enqueue/2`
  - Both `handle_message/3` callbacks call the shared function, differing only in the log prefix string

- [ ] **2.2** Extract shared timestamp extraction
  - **Files:** `fanout_broadway.ex` lines 84–96, `retry_broadway.ex` lines 74–88
  - **Action:** Add `extract_enqueued_at/1` to `Prism.Helpers` (takes stream message `id`, returns timestamp integer)
  - Replace both inline blocks with `Helpers.extract_enqueued_at(id)`

- [ ] **2.3** Extract shared rate-limit defer/sleep decision
  - **File:** `discord_worker.ex` lines 127–150 (`process_target`) and lines 450–461 (`process_retry`)
  - **Action:** Create private helper `defer_or_sleep(rate_limit_result, defer_threshold)` → `{should_defer, should_sleep, delay_ms}` in `DiscordWorker`
  - `process_target` passes either `preflight.rate_limit` or `RateLimit.check(...)` result; helper handles the decomposing

- [ ] **2.4** Extract batch re-enqueue payload builder
  - **File:** `fanout_broadway/batch.ex` lines 481–492 and lines 504–515
  - **Action:** Create private `build_re_enqueue_payload(action, batch_id, discord_payload, targets, parent_message_id, metadata, hub_id, shard_index)` → `%{...}` map
  - Both crash rescue and start_child error blocks call this, differing only in the delay value passed to `DelayedQueue.enqueue/2`

- [ ] **2.5** Compute `parent_msg_id` once in `process_target`
  - **File:** `discord_worker.ex` lines 101, 157, 246 (and `batch.ex` line 95)
  - **Action:** Compute `parent_msg_id = if action == "execute", do: parent_message_id, else: nil` once at the top of `process_target` (or at each usage site that currently re-computes — consolidate to one binding per scope)
  - For `batch.ex` line 95: extract to a local binding before the `spawn_retry` call site

### Phase 3: Medium-Severity Deduplication

- [ ] **3.1** Fix `StreamTrimmer` to use `Helpers.redix_command/1`
  - **File:** `stream_trimmer.ex` lines 65–66
  - **Action:** Replace `:erlang.phash2(...)` + `Redix.command(redix, ...)` with `Helpers.redix_command([...])` in `trim_stream/3` and `get_safe_trim_id/2`
  - Note: Lua scripts are not used here, so `Helpers.redix_command/1` is sufficient (it handles pool selection internally)
  - This eliminates the duplicated pool hashing logic

- [ ] **3.2** Extract dead message cache check
  - **File:** `discord_worker.ex` lines 41–50 (`process_target`) and lines 428–448 (`process_retry`)
  - **Action:** The `dead_cache_hit` computation (`action in ["edit", "delete"] and is_binary(message_id) and DeadMessage.dead_message_cached?(webhook_id, message_id)`) is identical
  - Create private `dead_cache_hit?(action, webhook_id, message_id)` in `DiscordWorker`
  - The response handling differs (direct return vs callback publish), so keep those separate

- [ ] **3.3** Extract callback stream XADD into shared helper
  - **Files:** `fanout_broadway/batch.ex` lines 389–400, `discord_worker/callbacks.ex` lines 75–86
  - **Action:** Add `publish_callback/1` to `Prism.Helpers` (or `Prism.FanoutBroadway.Callbacks`) that takes a JSON-encoded payload string and XADDs it to `Config.stream_callbacks()` with `MAXLEN ~ 100000`
  - Move the `100000` constant to `Prism.Config` (e.g., `callback_stream_maxlen/0`)

- [ ] **3.4** Extract overrides merge logic
  - **Files:** `discord_worker.ex` lines 52–58, `fanout_broadway/batch.ex` lines 97–104
  - **Action:** Add `merge_overrides(content_or_payload, target)` to `Prism.Helpers`
  - Returns merged map; callers JSON-encode or use directly as needed

### Phase 4: Low-Severity Deduplication

- [ ] **4.1** Centralize error-to-string mappings
  - **Files:** `discord_worker/callbacks.ex` lines 38–47 (6 entries, `cond`), `fanout_broadway/batch.ex` lines 306–343 (11 entries, `case` with 3-tuples)
  - **Action:** Create `Prism.ErrorMapping` module with `to_error_info/1` → `{error_string, error_type, extra_map}`
  - `batch.ex` and `callbacks.ex` both call this function; the `extra` map defaults to `%{}` for `callbacks.ex`

- [ ] **4.2** Deduplicate checkpoint SETEX write pattern
  - **File:** `discord_worker.ex` lines 221–244 (`process_target`) and lines 642–653 (`process_retry`)
  - **Action:** Create private `write_checkpoint(action, batch_id, webhook_id, result, skip?)` in `DiscordWorker`
  - Encapsulates the `checkpoint_key`, `checkpoint_ttl`, and `SETEX` command with result branching

- [ ] **4.3** Extract duplicate emptiness check in `empty_discord_payload?/1`
  - **File:** `helpers.ex` lines 61–67 and 73–79
  - **Action:** Extract `empty_payload_fields?(map)` private helper that checks `content`, `embeds`, `components`
  - Both map and binary branches call it after extracting fields

- [ ] **4.4** Extract shared `key_exists?/1` helper
  - **Files:** `dead_message.ex` lines 23, 28 and `cancel_checker.ex` line 25
  - **Action:** Add `key_exists?(redis_key)` to `Prism.Helpers` that wraps `Helpers.redix_command(["EXISTS", key])` and returns boolean with error logging
  - `DeadMessage` and `CancelChecker` both call it instead of inlining

### Phase 5: Verify

- [ ] **5.1** Run `mix compile --warnings-as-errors` to verify no compile errors
- [ ] **5.2** Run `mix test` to verify all tests pass with new defaults
- [ ] **5.3** Run `mix format --check-formatted` to verify formatting
- [ ] **5.4** Grep codebase for any remaining old key strings (`"discord:fanout`, `"rl:b:`, `"dead_msg:`, `"checkpoint:`, `"p:d`, `"dashboard:stream`, `"elixir_fanout_pool`, `"bot_team`, `"prism:wakeup`: check it's only the config default location)

## Notes

- **No backward-compat code.** Operators who need old keys can set the corresponding environment variables in `runtime.exs`.
- **No migration script.** The env var escape hatch is the migration path.
- **All keys are overridable.** Every key has a config getter in `Prism.Config` that reads from `Application.get_env/3` with a default. The defaults are what change.
- **Finding #13 skipped.** Two modules reading `slow_lane_threshold()` from config is normal, not duplication.
