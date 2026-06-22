# Rate-Limit Edge Case & Concurrency Stress Tests

## Goal

Create a deterministic integration stress-test harness that mocks HTTP at the Finch boundary, injects every rate-limit edge case (shared, per-webhook, global, Cloudflare), and verifies `Supervisor.count_children(Prism.TaskSup).active` stays accurate under task crashes.

## Why Mock HTTP?

The `DiscordWorker.do_http_request_internal/7` calls `Finch.request(DiscordFinch, ...)`. The Finch pool is hardcoded to `"https://discord.com"` in `application.ex`. To inject controlled responses, we need a local HTTP server that returns configurable responses per webhook URL, and we point Finch at it during tests.

---

## Changes

### 1. Add test dependencies (`mix.exs`)

```elixir
{:bandit, "~> 1.5", only: :test},
{:plug, "~> 1.6", only: :test}
```

Bandit is a lightweight HTTP server. Plug provides the web framework for the mock server. Both are `:test`-only.

### 2. Create `config/test.exs` — test-only config

```elixir
import Config

config :prism,
  discord_base_url: "http://localhost:4002",
  finch_pool_count: 10,
  backpressure_enabled: true,
  max_async_batches: 10,
  batch_max_concurrency: 5,
  callback_include_parent_message_id: false,
  reply_index_enabled: false
```

`discord_base_url` is new — currently the Finch pool domain is hardcoded. We make it configurable.

### 3. Modify `lib/prism/application.ex` — read `discord_base_url` from config

Change the Finch pool from:
```elixir
{Finch, name: DiscordFinch,
  pools: %{"https://discord.com" => [...]}}
```
to:
```elixir
{Finch, name: DiscordFinch,
  pools: %{discord_base_url() => [...]}}

defp discord_base_url do
  Application.get_env(:prism, :discord_base_url, "https://discord.com")
end
```

The default is `"https://discord.com"` so prod is unaffected.

### 4. Add `discord_base_url` to runtime config default list

In `config/runtime.exs`, add the env var override (optional — system env can override):
```elixir
discord_base_url: System.get_env("PRISM_DISCORD_BASE_URL") || "https://discord.com"
```

### 5. Create `test/support/mock_discord_server.ex` — controllable mock HTTP server

A GenServer that starts a Bandit server on a random port and stores response templates keyed by webhook ID.

**Public API:**

```elixir
# Start on a free port, return {:ok, port}
MockDiscordServer.start_link()

# Register a response for a specific webhook ID
MockDiscordServer.stub(server, webhook_id, %{status: 200, headers: [...], body: "..."})

# Register a default response for any unmatched webhook
MockDiscordServer.stub_default(server, %{status: 200, ...})

# Shorthand factories for common responses
MockDiscordServer.stub_ok(server, webhook_id, msg_id \\ "mock_msg_123")
# Cloudflare HTML 429: status=429, headers=[retry-after, cf-ray, server:cloudflare], body=HTML
MockDiscordServer.stub_cloudflare_429(server, webhook_id, retry_after_sec \\ 5.0)
MockDiscordServer.stub_discord_global_429(server, webhook_id, retry_after_sec \\ 1.5)
MockDiscordServer.stub_discord_per_webhook_429(server, webhook_id, opts)
MockDiscordServer.stub_server_error(server, webhook_id, status \\ 500)
MockDiscordServer.stub_network_error(server, webhook_id)  # connection refused

# Inspect received requests
MockDiscordServer.requests(server)  # returns list of %{method, path, headers, body}
```

**Implementation:**

- A Plug router that matches on the URL path (`/api/webhooks/:webhook_id/:webhook_token`)
- Looks up the response from an ETS table or Agent state
- Logs each received request to an Agent list for later inspection
- The server starts on `{127, 0, 0, 1, 0}` (random free port), returns the actual port

### 6. Create `test/support/stress_helpers.ex` — shared test helpers

```elixir
defmodule Prism.StressHelpers do
  # Build a minimal valid batch payload map (like what would come from Redis stream)
  def build_payload(webhook_ids, action \\ "execute")

  # Build a target map for a given webhook_id
  def build_target(webhook_id, opts \\ [])

  # Spawn a task under Prism.TaskSup that sleeps then returns
  def spawn_sleep_task(sleep_ms \\ 100)

  # Count active children under Prism.TaskSup
  def active_count, do: Supervisor.count_children(Prism.TaskSup).active

  # Wait for active count to reach expected (with timeout)
  def wait_for_active_count(expected, timeout_ms \\ 1000)

  # Inject a Cloudflare block at the backpressure level
  def inject_cloudflare_block(retry_after_ms \\ 120_000)

  # Clear all rate-limit state (Redis keys, persistent_term, ETS)
  def reset_rate_limit_state()
end
```

### 7. Create `test/prism/stress_test.exs` — the stress test suite

#### Scenario A: Normal flow — 2xx response updates bucket

```
1. Start mock server, stub 2xx responses for webhook_A
2. Call DiscordWorker.process_target("execute", target_A, payload, "batch_1")
3. Assert: returns {:ok, "mock_msg_123"}
4. Assert: bucket.remaining decreased by 1
5. Assert: backpressure not triggered (unhealthy?() == false)
```

#### Scenario B: Cloudflare 429 triggers full backpressure

Cloudflare 429 responses are HTML, not JSON. The mock server must return:
- Status: 429
- Headers: `retry-after: 5`, `cf-ray: abc123`, `server: cloudflare`
- Body: `<html><head><title>429 Too Many Requests</title></head><body><center><h1>429 Too Many Requests</h1></center><hr><center>cloudflare</center></body></html>`

`Headers.parse_429/2` detects Cloudflare via: non-JSON body → `is_cloudflare: true`. Or, if the body happens to be JSON with `{"code": 0, ...}` → `is_cloudflare: true`.

```
1. Start mock server, stub Cloudflare HTML 429 for webhook_B
2. Call process_target → 429 response
3. Assert: Headers.parse_429 returns is_cloudflare == true
4. Assert: Backpressure.unhealthy?() returns true
5. Assert: Prism.RateLimit.unhealthy?() returns true
6. Verify subsequent process_target call is immediately deferred (no HTTP call made)
7. Assert: mock server received exactly 1 request (second was deferred)
8. Assert: InvalidRequestTracker.count_in_window() increased by 1 (Cloudflare blocks are always tracked)
```

#### Scenario C: Discord global 429 updates global bucket

```
1. Start mock server, stub Discord JSON 429 with global:true for webhook_C
2. Call process_target → 429 response
3. Assert: is_global == true, is_cloudflare == false
4. Assert: global bucket remaining == 0
5. Assert: another webhook's pre-flight check is blocked (global bucket exhausted)
```

#### Scenario D: Per-webhook 429 with shared scope does NOT increment tracker

```
1. Start mock server, stub Discord 429 with scope:"shared" for webhook_D
2. Call process_target → 429 response
3. Assert: InvalidRequestTracker.count_in_window() unchanged (shared = expected)
4. Assert: webhook_D's bucket has remaining == 0
```

#### Scenario E: Per-webhook 429 with user scope DOES increment tracker

```
1. Start mock server, stub Discord 429 with scope:"user" for webhook_E
2. Call process_target → 429 response
3. Assert: InvalidRequestTracker.count_in_window() increased by 1
```

#### Scenario F: Supervisor.count_children stays accurate on task exit

```
1. Assert: active_count() == 0
2. Spawn 5 sleep tasks
3. Assert: active_count() == 5
4. Kill 2 tasks via Process.exit(pid, :kill)
5. Wait for DOWN messages to propagate (~50ms)
6. Assert: active_count() == 3
7. Kill remaining 3 tasks
8. Assert: active_count() == 0
```

This is the core verification — the atomics counter would have stayed at 5 after kills. `Supervisor.count_children` must accurately drop.

#### Scenario G: Cap check does not permanently block after task crashes

```
1. Set max_async_batches to 3
2. Spawn 3 tasks → cap reached, active_count == 3
3. Kill all 3 tasks
4. Assert: active_count() == 0
5. Spawn a new task → must succeed (cap check should now pass)
```

#### Scenario H: Concurrent batches under backpressure all get re-enqueued

```
1. Inject Cloudflare backpressure
2. Push 5 batches to the fast stream
3. Verify all 5 are re-enqueued to delayed queue (not processed)
4. Verify no HTTP requests reached the mock server
5. Clear backpressure
6. Push another batch → verify it IS processed normally
```

#### Scenario I: Mixed rate limits across targets in a single batch

```
1. Stub: webhook_1 → 2xx, webhook_2 → per-webhook 429, webhook_3 → Cloudflare 429
2. Submit batch with targets [webhook_1, webhook_2, webhook_3]
3. Assert: webhook_1 succeeds, webhook_2 returns rate_limited, webhook_3 triggers backpressure
4. Assert: after batch, unhealthy?() == true (webhook_3's Cloudflare block)
5. Assert: InvalidRequestTracker incremented for webhook_3 only
```

---

### 8. Create `scripts/benchmark.exs` — standalone benchmark (optional, phase 2)

A `mix run` script that:
- Starts the full application (if Redis is available)
- Runs N concurrent batches with M targets each
- Toggles between different failure injection rates
- Measures: batch throughput/sec, task completion rate, counter drift (if any)
- Outputs CSV-like metrics to stdout

**Parameters** (env vars):
- `PRISM_BENCH_BATCHES` — number of batches (default 100)
- `PRISM_BENCH_TARGETS_PER_BATCH` — targets per batch (default 10)
- `PRISM_BENCH_FAILURE_RATE` — % of targets that get 429 (default 10)
- `PRISM_BENCH_CLOUDFLARE_RATE` — % of 429s that are Cloudflare (default 20)

---

## Verification

1. `mix test test/prism/stress_test.exs` — all scenarios pass
2. `mix test` — all 87 existing tests still pass, no regressions
3. `mix compile --warnings-as-errors` — clean

---

## Files to Create

| File | Purpose |
|---|---|
| `config/test.exs` | Test-only app config (discord_base_url, reduced pool sizes) |
| `test/support/mock_discord_server.ex` | Controllable mock HTTP server |
| `test/support/stress_helpers.ex` | Shared test helper functions |
| `test/prism/stress_test.exs` | Stress test scenarios A-I |
| `scripts/benchmark.exs` | Standalone benchmark script (phase 2) |

## Files to Modify

| File | Change |
|---|---|
| `mix.exs` | Add `:bandit` and `:plug` to deps (test only) |
| `lib/prism/application.ex` | Extract `discord_base_url()` from config for Finch pool domain |
| `config/runtime.exs` | Add `discord_base_url` env var override |

---

## Rollback

Revert commit. No data migration — all test-only code, no prod behavior change.
