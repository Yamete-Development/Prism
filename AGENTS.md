# Elixir Prism Service — Agent Context (`Prism/`)

This document is the specialized guide for agents working on the Elixir-based webhook broadcast service `Prism`. It covers Stream lane consumers, key expansion functions, rate limit tracking, and the atomic delayed queue system.

---

## Service Architecture

Prism is an OTP application built in Elixir that consumes payload batches from Redis Streams and dispatches them concurrently via Finch HTTP clients to Discord's webhooks.

```
Prism/
├── lib/
│   ├── prism/
│   │   ├── rate_limit/            # Local rate limit buckets & Cloudflare backpressure
│   │   │   ├── backpressure.ex    # Tracks IP-level Cloudflare blocks
│   │   │   ├── bucket.ex          # Local token bucket cache
│   │   │   └── headers.ex         # Parses Discord/Cloudflare HTTP headers
│   │   ├── application.ex         # Supervision tree configuration
│   │   ├── delayed_queue.ex       # Atomic Redis ZSET enqueue/pop handlers
│   │   ├── delayed_scheduler.ex   # Event-driven queue scheduler
│   │   ├── discord_worker.ex      # Discord HTTP payload dispatcher
│   │   ├── fanout_broadway.ex     # Fast/Slow stream consumers
│   │   ├── redis_client.ex        # Redix command helpers
│   │   └── retry_broadway.ex      # Retry stream consumer
│   └── prism.ex
├── config/                        # Elixir compile-time configurations
├── mix.exs                        # Package manifest & deps (Broadway, Finch, Redix)
└── CONTRACT.md                    # Data structures & serialization rules
```

---

## Consumer Lanes (`FanoutBroadway`)

Prism runs Broadway pipelines to consume batches from Redis Streams concurrently:

1. **Fast Lane (`discord:fanout:stream:fast`):** Processed by workers configured for lower batches.
2. **Slow Lane (`discord:fanout:stream:slow`):** Reserved for larger message fanouts (batches containing $> 80$ target channels).
3. **Retry Lane (`discord:fanout:stream:retries`):** Dedicated stream fed by the delayed scheduler for failed webhooks.

---

## Key Expansion Function (`expand_keys/1`)

To optimize memory usage, the Python bot minifies JSON object keys before pushing batches to Redis. Prism automatically expands these keys to their full names using compile-time `@key_map` configurations:

```elixir
# Detects format: if the first key is inside @key_map, recursively maps keys
def expand_keys(map) when is_map(map) do ... end
```
- **Backward Compatibility:** If the incoming payload is already in long-key format (e.g., the key `"action"` is present and not a known minified short key), it passes through unchanged without key modification.

---

## Atomic Delayed Queue System

Failed webhook targets (rate-limited, server errors, network dropouts) are enqueued in Redis for delayed execution using a **ZSET** at key `discord:fanout:delayed`.

### Enqueueing (`Prism.DelayedQueue.enqueue/2`)
- Adds a unique `retry_id` to the payload to prevent duplicates in the ZSET.
- Scores the payload by epoch timestamp `execute_at_ms`.
- Uses a Lua script to add the item. If the item has the *lowest score* (soonest tick), the script publishes a wakeup command to the Redis PubSub channel `"prism:wakeup"`.

### Event-Driven Scheduling (`Prism.DelayedScheduler`)
- A zero-polling GenServer subscribes to `"prism:wakeup"`.
- It executes `migrate_due_items/1` using an atomic Lua script:
  1. Queries all items whose execution timestamp is $\le$ `now`.
  2. Removes them from the ZSET and adds them to `discord:fanout:stream:retries`.
  3. Returns the score of the *new* earliest item in the ZSET.
- The scheduler calculates the delay to the next earliest item and registers a `Process.send_after/3` timer to sleep until that tick.
- If a `"new_earliest"` PubSub wakeup event is received, the current timer is immediately cancelled, and the process ticks.

---

## Rate Limiting & Backpressure

### Local Rate-Limit Buckets (`Prism.RateLimit.Bucket`)
- Keeps local counters of remaining requests per webhook ID.
- Pre-flight checks (`check/2`) block or defer executions *before* making network calls.
- Updates bucket state on HTTP responses (2xx extracts `x-ratelimit-*` headers; 429 locks the bucket).

### Cloudflare IP-Level Blocks (`Prism.RateLimit.Backpressure`)
- If a 429 response is identified as coming from Cloudflare (IP-level blocking):
  1. Backpressure is triggered via `record_cloudflare_block/1`.
  2. The backoff target is saved to `:persistent_term` to survive GenServer crashes.
  3. Outbound requests are blocked (`unhealthy?() == true`).
  4. Once a worker records a successful 2xx request, `record_success/0` clears the backpressure blocks.
