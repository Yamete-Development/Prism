# Prism — A High-Performance Discord Webhook Router

Prism is a high-performance, asynchronous worker pool written in Elixir. It acts as a fast pipe for routing webhook requests to Discord at scale.

Instead of managing HTTP request loops, retries, and rate limits within your main application, you enqueue standard JSON payloads (CloudEvents) to an EventBus (Redis Streams or Kafka). Prism pulls these batches, fans them out concurrently, handles all Discord HTTP `429 Too Many Requests` backpressure automatically, and publishes a summary callback via EventBus when the batch is finished.

Prism is entirely payload-agnostic. It does not enforce any specific Discord formatting rules, making it a general-purpose tool for any Discord bot or application that needs to broadcast messages to hundreds or thousands of webhooks quickly.

## Architecture

This worker uses **Broadway** to process Redis Stream messages concurrently, with **Finch** providing high-performance HTTP pooling.

### Key Features
- **Unbounded Fan-out:** Group requests into batches; Prism concurrently dispatches up to 80 targets per batch (configurable).
- **Smart Retries & Backpressure:** Automatically intercepts Discord's `429` rate limit responses, reads the `retry_after` header, and spawns background tasks to complete the request without blocking the main pipeline.
- **Target Overrides:** Efficiently send the exact same base payload to most targets, while providing a target-specific `overrides` dictionary that Prism will merge on the fly.
- **Callback System:** Emits real-time event summaries (successes vs. permanent/transient failures) back to a Redis Stream so your main application can delete dead webhooks from its database.
- **Configurable Everything:** All thresholds, delays, stream keys, pool sizes, and feature gates are configurable via environment variables. See `.env.example` for the full list.
- **Feature Gates:** Dead message cache, key expansion, cancel checker, and stream trimmer can all be disabled independently.

## Integration Contract

Prism communicates purely over EventBus using JSON payloads wrapped in CloudEvents. For the complete schema detailing how to enqueue tasks and consume callbacks, see [CONTRACT.md](CONTRACT.md).

## Configuration

All runtime configuration is done via environment variables with sensible defaults. Copy `.env.example` to `.env` and adjust as needed:

```bash
cp .env.example .env
```

### Essential Variables

| Variable | Description | Default |
|---|---|---|
| `REDIS_HOST` | Redis hostname | `localhost` |
| `REDIS_PORT` | Redis port | `6379` |
| `PRISM_STREAM_JOBS` | Jobs lane stream topic/key | `prism.stream.jobs` |
| `EVENT_BUS_TRANSPORT` | Transport backend | `redis` or `kafka` |
| `KAFKA_BROKERS` | Comma-separated Kafka brokers | `localhost:9092` |
| `PRISM_CONSUMER_GROUP` | Consumer group name | `prism:cg:fanout` |

### Performance Tuning

| Variable | Description | Default |
|---|---|---|
| `PRISM_REDIX_POOL_SIZE` | Redix connection pool size | `5` |
| `PRISM_FINCH_POOL_COUNT` | Finch HTTP connection pool size | `50` |
| `PRISM_BROADWAY_CONCURRENCY` | Max concurrent batches per fanout lane | `50` |
| `PRISM_BATCH_MAX_CONCURRENCY` | Max concurrent HTTP requests per batch | `80` |
| `PRISM_RETRY_BROADWAY_CONCURRENCY` | Max concurrent batches for retry lane | `10` |
| `PRISM_MAX_ASYNC_BATCHES` | Max in-flight async batches before re-enqueue | `300` |

See `.env.example` for the complete list of all configurable variables including retry parameters, rate limit thresholds, feature gates, timeouts, and cluster settings.

## Getting Started

1. Ensure you have [Elixir installed](https://elixir-lang.org/install.html).
2. Clone the repository.
3. Install dependencies:
   ```bash
   mix deps.get
   ```
4. Copy the `.env.example` file and set your environment variables:
   ```bash
   cp .env.example .env
   ```
5. Source your environment and start the application:
   ```bash
   source .env && mix run --no-halt
   ```

## Docker

```bash
# Build
docker build -t prism .

# Run
docker run -d \
  -e REDIS_HOST=host.docker.internal \
  -e REDIS_PORT=6379 \
  prism
```

## Project Structure

```
lib/
├── prism.ex                                  # Application entrypoint
├── prism/
│   ├── application.ex                        # Supervision tree
│   ├── config.ex                             # Centralized configuration module
│   ├── helpers.ex                            # Shared utility functions
│   ├── discord_worker.ex                     # Core orchestration (process_target, process_retry)
│   ├── discord_worker/
│   │   ├── http.ex                           # HTTP request building & execution
│   │   ├── retry.ex                          # Retry spawning & delayed queue enqueue
│   │   ├── callbacks.ex                      # Partial callback publishing
│   │   └── dead_message.ex                   # Dead message cache
│   ├── fanout_broadway.ex                    # Broadway pipeline (Jobs lane)
│   ├── fanout_broadway/
│   │   ├── key_expansion.ex                  # Short→long JSON key mapping
│   │   ├── batch.ex                          # Batch fan-out & result aggregation

│   ├── rate_limit.ex                         # Public facade for rate-limit operations
│   ├── rate_limit/
│   │   ├── backpressure.ex                   # Cloudflare IP-level block tracking
│   │   ├── bucket.ex                         # Redis-backed token bucket
│   │   ├── headers.ex                        # HTTP header parsing
│   │   └── invalid_request_tracker.ex        # Sliding-window invalid request counter
│   ├── delayed_queue.ex                      # Atomic Redis ZSET enqueue/pop
│   ├── delayed_scheduler.ex                  # Event-driven queue scheduler
│   ├── retry_broadway.ex                     # Retry stream consumer
│   ├── cancel_checker.ex                     # Source message cancel detection
│   ├── stream_trimmer.ex                     # Periodic stream XTRIM
│   ├── redis_client.ex                       # OffBroadwayRedisStream adapter
│   ├── metrics_api.ex                        # Telemetry/metrics HTTP API
│   └── metrics_logger.ex                     # Periodic server metrics logging
config/
├── runtime.exs                               # Env var → config mapping
└── test.exs                                  # Test-specific config overrides
test/                                         # Unit and integration tests
```

## License

This project is licensed under the GNU Affero General Public License v3.0 (AGPL-3.0). See the [LICENSE](LICENSE) file for details.
