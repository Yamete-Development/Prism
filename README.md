# Prism - A High-Performance Discord Webhook Router

Prism is a high-performance, asynchronous worker pool written in Elixir. It acts as a "dumb, fast pipe" for routing webhook requests to Discord at scale. 

Instead of managing HTTP request loops, retries, and rate limits within your main application (like a Python bot), you enqueue standard JSON payloads to a Redis Stream. Prism pulls these batches, fans them out concurrently, handles all Discord HTTP `429 Too Many Requests` backpressure automatically, and publishes a summary callback via Redis when the batch is finished.

Prism is entirely payload-agnostic. It does not enforce any specific Discord formatting rules, making it a perfectly general-purpose tool for any Discord bot or application that needs to broadcast messages to hundreds or thousands of webhooks quickly.

## Architecture

This worker uses **Broadway** to process Redis Stream messages concurrently, with **Finch** providing high-performance HTTP pooling.

### Key Features
- **Unbounded Fan-out:** Group requests into batches; Prism concurrently dispatches up to 80 targets per batch.
- **Smart Retries & Backpressure:** Automatically intercepts Discord's `429` rate limit responses, reads the `retry_after` header, and spawns background tasks to complete the request without blocking the main pipeline. 
- **Target Overrides:** Efficiently send the exact same base payload to 99% of targets, while providing a target-specific `overrides` dictionary (e.g. for custom mentions) that Prism will merge on the fly.
- **Callback System:** Emits real-time event summaries (successes vs. permanent/transient failures) back to a Redis Stream so your main application can delete dead webhooks from its database.

## Integration Contract

Prism communicates purely over Redis Streams using JSON payloads.
For the complete schema detailing how to enqueue tasks and consume callbacks, see [CONTRACT.md](CONTRACT.md).

## Environment Variables

Configuration is handled dynamically via environment variables. See the provided `.env.example` file for defaults.

| Variable | Description | Default |
| --- | --- | --- |
| `REDIS_HOST` | The hostname of the Redis instance. | `localhost` |
| `REDIS_PORT` | The port of the Redis instance. | `6379` |
| `REDIS_PASSWORD` | The password for the Redis instance, if required. | _none_ |
| `REDIS_STREAM_FAST` | The name of the fast lane Redis stream to consume from. | `discord:fanout:stream:fast` |
| `REDIS_STREAM_SLOW` | The name of the slow lane Redis stream to consume from. | `discord:fanout:stream:slow` |
| `REDIS_CALLBACK_STREAM` | The name of the Redis stream to publish callbacks to. | `discord:fanout:callbacks` |
| `REDIS_GROUP` | The Redis stream consumer group name. | `elixir_fanout_pool` |
| `MAX_BATCHES_PER_SEC` | Rate limit control. 1 batch = up to 80 targets. | `1` |

## Getting Started

1. Ensure you have [Elixir installed](https://elixir-lang.org/install.html). (This project recommends using `mise` or `asdf`).
2. Clone the repository.
3. Install dependencies:
   ```bash
   mix deps.get
   ```
4. Copy the `.env.example` file to set your own environment variables:
   ```bash
   cp .env.example .env
   ```
5. Source your environment variables (e.g. `source .env`) and start the application:
   ```bash
   mix run --no-halt
   ```
