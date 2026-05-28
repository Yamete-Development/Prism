# InterchatBroadcastWorker

A high-performance, asynchronous worker pool written in Elixir. This service acts as a fan-out processor for webhook requests, pulling batched messages from a Redis Stream and dispatching them concurrently to Discord APIs while handling Discord's rate limits and connection backpressure automatically.

## Architecture

This worker uses **Broadway** to process Redis Stream messages concurrently, with **Finch** providing high-performance HTTP pooling. Once a batch is fully processed, it publishes a callback event via Redis Pub/Sub back to the main bot service.

Key Features:
- Controlled concurrent request fan-out (prevents task storming)
- Automatic handling and sleeping for Discord's HTTP `429 Too Many Requests`
- Automatic Redis Pub/Sub callback notifications when a batch finishes

## Environment Variables

Configuration is handled dynamically via environment variables. See the provided `.env.example` file for defaults.

| Variable | Description | Default |
| --- | --- | --- |
| `REDIS_HOST` | The hostname of the Redis instance. | `localhost` |
| `REDIS_PORT` | The port of the Redis instance. | `6379` |
| `REDIS_PASSWORD` | The password for the Redis instance, if required. | _none_ |
| `REDIS_STREAM` | The name of the Redis stream to consume from. | `discord:fanout:stream` |
| `REDIS_GROUP` | The Redis stream consumer group name. | `elixir_fanout_pool` |

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
