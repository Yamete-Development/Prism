defmodule Prism.EventBus do
  @moduledoc """
  Public API for the inter-service event bus.

  Provides `publish` and `subscribe` primitives backed by Redis Streams,
  with CloudEvents v1.0 envelopes, OTel trace propagation, and dead-letter
  queue support.

  ## Usage

      # Publish an event
      EventBus.publish("events:bus", type: "fun.interchat.broadcast.completed", data: %{...})

      # Subscribe to events
      {:ok, pid} = EventBus.subscribe("events:bus", "my-consumer", &MyHandler.handle/2)

  ## Adapter API Contract

  This module implements the same conceptual contract across Elixir, Python,
  and Go. The transport backend (Redis Streams) is pluggable — swapping to
  Kafka only requires a new adapter implementation behind the same API.
  """

  alias Prism.EventBus.{Config, Consumer, Publisher}

  @doc false
  @deprecated "Use Transport backends instead. This function will be removed in a future version."
  @spec redis_command([term()]) :: {:ok, term()} | {:error, term()}
  def redis_command(command) do
    Prism.Helpers.redix_command(command)
  end

  @doc """
  Publishes an event to the given stream.

  Builds a CloudEvents v1.0 envelope around the provided data and XADD's
  it to the stream.

  ## Options
    - `:type` (required) — CloudEvent type, e.g. `"fun.interchat.broadcast.completed"`
    - `:data` (required) — event payload map
    - `:source` — CloudEvent source identifier (default from config: `"/prism"`)
    - `:maxlen` — approximate stream length cap (default from config: `100_000`)

  ## Returns
    - `:ok` on success
    - `{:error, reason}` on Redis failure

  ## Examples

      EventBus.publish("events:bus",
        type: "fun.interchat.broadcast.completed",
        data: %{batch_id: "b_123", ok_count: 5, fail_count: 0}
      )

  """
  @spec publish(binary(), keyword()) :: :ok | {:error, term()}
  def publish(stream, opts) do
    type = Keyword.fetch!(opts, :type)
    data = Keyword.fetch!(opts, :data)
    source = Keyword.get(opts, :source, Config.event_source())
    maxlen = Keyword.get(opts, :maxlen, Config.events_stream_maxlen())

    Publisher.publish(stream, data, type: type, source: source, maxlen: maxlen)
  end

  @doc """
  Publishes a pre-built CloudEvent map directly to the stream.

  Useful for forwarding events or when the envelope is constructed externally.

  ## Options
    - `:maxlen` — approximate stream length cap

  ## Returns
    - `:ok` on success
    - `{:error, reason}` on Redis failure
  """
  @spec publish_cloud_event(binary(), map(), keyword()) :: :ok | {:error, term()}
  def publish_cloud_event(stream, cloud_event, opts \\ []) do
    Publisher.publish_cloud_event(stream, cloud_event, opts)
  end

  @doc """
  Starts a consumer GenServer that reads events from the given stream.

  The consumer uses a Redis consumer group for at-least-once delivery and
  includes automatic stale message recovery via XAUTOCLAIM.

  The handler function receives `(cloud_event, handler_opts)` and should
  return `:ok` or `{:error, reason}`.

  ## Options
    - `:stream` (required) — Redis stream key
    - `:consumer_group` (required) — unique consumer group name
    - `:handler` (required) — `fn cloud_event, handler_opts -> :ok | {:error, reason} end`
    - `:handler_opts` — arbitrary data passed to handler (default: `nil`)
    - `:max_retries` — max delivery attempts before DLQ (default: `3`)
    - `:retry_backoff_base_ms` — base backoff in ms (default: `1000`)
    - `:retry_backoff_max_ms` — max backoff cap in ms (default: `30000`)
    - `:consumer_batch_size` — messages per XREADGROUP batch (default: `10`)
    - `:consumer_block_ms` — XREADGROUP block timeout in ms (default: `3000`)
    - `:stale_claim_idle_ms` — XAUTOCLAIM idle threshold in ms (default: `30000`)
    - `:stale_claim_interval_ms` — XAUTOCLAIM interval in ms (default: `60000`)
    - `:name` — optional GenServer registered name

  ## Returns
    - `{:ok, pid}` on success
    - `{:error, reason}` on failure

  ## Examples

      EventBus.subscribe(
        stream: "events:bus",
        consumer_group: "beacon-hub-fanout",
        handler: &HubFanout.handle_event/2,
        handler_opts: %{pubsub: MyApp.PubSub}
      )

  """
  @spec subscribe(keyword()) :: {:ok, pid()} | {:error, term()}
  def subscribe(opts) do
    Consumer.start_link(opts)
  end
end
