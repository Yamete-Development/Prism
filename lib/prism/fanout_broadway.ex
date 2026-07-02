defmodule Prism.FanoutBroadway do
  use Broadway

  require Logger
  require OpenTelemetry.Tracer

  alias Broadway.Message
  alias Prism.Helpers

  def start_link(opts) do
    lane = Keyword.fetch!(opts, :lane)
    name = Keyword.get(opts, :name, __MODULE__)

    stream_key = Prism.Config.stream_jobs()
    receive_interval = Prism.Config.jobs_receive_interval()
    consumer_group = Prism.Config.consumer_group()
    broadway_concurrency = Prism.Config.broadway_concurrency()

    transport_backend = Prism.EventBus.Config.transport_backend()

    producer =
      if transport_backend == Prism.EventBus.Transport.Kafka do
        [
          module: {
            BroadwayKafka.Producer,
            [
              hosts: Prism.EventBus.Config.kafka_brokers(),
              group_id: consumer_group,
              topics: [stream_key],
              receive_interval: receive_interval,
              client_config: [
                connect_timeout: 10_000,
                request_timeout: 30_000
              ],
              group_config: [
                session_timeout_seconds: 60,
                heartbeat_rate_seconds: 10,
                rebalance_timeout_seconds: 300
              ],
              fetch_config: [max_wait_time: 100],
              offset_reset_policy: :earliest
            ]
          },
          concurrency: 5
        ]
      else
        [
          module: {
            OffBroadwayRedisStream.Producer,
            [
              client: Prism.RedisClient,
              redis_client_opts: Prism.Config.redis_opts(),
              stream: stream_key,
              group: consumer_group,
              consumer_name:
                "broadcast_worker_#{lane}_" <> Integer.to_string(:os.system_time(:microsecond)),
              make_stream: true,
              receive_interval: receive_interval
            ]
          }
        ]
      end

    Broadway.start_link(__MODULE__,
      name: name,
      producer: producer,
      processors: [
        default: [
          concurrency: broadway_concurrency,
          max_demand: Prism.Config.broadway_max_demand(),
          min_demand: Prism.Config.broadway_min_demand()
        ]
      ]
    )
  end

  defp extract_payload_and_time(data) do
    case data do
      [id, fields] ->
        enqueued_at = Helpers.extract_enqueued_at(id)
        payload_json = Helpers.get_payload_from_redis_data(fields)
        {payload_json, enqueued_at}

      binary when is_binary(binary) ->
        enqueued_at = :os.system_time(:millisecond)
        {binary, enqueued_at}

      _ ->
        {"", :os.system_time(:millisecond)}
    end
  end

  @impl true
  def handle_message(_, %Message{data: data} = message, _) do
    if Prism.Config.backpressure_enabled?() and Prism.RateLimit.unhealthy?() do
      delay_ms = Prism.RateLimit.backoff_ms()
      {payload_binary, _} = extract_payload_and_time(data)

      try do
        # Wrap the protobuf binary in a map so DelayedQueue can encode it as JSON
        payload_map = %{
          "type" => "protobuf_batch",
          "bytes" => Base.encode64(payload_binary)
        }

        Helpers.re_enqueue_on_backpressure(payload_map, "", delay_ms)
      rescue
        _ -> :ok
      end

      message
    else
      polled_at = :os.system_time(:millisecond)
      {payload_binary, enqueued_at} = extract_payload_and_time(data)

      try do
        payload =
          case payload_binary do
            <<0, schema_id::32-integer, confluent_data::binary>> ->
              case Prism.SchemaRegistry.get_schema(schema_id) do
                {:ok, _} ->
                  protobuf_data = Prism.Helpers.strip_confluent_message_indexes(confluent_data)
                  Prism.PrismStreamPayload.decode!(protobuf_data)

                {:error, _} ->
                  raise "Unknown Schema ID: #{schema_id}"
              end

            _ ->
              Prism.PrismStreamPayload.decode!(payload_binary)
          end

        batch_id = payload.batch_id

        targets =
          Enum.map(payload.targets, fn t ->
            # Parse overrides from JSON string
            overrides =
              if is_nil(t.overrides) or t.overrides == "" do
                nil
              else
                Jason.decode!(t.overrides)
              end

            %{
              "channel_id" => t.channel_id,
              "webhook_id" => t.webhook_id,
              "webhook_token" => t.webhook_token,
              "guild_id" => t.guild_id,
              "hub_id" => t.hub_id,
              "thread_id" =>
                if(is_nil(t.thread_id) or t.thread_id == "", do: nil, else: t.thread_id),
              "message_id" =>
                if(is_nil(t.message_id) or t.message_id == "", do: nil, else: t.message_id),
              "overrides" => overrides
            }
          end)

        action = if payload.action == "", do: "execute", else: payload.action

        # Payload is now a JSON string
        discord_payload = if is_nil(payload.payload) or payload.payload == "", do: %{}, else: Jason.decode!(payload.payload)

        parent_message_id = payload.message_id

        metadata =
          if payload.metadata do
            %{
              "author_id" => payload.metadata.author_id,
              "guild_id" => payload.metadata.guild_id,
              "guild_name" => payload.metadata.guild_name,
              "badges" => payload.metadata.badges
            }
          else
            %{}
          end

        hub_id = payload.hub_id

        if parent_message_id != nil and parent_message_id != "" and
             Prism.CancelChecker.cancelled?(parent_message_id) do
          Logger.info(
            "FanoutBroadway: skipping cancelled batch batch_id=#{batch_id} message_id=#{parent_message_id}"
          )

          message
        else
          shard_index = payload.shard_index || 0

          OpenTelemetry.Tracer.set_attributes([
            {:batch_id, batch_id},
            {:action, action},
            {:target_count, length(targets)},
            {:shard_index, shard_index}
          ])

          if action == "execute" and Helpers.empty_discord_payload?(discord_payload) do
            Logger.warning(
              "FanoutBroadway: skipping empty execute batch batch_id=#{batch_id} — no content, embeds, or components"
            )
          else
            max_async = Prism.Config.max_async_batches()
            current = Prism.AsyncBatchCounter.count()

            if current < max_async do
              Prism.FanoutBroadway.Batch.spawn_async_batch(
                action,
                batch_id,
                discord_payload,
                targets,
                polled_at,
                enqueued_at,
                parent_message_id,
                metadata,
                hub_id,
                shard_index
              )
            else
              Logger.warning(
                "Async batch cap reached (#{current}/#{max_async}). " <>
                  "Re-enqueueing batch #{batch_id} to delayed queue (200ms)."
              )

              payload_map = %{
                "type" => "protobuf_batch",
                "bytes" => Base.encode64(payload_binary)
              }

              Prism.DelayedQueue.enqueue(payload_map, 200)
            end
          end

          message
        end
      rescue
        e ->
          Logger.error("Failed to parse protobuf payload: #{inspect(e)}")
          Message.failed(message, "invalid payload")
      end
    end
  end
end
