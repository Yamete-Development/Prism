defmodule Prism.FanoutBroadway do
  use Broadway

  require Logger
  require OpenTelemetry.Tracer

  alias Broadway.Message
  alias Prism.Helpers

  def start_link(opts) do
    lane = Keyword.fetch!(opts, :lane)
    name = Keyword.get(opts, :name, __MODULE__)

    redis_opts = Prism.Config.redis_opts()

    stream_key =
      if lane == :fast do
        Prism.Config.stream_fast()
      else
        Prism.Config.stream_slow()
      end

    receive_interval =
      if lane == :fast do
        Prism.Config.fast_receive_interval()
      else
        Prism.Config.slow_receive_interval()
      end

    redis_group = Prism.Config.redis_group()

    broadway_concurrency = Prism.Config.broadway_concurrency()

    Broadway.start_link(__MODULE__,
      name: name,
      producer: [
        module: {
          OffBroadwayRedisStream.Producer,
          [
            client: Prism.RedisClient,
            redis_client_opts: redis_opts,
            stream: stream_key,
            group: redis_group,
            consumer_name:
              "broadcast_worker_#{lane}_" <> Integer.to_string(:os.system_time(:microsecond)),
            make_stream: true,
            receive_interval: receive_interval
          ]
        }
      ],
      processors: [
        default: [
          concurrency: broadway_concurrency,
          max_demand: 10,
          min_demand: 0
        ]
      ]
    )
  end

  @impl true
  def handle_message(_, %Message{data: data} = message, _) do
    if Prism.Config.backpressure_enabled?() and Prism.RateLimit.unhealthy?() do
      delay_ms = Prism.RateLimit.backoff_ms()
      [_id, fields] = data
      payload_json = Helpers.get_payload_from_redis_data(fields)

      with {:ok, raw} <- Jason.decode(payload_json) do
        payload = Prism.FanoutBroadway.KeyExpansion.expand_keys(raw)
        Helpers.re_enqueue_on_backpressure(payload, "", delay_ms)
      end

      message
    else
      polled_at = :os.system_time(:millisecond)
      [id, fields] = data
      enqueued_at = Helpers.extract_enqueued_at(id)

      payload_json = Helpers.get_payload_from_redis_data(fields)

      case Jason.decode(payload_json) do
        {:ok, raw} ->
          payload = Prism.FanoutBroadway.KeyExpansion.expand_keys(raw)

          %{"batch_id" => batch_id, "targets" => targets} = payload
          action = Map.get(payload, "action", "execute")
          discord_payload = Map.get(payload, "payload", %{})
          parent_message_id = Map.get(payload, "message_id")
          metadata = Map.get(payload, "metadata", %{})
          hub_id = Map.get(payload, "hub_id")

          if parent_message_id && Prism.CancelChecker.cancelled?(parent_message_id) do
            Logger.info(
              "FanoutBroadway: skipping cancelled batch batch_id=#{batch_id} message_id=#{parent_message_id}"
            )

            message
          else
            shard_index = Map.get(payload, "shard_index", 0)
            trace_headers = Map.get(payload, "trace_headers", %{}) |> Enum.to_list()

            ctx = :otel_propagator_text_map.extract(trace_headers)
            OpenTelemetry.Ctx.attach(ctx)

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
              current = Supervisor.count_children(Prism.TaskSup).active

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

                Prism.DelayedQueue.enqueue(payload, 200)
              end
            end

            message
          end

        _ ->
          Logger.error("Failed to parse or invalid payload: #{inspect(payload_json)}")
          Message.failed(message, "invalid payload")
      end
    end
  end
end
