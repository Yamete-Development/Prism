defmodule Prism.RetryBroadway do
  use Broadway

  require Logger

  alias Broadway.Message

  def start_link(opts) do
    name = Keyword.get(opts, :name, __MODULE__)

    redis_opts = Application.get_env(:prism, :redis_opts, host: "localhost", port: 6379)
    stream_key = "discord:fanout:stream:retries"
    redis_group = Application.get_env(:prism, :redis_group, "elixir_fanout_pool") <> "_retries"

    max_batches_per_sec = Application.get_env(:prism, :retry_max_batches_per_sec, 2)
    broadway_concurrency = Application.get_env(:prism, :retry_broadway_concurrency, 10)

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
              "broadcast_worker_retry_" <> Integer.to_string(:os.system_time(:microsecond)),
            make_stream: true,
            receive_interval: 100
          ]
        },
        rate_limiting: [
          allowed_messages: max_batches_per_sec,
          interval: 1000
        ]
      ],
      processors: [
        default: [
          concurrency: broadway_concurrency,
          max_demand: 1,
          min_demand: 0
        ]
      ]
    )
  end

  @impl true
  def handle_message(_, %Message{data: data} = message, _) do
    if backpressure_enabled?() and Prism.RateLimit.unhealthy?() do
      delay_ms = Prism.RateLimit.backoff_ms()
      [_id, fields] = data
      payload_json = get_payload_from_redis_data(fields)

      with {:ok, raw} <- Jason.decode(payload_json) do
        batch_id = Map.get(raw, "batch_id", "unknown")
        action = Map.get(raw, "action", "execute")

        Logger.info(
          "[Backpressure-Retry] Active Cloudflare block (remaining: #{delay_ms}ms). " <>
            "Re-enqueueing retry for #{action} batch #{batch_id} to delayed queue."
        )

        Prism.DelayedQueue.enqueue(raw, delay_ms)
      else
        _ ->
          Logger.error(
            "Failed to parse retry payload during backpressure: #{inspect(payload_json)}"
          )

          Message.failed(message, "invalid payload during backpressure")
      end

      message
    else
      polled_at = :os.system_time(:millisecond)
      # OffBroadwayRedisStream returns data as [entry_id, [field1, value1, ...]]
      [id, fields] = data

      enqueued_at =
        case String.split(id, "-") do
          [timestamp_str, _] ->
            case Integer.parse(timestamp_str) do
              {ts, ""} -> ts
              _ -> :os.system_time(:millisecond)
            end

          _ ->
            :os.system_time(:millisecond)
        end

      payload_json = get_payload_from_redis_data(fields)

      case Jason.decode(payload_json) do
        {:ok, payload} ->
          if Map.has_key?(payload, "targets") do
            # Batch payload from FanoutBroadway backpressure — route back to the
            # appropriate fanout stream so it can be fanned out to individual targets.
            route_batch_to_fanout(payload, payload_json)
          else
            Prism.DiscordWorker.process_retry(payload, polled_at, enqueued_at)
          end

          message

        _ ->
          Logger.error("Failed to parse retry payload: #{inspect(payload_json)}")
          Message.failed(message, "invalid payload")
      end
    end
  end

  defp get_payload_from_redis_data(data) when is_list(data) do
    Enum.chunk_every(data, 2)
    |> Enum.find_value(fn
      ["payload", value] -> value
      _ -> nil
    end) || ""
  end

  defp get_payload_from_redis_data(%{"payload" => payload}), do: payload
  defp get_payload_from_redis_data(_), do: ""

  # Routes a batch-level payload (containing "targets") back to the appropriate
  # fanout stream so it can be fanned out to individual webhook targets.
  #
  # The batch payload arrives here when FanoutBroadway enqueues it to the delayed
  # queue during Cloudflare backpressure. Once the delay expires and backpressure
  # is cleared, we re-publish it to the fanout stream for normal processing.
  defp route_batch_to_fanout(payload, payload_json) do
    targets = Map.get(payload, "targets", [])
    target_count = length(targets)
    batch_id = Map.get(payload, "batch_id", "unknown")

    stream_key =
      if target_count > 80 do
        Application.get_env(:prism, :redis_stream_slow, "discord:fanout:stream:slow")
      else
        Application.get_env(:prism, :redis_stream_fast, "discord:fanout:stream:fast")
      end

    Logger.info(
      "[RetryBroadway] Routing batch #{batch_id} (#{target_count} targets) back to fanout stream #{stream_key}"
    )

    idx = :erlang.phash2(System.unique_integer(), 5)

    case Redix.command(:"my_redix_#{idx}", ["XADD", stream_key, "*", "payload", payload_json]) do
      {:ok, _entry_id} ->
        :ok

      {:error, reason} ->
        Logger.error(
          "[RetryBroadway] Failed to XADD batch #{batch_id} to #{stream_key}: #{inspect(reason)}"
        )
    end
  end

  defp backpressure_enabled? do
    Application.get_env(:prism, :backpressure_enabled, true)
  end
end
