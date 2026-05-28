defmodule InterchatBroadcastWorker.FanoutBroadway do
  use Broadway

  require Logger

  alias Broadway.Message
  alias InterchatBroadcastWorker.DiscordWorker

  def start_link(_opts) do
    redis_opts = Application.get_env(:interchat_broadcast_worker, :redis_opts, [host: "localhost", port: 6379])
    redis_stream = Application.get_env(:interchat_broadcast_worker, :redis_stream, "discord:fanout:stream")
    redis_group = Application.get_env(:interchat_broadcast_worker, :redis_group, "elixir_fanout_pool")

    Broadway.start_link(__MODULE__,
      name: __MODULE__,
      producer: [
        module: {
          OffBroadwayRedisStream.Producer,
          [
            redis_client_opts: redis_opts,
            stream: redis_stream,
            group: redis_group,
            consumer_name: "interchat_worker_" <> Integer.to_string(:os.system_time(:microsecond)),
            make_stream: true
          ]
        },
        rate_limiting: [
          allowed_messages: 50,
          interval: 1000
        ]
      ],
      processors: [
        default: [concurrency: 50]
      ]
      # No batcher needed — each message is processed independently
    )
  end

  @impl true
  def handle_message(_, %Message{data: data} = message, _) do
    # OffBroadwayRedisStream returns data as [entry_id, [field1, value1, ...]]
    [_id, fields] = data
    payload_json = get_payload_from_redis_data(fields)

    case Jason.decode(payload_json) do
      {:ok, %{"batch_id" => batch_id, "payload" => discord_payload, "targets" => targets}} ->
        process_batch(batch_id, discord_payload, targets)
        message

      _ ->
        Logger.error("Failed to parse or invalid payload: #{inspect(payload_json)}")
        Message.failed(message, "invalid payload")
    end
  end

  # Helper to extract the 'payload' field from the Redis stream data
  defp get_payload_from_redis_data(data) when is_list(data) do
    Enum.chunk_every(data, 2)
    |> Enum.find_value(fn
      ["payload", value] -> value
      _ -> nil
    end) || ""
  end

  defp get_payload_from_redis_data(%{"payload" => payload}), do: payload
  defp get_payload_from_redis_data(_), do: ""

  defp process_batch(batch_id, discord_payload, targets) do
    target_count = length(targets)
    Logger.info("Starting batch #{batch_id} with #{target_count} target(s)")

    results =
      Task.async_stream(
        targets,
        fn target ->
          DiscordWorker.send_to_discord_with_retries(target, discord_payload)
        end,
        max_concurrency: 20,
        timeout: :infinity
      )
      |> Enum.to_list()

    {broadcasts, failures} =
      targets
      |> Enum.zip(results)
      |> Enum.reduce({[], []}, fn {target, result}, {bcasts, fails} ->
        conn_id = Map.get(target, "connection_id")
        hub_id = Map.get(target, "hub_id")
        channel_id = Map.get(target, "channel_id")
        guild_id = Map.get(target, "guild_id")

        case result do
          {:ok, msg_id} ->
            broadcast = %{
              "broadcast_id" => msg_id,
              "channel_id" => channel_id,
              "guild_id" => guild_id
            }
            {[broadcast | bcasts], fails}

          {:error, _reason} ->
            failure = %{
              "connection_id" => conn_id,
              "hub_id" => hub_id,
              "error_type" => "permanent"
            }
            {bcasts, [failure | fails]}
        end
      end)

    ok_count = length(broadcasts)
    drop_count = length(failures)
    Logger.info("Batch #{batch_id} done: #{ok_count} ok, #{drop_count} dropped")

    payload = Jason.encode!(%{
      status: "success",
      broadcasts: Enum.reverse(broadcasts),
      failures: Enum.reverse(failures)
    })
    Redix.command(:my_redix, ["PUBLISH", "callbacks:#{batch_id}", payload])
    Logger.info("Published callback for batch #{batch_id}")
  end
end
