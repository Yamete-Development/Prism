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
      {:ok, %{"batch_id" => batch_id, "targets" => targets} = payload} ->
        action = Map.get(payload, "action", "execute")
        discord_payload = Map.get(payload, "payload", %{})
        process_batch(action, batch_id, discord_payload, targets)
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

  defp process_batch(action, batch_id, discord_payload, targets) do
    # Process targets with bounded concurrency and wait for completion
    results =
      Task.async_stream(
        targets,
        fn target ->
          DiscordWorker.process_target(action, target, discord_payload)
        end,
        max_concurrency: 20,
        timeout: :infinity
      )
      |> Enum.to_list()

    {successes, failures} =
      targets
      |> Enum.zip(results)
      |> Enum.reduce({[], []}, fn {target, result_tuple}, {succ_acc, fail_acc} ->
        # result_tuple from Task.async_stream is {:ok, worker_result} or {:exit, reason}
        worker_result = case result_tuple do
          {:ok, res} -> res
          _ -> {:error, :task_crashed}
        end

        webhook_id = Map.get(target, "webhook_id") || "unknown"
        # Optional bot team identifiers that might be in the target
        conn_id = Map.get(target, "connection_id")
        hub_id = Map.get(target, "hub_id")
        channel_id = Map.get(target, "channel_id")
        guild_id = Map.get(target, "guild_id")
        
        base_info = %{
          "webhook_id" => webhook_id,
          "connection_id" => conn_id,
          "hub_id" => hub_id,
          "channel_id" => channel_id,
          "guild_id" => guild_id
        }
        # Clean up nil values so the JSON is tidy
        base_info = :maps.filter(fn _, v -> v != nil end, base_info)

        case worker_result do
          {:ok, msg_id} ->
            # msg_id might be nil for edit/delete
            succ_info = if msg_id, do: Map.put(base_info, "message_id", msg_id), else: base_info
            {[succ_info | succ_acc], fail_acc}

          {:error, reason} ->
            fail_info = Map.put(base_info, "error", inspect(reason))
            {succ_acc, [fail_info | fail_acc]}
        end
      end)

    ok_count = length(successes)
    fail_count = length(failures)
    Logger.info("Batch #{batch_id} done: #{ok_count} ok, #{fail_count} failed")

    payload = Jason.encode!(%{
      status: "success",
      action: action,
      message_ids: Enum.reverse(successes), # We map successes to message_ids list
      failures: Enum.reverse(failures)
    })
    
    Redix.command(:my_redix, ["PUBLISH", "callbacks:#{batch_id}", payload])
    Logger.info("Published callback for batch #{batch_id}")
  end
end
