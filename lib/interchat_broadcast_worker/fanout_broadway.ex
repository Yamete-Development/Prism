defmodule InterchatBroadcastWorker.FanoutBroadway do
  use Broadway

  require Logger

  alias Broadway.Message
  alias InterchatBroadcastWorker.DiscordWorker

  def start_link(_opts) do
    Broadway.start_link(__MODULE__,
      name: __MODULE__,
      producer: [
        module: {
          OffBroadwayRedisStream.Producer,
          [
            redis_client_opts: [host: "localhost", port: 6379],
            stream: "discord:fanout:stream",
            group: "elixir_fanout_pool",
            consumer_name: "interchat_worker_" <> Integer.to_string(:os.system_time(:microsecond))
          ]
        },
        rate_limiting: [
          allowed_messages: 50,
          interval: 1000
        ]
      ],
      processors: [
        default: [concurrency: 50]
      ],
      # We don't necessarily need a batcher unless we want to group them
      batchers: [
        default: [concurrency: 1, batch_size: 1]
      ]
    )
  end

  @impl true
  def handle_message(_, %Message{data: data} = message, _) do
    # Assuming data is a list of elements from Redis stream, usually [id, key1, val1, ...]
    # The prompt says: You will receive a JSON string in the payload field of the Redis Stream message.
    payload_json = get_payload_from_redis_data(data)

    case Jason.decode(payload_json) do
      {:ok, %{"batch_id" => batch_id, "payload" => discord_payload, "targets" => targets}} ->
        process_batch(batch_id, discord_payload, targets)
        message

      _ ->
        Logger.error("Failed to parse or invalid payload: #{inspect(payload_json)}")
        # We can mark as failed or just acknowledge it as dropped
        Message.failed(message, "invalid payload")
    end
  end

  # Helper to extract the 'payload' field from the Redis stream data
  # Redis stream data in Elixir usually comes as a list or map of string pairs.
  defp get_payload_from_redis_data(data) when is_list(data) do
    # Note: off_broadway_redis_stream returns data in a specific format, typically a list of key-value pairs.
    # We look for "payload" key
    Enum.chunk_every(data, 2)
    |> Enum.find_value(fn
      ["payload", value] -> value
      _ -> nil
    end) || ""
  end

  defp get_payload_from_redis_data(%{"payload" => payload}), do: payload
  defp get_payload_from_redis_data(_), do: ""

  defp process_batch(batch_id, discord_payload, targets) do
    # Process targets with bounded concurrency and wait for completion
    Task.async_stream(
      targets,
      fn target ->
        DiscordWorker.send_to_discord_with_retries(target, discord_payload)
      end,
      max_concurrency: 20,
      timeout: :infinity
    )
    |> Stream.run()

    # Once all targets are processed (or dropped), notify completion
    payload = Jason.encode!(%{status: "success"})
    Redix.command(:my_redix, ["PUBLISH", "callbacks:#{batch_id}", payload])
    Logger.info("Batch #{batch_id} fully completed and bot notified.")
  end
end
