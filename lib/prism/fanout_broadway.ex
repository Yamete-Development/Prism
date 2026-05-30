defmodule Prism.FanoutBroadway do
  use Broadway

  require Logger

  alias Broadway.Message

  def start_link(opts) do
    lane = Keyword.fetch!(opts, :lane)
    name = Keyword.get(opts, :name, __MODULE__)

    redis_opts = Application.get_env(:prism, :redis_opts, host: "localhost", port: 6379)

    stream_key =
      if lane == :fast do
        Application.get_env(:prism, :redis_stream_fast, "discord:fanout:stream:fast")
      else
        Application.get_env(:prism, :redis_stream_slow, "discord:fanout:stream:slow")
      end

    redis_group = Application.get_env(:prism, :redis_group, "elixir_fanout_pool")

    max_batches_per_sec = Application.get_env(:prism, :max_batches_per_sec, 2)

    Broadway.start_link(__MODULE__,
      name: name,
      producer: [
        module: {
          OffBroadwayRedisStream.Producer,
          [
            redis_client_opts: redis_opts,
            stream: stream_key,
            group: redis_group,
            consumer_name:
              "broadcast_worker_#{lane}_" <> Integer.to_string(:os.system_time(:microsecond)),
            make_stream: true,
            receive_interval: 50
          ]
        },
        rate_limiting: [
          allowed_messages: max_batches_per_sec,
          interval: 1000
        ]
      ],
      processors: [
        default: [concurrency: 50]
      ]
      # No batcher needed, each message is processed independently
    )
  end

  @impl true
  def handle_message(_, %Message{data: data} = message, _) do
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
      {:ok, %{"batch_id" => batch_id, "targets" => targets} = payload} ->
        action = Map.get(payload, "action", "execute")
        discord_payload = Map.get(payload, "payload", %{})
        parent_message_id = Map.get(payload, "message_id")

        process_batch(
          action,
          batch_id,
          discord_payload,
          targets,
          polled_at,
          enqueued_at,
          parent_message_id
        )

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

  defp process_batch(
         action,
         batch_id,
         discord_payload,
         targets,
         polled_at,
         enqueued_at,
         parent_message_id
       ) do
    if ref = :persistent_term.get(:active_batches, nil) do
      :atomics.add(ref, 1, 1)
    end

    try do
      # Process targets with bounded concurrency and wait for completion
      results =
        Task.async_stream(
          targets,
          fn target ->
            Prism.DiscordWorker.process_target(
              action,
              target,
              discord_payload,
              batch_id,
              polled_at,
              enqueued_at,
              parent_message_id
            )
          end,
          max_concurrency: 20,
          timeout: 15_000
        )
        |> Enum.to_list()

      {successes, failures} =
        targets
        |> Enum.zip(results)
        |> Enum.reduce({[], []}, fn {target, result_tuple}, {succ_acc, fail_acc} ->
          # result_tuple from Task.async_stream is {:ok, worker_result} or {:exit, reason}
          worker_result =
            case result_tuple do
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
              {error_string, error_type, extra} =
                case reason do
                  {:rate_limited, retry_after_ms} ->
                    {"rate_limited", "transient", %{"retry_after_ms" => retry_after_ms}}

                  :invalid_webhook ->
                    {"invalid_webhook", "permanent", %{}}

                  :message_not_found ->
                    {"message_not_found", "transient", %{}}

                  :bad_request ->
                    {"bad_request", "transient", %{}}

                  :missing_webhook ->
                    {"missing_webhook", "permanent", %{}}

                  :invalid_action ->
                    {"invalid_action", "permanent", %{}}

                  {:server_error, _} ->
                    {"server_error", "transient", %{}}

                  :network_error ->
                    {"network_error", "transient", %{}}

                  :task_crashed ->
                    {"task_crashed", "transient", %{}}

                  _ ->
                    {inspect(reason), "transient", %{}}
                end

              fail_info =
                base_info
                |> Map.put("error", error_string)
                |> Map.put("error_type", error_type)
                |> Map.merge(extra)

              {succ_acc, [fail_info | fail_acc]}
          end
        end)

      ok_count = length(successes)
      fail_count = length(failures)
      Logger.info("Batch #{batch_id} done: #{ok_count} ok, #{fail_count} failed")

      payload =
        Jason.encode!(%{
          batch_id: batch_id,
          status: "success",
          action: action,
          # We map successes to message_ids list
          message_ids: Enum.reverse(successes),
          failures: Enum.reverse(failures)
        })

      callback_stream =
        Application.get_env(:prism, :redis_callback_stream, "discord:fanout:callbacks")

      idx = :erlang.phash2(System.unique_integer(), 5)
      Redix.command(:"my_redix_#{idx}", ["XADD", callback_stream, "*", "payload", payload])
      Logger.info("Published callback to #{callback_stream} for batch #{batch_id}")
    after
      if ref = :persistent_term.get(:active_batches, nil) do
        :atomics.sub(ref, 1, 1)
      end
    end
  end
end
