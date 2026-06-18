defmodule Prism.FanoutBroadway do
  use Broadway

  require Logger
  require OpenTelemetry.Tracer

  alias Broadway.Message

  # Key expansion mapping: short → long. Applied to decoded JSON from the Python
  # publisher to reverse the minification that reduces Redis stream memory.
  # Keys not in this map pass through unchanged for backward compatibility.
  @key_map %{
    # Top-level
    "a" => "action",
    "b" => "batch_id",
    "m" => "message_id",
    "s" => "shard_index",
    "h" => "hub_id",
    "n" => "hub_name",
    "p" => "payload",
    "t" => "targets",
    "d" => "metadata",
    "r" => "trace_headers",
    # Target
    "c" => "channel_id",
    "w" => "webhook_id",
    "k" => "webhook_token",
    "g" => "guild_id",
    "f" => "thread_id",
    "o" => "overrides",
    "ci" => "connection_id",
    # Payload body
    "u" => "username",
    "v" => "avatar_url",
    "x" => "content",
    "e" => "embeds",
    "q" => "components",
    "l" => "allowed_mentions",
    "fl" => "flags",
    # Metadata
    "ai" => "author_id",
    "gn" => "guild_name",
    "bg" => "badges"
  }

  @doc """
  Recursively expands minified JSON keys back to their full names.
  If the payload is already in long-key format (e.g. key \"action\" is present
  and is not a known short key), it passes through unchanged for backward
  compatibility with older Python publishers.
  """
  def expand_keys(map) when is_map(map) do
    # Detect format: if the first key maps to a known long key, expand.
    first_key =
      map
      |> Map.keys()
      |> Enum.find(fn _ -> true end)

    is_minified = first_key && Map.has_key?(@key_map, first_key)

    if is_minified do
      do_expand_keys(map)
    else
      map
    end
  end

  def expand_keys(value), do: value

  defp do_expand_keys(map) when is_map(map) do
    map
    |> Enum.reduce(%{}, fn {key, value}, acc ->
      long_key = Map.get(@key_map, key, key)

      expanded_value =
        cond do
          is_map(value) ->
            do_expand_keys(value)

          is_list(value) ->
            Enum.map(value, fn item -> if is_map(item), do: do_expand_keys(item), else: item end)

          true ->
            value
        end

      Map.put(acc, long_key, expanded_value)
    end)
  end

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

    max_batches_per_sec = Application.get_env(:prism, :max_batches_per_sec, 5)
    broadway_concurrency = Application.get_env(:prism, :broadway_concurrency, 50)

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
            receive_interval: 5
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
      # No batcher needed, each message is processed independently
    )
  end

  @impl true
  def handle_message(_, %Message{data: data} = message, _) do
    # If this worker is under a Cloudflare ban, re‑enqueue the batch and skip all network work
    if backpressure_enabled?() and Prism.RateLimit.unhealthy?() do
      delay_ms = Prism.RateLimit.backoff_ms()
      [_id, fields] = data
      payload_json = get_payload_from_redis_data(fields)

      # Decode, expand, and re‑enqueue the full batch payload with the remaining ban delay
      with {:ok, raw} <- Jason.decode(payload_json) do
        payload = expand_keys(raw)
        Prism.DelayedQueue.enqueue(payload, delay_ms)
      end

      # Ack the original stream message immediately – the batch will be processed later
      message
    else
      polled_at = :os.system_time(:millisecond)
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
        {:ok, raw} ->
          # Expand minified keys to full names for downstream processing.
          # If the payload is already in long-key format, expand_keys is a no-op.
          payload = expand_keys(raw)

          %{"batch_id" => batch_id, "targets" => targets} = payload
          action = Map.get(payload, "action", "execute")
          discord_payload = Map.get(payload, "payload", %{})
          parent_message_id = Map.get(payload, "message_id")
          metadata = Map.get(payload, "metadata", %{})
          hub_id = Map.get(payload, "hub_id")

          shard_index = Map.get(payload, "shard_index", 0)
          trace_headers = Map.get(payload, "trace_headers", %{}) |> Enum.to_list()

          ctx = :otel_propagator_text_map.extract(trace_headers)
          OpenTelemetry.Ctx.attach(ctx)

          OpenTelemetry.Tracer.with_span "prism.worker.process_batch" do
            OpenTelemetry.Tracer.set_attributes([
              {:batch_id, batch_id},
              {:action, action},
              {:target_count, length(targets)},
              {:shard_index, shard_index}
            ])

            process_batch(
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
          end

          message

        _ ->
          Logger.error("Failed to parse or invalid payload: #{inspect(payload_json)}")
          Message.failed(message, "invalid payload")
      end
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
         parent_message_id,
         payload_metadata,
         root_hub_id,
         shard_index
       ) do
    include_parent_message_id =
      Application.get_env(:prism, :callback_include_parent_message_id, true)

    queue_time = polled_at - enqueued_at

    Logger.debug(
      "Started batch #{batch_id} (Queue Time: #{queue_time}ms, Targets: #{length(targets)})"
    )

    if ref = :persistent_term.get(:active_batches, nil) do
      :atomics.add(ref, 1, 1)
    end

    batch_max_concurrency = Application.get_env(:prism, :batch_max_concurrency, 80)

    try do
      # Capture current context to propagate into child processes
      ctx = OpenTelemetry.Ctx.get_current()

      # Process targets with bounded concurrency and wait for completion
      results =
        Task.async_stream(
          targets,
          fn target ->
            OpenTelemetry.Ctx.attach(ctx)

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
          max_concurrency: batch_max_concurrency,
          timeout: 60_000
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
                    {"message_not_found", "permanent", %{}}

                  :bad_request ->
                    {"bad_request", "transient", %{}}

                  :missing_webhook ->
                    {"missing_webhook", "permanent", %{}}

                  :invalid_action ->
                    {"invalid_action", "permanent", %{}}

                  {:permanent, detail} ->
                    {"permanent_error", "permanent", %{"detail" => inspect(detail)}}

                  {:server_error, _} ->
                    {"server_error", "transient", %{}}

                  :network_error ->
                    {"network_error", "transient", %{}}

                  :rate_limited ->
                    {"rate_limited", "transient", %{}}

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
      batch_time = :os.system_time(:millisecond) - polled_at
      parent_log = if parent_message_id, do: " (Parent Msg: #{parent_message_id})", else: ""

      Logger.debug(
        "Batch #{batch_id}#{parent_log} done in #{batch_time}ms: #{ok_count} ok, #{fail_count} unsuccessful"
      )

      if action == "execute" and not is_nil(parent_message_id) and reply_index_enabled?() do
        store_reply_index(parent_message_id, successes)
      end

      new_trace_headers = :otel_propagator_text_map.inject([]) |> Enum.into(%{})

      payload_map = %{
        batch_id: batch_id,
        status: "success",
        action: action,
        # We map successes to message_ids list
        message_ids: Enum.reverse(successes),
        failures: Enum.reverse(failures),
        trace_headers: new_trace_headers
      }

      payload_map =
        if include_parent_message_id and parent_message_id do
          Map.put(payload_map, :parent_message_id, parent_message_id)
        else
          payload_map
        end

      payload = Jason.encode!(payload_map)

      callback_stream =
        Application.get_env(:prism, :redis_callback_stream, "discord:fanout:callbacks")

      idx = :erlang.phash2(System.unique_integer(), 5)

      Redix.command(:"my_redix_#{idx}", [
        "XADD",
        callback_stream,
        "MAXLEN",
        "~",
        "100000",
        "*",
        "payload",
        payload
      ])

      Logger.debug("Published callback to #{callback_stream} for batch #{batch_id}#{parent_log}")

      # Send a real-time event via :pg for the dashboard to render
      event_data = %{
        batch_id: batch_id,
        action: action,
        ok_count: ok_count,
        fail_count: fail_count,
        timestamp: :os.system_time(:millisecond)
      }

      event_data = Map.merge(event_data, payload_metadata || %{})

      sse_enabled = Application.get_env(:prism, :redis_sse_enabled, false)

      Logger.debug(
        "SSE Evaluation -> enabled: #{sse_enabled}, action: #{action}, targets: #{length(targets)}, shard_index: #{shard_index}"
      )

      # Publish to Redis for web UI SSE streams if enabled
      if sse_enabled and action == "execute" and length(targets) > 0 and shard_index == 0 do
        first_target = hd(targets)

        # Use root_hub_id if provided by the Python bot payload, else fallback to the target's hub_id
        hub_id = root_hub_id || Map.get(first_target, "hub_id")

        Logger.debug("SSE Extracted hub_id: #{inspect(hub_id)}")

        if hub_id do
          safe_metadata = payload_metadata || %{}

          stream_payload =
            %{
              content: Map.get(discord_payload, "content", ""),
              authorId: Map.get(safe_metadata, "author_id", ""),
              guildId: Map.get(safe_metadata, "guild_id", ""),
              authorName: Map.get(discord_payload, "username", "Unknown User"),
              guildName: Map.get(safe_metadata, "guild_name", "Unknown Server"),
              badges: Map.get(safe_metadata, "badges", []),
              createdAt: DateTime.utc_now() |> DateTime.to_iso8601(),
              id: parent_message_id || batch_id,
              authorAvatarUrl: Map.get(discord_payload, "avatar_url", nil)
            }
            |> Jason.encode!()

          sse_topic_prefix =
            Application.get_env(:prism, :redis_sse_topic_prefix, "dashboard:stream:hub:")

          idx = :erlang.phash2(System.unique_integer(), 5)

          case Redix.command(:"my_redix_#{idx}", [
                 "PUBLISH",
                 "#{sse_topic_prefix}#{hub_id}",
                 stream_payload
               ]) do
            {:ok, _} -> Logger.debug("SSE Publish Success to #{sse_topic_prefix}#{hub_id}")
            {:error, reason} -> Logger.error("SSE Publish Failed: #{inspect(reason)}")
          end
        end
      end

      Enum.each(:pg.get_members(:prism_events), fn pid ->
        send(pid, {:batch_processed, event_data})
      end)
    after
      if ref = :persistent_term.get(:active_batches, nil) do
        :atomics.sub(ref, 1, 1)
      end
    end
  end

  defp store_reply_index(parent_message_id, successes) when is_binary(parent_message_id) do
    reply_index_prefix = Application.get_env(:prism, :reply_index_prefix, "p:d")

    reply_index_ttl =
      Integer.to_string(Application.get_env(:prism, :reply_index_ttl_seconds, 604_800))

    reply_key = "#{reply_index_prefix}:reply:#{parent_message_id}"

    commands =
      Enum.flat_map(successes, fn success ->
        channel_id = Map.get(success, "channel_id")
        broadcast_id = Map.get(success, "message_id")

        if is_binary(channel_id) and is_binary(broadcast_id) do
          [
            ["HSET", reply_key, channel_id, broadcast_id],
            [
              "SETEX",
              "#{reply_index_prefix}:copy:#{broadcast_id}",
              reply_index_ttl,
              parent_message_id
            ]
          ]
        else
          []
        end
      end)

    if commands != [] do
      # Add exactly one EXPIRE for the reply_key at the end
      commands = commands ++ [["EXPIRE", reply_key, reply_index_ttl]]

      idx = :erlang.phash2(System.unique_integer(), 5)
      Redix.pipeline(:"my_redix_#{idx}", commands)
    end
  end

  defp store_reply_index(_parent_message_id, _successes), do: :ok

  defp reply_index_enabled? do
    Application.get_env(:prism, :reply_index_enabled, true)
  end

  defp backpressure_enabled? do
    Application.get_env(:prism, :backpressure_enabled, true)
  end
end
