defmodule Prism.FanoutBroadway.Batch do
  @moduledoc """
  Batch processing: fans out webhook targets with bounded concurrency,
  aggregates results, stores reply index, and publishes callbacks to Redis.
  """
  alias Prism.Helpers

  require Logger
  require OpenTelemetry.Tracer

  @doc """
  Processes a batch of webhook targets asynchronously, aggregates results,
  publishes callback stream and SSE events, and broadcasts to pg.
  """
  @spec process_batch(
          String.t(),
          String.t(),
          map(),
          [map()],
          integer(),
          integer(),
          String.t() | nil,
          map(),
          String.t() | nil,
          integer()
        ) :: :ok
  def process_batch(
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
    include_parent_message_id = Prism.Config.callback_include_parent_message_id?()

    queue_time = polled_at - enqueued_at

    Logger.debug(
      "Started batch #{batch_id} (Queue Time: #{queue_time}ms, Targets: #{length(targets)})"
    )

    queue_warn = Prism.Config.queue_time_warn_ms()

    if queue_time > queue_warn do
      Logger.warning(
        "High queue time for batch #{batch_id}: #{queue_time}ms (Targets: #{length(targets)})"
      )
    end

    batch_max_concurrency = Prism.Config.batch_max_concurrency()

    OpenTelemetry.Tracer.with_span "prism.worker.process_batch" do
      OpenTelemetry.Tracer.set_attributes([
        {:batch_id, batch_id},
        {:action, action},
        {:target_count, length(targets)},
        {:shard_index, shard_index}
      ])

      ctx = OpenTelemetry.Ctx.get_current()

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
          timeout: Prism.Config.task_timeout_ms()
        )
        |> Enum.to_list()

      {successes, failures} =
        targets
        |> Enum.zip(results)
        |> Enum.reduce({[], []}, fn {target, result_tuple}, {succ_acc, fail_acc} ->
          worker_result =
            case result_tuple do
              {:ok, res} -> res
              _ -> {:error, :task_crashed}
            end

          webhook_id = Map.get(target, "webhook_id") || "unknown"
          conn_id = Map.get(target, "connection_id")
          hub_id = Map.get(target, "hub_id")
          channel_id = Map.get(target, "channel_id")
          guild_id = Map.get(target, "guild_id")

          base_info = %{
            "webhook_id" => webhook_id,
            "message_id" => Map.get(target, "message_id"),
            "connection_id" => conn_id,
            "hub_id" => hub_id,
            "channel_id" => channel_id,
            "guild_id" => guild_id
          }

          base_info = :maps.filter(fn _, v -> v != nil end, base_info)

          case worker_result do
            {:ok, msg_id} ->
              succ_info =
                if msg_id, do: Map.put(base_info, "message_id", msg_id), else: base_info

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

      if action == "execute" and not is_nil(parent_message_id) and
           Prism.Config.reply_index_enabled?() do
        store_reply_index(parent_message_id, successes)
      end

      new_trace_headers = :otel_propagator_text_map.inject([]) |> Enum.into(%{})

      payload_map = %{
        batch_id: batch_id,
        status: "success",
        action: action,
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

      callback_stream = Prism.Config.stream_callbacks()

      Helpers.redix_command([
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

      event_data = %{
        batch_id: batch_id,
        action: action,
        ok_count: ok_count,
        fail_count: fail_count,
        timestamp: :os.system_time(:millisecond)
      }

      event_data = Map.merge(event_data, payload_metadata || %{})

      Prism.FanoutBroadway.SSE.publish_sse_event(
        action,
        targets,
        shard_index,
        discord_payload,
        root_hub_id,
        payload_metadata,
        parent_message_id
      )

      Enum.each(:pg.get_members(:prism_events), fn pid ->
        send(pid, {:batch_processed, event_data})
      end)
    end
  end

  @doc """
  Spawns `process_batch/10` as an async task under `Prism.TaskSup`.
  Returns immediately so the Broadway processor can pull the next message.

  On task crash, re-enqueues the full batch payload to the delayed queue.
  """
  @spec spawn_async_batch(
          String.t(),
          String.t(),
          map(),
          [map()],
          integer(),
          integer(),
          String.t() | nil,
          map(),
          String.t() | nil,
          integer()
        ) :: :ok
  def spawn_async_batch(
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
      ) do
    case Task.Supervisor.start_child(Prism.TaskSup, fn ->
           try do
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
           rescue
             e ->
               Logger.error(
                 "Async batch #{batch_id} crashed: #{Exception.message(e)}\n" <>
                   Exception.format_stacktrace(__STACKTRACE__)
               )

               payload = %{
                 "action" => action,
                 "batch_id" => batch_id,
                 "payload" => discord_payload,
                 "targets" => targets,
                 "message_id" => parent_message_id,
                 "metadata" => metadata,
                 "hub_id" => hub_id,
                 "shard_index" => shard_index
               }

               Prism.DelayedQueue.enqueue(payload, 5_000)
           end
         end) do
      {:ok, _pid} ->
        :ok

      {:error, reason} ->
        Logger.error(
          "Failed to start async task for batch #{batch_id}: #{inspect(reason)}. " <>
            "Re-enqueueing to delayed queue (200ms)."
        )

        payload = %{
          "action" => action,
          "batch_id" => batch_id,
          "payload" => discord_payload,
          "targets" => targets,
          "message_id" => parent_message_id,
          "metadata" => metadata,
          "hub_id" => hub_id,
          "shard_index" => shard_index
        }

        Prism.DelayedQueue.enqueue(payload, 200)
    end
  end

  defp store_reply_index(parent_message_id, successes) when is_binary(parent_message_id) do
    reply_index_prefix = Prism.Config.reply_index_prefix()
    reply_index_ttl = Integer.to_string(Prism.Config.reply_index_ttl_seconds())
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
      commands = commands ++ [["EXPIRE", reply_key, reply_index_ttl]]
      Helpers.redix_pipeline(commands)
    end
  end

  defp store_reply_index(_parent_message_id, _successes), do: :ok
end
