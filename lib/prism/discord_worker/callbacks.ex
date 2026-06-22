defmodule Prism.DiscordWorker.Callbacks do
  @moduledoc """
  Partial callback publishing for individual retry results.
  """
  alias Prism.Helpers

  require Logger

  @doc """
  Publishes a partial (single-target) result to the callback stream.

  Used by `process_retry` to report individual retry outcomes before a batch
  is fully complete.
  """
  @spec publish_partial(
          String.t(),
          map(),
          String.t() | nil,
          String.t() | nil,
          String.t() | nil,
          atom() | nil
        ) :: :ok
  def publish_partial(action, target, batch_id, parent_msg_id, success_msg_id, error_reason) do
    if not is_nil(batch_id) do
      base_info =
        %{
          "webhook_id" => target["webhook_id"],
          "message_id" => target["message_id"],
          "channel_id" => target["channel_id"],
          "guild_id" => target["guild_id"],
          "connection_id" => target["connection_id"],
          "hub_id" => target["hub_id"]
        }
        |> Map.reject(fn {_, v} -> is_nil(v) end)

      {successes, failures} =
        if error_reason do
          {error_string, error_type} =
            cond do
              error_reason == :invalid_webhook -> {"invalid_webhook", "permanent"}
              error_reason == :message_not_found -> {"message_not_found", "permanent"}
              error_reason == :bad_request -> {"bad_request", "transient"}
              error_reason == :server_error -> {"server_error", "transient"}
              error_reason == :network_error -> {"network_error", "transient"}
              error_reason == :permanent -> {"permanent_error", "permanent"}
              true -> {inspect(error_reason), "transient"}
            end

          {[], [Map.merge(base_info, %{"error" => error_string, "error_type" => error_type})]}
        else
          succ_info =
            if success_msg_id,
              do: Map.put(base_info, "message_id", success_msg_id),
              else: base_info

          {[succ_info], []}
        end

      new_trace_headers = :otel_propagator_text_map.inject([]) |> Enum.into(%{})

      payload = %{
        "batch_id" => batch_id,
        "status" => "partial_retry",
        "action" => action,
        "message_ids" => successes,
        "failures" => failures,
        "trace_headers" => new_trace_headers
      }

      payload =
        if parent_msg_id, do: Map.put(payload, "parent_message_id", parent_msg_id), else: payload

      json = Jason.encode!(payload)

      callback_stream = Prism.Config.stream_callbacks()

      Helpers.redix_command([
        "XADD",
        callback_stream,
        "MAXLEN",
        "~",
        "100000",
        "*",
        "payload",
        json
      ])
    end
  end
end
