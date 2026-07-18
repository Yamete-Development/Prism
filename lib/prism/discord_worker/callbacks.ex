defmodule Prism.DiscordWorker.Callbacks do
  @moduledoc """
  Partial callback publishing for individual retry results.
  """
  alias Prism.Helpers

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
          {error_string, error_type, _extra} = Prism.ErrorMapping.to_error_info(error_reason)

          {[], [Map.merge(base_info, %{"error" => error_string, "error_type" => error_type})]}
        else
          succ_info =
            if success_msg_id,
              do: Map.put(base_info, "message_id", success_msg_id),
              else: base_info

          {[succ_info], []}
        end

      payload = %{
        "batch_id" => batch_id,
        "action_id" => target["polarizer_action_id"],
        "status" => "partial_retry",
        "action" => action,
        "message_ids" => successes,
        "failures" => failures
      }

      payload =
        payload
        |> then(fn value ->
          if parent_msg_id, do: Map.put(value, "parent_message_id", parent_msg_id), else: value
        end)
        |> Map.reject(fn {_key, value} -> is_nil(value) end)

      Helpers.publish_callback(payload)

      if is_nil(error_reason) do
        Helpers.publish_delivery_callback(
          target["polarizer_action_id"],
          parent_msg_id,
          :MESSAGE_STATE_ACTIVE
        )
      end
    end
  end
end
