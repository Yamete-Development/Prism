defmodule Prism.DiscordWorker.DeadMessage do
  @moduledoc """
  Dead message cache: prevents retrying webhook deliveries to known-deleted messages.

  Checks both scoped (`dead_msg:{webhook_id}:{message_id}`) and unscoped
  (`dead_msg:{message_id}`) cache keys. Gated by `Prism.Config.dead_message_cache_enabled?/0`.
  """
  alias Prism.Helpers

  require Logger

  @doc """
  Returns `true` if the message is known to be dead (deleted on Discord).

  Checks both the scoped key (webhook_id + message_id) and the unscoped key
  (message_id only) for backward compatibility.
  """
  @spec dead_message_cached?(String.t(), String.t()) :: boolean()
  def dead_message_cached?(webhook_id, message_id) do
    if Prism.Config.dead_message_cache_enabled?() do
      prefix = Prism.Config.dead_message_cache_prefix()

      case Helpers.redix_command(["EXISTS", "#{prefix}#{webhook_id}:#{message_id}"]) do
        {:ok, 1} ->
          true

        _ ->
          case Helpers.redix_command(["EXISTS", "#{prefix}#{message_id}"]) do
            {:ok, 1} -> true
            _ -> false
          end
      end
    else
      false
    end
  end

  @doc """
  Stores a dead message entry in the cache with a configurable TTL.
  """
  @spec cache_dead_message(String.t(), String.t(), integer()) :: :ok | {:error, term()}
  def cache_dead_message(webhook_id, message_id, ttl_seconds \\ nil) do
    ttl = ttl_seconds || Prism.Config.dead_message_cache_ttl()
    prefix = Prism.Config.dead_message_cache_prefix()

    Helpers.redix_command([
      "SETEX",
      "#{prefix}#{webhook_id}:#{message_id}",
      to_string(ttl),
      "1"
    ])
  end
end
