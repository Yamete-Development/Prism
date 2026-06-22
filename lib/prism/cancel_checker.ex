defmodule Prism.CancelChecker do
  @moduledoc """
  Checks whether a message has been cancelled (source deleted on Discord).

  Reads the Redis key `prism:cancel:{message_id}`. Gated by
  `Prism.Config.cancel_checker_enabled?/0` — when disabled, always returns
  `false` so that Redis is never queried.

  Fails open — returns `false` on Redis errors so a Redis outage doesn't
  block all message broadcasting.
  """

  alias Prism.Helpers

  require Logger

  @doc """
  Returns `true` if the message has been cancelled.
  """
  @spec cancelled?(String.t()) :: boolean()
  def cancelled?(message_id) when is_binary(message_id) do
    if Prism.Config.cancel_checker_enabled?() do
      cancel_prefix = Prism.Config.cancel_prefix()

      case Helpers.redix_command(["EXISTS", "#{cancel_prefix}#{message_id}"]) do
        {:ok, 1} ->
          true

        {:ok, 0} ->
          false

        {:error, reason} ->
          Logger.warning("CancelChecker: Redis error checking #{message_id}: #{inspect(reason)}")
          false
      end
    else
      false
    end
  end
end
