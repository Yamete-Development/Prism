defmodule Prism.CancelChecker do
  @moduledoc """
  Checks whether a message has been cancelled (source deleted on Discord).
  Reads the Redis key `prism:cancel:{message_id}`.
  """

  require Logger

  @cancel_prefix "prism:cancel:"

  @doc """
  Returns `true` if the message has been cancelled.
  Fails open — returns `false` on Redis errors so a Redis outage doesn't
  block all message broadcasting.
  """
  @spec cancelled?(String.t()) :: boolean()
  def cancelled?(message_id) when is_binary(message_id) do
    case redix_command(["EXISTS", "#{@cancel_prefix}#{message_id}"]) do
      {:ok, 1} ->
        true

      {:ok, 0} ->
        false

      {:error, reason} ->
        Logger.warning("CancelChecker: Redis error checking #{message_id}: #{inspect(reason)}")
        false
    end
  end

  defp redix_command(command) do
    idx = :erlang.phash2(System.unique_integer(), 5)
    Redix.command(:"my_redix_#{idx}", command)
  end
end
