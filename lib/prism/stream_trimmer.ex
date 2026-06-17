defmodule Prism.StreamTrimmer do
  @moduledoc """
  Periodically trims Redis streams using MINID ~ based on the consumer group's
  oldest pending message. This ensures we never trim messages that haven't been
  processed yet, while keeping streams compact during normal operation.

  Runs every 30 seconds for each registered stream. During normal operation
  streams stay near-empty because processed messages are trimmed immediately.
  During an outage streams grow to hold the full backlog and shrink back down
  once the consumer catches up.
  """
  use GenServer
  require Logger

  @trim_interval_ms 30_000

  # --- Public API ---

  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  # --- GenServer Callbacks ---

  @impl true
  def init(_opts) do
    fanout_group = Application.get_env(:prism, :redis_group, "elixir_fanout_pool")
    callback_group = "bot_team"

    stream_fast = Application.get_env(:prism, :redis_stream_fast, "discord:fanout:stream:fast")
    stream_slow = Application.get_env(:prism, :redis_stream_slow, "discord:fanout:stream:slow")
    callback_stream =
      Application.get_env(:prism, :redis_callback_stream, "discord:fanout:callbacks")

    # Each entry is {stream_key, consumer_group, label}
    streams = [
      {stream_fast, fanout_group, "fast"},
      {stream_slow, fanout_group, "slow"},
      {callback_stream, callback_group, "callbacks"},
    ]

    Logger.info("[StreamTrimmer] Starting periodic trim every #{div(@trim_interval_ms, 1000)}s for #{length(streams)} streams")

    state = %{streams: streams}

    Process.send_after(self(), :trim, 5_000)
    {:ok, state}
  end

  @impl true
  def handle_info(:trim, state) do
    Enum.each(state.streams, fn {stream, group, label} ->
      trim_stream(stream, group, label)
    end)

    Process.send_after(self(), :trim, @trim_interval_ms)
    {:noreply, state}
  end

  # --- Private ---

  defp trim_stream(stream, group, label) do
    idx = :erlang.phash2(System.unique_integer(), 5)
    redix = :"my_redix_#{idx}"

    case get_safe_trim_id(redix, stream, group) do
      {:ok, safe_id} ->
        case Redix.command(redix, ["XTRIM", stream, "MINID", "~", safe_id]) do
          {:ok, trimmed} when is_integer(trimmed) and trimmed > 0 ->
            Logger.debug("[StreamTrimmer] Trimmed #{trimmed} entries from #{label} (#{stream})")

          {:ok, 0} ->
            :ok

          {:error, reason} ->
            Logger.warning("[StreamTrimmer] Failed to trim #{label}: #{inspect(reason)}")
        end

      {:error, reason} ->
        Logger.warning("[StreamTrimmer] Skipping trim for #{label}: #{inspect(reason)}")
    end
  end

  defp get_safe_trim_id(redix, stream, group) do
    case Redix.command(redix, ["XPENDING", stream, group, "-", "+", "1"]) do
      {:ok, [[min_id | _] | _]} when is_binary(min_id) ->
        {:ok, min_id}

      {:ok, []} ->
        case Redix.command(redix, ["XREVRANGE", stream, "+", "-", "COUNT", "1"]) do
          {:ok, [[last_id | _] | _]} when is_binary(last_id) -> {:ok, last_id}
          {:ok, []} -> {:error, :empty_stream}
          {:error, reason} -> {:error, reason}
        end

      {:error, reason} ->
        {:error, reason}
    end
  end
end
