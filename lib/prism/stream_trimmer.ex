defmodule Prism.StreamTrimmer do
  @moduledoc """
  Periodically trims Redis fanout streams using MINID ~ based on the consumer
  group's oldest pending message. This ensures we never trim messages that
  haven't been processed yet, while keeping the stream compact during normal
  operation.

  Runs every 30 seconds for each stream (fast and slow). During normal
  operation the stream stays near-empty because processed messages are trimmed
  immediately. During an outage the stream grows to hold the full backlog and
  shrinks back down once the consumer catches up.
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
    stream_fast = Application.get_env(:prism, :redis_stream_fast, "discord:fanout:stream:fast")
    stream_slow = Application.get_env(:prism, :redis_stream_slow, "discord:fanout:stream:slow")
    group = Application.get_env(:prism, :redis_group, "elixir_fanout_pool")

    Logger.info("[StreamTrimmer] Starting periodic trim every #{div(@trim_interval_ms, 1000)}s")

    state = %{
      stream_fast: stream_fast,
      stream_slow: stream_slow,
      group: group,
    }

    Process.send_after(self(), :trim, 5_000)
    {:ok, state}
  end

  @impl true
  def handle_info(:trim, state) do
    trim_stream(state.stream_fast, state.group, "fast")
    trim_stream(state.stream_slow, state.group, "slow")
    Process.send_after(self(), :trim, @trim_interval_ms)
    {:noreply, state}
  end

  # --- Private ---

  defp trim_stream(stream, group, _lane) do
    idx = :erlang.phash2(System.unique_integer(), 5)
    redix = :"my_redix_#{idx}"

    case get_safe_trim_id(redix, stream, group) do
      {:ok, safe_id} ->
        case Redix.command(redix, ["XTRIM", stream, "MINID", "~", safe_id]) do
          {:ok, trimmed} when is_integer(trimmed) and trimmed > 0 ->
            Logger.debug("[StreamTrimmer] Trimmed #{trimmed} entries from #{stream}")

          {:ok, 0} ->
            :ok

          {:error, reason} ->
            Logger.warning("[StreamTrimmer] Failed to trim #{stream}: #{inspect(reason)}")
        end

      {:error, reason} ->
        Logger.warning("[StreamTrimmer] Skipping trim for #{stream}: #{inspect(reason)}")
    end
  end

  defp get_safe_trim_id(redix, stream, group) do
    case Redix.command(redix, ["XPENDING", stream, group, "-", "+", "1"]) do
      {:ok, [[min_id | _] | _]} when is_binary(min_id) ->
        {:ok, min_id}

      {:ok, []} ->
        # No pending messages — everything has been processed. Trim to the
        # last entry in the stream so we shrink it to nearly zero.
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
