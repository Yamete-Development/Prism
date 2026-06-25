defmodule Prism.StreamTrimmer do
  @moduledoc """
  Periodically trims Redis streams using MINID ~ based on the consumer group's
  oldest pending message. This ensures we never trim messages that haven't been
  processed yet, while keeping streams compact during normal operation.

  Runs at a configurable interval for each registered stream. During normal
  operation streams stay near-empty because processed messages are trimmed
  immediately. During an outage streams grow to hold the full backlog and
  shrink back down once the consumer catches up.
  """
  use GenServer
  require Logger

  alias Prism.Helpers

  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @impl true
  def init(_opts) do
    if Prism.Config.stream_trimmer_enabled?() do
      fanout_group = Prism.Config.consumer_group()
      stream_jobs = Prism.Config.stream_jobs()
      stream_retries = Prism.Config.stream_retries()
      retry_group = fanout_group <> "_retries"
      trim_interval = Prism.Config.stream_trim_interval_ms()

      transport_backend = Prism.EventBus.Config.transport_backend()

      streams =
        if transport_backend == Prism.EventBus.Transport.Redis do
          [{stream_jobs, fanout_group, "jobs"}]
        else
          []
        end

      streams = streams ++ [{stream_retries, retry_group, "retries"}]

      Logger.info(
        "[StreamTrimmer] Starting periodic trim every #{div(trim_interval, 1000)}s for #{length(streams)} streams"
      )

      state = %{streams: streams, trim_interval: trim_interval, enabled: true}

      Process.send_after(self(), :trim, 5_000)
      {:ok, state}
    else
      Logger.info("[StreamTrimmer] Disabled via config — not starting.")
      {:ok, %{enabled: false}}
    end
  end

  @impl true
  def handle_info(:trim, %{enabled: false} = state) do
    {:noreply, state}
  end

  def handle_info(:trim, state) do
    Enum.each(state.streams, fn {stream, group, label} ->
      trim_stream(stream, group, label)
    end)

    Process.send_after(self(), :trim, state.trim_interval)
    {:noreply, state}
  end

  defp trim_stream(stream, group, label) do
    case get_safe_trim_id(stream, group) do
      {:ok, safe_id} ->
        case Helpers.redix_command(["XTRIM", stream, "MINID", "~", safe_id]) do
          {:ok, trimmed} when is_integer(trimmed) and trimmed > 0 ->
            Logger.debug("[StreamTrimmer] Trimmed #{trimmed} entries from #{label} (#{stream})")

          {:ok, 0} ->
            :ok

          {:error, reason} ->
            Logger.warning("[StreamTrimmer] Failed to trim #{label}: #{inspect(reason)}")
        end

      {:error, :empty_stream} ->
        # The stream is empty, nothing to trim
        :ok

      {:error, %Redix.Error{message: "NOGROUP" <> _}} ->
        # Consumer group hasn't been created yet
        Logger.debug(
          "[StreamTrimmer] Skipping trim for #{label}: Consumer group doesn't exist yet"
        )

      {:error, reason} ->
        Logger.warning("[StreamTrimmer] Skipping trim for #{label}: #{inspect(reason)}")
    end
  end

  defp get_safe_trim_id(stream, group) do
    case Helpers.redix_command(["XPENDING", stream, group, "-", "+", "1"]) do
      {:ok, [[min_id | _] | _]} when is_binary(min_id) ->
        {:ok, min_id}

      {:ok, []} ->
        case Helpers.redix_command(["XREVRANGE", stream, "+", "-", "COUNT", "1"]) do
          {:ok, [[last_id | _] | _]} when is_binary(last_id) -> {:ok, last_id}
          {:ok, []} -> {:error, :empty_stream}
          {:error, reason} -> {:error, reason}
        end

      {:error, reason} ->
        {:error, reason}
    end
  end
end
