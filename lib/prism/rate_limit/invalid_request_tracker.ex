defmodule Prism.RateLimit.InvalidRequestTracker do
  @moduledoc """
  Per-worker ETS-based sliding window counter for invalid HTTP requests.

  Discord's Cloudflare IP ban triggers at 10,000 invalid requests per 10
  minutes. An invalid request is any response with status 401, 403, or 429
  where `X-RateLimit-Scope` is not `"shared"`.

  This GenServer maintains a local `:ordered_set` ETS table with millisecond
  timestamps as keys. Entries older than the window are pruned periodically.
  When the in-window count exceeds the backpressure threshold new outbound
  HTTP is deferred to the retry queue.
  """
  use GenServer

  require Logger

  @table_name :prism_invalid_tracker

  # ---------------------------------------------------------------------------
  # Public API
  # ---------------------------------------------------------------------------

  @doc false
  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @doc "Records a single invalid response at the current monotonic time."
  @spec record_invalid() :: :ok
  def record_invalid do
    GenServer.cast(__MODULE__, :record)
  end

  @doc """
  Returns the number of invalid responses recorded in the configured window.
  """
  @spec count_in_window() :: non_neg_integer()
  def count_in_window do
    GenServer.call(__MODULE__, :count)
  end

  @doc """
  Returns `true` when the in-window invalid count exceeds the backpressure
  threshold, signalling that this worker should pause outbound HTTP.
  """
  @spec approaching_limit?() :: boolean()
  def approaching_limit? do
    GenServer.call(__MODULE__, :approaching)
  end

  # ---------------------------------------------------------------------------
  # GenServer callbacks
  # ---------------------------------------------------------------------------

  @impl true
  def init(_opts) do
    :ets.new(@table_name, [:ordered_set, :public, :named_table])
    :timer.send_interval(30_000, :prune)
    :timer.send_interval(60_000, :report)
    {:ok, %{}}
  end

  @impl true
  def handle_cast(:record, state) do
    now = System.monotonic_time(:millisecond)
    :ets.insert(@table_name, {now})
    {:noreply, state}
  end

  @impl true
  def handle_call(:count, _from, state) do
    {:reply, count_local(), state}
  end

  @impl true
  def handle_call(:approaching, _from, state) do
    threshold = Prism.Config.invalid_request_backpressure_threshold()
    {:reply, count_local() >= threshold, state}
  end

  @impl true
  def handle_info(:prune, state) do
    window_ms = Prism.Config.invalid_request_window_ms()
    cutoff = System.monotonic_time(:millisecond) - window_ms
    :ets.select_delete(@table_name, [{{:"$1"}, [{:<, :"$1", cutoff}], [true]}])
    {:noreply, state}
  end

  @impl true
  def handle_info(:report, state) do
    count = count_local()
    backpressure_threshold = Prism.Config.invalid_request_backpressure_threshold()
    critical_threshold = Prism.Config.invalid_request_critical_threshold()

    cond do
      count >= critical_threshold ->
        Logger.error(
          "[InvalidRequestTracker] CRITICAL: #{count} invalid requests in the " <>
            "last window. Cloudflare ban may be imminent."
        )

      count >= backpressure_threshold ->
        Logger.warning(
          "[InvalidRequestTracker] #{count} invalid requests in the last " <>
            "window. Approaching Cloudflare ban threshold " <>
            "(#{critical_threshold}). Backpressure active."
        )

      true ->
        :ok
    end

    {:noreply, state}
  end

  # ---------------------------------------------------------------------------
  # Private helpers
  # ---------------------------------------------------------------------------

  defp count_local do
    now = System.monotonic_time(:millisecond)
    window_ms = Prism.Config.invalid_request_window_ms()
    cutoff = now - window_ms
    :ets.select_count(@table_name, [{{:"$1"}, [{:>=, :"$1", cutoff}], [true]}])
  end
end
