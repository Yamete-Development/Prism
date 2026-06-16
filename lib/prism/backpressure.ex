defmodule Prism.Backpressure do
  @moduledoc """
  Per-node (local ETS) backpressure for rate-limited Prism workers.

  When a worker receives a Cloudflare 429 (IP-level block), it records the
  exact ``retry_after`` delay from the response and sleeps between batches
  for that duration. This lets healthy workers on other IPs claim messages
  from the shared Redis consumer group.

  The ETS table is local to the BEAM node — **not** shared via Redis — so
  each worker manages its own backpressure independently.
  """

  use GenServer
  require Logger

  @max_backoff_ms 600_000
  # 10 minutes

  # ---------------------------------------------------------------------------
  # Public API
  # ---------------------------------------------------------------------------

  @doc """
  Returns the remaining backoff sleep in milliseconds, or 0 if healthy.
  Computed dynamically from the stored ``backoff_until`` timestamp so the
  value decays automatically as time passes.
  """
  @spec backoff_ms() :: non_neg_integer()
  def backoff_ms do
    case :ets.lookup(:prism_backpressure, :backoff_until) do
      [{:backoff_until, 0}] ->
        0

      [{:backoff_until, until}] when is_integer(until) ->
        now = System.monotonic_time(:millisecond)
        if now < until, do: until - now, else: 0

      _ ->
        0
    end
  end

  @doc "Returns true if the worker is actively throttling consumption."
  @spec unhealthy?() :: boolean()
  def unhealthy?, do: backoff_ms() > 0

  @doc """
  Called from the 429 handler when a Cloudflare (non-JSON body) response
  is received. Sets the backoff to the exact ``retry_after`` value from
  the response, capped at #{@max_backoff_ms}ms.
  """
  @spec record_cloudflare_block(pos_integer()) :: :ok
  def record_cloudflare_block(retry_after_ms) when is_integer(retry_after_ms) and retry_after_ms > 0 do
    ms = min(retry_after_ms, @max_backoff_ms)
    until = System.monotonic_time(:millisecond) + ms

    was_active = unhealthy?()
    :ets.insert(:prism_backpressure, {:backoff_until, until})

    unless was_active do
      Logger.warning(
        "[Backpressure] Cloudflare block detected (retry_after=#{retry_after_ms}ms). " <>
          "Throttling for #{ms}ms to let healthy workers claim messages."
      )
    end

    :ok
  end

  def record_cloudflare_block(_), do: :ok

  @doc """
  Called when any target is successfully delivered to Discord.
  Immediately resets backpressure so the worker resumes full speed.
  """
  @spec record_success() :: :ok
  def record_success do
    was_active = unhealthy?()
    :ets.insert(:prism_backpressure, {:backoff_until, 0})

    if was_active do
      Logger.info("[Backpressure] Successful delivery detected. Backpressure released.")
    end

    :ok
  end

  # ---------------------------------------------------------------------------
  # GenServer (ETS table initialization)
  # ---------------------------------------------------------------------------

  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @impl true
  def init(_opts) do
    :ets.new(:prism_backpressure, [:set, :public, :named_table])
    :ets.insert(:prism_backpressure, {:backoff_until, 0})
    {:ok, %{}}
  end
end
