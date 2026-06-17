defmodule Prism.Backpressure do
  @moduledoc """
  Per-node (local) backpressure for Cloudflare IP-level blocks.

  Uses :persistent_term so the backoff timer survives GenServer restarts.
  """
  use GenServer
  require Logger

  @max_backoff_ms 600_000
  @term_key :prism_backoff_until

  # ---------------------------------------------------------------------------
  # Public API
  # ---------------------------------------------------------------------------

  @spec backoff_ms() :: non_neg_integer()
  def backoff_ms do
    case :persistent_term.get(@term_key, 0) do
      0 ->
        0

      until when is_integer(until) ->
        now = System.monotonic_time(:millisecond)
        if now < until, do: until - now, else: 0
    end
  end

  @spec unhealthy?() :: boolean()
  def unhealthy?, do: backoff_ms() > 0

  @spec record_cloudflare_block(pos_integer()) :: :ok
  def record_cloudflare_block(retry_after_ms)
      when is_integer(retry_after_ms) and retry_after_ms > 0 do
    ms = min(retry_after_ms, @max_backoff_ms)
    until = System.monotonic_time(:millisecond) + ms

    was_active = unhealthy?()
    :persistent_term.put(@term_key, until)

    unless was_active do
      Logger.warning(
        "[Backpressure] Cloudflare block detected (retry_after=#{retry_after_ms}ms). " <>
          "Throttling for #{ms}ms to let healthy workers claim messages."
      )
    end

    :ok
  end

  def record_cloudflare_block(_), do: :ok

  @spec record_success() :: :ok
  def record_success do
    was_active = unhealthy?()
    :persistent_term.put(@term_key, 0)

    if was_active do
      Logger.info("[Backpressure] Successful delivery detected. Backpressure released.")
    end

    :ok
  end

  # ---------------------------------------------------------------------------
  # GenServer (initialises the term)
  # ---------------------------------------------------------------------------

  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @impl true
  def init(_opts) do
    :persistent_term.put(@term_key, 0)
    {:ok, %{}}
  end
end
