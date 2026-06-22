defmodule Prism.RateLimit.Backpressure do
  @moduledoc """
  Per-node (local) backpressure for Cloudflare IP-level blocks.

  Uses :persistent_term so the backoff timer survives GenServer restarts.
  """
  use GenServer
  require Logger

  @term_key :prism_backoff_until
  @blocked_at_key :prism_blocked_at

  # ---------------------------------------------------------------------------
  # Public API
  # ---------------------------------------------------------------------------

  @spec backoff_ms() :: non_neg_integer()
  def backoff_ms do
    now = System.monotonic_time(:millisecond)
    blocked_at = :persistent_term.get(@blocked_at_key, 0)

    min_cooldown = Prism.Config.backpressure_min_cooldown_ms()

    cooldown_remaining =
      if blocked_at > 0, do: max(0, min_cooldown - (now - blocked_at)), else: 0

    natural_remaining =
      case :persistent_term.get(@term_key, 0) do
        0 -> 0
        until when is_integer(until) -> if now < until, do: until - now, else: 0
      end

    max(cooldown_remaining, natural_remaining)
  end

  @spec unhealthy?() :: boolean()
  def unhealthy?, do: backoff_ms() > 0

  @spec record_cloudflare_block(pos_integer()) :: :ok
  def record_cloudflare_block(retry_after_ms)
      when is_integer(retry_after_ms) and retry_after_ms > 0 do
    GenServer.cast(__MODULE__, {:block, retry_after_ms})
  end

  def record_cloudflare_block(_), do: :ok

  @spec record_success() :: :ok
  def record_success do
    GenServer.cast(__MODULE__, :success)
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
    :persistent_term.put(@blocked_at_key, 0)
    {:ok, %{}}
  end

  @impl true
  def handle_cast({:block, retry_after_ms}, state) do
    max_backoff = Prism.Config.backpressure_max_backoff_ms()
    ms = min(retry_after_ms, max_backoff)
    now = System.monotonic_time(:millisecond)
    until = now + ms

    was_active = unhealthy?()
    :persistent_term.put(@blocked_at_key, now)
    :persistent_term.put(@term_key, until)

    if was_active do
      Logger.debug(
        "[Backpressure] Cloudflare block extends existing backpressure " <>
          "(retry_after=#{retry_after_ms}ms, capped=#{ms}ms)."
      )
    else
      Logger.warning(
        "[Backpressure] Cloudflare block detected (retry_after=#{retry_after_ms}ms). " <>
          "Throttling for #{ms}ms to let healthy workers claim messages."
      )
    end

    {:noreply, state}
  end

  @impl true
  def handle_cast(:success, state) do
    now = System.monotonic_time(:millisecond)
    blocked_at = :persistent_term.get(@blocked_at_key, 0)
    min_cooldown = Prism.Config.backpressure_min_cooldown_ms()

    if blocked_at > 0 and now - blocked_at < min_cooldown do
      Logger.debug(
        "[Backpressure] record_success ignored — within minimum cooldown window " <>
          "(#{now - blocked_at}ms elapsed of #{min_cooldown}ms)."
      )
    else
      was_active = unhealthy?()
      :persistent_term.put(@term_key, 0)

      if was_active do
        Logger.info("[Backpressure] Successful delivery detected. Backpressure released.")
      end
    end

    {:noreply, state}
  end
end
