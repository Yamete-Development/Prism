defmodule Prism.DelayedScheduler do
  @moduledoc """
  Event-driven zero-polling scheduler for the delayed retry queue.
  """
  use GenServer
  require Logger

  @pubsub_channel "prism:wakeup"

  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @impl true
  def init(_opts) do
    Logger.info("[DelayedScheduler] Starting up...")

    # Subscribe to Redis Pub/Sub for early wakeups
    if Process.whereis(Prism.PubSub) do
      Redix.PubSub.subscribe(Prism.PubSub, @pubsub_channel, self())
    end

    # Schedule the first check immediately
    send(self(), :tick)

    {:ok, %{timer_ref: nil}}
  end

  @impl true
  def handle_info(:tick, state) do
    now = :os.system_time(:millisecond)

    case Prism.DelayedQueue.migrate_due_items(now) do
      {:ok, nil} ->
        # No items in the queue, sleep forever until a wakeup event arrives
        {:noreply, %{state | timer_ref: nil}}

      {:ok, next_score} ->
        # Schedule the next tick based on the earliest item
        delay_ms = max(next_score - :os.system_time(:millisecond), 0)
        
        timer_ref = Process.send_after(self(), :tick, delay_ms)
        {:noreply, %{state | timer_ref: timer_ref}}

      {:error, _reason} ->
        # If Redis fails, retry in 5 seconds
        timer_ref = Process.send_after(self(), :tick, 5_000)
        {:noreply, %{state | timer_ref: timer_ref}}
    end
  end

  @impl true
  def handle_info({:redix_pubsub, _pubsub_pid, _ref, :message, %{channel: @pubsub_channel, payload: payload}}, state) do
    # When a new item is added that is earlier than our current timer, we receive this
    Logger.info("Received wakeup: #{payload}")
    if String.starts_with?(payload, "new_earliest:") do
      # Cancel current timer and re-evaluate immediately
      if state.timer_ref, do: Process.cancel_timer(state.timer_ref)
      send(self(), :tick)
    end
    {:noreply, state}
  end
  
  @impl true
  def handle_info({:redix_pubsub, _pubsub, _ref, _type, _msg}, state) do
    # Ignore other pubsub events
    {:noreply, state}
  end
end
