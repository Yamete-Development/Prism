defmodule Prism.DelayedScheduler do
  @moduledoc """
  Event-driven zero-polling scheduler for the delayed retry queue.
  """
  use GenServer
  require Logger

  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @impl true
  def init(_opts) do
    Logger.info("[DelayedScheduler] Starting up...")

    pubsub_channel = Prism.Config.pubsub_channel()

    if Process.whereis(Prism.PubSub) do
      Redix.PubSub.subscribe(Prism.PubSub, pubsub_channel, self())
    end

    send(self(), :tick)

    {:ok, %{timer_ref: nil, pubsub_channel: pubsub_channel}}
  end

  @impl true
  def handle_info(:tick, state) do
    now = :os.system_time(:millisecond)

    case Prism.DelayedQueue.migrate_due_items(now) do
      {:ok, nil} ->
        {:noreply, %{state | timer_ref: nil}}

      {:ok, next_score} ->
        delay_ms = max(next_score - :os.system_time(:millisecond), 0)

        timer_ref = Process.send_after(self(), :tick, delay_ms)
        {:noreply, %{state | timer_ref: timer_ref}}

      {:error, _reason} ->
        retry_ms = Prism.Config.delayed_scheduler_error_retry_ms()
        timer_ref = Process.send_after(self(), :tick, retry_ms)
        {:noreply, %{state | timer_ref: timer_ref}}
    end
  end

  @impl true
  def handle_info(
        {:redix_pubsub, _pubsub_pid, _ref, :message, %{channel: channel, payload: payload}},
        %{pubsub_channel: channel} = state
      ) do
    Logger.info("Received wakeup: #{payload}")

    if String.starts_with?(payload, "new_earliest:") do
      if state.timer_ref, do: Process.cancel_timer(state.timer_ref)
      send(self(), :tick)
    end

    {:noreply, state}
  end

  @impl true
  def handle_info({:redix_pubsub, _pubsub, _ref, _type, _msg}, state) do
    {:noreply, state}
  end
end
