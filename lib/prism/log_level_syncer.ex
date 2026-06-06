defmodule Prism.LogLevelSyncer do
  @moduledoc """
  Periodically syncs the global Logger level from a Redis configuration key.
  This allows turning on aggressive debugging without restarting the worker.
  """
  use GenServer
  require Logger

  @interval 30_000
  @redis_key "interchat:config:log_level"

  def start_link(_) do
    GenServer.start_link(__MODULE__, %{}, name: __MODULE__)
  end

  @impl true
  def init(state) do
    # Start the periodic timer immediately after initialization
    Process.send_after(self(), :sync, 1000)
    {:ok, state}
  end

  @impl true
  def handle_info(:sync, state) do
    sync_level()
    Process.send_after(self(), :sync, @interval)
    {:noreply, state}
  end

  defp sync_level() do
    # We use :my_redix_0 which is guaranteed to be started by Application.ex
    case Redix.command(:my_redix_0, ["GET", @redis_key]) do
      {:ok, nil} ->
        # Default fallback
        Logger.configure(level: :info)

      {:ok, level_str} when is_binary(level_str) ->
        case String.downcase(String.trim(level_str)) do
          "debug" -> Logger.configure(level: :debug)
          "info" -> Logger.configure(level: :info)
          "warning" -> Logger.configure(level: :warning)
          "error" -> Logger.configure(level: :error)
          _ -> Logger.configure(level: :info)
        end

      _ ->
        :ok
    end
  rescue
    _ -> :ok
  end
end
