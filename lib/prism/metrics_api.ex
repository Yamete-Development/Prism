defmodule Prism.MetricsAPI do
  use GenServer
  require Logger

  @name __MODULE__

  def start_link(_) do
    GenServer.start_link(__MODULE__, %{}, name: @name)
  end

  # Called by the dashboard to get current metrics
  def get_metrics do
    GenServer.call(@name, :get_metrics)
  end

  def init(_) do
    # Join the pg group so the dashboard can find this node
    :pg.start_link()
    :pg.join(:prism_nodes, self())
    {:ok, %{start_time: System.system_time(:second)}}
  end

  def handle_call(:get_metrics, _from, state) do
    # Just basic metrics for now
    active_batches = :atomics.get(:persistent_term.get(:active_batches), 1)

    metrics = %{
      node: node(),
      uptime: System.system_time(:second) - state.start_time,
      active_batches: active_batches,
      memory: :erlang.memory(:total)
    }

    {:reply, metrics, state}
  end
end
