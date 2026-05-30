defmodule Prism.MetricsLogger do
  use GenServer
  require Logger

  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @impl true
  def init(_opts) do
    # Log every 10 seconds
    :timer.send_interval(10_000, :log_metrics)
    {:ok, %{}}
  end

  @impl true
  def handle_info(:log_metrics, state) do
    task_count = Task.Supervisor.children(Prism.TaskSup) |> length()
    process_count = length(:erlang.processes())
    run_queue = :erlang.statistics(:run_queue)
    port_count = length(:erlang.ports())

    active_batches =
      case :persistent_term.get(:active_batches, nil) do
        nil -> 0
        ref -> :atomics.get(ref, 1)
      end

    Logger.info(
      "[Metrics] Active Broadway Batches: #{active_batches} | Active Retries (TaskSup): #{task_count} | Erlang Processes: #{process_count} | Run Queue: #{run_queue} | Open Ports/Sockets: #{port_count}"
    )

    {:noreply, state}
  end
end
