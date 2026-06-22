defmodule Prism.MetricsLogger do
  use GenServer
  require Logger

  alias Prism.Helpers

  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @impl true
  def init(_opts) do
    :timer.send_interval(10_000, :log_metrics)
    {:ok, %{}}
  end

  @impl true
  def handle_info(:log_metrics, state) do
    batch_count = Supervisor.count_children(Prism.TaskSup).active
    process_count = length(:erlang.processes())
    run_queue = :erlang.statistics(:run_queue)
    port_count = length(:erlang.ports())

    Logger.info(
      "[Metrics] Active Broadway Batches: #{batch_count} | Erlang Processes: #{process_count} | Run Queue: #{run_queue} | Open Ports/Sockets: #{port_count} | Fast Stream Len: #{stream_length(Prism.Config.stream_fast())} | Slow Stream Len: #{stream_length(Prism.Config.stream_slow())}"
    )

    {:noreply, state}
  end

  defp stream_length(stream_key) do
    case Helpers.redix_command(["XLEN", stream_key]) do
      {:ok, len} -> len
      _ -> -1
    end
  end
end
