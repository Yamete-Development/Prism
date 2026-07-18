defmodule Prism.Health do
  @moduledoc """
  Minimal liveness and readiness endpoints for orchestration probes.

  Liveness only proves the BEAM can serve requests. Readiness additionally
  requires Prism's supervisor and the critical Kafka, Redis, and fanout
  processes to be alive, so a partially started worker cannot receive traffic.
  """

  use Plug.Router

  plug(:match)
  plug(:dispatch)

  get "/live" do
    send_resp(conn, 200, "live\n")
  end

  get "/ready" do
    if ready?() do
      send_resp(conn, 200, "ready\n")
    else
      send_resp(conn, 503, "not ready\n")
    end
  end

  match _ do
    send_resp(conn, 404, "not found\n")
  end

  @doc false
  def ready? do
    process_alive?(Process.whereis(Prism.Supervisor)) and
      process_alive?(Process.whereis(:kafka_client)) and
      process_alive?(Process.whereis(Prism.PubSub)) and
      process_alive?(Process.whereis(Prism.RetryBroadway)) and
      fanout_alive?()
  end

  defp fanout_alive? do
    count = Prism.Config.fanout_producer_count()

    count > 0 and
      Enum.all?(0..(count - 1), fn index ->
        name = Module.concat(Prism.FanoutBroadway, :"Jobs_#{index}")
        process_alive?(Process.whereis(name))
      end)
  end

  defp process_alive?(pid) when is_pid(pid), do: Process.alive?(pid)
  defp process_alive?(_pid), do: false
end
