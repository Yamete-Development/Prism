defmodule BroadcastWorker.Application do
  # See https://hexdocs.pm/elixir/Application.html
  # for more information on OTP Applications
  @moduledoc false

  use Application

  @impl true
  def start(_type, _args) do
    require Logger
    Logger.info("[BroadcastWorker] Starting up! Initializing Redix pool (5 conns) and Finch pool (250 conns).")

    redis_opts = Application.get_env(:broadcast_worker, :redis_opts, [host: "localhost", port: 6379])

    redix_children = for i <- 0..4 do
      Supervisor.child_spec({Redix, Keyword.put(redis_opts, :name, :"my_redix_#{i}")}, id: :"my_redix_#{i}")
    end

    children = [
      {Finch, name: DiscordFinch, pools: %{"https://discord.com" => [size: 250]}},
      {Task.Supervisor, name: BroadcastWorker.TaskSup}
    ] ++ redix_children ++ [
      Supervisor.child_spec({BroadcastWorker.FanoutBroadway, [name: BroadcastWorker.FanoutBroadway.Fast, lane: :fast]}, id: :fanout_broadway_fast),
      Supervisor.child_spec({BroadcastWorker.FanoutBroadway, [name: BroadcastWorker.FanoutBroadway.Slow, lane: :slow]}, id: :fanout_broadway_slow)
    ]

    # See https://hexdocs.pm/elixir/Supervisor.html
    # for other strategies and supported options
    opts = [strategy: :one_for_one, name: BroadcastWorker.Supervisor]
    
    Logger.info("[BroadcastWorker] Application supervisor starting children...")
    Supervisor.start_link(children, opts)
  end
end
