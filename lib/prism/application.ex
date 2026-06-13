defmodule Prism.Application do
  # See https://hexdocs.pm/elixir/Application.html
  # for more information on OTP Applications
  @moduledoc false

  use Application

  @impl true
  def start(_type, _args) do
    require Logger

    unless Node.alive?() do
      Node.start(:prism, :shortnames)
    end

    Logger.info(
      "[Prism] Starting up! Initializing Redix pool (5 conns) and Finch pool (250 conns)."
    )

    # Initialize atomic counter for Broadway batches
    :persistent_term.put(:active_batches, :atomics.new(1, signed: false))

    redis_opts = Application.get_env(:prism, :redis_opts, host: "localhost", port: 6379)

    redix_children =
      for i <- 0..4 do
        Supervisor.child_spec({Redix, Keyword.put(redis_opts, :name, :"my_redix_#{i}")},
          id: :"my_redix_#{i}"
        )
      end

    topologies = [
      interchat: [
        strategy: Cluster.Strategy.LocalEpmd
      ]
    ]

    children =
      [
        {Cluster.Supervisor, [topologies, [name: Prism.ClusterSupervisor]]},
        {Finch,
         name: DiscordFinch, pools: %{"https://discord.com" => [protocols: [:http2], count: 20]}},
        {Task.Supervisor, name: Prism.TaskSup},
        {Prism.MetricsAPI, []}
      ] ++
        redix_children ++
        [
          %{
            id: Prism.PubSub,
            start: {Redix.PubSub, :start_link, [Keyword.put(redis_opts, :name, Prism.PubSub)]}
          },
          {Prism.DelayedScheduler, []},
          Supervisor.child_spec(
            {Prism.FanoutBroadway, [name: Prism.FanoutBroadway.Fast, lane: :fast]},
            id: :fanout_broadway_fast
          ),
          Supervisor.child_spec(
            {Prism.FanoutBroadway, [name: Prism.FanoutBroadway.Slow, lane: :slow]},
            id: :fanout_broadway_slow
          ),
          Supervisor.child_spec(
            {Prism.RetryBroadway, [name: Prism.RetryBroadway]},
            id: :retry_broadway
          ),
          {Prism.MetricsLogger, []}
        ]

    # See https://hexdocs.pm/elixir/Supervisor.html
    # for other strategies and supported options
    opts = [strategy: :one_for_one, name: Prism.Supervisor]

    Logger.info("[Prism] Application supervisor starting children...")
    Supervisor.start_link(children, opts)
  end
end
