defmodule InterchatBroadcastWorker.Application do
  # See https://hexdocs.pm/elixir/Application.html
  # for more information on OTP Applications
  @moduledoc false

  use Application

  @impl true
  def start(_type, _args) do
    redis_opts = Application.get_env(:interchat_broadcast_worker, :redis_opts, [host: "localhost", port: 6379])
    
    children = [
      {Redix, Keyword.put(redis_opts, :name, :my_redix)},
      {Finch, name: DiscordFinch, pools: %{"https://discord.com" => [size: 100]}},
      {Task.Supervisor, name: InterchatBroadcastWorker.TaskSup},
      InterchatBroadcastWorker.FanoutBroadway
    ]

    # See https://hexdocs.pm/elixir/Supervisor.html
    # for other strategies and supported options
    opts = [strategy: :one_for_one, name: InterchatBroadcastWorker.Supervisor]
    Supervisor.start_link(children, opts)
  end
end
