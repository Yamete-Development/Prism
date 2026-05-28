defmodule InterchatBroadcastWorker.MixProject do
  use Mix.Project

  def project do
    [
      app: :interchat_broadcast_worker,
      version: "0.1.0",
      elixir: "~> 1.19",
      start_permanent: Mix.env() == :prod,
      deps: deps()
    ]
  end

  # Run "mix help compile.app" to learn about applications.
  def application do
    [
      extra_applications: [:logger],
      mod: {InterchatBroadcastWorker.Application, []}
    ]
  end

  # Run "mix help deps" to learn about dependencies.
  defp deps do
    [
      {:broadway, "~> 1.1"},
      {:off_broadway_redis_stream, "~> 0.7"},
      {:finch, "~> 0.18"},
      {:jason, "~> 1.4"},
      {:redix, "~> 1.4"}
    ]
  end
end
