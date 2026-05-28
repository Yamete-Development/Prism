import Config

redis_host = System.get_env("REDIS_HOST") || "localhost"
redis_port = String.to_integer(System.get_env("REDIS_PORT") || "6379")
redis_password = System.get_env("REDIS_PASSWORD")

redis_opts = [
  host: redis_host,
  port: redis_port
]

redis_opts = if redis_password do
  Keyword.put(redis_opts, :password, redis_password)
else
  redis_opts
end

config :broadcast_worker,
  redis_opts: redis_opts,
  redis_stream: System.get_env("REDIS_STREAM") || "discord:fanout:stream",
  redis_group: System.get_env("REDIS_GROUP") || "elixir_fanout_pool"
