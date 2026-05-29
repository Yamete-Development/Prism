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
  redis_stream_fast: System.get_env("REDIS_STREAM_FAST") || "discord:fanout:stream:fast",
  redis_stream_slow: System.get_env("REDIS_STREAM_SLOW") || "discord:fanout:stream:slow",
  redis_callback_stream: System.get_env("REDIS_CALLBACK_STREAM") || "discord:fanout:callbacks",
  redis_group: System.get_env("REDIS_GROUP") || "elixir_fanout_pool",
  max_batches_per_sec: String.to_integer(System.get_env("MAX_BATCHES_PER_SEC") || "1")
