import Config

parse_bool = fn value ->
  String.trim(String.downcase(to_string(value || "true"))) in ["1", "true", "yes", "on"]
end

redis_host = System.get_env("REDIS_HOST") || "localhost"
redis_port = String.to_integer(System.get_env("REDIS_PORT") || "6379")
redis_password = System.get_env("REDIS_PASSWORD")

redis_opts = [
  host: redis_host,
  port: redis_port
]

redis_opts =
  if redis_password do
    Keyword.put(redis_opts, :password, redis_password)
  else
    redis_opts
  end

config :prism,
  finch_pool_count: String.to_integer(System.get_env("PRISM_FINCH_POOL_COUNT") || "50"),
  redis_opts: redis_opts,
  redis_stream_fast: System.get_env("REDIS_STREAM_FAST") || "discord:fanout:stream:fast",
  redis_stream_slow: System.get_env("REDIS_STREAM_SLOW") || "discord:fanout:stream:slow",
  redis_callback_stream: System.get_env("REDIS_CALLBACK_STREAM") || "discord:fanout:callbacks",
  redis_group: System.get_env("REDIS_GROUP") || "elixir_fanout_pool",
  broadway_concurrency: String.to_integer(System.get_env("PRISM_BROADWAY_CONCURRENCY") || "50"),
  batch_max_concurrency: String.to_integer(System.get_env("PRISM_BATCH_MAX_CONCURRENCY") || "80"),
  retry_broadway_concurrency:
    String.to_integer(System.get_env("PRISM_RETRY_BROADWAY_CONCURRENCY") || "50"),
  backpressure_enabled: parse_bool.(System.get_env("PRISM_BACKPRESSURE_ENABLED") || "true"),
  callback_include_parent_message_id:
    parse_bool.(System.get_env("PRISM_INCLUDE_PARENT_MESSAGE_ID")),
  reply_index_enabled: parse_bool.(System.get_env("PRISM_REPLY_INDEX_ENABLED")),
  reply_index_prefix: System.get_env("PRISM_REPLY_INDEX_PREFIX") || "p:d",
  reply_index_ttl_seconds:
    String.to_integer(System.get_env("PRISM_REPLY_INDEX_TTL_SECONDS") || "604800"),
  redis_sse_enabled: parse_bool.(System.get_env("PRISM_REDIS_SSE_ENABLED") || "false"),
  redis_sse_topic_prefix:
    System.get_env("PRISM_REDIS_SSE_TOPIC_PREFIX") || "dashboard:stream:hub:",
  cancel_ttl: String.to_integer(System.get_env("PRISM_CANCEL_TTL", "300")),
  max_async_batches: String.to_integer(System.get_env("PRISM_MAX_ASYNC_BATCHES") || "300")

config :opentelemetry,
  span_processor: :batch,
  traces_exporter: :otlp

config :opentelemetry_exporter,
  otlp_protocol: :http_protobuf,
  otlp_endpoint: System.get_env("OTEL_EXPORTER_OTLP_ENDPOINT") || "http://localhost:4318"
