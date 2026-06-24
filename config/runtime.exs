import Config

log_level =
  case String.downcase(System.get_env("LOG_LEVEL") || "info") do
    "debug" -> :debug
    "info" -> :info
    "warning" -> :warning
    "warn" -> :warning
    "error" -> :error
    _ -> :info
  end

config :logger, level: log_level

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

config :prism, redis_opts: redis_opts

if config_env() != :test do
  config :prism,
    redix_pool_size: String.to_integer(System.get_env("PRISM_REDIX_POOL_SIZE") || "5"),
    finch_pool_count: String.to_integer(System.get_env("PRISM_FINCH_POOL_COUNT") || "50"),
    finch_receive_timeout_ms:
      String.to_integer(System.get_env("PRISM_FINCH_RECEIVE_TIMEOUT_MS") || "30000"),
    finch_pool_timeout_ms:
      String.to_integer(System.get_env("PRISM_FINCH_POOL_TIMEOUT_MS") || "10000"),
    finch_idle_timeout_ms:
      String.to_integer(System.get_env("PRISM_FINCH_IDLE_TIMEOUT_MS") || "60000"),
    finch_keepalive_ms: String.to_integer(System.get_env("PRISM_FINCH_KEEPALIVE_MS") || "30000"),
    discord_base_url: System.get_env("PRISM_DISCORD_BASE_URL") || "https://discord.com",
    stream_jobs: System.get_env("PRISM_STREAM_JOBS") || "prism.stream.jobs",
    redis_retry_stream: System.get_env("REDIS_RETRY_STREAM") || "prism.stream.retries",
    consumer_group: System.get_env("PRISM_CONSUMER_GROUP") || "prism:cg:fanout",
    delayed_zset_key: System.get_env("PRISM_DELAYED_ZSET_KEY") || "prism:delayed",
    pubsub_channel: System.get_env("PRISM_PUBSUB_CHANNEL") || "prism:wakeup",
    delayed_scheduler_error_retry_ms:
      String.to_integer(System.get_env("PRISM_DELAYED_SCHEDULER_ERROR_RETRY_MS") || "5000"),
    broadway_concurrency: String.to_integer(System.get_env("PRISM_BROADWAY_CONCURRENCY") || "50"),
    batch_max_concurrency:
      String.to_integer(System.get_env("PRISM_BATCH_MAX_CONCURRENCY") || "80"),
    retry_broadway_concurrency:
      String.to_integer(System.get_env("PRISM_RETRY_BROADWAY_CONCURRENCY") || "10"),
    jobs_receive_interval:
      String.to_integer(System.get_env("PRISM_JOBS_RECEIVE_INTERVAL") || "5"),
    retry_receive_interval:
      String.to_integer(System.get_env("PRISM_RETRY_RECEIVE_INTERVAL") || "100"),
    queue_time_warn_ms: String.to_integer(System.get_env("PRISM_QUEUE_TIME_WARN_MS") || "2000"),
    task_timeout_ms: String.to_integer(System.get_env("PRISM_TASK_TIMEOUT_MS") || "60000"),
    max_async_batches: String.to_integer(System.get_env("PRISM_MAX_ASYNC_BATCHES") || "300"),
    backpressure_enabled: parse_bool.(System.get_env("PRISM_BACKPRESSURE_ENABLED") || "true"),
    backpressure_max_backoff_ms:
      String.to_integer(System.get_env("PRISM_BACKPRESSURE_MAX_BACKOFF_MS") || "600000"),
    backpressure_min_cooldown_ms:
      String.to_integer(System.get_env("PRISM_BACKPRESSURE_MIN_COOLDOWN_MS") || "60000"),
    invalid_request_window_ms:
      String.to_integer(System.get_env("PRISM_INVALID_REQUEST_WINDOW_MS") || "600000"),
    invalid_request_backpressure_threshold:
      String.to_integer(System.get_env("PRISM_INVALID_REQUEST_BACKPRESSURE_THRESHOLD") || "9500"),
    invalid_request_critical_threshold:
      String.to_integer(System.get_env("PRISM_INVALID_REQUEST_CRITICAL_THRESHOLD") || "10000"),
    bucket_hash_ttl_seconds:
      String.to_integer(System.get_env("PRISM_BUCKET_HASH_TTL_SECONDS") || "3600"),
    server_error_base_delay_ms:
      String.to_integer(System.get_env("PRISM_SERVER_ERROR_BASE_DELAY_MS") || "2000"),
    server_error_max_retries:
      String.to_integer(System.get_env("PRISM_SERVER_ERROR_MAX_RETRIES") || "3"),
    network_error_base_delay_ms:
      String.to_integer(System.get_env("PRISM_NETWORK_ERROR_BASE_DELAY_MS") || "1000"),
    network_error_max_retries:
      String.to_integer(System.get_env("PRISM_NETWORK_ERROR_MAX_RETRIES") || "5"),
    message_not_found_max_retries:
      String.to_integer(System.get_env("PRISM_MESSAGE_NOT_FOUND_MAX_RETRIES") || "5"),
    rate_limit_defer_threshold_ms:
      String.to_integer(System.get_env("PRISM_RATE_LIMIT_DEFER_THRESHOLD_MS") || "10000"),
    checkpoint_ttl_seconds:
      String.to_integer(System.get_env("PRISM_CHECKPOINT_TTL_SECONDS") || "86400"),
    dead_message_cache_enabled:
      parse_bool.(System.get_env("PRISM_DEAD_MESSAGE_CACHE_ENABLED") || "true"),
    key_expansion_enabled: parse_bool.(System.get_env("PRISM_KEY_EXPANSION_ENABLED") || "true"),
    cancel_checker_enabled: parse_bool.(System.get_env("PRISM_CANCEL_CHECKER_ENABLED") || "true"),
    stream_trimmer_enabled: parse_bool.(System.get_env("PRISM_STREAM_TRIMMER_ENABLED") || "true"),
    dead_message_cache_prefix: System.get_env("PRISM_DEAD_MESSAGE_CACHE_PREFIX") || "prism:dead:",
    dead_message_cache_ttl:
      String.to_integer(System.get_env("PRISM_DEAD_MESSAGE_CACHE_TTL") || "1800"),
    cancel_prefix: System.get_env("PRISM_CANCEL_PREFIX") || "prism:cancel:",
    stream_trim_interval_ms:
      String.to_integer(System.get_env("PRISM_STREAM_TRIM_INTERVAL_MS") || "30000"),
    callback_include_parent_message_id:
      parse_bool.(System.get_env("PRISM_INCLUDE_PARENT_MESSAGE_ID")),
    reply_index_enabled: parse_bool.(System.get_env("PRISM_REPLY_INDEX_ENABLED")),
    reply_index_prefix: System.get_env("PRISM_REPLY_INDEX_PREFIX") || "prism",
    reply_index_ttl_seconds:
      String.to_integer(System.get_env("PRISM_REPLY_INDEX_TTL_SECONDS") || "604800"),
    cancel_ttl: String.to_integer(System.get_env("PRISM_CANCEL_TTL", "300")),
    cluster_topology: System.get_env("PRISM_CLUSTER_TOPOLOGY") || "prism_cluster",
    events_stream: System.get_env("EVENTS_STREAM") || "events.bus",
    events_dlq_stream: System.get_env("EVENTS_DLQ_STREAM") || "events.bus.dlq",
    events_stream_maxlen: String.to_integer(System.get_env("EVENTS_STREAM_MAXLEN") || "100000"),
    event_source: System.get_env("EVENT_SOURCE") || "/prism",
    event_bus_max_retries: String.to_integer(System.get_env("EVENT_BUS_MAX_RETRIES") || "3"),
    event_bus_retry_backoff_base_ms:
      String.to_integer(System.get_env("EVENT_BUS_RETRY_BACKOFF_BASE_MS") || "1000"),
    event_bus_retry_backoff_max_ms:
      String.to_integer(System.get_env("EVENT_BUS_RETRY_BACKOFF_MAX_MS") || "30000"),
    event_bus_consumer_batch_size:
      String.to_integer(System.get_env("EVENT_BUS_CONSUMER_BATCH_SIZE") || "10"),
    event_bus_consumer_block_ms:
      String.to_integer(System.get_env("EVENT_BUS_CONSUMER_BLOCK_MS") || "3000"),
    event_bus_stale_claim_idle_ms:
      String.to_integer(System.get_env("EVENT_BUS_STALE_CLAIM_IDLE_MS") || "30000"),
    event_bus_stale_claim_interval_ms:
      String.to_integer(System.get_env("EVENT_BUS_STALE_CLAIM_INTERVAL_MS") || "60000"),
    event_bus_broadcast_type: System.get_env("EVENT_BUS_BROADCAST_TYPE") || "prism.broadcast.completed",
    event_bus_callback_type: System.get_env("EVENT_BUS_CALLBACK_TYPE") || "prism.callback",
    kafka_brokers:
      (System.get_env("KAFKA_BROKERS") || "localhost:9092")
      |> String.split(",")
      |> Enum.map(&String.trim/1)
      |> Enum.map(fn broker ->
        case String.split(broker, ":") do
          [host, port] -> {String.to_charlist(host), String.to_integer(port)}
          [host] -> {String.to_charlist(host), 9092}
        end
      end)

  event_bus_transport =
    case System.get_env("EVENT_BUS_TRANSPORT") || "redis" do
      "redis" -> Prism.EventBus.Transport.Redis
      "kafka" -> Module.concat(["Prism.EventBus.Transport.Kafka"])
      other -> Module.concat([other])
    end

  config :prism, event_bus_transport_backend: event_bus_transport
end

config :opentelemetry,
  span_processor: :batch,
  traces_exporter: :otlp

config :opentelemetry_exporter,
  otlp_protocol: :http_protobuf,
  otlp_endpoint: System.get_env("OTEL_EXPORTER_OTLP_ENDPOINT") || "http://localhost:4318"
