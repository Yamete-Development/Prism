import Config

config :prism,
  discord_base_url: "http://localhost:4002",
  finch_pool_count: 10,
  finch_protocols: [:http1],
  backpressure_enabled: true,
  max_async_batches: 10,
  batch_max_concurrency: 5,
  callback_include_parent_message_id: false,
  reply_index_enabled: false,
  redis_opts: [host: "localhost", port: 6379],
  show_test_logs: System.get_env("PRISM_SHOW_LOGS") == "1"
