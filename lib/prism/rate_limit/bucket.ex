defmodule Prism.RateLimit.Bucket do
  @moduledoc """
  Per-worker rate-limit bucket backed by a Redis hash.

  Replaces the binary `rl:*` TTL keys with a counter-based model that tracks
  `limit`, `remaining`, `reset_at`, and `bucket` per webhook+method combination.
  Uses Redis Lua scripting for atomic check-and-decrement operations.

  Keys are scoped by the runtime `:persistent_term.get(:prism_worker_id, "default")`
  value set in `Prism.Application.start/2`.  Prism sends webhook requests without
  an `Authorization` header, so Discord applies `X-RateLimit-Scope: user` and
  global rate limits to the worker's IP address, *not* to the webhook token.
  Sharing state across workers on different IPs would cause one worker's 429 to
  falsely block another worker that still has a clean bucket.
  """

  require Logger

  alias Prism.Helpers

  @acquire_script """
  local now = tonumber(ARGV[1])

  -- Check global rate limit first
  local global_data = redis.call("HMGET", KEYS[2], "limit", "remaining", "reset_at")
  local g_limit = tonumber(global_data[1])
  local g_remaining = tonumber(global_data[2])
  local g_reset_at = tonumber(global_data[3])

  if g_limit then
    if g_reset_at and g_reset_at <= now then
      redis.call("DEL", KEYS[2])
    elseif g_remaining and g_remaining <= 0 then
      return {0, 0, g_reset_at - now}
    end
  end

  -- Check webhook-specific rate limit
  local data = redis.call("HMGET", KEYS[1], "limit", "remaining", "reset_at")
  local limit = tonumber(data[1])
  local remaining = tonumber(data[2])
  local reset_at = tonumber(data[3])

  -- No state yet: allow. The response will populate the bucket.
  if not limit then
    return {1, -1, 0}
  end

  -- Window expired: clear stale state, allow. Fresh state arrives with the response.
  if reset_at and reset_at <= now then
    redis.call("DEL", KEYS[1])
    return {1, -1, 0}
  end

  -- Requests still available: atomically decrement and allow.
  if remaining and remaining > 0 then
    redis.call("HSET", KEYS[1], "remaining", remaining - 1)
    return {1, remaining - 1, 0}
  end

  -- Bucket exhausted. Return remaining TTL so the caller can sleep/defer.
  return {0, 0, reset_at - now}
  """

  @update_script """
  redis.call("HSET", KEYS[1],
    "limit", ARGV[1],
    "remaining", ARGV[2],
    "reset_at", ARGV[3],
    "bucket", ARGV[4])
  redis.call("EXPIRE", KEYS[1], ARGV[5])
  return "OK"
  """

  @doc """
  Atomically check and decrement the rate-limit bucket for a webhook+method.

  Returns `{:ok, remaining}` when the request is allowed (remaining is the
  count after decrement, or -1 if bucket state is unknown), or
  `{:blocked, ttl_ms}` when the bucket is exhausted and the window hasn't
  expired yet.
  """
  @spec acquire(webhook_id :: String.t(), method :: String.t()) ::
          {:ok, remaining :: integer()} | {:blocked, ttl_ms :: integer()}
  def acquire(webhook_id, method) do
    key = bucket_key(webhook_id, method)
    g_key = global_key()
    now_ms = System.monotonic_time(:millisecond)

    case Helpers.redix_command(["EVAL", @acquire_script, "2", key, g_key, to_string(now_ms)]) do
      {:ok, [1, remaining, _]} ->
        {:ok, remaining}

      {:ok, [0, _, ttl_ms]} ->
        {:blocked, ttl_ms}

      {:error, reason} ->
        Logger.error("Bucket.acquire failed for #{key}: #{inspect(reason)}")
        {:ok, -1}
    end
  end

  @doc """
  Update the bucket state after an HTTP response.

  Called on every response (2xx and 429). `limit` and `remaining` are parsed
  from the `x-ratelimit-*` headers. `reset_at_ms` is the absolute epoch
  timestamp (in milliseconds) when the rate-limit window resets.
  """
  @spec update(
          webhook_id :: String.t(),
          method :: String.t(),
          limit :: integer(),
          remaining :: integer(),
          reset_at_ms :: integer()
        ) :: :ok
  def update(webhook_id, method, limit, remaining, reset_at_ms)
      when is_integer(limit) and is_integer(remaining) and is_integer(reset_at_ms) do
    key = bucket_key(webhook_id, method)
    bucket = ""
    ttl = Prism.Config.bucket_hash_ttl_seconds()

    Helpers.redix_command([
      "EVAL",
      @update_script,
      "1",
      key,
      to_string(limit),
      to_string(remaining),
      to_string(reset_at_ms),
      bucket,
      to_string(ttl)
    ])
    |> case do
      {:ok, _} ->
        :ok

      {:error, reason} ->
        Logger.error("Bucket.update failed for #{key}: #{inspect(reason)}")
        :ok
    end
  end

  @doc """
  Update the bucket state for a global rate limit key.
  """
  @spec update_global(limit :: integer(), remaining :: integer(), reset_at_ms :: integer()) :: :ok
  def update_global(limit, remaining, reset_at_ms)
      when is_integer(limit) and is_integer(remaining) and is_integer(reset_at_ms) do
    key = global_key()
    bucket = ""
    ttl = Prism.Config.bucket_hash_ttl_seconds()

    Helpers.redix_command([
      "EVAL",
      @update_script,
      "1",
      key,
      to_string(limit),
      to_string(remaining),
      to_string(reset_at_ms),
      bucket,
      to_string(ttl)
    ])
    |> case do
      {:ok, _} ->
        :ok

      {:error, reason} ->
        Logger.error("Bucket.update_global failed: #{inspect(reason)}")
        :ok
    end
  end

  @doc false
  def bucket_key(webhook_id, method), do: "#{key_prefix()}:#{webhook_id}:#{method}"

  @doc false
  def global_key, do: "#{key_prefix()}:global"

  @doc false
  def acquire_script, do: @acquire_script

  @doc false
  def acquire_pipeline_commands(targets) when is_list(targets) do
    g_key = global_key()
    now_ms = System.monotonic_time(:millisecond)
    script = @acquire_script

    Enum.map(targets, fn {webhook_id, method_str} ->
      key = bucket_key(webhook_id, method_str)
      ["EVAL", script, "2", key, g_key, to_string(now_ms)]
    end)
  end

  defp key_prefix do
    worker_id = :persistent_term.get(:prism_worker_id, "default")
    "prism:rl:#{worker_id}"
  end
end
