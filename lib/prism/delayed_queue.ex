defmodule Prism.DelayedQueue do
  @moduledoc """
  Manages the pure-Redis delayed retry queue using a ZSET.
  """
  require Logger

  @zset_key "discord:fanout:delayed"
  @stream_key "discord:fanout:stream:retries"
  @pubsub_channel "prism:wakeup"

  @doc """
  Enqueues a payload for delayed execution.
  `delay_ms` is the number of milliseconds to wait.
  """
  def enqueue(payload, delay_ms) when is_map(payload) and is_integer(delay_ms) do
    # Add a unique retry_id so payloads don't overwrite each other in the ZSET
    retry_id = :crypto.strong_rand_bytes(16) |> Base.encode16()
    payload_with_id = Map.put_new(payload, "retry_id", retry_id)
    json_payload = Jason.encode!(payload_with_id)
    execute_at_ms = :os.system_time(:millisecond) + delay_ms

    # Lua script: ZADD the item. If it is the ONLY item or it has the lowest score,
    # publish a wakeup event so the scheduler knows there's a sooner item.
    script = """
    local zset_key = KEYS[1]
    local pubsub_channel = KEYS[2]
    local score = tonumber(ARGV[1])
    local member = ARGV[2]

    redis.call("ZADD", zset_key, score, member)

    -- Check if it's the earliest item
    local earliest = redis.call("ZRANGE", zset_key, 0, 0, "WITHSCORES")
    if tonumber(earliest[2]) == tonumber(ARGV[1]) then
      redis.call("PUBLISH", pubsub_channel, "new_earliest:" .. score)
      return 1
    end
    return 0
    """

    idx = :erlang.phash2(System.unique_integer(), 5)

    case Redix.command(:"my_redix_#{idx}", [
           "EVAL",
           script,
           "2",
           @zset_key,
           @pubsub_channel,
           to_string(execute_at_ms),
           json_payload
         ]) do
      {:ok, 1} ->
        Logger.debug("Enqueued retry at #{execute_at_ms} (Earliest item)")
        :ok

      {:ok, 0} ->
        Logger.debug("Enqueued retry at #{execute_at_ms}")
        :ok

      {:error, reason} ->
        Logger.error("Failed to enqueue retry: #{inspect(reason)}")
        {:error, reason}
    end
  end

  @doc """
  Migrates items whose execute_at timestamp is <= now_ms from the ZSET to the retry stream.
  Returns the timestamp of the NEXT earliest item, or nil if empty.
  """
  def migrate_due_items(now_ms) do
    # Lua script: Get all items <= now_ms, ZREM them, and XADD them to the stream.
    # Then return the score of the *new* earliest item in the ZSET.
    script = """
    local zset_key = KEYS[1]
    local stream_key = KEYS[2]
    local now_ms = tonumber(ARGV[1])

    local due_items = redis.call("ZRANGEBYSCORE", zset_key, "-inf", now_ms)

    if #due_items > 0 then
      redis.call("ZREM", zset_key, unpack(due_items))
      
      for i, item in ipairs(due_items) do
        -- We encode it into the field 'payload' just like the main Broadway producer expects
        redis.call("XADD", stream_key, "*", "payload", item)
      end
    end

    local next_earliest = redis.call("ZRANGE", zset_key, 0, 0, "WITHSCORES")
    if #next_earliest > 0 then
      return next_earliest[2]
    else
      return nil
    end
    """

    idx = :erlang.phash2(System.unique_integer(), 5)

    case Redix.command(:"my_redix_#{idx}", [
           "EVAL",
           script,
           "2",
           @zset_key,
           @stream_key,
           to_string(now_ms)
         ]) do
      {:ok, nil} ->
        {:ok, nil}

      {:ok, next_score_str} when is_binary(next_score_str) ->
        case Float.parse(next_score_str) do
          {score, _} -> {:ok, trunc(score)}
          :error -> {:ok, nil}
        end

      {:ok, next_score} when is_integer(next_score) ->
        {:ok, next_score}

      {:error, reason} ->
        Logger.error("Failed to migrate due items: #{inspect(reason)}")
        {:error, reason}
    end
  end
end
