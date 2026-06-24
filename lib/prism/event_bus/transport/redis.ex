defmodule Prism.EventBus.Transport.Redis do
  @moduledoc """
  Redis Streams implementation of the `Transport.Behaviour`.

  Translates Redis Stream commands (XADD, XREADGROUP, XACK, XAUTOCLAIM)
  into the normalized transport contract.
  """

  @behaviour Prism.EventBus.Transport.Behaviour

  alias Prism.EventBus.Message

  require Logger

  @impl true
  def publish(stream, json_payload, maxlen) do
    redis_command([
      "XADD",
      stream,
      "MAXLEN",
      "~",
      to_string(maxlen),
      "*",
      "payload",
      json_payload
    ])
  end

  @impl true
  def create_consumer_group(stream, consumer_group) do
    redis_command([
      "XGROUP",
      "CREATE",
      stream,
      consumer_group,
      "0",
      "MKSTREAM"
    ])
  end

  @impl true
  def read_batch(stream, consumer_group, consumer_name, block_ms, batch_size) do
    cmd = [
      "XREADGROUP",
      "GROUP",
      consumer_group,
      consumer_name,
      "BLOCK",
      to_string(block_ms),
      "COUNT",
      to_string(batch_size),
      "STREAMS",
      stream,
      ">"
    ]

    redis_command(cmd)
    |> case do
      {:ok, [[^stream, messages]]} -> {:ok, to_messages(stream, messages)}
      {:ok, nil} -> {:ok, []}
      error -> error
    end
  end

  @impl true
  def ack(stream, consumer_group, ids) do
    cmd = ["XACK", stream, consumer_group] ++ ids

    case redis_command(cmd) do
      {:ok, _count} -> :ok
      {:error, _reason} -> :ok
    end
  end

  @impl true
  def claim_stale(stream, consumer_group, consumer_name, idle_ms, count) do
    case redis_command([
           "XAUTOCLAIM",
           stream,
           consumer_group,
           consumer_name,
           to_string(idle_ms),
           "0-0",
           "COUNT",
           to_string(count)
         ]) do
      {:ok, [^stream, messages]} when is_list(messages) -> to_messages(stream, messages)
      {:ok, [^stream, []]} -> []
      {:ok, nil} -> []
      {:error, _reason} -> []
    end
  end

  @impl true
  def system_name, do: "redis"

  # ── Private ─────────────────────────────────────────────────────────────────

  defp redis_command(command) do
    Prism.Helpers.redix_command(command)
  rescue
    e ->
      Logger.error("[EventBus.Transport.Redis] Redis error: #{Exception.message(e)}")
      {:error, e}
  end

  defp to_messages(stream, entries) when is_list(entries) do
    Enum.map(entries, fn [id, fields] ->
      payload = extract_payload(fields)

      %Message{
        id: id,
        stream: stream,
        data: payload || ""
      }
    end)
  end

  defp extract_payload(fields) when is_list(fields) do
    fields
    |> Enum.chunk_every(2)
    |> Enum.find_value(fn
      ["payload", value] -> value
      _ -> nil
    end)
  end
end
