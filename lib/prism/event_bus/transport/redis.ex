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
  def publish(stream, payload, maxlen, headers) do
    header_fields = Enum.flat_map(headers, fn {k, v} -> ["ce_#{k}", to_string(v)] end)
    fields = ["payload", payload | header_fields]

    redis_command([
      "XADD",
      stream,
      "MAXLEN",
      "~",
      to_string(maxlen),
      "*" | fields
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
      {payload, headers} = extract_payload_and_headers(fields)

      %Message{
        id: id,
        stream: stream,
        data: payload || "",
        headers: headers
      }
    end)
  end

  defp extract_payload_and_headers(fields) when is_list(fields) do
    fields
    |> Enum.chunk_every(2)
    |> Enum.reduce({nil, %{}}, fn
      ["payload", value], {_, headers} ->
        {value, headers}

      [<<"ce_", key::binary>>, value], {payload, headers} ->
        {payload, Map.put(headers, key, value)}

      [key, value], {payload, headers} ->
        {payload, Map.put(headers, key, value)}
    end)
  end
end
