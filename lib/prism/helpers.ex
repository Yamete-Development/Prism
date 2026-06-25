defmodule Prism.Helpers do
  @moduledoc """
  Shared utility functions consumed across the Prism codebase.
  """

  require Logger
  import Bitwise

  # ── Redis command helpers ──────────────────────────────────────────────

  @doc """
  Executes a Redis command against a randomly selected pool connection.
  Uses `Prism.Config.redix_pool_size/0` to determine the pool size.
  """
  @spec redix_command([term()]) :: {:ok, term()} | {:error, term()}
  def redix_command(command) do
    idx = :erlang.phash2(System.unique_integer(), Prism.Config.redix_pool_size())
    Redix.command(:"my_redix_#{idx}", command)
  end

  @doc """
  Executes a Redis pipeline against a randomly selected pool connection.
  """
  @spec redix_pipeline([[term()]]) :: {:ok, term()} | {:error, term()}
  def redix_pipeline(commands) do
    idx = :erlang.phash2(System.unique_integer(), Prism.Config.redix_pool_size())
    Redix.pipeline(:"my_redix_#{idx}", commands)
  end

  # ── Redis stream payload extraction ────────────────────────────────────

  @doc """
  Extracts the `payload` field from OffBroadwayRedisStream data.

  Handles both list format (`["payload", value]` chunks) and map format
  (`%{"payload" => value}`) for backward compatibility.
  """
  @spec get_payload_from_redis_data(list() | map()) :: String.t()
  def get_payload_from_redis_data(data) when is_list(data) do
    data
    |> Enum.chunk_every(2)
    |> Enum.find_value(fn
      ["payload", value] -> value
      _ -> nil
    end) || ""
  end

  def get_payload_from_redis_data(%{"payload" => payload}), do: payload
  def get_payload_from_redis_data(_), do: ""

  # ── Empty payload check ────────────────────────────────────────────────

  @doc """
  Returns `true` when the payload has no content, embeds, or components.

  Accepts both decoded maps and raw JSON binary/iodata.
  """
  @spec empty_discord_payload?(map() | binary() | iodata()) :: boolean()
  def empty_discord_payload?(nil), do: true

  def empty_discord_payload?(payload) when is_map(payload) do
    empty_payload_fields?(payload)
  end

  def empty_discord_payload?(body) do
    case Jason.decode(body) do
      {:ok, decoded} when is_map(decoded) ->
        empty_payload_fields?(decoded)

      _ ->
        false
    end
  end

  # ── Action/method helpers ──────────────────────────────────────────────

  @doc """
  Converts a Prism action string to an HTTP method string.
  """
  @spec action_to_method_string(String.t()) :: String.t()
  def action_to_method_string("execute"), do: "post"
  def action_to_method_string("edit"), do: "patch"
  def action_to_method_string("delete"), do: "delete"
  def action_to_method_string(_), do: "post"

  @doc """
  Safely converts a method string to an atom.

  Unknown methods log a warning and fall back to `:post`.
  """
  @spec safe_method_atom(String.t()) :: atom()
  def safe_method_atom("post"), do: :post
  def safe_method_atom("patch"), do: :patch
  def safe_method_atom("delete"), do: :delete

  def safe_method_atom(other) do
    Logger.warning("Unknown HTTP method in payload: #{inspect(other)}, defaulting to :post")

    :post
  end

  # ── Checkpoint helpers ──────────────────────────────────────────────

  @doc """
  Builds a Redis checkpoint key from batch metadata.
   Format: `prism:ck:<action>:<batch_id>:<webhook_id>`
  """
  @spec checkpoint_key(String.t(), String.t(), String.t()) :: String.t()
  def checkpoint_key(action, batch_id, webhook_id),
    do: "prism:ck:#{action}:#{batch_id}:#{webhook_id}"

  # ── Shared backpressure re-enqueue ────────────────────────────────────

  @doc """
  Logs a backpressure re-enqueue event and enqueues the payload to the delayed queue.
  """
  @spec re_enqueue_on_backpressure(map(), String.t(), integer()) :: :ok
  def re_enqueue_on_backpressure(payload, label, delay_ms) do
    batch_id = Map.get(payload, "batch_id", "unknown")
    action = Map.get(payload, "action", "execute")

    Logger.info(
      "[Backpressure#{label}] Active Cloudflare block (remaining: #{delay_ms}ms). " <>
        "Re-enqueueing #{action} batch #{batch_id} to delayed queue."
    )

    Prism.DelayedQueue.enqueue(payload, delay_ms)
  end

  # ── Stream timestamp extraction ────────────────────────────────────────

  @doc """
  Extracts the enqueue timestamp from a Redis stream message ID.
  Returns the timestamp as an integer, or the current time if parsing fails.
  """
  @spec extract_enqueued_at(String.t()) :: integer()
  def extract_enqueued_at(id) do
    case String.split(id, "-") do
      [timestamp_str, _] ->
        case Integer.parse(timestamp_str) do
          {ts, ""} -> ts
          _ -> :os.system_time(:millisecond)
        end

      _ ->
        :os.system_time(:millisecond)
    end
  end

  # ── Key existence check ─────────────────────────────────────────────────

  @doc """
  Checks whether a Redis key exists. Returns `true` or `false`, logging on error.
  """
  @spec key_exists?(String.t()) :: boolean()
  def key_exists?(redis_key) do
    case redix_command(["EXISTS", redis_key]) do
      {:ok, 1} ->
        true

      {:ok, 0} ->
        false

      {:error, reason} ->
        Logger.warning("Exists check failed for #{redis_key}: #{inspect(reason)}")
        false
    end
  end

  # ── Callback publishing ─────────────────────────────────────────────────

  @doc """
  Publishes a callback payload as a CloudEvent to the callback stream via EventBus.
  """
  @spec publish_callback(map()) :: :ok | {:error, term()}
  def publish_callback(payload_map) do
    events_stream = Prism.EventBus.Config.events_stream()
    type = Prism.EventBus.Config.callback_event_type()

    Prism.EventBus.publish(events_stream, type: type, data: payload_map)
  end

  # ── Overrides merge ─────────────────────────────────────────────────────

  @doc """
  Merges a target's `overrides` into the base content/body map.
  Returns the merged map, or nil if the target is a delete action.
  """
  @spec merge_overrides(map(), map(), String.t()) :: map() | nil
  def merge_overrides(content, target, action) do
    if action == "delete" do
      nil
    else
      Map.merge(content, Map.get(target, "overrides", %{}))
    end
  end

  # ── Private ────────────────────────────────────────────────────────────

  defp empty_payload_fields?(map) do
    content = Map.get(map, "content")
    embeds = Map.get(map, "embeds")
    components = Map.get(map, "components")

    (is_nil(content) or content == "") and
      is_nil_or_empty_list?(embeds) and
      is_nil_or_empty_list?(components)
  end

  defp is_nil_or_empty_list?(nil), do: true
  defp is_nil_or_empty_list?(list) when is_list(list), do: list == []
  defp is_nil_or_empty_list?(_), do: false

  # ── Protobuf Struct Decoder ──────────────────────────────────────────────

  @doc """
  Recursively converts a Google.Protobuf.Struct into a standard Elixir map.
  """
  def struct_to_map(%Google.Protobuf.Struct{fields: fields}) do
    Map.new(fields, fn {k, v} -> {k, value_to_elixir(v)} end)
  end
  def struct_to_map(nil), do: %{}

  defp value_to_elixir(%Google.Protobuf.Value{kind: {_, v}}) do
    case v do
      %Google.Protobuf.Struct{} = s -> struct_to_map(s)
      %Google.Protobuf.ListValue{values: list} -> Enum.map(list, &value_to_elixir/1)
      val -> val
    end
  end
  defp value_to_elixir(nil), do: nil

  # ── Confluent Protobuf Wire Format ───────────────────────────────────────

  @doc """
  Strips the message index array from a Confluent Protobuf binary payload.
  """
  def strip_confluent_message_indexes(binary) do
    {length_zigzag, rest} = decode_varint(binary)
    length = (length_zigzag >>> 1) ^^^ -(length_zigzag &&& 1)
    skip_varints(rest, length)
  end

  defp decode_varint(binary, acc \\ 0, shift \\ 0)
  defp decode_varint(<<0::1, val::7, rest::binary>>, acc, shift) do
    {acc ||| (val <<< shift), rest}
  end
  defp decode_varint(<<1::1, val::7, rest::binary>>, acc, shift) do
    decode_varint(rest, acc ||| (val <<< shift), shift + 7)
  end

  defp skip_varints(binary, 0), do: binary
  defp skip_varints(binary, count) do
    {_, rest} = decode_varint(binary)
    skip_varints(rest, count - 1)
  end
end
