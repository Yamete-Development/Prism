defmodule Prism.Helpers do
  @moduledoc """
  Shared utility functions consumed across the Prism codebase.
  """

  require Logger

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
    content = Map.get(payload, "content")
    embeds = Map.get(payload, "embeds")
    components = Map.get(payload, "components")

    (is_nil(content) or content == "") and
      is_nil_or_empty_list?(embeds) and
      is_nil_or_empty_list?(components)
  end

  def empty_discord_payload?(body) do
    case Jason.decode(body) do
      {:ok, decoded} when is_map(decoded) ->
        content = Map.get(decoded, "content")
        embeds = Map.get(decoded, "embeds")
        components = Map.get(decoded, "components")

        (is_nil(content) or content == "") and
          is_nil_or_empty_list?(embeds) and
          is_nil_or_empty_list?(components)

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

  # ── Private ────────────────────────────────────────────────────────────

  defp is_nil_or_empty_list?(nil), do: true
  defp is_nil_or_empty_list?(list) when is_list(list), do: list == []
  defp is_nil_or_empty_list?(_), do: false
end
