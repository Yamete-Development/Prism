defmodule Prism.FanoutBroadway.KeyExpansion do
  @moduledoc """
  Key expansion: reverses JSON key minification applied by the publisher.

  When `Prism.Config.key_expansion_enabled?/0` is `true`, recursively maps
  known short keys to their full names. Keys not in `@key_map` pass through
  unchanged for backward compatibility with publishers that already use
  long-key format.

  The key_map mapping is InterChat-specific by default. Users who do not use
  key minification can set `PRISM_KEY_EXPANSION_ENABLED=false` to skip this
  step entirely.
  """

  @key_map %{
    "a" => "action",
    "b" => "batch_id",
    "m" => "message_id",
    "s" => "shard_index",
    "h" => "hub_id",
    "n" => "hub_name",
    "p" => "payload",
    "t" => "targets",
    "d" => "metadata",
    "r" => "trace_headers",
    "c" => "channel_id",
    "w" => "webhook_id",
    "k" => "webhook_token",
    "g" => "guild_id",
    "f" => "thread_id",
    "o" => "overrides",
    "ci" => "connection_id",
    "u" => "username",
    "v" => "avatar_url",
    "x" => "content",
    "e" => "embeds",
    "q" => "components",
    "l" => "allowed_mentions",
    "fl" => "flags",
    "ai" => "author_id",
    "gn" => "guild_name",
    "bg" => "badges"
  }

  @doc """
  Expands minified JSON keys back to their full names.

  If key expansion is disabled via config, returns the input unchanged.
  If the payload is already in long-key format, returns it unchanged.
  """
  @spec expand_keys(any()) :: any()
  def expand_keys(map) when is_map(map) do
    if Prism.Config.key_expansion_enabled?() do
      first_key =
        map
        |> Map.keys()
        |> Enum.find(fn _ -> true end)

      is_minified = first_key && Map.has_key?(@key_map, first_key)

      if is_minified do
        do_expand_keys(map)
      else
        map
      end
    else
      map
    end
  end

  def expand_keys(value), do: value

  defp do_expand_keys(map) when is_map(map) do
    map
    |> Enum.reduce(%{}, fn {key, value}, acc ->
      long_key = Map.get(@key_map, key, key)

      expanded_value =
        cond do
          is_map(value) ->
            do_expand_keys(value)

          is_list(value) ->
            Enum.map(value, fn item -> if is_map(item), do: do_expand_keys(item), else: item end)

          true ->
            value
        end

      Map.put(acc, long_key, expanded_value)
    end)
  end
end
