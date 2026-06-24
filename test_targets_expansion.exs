defmodule KeyMapTest do
  @key_map %{
    "a" => "action",
    "b" => "batch_id",
    "t" => "targets",
    "c" => "channel_id"
  }

  def expand_keys(map) when is_map(map) do
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

payload_json = """
{"trace_headers":{},"a":"execute","t":[{"c":"123","w":"456"}],"b":"uuid123"}
"""
# Assuming Jason decodes to %{"trace_headers" => %{}, "a" => "execute", ...}
{:ok, payload} = Jason.decode(payload_json)
IO.inspect(KeyMapTest.expand_keys(payload))
