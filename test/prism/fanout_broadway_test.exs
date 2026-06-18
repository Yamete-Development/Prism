defmodule Prism.FanoutBroadwayTest do
  use ExUnit.Case, async: true
  alias Prism.FanoutBroadway

  test "expand_keys/1 recursively expands minified payloads including nested layouts and components" do
    # A payload matching what the python client produces for layouts
    payload = %{
      "a" => "execute",
      "p" => %{
        "q" => [
          %{
            "type" => 17,
            "accent_color" => 5793266,
            "q" => [
              %{"type" => 10, "x" => "hello"}
            ]
          }
        ]
      }
    }

    expected = %{
      "action" => "execute",
      "payload" => %{
        "components" => [
          %{
            "type" => 17,
            "accent_color" => 5793266,
            "components" => [
              %{"type" => 10, "content" => "hello"}
            ]
          }
        ]
      }
    }

    assert FanoutBroadway.expand_keys(payload) == expected
  end

  test "expand_keys/1 passes already-expanded payloads through unchanged" do
    payload = %{
      "action" => "execute",
      "payload" => %{
        "components" => [
          %{
            "type" => 17,
            "components" => [
              %{"type" => 10, "content" => "hello"}
            ]
          }
        ]
      }
    }

    assert FanoutBroadway.expand_keys(payload) == payload
  end
end
