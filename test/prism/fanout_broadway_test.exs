defmodule Prism.FanoutBroadwayTest do
  use ExUnit.Case, async: true
  alias Prism.FanoutBroadway

  describe "expand_keys/1" do
    test "recursively expands minified payloads including nested layouts and components" do
      payload = %{
        "a" => "execute",
        "p" => %{
          "q" => [
            %{
              "type" => 17,
              "accent_color" => 5_793_266,
              "q" => [%{"type" => 10, "x" => "hello"}]
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
              "accent_color" => 5_793_266,
              "components" => [%{"type" => 10, "content" => "hello"}]
            }
          ]
        }
      }

      assert FanoutBroadway.expand_keys(payload) == expected
    end

    test "passes already-expanded payloads through unchanged" do
      payload = %{
        "action" => "execute",
        "payload" => %{
          "components" => [
            %{
              "type" => 17,
              "components" => [%{"type" => 10, "content" => "hello"}]
            }
          ]
        }
      }

      assert FanoutBroadway.expand_keys(payload) == payload
    end

    test "expands all top-level minified keys" do
      payload = %{
        "a" => "execute",
        "b" => "batch-123",
        "m" => "msg-456",
        "s" => 0,
        "h" => "hub-789",
        "n" => "My Hub",
        "d" => %{"ai" => "author1", "gn" => "My Guild"}
      }

      expanded = FanoutBroadway.expand_keys(payload)
      assert expanded["action"] == "execute"
      assert expanded["batch_id"] == "batch-123"
      assert expanded["message_id"] == "msg-456"
      assert expanded["shard_index"] == 0
      assert expanded["hub_id"] == "hub-789"
      assert expanded["hub_name"] == "My Hub"
      assert expanded["metadata"]["author_id"] == "author1"
      assert expanded["metadata"]["guild_name"] == "My Guild"
    end

    test "expands target list entries" do
      payload = %{
        "a" => "execute",
        "t" => [
          %{"c" => "ch1", "w" => "wh1", "k" => "tok1", "g" => "g1"},
          %{"c" => "ch2", "w" => "wh2", "k" => "tok2"}
        ]
      }

      expanded = FanoutBroadway.expand_keys(payload)
      targets = expanded["targets"]
      assert length(targets) == 2
      assert Enum.at(targets, 0)["channel_id"] == "ch1"
      assert Enum.at(targets, 0)["webhook_id"] == "wh1"
      assert Enum.at(targets, 0)["webhook_token"] == "tok1"
      assert Enum.at(targets, 0)["guild_id"] == "g1"
      assert Enum.at(targets, 1)["channel_id"] == "ch2"
      assert Enum.at(targets, 1)["webhook_id"] == "wh2"
      assert Enum.at(targets, 1)["webhook_token"] == "tok2"
    end

    test "unknown keys pass through unchanged" do
      payload = %{
        "a" => "execute",
        "custom_field" => "value",
        "p" => %{"unknown_nested" => "stuff"}
      }

      expanded = FanoutBroadway.expand_keys(payload)
      assert expanded["action"] == "execute"
      assert expanded["custom_field"] == "value"
      assert expanded["payload"]["unknown_nested"] == "stuff"
    end

    test "empty map returns empty map" do
      assert FanoutBroadway.expand_keys(%{}) == %{}
    end

    test "non-map values pass through unchanged" do
      assert FanoutBroadway.expand_keys("string") == "string"
      assert FanoutBroadway.expand_keys(42) == 42
      assert FanoutBroadway.expand_keys(nil) == nil
      assert FanoutBroadway.expand_keys([1, 2, 3]) == [1, 2, 3]
    end

    test "already-expanded keys are not re-expanded" do
      payload = %{
        "action" => "execute",
        "batch_id" => "123",
        "targets" => [%{"channel_id" => "ch1"}]
      }

      assert FanoutBroadway.expand_keys(payload) == payload
    end

    test "all short-key abbreviations are mapped correctly" do
      payload = %{
        "a" => "execute",
        "b" => "b1",
        "m" => "m1",
        "s" => 0,
        "h" => "h1",
        "n" => "n1",
        "p" => %{
          "u" => "user1",
          "v" => "https://example.com",
          "x" => "hello world",
          "e" => [],
          "q" => [],
          "l" => %{},
          "fl" => 0
        },
        "t" => [
          %{
            "c" => "ch1",
            "w" => "wh1",
            "k" => "tok1",
            "g" => "g1",
            "f" => "thread1",
            "o" => %{"x" => "override content"},
            "ci" => "conn1"
          }
        ],
        "d" => %{"ai" => "author1", "gn" => "Guild Name", "bg" => ["badge1"]},
        "r" => %{}
      }

      expanded = FanoutBroadway.expand_keys(payload)
      assert expanded["action"] == "execute"
      assert expanded["payload"]["username"] == "user1"
      assert expanded["payload"]["content"] == "hello world"
      assert expanded["payload"]["embeds"] == []
      assert expanded["payload"]["components"] == []
      target = hd(expanded["targets"])
      assert target["channel_id"] == "ch1"
      assert target["webhook_id"] == "wh1"
      assert target["webhook_token"] == "tok1"
      assert target["guild_id"] == "g1"
      assert target["thread_id"] == "thread1"
      assert target["overrides"]["content"] == "override content"
      assert target["connection_id"] == "conn1"
      assert expanded["metadata"]["author_id"] == "author1"
      assert expanded["metadata"]["guild_name"] == "Guild Name"
      assert expanded["metadata"]["badges"] == ["badge1"]
    end
  end
end
