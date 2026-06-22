defmodule Prism.FanoutBroadway.SSE do
  @moduledoc """
  SSE / Dashboard publishing: broadcasts batch-completion events to Redis PubSub
  for real-time web UI streaming.

  This module is opinionated with InterChat-specific field names (authorId,
  guildId, badges, etc.) in the SSE payload. Gateway consumers should be
  aware of this schema. Set `PRISM_REDIS_SSE_ENABLED=false` to disable.
  """
  alias Prism.Helpers

  require Logger

  @doc """
  Publishes an SSE event for a completed batch when enabled and appropriate.

  Only publishes for `execute` actions with > 0 targets and shard_index 0.
  """
  @spec publish_sse_event(
          String.t(),
          [map()],
          integer(),
          map(),
          String.t() | nil,
          map(),
          String.t() | nil
        ) :: :ok
  def publish_sse_event(
        action,
        targets,
        shard_index,
        discord_payload,
        root_hub_id,
        payload_metadata,
        parent_message_id
      ) do
    if sse_eligible?(action, targets, shard_index) do
      first_target = hd(targets)
      hub_id = root_hub_id || Map.get(first_target, "hub_id")

      if hub_id do
        safe_metadata = payload_metadata || %{}

        stream_payload =
          %{
            content: Map.get(discord_payload, "content", ""),
            authorId: Map.get(safe_metadata, "author_id", ""),
            guildId: Map.get(safe_metadata, "guild_id", ""),
            authorName: Map.get(discord_payload, "username", "Unknown User"),
            guildName: Map.get(safe_metadata, "guild_name", "Unknown Server"),
            badges: Map.get(safe_metadata, "badges", []),
            createdAt: DateTime.utc_now() |> DateTime.to_iso8601(),
            id: parent_message_id || Map.get(discord_payload, "batch_id"),
            authorAvatarUrl: Map.get(discord_payload, "avatar_url", nil)
          }
          |> Jason.encode!()

        sse_topic_prefix = Prism.Config.sse_topic_prefix()

        case Helpers.redix_command([
               "PUBLISH",
               "#{sse_topic_prefix}#{hub_id}",
               stream_payload
             ]) do
          {:ok, _} -> Logger.debug("SSE Publish Success to #{sse_topic_prefix}#{hub_id}")
          {:error, reason} -> Logger.error("SSE Publish Failed: #{inspect(reason)}")
        end
      end
    end
  end

  defp sse_eligible?(action, targets, shard_index) do
    Prism.Config.sse_enabled?() and action == "execute" and length(targets) > 0 and
      shard_index == 0
  end
end
