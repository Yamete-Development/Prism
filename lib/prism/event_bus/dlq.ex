defmodule Prism.EventBus.DLQ do
  @moduledoc """
  Dead-letter queue for failed event bus messages.

  When an event cannot be processed after all retries are exhausted,
  it is wrapped with failure metadata and published to the DLQ stream.
  """

  require Logger

  alias Prism.EventBus.Transport

  @doc """
  Publishes a failed CloudEvent to the dead-letter queue with failure metadata.

  Returns `:ok` or `{:error, reason}`.
  """
  @spec publish(map(), term(), integer(), binary(), keyword()) :: :ok | {:error, term()}
  def publish(cloud_event, error, attempts, consumer_group, opts \\ []) do
    dlq_stream = Keyword.get(opts, :dlq_stream, Prism.EventBus.Config.events_dlq_stream())
    maxlen = Keyword.get(opts, :maxlen, Prism.EventBus.Config.events_stream_maxlen())

    dlq_envelope = %{
      "original_event" => cloud_event,
      "error" => format_error(error),
      "failed_at" => DateTime.utc_now() |> DateTime.to_iso8601(),
      "attempts" => attempts,
      "consumer_group" => consumer_group
    }

    json = Jason.encode!(dlq_envelope)

    :telemetry.execute([:prism, :event_bus, :dlq], %{count: 1}, %{
      type: cloud_event["type"],
      consumer_group: consumer_group,
      error: format_error(error)
    })

    case Transport.publish(dlq_stream, json, maxlen) do
      :ok ->
        Logger.warning(
          "[EventBus.DLQ] Published event #{cloud_event["id"]} to #{dlq_stream} " <>
            "(type=#{cloud_event["type"]}, attempts=#{attempts}, error=#{format_error(error)})"
        )

        :ok

      {:ok, _id} ->
        Logger.warning(
          "[EventBus.DLQ] Published event #{cloud_event["id"]} to #{dlq_stream} " <>
            "(type=#{cloud_event["type"]}, attempts=#{attempts}, error=#{format_error(error)})"
        )

        :ok

      {:error, reason} ->
        Logger.error(
          "[EventBus.DLQ] Failed to publish event #{cloud_event["id"]} to DLQ: #{inspect(reason)}"
        )

        {:error, reason}
    end
  end

  defp format_error(error) when is_binary(error), do: error
  defp format_error(%{message: msg}), do: msg
  defp format_error(exception) when is_exception(exception), do: Exception.message(exception)
  defp format_error(term), do: inspect(term)
end
