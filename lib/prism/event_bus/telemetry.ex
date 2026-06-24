defmodule Prism.EventBus.Telemetry do
  @moduledoc """
  Telemetry instrumentation for EventBus operations.
  Provides :telemetry event emission and OpenTelemetry span management.
  """

  require Logger
  require OpenTelemetry.Tracer

  @doc """
  Emits a `[:prism, :event_bus, :consumed]` telemetry event.
  """
  @spec emit_consumed(binary(), binary(), binary()) :: :ok
  def emit_consumed(stream, consumer_group, type) do
    :telemetry.execute([:prism, :event_bus, :consumed], %{count: 1}, %{
      stream: stream,
      consumer_group: consumer_group,
      type: type
    })
  end

  @doc """
  Emits a `[:prism, :event_bus, :retries]` telemetry event.
  """
  @spec emit_retry(binary(), binary(), binary(), pos_integer()) :: :ok
  def emit_retry(stream, consumer_group, type, attempt) do
    :telemetry.execute([:prism, :event_bus, :retries], %{count: 1}, %{
      stream: stream,
      consumer_group: consumer_group,
      type: type,
      attempt: attempt
    })
  end

  @doc """
  Creates an OpenTelemetry span for a consumed event.

  Returns `{ctx, span_ctx}` for the attach/end pattern or `{nil, nil}` on error.
  """
  @spec span_consume(map(), binary(), binary()) :: {OpenTelemetry.Ctx.t(), OpenTelemetry.Span.span_ctx()}
  def span_consume(cloud_event, stream, consumer_group) do
    type = cloud_event["type"]
    event_id = cloud_event["id"]

    trace_headers =
      []
      |> maybe_add_header("traceparent", cloud_event["traceparent"])
      |> maybe_add_header("tracestate", cloud_event["tracestate"])

    parent_ctx = :otel_propagator_text_map.extract(trace_headers)

    s_ctx =
      OpenTelemetry.Tracer.start_span("eventbus.subscribe", %{
        attributes: [
          {:messaging_system, String.to_atom(Prism.EventBus.Transport.system_name())},
          {:messaging_destination, stream},
          {:cloudevents_type, type},
          {:messaging_message_id, event_id},
          {:messaging_consumer_group, consumer_group}
        ],
        kind: :consumer
      })

    OpenTelemetry.Ctx.attach(parent_ctx)

    {parent_ctx, s_ctx}
  end

  @doc """
  Creates an OpenTelemetry span for a retry attempt.
  """
  @spec span_retry(map(), binary(), binary(), pos_integer()) :: OpenTelemetry.Span.span_ctx()
  def span_retry(cloud_event, stream, consumer_group, attempt) do
    type = cloud_event["type"]
    event_id = cloud_event["id"]

    s_ctx =
      OpenTelemetry.Tracer.start_span("eventbus.retry", %{
        attributes: [
          {:messaging_system, String.to_atom(Prism.EventBus.Transport.system_name())},
          {:messaging_destination, stream},
          {:cloudevents_type, type},
          {:messaging_message_id, event_id},
          {:messaging_consumer_group, consumer_group},
          {:messaging_retry_attempt, attempt}
        ]
      })

    s_ctx
  end

  @doc """
  Creates an OpenTelemetry span for a DLQ publish.
  """
  @spec span_dlq(map(), binary(), binary(), term()) :: OpenTelemetry.Span.span_ctx()
  def span_dlq(cloud_event, stream, consumer_group, error) do
    type = cloud_event["type"]
    event_id = cloud_event["id"]

    s_ctx =
      OpenTelemetry.Tracer.start_span("eventbus.dlq", %{
        attributes: [
          {:messaging_system, String.to_atom(Prism.EventBus.Transport.system_name())},
          {:messaging_destination, stream},
          {:cloudevents_type, type},
          {:messaging_message_id, event_id},
          {:messaging_consumer_group, consumer_group},
          {:messaging_dlq_reason, format_error(error)}
        ]
      })

    s_ctx
  end

  defp format_error(error) when is_binary(error), do: error
  defp format_error(%{message: msg}), do: msg
  defp format_error(exception) when is_exception(exception), do: Exception.message(exception)
  defp format_error(_), do: "unknown"

  defp maybe_add_header(acc, _key, nil), do: acc
  defp maybe_add_header(acc, _key, ""), do: acc
  defp maybe_add_header(acc, key, value), do: [{key, value} | acc]
end
