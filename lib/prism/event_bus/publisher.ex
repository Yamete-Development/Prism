defmodule Prism.EventBus.Publisher do
  @moduledoc """
  Builds CloudEvents v1.0 envelopes and publishes them to the event bus via Redis Streams.
  """

  require Logger
  require OpenTelemetry.Tracer

  alias Prism.EventBus.Transport

  @spec publish(binary(), map(), keyword()) :: :ok | {:error, term()}
  def publish(stream, data, opts \\ []) do
    type = Keyword.fetch!(opts, :type)
    source = Keyword.get(opts, :source, Prism.EventBus.Config.event_source())
    maxlen = Keyword.get(opts, :maxlen, Prism.EventBus.Config.events_stream_maxlen())

    cloud_event = build_envelope(type, source, data)

    json = Jason.encode!(cloud_event)

    OpenTelemetry.Tracer.with_span "eventbus.publish" do
      OpenTelemetry.Tracer.set_attributes([
        {:messaging_system, String.to_atom(Transport.system_name())},
        {:messaging_destination, stream},
        {:cloudevents_type, type},
        {:messaging_message_id, cloud_event["id"]}
      ])

      :telemetry.execute([:prism, :event_bus, :published], %{count: 1}, %{
        stream: stream,
        type: type
      })

      case Transport.publish(stream, json, maxlen) do
        {:ok, _id} -> :ok
        {:error, reason} -> {:error, reason}
      end
    end
  end

  @spec publish_cloud_event(binary(), map(), keyword()) :: :ok | {:error, term()}
  def publish_cloud_event(stream, cloud_event, opts \\ []) do
    maxlen = Keyword.get(opts, :maxlen, Prism.EventBus.Config.events_stream_maxlen())

    type = cloud_event["type"]
    id = cloud_event["id"]

    json = Jason.encode!(cloud_event)

    OpenTelemetry.Tracer.with_span "eventbus.publish" do
      OpenTelemetry.Tracer.set_attributes([
        {:messaging_system, String.to_atom(Transport.system_name())},
        {:messaging_destination, stream},
        {:cloudevents_type, type},
        {:messaging_message_id, id}
      ])

      :telemetry.execute([:prism, :event_bus, :published], %{count: 1}, %{
        stream: stream,
        type: type
      })

      case Transport.publish(stream, json, maxlen) do
        {:ok, _id} -> :ok
        {:error, reason} -> {:error, reason}
      end
    end
  end

  @spec build_envelope(binary(), binary(), map()) :: map()
  def build_envelope(type, source, data) do
    event_id = "evt_#{generate_id()}"
    time = DateTime.utc_now() |> DateTime.to_iso8601()

    trace_headers =
      :otel_propagator_text_map.inject([])
      |> Enum.into(%{})

    %{}
    |> Map.put("specversion", "1.0")
    |> Map.put("type", type)
    |> Map.put("source", source)
    |> Map.put("id", event_id)
    |> Map.put("time", time)
    |> Map.put("datacontenttype", "application/json")
    |> Map.put("data", data)
    |> Map.put("traceparent", Map.get(trace_headers, "traceparent"))
    |> Map.put("tracestate", Map.get(trace_headers, "tracestate"))
    |> then(fn env ->
      if is_nil(env["tracestate"]) do
        Map.delete(env, "tracestate")
      else
        env
      end
    end)
  end

  defp generate_id do
    :crypto.strong_rand_bytes(16) |> Base.encode16(case: :lower)
  end
end
