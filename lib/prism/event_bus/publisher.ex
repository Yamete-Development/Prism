defmodule Prism.EventBus.Publisher do
  @moduledoc """
  Builds CloudEvents v1.0 envelopes and publishes them to the event bus via Redis Streams.
  """

  require OpenTelemetry.Tracer

  alias Prism.EventBus.Transport

  @spec publish(binary(), map(), keyword()) :: :ok | {:error, term()}
  def publish(stream, data, opts \\ []) do
    type = Keyword.fetch!(opts, :type)
    source = Keyword.get(opts, :source, Prism.EventBus.Config.event_source())
    maxlen = Keyword.get(opts, :maxlen, Prism.EventBus.Config.events_stream_maxlen())

    cloud_event = build_envelope(type, source, data)

    payload_json = Jason.encode!(cloud_event["data"])
    headers = extract_headers(cloud_event)

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

      case Transport.publish(stream, payload_json, maxlen, headers) do
        :ok -> :ok
        {:ok, _id} -> :ok
        {:error, reason} -> {:error, reason}
      end
    end
  end

  @spec publish_raw(binary(), binary(), keyword()) :: :ok | {:error, term()}
  def publish_raw(stream, data_bytes, opts \\ []) do
    maxlen = Keyword.get(opts, :maxlen, Prism.EventBus.Config.events_stream_maxlen())
    headers = Keyword.get(opts, :headers, %{})
    key = Keyword.get(opts, :key)
    headers = if key, do: Map.put(headers, "partition-key", key), else: headers

    OpenTelemetry.Tracer.with_span "eventbus.publish_raw" do
      OpenTelemetry.Tracer.set_attributes([
        {:messaging_system, String.to_atom(Transport.system_name())},
        {:messaging_destination, stream}
      ])

      case Transport.publish(stream, data_bytes, maxlen, headers) do
        :ok -> :ok
        {:ok, _id} -> :ok
        {:error, reason} -> {:error, reason}
      end
    end
  end

  @spec publish_protobuf(binary(), struct(), keyword()) :: :ok | {:error, term()}
  def publish_protobuf(stream, message, opts) do
    type = Keyword.fetch!(opts, :type)
    key = Keyword.fetch!(opts, :key)
    source = Keyword.get(opts, :source, Prism.EventBus.Config.event_source())
    maxlen = Keyword.get(opts, :maxlen, Prism.EventBus.Config.events_stream_maxlen())
    module = message.__struct__
    {iodata, _size} = module.encode!(message)
    payload = IO.iodata_to_binary(iodata)
    trace_headers = :otel_propagator_text_map.inject([]) |> Enum.into(%{})

    headers = %{
      "ce_specversion" => "1.0",
      "ce_type" => type,
      "ce_source" => source,
      "ce_id" => "evt_#{generate_id()}",
      "ce_time" => DateTime.utc_now() |> DateTime.to_iso8601(),
      "ce_datacontenttype" => "application/protobuf",
      "content-type" => "application/protobuf",
      "partition-key" => key
    }

    headers =
      headers
      |> maybe_put_header("ce_traceparent", Map.get(trace_headers, "traceparent"))
      |> maybe_put_header("ce_tracestate", Map.get(trace_headers, "tracestate"))

    Transport.publish(stream, payload, maxlen, headers)
  end

  @spec publish_cloud_event(binary(), map(), keyword()) :: :ok | {:error, term()}
  def publish_cloud_event(stream, cloud_event, opts \\ []) do
    maxlen = Keyword.get(opts, :maxlen, Prism.EventBus.Config.events_stream_maxlen())

    type = cloud_event["type"]
    id = cloud_event["id"]

    payload_json = Jason.encode!(cloud_event["data"])
    headers = extract_headers(cloud_event)

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

      case Transport.publish(stream, payload_json, maxlen, headers) do
        :ok -> :ok
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

  defp maybe_put_header(headers, _key, nil), do: headers
  defp maybe_put_header(headers, key, value), do: Map.put(headers, key, value)

  defp extract_headers(cloud_event) do
    cloud_event
    |> Map.delete("data")
    |> Enum.map(fn {k, v} -> {"ce_#{k}", v} end)
    |> Enum.into(%{})
  end
end
