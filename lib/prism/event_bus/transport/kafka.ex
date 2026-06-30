defmodule Prism.EventBus.Transport.Kafka do
  @behaviour Prism.EventBus.Transport.Behaviour

  @impl true
  def publish(stream, payload, _maxlen, headers) do
    # Ensure a producer is started for this topic (idempotent; ignores {:error, {:already_started, _}})
    # We use required_acks: 1 to prevent request_timed_out if Kafka replicas are lagging or offline.
    _ = :brod.start_producer(:kafka_client, stream, [required_acks: 1])

    kafka_headers = Enum.map(headers, fn {k, v} -> {to_string(k), to_string(v)} end)
    batch = [%{key: "", value: payload, headers: kafka_headers}]

    case :brod.produce_sync(:kafka_client, stream, 0, "", batch) do
      :ok -> :ok
      {:error, reason} -> {:error, reason}
    end
  end

  @impl true
  def create_consumer_group(_stream, _consumer_group) do
    :ok
  end

  @impl true
  def read_batch(_stream, _consumer_group, _consumer_name, _block_ms, _batch_size) do
    {:ok, []}
  end

  @impl true
  def ack(_stream, _consumer_group, _ids) do
    :ok
  end

  @impl true
  def claim_stale(_stream, _consumer_group, _consumer_name, _idle_ms, _count) do
    []
  end

  @impl true
  def system_name() do
    "kafka"
  end
end
