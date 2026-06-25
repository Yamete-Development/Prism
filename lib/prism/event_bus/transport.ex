defmodule Prism.EventBus.Transport do
  @moduledoc """
  Transport facade for the EventBus adapter.

  Delegates to the configured backend module (default: `Transport.Redis`).
  Set `EVENT_BUS_TRANSPORT` or the application env key
  `:prism, [:event_bus, :transport_backend]` to swap backends.

  ## Transport Contract

  The adapter requires these operations:
    - `publish/3` — write an event to a stream/topic
    - `create_consumer_group/2` — ensure a consumer group exists
    - `read_batch/5` — read a batch of pending messages as `%Message{}` structs
    - `ack/3` — acknowledge processed messages
    - `claim_stale/5` — recover messages from failed consumers as `%Message{}` structs
  """

  @doc """
  Publishes a JSON-encoded event to a stream with approximate length capping.
  """
  @spec publish(binary(), binary(), pos_integer(), map()) :: :ok | {:error, term()}
  def publish(stream, json_payload, maxlen, headers \\ %{}) do
    backend().publish(stream, json_payload, maxlen, headers)
  end

  @doc """
  Creates a consumer group on a stream, creating the stream if it does not exist.
  """
  @spec create_consumer_group(binary(), binary()) :: :ok | {:error, term()}
  def create_consumer_group(stream, consumer_group) do
    backend().create_consumer_group(stream, consumer_group)
  end

  @doc """
  Reads a batch of pending messages from a stream via a consumer group.

  Returns `{:ok, messages}` where messages is a list of `Prism.EventBus.Message` structs,
  or `{:ok, []}` when no messages are pending.
  """
  @spec read_batch(binary(), binary(), binary(), pos_integer(), pos_integer()) ::
          {:ok, list(Prism.EventBus.Message.t())} | {:ok, []} | {:error, term()}
  def read_batch(stream, consumer_group, consumer_name, block_ms, batch_size) do
    backend().read_batch(stream, consumer_group, consumer_name, block_ms, batch_size)
  end

  @doc """
  Acknowledges one or more message IDs in a consumer group.
  """
  @spec ack(binary(), binary(), [binary()]) :: :ok
  def ack(stream, consumer_group, ids) do
    backend().ack(stream, consumer_group, ids)
  end

  @doc """
  Recovers stale (unacknowledged) messages from other consumers in the group.

  Returns the claimed messages as a list of `Prism.EventBus.Message` structs,
  or an empty list.
  """
  @spec claim_stale(binary(), binary(), binary(), pos_integer(), pos_integer()) ::
          [Prism.EventBus.Message.t()]
  def claim_stale(stream, consumer_group, consumer_name, idle_ms, count) do
    backend().claim_stale(stream, consumer_group, consumer_name, idle_ms, count)
  end

  @doc """
  Returns the system name of the configured transport backend.
  """
  @spec system_name() :: String.t()
  def system_name do
    backend().system_name()
  end

  # ── Private ─────────────────────────────────────────────────────────────────

  defp backend do
    Prism.EventBus.Config.transport_backend()
  end
end
