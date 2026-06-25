defmodule Prism.EventBus.Transport.Behaviour do
  @moduledoc """
  Transport backend behaviour for the EventBus adapter.

  Implement this behaviour to support a new message broker backend
  (e.g., Kafka, NATS, RabbitMQ) behind the same EventBus API.

  ## Required operations

    - `publish/3` — write a payload to a stream/topic
    - `create_consumer_group/2` — ensure a consumer group exists
    - `read_batch/5` — read a batch of pending messages
    - `ack/3` — acknowledge processed messages
    - `claim_stale/5` — recover messages from failed consumers
  """

  @doc """
  Publishes a payload to a stream/topic.

  Returns `:ok` on success or `{:error, reason}` on failure.
  """
  @callback publish(stream :: String.t(), payload :: binary(), maxlen :: pos_integer(), headers :: map()) ::
              :ok | {:error, term()}

  @doc """
  Ensures a consumer group exists on the stream/topic, creating
  the underlying resource if necessary.

  Returns `:ok` on success or `{:error, reason}` on failure.
  """
  @callback create_consumer_group(stream :: String.t(), consumer_group :: String.t()) ::
              :ok | {:error, term()}

  @doc """
  Reads a batch of pending messages from the stream/topic via a consumer group.

  Returns `{:ok, messages}` where messages is a list of `Prism.EventBus.Message` structs,
  or `{:ok, []}` when no messages are pending.
  """
  @callback read_batch(
              stream :: String.t(),
              consumer_group :: String.t(),
              consumer_name :: String.t(),
              block_ms :: pos_integer(),
              batch_size :: pos_integer()
            ) :: {:ok, [Prism.EventBus.Message.t()]} | {:ok, []} | {:error, term()}

  @doc """
  Acknowledges one or more processed message IDs in a consumer group.

  Returns `:ok`.
  """
  @callback ack(stream :: String.t(), consumer_group :: String.t(), ids :: [String.t()]) :: :ok

  @doc """
  Recovers stale (unacknowledged) messages from other consumers in the group.

  Returns a list of `Prism.EventBus.Message` structs, or an empty list.
  For backends that handle stale recovery differently (e.g., Kafka rebalancing),
  this may be a no-op returning `[]`.
  """
  @callback claim_stale(
              stream :: String.t(),
              consumer_group :: String.t(),
              consumer_name :: String.t(),
              idle_ms :: pos_integer(),
              count :: pos_integer()
            ) :: [Prism.EventBus.Message.t()]

  @doc """
  Returns the system name of the transport backend (e.g. `"redis"`, `"kafka"`).

  Used for OpenTelemetry `messaging.system` attribute.
  """
  @callback system_name() :: String.t()
end
