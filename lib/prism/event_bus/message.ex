defmodule Prism.EventBus.Message do
  @moduledoc """
  Normalized message type returned by transport backends.

  Abstracts away broker-specific wire formats so consumers
  don't need to know whether messages came from Redis Streams,
  Kafka, or any other backend.
  """

  defstruct [:id, :stream, :data, headers: %{}]

  @type t :: %__MODULE__{
          id: String.t(),
          stream: String.t(),
          data: binary(),
          headers: map()
        }
end
