defmodule Prism.DiscordWorker.Retry do
  @moduledoc """
  Retry spawning: encodes a retry payload and enqueues it to the delayed queue.
  """

  require Logger

  @doc """
  Encodes a retry payload and enqueues it to `Prism.DelayedQueue`.

  The delay is computed from `reason` and `attempt` count, with jitter baked in
  by the caller. This function handles the payload assembly and enqueue.
  """
  @spec spawn_retry(
          String.t(),
          map(),
          atom(),
          String.t(),
          keyword(),
          iodata() | nil,
          String.t(),
          String.t() | nil,
          String.t() | nil,
          integer(),
          integer(),
          String.t() | nil,
          atom()
        ) :: :ok | {:error, term()}
  def spawn_retry(
        action,
        target,
        method,
        url,
        headers,
        body,
        webhook_id,
        message_id,
        batch_id,
        delay_ms,
        attempt,
        parent_msg_id,
        reason
      ) do
    payload = %{
      "action" => action,
      "target" => target,
      "method" => to_string(method),
      "url" => url,
      "headers" => Enum.into(headers, %{}),
      "body" => if(is_nil(body), do: nil, else: IO.iodata_to_binary(body)),
      "webhook_id" => webhook_id,
      "message_id" => message_id,
      "batch_id" => batch_id,
      "attempt" => attempt,
      "parent_msg_id" => parent_msg_id,
      "reason" => to_string(reason)
    }

    Logger.info(
      "Scheduling retry for webhook_id=#{webhook_id} reason=#{reason} delay=#{delay_ms}ms attempt=#{attempt}"
    )

    Prism.DelayedQueue.enqueue(payload, delay_ms)
  end
end
