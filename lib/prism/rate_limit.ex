defmodule Prism.RateLimit do
  @moduledoc """
  Thin facade for rate-limit operations.

  Aggregates `Prism.RateLimit.Bucket`, `Prism.RateLimit.Backpressure`, and
  `Prism.RateLimit.Headers` so callers (`DiscordWorker`, `FanoutBroadway`,
  `RetryBroadway`) import one module.
  """

  alias Prism.RateLimit.{Bucket, Backpressure, Headers}

  @doc """
  Pre-flight check: atomically tests whether a webhook+method request is
  allowed by the rate-limit bucket.

  Delegates to `Bucket.acquire/2`.
  """
  @spec check(webhook_id :: String.t(), method :: String.t()) ::
          {:ok, remaining :: integer()} | {:blocked, ttl_ms :: integer()}
  def check(webhook_id, method) do
    Bucket.acquire(webhook_id, method)
  end

  @doc """
  Processes an HTTP response to update rate-limit bucket state.

  - **2xx**: extracts `x-ratelimit-*` headers and updates the per-webhook bucket.
  - **429**: parses the body + headers, then updates the bucket (Discord) or
    backpressure state (Cloudflare). Returns `{:error, parsed}` with the
    parsed 429 fields so the caller can extract `retry_after_ms` for retry
    scheduling and logging.
  - **Other statuses**: no-op, returns `:ok`.

  `now_ms` is computed internally via `System.monotonic_time(:millisecond)`.
  """
  @spec handle_response(
          webhook_id :: String.t(),
          method_str :: String.t(),
          status :: integer(),
          headers :: keyword(),
          body :: String.t()
        ) :: :ok | {:error, map()}
  def handle_response(webhook_id, method_str, status, headers, body)

  def handle_response(webhook_id, method_str, status, headers, _body)
      when status in 200..299 do
    case Headers.parse_2xx(headers) do
      {limit, remaining, reset_at_ms} ->
        Bucket.update(webhook_id, method_str, limit, remaining, reset_at_ms)

      nil ->
        :ok
    end

    :ok
  end

  def handle_response(webhook_id, method_str, 429, headers, body) do
    parsed = Headers.parse_429(headers, body)

    if parsed.is_cloudflare do
      Backpressure.record_cloudflare_block(parsed.retry_after_ms)
    else
      if parsed.is_global do
        Bucket.update_global(parsed.limit, 0, parsed.reset_at_ms)
      else
        Bucket.update(webhook_id, method_str, parsed.limit, 0, parsed.reset_at_ms)
      end
    end

    {:error, parsed}
  end

  def handle_response(_webhook_id, _method_str, _status, _headers, _body) do
    :ok
  end

  @doc """
  Returns `true` when this worker is under a Cloudflare IP-level block and
  should avoid making any outbound HTTP requests.
  """
  @spec unhealthy?() :: boolean()
  def unhealthy?, do: Backpressure.unhealthy?()

  @doc """
  Returns the remaining backoff duration in milliseconds.

  Returns `0` when the worker is healthy (no active Cloudflare block).
  """
  @spec backoff_ms() :: non_neg_integer()
  def backoff_ms, do: Backpressure.backoff_ms()

  @doc """
  Resets Cloudflare backpressure after a successful delivery.

  Called by `DiscordWorker` when a 2xx response confirms the worker is
  no longer blocked.
  """
  @spec record_success() :: :ok
  def record_success, do: Backpressure.record_success()
end
