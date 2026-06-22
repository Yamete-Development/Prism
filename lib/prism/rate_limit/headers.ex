defmodule Prism.RateLimit.Headers do
  @moduledoc """
  HTTP header extraction and 429 response parsing for Discord rate limits.

  Extracted from `Prism.DiscordWorker` to consolidate rate-limit logic into the
  `Prism.RateLimit.*` namespace.
  """

  @doc """
  Extracts an integer value from response headers by case-insensitive name.
  Returns `nil` if the header is missing or not a valid integer.
  """
  @spec extract_int(headers :: keyword(), name :: String.t()) :: integer() | nil
  def extract_int(headers, name) do
    Enum.find_value(headers, fn {k, v} ->
      if String.downcase(k) == name do
        case Integer.parse(v) do
          {n, _} -> n
          :error -> nil
        end
      end
    end)
  end

  @doc """
  Extracts a float value from response headers by case-insensitive name.
  Returns `nil` if the header is missing or not a valid float.
  """
  @spec extract_float(headers :: keyword(), name :: String.t()) :: float() | nil
  def extract_float(headers, name) do
    Enum.find_value(headers, fn {k, v} ->
      if String.downcase(k) == name do
        case Float.parse(v) do
          {f, _} -> f
          :error -> nil
        end
      end
    end)
  end

  @doc """
  Returns the current monotonic time in milliseconds.

  Used by rate-limit computations to produce `reset_at` timestamps that are
  comparable with values read back via `acquire` (which also uses monotonic time).
  """
  @spec now_ms() :: integer()
  def now_ms, do: System.monotonic_time(:millisecond)

  @doc """
  Parses a Discord 2xx response's rate-limit headers into bucket update parameters.

  Returns `{limit, remaining, reset_at_ms}` when all three headers are present,
  or `nil` when any header is missing (the client does not update bucket state
  when Discord sends a partial response).
  """
  @spec parse_2xx(headers :: keyword()) ::
          {limit :: integer(), remaining :: integer(), reset_at_ms :: integer()} | nil
  def parse_2xx(headers) do
    limit = extract_int(headers, "x-ratelimit-limit")
    remaining = extract_int(headers, "x-ratelimit-remaining")
    reset_after_sec = extract_float(headers, "x-ratelimit-reset-after")

    if limit && remaining != nil && reset_after_sec do
      reset_at_ms = now_ms() + trunc(reset_after_sec * 1000)
      {limit, remaining, reset_at_ms}
    end
  end

  @doc """
  Parses a Discord 429 (Too Many Requests) response body and headers.

  Returns a map with the fields needed by `Prism.RateLimit` to update bucket
  state and by callers to log the rate-limit event.

  ## Cloudflare vs Discord

  Cloudflare 429 responses come in two forms:
  - Non-JSON (HTML) — always treated as Cloudflare
  - JSON with `"code": 0` — also a Cloudflare block (previously misclassified as `is_global`)

  Discord 429 responses are JSON with either `"global": true` or a `"message"`
  containing "global rate limits".
  """
  @spec parse_429(headers :: keyword(), body :: String.t()) :: %{
          retry_after_ms: integer(),
          is_cloudflare: boolean(),
          is_global: boolean(),
          limit: integer() | nil,
          remaining: integer() | nil,
          reset_at_ms: integer() | nil,
          bucket: String.t() | nil,
          scope: String.t() | nil
        }
  def parse_429(headers, body) do
    {retry_after_ms, is_cloudflare, is_global} =
      case Jason.decode(body) do
        {:ok, parsed} when is_map(parsed) ->
          retry_after_ms =
            case Map.get(parsed, "retry_after") do
              val when is_number(val) ->
                trunc(val * 1000)

              _ ->
                case extract_float(headers, "retry-after") do
                  val when is_number(val) -> trunc(val * 1000)
                  _ -> 5000
                end
            end

          is_cloudflare = Map.get(parsed, "code") == 0

          is_global =
            Map.get(parsed, "global", false) == true or
              String.contains?(
                String.downcase(Map.get(parsed, "message", "")),
                "global rate limits"
              )

          {retry_after_ms, is_cloudflare, is_global}

        _ ->
          cf_delay =
            case extract_float(headers, "retry-after") do
              val when is_number(val) -> trunc(val * 1000)
              _ -> 5000
            end

          {cf_delay, true, false}
      end

    bucket =
      Enum.find_value(headers, fn {k, v} ->
        if String.downcase(k) == "x-ratelimit-bucket", do: v
      end)

    scope =
      Enum.find_value(headers, fn {k, v} ->
        if String.downcase(k) == "x-ratelimit-scope", do: v
      end)

    global_header =
      Enum.find_value(headers, fn {k, v} ->
        if String.downcase(k) == "x-ratelimit-global", do: v
      end)

    is_global = is_global or global_header == "true" or scope == "global"

    limit = extract_int(headers, "x-ratelimit-limit") || 5

    # Use x-ratelimit-reset-after header for bucket state when available.
    # The body's retry_after (e.g. 300ms) is the minimum wait for THIS request,
    # but x-ratelimit-reset-after (e.g. 2s) is when the actual rate limit window
    # resets. Using the body value causes the pre-flight check to unlock too early.
    reset_after_header = extract_float(headers, "x-ratelimit-reset-after")

    reset_at_ms =
      if reset_after_header do
        now_ms() + trunc(reset_after_header * 1000)
      else
        now_ms() + retry_after_ms
      end

    %{
      retry_after_ms: retry_after_ms,
      is_cloudflare: is_cloudflare,
      is_global: is_global,
      limit: limit,
      remaining: 0,
      reset_at_ms: reset_at_ms,
      bucket: bucket,
      scope: scope
    }
  end

  @doc """
  Detects if a response is from Cloudflare by inspecting headers and body.
  """
  @spec cloudflare_response?(headers :: keyword() | map(), body :: String.t()) :: boolean()
  def cloudflare_response?(headers, body) do
    header_list = if is_map(headers), do: Map.to_list(headers), else: headers

    has_cf_header =
      Enum.any?(header_list, fn {k, v} ->
        k_lower = String.downcase(to_string(k))
        v_lower = String.downcase(to_string(v))
        k_lower == "cf-ray" or (k_lower == "server" and String.contains?(v_lower, "cloudflare"))
      end)

    has_cf_body = String.contains?(String.downcase(body), "cloudflare")

    has_cf_header or has_cf_body
  end
end
