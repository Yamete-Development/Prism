defmodule Prism.DiscordWorker.HTTP do
  @moduledoc """
  HTTP request building and execution for Discord webhook delivery.
  """
  alias Prism.Helpers

  require Logger
  require OpenTelemetry.Tracer

  @doc """
  Builds the HTTP request (method and URL) for a given action.
  """
  @spec build_request(String.t(), String.t(), String.t() | nil, String.t() | nil) ::
          {:ok, atom(), String.t()} | {:error, atom()}
  def build_request("execute", base_url, _msg_id, thread_id) do
    url = base_url <> "?wait=true&with_components=true"
    url = if is_binary(thread_id), do: url <> "&thread_id=#{thread_id}", else: url
    {:ok, :post, url}
  end

  def build_request("edit", base_url, msg_id, thread_id) when is_binary(msg_id) do
    url = base_url <> "/messages/#{msg_id}?with_components=true"
    url = if is_binary(thread_id), do: url <> "&thread_id=#{thread_id}", else: url
    {:ok, :patch, url}
  end

  def build_request("delete", base_url, msg_id, thread_id) when is_binary(msg_id) do
    url = base_url <> "/messages/#{msg_id}"
    url = if is_binary(thread_id), do: url <> "?thread_id=#{thread_id}", else: url
    {:ok, :delete, url}
  end

  def build_request(action, _base_url, _msg_id, _thread_id) do
    Logger.error("Invalid action or missing message_id for action: #{action}")
    {:error, :invalid_action}
  end

  @doc """
  Performs an HTTP request with OpenTelemetry tracing.
  """
  @spec do_http_request(
          atom(),
          String.t(),
          String.t(),
          keyword(),
          iodata() | nil,
          String.t(),
          String.t() | nil
        ) ::
          {:ok, String.t() | nil} | {:error, term()}
  def do_http_request(method, method_str, url, headers, body, webhook_id, message_id) do
    OpenTelemetry.Tracer.with_span "prism.worker.http_request" do
      OpenTelemetry.Tracer.set_attributes([
        {:http_method, to_string(method)},
        {:webhook_id, webhook_id}
      ])

      result =
        do_http_request_internal(method, method_str, url, headers, body, webhook_id, message_id)

      case result do
        {:ok, _} ->
          OpenTelemetry.Tracer.set_attribute(:http_success, true)

        {:error, {:rate_limited, _}} ->
          OpenTelemetry.Tracer.set_attribute(:error_type, "rate_limited")

        {:error, {:server_error, _}} ->
          OpenTelemetry.Tracer.set_attribute(:error_type, "server_error")

        {:error, :permanent} ->
          OpenTelemetry.Tracer.set_attribute(:error_type, "permanent")

        {:error, reason} ->
          OpenTelemetry.Tracer.set_attribute(:error_type, inspect(reason))
      end

      result
    end
  end

  defp do_http_request_internal(method, method_str, url, headers, body, webhook_id, message_id) do
    if Helpers.empty_discord_payload?(body) do
      Logger.warning(
        "Skipping webhook_id=#{webhook_id} method=#{method_str} — empty payload (no content, embeds, or components)"
      )

      {:error, :empty_payload}
    else
      receive_timeout = Prism.Config.finch_receive_timeout_ms()
      pool_timeout = Prism.Config.finch_pool_timeout_ms()

      case Finch.build(method, url, headers, body)
           |> Finch.request(DiscordFinch,
             receive_timeout: receive_timeout,
             pool_timeout: pool_timeout
           ) do
        {:ok, %{status: status, body: resp_body, headers: resp_headers}}
        when status in 200..299 ->
          Prism.RateLimit.handle_response(webhook_id, method_str, status, resp_headers, resp_body)

          if method == :post do
            case Jason.decode(resp_body) do
              {:ok, %{"id" => msg_id}} ->
                Prism.RateLimit.record_success()
                {:ok, msg_id}

              {:ok, parsed} ->
                Logger.warning(
                  "Webhook #{webhook_id} returned #{status} but no 'id' in body: #{inspect(parsed)}"
                )

                {:ok, nil}

              {:error, decode_err} ->
                Logger.warning(
                  "Webhook #{webhook_id} returned #{status} but body is not valid JSON: #{inspect(decode_err)}"
                )

                {:ok, nil}
            end
          else
            {:ok, nil}
          end

        {:ok, %{status: 429, body: resp_body, headers: resp_headers}} ->
          {:error, parsed} =
            Prism.RateLimit.handle_response(webhook_id, method_str, 429, resp_headers, resp_body)

          if parsed.is_cloudflare do
            Logger.error(
              "Cloudflare IP-level block (429) on webhook_id=#{webhook_id}! " <>
                "Delay: #{parsed.retry_after_ms}ms | Headers: #{inspect(resp_headers)} | Body: #{resp_body}"
            )
          else
            Logger.warning(
              "Discord Rate Limited (429) on webhook_id=#{webhook_id}! " <>
                "Method: #{method_str} | " <>
                "Delay: #{parsed.retry_after_ms}ms | " <>
                "Scope: #{parsed.scope || "unknown"} | Bucket: #{parsed.bucket || "unknown"} | " <>
                "Global: #{parsed.is_global} | " <>
                "Request Body: #{body} | " <>
                "Response Body: #{resp_body} | " <>
                "Response Headers: #{inspect(resp_headers)}"
            )
          end

          {:error, {:rate_limited, parsed.retry_after_ms}}

        {:ok, %{status: status, body: resp_body, headers: resp_headers}}
        when status in [401, 403] ->
          is_cf = Prism.RateLimit.Headers.cloudflare_response?(resp_headers, resp_body)

          Logger.warning(
            "Permanent error #{status} for webhook_id=#{webhook_id} – token invalid or missing permissions. " <>
              "Cloudflare: #{is_cf} | Headers: #{inspect(resp_headers)} | Body: #{resp_body}"
          )

          {:error, :permanent}

        {:ok, %{status: 400, body: resp_body}} ->
          Logger.error(
            "Bad request webhook_id=#{webhook_id} body=#{resp_body} – sending to DLQ and dropping permanently."
          )

          Prism.Helpers.redix_command([
            "XADD",
            "prism.dlq.bad_requests",
            "MAXLEN",
            "~",
            "10000",
            "*",
            "webhook_id",
            webhook_id || "",
            "url",
            url || "",
            "method",
            method_str || "",
            "request_body",
            body || "",
            "response_body",
            resp_body || ""
          ])

          {:error, :permanent}

        {:ok, %{status: 404, body: resp_body}} ->
          case Jason.decode(resp_body) do
            {:ok, %{"code" => 10008}} ->
              if method == :delete do
                Logger.debug(
                  "Webhook_id=#{webhook_id} returned 10008 on delete. Message already deleted, treating as success."
                )

                if is_binary(message_id),
                  do: Prism.DiscordWorker.DeadMessage.cache_dead_message(webhook_id, message_id)

                Prism.RateLimit.InvalidRequestTracker.record_invalid()
                {:ok, nil}
              else
                Logger.info(
                  "Webhook_id=#{webhook_id} returned 10008 on #{method}. Target message not found (deleted)."
                )

                if is_binary(message_id),
                  do: Prism.DiscordWorker.DeadMessage.cache_dead_message(webhook_id, message_id)

                Prism.RateLimit.InvalidRequestTracker.record_invalid()
                {:error, :message_not_found}
              end

            {:ok, %{"code" => code}} when code in [10003, 10015] ->
              Logger.warning(
                "Dropping webhook_id=#{webhook_id} status=404 body=#{resp_body} (invalid webhook)"
              )

              Prism.RateLimit.InvalidRequestTracker.record_invalid()
              {:error, :invalid_webhook}

            _ ->
              Logger.warning(
                "Treating 404 as transient webhook_id=#{webhook_id} body=#{resp_body}"
              )

              Prism.RateLimit.InvalidRequestTracker.record_invalid()
              {:error, :network_error}
          end

        {:ok, %{status: status, body: resp_body}} when status in 500..599 ->
          Logger.error("Server error #{status} for webhook_id=#{webhook_id}, body=#{resp_body}")
          {:error, {:server_error, status}}

        {:error, reason} ->
          Logger.error("Network error for webhook_id=#{webhook_id}: #{inspect(reason)}")
          {:error, :network_error}

        {:ok, %{status: status, body: resp_body, headers: resp_headers}} ->
          Logger.warning(
            "Unexpected status #{status} for webhook_id=#{webhook_id}. " <>
              "Headers: #{inspect(resp_headers)} | Body: #{resp_body}"
          )

          {:ok, nil}
      end
    end
  end
end
