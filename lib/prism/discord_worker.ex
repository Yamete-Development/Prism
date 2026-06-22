defmodule Prism.DiscordWorker do
  require Logger
  require OpenTelemetry.Tracer

  @doc """
  Sends the webhook content to a guild's discord webhook URL with retry logic.
  Returns `{:ok, message_id}` on success or `{:error, reason}` on failure.
  """
  def process_target(
        action,
        target,
        content \\ %{},
        batch_id \\ nil,
        polled_at \\ nil,
        enqueued_at \\ nil,
        parent_message_id \\ nil
      )

  def process_target(
        action,
        %{"webhook_id" => webhook_id, "webhook_token" => webhook_token} = target,
        content,
        batch_id,
        polled_at,
        enqueued_at,
        parent_message_id
      ) do
    if is_binary(webhook_id) and is_binary(webhook_token) do
      base_url = "#{discord_base_url()}/api/webhooks/#{webhook_id}/#{webhook_token}"
      thread_id = Map.get(target, "thread_id")
      message_id = Map.get(target, "message_id")

      dead_cache_hit =
        action in ["edit", "delete"] and is_binary(message_id) and
          dead_message_cached?(webhook_id, message_id)

      if dead_cache_hit do
        Logger.debug(
          "Skipping #{action} for webhook_id=#{webhook_id} message_id=#{message_id} — known dead in cache"
        )

        if action == "delete", do: {:ok, nil}, else: {:error, :message_not_found}
      else
        content =
          if is_map(content) and action in ["execute", "edit"] do
            overrides = Map.get(target, "overrides") || %{}
            Map.merge(content, overrides)
          else
            content
          end

        headers = [{"Content-Type", "application/json"}]
        body = if action == "delete", do: nil, else: Jason.encode_to_iodata!(content)

        checkpoint_key = "checkpoint:#{action}:#{batch_id}:#{webhook_id}"
        method_str = action_to_method_string(action)

        cached_result =
          if batch_id do
            case redix_command(["GET", checkpoint_key]) do
              {:ok, "done"} -> {:ok, nil}
              {:ok, msg_id} when is_binary(msg_id) -> {:ok, msg_id}
              _ -> nil
            end
          end

        OpenTelemetry.Tracer.with_span "prism.worker.process_target" do
          OpenTelemetry.Tracer.set_attributes([
            {:action, action},
            {:webhook_id, webhook_id},
            {:batch_id, batch_id}
          ])

          if cached_result do
            cached_result
          else
            if backpressure_enabled?() and Prism.RateLimit.unhealthy?() do
              delay_ms = Prism.RateLimit.backoff_ms()

              parent_msg_id = if action == "execute", do: parent_message_id, else: nil

              case build_request(action, base_url, message_id, thread_id) do
                {:ok, method, url} ->
                  spawn_retry(
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
                    1,
                    parent_msg_id,
                    :rate_limited
                  )

                  {:error, {:rate_limited, delay_ms}}

                {:error, reason} ->
                  {:error, reason}
              end
            else
              # Pre-flight rate-limit check
              {should_defer, should_sleep, rate_limit_delay_ms} =
                case Prism.RateLimit.check(webhook_id, method_str) do
                  {:ok, _remaining} -> {false, false, 0}
                  {:blocked, ttl_ms} when ttl_ms > 10000 -> {true, false, ttl_ms}
                  {:blocked, ttl_ms} -> {false, true, ttl_ms}
                end

              if should_defer do
                Logger.debug(
                  "Pre-flight rate limit check triggered for webhook_id=#{webhook_id} with long TTL=#{rate_limit_delay_ms}ms. Rescheduling immediately."
                )

                parent_msg_id = if action == "execute", do: parent_message_id, else: nil

                case build_request(action, base_url, message_id, thread_id) do
                  {:ok, method, url} ->
                    spawn_retry(
                      action,
                      target,
                      method,
                      url,
                      headers,
                      body,
                      webhook_id,
                      message_id,
                      batch_id,
                      rate_limit_delay_ms,
                      1,
                      parent_msg_id,
                      :rate_limited
                    )

                    # Return the error so the batch knows this target was not a success
                    {:error, {:rate_limited, rate_limit_delay_ms}}

                  {:error, reason} ->
                    {:error, reason}
                end
              else
                if should_sleep do
                  Logger.debug(
                    "Pre-flight rate limit check triggered for webhook_id=#{webhook_id}. Sleeping for #{rate_limit_delay_ms}ms."
                  )

                  Process.sleep(rate_limit_delay_ms)
                end

                case build_request(action, base_url, message_id, thread_id) do
                  {:ok, method, url} ->
                    req_start = System.monotonic_time(:millisecond)
                    req_start_wall = :os.system_time(:millisecond)

                    result =
                      do_http_request(
                        method,
                        method_str,
                        url,
                        headers,
                        body,
                        webhook_id,
                        message_id
                      )

                    req_end = System.monotonic_time(:millisecond)
                    req_end_wall = :os.system_time(:millisecond)

                    if Code.ensure_loaded?(Mix) and Mix.env() == :dev do
                      queue_time = if enqueued_at, do: polled_at - enqueued_at, else: 0
                      prep_time = if polled_at, do: req_start_wall - polled_at, else: 0
                      http_time = req_end - req_start
                      total_time = if enqueued_at, do: req_end_wall - enqueued_at, else: 0

                      Logger.info(
                        "[Timing] Webhook #{webhook_id} (batch #{batch_id || "N/A"}) - Queue: #{queue_time}ms | Prep: #{prep_time}ms | Discord HTTP: #{http_time}ms | Total End-to-End: #{total_time}ms"
                      )
                    end

                    # Cache success
                    if batch_id do
                      case result do
                        {:ok, msg_id} when is_binary(msg_id) ->
                          redix_command(["SETEX", checkpoint_key, "86400", msg_id])

                        {:ok, nil} ->
                          redix_command(["SETEX", checkpoint_key, "86400", "done"])

                        _ ->
                          :ok
                      end
                    end

                    parent_msg_id = if action == "execute", do: parent_message_id, else: nil

                    case result do
                      {:error, {:rate_limited, delay_ms}} ->
                        OpenTelemetry.Tracer.set_attribute(:error_type, "rate_limited")

                        spawn_retry(
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
                          1,
                          parent_msg_id,
                          :rate_limited
                        )

                        # ★ Return the error so the batch knows it failed
                        {:error, {:rate_limited, delay_ms}}

                      {:error, {:server_error, _}} ->
                        OpenTelemetry.Tracer.set_attribute(:error_type, "server_error")

                        spawn_retry(
                          action,
                          target,
                          method,
                          url,
                          headers,
                          body,
                          webhook_id,
                          message_id,
                          batch_id,
                          2000,
                          1,
                          parent_msg_id,
                          :server_error
                        )

                        # server errors are retried, batch can consider this as “handled”
                        {:ok, nil}

                      {:error, :network_error} ->
                        OpenTelemetry.Tracer.set_attribute(:error_type, "network_error")

                        spawn_retry(
                          action,
                          target,
                          method,
                          url,
                          headers,
                          body,
                          webhook_id,
                          message_id,
                          batch_id,
                          1000,
                          1,
                          parent_msg_id,
                          :network_error
                        )

                        {:ok, nil}

                      {:error, :message_not_found_transient} ->
                        OpenTelemetry.Tracer.set_attribute(
                          :error_type,
                          "message_not_found_transient"
                        )

                        spawn_retry(
                          action,
                          target,
                          method,
                          url,
                          headers,
                          body,
                          webhook_id,
                          message_id,
                          batch_id,
                          1000,
                          1,
                          parent_msg_id,
                          :message_not_found_transient
                        )

                        {:ok, nil}

                      # ★ Permanent errors: never retry, return error immediately
                      {:error, :permanent} ->
                        OpenTelemetry.Tracer.set_attribute(:error_type, "permanent")
                        {:error, :permanent}

                      other ->
                        other
                    end

                  {:error, reason} ->
                    OpenTelemetry.Tracer.set_attribute(:error_type, inspect(reason))
                    {:error, reason}
                end
              end
            end
          end
        end
      end
    else
      Logger.warning("Invalid webhook data. Skipping.")
      {:error, :invalid_webhook}
    end
  end

  def process_target(
        _action,
        target,
        _content,
        _batch_id,
        _polled_at,
        _enqueued_at,
        _parent_message_id
      ) do
    Logger.warning("Missing webhook data in target: #{inspect(target)}. Skipping.")
    {:error, :missing_webhook}
  end

  defp redix_command(command) do
    idx = :erlang.phash2(System.unique_integer(), 5)
    Redix.command(:"my_redix_#{idx}", command)
  end

  defp action_to_method_string("execute"), do: "post"
  defp action_to_method_string("edit"), do: "patch"
  defp action_to_method_string("delete"), do: "delete"
  defp action_to_method_string(_), do: "post"

  def process_retry(payload, _polled_at, _enqueued_at) do
    source_message_id =
      payload["source_message_id"] || payload["parent_msg_id"] || payload["batch_id"]

    if source_message_id && Prism.CancelChecker.cancelled?(source_message_id) do
      Logger.info("RetryBroadway: skipping cancelled retry for #{source_message_id}")
    else
      action = payload["action"]
      target = payload["target"]
      method = safe_method_atom(payload["method"])
      method_str = to_string(method)
      url = payload["url"]

      headers =
        case payload["headers"] do
          nil -> []
          map when is_map(map) -> Enum.to_list(map)
          list when is_list(list) -> list
        end

      body = payload["body"]
      webhook_id = payload["webhook_id"]
      message_id = payload["message_id"]
      batch_id = payload["batch_id"]
      attempt = payload["attempt"]
      parent_msg_id = payload["parent_msg_id"]
      reason = payload["reason"]

      reason_str = if reason, do: " (Reason: #{reason})", else: ""

      Logger.debug(
        "Retrying webhook_id=#{webhook_id} (Attempt #{attempt})#{reason_str} in Broadway pipeline..."
      )

      if backpressure_enabled?() and Prism.RateLimit.unhealthy?() do
        delay_ms = Prism.RateLimit.backoff_ms()

        spawn_retry(
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
          :rate_limited
        )
      else
        dead_cache_hit =
          action in ["edit", "delete"] and is_binary(message_id) and
            dead_message_cached?(webhook_id, message_id)

        if dead_cache_hit do
          Logger.debug(
            "Retry skipping #{action} for webhook_id=#{webhook_id} message_id=#{message_id} — known dead in cache"
          )

          if action == "delete" do
            publish_partial(action, target, batch_id, parent_msg_id, nil, nil)
          else
            publish_partial(action, target, batch_id, parent_msg_id, nil, :message_not_found)
          end
        else
          {should_defer, should_sleep, rate_limit_delay_ms} =
            case Prism.RateLimit.check(webhook_id, method_str) do
              {:ok, _remaining} -> {false, false, 0}
              {:blocked, ttl_ms} when ttl_ms > 10000 -> {true, false, ttl_ms}
              {:blocked, ttl_ms} -> {false, true, ttl_ms}
            end

          if should_defer do
            Logger.debug(
              "Retry pre-flight rate limit triggered for webhook_id=#{webhook_id} with long TTL=#{rate_limit_delay_ms}ms. Rescheduling immediately."
            )

            spawn_retry(
              action,
              target,
              method,
              url,
              headers,
              body,
              webhook_id,
              message_id,
              batch_id,
              rate_limit_delay_ms,
              attempt,
              parent_msg_id,
              :rate_limited
            )
          else
            if should_sleep do
              Logger.debug(
                "Retry pre-flight rate limit triggered for webhook_id=#{webhook_id}. Sleeping for #{rate_limit_delay_ms}ms."
              )

              Process.sleep(rate_limit_delay_ms)
            end

            result =
              do_http_request(method, method_str, url, headers, body, webhook_id, message_id)

            case result do
              {:error, {:rate_limited, delay_ms}} ->
                spawn_retry(
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
                  attempt + 1,
                  parent_msg_id,
                  :rate_limited
                )

              {:error, {:server_error, _}} ->
                if attempt >= 3 do
                  publish_partial(action, target, batch_id, parent_msg_id, nil, :server_error)
                else
                  backoff_ms = 2000 * attempt
                  jitter_ms = :rand.uniform(1000)
                  delay_ms = backoff_ms + jitter_ms

                  spawn_retry(
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
                    attempt + 1,
                    parent_msg_id,
                    :server_error
                  )
                end

              {:error, :network_error} ->
                if attempt >= 5 do
                  publish_partial(action, target, batch_id, parent_msg_id, nil, :network_error)
                else
                  backoff_ms = 1000 * attempt
                  jitter_ms = :rand.uniform(500)
                  delay_ms = backoff_ms + jitter_ms

                  spawn_retry(
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
                    attempt + 1,
                    parent_msg_id,
                    :network_error
                  )
                end

              {:error, :message_not_found_transient} ->
                if attempt >= 5 do
                  if method == :delete do
                    Logger.info(
                      "Webhook_id=#{webhook_id} still 10008 on attempt #{attempt} for delete, assuming deleted."
                    )

                    publish_partial(action, target, batch_id, parent_msg_id, nil, nil)
                  else
                    publish_partial(
                      action,
                      target,
                      batch_id,
                      parent_msg_id,
                      nil,
                      :message_not_found
                    )
                  end
                else
                  backoff_ms = 1000 * attempt
                  jitter_ms = :rand.uniform(500)
                  delay_ms = backoff_ms + jitter_ms

                  spawn_retry(
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
                    attempt + 1,
                    parent_msg_id,
                    :message_not_found_transient
                  )
                end

              # ★ Permanent errors: stop retrying immediately
              {:error, :permanent} ->
                Logger.warning("Permanent error for webhook_id=#{webhook_id}, not retrying.")
                publish_partial(action, target, batch_id, parent_msg_id, nil, :permanent)

              {:ok, msg_id} ->
                Logger.info(
                  "Successfully delivered to webhook_id=#{webhook_id} on Attempt #{attempt}!"
                )

                Prism.RateLimit.record_success()

                if batch_id do
                  checkpoint_key = "checkpoint:#{action}:#{batch_id}:#{webhook_id}"
                  msg_id_val = if is_binary(msg_id), do: msg_id, else: "done"
                  redix_command(["SETEX", checkpoint_key, "86400", msg_id_val])
                end

                publish_partial(action, target, batch_id, parent_msg_id, msg_id, nil)

              other ->
                Logger.error(
                  "Unexpected retry result for webhook_id=#{webhook_id}: #{inspect(other)}"
                )

                publish_partial(action, target, batch_id, parent_msg_id, nil, other)
            end
          end
        end
      end
    end
  end

  defp spawn_retry(
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
      "source_message_id" => parent_msg_id,
      "reason" => to_string(reason)
    }

    Logger.info(
      "Scheduling retry for webhook_id=#{webhook_id} reason=#{reason} delay=#{delay_ms}ms attempt=#{attempt}"
    )

    Prism.DelayedQueue.enqueue(payload, delay_ms)
  end

  defp backpressure_enabled? do
    Application.get_env(:prism, :backpressure_enabled, true)
  end

  defp discord_base_url do
    Application.get_env(:prism, :discord_base_url, "https://discord.com")
  end

  defp publish_partial(action, target, batch_id, parent_msg_id, success_msg_id, error_reason) do
    if not is_nil(batch_id) do
      base_info =
        %{
          "webhook_id" => target["webhook_id"],
          "message_id" => target["message_id"],
          "channel_id" => target["channel_id"],
          "guild_id" => target["guild_id"],
          "connection_id" => target["connection_id"],
          "hub_id" => target["hub_id"]
        }
        |> Map.reject(fn {_, v} -> is_nil(v) end)

      {successes, failures} =
        if error_reason do
          {error_string, error_type} =
            cond do
              error_reason == :invalid_webhook -> {"invalid_webhook", "permanent"}
              error_reason == :message_not_found -> {"message_not_found", "permanent"}
              error_reason == :bad_request -> {"bad_request", "transient"}
              error_reason == :server_error -> {"server_error", "transient"}
              error_reason == :network_error -> {"network_error", "transient"}
              error_reason == :permanent -> {"permanent_error", "permanent"}
              true -> {inspect(error_reason), "transient"}
            end

          {[], [Map.merge(base_info, %{"error" => error_string, "error_type" => error_type})]}
        else
          succ_info =
            if success_msg_id,
              do: Map.put(base_info, "message_id", success_msg_id),
              else: base_info

          {[succ_info], []}
        end

      new_trace_headers = :otel_propagator_text_map.inject([]) |> Enum.into(%{})

      payload = %{
        "batch_id" => batch_id,
        "status" => "partial_retry",
        "action" => action,
        "message_ids" => successes,
        "failures" => failures,
        "trace_headers" => new_trace_headers
      }

      payload =
        if parent_msg_id, do: Map.put(payload, "parent_message_id", parent_msg_id), else: payload

      json = Jason.encode!(payload)

      callback_stream =
        Application.get_env(:prism, :redis_callback_stream, "discord:fanout:callbacks")

      redix_command(["XADD", callback_stream, "MAXLEN", "~", "100000", "*", "payload", json])
    end
  end

  defp build_request("execute", base_url, _msg_id, thread_id) do
    url = base_url <> "?wait=true&with_components=true"
    url = if is_binary(thread_id), do: url <> "&thread_id=#{thread_id}", else: url
    {:ok, :post, url}
  end

  defp build_request("edit", base_url, msg_id, thread_id) when is_binary(msg_id) do
    url = base_url <> "/messages/#{msg_id}?with_components=true"
    url = if is_binary(thread_id), do: url <> "&thread_id=#{thread_id}", else: url
    {:ok, :patch, url}
  end

  defp build_request("delete", base_url, msg_id, thread_id) when is_binary(msg_id) do
    url = base_url <> "/messages/#{msg_id}"
    url = if is_binary(thread_id), do: url <> "?thread_id=#{thread_id}", else: url
    {:ok, :delete, url}
  end

  defp build_request(action, _base_url, _msg_id, _thread_id) do
    Logger.error("Invalid action or missing message_id for action: #{action}")
    {:error, :invalid_action}
  end

  defp do_http_request(method, method_str, url, headers, body, webhook_id, message_id) do
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
    if empty_webhook_body?(body) do
      Logger.warning(
        "Skipping webhook_id=#{webhook_id} method=#{method_str} — empty payload (no content, embeds, or components)"
      )

      {:error, :empty_payload}
    else
      case Finch.build(method, url, headers, body)
           |> Finch.request(DiscordFinch, receive_timeout: 30_000, pool_timeout: 10_000) do
        {:ok, %{status: status, body: resp_body, headers: headers}} when status in 200..299 ->
          Prism.RateLimit.handle_response(webhook_id, method_str, status, headers, resp_body)

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

        {:ok, %{status: 429, body: resp_body, headers: headers}} ->
          {:error, parsed} =
            Prism.RateLimit.handle_response(webhook_id, method_str, 429, headers, resp_body)

          if parsed.is_cloudflare do
            Logger.error(
              "Cloudflare IP-level block (429) on webhook_id=#{webhook_id}! " <>
                "Delay: #{parsed.retry_after_ms}ms | Headers: #{inspect(headers)} | Body: #{resp_body}"
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
                "Response Headers: #{inspect(headers)}"
            )
          end

          {:error, {:rate_limited, parsed.retry_after_ms}}

        # ★ Permanent errors: 401, 403, 400
        {:ok, %{status: status, body: resp_body, headers: headers}} when status in [401, 403] ->
          is_cf = Prism.RateLimit.Headers.cloudflare_response?(headers, resp_body)

          Logger.warning(
            "Permanent error #{status} for webhook_id=#{webhook_id} – token invalid or missing permissions. " <>
              "Cloudflare: #{is_cf} | Headers: #{inspect(headers)} | Body: #{resp_body}"
          )

          {:error, :permanent}

        {:ok, %{status: 400, body: resp_body}} ->
          Logger.error(
            "Bad request webhook_id=#{webhook_id} body=#{resp_body} – dropping permanently."
          )

          {:error, :permanent}

        {:ok, %{status: 404, body: resp_body}} ->
          case Jason.decode(resp_body) do
            {:ok, %{"code" => 10008}} ->
              if method == :delete do
                Logger.debug(
                  "Webhook_id=#{webhook_id} returned 10008 on delete. Message already deleted, treating as success."
                )

                if is_binary(message_id), do: cache_dead_message(webhook_id, message_id)
                Prism.RateLimit.InvalidRequestTracker.record_invalid()
                {:ok, nil}
              else
                Logger.info(
                  "Webhook_id=#{webhook_id} returned 10008 on #{method}. Target message not found (deleted)."
                )

                if is_binary(message_id), do: cache_dead_message(webhook_id, message_id)
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

        {:ok, %{status: status, body: resp_body, headers: headers}} ->
          Logger.warning(
            "Unexpected status #{status} for webhook_id=#{webhook_id}. " <>
              "Headers: #{inspect(headers)} | Body: #{resp_body}"
          )

          {:ok, nil}
      end
    end
  end

  defp empty_webhook_body?(nil), do: true

  defp empty_webhook_body?(body) do
    case Jason.decode(body) do
      {:ok, decoded} when is_map(decoded) ->
        content = Map.get(decoded, "content")
        embeds = Map.get(decoded, "embeds")
        components = Map.get(decoded, "components")

        (is_nil(content) or content == "") and
          is_nil_or_empty?(embeds) and
          is_nil_or_empty?(components)

      _ ->
        false
    end
  end

  defp is_nil_or_empty?(nil), do: true
  defp is_nil_or_empty?(list) when is_list(list), do: list == []
  defp is_nil_or_empty?(_), do: false

  defp safe_method_atom("post"), do: :post
  defp safe_method_atom("patch"), do: :patch
  defp safe_method_atom("delete"), do: :delete

  defp safe_method_atom(other) do
    Logger.warning("Unknown HTTP method in retry payload: #{inspect(other)}, defaulting to :post")
    :post
  end

  defp dead_message_cached?(webhook_id, message_id) do
    case redix_command(["EXISTS", "dead_msg:#{webhook_id}:#{message_id}"]) do
      {:ok, 1} ->
        true

      _ ->
        case redix_command(["EXISTS", "dead_msg:#{message_id}"]) do
          {:ok, 1} -> true
          _ -> false
        end
    end
  end

  defp cache_dead_message(webhook_id, message_id, ttl_seconds \\ 1800) do
    redix_command([
      "SETEX",
      "dead_msg:#{webhook_id}:#{message_id}",
      to_string(ttl_seconds),
      "1"
    ])
  end
end
