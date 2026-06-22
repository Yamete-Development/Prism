defmodule Prism.DiscordWorker do
  require Logger
  require OpenTelemetry.Tracer

  alias Prism.Helpers
  alias Prism.DiscordWorker.{HTTP, Retry, Callbacks, DeadMessage}

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
      base_url = "#{Prism.Config.discord_base_url()}/api/webhooks/#{webhook_id}/#{webhook_token}"
      thread_id = Map.get(target, "thread_id")
      message_id = Map.get(target, "message_id")

      dead_cache_hit =
        action in ["edit", "delete"] and is_binary(message_id) and
          DeadMessage.dead_message_cached?(webhook_id, message_id)

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
        method_str = Helpers.action_to_method_string(action)

        cached_result =
          if batch_id do
            case Helpers.redix_command(["GET", checkpoint_key]) do
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
            if Prism.Config.backpressure_enabled?() and Prism.RateLimit.unhealthy?() do
              delay_ms = Prism.RateLimit.backoff_ms()

              parent_msg_id = if action == "execute", do: parent_message_id, else: nil

              case HTTP.build_request(action, base_url, message_id, thread_id) do
                {:ok, method, url} ->
                  Retry.spawn_retry(
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
              defer_threshold = Prism.Config.rate_limit_defer_threshold_ms()

              {should_defer, should_sleep, rate_limit_delay_ms} =
                case Prism.RateLimit.check(webhook_id, method_str) do
                  {:ok, _remaining} ->
                    {false, false, 0}

                  {:blocked, ttl_ms} ->
                    if ttl_ms > defer_threshold,
                      do: {true, false, ttl_ms},
                      else: {false, true, ttl_ms}
                end

              if should_defer do
                Logger.debug(
                  "Pre-flight rate limit check triggered for webhook_id=#{webhook_id} with long TTL=#{rate_limit_delay_ms}ms. Rescheduling immediately."
                )

                parent_msg_id = if action == "execute", do: parent_message_id, else: nil

                case HTTP.build_request(action, base_url, message_id, thread_id) do
                  {:ok, method, url} ->
                    Retry.spawn_retry(
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

                case HTTP.build_request(action, base_url, message_id, thread_id) do
                  {:ok, method, url} ->
                    req_start = System.monotonic_time(:millisecond)
                    req_start_wall = :os.system_time(:millisecond)

                    result =
                      HTTP.do_http_request(
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

                    if batch_id do
                      checkpoint_ttl = to_string(Prism.Config.checkpoint_ttl_seconds())

                      case result do
                        {:ok, msg_id} when is_binary(msg_id) ->
                          Helpers.redix_command([
                            "SETEX",
                            checkpoint_key,
                            checkpoint_ttl,
                            msg_id
                          ])

                        {:ok, nil} ->
                          Helpers.redix_command([
                            "SETEX",
                            checkpoint_key,
                            checkpoint_ttl,
                            "done"
                          ])

                        _ ->
                          :ok
                      end
                    end

                    parent_msg_id = if action == "execute", do: parent_message_id, else: nil

                    case result do
                      {:error, {:rate_limited, delay_ms}} ->
                        OpenTelemetry.Tracer.set_attribute(:error_type, "rate_limited")

                        Retry.spawn_retry(
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

                      {:error, {:server_error, _}} ->
                        OpenTelemetry.Tracer.set_attribute(:error_type, "server_error")

                        Retry.spawn_retry(
                          action,
                          target,
                          method,
                          url,
                          headers,
                          body,
                          webhook_id,
                          message_id,
                          batch_id,
                          Prism.Config.server_error_base_delay_ms(),
                          1,
                          parent_msg_id,
                          :server_error
                        )

                        {:ok, nil}

                      {:error, :network_error} ->
                        OpenTelemetry.Tracer.set_attribute(:error_type, "network_error")

                        Retry.spawn_retry(
                          action,
                          target,
                          method,
                          url,
                          headers,
                          body,
                          webhook_id,
                          message_id,
                          batch_id,
                          Prism.Config.network_error_base_delay_ms(),
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

                        Retry.spawn_retry(
                          action,
                          target,
                          method,
                          url,
                          headers,
                          body,
                          webhook_id,
                          message_id,
                          batch_id,
                          Prism.Config.network_error_base_delay_ms(),
                          1,
                          parent_msg_id,
                          :message_not_found_transient
                        )

                        {:ok, nil}

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

  def process_retry(payload, _polled_at, _enqueued_at) do
    source_message_id =
      payload["source_message_id"] || payload["parent_msg_id"] || payload["batch_id"]

    if source_message_id && Prism.CancelChecker.cancelled?(source_message_id) do
      Logger.info("RetryBroadway: skipping cancelled retry for #{source_message_id}")
    else
      action = payload["action"]
      target = payload["target"]
      method = Helpers.safe_method_atom(payload["method"])
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

      if Prism.Config.backpressure_enabled?() and Prism.RateLimit.unhealthy?() do
        delay_ms = Prism.RateLimit.backoff_ms()

        Retry.spawn_retry(
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
            DeadMessage.dead_message_cached?(webhook_id, message_id)

        if dead_cache_hit do
          Logger.debug(
            "Retry skipping #{action} for webhook_id=#{webhook_id} message_id=#{message_id} — known dead in cache"
          )

          if action == "delete" do
            Callbacks.publish_partial(action, target, batch_id, parent_msg_id, nil, nil)
          else
            Callbacks.publish_partial(
              action,
              target,
              batch_id,
              parent_msg_id,
              nil,
              :message_not_found
            )
          end
        else
          defer_threshold = Prism.Config.rate_limit_defer_threshold_ms()

          {should_defer, should_sleep, rate_limit_delay_ms} =
            case Prism.RateLimit.check(webhook_id, method_str) do
              {:ok, _remaining} ->
                {false, false, 0}

              {:blocked, ttl_ms} ->
                if ttl_ms > defer_threshold,
                  do: {true, false, ttl_ms},
                  else: {false, true, ttl_ms}
            end

          if should_defer do
            Logger.debug(
              "Retry pre-flight rate limit triggered for webhook_id=#{webhook_id} with long TTL=#{rate_limit_delay_ms}ms. Rescheduling immediately."
            )

            Retry.spawn_retry(
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
              HTTP.do_http_request(method, method_str, url, headers, body, webhook_id, message_id)

            case result do
              {:error, {:rate_limited, delay_ms}} ->
                Retry.spawn_retry(
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
                max_retries = Prism.Config.server_error_max_retries()

                if attempt >= max_retries do
                  Callbacks.publish_partial(
                    action,
                    target,
                    batch_id,
                    parent_msg_id,
                    nil,
                    :server_error
                  )
                else
                  backoff_ms = Prism.Config.server_error_base_delay_ms() * attempt
                  jitter_ms = :rand.uniform(1000)
                  delay_ms = backoff_ms + jitter_ms

                  Retry.spawn_retry(
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
                max_retries = Prism.Config.network_error_max_retries()

                if attempt >= max_retries do
                  Callbacks.publish_partial(
                    action,
                    target,
                    batch_id,
                    parent_msg_id,
                    nil,
                    :network_error
                  )
                else
                  backoff_ms = Prism.Config.network_error_base_delay_ms() * attempt
                  jitter_ms = :rand.uniform(500)
                  delay_ms = backoff_ms + jitter_ms

                  Retry.spawn_retry(
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
                max_retries = Prism.Config.message_not_found_max_retries()

                if attempt >= max_retries do
                  if method == :delete do
                    Logger.info(
                      "Webhook_id=#{webhook_id} still 10008 on attempt #{attempt} for delete, assuming deleted."
                    )

                    Callbacks.publish_partial(action, target, batch_id, parent_msg_id, nil, nil)
                  else
                    Callbacks.publish_partial(
                      action,
                      target,
                      batch_id,
                      parent_msg_id,
                      nil,
                      :message_not_found
                    )
                  end
                else
                  backoff_ms = Prism.Config.network_error_base_delay_ms() * attempt
                  jitter_ms = :rand.uniform(500)
                  delay_ms = backoff_ms + jitter_ms

                  Retry.spawn_retry(
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

              {:error, :permanent} ->
                Logger.warning("Permanent error for webhook_id=#{webhook_id}, not retrying.")

                Callbacks.publish_partial(
                  action,
                  target,
                  batch_id,
                  parent_msg_id,
                  nil,
                  :permanent
                )

              {:ok, msg_id} ->
                Logger.info(
                  "Successfully delivered to webhook_id=#{webhook_id} on Attempt #{attempt}!"
                )

                Prism.RateLimit.record_success()

                if batch_id do
                  checkpoint_key = "checkpoint:#{action}:#{batch_id}:#{webhook_id}"
                  checkpoint_ttl = to_string(Prism.Config.checkpoint_ttl_seconds())
                  msg_id_val = if is_binary(msg_id), do: msg_id, else: "done"

                  Helpers.redix_command([
                    "SETEX",
                    checkpoint_key,
                    checkpoint_ttl,
                    msg_id_val
                  ])
                end

                Callbacks.publish_partial(action, target, batch_id, parent_msg_id, msg_id, nil)

              other ->
                Logger.error(
                  "Unexpected retry result for webhook_id=#{webhook_id}: #{inspect(other)}"
                )

                Callbacks.publish_partial(action, target, batch_id, parent_msg_id, nil, other)
            end
          end
        end
      end
    end
  end
end
