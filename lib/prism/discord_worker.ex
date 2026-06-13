defmodule Prism.DiscordWorker do
  require Logger

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
      base_url = "https://discord.com/api/webhooks/#{webhook_id}/#{webhook_token}"
      thread_id = Map.get(target, "thread_id")
      message_id = Map.get(target, "message_id")

      content =
        if is_map(content) and action in ["execute", "edit"] do
          overrides = Map.get(target, "overrides") || %{}
          Map.merge(content, overrides)
        else
          content
        end

      headers = [{"Content-Type", "application/json"}]
      body = if action == "delete", do: "", else: Jason.encode_to_iodata!(content)

      checkpoint_key = "checkpoint:#{action}:#{batch_id}:#{webhook_id}"
      rl_key = "rl:#{webhook_id}"
      global_rl_key = "rl:global"

      {cached_result, rl_ttl} =
        if batch_id do
          case redix_pipeline([["GET", checkpoint_key], ["PTTL", rl_key], ["PTTL", global_rl_key]]) do
            {:ok, [get_res, pttl_res, global_pttl_res]} ->
              cached =
                case get_res do
                  "done" -> {:ok, nil}
                  msg_id when is_binary(msg_id) -> {:ok, msg_id}
                  _ -> nil
                end

              ttl =
                case pttl_res do
                  ttl when is_integer(ttl) and ttl > 0 -> ttl
                  _ -> 0
                end

              global_ttl =
                case global_pttl_res do
                  ttl when is_integer(ttl) and ttl > 0 -> ttl
                  _ -> 0
                end

              {cached, max(ttl, global_ttl)}

            _ ->
              {nil, 0}
          end
        else
          case redix_pipeline([["PTTL", rl_key], ["PTTL", global_rl_key]]) do
            {:ok, [pttl_res, global_pttl_res]} ->
              ttl =
                case pttl_res do
                  ttl when is_integer(ttl) and ttl > 0 -> ttl
                  _ -> 0
                end

              global_ttl =
                case global_pttl_res do
                  ttl when is_integer(ttl) and ttl > 0 -> ttl
                  _ -> 0
                end

              {nil, max(ttl, global_ttl)}

            _ ->
              {nil, 0}
          end
        end

      if cached_result do
        cached_result
      else
        if rl_ttl > 10000 do
          Logger.debug(
            "Pre-flight rate limit check triggered for webhook_id=#{webhook_id} with long TTL=#{rl_ttl}ms. Rescheduling immediately."
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
                rl_ttl,
                1,
                parent_msg_id,
                :rate_limited
              )

              {:ok, nil}

            {:error, reason} ->
              {:error, reason}
          end
        else
          if rl_ttl > 0 do
            Logger.debug(
              "Pre-flight rate limit check triggered for webhook_id=#{webhook_id}. Sleeping for #{rl_ttl}ms."
            )

            Process.sleep(rl_ttl)
          end

        case build_request(action, base_url, message_id, thread_id) do
          {:ok, method, url} ->
            req_start = System.monotonic_time(:millisecond)
            req_start_wall = :os.system_time(:millisecond)
            result = do_http_request(method, url, headers, body, webhook_id, message_id)
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

            # Handle retries if needed
            # The parent_message_id is now passed down from Broadway in 'message_id' param if it's the execute action
            parent_msg_id = if action == "execute", do: parent_message_id, else: nil

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
                  1,
                  parent_msg_id,
                  :rate_limited
                )

                # Unblock batch
                {:ok, nil}

              {:error, {:server_error, _}} ->
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

                {:ok, nil}

              {:error, :network_error} ->
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

              other ->
                other
            end

          {:error, reason} ->
            {:error, reason}
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

  defp redix_pipeline(commands) do
    idx = :erlang.phash2(System.unique_integer(), 5)
    Redix.pipeline(:"my_redix_#{idx}", commands)
  end

  def process_retry(payload, _polled_at, _enqueued_at) do
    action = payload["action"]
    target = payload["target"]
    method = String.to_existing_atom(payload["method"])
    url = payload["url"]
    headers = payload["headers"] |> Enum.to_list()
    body = payload["body"]
    webhook_id = payload["webhook_id"]
    message_id = payload["message_id"]
    batch_id = payload["batch_id"]
    attempt = payload["attempt"]
    parent_msg_id = payload["parent_msg_id"]
    reason = String.to_existing_atom(payload["reason"])

    reason_str = if reason, do: " (Reason: #{reason})", else: ""

    Logger.info(
      "Retrying webhook_id=#{webhook_id} (Attempt #{attempt})#{reason_str} in Broadway pipeline..."
    )

    result = do_http_request(method, url, headers, body, webhook_id, message_id)

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
            Logger.info("Webhook_id=#{webhook_id} still 10008 on attempt #{attempt} for delete, assuming deleted.")
            publish_partial(action, target, batch_id, parent_msg_id, nil, nil)
          else
            publish_partial(action, target, batch_id, parent_msg_id, nil, :message_not_found)
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

      {:error, permanent_reason} ->
        publish_partial(action, target, batch_id, parent_msg_id, nil, permanent_reason)

      {:ok, msg_id} ->
        Logger.info("Successfully delivered to webhook_id=#{webhook_id} on Attempt #{attempt}!")
        publish_partial(action, target, batch_id, parent_msg_id, msg_id, nil)
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
      "body" => IO.iodata_to_binary(body),
      "webhook_id" => webhook_id,
      "message_id" => message_id,
      "batch_id" => batch_id,
      "attempt" => attempt,
      "parent_msg_id" => parent_msg_id,
      "reason" => to_string(reason)
    }

    Prism.DelayedQueue.enqueue(payload, delay_ms)
  end

  defp publish_partial(action, target, batch_id, parent_msg_id, success_msg_id, error_reason) do
    if not is_nil(batch_id) do
      base_info =
        %{
          "webhook_id" => target["webhook_id"],
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

      payload = %{
        "batch_id" => batch_id,
        "status" => "partial_retry",
        "action" => action,
        "message_ids" => successes,
        "failures" => failures
      }

      payload =
        if parent_msg_id, do: Map.put(payload, "parent_message_id", parent_msg_id), else: payload

      json = Jason.encode!(payload)

      callback_stream =
        Application.get_env(:prism, :redis_callback_stream, "discord:fanout:callbacks")

      redix_command(["XADD", callback_stream, "*", "payload", json])
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

  defp do_http_request(method, url, headers, body, webhook_id, _message_id) do
    case Finch.build(method, url, headers, body)
         |> Finch.request(DiscordFinch, receive_timeout: 30_000, pool_timeout: 30_000) do
      {:ok, %{status: status, body: resp_body, headers: headers}} when status in 200..299 ->
        # Traffic shaping: record if bucket is empty
        remaining =
          Enum.find_value(headers, fn {k, v} ->
            if String.downcase(k) == "x-ratelimit-remaining", do: v
          end)

        if remaining == "0" do
          reset_after_str =
            Enum.find_value(headers, fn {k, v} ->
              if String.downcase(k) == "x-ratelimit-reset-after", do: v
            end)

          if reset_after_str do
            case Float.parse(reset_after_str) do
              {reset_after_sec, _} ->
                ttl_ms = trunc(reset_after_sec * 1000)

                if ttl_ms > 0 do
                  redix_command(["PSETEX", "rl:#{webhook_id}", Integer.to_string(ttl_ms), "1"])
                end

              :error ->
                :ok
            end
          end
        end

        if method == :post do
          case Jason.decode(resp_body) do
            {:ok, %{"id" => msg_id}} ->
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
        # Try Discord JSON body first, then fall back to the standard HTTP
        # retry-after header (used by Cloudflare 1015 bans where the body is
        # plain text like "error code: 1015", not JSON).
        retry_after_ms =
          case Jason.decode(resp_body) do
            {:ok, %{"retry_after" => retry_after}} when is_number(retry_after) ->
              trunc(retry_after * 1000)

            _ ->
              # Cloudflare / non-JSON response — read the HTTP retry-after header (in seconds)
              case Enum.find_value(headers, fn {k, v} ->
                     if String.downcase(k) == "retry-after", do: v
                   end) do
                nil ->
                  5000

                value ->
                  case Integer.parse(value) do
                    {seconds, _} -> seconds * 1000
                    :error -> 5000
                  end
              end
          end

        bucket =
          Enum.find_value(headers, fn {k, v} ->
            if String.downcase(k) == "x-ratelimit-bucket", do: v
          end)

        scope =
          Enum.find_value(headers, fn {k, v} ->
            if String.downcase(k) == "x-ratelimit-scope", do: v
          end)

        global =
          Enum.find_value(headers, fn {k, v} ->
            if String.downcase(k) == "x-ratelimit-global", do: v
          end)

        Logger.info(
          "Rate limited on webhook_id=#{webhook_id} - Delay: #{retry_after_ms}ms | Scope: #{scope || "unknown"} | Bucket: #{bucket || "unknown"} | Global: #{global || "false"}"
        )

        capped_retry_after_ms = min(retry_after_ms, 3_600_000)
        if capped_retry_after_ms > 0 do
          if global == "true" do
            Logger.debug("Caching global rate limit for #{capped_retry_after_ms}ms")
            redix_command(["PSETEX", "rl:global", Integer.to_string(capped_retry_after_ms), "1"])
          else
            redix_command(["PSETEX", "rl:#{webhook_id}", Integer.to_string(capped_retry_after_ms), "1"])
          end
        end

        {:error, {:rate_limited, retry_after_ms}}

      {:ok, %{status: 404, body: resp_body}} ->
        case Jason.decode(resp_body) do
          {:ok, %{"code" => 10008}} ->
            Logger.info("Webhook_id=#{webhook_id} returned 10008. Treating as transient to handle eventual consistency.")
            {:error, :message_not_found_transient}

          {:ok, %{"code" => code}} when code in [10003, 10015] ->
            Logger.warning(
              "Dropping webhook_id=#{webhook_id} status=404 body=#{resp_body} (invalid webhook)"
            )

            {:error, :invalid_webhook}

          _ ->
            Logger.warning("Treating 404 as transient webhook_id=#{webhook_id} body=#{resp_body}")
            {:error, :network_error}
        end

      {:ok, %{status: status, body: resp_body}} when status in [401, 403] ->
        Logger.warning(
          "Treating #{status} as transient webhook_id=#{webhook_id} body=#{resp_body}"
        )

        {:error, :network_error}

      {:ok, %{status: 400, body: resp_body}} ->
        Logger.error("Bad request webhook_id=#{webhook_id} body=#{resp_body}")
        {:error, :bad_request}

      {:ok, %{status: status, body: resp_body}} when status in 500..599 ->
        Logger.error("Server error #{status} for webhook_id=#{webhook_id}, body=#{resp_body}")
        {:error, {:server_error, status}}

      {:error, reason} ->
        Logger.error("Network error for webhook_id=#{webhook_id}: #{inspect(reason)}")
        {:error, :network_error}

      {:ok, %{status: status}} ->
        Logger.warning(
          "Unexpected status #{status} for webhook_id=#{webhook_id}. Treating as done."
        )

        {:ok, nil}
    end
  end
end
