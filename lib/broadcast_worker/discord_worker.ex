defmodule BroadcastWorker.DiscordWorker do
  require Logger

  @doc """
  Sends the webhook content to a guild's discord webhook URL with retry logic.
  Returns `{:ok, message_id}` on success or `{:error, reason}` on failure.
  """
  def process_target(action, target, content \\ %{}, batch_id \\ nil)

  def process_target(action, %{"webhook_id" => webhook_id, "webhook_token" => webhook_token} = target, content, batch_id) do
    if is_binary(webhook_id) and is_binary(webhook_token) do
      base_url = "https://discord.com/api/webhooks/#{webhook_id}/#{webhook_token}"
      thread_id = Map.get(target, "thread_id")
      message_id = Map.get(target, "message_id")

      mutations = Map.get(target, "mutations") || %{}
      mention_id = Map.get(mutations, "reply_mention_id")

      content = if is_binary(mention_id) and action in ["execute", "edit"] do
        current_content = Map.get(content, "content", "")
        Map.put(content, "content", "<@#{mention_id}> " <> current_content)
      else
        content
      end

      headers = [{"Content-Type", "application/json"}]
      body = if action == "delete", do: "", else: Jason.encode!(content)

      checkpoint_key = "checkpoint:#{action}:#{batch_id}:#{webhook_id}"

      cached_result = if batch_id do
        case Redix.command(:my_redix, ["GET", checkpoint_key]) do
          {:ok, "done"} -> {:ok, nil}
          {:ok, cached_msg_id} when is_binary(cached_msg_id) -> {:ok, cached_msg_id}
          _ -> nil
        end
      else
        nil
      end

      if cached_result do
        cached_result
      else
        case build_request(action, base_url, message_id, thread_id) do
          {:ok, method, url} ->
            result = do_http_request(method, url, headers, body, webhook_id)
            if batch_id do
              case result do
                {:ok, msg_id} when is_binary(msg_id) ->
                  Redix.command(:my_redix, ["SETEX", checkpoint_key, "86400", msg_id])
                {:ok, nil} ->
                  Redix.command(:my_redix, ["SETEX", checkpoint_key, "86400", "done"])
                _ -> :ok
              end
            end
            result
          {:error, reason} ->
            {:error, reason}
        end
      end
    else
      Logger.warning("Invalid webhook data. Skipping.")
      {:error, :invalid_webhook}
    end
  end

  def process_target(_action, target, _content, _batch_id) do
    Logger.warning("Missing webhook data in target: #{inspect(target)}. Skipping.")
    {:error, :missing_webhook}
  end

  defp build_request("execute", base_url, _msg_id, thread_id) do
    url = base_url <> "?wait=true"
    url = if is_binary(thread_id), do: url <> "&thread_id=#{thread_id}", else: url
    {:ok, :post, url}
  end

  defp build_request("edit", base_url, msg_id, thread_id) when is_binary(msg_id) do
    url = base_url <> "/messages/#{msg_id}"
    url = if is_binary(thread_id), do: url <> "?thread_id=#{thread_id}", else: url
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

  defp do_http_request(method, url, headers, body, webhook_id, attempt \\ 1) do
    case Finch.build(method, url, headers, body) |> Finch.request(DiscordFinch) do
      {:ok, %{status: status, body: resp_body}} when status in 200..299 ->
        if method == :post do
          case Jason.decode(resp_body) do
            {:ok, %{"id" => msg_id}} ->
              {:ok, msg_id}

            {:ok, parsed} ->
              Logger.warning(
                "Webhook #{webhook_id} returned #{status} but no 'id' in body: " <>
                "#{inspect(parsed)}"
              )
              {:ok, nil}

            {:error, decode_err} ->
              Logger.warning(
                "Webhook #{webhook_id} returned #{status} but body is not valid JSON: " <>
                "#{inspect(decode_err)} body=#{inspect(resp_body)}"
              )
              {:ok, nil}
          end
        else
          {:ok, nil}
        end

      {:ok, %{status: 429, body: resp_body}} ->
        payload = Jason.decode!(resp_body)
        retry_after_ms = trunc(payload["retry_after"] * 1000)

        Logger.warning(
          "Rate limited (429) webhook_id=#{webhook_id} " <>
          "attempt=#{attempt} retry_after=#{retry_after_ms}ms"
        )
        Process.sleep(retry_after_ms)
        do_http_request(method, url, headers, body, webhook_id, attempt + 1)

      {:ok, %{status: status, body: resp_body}} when status in [401, 403, 404] ->
        Logger.warning(
          "Dropping webhook_id=#{webhook_id} status=#{status} body=#{resp_body} " <>
          "(invalid webhook, no retry)"
        )
        {:error, :invalid_webhook}

      {:ok, %{status: 400, body: resp_body}} ->
        Logger.error(
          "Bad request webhook_id=#{webhook_id} body=#{resp_body} " <>
          "(check payload field names/types)"
        )
        {:error, :bad_request}

      {:ok, %{status: status, body: resp_body}} when status in 500..599 ->
        if attempt >= 3 do
          Logger.error("Discord 5xx error webhook_id=#{webhook_id} status=#{status} body=#{resp_body}. Giving up.")
          {:error, {:server_error, status}}
        else
          Logger.warning("Discord 5xx error webhook_id=#{webhook_id} status=#{status} attempt=#{attempt}. Retrying in 2s...")
          Process.sleep(2000)
          do_http_request(method, url, headers, body, webhook_id, attempt + 1)
        end

      {:error, reason} ->
        if attempt >= 5 do
          Logger.error(
            "Giving up on webhook_id=#{webhook_id} after #{attempt} attempts: " <>
            "#{inspect(reason)}"
          )
          {:error, :network_error}
        else
          Logger.warning(
            "Network error webhook_id=#{webhook_id} attempt=#{attempt}: " <>
            "#{inspect(reason)}. Retrying in 1s..."
          )
          Process.sleep(1000)
          do_http_request(method, url, headers, body, webhook_id, attempt + 1)
        end

      {:ok, %{status: status}} ->
        Logger.warning(
          "Unexpected status #{status} for webhook_id=#{webhook_id}. Treating as done."
        )
        {:ok, nil}
    end
  end
end
