defmodule InterchatBroadcastWorker.DiscordWorker do
  require Logger

  @doc """
  Sends the webhook content to a guild's discord webhook URL with retry logic.
  Returns `{:ok, message_id}` on success or `{:error, reason}` on failure.
  """
  def send_to_discord_with_retries(%{"webhook_id" => webhook_id, "webhook_token" => webhook_token} = target, content) do
    if is_binary(webhook_id) and is_binary(webhook_token) do
      # wait=true tells Discord to return the created message object
      url = "https://discord.com/api/webhooks/#{webhook_id}/#{webhook_token}?wait=true"

      thread_id = Map.get(target, "thread_id")
      url = if is_binary(thread_id), do: url <> "&thread_id=#{thread_id}", else: url

      mutations = Map.get(target, "mutations") || %{}
      mention_id = Map.get(mutations, "reply_mention_id")

      content = if is_binary(mention_id) do
        current_content = Map.get(content, "content", "")
        Map.put(content, "content", current_content <> " <@#{mention_id}>")
      else
        content
      end

      headers = [{"Content-Type", "application/json"}]
      body = Jason.encode!(content)

      do_http_post(url, headers, body, webhook_id)
    else
      Logger.warning("Invalid webhook data. Skipping.")
      {:error, :invalid_webhook}
    end
  end

  def send_to_discord_with_retries(target, _content) do
    Logger.warning("Missing webhook data in target: #{inspect(target)}. Skipping.")
    {:error, :missing_webhook}
  end

  defp do_http_post(url, headers, body, webhook_id, attempt \\ 1) do
    case Finch.build(:post, url, headers, body) |> Finch.request(DiscordFinch) do
      {:ok, %{status: status, body: resp_body}} when status in 200..299 ->
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

      {:ok, %{status: 429, body: resp_body}} ->
        payload = Jason.decode!(resp_body)
        retry_after_ms = trunc(payload["retry_after"] * 1000)

        Logger.warning(
          "Rate limited (429) webhook_id=#{webhook_id} " <>
          "attempt=#{attempt} retry_after=#{retry_after_ms}ms"
        )
        Process.sleep(retry_after_ms)
        do_http_post(url, headers, body, webhook_id, attempt + 1)

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
          do_http_post(url, headers, body, webhook_id, attempt + 1)
        end

      {:ok, %{status: status}} ->
        Logger.warning(
          "Unexpected status #{status} for webhook_id=#{webhook_id}. Treating as done."
        )
        {:ok, nil}
    end
  end
end
