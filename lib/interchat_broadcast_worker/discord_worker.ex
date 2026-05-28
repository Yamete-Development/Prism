defmodule InterchatBroadcastWorker.DiscordWorker do
  require Logger

  @doc """
  Sends the webhook content to a guild's discord webhook URL with retry logic.
  """
  def send_to_discord_with_retries(%{"webhook_id" => webhook_id, "webhook_token" => webhook_token} = target, content) do
    if is_binary(webhook_id) and is_binary(webhook_token) do
      url = "https://discord.com/api/webhooks/#{webhook_id}/#{webhook_token}"

      thread_id = Map.get(target, "thread_id")
      url = if is_binary(thread_id), do: url <> "?thread_id=#{thread_id}", else: url

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

      do_http_post(url, headers, body, webhook_id, content)
    else
      Logger.warning("Invalid webhook data. Skipping.")
      :ok
    end
  end

  def send_to_discord_with_retries(target, _content) do
    Logger.warning("Missing webhook data in target: #{inspect(target)}. Skipping.")
    :ok
  end

  defp do_http_post(url, headers, body, webhook_id, content) do
    # 1. Send HTTP POST via Finch
    case Finch.build(:post, url, headers, body) |> Finch.request(DiscordFinch) do
      {:ok, %{status: status}} when status in 200..299 ->
        # 2. Success!
        :ok

      {:ok, %{status: 429, body: resp_body}} ->
        # 3. Rate Limited: Sleep and retry
        # Discord returns retry_after in seconds (float). Process.sleep needs milliseconds.
        payload = Jason.decode!(resp_body)
        retry_after_ms = trunc(payload["retry_after"] * 1000)

        Logger.warning("Rate limited (429) for webhook_id: #{webhook_id}. Retrying after #{retry_after_ms}ms")
        Process.sleep(retry_after_ms)
        do_http_post(url, headers, body, webhook_id, content)

      {:ok, %{status: status}} when status in [401, 403, 404] ->
        # 4. Invalid Requests (Unauthorized, Forbidden, Not Found).
        # CRITICAL: Discord bans IPs with > 10,000 invalid requests in 10 minutes.
        # Do NOT retry. Drop the webhook and treat as 'done' so the batch can finish.
        Logger.warning("Dropping invalid webhook request: #{status} for webhook_id #{webhook_id}")
        :ok

      {:error, reason} ->
        # 5. Network timeouts/errors. Brief backoff and retry.
        Logger.error("Network error for webhook_id #{webhook_id}: #{inspect(reason)}. Retrying in 1s...")
        Process.sleep(1000)
        do_http_post(url, headers, body, webhook_id, content)

      {:ok, %{status: status}} ->
        # Handle other unexpected HTTP statuses
        Logger.warning("Unexpected status #{status} for webhook_id #{webhook_id}. Treating as done.")
        :ok
    end
  end
end
