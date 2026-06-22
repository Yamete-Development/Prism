defmodule MockDiscordServer do
  @moduledoc """
  Controllable mock HTTP server for Discord webhook API testing.

  Uses ETS tables for stub configuration — no GenServer needed for state.
  Bandit is started externally (via `setup_all`) to manage the HTTP lifecycle.

  ## Tables
    - `:mock_discord_stubs` — `{:set, :public, :named_table}` mapping webhook_id → response template
    - `:mock_discord_requests` — `{:ordered_set, :public, :named_table}` recording all received requests
  """

  @stubs_table :mock_discord_stubs
  @requests_table :mock_discord_requests
  @default_key :__default__

  # ---------------------------------------------------------------------------
  # Public API — table management
  # ---------------------------------------------------------------------------

  @doc "Ensures both ETS tables exist. Idempotent — safe to call multiple times."
  def ensure_tables do
    if :ets.whereis(@stubs_table) == :undefined,
      do: :ets.new(@stubs_table, [:set, :public, :named_table])

    if :ets.whereis(@requests_table) == :undefined,
      do: :ets.new(@requests_table, [:ordered_set, :public, :named_table])
  end

  @doc "Clears all stubs and requests from the tables."
  def reset do
    if :ets.whereis(@stubs_table) != :undefined, do: :ets.delete_all_objects(@stubs_table)
    if :ets.whereis(@requests_table) != :undefined, do: :ets.delete_all_objects(@requests_table)
  end

  # ---------------------------------------------------------------------------
  # Public API — stubbing
  # ---------------------------------------------------------------------------

  @doc "Registers a response stub for a specific webhook ID."
  def stub(webhook_id, %{status: _} = template) do
    :ets.insert(@stubs_table, {webhook_id, template})
    :ok
  end

  @doc "Registers a default response for unmatched webhooks."
  def stub_default(template) do
    :ets.insert(@stubs_table, {@default_key, template})
    :ok
  end

  @doc "Convenience: stubs a 200 OK response with a custom or default message ID."
  def stub_ok(webhook_id, msg_id \\ "mock_msg_123") do
    stub(webhook_id, %{
      status: 200,
      headers: [
        {"x-ratelimit-limit", "50"},
        {"x-ratelimit-remaining", "49"},
        {"x-ratelimit-reset-after", "1.0"}
      ],
      body: Jason.encode!(%{id: msg_id})
    })
  end

  @doc "Stubs a Cloudflare HTML 429 response."
  def stub_cloudflare_429(webhook_id, retry_after_sec \\ 5.0) do
    stub(webhook_id, %{
      status: 429,
      headers: [
        {"retry-after", to_string(retry_after_sec)},
        {"cf-ray", "abc123def456"},
        {"server", "cloudflare"}
      ],
      body: """
      <html>
      <head><title>429 Too Many Requests</title></head>
      <body>
      <center><h1>429 Too Many Requests</h1></center>
      <hr><center>cloudflare</center>
      </body>
      </html>
      """
    })
  end

  @doc "Stubs a Discord global 429 response (JSON, global: true)."
  def stub_discord_global_429(webhook_id, retry_after_sec \\ 1.5) do
    stub(webhook_id, %{
      status: 429,
      headers: [
        {"retry-after", to_string(retry_after_sec)},
        {"x-ratelimit-limit", "50"},
        {"x-ratelimit-remaining", "0"},
        {"x-ratelimit-reset-after", to_string(retry_after_sec)},
        {"x-ratelimit-global", "true"}
      ],
      body:
        Jason.encode!(%{
          retry_after: retry_after_sec,
          global: true,
          message: "You are being rate limited."
        })
    })
  end

  @doc "Stubs a Discord per-webhook 429 response with configurable scope."
  def stub_discord_per_webhook_429(webhook_id, opts \\ []) do
    retry_after_sec = Keyword.get(opts, :retry_after_sec, 2.0)
    scope = Keyword.get(opts, :scope, "user")
    limit = Keyword.get(opts, :limit, 5)

    stub(webhook_id, %{
      status: 429,
      headers: [
        {"retry-after", to_string(retry_after_sec)},
        {"x-ratelimit-limit", to_string(limit)},
        {"x-ratelimit-remaining", "0"},
        {"x-ratelimit-reset-after", to_string(retry_after_sec)},
        {"x-ratelimit-scope", scope},
        {"x-ratelimit-bucket", "abc123"}
      ],
      body:
        Jason.encode!(%{
          retry_after: retry_after_sec,
          global: false,
          message: "You are being rate limited."
        })
    })
  end

  @doc "Stubs a server error response."
  def stub_server_error(webhook_id, status \\ 500) do
    stub(webhook_id, %{
      status: status,
      headers: [],
      body: Jason.encode!(%{message: "Internal Server Error"})
    })
  end

  # ---------------------------------------------------------------------------
  # Public API — request inspection
  # ---------------------------------------------------------------------------

  @doc "Returns all recorded requests ordered oldest-first."
  def requests do
    if :ets.whereis(@requests_table) != :undefined do
      @requests_table
      |> :ets.tab2list()
      |> Enum.sort_by(fn {ts, _} -> ts end)
      |> Enum.map(fn {_, req} -> req end)
    else
      []
    end
  end

  @doc "Returns the count of recorded requests."
  def request_count do
    if :ets.whereis(@requests_table) != :undefined, do: :ets.info(@requests_table, :size), else: 0
  end

  @doc "Clears all recorded requests."
  def clear_requests do
    if :ets.whereis(@requests_table) != :undefined, do: :ets.delete_all_objects(@requests_table)
    :ok
  end

  # ---------------------------------------------------------------------------
  # Public API — helper: start Bandit with the plug router
  # ---------------------------------------------------------------------------

  @doc """
  Starts a Bandit HTTP server on the given port with the mock router plug.
  Returns the Bandit PID for supervision.
  """
  def start_bandit(port \\ 4002) do
    ensure_tables()

    if :ets.lookup(@stubs_table, @default_key) == [] do
      :ets.insert(
        @stubs_table,
        {@default_key, %{status: 200, headers: [], body: Jason.encode!(%{id: "fallback"})}}
      )
    end

    Bandit.start_link(scheme: :http, port: port, plug: MockDiscordServer.Router)
  end
end

defmodule MockDiscordServer.Router do
  @moduledoc false
  use Plug.Router

  plug(:match)

  plug(Plug.Parsers,
    parsers: [:json],
    json_decoder: Jason
  )

  plug(:dispatch)

  match _ do
    conn = Plug.Conn.fetch_query_params(conn)
    path = conn.request_path
    webhook_id = extract_webhook_id(path)

    lookup_key =
      if webhook_id && :ets.lookup(:mock_discord_stubs, webhook_id) != [] do
        webhook_id
      else
        :__default__
      end

    stub =
      case :ets.lookup(:mock_discord_stubs, lookup_key) do
        [{^lookup_key, s}] -> s
        [] -> %{status: 200, headers: [], body: Jason.encode!(%{id: "unknown"})}
      end

    request_info = %{
      method: conn.method,
      path: path,
      query_string: conn.query_string,
      webhook_id: webhook_id,
      headers: Map.new(conn.req_headers, fn {k, v} -> {String.downcase(k), v} end),
      body: conn.body_params
    }

    :ets.insert(:mock_discord_requests, {System.monotonic_time(:millisecond), request_info})

    conn
    |> then(fn c ->
      Enum.reduce(stub.headers || [], c, fn {name, value}, acc ->
        Plug.Conn.put_resp_header(acc, name, value)
      end)
    end)
    |> Plug.Conn.put_resp_content_type("application/json")
    |> Plug.Conn.send_resp(stub.status, stub.body || "")
  end

  defp extract_webhook_id(path) do
    case Regex.run(~r|/api/webhooks/([^/]+)/|, path) do
      [_, id] -> id
      nil -> nil
    end
  end
end
