defmodule Prism.SchemaRegistry do
  @moduledoc """
  A lightweight, ETS-backed client for the Confluent Schema Registry.
  Caches schema IDs and fetches schemas via Finch.
  This avoids the need for heavy, unmaintained third-party hex packages
  while adhering strictly to the Confluent Wire Format best practices.
  """
  use GenServer
  require Logger

  @table :prism_schema_registry

  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @impl true
  def init(_opts) do
    :ets.new(@table, [:set, :public, :named_table, read_concurrency: true])
    {:ok, %{}}
  end

  @doc """
  Retrieves a schema string by its ID, fetching from the registry if not cached.
  """
  def get_schema(schema_id) when is_integer(schema_id) do
    case :ets.lookup(@table, {:id, schema_id}) do
      [{_, schema}] -> {:ok, schema}
      [] -> fetch_and_cache_schema(schema_id)
    end
  end

  @doc """
  Registers a schema (if needed) and returns its ID, utilizing local cache.
  """
  def get_schema_id(subject, schema_string)
      when is_binary(subject) and is_binary(schema_string) do
    case :ets.lookup(@table, {:subject, subject}) do
      [{_, id}] -> {:ok, id}
      [] -> register_and_cache_schema(subject, schema_string)
    end
  end

  defp url do
    System.get_env("SCHEMA_REGISTRY_URL") || "http://localhost:8081"
  end

  defp fetch_and_cache_schema(schema_id) do
    req_url = "#{url()}/schemas/ids/#{schema_id}"
    req = Finch.build(:get, req_url)

    case Finch.request(req, DiscordFinch) do
      {:ok, %Finch.Response{status: 200, body: body}} ->
        schema = Jason.decode!(body)["schema"]
        :ets.insert(@table, {{:id, schema_id}, schema})
        {:ok, schema}

      {:ok, %Finch.Response{status: status, body: body}} ->
        Logger.error("[SchemaRegistry] Error fetching #{schema_id}: #{status} - #{body}")
        {:error, :not_found}

      {:error, reason} ->
        Logger.error("[SchemaRegistry] Connection error: #{inspect(reason)}")
        {:error, reason}
    end
  end

  defp register_and_cache_schema(subject, schema_string) do
    req_url = "#{url()}/subjects/#{subject}/versions"
    body = Jason.encode!(%{schema: schema_string, schemaType: "PROTOBUF"})
    headers = [{"content-type", "application/vnd.schemaregistry.v1+json"}]
    req = Finch.build(:post, req_url, headers, body)

    case Finch.request(req, DiscordFinch) do
      {:ok, %Finch.Response{status: status, body: resp_body}} when status in 200..299 ->
        id = Jason.decode!(resp_body)["id"]
        :ets.insert(@table, {{:subject, subject}, id})
        :ets.insert(@table, {{:id, id}, schema_string})
        {:ok, id}

      {:ok, %Finch.Response{status: status, body: resp_body}} ->
        Logger.error("[SchemaRegistry] Error registering #{subject}: #{status} - #{resp_body}")
        {:error, :registration_failed}

      {:error, reason} ->
        Logger.error("[SchemaRegistry] Connection error: #{inspect(reason)}")
        {:error, reason}
    end
  end
end
