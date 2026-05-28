defmodule InterchatBroadcastWorker do
  @moduledoc """
  Top-level namespace for the InterchatBroadcastWorker application.
  This application polls a Redis Stream and fans out webhook requests to Discord concurrently.
  """
end
