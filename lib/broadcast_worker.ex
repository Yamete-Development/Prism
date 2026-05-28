defmodule BroadcastWorker do
  @moduledoc """
  Top-level namespace for the BroadcastWorker application.
  This application polls a Redis Stream and fans out webhook requests to Discord concurrently.
  """
end
