defmodule Prism.AsyncBatchCounter do
  @moduledoc """
  Lock-free counter for tracking the number of in-flight async batch tasks
  and the total number of processed batches and messages for metrics logging.

  Uses `:atomics` for O(1) reads and writes with no process mailbox
  contention, replacing the previous `Supervisor.count_children/1` approach
  which serialized all Broadway processors through the TaskSup GenServer.
  """

  @inflight_index 1
  @processed_batches_index 2
  @processed_targets_index 3

  @doc """
  Initialises the atomic counter. Called once from `Application.start/2`.
  """
  @spec init() :: :ok
  def init do
    ref = :atomics.new(3, signed: true)
    :persistent_term.put(__MODULE__, ref)
    :ok
  end

  @doc """
  Returns the current number of in-flight async batches.
  """
  @spec count() :: integer()
  def count do
    :atomics.get(ref(), @inflight_index)
  end

  @doc """
  Returns the total number of processed batches.
  """
  @spec get_processed_batches() :: integer()
  def get_processed_batches do
    :atomics.get(ref(), @processed_batches_index)
  end

  @doc """
  Returns the total number of processed targets.
  """
  @spec get_processed_targets() :: integer()
  def get_processed_targets do
    :atomics.get(ref(), @processed_targets_index)
  end

  @doc """
  Atomically increments the counter (call on task spawn).
  """
  @spec increment() :: :ok
  def increment do
    :atomics.add(ref(), @inflight_index, 1)
    :ok
  end

  @doc """
  Atomically decrements the counter (call on task exit).
  """
  @spec decrement() :: :ok
  def decrement do
    :atomics.add(ref(), @inflight_index, -1)
    :ok
  end

  @doc """
  Atomically adds to the processed counters.
  """
  @spec add_processed(integer()) :: :ok
  def add_processed(target_count) do
    :atomics.add(ref(), @processed_batches_index, 1)
    :atomics.add(ref(), @processed_targets_index, target_count)
    :ok
  end

  defp ref, do: :persistent_term.get(__MODULE__)
end
