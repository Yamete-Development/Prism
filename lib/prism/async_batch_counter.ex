defmodule Prism.AsyncBatchCounter do
  @moduledoc """
  Lock-free counter for tracking the number of in-flight async batch tasks.

  Uses `:atomics` for O(1) reads and writes with no process mailbox
  contention, replacing the previous `Supervisor.count_children/1` approach
  which serialized all Broadway processors through the TaskSup GenServer.
  """

  @counter_index 1

  @doc """
  Initialises the atomic counter. Called once from `Application.start/2`.
  """
  @spec init() :: :ok
  def init do
    ref = :atomics.new(1, signed: true)
    :persistent_term.put(__MODULE__, ref)
    :ok
  end

  @doc """
  Returns the current number of in-flight async batches.
  """
  @spec count() :: integer()
  def count do
    :atomics.get(ref(), @counter_index)
  end

  @doc """
  Atomically increments the counter (call on task spawn).
  """
  @spec increment() :: :ok
  def increment do
    :atomics.add(ref(), @counter_index, 1)
    :ok
  end

  @doc """
  Atomically decrements the counter (call on task exit).
  """
  @spec decrement() :: :ok
  def decrement do
    :atomics.add(ref(), @counter_index, -1)
    :ok
  end

  defp ref, do: :persistent_term.get(__MODULE__)
end
