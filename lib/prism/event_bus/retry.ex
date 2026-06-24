defmodule Prism.EventBus.Retry do
  @moduledoc """
  Retry logic with exponential backoff for event bus processing failures.
  Handles transport-level failures (deserialization errors, handler crashes)
  — distinct from Prism's domain-specific webhook retry in DelayedQueue.
  """

  @doc """
  Calculates the backoff delay in milliseconds for a given attempt number.

  Uses exponential backoff with base * 2^(attempt-1), capped at max_ms.

  ## Examples
      iex> backoff_ms(1, 1000)
      1000
      iex> backoff_ms(2, 1000)
      2000
      iex> backoff_ms(3, 1000)
      4000
      iex> backoff_ms(3, 1000, 3000)
      3000
  """
  @spec backoff_ms(pos_integer(), pos_integer(), pos_integer()) :: pos_integer()
  def backoff_ms(attempt, base_ms, max_ms \\ 30_000) do
    delay = base_ms * trunc(:math.pow(2, attempt - 1))
    min(delay, max_ms)
  end

  @doc """
  Returns whether a retry should be attempted based on current attempt and max_retries.
  Attempt 1 is the initial delivery, so retries happen on attempts 2..max_retries.
  """
  @spec should_retry?(pos_integer(), pos_integer()) :: boolean()
  def should_retry?(attempt, max_retries) do
    attempt <= max_retries
  end

  @doc """
  Sleeps for the calculated backoff duration for the given attempt.
  Only sleeps if attempt > 1 (initial delivery has no backoff).
  """
  @spec sleep_for_retry(pos_integer(), pos_integer(), pos_integer()) :: :ok
  def sleep_for_retry(1, _base_ms, _max_ms), do: :ok

  def sleep_for_retry(attempt, base_ms, max_ms) do
    delay = backoff_ms(attempt, base_ms, max_ms)
    Process.sleep(delay)
  end
end
