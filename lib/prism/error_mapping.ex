defmodule Prism.ErrorMapping do
  @moduledoc """
  Centralized error-to-string mappings for callback and batch error classification.
  Returns `{error_string, error_type, extra_map}` where `extra_map` defaults to `%{}`.
  """

  @doc """
  Converts an error reason to a standardized `{error_string, error_type, extra}` tuple.
  """
  @spec to_error_info(term()) :: {String.t(), String.t(), map()}
  def to_error_info({:rate_limited, retry_after_ms}),
    do: {"rate_limited", "transient", %{"retry_after_ms" => retry_after_ms}}

  def to_error_info(:invalid_webhook), do: {"invalid_webhook", "permanent", %{}}
  def to_error_info(:message_not_found), do: {"message_not_found", "permanent", %{}}
  def to_error_info(:bad_request), do: {"bad_request", "transient", %{}}
  def to_error_info(:missing_webhook), do: {"missing_webhook", "permanent", %{}}
  def to_error_info(:invalid_action), do: {"invalid_action", "permanent", %{}}

  def to_error_info({:permanent, detail}),
    do: {"permanent_error", "permanent", %{"detail" => inspect(detail)}}

  def to_error_info({:server_error, _}), do: {"server_error", "transient", %{}}
  def to_error_info(:server_error), do: {"server_error", "transient", %{}}
  def to_error_info(:network_error), do: {"network_error", "transient", %{}}
  def to_error_info(:rate_limited), do: {"rate_limited", "transient", %{}}
  def to_error_info(:task_crashed), do: {"task_crashed", "transient", %{}}
  def to_error_info(:permanent), do: {"permanent_error", "permanent", %{}}

  def to_error_info(reason) when is_atom(reason),
    do: {inspect(reason), "transient", %{}}

  def to_error_info(reason),
    do: {inspect(reason), "transient", %{}}
end
