defmodule Prism.FanoutBroadway.Preflight do
  @moduledoc """
  Batched pre-flight checks for all targets in a batch.
  Pipelines checkpoint GETs and rate-limit EVALs before fan-out to eliminate
  per-target Redis round-trips.

  Used by `Prism.FanoutBroadway.Batch.process_batch/10` when
  `Prism.Config.preflight_batching_enabled?/0` is true.
  """

  alias Prism.Helpers
  require Logger

  @type checkpoint_result :: :not_found | {:done} | {:ok, String.t()}
  @type rate_limit_result :: {:ok, integer()} | {:blocked, integer()}
  @type preflight_map :: %{
          target: map(),
          webhook_id: String.t(),
          preflight: %{checkpoint: checkpoint_result(), rate_limit: rate_limit_result()}
        }

  @doc """
  Runs batched pre-flight checks for all targets.

  Returns a list of `preflight_map` structs, one per target, or
  `{:error, reason}` if either pipeline fails (caller should fall back to
  per-target Redis calls).
  """
  @spec run([map()], String.t(), String.t() | nil) :: {:ok, [preflight_map()]} | {:error, term()}
  def run(targets, action, batch_id)

  def run(targets, action, batch_id) when is_list(targets) and is_binary(batch_id) do
    method_str = Helpers.action_to_method_string(action)

    target_infos =
      Enum.map(targets, fn target ->
        webhook_id = Map.get(target, "webhook_id")

        %{
          target: target,
          webhook_id: webhook_id,
          checkpoint_key:
            Helpers.checkpoint_key(
              action,
              batch_id,
              webhook_id,
              Map.get(target, "polarizer_action_id")
            ),
          bucket_method: method_str
        }
      end)

    with {:ok, checkpoint_results} <- pipeline_checkpoints(target_infos),
         {:ok, rate_limit_results} <- pipeline_rate_limits(target_infos) do
      preflights =
        Enum.zip_with(
          [target_infos, checkpoint_results, rate_limit_results],
          fn [ti, ck_res, rl_res] ->
            %{
              target: ti.target,
              webhook_id: ti.webhook_id,
              preflight: %{
                checkpoint: parse_checkpoint_result(ck_res),
                rate_limit: parse_rate_limit_result(rl_res)
              }
            }
          end
        )

      {:ok, preflights}
    end
  end

  def run(_targets, _action, nil), do: {:ok, []}

  # ── Pipeline helpers ────────────────────────────────────────────────

  defp pipeline_checkpoints(target_infos) do
    commands = Enum.map(target_infos, fn ti -> ["GET", ti.checkpoint_key] end)

    case Helpers.redix_pipeline(commands) do
      {:ok, results} ->
        {:ok, results}

      {:error, reason} ->
        Logger.warning("Preflight checkpoint pipeline failed: #{inspect(reason)}")
        {:error, reason}
    end
  end

  defp pipeline_rate_limits(target_infos) do
    acquire_targets =
      Enum.map(target_infos, fn ti ->
        {ti.webhook_id, ti.bucket_method}
      end)

    commands = Prism.RateLimit.Bucket.acquire_pipeline_commands(acquire_targets)

    case Helpers.redix_pipeline(commands) do
      {:ok, results} ->
        {:ok, results}

      {:error, reason} ->
        Logger.warning("Preflight rate-limit pipeline failed: #{inspect(reason)}")
        {:error, reason}
    end
  end

  # ── Result parsers ──────────────────────────────────────────────────

  defp parse_checkpoint_result("done"), do: {:done}

  defp parse_checkpoint_result(msg_id) when is_binary(msg_id) and byte_size(msg_id) > 0,
    do: {:ok, msg_id}

  defp parse_checkpoint_result(_), do: :not_found

  defp parse_rate_limit_result([1, remaining, _]) when is_integer(remaining),
    do: {:ok, remaining}

  defp parse_rate_limit_result([0, _, ttl_ms]) when is_integer(ttl_ms),
    do: {:blocked, ttl_ms}

  defp parse_rate_limit_result(_), do: {:ok, -1}
end
