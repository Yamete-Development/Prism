# Remove `:active_batches` Atomics Counter — Use Supervisor as Source of Truth

## Problem

The `:active_batches` atomics counter mirrors `Task.Supervisor` child count for the async-batch cap check. When a Task dies via exit signal (OOM, supervisor shutdown, `Process.exit`), the `rescue` block in `spawn_async_batch` does NOT catch it — only Elixir exceptions. The `after` block that decrements the counter never runs. The counter drifts permanently upward, eventually blocking all new batches (cap check `current < max_async` always false → infinite bounce loop between delayed queue and retry stream).

`Process.monitor` would fix this but is an anti-pattern: it spawns a companion process per Task, each with its own failure modes.

## Solution

`Task.Supervisor` already tracks exactly which children are alive. Replace all atomics reads with `Supervisor.count_children(Prism.TaskSup).active` — an O(1) GenServer call that returns the supervisor's internal counter. No counter to leak, no cleanup block needed, no monitoring overhead.

## Changes

### 1. `lib/prism/application.ex` — Remove initialization
- **Delete line ~34:** `:persistent_term.put(:active_batches, :atomics.new(1, signed: false))`
- The atomics counter no longer exists.

### 2. `lib/prism/fanout_broadway.ex` — Three sites

**Site A: Cap check in `handle_message/3` (~line 216-219)**
```elixir
# Remove:
max_async = Application.get_env(:prism, :max_async_batches, 300)
active_ref = :persistent_term.get(:active_batches, nil)
current = if active_ref, do: :atomics.get(active_ref, 1), else: 0

# Replace with:
max_async = Application.get_env(:prism, :max_async_batches, 300)
current = Supervisor.count_children(Prism.TaskSup).active
```

**Site B: Increment in `process_batch/11` (~line 292-294)**
```elixir
# Delete:
if ref = :persistent_term.get(:active_batches, nil) do
  :atomics.add(ref, 1, 1)
end
```

**Site C: Decrement in `after` block (~line 535-538)**
```elixir
# Delete entire `after` block:
after
  if ref = :persistent_term.get(:active_batches, nil) do
    :atomics.sub(ref, 1, 1)
  end
end

# The `after` block closing the `try` is removed. The `try/after` becomes just `try` wrapped by `with_span`.
# Actually, the `after` block can be removed entirely — no cleanup needed.
```

**Structural note:** The `try do ... after ... end` wrapper becomes `try do ... end`. The `after` keyword and its body are removed. The `end` that closes `try` must remain at the same indentation level.

### 3. `lib/prism/metrics_logger.ex` — Two reads to unify (~lines 18-26)

```elixir
# Current (reads both Task.Supervisor.children AND atomics):
task_count = Task.Supervisor.children(Prism.TaskSup) |> length()
...
active_batches =
  case :persistent_term.get(:active_batches, nil) do
    nil -> 0
    ref -> :atomics.get(ref, 1)
  end

# Replace with single source:
batch_count = Supervisor.count_children(Prism.TaskSup).active

# Use `batch_count` in the log line instead of both `active_batches` and `task_count`.
# `task_count` is now redundant (same value).
```

Update the log line to use `batch_count` instead of the old two counters.

### 4. `lib/prism/metrics_api.ex` — One read (~lines 25-29)

```elixir
# Current:
active_batches =
  case :persistent_term.get(:active_batches, nil) do
    nil -> 0
    ref -> :atomics.get(ref, 1)
  end
...
metrics = %{..., active_batches: active_batches, ...}

# Replace with:
metrics = %{
  ...,
  active_batches: Supervisor.count_children(Prism.TaskSup).active,
  ...
}
```

## Verification

1. `mix compile --warnings-as-errors` — no warnings
2. `mix format --check-formatted` — all files pass
3. `mix test` — all 87 tests pass, no regressions
4. Confirm `Supervisor` module is available (it's stdlib, always loaded)

## Rollback

Revert commit. No migration needed — no data is persisted, the atomics counter was purely in-memory.
