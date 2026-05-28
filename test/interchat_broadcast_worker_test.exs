defmodule BroadcastWorkerTest do
  use ExUnit.Case

  test "worker module exists" do
    assert BroadcastWorker.__info__(:module) == BroadcastWorker
  end
end
