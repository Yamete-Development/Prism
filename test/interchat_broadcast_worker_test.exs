defmodule InterchatBroadcastWorkerTest do
  use ExUnit.Case

  test "worker module exists" do
    assert InterchatBroadcastWorker.__info__(:module) == InterchatBroadcastWorker
  end
end
