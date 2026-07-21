defmodule Prism.CongestionWindowTest do
  use ExUnit.Case
  alias Prism.CongestionWindow

  setup do
    # Temporarily enable congestion control for tests
    old_enabled = Application.get_env(:prism, :congestion_control_enabled)
    Application.put_env(:prism, :congestion_control_enabled, true)

    # Reset any state
    if Process.whereis(Prism.CongestionWindow) do
      GenServer.stop(Prism.CongestionWindow)
    end
    
    # Prune ETS table if exists
    if :ets.info(:prism_4xx_budget) != :undefined do
      :ets.delete(:prism_4xx_budget)
    end
    
    # Erase persistent terms
    for key <- [
          :prism_cwnd,
          :prism_cwnd_cubic,
          :prism_cwnd_safety,
          :prism_cwnd_phase,
          :prism_cwnd_w_max
        ] do
      :persistent_term.erase(key)
    end

    {:ok, _pid} = start_supervised(Prism.CongestionWindow)

    on_exit(fn ->
      if old_enabled != nil do
        Application.put_env(:prism, :congestion_control_enabled, old_enabled)
      else
        Application.delete_env(:prism, :congestion_control_enabled)
      end
    end)
    :ok
  end

  test "initialization matches config" do
    assert CongestionWindow.window_size() == 100
    assert CongestionWindow.phase() == :slow_start
    assert CongestionWindow.w_max() == 100.0
    assert CongestionWindow.in_flight() == 0
  end

  test "acquire/release increments and decrements in-flight" do
    assert :ok = CongestionWindow.acquire()
    assert CongestionWindow.in_flight() == 1
    
    assert :ok = CongestionWindow.release()
    assert CongestionWindow.in_flight() == 0
  end

  test "acquire returns backoff when over limit" do
    # Acquire 100 times (cwnd_initial is 100)
    for _ <- 1..100 do
      assert :ok = CongestionWindow.acquire()
    end
    
    # 101st should backoff
    assert {:backoff, delay} = CongestionWindow.acquire()
    assert is_integer(delay)
    assert delay > 0
  end

  test "record_global_429 reduces cwnd to 70%" do
    # Send a global 429
    CongestionWindow.record_global_429()
    
    # Need to wait for cast to process
    :sys.get_state(Prism.CongestionWindow)
    
    assert CongestionWindow.phase() == :concave
    assert CongestionWindow.w_max() == 100.0
    # 100 * 0.7 = 70
    assert CongestionWindow.window_size() == 70
  end

  test "record_cloudflare_429 reduces cwnd to 30%" do
    CongestionWindow.record_cloudflare_429()
    :sys.get_state(Prism.CongestionWindow)
    
    assert CongestionWindow.phase() == :concave
    assert CongestionWindow.w_max() == 100.0
    assert CongestionWindow.window_size() == 30
  end

  test "4xx budget throttles proportional to budget" do
    # Budget is 200. Max is 2000, Min is 10.
    # Safe is 0.3 (60 errors). Critical is 0.8 (160 errors).
    
    # Record 70 errors (utilization = 70/200 = 0.35)
    # t = (0.35 - 0.30) / (0.80 - 0.30) = 0.05 / 0.50 = 0.1
    # expected safety_cwnd = 2000 * 0.9 + 10 * 0.1 = 1800 + 1 = 1801
    
    for _ <- 1..70, do: CongestionWindow.record_4xx()
    
    # Trigger a probe to recompute safety budget
    send(Prism.CongestionWindow, :probe)
    :sys.get_state(Prism.CongestionWindow)
    
    # Cubic is in slow start (cwnd=200 after probe), so effective should be 200
    assert CongestionWindow.safety_window() == 1801.0
    assert CongestionWindow.window_size() == 200 # Since 200 < 1801
  end

  test "4xx budget clamps to cwnd_min at critical threshold" do
    # Record 160+ errors (utilization >= 0.8)
    for _ <- 1..165, do: CongestionWindow.record_4xx()
    
    send(Prism.CongestionWindow, :probe)
    :sys.get_state(Prism.CongestionWindow)
    
    assert CongestionWindow.safety_window() == 10.0
    assert CongestionWindow.window_size() == 10
  end
end
