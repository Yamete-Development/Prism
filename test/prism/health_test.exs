defmodule Prism.HealthTest do
  use ExUnit.Case, async: false
  import Plug.Test

  test "liveness does not depend on downstream services" do
    conn = conn(:get, "/live") |> Prism.Health.call([])
    assert conn.status == 200
    assert conn.resp_body == "live\n"
  end

  test "readiness fails closed when critical processes are unavailable" do
    conn = conn(:get, "/ready") |> Prism.Health.call([])
    assert conn.status == 503
    assert conn.resp_body == "not ready\n"
  end

  test "unknown paths return not found" do
    conn = conn(:get, "/unknown") |> Prism.Health.call([])
    assert conn.status == 404
  end
end
