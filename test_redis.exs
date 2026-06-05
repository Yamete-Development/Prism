{:ok, conn} = Redix.start_link(host: "localhost", port: 6379)
IO.inspect(Redix.command(conn, ["PING"]))
