ExUnit.start()

# Stop background consumers so they don't interfere with our unit tests
Supervisor.terminate_child(Prism.Supervisor, Prism.DelayedScheduler)
Supervisor.terminate_child(Prism.Supervisor, :retry_broadway)
Supervisor.terminate_child(Prism.Supervisor, :fanout_broadway_fast)
Supervisor.terminate_child(Prism.Supervisor, :fanout_broadway_slow)
