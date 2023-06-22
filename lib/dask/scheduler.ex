defmodule Dask.Scheduler do
  use GenServer

  import Ecto.Query

  def start_link(_ \\ nil) do
    GenServer.start_link(__MODULE__, nil, name: __MODULE__)
  end

  def refresh do
    GenServer.cast(__MODULE__, :refresh)
  end

  @impl GenServer
  def init(_init_arg) do
    {:ok, nil}
  end

  @impl GenServer
  def handle_cast(:refresh, state) do
    node = Atom.to_string(node())
    time_now = DateTime.utc_now()

    Dask.Repo.transaction(fn ->
      tasks_query =
        from(t in Dask.DB.Task,
          where:
            t.scheduled_time < ^time_now and
              (^node in t.possible_nodes or is_nil(t.possible_nodes)) and
              is_nil(t.executor),
          limit: 1,
          lock: "FOR UPDATE"
        )

      if Dask.Repo.exists?(tasks_query) do
        selected = Dask.Repo.one(tasks_query)
        changeset = Ecto.Changeset.cast(selected, %{executor: node}, [:executor])

        Dask.Repo.update(changeset)
        Dask.Executor.execute(selected)
      end
    end)

    {:noreply, state}
  end
end
