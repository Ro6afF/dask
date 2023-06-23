defmodule Dask.Executor do
  use GenServer

  def start_link(_ \\ []) do
    GenServer.start_link(__MODULE__, [], name: __MODULE__)
  end

  def execute(task) do
    GenServer.cast(__MODULE__, {:exec, task})
  end

  @impl GenServer
  def init(_) do
    Supervisor.start_link([{Task.Supervisor, name: Dask.Executor.Elixir}], strategy: :one_for_one)

    {:ok, %{}}
  end

  @impl GenServer
  def handle_cast({:exec, task}, state) do
    {:ok, %{id: result_id}} =
      Dask.Repo.insert(%Dask.DB.Result{
        time_of_start: DateTime.utc_now(),
        executor: Atom.to_string(node()),
        task: task
      })

    port = Port.open({:spawn, task.command}, [:exit_status])

    {:noreply, Map.put(state, port, result_id)}
  end

  @impl GenServer
  def handle_info({port, {:exit_status, status}}, state) when is_port(port) do
    result = Dask.Repo.get(Dask.DB.Result, Map.get(state, port))

    changeset =
      Ecto.Changeset.cast(
        result,
        %{exit_status: "#{status}", time_of_finish: DateTime.utc_now()},
        [
          :exit_status,
          :time_of_finish
        ]
      )

    Dask.Repo.update(changeset)

    {:noreply, Map.drop(state, [port])}
  end

  @impl GenServer
  def handle_info({port, {:data, data}}, state) when is_port(port) do
    result = Dask.Repo.get(Dask.DB.Result, Map.get(state, port))

    new_output =
      if not is_nil(result.output) do
        result.output
      else
        ""
      end <>
        List.to_string(data)

    changeset = Ecto.Changeset.cast(result, %{output: new_output}, [:output])

    Dask.Repo.update(changeset)

    Dask.Scheduler.reschedule(result.task_id)

    {:noreply, state}
  end

  @impl GenServer
  def handle_info({ref, return}, state) when is_reference(ref) do
    result = Dask.Repo.get(Dask.DB.Result, Map.get(state, ref))

    changeset = Ecto.Changeset.cast(result, %{output: return}, [:output])

    Dask.Repo.update(changeset)

    {:noreply, state}
  end

  @impl GenServer
  def handle_info({:DOWN, ref, _, _, status}, state) do
    result = Dask.Repo.get(Dask.DB.Result, Map.get(state, ref))

    changeset =
      Ecto.Changeset.cast(result, %{exit_status: status, time_of_finish: DateTime.utc_now()}, [
        :exit_status,
        :time_of_finish
      ])

    Dask.Repo.update(changeset)

    Dask.Scheduler.reschedule(result.task_id)

    {:noreply, Map.drop(state, [ref])}
  end
end
