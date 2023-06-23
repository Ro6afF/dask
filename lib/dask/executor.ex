defmodule Dask.Executor do
  use GenServer

  def start_link(_ \\ []) do
    GenServer.start_link(__MODULE__, [], name: __MODULE__)
  end

  def execute(command) when is_binary(command) do
    GenServer.cast(__MODULE__, {:exec, command})
  end

  def execute(function) when is_function(function) do
    GenServer.cast(__MODULE__, {:exec_func, function})
  end

  def execute(module, function, args) do
    GenServer.cast(__MODULE__, {:exec_mfa, module, function, args})
  end

  @impl GenServer
  def init(_) do
    Supervisor.start_link([{Task.Supervisor, name: Dask.Executor.Elixir}], strategy: :one_for_one)

    {:ok, %{}}
  end

  @impl GenServer
  def handle_cast({:exec, command}, state) do
    {:ok, %{id: job_id}} =
      Dask.Repo.insert(%Dask.Job{time_of_start: DateTime.utc_now(), command: command})

    port = Port.open({:spawn, command}, [:exit_status])

    {:noreply, Map.put(state, port, job_id)}
  end

  @impl GenServer
  def handle_cast({:exec_func, function}, state) do
    {:ok, %{id: job_id}} =
      Dask.Repo.insert(%Dask.Job{time_of_start: DateTime.utc_now(), command: "Anonymous function"})

    task = Task.Supervisor.async_nolink(Dask.Executor.Elixir, function)

    {:noreply, Map.put(state, task.ref, job_id)}
  end

  @impl GenServer
  def handle_cast({:exec_mfa, module, function, arguments}, state) do
    {:ok, %{id: job_id}} =
      Dask.Repo.insert(%Dask.Job{
        time_of_start: DateTime.utc_now(),
        command: inspect({module, function, arguments})
      })

    task = Task.Supervisor.async_nolink(Dask.Executor.Elixir, module, function, arguments)

    {:noreply, Map.put(state, task.ref, job_id)}
  end

  @impl GenServer
  def handle_info({port, {:exit_status, status}}, state) when is_port(port) do
    job = Dask.Repo.get(Dask.Job, Map.get(state, port))

    changeset =
      Ecto.Changeset.cast(job, %{exit_status: "#{status}", time_of_finish: DateTime.utc_now()}, [
        :exit_status,
        :time_of_finish
      ])

    Dask.Repo.update(changeset)

    {:noreply, Map.drop(state, [port])}
  end

  @impl GenServer
  def handle_info({port, {:data, data}}, state) when is_port(port) do
    job = Dask.Repo.get(Dask.Job, Map.get(state, port))

    new_output =
      if not is_nil(job.output) do
        job.output
      else
        ""
      end <>
        List.to_string(data)

    changeset = Ecto.Changeset.cast(job, %{output: new_output}, [:output])

    Dask.Repo.update(changeset)

    {:noreply, state}
  end

  @impl GenServer
  def handle_info({ref, result}, state) when is_reference(ref) do
    job = Dask.Repo.get(Dask.Job, Map.get(state, ref))

    changeset = Ecto.Changeset.cast(job, %{output: result}, [:output])

    Dask.Repo.update(changeset)

    {:noreply, state}
  end

  @impl GenServer
  def handle_info({:DOWN, ref, _, _, status}, state) do
    job = Dask.Repo.get(Dask.Job, Map.get(state, ref))

    changeset =
      Ecto.Changeset.cast(job, %{exit_status: status, time_of_finish: DateTime.utc_now()}, [
        :exit_status,
        :time_of_finish
      ])

    Dask.Repo.update(changeset)

    {:noreply, Map.drop(state, [ref])}
  end
end
