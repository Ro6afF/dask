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
    %{id: job_id} =
      Memento.transaction!(fn ->
        Memento.Query.write(%Dask.DB.Job{time_of_start: DateTime.utc_now(), command: command})
      end)

    port = Port.open({:spawn, command}, [:exit_status])

    {:noreply, Map.put(state, port, job_id)}
  end

  @impl GenServer
  def handle_cast({:exec_func, function}, state) do
    %{id: job_id} =
      Memento.transaction!(fn ->
        Memento.Query.write(%Dask.DB.Job{
          time_of_start: DateTime.utc_now(),
          command: "Anonymous function"
        })
      end)

    task = Task.Supervisor.async_nolink(Dask.Executor.Elixir, function)

    {:noreply, Map.put(state, task.ref, job_id)}
  end

  @impl GenServer
  def handle_cast({:exec_mfa, module, function, arguments}, state) do
    %{id: job_id} =
      Memento.transaction!(fn ->
        Memento.Query.write(%Dask.DB.Job{
          time_of_start: DateTime.utc_now(),
          command: inspect({module, function, arguments})
        })
      end)

    task = Task.Supervisor.async_nolink(Dask.Executor.Elixir, module, function, arguments)

    {:noreply, Map.put(state, task.ref, job_id)}
  end

  @impl GenServer
  def handle_info({port, {:exit_status, status}}, state) when is_port(port) do
    Memento.transaction!(fn ->
      job = Memento.Query.read(Dask.DB.Job, Map.get(state, port))

      Memento.Query.write(%Dask.DB.Job{
        job
        | exit_status: status,
          time_of_finish: DateTime.utc_now()
      })
    end)

    {:noreply, Map.drop(state, [port])}
  end

  @impl GenServer
  def handle_info({port, {:data, data}}, state) when is_port(port) do
    Memento.transaction!(fn ->
      job = Memento.Query.read(Dask.DB.Job, Map.get(state, port))

      job =
        Map.update(job, :output, "", fn x ->
          if not is_nil(x) do
            x
          else
            ""
          end <>
            List.to_string(data)
        end)

      Memento.Query.write(job)
    end)

    {:noreply, state}
  end

  @impl GenServer
  def handle_info({ref, result}, state) when is_reference(ref) do
    Memento.transaction!(fn ->
      job = Memento.Query.read(Dask.DB.Job, Map.get(state, ref))

      job = Map.put(job, :output, result)

      Memento.Query.write(%Dask.DB.Job{job | output: result})
    end)

    {:noreply, state}
  end

  @impl GenServer
  def handle_info({:DOWN, ref, _, _, status}, state) do
    Memento.transaction!(fn ->
      job = Memento.Query.read(Dask.DB.Job, Map.get(state, ref))

      Memento.Query.write(%Dask.DB.Job{
        job
        | exit_status: status,
          time_of_finish: DateTime.utc_now()
      })
    end)

    {:noreply, state}
  end
end
