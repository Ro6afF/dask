defmodule Dask.Scheduler do
  use GenServer

  import Ecto.Query

  def start_link(_ \\ nil) do
    GenServer.start_link(__MODULE__, nil, name: __MODULE__)
  end

  def refresh do
    GenServer.cast(__MODULE__, :refresh)
  end

  def reschedule(task_id) do
    GenServer.cast(__MODULE__, {:reschedule, task_id})
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

  @impl GenServer
  def handle_cast({:reschedule, task_id}, state) do
    Dask.Repo.transaction(fn ->
      task =
        Dask.Repo.one(from(t in Dask.DB.Task, where: t.id == ^task_id, lock: "FOR UPDATE"))

      cron =
        if is_nil(task.cron_schedule) do
          nil
        else
          cron_from_string(task.cron_schedule)
        end

      next_time =
        if is_nil(cron) do
          task.scheduled_time
        else
          get_next_time(cron, DateTime.add(task.scheduled_time, 1, :minute))
        end

      changeset =
        Ecto.Changeset.cast(
          task,
          %{
            executor:
              if is_nil(task.cron_schedule) do
                "done"
              else
                nil
              end,
            scheduled_time: next_time
          },
          [:executor, :scheduled_time]
        )

      Dask.Repo.update(changeset)
    end)

    {:noreply, state}
  end

  defp to_integer_if_ok(str) do
    case Integer.parse(str) do
      {num, _} -> num
      _ -> str
    end
  end

  defp cron_from_string(str) do
    [minute, hour, day, month, weekday] = String.split(str)

    minute = to_integer_if_ok(minute)
    hour = to_integer_if_ok(hour)
    day = to_integer_if_ok(day)
    month = to_integer_if_ok(month)
    weekday = to_integer_if_ok(weekday)

    %{minute: minute, hour: hour, day: day, month: month, weekday: weekday}
  end

  defp get_next_time(cron, datetime) do
    cond do
      cron.month != "*" and datetime.month != cron.month ->
        datetime =
          DateTime.add(datetime, Date.days_in_month(DateTime.to_date(datetime)), :day)

        datetime = %{datetime | day: 1, hour: 0, minute: 0}
        get_next_time(cron, datetime)

      cron.day != "*" and datetime.day != cron.day ->
        datetime = DateTime.add(datetime, 1, :day)
        datetime = %{datetime | hour: 0, minute: 0}
        get_next_time(cron, datetime)

      cron.weekday != "*" and Date.day_of_week(datetime, :sunday) - 1 != cron.weekday ->
        IO.inspect({Date.day_of_week(datetime, :sunday) - 1, cron.weekday})
        datetime = DateTime.add(datetime, 1, :day) |> IO.inspect()
        datetime = %{datetime | hour: 0, minute: 0}
        get_next_time(cron, datetime)

      cron.hour != "*" and datetime.hour != cron.hour ->
        datetime = DateTime.add(datetime, 1, :hour)
        datetime = %{datetime | minute: 0}
        get_next_time(cron, datetime)

      cron.minute != "*" and datetime.minute != cron.minute ->
        datetime = DateTime.add(datetime, 1, :minute)
        get_next_time(cron, datetime)

      true ->
        %{datetime | second: 0}
    end
  end
end
