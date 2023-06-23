defmodule Dask.DB.Task do
  use Ecto.Schema

  schema "tasks" do
    field(:name, :string)
    field(:command, :string)
    field(:possible_nodes, {:array, :string})
    field(:scheduled_time, :utc_datetime_usec)
    field(:cron_schedule, :string)
    field(:executor, :string)
  end
end
