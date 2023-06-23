defmodule Dask.Job do
  use Ecto.Schema

  schema "jobs" do
    field(:command, :string)
    field(:exit_status, :string)
    field(:output, :string)
    field(:time_of_start, :utc_datetime_usec)
    field(:time_of_finish, :utc_datetime_usec)
  end
end
