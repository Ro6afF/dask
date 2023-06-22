defmodule Dask.DB.Result do
  use Ecto.Schema

  schema "results" do
    field(:time_of_start, :utc_datetime_usec)
    field(:executor, :string)
    field(:output, :string)
    field(:time_of_finish, :utc_datetime_usec)
    field(:exit_status, :integer)

    belongs_to(:task, Dask.DB.Task)
  end
end
