defmodule Dask.Repo.Migrations.CreateJobs do
  use Ecto.Migration

  def change do
    create table(:jobs) do
      add :command, :string
      add :exit_status, :string
      add :output, :string
      add :time_of_start, :utc_datetime_usec
      add :time_of_finish, :utc_datetime_usec
    end
  end
end
