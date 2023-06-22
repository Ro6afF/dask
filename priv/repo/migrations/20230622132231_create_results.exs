defmodule Dask.Repo.Migrations.CreateResults do
  use Ecto.Migration

  def change do
    create table(:tasks) do
      add :name, :string
      add :command, :string
      add :possible_nodes, {:array, :string}
      add :scheduled_time, :utc_datetime_usec
      add :executor, :string
    end

    create table(:results) do
      add :task_id, references(:tasks)
      add :time_of_start, :utc_datetime_usec
      add :executor, :string
      add :output, :string, size: 10000
      add :time_of_finish, :utc_datetime_usec
      add :exit_status, :integer
    end
  end
end
