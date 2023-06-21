defmodule Dask.DB.Job do
  use Memento.Table,
    attributes: [:id, :command, :exit_status, :output, :time_of_start, :time_of_finish],
    type: :ordered_set,
    autoincrement: true

  def init do
    Memento.Table.create!(Dask.DB.Job)
  end
end
