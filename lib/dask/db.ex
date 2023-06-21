defmodule Dask.DB do
  def init do
    Dask.DB.Job.init()
  end
end
