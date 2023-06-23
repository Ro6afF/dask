defmodule Dask.Repo do
  use Ecto.Repo,
    otp_app: :dask,
    adapter: Ecto.Adapters.Postgres
end
