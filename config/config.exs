import Config

config :dask, Dask.Repo,
  database: "dask",
  username: "dask",
  password: "dask",
  hostname: "localhost"

config :dask, ecto_repos: [Dask.Repo]
