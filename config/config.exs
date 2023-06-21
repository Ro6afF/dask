import Config

config :mnesia,
  dir: ~c".mnesia/#{Mix.env()}/#{node()}"
