defmodule Dask.Clock do
  use GenServer

  def start_link(_ \\ nil) do
    GenServer.start_link(__MODULE__, nil, name: __MODULE__)
  end

  @impl GenServer
  def init(_init_arg) do
    Process.send_after(self(), :heartbeat, 1000)

    {:ok, nil}
  end

  @impl GenServer
  def handle_info(:heartbeat, state) do
    # TODO: dispatch via registry
    Dask.Scheduler.refresh()

    Process.send_after(self(), :heartbeat, 1000)
    {:noreply, state}
  end
end
