defmodule DaskTest do
  use ExUnit.Case
  doctest Dask

  test "greets the world" do
    assert Dask.hello() == :world
  end
end
