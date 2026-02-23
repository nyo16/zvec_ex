defmodule Zvec.MixProject do
  use Mix.Project

  def project do
    [
      app: :zvec,
      version: "0.1.0",
      elixir: "~> 1.20-rc",
      start_permanent: Mix.env() == :prod,
      compilers: [:elixir_make] ++ Mix.compilers(),
      make_env: fn -> %{"FINE_INCLUDE_DIR" => Fine.include_dir()} end,
      deps: deps()
    ]
  end

  def application do
    [
      extra_applications: [:logger],
      mod: {Zvec.Application, []}
    ]
  end

  defp deps do
    [
      {:elixir_make, "~> 0.9", runtime: false},
      {:fine, "~> 0.1.4", runtime: false}
    ]
  end
end
