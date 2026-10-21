defmodule Zvec.Precompiler do
  @moduledoc false

  @all_targets ["aarch64-apple-darwin"]

  def all_supported_targets(:fetch), do: @all_targets

  def all_supported_targets(:compile) do
    case current_target() do
      {:ok, target} -> [target]
      _ -> []
    end
  end

  def current_target do
    system_arch = to_string(:erlang.system_info(:system_architecture))

    cond do
      system_arch =~ ~r/aarch64.*apple.*darwin/ -> {:ok, "aarch64-apple-darwin"}
      true -> {:error, "unsupported target: #{system_arch}"}
    end
  end

  def build_native(args), do: ElixirMake.Precompiler.mix_compile(args)

  def precompile(args, _target) do
    case ElixirMake.Precompiler.mix_compile(args) do
      {:ok, _} -> :ok
      error -> error
    end
  end

  def unavailable_target(_target), do: :compile
end

defmodule Zvec.MixProject do
  use Mix.Project

  @version "0.8.0"
  @source_url "https://github.com/nyo16/zvec_ex"

  def project do
    [
      app: :zvec,
      version: @version,
      elixir: "~> 1.16",
      start_permanent: Mix.env() == :prod,
      compilers: Mix.compilers() ++ [:elixir_make],
      make_env: fn -> %{"FINE_INCLUDE_DIR" => Fine.include_dir()} end,
      make_clean: ["clean"],
      make_precompiler: {:nif, Zvec.Precompiler},
      make_precompiler_url:
        "https://github.com/nyo16/zvec_ex/releases/download/v#{@version}/@{artefact_filename}",
      make_precompiler_filename: "libzvec_nif",
      make_precompiler_priv_paths: ["libzvec_nif.so"],
      make_precompiler_nif_versions: [versions: ["2.17", "2.18"]],
      # Set ZVEC_BUILD=true to compile the NIF from source instead of
      # downloading (or reusing) a precompiled artefact.
      make_force_build: System.get_env("ZVEC_BUILD") in ["1", "true"],
      deps: deps(),
      package: package(),
      description: "Elixir NIF bindings for zvec, an in-process vector database from Alibaba.",
      source_url: @source_url,
      docs: docs()
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
      {:fine, "~> 0.1.4", runtime: false},
      {:ex_doc, "~> 0.35", only: :dev, runtime: false}
    ]
  end

  defp package do
    [
      licenses: ["Apache-2.0"],
      links: %{
        "GitHub" => @source_url,
        "zvec" => "https://github.com/alibaba/zvec"
      },
      files:
        ~w(lib c_src/zvec_nif.cpp Makefile mix.exs README.md LICENSE .formatter.exs checksum.exs)
    ]
  end

  defp docs do
    [
      main: "readme",
      extras: ["README.md", "LICENSE"],
      source_ref: "v#{@version}",
      source_url: @source_url
    ]
  end
end
