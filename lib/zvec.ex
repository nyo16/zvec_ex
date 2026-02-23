defmodule Zvec do
  @moduledoc """
  Elixir bindings for [zvec](https://github.com/alibaba/zvec), an in-process vector database.

  ## Quick Start

      schema =
        Zvec.Schema.new("demo")
        |> Zvec.Schema.add_field("text", :string)
        |> Zvec.Schema.add_vector("embedding", 384, index: %{type: :hnsw, metric_type: :cosine})

      {:ok, col} = Zvec.Collection.create_and_open("/tmp/mydb", schema)

      doc = Zvec.Doc.new("doc1", %{
        "text" => "hello world",
        "embedding" => Zvec.Query.float_list_to_binary(List.duplicate(0.1, 384))
      })

      :ok = Zvec.Collection.insert(col, [doc])
      :ok = Zvec.Collection.optimize(col)

      {:ok, results} = Zvec.Collection.query(col,
        Zvec.Query.vector("embedding", List.duplicate(0.1, 384), topk: 5))
  """
end
