defmodule Zvec.Query do
  @moduledoc """
  Query builders for zvec searches.

  Build a query with `vector/3` (dense vector similarity), `fts/3` (full-text
  search), or `multi/2` (hybrid search over several sub-queries with
  reranking), then run it with `Zvec.Collection.query/2`.

  ## Example

      query = Zvec.Query.vector("embedding", [0.1, 0.2, 0.3, 0.4], topk: 10)
      query = Zvec.Query.vector("embedding", vec_binary, topk: 10, filter: "category = 'ai'")
      query = Zvec.Query.fts("content", "vector database", topk: 10)

      query =
        Zvec.Query.multi(
          [
            Zvec.Query.vector("embedding", vec, topk: 50),
            Zvec.Query.fts("content", "vector database", topk: 50)
          ],
          topk: 10,
          rerank: {:rrf, 60}
        )

  ## Common options

  `vector/3`, `fts/3` and `multi/2` accept:

    * `:topk` - number of results to return (default: 10)
    * `:filter` - SQL-like filter expression
    * `:include_vector` - whether to include vectors in results (default: false)
    * `:output_fields` - list of field names to include in results (default: all)
  """

  @doc """
  Build a dense vector similarity query.

  The `vector` argument can be:
  - A list of floats (will be converted to a raw float32 binary)
  - A raw binary (used as-is, e.g. `Nx.to_binary/1` output)

  ## Options

  The common options above, plus:

    * `:query_params` - search params map, e.g. `%{type: :hnsw, ef: 200}`. The
      `:type` must match the field's index type. Unset keys keep zvec's defaults.

      | `:type` | Keys |
      |---------|------|
      | `:hnsw` | `:ef` (default 200), `:prefetch_offset`, `:prefetch_lines` |
      | `:hnsw_rabitq` | `:ef` (default 200) |
      | `:vamana` | `:ef_search` (default 200), `:prefetch_offset`, `:prefetch_lines` |
      | `:ivf` | `:nprobe` (default 10) |
      | `:ivf_rabitq` | `:nprobe` (default 10) |
      | `:diskann` | `:list_size` (default 300) |
      | `:flat` | — |

      Every type also accepts `:radius`, `:is_linear`, `:is_using_refiner`, and
      `:scale_factor` (coarse candidate multiplier when refining).
  """
  @spec vector(String.t(), [float()] | binary(), keyword()) :: map()
  def vector(field_name, vector, opts \\ []) do
    query_vector =
      case vector do
        v when is_list(v) -> float_list_to_binary(v)
        v when is_binary(v) -> v
      end

    opts
    |> base(field_name)
    |> Map.merge(%{
      query_vector: query_vector,
      query_params: Keyword.get(opts, :query_params)
    })
  end

  @doc """
  Build a full-text search query against a field with an `:fts` index.

  Results are scored with BM25.

  ## Options

  The common options above, plus:

    * `:syntax` - how `text` is interpreted:
      * `:match` (default) - natural language; `text` is tokenized and the terms
        are combined with `:default_operator`
      * `:query` - query expression syntax, e.g. `~s(+vector -slow "exact phrase")`
    * `:default_operator` - `:or` (default) or `:and`, used to combine bare terms
  """
  @spec fts(String.t(), String.t(), keyword()) :: map()
  def fts(field_name, text, opts \\ []) when is_binary(text) do
    fts =
      case Keyword.get(opts, :syntax, :match) do
        :match -> %{match_string: text}
        :query -> %{query_string: text}
      end

    query_params =
      case Keyword.get(opts, :default_operator) do
        nil ->
          nil

        op when op in [:or, :and] ->
          %{type: :fts, default_operator: op |> to_string() |> String.upcase()}
      end

    opts
    |> base(field_name)
    |> Map.merge(%{fts: fts, query_params: query_params})
  end

  @doc """
  Build a hybrid query that runs several sub-queries and fuses their results.

  Each sub-query is built with `vector/3` or `fts/3`; its `:topk` sets how many
  candidates it contributes to reranking, and its `:query_params` are honored.
  Sub-query `:filter`, `:include_vector` and `:output_fields` are ignored — set
  them on the multi query instead.

  ## Options

  The common options above, plus:

    * `:rerank` - fusion strategy:
      * `:rrf` or `{:rrf, rank_constant}` (default `{:rrf, 60}`) - Reciprocal
        Rank Fusion, score `1 / (rank_constant + rank + 1)`
      * `{:weighted, weights}` - per-sub-query weights (list of numbers, same
        order as `queries`) applied to metric-normalized scores
  """
  @spec multi([map()], keyword()) :: map()
  def multi(queries, opts \\ []) when is_list(queries) and queries != [] do
    rerank =
      case Keyword.get(opts, :rerank, :rrf) do
        :rrf -> %{type: :rrf, rank_constant: 60}
        {:rrf, k} when is_integer(k) -> %{type: :rrf, rank_constant: k}
        {:weighted, weights} when is_list(weights) -> %{type: :weighted, weights: weights}
      end

    %{
      queries: queries,
      topk: Keyword.get(opts, :topk, 10),
      filter: Keyword.get(opts, :filter),
      include_vector: Keyword.get(opts, :include_vector, false),
      output_fields: Keyword.get(opts, :output_fields),
      rerank: rerank
    }
  end

  defp base(opts, field_name) do
    %{
      field_name: field_name,
      topk: Keyword.get(opts, :topk, 10),
      filter: Keyword.get(opts, :filter),
      include_vector: Keyword.get(opts, :include_vector, false),
      output_fields: Keyword.get(opts, :output_fields)
    }
  end

  @doc """
  Convert a list of floats to a raw float-32-native binary.
  """
  @spec float_list_to_binary([float()]) :: binary()
  def float_list_to_binary(list) when is_list(list) do
    for f <- list, into: <<>>, do: <<f::float-32-native>>
  end

  @doc """
  Convert a raw float-32-native binary back to a list of floats.
  """
  @spec binary_to_float_list(binary()) :: [float()]
  def binary_to_float_list(bin) when is_binary(bin) do
    for <<f::float-32-native <- bin>>, do: f
  end
end
