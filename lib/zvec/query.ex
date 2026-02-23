defmodule Zvec.Query do
  @moduledoc """
  Query builder for zvec vector searches.

  ## Example

      query = Zvec.Query.vector("embedding", [0.1, 0.2, 0.3, 0.4], topk: 10)
      query = Zvec.Query.vector("embedding", vec_binary, topk: 10, filter: "category = 'ai'")
  """

  @doc """
  Build a vector query.

  The `vector` argument can be:
  - A list of floats (will be converted to a raw float32 binary)
  - A raw binary (used as-is)

  ## Options

    * `:topk` - number of results to return (default: 10)
    * `:filter` - SQL-like filter expression
    * `:include_vector` - whether to include vectors in results (default: false)
    * `:output_fields` - list of field names to include in results
    * `:query_params` - search params map, e.g. `%{type: :hnsw, ef: 200}`
  """
  @spec vector(String.t(), [float()] | binary(), keyword()) :: map()
  def vector(field_name, vector, opts \\ []) do
    query_vector =
      case vector do
        v when is_list(v) -> float_list_to_binary(v)
        v when is_binary(v) -> v
      end

    %{
      field_name: field_name,
      query_vector: query_vector,
      topk: Keyword.get(opts, :topk, 10),
      filter: Keyword.get(opts, :filter),
      include_vector: Keyword.get(opts, :include_vector, false),
      output_fields: Keyword.get(opts, :output_fields),
      query_params: Keyword.get(opts, :query_params)
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
