defmodule Zvec.Schema do
  @moduledoc """
  Builder for zvec collection schemas.

  ## Example

      schema =
        Zvec.Schema.new("my_collection")
        |> Zvec.Schema.add_field("text", :string, index: %{type: :invert})
        |> Zvec.Schema.add_vector("embedding", 384, index: %{type: :hnsw, metric_type: :cosine})
  """

  defstruct name: "", fields: [], max_doc_count_per_segment: nil

  @type t :: %__MODULE__{
          name: String.t(),
          fields: [map()],
          max_doc_count_per_segment: pos_integer() | nil
        }

  @doc "Create a new schema with the given collection name."
  @spec new(String.t()) :: t()
  def new(name) when is_binary(name) do
    %__MODULE__{name: name}
  end

  @doc """
  Add a scalar field to the schema.

  ## Options

    * `:nullable` - whether the field can be null (default: `false`)
    * `:index` - index params map. Supported types for scalar fields:
      * `%{type: :invert}` - inverted index for filters. Options:
        `:enable_range_optimization` (default `true`),
        `:enable_extended_wildcard` (default `false`, enables suffix/infix `LIKE`)
      * `%{type: :fts}` - full-text index on a `:string` field, queried with
        `Zvec.Query.fts/3`. Options:
        * `:tokenizer` - `"standard"` (default), `"ngram"`, `"jieba"`, or `"whitespace"`
        * `:filters` - token filters applied in order, from `"lowercase"`,
          `"ascii_folding"`, `"stemmer"` (default `["lowercase"]`)
        * `:extra_params` - JSON object string with tokenizer/filter settings,
          e.g. `~s({"ngram_min":2,"ngram_max":3})` or `~s({"stemmer_lang":"porter"})`
  """
  @spec add_field(t(), String.t(), atom(), keyword()) :: t()
  def add_field(%__MODULE__{} = schema, name, type, opts \\ []) do
    field = %{
      name: name,
      type: type,
      nullable: Keyword.get(opts, :nullable, false),
      dimension: nil,
      index_params: Keyword.get(opts, :index)
    }

    %{schema | fields: schema.fields ++ [field]}
  end

  @doc """
  Add a dense vector field to the schema.

  ## Options

    * `:type` - vector element type: `:vector_fp32` (default), `:vector_fp16`,
      `:vector_fp64`, `:vector_int8`, or `:vector_int16`. Values are raw native-endian binaries.
    * `:nullable` - whether the field can be null (default: `false`)
    * `:index` - index params map with `:type` and `:metric_type`
      (`:cosine`, `:l2`, or `:ip`), e.g. `%{type: :hnsw, metric_type: :cosine}`.
      Defaults are shown in parentheses.

      | `:type` | Options |
      |---------|---------|
      | `:hnsw` | `:m` (16), `:ef_construction` (200), `:use_contiguous_memory` (false), `:use_flat_contiguous_memory` (false), `:flat_data_type` (`:vector_fp32`) |
      | `:flat` | `:use_contiguous_memory` (false), `:storage_data_type` |
      | `:ivf` | `:n_list` (1024), `:n_iters` (10), `:use_soar` (false) |
      | `:vamana` | `:max_degree` (64), `:search_list_size` (100), `:alpha` (1.2), `:saturate_graph` (false), `:use_contiguous_memory` (false), `:use_id_map` (false), `:two_pass_build` (true), `:use_flat_contiguous_memory` (false), `:flat_data_type` (`:vector_fp32`) |
      | `:diskann` | `:max_degree` (100), `:list_size` (50), `:pq_chunk_num` (0) |
      | `:hnsw_rabitq` | `:total_bits` (7), `:num_clusters` (16), `:m` (16), `:ef_construction` (200), `:sample_count` (0) |
      | `:ivf_rabitq` | `:n_list` (1024), `:total_bits` (7), `:sample_count` (0) |

      `:hnsw`, `:flat`, `:ivf`, `:vamana`, and `:diskann` also accept
      `:quantize_type` (`:fp16`, `:int8`, `:int4`, `:uniform_uint4`,
      `:uniform_uint7`, `:uniform_uint8`) and `:enable_rotate` (random rotation
      before INT8/INT4 quantization to reduce quantization error).
      `:flat_data_type` (`:vector_fp32`, `:vector_fp16`, `:vector_uint8`) sets the
      storage type of the auxiliary Flat index graph indexes use for refinement.

      RaBitQ indexes (`:hnsw_rabitq`, `:ivf_rabitq`) are only available on
      x86_64 Linux and Windows builds of zvec.
  """
  @spec add_vector(t(), String.t(), pos_integer(), keyword()) :: t()
  def add_vector(%__MODULE__{} = schema, name, dimension, opts \\ []) do
    field = %{
      name: name,
      type: Keyword.get(opts, :type, :vector_fp32),
      nullable: Keyword.get(opts, :nullable, false),
      dimension: dimension,
      index_params: Keyword.get(opts, :index)
    }

    %{schema | fields: schema.fields ++ [field]}
  end

  @doc "Set the maximum document count per segment."
  @spec max_doc_count_per_segment(t(), pos_integer()) :: t()
  def max_doc_count_per_segment(%__MODULE__{} = schema, count) do
    %{schema | max_doc_count_per_segment: count}
  end

  @doc "Convert the schema struct to the NIF-compatible map format."
  @spec to_nif(t()) :: map()
  def to_nif(%__MODULE__{} = schema) do
    %{
      name: schema.name,
      max_doc_count_per_segment: schema.max_doc_count_per_segment,
      fields:
        Enum.map(schema.fields, fn field ->
          %{
            name: field.name,
            type: field.type,
            nullable: field.nullable,
            dimension: field.dimension,
            index_params: field.index_params
          }
        end)
    }
  end
end
