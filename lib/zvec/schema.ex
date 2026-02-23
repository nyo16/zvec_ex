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
    * `:index` - index params map, e.g. `%{type: :invert}`
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

    * `:type` - vector element type (default: `:vector_fp32`)
    * `:nullable` - whether the field can be null (default: `false`)
    * `:index` - index params map, e.g. `%{type: :hnsw, metric_type: :cosine}`
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
