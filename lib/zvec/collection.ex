defmodule Zvec.Collection do
  @moduledoc """
  High-level API for zvec collections.

  ## Example

      schema =
        Zvec.Schema.new("test")
        |> Zvec.Schema.add_vector("vec", 4, index: %{type: :hnsw, metric_type: :cosine})

      {:ok, col} = Zvec.Collection.create_and_open("/tmp/mydb", schema)
      :ok = Zvec.Collection.insert(col, [Zvec.Doc.new("pk1", %{"vec" => vec_binary})])
      :ok = Zvec.Collection.optimize(col)
      {:ok, results} = Zvec.Collection.query(col, Zvec.Query.vector("vec", [0.1, 0.2, 0.3, 0.4], topk: 1))
  """

  alias Zvec.{Native, Schema, Doc}

  @type ref :: reference()

  @doc """
  Create a new collection and open it.

  ## Options

    * `:read_only` - open read-only (default: `false`)
    * `:enable_mmap` - enable memory mapping (default: `true`)
    * `:max_buffer_size` - write buffer size in bytes (default: 64MB)
  """
  @spec create_and_open(String.t(), Schema.t(), keyword()) ::
          {:ok, ref()} | {:error, {atom(), String.t()}}
  def create_and_open(path, %Schema{} = schema, opts \\ []) do
    options = opts_to_map(opts)
    Native.collection_create_and_open(path, Schema.to_nif(schema), options) |> normalize()
  end

  @doc "Open an existing collection."
  @spec open(String.t(), keyword()) :: {:ok, ref()} | {:error, {atom(), String.t()}}
  def open(path, opts \\ []) do
    options = opts_to_map(opts)
    Native.collection_open(path, options) |> normalize()
  end

  @doc "Destroy the collection (deletes data on disk)."
  @spec destroy(ref()) :: :ok | {:error, {atom(), String.t()}}
  def destroy(ref), do: Native.collection_destroy(ref) |> normalize()

  @doc "Flush pending writes to disk."
  @spec flush(ref()) :: :ok | {:error, {atom(), String.t()}}
  def flush(ref), do: Native.collection_flush(ref) |> normalize()

  @doc "Get collection statistics."
  @spec stats(ref()) :: {:ok, map()} | {:error, {atom(), String.t()}}
  def stats(ref), do: Native.collection_stats(ref) |> normalize()

  @doc "Get the collection schema."
  @spec schema(ref()) :: {:ok, map()} | {:error, {atom(), String.t()}}
  def schema(ref), do: Native.collection_schema(ref) |> normalize()

  @doc """
  Insert documents into the collection.

  Returns `:ok` if all documents were inserted successfully.
  """
  @spec insert(ref(), [Doc.t()]) :: :ok | {:error, {atom(), String.t()}}
  def insert(ref, docs) when is_list(docs) do
    nif_docs = Enum.map(docs, &Doc.to_nif/1)

    case Native.collection_insert(ref, nif_docs) |> normalize() do
      {:ok, _write_results} -> :ok
      {:error, _} = err -> err
    end
  end

  @doc """
  Upsert documents (insert or update if pk exists).

  Returns `:ok` if all documents were upserted successfully.
  """
  @spec upsert(ref(), [Doc.t()]) :: :ok | {:error, {atom(), String.t()}}
  def upsert(ref, docs) when is_list(docs) do
    nif_docs = Enum.map(docs, &Doc.to_nif/1)

    case Native.collection_upsert(ref, nif_docs) |> normalize() do
      {:ok, _write_results} -> :ok
      {:error, _} = err -> err
    end
  end

  @doc "Delete documents by primary keys."
  @spec delete(ref(), [String.t()]) :: :ok | {:error, {atom(), String.t()}}
  def delete(ref, pks) when is_list(pks), do: Native.collection_delete(ref, pks) |> normalize()

  @doc "Delete documents matching a filter expression."
  @spec delete_by_filter(ref(), String.t()) :: :ok | {:error, {atom(), String.t()}}
  def delete_by_filter(ref, filter) when is_binary(filter) do
    Native.collection_delete_by_filter(ref, filter) |> normalize()
  end

  @doc """
  Execute a vector similarity query.

  Returns `{:ok, results}` where each result is a `Zvec.Doc` with `:pk`, `:score`, and `:fields`.
  """
  @spec query(ref(), map()) :: {:ok, [Doc.t()]} | {:error, {atom(), String.t()}}
  def query(ref, query_map) when is_map(query_map) do
    case Native.collection_query(ref, query_map) |> normalize() do
      {:ok, docs} -> {:ok, Enum.map(docs, &Doc.from_nif/1)}
      {:error, _} = err -> err
    end
  end

  @doc "Fetch documents by primary keys."
  @spec fetch(ref(), [String.t()]) :: {:ok, [Doc.t()]} | {:error, {atom(), String.t()}}
  def fetch(ref, pks) when is_list(pks) do
    case Native.collection_fetch(ref, pks) |> normalize() do
      {:ok, docs} -> {:ok, Enum.map(docs, &Doc.from_nif/1)}
      {:error, _} = err -> err
    end
  end

  @doc "Create an index on a column."
  @spec create_index(ref(), String.t(), map()) :: :ok | {:error, {atom(), String.t()}}
  def create_index(ref, column, params) when is_binary(column) and is_map(params) do
    Native.collection_create_index(ref, column, params) |> normalize()
  end

  @doc "Drop an index from a column."
  @spec drop_index(ref(), String.t()) :: :ok | {:error, {atom(), String.t()}}
  def drop_index(ref, column) when is_binary(column) do
    Native.collection_drop_index(ref, column) |> normalize()
  end

  @doc "Optimize the collection (build ANN indexes, compact segments)."
  @spec optimize(ref()) :: :ok | {:error, {atom(), String.t()}}
  def optimize(ref), do: Native.collection_optimize(ref) |> normalize()

  defp opts_to_map(opts) do
    %{
      read_only: Keyword.get(opts, :read_only, false),
      enable_mmap: Keyword.get(opts, :enable_mmap, true),
      max_buffer_size: Keyword.get(opts, :max_buffer_size, 64 * 1024 * 1024)
    }
  end

  # Fine's Error<Atom, string> produces {:error, atom, string}
  # Normalize to {:error, {atom, string}}
  defp normalize({:error, code, msg}) when is_atom(code), do: {:error, {code, msg}}
  defp normalize(other), do: other
end
