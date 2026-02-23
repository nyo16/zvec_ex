defmodule Zvec.Native do
  @moduledoc false
  @on_load :load_nif

  defp load_nif do
    path = :filename.join(:code.priv_dir(:zvec), ~c"libzvec_nif")
    :erlang.load_nif(path, 0)
  end

  def collection_create_and_open(_path, _schema, _options),
    do: :erlang.nif_error(:not_loaded)

  def collection_open(_path, _options),
    do: :erlang.nif_error(:not_loaded)

  def collection_destroy(_ref),
    do: :erlang.nif_error(:not_loaded)

  def collection_flush(_ref),
    do: :erlang.nif_error(:not_loaded)

  def collection_stats(_ref),
    do: :erlang.nif_error(:not_loaded)

  def collection_schema(_ref),
    do: :erlang.nif_error(:not_loaded)

  def collection_insert(_ref, _docs),
    do: :erlang.nif_error(:not_loaded)

  def collection_upsert(_ref, _docs),
    do: :erlang.nif_error(:not_loaded)

  def collection_delete(_ref, _pks),
    do: :erlang.nif_error(:not_loaded)

  def collection_delete_by_filter(_ref, _filter),
    do: :erlang.nif_error(:not_loaded)

  def collection_query(_ref, _query),
    do: :erlang.nif_error(:not_loaded)

  def collection_fetch(_ref, _pks),
    do: :erlang.nif_error(:not_loaded)

  def collection_create_index(_ref, _column, _params),
    do: :erlang.nif_error(:not_loaded)

  def collection_drop_index(_ref, _column),
    do: :erlang.nif_error(:not_loaded)

  def collection_optimize(_ref),
    do: :erlang.nif_error(:not_loaded)
end
