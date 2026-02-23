defmodule Zvec.Doc do
  @moduledoc """
  Document struct for zvec collections.

  ## Example

      doc = Zvec.Doc.new("pk1", %{"text" => "hello", "embedding" => vec_binary})
  """

  defstruct pk: nil, score: nil, fields: %{}

  @type t :: %__MODULE__{
          pk: String.t() | nil,
          score: float() | nil,
          fields: map()
        }

  @doc "Create a new document with primary key and field values."
  @spec new(String.t(), map()) :: t()
  def new(pk, fields \\ %{}) when is_binary(pk) and is_map(fields) do
    %__MODULE__{pk: pk, fields: fields}
  end

  @doc false
  def to_nif(%__MODULE__{} = doc) do
    %{pk: doc.pk, fields: doc.fields}
  end

  @doc false
  def from_nif(%{} = map) do
    %__MODULE__{
      pk: map["pk"] || map[:pk],
      score: map["score"] || map[:score],
      fields: map["fields"] || map[:fields] || %{}
    }
  end
end
