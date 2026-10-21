defmodule Zvec.Error do
  @moduledoc """
  Raised for zvec failures that cannot be returned as `{:error, {code, msg}}`,
  such as errors while enumerating a stream from `Zvec.Collection.stream/2`.

  `:code` is the zvec status atom (e.g. `:failed_precondition`) and `:message`
  is zvec's error message.
  """

  defexception [:code, :message]

  @impl true
  def message(%__MODULE__{code: code, message: message}) do
    "zvec error (#{code}): #{message}"
  end
end
