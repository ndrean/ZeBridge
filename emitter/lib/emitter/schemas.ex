defmodule User do
  use Ecto.Schema

  schema "users" do
    field(:name, :string)
    field(:email, :string)

    timestamps()
  end
end

defmodule TestType do
  use Ecto.Schema
  import Ecto.Changeset

  schema "test_types" do
    field(:uid, Ecto.UUID, read_after_writes: true)
    field(:age, :integer)
    field(:temperature, :float)
    field(:price, :decimal)
    field(:is_true, :boolean)
    field(:some_text, :string)
    field(:tags, {:array, :string})
    field(:matrix, {:array, {:array, :integer}})
    field(:metadata, :map)

    timestamps(type: :utc_datetime_usec)
  end

  @doc false
  def changeset(test_type, _attrs) do
    test_type
    # |> cast(attrs, [
    #   :string_field,
    #   :integer_field,
    #   :float_field,
    #   :boolean_field,
    #   :decimal_field,
    #   :date_field,
    #   :time_field,
    #   :naive_datetime_field,
    #   :utc_datetime_field
    # ])
    # |> validate_required([:string_field, :integer_field])
  end
end
