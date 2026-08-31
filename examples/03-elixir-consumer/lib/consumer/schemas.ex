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
    field(:string_field, :string)
    field(:integer_field, :integer)
    field(:float_field, :float)
    field(:boolean_field, :boolean)
    field(:decimal_field, :decimal)
    field(:date_field, :date)
    field(:time_field, :time)
    field(:naive_datetime_field, :naive_datetime)
    field(:utc_datetime_field, :utc_datetime)

    timestamps()
  end

  @doc false
  def changeset(test_type, attrs) do
    test_type
    |> cast(attrs, [
      :string_field,
      :integer_field,
      :float_field,
      :boolean_field,
      :decimal_field,
      :date_field,
      :time_field,
      :naive_datetime_field,
      :utc_datetime_field
    ])
    |> validate_required([:string_field, :integer_field])
  end
end
