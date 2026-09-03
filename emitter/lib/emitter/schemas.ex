defmodule User do
  use Ecto.Schema

  schema "users" do
    field(:name, :string)
    field(:email, :string)

    timestamps(type: :timestamptz)
  end

  def changeset( %User{} = user, attrs \\ %{}) do
    cast(users, [:name, :email])
  end
end

defmodule TestType do
  use Ecto.Schema
  # import Ecto.Changeset

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
    field(:deleted_at, :timestamptz)
    field(:tenant_id, :string, null: false)
    field(:last_writer, :string)

    timestamps(type: :timestamptz)
  end

  @doc false
  def changeset(%TestType{} = test_type, attrs \\ %{}) do
    test_type
    |> cast(attrs, [
      :age, :temperature, :price, :is_true, :some_text, :tags, :atrix, :metadata, :deleted_at, :tenant_id, :last_writer
    ])
  end
end
