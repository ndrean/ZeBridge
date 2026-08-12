defmodule Emitter.Producer.Repo do
  use Ecto.Repo,
    otp_app: :emitter,
    adapter: Ecto.Adapters.Postgres
end
