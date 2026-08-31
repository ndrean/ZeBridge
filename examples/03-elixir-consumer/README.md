# Consumer

```sh
TABLES=users,test_types MIX_ENV=prod iex -S mix
```

```elixir
iex> PgProducer.crud()
iex> PgProducer.bulk(20, "users")
iex> PgProducer.stream(10_000, 100, 1)
```
