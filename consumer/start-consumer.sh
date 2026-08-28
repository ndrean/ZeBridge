#!/bin/bash
# Unset conflicting PostgreSQL environment variables
unset PG_HOST PG_PORT PG_USER PG_PASSWORD PG_DB
unset POSTGRES_READER_USER POSTGRES_READER_PASSWORD

# Run Elixir consumer with explicit environment variables
NATS_USER=bridge_user \
NATS_PASSWORD=bridge_secure_password \
TABLES=users,test_types \
MIX_ENV=prod \
FORMAT=msgpack \
PG_HOST=localhost \
PG_PORT=5432 \
PG_USER=postgres \
PG_PASSWORD=postgres_password \
PG_DB=postgres \
iex -S mix
