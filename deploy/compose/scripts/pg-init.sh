#!/bin/sh
# The bridge's roles, functions and publication, from `bridge --init-sql` (the SQL is
# inside the binary, so it matches the bridge that runs on it). Once: a database that
# already has the publication is left alone.
set -eu
has=$(psql "$ADMIN_DATABASE_URL" -tAc "select 1 from pg_publication where pubname = '$BRIDGE_CDC_PUBLICATION'")
if [ "$has" = 1 ]; then
  echo "publication $BRIDGE_CDC_PUBLICATION present: nothing to do"
  exit 0
fi
bridge --init-sql | psql "$ADMIN_DATABASE_URL" -X -q -v ON_ERROR_STOP=1
echo "database initialised: roles, functions, publication $BRIDGE_CDC_PUBLICATION"
