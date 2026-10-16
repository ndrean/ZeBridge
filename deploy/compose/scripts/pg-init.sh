#!/bin/sh
# The bridge's roles, functions and publication, from `bridge --init-sql` (the SQL is inside
# the binary, so it matches the bridge that runs on it). At every start: the SQL is
# idempotent, so an upgrade brings the new release's functions, and stored data, grants and
# guards stay as they are. Then the backend's invite login, granted zebridge_inviter only.
set -eu
# PostgreSQL's NOTICEs ("already exists, skipping") are the idempotent SQL talking: warnings
# and errors only, or every start would fill the log with them.
export PGOPTIONS="-c client_min_messages=warning"
bridge --init-sql | psql "$ADMIN_DATABASE_URL" -X -q -v ON_ERROR_STOP=1 > /dev/null
echo "database up to date: roles, functions, publication $BRIDGE_CDC_PUBLICATION"
if [ -n "${ZB_INVITER_PASSWORD:-}" ]; then
  # The password reaches psql on stdin, never in a process's arguments.
  psql "$ADMIN_DATABASE_URL" -X -q -v ON_ERROR_STOP=1 > /dev/null <<SQL
\set pw '$ZB_INVITER_PASSWORD'
SELECT 'CREATE ROLE zb_inviter LOGIN' WHERE NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'zb_inviter') \gexec
ALTER ROLE zb_inviter LOGIN PASSWORD :'pw';
GRANT zebridge_inviter TO zb_inviter;
SQL
  echo "invite login zb_inviter: may create invites, nothing else"
fi
