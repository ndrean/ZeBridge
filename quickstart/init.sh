#!/bin/sh
# The quickstart's one-shot setup, run in the bridge image (which carries psql):
# the NATS configuration, the database, the demo tables, and the invites the README
# links to. Idempotent: a second `up` finds everything in place.
#
# No password is written anywhere in the repository: PostgreSQL trusts connections on
# the compose network (it publishes no port), and the bridge's two roles get random
# passwords on the first run, kept only in the generated .env.bridge in the volume.
set -eu

DIR=/zb-nats
ADMIN_URL="postgres://postgres@postgres:5432/app"

# 1. NATS: an operator, an account, three scoped signing keys, the bridge's creds.
if [ ! -f "$DIR/nats-server.conf" ]; then
  bridge --init-nats operator --dir "$DIR"
fi

# 2. Point the generated .env.bridge at the quickstart's services.
set_env() {
  if grep -q "^$1=" "$DIR/.env.bridge"; then
    sed -i "s|^$1=.*|$1=$2|" "$DIR/.env.bridge"
  else
    echo "$1=$2" >> "$DIR/.env.bridge"
  fi
}
random() { head -c 16 /dev/urandom | od -An -tx1 | tr -d ' \n'; }
# --init-nats writes placeholder URLs: replace them once, on the first run.
if ! grep -q "^DATABASE_READER_URL=.*@postgres:5432/app" "$DIR/.env.bridge"; then
  set_env DATABASE_READER_URL "postgres://bridge_reader:$(random)@postgres:5432/app"
  set_env DATABASE_WRITER_URL "postgres://bridge_writer:$(random)@postgres:5432/app"
fi
set_env NATS_URL "nats://nats:4222"
set_env BRIDGE_CDC_PUBLICATION "quickstart"
set_env BRIDGE_CDC_SLOT "quickstart"
set_env BRIDGE_PORT "27434"
set_env GENERATION_CADENCE_SECONDS "60"
set -a; . "$DIR/.env.bridge"; set +a

# 3. PostgreSQL: roles, functions, triggers and the publication, then the demo tables.
if ! psql "$ADMIN_URL" -tAc "SELECT 1 FROM pg_publication WHERE pubname = 'quickstart'" | grep -q 1; then
  bridge --init-sql | psql "$ADMIN_URL" -q -v ON_ERROR_STOP=1 > /dev/null
fi
psql "$ADMIN_URL" -q -v ON_ERROR_STOP=1 -v pub=quickstart -f /quickstart/demo.sql > /dev/null

# 4. One-time invites with fixed codes, so the README can print the links. A code
#    has at least 16 characters: the bridge refuses shorter ones.
psql "$ADMIN_URL" -q -v ON_ERROR_STOP=1 <<'SQL'
INSERT INTO public.zebridge_invites (code, principal, tenant_id, expires_at) VALUES
  ('quickstart-alice-1', 'alice', 'acme',   now() + interval '1 year'),
  ('quickstart-alice-2', 'alice', 'acme',   now() + interval '1 year'),
  ('quickstart-bob-1',   'bob',   'globex', now() + interval '1 year')
ON CONFLICT (code) DO NOTHING;
SQL

PORT="${QS_WEB_PORT:-5173}"
cat <<EOF

  ZeBridge quickstart is ready. Open, each in its own tab:

    http://localhost:$PORT/?invite=quickstart-alice-1   alice, tenant acme
    http://localhost:$PORT/?invite=quickstart-alice-2   alice again, same tenant: edit the same rows
    http://localhost:$PORT/?invite=quickstart-bob-1     bob, tenant globex: shares only public tables

  Each link enrolls one tab once. Start again from scratch:
    docker compose -f docker-compose.quickstart.yml down -v

EOF
