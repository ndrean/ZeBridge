#!/bin/sh
# The quickstart's one-shot setup, run in the bridge image (which carries psql):
# the NATS configuration, the database, the demo tables, and the invites the README
# links to. Idempotent: a second `up` finds everything in place.
#
# No password is written anywhere in the repository: PostgreSQL trusts connections on
# the compose network (it publishes no port), and the bridge's two roles get random
# passwords on the first run, kept only in the volume's .env.bridge.
set -eu

DIR=/zb-nats
ADMIN_URL="postgres://postgres@postgres:5432/app"

# 1. NATS: an operator, an account, three scoped signing keys, the bridge's creds, and
#    .env.nats (NATS only).
if [ ! -f "$DIR/nats-server.conf" ]; then
  bridge --init-nats operator --dir "$DIR"
fi
if [ ! -f "$DIR/.env.nats" ]; then
  echo "This volume was made by an older quickstart (no .env.nats). Start again:"
  echo "  docker compose -f docker-compose.quickstart.yml down -v"
  exit 1
fi

# 2. Two files: .env.nats (generated) points at the compose NATS; .env.bridge holds the
#    database and the bridge's own settings, written once with random role passwords.
sed -i "s|^NATS_URL=.*|NATS_URL=nats://nats:4222|" "$DIR/.env.nats"
random() { head -c 16 /dev/urandom | od -An -tx1 | tr -d ' \n'; }
if [ ! -f "$DIR/.env.bridge" ]; then
  umask 077
  cat > "$DIR/.env.bridge" <<EOF
DATABASE_READER_URL=postgres://bridge_reader:$(random)@postgres:5432/app
DATABASE_WRITER_URL=postgres://bridge_writer:$(random)@postgres:5432/app
BRIDGE_CDC_PUBLICATION=quickstart
BRIDGE_CDC_SLOT=quickstart
BRIDGE_PORT=27434
GENERATIONS_ENABLED=1
GENERATION_CADENCE_SECONDS=60
EOF
fi
set -a; . "$DIR/.env.nats"; . "$DIR/.env.bridge"; set +a

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
