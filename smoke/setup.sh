#!/bin/sh
# The smoke stack's one-time setup, in the APT-installed bridge image: NATS, the database,
# the table, one invite per client. Like quickstart/init.sh, with no password anywhere.
set -eu
DIR=/zb-nats
ADMIN_URL="postgres://postgres@postgres:5432/app"
[ -f "$DIR/nats-server.conf" ] || /usr/bin/bridge --init-nats operator --dir "$DIR"
sed -i "s|^NATS_URL=.*|NATS_URL=nats://nats:4222|" "$DIR/.env.nats"
random() { head -c 16 /dev/urandom | od -An -tx1 | tr -d ' \n'; }
if [ ! -f "$DIR/.env.bridge" ]; then
  umask 077
  cat > "$DIR/.env.bridge" <<ENV
DATABASE_READER_URL=postgres://bridge_reader:$(random)@postgres:5432/app
DATABASE_WRITER_URL=postgres://bridge_writer:$(random)@postgres:5432/app
BRIDGE_CDC_PUBLICATION=smoke
BRIDGE_CDC_SLOT=smoke
BRIDGE_PORT=27434
BRIDGE_BIND=0.0.0.0
GENERATIONS_ENABLED=1
GENERATION_CADENCE_SECONDS=30
ENV
fi
set -a; . "$DIR/.env.nats"; . "$DIR/.env.bridge"; set +a
if ! psql "$ADMIN_URL" -tAc "SELECT 1 FROM pg_publication WHERE pubname = 'smoke'" | grep -q 1; then
  /usr/bin/bridge --init-sql | psql "$ADMIN_URL" -q -v ON_ERROR_STOP=1 > /dev/null
fi
psql "$ADMIN_URL" -q -v ON_ERROR_STOP=1 -v pub=smoke -f /smoke/smoke.sql > /dev/null
psql "$ADMIN_URL" -q -v ON_ERROR_STOP=1 <<'SQL'
INSERT INTO public.zebridge_invites (code, principal, tenant_id, expires_at) VALUES
  ('smoke-invite-node',   'smoke-node',   'smoke', now() + interval '1 day'),
  ('smoke-invite-python', 'smoke-python', 'smoke', now() + interval '1 day'),
  ('smoke-invite-dart',   'smoke-dart',   'smoke', now() + interval '1 day')
ON CONFLICT (code) DO NOTHING;
SQL
echo "smoke setup: bridge $(/usr/bin/bridge --version | cut -d' ' -f2), table and invites in place"
