#!/bin/bash
# The comparison's database steps, from your machine. Each checks the database's identity first (lib.sh).
#   db.sh init        ZeBridge's init SQL, rendered by the host's installed bridge, applied as the administrator
#   db.sh table       the comparison tables, dropped and recreated, both enabled by zebridge_enable so both systems
#                     write through the same triggers and tenant policy: storm_items, 10,000 rows in tenant `cmp`
#                     (storm/setup.sql), and sensor_events, empty, for the write crowd (scripts/bench/ingress.py's)
#   db.sh inventory   what is on the database: slots, publications, tables, ZeBridge's objects
#   db.sh count       the table's rows by kind (seed, markers, burst, writes)
#   db.sh rtt         the comparison host's distance to the database: 50 TCP connects, IPv6 and IPv4
#   db.sh wipe        remove both systems' objects: PowerSync's slots, publication, storage schema, roles and
#                     pre-request function; ZeBridge's slot, publication, tables, functions and roles
# Prints no secret.
. "$(dirname "$0")/lib.sh"
db_env
cd "$ROOT"

case "${1:-}" in
init)
  # Rendered by the host's installed bridge, the version the stack will run (OPERATIONS: the SQL comes from the
  # package that runs it), from the two URLs and the publication only; nothing stays on the host.
  echo "rendered by $(ssh_h '/usr/bin/bridge --version | head -1') on the host"
  trap 'rm -f "$SEC/init.sql" "$SEC/init.out"' EXIT   # the rendered SQL holds the roles' passwords
  # The assignments and the command go to a POSIX `sh -s` on stdin, whatever the remote account's login shell
  # (`. /dev/stdin` and `. <(cat)` each failed on some host).
  (umask 077; { echo 'set -a'; grep -E '^(DATABASE_READER_URL|DATABASE_WRITER_URL|BRIDGE_CDC_PUBLICATION)=' "$DB_ENV_FILE"; echo 'set +a'; echo 'exec /usr/bin/bridge --init-sql'; } \
    | ssh_h 'sh -s' > "$SEC/init.sql")
  admin_sql --single-transaction -f "$SEC/init.sql" > "$SEC/init.out"
  if grep -qiE "error|fatal" "$SEC/init.out"; then grep -iE "error|fatal" "$SEC/init.out" | head -3; exit 1; fi
  echo "init SQL applied"
  ;;
table)
  ROWS="${2:-10000}"
  [[ "$ROWS" =~ ^[1-9][0-9]*$ ]] || { echo "row count must be a positive integer" >&2; exit 2; }
  admin_sql -c "DROP TABLE IF EXISTS public.storm_items" -c "DELETE FROM public.zebridge_catalogue WHERE tbl::text IN ('storm_items', 'public.storm_items')"
  sed -e "s/'storm'/'cmp'/" -e "s/generate_series(1, 10000)/generate_series(1, $ROWS)/" storm/setup.sql | admin_sql -v pub="$BRIDGE_CDC_PUBLICATION" > /dev/null
  admin_sql -c "DROP TABLE IF EXISTS public.sensor_events" -c "DELETE FROM public.zebridge_catalogue WHERE tbl::text IN ('sensor_events', 'public.sensor_events')" \
    -c "CREATE TABLE public.sensor_events (event_id uuid PRIMARY KEY, tenant_id text NOT NULL, sensor_id integer NOT NULL, kind text NOT NULL, value double precision NOT NULL, updated_at timestamptz NOT NULL DEFAULT now(), deleted_at timestamptz)"
  refused=$(PGOPTIONS="-c client_min_messages=warning" psql "$ADMIN" -X -tA -v ON_ERROR_STOP=1 -c "SELECT step || ': ' || detail FROM zebridge_enable('public.sensor_events'::regclass, tenant_col => 'tenant_id', writable => true, version_col => 'updated_at', tombstone_col => 'deleted_at', publication => '$BRIDGE_CDC_PUBLICATION', dry_run => false) WHERE status = 'ERROR'" 2>&1 | redact)
  [ -z "$refused" ] || { echo "zebridge_enable refused sensor_events: $refused" >&2; exit 1; }
  psql "$ADMIN" -X -tA -c "SELECT 'storm_items: ' || count(*) || ' rows in tenant ' || string_agg(DISTINCT tenant_id, ',') FROM storm_items" -c "SELECT 'sensor_events: ' || count(*) || ' rows' FROM sensor_events" 2>&1 | redact
  ;;
inventory)
  psql "$ADMIN" -X -tA -F ' | ' 2>&1 <<'SQL' | redact
\echo '== slots'
SELECT slot_name, active, plugin FROM pg_replication_slots;
\echo '== publications'
SELECT pubname FROM pg_publication;
\echo '== public tables'
SELECT c.relname FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace WHERE n.nspname = 'public' AND c.relkind = 'r' ORDER BY 1;
\echo '== ZeBridge functions in public, roles'
SELECT count(*) FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace WHERE n.nspname = 'public' AND p.proname LIKE 'zebridge%';
SELECT rolname FROM pg_roles WHERE rolname IN ('zb_reader', 'zb_writer', 'zebridge_inviter', 'powersync_role', 'ps_api');
SQL
  ;;
count)
  psql "$ADMIN" -X -tA -F ' ' -c "SELECT CASE WHEN id LIKE 'row-%' THEN 'seed' WHEN id LIKE 'm-%' THEN 'markers' WHEN id LIKE 'b%' THEN 'burst' WHEN id LIKE 'w%' THEN 'writes' ELSE id END, count(*), count(DISTINCT tenant_id) AS tenants FROM storm_items GROUP BY 1 ORDER BY 1" 2>&1 | redact
  ;;
rtt)
  # The distance that sets both systems' write figures: 50 TCP connects from the comparison host to the database.
  read -r DBH DBP < <(python3 -c "import sys,urllib.parse as u; p=u.urlparse(sys.argv[1]); print(p.hostname, p.port or 5432)" "$ADMIN")
  ssh_h "python3 - '$DBH' '$DBP'" <<'EOF'
import socket, statistics, sys, time
host, port = sys.argv[1], int(sys.argv[2])
for fam, label in ((socket.AF_INET6, "IPv6"), (socket.AF_INET, "IPv4")):
    try: addr = socket.getaddrinfo(host, port, fam, socket.SOCK_STREAM)[0][4]
    except socket.gaierror: print(f"{label}: no address"); continue
    ms = []
    for _ in range(50):
        s = socket.socket(fam, socket.SOCK_STREAM); s.settimeout(3); t = time.perf_counter()
        try: s.connect(addr); ms.append((time.perf_counter() - t) * 1000)
        except OSError: pass
        finally: s.close()
    print(f"{label}: median {statistics.median(ms):.1f} ms, min {min(ms):.1f}, max {max(ms):.1f} ({len(ms)}/50)" if ms else f"{label}: no connection")
EOF
  ;;
wipe)
  # Slots first, outside a transaction, as PostgreSQL requires; only inactive ones (stop both stacks before).
  psql "$ADMIN" -X -tA -c "SELECT 'dropped slot ' || slot_name || pg_drop_replication_slot(slot_name)::text FROM pg_replication_slots WHERE (slot_name LIKE 'powersync%' OR slot_name = '$BRIDGE_CDC_SLOT') AND NOT active" 2>&1 | redact | grep -v '^$' || true
  admin_sql --single-transaction <<SQL
DROP PUBLICATION IF EXISTS powersync;
DROP PUBLICATION IF EXISTS "$BRIDGE_CDC_PUBLICATION";
DROP SCHEMA IF EXISTS powersync CASCADE;
DROP FUNCTION IF EXISTS public.ps_pre();
DO \$\$ DECLARE r record; BEGIN
  FOR r IN SELECT evtname FROM pg_event_trigger WHERE evtname LIKE 'zebridge%' LOOP
    EXECUTE format('DROP EVENT TRIGGER %I', r.evtname);
  END LOOP;
  FOR r IN SELECT c.relname FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
           WHERE n.nspname = 'public' AND c.relkind = 'r' AND (c.relname LIKE 'zebridge\_%' OR c.relname IN ('storm_items', 'sensor_events')) LOOP
    EXECUTE format('DROP TABLE public.%I CASCADE', r.relname);
  END LOOP;
END \$\$;
DROP SCHEMA IF EXISTS zebridge_private CASCADE;
DO \$\$ DECLARE r record; BEGIN
  FOR r IN SELECT p.oid::regprocedure AS sig FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace
           WHERE n.nspname = 'public' AND p.proname LIKE 'zebridge%' LOOP
    EXECUTE format('DROP FUNCTION IF EXISTS %s CASCADE', r.sig);
  END LOOP;
  -- Not a superuser on a managed database: the administrator must hold a role to drop what it owns.
  FOR r IN SELECT rolname FROM pg_roles WHERE rolname IN ('zb_reader', 'zb_writer', 'zebridge_inviter', 'powersync_role', 'ps_api') LOOP
    EXECUTE format('GRANT %I TO CURRENT_USER', r.rolname);
    EXECUTE format('DROP OWNED BY %I CASCADE', r.rolname);
    EXECUTE format('REVOKE %I FROM CURRENT_USER', r.rolname);
    EXECUTE format('DROP ROLE %I', r.rolname);
  END LOOP;
END \$\$;
SQL
  echo "wiped; check with: db.sh inventory"
  ;;
*) sed -n '2,11p' "$0" | sed 's/^# \{0,1\}//'; exit 2 ;;
esac
