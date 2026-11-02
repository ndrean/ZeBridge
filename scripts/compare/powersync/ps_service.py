"""PowerSync and PostgREST on the comparison host, run by `powersync.sh up` (after `powersync.sh roles`).

Three containers, host network, local-only ports, each with a memory cap so a load cannot take the host down:
- `zbcmp-psstore`: PostgreSQL 17 on 127.0.0.1:15432, PowerSync's bucket storage, local to the service as
  PowerSync's own deployments place it (768 MB);
- `zbcmp-powersync`: PowerSync's replication process (`start -r sync`, 127.0.0.1:18080), and `zbcmp-psapi1..5` its API
  processes (`start -r api`, 127.0.0.1:18081..), as PowerSync's production layout has them: one per 100
  connections, 1 GB each. Sync Streams: one auto-subscribed stream over storm_items, one nobody subscribes to over
  sensor_events;
- `zbcmp-postgrest`: PostgREST on 127.0.0.1:13000, the backend `uploadData` writes to, as `zb_writer` with
  `ps_pre()` (256 MB).

The configuration is written here from ./secrets and copied 0600 to ~/zbcmp/ps on the host (CMP_HOST, from
powersync.sh). Prints no secret."""
import base64, os, pathlib, subprocess, textwrap

SEC = pathlib.Path(__file__).parent / "secrets"
H = os.environ["CMP_HOST"]
rd = lambda n: (SEC / n).read_text().strip()

config = textwrap.dedent(f"""\
    replication:
      connections:
        - type: postgresql
          uri: "{rd('replication.uri')}"
    storage:
      type: postgresql
      uri: "postgresql://postgres:{rd('store.pw')}@127.0.0.1:15432/postgres"
      sslmode: disable
    port: PORT_HERE
    sync_rules:
      content: |
        config:
          edition: 3
        streams:
          items:
            auto_subscribe: true
            query: SELECT id, tenant_id, note, updated_at FROM storm_items
          events:
            auto_subscribe: false
            query: SELECT event_id AS id, tenant_id, sensor_id, kind, value, updated_at FROM sensor_events
    client_auth:
      audience: ["powersync-cmp"]
      jwks:
        keys:
          - kty: oct
            alg: HS256
            kid: cmp-key
            k: "{rd('jwt.key')}"
    """)
# PowerSync's production layout (docs, "Deployment Architecture"): one replication process (`start -r sync`) and one
# API process (`start -r api`) per 100 connections, 1 GB each, the Node heap at 80% of it. APIS API processes on
# 18081.., the replication one on 18080; ps_crowd.mjs spreads its readers over the API ports, as a load balancer would.
APIS = int(os.environ.get("PS_API_PROCESSES", "5"))
# PostgREST's connections to PostgreSQL: its default is 10; count them against the database's max_connections.
POOL = int(os.environ.get("PS_REST_POOL", "10"))
ps_env = lambda port: f"POWERSYNC_CONFIG_B64={base64.b64encode(config.replace('PORT_HERE', str(port)).encode()).decode()}\nNODE_OPTIONS=--max-old-space-size-percentage=80\n"
files = {
    "ps.env": ps_env(18080),
    **{f"ps-api{k}.env": ps_env(18080 + k) for k in range(1, APIS + 1)},
    "rest.env": f"PGRST_DB_URI={rd('api.uri')}\nPGRST_DB_ANON_ROLE=zb_writer\nPGRST_DB_PRE_REQUEST=public.ps_pre\nPGRST_DB_SCHEMAS=public\nPGRST_SERVER_HOST=127.0.0.1\nPGRST_SERVER_PORT=13000\nPGRST_DB_POOL={POOL}\n",
    "store.env": f"POSTGRES_PASSWORD={rd('store.pw')}\nPGPORT=15432\n",
}
for name, body in files.items():
    subprocess.run(["ssh", "-o", "BatchMode=yes", H, f"mkdir -p -m 700 ~/zbcmp/ps && umask 077 && cat > ~/zbcmp/ps/{name}"], input=body, text=True, check=True)
remote = r"""
set -e
cd ~/zbcmp/ps
sudo docker ps -aq --filter name=zbcmp- | xargs -r sudo docker rm -f >/dev/null
sudo docker run -d --name zbcmp-psstore --network host --memory 768m --env-file store.env postgres:17 -c listen_addresses=127.0.0.1 >/dev/null
for i in $(seq 60); do sudo docker exec zbcmp-psstore pg_isready -h 127.0.0.1 -p 15432 -q && break; sleep 1; done
sudo docker run -d --name zbcmp-postgrest --network host --memory 256m --env-file rest.env postgrest/postgrest:latest >/dev/null
sudo docker run -d --name zbcmp-powersync --network host --memory 1g --env-file ps.env journeyapps/powersync-service:latest start -r sync >/dev/null
for k in $(seq APIS_HERE); do sudo docker run -d --name zbcmp-psapi$k --network host --memory 1g --env-file ps-api$k.env journeyapps/powersync-service:latest start -r api >/dev/null; done
for k in $(seq APIS_HERE); do port=$((18080 + k)); for i in $(seq 90); do curl -s -o /dev/null -w "%{http_code}" http://127.0.0.1:$port/probes/liveness 2>/dev/null | grep -q 200 && break; sleep 2; done; done
echo "powersync: one replication process (start -r sync), APIS_HERE API processes (start -r api) on 18081..$((18080 + APIS_HERE)), liveness $(for k in $(seq APIS_HERE); do curl -s -o /dev/null -w '%{http_code} ' http://127.0.0.1:$((18080 + k))/probes/liveness; done)$(sudo docker logs zbcmp-powersync 2>&1 | grep -oE 'PowerSync Service v[0-9.]+' | head -1)"
echo "postgrest: $(curl -s -o /dev/null -w '%{http_code}' 'http://127.0.0.1:13000/storm_items?limit=1')"
sudo docker ps --filter name=zbcmp- --format '{{.Names}} {{.Image}} {{.Status}}'
"""
print(subprocess.run(["ssh", "-o", "BatchMode=yes", H, remote.replace("APIS_HERE", str(APIS))], capture_output=True, text=True).stdout, end="")
