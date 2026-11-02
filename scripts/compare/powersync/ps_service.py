"""PowerSync and PostgREST on the comparison host, run by `powersync.sh up` (after `powersync.sh roles`).

Three containers, host network, local-only ports, each with a memory cap so a load cannot take the host down:
- `zbcmp-psstore`: PostgreSQL 17 on 127.0.0.1:15432, PowerSync's bucket storage, local to the service as
  PowerSync's own deployments place it (768 MB);
- `zbcmp-powersync`: the PowerSync service on 127.0.0.1:18080, replicating from the comparison database, Sync
  Streams: one auto-subscribed stream over storm_items, one nobody subscribes to over sensor_events (1 GB);
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
    port: 18080
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
files = {
    "ps.env": f"POWERSYNC_CONFIG_B64={base64.b64encode(config.encode()).decode()}\n",
    "rest.env": f"PGRST_DB_URI={rd('api.uri')}\nPGRST_DB_ANON_ROLE=zb_writer\nPGRST_DB_PRE_REQUEST=public.ps_pre\nPGRST_DB_SCHEMAS=public\nPGRST_SERVER_HOST=127.0.0.1\nPGRST_SERVER_PORT=13000\n",
    "store.env": f"POSTGRES_PASSWORD={rd('store.pw')}\nPGPORT=15432\n",
}
for name, body in files.items():
    subprocess.run(["ssh", "-o", "BatchMode=yes", H, f"mkdir -p -m 700 ~/zbcmp/ps && umask 077 && cat > ~/zbcmp/ps/{name}"], input=body, text=True, check=True)
remote = r"""
set -e
cd ~/zbcmp/ps
sudo docker rm -f zbcmp-powersync zbcmp-postgrest zbcmp-psstore >/dev/null 2>&1 || true
sudo docker run -d --name zbcmp-psstore --network host --memory 768m --env-file store.env postgres:17 -c listen_addresses=127.0.0.1 >/dev/null
for i in $(seq 60); do sudo docker exec zbcmp-psstore pg_isready -h 127.0.0.1 -p 15432 -q && break; sleep 1; done
sudo docker run -d --name zbcmp-postgrest --network host --memory 256m --env-file rest.env postgrest/postgrest:latest >/dev/null
sudo docker run -d --name zbcmp-powersync --network host --memory 1g --env-file ps.env journeyapps/powersync-service:latest >/dev/null
for i in $(seq 90); do curl -s -o /dev/null -w "%{http_code}" http://127.0.0.1:18080/probes/liveness 2>/dev/null | grep -q 200 && break; sleep 2; done
echo "powersync liveness: $(curl -s -o /dev/null -w '%{http_code}' http://127.0.0.1:18080/probes/liveness), $(sudo docker logs zbcmp-powersync 2>&1 | grep -oE 'PowerSync Service v[0-9.]+' | head -1)"
echo "postgrest: $(curl -s -o /dev/null -w '%{http_code}' 'http://127.0.0.1:13000/storm_items?limit=1')"
sudo docker ps --filter name=zbcmp- --format '{{.Names}} {{.Image}} {{.Status}}'
"""
print(subprocess.run(["ssh", "-o", "BatchMode=yes", H, remote], capture_output=True, text=True).stdout, end="")
