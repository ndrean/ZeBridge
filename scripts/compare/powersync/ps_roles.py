"""PowerSync's side of the database, run by `powersync.sh roles` (after `db.sh init` and `db.sh table`):

- `powersync_role`, the service's replication login, and the publication `powersync` on the comparison table;
- `ps_api`, PostgREST's login: it switches to ZeBridge's write role `zb_writer`, so PowerSync's uploads meet the same
  tenant policy and triggers as ZeBridge's writes (no BYPASSRLS);
- `ps_pre()`, PostgREST's pre-request function: it sets `zb.principal` from the request, as the bridge does in each
  write's transaction (the policy checks it), and the principals p00..p09 mapped to tenant `cmp`.

Passwords and the JWT signing key are generated here and kept, with the connection URIs, in 0600 files in
./secrets. The administrator URL comes from the environment (CMP_ADMIN_URL, set by powersync.sh after the database
guard). Prints no secret."""
import os, pathlib, re, secrets, subprocess, urllib.parse as up

SEC = pathlib.Path(__file__).parent / "secrets"
SEC.mkdir(mode=0o700, exist_ok=True)
admin = os.environ["CMP_ADMIN_URL"]
psql = lambda sql: subprocess.run(["psql", admin, "-X", "-tA", "-v", "ON_ERROR_STOP=1", "-c", sql], capture_output=True, text=True)


def save(name, value):
    fd = os.open(SEC / name, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600); os.write(fd, value.encode()); os.close(fd)


def uri_for(user, password):
    p = up.urlparse(admin)
    q = dict(up.parse_qsl(p.query)); q.setdefault("sslmode", "require")
    return up.urlunparse(p._replace(netloc=f"{up.quote(user)}:{up.quote(password)}@{p.hostname}:{p.port or 5432}", query=up.urlencode(q)))


rep_pw, api_pw = secrets.token_urlsafe(24), secrets.token_urlsafe(24)
steps = [
    "DROP PUBLICATION IF EXISTS powersync",
    "DO $$ BEGIN IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'powersync_role') THEN CREATE ROLE powersync_role WITH REPLICATION BYPASSRLS LOGIN; END IF; END $$",
    f"ALTER ROLE powersync_role WITH PASSWORD '{rep_pw}'",
    "GRANT SELECT ON public.storm_items TO powersync_role",
    "CREATE PUBLICATION powersync FOR TABLE public.storm_items",
    "DO $$ BEGIN IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'ps_api') THEN CREATE ROLE ps_api WITH LOGIN NOINHERIT; END IF; END $$",
    f"ALTER ROLE ps_api WITH PASSWORD '{api_pw}'",
    "GRANT zb_writer TO ps_api",
    "CREATE OR REPLACE FUNCTION public.ps_pre() RETURNS void LANGUAGE sql AS $f$ SELECT set_config('zb.principal', current_setting('request.headers', true)::json->>'x-principal', true) $f$",
    "GRANT EXECUTE ON FUNCTION public.ps_pre() TO zb_writer",
    "INSERT INTO zebridge_user_tenants (principal, tenant_id) SELECT 'p' || lpad(i::text, 2, '0'), 'cmp' FROM generate_series(0, 9) i ON CONFLICT DO NOTHING",
]
failed = 0
for sql in steps:
    r = psql(sql)
    failed += r.returncode != 0
    label = re.sub(r"PASSWORD '[^']*'", "PASSWORD '***'", sql)[:90]
    print(("ok   " if r.returncode == 0 else "FAIL ") + label + ("" if r.returncode == 0 else "  -> " + re.sub(r"postgres\w*://\S+", "<url>", r.stderr.strip())[:160]))
if failed:
    raise SystemExit(f"{failed} step(s) failed: the secrets were not written")
save("replication.uri", uri_for("powersync_role", rep_pw))
save("api.uri", uri_for("ps_api", api_pw))
save("admin.uri", admin)   # the burst phase inserts as the administrator
save("jwt.key", secrets.token_urlsafe(32))
if not (SEC / "store.pw").exists():
    save("store.pw", secrets.token_urlsafe(24))
print("secrets kept in ./secrets:", sorted(p.name for p in SEC.iterdir()))
