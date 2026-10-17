# Reconnect storm

N real libzb clients (the published `zebridge` Python package, many in one process) started at once against a ZeBridge stack, timed: how long each takes to be synced, how fast one write reaches all of them, and how long they take to come back when NATS restarts. A test tool for a scratch server, never for a server in use.

## Once, on the server

The table, 10,000 rows in tenant `storm` ([setup.sql](setup.sql)), then N invites, then one identity per invite. The enrollment runs inside the server's network, at the bridge's own port, so a proxy's rate limit does not slow it (here with the compose stack of `deploy/compose`):

```sh
docker compose exec -T postgres psql -U postgres -d zebridge -v pub=zb_pub < setup.sql
docker compose exec -T postgres psql -U postgres -d zebridge -XAtc \
  "SELECT zebridge_invite('storm-' || lpad(i::text, 4, '0'), 'storm', interval '1 day') FROM generate_series(1, 500) AS i" > codes.txt
docker run --rm --network zebridge_default -v "$PWD":/storm python:3.13-slim sh -c \
  "pip install -q zebridge && python /storm/storm.py enroll /storm/codes.txt /storm/ids --bridge http://bridge:27434"
```

Then copy `ids/` to the machine that runs the storm. The identities are secrets: keep them there, 0700.

## The storm

```sh
python3 -m venv .venv && .venv/bin/pip install zebridge
.venv/bin/python storm.py run ids 100 --bridge https://bridge.example.com --markers 60
.venv/bin/python storm.py run ids 100 --bridge https://bridge.example.com --markers 60 --warm
```

- **Cold** (the default): each client starts with no replica and seeds the table.
- **Warm** (`--warm`): the replicas of the last run are kept, as after a server restart.
- **Markers** (`--markers S`): for S seconds, client 0 writes one row a second and every client records when each arrives. Restart NATS on the server during that time (`docker compose restart nats`): each client's longest gap is how long it took to come back.

It prints the median, the 95th percentile and the slowest time to synced, the marker latency, the longest gap per client, and the process's memory per client. `--json FILE` keeps every number.
