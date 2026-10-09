#!/usr/bin/env python3
"""The map's cells as tenants (NOTES §10fo, option A): a 5×5 grid of geohash-5 cells
around a centre, each an ordinary tenant `c_<geohash>`; one principal, `mapper`,
a member of every cell; one invite, one GET /enroll, and the JWT carries one tag per
cell. The bridge creates a CDC stream per cell at its next boot (it reads the roster
then), and the producer cuts a `pois` chain per cell.

    examples/08-map/provision.py            # roster rows + invite + enrol → creds
    examples/08-map/provision.py --seed     # also a few POIs, each in its cell

Needs the dev stack up and the bridge started with the enrol endpoint armed. The creds
land in scripts/native/creds/mapper.creds (git-ignored). Restart the bridge after the
first run so the cells' streams exist.
"""
import json, os, pathlib, re, subprocess, sys, urllib.request

ROOT = pathlib.Path(__file__).resolve().parents[2]
PRINCIPAL = "mapper"
CENTRE = (47.22, -1.585)  # Nantes
PSQL = os.environ.get("ZB_PSQL", "/opt/homebrew/opt/postgresql@18/bin/psql")
B32 = "0123456789bcdefghjkmnpqrstuvwxyz"


def geohash(lat, lng, precision=5):
    lat_r, lng_r = [-90.0, 90.0], [-180.0, 180.0]
    out, bits, ch, even = "", 0, 0, True
    while len(out) < precision:
        r, v = (lng_r, lng) if even else (lat_r, lat)
        mid = (r[0] + r[1]) / 2
        if v >= mid:
            ch = ch * 2 + 1; r[0] = mid
        else:
            ch = ch * 2; r[1] = mid
        even = not even; bits += 1
        if bits == 5:
            out += B32[ch]; bits = ch = 0
    return out


def grid(centre, precision=5, radius=2):
    """The cells around the centre's cell: (2r+1)² geohashes, by stepping one cell."""
    # Longitude takes the odd bits: ceil(5p/2) of them, latitude floor(5p/2).
    lng_span = 360.0 / (2 ** ((5 * precision + 1) // 2))
    lat_span = 180.0 / (2 ** (5 * precision // 2))
    cells = []
    for i in range(-radius, radius + 1):
        for j in range(-radius, radius + 1):
            cells.append(geohash(centre[0] + i * lat_span, centre[1] + j * lng_span, precision))
    return sorted(set(cells))


def psql(sql):
    r = subprocess.run([PSQL, "-h", "127.0.0.1", "-p", "5432", "-U", "postgres", "-d", "postgres", "-X", "-A", "-t", "-q", "-c", sql],
                       capture_output=True, text=True)
    if r.returncode != 0:
        sys.exit(r.stderr.strip())
    return r.stdout.strip()


def main():
    cells = grid(CENTRE)
    tenants = ["c_" + c for c in cells]
    values = ", ".join(f"('{PRINCIPAL}', '{t}')" for t in tenants)
    psql(f"INSERT INTO public.zebridge_user_tenants (principal, tenant_id) VALUES {values} ON CONFLICT DO NOTHING")
    print(f"{PRINCIPAL}: member of {len(tenants)} cells ({tenants[0]} … {tenants[-1]})")

    creds = ROOT / "scripts/native/creds" / f"{PRINCIPAL}.creds"
    code = os.urandom(16).hex()
    psql(f"INSERT INTO public.zebridge_invites (code, principal, tenant_id, expires_at) "
         f"VALUES ('{code}', '{PRINCIPAL}', '{tenants[0]}', now() + interval '10 minutes')")
    gen = subprocess.run([str(ROOT / "zig-out/bin/bridge"), "--gen-nkey"], capture_output=True, text=True)
    user_pub = re.search(r"NATS_BRIDGE_NKEY_PUB=(U[A-Z0-9]+)", gen.stdout).group(1)
    user_seed = re.search(r"NATS_BRIDGE_NKEY_SEED=(SU[A-Z0-9]+)", gen.stdout).group(1)
    port = os.environ.get("BRIDGE_PORT", "27434")
    with urllib.request.urlopen(f"http://127.0.0.1:{port}/enroll?code={code}&user_pubkey={user_pub}", timeout=10) as r:
        payload = json.loads(r.read().decode())
    creds.write_text("-----BEGIN NATS USER JWT-----\n" + payload["jwt"] + "\n------END NATS USER JWT------\n\n"
                     "-----BEGIN USER NKEY SEED-----\n" + user_seed + "\n------END USER NKEY SEED------\n")
    creds.chmod(0o600)
    print(f"enrolled: {creds} (one tag per cell — restart the bridge once so every cell has its stream)")

    if "--seed" in sys.argv:
        pois = [(47.2184, -1.5536, "Château des ducs de Bretagne"), (47.2126, -1.5647, "Les Machines de l'île"),
                (47.2144, -1.5602, "Passage Pommeraye"), (47.2075, -1.5545, "Jardin des plantes"),
                (47.2470, -1.6050, "Parc de la Chantrerie"), (47.1900, -1.5300, "Rezé, Trentemoult")]
        rows = ", ".join(f"({lat}, {lng}, '{note.replace(chr(39), chr(39)*2)}', 'c_{geohash(lat, lng)}')" for lat, lng, note in pois)
        psql(f"INSERT INTO public.pois (lat, lng, note, tenant_id) VALUES {rows}")
        print(f"seeded {len(pois)} POI(s), each under its cell")


if __name__ == "__main__":
    main()
