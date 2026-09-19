#!/usr/bin/env python3
"""Who answers a phone's asks: the leaf's responder or the hub's? (NOTES §10hl)

    examples/08-map/leaf/who_answers.py --url nats://127.0.0.1:4223 --asks 50   # a phone on the leaf
    examples/08-map/leaf/who_answers.py --url nats://127.0.0.1:4222 --asks 50   # a phone on the hub

Asks `query._default.pois_near` N times as a client principal and tallies the
`answered_by` label each answer carries, with the median round trip per label.
"""
import argparse, asyncio, json, os, pathlib, statistics, sys, time

ROOT = pathlib.Path(__file__).resolve().parents[3]


async def main(a):
    import nats
    who = pathlib.Path(a.creds).stem
    nc = await nats.connect(a.url, user_credentials=a.creds, name="who-answers",
                            inbox_prefix=f"_INBOX.{who}".encode())
    q = json.dumps({"lat": 47.2184, "lng": -1.5536, "radius_m": 600, "limit": 200}).encode()
    by = {}
    for _ in range(a.asks):
        t0 = time.time()
        try:
            ans = json.loads((await nc.request(f"query.{a.tenant}.pois_near", q, timeout=a.timeout)).data)
            who = ans.get("answered_by", "unlabelled")
        except Exception as e:
            who = f"no answer ({type(e).__name__})"
        by.setdefault(who, []).append((time.time() - t0) * 1000)
    await nc.close()
    print(f"{a.asks} asks on {a.url}:")
    for who, ms in sorted(by.items(), key=lambda kv: -len(kv[1])):
        print(f"  {len(ms):>4}  answered by {who:24} median {statistics.median(ms):.0f} ms")


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--url", default=os.environ.get("NATS_URL", "nats://127.0.0.1:4223"))
    ap.add_argument("--creds", default=str(ROOT / "scripts" / "native" / "creds" / "omar.creds"))
    ap.add_argument("--tenant", default="_default")
    ap.add_argument("--asks", type=int, default=50)
    ap.add_argument("--timeout", type=float, default=5.0)
    asyncio.run(main(ap.parse_args()))
