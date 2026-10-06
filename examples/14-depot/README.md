# 14 — a depot: trucks, chargers and routes, edited together in the browser

Five trucks on a map of France's charge points. Pick a truck, a destination charger, and trace
the route: the truck leaves and drives along the road, on every open screen. Change its
destination while it drives, from any screen, and it turns from where it is.

Two flows meet on the page:

- **Data**, replicated into the browser: `charge_points` (16,173 chargers from OpenChargeMap,
  all of France) and `trucks` (five trucks, each with a depot and a plan). The chargers in view
  are a local query on every pan; no service is asked for them.
- **Questions**, answered on the hub: `query._default.route` goes to the routing service of
  [13-routing](../13-routing), which forwards it to Valhalla. Valhalla knows roads only: the page
  sends coordinates and draws the line it gets back.

PostgreSQL holds the decisions, never the routes. A route's line, about 2,000 points, is computed
by each browser from its trip, and the truck is moved along it by each browser too: every screen
shows it at the same place, and nothing is sent while it drives.

## The plan: one register, and a draft

A truck's `plan` is a JSON document holding one register, `leg`: `{v, t, w}`, a value, a stamp and
its writer (see [COOPERATIVE_EDITING.md](../../COOPERATIVE_EDITING.md)). Its value is the trip
under way: `{from, stops, to, started_at}`, the stops being the places between, in order.

From, the stops and To are a draft, kept in the browser that picks them: choosing a charger writes nothing
and moves no other screen. Only **Trace route** (or **Change destination**) writes, the leg. If two
screens send the same truck somewhere at once, the later stamp wins on every screen; the other
screen says who sent it, and drops the To it had picked.

Each browser asks for the current leg's route, one question through every stop, and walks the
truck along it by the durations
Valhalla gives for each part of the route (slower in town, faster on the motorway), from
`started_at`. The arrival time is `started_at` plus the route's duration; each stop's, the time
Valhalla gives for the stretches before it.

While the truck drives, the panel shows what is left of its trip: the stops ahead, and To. Adding
a stop, removing one or changing To starts from that, and **Update route** writes a new `leg` from
the truck's position at that moment: A → B becomes AB → C, or AB → C → B with a stop added. The start of the new leg is stored as a point,
with the truck's heading, so Valhalla carries on forward rather than plan a U-turn, and a browser
that opens later does not have to replay the old leg.

## 1. The data

The chargers, with their PostGIS `geom` column (enable PostGIS first; on Supabase, `CREATE
EXTENSION postgis` and restart the bridge). The script reads the database from
`ADMIN_DATABASE_URL`, or uses the local one:

```sh
ADMIN_DATABASE_URL=postgresql://… ../08-map/load_chargers.py --create    # 16,173 rows, published
```

The trucks, each based at the charger nearest one town (Nantes, Angers, Saint-Nazaire, Cholet,
La Roche-sur-Yon). `--check` runs everything in a transaction and rolls it back:

```sh
ADMIN_DATABASE_URL=postgresql://… ./load_trucks.py --check
ADMIN_DATABASE_URL=postgresql://… ./load_trucks.py
```

Both tables are public and writable from the edge. A table's description travels as one change
event: `charge_points` has 25 columns, which needs `BASE_BUF=13` (an 8 KB event buffer).

## 2. Routing

The routing service of [13-routing](../13-routing) answers `query._default.route`: Valhalla and
`zb-respond`, on the hub here. Its tiles cover Pays de la Loire; a charger outside gets "no route".

## 3. The page

```sh
cd web
pnpm install
VITE_ZB_BRIDGE_URL=https://bridge.example.com pnpm dev      # http://localhost:5177
```

Open it with an invite: `http://localhost:5177/?invite=<code>`. The identity and the replica stay
in the browser. Two editors in one browser: `?as=a` and `?as=b`, each with its own invite.

Then:

1. Choose a truck. From is its depot.
2. Click To, then a charger. Add stops with **+ Add a stop**, then a charger, as many as needed.
3. Press **Trace route**: the truck leaves, and goes through the stops in order.
4. While it drives, add a stop or change To, and press **Update route**: it turns from where it is.
5. Open **What does my fleet do?**: SQL on the browser's own copy of the data, read-only and
   offline too. The ready queries read the plans' JSON with SQLite's own functions: the
   roadmaps (from, the stops in order, to, since when, sent by whom), every stop on its own
   row, the depots, the fast chargers per town. With **live** on, a truck sent anywhere
   re-runs the query.

## What it measured

On 2026-10-06 and 07, the page on a laptop and two phones, the hub on a VPS (Supabase, the bridge, NATS, Valhalla):

- **A first visit**, from the invite link to the chargers in view: enrolment and the copy of
  16,173 chargers into the browser included, as the page's status line reports it.

  | device | page loaded | ready |
  |---|---|---|
  | laptop browser | 0.2 s | 3.0 s |
  | iPhone, Safari or Chrome | 0.1–0.3 s | 2.5 s |
  | moto e20 (an old Android), Chrome | 0.8 s | 11.5 s |

  The first draw of the chargers in view, 1,237 of them, takes 0.1 s. A returning visit is
  immediate: the replica is still there and only catches up.
- **A route**, Nantes → Angers by truck (92.5 km, 58 min): 88–190 ms in Valhalla, 119–264 ms
  round trip from the browser.
- **A change of destination** on one screen: the new route is drawn at once there, and on the
  other screen as fast as the eye can tell.
