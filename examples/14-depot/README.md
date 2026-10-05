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

## The plan: three registers

A truck's `plan` is a JSON document of three registers, each `{v, t, w}`: a value, a stamp and
its writer (see [COOPERATIVE_EDITING.md](../../COOPERATIVE_EDITING.md)).

| register | holds | written by |
|---|---|---|
| `from`, `to` | the planned ends, a charger each | a click on a charger |
| `leg` | the trip under way: `{from, to, started_at}` | Trace route, or Change destination |

Two people setting different ends both keep their change; on the same register, the later stamp
wins on every screen, and the other screen says who changed it.

Each browser asks for the current leg's route and walks the truck along it by the durations
Valhalla gives for each part of the route (slower in town, faster on the motorway), from
`started_at`. The arrival time is `started_at` plus the route's duration.

A new destination while the truck drives writes a new `leg` from the truck's position at that
moment to the new charger: A → B becomes AB → C. The start of the new leg is stored as a point,
so a browser that opens later does not have to replay the old one.

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
2. Click To, then a charger, and press **Trace route**: the truck leaves.
3. While it drives, set another To and press **Change destination**: it turns towards it.

## What it measured

On 2026-10-06, the page on a laptop, the hub on a VPS (Supabase, the bridge, NATS, Valhalla):

- **A first visit**, from the invite link to the chargers in view: about 3 s, enrolment and the
  copy of 16,173 chargers into the browser included. A returning visit is immediate.
- **A route**, Nantes → Angers by truck (92.5 km, 58 min): 88–190 ms in Valhalla, 119–264 ms
  round trip from the browser.
- **A change of destination** on one screen: the new route is drawn at once there, and on the
  other screen as fast as the eye can tell.
