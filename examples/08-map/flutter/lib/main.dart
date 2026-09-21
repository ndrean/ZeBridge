/// ZeMap — one screen: France's vector tiles from R2, the OpenStreetMap points of
/// interest around the viewport as markers, editable (NOTES §10hj, design B).
///
/// The phone holds what it asked for. `charge_points` is an ON-DEMAND table: libzb creates it
/// from the descriptor, nothing seeds it and nothing is tailed — on every move the map
/// asks the map service (`query._default.chargers_near`, a DuckDB replica of all of France
/// answering from its own copy, PostgreSQL never asked) for the points around the
/// centre, keeps the answer in its SQLite through the version-guarded upsert, and draws
/// from the local table. Offline, every area visited is still there. An edit (add, rename,
/// erase) is a `mutate` on that same local table: optimistic at once, sent to the
/// bridge, judged upstream, and the service's replica has it before the next ask.
///
/// Threading: the libzb handle is NOT thread-safe (one thread drives one client), so
/// every call on it — sync, the blocking poll loop, query, mutate, request, ingest,
/// flush, close — runs on ONE long-lived worker isolate (`zebridge_worker.dart`). The
/// UI isolate only sends messages.
library;

import 'dart:async';
import 'dart:convert';
import 'dart:io';
import 'dart:math';
import 'dart:typed_data';

// import 'package:flutter/foundation.dart' show debugPrint;
import 'package:flutter/material.dart';
import 'package:flutter_map/flutter_map.dart';
import 'package:latlong2/latlong.dart';
import 'package:pmtiles/pmtiles.dart';
import 'package:vector_map_tiles/vector_map_tiles.dart';
import 'package:vector_map_tiles_pmtiles/vector_map_tiles_pmtiles.dart';
import 'package:vector_tile_renderer/vector_tile_renderer.dart'
    show ProvidedThemes;

import 'zebridge.dart' show PollReport;
import 'zebridge_worker.dart';

// Dev copy: the repository paths, like 05-mobile. A shipped app bundles the creds it
// enrolled and the pmtiles it downloaded.
const _repo = '/Users/nevendrean/code/zig/ZeBridge';
// `omar`, a demo principal of the dev stack (its tenant is `kilo`); the `mapper` of the
// cell design was revoked with its grid, and a revoked principal stays revoked.
const _credsPath = '$_repo/scripts/native/creds/omar.creds';
const _pmtilesPath = '$_repo/examples/08-map/flutter/test_region.pmtiles';

/// France at zoom 14 (3.4 GB) on Cloudflare R2, behind the Worker in ../worker: read
/// through HTTP range requests, so only the tiles in view travel, cached at the edge.
/// The local extract is the offline fallback.
const _pmtilesUrl = 'https://ze-map-worker.ze-map.workers.dev/france.pmtiles';
final _dbPath = '${Directory.systemTemp.path}/zb-flutter-map-omar.sqlite3';

/// The tenant the POI service answers for (it serves `_default` and `kilo`).
const _queryTenant = '_default';
const _table = 'charge_points';

void main() => runApp(const ZeMapApp());

class ZeMapApp extends StatelessWidget {
  const ZeMapApp({super.key});

  @override
  Widget build(BuildContext context) => MaterialApp(
        title: 'ZeMap',
        theme: ThemeData(
            colorScheme: ColorScheme.fromSeed(seedColor: Colors.teal),
            useMaterial3: true),
        home: const MapScreen(),
      );
}

class MapScreen extends StatefulWidget {
  const MapScreen({super.key});

  @override
  State<MapScreen> createState() => _MapScreenState();
}

class _MapScreenState extends State<MapScreen> with WidgetsBindingObserver {
  ZeBridgeWorker? zb;
  StreamSubscription<PollReport>? reportsSub;
  String status = 'connecting…';
  List<Map<String, dynamic>> pois = const [];
  bool addingMode = false;
  VectorTileLayer? vectorLayer;
  _CountingTiles? tiles;

  /// The viewport last asked for; a move is asked once the previous ask landed.
  LatLng? wantedCentre;
  double wantedRadius = 0;
  double wantedFuelRadius = 3000;
  bool asking = false;
  Timer? askDebounce;
  Timer? refreshDebounce;
  int held = 0;

  /// The fuel switch: null is off; a fuel asks the service for the stations around the
  /// centre after each move and draws them as price tags. An overlay from the service's
  /// replica, not a local table — prices move several times a day.
  /// §10hr: the charge-point switch, the same shape as the fuel one. Null is off — the
  /// map asks for nothing and draws nothing, keeping what it already holds; a value is
  /// the minimum power in kW, 0 meaning every charger. 43 is the rapid band.
  double? chargers = 0;
  String? fuel;
  List<Map<String, dynamic>> stations = const [];

  /// Route mode (§10ho): the ONE shared route, edited by two people at once. The row
  /// `routes.doc` is a map of registers — `start` and `end`, each with the writer's
  /// stamp and name; a tap writes this phone's register merged into the document it
  /// last saw (the library's `mergeRegisters`, through `zb_call`), and the pins are
  /// drawn from the DOCUMENT as CDC delivers it, never from the tap itself. So what is
  /// on screen is what the row holds, whoever moved it last.
  bool routeMode = false;
  Map<String, dynamic> routeDoc = {};
  final routeMine = <String,
      dynamic>{}; // this phone's own registers, shipped whole on every write
  int routeRounds = 0;
  String routeNext = 'start';
  List<MapEntry<String, LatLng>> routePoints = const [];
  List<LatLng> routeLine = const [];
  String routeInfo = '';
  static const _routeId = '11111111-1111-4111-8111-111111111111';
  static const _routeWriter = 'phone-omar';
  final mapController = MapController();

  Map<String, dynamic>? get _cheapest {
    Map<String, dynamic>? best;
    for (final st in stations) {
      if (st['outage'] != null) continue;
      if (best == null ||
          double.parse(_price(st)) < double.parse(_price(best))) {
        best = st;
      }
    }
    return best;
  }

  @override
  void initState() {
    super.initState();
    WidgetsBinding.instance.addObserver(this);
    _initTiles();
    _initZeBridge();
  }

  /// Background: pause the poll loop (nothing touches the broker); foreground: resume,
  /// the next poll catches up and the next flush sends what was written meanwhile.
  @override
  void didChangeAppLifecycleState(AppLifecycleState state) {
    final w = zb;
    if (w == null) return;
    if (state == AppLifecycleState.resumed) {
      w.resume();
    } else {
      w.pause();
    }
  }

  Future<void> _initTiles() async {
    try {
      final archive =
          await PmTilesArchive.from(_pmtilesUrl).catchError((Object e) {
        debugPrint(
            'pmtiles: $_pmtilesUrl unreachable ($e) — the local extract instead');
        return PmTilesArchive.fromFile(File(_pmtilesPath));
      });
      if (!mounted) return;
      // The counters redraw the status line at most twice a second: a rebuild per tile
      // request would itself disturb the tiles' loading.
      Timer? tick;
      tiles =
          _CountingTiles(PmTilesVectorTileProvider.fromArchive(archive), () {
        tick ??= Timer(const Duration(milliseconds: 500), () {
          tick = null;
          if (mounted) setState(() {});
        });
      });
      setState(() {
        // The archive is Planetiler's default profile: the OpenMapTiles schema (16 layers:
        // water, transportation, building, place, …). A theme names its source and its
        // layers; Protomaps's theme drew only the water layer the two schemas share.
        vectorLayer = VectorTileLayer(
          theme: ProvidedThemes.lightTheme(),
          tileProviders: TileProviders({'openmaptiles': tiles!}),
          // Vector mode paints the tiles itself. The default raster mode renders each
          // tile to an image for Flutter's image cache, and those render jobs were
          // cancelled as the view moved ("CancellationException … IMAGE RESOURCE
          // SERVICE"): tiles beyond the first view never finished. Vector mode also
          // overzooms the archive's zoom-14 tiles at 15 without a substitution step.
          layerMode: VectorTileLayerMode.vector,
          showTileDebugInfo:
              false, // true draws each tile's id: what the layer asks for as you pan
        );
      });
    } catch (e) {
      debugPrint('pmtiles: $e (falling back to OSM raster tiles)');
    }
  }

  Future<void> _initZeBridge() async {
    try {
      final worker = await ZeBridgeWorker.spawn({
        'natsUrl': 'nats://127.0.0.1:4222',
        'credsPath': _credsPath,
        'dbPath': _dbPath,
        'principal': 'omar',
        'ondemandTables': [_table],
        'tables': [
          'routes'
        ], // §10ho: the shared route, seeded and tailed like any row
        'clientId': 'flutter-map',
      });
      if (!mounted) {
        await worker.close();
        return;
      }
      zb = worker;
      setState(() => status = 'connected as ${worker.tenant}');
      // What the phone already holds is on screen before the first answer arrives.
      await _refresh();
      _wantArea(mapController.camera);
      // An edit's verdict changed the table: redraw.
      reportsSub = worker.reports.listen((r) {
        if (r.error != null) {
          setState(() =>
              status = 'offline: ${r.error} — showing what the phone holds');
          return;
        }
        if (r.changedTables.contains(_table)) _refresh();
        if (r.changedTables.contains('routes')) _onRouteChanged();
      });
    } catch (e) {
      if (mounted) {
        setState(
            () => status = 'not connected: $e — showing what the phone holds');
      }
    }
  }

  /// The replica is what the screen shows: the points held inside the viewport.
  /// A v4 uuid from Dart's own random: the key of a charge point this phone adds.
  static String _uuidV4() {
    final r = Random.secure();
    final b = List<int>.generate(16, (_) => r.nextInt(256));
    b[6] = (b[6] & 0x0f) | 0x40;
    b[8] = (b[8] & 0x3f) | 0x80;
    final h = b.map((x) => x.toRadixString(16).padLeft(2, '0')).join();
    return '${h.substring(0, 8)}-${h.substring(8, 12)}-${h.substring(12, 16)}-${h.substring(16, 20)}-${h.substring(20)}';
  }

  Future<void> _refresh() async {
    final w = zb;
    if (w == null) return;
    try {
      final b = mapController.camera.visibleBounds;
      final rows = await w.query(
        'SELECT id, lat, lng, title, max_power_kw, points, status_type_id, ocm_id FROM $_table '
        'WHERE lat BETWEEN ? AND ? AND lng BETWEEN ? AND ? '
        'AND coalesce(max_power_kw, 0) >= ? LIMIT 3000',
        [b.south, b.north, b.west, b.east, chargers ?? 0],
      );
      final all = await w.query('SELECT count(*) AS n FROM $_table');
      if (!mounted) return;
      setState(() {
        pois = rows;
        held = (all.first['n'] as num).toInt();
      });
    } catch (e) {
      if (mounted) setState(() => status = 'query: $e');
    }
  }

  /// The viewport moved: ask for what is around its centre. Debounced — a pan is many
  /// events — and one ask at a time. The radius is half the visible diagonal, at most
  /// 150 km for the chargers and 20 km for the stations, which are sparse. Below zoom
  /// 10 (a département on screen) the map stops asking and draws what it holds.
  ///
  /// §10hu: the charger cap was 5 km, which kept every answer inline and made the
  /// large-answer path unreachable from the app. Measured before raising it, from
  /// Nantes with this same limit of 2,000: an 814 KB answer costs about 5 ms more at
  /// the median than a 6.9 KB one, because the object fetch is 2 ms and the
  /// responder's 100 ms poll wait dominates both. The cap was buying nothing and it
  /// hid the mechanism the demo exists to show. `limit` still bounds the answer:
  /// zoomed out this is the 2,000 nearest, a disc around the centre.
  void _wantArea(MapCamera camera) {
    if (camera.zoom < 10) {
      wantedCentre = null;
      return;
    }
    final b = camera.visibleBounds;
    final half =
        const Distance().as(LengthUnit.Meter, b.southWest, b.northEast) / 2;
    // §10hv: the geometry is recorded BEFORE any switch is consulted. It used to be
    // computed after an early return on the charger switch, so with chargers off the
    // fuel radius kept its initial 3 km however far out the map was zoomed.
    wantedCentre = camera.center;
    wantedRadius = min(150000.0, max(150.0, half));
    wantedFuelRadius = min(20000.0, max(3000.0, half));
    // Both off: the viewport is up to date and there is simply nothing to ask for.
    if (chargers == null && fuel == null) return;
    askDebounce?.cancel();
    askDebounce = Timer(const Duration(milliseconds: 350), _askArea);
  }

  /// §10hv: one debounce, two INDEPENDENT asks — the browser's shape, where `moveend`
  /// tests each switch on its own. The fuel ask used to be the last statement inside
  /// the charger ask's `try`, so fuel went stale whenever chargers were switched off
  /// or a charger request failed. Each ask reports its own failure and neither can
  /// cancel the other.
  Future<void> _askArea() async {
    final at = wantedCentre;
    if (zb == null || at == null || asking) return;
    asking = true;
    try {
      if (chargers != null) await _ask(at);
      if (fuel != null) await _askFuel(at);
    } finally {
      asking = false;
    }
    // The viewport may have moved on while this was in flight.
    final again = wantedCentre;
    if (again != null && again != at) _askArea();
  }

  /// §10hu: how the answer travelled, the SAME way for every dataset. Both libraries
  /// splice `zb_transport` into every answer, so fuel and chargers are read on one
  /// clock: `wire` is ask→reply (it carries the responder's poll wait, which dominates
  /// everything else), `fetch` is the object read and is 0 when the answer came
  /// inline, `db` is the service's own query time.
  String _transport(Map ans) {
    final t = ans['zb_transport'] as Map?;
    if (t == null) return 'db ${ans['ms']} ms';
    final kb = ((t['bytes'] as num) / 1024).toStringAsFixed(1);
    final via = t['via'] == 'object'
        ? 'object $kb KB · fetch ${t['fetch_ms']} ms'
        : 'inline $kb KB';
    return '$via · wire ${t['wire_ms']} ms · db ${ans['ms']} ms';
  }

  Future<void> _ask(LatLng at) async {
    final w = zb;
    if (w == null) return;
    final radius = wantedRadius;
    final t0 = DateTime.now();
    try {
      final ans = await w.request('query.$_queryTenant.chargers_near', {
        'lat': at.latitude,
        'lng': at.longitude,
        'radius_m': radius,
        if ((chargers ?? 0) > 0) 'min_kw': chargers,
        'limit': 2000
      });
      // The scope — "what I hold in this box and the answer lacks is gone" — only when
      // the answer is complete; one cut by the limit says nothing about the rest.
      final dlat = radius / 111320.0;
      final dlng = radius / (111320.0 * max(0.1, cos(at.latitude * pi / 180)));
      final scope = ans['complete'] == true
          ? {
              'where': 'lat BETWEEN ? AND ? AND lng BETWEEN ? AND ?',
              'params': [
                at.latitude - dlat,
                at.latitude + dlat,
                at.longitude - dlng,
                at.longitude + dlng
              ],
            }
          : null;
      final tIngest = DateTime.now();
      final applied = await w.ingest(_table, ans, scope);
      final ingestMs = DateTime.now().difference(tIngest).inMilliseconds;
      final ms = DateTime.now().difference(t0).inMilliseconds;
      if (mounted) {
        setState(() => status =
            '${ans['count']} charger(s) within ${(radius / 1000).toStringAsFixed(0)} km · '
            '${_transport(ans)} · ingest $ingestMs ms · $applied kept · $ms ms total');
      }
      await _refresh();
    } catch (e) {
      // Caught HERE, not by the caller: the fuel ask that follows must still run.
      if (mounted) {
        setState(() => status = 'chargers: $e — showing what the phone holds');
      }
    }
  }

  /// The stations selling the chosen fuel around the centre, nearest first — the
  /// service joins its fuel tables (load_fuel.py) in DuckDB; PostgreSQL is never asked.
  Future<void> _askFuel(LatLng at) async {
    final w = zb;
    final f = fuel;
    if (w == null || f == null) {
      if (stations.isNotEmpty && mounted) setState(() => stations = const []);
      return;
    }
    final radius = wantedFuelRadius;
    try {
      final ans = await w.request('query.$_queryTenant.fuel_near', {
        'lat': at.latitude,
        'lng': at.longitude,
        'radius_m': radius,
        'fuel': f,
        'sort': 'distance',
        'limit': 40
      });
      final cols = List<String>.from(ans['columns'] as List);
      final rows = (ans['rows'] as List).map((r) {
        final row = r as List;
        return {for (var i = 0; i < cols.length; i++) cols[i]: row[i]};
      }).toList();
      if (mounted) {
        setState(() {
          stations = rows;
          status =
              '${rows.length} station(s) selling $f within ${(radius / 1000).toStringAsFixed(0)} km · '
              '${_transport(ans)}';
        });
      }
    } catch (e) {
      if (mounted) setState(() => status = 'fuel: $e');
    }
  }

  /// A stamp every editor orders the same way: RFC 3339 UTC with SIX fractional
  /// digits — Dart prints three when the microseconds are zero, and "…123Z" would sort
  /// AFTER "…123456Z".
  static String _stampNow() {
    final s = DateTime.now().toUtc().toIso8601String();
    final dot = s.indexOf('.');
    final frac = s.substring(dot + 1, s.length - 1);
    return '${s.substring(0, dot + 1)}${frac.padRight(6, '0')}Z';
  }

  /// The document as the local row holds it, and the pins from it.
  Future<void> _readRoute() async {
    final w = zb;
    if (w == null) return;
    final rows = await w
        .query('SELECT doc, last_writer FROM routes WHERE id = ?', [_routeId]);
    if (rows.isEmpty) return;
    final raw = rows.first['doc'];
    final doc = raw is String
        ? (jsonDecode(raw) as Map<String, dynamic>)
        : Map<String, dynamic>.from(raw as Map? ?? {});
    final pins = <MapEntry<String, LatLng>>[];
    for (final key in const ['start', 'end']) {
      final v = doc[key]?['v'];
      if (v is Map && v['lat'] is num && v['lng'] is num) {
        pins.add(MapEntry(
            key,
            LatLng(
                (v['lat'] as num).toDouble(), (v['lng'] as num).toDouble())));
      }
    }
    final writers = [
      for (final k in const ['start', 'end'])
        if (doc[k] != null) '$k by ${doc[k]['w']}'
    ].join(', ');
    if (mounted) {
      setState(() {
        routeDoc = doc;
        routePoints = pins;
        routeInfo = pins.length < 2
            ? (pins.isEmpty ? 'tap the start' : 'tap the end · $writers')
            : writers;
      });
    }
    if (pins.length == 2) await _askRoad(pins[0].value, pins[1].value);
  }

  /// This phone's registers merged into the document it last saw — the union, whole.
  Future<void> _writeRoute() async {
    final w = zb;
    if (w == null) return;
    final merged =
        await w.call('mergeRegisters', {'a': routeDoc, 'b': routeMine});
    await w.mutate('routes', 'UPDATE', {'id': _routeId}, {'doc': merged});
  }

  /// The row moved (mine or someone else's): redraw from it, then reconcile — write the
  /// union again while what is observed does not contain what this phone wrote (§10cr).
  Future<void> _onRouteChanged() async {
    await _readRoute();
    final w = zb;
    if (w == null || routeMine.isEmpty) return;
    final merged =
        await w.call('mergeRegisters', {'a': routeDoc, 'b': routeMine});
    if (jsonEncode(merged) != jsonEncode(routeDoc) && routeRounds < 10) {
      routeRounds += 1;
      await _writeRoute();
    }
  }

  /// A tap sets the next register — start, then end, then start again — as this
  /// phone's move, and writes. The pin appears when the row comes back.
  Future<void> _routeTap(LatLng at) async {
    if (zb == null) return;
    routeMine[routeNext] = {
      'v': {'lat': at.latitude, 'lng': at.longitude},
      't': _stampNow(),
      'w': _routeWriter,
    };
    routeNext = routeNext == 'start' ? 'end' : 'start';
    setState(() => routeInfo = 'moving…');
    try {
      await _writeRoute();
    } catch (e) {
      if (mounted) setState(() => routeInfo = 'route: $e');
    }
  }

  Future<void> _askRoad(LatLng from, LatLng to) async {
    final w = zb;
    if (w == null) return;
    final t0 = DateTime.now();
    try {
      final ans = await w.request(
          'query.$_queryTenant.route',
          {
            'points': [
              {'lat': from.latitude, 'lng': from.longitude},
              {'lat': to.latitude, 'lng': to.longitude}
            ],
            'costing': 'auto'
          },
          timeoutMs: 15000);
      final line = [
        for (final pt in ans['polyline'] as List)
          LatLng((pt[0] as num).toDouble(), (pt[1] as num).toDouble())
      ];
      final ms = DateTime.now().difference(t0).inMilliseconds;
      if (mounted) {
        setState(() {
          routeLine = line;
          routeInfo =
              '${ans['km']} km, ${ans['min']} min by road · $ms ms · $routeInfo';
        });
      }
    } catch (e) {
      if (mounted) setState(() => routeInfo = 'route: $e');
    }
  }

  Future<void> _addPoi(LatLng at) async {
    final w = zb;
    if (w == null) return;
    setState(() => addingMode = false);
    // §10hr: the key is a uuid this phone mints; `ocm_id` stays null, since this
    // charge point is not OpenChargeMap's. An INSERT's values are the WHOLE row, key
    // included (the bridge builds the statement from them); `geom` is PostGIS's own
    // bytes, as libzb's bytes marker.
    final id = _uuidV4();
    try {
      await w.mutate(_table, 'INSERT', {
        'id': id
      }, {
        'id': id,
        'title': 'Nouvelle borne',
        'lat': at.latitude,
        'lng': at.longitude,
        'max_power_kw': 22,
        'points': 2,
        'status_type_id': 50,
        'usage_type_id': 1,
        'geom': {r'$bin': ewkbPoint(at.longitude, at.latitude)},
      });
      await _refresh();
    } catch (e) {
      if (mounted) setState(() => status = 'insert: $e');
    }
  }

  Future<void> _editPoi(Map<String, dynamic> poi) async {
    final w = zb;
    if (w == null) return;
    var name = (poi['title'] as String?) ?? '';
    final action = await showModalBottomSheet<String>(
      context: context,
      builder: (ctx) => Padding(
        padding: EdgeInsets.fromLTRB(
            16, 16, 16, 16 + MediaQuery.of(ctx).viewInsets.bottom),
        child: Column(
          mainAxisSize: MainAxisSize.min,
          children: [
            Text(
                '${poi['max_power_kw'] ?? '?'} kW · ${poi['points'] ?? '?'} point(s)',
                style: Theme.of(ctx).textTheme.bodySmall),
            TextField(
              controller: TextEditingController(text: name),
              onChanged: (v) => name = v,
              decoration: const InputDecoration(labelText: 'Name'),
              autofocus: true,
            ),
            const SizedBox(height: 16),
            Row(
              mainAxisAlignment: MainAxisAlignment.end,
              children: [
                TextButton.icon(
                  onPressed: () => Navigator.pop(ctx, 'erase'),
                  icon: const Icon(Icons.delete_outline),
                  label: const Text('Erase'),
                ),
                const SizedBox(width: 8),
                FilledButton(
                    onPressed: () => Navigator.pop(ctx, 'save'),
                    child: const Text('Save')),
              ],
            ),
          ],
        ),
      ),
    );
    if (action == null) return;
    try {
      if (action == 'erase') {
        // Locally a delete; upstream the delete guard sets deleted_at (the tombstone).
        await w.mutate(_table, 'DELETE', {'id': poi['id']});
      } else {
        await w.mutate(_table, 'UPDATE', {'id': poi['id']}, {'title': name});
      }
      await _refresh();
    } catch (e) {
      if (mounted) setState(() => status = '$action: $e');
    }
  }

  @override
  void dispose() {
    WidgetsBinding.instance.removeObserver(this);
    askDebounce?.cancel();
    refreshDebounce?.cancel();
    reportsSub?.cancel();
    zb?.close();
    super.dispose();
  }

  @override
  Widget build(BuildContext context) {
    return Scaffold(
      appBar: AppBar(
        title: const Text('ZeMap'),
        actions: [
          PopupMenuButton<String>(
            tooltip: 'Charge points',
            icon: Icon(Icons.ev_station,
                color: chargers == null ? null : Colors.greenAccent),
            onSelected: (v) {
              setState(() => chargers = v.isEmpty ? null : double.parse(v));
              if (chargers == null) {
                setState(() => status =
                    'charge points off — the phone keeps what it holds');
              } else {
                _wantArea(mapController.camera);
              }
              _refresh();
            },
            itemBuilder: (_) => const [
              PopupMenuItem(value: '', child: Text('Off')),
              PopupMenuItem(value: '0', child: Text('All charge points')),
              PopupMenuItem(value: '22', child: Text('22 kW and up')),
              PopupMenuItem(value: '43', child: Text('Rapid — 43 kW and up')),
              PopupMenuItem(value: '150', child: Text('Ultra — 150 kW and up')),
            ],
          ),
          IconButton(
            tooltip: 'Route between two taps (Valhalla)',
            icon: Icon(Icons.directions,
                color: routeMode ? Colors.lightBlueAccent : null),
            onPressed: zb == null
                ? null
                : () => setState(() {
                      routeMode = !routeMode;
                      if (!routeMode) {
                        routeLine = const [];
                        routeInfo = '';
                      } else {
                        addingMode = false;
                        routeInfo = 'loading the shared route…';
                        _readRoute();
                      }
                    }),
          ),
          PopupMenuButton<String>(
            tooltip: 'Fuel prices nearby',
            icon: Icon(Icons.local_gas_station,
                color: fuel == null ? null : Colors.amber),
            initialValue: fuel ?? '',
            onSelected: (v) {
              setState(() => fuel = v.isEmpty ? null : v);
              if (fuel == null) {
                setState(() => stations = const []);
                return;
              }
              // §10hv: through the pan handler, so the radius is the CURRENT
              // viewport's rather than whatever a previous charger pan left behind.
              _wantArea(mapController.camera);
            },
            itemBuilder: (_) => [
              const PopupMenuItem(value: '', child: Text('Off')),
              for (final f in const [
                'SP95',
                'SP98',
                'E10',
                'E85',
                'Gazole',
                'GPLc'
              ])
                PopupMenuItem(value: f, child: Text(f)),
            ],
          ),
        ],
        bottom: PreferredSize(
          preferredSize: const Size.fromHeight(20),
          child: Padding(
            padding: const EdgeInsets.only(bottom: 4),
            child: Text(
                routeMode
                    ? 'route: $routeInfo'
                    : '$status · $held held · tiles ${tiles?.requested ?? 0} asked / ${tiles?.served ?? 0} served / ${tiles?.failed ?? 0} failed',
                style: const TextStyle(fontSize: 12)),
          ),
        ),
      ),
      body: FlutterMap(
        mapController: mapController,
        options: MapOptions(
          initialCenter: const LatLng(47.2184, -1.5536), // Nantes
          initialZoom: 15,
          onTap: (_, at) {
            if (addingMode) {
              _addPoi(at);
            } else if (routeMode) {
              _routeTap(at);
            }
          },
          // A pan is many events. The ask is debounced, and so is the redraw: a refresh
          // per event rebuilt the whole map subtree dozens of times a second and the
          // tile layer never got to load (measured: tiles stopped after the first view).
          onPositionChanged: (camera, _) {
            _wantArea(camera);
            refreshDebounce?.cancel();
            refreshDebounce =
                Timer(const Duration(milliseconds: 250), _refresh);
          },
        ),
        children: [
          if (vectorLayer != null)
            vectorLayer!
          else
            TileLayer(
                urlTemplate: 'https://tile.openstreetmap.org/{z}/{x}/{y}.png',
                userAgentPackageName: 'zemap'),
          if (routeLine.isNotEmpty)
            PolylineLayer(polylines: [
              Polyline(
                  points: routeLine,
                  color: Colors.blue.shade700,
                  strokeWidth: 5)
            ]),
          if (routeMode && routePoints.isNotEmpty)
            MarkerLayer(
              markers: [
                for (final e in routePoints)
                  Marker(
                    point: e.value,
                    width: 34,
                    height: 34,
                    alignment: Alignment.topCenter,
                    child: Icon(
                        e.key == 'start' ? Icons.trip_origin : Icons.flag,
                        color: Colors.blue.shade900,
                        size: 30),
                  ),
              ],
            ),
          // One layer at a time: the fuel switch on shows the price tags and hides the POIs,
          // off shows the POIs — the phone still holds both.
          if (fuel != null && stations.isNotEmpty)
            MarkerLayer(
              markers: [
                for (final st in stations)
                  Marker(
                    point: LatLng((st['lat'] as num).toDouble(),
                        (st['lng'] as num).toDouble()),
                    width: 64,
                    height: 26,
                    child: Tooltip(
                      message:
                          '${st['address'] ?? ''}, ${st['city'] ?? ''} · ${((st['m'] as num?) ?? 0).round()} m'
                          '${st['outage'] != null ? ' · ${st['outage']} outage' : ''}',
                      child: Container(
                        alignment: Alignment.center,
                        decoration: BoxDecoration(
                          color: st == _cheapest
                              ? Colors.green.shade700
                              : (st['outage'] != null
                                  ? Colors.grey
                                  : Colors.amber.shade800),
                          borderRadius: BorderRadius.circular(6),
                          border: Border.all(color: Colors.white, width: 1.5),
                        ),
                        child: Text(
                          '${_price(st)} €',
                          style: const TextStyle(
                              color: Colors.white,
                              fontSize: 12,
                              fontWeight: FontWeight.bold),
                        ),
                      ),
                    ),
                  ),
              ],
            ),
          if (fuel == null && chargers != null)
            MarkerLayer(
              markers: [
                for (final poi in pois)
                  Marker(
                    point: LatLng((poi['lat'] as num).toDouble(),
                        (poi['lng'] as num).toDouble()),
                    width: 28,
                    height: 28,
                    alignment: Alignment.topCenter,
                    child: Tooltip(
                      message: '${poi['title'] ?? ''} · '
                          '${poi['max_power_kw'] ?? '?'} kW · ${poi['points'] ?? '?'} pt'
                          '${poi['status_type_id'] == 50 ? '' : ' · out of service'}',
                      child: GestureDetector(
                        onTap: () => _editPoi(poi),
                        // Green for rapid (≥43 kW), grey for out of service, orange
                        // for one this phone added, blue otherwise.
                        child: Icon(
                          Icons.ev_station,
                          color: poi['ocm_id'] == null
                              ? Colors.deepOrange
                              : (poi['status_type_id'] != 50
                                  ? Colors.grey
                                  : ((poi['max_power_kw'] as num?) ?? 0) >= 43
                                      ? Colors.green.shade700
                                      : Colors.blue.shade700),
                          size: 26,
                        ),
                      ),
                    ),
                  ),
              ],
            ),
        ],
      ),
      floatingActionButton: FloatingActionButton(
        backgroundColor: addingMode ? Colors.red : Colors.teal,
        onPressed:
            zb == null ? null : () => setState(() => addingMode = !addingMode),
        child: Icon(addingMode ? Icons.close : Icons.add_location_alt,
            color: Colors.white),
      ),
    );
  }
}

/// The price of a station row, whatever DuckDB's decimal came through as.
String _price(Map<String, dynamic> st) {
  final v = st['price'];
  final d = v is num ? v.toDouble() : double.tryParse('$v') ?? 0;
  return d.toStringAsFixed(3);
}

/// PostGIS's bytes for a point with an SRID — extended WKB, little endian: byte order,
/// type with the SRID flag, the SRID, x (longitude), y (latitude) — as base64 for libzb's
/// `$bin` marker. 25 bytes, the same the bridge sends back.
String ewkbPoint(double lng, double lat, {int srid = 4326}) {
  final b = ByteData(25);
  b.setUint8(0, 1);
  b.setUint32(1, 0x20000001, Endian.little);
  b.setUint32(5, srid, Endian.little);
  b.setFloat64(9, lng, Endian.little);
  b.setFloat64(17, lat, Endian.little);
  return base64Encode(b.buffer.asUint8List());
}

/// The tile provider, counted: what the layer asks for, what came back, what failed —
/// on the status line and in the console. Diagnostics for "tiles stop after the first
/// view"; harmless to keep.
class _CountingTiles extends VectorTileProvider {
  _CountingTiles(this.inner, this.onChange);
  final VectorTileProvider inner;
  final void Function() onChange;
  int requested = 0, served = 0, failed = 0;

  @override
  int get maximumZoom => inner.maximumZoom;
  @override
  int get minimumZoom => inner.minimumZoom;
  @override
  TileProviderType get type => inner.type;

  @override
  Future<Uint8List> provide(TileIdentity tile) async {
    requested++;
    onChange();
    try {
      final bytes = await inner.provide(tile);
      served++;
      return bytes;
    } catch (e) {
      failed++;
      debugPrint('tile z${tile.z}/${tile.x}/${tile.y} failed: $e');
      rethrow;
    } finally {
      onChange();
    }
  }
}
