/// 10-airports on a phone, through libzb: the airports around the map's centre, and the
/// flight the tenant edits together — the same flight the browsers draw.
///
///   * the AIRPORTS are a question — `query._default.airports_near` to the DuckDB service,
///     asked after every pan. The phone stores none of them.
///   * the FLIGHT is a row — `flights`, replicated into the phone's SQLite and written with
///     `mutate`. Its `doc` holds two registers {v, t, w}, the departure and the arrival; `t`
///     comes from libzb's `stamp()`, on the bridge's clock (COOPERATIVE_EDITING.md).
///
///   --dart-define=ZB_BRIDGE_URL=http://192.168.1.22:27434   the bridge (/enroll, /renew)
///   --dart-define=ZB_NATS_URL=nats://192.168.1.22:4232
///   --dart-define=ZB_INVITE=<code>                          used once, at enrollment
library;

import 'dart:async';
import 'dart:convert';
import 'dart:io';
import 'dart:math' as math;

import 'package:flutter/material.dart';
import 'package:flutter_map/flutter_map.dart';
import 'package:latlong2/latlong.dart';
import 'package:path_provider/path_provider.dart';

import 'package:zebridge/zebridge.dart';

const bridgeUrl = String.fromEnvironment('ZB_BRIDGE_URL', defaultValue: 'http://192.168.1.22:27434');
const natsUrl = String.fromEnvironment('ZB_NATS_URL', defaultValue: 'nats://192.168.1.22:4232');
const invite = String.fromEnvironment('ZB_INVITE');

const sanMateo = LatLng(37.563, -122.326);
const radiusKm = 100.0; // a circle 200 km across, around the centre of the map
const limit = 500;

const ends = ['origin', 'destination'];
const label = {'origin': 'departure', 'destination': 'arrival'};
const green = Color(0xFF1A7F37), purple = Color(0xFF7A1FD1), red = Color(0xFFD1361F), blue = Color(0xFF1F4FD1);

void main() => runApp(const MaterialApp(debugShowCheckedModeBanner: false, home: AirportsScreen()));

class AirportsScreen extends StatefulWidget {
  const AirportsScreen({super.key});
  @override
  State<AirportsScreen> createState() => _AirportsScreenState();
}

class _AirportsScreenState extends State<AirportsScreen> {
  final map = MapController();
  ZeBridgeWorker? zb;
  StreamSubscription<PollReport>? reports;
  String principal = '';
  String flightId = '';

  // the airports: the last answer
  List<Map<String, dynamic>> airports = [];
  String count = 'connecting…', detail = '';
  int asked = 0;
  Timer? askLater;
  LatLng centre = sanMateo;

  // the flight: what the row holds, and what this phone wrote and has not seen in it
  Map<String, dynamic> doc = {};
  bool rowExists = false;
  final mine = <String, Map<String, dynamic>>{};
  int rounds = 0;
  String notice = '';
  Timer? noticeTimer;

  @override
  void initState() {
    super.initState();
    _open();
  }

  @override
  void dispose() {
    reports?.cancel();
    zb?.close();
    super.dispose();
  }

  Future<void> _open() async {
    final dir = await getApplicationSupportDirectory();
    final dbPath = '${dir.path}/airports.sqlite3';
    final identity = File('$dbPath.identity');
    final enrolled = identity.existsSync();
    if (!enrolled && invite.isEmpty) {
      setState(() => count = 'no identity and no ZB_INVITE: rebuild with an invite');
      return;
    }
    try {
      final w = await ZeBridgeWorker.spawn({
        'bridgeUrl': bridgeUrl,
        'natsUrl': natsUrl,
        'dbPath': dbPath,
        'tables': ['flights'],
        if (!enrolled) 'invite': invite,
      });
      principal = (jsonDecode(identity.readAsStringSync()) as Map)['principal'] as String? ?? '';
      flightId = 'flight-${w.tenant}';
      zb = w;
      // The row moved, by me or by someone else: redraw from it, then reconcile.
      reports = w.reports.listen((r) {
        if (r.error != null) say('poll: ${r.error}');
        if (r.changedTables.contains('flights') || r.seeded.contains('flights')) _onFlightChanged();
      });
      await _readFlight();
      await _ask();
    } catch (e) {
      setState(() => count = 'open failed: $e');
    }
  }

  // ── the airports: a question ─────────────────────────────────────────────────

  /// flutter_map may report a longitude past ±180 after a long pan: the service is asked
  /// with the wrapped value, and what is drawn moves onto the copy of the world in view.
  double onView(double lng) => lng + 360 * ((centre.longitude - lng) / 360).roundToDouble();
  double wrap(double lng) => (lng + 180) % 360 - 180;

  Future<void> _ask() async {
    final w = zb;
    if (w == null) return;
    final mineAsk = ++asked;
    final t0 = DateTime.now();
    try {
      final a = await w.request('query._default.airports_near', {
        'lat': centre.latitude, 'lng': wrap(centre.longitude), 'radius_km': radiusKm, 'limit': limit,
      });
      if (mineAsk != asked) return; // a newer pan asked meanwhile: its answer wins
      if (a['error'] != null) {
        setState(() => count = 'the service refused: ${a['error']}');
        return;
      }
      final cols = List<String>.from(a['columns'] as List);
      final n = a['count'] as num;
      setState(() {
        airports = [
          for (final r in a['rows'] as List) {for (var i = 0; i < cols.length; i++) cols[i]: (r as List)[i]}
        ];
        count = '$n${a['complete'] == true ? '' : '+'} airport${n == 1 ? '' : 's'} within ${radiusKm.round()} km of the centre';
        detail = '${a['ms']} ms in the service, ${DateTime.now().difference(t0).inMilliseconds} ms round trip';
      });
    } catch (e) {
      if (mineAsk == asked) setState(() => count = 'no answer: $e');
    }
  }

  Map<String, dynamic> _airport(Map<String, dynamic> r) => {
        'code': r['code'], 'name': r['name'],
        'lat': (r['latitude'] as num).toDouble(), 'lng': (r['longitude'] as num).toDouble(),
      };

  /// The sheet on an airport: make it the departure or the arrival.
  Future<void> _choose(Map<String, dynamic> r) async {
    final ap = _airport(r);
    final end = await showModalBottomSheet<String>(
      context: context,
      builder: (ctx) => SafeArea(
        child: Padding(
          padding: const EdgeInsets.all(16),
          child: Column(mainAxisSize: MainAxisSize.min, crossAxisAlignment: CrossAxisAlignment.start, children: [
            Text('${ap['code']} — ${ap['name']}', style: const TextStyle(fontWeight: FontWeight.bold, fontSize: 16)),
            Text('${r['distance_km']} km from the centre'),
            const SizedBox(height: 12),
            Row(children: [
              FilledButton(style: FilledButton.styleFrom(backgroundColor: green), onPressed: () => Navigator.pop(ctx, 'origin'), child: const Text('Departure')),
              const SizedBox(width: 12),
              FilledButton(style: FilledButton.styleFrom(backgroundColor: purple), onPressed: () => Navigator.pop(ctx, 'destination'), child: const Text('Arrival')),
            ]),
          ]),
        ),
      ),
    );
    if (end != null) await _setEnd(end, ap);
  }

  // ── the flight: a row, two registers ─────────────────────────────────────────

  Future<void> _readFlight() async {
    final w = zb!;
    final rows = await w.query('SELECT doc FROM flights WHERE id = ?', [flightId]);
    final raw = rows.isEmpty ? null : rows.first['doc'];
    final next = raw == null ? <String, dynamic>{} : Map<String, dynamic>.from(raw is String ? jsonDecode(raw) as Map : raw as Map);
    rowExists = rows.isNotEmpty;
    for (final end in ends) {
      final was = doc[end] as Map?, now = next[end] as Map?;
      if (now == null || now['t'] == was?['t']) continue;
      final m = mine[end];
      final code = (now['v'] as Map)['code'];
      if (now['w'] != principal) {
        if (m != null && (now['t'] as String).compareTo(m['t'] as String) > 0) {
          say("${now['w']}'s $code came after your ${(m['v'] as Map)['code']}: the ${label[end]} is $code");
        } else {
          say("${now['w']} set the ${label[end]} to $code");
        }
      }
      if (m != null && (now['t'] as String).compareTo(m['t'] as String) >= 0) mine.remove(end);
    }
    setState(() => doc = next);
  }

  /// libzb's own merge (the same function zb-client-ts runs): per end, the later stamp.
  Map<String, dynamic> _merged() => ZeBridge.call('mergeRegisters', {'a': doc, 'b': mine});

  Future<void> _writeFlight() async {
    final w = zb!;
    final merged = _merged();
    if (rowExists) {
      await w.mutate('flights', 'UPDATE', {'id': flightId}, {'doc': merged});
    } else {
      await w.mutate('flights', 'INSERT', {'id': flightId}, {'tenant_id': w.tenant, 'doc': merged});
    }
  }

  Future<void> _setEnd(String end, Map<String, dynamic> ap) async {
    final w = zb!;
    mine[end] = {'v': ap, 't': await w.stamp(), 'w': principal};
    rounds = 0;
    setState(() {});
    try {
      await _writeFlight();
    } catch (e) {
      say('write: $e');
    }
  }

  /// Write the merge again while the row does not hold what this phone wrote.
  Future<void> _onFlightChanged() async {
    await _readFlight();
    if (mine.isEmpty || rounds >= 10) return;
    if (jsonEncode(_merged()) != jsonEncode(doc)) {
      rounds += 1;
      await _writeFlight();
    }
  }

  void say(String text) {
    noticeTimer?.cancel();
    setState(() => notice = text);
    noticeTimer = Timer(const Duration(seconds: 8), () => setState(() => notice = ''));
  }

  // ── drawing ──────────────────────────────────────────────────────────────────

  @override
  Widget build(BuildContext context) {
    // Each end: what this phone wrote and has not seen yet (hollow), else the row's.
    final shown = <String, ({Map ap, bool pending, Map reg})>{};
    for (final end in ends) {
      final reg = mine[end] ?? doc[end] as Map?;
      if (reg != null) shown[end] = (ap: reg['v'] as Map, pending: mine.containsKey(end), reg: reg);
    }
    final o = shown['origin'], d = shown['destination'];
    // The whole flight moves by one shift (the departure's), so the line stays continuous.
    final first = o ?? d;
    final shift = first == null ? 0.0 : onView(_lng(first.ap)) - _lng(first.ap);
    var line = <LatLng>[];
    if (o != null && d != null) line = greatCircle(o.ap, d.ap).map((p) => LatLng(p.latitude, p.longitude + shift)).toList();

    String part(String end) {
      final e = shown[end];
      if (e == null) return '${label[end]}: —';
      final t = e.reg['t'] as String;
      return '${label[end]}: ${e.ap['code']}${e.pending ? ' (pending)' : ' by ${e.reg['w']} at ${t.substring(11, 19)}'}';
    }

    final leg = o != null && d != null
        ? ' · ${distanceKm(o.ap, d.ap).round()} km, heading ${bearing(o.ap, d.ap).round()}°'
        : '';

    return Scaffold(
      body: Column(children: [
        Expanded(
          child: FlutterMap(
            mapController: map,
            options: MapOptions(
              initialCenter: sanMateo,
              initialZoom: 7,
              // A pan is many events: ask once it settles.
              onPositionChanged: (camera, _) {
                centre = camera.center;
                askLater?.cancel();
                askLater = Timer(const Duration(milliseconds: 250), () {
                  setState(() {});
                  _ask();
                });
              },
            ),
            children: [
              TileLayer(urlTemplate: 'https://tile.openstreetmap.org/{z}/{x}/{y}.png', userAgentPackageName: 'dev.zebridge.airports'),
              CircleLayer(circles: [
                CircleMarker(point: centre, radius: radiusKm * 1000, useRadiusInMeter: true, color: Colors.transparent, borderColor: blue, borderStrokeWidth: 3),
              ]),
              MarkerLayer(markers: [
                for (final r in airports)
                  Marker(
                    point: LatLng((r['latitude'] as num).toDouble(), onView((r['longitude'] as num).toDouble())),
                    width: 26,
                    height: 26,
                    child: GestureDetector(
                      behavior: HitTestBehavior.opaque,
                      onTap: () => _choose(r),
                      child: Container(
                        margin: const EdgeInsets.all(4),
                        decoration: BoxDecoration(shape: BoxShape.circle, color: red.withValues(alpha: 0.9), border: Border.all(color: Colors.white, width: 2)),
                      ),
                    ),
                  ),
              ]),
              if (line.isNotEmpty) PolylineLayer(polylines: [Polyline(points: line, color: purple, strokeWidth: 3)]),
              MarkerLayer(markers: [
                for (final MapEntry(key: end, value: e) in shown.entries)
                  Marker(
                    // the arrival is drawn at the line's end, which may be past ±180
                    point: end == 'destination' && line.isNotEmpty ? line.last : LatLng(_lat(e.ap), _lng(e.ap) + shift),
                    width: 30,
                    height: 30,
                    child: IgnorePointer(
                      child: Container(
                        decoration: BoxDecoration(
                          shape: BoxShape.circle,
                          color: e.pending ? Colors.transparent : (end == 'origin' ? green : purple),
                          border: Border.all(color: end == 'origin' ? green : purple, width: 4),
                        ),
                      ),
                    ),
                  ),
              ]),
            ],
          ),
        ),
        SafeArea(
          top: false,
          child: Padding(
            padding: const EdgeInsets.fromLTRB(12, 8, 12, 4),
            child: Column(crossAxisAlignment: CrossAxisAlignment.start, children: [
              Text(count, style: const TextStyle(fontWeight: FontWeight.w600)),
              if (detail.isNotEmpty) Text(detail, style: const TextStyle(fontSize: 12, color: Colors.black54)),
              const SizedBox(height: 4),
              Text('Flight ${zb?.tenant ?? '…'}: ${part('origin')} → ${part('destination')}$leg', style: const TextStyle(fontSize: 13)),
              if (notice.isNotEmpty) Text(notice, style: const TextStyle(fontSize: 13, color: Color(0xFFB35900))),
            ]),
          ),
        ),
      ]),
    );
  }
}

// ── great-circle geometry ──────────────────────────────────────────────────────
double _lat(Map ap) => (ap['lat'] as num).toDouble();
double _lng(Map ap) => (ap['lng'] as num).toDouble();
double rad(double d) => d * math.pi / 180;
double deg(double r) => r * 180 / math.pi;

double centralAngle(Map a, Map b) {
  final dp = rad(_lat(b) - _lat(a)), dl = rad(_lng(b) - _lng(a));
  final h = math.pow(math.sin(dp / 2), 2) + math.cos(rad(_lat(a))) * math.cos(rad(_lat(b))) * math.pow(math.sin(dl / 2), 2);
  return 2 * math.asin(math.sqrt(h));
}

double distanceKm(Map a, Map b) => 6371 * centralAngle(a, b);

double bearing(Map a, Map b) {
  final p1 = rad(_lat(a)), p2 = rad(_lat(b)), dl = rad(_lng(b) - _lng(a));
  return (deg(math.atan2(math.sin(dl) * math.cos(p2), math.cos(p1) * math.sin(p2) - math.sin(p1) * math.cos(p2) * math.cos(dl))) + 360) % 360;
}

/// The shortest path over the sphere, as points. Longitudes are kept continuous (no jump
/// from +180 to -180), so a flight across the Pacific draws as one line.
List<LatLng> greatCircle(Map a, Map b, [int n = 128]) {
  final d = centralAngle(a, b);
  if (d == 0) return [LatLng(_lat(a), _lng(a)), LatLng(_lat(b), _lng(b))];
  final p1 = rad(_lat(a)), l1 = rad(_lng(a)), p2 = rad(_lat(b)), l2 = rad(_lng(b));
  final pts = <LatLng>[];
  var prev = _lng(a);
  for (var i = 0; i <= n; i++) {
    final f = i / n;
    final A = math.sin((1 - f) * d) / math.sin(d), B = math.sin(f * d) / math.sin(d);
    final x = A * math.cos(p1) * math.cos(l1) + B * math.cos(p2) * math.cos(l2);
    final y = A * math.cos(p1) * math.sin(l1) + B * math.cos(p2) * math.sin(l2);
    final z = A * math.sin(p1) + B * math.sin(p2);
    var lng = deg(math.atan2(y, x));
    while (lng - prev > 180) { lng -= 360; }
    while (lng - prev < -180) { lng += 360; }
    prev = lng;
    pts.add(LatLng(deg(math.atan2(z, math.sqrt(x * x + y * y))), lng));
  }
  return pts;
}
