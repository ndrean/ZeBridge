/// ZeMap — one screen: a vector map of Nantes, the `pois` table as markers.
///
/// The table lives in PostgreSQL and reaches the phone through the bridge; libzb
/// creates the local replica from the descriptor, so the app never runs a CREATE
/// TABLE. Tap "+", then the map, to add a point; tap a marker to edit its note or
/// erase it. An erase is a tombstone upstream (`deleted_at` is set, the master keeps
/// the row); every replica applies it as a delete, so the local table never holds a
/// tombstoned row and the marker query needs no filter.
///
/// Threading: the libzb handle is NOT thread-safe (one thread drives one client), so
/// every call on it — sync, the blocking poll loop, query, mutate, flush, close — runs
/// on ONE long-lived worker isolate (`zebridge_worker.dart`, the 05-mobile design).
/// The UI isolate only sends messages. `Isolate.run` per poll would put the poll on a
/// fresh thread while the UI thread still called query/mutate on the same handle: two
/// threads on one handle, exactly what the contract forbids.
import 'dart:async';
import 'dart:io';
import 'dart:math';

import 'package:flutter/material.dart';
import 'package:flutter_map/flutter_map.dart';
import 'package:latlong2/latlong.dart';
import 'package:pmtiles/pmtiles.dart';
import 'package:vector_map_tiles/vector_map_tiles.dart';
import 'package:vector_map_tiles_pmtiles/vector_map_tiles_pmtiles.dart';

import 'zebridge.dart' show PollReport;
import 'zebridge_worker.dart';

// Dev copy: the repository paths, like 05-mobile. A shipped app bundles the creds it
// enrolled and the pmtiles it downloaded.
const _repo = '/Users/nevendrean/code/zig/ZeBridge';
const _credsPath = '$_repo/scripts/native/creds/alice.creds';
const _pmtilesPath = '$_repo/examples/08-map/flutter/test_region.pmtiles';
final _dbPath = '${Directory.systemTemp.path}/zb-flutter-map-alice.sqlite3';

void main() => runApp(const ZeMapApp());

class ZeMapApp extends StatelessWidget {
  const ZeMapApp({super.key});

  @override
  Widget build(BuildContext context) => MaterialApp(
        title: 'ZeMap',
        theme: ThemeData(colorScheme: ColorScheme.fromSeed(seedColor: Colors.teal), useMaterial3: true),
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
      final archive = await PmTilesArchive.fromFile(File(_pmtilesPath));
      if (!mounted) return;
      setState(() {
        vectorLayer = VectorTileLayer(
          theme: ProtomapsThemes.lightV4(),
          tileProviders: TileProviders({'protomaps': PmTilesVectorTileProvider.fromArchive(archive)}),
        );
      });
    } catch (e) {
      debugPrint('pmtiles: $e (falling back to OSM raster tiles)');
    }
  }

  Future<void> _initZeBridge() async {
    try {
      final worker = await ZeBridgeWorker.spawn({
        'url': 'nats://127.0.0.1:4222',
        'credsPath': _credsPath,
        'dbPath': _dbPath,
        'principal': 'alice',
        'tables': ['pois'],
        'clientId': 'flutter-map',
      });
      if (!mounted) {
        await worker.close();
        return;
      }
      zb = worker;
      setState(() => status = 'tenant ${worker.tenant}');
      await _refresh();
      // One report per poll that changed something; re-read when pois moved.
      reportsSub = worker.reports.listen((r) {
        if (r.error != null) {
          setState(() => status = 'offline: ${r.error}');
          return;
        }
        if (r.changedTables.contains('pois') || r.seeded.contains('pois')) _refresh();
      });
    } catch (e) {
      if (mounted) setState(() => status = 'not connected: $e');
    }
  }

  /// The replica is what the screen shows; libzb already dropped what was erased.
  Future<void> _refresh() async {
    final w = zb;
    if (w == null) return;
    try {
      final rows = await w.query('SELECT uid, lat, lng, note FROM pois ORDER BY inserted_at');
      if (!mounted) return;
      setState(() {
        pois = rows;
        status = 'tenant ${w.tenant} · ${rows.length} POI(s)';
      });
    } catch (e) {
      if (mounted) setState(() => status = 'query: $e');
    }
  }

  Future<void> _addPoi(LatLng at) async {
    final w = zb;
    if (w == null) return;
    setState(() => addingMode = false);
    final uid = newUuid();
    final now = DateTime.now().toUtc().toIso8601String();
    try {
      // Optimistic: the row lands in the replica at once and is sent; the bridge
      // stamps the version (LEAST of ours and now) and the echo settles it.
      await w.mutate('pois', 'INSERT', {'uid': uid}, {
        'uid': uid,
        'lat': at.latitude,
        'lng': at.longitude,
        'note': 'New POI',
        'tenant_id': w.tenant,
        'inserted_at': now,
        'updated_at': now,
      });
      await _refresh();
    } catch (e) {
      if (mounted) setState(() => status = 'insert: $e');
    }
  }

  Future<void> _editPoi(Map<String, dynamic> poi) async {
    final w = zb;
    if (w == null) return;
    var note = (poi['note'] as String?) ?? '';
    final action = await showModalBottomSheet<String>(
      context: context,
      builder: (ctx) => Padding(
        padding: EdgeInsets.fromLTRB(16, 16, 16, 16 + MediaQuery.of(ctx).viewInsets.bottom),
        child: Column(
          mainAxisSize: MainAxisSize.min,
          children: [
            TextField(
              controller: TextEditingController(text: note),
              onChanged: (v) => note = v,
              decoration: const InputDecoration(labelText: 'Note'),
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
                FilledButton(onPressed: () => Navigator.pop(ctx, 'save'), child: const Text('Save')),
              ],
            ),
          ],
        ),
      ),
    );
    if (action == null) return;
    try {
      if (action == 'erase') {
        // Locally a delete; upstream the bridge sets deleted_at (the tombstone).
        await w.mutate('pois', 'DELETE', {'uid': poi['uid']});
      } else {
        await w.mutate('pois', 'UPDATE', {'uid': poi['uid']}, {'note': note});
      }
      await _refresh();
    } catch (e) {
      if (mounted) setState(() => status = '$action: $e');
    }
  }

  @override
  void dispose() {
    WidgetsBinding.instance.removeObserver(this);
    reportsSub?.cancel();
    zb?.close();
    super.dispose();
  }

  @override
  Widget build(BuildContext context) {
    return Scaffold(
      appBar: AppBar(
        title: const Text('ZeMap'),
        bottom: PreferredSize(
          preferredSize: const Size.fromHeight(20),
          child: Padding(padding: const EdgeInsets.only(bottom: 4), child: Text(status, style: const TextStyle(fontSize: 12))),
        ),
      ),
      body: FlutterMap(
        options: MapOptions(
          initialCenter: const LatLng(47.22, -1.585), // Nantes
          initialZoom: 13,
          onTap: (_, at) {
            if (addingMode) _addPoi(at);
          },
        ),
        children: [
          if (vectorLayer != null)
            vectorLayer!
          else
            TileLayer(urlTemplate: 'https://tile.openstreetmap.org/{z}/{x}/{y}.png', userAgentPackageName: 'zemap'),
          MarkerLayer(
            markers: [
              for (final poi in pois)
                Marker(
                  point: LatLng((poi['lat'] as num).toDouble(), (poi['lng'] as num).toDouble()),
                  width: 40,
                  height: 40,
                  alignment: Alignment.topCenter,
                  child: Tooltip(
                    message: (poi['note'] as String?) ?? '',
                    child: GestureDetector(
                      onTap: () => _editPoi(poi),
                      child: const Icon(Icons.location_pin, color: Colors.red, size: 40),
                    ),
                  ),
                ),
            ],
          ),
        ],
      ),
      floatingActionButton: FloatingActionButton(
        backgroundColor: addingMode ? Colors.red : Colors.teal,
        onPressed: zb == null ? null : () => setState(() => addingMode = !addingMode),
        child: Icon(addingMode ? Icons.close : Icons.add_location_alt, color: Colors.white),
      ),
    );
  }
}

/// A random UUID v4 (the table's key is `uuid`): 122 random bits, version and variant
/// nibbles set, no package needed.
String newUuid() {
  final rnd = Random.secure();
  final b = List<int>.generate(16, (_) => rnd.nextInt(256));
  b[6] = (b[6] & 0x0f) | 0x40;
  b[8] = (b[8] & 0x3f) | 0x80;
  final h = b.map((x) => x.toRadixString(16).padLeft(2, '0')).join();
  return '${h.substring(0, 8)}-${h.substring(8, 12)}-${h.substring(12, 16)}-${h.substring(16, 20)}-${h.substring(20)}';
}
