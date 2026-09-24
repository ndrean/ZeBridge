/// One table, one clock — on a phone, through libzb (the C ABI, Zig inside).
/// The SEED of a big table from the generation chain: `zb_client_sync` streams the
/// object, sorts each window and applies it in C (`seedStreaming`), on the worker
/// isolate, while this screen keeps the time. Nothing else — no mutation. The three
/// facts at the end (rows, distinct keys, a sum) are the ones every other host is
/// checked with against PostgreSQL, so a run here is a measurement — the native one,
/// against ../react-native on the same phone (NOTES §10iy).
///
/// No progress bar yet: libzb reports nothing while `zb_client_sync` runs (it is one
/// blocking call). A `seeding` field in `zb_client_poll`'s report is the next piece.
library;

import 'dart:async';
import 'dart:io';

import 'package:flutter/material.dart';
import 'package:flutter/services.dart' show rootBundle;
import 'package:path_provider/path_provider.dart';

import 'src/data/zebridge_worker.dart';

/// The simulator shares the Mac's network; a real phone needs the Mac's LAN address:
///   flutter run -d `<device>` --dart-define=ZB_NATS_URL=nats://192.168.1.11:4222
const natsUrl = String.fromEnvironment('ZB_NATS_URL', defaultValue: 'nats://127.0.0.1:4222');
const principal = 'bob'; // on globex, the tenant that holds the fixture
const table = 'test_types';

void main() => runApp(const MaterialApp(
      debugShowCheckedModeBanner: false,
      home: SeedScreen(),
    ));

class SeedScreen extends StatefulWidget {
  const SeedScreen({super.key});
  @override
  State<SeedScreen> createState() => _SeedScreenState();
}

class _SeedScreenState extends State<SeedScreen> {
  ZeBridgeWorker? _worker;
  final _clock = Stopwatch();
  Timer? _tick;
  String _phase = 'starting';
  String? _error;
  Map<String, dynamic>? _facts;
  String _dbPath = '';
  final _log = <String>[];

  @override
  void initState() {
    super.initState();
    _start();
  }

  @override
  void dispose() {
    _tick?.cancel();
    _worker?.close();
    super.dispose();
  }

  void _say(String line) => setState(() => _log.add(line));

  Future<void> _start() async {
    setState(() { _phase = 'starting'; _error = null; _facts = null; _log.clear(); });
    try {
      // The creds travel as a bundled asset (dev only) and libzb wants a file path;
      // the replica lives in the app's own support directory, kept across launches.
      final dir = await getApplicationSupportDirectory();
      final creds = File('${dir.path}/$principal.creds');
      await creds.writeAsString(await rootBundle.loadString('assets/creds/$principal.creds'));
      _dbPath = '${dir.path}/zebridge_$principal.sqlite3';
      final fresh = !File(_dbPath).existsSync();
      _say(fresh ? 'fresh replica — the seed is the whole table' : 'replica present — no seed unless the chain moved');

      _clock..reset()..start();
      _tick = Timer.periodic(const Duration(milliseconds: 100), (_) => setState(() {}));
      setState(() => _phase = 'connect + seed');
      _say('connecting to $natsUrl as $principal, following [$table]');

      // `spawn` returns once the worker's first `zb_client_sync` is done — schema,
      // seed, positions — so the clock stops exactly at "usable".
      final w = await ZeBridgeWorker.spawn({
        'natsUrl': natsUrl,
        'credsPath': creds.path,
        'dbPath': _dbPath,
        'principal': principal,
        'tables': [table],
        // §10ix / libzb `seed_streaming`: the object is inflated and applied as it
        // arrives, a window at a time — never whole in memory.
        'seedStreaming': true,
      });
      _clock.stop();
      _tick?.cancel();
      _worker = w;
      // §10iz: "usable" is a claim about the table, not about the call returning.
      final failed = w.unseeded.where((u) => u['table'] == table).toList();
      if (failed.isNotEmpty) {
        setState(() { _phase = 'failed'; _error = '$table: seeding failed: ${failed.first['reason']} — libzb retries at each poll'; });
        _say('not seeded after ${_secs(_clock.elapsedMilliseconds)}: ${failed.first['reason']}');
        return;
      }
      setState(() => _phase = 'usable');
      _say('usable after ${_secs(_clock.elapsedMilliseconds)} (tenant ${w.tenant})');

      final rows = await w.query('SELECT count(*) AS count, count(DISTINCT uid) AS "distinct", sum(age) AS sum FROM $table');
      final size = File(_dbPath).existsSync() ? File(_dbPath).lengthSync() : 0;
      setState(() => _facts = {...rows.first, 'bytes': size});
    } catch (e) {
      _clock.stop();
      _tick?.cancel();
      setState(() { _phase = 'failed'; _error = '$e'; });
    }
  }

  /// Close, delete the replica (and SQLite's side files), start over.
  Future<void> _wipe() async {
    await _worker?.close();
    _worker = null;
    for (final suffix in ['', '-wal', '-shm', '-journal']) {
      final f = File('$_dbPath$suffix');
      if (f.existsSync()) f.deleteSync();
    }
    await _start();
  }

  static String _secs(int ms) => '${(ms / 1000).toStringAsFixed(1)} s';
  static String _n(Object? v) {
    final n = int.tryParse('$v') ?? 0;
    return n.toString().replaceAllMapped(RegExp(r'(\d)(?=(\d{3})+$)'), (m) => '${m[1]},');
  }

  @override
  Widget build(BuildContext context) {
    const mono = TextStyle(fontFamily: 'Menlo', fontSize: 13, color: Color(0xFFCCCCCC));
    final dim = mono.copyWith(color: const Color(0xFF888888));
    return Scaffold(
      backgroundColor: const Color(0xFF121212),
      body: SafeArea(
        child: Padding(
          padding: const EdgeInsets.all(14),
          child: Column(crossAxisAlignment: CrossAxisAlignment.start, children: [
            const Text('ZeBridge — one large table, libzb',
                style: TextStyle(color: Color(0xFFE0E0E0), fontSize: 18, fontWeight: FontWeight.w700)),
            const SizedBox(height: 4),
            const Text('test_types as bob, seeded from the generation chain by the C client: streamed, '
                'sorted per window, applied in Zig. One database, kept across launches.',
                style: TextStyle(color: Color(0xFF999999), fontSize: 12)),
            const SizedBox(height: 14),
            Container(
              padding: const EdgeInsets.all(12),
              decoration: BoxDecoration(
                  color: const Color(0xFF1C1C1C),
                  border: Border.all(color: const Color(0xFF333333)),
                  borderRadius: BorderRadius.circular(6)),
              child: Row(children: [
                Expanded(
                    child: Text(
                        _phase == 'connect + seed'
                            ? 'connecting + seeding…  (libzb reports nothing until it is done)'
                            : _phase,
                        style: dim)),
                Text(_secs(_clock.elapsedMilliseconds),
                    style: const TextStyle(fontFamily: 'Menlo', fontSize: 24, color: Color(0xFFE0E0E0))),
              ]),
            ),
            const SizedBox(height: 12),
            if (_facts != null) ...[
              _fact('rows', _n(_facts!['count']), mono, dim),
              _fact('distinct uid', _n(_facts!['distinct']), mono, dim),
              _fact('sum(age)', _n(_facts!['sum']), mono, dim),
              _fact('connect → usable', _secs(_clock.elapsedMilliseconds), mono, dim),
              _fact('replica', '${((_facts!['bytes'] as int) / 1e9).toStringAsFixed(2)} GB', mono, dim),
              const SizedBox(height: 10),
            ],
            if (_error != null)
              Text(_error!, style: mono.copyWith(color: const Color(0xFFEF9A9A))),
            const SizedBox(height: 6),
            OutlinedButton(
              onPressed: _phase == 'connect + seed' ? null : _wipe,
              child: const Text('wipe & seed again'),
            ),
            const SizedBox(height: 10),
            Expanded(
              child: Container(
                width: double.infinity,
                padding: const EdgeInsets.all(8),
                decoration: BoxDecoration(color: const Color(0xFF0D0D0D), border: Border.all(color: const Color(0xFF2A2A2A))),
                child: SingleChildScrollView(
                  child: Text(_log.join('\n'), style: mono.copyWith(fontSize: 11, color: const Color(0xFF9E9E9E))),
                ),
              ),
            ),
          ]),
        ),
      ),
    );
  }

  Widget _fact(String k, String v, TextStyle mono, TextStyle dim) => Padding(
        padding: const EdgeInsets.symmetric(vertical: 2),
        child: Row(children: [SizedBox(width: 150, child: Text(k, style: dim)), Text(v, style: mono)]),
      );
}
