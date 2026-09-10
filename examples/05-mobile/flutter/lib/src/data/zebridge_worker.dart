/// The worker: ONE isolate owns the libzb handle and the poll loop; the UI talks to it
/// over ports. This is the honest Flutter shape for a host-driven library
/// (README, "The C ABI library"): `zb_client_poll(handle, wait_ms)` BLOCKS the calling
/// thread for up to `wait_ms` when nothing arrives, and Dart's FFI holds the isolate
/// for the whole call — on the UI isolate that was a frozen frame every idle second.
///
/// Everything that touches the handle runs here: poll, flush, query, mutate, close.
/// libzb's client is single-threaded by contract (the host owns the thread), so the
/// handle is never shared between isolates — the UI isolate only ever sends messages.
///
/// Reports flow UI-ward as a stream, one per poll that changed something; commands
/// (query, mutate, flush, close, pause, resume) flow worker-ward and are answered by
/// id. Commands are served BETWEEN polls: the loop waits at most `pollWaitMs` on the
/// broker, then yields to the command port, so a click reaches the handle within that
/// bound (100 ms by default — the trade between broker round trips and UI latency).
///
/// App lifecycle: `pause()` stops polling (the loop idles without touching the broker)
/// and `resume()` restarts it — `poll` then catches up on everything missed and
/// `flush` sends what was written meanwhile. The outbox is what makes the pause
/// harmless; iOS suspends the process anyway, Android and desktop do not.
import 'dart:async';
import 'dart:isolate';

import 'zebridge.dart';

class ZeBridgeWorker {
  ZeBridgeWorker._(this._toWorker, this._fromWorker, this.tenant);

  final SendPort _toWorker;
  final ReceivePort _fromWorker;
  final String tenant;

  final _reports = StreamController<PollReport>.broadcast();
  final _pending = <int, Completer<dynamic>>{};
  int _nextId = 1;
  bool _closed = false;

  /// One report per poll that applied or seeded something.
  Stream<PollReport> get reports => _reports.stream;

  /// Spawn the worker, open the client there, run the first sync; resolves once the
  /// tenant is known (or throws with the worker's reason).
  static Future<ZeBridgeWorker> spawn(Map<String, dynamic> options,
      {int pollWaitMs = 100}) async {
    final fromWorker = ReceivePort();
    final ready = Completer<Map<String, dynamic>>();
    late final ZeBridgeWorker worker;
    late final StreamSubscription sub;
    SendPort? toWorker;

    sub = fromWorker.listen((msg) {
      final m = msg as Map<String, dynamic>;
      switch (m['type']) {
        case 'port':
          toWorker = m['port'] as SendPort;
          break;
        case 'ready':
          ready.complete(m);
          break;
        case 'fatal':
          if (!ready.isCompleted) ready.completeError(Exception(m['error']));
          break;
        case 'report':
          worker._reports.add(PollReport.fromJson(m['report'] as Map<String, dynamic>));
          break;
        case 'reply':
          final c = worker._pending.remove(m['id'] as int);
          if (c == null) break;
          if (m.containsKey('error')) {
            c.completeError(Exception(m['error']));
          } else {
            c.complete(m['result']);
          }
          break;
      }
    });

    await Isolate.spawn(_workerMain, _Boot(fromWorker.sendPort, options, pollWaitMs));
    final info = await ready.future;
    worker = ZeBridgeWorker._(toWorker!, fromWorker, (info['tenant'] as String?) ?? '—');
    // keep the subscription alive with the worker
    worker._sub = sub;
    return worker;
  }

  StreamSubscription? _sub;

  Future<T> _call<T>(String op, Map<String, dynamic> args) {
    if (_closed) return Future.error(Exception('worker closed'));
    final id = _nextId++;
    final c = Completer<T>();
    _pending[id] = c;
    _toWorker.send({'id': id, 'op': op, ...args});
    return c.future;
  }

  /// Reads against the replica: any SQL, answered as a list of maps.
  Future<List<Map<String, dynamic>>> query(String sql, [List<dynamic> params = const []]) async {
    final rows = await _call<dynamic>('query', {'sql': sql, 'params': params});
    return (rows as List).map((r) => Map<String, dynamic>.from(r as Map)).toList();
  }

  /// The blessed write path: optimistic locally, sent at once, judged upstream.
  Future<void> mutate(String table, String op, Map<String, dynamic> key,
      [Map<String, dynamic>? values]) =>
      _call<dynamic>('mutate', {'table': table, 'mop': op, 'key': key, 'values': values});

  /// Send the outbox and collect verdicts (the loop does this after every poll; call
  /// it yourself to wait for a verdict).
  Future<Map<String, dynamic>> flush(int waitMs) async =>
      Map<String, dynamic>.from(await _call<dynamic>('flush', {'waitMs': waitMs}) as Map);

  /// Stop polling while the app is in the background; nothing touches the broker.
  void pause() => _toWorker.send({'op': 'pause'});

  /// Resume: the next poll catches up, the next flush sends what was written.
  void resume() => _toWorker.send({'op': 'resume'});

  Future<void> close() async {
    if (_closed) return;
    _closed = true;
    try {
      await _call<dynamic>('close', {}).timeout(const Duration(seconds: 5));
    } catch (_) {}
    await _sub?.cancel();
    _fromWorker.close();
    await _reports.close();
  }
}

class _Boot {
  _Boot(this.toUi, this.options, this.pollWaitMs);
  final SendPort toUi;
  final Map<String, dynamic> options;
  final int pollWaitMs;
}

/// The worker isolate. Static FFI lookups are per isolate, so the library is opened
/// again HERE; the handle lives and dies here.
Future<void> _workerMain(_Boot boot) async {
  final toUi = boot.toUi;
  final commands = ReceivePort();
  toUi.send({'type': 'port', 'port': commands.sendPort});

  ZeBridge zb;
  try {
    ZeBridge.init();
    zb = ZeBridge(boot.options);
    final info = zb.sync();
    toUi.send({'type': 'ready', 'tenant': info['tenant']});
  } catch (e) {
    toUi.send({'type': 'fatal', 'error': e.toString()});
    commands.close();
    return;
  }

  var paused = false;
  var closing = false;

  // Commands are served when the loop yields (between polls): query and mutate are
  // fast, so serving them inline keeps every handle call on this one isolate.
  commands.listen((msg) {
    final m = msg as Map;
    final op = m['op'] as String;
    final id = m['id'] as int?;
    void reply(dynamic result) {
      if (id != null) toUi.send({'type': 'reply', 'id': id, 'result': result});
    }
    void fail(Object e) {
      if (id != null) toUi.send({'type': 'reply', 'id': id, 'error': e.toString()});
    }
    try {
      switch (op) {
        case 'query':
          reply(zb.query(m['sql'] as String, List<dynamic>.from(m['params'] as List? ?? const [])));
          break;
        case 'mutate':
          zb.mutate(m['table'] as String, m['mop'] as String,
              Map<String, dynamic>.from(m['key'] as Map),
              m['values'] == null ? null : Map<String, dynamic>.from(m['values'] as Map));
          // sent at once: the report of the echo follows on a later poll
          reply(null);
          break;
        case 'flush':
          reply(zb.flush(m['waitMs'] as int));
          break;
        case 'pause':
          paused = true;
          break;
        case 'resume':
          paused = false;
          break;
        case 'close':
          closing = true;
          reply(null);
          break;
        default:
          fail(Exception('unknown op $op'));
      }
    } catch (e) {
      fail(e);
    }
  });

  while (!closing) {
    if (paused) {
      await Future.delayed(const Duration(milliseconds: 200));
      continue;
    }
    try {
      final report = zb.poll(boot.pollWaitMs);
      if (report.changedTables.isNotEmpty || report.seeded.isNotEmpty) {
        toUi.send({
          'type': 'report',
          'report': {
            'applied': report.applied,
            'settled': report.settled,
            'changed_tables': report.changedTables,
            'seeded': report.seeded,
          },
        });
      }
      zb.flush(0);
    } catch (e) {
      // the connection is gone, or the principal was revoked: say so, back off
      toUi.send({'type': 'report', 'report': {'applied': 0, 'settled': 0, 'changed_tables': <String>[], 'seeded': <String>[], 'error': e.toString()}});
      await Future.delayed(const Duration(seconds: 1));
    }
    // Yield: the command port's listener runs here, between two polls.
    await Future.delayed(Duration.zero);
  }

  zb.close();
  commands.close();
}
