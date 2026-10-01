/// JWT renewal on a phone, watched by hand (NOTES §10kr).
///
/// libzb enrolls with an invite, follows `counter_public`, and renews its JWT by itself.
/// The screen shows the two clocks (this phone's, and its estimate of the bridge's),
/// the JWT's times, and every renewal. Move the phone's clock in Settings and watch:
/// the renewal still comes on the bridge's schedule, and a JWT that NATS refuses is
/// renewed at once. When the server ends a session, the screen reopens the client on
/// the stored identity, as a real app would.
///
///   --dart-define=ZB_BRIDGE_URL=http://192.168.1.11:27434   the bridge (/enroll, /renew)
///   --dart-define=ZB_NATS_URL=nats://192.168.1.11:4222
///   --dart-define=ZB_INVITE=<code>                          used once, at enrollment
library;

import 'dart:async';
import 'dart:convert';
import 'dart:io';

import 'package:flutter/material.dart';
import 'package:path_provider/path_provider.dart';

import 'package:zebridge/zebridge.dart';

const bridgeUrl = String.fromEnvironment('ZB_BRIDGE_URL', defaultValue: 'http://192.168.1.11:27434');
const natsUrl = String.fromEnvironment('ZB_NATS_URL', defaultValue: 'nats://192.168.1.11:4222');
const invite = String.fromEnvironment('ZB_INVITE');
const table = 'counter_public';

void main() => runApp(const MaterialApp(debugShowCheckedModeBanner: false, home: RenewScreen()));

class RenewScreen extends StatefulWidget {
  const RenewScreen({super.key});
  @override
  State<RenewScreen> createState() => _RenewScreenState();
}

class _RenewScreenState extends State<RenewScreen> {
  ZeBridgeWorker? _worker;
  StreamSubscription<PollReport>? _reports;
  Timer? _tick;
  String _dbPath = '';
  String _state = 'starting';
  bool _opening = false;
  int _reopens = 0;
  Object? _value;
  final _jwts = <String>[]; // in order of arrival
  Map<String, dynamic>? _id;
  final _log = <String>[];
  // The bridge's time as libzb now counts it (§10ks): an anchor (the stored offset at
  // start, then each new JWT's `iat`) plus the time elapsed since, on a stopwatch that
  // the Settings clock does not move.
  int? _anchor;
  final _sinceAnchor = Stopwatch();

  @override
  void initState() {
    super.initState();
    _boot();
  }

  @override
  void dispose() {
    _tick?.cancel();
    _reports?.cancel();
    _worker?.close();
    super.dispose();
  }

  void _say(String line) {
    final t = DateTime.now();
    final hh = '${t.hour.toString().padLeft(2, '0')}:${t.minute.toString().padLeft(2, '0')}:${t.second.toString().padLeft(2, '0')}';
    setState(() => _log.insert(0, '$hh (phone)  $line'));
  }

  Future<void> _boot() async {
    final dir = await getApplicationSupportDirectory();
    _dbPath = '${dir.path}/renew.sqlite3';
    _readIdentity();
    _tick = Timer.periodic(const Duration(seconds: 1), (_) => _everySecond());
    await _open();
  }

  File get _identityFile => File('$_dbPath.identity');

  /// The identity file libzb keeps: the JWT, the key, and the clock offset.
  void _readIdentity() {
    try {
      final id = jsonDecode(_identityFile.readAsStringSync()) as Map<String, dynamic>;
      final jwt = (id['creds'] as String).split('\n')[1];
      if (_jwts.isEmpty || _jwts.last != jwt) {
        if (_jwts.isNotEmpty) {
          _say('🔁 new JWT (#${_jwts.length + 1}), stored offset ${id['clock_offset']} s');
          _anchorAt(_iatOf(jwt));
        }
        _jwts.add(jwt);
      }
      _id = id;
      if (_anchor == null) _anchorAt(_phoneNow + ((id['clock_offset'] as num?)?.toInt() ?? 0));
    } catch (_) {
      _id = null;
    }
  }

  void _anchorAt(int bridgeTime) {
    _anchor = bridgeTime;
    _sinceAnchor..reset()..start();
  }

  int _iatOf(String jwt) {
    final p = jwt.split('.')[1];
    return ((jsonDecode(utf8.decode(base64Url.decode(base64Url.normalize(p)))) as Map)['iat'] as num).toInt();
  }

  Map<String, dynamic>? get _claims {
    if (_jwts.isEmpty) return null;
    final p = _jwts.last.split('.')[1];
    return jsonDecode(utf8.decode(base64Url.decode(base64Url.normalize(p)))) as Map<String, dynamic>;
  }

  int get _phoneNow => DateTime.now().millisecondsSinceEpoch ~/ 1000;
  int get _offset => (_id?['clock_offset'] as num?)?.toInt() ?? 0;
  int get _bridgeNow => _anchor == null ? _phoneNow + _offset : _anchor! + _sinceAnchor.elapsed.inSeconds;

  Future<void> _open() async {
    if (_opening) return;
    _opening = true;
    final enrolled = _identityFile.existsSync();
    if (!enrolled && invite.isEmpty) {
      setState(() => _state = 'no identity and no ZB_INVITE: rebuild with an invite');
      _opening = false;
      return;
    }
    setState(() => _state = enrolled ? 'opening on the stored identity' : 'enrolling');
    try {
      final w = await ZeBridgeWorker.spawn({
        'bridgeUrl': bridgeUrl,
        'natsUrl': natsUrl,
        'dbPath': _dbPath,
        'tables': [table],
        if (!enrolled) 'invite': invite,
      });
      _worker = w;
      _readIdentity();
      _say(enrolled ? 'opened (tenant ${w.tenant})' : 'enrolled as ${_id?['principal']} (tenant ${w.tenant})');
      setState(() => _state = 'connected');
      _reports = w.reports.listen((r) {
        if (r.error != null) _onError(r.error!);
      });
    } catch (e) {
      _say('open failed: $e — retried in 10 s');
      setState(() => _state = 'open failed, retrying');
      Future.delayed(const Duration(seconds: 10), _open);
    } finally {
      _opening = false;
    }
  }

  /// What a host does when the server ended the session: close, open again. libzb has
  /// renewed the JWT before handing back AuthExpired, so the reopen presents a fresh one.
  Future<void> _onError(String error) async {
    if (_opening || _worker == null) return;
    _say('poll: $error — reopening');
    final w = _worker;
    _worker = null;
    await _reports?.cancel();
    _reports = null;
    await w?.close();
    _reopens++;
    await _open();
  }

  Future<void> _everySecond() async {
    _readIdentity();
    final w = _worker;
    if (w != null) {
      try {
        final rows = await w.query('SELECT value FROM $table LIMIT 1');
        final v = rows.isEmpty ? null : rows.first['value'];
        if (v != _value && _value != null) _say('live: value $_value → $v');
        _value = v;
      } catch (_) {}
    }
    if (mounted) setState(() {});
  }

  /// Start over: a new invite is needed afterwards.
  Future<void> _forget() async {
    await _reports?.cancel();
    await _worker?.close();
    _worker = null;
    for (final f in [_dbPath, '$_dbPath-wal', '$_dbPath-shm', '$_dbPath.identity']) {
      final file = File(f);
      if (file.existsSync()) file.deleteSync();
    }
    _jwts.clear();
    _anchor = null;
    _say('replica and identity deleted');
    await _open();
  }

  String _hms(int unix) {
    final t = DateTime.fromMillisecondsSinceEpoch(unix * 1000);
    return '${t.hour.toString().padLeft(2, '0')}:${t.minute.toString().padLeft(2, '0')}:${t.second.toString().padLeft(2, '0')}';
  }

  String _span(int s) => s < 0
      ? '−${_span(-s)}'
      : (s >= 3600 ? '${s ~/ 3600} h ${(s % 3600) ~/ 60} min' : (s >= 60 ? '${s ~/ 60} min ${s % 60} s' : '$s s'));

  @override
  Widget build(BuildContext context) {
    final c = _claims;
    final iat = (c?['iat'] as num?)?.toInt();
    final exp = (c?['exp'] as num?)?.toInt();
    final dueAt = (iat != null && exp != null) ? exp - (exp - iat) ~/ 4 : null;
    const mono = TextStyle(fontFamily: 'Menlo', fontSize: 13);
    Widget row(String k, String v) => Padding(
          padding: const EdgeInsets.symmetric(vertical: 2),
          child: Row(children: [
            SizedBox(width: 150, child: Text(k, style: mono.copyWith(color: Colors.grey))),
            Expanded(child: Text(v, style: mono)),
          ]),
        );
    return Scaffold(
      appBar: AppBar(title: const Text('JWT renewal'), actions: [
        IconButton(icon: const Icon(Icons.refresh), tooltip: 'reopen', onPressed: () => _onError('reopen by hand')),
        // Long press only: a tap next to "reopen" deleted the identity once (2026-10-01).
        Padding(
          padding: const EdgeInsets.symmetric(horizontal: 12),
          child: GestureDetector(onLongPress: _forget, child: const Icon(Icons.delete_outline)),
        ),
      ]),
      body: SafeArea(
        child: Padding(
          padding: const EdgeInsets.all(12),
          child: Column(crossAxisAlignment: CrossAxisAlignment.start, children: [
            row('state', _state),
            row('principal', '${_id?['principal'] ?? '—'}'),
            const Divider(),
            row('phone clock', _hms(_phoneNow)),
            row('bridge clock (est.)', '${_hms(_bridgeNow)}   phone ${_span(_phoneNow - _bridgeNow)} off'),
            row('stored offset', '$_offset s (used at the next launch)'),
            const Divider(),
            row('JWT issued', iat == null ? '—' : '${_hms(iat)} (bridge)'),
            row('JWT expires', exp == null ? '—' : '${_hms(exp)}   in ${_span(exp - _bridgeNow)}'),
            row('renewal due', dueAt == null ? '—' : '${_hms(dueAt)}   in ${_span(dueAt - _bridgeNow)}'),
            row('JWTs so far', '${_jwts.length}   (reopens $_reopens)'),
            const Divider(),
            row('$table.value', '${_value ?? '—'}'),
            const Divider(),
            Expanded(child: ListView(children: [for (final l in _log) Text(l, style: mono.copyWith(fontSize: 12))])),
          ]),
        ),
      ),
    );
  }
}
