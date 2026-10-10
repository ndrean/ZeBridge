/// libzb's C ABI (libzb/abi.json) for Dart. One handle, one isolate — `ZeBridgeWorker`
/// owns both; this class is what runs inside it.
///
/// Every string crosses as UTF-8; every returned string is JSON the caller owns and
/// frees with `zb_free` (`_take` does both). A NULL result means the call failed, and
/// `zb_last_error` — per thread, like errno — says why.
///
/// libzb itself comes from the build hook (hook/build.dart): the build bundles the
/// library made for its target, and every `@Native` below binds to it by this asset id.
@ffi.DefaultAsset('package:zebridge/libzb')
library;

import 'dart:convert';
import 'dart:ffi' as ffi;

import 'package:ffi/ffi.dart';

/// libzb's C ABI version this package was written for; libzb/python/abi_check.py checks it.
const zbAbi = 6;

/// A call libzb refused, with libzb's own words.
class ZeBridgeException implements Exception {
  ZeBridgeException(this.message);
  final String message;
  @override
  String toString() => message;
}

class PollReport {
  PollReport({
    required this.applied,
    required this.settled,
    required this.changedTables,
    required this.seeded,
    this.unreadable = const [],
    this.requests = const [],
    this.outcomes = const [],
    this.pending = 0,
    this.error,
  });

  final int applied;
  final int settled;
  final List<String> changedTables;
  final List<String> seeded;

  /// §10fq: tenants whose streams cannot be read right now — set aside and retried.
  final List<String> unreadable;

  /// §10hp: questions to answer when this client serves.
  final List<Map<String, dynamic>> requests;

  /// What became of this client's writes since the last report, each once:
  /// {msgId, version, table, columns, outcome: applied|rebased|lost|deleted|rejected,
  /// lostColumns?, rebasedAs?, reason?, sqlstate?, detail?} — `msgId` is what [mutate]
  /// returned; `sqlstate` and `detail` are PostgreSQL's code and message on a rejection.
  final List<Map<String, dynamic>> outcomes;

  /// The writes still in the outbox: applied here, not yet judged by PostgreSQL.
  final int pending;

  /// Set by the worker when a poll itself failed (connection gone, principal revoked).
  final String? error;

  factory PollReport.fromJson(Map<String, dynamic> j) => PollReport(
        applied: (j['applied'] as num?)?.toInt() ?? 0,
        settled: (j['settled'] as num?)?.toInt() ?? 0,
        changedTables: List<String>.from(j['changed_tables'] ?? const []),
        seeded: List<String>.from(j['seeded'] ?? const []),
        unreadable: List<String>.from(j['unreadable'] ?? const []),
        requests: ((j['requests'] as List?) ?? const []).map((e) => Map<String, dynamic>.from(e as Map)).toList(),
        outcomes: ((j['outcomes'] as List?) ?? const []).map((e) => Map<String, dynamic>.from(e as Map)).toList(),
        pending: (j['pending'] as num?)?.toInt() ?? 0,
        error: j['error'] as String?,
      );

  Map<String, dynamic> toJson() => {
        'applied': applied,
        'settled': settled,
        'changed_tables': changedTables,
        'seeded': seeded,
        'unreadable': unreadable,
        'requests': requests,
        'outcomes': outcomes,
        'pending': pending,
        if (error != null) 'error': error,
      };
}

typedef _PtrOf0C = ffi.Pointer<Utf8> Function();
typedef _PtrOf0 = ffi.Pointer<Utf8> Function();
typedef _IntOf0C = ffi.Int32 Function();
typedef _IntOf0 = int Function();
typedef _OpenC = ffi.Uint64 Function(ffi.Pointer<Utf8>);
typedef _Open = int Function(ffi.Pointer<Utf8>);
typedef _IntOfHC = ffi.Int32 Function(ffi.Uint64);
typedef _IntOfH = int Function(int);
typedef _PtrOfHC = ffi.Pointer<Utf8> Function(ffi.Uint64);
typedef _PtrOfH = ffi.Pointer<Utf8> Function(int);
typedef _PtrOfHNC = ffi.Pointer<Utf8> Function(ffi.Uint64, ffi.Uint64);
typedef _PtrOfHN = ffi.Pointer<Utf8> Function(int, int);
typedef _PtrOfHSC = ffi.Pointer<Utf8> Function(ffi.Uint64, ffi.Pointer<Utf8>);
typedef _PtrOfHS = ffi.Pointer<Utf8> Function(int, ffi.Pointer<Utf8>);
typedef _PtrOfHSSC = ffi.Pointer<Utf8> Function(ffi.Uint64, ffi.Pointer<Utf8>, ffi.Pointer<Utf8>);
typedef _PtrOfHSS = ffi.Pointer<Utf8> Function(int, ffi.Pointer<Utf8>, ffi.Pointer<Utf8>);
typedef _PtrOfHSSSC = ffi.Pointer<Utf8> Function(ffi.Uint64, ffi.Pointer<Utf8>, ffi.Pointer<Utf8>, ffi.Pointer<Utf8>);
typedef _PtrOfHSSS = ffi.Pointer<Utf8> Function(int, ffi.Pointer<Utf8>, ffi.Pointer<Utf8>, ffi.Pointer<Utf8>);
typedef _PtrOfHSSSSC = ffi.Pointer<Utf8> Function(ffi.Uint64, ffi.Pointer<Utf8>, ffi.Pointer<Utf8>, ffi.Pointer<Utf8>, ffi.Pointer<Utf8>);
typedef _PtrOfHSSSS = ffi.Pointer<Utf8> Function(int, ffi.Pointer<Utf8>, ffi.Pointer<Utf8>, ffi.Pointer<Utf8>, ffi.Pointer<Utf8>);
typedef _PtrOfHSSNC = ffi.Pointer<Utf8> Function(ffi.Uint64, ffi.Pointer<Utf8>, ffi.Pointer<Utf8>, ffi.Uint64);
typedef _PtrOfHSSN = ffi.Pointer<Utf8> Function(int, ffi.Pointer<Utf8>, ffi.Pointer<Utf8>, int);
typedef _PtrOfHNSC = ffi.Pointer<Utf8> Function(ffi.Uint64, ffi.Uint64, ffi.Pointer<Utf8>);
typedef _PtrOfHNS = ffi.Pointer<Utf8> Function(int, int, ffi.Pointer<Utf8>);
typedef _PtrOfSSC = ffi.Pointer<Utf8> Function(ffi.Pointer<Utf8>, ffi.Pointer<Utf8>);
typedef _PtrOfSS = ffi.Pointer<Utf8> Function(ffi.Pointer<Utf8>, ffi.Pointer<Utf8>);
typedef _FreeC = ffi.Void Function(ffi.Pointer<Utf8>);
typedef _Free = void Function(ffi.Pointer<Utf8>);

// libzb's C functions, one per symbol the client calls.
@ffi.Native<_IntOf0C>(symbol: 'zb_abi_version')
external int _zbAbiVersion();
@ffi.Native<_PtrOf0C>(symbol: 'zb_last_error')
external ffi.Pointer<Utf8> _zbLastError();
@ffi.Native<_FreeC>(symbol: 'zb_free')
external void _zbFree(ffi.Pointer<Utf8> p);
@ffi.Native<_OpenC>(symbol: 'zb_client_connect')
external int _zbConnect(ffi.Pointer<Utf8> opts);
@ffi.Native<_IntOfHC>(symbol: 'zb_client_close')
external int _zbClose(int h);
@ffi.Native<_IntOfHC>(symbol: 'zb_client_wipe')
external int _zbWipe(int h);
@ffi.Native<_IntOfHC>(symbol: 'zb_client_revoked')
external int _zbRevoked(int h);
@ffi.Native<_IntOfHC>(symbol: 'zb_client_wake')
external int _zbWake(int h);
@ffi.Native<_PtrOfHC>(symbol: 'zb_client_sync')
external ffi.Pointer<Utf8> _zbSync(int h);
@ffi.Native<_PtrOfHC>(symbol: 'zb_client_stamp')
external ffi.Pointer<Utf8> _zbStamp(int h);
@ffi.Native<_PtrOfHNC>(symbol: 'zb_client_poll')
external ffi.Pointer<Utf8> _zbPoll(int h, int ms);
@ffi.Native<_PtrOfHNC>(symbol: 'zb_client_flush_outbox')
external ffi.Pointer<Utf8> _zbFlush(int h, int ms);
@ffi.Native<_PtrOfHSSC>(symbol: 'zb_client_query')
external ffi.Pointer<Utf8> _zbQuery(int h, ffi.Pointer<Utf8> sql, ffi.Pointer<Utf8> params);
@ffi.Native<_PtrOfHSSSSC>(symbol: 'zb_client_mutate')
external ffi.Pointer<Utf8> _zbMutate(int h, ffi.Pointer<Utf8> table, ffi.Pointer<Utf8> op, ffi.Pointer<Utf8> key, ffi.Pointer<Utf8> values);
@ffi.Native<_PtrOfHSC>(symbol: 'zb_client_join')
external ffi.Pointer<Utf8> _zbJoin(int h, ffi.Pointer<Utf8> tenant);
@ffi.Native<_PtrOfHSC>(symbol: 'zb_client_leave')
external ffi.Pointer<Utf8> _zbLeave(int h, ffi.Pointer<Utf8> tenant);
@ffi.Native<_PtrOfHSC>(symbol: 'zb_client_serve')
external ffi.Pointer<Utf8> _zbServe(int h, ffi.Pointer<Utf8> opts);
@ffi.Native<_PtrOfHSSNC>(symbol: 'zb_client_request')
external ffi.Pointer<Utf8> _zbRequest(int h, ffi.Pointer<Utf8> subject, ffi.Pointer<Utf8> payload, int timeoutMs);
@ffi.Native<_PtrOfHSSSC>(symbol: 'zb_client_ingest')
external ffi.Pointer<Utf8> _zbIngest(int h, ffi.Pointer<Utf8> table, ffi.Pointer<Utf8> answer, ffi.Pointer<Utf8> scope);
@ffi.Native<_PtrOfHNSC>(symbol: 'zb_client_reply')
external ffi.Pointer<Utf8> _zbReply(int h, int id, ffi.Pointer<Utf8> answer);
@ffi.Native<_PtrOfSSC>(symbol: 'zb_call')
external ffi.Pointer<Utf8> _zbCall(ffi.Pointer<Utf8> fn, ffi.Pointer<Utf8> args);

class ZeBridge {
  static late _IntOf0 _abi;
  static late _PtrOf0 _lastError;
  static late _Free _free;
  static late _Open _connect;
  static late _IntOfH _close, _wipe, _revoked, _wake;
  static late _PtrOfH _sync;
  static late _PtrOfH _stamp;
  static late _PtrOfHN _poll, _flush;
  static late _PtrOfHSS _query;
  static late _PtrOfHSSSS _mutate;
  static late _PtrOfHS _join, _leave, _serve;
  static late _PtrOfHSSN _request;
  static late _PtrOfHSSS _ingest;
  static late _PtrOfHNS _reply;
  static late _PtrOfSS _call;
  static bool _ready = false;

  /// Bind libzb (once per isolate) and check its ABI.
  static void init() {
    if (_ready) return;
    _abi = () => _zbAbiVersion();
    _lastError = () => _zbLastError();
    _free = (p) => _zbFree(p);
    _connect = (o) => _zbConnect(o);
    _close = (h) => _zbClose(h);
    _wipe = (h) => _zbWipe(h);
    _revoked = (h) => _zbRevoked(h);
    _wake = (h) => _zbWake(h);
    _sync = (h) => _zbSync(h);
    _stamp = (h) => _zbStamp(h);
    _poll = (h, ms) => _zbPoll(h, ms);
    _flush = (h, ms) => _zbFlush(h, ms);
    _query = (h, a, b) => _zbQuery(h, a, b);
    _mutate = (h, a, b, c, d) => _zbMutate(h, a, b, c, d);
    _join = (h, t) => _zbJoin(h, t);
    _leave = (h, t) => _zbLeave(h, t);
    _serve = (h, o) => _zbServe(h, o);
    _request = (h, a, b, n) => _zbRequest(h, a, b, n);
    _ingest = (h, a, b, c) => _zbIngest(h, a, b, c);
    _reply = (h, i, a) => _zbReply(h, i, a);
    _call = (f, a) => _zbCall(f, a);
    final abi = _abi();
    if (abi != zbAbi) throw ZeBridgeException('libzb speaks ABI $abi, this package $zbAbi: rebuild one of them');
    _ready = true;
  }

  static String? _lastErrorText() {
    final p = _lastError();
    return p == ffi.nullptr ? null : p.toDartString();
  }

  /// An owned JSON result: decoded, freed; `{"error"}` and NULL raise with libzb's words.
  static dynamic _take(ffi.Pointer<Utf8> p) {
    if (p == ffi.nullptr) throw ZeBridgeException(_lastErrorText() ?? 'libzb returned nothing');
    final text = p.toDartString();
    _free(p);
    final decoded = jsonDecode(text);
    if (decoded is Map && decoded.containsKey('error')) {
      final d = decoded['detail'];
      throw ZeBridgeException(d == null ? '${decoded['error']}' : '${decoded['error']}: $d');
    }
    return decoded;
  }

  /// UTF-8 arguments freed after the call, whatever it returns.
  static T _with<T>(List<Object?> args, T Function(List<ffi.Pointer<Utf8>>) fn) {
    final ptrs = args.map((a) => a == null ? ffi.nullptr.cast<Utf8>() : (a is String ? a : jsonEncode(a)).toNativeUtf8()).toList();
    try {
      return fn(ptrs);
    } finally {
      for (final p in ptrs) {
        if (p != ffi.nullptr) malloc.free(p);
      }
    }
  }

  late final int _handle;

  /// Open the client (enrolling with `invite` when there is no identity yet). Throws
  /// [ZeBridgeException] with libzb's reason.
  ZeBridge(Map<String, dynamic> options) {
    init();
    _handle = _with([options], (p) => _connect(p[0]));
    if (_handle == 0) throw ZeBridgeException(_lastErrorText() ?? 'zb_client_connect failed');
  }

  Map<String, dynamic> sync() => Map<String, dynamic>.from(_take(_sync(_handle)) as Map);

  PollReport poll(int waitMs) => PollReport.fromJson(Map<String, dynamic>.from(_take(_poll(_handle, waitMs)) as Map));

  Map<String, dynamic> flush(int waitMs) => Map<String, dynamic>.from(_take(_flush(_handle, waitMs)) as Map);

  /// Read the replica: SQL with `?` placeholders; rows as maps. Writes are refused.
  List<Map<String, dynamic>> query(String sql, [List<dynamic> params = const []]) {
    final r = _with([sql, params], (p) => _take(_query(_handle, p[0], p[1]))) as Map;
    final cols = List<String>.from(r['columns'] as List);
    return (r['rows'] as List).map((row) => {for (var i = 0; i < cols.length; i++) cols[i]: (row as List)[i]}).toList();
  }

  /// Write a row (INSERT, UPDATE or DELETE, any case): local at once, settled by its verdict.
  Map<String, dynamic> mutate(String table, String op, Map<String, dynamic> key, [Map<String, dynamic>? values]) =>
      Map<String, dynamic>.from(_with([table, op, key, values], (p) => _take(_mutate(_handle, p[0], p[1], p[2], p[3]))) as Map);

  /// A register stamp (the `t` of {v, t, w}, COOPERATIVE_EDITING.md): the bridge's time as
  /// this client estimates it, never behind what it has seen or stamped.
  String stamp() => (_take(_stamp(_handle)) as Map)['stamp'] as String;

  List<String> join(String tenant) => List<String>.from((_with([tenant], (p) => _take(_join(_handle, p[0]))) as Map)['tenants'] as List);
  List<String> leave(String tenant) => List<String>.from((_with([tenant], (p) => _take(_leave(_handle, p[0]))) as Map)['tenants'] as List);

  Map<String, dynamic> request(String subject, Map<String, dynamic> payload, int timeoutMs) =>
      Map<String, dynamic>.from(_with([subject, payload], (p) => _take(_request(_handle, p[0], p[1], timeoutMs))) as Map);

  /// Store a request's answer rows in an on-demand table; the rows applied.
  int ingest(String table, Map<String, dynamic> answer, [Map<String, dynamic>? scope]) {
    final r = _with([table, answer, scope], (p) => _take(_ingest(_handle, p[0], p[1], p[2])));
    return r is Map ? ((r['applied'] as num?)?.toInt() ?? 0) : (r as num).toInt();
  }

  Map<String, dynamic> serve(Map<String, dynamic> options) => Map<String, dynamic>.from(_with([options], (p) => _take(_serve(_handle, p[0]))) as Map);
  Map<String, dynamic> reply(int id, Map<String, dynamic> answer) => Map<String, dynamic>.from(_with([answer], (p) => _take(_reply(_handle, id, p[0]))) as Map);

  /// One of libzb's CORE functions by name (e.g. `mergeRegisters`) — handle-free.
  static Map<String, dynamic> call(String fn, Map<String, dynamic> args) {
    init();
    return Map<String, dynamic>.from(_with([fn, args], (p) => _take(_call(p[0], p[1]))) as Map);
  }

  bool get revoked => _revoked(_handle) == 1;

  /// The handle, for [wake] from another isolate (it is a number, safe to send).
  int get handle => _handle;

  /// End the poll's wait on [handle] — the one call allowed from an isolate that does
  /// not own the client: the UI isolate has queued a command for the worker.
  static void wake(int handle) {
    init();
    _wake(handle);
  }
  void close() => _close(_handle);
  void wipe() => _wipe(_handle);
}
