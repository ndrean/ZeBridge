/// libzb's C ABI (libzb/abi.json) for Dart. One handle, one isolate — `ZeBridgeWorker`
/// owns both; this class is what runs inside it.
///
/// Every string crosses as UTF-8; every returned string is JSON the caller owns and
/// frees with `zb_free` (`_take` does both). A NULL result means the call failed, and
/// `zb_last_error` — per thread, like errno — says why.
library;

import 'dart:convert';
import 'dart:ffi' as ffi;
import 'dart:io';

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
  /// lostColumns?, rebasedAs?, reason?} — `msgId` is what [mutate] returned.
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

/// Where libzb is. iOS links it into the app (its symbols are the process's own);
/// Android loads `libzbcore.so` from jniLibs; a desktop host takes `ZB_LIB`, else the
/// repository's own build found by walking up from the working directory — never a
/// path written into the code.
ffi.DynamicLibrary _open() {
  if (Platform.isIOS) return ffi.DynamicLibrary.process();
  if (Platform.isAndroid) return ffi.DynamicLibrary.open('libzbcore.so');
  final name = Platform.isMacOS ? 'libzbcore.dylib' : (Platform.isWindows ? 'zbcore.dll' : 'libzbcore.so');
  final env = Platform.environment['ZB_LIB'];
  if (env != null && env.isNotEmpty) return ffi.DynamicLibrary.open(env);
  var dir = Directory.current;
  for (var i = 0; i < 8; i++) {
    final f = File('${dir.path}/libzb/zig-out/lib/$name');
    if (f.existsSync()) return ffi.DynamicLibrary.open(f.path);
    if (dir.parent.path == dir.path) break;
    dir = dir.parent;
  }
  return ffi.DynamicLibrary.open(name); // the loader's own search path
}

class ZeBridge {
  static late ffi.DynamicLibrary _lib;
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

  /// Open libzb (once per isolate: FFI lookups are per isolate) and check its ABI.
  static void init() {
    if (_ready) return;
    _lib = _open();
    _abi = _lib.lookupFunction<_IntOf0C, _IntOf0>('zb_abi_version');
    _lastError = _lib.lookupFunction<_PtrOf0C, _PtrOf0>('zb_last_error');
    _free = _lib.lookupFunction<_FreeC, _Free>('zb_free');
    _connect = _lib.lookupFunction<_OpenC, _Open>('zb_client_connect');
    _close = _lib.lookupFunction<_IntOfHC, _IntOfH>('zb_client_close');
    _wipe = _lib.lookupFunction<_IntOfHC, _IntOfH>('zb_client_wipe');
    _revoked = _lib.lookupFunction<_IntOfHC, _IntOfH>('zb_client_revoked');
    _wake = _lib.lookupFunction<_IntOfHC, _IntOfH>('zb_client_wake');
    _sync = _lib.lookupFunction<_PtrOfHC, _PtrOfH>('zb_client_sync');
    _stamp = _lib.lookupFunction<_PtrOfHC, _PtrOfH>('zb_client_stamp');
    _poll = _lib.lookupFunction<_PtrOfHNC, _PtrOfHN>('zb_client_poll');
    _flush = _lib.lookupFunction<_PtrOfHNC, _PtrOfHN>('zb_client_flush_outbox');
    _query = _lib.lookupFunction<_PtrOfHSSC, _PtrOfHSS>('zb_client_query');
    _mutate = _lib.lookupFunction<_PtrOfHSSSSC, _PtrOfHSSSS>('zb_client_mutate');
    _join = _lib.lookupFunction<_PtrOfHSC, _PtrOfHS>('zb_client_join');
    _leave = _lib.lookupFunction<_PtrOfHSC, _PtrOfHS>('zb_client_leave');
    _serve = _lib.lookupFunction<_PtrOfHSC, _PtrOfHS>('zb_client_serve');
    _request = _lib.lookupFunction<_PtrOfHSSNC, _PtrOfHSSN>('zb_client_request');
    _ingest = _lib.lookupFunction<_PtrOfHSSSC, _PtrOfHSSS>('zb_client_ingest');
    _reply = _lib.lookupFunction<_PtrOfHNSC, _PtrOfHNS>('zb_client_reply');
    _call = _lib.lookupFunction<_PtrOfSSC, _PtrOfSS>('zb_call');
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
