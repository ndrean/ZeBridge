import 'dart:ffi' as ffi;
import 'dart:convert';
import 'package:ffi/ffi.dart';
import 'dart:io';

typedef ZbClientOpenC = ffi.Uint64 Function(ffi.Pointer<Utf8> optsJson);
typedef ZbClientOpenDart = int Function(ffi.Pointer<Utf8> optsJson);

typedef ZbClientCloseC = ffi.Int32 Function(ffi.Uint64 handle);
typedef ZbClientCloseDart = int Function(int handle);

typedef ZbClientSyncC = ffi.Pointer<Utf8> Function(ffi.Uint64 handle);
typedef ZbClientSyncDart = ffi.Pointer<Utf8> Function(int handle);

typedef ZbClientQueryC = ffi.Pointer<Utf8> Function(
    ffi.Uint64 handle, ffi.Pointer<Utf8> sql, ffi.Pointer<Utf8> params);
typedef ZbClientQueryDart = ffi.Pointer<Utf8> Function(
    int handle, ffi.Pointer<Utf8> sql, ffi.Pointer<Utf8> params);

typedef ZbClientMutateC = ffi.Pointer<Utf8> Function(
    ffi.Uint64 handle,
    ffi.Pointer<Utf8> table,
    ffi.Pointer<Utf8> op,
    ffi.Pointer<Utf8> keyJson,
    ffi.Pointer<Utf8> valuesJson);
typedef ZbClientMutateDart = ffi.Pointer<Utf8> Function(
    int handle,
    ffi.Pointer<Utf8> table,
    ffi.Pointer<Utf8> op,
    ffi.Pointer<Utf8> keyJson,
    ffi.Pointer<Utf8> valuesJson);

typedef ZbClientTenantC = ffi.Pointer<Utf8> Function(
    ffi.Uint64 handle, ffi.Pointer<Utf8> tenant);
typedef ZbClientTenantDart = ffi.Pointer<Utf8> Function(
    int handle, ffi.Pointer<Utf8> tenant);

typedef ZbClientFlushC = ffi.Pointer<Utf8> Function(
    ffi.Uint64 handle, ffi.Uint64 waitMs);
typedef ZbClientFlushDart = ffi.Pointer<Utf8> Function(int handle, int waitMs);

typedef ZbClientPollC = ffi.Pointer<Utf8> Function(
    ffi.Uint64 handle, ffi.Uint64 waitMs);
typedef ZbClientPollDart = ffi.Pointer<Utf8> Function(int handle, int waitMs);

typedef ZbClientRequestC = ffi.Pointer<Utf8> Function(
    ffi.Uint64 handle,
    ffi.Pointer<Utf8> subject,
    ffi.Pointer<Utf8> payloadJson,
    ffi.Uint64 timeoutMs);
typedef ZbClientRequestDart = ffi.Pointer<Utf8> Function(int handle,
    ffi.Pointer<Utf8> subject, ffi.Pointer<Utf8> payloadJson, int timeoutMs);

typedef ZbClientIngestC = ffi.Pointer<Utf8> Function(
    ffi.Uint64 handle,
    ffi.Pointer<Utf8> table,
    ffi.Pointer<Utf8> answerJson,
    ffi.Pointer<Utf8> scopeJson);
typedef ZbClientIngestDart = ffi.Pointer<Utf8> Function(
    int handle,
    ffi.Pointer<Utf8> table,
    ffi.Pointer<Utf8> answerJson,
    ffi.Pointer<Utf8> scopeJson);

typedef ZbCallC = ffi.Pointer<Utf8> Function(
    ffi.Pointer<Utf8> fn, ffi.Pointer<Utf8> argsJson);
typedef ZbCallDart = ffi.Pointer<Utf8> Function(
    ffi.Pointer<Utf8> fn, ffi.Pointer<Utf8> argsJson);

typedef ZbFreeC = ffi.Void Function(ffi.Pointer<Utf8> p);
typedef ZbFreeDart = void Function(ffi.Pointer<Utf8> p);

class PollReport {
  final int applied;
  final int settled;
  final List<String> changedTables;
  final List<String> seeded;

  /// §10fq: tenants whose streams cannot be read right now (deleted, or denied) —
  /// set aside by libzb and retried with backoff; the others keep being served.
  final List<String> unreadable;

  /// Set by the worker when a poll itself failed (connection gone, principal
  /// revoked): the loop backs off and says why; the lists are then empty.
  final String? error;

  PollReport(
      {required this.applied,
      required this.settled,
      required this.changedTables,
      required this.seeded,
      this.unreadable = const [],
      this.error});

  factory PollReport.fromJson(Map<String, dynamic> json) {
    return PollReport(
      applied: json['applied'] ?? 0,
      settled: json['settled'] ?? 0,
      changedTables: List<String>.from(json['changed_tables'] ?? []),
      seeded: List<String>.from(json['seeded'] ?? []),
      unreadable: List<String>.from(json['unreadable'] ?? []),
      error: json['error'] as String?,
    );
  }
}

/// `{"error": name}` — and, for a storage error, `"detail"` with SQLite's own words.
String _reason(Map decoded) {
  final d = decoded['detail'];
  return d == null ? '${decoded['error']}' : '${decoded['error']}: $d';
}

class ZeBridge {
  static late ffi.DynamicLibrary _lib;
  static late ZbClientOpenDart _open;
  static late ZbClientCloseDart _close;
  static late ZbClientSyncDart _sync;
  static late ZbClientQueryDart _query;
  static late ZbClientMutateDart _mutate;
  static late ZbClientFlushDart _flush;
  static late ZbClientTenantDart _join;
  static late ZbClientTenantDart _leave;
  static late ZbClientPollDart _poll;
  static late ZbClientRequestDart _request;
  static late ZbClientIngestDart _ingest;
  static late ZbCallDart _callFn;
  static late ZbFreeDart _free;

  static void init() {
    String libPath = '';
    if (Platform.isMacOS) {
      libPath =
          '/Users/nevendrean/code/zig/ZeBridge/libzb/zig-out/lib/libzbcore.dylib';
    } else if (Platform.isLinux) {
      libPath =
          '/Users/nevendrean/code/zig/ZeBridge/libzb/zig-out/lib/libzbcore.so';
    } else {
      throw Exception('Unsupported platform');
    }

    _lib = ffi.DynamicLibrary.open(libPath);

    _open =
        _lib.lookupFunction<ZbClientOpenC, ZbClientOpenDart>('zb_client_open');
    _close = _lib
        .lookupFunction<ZbClientCloseC, ZbClientCloseDart>('zb_client_close');
    _sync =
        _lib.lookupFunction<ZbClientSyncC, ZbClientSyncDart>('zb_client_sync');
    _query = _lib
        .lookupFunction<ZbClientQueryC, ZbClientQueryDart>('zb_client_query');
    _mutate = _lib.lookupFunction<ZbClientMutateC, ZbClientMutateDart>(
        'zb_client_mutate');
    _flush = _lib
        .lookupFunction<ZbClientFlushC, ZbClientFlushDart>('zb_client_flush');
    _join = _lib
        .lookupFunction<ZbClientTenantC, ZbClientTenantDart>('zb_client_join');
    _leave = _lib
        .lookupFunction<ZbClientTenantC, ZbClientTenantDart>('zb_client_leave');
    _poll =
        _lib.lookupFunction<ZbClientPollC, ZbClientPollDart>('zb_client_poll');
    _callFn = _lib.lookupFunction<ZbCallC, ZbCallDart>('zb_call');
    _request = _lib.lookupFunction<ZbClientRequestC, ZbClientRequestDart>(
        'zb_client_request');
    _ingest = _lib.lookupFunction<ZbClientIngestC, ZbClientIngestDart>(
        'zb_client_ingest');
    _free = _lib.lookupFunction<ZbFreeC, ZbFreeDart>('zb_free');
  }

  late int _handle;

  ZeBridge(Map<String, dynamic> options) {
    final optsStr = jsonEncode(options);
    final optsC = optsStr.toNativeUtf8();
    _handle = _open(optsC);
    malloc.free(optsC);

    if (_handle == 0) {
      throw Exception('Failed to open ZeBridge client');
    }
  }

  void close() {
    _close(_handle);
  }

  Map<String, dynamic> sync() {
    final resPtr = _sync(_handle);
    if (resPtr == ffi.nullptr) throw Exception('sync failed');
    final resStr = resPtr.toDartString();
    _free(resPtr);
    return jsonDecode(resStr);
  }

  List<Map<String, dynamic>> query(String sql,
      [List<dynamic> params = const []]) {
    final sqlC = sql.toNativeUtf8();
    final paramsC = jsonEncode(params).toNativeUtf8();
    final resPtr = _query(_handle, sqlC, paramsC);
    malloc.free(sqlC);
    malloc.free(paramsC);

    if (resPtr == ffi.nullptr) throw Exception('query failed');
    final resStr = resPtr.toDartString();
    _free(resPtr);

    final decoded = jsonDecode(resStr);
    if (decoded is Map && decoded.containsKey('error')) {
      throw Exception(_reason(decoded));
    }

    if (decoded is Map &&
        decoded.containsKey('columns') &&
        decoded.containsKey('rows')) {
      final cols = List<String>.from(decoded['columns']);
      final rows = List<List<dynamic>>.from(decoded['rows']);

      return rows.map((row) {
        final Map<String, dynamic> map = {};
        for (int i = 0; i < cols.length; i++) {
          map[cols[i]] = row[i];
        }
        return map;
      }).toList();
    }

    return [];
  }

  void mutate(String table, String op, Map<String, dynamic> key,
      [Map<String, dynamic>? values]) {
    final tableC = table.toNativeUtf8();
    final opC = op.toNativeUtf8();
    final keyC = jsonEncode(key).toNativeUtf8();
    final valuesC =
        values != null ? jsonEncode(values).toNativeUtf8() : ffi.nullptr;

    final resPtr = _mutate(_handle, tableC, opC, keyC, valuesC);
    malloc.free(tableC);
    malloc.free(opC);
    malloc.free(keyC);
    if (valuesC != ffi.nullptr) malloc.free(valuesC);

    if (resPtr == ffi.nullptr) throw Exception('mutate failed');
    final resStr = resPtr.toDartString();
    _free(resPtr);

    final decoded = jsonDecode(resStr);
    if (decoded is Map && decoded.containsKey('error')) {
      throw Exception(_reason(decoded));
    }
  }

  PollReport poll(int waitMs) {
    final resPtr = _poll(_handle, waitMs);
    if (resPtr == ffi.nullptr) throw Exception('poll failed');
    final resStr = resPtr.toDartString();
    _free(resPtr);

    final decoded = jsonDecode(resStr);
    if (decoded is Map && decoded.containsKey('error')) {
      throw Exception(_reason(decoded));
    }
    return PollReport.fromJson(decoded);
  }

  /// §10fn: follow one more tenant (its chains seed into the same tables, its
  /// stream joins the tail at the next poll). The JWT decides whether the broker
  /// allows it. Returns the memberships followed now.
  /// §10hj: ask a service on `query.<tenant>.<name>`; its reply, decoded.
  Map<String, dynamic> request(
      String subject, Map<String, dynamic> payload, int timeoutMs) {
    final subjectC = subject.toNativeUtf8();
    final payloadC = jsonEncode(payload).toNativeUtf8();
    final resPtr = _request(_handle, subjectC, payloadC, timeoutMs);
    malloc.free(subjectC);
    malloc.free(payloadC);
    if (resPtr == ffi.nullptr) throw Exception('request failed');
    final resStr = resPtr.toDartString();
    _free(resPtr);
    final decoded = jsonDecode(resStr);
    if (decoded is Map && decoded.containsKey('error')) {
      throw Exception(_reason(decoded));
    }
    return Map<String, dynamic>.from(decoded as Map);
  }

  /// §10ho: one of the library's CORE functions, by name, on JSON — `mergeRegisters`
  /// for the shared route. Handle-free: the same rule the fixtures pin, not a Dart copy.
  static Map<String, dynamic> call(String fn, Map<String, dynamic> args) {
    final fnC = fn.toNativeUtf8();
    final argsC = jsonEncode(args).toNativeUtf8();
    final resPtr = _callFn(fnC, argsC);
    malloc.free(fnC);
    malloc.free(argsC);
    if (resPtr == ffi.nullptr) throw Exception('call failed');
    final resStr = resPtr.toDartString();
    _free(resPtr);
    final decoded = jsonDecode(resStr);
    if (decoded is Map && decoded.containsKey('error')) {
      throw Exception(_reason(decoded));
    }
    return Map<String, dynamic>.from(decoded as Map);
  }

  /// §10hj: keep an answer ({"columns","rows"}) in an on-demand table; `scope` names the
  /// area the answer is complete for (rows held there and absent from it are deleted).
  int ingest(
      String table, Map<String, dynamic> answer, Map<String, dynamic>? scope) {
    final tableC = table.toNativeUtf8();
    final answerC =
        jsonEncode({'columns': answer['columns'], 'rows': answer['rows']})
            .toNativeUtf8();
    final scopeC = (scope == null ? '' : jsonEncode(scope)).toNativeUtf8();
    final resPtr = _ingest(_handle, tableC, answerC, scopeC);
    malloc.free(tableC);
    malloc.free(answerC);
    malloc.free(scopeC);
    if (resPtr == ffi.nullptr) throw Exception('ingest failed');
    final resStr = resPtr.toDartString();
    _free(resPtr);
    final decoded = jsonDecode(resStr);
    if (decoded is Map && decoded.containsKey('error')) {
      throw Exception(_reason(decoded));
    }
    return (decoded['applied'] as num).toInt();
  }

  List<String> join(String tenant) => _membership(_join, tenant);

  /// Stop following a tenant: its rows, watermarks and tail go.
  List<String> leave(String tenant) => _membership(_leave, tenant);

  List<String> _membership(ZbClientTenantDart fn, String tenant) {
    final tenantC = tenant.toNativeUtf8();
    final resPtr = fn(_handle, tenantC);
    malloc.free(tenantC);
    if (resPtr == ffi.nullptr) throw Exception('membership call failed');
    final resStr = resPtr.toDartString();
    _free(resPtr);
    final decoded = jsonDecode(resStr);
    if (decoded is Map && decoded.containsKey('error')) {
      throw Exception(_reason(decoded));
    }
    return List<String>.from(decoded['tenants'] ?? const []);
  }

  Map<String, dynamic> flush(int waitMs) {
    final resPtr = _flush(_handle, waitMs);
    if (resPtr == ffi.nullptr) throw Exception('flush failed');
    final resStr = resPtr.toDartString();
    _free(resPtr);

    final decoded = jsonDecode(resStr);
    if (decoded is Map && decoded.containsKey('error')) {
      throw Exception(_reason(decoded));
    }
    return decoded;
  }
}
