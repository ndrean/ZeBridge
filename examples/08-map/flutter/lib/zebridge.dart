import 'dart:ffi' as ffi;
import 'dart:convert';
import 'package:ffi/ffi.dart';
import 'dart:io';

typedef zb_client_open_C = ffi.Uint64 Function(ffi.Pointer<Utf8> opts_json);
typedef zb_client_open_Dart = int Function(ffi.Pointer<Utf8> opts_json);

typedef zb_client_close_C = ffi.Int32 Function(ffi.Uint64 handle);
typedef zb_client_close_Dart = int Function(int handle);

typedef zb_client_sync_C = ffi.Pointer<Utf8> Function(ffi.Uint64 handle);
typedef zb_client_sync_Dart = ffi.Pointer<Utf8> Function(int handle);

typedef zb_client_query_C = ffi.Pointer<Utf8> Function(
    ffi.Uint64 handle, ffi.Pointer<Utf8> sql, ffi.Pointer<Utf8> params);
typedef zb_client_query_Dart = ffi.Pointer<Utf8> Function(
    int handle, ffi.Pointer<Utf8> sql, ffi.Pointer<Utf8> params);

typedef zb_client_mutate_C = ffi.Pointer<Utf8> Function(
    ffi.Uint64 handle,
    ffi.Pointer<Utf8> table,
    ffi.Pointer<Utf8> op,
    ffi.Pointer<Utf8> key_json,
    ffi.Pointer<Utf8> values_json);
typedef zb_client_mutate_Dart = ffi.Pointer<Utf8> Function(
    int handle,
    ffi.Pointer<Utf8> table,
    ffi.Pointer<Utf8> op,
    ffi.Pointer<Utf8> key_json,
    ffi.Pointer<Utf8> values_json);

typedef zb_client_tenant_C = ffi.Pointer<Utf8> Function(
    ffi.Uint64 handle, ffi.Pointer<Utf8> tenant);
typedef zb_client_tenant_Dart = ffi.Pointer<Utf8> Function(
    int handle, ffi.Pointer<Utf8> tenant);

typedef zb_client_flush_C = ffi.Pointer<Utf8> Function(
    ffi.Uint64 handle, ffi.Uint64 wait_ms);
typedef zb_client_flush_Dart = ffi.Pointer<Utf8> Function(
    int handle, int wait_ms);

typedef zb_client_poll_C = ffi.Pointer<Utf8> Function(
    ffi.Uint64 handle, ffi.Uint64 wait_ms);
typedef zb_client_poll_Dart = ffi.Pointer<Utf8> Function(
    int handle, int wait_ms);

typedef zb_client_request_C = ffi.Pointer<Utf8> Function(
    ffi.Uint64 handle, ffi.Pointer<Utf8> subject, ffi.Pointer<Utf8> payload_json, ffi.Uint64 timeout_ms);
typedef zb_client_request_Dart = ffi.Pointer<Utf8> Function(
    int handle, ffi.Pointer<Utf8> subject, ffi.Pointer<Utf8> payload_json, int timeout_ms);

typedef zb_client_ingest_C = ffi.Pointer<Utf8> Function(
    ffi.Uint64 handle, ffi.Pointer<Utf8> table, ffi.Pointer<Utf8> answer_json, ffi.Pointer<Utf8> scope_json);
typedef zb_client_ingest_Dart = ffi.Pointer<Utf8> Function(
    int handle, ffi.Pointer<Utf8> table, ffi.Pointer<Utf8> answer_json, ffi.Pointer<Utf8> scope_json);

typedef zb_free_C = ffi.Void Function(ffi.Pointer<Utf8> p);
typedef zb_free_Dart = void Function(ffi.Pointer<Utf8> p);

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
  static late zb_client_open_Dart _open;
  static late zb_client_close_Dart _close;
  static late zb_client_sync_Dart _sync;
  static late zb_client_query_Dart _query;
  static late zb_client_mutate_Dart _mutate;
  static late zb_client_flush_Dart _flush;
  static late zb_client_tenant_Dart _join;
  static late zb_client_tenant_Dart _leave;
  static late zb_client_poll_Dart _poll;
  static late zb_client_request_Dart _request;
  static late zb_client_ingest_Dart _ingest;
  static late zb_free_Dart _free;

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

    _open = _lib.lookupFunction<zb_client_open_C, zb_client_open_Dart>(
        'zb_client_open');
    _close = _lib.lookupFunction<zb_client_close_C, zb_client_close_Dart>(
        'zb_client_close');
    _sync = _lib.lookupFunction<zb_client_sync_C, zb_client_sync_Dart>(
        'zb_client_sync');
    _query = _lib.lookupFunction<zb_client_query_C, zb_client_query_Dart>(
        'zb_client_query');
    _mutate = _lib.lookupFunction<zb_client_mutate_C, zb_client_mutate_Dart>(
        'zb_client_mutate');
    _flush = _lib.lookupFunction<zb_client_flush_C, zb_client_flush_Dart>(
        'zb_client_flush');
    _join = _lib.lookupFunction<zb_client_tenant_C, zb_client_tenant_Dart>(
        'zb_client_join');
    _leave = _lib.lookupFunction<zb_client_tenant_C, zb_client_tenant_Dart>(
        'zb_client_leave');
    _poll = _lib.lookupFunction<zb_client_poll_C, zb_client_poll_Dart>(
        'zb_client_poll');
    _request = _lib.lookupFunction<zb_client_request_C, zb_client_request_Dart>(
        'zb_client_request');
    _ingest = _lib.lookupFunction<zb_client_ingest_C, zb_client_ingest_Dart>(
        'zb_client_ingest');
    _free = _lib.lookupFunction<zb_free_C, zb_free_Dart>('zb_free');
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
  Map<String, dynamic> request(String subject, Map<String, dynamic> payload, int timeoutMs) {
    final subjectC = subject.toNativeUtf8();
    final payloadC = jsonEncode(payload).toNativeUtf8();
    final resPtr = _request(_handle, subjectC, payloadC, timeoutMs);
    malloc.free(subjectC);
    malloc.free(payloadC);
    if (resPtr == ffi.nullptr) throw Exception('request failed');
    final resStr = resPtr.toDartString();
    _free(resPtr);
    final decoded = jsonDecode(resStr);
    if (decoded is Map && decoded.containsKey('error')) throw Exception(_reason(decoded));
    return Map<String, dynamic>.from(decoded as Map);
  }

  /// §10hj: keep an answer ({"columns","rows"}) in an on-demand table; `scope` names the
  /// area the answer is complete for (rows held there and absent from it are deleted).
  int ingest(String table, Map<String, dynamic> answer, Map<String, dynamic>? scope) {
    final tableC = table.toNativeUtf8();
    final answerC = jsonEncode({'columns': answer['columns'], 'rows': answer['rows']}).toNativeUtf8();
    final scopeC = (scope == null ? '' : jsonEncode(scope)).toNativeUtf8();
    final resPtr = _ingest(_handle, tableC, answerC, scopeC);
    malloc.free(tableC);
    malloc.free(answerC);
    malloc.free(scopeC);
    if (resPtr == ffi.nullptr) throw Exception('ingest failed');
    final resStr = resPtr.toDartString();
    _free(resPtr);
    final decoded = jsonDecode(resStr);
    if (decoded is Map && decoded.containsKey('error')) throw Exception(_reason(decoded));
    return (decoded['applied'] as num).toInt();
  }

  List<String> join(String tenant) => _membership(_join, tenant);

  /// Stop following a tenant: its rows, watermarks and tail go.
  List<String> leave(String tenant) => _membership(_leave, tenant);

  List<String> _membership(zb_client_tenant_Dart fn, String tenant) {
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
