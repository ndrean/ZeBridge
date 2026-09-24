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

typedef ZbClientFlushC = ffi.Pointer<Utf8> Function(
    ffi.Uint64 handle, ffi.Uint64 waitMs);
typedef ZbClientFlushDart = ffi.Pointer<Utf8> Function(
    int handle, int waitMs);

typedef ZbClientPollC = ffi.Pointer<Utf8> Function(
    ffi.Uint64 handle, ffi.Uint64 waitMs);
typedef ZbClientPollDart = ffi.Pointer<Utf8> Function(
    int handle, int waitMs);

typedef ZbFreeC = ffi.Void Function(ffi.Pointer<Utf8> p);
typedef ZbFreeDart = void Function(ffi.Pointer<Utf8> p);

typedef ZbLastErrorC = ffi.Pointer<Utf8> Function();
typedef ZbLastErrorDart = ffi.Pointer<Utf8> Function();

class PollReport {
  final int applied;
  final int settled;
  final List<String> changedTables;
  final List<String> seeded;
  /// Set by the worker when a poll itself failed (connection gone, principal
  /// revoked): the loop backs off and says why; the lists are then empty.
  final String? error;

  PollReport(
      {required this.applied,
      required this.settled,
      required this.changedTables,
      required this.seeded,
      this.error});

  factory PollReport.fromJson(Map<String, dynamic> json) {
    return PollReport(
      applied: json['applied'] ?? 0,
      settled: json['settled'] ?? 0,
      changedTables: List<String>.from(json['changed_tables'] ?? []),
      seeded: List<String>.from(json['seeded'] ?? []),
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
  static late ZbClientPollDart _poll;
  static late ZbFreeDart _free;
  static late ZbLastErrorDart _lastError;

  static void init() {
    if (Platform.isIOS) {
      // libzbcore.a is linked INTO the Runner (ios/Flutter/*.xcconfig: `-force_load`
      // on the slice in ios/libzb/libzb.xcframework), so its symbols are the
      // process's own. Built with `zig build lib -Dtarget=aarch64-ios -Dvendor=true
      // -Dlibpq=false` — sqlite and zstd vendored, no libpq (NOTES §10iq/§10iy).
      _lib = ffi.DynamicLibrary.process();
    } else if (Platform.isAndroid) {
      // android/app/src/main/jniLibs/arm64-v8a/libzbcore.so: libzb's static archive
      // linked into a shared library by the NDK's clang (tool/build-libzb-android.sh)
      // — Zig cannot synthesise Android's libc, §10ir.
      _lib = ffi.DynamicLibrary.open('libzbcore.so');
    } else if (Platform.isMacOS) {
      _lib = ffi.DynamicLibrary.open(
          '/Users/nevendrean/code/zig/ZeBridge/libzb/zig-out/lib/libzbcore.dylib');
    } else {
      throw Exception('Unsupported platform');
    }

    _open = _lib.lookupFunction<ZbClientOpenC, ZbClientOpenDart>(
        'zb_client_connect');
    _close = _lib.lookupFunction<ZbClientCloseC, ZbClientCloseDart>(
        'zb_client_close');
    _sync = _lib.lookupFunction<ZbClientSyncC, ZbClientSyncDart>(
        'zb_client_sync');
    _query = _lib.lookupFunction<ZbClientQueryC, ZbClientQueryDart>(
        'zb_client_query');
    _mutate = _lib.lookupFunction<ZbClientMutateC, ZbClientMutateDart>(
        'zb_client_mutate');
    _flush = _lib.lookupFunction<ZbClientFlushC, ZbClientFlushDart>(
        'zb_client_flush_outbox');
    _poll = _lib.lookupFunction<ZbClientPollC, ZbClientPollDart>(
        'zb_client_poll');
    _free = _lib.lookupFunction<ZbFreeC, ZbFreeDart>('zb_free');
    _lastError = _lib.lookupFunction<ZbLastErrorC, ZbLastErrorDart>('zb_last_error');
  }

  late int _handle;

  ZeBridge(Map<String, dynamic> options) {
    final optsStr = jsonEncode(options);
    final optsC = optsStr.toNativeUtf8();
    _handle = _open(optsC);
    malloc.free(optsC);

    if (_handle == 0) {
      // §10iz: the words behind the 0 — libzb keeps them for zb_last_error(). Before
      // this an iPhone showed "Failed to open ZeBridge client" for a missing
      // local-network permission and nothing said so.
      final why = _lastError();
      throw Exception(why == ffi.nullptr ? 'zb_client_connect failed' : why.toDartString());
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
