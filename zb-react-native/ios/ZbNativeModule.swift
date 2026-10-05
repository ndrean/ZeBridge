import ExpoModulesCore

/// libzb behind the Expo Modules API: the same C client the Flutter app loads over
/// dart:ffi. Each call is one blocking C call, run on a serial queue of its own —
/// never the JS thread, and never two calls on one client at once. JS gets JSON text
/// back and parses it; the handle travels as a string (a u64 does not fit a JS number).
public class ZbNativeModule: Module {
  private let queue = DispatchQueue(label: "zebridge.libzb", qos: .userInitiated)

  public func definition() -> ModuleDefinition {
    Name("ZbNative")

    // The ABI of the libzb built into this app (libzb/abi.json); src/index.ts compares it
    // with the one its code was written for before the first call.
    Function("abiVersion") { () -> Int in
      Int(zb_abi_version())
    }

    // Diagnostics: libzb speaks on stderr (seeded, gap healed, seed anchor, pruned under)
    // and an app has no terminal. Appends stderr to `path`, line-buffered, for the rest of
    // the process; `trace` turns on ZB_TAIL_TRACE (a line per fetch and per applied batch)
    // for clients connected after this call.
    Function("captureStderr") { (path: String, trace: Bool) -> Bool in
      if trace { setenv("ZB_TAIL_TRACE", "1", 1) }
      guard freopen(path, "a", stderr) != nil else { return false }
      setvbuf(stderr, nil, _IOLBF, 0)
      return true
    }

    AsyncFunction("connect") { (opts: String) throws -> String in
      let h = zb_client_connect(opts)
      if h == 0 { throw ZbException(Self.lastError() ?? "zb_client_connect failed") }
      return String(h)
    }.runOnQueue(queue)

    AsyncFunction("sync") { (h: String) throws -> String in
      try Self.take(zb_client_sync(try Self.handle(h)), "zb_client_sync")
    }.runOnQueue(queue)

    AsyncFunction("poll") { (h: String, waitMs: Double) throws -> String in
      try Self.take(zb_client_poll(try Self.handle(h), UInt64(max(0, waitMs))), "zb_client_poll")
    }.runOnQueue(queue)

    AsyncFunction("query") { (h: String, sql: String, params: String) throws -> String in
      try Self.take(zb_client_query(try Self.handle(h), sql, params), "zb_client_query")
    }.runOnQueue(queue)

    AsyncFunction("mutate") { (h: String, table: String, op: String, key: String, values: String) throws -> String in
      try Self.take(zb_client_mutate(try Self.handle(h), table, op, key, values), "zb_client_mutate")
    }.runOnQueue(queue)

    AsyncFunction("request") { (h: String, subject: String, payload: String, timeoutMs: Double) throws -> String in
      try Self.take(zb_client_request(try Self.handle(h), subject, payload, UInt64(max(0, timeoutMs))), "zb_client_request")
    }.runOnQueue(queue)

    AsyncFunction("flushOutbox") { (h: String, waitMs: Double) throws -> String in
      try Self.take(zb_client_flush_outbox(try Self.handle(h), UInt64(max(0, waitMs))), "zb_client_flush_outbox")
    }.runOnQueue(queue)

    AsyncFunction("stamp") { (h: String) throws -> String in
      try Self.take(zb_client_stamp(try Self.handle(h)), "zb_client_stamp")
    }.runOnQueue(queue)

    // NOT on the queue: the one call libzb allows from any thread. A poll may be holding
    // the queue for its whole wait; this ends the wait, so the call queued behind it runs.
    Function("wake") { (h: String) throws -> Int in
      Int(zb_client_wake(try Self.handle(h)))
    }

    // Zig reads no trust store on iOS: libzb checks https:// and tls:// against a PEM file
    // (`caFile`). The module ships Apple's roots (scripts/build-ios.sh exports them), and
    // src/index.ts passes this path when the app names none.
    Function("defaultCaFile") { () -> String? in
      guard let url = Bundle(for: ZbNativeModule.self).url(forResource: "ZbNativeRoots", withExtension: "bundle"),
            let bundle = Bundle(url: url) else { return nil }
      return bundle.path(forResource: "roots", ofType: "pem")
    }

    AsyncFunction("close") { (h: String) throws -> Int in
      Int(zb_client_close(try Self.handle(h)))
    }.runOnQueue(queue)

    // libzb's zstd for zb-client-ts's own decoder (`zstdDecompressStream`), on the JS
    // thread: a chunk inflates in about a millisecond. Bytes cross only as arguments,
    // which JSI hands over without a copy: `zstdPush` inflates into a buffer kept here
    // and returns its length, JS allocates that and `zstdTake` fills it.
    Function("zstdNew") { () throws -> Int in
      guard let ctx = zb_zstd_new() else { throw ZbException("zb_zstd_new failed") }
      let id = self.nextZstd
      self.nextZstd += 1
      self.zstd[id] = ZstdState(ctx: ctx)
      return id
    }

    Function("zstdPush") { (id: Int, chunk: Uint8Array) throws -> Int in
      guard let st = self.zstd[id] else { throw ZbException("no zstd stream \(id)") }
      st.drop()
      var out: UnsafeMutablePointer<UInt8>? = nil
      var n = 0
      let src = chunk.rawPointer.assumingMemoryBound(to: UInt8.self)
      if zb_zstd_push(st.ctx, src, chunk.byteLength, &out, &n) != 0 {
        throw ZbException(Self.lastError() ?? "zb_zstd_push failed")
      }
      st.buf = out
      st.len = n
      return n
    }

    Function("zstdTake") { (id: Int, dest: Uint8Array) throws in
      guard let st = self.zstd[id], let buf = st.buf else { throw ZbException("nothing to take from zstd stream \(id)") }
      guard dest.byteLength >= st.len else { throw ZbException("zstdTake: \(dest.byteLength) bytes for \(st.len)") }
      dest.rawPointer.copyMemory(from: buf, byteCount: st.len)
      st.drop()
    }

    Function("zstdFree") { (id: Int) in
      if let st = self.zstd.removeValue(forKey: id) {
        st.drop()
        zb_zstd_free(st.ctx)
      }
    }
  }

  private var zstd: [Int: ZstdState] = [:]
  private var nextZstd = 1

  private final class ZstdState {
    let ctx: UnsafeMutableRawPointer
    var buf: UnsafeMutablePointer<UInt8>? = nil
    var len = 0
    init(ctx: UnsafeMutableRawPointer) { self.ctx = ctx }
    func drop() {
      if let b = buf { zb_free(UnsafeMutableRawPointer(b).assumingMemoryBound(to: CChar.self)) }
      buf = nil
      len = 0
    }
  }

  private static func handle(_ h: String) throws -> UInt64 {
    guard let v = UInt64(h), v != 0 else { throw ZbException("not a libzb handle: \(h)") }
    return v
  }

  /// libzb's strings are malloc'd on its side: copy, then hand back with zb_free.
  private static func take(_ p: UnsafeMutablePointer<CChar>?, _ call: String) throws -> String {
    guard let p else { throw ZbException(lastError() ?? "\(call) returned nothing") }
    defer { zb_free(p) }
    return String(cString: p)
  }

  private static func lastError() -> String? {
    guard let p = zb_last_error() else { return nil }
    return String(cString: p)
  }
}

final class ZbException: GenericException<String> {
  override var reason: String { param }
}
