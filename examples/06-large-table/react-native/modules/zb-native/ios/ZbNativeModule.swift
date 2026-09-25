import ExpoModulesCore

/// libzb behind the Expo Modules API: the same C client the Flutter app loads over
/// dart:ffi. Each call is one blocking C call, run on a serial queue of its own —
/// never the JS thread, and never two calls on one client at once. JS gets JSON text
/// back and parses it; the handle travels as a string (a u64 does not fit a JS number).
public class ZbNativeModule: Module {
  private let queue = DispatchQueue(label: "zebridge.libzb", qos: .userInitiated)

  public func definition() -> ModuleDefinition {
    Name("ZbNative")

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
