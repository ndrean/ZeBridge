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

    // A pure rule of libzb's core (mergeRegisters, …): no client, no I/O, so it runs on
    // the JS thread and answers at once.
    Function("call") { (fn: String, args: String) throws -> String in
      try Self.take(zb_call(fn, args), "zb_call")
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

    AsyncFunction("close") { (h: String) throws -> Int in
      Int(zb_client_close(try Self.handle(h)))
    }.runOnQueue(queue)
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
