package dev.zebridge.zbnative

import dev.zebridge.Native
import expo.modules.kotlin.Promise
import expo.modules.kotlin.modules.Module
import expo.modules.kotlin.modules.ModuleDefinition
import java.util.concurrent.Executors

/**
 * libzb behind the Expo Modules API, on Android: the same functions as the iOS module
 * (ZbNativeModule.swift), over zb-android's JNI binding. Each call is one blocking C call
 * on a single thread of its own — never the JS thread, never two calls on one client at
 * once. Strings cross as UTF-8 bytes, JSON comes back as text, and the handle travels as a
 * string (a u64 does not fit a JS number).
 */
class ZbNativeModule : Module() {
  private val worker = Executors.newSingleThreadExecutor { r -> Thread(r, "zebridge.libzb") }

  override fun definition() = ModuleDefinition {
    Name("ZbNative")

    Function("abiVersion") { Native.abiVersion() }

    // A pure rule of libzb's core (mergeRegisters, …): no client, no I/O, so it runs on
    // the JS thread and answers at once.
    Function("call") { fn: String, args: String -> take(Native.call(fn.toByteArray(), args.toByteArray()), "zb_call") }

    AsyncFunction("connect") { opts: String, promise: Promise ->
      run(promise) {
        val h = Native.connect(opts.toByteArray())
        if (h == 0L) throw ZbError(lastError() ?: "zb_client_connect failed")
        java.lang.Long.toUnsignedString(h)
      }
    }

    AsyncFunction("sync") { h: String, promise: Promise -> run(promise) { take(Native.sync(handle(h)), "zb_client_sync") } }
    AsyncFunction("poll") { h: String, waitMs: Double, promise: Promise ->
      run(promise) { take(Native.poll(handle(h), maxOf(0.0, waitMs).toLong()), "zb_client_poll") }
    }
    AsyncFunction("query") { h: String, sql: String, params: String, promise: Promise ->
      run(promise) { take(Native.query(handle(h), sql.toByteArray(), params.toByteArray()), "zb_client_query") }
    }
    AsyncFunction("mutate") { h: String, table: String, op: String, key: String, values: String, promise: Promise ->
      run(promise) { take(Native.mutate(handle(h), table.toByteArray(), op.toByteArray(), key.toByteArray(), values.toByteArray(), null), "zb_client_mutate") }
    }
    AsyncFunction("request") { h: String, subject: String, payload: String, timeoutMs: Double, promise: Promise ->
      run(promise) { take(Native.request(handle(h), subject.toByteArray(), payload.toByteArray(), maxOf(0.0, timeoutMs).toLong()), "zb_client_request") }
    }
    AsyncFunction("flushOutbox") { h: String, waitMs: Double, promise: Promise ->
      run(promise) { take(Native.flushOutbox(handle(h), maxOf(0.0, waitMs).toLong()), "zb_client_flush_outbox") }
    }
    AsyncFunction("stamp") { h: String, promise: Promise -> run(promise) { take(Native.stamp(handle(h)), "zb_client_stamp") } }
    AsyncFunction("close") { h: String, promise: Promise -> run(promise) { Native.close(handle(h)) } }

    // NOT on the worker: the one call libzb allows from any thread. A poll may hold the
    // worker for its whole wait; this ends the wait, so the call queued behind it runs.
    Function("wake") { h: String -> Native.wake(handle(h)) }
  }

  private fun run(promise: Promise, call: () -> Any?) {
    worker.execute {
      try {
        promise.resolve(call())
      } catch (e: Throwable) {
        promise.reject("ZB_LIBZB", e.message ?: e.toString(), e)
      }
    }
  }

  private class ZbError(message: String) : Exception(message)

  private fun handle(h: String): Long {
    val v = h.toULongOrNull() ?: throw ZbError("not a libzb handle: $h")
    if (v == 0UL) throw ZbError("not a libzb handle: $h")
    return v.toLong()
  }

  private fun take(bytes: ByteArray?, call: String): String =
    bytes?.toString(Charsets.UTF_8) ?: throw ZbError(lastError() ?: "$call returned nothing")

  private fun lastError(): String? = Native.lastError()?.toString(Charsets.UTF_8)
}
